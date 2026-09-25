"""
fund-scanner/app.py
Flask backend for the Poppa Alpha Fundamentals tab.

Endpoints:
  GET  /api/health                       — health check
  GET  /api/stats                        — cache status & last scan times
  GET  /api/results?universe=portfolio   — return cached scan results
  POST /api/scan  {"universe":"portfolio"}  — trigger async re-scan
  GET  /api/earnings?tickers=AAPL,MSFT   — earnings beat/miss per quarter (DB-first)
  GET  /api/political-trades             — congressional trading disclosures (DB)
"""

import threading
import time
import logging
import concurrent.futures
import hmac
import hashlib
import os
from datetime import datetime, timezone, timedelta, date

from flask import Flask, jsonify, request
from flask_cors import CORS

import scanner as _scanner
from scanner import (
    scan_tickers, _load_universes_from_db,
    _fetch_earnings_surprises, _fetch_earnings_calendar,
    _fetch_quote, _fetch_price_target, _fetch_ratios, _fetch_political_trades,
    backfill_trade_prices, refresh_last_prices,
    fetch_political_trades_historical,
    fetch_forward_prices,
)


def _purge_cloudflare_cache():
    """Purge all Cloudflare cached content after nightly DB writes."""
    import os, requests as _req
    token = os.environ.get('CF_API_TOKEN', '')
    zone  = '438f201792c897e59eca74ea26c35c1c'
    if not token:
        print('[CF purge] CF_API_TOKEN not set — skipping purge')
        return
    try:
        r = _req.post(
            f'https://api.cloudflare.com/client/v4/zones/{zone}/purge_cache',
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            json={'purge_everything': True},
            timeout=15
        )
        if r.ok:
            print('[CF purge] Cache purged successfully')
        else:
            print(f'[CF purge] FAILED: {r.status_code} {r.text[:200]}')
    except Exception as e:
        print(f'[CF purge] ERROR: {e}')

def _get_universes():
    """Always fetch latest universe definitions from the shared DB."""
    _scanner.UNIVERSES = _load_universes_from_db()
    return _scanner.UNIVERSES

UNIVERSES = _get_universes()
import db as _db

def _resolve_universe_tickers(universe_key: str) -> list:
    """Resolve tickers for a universe key from the shared DB (single source of truth)."""
    universes = _load_universes_from_db()
    return universes.get(universe_key, [])

# ── Setup ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Initialise DB (creates tables if missing)
try:
    _db.init_db()
except Exception as _e:
    logger.error("DB init failed: %s", _e)

# ── Auth ──────────────────────────────────────────────────────────────────────
_OSPREY_SECRET   = os.environ.get("OSPREY_SECRET",   "osprey-secret-change-me")
_OSPREY_PASSWORD = os.environ.get("OSPREY_PASSWORD",  "changeme")

def _make_token(password: str) -> str:
    return hmac.new(_OSPREY_SECRET.encode(), password.encode(), hashlib.sha256).hexdigest()

_VALID_TOKEN = _make_token(_OSPREY_PASSWORD)

@app.before_request
def check_auth():
    if request.method == "OPTIONS":
        return None
    if request.path in ("/api/health", "/api/political-trades/debug", "/api/political-trades/clear", "/api/political-trades/backfill-sectors", "/api/political-trades/backfill-prices", "/api/political-trades/refresh-last-prices", "/api/political-trades/backfill-historical", "/api/political-trades/backfill-forward-prices", "/api/political-trades/reset-prices", "/api/political-trades/backfill-year", "/api/political-trades/years", "/api/political-trades/months"):
        return None
    if request.path.startswith("/api/"):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Unauthorized"}), 401
        if not hmac.compare_digest(auth[7:], _VALID_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401
    return None

# ── In-memory cache ───────────────────────────────────────────────────────────
CACHE: dict = {}
SCANNING: set = set()
_cache_lock = threading.Lock()

# All universes scanned in the daily 4pm ET run (small → large)
SCAN_ORDER = [
    "portfolio", "myportfolio", "aiinfra", "cybersec",
    "rareearths", "energy", "orbital",
    "smh", "soxx", "ndx100", "sp500",
]

# All unique tickers across all universes
def _all_tickers() -> set:
    return {t for tickers in UNIVERSES.values() for t in tickers}


# ── Scanner logic ─────────────────────────────────────────────────────────────

def _run_scan(universe_key: str, save_to_db: bool = False, tickers: list = None):
    """Fetch fundamentals for one universe and update cache. Thread-safe."""
    if universe_key in SCANNING:
        logger.info("Already scanning %s — skipping", universe_key)
        return
    if not tickers:
        tickers = _resolve_universe_tickers(universe_key)
    if not tickers:
        logger.warning("Unknown universe and no tickers provided: %s", universe_key)
        return

    SCANNING.add(universe_key)
    logger.info("Starting scan: %s (%d tickers)%s", universe_key, len(tickers),
                " [EOD — will save to DB]" if save_to_db else "")
    try:
        results = scan_tickers(tickers)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with _cache_lock:
            CACHE[universe_key] = {
                "results":    results,
                "scanned_at": now,
                "count":      len(results),
                "universe":   universe_key,
            }
        logger.info("Done: %s — %d results", universe_key, len(results))
        if save_to_db:
            try:
                _db.save_snapshot(universe_key, results)
            except Exception as db_exc:
                logger.error("DB save error (%s): %s", universe_key, db_exc)
    except Exception as exc:
        logger.error("Scan error (%s): %s", universe_key, exc)
    finally:
        SCANNING.discard(universe_key)


def _next_4pm_eastern() -> float:
    """Return seconds until the next 4:00 PM Eastern time."""
    now_utc = datetime.now(timezone.utc)
    eastern_offset = timedelta(hours=-5)
    now_et = now_utc + eastern_offset
    target = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_et >= target:
        target += timedelta(days=1)
    delta = (target - now_et).total_seconds()
    logger.info("Next 4pm ET scan in %.0f seconds (%.1f hours)", delta, delta / 3600)
    return delta


def _refresh_earnings_calendar():
    """Fetch the next 3 weeks of earnings dates from FMP and upsert into report_calendar."""
    try:
        today     = date.today()
        from_date = today.strftime("%Y-%m-%d")
        to_date   = (today + timedelta(days=21)).strftime("%Y-%m-%d")
        all_tkrs  = _all_tickers()
        entries   = _fetch_earnings_calendar(from_date, to_date)
        # Filter to only tickers we track
        entries   = [e for e in entries if e["ticker"] in all_tkrs]
        inserted  = _db.upsert_calendar(entries)
        logger.info("Calendar refresh done — %d entries in window, %d new", len(entries), inserted)
    except Exception as exc:
        logger.error("Calendar refresh error: %s", exc)


def _fetch_and_persist_earnings(tickers: list, label: str = ""):
    """Fetch earnings surprises for a list of tickers and upsert to DB."""
    if not tickers:
        return
    logger.info("Earnings fetch+persist (%s): %d tickers", label, len(tickers))
    ok = err = 0
    for ticker in tickers:
        try:
            rows = _fetch_earnings_surprises(ticker, limit=8)
            if rows:
                _db.upsert_earnings(ticker, rows)
                ok += 1
            time.sleep(0.15)   # stay within FMP rate limits
        except Exception as exc:
            logger.warning("Earnings fetch error %s: %s", ticker, exc)
            err += 1
    logger.info("Earnings fetch+persist (%s) done — ok=%d err=%d", label, ok, err)


def _earnings_backfill():
    """One-time backfill — runs in a background thread if earnings table is empty."""
    logger.info("=== Earnings initial backfill starting ===")
    _fetch_and_persist_earnings(sorted(_all_tickers()), label="backfill")
    logger.info("=== Earnings initial backfill complete ===")


# ── Technicals & Options EOD helpers ─────────────────────────────────────────

def _sma(closes: list, period: int):
    if len(closes) < period:
        return None
    return round(sum(closes[-period:]) / period, 4)

def _ema(closes: list, period: int):
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    val = sum(closes[:period]) / period
    for p in closes[period:]:
        val = p * k + val * (1 - k)
    return round(val, 4)

def _rsi(closes: list, period: int = 14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 2)


def _adx(highs: list, lows: list, closes: list, period: int = 14):
    """
    Wilder-smoothed ADX, mirroring techCalcADX from osprey.html exactly.
    Returns (adx, plus_di, minus_di, prev_plus_di, prev_minus_di).
    All values as float % (e.g. 25.3), or (None, …) if insufficient data.
    """
    n = len(closes)
    if n < period * 2:
        return None, None, None, None, None

    tr_arr, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        hl  = highs[i] - lows[i]
        hpc = abs(highs[i] - closes[i - 1])
        lpc = abs(lows[i]  - closes[i - 1])
        tr_arr.append(max(hl, hpc, lpc))

        up_move   = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move   if up_move   > down_move and up_move   > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move  and down_move > 0 else 0.0)

    sm_tr  = sum(tr_arr[:period])
    sm_pdm = sum(plus_dm[:period])
    sm_mdm = sum(minus_dm[:period])

    di_plus, di_minus, dx_arr = [], [], []
    pdi = (sm_pdm / sm_tr * 100) if sm_tr > 0 else 0.0
    mdi = (sm_mdm / sm_tr * 100) if sm_tr > 0 else 0.0
    di_plus.append(pdi)
    di_minus.append(mdi)
    dx_arr.append(abs(pdi - mdi) / (pdi + mdi) * 100 if (pdi + mdi) > 0 else 0.0)

    for i in range(period, len(tr_arr)):
        sm_tr  = sm_tr  - sm_tr  / period + tr_arr[i]
        sm_pdm = sm_pdm - sm_pdm / period + plus_dm[i]
        sm_mdm = sm_mdm - sm_mdm / period + minus_dm[i]

        pdi = (sm_pdm / sm_tr * 100) if sm_tr > 0 else 0.0
        mdi = (sm_mdm / sm_tr * 100) if sm_tr > 0 else 0.0
        di_plus.append(pdi)
        di_minus.append(mdi)
        dx_arr.append(abs(pdi - mdi) / (pdi + mdi) * 100 if (pdi + mdi) > 0 else 0.0)

    if len(dx_arr) < period:
        return None, None, None, None, None

    adx_val = sum(dx_arr[:period]) / period
    for i in range(period, len(dx_arr)):
        adx_val = (adx_val * (period - 1) + dx_arr[i]) / period

    last = len(di_plus) - 1
    if last < 1:
        return None, None, None, None, None

    return (
        round(adx_val,        2),
        round(di_plus[last],  2),
        round(di_minus[last], 2),
        round(di_plus[last - 1],  2),
        round(di_minus[last - 1], 2),
    )


def _atr(highs: list, lows: list, closes: list, period: int = 14) -> float | None:
    """ATR-{period}, mirroring techCalcATR from osprey.html."""
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        ))
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return round(atr, 4)


def _tradier_history(ticker: str, tradier_key: str, days: int = 310) -> dict | None:
    """
    Fetch daily OHLCV history from Tradier for the past `days` calendar days.
    Mirrors the techFetch('/markets/history?...') call used by both
    maScanTicker (310d) and techScanTicker (120d) in osprey.html.
    Returns dict with lists: dates, opens, highs, lows, closes, volumes (oldest→newest).
    Returns None on failure.
    """
    import requests as _req
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=days)
    hdrs     = {"Authorization": f"Bearer {tradier_key}", "Accept": "application/json"}
    try:
        r = _req.get(
            "https://api.tradier.com/v1/markets/history",
            params={
                "symbol":   ticker,
                "interval": "daily",
                "start":    start_dt.strftime("%Y-%m-%d"),
                "end":      end_dt.strftime("%Y-%m-%d"),
            },
            headers=hdrs,
            timeout=15,
        )
        if not r.ok:
            return None
        hist = r.json().get("history") or {}
        if not hist or hist == "null":
            return None
        day_data = hist.get("day", [])
        if isinstance(day_data, dict):
            day_data = [day_data]
        if not day_data:
            return None
        return {
            "dates":   [d["date"]                   for d in day_data],
            "opens":   [float(d["open"])             for d in day_data],
            "highs":   [float(d["high"])             for d in day_data],
            "lows":    [float(d["low"])              for d in day_data],
            "closes":  [float(d["close"])            for d in day_data],
            "volumes": [int(d.get("volume") or 0)    for d in day_data],
        }
    except Exception as exc:
        logger.warning("_tradier_history %s: %s", ticker, exc)
        return None


def _fetch_technicals_for_ticker(ticker: str, tradier_key: str) -> dict | None:
    """
    Pull 310-day daily OHLCV from Tradier and compute all technicals.

    Mirrors the frontend exactly:
      • maScanTicker  (Moving Averages tab)  — SMA 20/50/200, cross_signal, alignment
      • techScanTicker (Market Strength tab) — RSI-14, ADX-14, +DI/-DI, di_cross, ATR-14, vol_ratio

    Uses Tradier /markets/history (same call as the frontend techFetch helper).
    TRADIER_API_KEY env var must be set; no yfinance dependency.
    """
    try:
        hist = _tradier_history(ticker, tradier_key, days=310)
        if not hist or len(hist["closes"]) < 10:
            return None

        closes  = hist["closes"]
        highs   = hist["highs"]
        lows    = hist["lows"]
        volumes = hist["volumes"]
        price   = closes[-1]

        # ── Moving Averages tab (mirrors maScanTicker) ────────────────────────
        sma_20  = _sma(closes, 20)
        sma_50  = _sma(closes, 50)
        sma_200 = _sma(closes, 200)
        ema_9   = _ema(closes, 9)
        ema_21  = _ema(closes, 21)

        def _pct_vs(ma):
            return round((price / ma - 1) * 100, 2) if ma and price else None

        vs20  = _pct_vs(sma_20)
        vs50  = _pct_vs(sma_50)
        vs200 = _pct_vs(sma_200)

        # cross_signal — mirrors maGetCross(sma50, sma200, sma50_prev, sma200_prev)
        cross_signal = None
        if sma_50 and sma_200:
            sma_50_prev  = _sma(closes[:-1], 50)
            sma_200_prev = _sma(closes[:-1], 200)
            if sma_50_prev and sma_200_prev:
                if   sma_50 > sma_200 and sma_50_prev <= sma_200_prev:
                    cross_signal = "golden_cross"
                elif sma_50 < sma_200 and sma_50_prev >= sma_200_prev:
                    cross_signal = "death_cross"
                elif sma_50 > sma_200:
                    cross_signal = "above_200"
                else:
                    cross_signal = "below_200"

        # alignment — mirrors maGetAlignment(price, sma20, sma50, sma200)
        alignment = None
        if sma_20 and sma_50 and sma_200:
            if   price > sma_20 and sma_20 > sma_50 and sma_50 > sma_200:
                alignment = "bull_stack"
            elif price < sma_20 and sma_20 < sma_50 and sma_50 < sma_200:
                alignment = "bear_stack"
            elif price > sma_200:
                alignment = "bullish"
            elif price < sma_200:
                alignment = "bearish"
            else:
                alignment = "mixed"

        # ── Market Strength tab (mirrors techScanTicker) ──────────────────────
        rsi_14 = _rsi(closes, 14)

        adx_val, plus_di, minus_di, prev_pdi, prev_mdi = _adx(highs, lows, closes, 14)

        # di_cross — mirrors techGetDICross(pdi, mdi, ppdi, pmdi)
        di_cross = None
        if plus_di is not None and minus_di is not None and prev_pdi is not None and prev_mdi is not None:
            if   plus_di > minus_di and prev_pdi <= prev_mdi:
                di_cross = "bull_cross"
            elif plus_di < minus_di and prev_pdi >= prev_mdi:
                di_cross = "bear_cross"
            elif plus_di > minus_di:
                di_cross = "bull"
            else:
                di_cross = "bear"

        atr_14 = _atr(highs, lows, closes, 14)

        # Volume metrics
        avg_vol_20d = _sma(volumes, 20)
        vol_ratio   = round(volumes[-1] / avg_vol_20d, 2) if avg_vol_20d and volumes else None

        # 52-week range
        high_52w = max(closes)
        low_52w  = min(closes)

        # Legacy boolean columns kept for backward-compat with existing rows
        golden_cross = (cross_signal == "golden_cross")
        death_cross  = (cross_signal == "death_cross")

        return {
            "ticker":            ticker,
            "sma_20":            sma_20,
            "sma_50":            sma_50,
            "sma_200":           sma_200,
            "ema_9":             ema_9,
            "ema_21":            ema_21,
            "price_vs_sma20":    vs20,
            "price_vs_sma50":    vs50,
            "price_vs_sma200":   vs200,
            "rsi_14":            rsi_14,
            "high_52w":          round(high_52w, 4),
            "low_52w":           round(low_52w,  4),
            "pct_from_high_52w": round((price / high_52w - 1) * 100, 2),
            "pct_from_low_52w":  round((price / low_52w  - 1) * 100, 2),
            "avg_vol_20d":       avg_vol_20d,
            "vol_ratio":         vol_ratio,
            "rel_strength_1m":   None,   # requires SPY benchmark — not available without yfinance
            "rel_strength_3m":   None,
            "rel_strength_6m":   None,
            "beta_30d":          None,
            "golden_cross":      golden_cross,
            "death_cross":       death_cross,
            # MA cross + alignment (Moving Averages tab)
            "cross_signal":      cross_signal,
            "alignment":         alignment,
            # ADX / DI (Market Strength tab)
            "adx":               adx_val,
            "plus_di":           plus_di,
            "minus_di":          minus_di,
            "di_cross":          di_cross,
            # ATR (Market Strength tab)
            "atr":               atr_14,
        }
    except Exception as exc:
        logger.warning("technicals fetch error %s: %s", ticker, exc)
        return None


def _wing_scan_ticker(ticker: str, tradier_key: str,
                      min_otm: float = 0.20, min_iv_diff: float = 0.05,
                      min_vol: int = 50, min_oi: int = 100) -> list:
    """
    Scan Tradier options chain for equidistant OTM put/call pairs with confirmed IV skew bias.
    Mirrors the frontend Wing Scanner Web Worker logic exactly.

    Filters applied (matching UI defaults):
      - min_otm     = 0.20  (put must be ≥20% below spot)
      - min_iv_diff = 0.05  (|put_iv - call_iv| ≥ 5 percentage points, in decimal)
      - min_vol     = 50    (both legs must have ≥50 volume)
      - min_oi      = 100   (both legs must have ≥100 open interest)

    Bias confirmation (must pass to be included):
      put  bias: iv_diff < 0  AND put_oi  > call_oi AND pcr > 1
      call bias: iv_diff > 0  AND call_oi > put_oi  AND pcr < 1

    Returns list of pair dicts (may be empty). IVs stored as % (e.g. 35.0 = 35%).
    """
    import requests as _req

    base = "https://api.tradier.com/v1/markets"
    hdrs = {"Authorization": f"Bearer {tradier_key}", "Accept": "application/json"}
    current_year = str(date.today().year)
    today_dt     = date.today()

    try:
        # 1 — Expirations (current year only)
        r1 = _req.get(f"{base}/options/expirations",
                      params={"symbol": ticker, "includeAllRoots": "true", "strikes": "true"},
                      headers=hdrs, timeout=15)
        if not r1.ok:
            return []
        exp_raw = r1.json().get("expirations", {}).get("date", [])
        if not exp_raw:
            return []
        if isinstance(exp_raw, str):
            exp_raw = [exp_raw]
        expirations = [e for e in exp_raw if str(e).startswith(current_year)]
        if not expirations:
            return []

        # 2 — Spot price
        r2 = _req.get(f"{base}/quotes",
                      params={"symbols": ticker, "greeks": "false"},
                      headers=hdrs, timeout=15)
        if not r2.ok:
            return []
        quote = r2.json().get("quotes", {}).get("quote", {})
        if isinstance(quote, list):
            quote = quote[0] if quote else {}
        spot = quote.get("last") or quote.get("close") or quote.get("prevclose")
        if not spot or spot <= 0:
            return []
        spot = float(spot)

        pairs = []

        # 3 — Per-expiration chain scan
        for expiry in expirations:
            try:
                dte = (date.fromisoformat(str(expiry)) - today_dt).days
                if dte < 0:
                    continue

                r3 = _req.get(f"{base}/options/chains",
                              params={"symbol": ticker, "expiration": expiry, "greeks": "true"},
                              headers=hdrs, timeout=20)
                if not r3.ok:
                    continue
                chain = r3.json().get("options", {}).get("option", [])
                if not chain:
                    continue

                puts_by_strike  = {}
                calls_by_strike = {}
                for opt in chain:
                    s = opt.get("strike")
                    if s is None:
                        continue
                    ot = opt.get("option_type")
                    if ot == "put":
                        puts_by_strike[float(s)]  = opt
                    elif ot == "call":
                        calls_by_strike[float(s)] = opt

                def _get_iv(opt):
                    """Extract IV in decimal form from greeks, with fallback chain."""
                    if not opt:
                        return None
                    g = opt.get("greeks") or {}
                    iv = g.get("mid_iv") or g.get("smv_vol") or g.get("ask_iv") or opt.get("implied_volatility")
                    try:
                        return float(iv) if iv is not None else None
                    except (TypeError, ValueError):
                        return None

                def _mid(opt):
                    try:
                        bid = float(opt.get("bid") or 0)
                        ask = float(opt.get("ask") or 0)
                        return round((bid + ask) / 2, 4) if ask > 0 else None
                    except (TypeError, ValueError):
                        return None

                call_strikes_sorted = sorted(calls_by_strike.keys())

                for put_strike, put_opt in puts_by_strike.items():
                    otm_pct = (spot - put_strike) / spot
                    if otm_pct < min_otm:
                        continue

                    # Equidistant call: same absolute distance above spot
                    call_target = spot + (spot - put_strike)
                    if not call_strikes_sorted:
                        continue
                    call_strike = min(call_strikes_sorted, key=lambda s: abs(s - call_target))
                    call_opt    = calls_by_strike.get(call_strike)
                    if not call_opt:
                        continue

                    put_iv  = _get_iv(put_opt)
                    call_iv = _get_iv(call_opt)
                    if put_iv is None or call_iv is None:
                        continue

                    iv_diff = put_iv - call_iv   # decimal; negative = put skew
                    if abs(iv_diff) < min_iv_diff:
                        continue

                    put_vol  = int(put_opt.get("volume")        or 0)
                    call_vol = int(call_opt.get("volume")       or 0)
                    put_oi_v = int(put_opt.get("open_interest") or 0)
                    call_oi_v= int(call_opt.get("open_interest")or 0)

                    if put_vol < min_vol or call_vol < min_vol:
                        continue
                    if put_oi_v < min_oi or call_oi_v < min_oi:
                        continue

                    pcr = round(put_oi_v / call_oi_v, 3) if call_oi_v > 0 else None

                    put_bias  = iv_diff < 0 and put_oi_v > call_oi_v and pcr is not None and pcr > 1
                    call_bias = iv_diff > 0 and call_oi_v > put_oi_v and pcr is not None and pcr < 1
                    if not put_bias and not call_bias:
                        continue

                    pairs.append({
                        "ticker":      ticker,
                        "expiry":      str(expiry),
                        "dte":         dte,
                        "otm_pct":     round(otm_pct * 100, 2),   # % e.g. 22.5
                        "spot":        round(spot, 4),
                        "put_strike":  put_strike,
                        "call_strike": call_strike,
                        "put_iv":      round(put_iv  * 100, 4),   # % e.g. 35.0
                        "call_iv":     round(call_iv * 100, 4),
                        "iv_diff":     round(iv_diff * 100, 4),   # negative = put bias
                        "bias":        "put" if put_bias else "call",
                        "put_volume":  put_vol,
                        "call_volume": call_vol,
                        "put_oi":      put_oi_v,
                        "call_oi":     call_oi_v,
                        "pcr":         pcr,
                        "put_mid":     _mid(put_opt),
                        "call_mid":    _mid(call_opt),
                    })

            except Exception as exp_exc:
                logger.warning("wing scan %s expiry %s: %s", ticker, expiry, exp_exc)
                continue

        return pairs

    except Exception as exc:
        logger.warning("wing scan error %s: %s", ticker, exc)
        return []


def _run_technicals_eod():
    """
    Fetch technicals for every tracked ticker via Tradier and persist to DB by universe.
    Mirrors the exact frontend calls:
      • maScanTicker  → Moving Averages tab (SMA 20/50/200, cross_signal, alignment)
      • techScanTicker → Market Strength tab (RSI-14, ADX-14, +DI/-DI, di_cross, ATR-14)
    Both tabs use Tradier /markets/history (310 calendar days per ticker).
    """
    from collections import defaultdict as _dd

    tradier_key = os.environ.get('TRADIER_API_KEY', '')
    if not tradier_key:
        logger.warning("_run_technicals_eod: TRADIER_API_KEY not set — skipping")
        return

    all_tkrs = sorted(_all_tickers())
    if not all_tkrs:
        logger.warning("_run_technicals_eod: no tickers found")
        return

    universes_now = _load_universes_from_db()
    results_by_u  = _dd(list)

    logger.info("_run_technicals_eod: scanning %d tickers via Tradier", len(all_tkrs))
    for ticker in all_tkrs:
        data = _fetch_technicals_for_ticker(ticker, tradier_key)
        if data:
            for ukey, tlist in universes_now.items():
                if ticker in tlist:
                    results_by_u[ukey].append(data)
        time.sleep(0.4)   # Tradier rate limit (~150 req/min on sandbox)

    for ukey, rows in results_by_u.items():
        if rows:
            _db.save_technicals_snapshot(ukey, rows)
    logger.info("_run_technicals_eod complete — %d universes written",
                sum(1 for r in results_by_u.values() if r))


def _run_wing_eod():
    """
    Run Tradier-based Wing Scanner across all tracked tickers and persist to DB by universe.
    Uses the same filters as the frontend Wing Scanner (panel-wing):
      min_otm=20%, min_iv_diff=5%, min_vol=50, min_oi=100.
    """
    from collections import defaultdict as _dd

    tradier_key = os.environ.get('TRADIER_API_KEY', '')
    if not tradier_key:
        logger.warning("_run_wing_eod: TRADIER_API_KEY not set — skipping wing scan")
        return

    all_tkrs = sorted(_all_tickers())
    if not all_tkrs:
        logger.warning("_run_wing_eod: no tickers found")
        return

    universes_now = _load_universes_from_db()
    results_by_u  = _dd(list)
    total_pairs   = 0

    logger.info("_run_wing_eod: scanning %d tickers via Tradier", len(all_tkrs))
    for ticker in all_tkrs:
        pairs = _wing_scan_ticker(ticker, tradier_key)
        if pairs:
            total_pairs += len(pairs)
            for ukey, tlist in universes_now.items():
                if ticker in tlist:
                    results_by_u[ukey].extend(pairs)
        time.sleep(0.6)   # Tradier rate limits; chains with greeks are heavy

    for ukey, rows in results_by_u.items():
        if rows:
            _db.save_wing_snapshot(ukey, rows)

    logger.info("_run_wing_eod complete — %d pairs found, %d universes written",
                total_pairs, sum(1 for r in results_by_u.values() if r))


def _background_scheduler():
    """
    On startup:
      1. Warm portfolio cache.
      2. Refresh earnings calendar for the next 3 weeks.
      3. If earnings_surprises table is empty → backfill all tickers (background thread).

    Then at 4:00 PM Eastern every weekday:
      - Run EOD fundamentals scan (all universes, save to DB).
      - Fetch & persist earnings for today's reporters + any stale upcoming tickers.
      - Refresh political trades.

    Every Sunday after the EOD window:
      - Refresh earnings calendar for the next 3 weeks.
    """
    time.sleep(5)  # let gunicorn finish booting

    # ── Startup ───────────────────────────────────────────────────────────────
    _run_scan("portfolio")
    logger.info("Startup scan done.")

    # Calendar refresh on every startup
    _refresh_earnings_calendar()

    # Backfill earnings if table is empty
    if _db.count_earnings() == 0:
        logger.info("Earnings table empty — launching backfill thread")
        threading.Thread(target=_earnings_backfill, daemon=True).start()
    else:
        logger.info("Earnings table has data — skipping backfill")

    logger.info("Startup complete. Waiting for next 4pm ET window.")

    # ── Nightly loop ──────────────────────────────────────────────────────────
    while True:
        time.sleep(_next_4pm_eastern())

        # ── EOD fundamentals scan ─────────────────────────────────────────────
        logger.info("4pm ET — starting EOD scan (%d universes)", len(SCAN_ORDER))
        for ukey in SCAN_ORDER:
            _run_scan(ukey, save_to_db=True)
            time.sleep(15)
        logger.info("EOD scan complete.")

        # ── Earnings: today's reporters + stale upcoming ──────────────────────
        todays_reporters = set()
        try:
            todays   = _db.get_todays_reporters()
            todays_reporters = todays  # save for EPS snapshot update below
            stale    = set(_db.get_stale_upcoming_tickers())
            to_fetch = sorted(todays | stale)
            if to_fetch:
                _fetch_and_persist_earnings(to_fetch, label="nightly")
            else:
                logger.info("No earnings reporters today and no stale upcoming tickers.")
        except Exception as exc:
            logger.error("Nightly earnings fetch error: %s", exc)

        # ── Re-fetch EPS snapshot for today's reporters ───────────────────────
        if todays_reporters:
            logger.info("Re-fetching fundamentals for %d today's reporters", len(todays_reporters))
            from scanner import _fetch_all as _scan_one
            _universes_now = _load_universes_from_db()
            for _tk in sorted(todays_reporters):
                try:
                    _fresh = _scan_one(_tk)
                    if _fresh and _fresh.get("price"):
                        _fresh["ticker"] = _tk
                        for _ukey, _tlist in _universes_now.items():
                            if _tk in _tlist:
                                _db.save_snapshot(_ukey, [_fresh])
                    time.sleep(0.5)
                except Exception as _exc:
                    logger.warning("EPS re-fetch error %s: %s", _tk, _exc)

        # ── Political trades refresh ──────────────────────────────────────────
        try:
            trades   = _fetch_political_trades(tickers=_all_tickers(), limit=100)
            inserted = _db.upsert_political_trades(trades)
            logger.info("Political trades refresh — %d fetched, %d new", len(trades), inserted)
        except Exception as exc:
            logger.error("Political trades refresh error: %s", exc)

        # ── Political trades: refresh price_last for all priced rows ──────────
        try:
            updated = refresh_last_prices()
            logger.info("Political trades refresh-last-prices — %d rows updated", updated)
        except Exception as exc:
            logger.error("Political trades refresh-last-prices error: %s", exc)

        # ── Technicals EOD (Moving Averages + Market Strength tabs) ───────────
        try:
            logger.info("Starting technicals EOD scan…")
            _run_technicals_eod()
        except Exception as exc:
            logger.error("Technicals EOD error: %s", exc)

        # ── Wing Scanner EOD (Options Skew tab) ──────────────────────────────
        try:
            logger.info("Starting wing scanner EOD (Tradier)…")
            _run_wing_eod()
        except Exception as exc:
            logger.error("Wing scanner EOD error: %s", exc)

        # ── Weekly Sunday: refresh earnings calendar ──────────────────────────
        if date.today().weekday() == 6:  # 6 = Sunday
            _refresh_earnings_calendar()

        _purge_cloudflare_cache()


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "timestamp": _now()})


@app.route("/api/debug")
def debug_ticker():
    """Return raw quarterly income statement data for one ticker."""
    import scanner as sc
    symbol = request.args.get("symbol", "ALAB")
    try:
        sc._ensure_session()
        url = sc.QUOTE_URL.format(symbol=symbol, crumb=sc._crumb)
        resp = sc._session.get(url, timeout=15)
        data = resp.json()
        result = (data.get("quoteSummary") or {}).get("result") or [{}]
        quarterly = (result[0].get("incomeStatementHistoryQuarterly") or {}).get("incomeStatementHistory") or []
        sample = []
        for stmt in quarterly[:2]:
            sample.append({k: v for k, v in stmt.items()})
        return jsonify({"symbol": symbol, "count": len(quarterly), "statements": sample})
    except Exception as e:
        return jsonify({"error": str(e), "type": type(e).__name__}), 500


@app.route("/api/stats")
def stats():
    with _cache_lock:
        cached = [
            {"universe": k, "count": v["count"], "scanned_at": v["scanned_at"]}
            for k, v in CACHE.items()
        ]
    last_scan = max(cached, key=lambda x: x["scanned_at"]) if cached else None
    earnings_count = 0
    try:
        earnings_count = _db.count_earnings()
    except Exception:
        pass
    return jsonify({
        "status":         "ok",
        "last_scan":      last_scan,
        "cached":         cached,
        "scanning":       list(SCANNING),
        "universes":      list(UNIVERSES.keys()),
        "earnings_rows":  earnings_count,
    })


@app.route("/api/results")
def results():
    universe = request.args.get("universe", "portfolio")
    with _cache_lock:
        data = CACHE.get(universe)

    if not data:
        if universe in UNIVERSES and universe not in SCANNING:
            t = threading.Thread(target=_run_scan, args=(universe,), daemon=True)
            t.start()
        return jsonify({
            "status":     "no_data",
            "results":    [],
            "count":      0,
            "scanned_at": None,
            "universe":   universe,
        })

    return jsonify({
        "status":     "ok",
        "results":    data["results"],
        "count":      data["count"],
        "scanned_at": data["scanned_at"],
        "universe":   universe,
    })


@app.route("/api/scan", methods=["POST"])
def trigger_scan():
    """Kick off a fresh async scan for a universe."""
    body     = request.get_json(silent=True) or {}
    universe = body.get("universe", "portfolio")
    tickers  = body.get("tickers") or None

    if not tickers:
        tickers = _resolve_universe_tickers(universe)
    if not tickers:
        return jsonify({"error": f"Unknown universe: {universe}"}), 400

    if universe in SCANNING:
        return jsonify({"status": "already_scanning", "universe": universe})

    with _cache_lock:
        CACHE.pop(universe, None)

    t = threading.Thread(target=_run_scan, args=(universe,), kwargs={"tickers": tickers}, daemon=True)
    t.start()
    return jsonify({"status": "scanning", "universe": universe})


@app.route("/api/poll")
def poll():
    """Frontend polls this while waiting for a scan to finish."""
    universe = request.args.get("universe", "portfolio")
    with _cache_lock:
        data = CACHE.get(universe)
    scanning = universe in SCANNING
    if data and (data["count"] > 0 or not scanning):
        return jsonify({
            "ready":      True,
            "scanning":   scanning,
            "results":    data["results"],
            "count":      data["count"],
            "scanned_at": data["scanned_at"],
        })
    return jsonify({"ready": False, "scanning": scanning})


# ── Price targets ─────────────────────────────────────────────────────────────

@app.route("/api/price-targets")
def get_price_targets():
    """GET /api/price-targets?tickers=AAPL,MSFT — mkt cap + avg PT per ticker."""
    tickers_raw = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return jsonify({"error": "tickers param required"}), 400
    tickers = tickers[:80]

    def _fetch_pt(sym):
        quote  = _fetch_quote(sym)
        pt     = _fetch_price_target(sym)
        ratios = _fetch_ratios(sym)
        price   = quote.get("price")
        mkt_cap = quote.get("mkt_cap")
        avg_pt  = pt.get("avg_pt")
        pt_pct  = round((avg_pt / price - 1) * 100, 1) if price and avg_pt and avg_pt > 0 else None
        ps      = ratios.get("ps_fmp")
        return sym, {"mkt_cap": mkt_cap, "avg_pt": avg_pt, "pt_pct": pt_pct, "ps": ps}

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        for sym, data in pool.map(lambda t: _fetch_pt(t), tickers):
            results[sym] = data

    return jsonify({"results": results})


# ── Earnings tracker ──────────────────────────────────────────────────────────

@app.route("/api/earnings-raw")
def get_earnings_raw():
    """GET /api/earnings-raw?ticker=ALAB — raw FMP response for debugging."""
    symbol = request.args.get("ticker", "ALAB").upper()
    import scanner as sc
    data = sc._fmp_get(f"{sc.FMP_STABLE}/earnings", {"symbol": symbol, "limit": 2})
    return jsonify({"symbol": symbol, "raw": data})


@app.route("/api/earnings")
def get_earnings():
    """
    GET /api/earnings?tickers=AAPL,MSFT,NVDA
    Returns last 8 quarters of EPS/revenue beat/miss per ticker.

    Strategy:
    1. Read confirmed actuals from DB (fast, no FMP call).
    2. For tickers with no DB rows at all → live FMP fetch, persist to DB.
    3. Response always includes upcoming quarter data (live FMP for the 1 most
       recent upcoming row per ticker, so estimates stay fresh).
    """
    universe_key = request.args.get("universe", "")
    tickers_raw  = request.args.get("tickers", "")

    if universe_key and universe_key != "all":
        tickers = _resolve_universe_tickers(universe_key)
    elif tickers_raw:
        tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]
    else:
        # universe=all or no filter — return whatever is in the DB
        tickers = _db.get_all_earnings_tickers()

    tickers = tickers[:200]

    # 1. Read actuals from DB
    results = _db.get_earnings_db(tickers)

    # 2. Live fallback for tickers with zero DB rows
    missing = [t for t in tickers if not results.get(t)]
    if missing:
        def _live_fetch(sym):
            rows = _fetch_earnings_surprises(sym, limit=8)
            if rows:
                try:
                    _db.upsert_earnings(sym, rows)
                except Exception as de:
                    logger.warning("earnings persist error %s: %s", sym, de)
            return sym, rows

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            for sym, rows in pool.map(_live_fetch, missing):
                results[sym] = rows if rows else []

    # 3. For tickers that have DB data, patch in the most recent upcoming quarter
    #    (live FMP call so estimates stay current — only the 1 latest quarter)
    def _patch_upcoming(sym):
        live = _fetch_earnings_surprises(sym, limit=2)
        upcoming = [r for r in live if r.get("is_upcoming")]
        return sym, upcoming

    tickers_with_db = [t for t in tickers if t not in missing and results.get(t)]
    if tickers_with_db:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            for sym, upcoming_rows in pool.map(_patch_upcoming, tickers_with_db):
                if upcoming_rows:
                    existing = results.get(sym, [])
                    # Remove any stale upcoming rows already in DB result, prepend fresh ones
                    confirmed = [r for r in existing if not r.get("is_upcoming")]
                    results[sym] = upcoming_rows + confirmed

    resp = jsonify({"results": results, "count": len(results)})
    resp.headers['Cache-Control'] = 'public, max-age=86400'
    return resp


@app.route("/api/fundamentals")
def get_fundamentals():
    universe = request.args.get("universe", "portfolio")
    rows = _db.get_fundamentals_snapshot(universe)
    resp = jsonify({"results": rows, "universe": universe, "count": len(rows)})
    resp.headers['Cache-Control'] = 'public, max-age=86400'
    return resp


@app.route("/api/technicals")
def get_technicals():
    """
    GET /api/technicals?universe=portfolio
    Returns latest technicals snapshot (Moving Averages + Market Strength) from DB.
    Falls back to a live yfinance fetch for any tickers with no stored row.
    """
    universe = request.args.get("universe", "portfolio")
    rows = _db.get_technicals_snapshot(universe)

    # Live fallback: if DB is empty for this universe (first run), compute on-demand via Tradier
    if not rows:
        logger.info("/api/technicals: no DB data for %s — live Tradier fetch", universe)
        tradier_key = os.environ.get('TRADIER_API_KEY', '')
        if tradier_key:
            tickers = _resolve_universe_tickers(universe)
            results = []
            for tk in tickers:
                data = _fetch_technicals_for_ticker(tk, tradier_key)
                if data:
                    results.append(data)
                time.sleep(0.4)
            if results:
                _db.save_technicals_snapshot(universe, results)
            rows = results
        else:
            logger.warning("/api/technicals: TRADIER_API_KEY not set — cannot live-fetch")

    resp = jsonify({"results": rows, "universe": universe, "count": len(rows)})
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp


@app.route("/api/options-skew")
def get_options_skew():
    """
    GET /api/options-skew?universe=portfolio&bias=all
    Returns latest Wing Scanner results from DB (Options Skew tab).

    Query params:
      universe  — universe key (default: portfolio)
      bias      — 'put' | 'call' | 'all' (default: all)
    """
    universe = request.args.get("universe", "portfolio")
    bias     = request.args.get("bias", "all")

    rows = _db.get_wing_snapshot(universe, bias=None if bias == "all" else bias)

    resp = jsonify({"results": rows, "universe": universe, "count": len(rows), "bias": bias})
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp


# ── Political trades ──────────────────────────────────────────────────────────

@app.route("/api/political-trades")
def get_political_trades():
    """GET /api/political-trades?tickers=AAPL,MSFT&limit=2000&trade_year=2024&disc_year=2024 — serves from DB."""
    tickers_raw = request.args.get("tickers", "")
    tickers = set(t.strip().upper() for t in tickers_raw.split(",") if t.strip()) if tickers_raw else None
    limit = min(int(request.args.get("limit", 2000)), 5000)
    trade_year = request.args.get("trade_year") or None
    disc_year  = request.args.get("disc_year")  or None
    try:
        data = _db.get_political_trades(tickers=tickers, limit=limit,
                                        trade_year=trade_year, disc_year=disc_year)
        resp = jsonify({"results": data, "count": len(data)})
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        return resp
    except Exception as exc:
        logger.error("political-trades error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/years")
def get_political_trade_years():
    """GET /api/political-trades/years — distinct years for trade_date and disc_date."""
    try:
        return jsonify(_db.get_political_trade_years())
    except Exception as exc:
        logger.error("political-trade-years error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/months")
def get_political_trade_months():
    """GET /api/political-trades/months — distinct YYYY-MM months in disc_date with row counts."""
    try:
        return jsonify(_db.get_political_trade_months())
    except Exception as exc:
        logger.error("political-trade-months error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/backfill", methods=["POST"])
def backfill_political_trades():
    """POST /api/political-trades/backfill — fetch recent trades, upsert."""
    def _run():
        logger.info("Political trades backfill started")
        trades   = _fetch_political_trades(tickers=_all_tickers())
        inserted = _db.upsert_political_trades(trades)
        logger.info("Political trades backfill done — %d fetched, %d new", len(trades), inserted)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "backfill_started"})


@app.route("/api/political-trades/backfill-year", methods=["POST"])
def backfill_political_trades_year():
    """
    POST /api/political-trades/backfill-year?year=2024
    Fetches all trades per-ticker for a specific calendar year using FMP
    from/to date params, then upserts. Runs in background thread.
    Use this to fill in historical years (2024, 2025) where FMP's default
    per-ticker response only returns the most recent page of results.
    """
    year = request.args.get("year", "")
    if not year or not year.isdigit() or len(year) != 4:
        return jsonify({"error": "year param required (e.g. ?year=2024)"}), 400
    from_date = f"{year}-01-01"
    to_date   = f"{year}-12-31"

    def _run():
        logger.info("Political trades year backfill started — year=%s", year)
        trades   = _fetch_political_trades(
            tickers=_all_tickers(),
            from_date=from_date,
            to_date=to_date,
        )
        inserted = _db.upsert_political_trades(trades)
        logger.info("Political trades year backfill done — year=%s %d fetched, %d new", year, len(trades), inserted)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "year_backfill_started", "year": year, "from": from_date, "to": to_date})


@app.route("/api/political-trades/backfill-historical", methods=["POST"])
def backfill_political_trades_historical():
    """
    POST /api/political-trades/backfill-historical?from_date=2025-01-01
    Pages through FMP senate + house endpoints (no symbol filter) and upserts
    all trades with trade_date >= from_date. Runs in background thread.
    """
    from_date = request.args.get("from_date", "2025-01-01")
    to_date   = request.args.get("to_date") or None
    def _run():
        logger.info("Historical political trades backfill started (from_date=%s, to_date=%s)", from_date, to_date)
        trades   = fetch_political_trades_historical(from_date=from_date, to_date=to_date)
        inserted = _db.upsert_political_trades(trades)
        logger.info("Historical backfill done — %d fetched, %d new", len(trades), inserted)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "historical_backfill_started", "from_date": from_date, "to_date": to_date})


@app.route("/api/political-trades/backfill-forward-prices", methods=["POST"])
def backfill_political_trades_forward_prices():
    """
    POST /api/political-trades/backfill-forward-prices
    Fetches EOD close price 30 and 60 days after trade_date for all purchase rows
    that are missing price_30d. Runs in background thread.
    """
    def _run():
        from scanner import fetch_forward_prices
        session = _db._Session()
        try:
            rows = session.query(_db.PoliticalTrade).filter(
                _db.PoliticalTrade.price_30d == None,
                _db.PoliticalTrade.trade_date != None,
                _db.PoliticalTrade.ticker != None,
            ).all()
            trades = [{"id": r.id, "ticker": r.ticker, "trade_date": r.trade_date} for r in rows]
            session.close()
            logger.info("backfill-forward-prices — %d rows to price", len(trades))
            priced = fetch_forward_prices(trades)
            session2 = _db._Session()
            updated = 0
            for p in priced:
                if p.get("id") is None:
                    continue
                row = session2.query(_db.PoliticalTrade).filter_by(id=p["id"]).first()
                if not row:
                    continue
                if p.get("price_30d") is not None:
                    row.price_30d = p["price_30d"]
                if p.get("price_60d") is not None:
                    row.price_60d = p["price_60d"]
                updated += 1
            session2.commit()
            session2.close()
            logger.info("backfill-forward-prices done — %d rows updated", updated)
        except Exception as exc:
            logger.error("backfill-forward-prices error: %s", exc)
            try: session.close()
            except: pass
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "forward_price_backfill_started"})


@app.route("/api/political-trades/reset-prices", methods=["POST"])
def reset_trade_prices():
    """
    POST /api/political-trades/reset-prices?ticker=NFLX
    Nulls out price_at_trade and price_last for all trades of a given ticker so
    the next backfill-prices run re-fetches them from FMP.
    Use when a stored price looks obviously wrong (bad FMP data, etc.).
    No auth required — admin/debug endpoint.
    """
    ticker = (request.args.get("ticker") or "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker param required"}), 400
    try:
        session = _db._Session()
        rows = session.query(_db.PoliticalTrade).filter_by(ticker=ticker).all()
        count = 0
        for r in rows:
            r.price_at_trade     = None
            r.price_last         = None
            r.price_last_updated = None
            count += 1
        session.commit()
        session.close()
        logger.info("reset-prices: nulled price_at_trade + price_last for %d %s rows", count, ticker)
        return jsonify({"status": "ok", "ticker": ticker, "rows_reset": count})
    except Exception as exc:
        logger.error("reset-prices error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/clear")
def clear_political_trades():
    """DELETE blank-name rows from political_trades."""
    try:
        session = _db._Session()
        deleted = session.query(_db.PoliticalTrade).filter(
            (_db.PoliticalTrade.name == "—") | (_db.PoliticalTrade.name == "") | (_db.PoliticalTrade.name == None)
        ).delete()
        session.commit()
        session.close()
        return jsonify({"status": "ok", "deleted": deleted})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/debug")
def debug_political_trades():
    """GET /api/political-trades/debug — raw FMP response sample."""
    from scanner import FMP_STABLE, _fmp_get
    senate   = _fmp_get(f"{FMP_STABLE}/senate-trades", {"symbol": "MU"}) or []
    house    = _fmp_get(f"{FMP_STABLE}/house-trades",  {"symbol": "MU"}) or []
    earnings = _fmp_get(f"{FMP_STABLE}/earnings",      {"symbol": "MU", "limit": 2}) or []
    return jsonify({
        "senate_sample":   senate[:2]   if isinstance(senate,   list) else senate,
        "house_sample":    house[:2]    if isinstance(house,    list) else house,
        "earnings_sample": earnings[:2] if isinstance(earnings, list) else earnings,
    })


@app.route("/api/political-trades/refresh", methods=["POST"])
def refresh_political_trades():
    """POST /api/political-trades/refresh — fetch latest 100 records."""
    try:
        trades   = _fetch_political_trades(tickers=None, limit=100)
        inserted = _db.upsert_political_trades(trades)
        return jsonify({"status": "ok", "fetched": len(trades), "inserted": inserted})
    except Exception as exc:
        logger.error("political-trades refresh error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/backfill-sectors", methods=["POST"])
def backfill_political_trade_sectors():
    """POST /api/political-trades/backfill-sectors
    1. Fetches FMP /profile for any ticker in political_trades not yet in ticker_metadata.
    2. Backfills sector on existing political_trades rows that have empty sector.
    """
    from scanner import ensure_ticker_metadata
    try:
        # Collect all unique tickers in political_trades
        session = _db._Session()
        try:
            tickers = [r[0] for r in session.query(_db.PoliticalTrade.ticker).distinct().all()]
        finally:
            session.close()
        # Ensure sector data exists in ticker_metadata for all tickers
        sector_map = ensure_ticker_metadata(tickers)
        # Backfill empty sector on existing rows
        updated = _db.backfill_political_trade_sectors()
        return jsonify({
            "status": "ok",
            "tickers_resolved": len(sector_map),
            "rows_updated": updated,
        })
    except Exception as exc:
        logger.error("backfill-sectors error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/backfill-prices", methods=["POST"])
def backfill_trade_prices_endpoint():
    """
    POST /api/political-trades/backfill-prices
    Fires a background thread that continuously prices batches of trades missing
    price_at_trade until all are done. Returns immediately so Cloudflare doesn't 524.
    Safe to call multiple times — only processes rows still missing prices.
    """
    batch = int(request.args.get("batch", 500))
    def _run():
        total_filled = 0
        total_errors = 0
        run = 0
        while True:
            try:
                result = backfill_trade_prices(batch_size=batch)
                filled = result.get("filled_at", 0)
                total_filled += filled
                total_errors += result.get("errors", 0)
                run += 1
                logger.info("backfill-prices run=%d filled=%d total_filled=%d errors=%d",
                            run, filled, total_filled, total_errors)
                if filled == 0:
                    break  # nothing left to price
            except Exception as exc:
                logger.error("backfill-prices loop error: %s", exc)
                break
        logger.info("backfill-prices complete: total_filled=%d total_errors=%d",
                    total_filled, total_errors)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "backfill_prices_started", "batch": batch})


@app.route("/api/political-trades/refresh-last-prices", methods=["POST"])
def refresh_last_prices_endpoint():
    """
    POST /api/political-trades/refresh-last-prices
    Updates price_last for all political_trades that have price_at_trade set.
    Run periodically to keep return% calculations current.
    """
    try:
        updated = refresh_last_prices()
        return jsonify({"status": "ok", "updated": updated})
    except Exception as exc:
        logger.error("refresh-last-prices error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/leaderboard", methods=["GET"])
def political_trades_leaderboard():
    """
    GET /api/political-trades/leaderboard?year=2026
    Returns per-politician win rate, avg direction-adjusted return, trade counts.
    Only politicians with ≥3 priced trades are included.
    Optional ?year=YYYY filters to trades in that calendar year only.
    """
    try:
        year        = request.args.get("year", type=int)
        leaderboard = _db.get_political_leaderboard(year=year)
        counts      = _db.get_political_trades_count()
        resp = jsonify({"status": "ok", "leaderboard": leaderboard, "year_filter": year, **counts})
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        return resp
    except Exception as exc:
        logger.error("leaderboard error: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ── History endpoints ─────────────────────────────────────────────────────────

@app.route("/api/history/ticker")
def history_ticker():
    """GET /api/history/ticker?ticker=NVDA&universe=soxx&days=90"""
    ticker   = request.args.get("ticker", "").upper()
    universe = request.args.get("universe") or None
    days     = min(int(request.args.get("days", 90)), 365)
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    try:
        data = _db.get_ticker_history(ticker, universe, days)
        return jsonify({"ticker": ticker, "universe": universe, "days": days, "data": data})
    except Exception as exc:
        logger.error("history_ticker error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/history/universe")
def history_universe():
    """GET /api/history/universe?universe=soxx&days=90"""
    universe = request.args.get("universe", "portfolio")
    days     = min(int(request.args.get("days", 90)), 365)
    try:
        data = _db.get_universe_history(universe, days)
        return jsonify({"universe": universe, "days": days, "data": data})
    except Exception as exc:
        logger.error("history_universe error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/history/tickers")
def history_tickers():
    """GET /api/history/tickers?universe=soxx"""
    universe = request.args.get("universe") or None
    try:
        tickers = _db.get_ticker_list(universe)
        return jsonify({"universe": universe, "tickers": tickers, "count": len(tickers)})
    except Exception as exc:
        logger.error("history_tickers error: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Entry point ───────────────────────────────────────────────────────────────

# Start background scheduler when module loads (works with gunicorn too)
_bg_thread = threading.Thread(target=_background_scheduler, daemon=True)
_bg_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
