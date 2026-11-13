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
    if request.path in ("/api/health", "/api/ticker-sectors", "/api/earnings/catchup", "/api/earnings/backfill-labels", "/api/political-trades/debug", "/api/political-trades/clear", "/api/political-trades/backfill-sectors", "/api/political-trades/backfill-prices", "/api/political-trades/refresh-last-prices", "/api/political-trades/backfill-historical", "/api/political-trades/backfill-forward-prices", "/api/political-trades/reset-prices", "/api/political-trades/backfill-year", "/api/political-trades/years", "/api/political-trades/months"):
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


def _opt_mid_iv(opt):
    """IV (decimal) from a Tradier option's greeks, with the same fallback order as the wing scan."""
    if not opt:
        return None
    g = opt.get("greeks") or {}
    iv = g.get("mid_iv") or g.get("smv_vol") or g.get("ask_iv") or opt.get("implied_volatility")
    try:
        iv = float(iv) if iv is not None else None
    except (TypeError, ValueError):
        return None
    return iv if iv and iv > 0 else None


def _atm_iv_from_chain(chain: list, spot: float):
    """
    ATM IV (decimal) for one expiry: the strike nearest spot, averaging the put and
    call IV at that strike (whichever are available). Returns None if neither has IV.
    """
    by_strike = {}
    for opt in chain or []:
        s = opt.get("strike")
        if s is None:
            continue
        by_strike.setdefault(float(s), {})[opt.get("option_type")] = opt
    if not by_strike:
        return None
    atm = min(by_strike, key=lambda k: abs(k - spot))
    ivs = [iv for iv in (_opt_mid_iv(by_strike[atm].get("put")),
                         _opt_mid_iv(by_strike[atm].get("call"))) if iv is not None]
    return sum(ivs) / len(ivs) if ivs else None


def _constant_maturity_iv(points: list, target_dte: int = 30) -> dict:
    """
    points: [(dte, expiry, atm_iv_decimal)]. Interpolates ATM IV to target_dte in
    total variance (iv^2 * t) between the expiries bracketing it; falls back to the
    nearest side when only one side exists. Returns {} if no usable points.
    """
    import math
    pts = sorted((d, e, iv) for d, e, iv in points if d and d >= 1 and iv)
    if not pts:
        return {}
    near = [p for p in pts if p[0] <= target_dte]
    far  = [p for p in pts if p[0] >= target_dte]
    n = near[-1] if near else None
    f = far[0]   if far  else None
    out = {}
    if n and f and n[0] != f[0]:
        v1, v2 = n[2] ** 2 * n[0], f[2] ** 2 * f[0]
        w = (target_dte - n[0]) / (f[0] - n[0])
        iv = math.sqrt(max(v1 + w * (v2 - v1), 0) / target_dte)
        method = "interp"
    elif n and f:
        iv, method = n[2], "exact"
    elif f:
        iv, method = f[2], "far_only"
    else:
        iv, method = n[2], "near_only"
    if n:
        out.update(near_expiry=str(n[1]), near_dte=n[0], near_iv=round(n[2] * 100, 4))
    if f:
        out.update(far_expiry=str(f[1]), far_dte=f[0], far_iv=round(f[2] * 100, 4))
    out.update(atm_iv_30d=round(iv * 100, 4), method=method)
    return out


def _wing_scan_ticker(ticker: str, tradier_key: str, atm_out: dict = None, params: dict = None,
                      today: date = None) -> list:
    """
    Options-skew scan for one ticker via Tradier. Pair selection lives in wing.py — an exact, parity-tested
    port of the Options Skew tab's original browser scan (see wing.py for the rules).

    atm_out: optional dict, filled with this ticker's constant-maturity 30-day ATM IV (iv_history table)
    from the same chains, whether or not any pair qualifies.
    """
    import requests as _req
    import wing as W

    p = {**W.DEFAULTS, **(params or {})}
    base = "https://api.tradier.com/v1/markets"
    hdrs = {"Authorization": f"Bearer {tradier_key}", "Accept": "application/json"}
    today_dt = today or _ny_today()

    def get(path, **params_):
        r = _req.get(f"{base}{path}", params=params_, headers=hdrs, timeout=20)
        return r.json() if r.ok else None

    try:
        d = get("/options/expirations", symbol=ticker, includeAllRoots="true")
        exp_raw = ((d or {}).get("expirations") or {}).get("date") or []
        if isinstance(exp_raw, str):
            exp_raw = [exp_raw]
        expirations = W.target_expiries(exp_raw, today_dt)
        if not expirations:
            return []
        q = ((get("/quotes", symbols=ticker, greeks="false") or {}).get("quotes") or {}).get("quote") or {}
        if isinstance(q, list):
            q = q[0] if q else {}
        spot = W.spot_from_quote(q)
        if not spot or spot <= 0:
            return []

        pairs, atm_points = [], []
        for expiry in expirations:
            try:
                ch = ((get("/options/chains", symbol=ticker, expiration=expiry, greeks="true") or {}).get("options") or {}).get("option") or []
                if isinstance(ch, dict):
                    ch = [ch]
                if not ch:
                    continue
                if atm_out is not None:
                    _atm = _atm_iv_from_chain(ch, spot)
                    if _atm:
                        atm_points.append((W.dte(expiry, today_dt), str(expiry), _atm))
                pairs += W.pairs_for_expiry(ticker, expiry, ch, spot, today_dt, p["min_otm"], p["min_iv_diff"],
                                            p["min_vol"], p["min_oi"])
            except Exception as exp_exc:
                logger.warning("wing scan %s expiry %s: %s", ticker, expiry, exp_exc)
            time.sleep(0.08)

        if atm_out is not None:
            try:
                # current-year expiries may not reach 30 DTE late in the year: fetch the first one beyond it
                if not any(dd >= 30 for dd, _, _ in atm_points):
                    beyond = sorted(e for e in exp_raw if str(e) not in expirations and W.dte(e, today_dt) >= 30)
                    if beyond:
                        e = beyond[0]
                        ch = ((get("/options/chains", symbol=ticker, expiration=e, greeks="true") or {}).get("options") or {}).get("option") or []
                        if isinstance(ch, dict):
                            ch = [ch]
                        _atm = _atm_iv_from_chain(ch, spot)
                        if _atm:
                            atm_points.append((W.dte(e, today_dt), str(e), _atm))
                cm = _constant_maturity_iv(atm_points, 30)
                if cm:
                    atm_out.update(cm, ticker=ticker, spot=round(spot, 4))
            except Exception as atm_exc:
                logger.warning("ATM IV snapshot %s: %s", ticker, atm_exc)
        return pairs
    except Exception as exc:
        logger.warning("wing scan error %s: %s", ticker, exc)
        return []


def _ny_today() -> date:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).date()


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
    trade_day = _ny_today()
    iv_rows = []   # one ATM IV row per ticker for the iv_history table
    for ticker in all_tkrs:
        atm = {}
        pairs = _wing_scan_ticker(ticker, tradier_key, atm_out=atm, today=trade_day)
        if atm.get("atm_iv_30d") is not None:
            iv_rows.append(atm)
        if pairs:
            total_pairs += len(pairs)
            for ukey, tlist in universes_now.items():
                if ticker in tlist:
                    results_by_u[ukey].extend(pairs)
        time.sleep(0.6)   # Tradier rate limits; chains with greeks are heavy

    for ukey, rows in results_by_u.items():
        if rows:
            _db.save_wing_snapshot(ukey, rows, snap_date=trade_day)

    iv_written = _db.save_iv_snapshot(iv_rows, snap_date=trade_day)
    logger.info("_run_wing_eod: ATM IV saved for %d of %d tickers", iv_written, len(all_tkrs))

    logger.info("_run_wing_eod complete — %d pairs found, %d universes written",
                total_pairs, sum(1 for r in results_by_u.values() if r))


# ── Scheduled jobs ─────────────────────────────────────────────────────────────
# The tabs keep scanning live; this job runs the same live scans once each weekday after the close
# and persists the results (fundamentals_snapshot, technicals_snapshot, wing_snapshot, earnings,
# political trades) so every day's live view is stored. APScheduler (not a sleep loop) so it survives
# restarts, is DST-aware, and one failing step can't kill the others.
from apscheduler.schedulers.background import BackgroundScheduler

_EOD_STATE = {"running": False, "stage": None, "started": None, "finished": None, "error": None, "last_ok": None}
_eod_lock = threading.Lock()


def _eod_job():
    if not _eod_lock.acquire(blocking=False):
        logger.info("EOD job already running — skipping")
        return
    _EOD_STATE.update(running=True, started=_now(), finished=None, error=None, stage="starting")
    try:
        # ── EOD fundamentals scan ─────────────────────────────────────────────
        _eod_universes = list(dict.fromkeys(SCAN_ORDER + list(_load_universes_from_db().keys())))
        logger.info("EOD — starting fundamentals scan (%d universes)", len(_eod_universes))
        for ukey in _eod_universes:
            _EOD_STATE["stage"] = f"fundamentals:{ukey}"
            _run_scan(ukey, save_to_db=True)
            time.sleep(15)
        logger.info("EOD scan complete.")

        # ── Earnings: report-day + overdue actuals + last-quarter backstop ─────
        #   1. pending  — report date has arrived but EPS/revenue actual still
        #                 null (retried nightly until FMP posts both numbers)
        #   2. calendar — tickers on today's report_calendar
        #   3. missing  — no complete row for the last elapsed quarter
        todays_reporters = set()
        try:
            all_tkrs  = set(_all_tickers())
            pending   = set(_db.get_tickers_pending_actuals()) & all_tkrs
            calendar  = set(_db.get_todays_reporters()) & all_tkrs
            missing   = set(_db.get_tickers_missing_recent_earnings(sorted(all_tkrs)))
            to_fetch  = sorted(pending | calendar | missing)
            cutoff    = _db.last_elapsed_quarter_end()
            if to_fetch:
                logger.info("Earnings nightly: %d tickers (pending=%d, calendar=%d, missing since %s=%d)",
                            len(to_fetch), len(pending), len(calendar), cutoff, len(missing))
                _fetch_and_persist_earnings(to_fetch, label="nightly")
                todays_reporters = set(to_fetch)  # re-fetch fundamentals snapshot for these
            else:
                logger.info("Earnings nightly: all tickers current through %s", cutoff)
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


        _EOD_STATE["stage"] = "cloudflare purge"
        _purge_cloudflare_cache()
        _EOD_STATE["last_ok"] = _now()
    except Exception as exc:
        logger.exception("EOD job failed")
        _EOD_STATE["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        _EOD_STATE.update(running=False, finished=_now(), stage=None)
        _eod_lock.release()


def _startup_tasks():
    """Warm caches on boot. Each step is isolated so one failure can't stop the rest."""
    time.sleep(5)  # let gunicorn finish booting
    for name, fn in (("portfolio scan", lambda: _run_scan("portfolio")),
                     ("earnings calendar", _refresh_earnings_calendar)):
        try:
            fn()
        except Exception as exc:
            logger.error("Startup %s failed: %s", name, exc)
    try:
        if _db.count_earnings() == 0:
            logger.info("Earnings table empty — launching backfill thread")
            threading.Thread(target=_earnings_backfill, daemon=True).start()
    except Exception as exc:
        logger.error("Startup earnings check failed: %s", exc)
    # Catch-up: if the service was down at the scheduled time, run today's EOD once now
    try:
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("America/New_York"))
        if now_et.weekday() < 5 and (now_et.hour, now_et.minute) >= (16, 20) and not _db.has_snapshot_for(now_et.date()):
            logger.info("Startup catch-up: no fundamentals snapshot for %s — running EOD now", now_et.date())
            _eod_job()
    except Exception as exc:
        logger.error("Startup catch-up check failed: %s", exc)
    logger.info("Startup complete.")


_scheduler = BackgroundScheduler(timezone="America/New_York")
_scheduler.add_job(_eod_job, "cron", day_of_week="mon-fri", hour=16, minute=20, id="eod",
                   misfire_grace_time=6 * 3600, coalesce=True, max_instances=1)
_scheduler.add_job(_refresh_earnings_calendar, "cron", day_of_week="sun", hour=18, minute=0, id="calendar",
                   misfire_grace_time=6 * 3600, coalesce=True)


@app.route("/api/eod-status")
def eod_status():
    """Is the nightly persistence running, and what did it save today?"""
    try:
        today_rows = _db.snapshot_counts_for(date.today())
    except Exception as exc:
        today_rows = {"error": str(exc)}
    jobs = [{"id": j.id, "next_run": j.next_run_time.isoformat() if j.next_run_time else None} for j in _scheduler.get_jobs()]
    return jsonify({**_EOD_STATE, "jobs": jobs, "fundamentals_saved_today": today_rows})


@app.route("/api/eod-run", methods=["POST"])
def eod_run():
    if _EOD_STATE["running"]:
        return jsonify({"status": "already running", **_EOD_STATE})
    threading.Thread(target=_eod_job, daemon=True).start()
    return jsonify({"status": "started"})


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

@app.route("/api/earnings/catchup", methods=["POST"])
def earnings_catchup():
    """
    POST /api/earnings/catchup
    One-time (or on-demand) catch-up: find all universe tickers missing earnings
    for the last elapsed calendar quarter and fetch from FMP in a background thread.
    """
    all_tkrs = set(_all_tickers())
    pending  = set(_db.get_tickers_pending_actuals()) & all_tkrs
    missing  = set(_db.get_tickers_missing_recent_earnings(sorted(all_tkrs)))
    to_fetch = sorted(pending | missing)

    def _run():
        cutoff = _db.last_elapsed_quarter_end()
        logger.info("Earnings catch-up: %d tickers (pending=%d, missing since %s=%d)",
                    len(to_fetch), len(pending), cutoff, len(missing))
        _fetch_and_persist_earnings(to_fetch, label="catchup")
        logger.info("Earnings catch-up complete.")
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({
        "status":           "started",
        "target_label":     _db.current_target_label(),
        "tickers_to_fetch": len(to_fetch),
        "pending":          sorted(pending),
    })


@app.route("/api/earnings/backfill-labels", methods=["POST"])
def earnings_backfill_labels():
    """
    POST /api/earnings/backfill-labels
    One-time migration: populate calendar_label on all existing earnings rows that
    have an announced_date but no label yet. Safe to run multiple times (idempotent).
    """
    updated = _db.backfill_calendar_labels()
    return jsonify({"status": "done", "rows_updated": updated})


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


@app.route("/api/iv-history")
def get_iv_history():
    """
    GET /api/iv-history?ticker=NVDA&days=365
    Daily 30-day ATM IV history plus IV rank / percentile of the latest reading.
    rank and percentile are null until at least 20 days of history exist.
    """
    ticker = (request.args.get("ticker") or "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    try:
        days = max(1, min(int(request.args.get("days", 365)), 1825))
    except ValueError:
        days = 365
    hist = _db.get_iv_history(ticker, days)
    ivs  = [h["atm_iv_30d"] for h in hist if h["atm_iv_30d"] is not None]
    rank = pct = None
    if len(ivs) >= 20:
        cur, lo, hi = ivs[-1], min(ivs), max(ivs)
        rank = round((cur - lo) / (hi - lo) * 100, 1) if hi > lo else None
        pct  = round(sum(1 for v in ivs[:-1] if v < cur) / (len(ivs) - 1) * 100, 1)
    resp = jsonify({
        "ticker": ticker, "days": days, "n_days": len(ivs),
        "current": ivs[-1] if ivs else None,
        "iv_rank": rank, "iv_percentile": pct,
        "history": hist,
    })
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


# ── Options skew (Wing) — persisted nightly history + live scans, one implementation (wing.py) ──────────
WING_LIVE: dict = {}            # universe -> {running, scanned, total, results, started, scanned_at, error, params}
_wing_live_lock = threading.Lock()


def _json_rows(rows):
    out = []
    for r in rows:
        r = dict(r)
        for k, v in list(r.items()):
            if isinstance(v, (date, datetime)):
                r[k] = v.isoformat()
        out.append(r)
    return out


@app.route("/api/options-skew")
def get_options_skew():
    """
    GET /api/options-skew?universe=portfolio|all&bias=all|put|call&live=0|1
    The last nightly scan from wing_snapshot. With live=1, a live scan of a universe finished today
    replaces that universe's nightly rows (what the Radar uses intraday).
    """
    universe = request.args.get("universe", "portfolio")
    bias     = request.args.get("bias", "all")
    live     = request.args.get("live") == "1"
    b = None if bias == "all" else bias
    rows = _db.get_wing_snapshot_all(b) if universe == "all" else _db.get_wing_snapshot(universe, bias=b)
    rows = _json_rows(rows)
    source = "nightly"
    if live:
        today = _ny_today().isoformat()
        with _wing_live_lock:
            done = {u: st for u, st in WING_LIVE.items()
                    if not st.get("running") and (st.get("scanned_at") or "")[:10] == today and st.get("results") is not None
                    and (universe == "all" or u == universe)}
        if done:
            live_tk = set()
            live_rows = []
            for u, st in done.items():
                tks = set(st.get("tickers") or [])
                live_tk |= tks
                live_rows += [r for r in st["results"] if not b or r.get("bias") == b]
            rows = [r for r in rows if r.get("ticker") not in live_tk] + live_rows
            source = "nightly + live (" + ", ".join(done) + ")"
    resp = jsonify({"results": rows, "universe": universe, "count": len(rows), "bias": bias, "source": source})
    resp.headers['Cache-Control'] = 'no-store' if live else 'public, max-age=3600'
    return resp


def _wing_live_scan(universe: str, tickers: list, params: dict):
    key = os.environ.get('TRADIER_API_KEY', '')
    st = WING_LIVE[universe]
    try:
        if not key:
            raise RuntimeError("TRADIER_API_KEY not set")
        for t in tickers:
            if st.get("abort"):
                break
            rows = _wing_scan_ticker(t, key, params=params)
            with _wing_live_lock:
                st["results"].extend(rows)
                st["scanned"] += 1
                st["pairs_by_ticker"][t] = len(rows)
            time.sleep(0.3)
    except Exception as exc:
        logger.exception("live wing scan %s failed", universe)
        st["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        st["running"] = False
        st["scanned_at"] = datetime.now(timezone.utc).isoformat()


@app.route("/api/options-skew/scan", methods=["POST"])
def options_skew_scan():
    """Start a live scan of one universe (same rules as the nightly job). Poll /api/options-skew/poll."""
    import wing as W
    body = request.get_json(silent=True) or {}
    universe = body.get("universe") or request.args.get("universe") or "portfolio"
    tickers = body.get("tickers") or _resolve_universe_tickers(universe)
    if not tickers:
        return jsonify({"status": "error", "error": f"universe {universe} has no tickers"}), 400
    params = {k: float(body[k]) if k in ("min_otm", "min_iv_diff") else int(body[k])
              for k in W.DEFAULTS if body.get(k) not in (None, "")}
    with _wing_live_lock:
        cur = WING_LIVE.get(universe)
        if cur and cur.get("running"):
            return jsonify({"status": "already running", "scanned": cur["scanned"], "total": cur["total"]})
        WING_LIVE[universe] = {"running": True, "scanned": 0, "total": len(tickers), "results": [], "pairs_by_ticker": {},
                               "tickers": list(tickers), "started": _now(), "scanned_at": None, "error": None,
                               "params": {**W.DEFAULTS, **params}}
    threading.Thread(target=_wing_live_scan, args=(universe, list(tickers), params), daemon=True).start()
    return jsonify({"status": "started", "total": len(tickers)})


@app.route("/api/options-skew/poll")
def options_skew_poll():
    """Progress and results so far of the live scan for a universe."""
    universe = request.args.get("universe", "portfolio")
    since = int(request.args.get("since", 0) or 0)      # only rows after this index (incremental)
    with _wing_live_lock:
        st = WING_LIVE.get(universe)
        if not st:
            resp = jsonify({"universe": universe, "running": False, "started": None, "results": [], "count": 0})
        else:
            resp = jsonify({"universe": universe, "running": st["running"], "scanned": st["scanned"], "total": st["total"],
                            "started": st["started"], "scanned_at": st["scanned_at"], "error": st["error"], "params": st["params"],
                            "count": len(st["results"]), "results": st["results"][since:], "since": since})
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@app.route("/api/options-skew/stop", methods=["POST"])
def options_skew_stop():
    universe = (request.get_json(silent=True) or {}).get("universe") or request.args.get("universe", "portfolio")
    st = WING_LIVE.get(universe)
    if st and st.get("running"):
        st["abort"] = True
    return jsonify({"status": "stopping" if st and st.get("running") else "not running"})


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


@app.route("/api/ticker-sectors")
def get_ticker_sectors():
    """GET /api/ticker-sectors — returns {ticker: sector} map for all tickers with known sector."""
    try:
        session = _db._Session()
        rows = session.query(_db.TickerMetadata.ticker, _db.TickerMetadata.sector).filter(
            _db.TickerMetadata.sector.isnot(None),
            _db.TickerMetadata.sector != "",
        ).all()
        session.close()
        return jsonify({r[0]: r[1] for r in rows})
    except Exception as exc:
        logger.error("ticker-sectors error: %s", exc)
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
    Backfills sector on political_trades rows with empty sector using ticker_metadata
    (the single source of truth, written by insider-scanner on universe save).
    """
    try:
        # Backfill sector on political_trades rows that have empty sector,
        # using whatever is already in ticker_metadata (written by insider-scanner on universe save).
        updated = _db.backfill_political_trade_sectors()
        return jsonify({
            "status": "ok",
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
_bg_thread = threading.Thread(target=_startup_tasks, daemon=True)
_scheduler.start()
_bg_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
