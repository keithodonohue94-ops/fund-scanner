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

from scanner import (
    scan_tickers, UNIVERSES,
    _fetch_earnings_surprises, _fetch_earnings_calendar,
    _fetch_quote, _fetch_price_target, _fetch_ratios, _fetch_political_trades,
)
import db as _db

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
    if request.path in ("/api/health", "/api/political-trades/debug", "/api/political-trades/clear"):
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
        tickers = UNIVERSES.get(universe_key)
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
        try:
            todays   = _db.get_todays_reporters()
            stale    = set(_db.get_stale_upcoming_tickers())
            to_fetch = sorted(todays | stale)
            if to_fetch:
                _fetch_and_persist_earnings(to_fetch, label="nightly")
            else:
                logger.info("No earnings reporters today and no stale upcoming tickers.")
        except Exception as exc:
            logger.error("Nightly earnings fetch error: %s", exc)

        # ── Political trades refresh ──────────────────────────────────────────
        try:
            trades   = _fetch_political_trades(tickers=_all_tickers(), limit=100)
            inserted = _db.upsert_political_trades(trades)
            logger.info("Political trades refresh — %d fetched, %d new", len(trades), inserted)
        except Exception as exc:
            logger.error("Political trades refresh error: %s", exc)

        # ── Weekly Sunday: refresh earnings calendar ──────────────────────────
        if date.today().weekday() == 6:  # 6 = Sunday
            _refresh_earnings_calendar()


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

    if not tickers and universe not in UNIVERSES:
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
    tickers_raw = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return jsonify({"error": "tickers param required (comma-separated)"}), 400
    tickers = tickers[:80]

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

    return jsonify({"results": results, "count": len(results)})


# ── Political trades ──────────────────────────────────────────────────────────

@app.route("/api/political-trades")
def get_political_trades():
    """GET /api/political-trades?tickers=AAPL,MSFT&limit=2000 — serves from DB."""
    tickers_raw = request.args.get("tickers", "")
    tickers = set(t.strip().upper() for t in tickers_raw.split(",") if t.strip()) if tickers_raw else None
    limit = min(int(request.args.get("limit", 2000)), 5000)
    try:
        data = _db.get_political_trades(tickers=tickers, limit=limit)
        return jsonify({"results": data, "count": len(data)})
    except Exception as exc:
        logger.error("political-trades error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/political-trades/backfill", methods=["POST"])
def backfill_political_trades():
    """POST /api/political-trades/backfill — fetch up to 1000 records, upsert."""
    def _run():
        logger.info("Political trades backfill started")
        trades   = _fetch_political_trades(tickers=_all_tickers(), limit=1000)
        inserted = _db.upsert_political_trades(trades)
        logger.info("Political trades backfill done — %d fetched, %d new", len(trades), inserted)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "backfill_started"})


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
