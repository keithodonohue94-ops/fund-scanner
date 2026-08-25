"""
fund-scanner/app.py
Flask backend for the Poppa Alpha Fundamentals tab.

Endpoints:
  GET  /api/health                       — health check
  GET  /api/stats                        — cache status & last scan times
  GET  /api/results?universe=portfolio   — return cached scan results
  POST /api/scan  {"universe":"portfolio"}  — trigger async re-scan
"""

import threading
import time
import logging
import concurrent.futures
import hmac
import hashlib
import os
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request
from flask_cors import CORS

from scanner import scan_tickers, UNIVERSES, _fetch_earnings_surprises
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
    if request.path in ("/api/health",):
        return None
    if request.path.startswith("/api/"):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Unauthorized"}), 401
        if not hmac.compare_digest(auth[7:], _VALID_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401
    return None

# ── In-memory cache ───────────────────────────────────────────────────────────
# Structure:  CACHE[universe_key] = {
#   "results":    [...],
#   "scanned_at": "2026-07-28T02:00:00Z",
#   "count":      17,
#   "universe":   "portfolio",
# }
CACHE: dict = {}
SCANNING: set = set()          # universes currently being scanned
_cache_lock = threading.Lock()

# All universes scanned in the daily 4pm EST run (small → large)
SCAN_ORDER = [
    "portfolio", "myportfolio", "aiinfra", "cybersec",
    "rareearths", "energy", "orbital",
    "smh", "soxx", "ndx100", "sp500",
]


# ── Scanner logic ─────────────────────────────────────────────────────────────

def _run_scan(universe_key: str, save_to_db: bool = False, tickers: list = None):
    """Fetch fundamentals for one universe and update cache. Thread-safe.

    tickers: if provided, use these directly (from frontend) instead of hardcoded UNIVERSES.
    save_to_db: only True for the scheduled 4pm ET EOD run.
    """
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
    """Return seconds until the next 4:00 PM Eastern time (handles EST/EDT)."""
    # Eastern is UTC-5 (EST) or UTC-4 (EDT); we approximate with UTC-5 year-round.
    # Close enough for a market-close scan — worst case it runs at 4pm EST = 5pm EDT.
    now_utc = datetime.now(timezone.utc)
    eastern_offset = timedelta(hours=-5)
    now_et = now_utc + eastern_offset
    target = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_et >= target:
        target += timedelta(days=1)
    delta = (target - now_et).total_seconds()
    logger.info("Next 4pm ET scan in %.0f seconds (%.1f hours)", delta, delta / 3600)
    return delta


def _background_scheduler():
    """
    On startup: scan portfolio to warm the cache.
    Then at 4:00 PM Eastern every day: scan all universes.
    """
    time.sleep(5)  # let gunicorn finish booting
    _run_scan("portfolio")
    logger.info("Startup scan done. Waiting for next 4pm ET window.")
    while True:
        time.sleep(_next_4pm_eastern())
        logger.info("4pm ET — starting EOD scan (%d universes)", len(SCAN_ORDER))
        for ukey in SCAN_ORDER:
            _run_scan(ukey, save_to_db=True)
            time.sleep(15)   # 15s between universes to avoid FMP rate limits
        logger.info("EOD scan complete — all results saved to DB.")


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
        # Return just the keys and first values so we can see the structure
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
    # Most recent scan across all universes
    last_scan = max(cached, key=lambda x: x["scanned_at"]) if cached else None
    return jsonify({
        "status":        "ok",
        "last_scan":     last_scan,
        "cached":        cached,
        "scanning":      list(SCANNING),
        "universes":     list(UNIVERSES.keys()),
    })


@app.route("/api/results")
def results():
    universe = request.args.get("universe", "portfolio")
    with _cache_lock:
        data = CACHE.get(universe)

    if not data:
        # If we don't have this universe cached yet, kick off a scan
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
    """Kick off a fresh async scan for a universe.

    Accepts {universe, tickers} — if tickers provided, uses those directly
    rather than the hardcoded UNIVERSES dict, so any universe from the UI works.
    """
    body     = request.get_json(silent=True) or {}
    universe = body.get("universe", "portfolio")
    tickers  = body.get("tickers") or None  # list from frontend, or None to fall back to UNIVERSES

    if not tickers and universe not in UNIVERSES:
        return jsonify({"error": f"Unknown universe: {universe}"}), 400

    if universe in SCANNING:
        return jsonify({"status": "already_scanning", "universe": universe})

    # Clear stale cache so poll waits for fresh results instead of returning old data
    with _cache_lock:
        CACHE.pop(universe, None)

    t = threading.Thread(target=_run_scan, args=(universe,), kwargs={"tickers": tickers}, daemon=True)
    t.start()
    return jsonify({"status": "scanning", "universe": universe})


# ── Poll endpoint ─────────────────────────────────────────────────────────────

@app.route("/api/poll")
def poll():
    """
    Frontend can poll this while waiting for a scan to finish.
    Returns {ready: true/false, results: [...]}
    """
    universe = request.args.get("universe", "portfolio")
    with _cache_lock:
        data = CACHE.get(universe)
    scanning = universe in SCANNING
    # Don't serve stale empty results while a fresh scan is running
    if data and (data["count"] > 0 or not scanning):
        return jsonify({
            "ready":      True,
            "scanning":   scanning,
            "results":    data["results"],
            "count":      data["count"],
            "scanned_at": data["scanned_at"],
        })
    return jsonify({"ready": False, "scanning": scanning})


# ── Earnings tracker endpoint ─────────────────────────────────────────────────

@app.route("/api/earnings")
def get_earnings():
    """
    GET /api/earnings?tickers=AAPL,MSFT,NVDA
    Returns last 4 quarters of EPS/revenue actual vs estimate for each ticker.
    """
    tickers_raw = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return jsonify({"error": "tickers param required (comma-separated)"}), 400
    tickers = tickers[:80]  # safety cap

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        future_map = {pool.submit(_fetch_earnings_surprises, t): t for t in tickers}
        for fut in concurrent.futures.as_completed(future_map):
            ticker = future_map[fut]
            try:
                results[ticker] = fut.result()
            except Exception as exc:
                logger.warning("earnings fetch error %s: %s", ticker, exc)
                results[ticker] = []

    return jsonify({"results": results, "count": len(results)})


# ── History endpoints ─────────────────────────────────────────────────────────

@app.route("/api/history/ticker")
def history_ticker():
    """
    GET /api/history/ticker?ticker=NVDA&universe=soxx&days=90
    Returns daily snapshots for one ticker.
    """
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
    """
    GET /api/history/universe?universe=soxx&days=90
    Returns daily average metrics for a universe over time.
    """
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
    """
    GET /api/history/tickers?universe=soxx
    Returns list of tickers that have historical data.
    """
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
    import os
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
