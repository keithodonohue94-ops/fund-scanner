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
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from flask_cors import CORS

from scanner import scan_tickers, UNIVERSES

# ── Setup ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)   # allow the dashboard (any origin) to call this API

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

# Order to pre-scan on startup / nightly refresh (small → large)
SCAN_ORDER = ["portfolio", "myportfolio", "smh", "soxx", "ndx100", "sp500"]


# ── Scanner logic ─────────────────────────────────────────────────────────────

def _run_scan(universe_key: str):
    """Fetch fundamentals for one universe and update cache. Thread-safe."""
    if universe_key in SCANNING:
        logger.info("Already scanning %s — skipping", universe_key)
        return
    tickers = UNIVERSES.get(universe_key)
    if not tickers:
        logger.warning("Unknown universe: %s", universe_key)
        return

    SCANNING.add(universe_key)
    logger.info("Starting scan: %s (%d tickers)", universe_key, len(tickers))
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
    except Exception as exc:
        logger.error("Scan error (%s): %s", universe_key, exc)
    finally:
        SCANNING.discard(universe_key)


def _background_scheduler():
    """
    On startup: scan all universes sequentially (smallest first).
    Then repeat every 24 hours so cached data stays fresh.
    """
    while True:
        for ukey in SCAN_ORDER:
            _run_scan(ukey)
            time.sleep(10)          # brief pause between universes
        logger.info("Full cycle done. Sleeping 24 hours.")
        time.sleep(86_400)          # 24 hours


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "timestamp": _now()})


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
    """Kick off a fresh async scan for a universe."""
    body     = request.get_json(silent=True) or {}
    universe = body.get("universe", "portfolio")

    if universe not in UNIVERSES:
        return jsonify({"error": f"Unknown universe: {universe}"}), 400

    if universe in SCANNING:
        return jsonify({"status": "already_scanning", "universe": universe})

    t = threading.Thread(target=_run_scan, args=(universe,), daemon=True)
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
    if data:
        return jsonify({
            "ready":      True,
            "scanning":   scanning,
            "results":    data["results"],
            "count":      data["count"],
            "scanned_at": data["scanned_at"],
        })
    return jsonify({"ready": False, "scanning": scanning})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Start background scheduler in a daemon thread
    bg = threading.Thread(target=_background_scheduler, daemon=True)
    bg.start()

    import os
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
