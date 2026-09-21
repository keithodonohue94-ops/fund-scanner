"""
backfill_horizon_prices.py
──────────────────────────
One-time script: for every political trade that has a price_at_trade but is
missing price_30d / price_60d / price_90d, fetch FMP historical daily closes
and persist all three horizon prices to the DB.

Fetches ONE FMP request per ticker (full range), so ~hundreds of calls total,
not tens-of-thousands.

Run locally or as a Render one-off job:
    python backfill_horizon_prices.py

Env vars required:
    DATABASE_URL   — Postgres connection string
    FMP_API_KEY    — Financial Modelling Prep key
"""

import os
import sys
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import requests

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── env ───────────────────────────────────────────────────────────────────────
FMP_KEY = os.environ.get("FMP_API_KEY", "")
if not FMP_KEY:
    log.error("FMP_API_KEY not set — aborting.")
    sys.exit(1)

# Import db functions (script lives in same directory as db.py)
sys.path.insert(0, os.path.dirname(__file__))
import db as _db

# ── helpers ───────────────────────────────────────────────────────────────────
HORIZONS = [30, 60, 90]   # days post trade_date to price

def _add_days(date_str: str, n: int) -> str:
    """Return YYYY-MM-DD string n calendar days after date_str."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return (dt + timedelta(days=n)).strftime("%Y-%m-%d")

def _find_nearest_close(price_map: dict, target_date: str, max_search: int = 10) -> float | None:
    """
    Return the closing price for target_date or the next available trading day
    within max_search calendar days.  Returns None if nothing found.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    for offset in range(max_search + 1):
        candidate = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
        if candidate in price_map:
            return price_map[candidate]
    return None

def fetch_fmp_history(ticker: str, from_date: str, to_date: str) -> dict:
    """
    Fetch FMP historical daily closes for ticker between from_date and to_date.
    Returns {date_str: close_price} or {} on failure.
    """
    url = (
        f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
        f"?from={from_date}&to={to_date}&apikey={FMP_KEY}"
    )
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 429:
            log.warning("FMP rate limit hit — sleeping 60s")
            time.sleep(60)
            resp = requests.get(url, timeout=15)
        if not resp.ok:
            log.warning("FMP %s → HTTP %s", ticker, resp.status_code)
            return {}
        data = resp.json()
        historical = data.get("historical", [])
        return {row["date"]: row["close"] for row in historical if "date" in row and "close" in row}
    except Exception as exc:
        log.warning("FMP fetch error for %s: %s", ticker, exc)
        return {}

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    # Run DB migration to add price_90d column if not present
    try:
        from sqlalchemy import text
        with _db._engine.connect() as conn:
            conn.execute(text(
                "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS price_90d FLOAT"
            ))
            conn.commit()
        log.info("price_90d column ensured.")
    except Exception as exc:
        log.warning("Migration warning (may already exist): %s", exc)

    # Pull all trades needing horizon prices (batched in chunks of 5000)
    log.info("Fetching trades needing horizon prices…")
    trades = _db.get_trades_needing_horizon_prices(limit=100000)
    log.info("Found %d trades to process.", len(trades))

    if not trades:
        log.info("Nothing to do — all trades already have horizon prices.")
        return

    # Group by ticker
    by_ticker = defaultdict(list)
    for t in trades:
        by_ticker[t["ticker"]].append(t)

    log.info("Unique tickers to fetch: %d", len(by_ticker))

    total_filled = 0
    total_errors = 0

    for idx, (ticker, ticker_trades) in enumerate(by_ticker.items(), 1):
        # Date range: earliest trade_date → latest trade_date + 90 days
        dates = [t["trade_date"] for t in ticker_trades if t["trade_date"]]
        if not dates:
            continue
        from_date = min(dates)
        to_date   = _add_days(max(dates), 95)   # +5 buffer beyond 90d

        log.info("[%d/%d] %s — %d trades, range %s → %s",
                 idx, len(by_ticker), ticker, len(ticker_trades), from_date, to_date)

        price_map = fetch_fmp_history(ticker, from_date, to_date)
        if not price_map:
            log.warning("  No price history for %s — skipping", ticker)
            total_errors += len(ticker_trades)
            time.sleep(0.3)
            continue

        updates = []
        for trade in ticker_trades:
            td = trade["trade_date"]
            if not td:
                continue
            update = {"id": trade["id"]}
            for horizon in HORIZONS:
                target = _add_days(td, horizon)
                price  = _find_nearest_close(price_map, target)
                if price is not None:
                    update[f"price_{horizon}d"] = price
            if len(update) > 1:   # has at least one price beyond id
                updates.append(update)

        if updates:
            touched = _db.bulk_update_horizon_prices(updates)
            total_filled += touched
            log.info("  Updated %d rows for %s", touched, ticker)

        # Polite rate limiting: ~3 req/sec
        time.sleep(0.35)

    log.info("─" * 60)
    log.info("Backfill complete. filled=%d  errors=%d", total_filled, total_errors)


if __name__ == "__main__":
    main()
