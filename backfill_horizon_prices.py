"""
backfill_horizon_prices.py
──────────────────────────
One-time script: for every political trade that has a price_at_trade but is
missing price_30d / price_60d / price_90d, fetch historical daily closes via
yfinance and persist all three horizon prices to the DB.

Uses yfinance — free, no API key required, full history available.

Fetches ONE yfinance request per ticker (full range), so ~hundreds of calls
total, not tens-of-thousands.

Run locally or as a Render one-off job:
    pip install yfinance
    python backfill_horizon_prices.py

Env vars required:
    DATABASE_URL   — Postgres connection string
"""

import os
import sys
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── yfinance ──────────────────────────────────────────────────────────────────
try:
    import yfinance as yf
except ImportError:
    log.error("yfinance not installed. Run: pip install yfinance")
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


def fetch_yf_history(ticker: str, from_date: str, to_date: str) -> dict:
    """
    Fetch yfinance daily closes for ticker between from_date and to_date.
    Returns {date_str: close_price} or {} on failure.
    """
    try:
        # to_date is exclusive in yfinance, add 1 day buffer
        to_dt = (datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        t = yf.Ticker(ticker)
        hist = t.history(start=from_date, end=to_dt, auto_adjust=True)
        if hist.empty:
            log.warning("  yfinance returned empty history for %s", ticker)
            return {}
        # Convert index (DatetimeIndex) to YYYY-MM-DD strings
        price_map = {
            row.Index.strftime("%Y-%m-%d"): float(row.Close)
            for row in hist.itertuples()
            if hasattr(row, "Close") and row.Close is not None
        }
        return price_map
    except Exception as exc:
        log.warning("yfinance fetch error for %s: %s", ticker, exc)
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

    # Pull all trades needing horizon prices
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

        price_map = fetch_yf_history(ticker, from_date, to_date)
        if not price_map:
            log.warning("  No price history for %s — skipping", ticker)
            total_errors += len(ticker_trades)
            continue

        log.info("  Got %d trading days of history for %s", len(price_map), ticker)

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

        # Small pause to be polite to Yahoo
        time.sleep(0.2)

    log.info("─" * 60)
    log.info("Backfill complete. filled=%d  errors=%d", total_filled, total_errors)


if __name__ == "__main__":
    main()
