"""
db.py — Persistent storage for EOD fundamentals snapshots.

Schema: one row per (snapshot_date, universe, ticker).
Duplicate scans on the same day upsert (update) rather than insert.

Requires DATABASE_URL env var pointing to a PostgreSQL instance.
Falls back to SQLite for local development.
"""

import os
import logging
from collections import defaultdict
from datetime import date, datetime

from sqlalchemy import (
    create_engine, Column, Float, String, Date, DateTime,
    Integer, UniqueConstraint, Index, text
)
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger(__name__)

# ── Engine setup ──────────────────────────────────────────────────────────────

_DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///fundamentals.db")

# Render supplies postgres:// — SQLAlchemy needs postgresql://
if _DATABASE_URL.startswith("postgres://"):
    _DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1)

_engine = create_engine(
    _DATABASE_URL,
    pool_pre_ping=True,       # detect stale connections
    pool_recycle=300,         # recycle connections every 5 min
)
_Session = sessionmaker(bind=_engine)
Base = declarative_base()


# ── Model ─────────────────────────────────────────────────────────────────────

class FundamentalsSnapshot(Base):
    __tablename__ = "fundamentals_snapshot"

    id            = Column(Integer, primary_key=True)
    snapshot_date = Column(Date,    nullable=False)
    universe      = Column(String(50), nullable=False)
    ticker        = Column(String(20), nullable=False)

    # Price & market
    price         = Column(Float)
    chg_pct       = Column(Float)
    mkt_cap       = Column(Float)

    # Valuation
    pe            = Column(Float)
    fwd_pe        = Column(Float)
    peg           = Column(Float)
    ps            = Column(Float)

    # Price target
    avg_pt        = Column(Float)   # analyst consensus price target ($)
    pt_pct        = Column(Float)   # % current price is above (+) or below (-) avg PT

    # Capital structure
    debt_to_equity = Column(Float)

    # Growth & margins
    rev_growth    = Column(Float)
    gross_margin  = Column(Float)
    op_margin     = Column(Float)
    op_delta      = Column(Float)   # QoQ op margin change
    gm_delta      = Column(Float)   # QoQ gross margin change

    created_at    = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("snapshot_date", "universe", "ticker", name="uq_snap"),
        Index("ix_snap_ticker",   "ticker"),
        Index("ix_snap_universe", "universe", "snapshot_date"),
    )


# ── Political trades model ────────────────────────────────────────────────────

class PoliticalTrade(Base):
    __tablename__ = "political_trades"

    id          = Column(Integer, primary_key=True)
    chamber     = Column(String(10),  nullable=False)   # "Senate" | "House"
    name        = Column(String(200), nullable=False)
    party       = Column(String(20))
    district    = Column(String(100))
    ticker      = Column(String(20),  nullable=False)
    asset       = Column(String(500))
    type        = Column(String(100))                   # "Purchase", "Sale (Full)", …
    amount      = Column(String(100))                   # "$1,001 - $15,000"
    trade_date  = Column(String(10))                    # YYYY-MM-DD
    disc_date   = Column(String(10))                    # YYYY-MM-DD (disclosed / filed)
    lag_days    = Column(Integer)
    link        = Column(String(500))
    created_at  = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        # Deduplicate on the natural key — same politician, same ticker, same trade date, same type
        UniqueConstraint("chamber", "name", "ticker", "trade_date", "type", name="uq_pol_trade"),
        Index("ix_pol_ticker",    "ticker"),
        Index("ix_pol_disc_date", "disc_date"),
        Index("ix_pol_name",      "name"),
    )


# ── Public API ────────────────────────────────────────────────────────────────

def init_db():
    """Create tables if they don't exist. Call once at startup."""
    Base.metadata.create_all(_engine)
    logger.info("DB ready: %s", _DATABASE_URL.split("@")[-1])  # hide credentials


def save_snapshot(universe: str, results: list):
    """
    Upsert a list of ticker dicts for today's date.
    If a record already exists for (today, universe, ticker) it is updated.
    """
    if not results:
        return
    today = date.today()
    session = _Session()
    inserted = updated = 0
    try:
        for row in results:
            ticker = row.get("ticker", "").upper()
            if not ticker:
                continue

            existing = session.query(FundamentalsSnapshot).filter_by(
                snapshot_date=today,
                universe=universe,
                ticker=ticker,
            ).first()

            fields = {
                "price":          row.get("price"),
                "chg_pct":        row.get("chg_pct"),
                "mkt_cap":        row.get("mkt_cap"),
                "avg_pt":         row.get("avg_pt"),
                "pt_pct":         row.get("pt_pct"),
                "pe":             row.get("pe"),
                "fwd_pe":         row.get("fwd_pe"),
                "peg":            row.get("peg"),
                "ps":             row.get("ps"),
                "debt_to_equity": row.get("debt_to_equity"),
                "rev_growth":     row.get("rev_growth"),
                "gross_margin":   row.get("gross_margin"),
                "op_margin":      row.get("op_margin"),
                "op_delta":       row.get("op_delta"),
                "gm_delta":       row.get("gm_delta"),
            }

            if existing:
                for k, v in fields.items():
                    setattr(existing, k, v)
                updated += 1
            else:
                session.add(FundamentalsSnapshot(
                    snapshot_date=today,
                    universe=universe,
                    ticker=ticker,
                    **fields,
                ))
                inserted += 1

        session.commit()
        logger.info("DB snapshot saved — %s %s: %d inserted, %d updated",
                    today, universe, inserted, updated)
    except Exception as exc:
        session.rollback()
        logger.error("DB save_snapshot error: %s", exc)
    finally:
        session.close()


def get_ticker_history(ticker: str, universe: str | None = None, days: int = 90) -> list:
    """
    Return daily snapshots for a single ticker, newest-first limited to `days`.
    Optionally filter to a specific universe.
    """
    session = _Session()
    try:
        q = session.query(FundamentalsSnapshot).filter(
            FundamentalsSnapshot.ticker == ticker.upper()
        )
        if universe:
            q = q.filter(FundamentalsSnapshot.universe == universe)
        rows = q.order_by(FundamentalsSnapshot.snapshot_date.desc()).limit(days).all()
        return [_row_to_dict(r) for r in reversed(rows)]
    finally:
        session.close()


def get_universe_history(universe: str, days: int = 90) -> list:
    """
    Return daily aggregate (average) metrics for a universe over `days` trading days.
    """
    session = _Session()
    try:
        rows = (
            session.query(FundamentalsSnapshot)
            .filter(FundamentalsSnapshot.universe == universe)
            .order_by(FundamentalsSnapshot.snapshot_date.asc())
            .all()
        )
        by_date = defaultdict(list)
        for r in rows:
            by_date[r.snapshot_date].append(r)

        metric_fields = ["pe", "fwd_pe", "peg", "ps",
                         "rev_growth", "gross_margin", "op_margin"]

        result = []
        for dt in sorted(by_date.keys())[-days:]:
            day = by_date[dt]
            entry = {"date": dt.isoformat(), "ticker_count": len(day)}
            for f in metric_fields:
                vals = [getattr(r, f) for r in day if getattr(r, f) is not None]
                entry[f"avg_{f}"] = round(sum(vals) / len(vals), 4) if vals else None
            result.append(entry)

        return result
    finally:
        session.close()


def get_ticker_list(universe: str | None = None) -> list:
    """Return distinct tickers that have history (optionally filtered by universe)."""
    session = _Session()
    try:
        q = session.query(FundamentalsSnapshot.ticker).distinct()
        if universe:
            q = q.filter(FundamentalsSnapshot.universe == universe)
        return sorted(r[0] for r in q.all())
    finally:
        session.close()


# ── Political trades DB functions ────────────────────────────────────────────

def upsert_political_trades(trades: list) -> int:
    """
    Insert political trade records, ignoring duplicates (ON CONFLICT DO NOTHING).
    Returns number of new rows inserted.
    """
    if not trades:
        return 0
    session = _Session()
    inserted = 0
    try:
        for t in trades:
            existing = session.query(PoliticalTrade).filter_by(
                chamber=t.get("chamber", ""),
                name=t.get("name", ""),
                ticker=t.get("ticker", ""),
                trade_date=t.get("trade_date", ""),
                type=t.get("type", ""),
            ).first()
            if existing:
                continue
            row = PoliticalTrade(
                chamber    = t.get("chamber", ""),
                name       = t.get("name", ""),
                party      = t.get("party", ""),
                district   = t.get("district", ""),
                ticker     = t.get("ticker", ""),
                asset      = t.get("asset", ""),
                type       = t.get("type", ""),
                amount     = t.get("amount", ""),
                trade_date = t.get("trade_date", ""),
                disc_date  = t.get("disc_date", ""),
                lag_days   = t.get("lag_days"),
                link       = t.get("link", ""),
            )
            session.add(row)
            inserted += 1
        session.commit()
        return inserted
    except Exception as exc:
        session.rollback()
        logger.error("upsert_political_trades error: %s", exc)
        raise
    finally:
        session.close()


def get_political_trades(tickers: set = None, since_date: str = None, limit: int = 2000) -> list:
    """
    Query stored political trades.
    tickers:    optional set of uppercase ticker symbols to filter
    since_date: optional YYYY-MM-DD — only return rows with disc_date >= this
    limit:      max rows returned (newest disc_date first)
    """
    session = _Session()
    try:
        q = session.query(PoliticalTrade).order_by(PoliticalTrade.disc_date.desc())
        if tickers:
            q = q.filter(PoliticalTrade.ticker.in_(tickers))
        if since_date:
            q = q.filter(PoliticalTrade.disc_date >= since_date)
        rows = q.limit(limit).all()
        return [_pol_row_to_dict(r) for r in rows]
    finally:
        session.close()


def _pol_row_to_dict(r: PoliticalTrade) -> dict:
    return {
        "chamber":    r.chamber,
        "name":       r.name,
        "party":      r.party,
        "district":   r.district,
        "ticker":     r.ticker,
        "asset":      r.asset,
        "type":       r.type,
        "amount":     r.amount,
        "trade_date": r.trade_date,
        "disc_date":  r.disc_date,
        "lag_days":   r.lag_days,
        "link":       r.link,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_dict(r: FundamentalsSnapshot) -> dict:
    return {
        "date":           r.snapshot_date.isoformat(),
        "universe":       r.universe,
        "ticker":         r.ticker,
        "price":          r.price,
        "chg_pct":        r.chg_pct,
        "mkt_cap":        r.mkt_cap,
        "avg_pt":         r.avg_pt,
        "pt_pct":         r.pt_pct,
        "pe":             r.pe,
        "fwd_pe":         r.fwd_pe,
        "peg":            r.peg,
        "ps":             r.ps,
        "debt_to_equity": r.debt_to_equity,
        "rev_growth":     r.rev_growth,
        "gross_margin":   r.gross_margin,
        "op_margin":      r.op_margin,
        "op_delta":       r.op_delta,
        "gm_delta":       r.gm_delta,
    }
