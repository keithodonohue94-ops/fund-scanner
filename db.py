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
    Integer, Boolean, UniqueConstraint, Index, text
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


# ── Models ────────────────────────────────────────────────────────────────────

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
        UniqueConstraint("chamber", "name", "ticker", "trade_date", "type", name="uq_pol_trade"),
        Index("ix_pol_ticker",    "ticker"),
        Index("ix_pol_disc_date", "disc_date"),
        Index("ix_pol_name",      "name"),
    )


class EarningsSurprise(Base):
    """One row per company per fiscal quarter — never overwritten once actuals exist."""
    __tablename__ = "earnings_surprises"

    id             = Column(Integer, primary_key=True)
    ticker         = Column(String(20), nullable=False)
    fiscal_end     = Column(String(10), nullable=False)  # YYYY-MM-DD fiscal quarter end
    announced_date = Column(String(10))                   # YYYY-MM-DD actual report date
    is_upcoming    = Column(Boolean, default=False)        # True until company reports

    eps_actual     = Column(Float)
    eps_est        = Column(Float)
    eps_surp       = Column(Float)   # % beat/miss

    rev_actual     = Column(Float)
    rev_est        = Column(Float)
    rev_surp       = Column(Float)   # % beat/miss

    fetched_at     = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ticker", "fiscal_end", name="uq_earnings"),
        Index("ix_earn_ticker",     "ticker"),
        Index("ix_earn_fiscal_end", "fiscal_end"),
        Index("ix_earn_upcoming",   "is_upcoming"),
    )


class ReportCalendar(Base):
    """Upcoming earnings report dates fetched from FMP earnings calendar."""
    __tablename__ = "report_calendar"

    id          = Column(Integer, primary_key=True)
    ticker      = Column(String(20), nullable=False)
    report_date = Column(String(10), nullable=False)  # YYYY-MM-DD
    fetched_at  = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ticker", "report_date", name="uq_cal"),
        Index("ix_cal_report_date", "report_date"),
        Index("ix_cal_ticker",      "ticker"),
    )


# ── Public API ────────────────────────────────────────────────────────────────

def init_db():
    """Create tables if they don't exist. Call once at startup."""
    Base.metadata.create_all(_engine)
    logger.info("DB ready: %s", _DATABASE_URL.split("@")[-1])  # hide credentials


# ── Fundamentals snapshot functions ──────────────────────────────────────────

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
    """Return daily snapshots for a single ticker, newest-first limited to `days`."""
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
    """Return daily aggregate (average) metrics for a universe over `days` trading days."""
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


# ── Political trades functions ────────────────────────────────────────────────

def upsert_political_trades(trades: list) -> int:
    """Insert political trade records, ignoring duplicates. Returns new rows inserted."""
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
    """Query stored political trades, newest disc_date first."""
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


# ── Earnings surprise functions ───────────────────────────────────────────────

def upsert_earnings(ticker: str, rows: list) -> int:
    """
    Upsert earnings surprise rows for a ticker.
    - New rows are inserted.
    - Existing upcoming rows are updated when actuals arrive (eps_surp fills in).
    - Existing confirmed actuals are never overwritten.
    Returns number of new rows inserted.
    """
    if not rows:
        return 0
    session = _Session()
    inserted = updated = 0
    try:
        for row in rows:
            fiscal_end = (row.get("fiscal_end") or "")[:10]
            if not fiscal_end:
                continue

            is_upcoming = bool(row.get("is_upcoming"))
            existing = session.query(EarningsSurprise).filter_by(
                ticker=ticker.upper(),
                fiscal_end=fiscal_end,
            ).first()

            if existing:
                # Only update if: row was previously upcoming and now has actuals
                if existing.is_upcoming and not is_upcoming:
                    existing.announced_date = (row.get("date") or "")[:10]
                    existing.is_upcoming    = False
                    existing.eps_actual     = row.get("eps_actual")
                    existing.eps_est        = row.get("eps_est")
                    existing.eps_surp       = row.get("eps_surp")
                    existing.rev_actual     = row.get("rev_actual")
                    existing.rev_est        = row.get("rev_est")
                    existing.rev_surp       = row.get("rev_surp")
                    existing.fetched_at     = datetime.utcnow()
                    updated += 1
                # Confirmed actuals are left untouched
            else:
                session.add(EarningsSurprise(
                    ticker         = ticker.upper(),
                    fiscal_end     = fiscal_end,
                    announced_date = (row.get("date") or "")[:10],
                    is_upcoming    = is_upcoming,
                    eps_actual     = row.get("eps_actual"),
                    eps_est        = row.get("eps_est"),
                    eps_surp       = row.get("eps_surp"),
                    rev_actual     = row.get("rev_actual"),
                    rev_est        = row.get("rev_est"),
                    rev_surp       = row.get("rev_surp"),
                ))
                inserted += 1

        session.commit()
        logger.info("upsert_earnings %s: %d inserted, %d upcoming→actual", ticker, inserted, updated)
        return inserted
    except Exception as exc:
        session.rollback()
        logger.error("upsert_earnings error %s: %s", ticker, exc)
        raise
    finally:
        session.close()


def get_earnings_db(tickers: list) -> dict:
    """
    Return earnings surprise rows from DB keyed by ticker.
    Result format matches _fetch_earnings_surprises output.
    """
    if not tickers:
        return {}
    session = _Session()
    try:
        rows = (
            session.query(EarningsSurprise)
            .filter(EarningsSurprise.ticker.in_([t.upper() for t in tickers]))
            .order_by(EarningsSurprise.ticker, EarningsSurprise.fiscal_end.desc())
            .all()
        )
        result: dict = {}
        for r in rows:
            if r.ticker not in result:
                result[r.ticker] = []
            result[r.ticker].append({
                "date":        r.announced_date,
                "fiscal_end":  r.fiscal_end,
                "is_upcoming": r.is_upcoming,
                "eps_actual":  r.eps_actual,
                "eps_est":     r.eps_est,
                "eps_surp":    r.eps_surp,
                "rev_actual":  r.rev_actual,
                "rev_est":     r.rev_est,
                "rev_surp":    r.rev_surp,
            })
        return result
    finally:
        session.close()


def count_earnings() -> int:
    """Return total rows in earnings_surprises table (used for startup backfill check)."""
    session = _Session()
    try:
        return session.query(EarningsSurprise).count()
    finally:
        session.close()


def get_stale_upcoming_tickers() -> list:
    """
    Return tickers that have upcoming rows whose fiscal_end has already passed by 14+ days.
    These companies should have reported by now — re-fetch to pick up actuals.
    """
    cutoff = datetime.utcnow().strftime("%Y-%m-%d")
    # fiscal_end < today - 14 days means the quarter closed 2+ weeks ago
    from datetime import timedelta
    cutoff = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
    session = _Session()
    try:
        rows = (
            session.query(EarningsSurprise.ticker)
            .filter(
                EarningsSurprise.is_upcoming == True,
                EarningsSurprise.fiscal_end < cutoff,
            )
            .distinct()
            .all()
        )
        return [r[0] for r in rows]
    finally:
        session.close()


# ── Report calendar functions ─────────────────────────────────────────────────

def upsert_calendar(entries: list) -> int:
    """
    Upsert upcoming report dates. Returns number of new rows inserted.
    entries: list of {ticker, report_date} dicts.
    """
    if not entries:
        return 0
    session = _Session()
    inserted = 0
    try:
        for e in entries:
            ticker      = (e.get("ticker") or "").upper()
            report_date = (e.get("report_date") or "")[:10]
            if not ticker or not report_date:
                continue
            existing = session.query(ReportCalendar).filter_by(
                ticker=ticker, report_date=report_date
            ).first()
            if not existing:
                session.add(ReportCalendar(ticker=ticker, report_date=report_date))
                inserted += 1
        session.commit()
        logger.info("upsert_calendar: %d new entries", inserted)
        return inserted
    except Exception as exc:
        session.rollback()
        logger.error("upsert_calendar error: %s", exc)
        raise
    finally:
        session.close()


def get_todays_reporters() -> set:
    """Return set of tickers scheduled to report today."""
    today = date.today().strftime("%Y-%m-%d")
    session = _Session()
    try:
        rows = session.query(ReportCalendar).filter_by(report_date=today).all()
        return {r.ticker for r in rows}
    finally:
        session.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

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
