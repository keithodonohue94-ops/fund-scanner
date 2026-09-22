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

    # EPS components (for browser-side multiple recomputation)
    ttm_eps        = Column(Float, nullable=True)
    ntm_eps        = Column(Float, nullable=True)
    eps_growth_rate = Column(Float, nullable=True)

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
    sector      = Column(String(100))                   # Technology, Healthcare, …
    price_at_trade     = Column(Float)                  # EOD close on trade_date
    price_last         = Column(Float)                  # Most recent closing price
    price_last_updated = Column(DateTime)               # When price_last was fetched
    price_30d          = Column(Float)                  # EOD close ~30 days after trade_date
    price_60d          = Column(Float)                  # EOD close ~60 days after trade_date
    price_90d          = Column(Float)                  # EOD close ~90 days after trade_date
    created_at  = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("chamber", "name", "ticker", "trade_date", "type", name="uq_pol_trade"),
        Index("ix_pol_ticker",    "ticker"),
        Index("ix_pol_disc_date", "disc_date"),
        Index("ix_pol_name",      "name"),
    )




class TickerMetadata(Base):
    """Shared ticker→sector/company_name cache. Populated on demand from FMP /profile."""
    __tablename__ = "ticker_metadata"

    ticker        = Column(String(20), primary_key=True)
    company_name  = Column(String(500))
    sector        = Column(String(100))
    updated_at    = Column(DateTime, default=datetime.utcnow)


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


class TechnicalsSnapshot(Base):
    """Daily technicals snapshot — covers Moving Averages + Market Strength tabs."""
    __tablename__ = "technicals_snapshot"

    id            = Column(Integer, primary_key=True)
    snapshot_date = Column(Date,    nullable=False)
    universe      = Column(String(50), nullable=False)
    ticker        = Column(String(20), nullable=False)

    # Simple moving averages
    sma_20  = Column(Float)
    sma_50  = Column(Float)
    sma_200 = Column(Float)

    # Exponential moving averages
    ema_9   = Column(Float)
    ema_21  = Column(Float)

    # Price position relative to MAs (%)
    price_vs_sma20  = Column(Float)
    price_vs_sma50  = Column(Float)
    price_vs_sma200 = Column(Float)

    # Momentum
    rsi_14 = Column(Float)

    # 52-week range
    high_52w          = Column(Float)
    low_52w           = Column(Float)
    pct_from_high_52w = Column(Float)   # negative = below high
    pct_from_low_52w  = Column(Float)   # positive = above low

    # Volume
    avg_vol_20d = Column(Float)
    vol_ratio   = Column(Float)   # today / 20d avg

    # Relative strength vs SPY (excess return %)
    rel_strength_1m = Column(Float)
    rel_strength_3m = Column(Float)
    rel_strength_6m = Column(Float)

    # Beta (30-day rolling vs SPY)
    beta_30d = Column(Float)

    # MA cross signal string (mirrors frontend maGetCross return value)
    cross_signal = Column(String(20))   # 'golden_cross'|'death_cross'|'above_200'|'below_200'

    # MA alignment signal (mirrors frontend maGetAlignment return value)
    alignment    = Column(String(20))   # 'bull_stack'|'bear_stack'|'bullish'|'bearish'|'mixed'

    # ADX / Directional Movement (Market Strength tab — techCalcADX)
    adx          = Column(Float)
    plus_di      = Column(Float)
    minus_di     = Column(Float)
    di_cross     = Column(String(20))   # 'bull_cross'|'bear_cross'|'bull'|'bear'

    # ATR-14 (Market Strength tab)
    atr          = Column(Float)

    # Legacy boolean columns kept for any existing rows — new rows use cross_signal instead
    golden_cross = Column(Boolean)
    death_cross  = Column(Boolean)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("snapshot_date", "universe", "ticker", name="uq_tech_snap"),
        Index("ix_tech_ticker",   "ticker"),
        Index("ix_tech_universe", "universe", "snapshot_date"),
    )


class WingSnapshot(Base):
    """
    Wing Scanner results — one row per equidistant OTM put/call pair with confirmed IV skew bias.
    Mirrors the frontend Wing Scanner (panel-wing) using Tradier API.
    Multiple rows per (date, universe, ticker) — one per detected pair.
    """
    __tablename__ = "wing_snapshot"

    id            = Column(Integer, primary_key=True)
    snapshot_date = Column(Date,    nullable=False)
    universe      = Column(String(50), nullable=False)
    ticker        = Column(String(20), nullable=False)

    expiry        = Column(String(10), nullable=False)   # YYYY-MM-DD
    dte           = Column(Integer)                      # days to expiry
    otm_pct       = Column(Float)                        # % OTM of put below spot
    spot          = Column(Float)                        # underlying price at scan time

    put_strike    = Column(Float,  nullable=False)
    call_strike   = Column(Float)                        # equidistant on the call side

    # IVs stored as percentages (e.g. 35.0 = 35%)
    put_iv        = Column(Float)
    call_iv       = Column(Float)
    iv_diff       = Column(Float)   # put_iv - call_iv; negative = put bias, positive = call bias

    bias          = Column(String(10))   # 'put' | 'call'

    put_volume    = Column(Integer)
    call_volume   = Column(Integer)
    put_oi        = Column(Integer)
    call_oi       = Column(Integer)
    pcr           = Column(Float)        # put_oi / call_oi

    put_mid       = Column(Float)        # option mid price
    call_mid      = Column(Float)

    created_at    = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("snapshot_date", "universe", "ticker", "expiry", "put_strike",
                         name="uq_wing_snap"),
        Index("ix_wing_ticker",   "ticker"),
        Index("ix_wing_universe", "universe", "snapshot_date"),
        Index("ix_wing_bias",     "bias"),
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
    """Create tables if they don't exist and apply lightweight column migrations."""
    Base.metadata.create_all(_engine)
    # ── Column migrations (idempotent ALTER TABLE) ─────────────────────────
    # Add sector to political_trades if not present (new column, July 2025).
    _migrations = [
        "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS sector VARCHAR(100)",
        "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS price_at_trade FLOAT",
        "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS price_last FLOAT",
        "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS price_last_updated TIMESTAMP",
        "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS price_30d FLOAT",
        "ALTER TABLE political_trades ADD COLUMN IF NOT EXISTS price_60d FLOAT",
        # fundamentals_snapshot columns added Sep 2026
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS avg_pt FLOAT",
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS pt_pct FLOAT",
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS op_delta FLOAT",
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS gm_delta FLOAT",
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS ttm_eps FLOAT",
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS ntm_eps FLOAT",
        "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS eps_growth_rate FLOAT",
        # Widen uq_pol_trade to include amount (Sep 2026) — drop old, add new (idempotent via IF NOT EXISTS)
        "ALTER TABLE political_trades DROP CONSTRAINT IF EXISTS uq_pol_trade",
        "ALTER TABLE political_trades ADD CONSTRAINT uq_pol_trade UNIQUE (chamber, name, ticker, trade_date, type, amount)",
        # technicals_snapshot columns added Sep 2026
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS golden_cross BOOLEAN",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS death_cross  BOOLEAN",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS rel_strength_6m FLOAT",
        # technicals_snapshot: MA cross + Market Strength columns (mirrors frontend maScanTicker / techScanTicker)
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS cross_signal VARCHAR(20)",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS alignment    VARCHAR(20)",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS adx          FLOAT",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS plus_di      FLOAT",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS minus_di     FLOAT",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS di_cross     VARCHAR(20)",
        "ALTER TABLE technicals_snapshot ADD COLUMN IF NOT EXISTS atr          FLOAT",
    ]
    for sql in _migrations:
        try:
            with _engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception as exc:
            if "duplicate" not in str(exc).lower() and "already exists" not in str(exc).lower():
                logger.warning("Migration failed (%s): %s", sql[:60], exc)
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
                "ttm_eps":        row.get("ttm_eps"),
                "ntm_eps":        row.get("ntm_eps"),
                "eps_growth_rate": row.get("eps_growth_rate"),
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



def get_fundamentals_snapshot(universe: str) -> list:
    """Return the latest snapshot for all tickers in a given universe."""
    session = _Session()
    try:
        from sqlalchemy import text
        rows = session.execute(text("""
            SELECT DISTINCT ON (ticker) *
            FROM fundamentals_snapshot
            WHERE universe = :u
            ORDER BY ticker, snapshot_date DESC
        """), {"u": universe}).fetchall()
        return [_row_to_dict(r) for r in rows]
    except Exception as e:
        print(f"[db] get_fundamentals_snapshot error: {e}")
        return []
    finally:
        session.close()


# ── Technicals snapshot functions ────────────────────────────────────────────

def save_technicals_snapshot(universe: str, results: list):
    """Upsert technicals rows for today. One row per (date, universe, ticker)."""
    if not results:
        return
    today = date.today()
    session = _Session()
    inserted = updated = 0
    try:
        for row in results:
            ticker = (row.get("ticker") or "").upper()
            if not ticker:
                continue
            existing = session.query(TechnicalsSnapshot).filter_by(
                snapshot_date=today, universe=universe, ticker=ticker,
            ).first()
            fields = {k: row.get(k) for k in (
                "sma_20", "sma_50", "sma_200", "ema_9", "ema_21",
                "price_vs_sma20", "price_vs_sma50", "price_vs_sma200",
                "rsi_14", "high_52w", "low_52w",
                "pct_from_high_52w", "pct_from_low_52w",
                "avg_vol_20d", "vol_ratio",
                "rel_strength_1m", "rel_strength_3m", "rel_strength_6m",
                "beta_30d", "golden_cross", "death_cross",
                # MA cross + Market Strength (mirrors frontend maScanTicker / techScanTicker)
                "cross_signal", "alignment",
                "adx", "plus_di", "minus_di", "di_cross", "atr",
            )}
            if existing:
                for k, v in fields.items():
                    setattr(existing, k, v)
                updated += 1
            else:
                session.add(TechnicalsSnapshot(
                    snapshot_date=today, universe=universe, ticker=ticker, **fields,
                ))
                inserted += 1
        session.commit()
        logger.info("Technicals snapshot saved — %s %s: %d inserted, %d updated",
                    today, universe, inserted, updated)
    except Exception as exc:
        session.rollback()
        logger.error("save_technicals_snapshot error: %s", exc)
    finally:
        session.close()


def get_technicals_snapshot(universe: str) -> list:
    """Return the latest technicals row for each ticker in a universe."""
    session = _Session()
    try:
        rows = session.execute(text("""
            SELECT DISTINCT ON (ticker) *
            FROM technicals_snapshot
            WHERE universe = :u
            ORDER BY ticker, snapshot_date DESC
        """), {"u": universe}).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as e:
        logger.error("get_technicals_snapshot error: %s", e)
        return []
    finally:
        session.close()


# ── Wing snapshot functions (Options Skew tab) ────────────────────────────────

def save_wing_snapshot(universe: str, pairs: list):
    """
    Upsert wing scan pair rows for today.
    Multiple rows per ticker (one per detected equidistant OTM pair).
    """
    if not pairs:
        return
    today = date.today()
    session = _Session()
    inserted = updated = 0
    try:
        for row in pairs:
            ticker     = (row.get("ticker") or "").upper()
            expiry     = row.get("expiry", "")
            put_strike = row.get("put_strike")
            if not ticker or not expiry or put_strike is None:
                continue
            existing = session.query(WingSnapshot).filter_by(
                snapshot_date=today,
                universe=universe,
                ticker=ticker,
                expiry=expiry,
                put_strike=put_strike,
            ).first()
            fields = {k: row.get(k) for k in (
                "dte", "otm_pct", "spot", "call_strike",
                "put_iv", "call_iv", "iv_diff", "bias",
                "put_volume", "call_volume", "put_oi", "call_oi",
                "pcr", "put_mid", "call_mid",
            )}
            if existing:
                for k, v in fields.items():
                    setattr(existing, k, v)
                updated += 1
            else:
                session.add(WingSnapshot(
                    snapshot_date=today,
                    universe=universe,
                    ticker=ticker,
                    expiry=expiry,
                    put_strike=put_strike,
                    **fields,
                ))
                inserted += 1
        session.commit()
        logger.info("Wing snapshot saved — %s %s: %d inserted, %d updated",
                    today, universe, inserted, updated)
    except Exception as exc:
        session.rollback()
        logger.error("save_wing_snapshot error: %s", exc)
    finally:
        session.close()


def get_wing_snapshot(universe: str, bias: str = None) -> list:
    """
    Return the most recent wing scan results for a universe.
    bias: 'put' | 'call' | None (returns all).
    """
    session = _Session()
    try:
        rows = session.execute(text("""
            SELECT * FROM wing_snapshot
            WHERE universe = :u
              AND snapshot_date = (
                  SELECT MAX(snapshot_date) FROM wing_snapshot WHERE universe = :u
              )
            ORDER BY ticker, expiry, put_strike
        """), {"u": universe}).fetchall()
        result = [dict(r._mapping) for r in rows]
        if bias and bias != "all":
            result = [r for r in result if r.get("bias") == bias]
        return result
    except Exception as e:
        logger.error("get_wing_snapshot error: %s", e)
        return []
    finally:
        session.close()


# ── Political trades functions ────────────────────────────────────────────────

def upsert_political_trades(trades: list) -> int:
    """Insert political trade records, ignoring duplicates. Returns new rows inserted.

    Duplicate check matches the DB unique constraint uq_pol_trade:
    (chamber, name, ticker, trade_date, type) — amount is intentionally excluded
    so that trades with the same key but different amount ranges don't cause
    UniqueViolation errors.
    """
    if not trades:
        return 0
    inserted = 0
    for t in trades:
        session = _Session()
        try:
            existing = session.query(PoliticalTrade).filter_by(
                chamber=t.get("chamber", ""),
                name=t.get("name", ""),
                ticker=t.get("ticker", ""),
                trade_date=t.get("trade_date", ""),
                type=t.get("type", ""),
                amount=t.get("amount", ""),
            ).first()
            if existing:
                # Backfill sector if we now have it but the row doesn't
                if t.get("sector") and not existing.sector:
                    existing.sector = t.get("sector", "")
                session.commit()
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
                sector     = t.get("sector", ""),
            )
            session.add(row)
            session.commit()
            inserted += 1
        except Exception as exc:
            session.rollback()
            logger.warning("upsert_political_trades skipping duplicate: %s", exc)
        finally:
            session.close()
    return inserted


def get_political_trades(tickers: set = None, since_date: str = None, limit: int = 2000,
                         trade_year: str = None, disc_year: str = None) -> list:
    """Query stored political trades, newest disc_date first."""
    session = _Session()
    try:
        q = session.query(PoliticalTrade).order_by(PoliticalTrade.disc_date.desc())
        if tickers:
            q = q.filter(PoliticalTrade.ticker.in_(tickers))
        if since_date:
            q = q.filter(PoliticalTrade.disc_date >= since_date)
        if trade_year:
            q = q.filter(PoliticalTrade.trade_date.like(f"{trade_year}%"))
        if disc_year:
            q = q.filter(PoliticalTrade.disc_date.like(f"{disc_year}%"))
        rows = q.limit(limit).all()
        return [_pol_row_to_dict(r) for r in rows]
    finally:
        session.close()


def get_political_trade_years() -> dict:
    """Return distinct years present in trade_date and disc_date columns."""
    from sqlalchemy import text as _text
    session = _Session()
    try:
        trade_rows = session.execute(_text(
            "SELECT DISTINCT SUBSTRING(trade_date, 1, 4) AS yr FROM political_trades "
            "WHERE trade_date IS NOT NULL AND trade_date != '' ORDER BY yr DESC"
        )).fetchall()
        disc_rows = session.execute(_text(
            "SELECT DISTINCT SUBSTRING(disc_date, 1, 4) AS yr FROM political_trades "
            "WHERE disc_date IS NOT NULL AND disc_date != '' ORDER BY yr DESC"
        )).fetchall()
        return {
            "trade_years": [r[0] for r in trade_rows if r[0]],
            "disc_years":  [r[0] for r in disc_rows  if r[0]],
        }
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


def get_all_earnings_tickers() -> list:
    """Return all distinct tickers that have earnings rows in the DB."""
    session = _Session()
    try:
        from sqlalchemy import text
        rows = session.execute(text("SELECT DISTINCT ticker FROM earnings_surprises ORDER BY ticker")).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []
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
        "chamber":           r.chamber,
        "name":              r.name,
        "party":             r.party,
        "district":          r.district,
        "ticker":            r.ticker,
        "asset":             r.asset,
        "type":              r.type,
        "amount":            r.amount,
        "trade_date":        r.trade_date,
        "disc_date":         r.disc_date,
        "lag_days":          r.lag_days,
        "link":              r.link,
        "sector":            r.sector or "",
        "price_at_trade":    r.price_at_trade,
        "price_last":        r.price_last,
        "price_last_updated": r.price_last_updated.isoformat() if r.price_last_updated else None,
        "price_30d":         r.price_30d,
        "price_60d":         r.price_60d,
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
        "ttm_eps":        r.ttm_eps,
        "ntm_eps":        r.ntm_eps,
        "eps_growth_rate": r.eps_growth_rate,
    }

# ── Ticker metadata (shared sector/company cache) ─────────────────────────────

def get_tickers_missing_sector(tickers: list) -> list:
    """Return subset of tickers that have no sector in ticker_metadata."""
    if not tickers:
        return []
    session = _Session()
    try:
        rows = session.query(TickerMetadata.ticker).filter(
            TickerMetadata.ticker.in_([t.upper() for t in tickers]),
            TickerMetadata.sector.isnot(None),
            TickerMetadata.sector != "",
        ).all()
        existing = {r[0] for r in rows}
        return [t for t in tickers if t.upper() not in existing]
    finally:
        session.close()


def upsert_ticker_metadata(profiles: dict):
    """
    Upsert ticker → {company_name, sector} into ticker_metadata.
    profiles: {ticker: {company_name: str, sector: str}}
    """
    if not profiles:
        return
    session = _Session()
    try:
        for ticker, data in profiles.items():
            ticker = ticker.upper()
            existing = session.query(TickerMetadata).filter_by(ticker=ticker).first()
            if existing:
                if data.get("sector"):
                    existing.sector = data["sector"]
                if data.get("company_name"):
                    existing.company_name = data["company_name"]
                existing.updated_at = datetime.utcnow()
            else:
                session.add(TickerMetadata(
                    ticker       = ticker,
                    company_name = data.get("company_name", ""),
                    sector       = data.get("sector", ""),
                ))
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.error("upsert_ticker_metadata error: %s", exc)
        raise
    finally:
        session.close()


def get_ticker_sector_map(tickers: list) -> dict:
    """Return {ticker: sector} for all tickers that have sector populated."""
    if not tickers:
        return {}
    session = _Session()
    try:
        rows = session.query(TickerMetadata.ticker, TickerMetadata.sector).filter(
            TickerMetadata.ticker.in_([t.upper() for t in tickers]),
        ).all()
        return {r[0]: r[1] or "" for r in rows}
    finally:
        session.close()


def get_trades_missing_at_price(limit: int = 500) -> list:
    """Return list of {id, ticker, trade_date} for trades that have no price_at_trade yet."""
    session = _Session()
    try:
        rows = session.query(
            PoliticalTrade.id,
            PoliticalTrade.ticker,
            PoliticalTrade.trade_date,
        ).filter(
            PoliticalTrade.price_at_trade == None,
            PoliticalTrade.trade_date != None,
            PoliticalTrade.trade_date != "",
        ).limit(limit).all()
        return [{"id": r[0], "ticker": r[1], "trade_date": r[2]} for r in rows]
    finally:
        session.close()


def bulk_update_trade_prices(updates: list) -> int:
    """
    Batch-update price_at_trade and/or price_last on PoliticalTrade rows.
    updates: list of {id, price_at_trade?, price_last?} — only non-None fields are written.
    Returns number of rows touched.
    """
    if not updates:
        return 0
    session = _Session()
    touched = 0
    try:
        for u in updates:
            trade_id = u.get("id")
            if not trade_id:
                continue
            row = session.query(PoliticalTrade).filter_by(id=trade_id).first()
            if not row:
                continue
            if u.get("price_at_trade") is not None:
                row.price_at_trade = u["price_at_trade"]
            if u.get("price_last") is not None:
                row.price_last = u["price_last"]
                row.price_last_updated = datetime.utcnow()
            touched += 1
        session.commit()
        return touched
    except Exception as exc:
        session.rollback()
        logger.error("bulk_update_trade_prices error: %s", exc)
        raise
    finally:
        session.close()


def get_trades_needing_horizon_prices(limit: int = 5000) -> list:
    """Return trades that have price_at_trade but are missing any horizon price."""
    session = _Session()
    try:
        from sqlalchemy import or_
        rows = (
            session.query(
                PoliticalTrade.id,
                PoliticalTrade.ticker,
                PoliticalTrade.trade_date,
                PoliticalTrade.type,
            )
            .filter(PoliticalTrade.price_at_trade.isnot(None))
            .filter(PoliticalTrade.trade_date.isnot(None))
            .filter(
                or_(
                    PoliticalTrade.price_30d.is_(None),
                    PoliticalTrade.price_60d.is_(None),
                    PoliticalTrade.price_90d.is_(None),
                )
            )
            .limit(limit)
            .all()
        )
        return [{"id": r[0], "ticker": r[1], "trade_date": r[2], "type": r[3]} for r in rows]
    finally:
        session.close()


def bulk_update_horizon_prices(updates: list) -> int:
    """
    Batch-update price_30d / price_60d / price_90d on PoliticalTrade rows.
    updates: list of {id, price_30d?, price_60d?, price_90d?}
    Returns number of rows touched.
    """
    if not updates:
        return 0
    session = _Session()
    touched = 0
    try:
        for u in updates:
            trade_id = u.get("id")
            if not trade_id:
                continue
            row = session.query(PoliticalTrade).filter_by(id=trade_id).first()
            if not row:
                continue
            if u.get("price_30d") is not None:
                row.price_30d = u["price_30d"]
            if u.get("price_60d") is not None:
                row.price_60d = u["price_60d"]
            if u.get("price_90d") is not None:
                row.price_90d = u["price_90d"]
            touched += 1
        session.commit()
        return touched
    except Exception as exc:
        session.rollback()
        logger.error("bulk_update_horizon_prices error: %s", exc)
        raise
    finally:
        session.close()


def get_political_leaderboard(year: int = None) -> list:
    """
    Compute per-politician performance stats from stored prices.
    Only includes politicians with ≥3 priced trades to keep stats meaningful.
    Returns list of dicts sorted by win_rate desc.

    Win logic (direction-adjusted):
      - BUY wins if price_last > price_at_trade  (return > 0)
      - SELL wins if price_last < price_at_trade (return < 0, i.e. price fell after sale)

    year: optional int — if set, only trades with trade_date starting with that year are counted.
    """
    from collections import defaultdict
    session = _Session()
    try:
        q = session.query(PoliticalTrade).filter(
            PoliticalTrade.price_at_trade != None,
            PoliticalTrade.price_last != None,
            PoliticalTrade.price_at_trade > 0,
        )
        if year:
            q = q.filter(PoliticalTrade.trade_date.like(f"{year}%"))
        rows = q.all()

        by_pol = defaultdict(list)
        for r in rows:
            by_pol[(r.name, r.party, r.chamber)].append(r)

        leaderboard = []
        for (name, party, chamber), trades in by_pol.items():
            if len(trades) < 3:
                continue

            wins = 0
            adj_returns = []
            buy_count = sell_count = 0
            trade_detail = []

            for t in trades:
                raw_pct = (t.price_last - t.price_at_trade) / t.price_at_trade * 100
                is_sale = "sale" in (t.type or "").lower()
                if is_sale:
                    sell_count += 1
                    adj = -raw_pct   # positive = price fell after sale = win
                else:
                    buy_count += 1
                    adj = raw_pct    # positive = price rose after buy = win

                if adj > 0:
                    wins += 1
                adj_returns.append(adj)
                trade_detail.append({
                    "ticker":     t.ticker,
                    "adj_return": adj,
                    "type":       t.type,
                    "trade_date": t.trade_date,
                    "sector":     t.sector or "",
                })

            total    = len(trades)
            win_rate = wins / total * 100
            avg_ret  = sum(adj_returns) / total

            trade_detail.sort(key=lambda x: x["adj_return"], reverse=True)
            best  = trade_detail[0]
            worst = trade_detail[-1]

            leaderboard.append({
                "name":         name,
                "party":        party,
                "chamber":      chamber,
                "total":        total,
                "wins":         wins,
                "win_rate":     round(win_rate, 1),
                "avg_return":   round(avg_ret, 1),
                "buy_count":    buy_count,
                "sell_count":   sell_count,
                "best_ticker":  best["ticker"],
                "best_return":  round(best["adj_return"], 1),
                "best_date":    best["trade_date"],
                "worst_ticker": worst["ticker"],
                "worst_return": round(worst["adj_return"], 1),
                "worst_date":   worst["trade_date"],
            })

        leaderboard.sort(key=lambda x: (x["win_rate"], x["avg_return"]), reverse=True)
        return leaderboard
    finally:
        session.close()


def get_political_trades_count() -> dict:
    """Return count of priced vs total political trades (for backfill progress)."""
    session = _Session()
    try:
        total   = session.query(PoliticalTrade).count()
        priced  = session.query(PoliticalTrade).filter(
            PoliticalTrade.price_at_trade != None
        ).count()
        return {"total": total, "priced": priced}
    finally:
        session.close()


def backfill_political_trade_sectors():
    """
    Update existing political_trades rows that have empty sector
    using whatever is already in ticker_metadata.
    Returns number of rows updated.
    """
    session = _Session()
    updated = 0
    try:
        # Get all tickers in political_trades that lack sector
        from sqlalchemy import text as _text
        empty_rows = session.query(PoliticalTrade).filter(
            (PoliticalTrade.sector == None) | (PoliticalTrade.sector == "")
        ).all()
        if not empty_rows:
            return 0
        tickers = list({r.ticker.upper() for r in empty_rows})
        sector_map = get_ticker_sector_map(tickers)
        for row in empty_rows:
            s = sector_map.get(row.ticker.upper(), "")
            if s:
                row.sector = s
                updated += 1
        session.commit()
        return updated
    except Exception as exc:
        session.rollback()
        logger.error("backfill_political_trade_sectors error: %s", exc)
        raise
    finally:
        session.close()
