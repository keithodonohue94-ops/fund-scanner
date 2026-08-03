"""
fund-scanner/scanner.py
Yahoo Finance fundamental data fetcher using yfinance.
"""

import yfinance as yf
import time
import logging

logger = logging.getLogger(__name__)

# ── Universe definitions (mirrors the frontend) ───────────────────────────────
UNIVERSES = {
    "portfolio": [
        "ALAB","MRVL","CRDO","IREN","APLD","MU","GFS","FLNC",
        "AMD","MOD","TER","ARM","ANET","VICR","ORA","QCOM","STRL",
    ],
    "myportfolio": [
        "ALAB","MRVL","CRDO","MU","MOD","STRL","ANET","VICR",
        "ORA","FLNC","ETN","PWR","NVDA","SMCI","INTC","MPWR","VRT",
    ],
    "soxx": [
        "MU","AMD","AVGO","INTC","NVDA","MRVL","AMAT","TXN","QCOM",
        "NXPI","MPWR","LRCX","KLAC","ADI","TER","MCHP","TSM","ASML",
        "ON","ALAB","CRDO","MTSI","ENTG","STX","SWKS","WOLF","CRUS",
        "ACLS","FORM","MXL","AMBA","POWI","DIOD","AOSL",
    ],
    "smh": [
        "NVDA","TSM","AVGO","ASML","AMD","TXN","QCOM","AMAT","LRCX",
        "KLAC","MU","ADI","MRVL","INTC","NXPI","MPWR","ON","MCHP",
        "TER","STX","ENTG","SWKS","WOLF","AMBA","ACLS","CRUS",
    ],
    "ndx100": [
        "AAPL","MSFT","NVDA","AMZN","META","TSLA","GOOGL","GOOG",
        "AVGO","COST","NFLX","TMUS","AMD","PEP","CSCO","ADBE","QCOM",
        "TXN","AMGN","INTC","INTU","ISRG","BKNG","VRTX","CMCSA","MU",
        "AMAT","PANW","LRCX","REGN","KLAC","ADI","MRVL","CRWD","MDLZ",
        "CEG","CTAS","FTNT","SNPS","CDNS","MELI","AZN","ASML","CSX",
        "ORLY","MAR","ABNB","PYPL","WDAY","PCAR","MNST","ADSK","ADP",
        "FAST","ROST","KDP","DXCM","CHTR","ODFL","IDXX","CPRT","MCHP",
        "EXC","BIIB","TEAM","ILMN","CSGP","GEHC","NXPI","VRSK","ON",
        "DDOG","CTSH","GFS","TTD","ANSS","FANG","SMCI","ARM","APP",
        "AXON","MPWR","ZS","CRDO","ALAB","NET","DASH","COIN","PLTR",
    ],
    "sp500": [
        "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO",
        "JPM","LLY","V","XOM","MA","UNH","JNJ","PG","HD","MRK","ABBV",
        "CVX","COST","CRM","BAC","NFLX","AMD","WMT","KO","PEP","ADBE",
        "TMO","MCD","CSCO","ORCL","GE","GS","ABT","T","VZ","INTC",
        "IBM","QCOM","TXN","HON","INTU","AMGN","CAT","AMAT","BKNG",
        "ISRG","NOW","SPGI","BLK","PFE","AXP","LRCX","KLAC","ADI",
        "MRVL","PANW","CRWD","REGN","VRTX","MU","DE","SYK","GILD",
        "MDLZ","CMCSA","ETN","SBUX","TMUS","PLD","MMC","CB","AON",
        "ZTS","CME","ICE","TDG","WELL","DUK","SO","NEE","AEP","EXC",
    ],
}

# ── Quarterly delta helpers ───────────────────────────────────────────────────

def _safe_float(val):
    try:
        f = float(val)
        if f != f or abs(f) > 1e15:      # NaN or Inf guard
            return None
        return f
    except (TypeError, ValueError):
        return None


def _get_quarterly_deltas(t: yf.Ticker):
    """Return (op_delta, gm_delta) — QoQ change in operating / gross margin."""
    try:
        q = t.quarterly_income_stmt
        if q is None or q.empty or q.shape[1] < 2:
            return None, None

        # Normalise index labels to lower-case strings for robust matching
        idx_map = {str(i).lower(): i for i in q.index}

        def row(keywords):
            for kw in keywords:
                for k, v in idx_map.items():
                    if kw in k:
                        return v
            return None

        rev_row = row(["total revenue", "revenue"])
        gp_row  = row(["gross profit"])
        op_row  = row(["operating income", "ebit"])

        if rev_row is None:
            return None, None

        c0, c1 = q.columns[0], q.columns[1]
        r0 = _safe_float(q.at[rev_row, c0])
        r1 = _safe_float(q.at[rev_row, c1])
        if not r0 or not r1:
            return None, None

        gm_delta = None
        if gp_row:
            g0 = _safe_float(q.at[gp_row, c0])
            g1 = _safe_float(q.at[gp_row, c1])
            if g0 is not None and g1 is not None:
                gm_delta = g0 / r0 - g1 / r1

        op_delta = None
        if op_row:
            o0 = _safe_float(q.at[op_row, c0])
            o1 = _safe_float(q.at[op_row, c1])
            if o0 is not None and o1 is not None:
                op_delta = o0 / r0 - o1 / r1

        return op_delta, gm_delta

    except Exception as exc:
        logger.debug("quarterly delta error for ticker: %s", exc)
        return None, None


# ── Single-ticker scan ────────────────────────────────────────────────────────

def scan_ticker(symbol: str) -> dict:
    t = yf.Ticker(symbol)
    info = t.info or {}

    def safe(key):
        v = info.get(key)
        if v is None or v == "N/A":
            return None
        f = _safe_float(v)
        return f

    price    = safe("currentPrice") or safe("regularMarketPrice") or safe("previousClose")
    raw_chg  = safe("regularMarketChangePercent")
    chg_pct  = round(raw_chg * 100, 2) if raw_chg is not None else None

    op_delta, gm_delta = _get_quarterly_deltas(t)

    return {
        "ticker":       symbol,
        "price":        price,
        "chg_pct":      chg_pct,
        "mkt_cap":      safe("marketCap"),
        "pe":           safe("trailingPE"),
        "fwd_pe":       safe("forwardPE"),
        "peg":          safe("pegRatio"),
        "ps":           safe("priceToSalesTrailing12Months"),
        "rev_growth":   safe("revenueGrowth"),
        "gross_margin": safe("grossMargins"),
        "op_margin":    safe("operatingMargins"),
        "op_delta":     op_delta,
        "gm_delta":     gm_delta,
    }


# ── Batch scan ────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, delay: float = 0.6) -> list:
    """Scan a list of tickers, returning a list of result dicts."""
    results = []
    for sym in tickers:
        try:
            r = scan_ticker(sym)
            results.append(r)
            logger.info(
                "  %s ✓  P/E: %s  FwdPE: %s  RevG: %s  OpMgn: %s",
                sym,
                f"{r['pe']:.1f}"          if r["pe"]          is not None else "—",
                f"{r['fwd_pe']:.1f}"      if r["fwd_pe"]      is not None else "—",
                f"{r['rev_growth']*100:.1f}%" if r["rev_growth"] is not None else "—",
                f"{r['op_margin']*100:.1f}%"  if r["op_margin"]  is not None else "—",
            )
        except Exception as exc:
            logger.warning("  %s error: %s", sym, exc)
        time.sleep(delay)
    return results
