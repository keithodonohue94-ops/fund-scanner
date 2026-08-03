"""
fund-scanner/scanner.py
Yahoo Finance fundamental data fetcher.
Uses direct HTTP requests with browser-like headers instead of yfinance
to avoid Yahoo's bot detection / rate-limiting of cloud IPs.
"""

import time
import logging
import requests

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

# ── Yahoo Finance session with browser-like headers ───────────────────────────
# Using a persistent session + browser UA avoids most bot-detection blocks.

YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}

QUOTE_SUMMARY_URL = (
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    "?modules=defaultKeyStatistics,financialData,summaryDetail,"
    "incomeStatementHistoryQuarterly,price"
)

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(YAHOO_HEADERS)
        # Warm up the session with a visit to the finance homepage
        # so Yahoo sets a consent cookie
        try:
            _session.get("https://finance.yahoo.com/", timeout=10)
        except Exception:
            pass
    return _session


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(val):
    try:
        f = float(val)
        if f != f or abs(f) > 1e15:   # NaN / Inf guard
            return None
        return f
    except (TypeError, ValueError):
        return None


def _get_quarterly_deltas(quarterly_stmts: list) -> tuple:
    """
    Parse quarterly income statements from Yahoo's
    incomeStatementHistoryQuarterly response to get QoQ margin deltas.
    Returns (op_delta, gm_delta) or (None, None).
    """
    try:
        stmts = quarterly_stmts
        if not stmts or len(stmts) < 2:
            return None, None

        def raw(stmt, key):
            v = stmt.get(key, {})
            if isinstance(v, dict):
                return _safe_float(v.get("raw"))
            return _safe_float(v)

        s0, s1 = stmts[0], stmts[1]   # most recent, then prior quarter

        r0 = raw(s0, "totalRevenue")
        r1 = raw(s1, "totalRevenue")
        if not r0 or not r1:
            return None, None

        gp0 = raw(s0, "grossProfit")
        gp1 = raw(s1, "grossProfit")
        gm_delta = None
        if gp0 is not None and gp1 is not None:
            gm_delta = gp0 / r0 - gp1 / r1

        oi0 = raw(s0, "operatingIncome") or raw(s0, "ebit")
        oi1 = raw(s1, "operatingIncome") or raw(s1, "ebit")
        op_delta = None
        if oi0 is not None and oi1 is not None:
            op_delta = oi0 / r0 - oi1 / r1

        return op_delta, gm_delta

    except Exception as exc:
        logger.debug("quarterly delta error: %s", exc)
        return None, None


# ── Single-ticker scan ────────────────────────────────────────────────────────

def scan_ticker(symbol: str, retries: int = 2) -> dict:
    """Fetch fundamental data for one ticker from Yahoo Finance."""
    sess = _get_session()
    url  = QUOTE_SUMMARY_URL.format(symbol=symbol)

    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = sess.get(url, timeout=15)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("%s: rate limited — waiting %ds", symbol, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(5 * (attempt + 1))
    else:
        raise RuntimeError(f"{symbol}: {last_exc}")

    result = data.get("quoteSummary", {}).get("result") or []
    if not result:
        raise RuntimeError(f"{symbol}: empty quoteSummary result")
    r = result[0]

    def g(module, key):
        m = r.get(module, {})
        v = m.get(key)
        if isinstance(v, dict):
            return _safe_float(v.get("raw"))
        return _safe_float(v)

    # Price / change
    price   = g("price", "regularMarketPrice") or g("summaryDetail", "previousClose")
    chg_raw = g("price", "regularMarketChangePercent")
    chg_pct = round(chg_raw * 100, 2) if chg_raw is not None else None

    # Quarterly deltas
    quarterly = (
        r.get("incomeStatementHistoryQuarterly", {})
         .get("incomeStatementHistory", [])
    )
    op_delta, gm_delta = _get_quarterly_deltas(quarterly)

    return {
        "ticker":       symbol,
        "price":        price,
        "chg_pct":      chg_pct,
        "mkt_cap":      g("summaryDetail", "marketCap") or g("price", "marketCap"),
        "pe":           g("summaryDetail", "trailingPE"),
        "fwd_pe":       g("defaultKeyStatistics", "forwardPE"),
        "peg":          g("defaultKeyStatistics", "pegRatio"),
        "ps":           g("summaryDetail", "priceToSalesTrailing12Months"),
        "rev_growth":   g("financialData", "revenueGrowth"),
        "gross_margin": g("financialData", "grossMargins"),
        "op_margin":    g("financialData", "operatingMargins"),
        "op_delta":     op_delta,
        "gm_delta":     gm_delta,
    }


# ── Batch scan ────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, delay: float = 1.5) -> list:
    """Scan a list of tickers with rate-limit-friendly delays."""
    results = []
    for sym in tickers:
        try:
            r = scan_ticker(sym)
            results.append(r)
            logger.info(
                "  %s ✓  P/E: %s  FwdPE: %s  RevG: %s  OpMgn: %s",
                sym,
                f"{r['pe']:.1f}"               if r["pe"]          is not None else "—",
                f"{r['fwd_pe']:.1f}"           if r["fwd_pe"]      is not None else "—",
                f"{r['rev_growth']*100:.1f}%"  if r["rev_growth"]  is not None else "—",
                f"{r['op_margin']*100:.1f}%"   if r["op_margin"]   is not None else "—",
            )
        except Exception as exc:
            logger.warning("  %s error: %s", sym, exc)
        time.sleep(delay)
    return results
