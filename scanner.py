"""
fund-scanner/scanner.py
Yahoo Finance fundamental data fetcher.
Uses a proper cookie+crumb auth flow to avoid 401/429 errors from cloud IPs.
QoQ margin deltas sourced from Financial Modeling Prep (FMP) quarterly income statements.
"""

import os
import re
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

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
}

QUOTE_URL = (
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    "?modules=defaultKeyStatistics,financialData,summaryDetail,price"
    "&crumb={crumb}"
)

# Module-level session + crumb (shared across all ticker calls in a scan)
_session: requests.Session | None = None
_crumb:   str | None = None


_CRUMB_RE = re.compile(r'"crumb"\s*:\s*"([^"]{5,20})"')


def _init_session() -> bool:
    """
    Establish a Yahoo Finance session with valid cookie + crumb.
    Returns True on success.
    """
    global _session, _crumb
    try:
        sess = requests.Session()
        sess.headers.update(BROWSER_HEADERS)

        # Step 1: visit finance.yahoo.com to get consent cookies
        r1 = sess.get("https://finance.yahoo.com/", timeout=15)
        logger.info("Yahoo homepage: %d", r1.status_code)

        # Step 2a: try the getcrumb endpoint first
        crumb = None
        for base in ("https://query1.finance.yahoo.com",
                     "https://query2.finance.yahoo.com"):
            try:
                r2 = sess.get(f"{base}/v1/test/getcrumb", timeout=10)
                candidate = r2.text.strip().strip('"')
                if r2.status_code == 200 and 5 <= len(candidate) <= 20:
                    crumb = candidate
                    break
            except Exception:
                pass

        # Step 2b: fall back to extracting crumb from the page HTML
        if not crumb:
            logger.info("getcrumb blocked — extracting crumb from page HTML")
            for page_url in (
                "https://finance.yahoo.com/quote/AAPL/",
                "https://finance.yahoo.com/",
            ):
                try:
                    rp = sess.get(page_url, timeout=15)
                    m  = _CRUMB_RE.search(rp.text)
                    if m:
                        crumb = m.group(1).encode().decode("unicode_escape")
                        logger.info("Extracted crumb from HTML: %r", crumb)
                        break
                except Exception:
                    pass

        if not crumb:
            logger.warning("Could not obtain Yahoo Finance crumb")
            return False

        sess.headers.update(API_HEADERS)
        _session = sess
        _crumb   = crumb
        return True

    except Exception as exc:
        logger.error("Session init error: %s", exc)
        return False


def _ensure_session() -> bool:
    """Make sure we have a valid session/crumb, reinitialising if needed."""
    global _session, _crumb
    if _session and _crumb:
        return True
    return _init_session()


# ── FMP config ────────────────────────────────────────────────────────────────

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_INCOME_URL = (
    "https://financialmodelingprep.com/api/v3/income-statement/{symbol}"
    "?period=quarter&limit=3&apikey={apikey}"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(val):
    try:
        f = float(val)
        if f != f or abs(f) > 1e15:
            return None
        return f
    except (TypeError, ValueError):
        return None


def _get_quarterly_deltas_fmp(symbol: str) -> tuple:
    """
    Fetch last 2 quarterly income statements from FMP and compute
    QoQ operating margin delta and gross margin delta.
    Returns (op_delta, gm_delta) as fractions (e.g. 0.02 = +2pp).
    Returns (None, None) on any error.
    """
    if not FMP_API_KEY:
        return None, None
    try:
        url  = FMP_INCOME_URL.format(symbol=symbol, apikey=FMP_API_KEY)
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            logger.warning("FMP %s: HTTP %d", symbol, resp.status_code)
            return None, None
        stmts = resp.json()
        if not stmts or len(stmts) < 2:
            return None, None

        s0, s1 = stmts[0], stmts[1]  # most recent, then prior quarter

        r0 = _safe_float(s0.get("revenue"))
        r1 = _safe_float(s1.get("revenue"))
        if not r0 or not r1:
            return None, None

        gp0 = _safe_float(s0.get("grossProfit"))
        gp1 = _safe_float(s1.get("grossProfit"))
        gm_delta = (gp0 / r0 - gp1 / r1) if gp0 is not None and gp1 is not None else None

        oi0 = _safe_float(s0.get("operatingIncome"))
        oi1 = _safe_float(s1.get("operatingIncome"))
        op_delta = (oi0 / r0 - oi1 / r1) if oi0 is not None and oi1 is not None else None

        logger.debug(
            "FMP QoQ %s: r0=%s r1=%s gp0=%s gp1=%s oi0=%s oi1=%s → gm_Δ=%s op_Δ=%s",
            symbol, r0, r1, gp0, gp1, oi0, oi1, gm_delta, op_delta,
        )
        return op_delta, gm_delta

    except Exception as exc:
        logger.warning("FMP quarterly error %s: %s", symbol, exc)
        return None, None


def _get_quarterly_deltas(quarterly_stmts: list) -> tuple:
    try:
        if not quarterly_stmts or len(quarterly_stmts) < 2:
            return None, None

        def raw(stmt, key):
            v = stmt.get(key, {})
            if isinstance(v, dict):
                return _safe_float(v.get("raw"))
            return _safe_float(v)

        def first_nonzero(stmt, *keys):
            """Return first non-None, non-zero value across keys."""
            for k in keys:
                v = raw(stmt, k)
                if v is not None and v != 0.0:
                    return v
            return None

        s0, s1 = quarterly_stmts[0], quarterly_stmts[1]

        r0 = raw(s0, "totalRevenue")
        r1 = raw(s1, "totalRevenue")
        if not r0 or not r1:
            return None, None

        # grossProfit: direct field or compute from revenue - COGS (only if COGS is non-zero)
        def get_gross_profit(stmt, rev):
            gp = raw(stmt, "grossProfit")
            if gp:  # non-None, non-zero
                return gp
            cogs = raw(stmt, "costOfRevenue")
            if cogs:  # only use COGS fallback if COGS is also non-zero
                return rev - cogs
            return None  # Yahoo returned zeros — can't compute

        gp0 = get_gross_profit(s0, r0)
        gp1 = get_gross_profit(s1, r1)
        gm_delta = (gp0 / r0 - gp1 / r1) if gp0 is not None and gp1 is not None else None

        # operatingIncome: prefer operatingIncome, fall back to ebit
        # first_nonzero skips zero placeholders Yahoo sometimes returns
        oi0 = first_nonzero(s0, "operatingIncome", "ebit")
        oi1 = first_nonzero(s1, "operatingIncome", "ebit")
        op_delta = (oi0 / r0 - oi1 / r1) if oi0 is not None and oi1 is not None else None

        logger.debug("QoQ r0=%s r1=%s gp0=%s gp1=%s oi0=%s oi1=%s", r0, r1, gp0, gp1, oi0, oi1)
        return op_delta, gm_delta
    except Exception as exc:
        logger.debug("quarterly delta error: %s", exc)
        return None, None


# ── Single-ticker scan ────────────────────────────────────────────────────────

def scan_ticker(symbol: str, retries: int = 2) -> dict:
    global _session, _crumb

    if not _ensure_session():
        raise RuntimeError("Could not establish Yahoo Finance session")

    last_exc = None
    for attempt in range(retries + 1):
        try:
            url  = QUOTE_URL.format(symbol=symbol, crumb=_crumb)
            resp = _session.get(url, timeout=15)

            if resp.status_code == 401:
                # Crumb expired — reinit and retry
                logger.warning("%s: 401, reiniting session (attempt %d)", symbol, attempt)
                _session = None
                _crumb   = None
                if not _init_session():
                    raise RuntimeError("Session reinit failed")
                continue

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("%s: 429 rate limited, waiting %ds", symbol, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            break

        except (RuntimeError, requests.RequestException) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(5 * (attempt + 1))
    else:
        raise RuntimeError(f"{symbol}: {last_exc}")

    result = (data.get("quoteSummary") or {}).get("result") or []
    if not result:
        raise RuntimeError(f"{symbol}: empty result")
    r = result[0]

    def g(module, key):
        m = r.get(module) or {}
        v = m.get(key)
        if isinstance(v, dict):
            return _safe_float(v.get("raw"))
        return _safe_float(v)

    price   = g("price", "regularMarketPrice") or g("summaryDetail", "previousClose")
    chg_raw = g("price", "regularMarketChangePercent")
    chg_pct = round(chg_raw * 100, 2) if chg_raw is not None else None

    # Use FMP for quarterly P&L (Yahoo returns zeros for grossProfit/operatingIncome)
    op_delta, gm_delta = _get_quarterly_deltas_fmp(symbol)

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
    results = []
    for sym in tickers:
        try:
            r = scan_ticker(sym)
            results.append(r)
            logger.info(
                "  %s ✓  P/E: %s  FwdPE: %s  RevG: %s  OpMgn: %s",
                sym,
                f"{r['pe']:.1f}"              if r["pe"]         is not None else "—",
                f"{r['fwd_pe']:.1f}"          if r["fwd_pe"]     is not None else "—",
                f"{r['rev_growth']*100:.1f}%" if r["rev_growth"] is not None else "—",
                f"{r['op_margin']*100:.1f}%"  if r["op_margin"]  is not None else "—",
            )
        except Exception as exc:
            logger.warning("  %s error: %s", sym, exc)
        time.sleep(delay)
    return results
