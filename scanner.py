"""
fund-scanner/scanner.py
FMP-only fundamental data fetcher (Premium plan).

Data flow:
  - Batch quote    (/stable/quote)               → price, chg%, mkt cap, P/E, fwd P/E, EPS
  - Batch ratios   (/stable/ratios-ttm)          → PEG, P/S, gross/op margin, rev growth
  - Per-ticker quarterly income (/stable/income-statement) → QoQ margin deltas
"""

import os
import time
import logging
import concurrent.futures
import requests

logger = logging.getLogger(__name__)

# ── Universe definitions ──────────────────────────────────────────────────────
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

# ── FMP config ────────────────────────────────────────────────────────────────
FMP_API_KEY  = os.environ.get("FMP_API_KEY", "")
FMP_V3       = "https://financialmodelingprep.com/api/v3"
FMP_STABLE   = "https://financialmodelingprep.com/stable"
FMP_HEADERS  = {"Accept": "application/json"}

# Max tickers per batch call (FMP accepts comma-separated lists)
BATCH_SIZE   = 50
# Parallel workers for per-ticker quarterly calls
MAX_WORKERS  = 10


def _safe_float(val):
    try:
        f = float(val)
        if f != f or abs(f) > 1e15:
            return None
        return f
    except (TypeError, ValueError):
        return None


def _fmp_get(url: str, params: dict) -> list | dict | None:
    """GET from FMP API, return parsed JSON or None on error."""
    params["apikey"] = FMP_API_KEY
    try:
        r = requests.get(url, params=params, headers=FMP_HEADERS, timeout=15)
        if r.status_code == 429:
            logger.warning("FMP rate limit — waiting 10s")
            time.sleep(10)
            r = requests.get(url, params=params, headers=FMP_HEADERS, timeout=15)
        if r.status_code != 200:
            logger.warning("FMP %s HTTP %d: %s", url, r.status_code, r.text[:200])
            return None
        return r.json()
    except Exception as exc:
        logger.warning("FMP %s error: %s", url, exc)
        return None


def _batch_quotes(tickers: list) -> dict:
    """
    Fetch real-time quotes for all tickers via FMP stable API.
    Returns dict keyed by symbol.
    """
    out = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        chunk   = tickers[i:i + BATCH_SIZE]
        symbols = ",".join(chunk)
        url     = f"{FMP_STABLE}/quote"
        data    = _fmp_get(url, {"symbol": symbols})
        logger.info("Quote response type=%s len=%s sample=%s",
                    type(data).__name__,
                    len(data) if isinstance(data, (list, dict)) else "n/a",
                    str(data)[:200] if data else "None")
        if not isinstance(data, list):
            # Some stable endpoints wrap in {"data": [...]}
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            else:
                continue
        for q in data:
            sym = (q.get("symbol") or "").upper()
            if not sym:
                continue
            chg_raw = _safe_float(q.get("changesPercentage"))
            out[sym] = {
                "price":   _safe_float(q.get("price")),
                "chg_pct": round(chg_raw, 2) if chg_raw is not None else None,
                "mkt_cap": _safe_float(q.get("marketCap")),
                "pe":      _safe_float(q.get("pe")),
                "eps":     _safe_float(q.get("eps")),
            }
    return out


def _fetch_ratios(symbol: str) -> dict:
    """
    Fetch TTM ratios for one ticker via stable API.
    Returns dict with peg, ps, fwd_pe, rev_growth, gross_margin, op_margin.
    """
    url  = f"{FMP_STABLE}/ratios-ttm"
    data = _fmp_get(url, {"symbol": symbol.upper()})
    if isinstance(data, list) and data:
        r = data[0]
    elif isinstance(data, dict):
        r = data
    else:
        return {}
    return {
        "peg":          _safe_float(r.get("priceEarningsToGrowthRatioTTM")),
        "ps":           _safe_float(r.get("priceToSalesRatioTTM")),
        "fwd_pe":       _safe_float(r.get("priceToEarningsRatioTTM")),
        "rev_growth":   _safe_float(r.get("revenueGrowthTTM")),
        "gross_margin": _safe_float(r.get("grossProfitMarginTTM")),
        "op_margin":    _safe_float(r.get("operatingProfitMarginTTM")),
    }


def _batch_ratios(tickers: list) -> dict:
    """Parallel TTM ratios fetch for all tickers."""
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(_fetch_ratios, sym): sym for sym in tickers}
        for future in concurrent.futures.as_completed(future_map):
            sym = future_map[future]
            try:
                out[sym] = future.result()
            except Exception as exc:
                logger.warning("Ratios error %s: %s", sym, exc)
                out[sym] = {}
    return out


def _quarterly_deltas(symbol: str) -> tuple:
    """
    Fetch last 2 quarterly income statements from FMP.
    Returns (op_delta, gm_delta) as fractions (e.g. 0.02 = +2pp).
    """
    url  = f"{FMP_STABLE}/income-statement"
    data = _fmp_get(url, {"symbol": symbol.upper(), "period": "quarter", "limit": 3})
    if not isinstance(data, list) or len(data) < 2:
        return None, None
    try:
        s0, s1 = data[0], data[1]
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

        return op_delta, gm_delta
    except Exception as exc:
        logger.warning("QoQ delta error %s: %s", symbol, exc)
        return None, None


# ── Main scan ─────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, delay: float = 0) -> list:
    """
    Scan a list of tickers using FMP exclusively.
    1. Batch quote        → price, chg%, mkt cap, P/E
    2. Batch ratios-ttm   → P/S, PEG, margins, rev growth
    3. Parallel quarterly → QoQ margin deltas (per ticker, concurrent)
    """
    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY not set")

    logger.info("Scanning %d tickers via FMP...", len(tickers))

    # Step 1 & 2: batch calls (fast — 1-2 round trips each)
    quotes  = _batch_quotes(tickers)
    ratios  = _batch_ratios(tickers)

    logger.info("Batch quotes: %d, ratios: %d", len(quotes), len(ratios))

    # Step 3: parallel quarterly deltas
    deltas = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(_quarterly_deltas, sym): sym for sym in tickers}
        for future in concurrent.futures.as_completed(future_map):
            sym = future_map[future]
            try:
                deltas[sym] = future.result()
            except Exception as exc:
                logger.warning("Delta error %s: %s", sym, exc)
                deltas[sym] = (None, None)

    logger.info("Quarterly deltas fetched for %d tickers", len(deltas))

    # Merge
    results = []
    for sym in tickers:
        q  = quotes.get(sym, {})
        ra = ratios.get(sym, {})
        op_delta, gm_delta = deltas.get(sym, (None, None))

        if not q:
            logger.warning("  %s — no quote data", sym)
            continue

        row = {
            "ticker":       sym,
            "price":        q.get("price"),
            "chg_pct":      q.get("chg_pct"),
            "mkt_cap":      q.get("mkt_cap"),
            "pe":           q.get("pe"),
            "fwd_pe":       ra.get("fwd_pe"),
            "peg":          ra.get("peg"),
            "ps":           ra.get("ps"),
            "rev_growth":   ra.get("rev_growth"),
            "gross_margin": ra.get("gross_margin"),
            "op_margin":    ra.get("op_margin"),
            "op_delta":     op_delta,
            "gm_delta":     gm_delta,
        }
        results.append(row)
        logger.info(
            "  %s ✓  $%.2f  P/E: %s  RevG: %s  OpMgn: %s",
            sym,
            q.get("price") or 0,
            f"{row['pe']:.1f}"              if row["pe"]         is not None else "—",
            f"{row['rev_growth']*100:.1f}%" if row["rev_growth"] is not None else "—",
            f"{row['op_margin']*100:.1f}%"  if row["op_margin"]  is not None else "—",
        )

    return results
