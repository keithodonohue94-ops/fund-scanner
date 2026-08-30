"""
fund-scanner/scanner.py
FMP-only fundamental data fetcher.

Data flow:
  - Per-ticker quote      (/stable/quote)                → live price, chg%, mkt cap
  - Per-ticker income     (/stable/income-statement)     → TTM EPS, TTM revenue, margins
  - Per-ticker estimates  (/stable/analyst-estimates)    → forward EPS
All multiples (P/E TTM, FWD P/E, PEG, P/S) are calculated dynamically from live price
and raw fundamentals — NOT pulled from FMP's pre-calculated ratio endpoints.
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
    "aiinfra": [
        "ETN","PWR","LITE","MKSI","MOD","STRL","INOD","ANET","CLS",
        "MRVL","SITM","MPWR","CRDO","CRS","GLW","POWL","TTMI","INTC",
        "VRT","NVDA","SMCI","ALAB","MU","AMD","QCOM","ARM","TSM",
        "AVGO","LRCX","AMAT","KLAC","ADI",
    ],
    "cybersec": [
        "S","QLYS","OKTA","CRWD","PANW","ZS","NET","FTNT","CYBR",
        "SAIL","SHC","TENB","RPD",
    ],
    "rareearths": [
        "MP","IDR","USAR","TMC","NB","UUUU","DNN",
    ],
    "energy": [
        "ENPH","FSLR","PLUG","RUN","SEDG","BE","NOVA","ARRY","SHLS",
        "AES","NEE","CEG","VST","NRG",
    ],
    "orbital": [
        "BKSY","PLTR","PL","RDW","RKLB","ASTS","LUNR","MNTS",
    ],
    "soxx": [
        "NVDA","MU","AMD","AVGO","INTC","AMAT","KLAC","MRVL","TSM","LRCX",
        "ADI","TXN","MPWR","TER","NXPI","QCOM","ASML","ALAB","CRDO","MCHP",
        "ON","ENTG","ASX","MTSI","UMC","SWKS","ACLS","CRUS","STX","FORM",
    ],
    "smh": [
        "NVDA","TSM","AVGO","AMD","ASML","TXN","MU","ADI","AMAT","QCOM",
        "KLAC","LRCX","INTC","MRVL","CDNS","SNPS","MPWR","TER","NXPI","STM",
        "ARM","MCHP","ALAB","ON","SWKS",
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
FMP_STABLE   = "https://financialmodelingprep.com/stable"
FMP_HEADERS  = {"Accept": "application/json"}
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


def _fetch_quote(symbol: str) -> dict:
    """Live price, change%, market cap, and trailing EPS from FMP quote endpoint."""
    data = _fmp_get(f"{FMP_STABLE}/quote", {"symbol": symbol.upper()})
    if isinstance(data, list) and data:
        q = data[0]
    elif isinstance(data, dict):
        q = data
    else:
        return {}
    chg_raw = _safe_float(q.get("changePercentage"))
    return {
        "price":   _safe_float(q.get("price")),
        "chg_pct": round(chg_raw, 2) if chg_raw is not None else None,
        "mkt_cap": _safe_float(q.get("marketCap") or q.get("market_cap")),
        "eps":     _safe_float(q.get("eps")),   # trailing EPS (TTM) — FMP-computed
    }


def _fetch_ratios(symbol: str) -> dict:
    """
    Fetch FMP pre-calculated ratios to extract reliable E denominators.
    We pull forwardPE, pegRatio, and priceToSalesRatio — then reverse-engineer
    the underlying E (fwd_eps, eps_growth_pct, ttm_rev) so we can reapply
    a fresh live price on top.
    """
    data = _fmp_get(
        f"{FMP_STABLE}/ratios",
        {"symbol": symbol.upper(), "period": "annual", "limit": 1}
    )
    if isinstance(data, list) and data:
        r = data[0]
    elif isinstance(data, dict):
        r = data
    else:
        return {}
    logger.info("[ratios raw] %s → %s", symbol, {k: v for k, v in r.items() if v is not None})
    return {
        # TTM PE direct from ratios (avoids relying on quote.eps)
        "pe_fmp": _safe_float(
            r.get("priceEarningsRatio") or r.get("peRatio") or r.get("priceToEarningsRatio")
        ),
        # Forward PE
        "fwd_pe_fmp": _safe_float(
            r.get("forwardPE") or r.get("priceEarningsRatioForward")
            or r.get("forwardPriceEarningsRatio") or r.get("priceEarningsForward")
        ),
        # PEG
        "peg_fmp": _safe_float(
            r.get("priceEarningsToGrowthRatio") or r.get("pegRatio")
            or r.get("pegRatioTTM") or r.get("priceToEarningsGrowthRatio")
        ),
        # P/S
        "ps_fmp": _safe_float(
            r.get("priceToSalesRatioTTM") or r.get("priceToSalesRatio")
            or r.get("priceSalesRatio") or r.get("priceToSalesTTM")
        ),
        # D/E
        "debt_to_equity": _safe_float(
            r.get("debtToEquityRatio") or r.get("debtToEquity")
            or r.get("debtEquityRatio") or r.get("totalDebtToEquity")
        ),
    }


def _fetch_fwd_eps(symbol: str) -> dict:
    """
    Compute NTM (Next Twelve Months) EPS and return forward quarter data for PEG calculation.

    Method:
      1. Get last reported quarter date from income statement
      2. Get quarterly analyst estimates (limit=20 to cover NTM + 4 years beyond)
      3. Filter to unreported forward quarters (date > last reported)
      4. NTM EPS = sum of nearest 4 forward quarters
      5. beyond_ntm = remaining forward quarters after NTM window (used for PEG CAGR)

    Returns dict with:
      ntm_eps    — float or None
      beyond_ntm — list of quarterly estimate rows sorted ascending by date
    """
    sym = symbol.upper()

    # Step 1: last reported quarter = most recent date in the income statement
    income_data = _fmp_get(
        f"{FMP_STABLE}/income-statement",
        {"symbol": sym, "period": "quarter", "limit": 2}
    )
    last_reported = ""
    if isinstance(income_data, list) and income_data:
        last_reported = income_data[0].get("date", "") or ""
    logger.info("[ntm-eps] %s last reported quarter: %s", sym, last_reported or "none")

    # Step 2: get quarterly analyst estimates — limit=20 covers NTM + up to 4 years beyond
    estimates = _fmp_get(
        f"{FMP_STABLE}/analyst-estimates",
        {"symbol": sym, "period": "quarter", "page": 0, "limit": 20}
    )
    if not isinstance(estimates, list) or not estimates:
        logger.warning("[ntm-eps] %s — no quarterly estimates returned", sym)
        return {"ntm_eps": None, "beyond_ntm": []}

    logger.info("[ntm-eps] %s — %d estimate rows, dates: %s",
                sym, len(estimates), [r.get("date") for r in estimates])

    # Step 3: filter to unreported forward quarters (date > last reported), sort ascending
    forward = sorted(
        [r for r in estimates if (r.get("date") or "") > last_reported],
        key=lambda r: r.get("date", "")
    )

    if not forward:
        logger.warning("[ntm-eps] %s — no forward quarters after %s", sym, last_reported)
        return {"ntm_eps": None, "beyond_ntm": []}

    # Step 4: sum nearest 4 forward quarters = NTM EPS
    next4 = forward[:4]
    eps_values = [_safe_float(r.get("epsAvg")) for r in next4]
    logger.info("[ntm-eps] %s next 4 quarters: %s → epsAvg: %s",
                sym, [r.get("date") for r in next4], eps_values)

    if not any(v is not None for v in eps_values):
        logger.warning("[ntm-eps] %s — epsAvg missing in all forward quarters", sym)
        return {"ntm_eps": None, "beyond_ntm": []}

    ntm_eps = sum(v for v in eps_values if v is not None)
    logger.info("[ntm-eps] %s NTM EPS = %.4f", sym, ntm_eps)

    # Step 5: quarters beyond NTM window — used for PEG CAGR
    beyond_ntm = forward[4:]
    logger.info("[ntm-eps] %s beyond-NTM quarters: %d → %s",
                sym, len(beyond_ntm), [r.get("date") for r in beyond_ntm])

    return {
        "ntm_eps":    ntm_eps if ntm_eps > 0 else None,
        "beyond_ntm": beyond_ntm,
    }


def _compute_peg(fwd_pe: float, ntm_eps: float, beyond_ntm: list, symbol: str = "") -> float | None:
    """
    Compute forward PEG using quarter-based CAGR beyond the NTM window.

    Method:
      - Count complete years of quarterly estimates beyond NTM: n = len(beyond_ntm) // 4
      - Require n >= 2 (at least 2 full years of visibility)
      - Terminal EPS = sum of the nth year's 4 quarters
      - CAGR = (terminal_eps / ntm_eps) ** (1/n) - 1
      - PEG  = fwd_pe / (cagr * 100)
    """
    if not fwd_pe or not ntm_eps or ntm_eps <= 0 or not beyond_ntm:
        return None

    n = len(beyond_ntm) // 4
    if n < 2:
        logger.info("[peg] %s — only %d complete years beyond NTM, need ≥2", symbol, n)
        return None

    usable = beyond_ntm[:n * 4]
    terminal_quarters = usable[-4:]
    terminal_values = [_safe_float(r.get("epsAvg")) for r in terminal_quarters]

    if not any(v is not None for v in terminal_values):
        logger.warning("[peg] %s — epsAvg missing in terminal quarters", symbol)
        return None

    terminal_eps = sum(v for v in terminal_values if v is not None)
    if terminal_eps <= 0:
        logger.info("[peg] %s — terminal EPS non-positive (%.4f)", symbol, terminal_eps)
        return None

    cagr = (terminal_eps / ntm_eps) ** (1 / n) - 1
    cagr_pct = cagr * 100

    logger.info("[peg] %s ntm_eps=%.4f terminal_eps=%.4f n=%d cagr=%.1f%% fwd_pe=%.1f peg=%.2f",
                symbol, ntm_eps, terminal_eps, n, cagr_pct, fwd_pe,
                fwd_pe / cagr_pct if cagr_pct > 0 else 0)

    if cagr_pct <= 0:
        return None

    return round(fwd_pe / cagr_pct, 2)


def _fetch_price_target(symbol: str) -> dict:
    """Fetch analyst consensus price target from FMP."""
    data = _fmp_get(f"{FMP_STABLE}/price-target-consensus", {"symbol": symbol.upper()})
    if isinstance(data, list) and data:
        d = data[0]
    elif isinstance(data, dict):
        d = data
    else:
        return {}
    avg_pt = _safe_float(
        d.get("targetConsensus") or d.get("averageTargetPrice") or d.get("priceTarget")
    )
    return {"avg_pt": avg_pt}


def _fetch_earnings_surprises(symbol: str, limit: int = 8) -> list:
    """Fetch last N quarters of EPS/revenue actual vs estimate from FMP stable earnings endpoint."""
    data = _fmp_get(
        f"{FMP_STABLE}/earnings",
        {"symbol": symbol.upper(), "limit": limit}
    )
    if not isinstance(data, list):
        return []
    def _pick(row, *keys):
        """Return _safe_float of first key that is present and not None (handles 0 correctly)."""
        for k in keys:
            raw = row.get(k)
            if raw is not None:
                return _safe_float(raw)
        return None

    from datetime import date as _date
    today_str = str(_date.today())

    result = []
    for row in data[:limit]:
        logger.info("[earnings raw] %s %s", symbol, {k: v for k, v in row.items() if k in (
            "date", "eps", "epsEstimated", "actualEarningResult", "estimatedEarning",
            "revenue", "revenueEstimated", "actualRevenue", "revenueActual",
        )})
        date_str = row.get("date", "")
        is_upcoming = bool(date_str) and date_str > today_str

        actual = _pick(row, "eps", "actualEps", "actualEarningResult")
        est    = _pick(row, "epsEstimated", "estimatedEps", "estimatedEarning")
        eps_surp = None
        if actual is not None and est is not None and est != 0:
            eps_surp = round((actual - est) / abs(est) * 100, 1)
        rev_actual = _pick(row, "revenue", "revenueActual", "actualRevenue")
        rev_est    = _pick(row, "revenueEstimated", "estimatedRevenue")
        rev_surp   = None
        if rev_actual is not None and rev_est is not None and rev_est != 0:
            rev_surp = round((rev_actual - rev_est) / abs(rev_est) * 100, 1)
        fiscal_end = row.get("fiscalDateEnding") or row.get("fiscal_date_ending") or date_str
        result.append({
            "date":        date_str,    # announcement date — used for upcoming display
            "fiscal_end":  fiscal_end,  # fiscal period end — used for Q-label grouping
            "is_upcoming": is_upcoming,
            "eps_actual":  actual,
            "eps_est":     est,
            "eps_surp":    eps_surp,
            "rev_actual":  rev_actual,
            "rev_est":     rev_est,
            "rev_surp":    rev_surp,
        })
    return result


def _fetch_income(symbol: str) -> dict:
    """
    Fetch last 8 quarterly income statements and compute:
      - ttm_eps:       sum of last 4 quarters' EPS
      - ttm_rev:       sum of last 4 quarters' revenue
      - prior_ttm_eps: sum of quarters 5-8 (prior year TTM)
      - eps_growth:    YoY EPS growth as a fraction (e.g. 0.25 = +25%)
      - rev_growth:    YoY revenue growth (most recent Q vs same Q -1yr)
      - gross_margin:  most recent quarter gross profit / revenue
      - op_margin:     most recent quarter operating income / revenue
      - op_delta:      QoQ change in operating margin (pp)
      - gm_delta:      QoQ change in gross margin (pp)
    """
    data = _fmp_get(
        f"{FMP_STABLE}/income-statement",
        {"symbol": symbol.upper(), "period": "quarter", "limit": 8}
    )
    if not isinstance(data, list) or len(data) < 4:
        return {}

    out = {}

    # TTM EPS and revenue (last 4 quarters)
    ttm_eps = sum(filter(None, (_safe_float(q.get("eps")) for q in data[:4])))
    ttm_rev = sum(filter(None, (_safe_float(q.get("revenue")) for q in data[:4])))
    out["ttm_eps"] = ttm_eps if ttm_eps != 0 else None
    out["ttm_rev"] = ttm_rev if ttm_rev != 0 else None

    # Prior year TTM EPS (quarters 4-7, zero-indexed)
    if len(data) >= 8:
        prior_eps = sum(filter(None, (_safe_float(q.get("eps")) for q in data[4:8])))
        out["prior_ttm_eps"] = prior_eps if prior_eps != 0 else None
    else:
        out["prior_ttm_eps"] = None

    # EPS growth YoY
    if out.get("ttm_eps") and out.get("prior_ttm_eps") and out["prior_ttm_eps"] != 0:
        out["eps_growth"] = (out["ttm_eps"] - out["prior_ttm_eps"]) / abs(out["prior_ttm_eps"])
    else:
        out["eps_growth"] = None

    # YoY revenue growth: most recent quarter vs same quarter prior year (index 4)
    if len(data) >= 5:
        r0    = _safe_float(data[0].get("revenue"))
        r_yoy = _safe_float(data[4].get("revenue"))
        out["rev_growth"] = (r0 - r_yoy) / abs(r_yoy) if r0 and r_yoy else None
    else:
        out["rev_growth"] = None

    # Current quarter margins
    s0 = data[0]
    r0 = _safe_float(s0.get("revenue"))
    if r0:
        gp = _safe_float(s0.get("grossProfit"))
        oi = _safe_float(s0.get("operatingIncome"))
        out["gross_margin"] = gp / r0 if gp is not None else None
        out["op_margin"]    = oi / r0 if oi is not None else None
    else:
        out["gross_margin"] = out["op_margin"] = None

    # QoQ margin deltas
    if len(data) >= 2:
        s1 = data[1]
        r1 = _safe_float(s1.get("revenue"))
        if r0 and r1:
            gp1 = _safe_float(s1.get("grossProfit"))
            oi1 = _safe_float(s1.get("operatingIncome"))
            gp0 = _safe_float(s0.get("grossProfit"))
            oi0 = _safe_float(s0.get("operatingIncome"))
            out["gm_delta"] = (gp0/r0 - gp1/r1) if gp0 is not None and gp1 is not None else None
            out["op_delta"] = (oi0/r0 - oi1/r1) if oi0 is not None and oi1 is not None else None
        else:
            out["gm_delta"] = out["op_delta"] = None
    else:
        out["gm_delta"] = out["op_delta"] = None

    return out



def _fetch_all(symbol: str) -> dict:
    """
    Fetch quote + FMP ratios + income margins for one ticker, return merged dict.

    Strategy:
    - trailing_eps comes directly from the FMP quote (FMP's own TTM calculation)
    - fwd_eps, ttm_rev, eps_growth_pct are reverse-engineered from FMP's pre-computed
      ratios (forwardPE, priceToSalesRatio, pegRatio) so we get reliable E denominators
    - All multiples are then recalculated using the live price fetched in the same call,
      so P is always fresh and E reflects FMP's authoritative fundamentals data
    - _fetch_income() is retained only for margin/growth columns (not for E)
    """
    quote    = _fetch_quote(symbol)        # price, chg_pct, mkt_cap, trailing_eps
    ratios   = _fetch_ratios(symbol)       # pe, ps, de from ratios endpoint
    ntm_data = _fetch_fwd_eps(symbol)      # ntm_eps + beyond_ntm quarters
    income   = _fetch_income(symbol)       # margins only: gross, op, rev_growth, deltas
    pt       = _fetch_price_target(symbol) # analyst consensus price target

    price        = quote.get("price")
    mkt_cap      = quote.get("mkt_cap")
    quote_eps    = quote.get("eps")        # FMP quote trailing EPS (sometimes wrong)
    income_eps   = income.get("ttm_eps")   # Our own 4-quarter sum — more reliable

    fwd_eps    = ntm_data.get("ntm_eps")
    beyond_ntm = ntm_data.get("beyond_ntm", [])

    pe_fmp         = ratios.get("pe_fmp")
    ps_fmp         = ratios.get("ps_fmp")
    debt_to_equity = ratios.get("debt_to_equity")
    avg_pt         = pt.get("avg_pt")

    # ── TTM PE: prefer our own 4-quarter EPS sum, fall back to quote eps, then ratios ──
    trailing_eps = income_eps or quote_eps
    pe_ttm_raw = (price / trailing_eps) if price and trailing_eps and trailing_eps > 0 else None
    if pe_ttm_raw is None and pe_fmp and pe_fmp > 0:
        pe_ttm_raw = pe_fmp
    logger.info("[pe-debug] %s income_eps=%s quote_eps=%s pe_ttm_raw=%s",
                symbol, income_eps, quote_eps, pe_ttm_raw)

    # ── Forward PE: price / NTM EPS (direct, no fallback) ────────────────────
    logger.info("[fwd-pe-debug] %s fwd_eps=%s", symbol, fwd_eps)

    # ── TTM revenue from P/S ratio ────────────────────────────────────────────
    ttm_rev = (mkt_cap / ps_fmp) if mkt_cap and ps_fmp and ps_fmp > 0 else None

    # ── Final multiples ───────────────────────────────────────────────────────
    pe_ttm = round(pe_ttm_raw, 1) if pe_ttm_raw is not None else None
    fwd_pe = round(price / fwd_eps, 1) if price and fwd_eps and fwd_eps > 0 else None

    # PEG: forward PE / n-year CAGR from NTM EPS base to terminal (quarter-based)
    peg = _compute_peg(fwd_pe, fwd_eps, beyond_ntm, symbol=symbol)

    ps = round(mkt_cap / ttm_rev, 2) if mkt_cap and ttm_rev and ttm_rev > 0 else None

    # Price target
    pt_pct = round((avg_pt / price - 1) * 100, 1) if price and avg_pt and avg_pt > 0 else None

    return {
        "price":          price,
        "chg_pct":        quote.get("chg_pct"),
        "mkt_cap":        mkt_cap,
        "avg_pt":         round(avg_pt, 2) if avg_pt is not None else None,
        "pt_pct":         pt_pct,   # positive = stock above target, negative = upside to target
        "pe":             pe_ttm,
        "fwd_pe":         fwd_pe,
        "peg":            peg,
        "ps":             ps,
        "debt_to_equity": round(debt_to_equity, 2) if debt_to_equity is not None else None,
        "rev_growth":     income.get("rev_growth"),
        "gross_margin":   income.get("gross_margin"),
        "op_margin":      income.get("op_margin"),
        "op_delta":       income.get("op_delta"),
        "gm_delta":       income.get("gm_delta"),
    }


# ── Political trades ──────────────────────────────────────────────────────────

def _fetch_political_trades(tickers: set = None, limit: int = 500) -> list:
    """
    Fetch recent Senate + House trading disclosures from FMP.
    tickers: optional set of uppercase ticker symbols to filter to.
             If None, returns all disclosures up to limit.
    Returns list of normalised dicts sorted by disc_date desc.
    """
    from datetime import datetime as _dt

    def _lag(trade_date: str, disc_date: str):
        try:
            td = _dt.strptime(trade_date[:10], "%Y-%m-%d")
            dd = _dt.strptime(disc_date[:10], "%Y-%m-%d")
            return (dd - td).days
        except Exception:
            return None

    results = []

    # ── Senate ────────────────────────────────────────────────────────────────
    senate_raw = _fmp_get(f"{FMP_STABLE}/senate-trades", {"limit": limit}) or []
    if isinstance(senate_raw, dict):
        senate_raw = senate_raw.get("data", []) or []
    for row in senate_raw:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker or ticker in ("--", "N/A", ""):
            continue
        if tickers and ticker not in tickers:
            continue
        trade_dt  = (row.get("transactionDate") or "")[:10]
        disc_dt   = (row.get("dateRecieved") or row.get("disclosureDate") or "")[:10]
        results.append({
            "chamber":    "Senate",
            "name":       row.get("senator") or row.get("name") or "—",
            "party":      row.get("party") or "",
            "district":   row.get("district") or "",
            "ticker":     ticker,
            "asset":      row.get("assetDescription") or "",
            "type":       row.get("type") or "",
            "amount":     row.get("amount") or "",
            "trade_date": trade_dt,
            "disc_date":  disc_dt,
            "lag_days":   _lag(trade_dt, disc_dt),
            "link":       row.get("link") or "",
        })

    # ── House ─────────────────────────────────────────────────────────────────
    house_raw = _fmp_get(f"{FMP_STABLE}/house-trades", {"limit": limit}) or []
    if isinstance(house_raw, dict):
        house_raw = house_raw.get("data", []) or []
    for row in house_raw:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker or ticker in ("--", "N/A", ""):
            continue
        if tickers and ticker not in tickers:
            continue
        trade_dt = (row.get("transactionDate") or "")[:10]
        disc_dt  = (row.get("disclosureDate") or row.get("dateRecieved") or "")[:10]
        results.append({
            "chamber":    "House",
            "name":       row.get("representative") or row.get("name") or "—",
            "party":      row.get("party") or "",
            "district":   row.get("district") or "",
            "ticker":     ticker,
            "asset":      row.get("assetDescription") or "",
            "type":       row.get("type") or "",
            "amount":     row.get("amount") or "",
            "trade_date": trade_dt,
            "disc_date":  disc_dt,
            "lag_days":   _lag(trade_dt, disc_dt),
            "link":       row.get("link") or "",
        })

    results.sort(key=lambda x: x.get("disc_date") or "", reverse=True)
    return results


# ── Main scan ─────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, delay: float = 0) -> list:
    """
    Scan a list of tickers using FMP.
    All multiples are calculated from live price and raw fundamental data:
      P/E TTM  = price / TTM EPS (sum of last 4 quarters)
      FWD P/E  = price / analyst consensus fwd EPS
      PEG      = FWD P/E / n-year EPS CAGR (quarter-based, NTM EPS as base)
      P/S      = market cap / TTM revenue
    """
    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY not set")

    logger.info("Scanning %d tickers via FMP...", len(tickers))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(_fetch_all, sym): sym for sym in tickers}
        for future in concurrent.futures.as_completed(future_map):
            sym = future_map[future]
            try:
                row = future.result()
                if not row.get("price"):
                    logger.warning("  %s — no quote data", sym)
                    continue
                row["ticker"] = sym
                results.append(row)
                logger.info(
                    "  %s ✓  $%.2f  P/E TTM: %s  FWD P/E: %s  PEG: %s  P/S: %s",
                    sym,
                    row["price"] or 0,
                    f"{row['pe']:.1f}"     if row["pe"]     is not None else "—",
                    f"{row['fwd_pe']:.1f}" if row["fwd_pe"] is not None else "—",
                    f"{row['peg']:.2f}"    if row["peg"]    is not None else "—",
                    f"{row['ps']:.1f}"     if row["ps"]     is not None else "—",
                )
            except Exception as exc:
                logger.warning("Scan error %s: %s", sym, exc)

    # Return in original ticker order
    order = {sym: i for i, sym in enumerate(tickers)}
    results.sort(key=lambda r: order.get(r["ticker"], 9999))
    return results
