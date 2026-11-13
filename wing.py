"""
Options skew ("wing") scan — the single implementation behind the Options Skew tab (live), the nightly
wing_snapshot history, the Radar and the morning brief.

This is an exact port of the original browser scan (osprey.html, _WING_WORKER_SRC → scanTicker). It is
parity-tested against that JavaScript (insider-scanner/tests/wing_parity.py). Do not change the rules here
without changing them deliberately everywhere — the saved history is only comparable if they stay fixed.

Rules (defaults = the tab's defaults):
  - expiries: this calendar year, DTE > 0 (same-day expiries excluded)
  - per expiry: puts at least min_otm % below spot and calls at least min_otm % above spot must both exist
  - one pair per whole % OTM (rounded half-up), walking puts from nearest-to-spot outward
  - call leg: the call strike closest to spot × (1 + otm%/100)
  - IV: greeks.mid_iv → smv_vol → ask_iv → implied_volatility; zero/missing IV skips the pair
  - both legs: volume ≥ min_vol and open interest ≥ min_oi
  - iv_diff = call IV − put IV (decimal); |iv_diff| ≥ min_iv_diff %
  - pcr = put VOLUME / call VOLUME (3dp)
  - put bias:  iv_diff < 0 and put OI > call OI and pcr > 1
    call bias: iv_diff > 0 and call OI > put OI and pcr < 1
Units: IVs and iv_diff are decimals (0.35 = 35%); otm_pct is a whole percent.
"""
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import math

DEFAULTS = {"min_otm": 20.0, "min_iv_diff": 5.0, "min_vol": 50, "min_oi": 100}


def _num(v):
    """JS parseFloat(x || 0) semantics: falsy -> 0; unparsable -> nan."""
    if not v:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return math.nan


def _int(v):
    """JS parseInt(x || 0)."""
    if not v:
        return 0
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _first(*vals):
    """JS a || b || c — first truthy value (0, '', None are falsy)."""
    for v in vals:
        if v:
            return v
    return vals[-1] if vals else None


def _round_half_up(x: float) -> int:
    """JS Math.round."""
    return math.floor(x + 0.5)


def _to_fixed3(x: float) -> float:
    """JS Number(x.toFixed(3)) for x >= 0 (ties go to the larger value, on the exact binary value)."""
    return float(Decimal(x).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))


def _iv(opt) -> float:
    g = opt.get("greeks") or {}
    return _num(_first(g.get("mid_iv"), g.get("smv_vol"), g.get("ask_iv"), opt.get("implied_volatility"), 0))


def _mid(opt) -> float:
    bid, ask = opt.get("bid"), opt.get("ask")
    if bid and ask:
        return (float(bid) + float(ask)) / 2
    return _num(_first(opt.get("last"), opt.get("ask"), 0))


def dte(expiry: str, today: date) -> int:
    return (date.fromisoformat(str(expiry)[:10]) - today).days


def target_expiries(expirations: list, today: date) -> list:
    yr = str(today.year)
    return [e for e in expirations if dte(e, today) > 0 and str(e).startswith(yr)]


def pairs_for_expiry(ticker: str, expiry: str, chain: list, spot: float, today: date,
                     min_otm=DEFAULTS["min_otm"], min_iv_diff=DEFAULTS["min_iv_diff"],
                     min_vol=DEFAULTS["min_vol"], min_oi=DEFAULTS["min_oi"], stats: dict | None = None) -> list:
    """stats (optional): counts why candidate pairs were rejected — diagnostics only, never changes the result."""
    st = stats if stats is not None else {}
    bump = lambda k: st.__setitem__(k, st.get(k, 0) + 1)
    chain = chain or []
    puts = [o for o in chain if o.get("option_type") == "put"]
    calls = [o for o in chain if o.get("option_type") == "call"]
    put_thr, call_thr = spot * (1 - min_otm / 100), spot * (1 + min_otm / 100)
    put_strikes = sorted({float(o["strike"]) for o in puts if float(o["strike"]) <= put_thr}, reverse=True)
    call_strikes = sorted({float(o["strike"]) for o in calls if float(o["strike"]) >= call_thr})
    if not put_strikes or not call_strikes:
        bump("expiry_no_strikes_beyond_min_otm")
        return []
    rows, seen = [], set()
    for ps in put_strikes:
        otm = _round_half_up((spot - ps) / spot * 100)
        if otm < min_otm or otm in seen:
            continue
        seen.add(otm)
        bump("candidates")
        target = spot * (1 + otm / 100)
        call = None
        for o in calls:                       # closest call to target; first wins ties (JS reduce)
            if call is None or abs(float(o["strike"]) - target) < abs(float(call["strike"]) - target):
                call = o
        if call is None:
            continue
        put = next((o for o in puts if float(o["strike"]) == ps), None)
        if put is None:
            continue
        put_iv, call_iv = _iv(put), _iv(call)
        if not put_iv or not call_iv or math.isnan(put_iv) or math.isnan(call_iv):
            bump("rej_no_iv")
            continue
        call_mid, put_mid = _mid(call), _mid(put)
        put_vol, call_vol = _int(put.get("volume")), _int(call.get("volume"))
        put_oi, call_oi = _int(put.get("open_interest")), _int(call.get("open_interest"))
        if put_vol < min_vol or call_vol < min_vol:
            bump("rej_volume")
            continue
        if put_oi < min_oi or call_oi < min_oi:
            bump("rej_open_interest")
            continue
        iv_diff = call_iv - put_iv
        if abs(iv_diff) < min_iv_diff / 100:
            bump("rej_iv_diff")
            continue
        if call_vol <= 0:
            bump("rej_volume")
            continue
        pcr = _to_fixed3(put_vol / call_vol)
        put_bias = iv_diff < 0 and put_oi > call_oi and pcr > 1
        call_bias = iv_diff > 0 and call_oi > put_oi and pcr < 1
        if not put_bias and not call_bias:
            bump("rej_bias_not_confirmed")
            continue
        bump("kept")
        rows.append({"ticker": ticker, "expiry": str(expiry), "dte": dte(expiry, today), "otm_pct": otm, "spot": spot,
                     "put_strike": ps, "call_strike": float(call["strike"]),
                     "put_iv": put_iv, "call_iv": call_iv, "iv_diff": iv_diff,
                     "put_volume": put_vol, "call_volume": call_vol, "put_oi": put_oi, "call_oi": call_oi, "pcr": pcr,
                     "call_mid": call_mid, "put_mid": put_mid, "bias": "put" if put_bias else "call"})
    return rows


def spot_from_quote(q: dict) -> float:
    """last → close → prevclose → bid → ask (the tab used last → bid → ask; close/prevclose added for after-hours)."""
    q = q or {}
    return _num(_first(q.get("last"), q.get("close"), q.get("prevclose"), q.get("bid"), q.get("ask"), 0))
