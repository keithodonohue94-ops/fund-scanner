"""
Politician track records on fixed horizons, measured against the S&P 500 (SPY) over the same window.

The old leaderboard measured each trade from its trade date to today, across trades back to 2014, so a 2018 purchase
up 500% counted the same as last month's trade and most purchases "won" in a rising market. Here every trade is
scored over the same 30 and 60 days (the stored price_30d / price_60d) as excess return over SPY across the same
dates, direction-adjusted (a sale wins when the stock then lags SPY). Only recent trades count (default: last two
years), and a member needs at least MIN_TRADES scored trades.
"""
from datetime import date, datetime, timedelta

MIN_TRADES = 5


def _close_on_or_after(closes: dict, d: str, max_gap: int = 7):
    """closes: {'YYYY-MM-DD': close}. First close on or after d (within max_gap days)."""
    try:
        dt = date.fromisoformat(d[:10])
    except Exception:
        return None
    for i in range(max_gap + 1):
        v = closes.get((dt + timedelta(days=i)).isoformat())
        if v:
            return v
    return None


def trade_excess(t: dict, spy: dict, horizon: int):
    """Direction-adjusted excess return (pp) of one trade over `horizon` days, or None."""
    p0, p1 = t.get("price_at_trade"), t.get(f"price_{horizon}d")
    td = t.get("trade_date") or ""
    if not (p0 and p1 and p0 > 0 and td):
        return None
    s0 = _close_on_or_after(spy, td)
    end = (date.fromisoformat(td[:10]) + timedelta(days=horizon)).isoformat()
    s1 = _close_on_or_after(spy, end)
    if not (s0 and s1):
        return None
    ex = ((p1 / p0) - (s1 / s0)) * 100
    t_ = (t.get("type") or "").lower()
    if "sale" in t_:
        return -ex
    if "purchase" in t_:
        return ex
    return None                                            # exchanges etc. are not scored


def leaderboard(trades: list, spy: dict, since: str | None = None, year: int | None = None,
                min_trades: int = MIN_TRADES) -> list:
    """trades: cleaned API-shape dicts (member_id, name, party, chamber, ticker, type, trade_date, price_*)."""
    if year:
        trades = [t for t in trades if (t.get("trade_date") or "").startswith(str(year))]
    elif since:
        trades = [t for t in trades if (t.get("trade_date") or "") >= since]
    by: dict = {}
    for t in trades:
        e60, e30 = trade_excess(t, spy, 60), trade_excess(t, spy, 30)
        e = e60 if e60 is not None else e30
        if e is None:
            continue
        m = by.setdefault(t.get("member_id") or t.get("name"), {"name": t.get("name"), "party": t.get("party") or "",
                                                                 "chamber": t.get("chamber"), "rows": []})
        m["rows"].append((t, e, e30, e60))
    out = []
    for m in by.values():
        rows = m["rows"]
        if len(rows) < min_trades:
            continue
        n = len(rows)
        wins = sum(1 for _, e, _, _ in rows if e > 0)
        best = max(rows, key=lambda x: x[1])
        worst = min(rows, key=lambda x: x[1])
        e30s = [x[2] for x in rows if x[2] is not None]
        e60s = [x[3] for x in rows if x[3] is not None]
        out.append({
            "name": m["name"], "party": m["party"], "chamber": m["chamber"],
            "total": n, "wins": wins, "win_rate": round(wins / n * 100, 1),
            "avg_return": round(sum(x[1] for x in rows) / n, 1),          # excess vs SPY, 60d (30d where 60d not yet)
            "avg_excess_30d": round(sum(e30s) / len(e30s), 1) if e30s else None,
            "avg_excess_60d": round(sum(e60s) / len(e60s), 1) if e60s else None,
            "buy_count": sum(1 for t, *_ in rows if "purchase" in (t.get("type") or "").lower()),
            "sell_count": sum(1 for t, *_ in rows if "sale" in (t.get("type") or "").lower()),
            "best_ticker": best[0].get("ticker"), "best_return": round(best[1], 1), "best_date": best[0].get("trade_date"),
            "worst_ticker": worst[0].get("ticker"), "worst_return": round(worst[1], 1), "worst_date": worst[0].get("trade_date"),
        })
    out.sort(key=lambda x: (x["win_rate"], x["avg_return"]), reverse=True)
    return out
