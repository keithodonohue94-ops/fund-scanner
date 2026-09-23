"""
analyze_2025_30d.py
────────────────────
Alpha analysis across two eras with 20-day cluster window.
Outputs four columns per signal:

  ERA →          2025-today          |        2026-today
  HORIZON →   60d     price_last     |     60d     price_last

Cluster = ≥2 different politicians, same ticker, same direction, within 20 days.

Run on Render Shell:
    python analyze_2025_30d.py
"""

import os, sys
from collections import defaultdict
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set"); sys.exit(1)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

import sqlalchemy as sa
engine = sa.create_engine(DATABASE_URL, pool_pre_ping=True)

CLUSTER_DAYS = 30
ERA_A = "2025-01-01"   # 2025-today
ERA_B = "2026-01-01"   # 2026-today

print(f"Loading trades from {ERA_A} onward…")
with engine.connect() as conn:
    rows = conn.execute(sa.text(f"""
        SELECT
            id, chamber, name, party, ticker, type, amount,
            trade_date, sector,
            price_at_trade, price_last, price_30d, price_60d, price_90d
        FROM political_trades
        WHERE price_at_trade IS NOT NULL
          AND price_at_trade > 0
          AND trade_date >= '{ERA_A}'
        ORDER BY trade_date
    """)).fetchall()

cols = ["id","chamber","name","party","ticker","type","amount",
        "trade_date","sector",
        "price_at_trade","price_last","price_30d","price_60d","price_90d"]
all_trades = [dict(zip(cols, r)) for r in rows]
print(f"Loaded {len(all_trades):,} trades from {ERA_A} onward")

trades_2025 = all_trades                                           # 2025-01-01+
trades_2026 = [t for t in all_trades if t["trade_date"] >= ERA_B] # 2026-01-01+
print(f"  2025-today : {len(trades_2025):,} trades")
print(f"  2026-today : {len(trades_2026):,} trades")

# ── Helpers ────────────────────────────────────────────────────────────────────

def ret(price_at, price_later):
    if price_at and price_later and price_at > 0:
        return (price_later - price_at) / price_at * 100
    return None

def stats(rets):
    if not rets:
        return {"n": 0, "win_rate": 0.0, "avg_ret": 0.0}
    wins = sum(1 for r in rets if r > 0)
    return {
        "n":        len(rets),
        "win_rate": wins / len(rets) * 100,
        "avg_ret":  sum(rets) / len(rets),
    }

def is_buy(t):
    tp = (t.get("type") or "").lower()
    return "purchase" in tp or "buy" in tp

def is_sell(t):
    tp = (t.get("type") or "").lower()
    return "sale" in tp or "sell" in tp

def amount_bracket(raw):
    MAP = {
        "$1,001 - $15,000":      "$1K–$15K",
        "$15,001 - $50,000":     "$15K–$50K",
        "$50,001 - $100,000":    "$50K–$100K",
        "$100,001 - $250,000":   "$100K–$250K",
        "$250,001 - $500,000":   "$250K–$500K",
        "$500,001 - $1,000,000": "$500K–$1M",
        "Over $1,000,000":       "$1M+",
    }
    return MAP.get(raw, raw or "Unknown")

SIZES    = ["$1K–$15K","$15K–$50K","$50K–$100K","$100K–$250K","$250K–$500K","$500K–$1M","$1M+"]
BIG_SIZES = ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")

# ── Cluster detection ──────────────────────────────────────────────────────────
# Same direction means buys cluster with buys, sells cluster with sells.
# Call once for buys, once for sells.

def detect_clusters(trade_list, window_days=CLUSTER_DAYS):
    """Tag trades as cluster if ≥1 different politician traded same ticker
    in the same list (direction already filtered) within window_days."""
    by_ticker = defaultdict(list)
    for t in trade_list:
        by_ticker[t["ticker"]].append(t)

    cluster, non_cluster = [], []
    for ticker, tlist in by_ticker.items():
        tlist.sort(key=lambda t: t["trade_date"])
        for t in tlist:
            t_date = datetime.strptime(t["trade_date"], "%Y-%m-%d")
            neighbors = [
                o for o in tlist
                if o["name"] != t["name"]
                and abs((datetime.strptime(o["trade_date"], "%Y-%m-%d") - t_date).days) <= window_days
            ]
            (cluster if neighbors else non_cluster).append(t)
    return cluster, non_cluster

# ── 7-day successive (same politician, same ticker) ───────────────────────────

def detect_successive(trade_list, window_days=7):
    by_pol_ticker = defaultdict(list)
    for t in trade_list:
        by_pol_ticker[(t["name"], t["ticker"])].append(t)
    result = []
    for key, tlist in by_pol_ticker.items():
        tlist.sort(key=lambda t: t["trade_date"])
        for i, t in enumerate(tlist):
            if i == 0:
                continue
            prev = datetime.strptime(tlist[i-1]["trade_date"], "%Y-%m-%d")
            curr = datetime.strptime(t["trade_date"], "%Y-%m-%d")
            if (curr - prev).days <= window_days:
                result.append(t)
    return result

# ── Pre-build all lists for both eras ─────────────────────────────────────────

def build_lists(trades):
    buys  = [t for t in trades if is_buy(t)  and t["trade_date"]]
    sells = [t for t in trades if is_sell(t) and t["trade_date"]]
    cb, _ = detect_clusters(buys)
    cs, _ = detect_clusters(sells)
    sb    = detect_successive(buys)
    ss    = detect_successive(sells)
    return {
        "buys":  buys,  "sells":  sells,
        "cb":    cb,    "cs":     cs,
        "sb":    sb,    "ss":     ss,
    }

L25 = build_lists(trades_2025)
L26 = build_lists(trades_2026)

# ── Return helpers ─────────────────────────────────────────────────────────────

def buy_rets(tlist, price_key):
    return [r for t in tlist
            for r in [ret(t["price_at_trade"], t.get(price_key))]
            if r is not None]

def sell_rets_adj(tlist, price_key):
    return [-r for t in tlist
            for r in [ret(t["price_at_trade"], t.get(price_key))]
            if r is not None]

MIN_N = 10

def fmt(rets):
    if len(rets) < MIN_N:
        return "    —    "
    s = stats(rets)
    return f"{s['win_rate']:5.1f}%/{s['avg_ret']:+5.1f}%"

# ── Print layout ───────────────────────────────────────────────────────────────

SIG_W  = 36
COL_W  = 14

HEADER1 = f"{'':>{SIG_W}}  {'── 2025-today ──':^{COL_W*2+3}}  {'── 2026-today ──':^{COL_W*2+3}}"
HEADER2 = f"{'Signal':<{SIG_W}}  {'60d':^{COL_W}}  {'price_last':^{COL_W}}  {'60d':^{COL_W}}  {'price_last':^{COL_W}}"
DIVIDER = "─" * (SIG_W + 2 + (COL_W+2)*4)

def section(title):
    print()
    print(title)
    print(DIVIDER)

def row(label, direction, tlist25, tlist26, min_n=MIN_N):
    fn = buy_rets if direction == "buy" else sell_rets_adj
    r25_60   = fn(tlist25, "price_60d")
    r25_last = fn(tlist25, "price_last")
    r26_60   = fn(tlist26, "price_60d")
    r26_last = fn(tlist26, "price_last")
    print(f"  {label:<{SIG_W-2}}  {fmt(r25_60):^{COL_W}}  {fmt(r25_last):^{COL_W}}  {fmt(r26_60):^{COL_W}}  {fmt(r26_last):^{COL_W}}")

# ── OUTPUT ─────────────────────────────────────────────────────────────────────

print()
print("=" * (SIG_W + 2 + (COL_W+2)*4))
print(f"  CONGRESSIONAL TRADING ALPHA  |  {CLUSTER_DAYS}-day cluster window  |  min {MIN_N} trades")
print("=" * (SIG_W + 2 + (COL_W+2)*4))
print()
print(HEADER1)
print(HEADER2)
print(DIVIDER)

section("CHAMBER × DIRECTION")
row("Senate Buys",        "buy",  [t for t in L25["buys"]  if (t["chamber"] or "")=="Senate"], [t for t in L26["buys"]  if (t["chamber"] or "")=="Senate"])
row("House Buys",         "buy",  [t for t in L25["buys"]  if (t["chamber"] or "")=="House"],  [t for t in L26["buys"]  if (t["chamber"] or "")=="House"])
row("Senate Sells (adj)", "sell", [t for t in L25["sells"] if (t["chamber"] or "")=="Senate"], [t for t in L26["sells"] if (t["chamber"] or "")=="Senate"])
row("House Sells (adj)",  "sell", [t for t in L25["sells"] if (t["chamber"] or "")=="House"],  [t for t in L26["sells"] if (t["chamber"] or "")=="House"])

section(f"CLUSTER BUYS  (≥2 politicians, same ticker, {CLUSTER_DAYS} days)")
row("Cluster Buys — all",        "buy", L25["cb"], L26["cb"])
row("Senate Cluster Buys",       "buy", [t for t in L25["cb"] if (t["chamber"] or "")=="Senate"], [t for t in L26["cb"] if (t["chamber"] or "")=="Senate"])
row("House Cluster Buys",        "buy", [t for t in L25["cb"] if (t["chamber"] or "")=="House"],  [t for t in L26["cb"] if (t["chamber"] or "")=="House"])
row("Cluster Buys ≥$100K",       "buy", [t for t in L25["cb"] if amount_bracket(t["amount"]) in BIG_SIZES], [t for t in L26["cb"] if amount_bracket(t["amount"]) in BIG_SIZES])

section(f"CLUSTER SELLS  (≥2 politicians, same ticker, {CLUSTER_DAYS} days)")
row("Cluster Sells — all (adj)",  "sell", L25["cs"], L26["cs"])
row("Senate Cluster Sells (adj)", "sell", [t for t in L25["cs"] if (t["chamber"] or "")=="Senate"], [t for t in L26["cs"] if (t["chamber"] or "")=="Senate"])
row("House Cluster Sells (adj)",  "sell", [t for t in L25["cs"] if (t["chamber"] or "")=="House"],  [t for t in L26["cs"] if (t["chamber"] or "")=="House"])
row("Cluster Sells ≥$100K (adj)", "sell", [t for t in L25["cs"] if amount_bracket(t["amount"]) in BIG_SIZES], [t for t in L26["cs"] if amount_bracket(t["amount"]) in BIG_SIZES])

section("BUYS BY POSITION SIZE")
for sz in SIZES:
    row(f"Buy {sz}", "buy",
        [t for t in L25["buys"] if amount_bracket(t["amount"])==sz],
        [t for t in L26["buys"] if amount_bracket(t["amount"])==sz])

section("SELLS BY POSITION SIZE")
for sz in SIZES:
    row(f"Sell {sz} (adj)", "sell",
        [t for t in L25["sells"] if amount_bracket(t["amount"])==sz],
        [t for t in L26["sells"] if amount_bracket(t["amount"])==sz])

section(f"CLUSTER BUYS BY POSITION SIZE  ({CLUSTER_DAYS}-day window)")
for sz in SIZES:
    row(f"Cluster Buy {sz}", "buy",
        [t for t in L25["cb"] if amount_bracket(t["amount"])==sz],
        [t for t in L26["cb"] if amount_bracket(t["amount"])==sz])

section("7-DAY SUCCESSIVE TRADES  (same politician, same ticker)")
row("All 7d Succ Buys",          "buy",  L25["sb"], L26["sb"])
row("Senate 7d Succ Buys",       "buy",  [t for t in L25["sb"] if (t["chamber"] or "")=="Senate"], [t for t in L26["sb"] if (t["chamber"] or "")=="Senate"])
row("House 7d Succ Buys",        "buy",  [t for t in L25["sb"] if (t["chamber"] or "")=="House"],  [t for t in L26["sb"] if (t["chamber"] or "")=="House"])
row("All 7d Succ Sells (adj)",   "sell", L25["ss"], L26["ss"])
row("Senate 7d Succ Sells (adj)","sell", [t for t in L25["ss"] if (t["chamber"] or "")=="Senate"], [t for t in L26["ss"] if (t["chamber"] or "")=="Senate"])
row("House 7d Succ Sells (adj)", "sell", [t for t in L25["ss"] if (t["chamber"] or "")=="House"],  [t for t in L26["ss"] if (t["chamber"] or "")=="House"])

section("7-DAY SUCCESSIVE BUYS BY POSITION SIZE")
for sz in SIZES:
    row(f"7d Succ Buy {sz}", "buy",
        [t for t in L25["sb"] if amount_bracket(t["amount"])==sz],
        [t for t in L26["sb"] if amount_bracket(t["amount"])==sz])

section("7-DAY SUCCESSIVE SELLS BY POSITION SIZE")
for sz in SIZES:
    row(f"7d Succ Sell {sz} (adj)", "sell",
        [t for t in L25["ss"] if amount_bracket(t["amount"])==sz],
        [t for t in L26["ss"] if amount_bracket(t["amount"])==sz])

print()
print("=" * (SIG_W + 2 + (COL_W+2)*4))
print(f"  (adj) = direction-adjusted sell return  |  cluster = {CLUSTER_DAYS}-day same-direction window")
print(f"  price_last = most recent price in DB  |  — = fewer than {MIN_N} qualifying trades")
print("=" * (SIG_W + 2 + (COL_W+2)*4))
