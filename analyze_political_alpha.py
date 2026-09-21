"""
analyze_political_alpha.py
──────────────────────────
Forensic alpha analysis of congressional trading disclosures.

Run on Render Shell (or locally with DATABASE_URL set) AFTER
backfill_horizon_prices.py has populated price_30d / price_60d / price_90d.

    python analyze_political_alpha.py

Outputs a ranked breakdown of every signal dimension:
  - Overall baseline
  - Buy vs Sell
  - Chamber (Senate vs House)
  - Party
  - Position size bracket
  - Disclosure lag
  - Sector
  - Top politicians
  - Cluster trades (≥2 politicians same ticker within 7 days)
  - Year-over-year consistency
"""

import os, sys, argparse
sys.path.insert(0, os.path.dirname(__file__))

from collections import defaultdict
from datetime import datetime, timedelta

# ── CLI args ──────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Congressional trading alpha analysis")
parser.add_argument("--since", default=None,
                    help="Filter to trade_date >= YYYY-MM-DD  (e.g. --since 2021-01-01)")
parser.add_argument("--until", default=None,
                    help="Filter to trade_date <= YYYY-MM-DD  (e.g. --until 2023-12-31)")
args = parser.parse_args()

# ── DB connection ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set"); sys.exit(1)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

import sqlalchemy as sa
engine = sa.create_engine(DATABASE_URL, pool_pre_ping=True)

# ── Load data ─────────────────────────────────────────────────────────────────
date_label = ""
where_clauses = ["price_at_trade IS NOT NULL", "price_at_trade > 0"]
if args.since:
    where_clauses.append(f"trade_date >= '{args.since}'")
    date_label += f" since {args.since}"
if args.until:
    where_clauses.append(f"trade_date <= '{args.until}'")
    date_label += f" until {args.until}"

where_sql = " AND ".join(where_clauses)

print(f"Loading political_trades from DB{date_label}…")
with engine.connect() as conn:
    rows = conn.execute(sa.text(f"""
        SELECT
            id, chamber, name, party, ticker, type, amount,
            trade_date, disc_date, lag_days, sector,
            price_at_trade, price_last, price_30d, price_60d, price_90d
        FROM political_trades
        WHERE {where_sql}
    """)).fetchall()

cols = ["id","chamber","name","party","ticker","type","amount",
        "trade_date","disc_date","lag_days","sector",
        "price_at_trade","price_last","price_30d","price_60d","price_90d"]
trades = [dict(zip(cols, r)) for r in rows]
print(f"Loaded {len(trades):,} trades with price_at_trade{date_label}")

# ── Helpers ───────────────────────────────────────────────────────────────────
def ret(price_at, price_later):
    if price_at and price_later and price_at > 0:
        return (price_later - price_at) / price_at * 100
    return None

def is_buy(t):
    return "purchase" in (t["type"] or "").lower()

def is_sell(t):
    tp = (t["type"] or "").lower()
    return "sale" in tp or "sell" in tp

def amount_bracket(amt_str):
    """Classify amount string into bracket."""
    if not amt_str:
        return "Unknown"
    s = amt_str.replace("$","").replace(",","").replace(" ","")
    # grab first number
    import re
    m = re.search(r"(\d+)", s)
    if not m:
        return "Unknown"
    lo = int(m.group(1))
    if lo < 15_001:      return "$1K–$15K"
    if lo < 50_001:      return "$15K–$50K"
    if lo < 100_001:     return "$50K–$100K"
    if lo < 250_001:     return "$100K–$250K"
    if lo < 500_001:     return "$250K–$500K"
    if lo < 1_000_001:   return "$500K–$1M"
    return "$1M+"

def lag_bracket(lag):
    if lag is None: return "Unknown"
    if lag <= 30:   return "0–30d"
    if lag <= 60:   return "31–60d"
    if lag <= 90:   return "61–90d"
    if lag <= 180:  return "91–180d"
    return "180d+"

def stats(returns):
    """Compute stats from a list of return floats."""
    if not returns:
        return dict(n=0, win_rate=None, avg_ret=None, med_ret=None, avg_win=None, avg_loss=None)
    n = len(returns)
    wins  = [r for r in returns if r > 0]
    losses= [r for r in returns if r <= 0]
    wr    = len(wins) / n * 100
    avg   = sum(returns) / n
    srt   = sorted(returns)
    med   = srt[n//2] if n % 2 == 1 else (srt[n//2-1]+srt[n//2])/2
    aw    = sum(wins)/len(wins)   if wins   else 0
    al    = sum(losses)/len(losses) if losses else 0
    return dict(n=n, win_rate=wr, avg_ret=avg, med_ret=med, avg_win=aw, avg_loss=al)

def print_table(title, rows, min_n=10, top_n=20):
    """Print a ranked table. rows = list of (label, stats_dict)"""
    rows = [(lbl, s) for lbl, s in rows if s["n"] >= min_n]
    rows.sort(key=lambda x: (x[1]["win_rate"] or 0), reverse=True)
    print(f"\n{'─'*70}")
    print(f"  {title}  (min n={min_n}, showing top {top_n})")
    print(f"{'─'*70}")
    print(f"  {'Label':<30} {'N':>6}  {'WinRate':>8}  {'AvgRet':>8}  {'MedRet':>8}  {'AvgWin':>8}  {'AvgLoss':>8}")
    print(f"  {'-'*30} {'------':>6}  {'-------':>8}  {'------':>8}  {'-------':>8}  {'------':>8}  {'-------':>8}")
    for lbl, s in rows[:top_n]:
        print(f"  {lbl:<30} {s['n']:>6,}  {s['win_rate']:>7.1f}%  {s['avg_ret']:>+7.1f}%  {s['med_ret']:>+7.1f}%  {s['avg_win']:>+7.1f}%  {s['avg_loss']:>+7.1f}%")

# ── Build return series (60d is primary signal horizon) ───────────────────────
# For buys:  win if price_60d > price_at_trade (positive return)
# For sells: win if price_60d < price_at_trade (price fell — sell was prescient)
# We direction-adjust: sell return = -1 * raw return

def direction_ret_60(t):
    r = ret(t["price_at_trade"], t["price_60d"])
    if r is None: return None
    if is_sell(t): return -r   # invert: sell wins if price fell
    return r

def direction_ret_90(t):
    r = ret(t["price_at_trade"], t["price_90d"])
    if r is None: return None
    if is_sell(t): return -r
    return r

# Filter to trades with 60d prices
priced_60 = [t for t in trades if t["price_60d"] is not None]
priced_90 = [t for t in trades if t["price_90d"] is not None]
print(f"Trades with 60d price: {len(priced_60):,}")
print(f"Trades with 90d price: {len(priced_90):,}")

if not priced_60:
    print("\nNo 60d prices found — run backfill_horizon_prices.py first, then re-run this script.")
    sys.exit(0)

# ── 1. OVERALL BASELINE ───────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("  OVERALL BASELINE — all priced trades, direction-adjusted @ 60d")
print(f"{'='*70}")
all_rets = [direction_ret_60(t) for t in priced_60 if direction_ret_60(t) is not None]
s = stats(all_rets)
print(f"  N={s['n']:,}  WinRate={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%  "
      f"MedRet={s['med_ret']:+.1f}%  AvgWin={s['avg_win']:+.1f}%  AvgLoss={s['avg_loss']:+.1f}%")

# ── 2. BUY vs SELL ────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("  BUY vs SELL (raw, not direction-adjusted)")
print(f"{'='*70}")
for label, subset in [("Purchase (raw)", [t for t in priced_60 if is_buy(t)]),
                       ("Sale (raw, + = price fell)", [t for t in priced_60 if is_sell(t)])]:
    rets = [ret(t["price_at_trade"], t["price_60d"]) for t in subset]
    rets = [r for r in rets if r is not None]
    if is_sell(subset[0]) if subset else False:
        rets = [-r for r in rets]  # invert sells
    s = stats(rets)
    print(f"  {label:<35} N={s['n']:,}  WinRate={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

# ── 3. CHAMBER ────────────────────────────────────────────────────────────────
def build_dimension(data, key_fn, ret_fn=direction_ret_60):
    groups = defaultdict(list)
    for t in data:
        r = ret_fn(t)
        if r is None: continue
        groups[key_fn(t)].append(r)
    return [(k, stats(v)) for k, v in groups.items()]

print_table("CHAMBER @ 60d",
    build_dimension(priced_60, lambda t: t["chamber"] or "Unknown"), min_n=10)

# ── 4. PARTY ──────────────────────────────────────────────────────────────────
print_table("PARTY @ 60d",
    build_dimension(priced_60, lambda t: (t["party"] or "Unknown").strip()), min_n=10)

# ── 5. CHAMBER × PARTY ────────────────────────────────────────────────────────
print_table("CHAMBER × PARTY @ 60d",
    build_dimension(priced_60, lambda t: f"{t['chamber'] or '?'} / {(t['party'] or '?').strip()}"), min_n=10)

# ── 6. BUY/SELL × CHAMBER ─────────────────────────────────────────────────────
print_table("TYPE × CHAMBER @ 60d (direction-adjusted)",
    build_dimension(priced_60,
        lambda t: f"{'BUY' if is_buy(t) else 'SELL'} / {t['chamber'] or '?'}"), min_n=10)

# ── 7. POSITION SIZE ──────────────────────────────────────────────────────────
print_table("POSITION SIZE BRACKET @ 60d",
    build_dimension(priced_60, lambda t: amount_bracket(t["amount"])), min_n=10)

# ── 8. POSITION SIZE × BUY/SELL ───────────────────────────────────────────────
print_table("POSITION SIZE × BUY/SELL @ 60d",
    build_dimension(priced_60,
        lambda t: f"{amount_bracket(t['amount'])} / {'BUY' if is_buy(t) else 'SELL'}"), min_n=10)

# ── 9. DISCLOSURE LAG ─────────────────────────────────────────────────────────
print_table("DISCLOSURE LAG @ 60d",
    build_dimension(priced_60, lambda t: lag_bracket(t["lag_days"])), min_n=10)

# ── 10. SECTOR ────────────────────────────────────────────────────────────────
print_table("SECTOR @ 60d",
    build_dimension(priced_60, lambda t: t["sector"] or "Unknown"), min_n=15)

# ── 11. TOP POLITICIANS ────────────────────────────────────────────────────────
print_table("POLITICIAN @ 60d (top 30 by win rate, min 20 trades)",
    build_dimension(priced_60, lambda t: t["name"] or "Unknown"),
    min_n=20, top_n=30)

# ── 12. YEAR ──────────────────────────────────────────────────────────────────
print_table("YEAR (trade_date) @ 60d",
    build_dimension(priced_60,
        lambda t: (t["trade_date"] or "")[:4] or "Unknown"), min_n=10)

# ── 13. CLUSTER TRADES ────────────────────────────────────────────────────────
# Cluster = ≥2 different politicians bought the same ticker within 7 calendar days
print(f"\n{'='*70}")
print("  CLUSTER TRADE ANALYSIS @ 60d")
print("  (≥2 different politicians buy same ticker within 7 days)")
print(f"{'='*70}")

buys = [t for t in priced_60 if is_buy(t) and t["trade_date"]]
buys.sort(key=lambda t: (t["ticker"], t["trade_date"]))

# Group by ticker
by_ticker_buys = defaultdict(list)
for t in buys:
    by_ticker_buys[t["ticker"]].append(t)

cluster_trades = []
non_cluster_trades = []

for ticker, ticker_buys in by_ticker_buys.items():
    ticker_buys.sort(key=lambda t: t["trade_date"])
    # sliding window: mark trades as cluster if within 7d of any other trade by a diff politician
    for i, t in enumerate(ticker_buys):
        t_date = datetime.strptime(t["trade_date"], "%Y-%m-%d")
        window = [o for o in ticker_buys
                  if o["name"] != t["name"]
                  and abs((datetime.strptime(o["trade_date"], "%Y-%m-%d") - t_date).days) <= 7]
        if window:
            cluster_trades.append(t)
        else:
            non_cluster_trades.append(t)

cr_rets  = [r for t in cluster_trades  for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
ncr_rets = [r for t in non_cluster_trades for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]

sc  = stats(cr_rets)
snc = stats(ncr_rets)
print(f"\n  Cluster trades   : N={sc['n']:,}  WinRate={sc['win_rate']:.1f}%  AvgRet={sc['avg_ret']:+.1f}%  MedRet={sc['med_ret']:+.1f}%")
print(f"  Non-cluster buys : N={snc['n']:,}  WinRate={snc['win_rate']:.1f}%  AvgRet={snc['avg_ret']:+.1f}%  MedRet={snc['med_ret']:+.1f}%")

# Cluster by size (how many politicians piled in)
cluster_sizes = defaultdict(list)
# for each cluster event (ticker+window), count unique politicians
cluster_events = defaultdict(set)
for t in cluster_trades:
    t_date = datetime.strptime(t["trade_date"], "%Y-%m-%d")
    # use (ticker, week-start) as event key
    week_start = (t_date - timedelta(days=t_date.weekday())).strftime("%Y-%m-%d")
    cluster_events[(t["ticker"], week_start)].add(t["name"])

# tag each trade with its cluster size
event_sizes = {k: len(v) for k, v in cluster_events.items()}

for t in cluster_trades:
    t_date = datetime.strptime(t["trade_date"], "%Y-%m-%d")
    week_start = (t_date - timedelta(days=t_date.weekday())).strftime("%Y-%m-%d")
    sz = event_sizes.get((t["ticker"], week_start), 1)
    r = ret(t["price_at_trade"], t["price_60d"])
    if r is not None:
        cluster_sizes[f"{sz} politicians"].append(r)

print(f"\n  {'Cluster size':<25} {'N':>6}  {'WinRate':>8}  {'AvgRet':>8}")
for key in sorted(cluster_sizes.keys()):
    s = stats(cluster_sizes[key])
    if s["n"] >= 5:
        print(f"  {key:<25} {s['n']:>6,}  {s['win_rate']:>7.1f}%  {s['avg_ret']:>+7.1f}%")

# Cluster + large position size
big_cluster = [t for t in cluster_trades if amount_bracket(t["amount"]) in ("$50K–$100K","$100K–$250K","$250K–$500K","$500K–$1M","$1M+")]
bc_rets = [r for t in big_cluster for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
sbc = stats(bc_rets)
print(f"\n  Cluster + ≥$50K  : N={sbc['n']:,}  WinRate={sbc['win_rate']:.1f}%  AvgRet={sbc['avg_ret']:+.1f}%")

# ── 14. 90d CHECK ─────────────────────────────────────────────────────────────
if priced_90:
    print(f"\n{'='*70}")
    print("  90d HORIZON CHECK — does signal hold longer?")
    print(f"{'='*70}")
    all_90 = [direction_ret_90(t) for t in priced_90 if direction_ret_90(t) is not None]
    s = stats(all_90)
    print(f"  Overall @ 90d: N={s['n']:,}  WinRate={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

    cluster_90 = [t for t in cluster_trades if t["price_90d"] is not None]
    c90_rets = [ret(t["price_at_trade"], t["price_90d"]) for t in cluster_90]
    c90_rets = [r for r in c90_rets if r is not None]
    sc90 = stats(c90_rets)
    print(f"  Cluster @ 90d: N={sc90['n']:,}  WinRate={sc90['win_rate']:.1f}%  AvgRet={sc90['avg_ret']:+.1f}%")

print(f"\n{'='*70}")
print("  ANALYSIS COMPLETE")
print(f"{'='*70}\n")
