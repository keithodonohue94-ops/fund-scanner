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


# ═══════════════════════════════════════════════════════════════════════════════
#  EXTENDED ANALYSES
# ═══════════════════════════════════════════════════════════════════════════════

def trade_year(t):
    td = t.get("trade_date") or ""
    return td[:4] if len(td) >= 4 else "Unknown"

def lag_stats(trade_list):
    """Avg/median lag and % filed late for a list of trades."""
    lags = [t["lag_days"] for t in trade_list if t["lag_days"] is not None]
    if not lags:
        return None
    n = len(lags)
    ls = sorted(lags)
    avg = sum(lags) / n
    med = ls[n // 2] if n % 2 == 1 else (ls[n // 2 - 1] + ls[n // 2]) / 2
    pct_late      = sum(1 for l in lags if l > 45) / n * 100
    pct_very_late = sum(1 for l in lags if l > 90) / n * 100
    return dict(n=n, avg=avg, med=med, pct_late=pct_late, pct_very_late=pct_very_late)

SIZE_ORDER = ["$1K–$15K", "$15K–$50K", "$50K–$100K",
              "$100K–$250K", "$250K–$500K", "$500K–$1M", "$1M+"]
LAG_ORDER  = ["0–30d", "31–60d", "61–90d", "91–180d", "180d+"]

years_sorted = sorted(
    {trade_year(t) for t in trades if trade_year(t) != "Unknown"}
)

# ── 15. DISCLOSURE LAG EVOLUTION BY YEAR ──────────────────────────────────────
# NOTE: best run without --since/--until to see the full 2014-2026 picture.
print(f"\n{'='*70}")
print("  DISCLOSURE LAG EVOLUTION BY YEAR  (all trades with lag_days)")
print(f"{'='*70}")
print(f"  {'Year':<6}  {'N':>6}  {'AvgLag':>8}  {'MedLag':>8}  {'>45d%':>7}  {'>90d%':>7}")
print(f"  {'----':<6}  {'------':>6}  {'------':>8}  {'------':>8}  {'-----':>7}  {'-----':>7}")
for yr in years_sorted:
    yr_trades = [t for t in trades if trade_year(t) == yr]
    ls = lag_stats(yr_trades)
    if ls and ls["n"] >= 10:
        print(f"  {yr:<6}  {ls['n']:>6,}  {ls['avg']:>7.1f}d  {ls['med']:>7.1f}d  "
              f"{ls['pct_late']:>6.1f}%  {ls['pct_very_late']:>6.1f}%")

print(f"\n  Alpha by (year × lag bucket) @ 60d, direction-adjusted, min n=10")
print(f"  {'Year':<6}  {'LagBucket':<11}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}")
print(f"  {'----':<6}  {'-'*11}  {'-----':>5}  {'----':>7}  {'------':>8}")
for yr in years_sorted:
    yr_priced = [t for t in priced_60 if trade_year(t) == yr]
    lag_groups = defaultdict(list)
    for t in yr_priced:
        r = direction_ret_60(t)
        if r is not None:
            lag_groups[lag_bracket(t["lag_days"])].append(r)
    for lb in LAG_ORDER:
        if lb in lag_groups and len(lag_groups[lb]) >= 10:
            s = stats(lag_groups[lb])
            print(f"  {yr:<6}  {lb:<11}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%")

# ── 16. DISCLOSURE LAG BY CHAMBER × YEAR ──────────────────────────────────────
print(f"\n{'='*70}")
print("  DISCLOSURE LAG BY CHAMBER × YEAR")
print(f"{'='*70}")
print(f"  {'Year':<6}  {'Chamber':<7}  {'N':>5}  {'AvgLag':>7}  {'MedLag':>7}  "
      f"{'>45d%':>6}  {'Alpha@60d':>10}")
print(f"  {'----':<6}  {'-------':<7}  {'-----':>5}  {'------':>7}  {'------':>7}  "
      f"{'-----':>6}  {'---------':>10}")
for yr in years_sorted:
    for chamber in ["Senate", "House"]:
        subset = [t for t in trades
                  if trade_year(t) == yr and (t["chamber"] or "") == chamber]
        ls = lag_stats(subset)
        if not ls or ls["n"] < 10:
            continue
        priced_sub = [t for t in priced_60
                      if trade_year(t) == yr and (t["chamber"] or "") == chamber]
        alpha_rets = [r for t in priced_sub
                      for r in [direction_ret_60(t)] if r is not None]
        alpha_str = f"{sum(alpha_rets)/len(alpha_rets):>+6.1f}%" if alpha_rets else "     —"
        print(f"  {yr:<6}  {chamber:<7}  {ls['n']:>5,}  {ls['avg']:>6.1f}d  {ls['med']:>6.1f}d  "
              f"{ls['pct_late']:>5.1f}%  {alpha_str:>10}")

# ── 17. DISCLOSURE LAG BY POSITION SIZE ───────────────────────────────────────
print(f"\n{'='*70}")
print("  DISCLOSURE LAG BY POSITION SIZE")
print(f"{'='*70}")

print(f"\n  Filing speed by position size bracket (all trades with lag_days):")
print(f"  {'SizeBracket':<16}  {'N':>6}  {'AvgLag':>7}  {'MedLag':>7}  {'>45d%':>6}  {'>90d%':>6}")
print(f"  {'-'*16}  {'------':>6}  {'------':>7}  {'------':>7}  {'-----':>6}  {'-----':>6}")
for sz in SIZE_ORDER:
    subset = [t for t in trades if amount_bracket(t["amount"]) == sz]
    ls = lag_stats(subset)
    if ls and ls["n"] >= 10:
        print(f"  {sz:<16}  {ls['n']:>6,}  {ls['avg']:>6.1f}d  {ls['med']:>6.1f}d  "
              f"{ls['pct_late']:>5.1f}%  {ls['pct_very_late']:>5.1f}%")

print(f"\n  Alpha by (position size × lag bucket) @ 60d, direction-adjusted, min n=10:")
print(f"  {'SizeBracket':<16}  {'LagBucket':<11}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}")
print(f"  {'-'*16}  {'-'*11}  {'-----':>5}  {'----':>7}  {'------':>8}")
for sz in SIZE_ORDER:
    for lb in LAG_ORDER:
        subset = [t for t in priced_60
                  if amount_bracket(t["amount"]) == sz
                  and lag_bracket(t["lag_days"]) == lb]
        rets = [r for t in subset for r in [direction_ret_60(t)] if r is not None]
        if len(rets) >= 10:
            s = stats(rets)
            print(f"  {sz:<16}  {lb:<11}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%")

# ── 18. CLUSTER TRADES × POSITION SIZE ────────────────────────────────────────
print(f"\n{'='*70}")
print("  CLUSTER TRADES × POSITION SIZE  @ 60d & 90d")
print("  (≥2 politicians same ticker within 7 days)")
print(f"{'='*70}")

print(f"\n  All cluster buys by position size @ 60d (min n=5):")
print(f"  {'SizeBracket':<16}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}  {'MedRet':>8}")
print(f"  {'-'*16}  {'-----':>5}  {'----':>7}  {'------':>8}  {'------':>8}")
for sz in SIZE_ORDER:
    subset = [t for t in cluster_trades if amount_bracket(t["amount"]) == sz]
    rets = [r for t in subset
            for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    if len(rets) >= 5:
        s = stats(rets)
        print(f"  {sz:<16}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%  {s['med_ret']:>+7.1f}%")

BIG_SIZE_BUCKETS = ("$100K–$250K", "$250K–$500K", "$500K–$1M", "$1M+")
big_cluster_100  = [t for t in cluster_trades
                    if amount_bracket(t["amount"]) in BIG_SIZE_BUCKETS]
bc100_rets = [r for t in big_cluster_100
              for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
if bc100_rets:
    s = stats(bc100_rets)
    print(f"\n  Cluster buys ≥$100K combined @ 60d:")
    print(f"  N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%  MedRet={s['med_ret']:+.1f}%")

cluster_90_all = [t for t in cluster_trades if t["price_90d"] is not None]

print(f"\n  All cluster buys by position size @ 90d (min n=5):")
print(f"  {'SizeBracket':<16}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}  {'MedRet':>8}")
print(f"  {'-'*16}  {'-----':>5}  {'----':>7}  {'------':>8}  {'------':>8}")
for sz in SIZE_ORDER:
    subset = [t for t in cluster_90_all if amount_bracket(t["amount"]) == sz]
    rets = [r for t in subset
            for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
    if len(rets) >= 5:
        s = stats(rets)
        print(f"  {sz:<16}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%  {s['med_ret']:>+7.1f}%")

big_cluster_100_90 = [t for t in cluster_90_all
                      if amount_bracket(t["amount"]) in BIG_SIZE_BUCKETS]
bc100_90_rets = [r for t in big_cluster_100_90
                 for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
if bc100_90_rets:
    s = stats(bc100_90_rets)
    print(f"\n  Cluster buys ≥$100K combined @ 90d:")
    print(f"  N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%  MedRet={s['med_ret']:+.1f}%")

# ── 19. REPEAT BUYER ALPHA ────────────────────────────────────────────────────
# Same politician buys same ticker again within 30 days of a prior purchase.
print(f"\n{'='*70}")
print("  REPEAT BUYER ALPHA  @ 60d & 90d")
print("  (politician buys ticker X, then buys X again within 30 days)")
print(f"{'='*70}")

all_buys_rb = [t for t in trades if is_buy(t) and t["trade_date"]]
all_buys_rb.sort(key=lambda t: (t["name"] or "", t["ticker"] or "", t["trade_date"]))

by_name_ticker = defaultdict(list)
for t in all_buys_rb:
    by_name_ticker[(t["name"], t["ticker"])].append(t)

repeat_initial  = []   # first buy in a within-30d pair
repeat_followon = []   # the follow-on buy
solo_buys_rb    = []   # buys with no repeat partner

for (name, ticker), tbuy in by_name_ticker.items():
    tbuy.sort(key=lambda t: t["trade_date"])
    n_t = len(tbuy)
    is_repeat = [False] * n_t

    for i in range(n_t):
        d_i = datetime.strptime(tbuy[i]["trade_date"], "%Y-%m-%d")
        for j in range(i + 1, n_t):
            d_j = datetime.strptime(tbuy[j]["trade_date"], "%Y-%m-%d")
            if (d_j - d_i).days <= 30:
                is_repeat[i] = True
                is_repeat[j] = True
            else:
                break   # sorted, so no further j qualifies for this i

    for i, t in enumerate(tbuy):
        if not is_repeat[i]:
            solo_buys_rb.append(t)
            continue
        # determine if this trade is preceded by another repeat buy
        d_i = datetime.strptime(t["trade_date"], "%Y-%m-%d")
        preceded = any(
            0 < (d_i - datetime.strptime(tbuy[j]["trade_date"], "%Y-%m-%d")).days <= 30
            for j in range(i)
        )
        if preceded:
            repeat_followon.append(t)
        else:
            repeat_initial.append(t)

def group_alpha_60_90(trade_list):
    r60 = [r for t in trade_list
           for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    r90 = [r for t in trade_list
           for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
    return stats(r60), stats(r90)

ri_60,  ri_90  = group_alpha_60_90(repeat_initial)
rf_60,  rf_90  = group_alpha_60_90(repeat_followon)
so_60,  so_90  = group_alpha_60_90(solo_buys_rb)

# Combined repeat (initial + followon together)
combined_repeat = repeat_initial + repeat_followon
rc_60, rc_90 = group_alpha_60_90(combined_repeat)

print(f"\n  {'Group':<28}  {'N':>6}  {'Win%@60d':>9}  {'Ret@60d':>8}  "
      f"{'Win%@90d':>9}  {'Ret@90d':>8}")
print(f"  {'-'*28}  {'------':>6}  {'-'*9}  {'-'*8}  {'-'*9}  {'-'*8}")
for label, s60, s90 in [
    ("Initial conviction buy",  ri_60, ri_90),
    ("Follow-on buy (add)",     rf_60, rf_90),
    ("All repeat buys combined",rc_60, rc_90),
    ("Solo buy (baseline)",     so_60, so_90),
]:
    def fmt(s, key):
        return f"{s[key]:>+.1f}%" if s["n"] else "—"
    w60 = f"{s60['win_rate']:.1f}%" if s60["n"] else "—"
    w90 = f"{s90['win_rate']:.1f}%" if s90["n"] else "—"
    print(f"  {label:<28}  {s60['n']:>6,}  {w60:>9}  {fmt(s60,'avg_ret'):>8}  "
          f"{w90:>9}  {fmt(s90,'avg_ret'):>8}")

print(f"\n  Repeat buy alpha by position size — initial vs follow-on vs solo @ 60d (min n=5):")
print(f"  {'SizeBracket':<16}  {'Group':<10}  {'N':>5}  {'Win%':>7}  {'Ret@60d':>8}  {'Ret@90d':>8}")
print(f"  {'-'*16}  {'-'*10}  {'-----':>5}  {'----':>7}  {'------':>8}  {'------':>8}")
for sz in SIZE_ORDER:
    any_printed = False
    for label, trade_list in [
        ("Initial",  repeat_initial),
        ("Follow-on", repeat_followon),
        ("Solo",      solo_buys_rb),
    ]:
        sub = [t for t in trade_list if amount_bracket(t["amount"]) == sz]
        r60 = [r for t in sub for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
        r90 = [r for t in sub for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
        if len(r60) < 5:
            continue
        s60 = stats(r60)
        s90 = stats(r90)
        ret90_str = f"{s90['avg_ret']:>+7.1f}%" if s90["n"] else "     —"
        print(f"  {sz:<16}  {label:<10}  {s60['n']:>5,}  {s60['win_rate']:>6.1f}%  "
              f"{s60['avg_ret']:>+7.1f}%  {ret90_str:>8}")
        any_printed = True
    if any_printed:
        print()

print(f"\n  Politicians with most repeat-buy events (initial buys, min n=5, by win rate):")
print(f"  {'Politician':<30}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}")
print(f"  {'-'*30}  {'-----':>5}  {'----':>7}  {'------':>8}")
pol_rep = defaultdict(list)
for t in repeat_initial:
    r = ret(t["price_at_trade"], t["price_60d"])
    if r is not None:
        pol_rep[t["name"] or "Unknown"].append(r)
pol_rep_stats = [(nm, stats(v)) for nm, v in pol_rep.items() if len(v) >= 5]
pol_rep_stats.sort(key=lambda x: x[1]["win_rate"], reverse=True)
for nm, s in pol_rep_stats[:15]:
    print(f"  {nm:<30}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%")


# ═══════════════════════════════════════════════════════════════════════════════
#  PART B — ERA ANALYSIS + 30d HORIZON + FIRST-MOVER + LEADERBOARD
# ═══════════════════════════════════════════════════════════════════════════════

# ── Era definitions ──────────────────────────────────────────────────────────
ERAS = [
    ("ALL TIME",   None,       None),
    ("2014–2020",  "2014-01-01","2020-12-31"),
    ("2021–2024",  "2021-01-01","2024-12-31"),
    ("2025–2026",  "2025-01-01","2099-12-31"),
]

def era_filter(trade_list, since, until):
    out = []
    for t in trade_list:
        td = t.get("trade_date") or ""
        if since and td < since: continue
        if until and td > until: continue
        out.append(t)
    return out

# ── 30d return helper (direction-adjusted) ────────────────────────────────────
def direction_ret_30(t):
    r = ret(t["price_at_trade"], t["price_30d"])
    if r is None: return None
    if is_sell(t): return -r
    return r

# ── Build all three horizons at once ─────────────────────────────────────────
def tri_stats(trade_list):
    r30 = [r for t in trade_list for r in [direction_ret_30(t)] if r is not None]
    r60 = [r for t in trade_list for r in [direction_ret_60(t)] if r is not None]
    r90 = [r for t in trade_list for r in [direction_ret_90(t)] if r is not None]
    return stats(r30), stats(r60), stats(r90)

def fmt_s(s):
    if not s["n"]:
        return f"{'—':>6}  {'—':>7}  {'—':>7}"
    return (f"{s['n']:>6,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+6.1f}%")

# ── 20. ERA × KEY DIMENSIONS ──────────────────────────────────────────────────
DIMENSION_SPECS = [
    ("Chamber",           lambda t: t["chamber"] or "Unknown",                      10),
    ("Chamber×Type",      lambda t: f"{t['chamber'] or '?'} {'BUY' if is_buy(t) else 'SELL'}", 10),
    ("Party",             lambda t: (t["party"] or "Unknown").strip(),               10),
    ("PositionSize",      lambda t: amount_bracket(t["amount"]),                     10),
    ("DisclosureLag",     lambda t: lag_bracket(t["lag_days"]),                      10),
    ("Sector",            lambda t: t["sector"] or "Unknown",                        15),
]

print(f"\n{'='*70}")
print("  ERA ANALYSIS — KEY SIGNALS ACROSS TIME PERIODS")
print(f"{'='*70}")
print("  Compares 2014–2020 vs 2021–2024 vs 2025–2026 to spot signal drift.")
print("  Columns: N | Win% | AvgRet  (direction-adjusted, 60d horizon)")
print()

for dim_name, key_fn, min_n in DIMENSION_SPECS:
    print(f"\n  ── {dim_name} ──")
    # Collect all unique group keys across all data
    all_keys = sorted({key_fn(t) for t in priced_60})
    hdr = f"  {'Group':<30}"
    for era_label, since, until in ERAS:
        hdr += f"  {era_label:^22}"
    print(hdr)
    sub_hdr = f"  {'':<30}"
    for _ in ERAS:
        sub_hdr += f"  {'N':>6}  {'Win%':>6}  {'Ret':>6} "
    print(sub_hdr)
    print("  " + "-"*30 + ("  " + "-"*22) * len(ERAS))

    for key in all_keys:
        row = f"  {key:<30}"
        any_data = False
        for era_label, since, until in ERAS:
            era_data = era_filter(priced_60, since, until)
            subset = [t for t in era_data if key_fn(t) == key]
            rets = [r for t in subset for r in [direction_ret_60(t)] if r is not None]
            if len(rets) >= min_n:
                s = stats(rets)
                row += f"  {s['n']:>6,}  {s['win_rate']:>5.1f}%  {s['avg_ret']:>+5.1f}% "
                any_data = True
            else:
                row += f"  {'—':>6}  {'—':>6}  {'—':>6} "
        if any_data:
            print(row)

# ── 21. ERA × POSITION SIZE × CHAMBER ────────────────────────────────────────
print(f"\n{'='*70}")
print("  ERA × POSITION SIZE × CHAMBER  @ 60d (min n=10)")
print(f"{'='*70}")
print(f"\n  {'SizeBracket':<16}  {'Chamber':<8}", end="")
for era_label, _, _ in ERAS:
    print(f"  {era_label:^22}", end="")
print()
print(f"  {'-'*16}  {'-'*8}", end="")
for _ in ERAS:
    print(f"  {'N':>6}  {'Win%':>6}  {'Ret':>6} ", end="")
print()

for sz in SIZE_ORDER:
    for chamber in ["Senate", "House"]:
        row = f"  {sz:<16}  {chamber:<8}"
        any_data = False
        for era_label, since, until in ERAS:
            era_data = era_filter(priced_60, since, until)
            subset = [t for t in era_data
                      if amount_bracket(t["amount"]) == sz
                      and (t["chamber"] or "") == chamber]
            rets = [r for t in subset for r in [direction_ret_60(t)] if r is not None]
            if len(rets) >= 10:
                s = stats(rets)
                row += f"  {s['n']:>6,}  {s['win_rate']:>5.1f}%  {s['avg_ret']:>+5.1f}% "
                any_data = True
            else:
                row += f"  {'—':>6}  {'—':>6}  {'—':>6} "
        if any_data:
            print(row)

# ── 22. ERA × CLUSTER ANALYSIS ────────────────────────────────────────────────
print(f"\n{'='*70}")
print("  ERA × CLUSTER TRADE ALPHA  @ 60d")
print(f"{'='*70}")

# Pre-segment cluster_trades and non_cluster_trades by era
for era_label, since, until in ERAS:
    ct_era  = era_filter(cluster_trades, since, until)
    nct_era = era_filter(non_cluster_trades, since, until)
    cr  = [r for t in ct_era  for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    ncr = [r for t in nct_era for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    sc  = stats(cr)
    snc = stats(ncr)
    print(f"\n  {era_label}:")
    if sc["n"]:
        print(f"    Cluster buys    : N={sc['n']:,}  Win%={sc['win_rate']:.1f}%  AvgRet={sc['avg_ret']:+.1f}%")
    else:
        print(f"    Cluster buys    : —")
    if snc["n"]:
        print(f"    Non-cluster buys: N={snc['n']:,}  Win%={snc['win_rate']:.1f}%  AvgRet={snc['avg_ret']:+.1f}%")
    else:
        print(f"    Non-cluster buys: —")
    if sc["n"] and snc["n"]:
        edge = sc["avg_ret"] - snc["avg_ret"]
        print(f"    Cluster edge    : {edge:+.1f}% avg return advantage")

# ── 23. FIRST-MOVER vs FOLLOW-ON IN CLUSTERS ─────────────────────────────────
print(f"\n{'='*70}")
print("  FIRST-MOVER vs FOLLOW-ON IN CLUSTER WINDOWS  @ 60d & 90d")
print("  (first politician to buy a ticker that later becomes a cluster)")
print(f"{'='*70}")

# For each cluster buy, tag it as first-mover (earliest trade_date in the cluster window)
# or follow-on (any subsequent buy within 7 days of the earliest)
first_mover_trades = []
followon_trades    = []

for ticker, ticker_buys in by_ticker_buys.items():
    ticker_buys_sorted = sorted(ticker_buys, key=lambda t: t["trade_date"])
    # sliding 7-day windows: find the first trade of each cluster event
    assigned = set()
    for i, anchor in enumerate(ticker_buys_sorted):
        if anchor["id"] in assigned:
            continue
        anchor_date = datetime.strptime(anchor["trade_date"], "%Y-%m-%d")
        # gather all buys within 7 days of anchor by different politicians
        window = [o for o in ticker_buys_sorted
                  if o["name"] != anchor["name"]
                  and 0 <= (datetime.strptime(o["trade_date"], "%Y-%m-%d") - anchor_date).days <= 7]
        if window:
            # anchor is a first-mover
            first_mover_trades.append(anchor)
            assigned.add(anchor["id"])
            for o in window:
                if o["id"] not in assigned:
                    followon_trades.append(o)
                    assigned.add(o["id"])

fm_rets_60 = [r for t in first_mover_trades for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
fo_rets_60 = [r for t in followon_trades    for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
fm_rets_90 = [r for t in first_mover_trades for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
fo_rets_90 = [r for t in followon_trades    for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
sfm60 = stats(fm_rets_60); sfo60 = stats(fo_rets_60)
sfm90 = stats(fm_rets_90); sfo90 = stats(fo_rets_90)

print(f"\n  {'Group':<28}  {'N@60d':>6}  {'Win%@60d':>9}  {'Ret@60d':>8}  {'N@90d':>6}  {'Win%@90d':>9}  {'Ret@90d':>8}")
print(f"  {'-'*28}  {'------':>6}  {'-'*9}  {'-'*8}  {'------':>6}  {'-'*9}  {'-'*8}")
for label, s60, s90 in [
    ("First-mover (triggering buy)", sfm60, sfm90),
    ("Follow-on  (pile-in buy)",     sfo60, sfo90),
]:
    w60 = f"{s60['win_rate']:.1f}%" if s60["n"] else "—"
    w90 = f"{s90['win_rate']:.1f}%" if s90["n"] else "—"
    r60 = f"{s60['avg_ret']:>+.1f}%" if s60["n"] else "—"
    r90 = f"{s90['avg_ret']:>+.1f}%" if s90["n"] else "—"
    print(f"  {label:<28}  {s60['n']:>6,}  {w60:>9}  {r60:>8}  {s90['n']:>6,}  {w90:>9}  {r90:>8}")

# Era breakdown for first-mover
print(f"\n  First-mover alpha by era @ 60d:")
for era_label, since, until in ERAS:
    fm_era = era_filter(first_mover_trades, since, until)
    rets = [r for t in fm_era for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    if rets:
        s = stats(rets)
        print(f"    {era_label:<12}: N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

# ── 24. 30d / 60d / 90d HORIZON COMPARISON ────────────────────────────────────
print(f"\n{'='*70}")
print("  HORIZON COMPARISON — does alpha peak at 30d, 60d, or 90d?")
print(f"{'='*70}")
print(f"\n  {'Dimension':<30}  {'30d Win%':>9}  {'30d Ret':>8}  {'60d Win%':>9}  {'60d Ret':>8}  {'90d Win%':>9}  {'90d Ret':>8}")
print(f"  {'-'*30}  {'-'*9}  {'-'*8}  {'-'*9}  {'-'*8}  {'-'*9}  {'-'*8}")

def horizon_row(label, trade_list):
    s30, s60, s90 = tri_stats(trade_list)
    def fmt(s):
        if not s["n"]: return f"{'—':>9}  {'—':>8}"
        return f"{s['win_rate']:>8.1f}%  {s['avg_ret']:>+7.1f}%"
    print(f"  {label:<30}  {fmt(s30)}  {fmt(s60)}  {fmt(s90)}")

horizon_row("ALL (direction-adjusted)",    priced_60)
horizon_row("Buys only",                   [t for t in priced_60 if is_buy(t)])
horizon_row("Sells only (adj)",            [t for t in priced_60 if is_sell(t)])
horizon_row("Senate",                      [t for t in priced_60 if (t["chamber"] or "") == "Senate"])
horizon_row("House",                       [t for t in priced_60 if (t["chamber"] or "") == "House"])
horizon_row("Senate Buys",                 [t for t in priced_60 if (t["chamber"] or "") == "Senate" and is_buy(t)])
horizon_row("House Buys",                  [t for t in priced_60 if (t["chamber"] or "") == "House" and is_buy(t)])
horizon_row("Cluster buys",                cluster_trades)
horizon_row("First-mover buys",            first_mover_trades)
horizon_row("Follow-on buys",              followon_trades)
horizon_row("Repeat buys (≤30d)",          repeat_initial + repeat_followon)
horizon_row("Size ≥$100K (all)",           [t for t in priced_60 if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")])
horizon_row("Cluster + ≥$100K",            [t for t in cluster_trades if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")])
horizon_row("First-mover + ≥$100K",       [t for t in first_mover_trades if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")])

# ── 25. POLITICIAN ERA TRENDS ──────────────────────────────────────────────────
print(f"\n{'='*70}")
print("  POLITICIAN ERA TRENDS  @ 60d  (top traders per era, min 10 priced trades)")
print(f"{'='*70}")

for era_label, since, until in ERAS[1:]:   # skip ALL TIME; too much noise
    era_data = era_filter(priced_60, since, until)
    pol_groups = defaultdict(list)
    for t in era_data:
        r = direction_ret_60(t)
        if r is not None:
            pol_groups[t["name"] or "Unknown"].append(r)
    ranked = [(nm, stats(v)) for nm, v in pol_groups.items() if len(v) >= 10]
    ranked.sort(key=lambda x: x[1]["avg_ret"], reverse=True)
    print(f"\n  {era_label} — top 10 by avg return @ 60d:")
    print(f"  {'Politician':<30}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}  {'MedRet':>8}")
    print(f"  {'-'*30}  {'-----':>5}  {'----':>7}  {'------':>8}  {'------':>8}")
    for nm, s in ranked[:10]:
        print(f"  {nm:<30}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%  {s['med_ret']:>+7.1f}%")

# ── 26. SIGNAL LEADERBOARD ────────────────────────────────────────────────────
# Composite score = win_rate * log(1 + avg_ret) capped to 100 range
# Higher = better quality signal
print(f"\n{'='*70}")
print("  SIGNAL LEADERBOARD — COMPOSITE SCORE  @ 60d, direction-adjusted")
print("  Score = win_rate% × (1 + avg_return%) / 100   (min n=20)")
print(f"{'='*70}")

import math

def composite(s):
    if not s["n"] or s["win_rate"] is None or s["avg_ret"] is None:
        return -999
    return s["win_rate"] * (1 + s["avg_ret"] / 100)

leaderboard = []

def add_signal(label, trade_list, era="ALL"):
    rets = [r for t in trade_list for r in [direction_ret_60(t)] if r is not None]
    if len(rets) < 20:
        return
    s = stats(rets)
    leaderboard.append((label, era, s, composite(s)))

# Prime the leaderboard with every signal we've computed
for era_label, since, until in ERAS:
    era_p60 = era_filter(priced_60, since, until)
    era_ct  = era_filter(cluster_trades, since, until)
    era_fm  = era_filter(first_mover_trades, since, until)
    era_fo  = era_filter(followon_trades, since, until)
    era_ri  = era_filter(repeat_initial, since, until)
    era_rf  = era_filter(repeat_followon, since, until)

    add_signal("All trades",                    era_p60, era_label)
    add_signal("Buys",                          [t for t in era_p60 if is_buy(t)], era_label)
    add_signal("Sells (adj)",                   [t for t in era_p60 if is_sell(t)], era_label)
    add_signal("Senate",                        [t for t in era_p60 if (t["chamber"] or "") == "Senate"], era_label)
    add_signal("House",                         [t for t in era_p60 if (t["chamber"] or "") == "House"], era_label)
    add_signal("Senate Buys",                   [t for t in era_p60 if (t["chamber"] or "") == "Senate" and is_buy(t)], era_label)
    add_signal("House Buys",                    [t for t in era_p60 if (t["chamber"] or "") == "House" and is_buy(t)], era_label)
    add_signal("Cluster buys",                  era_ct, era_label)
    add_signal("First-mover buys",              era_fm, era_label)
    add_signal("Follow-on buys",                era_fo, era_label)
    add_signal("Repeat buys",                   era_ri + era_rf, era_label)
    add_signal("Size ≥$100K",                   [t for t in era_p60 if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")], era_label)
    add_signal("Cluster + ≥$100K",              [t for t in era_ct if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")], era_label)
    add_signal("First-mover + ≥$100K",         [t for t in era_fm if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")], era_label)
    add_signal("Senate Cluster buys",           [t for t in era_ct if (t["chamber"] or "") == "Senate"], era_label)
    add_signal("House Cluster buys",            [t for t in era_ct if (t["chamber"] or "") == "House"], era_label)
    add_signal("Lag 0–30d",                     [t for t in era_p60 if lag_bracket(t["lag_days"]) == "0–30d"], era_label)
    add_signal("Lag 0–30d + Cluster",           [t for t in era_ct if lag_bracket(t["lag_days"]) == "0–30d"], era_label)
    # By sector (top sectors)
    for sector in ["Technology", "Healthcare", "Financials", "Energy", "Industrials", "Consumer Discretionary"]:
        sub = [t for t in era_p60 if (t["sector"] or "") == sector]
        add_signal(f"Sector: {sector}", sub, era_label)
        sub_buy = [t for t in sub if is_buy(t)]
        add_signal(f"Sector: {sector} Buys", sub_buy, era_label)

leaderboard.sort(key=lambda x: x[3], reverse=True)

print(f"\n  TOP 30 SIGNAL + ERA COMBINATIONS  (composite score)")
print(f"  {'Signal':<30}  {'Era':<12}  {'N':>6}  {'Win%':>8}  {'AvgRet':>8}  {'Score':>7}")
print(f"  {'-'*30}  {'-'*12}  {'------':>6}  {'------':>8}  {'------':>8}  {'-----':>7}")
for label, era, s, score in leaderboard[:30]:
    print(f"  {label:<30}  {era:<12}  {s['n']:>6,}  {s['win_rate']:>7.1f}%  {s['avg_ret']:>+7.1f}%  {score:>7.2f}")

# Bottom 10 (worst signals)
print(f"\n  BOTTOM 10 SIGNALS (worst composite score):")
print(f"  {'Signal':<30}  {'Era':<12}  {'N':>6}  {'Win%':>8}  {'AvgRet':>8}  {'Score':>7}")
print(f"  {'-'*30}  {'-'*12}  {'------':>6}  {'------':>8}  {'------':>8}  {'-----':>7}")
worst = [x for x in leaderboard if x[2]["n"] >= 20]
worst.sort(key=lambda x: x[3])
for label, era, s, score in worst[:10]:
    print(f"  {label:<30}  {era:<12}  {s['n']:>6,}  {s['win_rate']:>7.1f}%  {s['avg_ret']:>+7.1f}%  {score:>7.2f}")


# ═══════════════════════════════════════════════════════════════════════════════
#  PART C — CLUSTER SELLS + 7-DAY SUCCESSIVE TRADES
# ═══════════════════════════════════════════════════════════════════════════════

# ── 27. CLUSTER SELLS ─────────────────────────────────────────────────────────
# Cluster sell = ≥2 different politicians SELL the same ticker within 7 days
print(f"\n{'='*70}")
print("  CLUSTER SELL ANALYSIS  @ 60d & 90d")
print("  (≥2 different politicians sell same ticker within 7 calendar days)")
print(f"{'='*70}")

sells_all = [t for t in priced_60 if is_sell(t) and t["trade_date"]]
by_ticker_sells = defaultdict(list)
for t in sells_all:
    by_ticker_sells[t["ticker"]].append(t)

cluster_sells     = []
non_cluster_sells = []

for ticker, ticker_sells in by_ticker_sells.items():
    ticker_sells.sort(key=lambda t: t["trade_date"])
    for t in ticker_sells:
        t_date = datetime.strptime(t["trade_date"], "%Y-%m-%d")
        window = [o for o in ticker_sells
                  if o["name"] != t["name"]
                  and abs((datetime.strptime(o["trade_date"], "%Y-%m-%d") - t_date).days) <= 7]
        if window:
            cluster_sells.append(t)
        else:
            non_cluster_sells.append(t)

def _sell_adj_rets(trade_list, horizon="60d"):
    key = "price_60d" if horizon == "60d" else "price_90d"
    return [r for t in trade_list
            for r in [-ret(t["price_at_trade"], t[key])]   # invert: sell wins if price fell
            if ret(t["price_at_trade"], t[key]) is not None]

cs_rets  = _sell_adj_rets(cluster_sells,     "60d")
ncs_rets = _sell_adj_rets(non_cluster_sells, "60d")
scs  = stats(cs_rets)
sncs = stats(ncs_rets)

print(f"\n  Cluster sells (adj)    : N={scs['n']:,}  Win%={scs['win_rate']:.1f}%  AvgRet={scs['avg_ret']:+.1f}%  MedRet={scs['med_ret']:+.1f}%")
print(f"  Non-cluster sells (adj): N={sncs['n']:,}  Win%={sncs['win_rate']:.1f}%  AvgRet={sncs['avg_ret']:+.1f}%  MedRet={sncs['med_ret']:+.1f}%")
if scs['n'] and sncs['n']:
    print(f"  Cluster sell edge      : {scs['avg_ret'] - sncs['avg_ret']:+.1f}% avg return advantage")

# Cluster sells by chamber
print(f"\n  Cluster sells by chamber @ 60d (direction-adjusted):")
for chamber in ["Senate", "House"]:
    sub = [t for t in cluster_sells if (t["chamber"] or "") == chamber]
    rets = _sell_adj_rets(sub, "60d")
    if rets:
        s = stats(rets)
        print(f"    {chamber:<8}: N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

# Cluster sells by position size
print(f"\n  Cluster sells by position size @ 60d (min n=5):")
print(f"  {'SizeBracket':<16}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}  {'MedRet':>8}")
print(f"  {'-'*16}  {'-----':>5}  {'----':>7}  {'------':>8}  {'------':>8}")
for sz in SIZE_ORDER:
    sub = [t for t in cluster_sells if amount_bracket(t["amount"]) == sz]
    rets = _sell_adj_rets(sub, "60d")
    if len(rets) >= 5:
        s = stats(rets)
        print(f"  {sz:<16}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%  {s['med_ret']:>+7.1f}%")

# Cluster sells by era
print(f"\n  Cluster sell alpha by era @ 60d:")
for era_label, since, until in ERAS:
    sub = era_filter(cluster_sells, since, until)
    rets = _sell_adj_rets(sub, "60d")
    if len(rets) >= 10:
        s = stats(rets)
        print(f"    {era_label:<12}: N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

# ── 28. 7-DAY SUCCESSIVE TRADES BY SAME POLITICIAN ────────────────────────────
# A politician trades ticker X, then trades X again within 7 calendar days.
# Tests whether rapid accumulation / distribution within a week signals alpha.
print(f"\n{'='*70}")
print("  7-DAY SUCCESSIVE TRADES — SAME POLITICIAN, SAME TICKER  @ 60d & 90d")
print("  (politician trades ticker, then trades it again within 7 days)")
print(f"{'='*70}")

# Build by (politician, ticker) — all trade types
all_dated = [t for t in trades if t["trade_date"]]
all_dated.sort(key=lambda t: (t["name"] or "", t["ticker"] or "", t["trade_date"]))

by_pol_ticker = defaultdict(list)
for t in all_dated:
    by_pol_ticker[(t["name"], t["ticker"])].append(t)

successive_7d_buys  = []   # buy trades that are part of a 7-day successive pair
successive_7d_sells = []
solo_7d_buys        = []   # buys with no 7-day partner
solo_7d_sells       = []

for (name, ticker), tlist in by_pol_ticker.items():
    tlist.sort(key=lambda t: t["trade_date"])
    n_t = len(tlist)
    in_7d_window = [False] * n_t

    for i in range(n_t):
        d_i = datetime.strptime(tlist[i]["trade_date"], "%Y-%m-%d")
        for j in range(i + 1, n_t):
            d_j = datetime.strptime(tlist[j]["trade_date"], "%Y-%m-%d")
            if (d_j - d_i).days <= 7:
                in_7d_window[i] = True
                in_7d_window[j] = True
            else:
                break   # sorted, so no further j qualifies for this i

    for i, t in enumerate(tlist):
        if is_buy(t):
            if in_7d_window[i]:
                successive_7d_buys.append(t)
            else:
                solo_7d_buys.append(t)
        elif is_sell(t):
            if in_7d_window[i]:
                successive_7d_sells.append(t)
            else:
                solo_7d_sells.append(t)

# Stats at 60d & 90d
def buy_rets_60_90(tlist):
    r60 = [r for t in tlist for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    r90 = [r for t in tlist for r in [ret(t["price_at_trade"], t["price_90d"])] if r is not None]
    return stats(r60), stats(r90)

def sell_adj_rets_60_90(tlist):
    r60 = _sell_adj_rets(tlist, "60d")
    r90 = _sell_adj_rets(tlist, "90d")
    return stats(r60), stats(r90)

sb_60,  sb_90  = buy_rets_60_90(successive_7d_buys)
slb_60, slb_90 = buy_rets_60_90(solo_7d_buys)
ss_60,  ss_90  = sell_adj_rets_60_90(successive_7d_sells)
sls_60, sls_90 = sell_adj_rets_60_90(solo_7d_sells)

print(f"\n  {'Group':<35}  {'N@60d':>6}  {'Win%@60':>8}  {'Ret@60':>7}  {'N@90d':>6}  {'Win%@90':>8}  {'Ret@90':>7}")
print(f"  {'-'*35}  {'------':>6}  {'-'*8}  {'-'*7}  {'------':>6}  {'-'*8}  {'-'*7}")
for label, s60, s90 in [
    ("7-day successive BUYS",             sb_60,  sb_90),
    ("Solo buys (no 7d repeat)",          slb_60, slb_90),
    ("7-day successive SELLS (adj)",      ss_60,  ss_90),
    ("Solo sells (no 7d repeat) (adj)",   sls_60, sls_90),
]:
    w60 = f"{s60['win_rate']:.1f}%" if s60["n"] else "—"
    w90 = f"{s90['win_rate']:.1f}%" if s90["n"] else "—"
    r60 = f"{s60['avg_ret']:>+.1f}%" if s60["n"] else "—"
    r90 = f"{s90['avg_ret']:>+.1f}%" if s90["n"] else "—"
    print(f"  {label:<35}  {s60['n']:>6,}  {w60:>8}  {r60:>7}  {s90['n']:>6,}  {w90:>8}  {r90:>7}")

# Successive 7d by chamber
print(f"\n  7-day successive BUYS by chamber @ 60d:")
for chamber in ["Senate", "House"]:
    sub = [t for t in successive_7d_buys if (t["chamber"] or "") == chamber]
    s60, _ = buy_rets_60_90(sub)
    if s60["n"]:
        print(f"    {chamber:<8}: N={s60['n']:,}  Win%={s60['win_rate']:.1f}%  AvgRet={s60['avg_ret']:+.1f}%")

print(f"\n  7-day successive SELLS by chamber @ 60d (adj):")
for chamber in ["Senate", "House"]:
    sub = [t for t in successive_7d_sells if (t["chamber"] or "") == chamber]
    rets = _sell_adj_rets(sub, "60d")
    if rets:
        s = stats(rets)
        print(f"    {chamber:<8}: N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

# Successive 7d buys by position size
print(f"\n  7-day successive BUYS by position size @ 60d (min n=5):")
print(f"  {'SizeBracket':<16}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}")
print(f"  {'-'*16}  {'-----':>5}  {'----':>7}  {'------':>8}")
for sz in SIZE_ORDER:
    sub = [t for t in successive_7d_buys if amount_bracket(t["amount"]) == sz]
    s60, _ = buy_rets_60_90(sub)
    if s60["n"] >= 5:
        print(f"  {sz:<16}  {s60['n']:>5,}  {s60['win_rate']:>6.1f}%  {s60['avg_ret']:>+7.1f}%")

# Successive 7d sells by position size
print(f"\n  7-day successive SELLS by position size @ 60d adj (min n=5):")
print(f"  {'SizeBracket':<16}  {'N':>5}  {'Win%':>7}  {'AvgRet':>8}")
print(f"  {'-'*16}  {'-----':>5}  {'----':>7}  {'------':>8}")
for sz in SIZE_ORDER:
    sub = [t for t in successive_7d_sells if amount_bracket(t["amount"]) == sz]
    rets = _sell_adj_rets(sub, "60d")
    if len(rets) >= 5:
        s = stats(rets)
        print(f"  {sz:<16}  {s['n']:>5,}  {s['win_rate']:>6.1f}%  {s['avg_ret']:>+7.1f}%")

# Successive 7d buys by era
print(f"\n  7-day successive BUYS by era @ 60d:")
for era_label, since, until in ERAS:
    sub = era_filter(successive_7d_buys, since, until)
    s60, _ = buy_rets_60_90(sub)
    if s60["n"] >= 10:
        print(f"    {era_label:<12}: N={s60['n']:,}  Win%={s60['win_rate']:.1f}%  AvgRet={s60['avg_ret']:+.1f}%")

print(f"\n  7-day successive SELLS by era @ 60d (adj):")
for era_label, since, until in ERAS:
    sub = era_filter(successive_7d_sells, since, until)
    rets = _sell_adj_rets(sub, "60d")
    if len(rets) >= 10:
        s = stats(rets)
        print(f"    {era_label:<12}: N={s['n']:,}  Win%={s['win_rate']:.1f}%  AvgRet={s['avg_ret']:+.1f}%")

# ── 29. ADD MISSING SIGNALS TO LEADERBOARD ────────────────────────────────────
# Re-score and extend the leaderboard with cluster sells + successive 7d signals
print(f"\n{'='*70}")
print("  EXTENDED LEADERBOARD — ALL SIGNALS INCLUDING CLUSTER SELLS & 7-DAY SUCCESSIVE")
print(f"{'='*70}")

def add_sell_signal(label, trade_list, era="ALL"):
    rets = _sell_adj_rets(trade_list, "60d")
    if len(rets) < 20:
        return
    s = stats(rets)
    leaderboard.append((label, era, s, composite(s)))

def add_buy_signal(label, trade_list, era="ALL"):
    rets = [r for t in trade_list for r in [ret(t["price_at_trade"], t["price_60d"])] if r is not None]
    if len(rets) < 20:
        return
    s = stats(rets)
    leaderboard.append((label, era, s, composite(s)))

for era_label, since, until in ERAS:
    cs_era   = era_filter(cluster_sells,     since, until)
    ncs_era  = era_filter(non_cluster_sells, since, until)
    s7b_era  = era_filter(successive_7d_buys,  since, until)
    s7s_era  = era_filter(successive_7d_sells, since, until)
    sl7b_era = era_filter(solo_7d_buys,        since, until)
    sl7s_era = era_filter(solo_7d_sells,       since, until)

    add_sell_signal("Cluster SELLS",                cs_era, era_label)
    add_sell_signal("Non-cluster SELLS",            ncs_era, era_label)
    add_sell_signal("Senate Cluster SELLS",         [t for t in cs_era if (t["chamber"] or "") == "Senate"], era_label)
    add_sell_signal("House Cluster SELLS",          [t for t in cs_era if (t["chamber"] or "") == "House"], era_label)
    add_buy_signal("7d Successive BUYS",            s7b_era, era_label)
    add_sell_signal("7d Successive SELLS",          s7s_era, era_label)
    add_buy_signal("Senate 7d Successive BUYS",    [t for t in s7b_era if (t["chamber"] or "") == "Senate"], era_label)
    add_buy_signal("House 7d Successive BUYS",     [t for t in s7b_era if (t["chamber"] or "") == "House"], era_label)
    add_buy_signal("7d Succ BUYS ≥$100K",          [t for t in s7b_era if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")], era_label)
    add_sell_signal("7d Succ SELLS ≥$100K",        [t for t in s7s_era if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")], era_label)
    add_sell_signal("Cluster SELLS ≥$100K",        [t for t in cs_era if amount_bracket(t["amount"]) in ("$100K–$250K","$250K–$500K","$500K–$1M","$1M+")], era_label)

leaderboard.sort(key=lambda x: x[3], reverse=True)
# Deduplicate (same label+era may appear from both passes — keep highest score)
seen = set()
deduped = []
for row in leaderboard:
    key = (row[0], row[1])
    if key not in seen:
        seen.add(key)
        deduped.append(row)

print(f"\n  TOP 40 SIGNAL + ERA COMBINATIONS  (composite score)")
print(f"  {'Signal':<35}  {'Era':<12}  {'N':>6}  {'Win%':>8}  {'AvgRet':>8}  {'Score':>7}")
print(f"  {'-'*35}  {'-'*12}  {'------':>6}  {'------':>8}  {'------':>8}  {'-----':>7}")
for label, era, s, score in deduped[:40]:
    print(f"  {label:<35}  {era:<12}  {s['n']:>6,}  {s['win_rate']:>7.1f}%  {s['avg_ret']:>+7.1f}%  {score:>7.2f}")

print(f"\n  BOTTOM 10 SIGNALS (worst composite score, min n=20):")
print(f"  {'Signal':<35}  {'Era':<12}  {'N':>6}  {'Win%':>8}  {'AvgRet':>8}  {'Score':>7}")
print(f"  {'-'*35}  {'-'*12}  {'------':>6}  {'------':>8}  {'------':>8}  {'-----':>7}")
worst2 = [x for x in deduped if x[2]["n"] >= 20]
worst2.sort(key=lambda x: x[3])
for label, era, s, score in worst2[:10]:
    print(f"  {label:<35}  {era:<12}  {s['n']:>6,}  {s['win_rate']:>7.1f}%  {s['avg_ret']:>+7.1f}%  {score:>7.2f}")

print(f"\n{'='*70}")
print("  ANALYSIS COMPLETE")
print(f"{'='*70}\n")
