"""Political rows: one identity per member, duplicate sales removed, fixed-horizon excess-return leaderboard."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import legislators as L  # noqa: E402
import pol_perf as P  # noqa: E402

MEMBERS = [
    {"id": {"bioguide": "C001047"}, "name": {"first": "Shelley", "last": "Capito", "official_full": "Shelley Moore Capito"},
     "terms": [{"type": "sen", "state": "WV", "party": "Republican"}]},
    {"id": {"bioguide": "F000472"}, "name": {"first": "C.", "last": "Franklin", "nickname": "Scott", "official_full": "Scott Franklin"},
     "terms": [{"type": "rep", "state": "FL", "district": 18, "party": "Republican"}]},
    {"id": {"bioguide": "A000372"}, "name": {"first": "Richard", "last": "Allen", "nickname": "Rick", "official_full": "Rick W. Allen"},
     "terms": [{"type": "rep", "state": "GA", "district": 12, "party": "Republican"}]},
    {"id": {"bioguide": "A000383"}, "name": {"first": "Alan", "last": "Armstrong", "official_full": "Alan Armstrong"},
     "terms": [{"type": "sen", "state": "OK", "party": "Republican"}]},
    {"id": {"bioguide": "A000379"}, "name": {"first": "Mark", "last": "Alford", "official_full": "Mark Alford"},
     "terms": [{"type": "rep", "state": "MO", "district": 4, "party": "Republican"}]},
]
idx = L.build_index(MEMBERS)
same = lambda *names: len({L.identify(c, n, d, idx)["id"] for c, n, d in names}) == 1
assert same(("House", "Scott Franklin", "FL18"), ("House", "Scott Mr Franklin", "FL18"))
assert same(("House", "Rick Allen", "GA12"), ("House", "Richard W. Allen", "GA12"))
assert same(("Senate", "Armstrong, Alan (Senator)", ""), ("Senate", "Alan Armstrong", ""))
assert L.identify("House", "Someone Else", "FL18", idx)["id"].startswith("n:")      # seat holder's surname must match

R = lambda **k: {"chamber": "Senate", "name": "Shelley Moore Capito", "district": "WV", "ticker": "META",
                 "trade_date": "2026-10-21", "amount": "$1,001 - $15,000", "type": "Sale", "party": "", **k}
rows = [R(), R(type="Sale (Partial)"),                                      # the same sale listed twice
        R(ticker="NVDA"),                                                   # generic sale with no twin: kept
        R(chamber="House", name="Scott Franklin", district="FL18", ticker="AAPL", type="Purchase"),
        R(chamber="House", name="Scott Mr Franklin", district="FL18", ticker="AAPL", type="Purchase"),   # spelling twin
        R(chamber="House", name="Mark Alford", district="MO04", ticker="T", type="Sale (Partial)"),
        R(chamber="House", name="Mark Alford", district="MO04", ticker="T", type="Sale (Partial)"),      # two lots: kept
        R(type="Sale Partial", ticker="ZTS")]
out = L.clean_rows([dict(r) for r in rows], idx)
assert [(r["ticker"], r["type"]) for r in out] == [("META", "Sale (Partial)"), ("NVDA", "Sale"), ("AAPL", "Purchase"),
                                                   ("T", "Sale (Partial)"), ("T", "Sale (Partial)"), ("ZTS", "Sale (Partial)")], out
assert all(r["party"] == "R" for r in out) and out[2]["name_filed"] == "Scott Franklin"

spy = {f"2026-{m:02d}-{d:02d}": 100.0 + m for m in range(1, 13) for d in range(1, 29)}
T = lambda **k: {"member_id": "X", "name": "X", "party": "D", "chamber": "House", "ticker": "A", "type": "Purchase",
                 "trade_date": "2026-03-02", "price_at_trade": 100, "price_30d": 110, "price_60d": 120, **k}
assert abs(P.trade_excess(T(), spy, 60) - (20 - (105 / 103 - 1) * 100)) < 1e-9
assert P.trade_excess(T(type="Sale"), spy, 60) < 0                          # stock beat SPY after a sale: a loss
assert P.trade_excess(T(type="Exchange"), spy, 60) is None
assert P.leaderboard([T()] * 4, spy) == []                                   # fewer than MIN_TRADES
lb = P.leaderboard([T()] * 5, spy)
assert lb[0]["win_rate"] == 100 and lb[0]["avg_excess_30d"] is not None
assert P.leaderboard([T(trade_date="2023-03-02")] * 5, spy, since="2024-11-16") == []   # outside the window
print("POLITICAL CLEAN OK")
