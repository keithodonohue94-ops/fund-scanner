"""Superseded projected report dates are dropped (NVDA 11-18 vs confirmed 11-17; AAPL's 10-29 estimate after the 11-02 report)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db  # noqa: E402

U = lambda d, fa="2026-11-10": dict(date=d, fiscal_end=d, is_upcoming=True, eps_actual=None, rev_actual=None, fetched_at=fa)
R = lambda d: dict(date=d, fiscal_end=d, is_upcoming=False, eps_actual=1.0, rev_actual=1.0, fetched_at="2026-11-03")
f = lambda rows: [r["date"] for r in db.drop_superseded_upcoming(rows)]

assert f([U("2027-02-04"), R("2026-11-02"), U("2026-10-29", "2026-10-20"), R("2026-07-30")]) == ["2027-02-04", "2026-11-02", "2026-07-30"]
assert f([U("2026-11-18", "2026-10-01"), U("2026-11-17", "2026-11-15"), R("2026-08-26")]) == ["2026-11-17", "2026-08-26"]
assert f([U("2026-11-18", "x"), U("2026-11-17", "x")]) == ["2026-11-17"]          # same fetch: earlier date
assert f([U("2026-11-05"), R("2026-08-05")]) == ["2026-11-05", "2026-08-05"]       # reported, actuals pending: kept
assert f([U("2027-02-20"), U("2026-11-20")]) == ["2027-02-20", "2026-11-20"]       # different quarters: both kept
print("EARNINGS DEDUPE OK")
