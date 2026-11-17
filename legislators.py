"""
Congressional trade rows: party, one identity per member, and duplicate removal.

The House and Senate disclosure feeds carry no party, spell the same member several ways ("Scott Franklin" /
"Scott Mr Franklin", "Rick Allen" / "Richard W. Allen", "Armstrong, Alan (Senator)") and sometimes list one sale
twice — once as a generic "Sale" and again, a day later, as "Sale (Partial)" or "Sale (Full)". Members are looked
up in the public congress-legislators dataset (current members), cached for a day: House members by state +
district (the feed's "OH02"), senators by state + surname, anyone else by first initial + surname. A member who
can't be matched keeps a normalised-name identity; nothing is guessed.
"""
import logging
import re
import time
import unicodedata

import requests

logger = logging.getLogger(__name__)

SOURCES = ("https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json",
           "https://unitedstates.github.io/congress-legislators/legislators-current.json")
_CACHE = {"at": 0.0, "idx": None}
PARTY = {"Democrat": "D", "Republican": "R", "Independent": "I", "Libertarian": "L"}
_HONORIFIC = {"jr", "sr", "ii", "iii", "iv", "senator", "hon", "dr", "mr", "mrs", "ms", "the", "honorable", "rep"}


def _fold(s: str) -> list:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", " ", s).split()


def _surname(words: list) -> str:
    words = [w for w in words if w not in _HONORIFIC]
    return words[-1] if words else ""


def _words(name: str) -> list:
    """'Armstrong, Alan (Senator)' → ['alan', 'armstrong']; honorifics dropped."""
    raw = name or ""
    if "," in raw:
        last_part, first_part = raw.split(",", 1)
        raw = f"{first_part} {last_part}"
    raw = re.sub(r"\(.*?\)|\".*?\"", " ", raw)
    return [w for w in _fold(raw) if w not in _HONORIFIC]


def build_index(members: list) -> dict:
    """seat 'OH02' / (state, surname) / (first initial, surname) → (party, member id, official name)."""
    by_seat, by_sen, by_name = {}, {}, {}
    for m in members:
        t = (m.get("terms") or [{}])[-1]
        nm = m.get("name") or {}
        rec = (PARTY.get(t.get("party"), (t.get("party") or "")[:1]),
               (m.get("id") or {}).get("bioguide") or "",
               nm.get("official_full") or " ".join(filter(None, [nm.get("first"), nm.get("last")])))
        last = _surname(_fold(nm.get("last", "")))
        firsts = {w[:1] for w in _fold(" ".join(filter(None, [nm.get("first"), nm.get("nickname"), nm.get("middle")])))}
        if t.get("type") == "rep" and t.get("state") is not None:
            by_seat[f"{t['state']}{int(t.get('district') or 0):02d}"] = rec
        if t.get("type") == "sen":
            by_sen.setdefault((t.get("state"), last), rec)
        for f in firsts:
            by_name.setdefault((f, last), set()).add(rec)
    return {"seat": by_seat, "sen": by_sen, "name": by_name}


def _index():
    if _CACHE["idx"] is not None and time.time() - _CACHE["at"] < 86400:
        return _CACHE["idx"]
    for url in SOURCES:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            _CACHE.update(idx=build_index(r.json()), at=time.time())
            return _CACHE["idx"]
        except Exception as exc:
            logger.warning("legislators: could not load %s: %s", url, exc)
    if _CACHE["idx"] is None:
        _CACHE.update(idx={"seat": {}, "sen": {}, "name": {}}, at=time.time() - 86400 + 900)  # retry in 15 min
    return _CACHE["idx"]


def identify(chamber: str, name: str, district: str, idx: dict | None = None) -> dict:
    """{'id', 'name', 'party'} for one filed name. id is the bioguide id when matched, else 'n:first last'."""
    idx = idx if idx is not None else _index()
    words = _words(name)
    last = words[-1] if words else ""
    d = (district or "").strip().upper()
    rec = None
    if chamber == "House" and re.fullmatch(r"[A-Z]{2}\d{1,2}", d):
        rec = idx["seat"].get(f"{d[:2]}{int(d[2:]):02d}")
        if rec and last and _surname(_fold(rec[2])) != last:
            rec = None                                   # seat changed hands: don't attach someone else
    if rec is None and chamber == "Senate" and re.fullmatch(r"[A-Z]{2}", d):
        rec = idx["sen"].get((d, last))
    if rec is None and words:
        recs = idx["name"].get((words[0][:1], last))
        if recs and len(recs) == 1:
            rec = next(iter(recs))
    if rec:
        return {"id": rec[1] or "n:" + " ".join(words), "name": rec[2] or name, "party": rec[0]}
    key = f"{words[0]} {last}" if len(words) >= 2 else (last or (name or "").lower())
    return {"id": "n:" + key, "name": name, "party": ""}


def party_for(chamber: str, name: str, district: str, idx: dict | None = None) -> str:
    return identify(chamber, name, district, idx)["party"]


_GENERIC_SALE = "Sale"


def clean_rows(rows: list, idx: dict | None = None) -> list:
    """API rows → one identity per member (name = official name, name_filed = as filed, member_id), party filled,
    "Sale Partial" spelled "Sale (Partial)", and duplicates removed:
      - a generic "Sale" when the same member filed a "Sale (Partial)"/"Sale (Full)" for the same ticker, trade date
        and amount (the same sale listed twice — 327 such pairs in the 2026 disclosures);
      - the same trade filed under two spellings of one member's name.
    Repeated lots under the same filed name are kept. Order is preserved."""
    if not rows:
        return rows
    idx = idx if idx is not None else _index()
    cache = {}
    for r in rows:
        k = (r.get("chamber"), r.get("name"), r.get("district"))
        if k not in cache:
            cache[k] = identify(*k, idx=idx)
        who = cache[k]
        r["name_filed"] = r.get("name")
        r["member_id"] = who["id"]
        r["name"] = who["name"] or r.get("name")
        if not r.get("party"):
            r["party"] = who["party"]
        if r.get("type") == "Sale Partial":
            r["type"] = "Sale (Partial)"
    specific = {(r["member_id"], r.get("ticker"), r.get("trade_date"), r.get("amount"))
                for r in rows if (r.get("type") or "").startswith("Sale (")}
    seen, out = {}, []
    for r in rows:
        base = (r["member_id"], r.get("ticker"), r.get("trade_date"), r.get("amount"))
        if r.get("type") == _GENERIC_SALE and base in specific:
            continue
        full = base + (r.get("type"),)
        first_name = seen.setdefault(full, r["name_filed"])
        if first_name != r["name_filed"]:
            continue                                       # same trade, other spelling of the member
        out.append(r)
    return out


def fill_party(rows: list) -> list:
    """Back-compat: party only, in place."""
    idx = _index()
    for r in rows:
        if not r.get("party"):
            r["party"] = party_for(r.get("chamber"), r.get("name"), r.get("district"), idx=idx)
    return rows
