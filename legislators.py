"""
Party for congressional trade rows. The House and Senate disclosure feeds carry no party, so every row had a
blank one. This looks members up in the public congress-legislators dataset (current members), cached for a day:
House members by state + district (the feed's "OH02"), senators by state + surname, and anyone else by
first initial + surname. Unknown names stay blank rather than guessed.
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


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", " ", s).split()


def _surname(words: list) -> str:
    words = [w for w in words if w not in ("jr", "sr", "ii", "iii", "iv", "senator", "hon", "dr", "mr", "mrs", "ms")]
    return words[-1] if words else ""


def build_index(members: list) -> dict:
    by_seat, by_sen, by_name = {}, {}, {}
    for m in members:
        t = (m.get("terms") or [{}])[-1]
        p = PARTY.get(t.get("party"), (t.get("party") or "")[:1])
        nm = m.get("name") or {}
        last = _surname(_fold(nm.get("last", "")))
        firsts = {w[:1] for w in _fold(" ".join(filter(None, [nm.get("first"), nm.get("nickname"), nm.get("middle")])))}
        if t.get("type") == "rep" and t.get("state") is not None:
            by_seat[f"{t['state']}{int(t.get('district') or 0):02d}"] = p
        if t.get("type") == "sen":
            by_sen.setdefault((t.get("state"), last), p)
        for f in firsts:
            by_name.setdefault((f, last), set()).add(p)
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


def party_for(chamber: str, name: str, district: str, idx: dict | None = None) -> str:
    idx = idx if idx is not None else _index()
    raw = name or ""
    if "," in raw:                                     # "Armstrong, Alan (Senator)"
        last_part, first_part = raw.split(",", 1)
        raw = f"{first_part} {last_part}"
    raw = re.sub(r"\(.*?\)", " ", raw)
    words = _fold(raw)
    last = _surname(words)
    d = (district or "").strip().upper()
    if chamber == "House" and re.fullmatch(r"[A-Z]{2}\d{1,2}", d):
        p = idx["seat"].get(f"{d[:2]}{int(d[2:]):02d}")
        if p:
            return p
    if chamber == "Senate" and re.fullmatch(r"[A-Z]{2}", d):
        p = idx["sen"].get((d, last))
        if p:
            return p
    if words:
        ps = idx["name"].get((words[0][:1], last))
        if ps and len(ps) == 1:
            return next(iter(ps))
    return ""


def fill_party(rows: list) -> list:
    """Fill a blank 'party' on API rows in place (rows that already have one are left alone)."""
    if not any(not r.get("party") for r in rows):
        return rows
    idx = _index()
    cache = {}
    for r in rows:
        if r.get("party"):
            continue
        k = (r.get("chamber"), r.get("name"), r.get("district"))
        if k not in cache:
            cache[k] = party_for(*k, idx=idx)
        r["party"] = cache[k]
    return rows
