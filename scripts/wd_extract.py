"""W5 stage A: one Wikidata full-history XML stream -> statement-level change records.

    7za x -so FILE.7z | python -I wd_extract.py --subset NODES.txt --out OUT.jsonl.gz

Reads MediaWiki XML on stdin (pages-meta-history, one revision text per line,
which holds for Wikibase JSON). Pages whose title is not an item in the subset
are skipped before any JSON parse (their revisions are only counted). For each
subset page, every revision's entity JSON is parsed with stdlib json and
reduced to the page's *temporal edge statements*:

    statement GUID -> (property, object Q-number, vt_s, vt_e)

kept only when the main snak is item-valued, the object is in the subset, the
rank is not deprecated, and at least one of P580 (start), P582 (end), P585
(point in time) gives a usable interval (see `interval`). A revision emits one
`chg` record per GUID whose reduced value differs from the previous revision's
(None = absent), plus one `rev` record with the revision metadata and its
revert classification:

    rollback  comment starts "Reverted edits by"   (MediaWiki rollback)
    undo      comment contains "/* undo:"          (Wikibase undo)
    restore   comment contains "/* restore:"       (Wikibase restore)
    manual    sha1 equals an earlier, non-parent revision's sha1, no marker above

Valid time is microseconds since 0000-01-01 (proleptic Gregorian, astronomical
year 0), so every date the subset keeps (years 0..9999, as TGB 2.0) is >= 0.
Calendar model (Julian vs Gregorian) is not converted. An end date is
inclusive at its precision (end 2010, precision year -> vt_e = 2011-01-01);
a point in time spans one unit of its precision.
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import time

OPEN = 2 ** 62
DAY_US = 86_400_000_000
TIME_RE = re.compile(r"^([+-])(\d+)-(\d\d)-(\d\d)T")


def days_from_civil(y: int, m: int, d: int) -> int:
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe


D0 = days_from_civil(0, 1, 1)


def us_of(y: int, m: int, d: int) -> int:
    return (days_from_civil(y, m, d) - D0) * DAY_US


def parse_time(snak):
    """-> (y, m, d, precision) or None for somevalue/novalue/out-of-range."""
    if snak.get("snaktype") != "value":
        return None
    v = snak.get("datavalue", {}).get("value")
    if not isinstance(v, dict):
        return None
    mt = TIME_RE.match(v.get("time", ""))
    if not mt:
        return None
    y = int(mt.group(2)) * (-1 if mt.group(1) == "-" else 1)
    if y < 0 or y > 9998:
        return None
    p = int(v.get("precision", 11))
    m, d = int(mt.group(3)), int(mt.group(4))
    if p <= 9:
        m, d, p = 1, 1, 9
    elif p == 10:
        m, d = max(m, 1), 1
    else:
        m, d, p = max(m, 1), max(d, 1), 11
    if m > 12 or d > 31:
        return None
    return y, m, d, p


def lo_hi(t):
    """Start and exclusive end of the unit containing t."""
    y, m, d, p = t
    s = us_of(y, m, d)
    if p == 9:
        e = us_of(y + 1, 1, 1)
    elif p == 10:
        e = us_of(y + (m == 12), 1 if m == 12 else m + 1, 1)
    else:
        e = s + DAY_US
    return s, e


def interval(quals):
    def first(pid):
        for q in quals.get(pid, ()):
            t = parse_time(q)
            if t is not None:
                return t
        return None
    st, en, pt = first("P580"), first("P582"), first("P585")
    if st is None and en is None and pt is None:
        return None
    if st is not None or en is not None:
        vs = lo_hi(st)[0] if st is not None else 0
        ve = lo_hi(en)[1] if en is not None else OPEN
        if ve <= vs:  # inconsistent dates: keep the start's unit
            ve = lo_hi(st)[1] if st is not None else vs + DAY_US
        return vs, ve
    return lo_hi(pt)


def reduce_entity(d, subset):
    out = {}
    claims = d.get("claims")
    if not isinstance(claims, dict):
        return out
    for prop, cl in claims.items():
        for c in cl:
            if c.get("rank") == "deprecated":
                continue
            ms = c.get("mainsnak", {})
            if ms.get("snaktype") != "value":
                continue
            dv = ms.get("datavalue", {})
            if dv.get("type") != "wikibase-entityid":
                continue
            v = dv.get("value", {})
            nid = v.get("numeric-id")
            if nid is None or v.get("entity-type", "item") != "item" or nid not in subset:
                continue
            iv = interval(c.get("qualifiers") or {})
            if iv is None:
                continue
            out[c.get("id")] = (prop, nid, iv[0], iv[1])
    return out


def unesc(s: str) -> str:
    if "&" not in s:
        return s
    return s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&#039;", "'").replace("&amp;", "&")


def tag_val(line: bytes, tag: bytes) -> str:
    i = line.find(b">") + 1
    j = line.rfind(b"</" + tag)
    return unesc(line[i:j].decode("utf-8", "replace"))


def classify(comment: str, sha1: str, parent_sha: str | None, seen: dict) -> str | None:
    if comment.startswith("Reverted edits by"):
        return "rollback"
    if "/* undo:" in comment:
        return "undo"
    if "/* restore:" in comment:
        return "restore"
    if sha1 and sha1 in seen and sha1 != parent_sha:
        return "manual"
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--subset", required=True, help="file with one Q-number per line")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    subset = {int(x) for x in open(a.subset) if x.strip()}
    out = gzip.open(a.out + ".tmp", "wt", encoding="utf-8", compresslevel=3)
    t0 = time.time()
    st = dict(pages=0, sub_pages=0, revs=0, sub_revs=0, parsed=0, bad_json=0, deleted_text=0, chg=0, nbytes=0)
    in_sub = False; title = None; page_id = None; in_rev = False; in_contrib = False
    rev = {}
    state = {}; seen_sha = {}; prev_sha = None; last_chg = {}
    for line in sys.stdin.buffer:
        st["nbytes"] += len(line)
        s = line.lstrip()
        if not s.startswith(b"<"):
            continue
        if in_rev:
            if s.startswith(b"<text"):
                if not in_sub:
                    continue
                if s.startswith(b"<text") and (b'deleted="deleted"' in s[:120] or s.rstrip().endswith(b"/>")):
                    rev["text"] = None
                else:
                    rev["text"] = s
                continue
            if s.startswith(b"</revision>"):
                in_rev = False
                if not in_sub:
                    continue
                st["sub_revs"] += 1
                emit_rev(out, rev, title, state, seen_sha, prev_sha, last_chg, subset, st)
                if rev.get("sha1"):
                    seen_sha.setdefault(rev["sha1"], rev["id"])
                    prev_sha = rev["sha1"]
                continue
            if not in_sub:
                continue
            if s.startswith(b"<contributor"):
                in_contrib = not s.rstrip().endswith(b"/>")
            elif s.startswith(b"</contributor"):
                in_contrib = False
            elif s.startswith(b"<id>"):
                if in_contrib:
                    rev["uid"] = int(tag_val(s, b"id"))
                elif "id" not in rev:
                    rev["id"] = int(tag_val(s, b"id"))
            elif s.startswith(b"<parentid>"):
                rev["parent"] = int(tag_val(s, b"parentid"))
            elif s.startswith(b"<timestamp>"):
                rev["ts"] = tag_val(s, b"timestamp")
            elif s.startswith(b"<username>"):
                rev["user"] = tag_val(s, b"username")
            elif s.startswith(b"<ip>"):
                rev["user"] = "ip:" + tag_val(s, b"ip")
            elif s.startswith(b"<comment>"):
                rev["comment"] = tag_val(s, b"comment")
            elif s.startswith(b"<sha1>"):
                rev["sha1"] = tag_val(s, b"sha1")
            elif s.startswith(b"<minor"):
                rev["minor"] = 1
            continue
        if s.startswith(b"<revision>"):
            in_rev = True; rev = {}; in_contrib = False
            st["revs"] += 1
        elif s.startswith(b"<page>"):
            st["pages"] += 1; title = None; page_id = None; in_sub = False
            state = {}; seen_sha = {}; prev_sha = None; last_chg = {}
        elif s.startswith(b"<title>"):
            title = tag_val(s, b"title")
            in_sub = len(title) > 1 and title[0] == "Q" and title[1:].isdigit() and int(title[1:]) in subset
            st["sub_pages"] += in_sub
        elif s.startswith(b"<id>") and page_id is None:
            page_id = int(tag_val(s, b"id"))
    out.close()
    import os
    os.replace(a.out + ".tmp", a.out)
    st["wall_s"] = round(time.time() - t0, 1)
    print(json.dumps(st))
    return 0


def emit_rev(out, rev, title, state, seen_sha, prev_sha, last_chg, subset, st):
    q = int(title[1:])
    ts = rev.get("ts", "")
    comment = rev.get("comment", "")
    kind = classify(comment, rev.get("sha1", ""), prev_sha, seen_sha)
    rec = {"t": "rev", "q": q, "id": rev.get("id"), "p": rev.get("parent"), "ts": ts, "u": rev.get("user", ""),
           "c": comment[:240], "sha1": rev.get("sha1", ""), "rv": kind}
    if kind == "manual" or kind in ("rollback", "undo", "restore"):
        rec["rv_to"] = seen_sha.get(rev.get("sha1", ""))
    txt = rev.get("text")
    if txt is None:
        st["deleted_text"] += 1
        rec["text"] = "missing"
        out.write(json.dumps(rec, separators=(",", ":")) + "\n")
        return
    raw = txt[txt.find(b">") + 1:].rstrip()
    if raw.endswith(b"</text>"):
        raw = raw[:-7]
    try:
        d = json.loads(unesc(raw.decode("utf-8")))
        st["parsed"] += 1
    except Exception:
        st["bad_json"] += 1
        rec["text"] = "bad_json"
        out.write(json.dumps(rec, separators=(",", ":")) + "\n")
        return
    if isinstance(d, dict) and "redirect" in d:
        rec["redirect"] = d.get("redirect")
        new = {}
    else:
        new = reduce_entity(d, subset) if isinstance(d, dict) else {}
    rec["n"] = len(new)
    out.write(json.dumps(rec, separators=(",", ":")) + "\n")
    for g in set(state) | set(new):
        b, n = state.get(g), new.get(g)
        if b != n:
            c = {"t": "chg", "q": q, "id": rev.get("id"), "ts": ts, "g": g, "b": b, "a": n}
            if kind:
                c["lag_from"] = last_chg.get(g)
            out.write(json.dumps(c, separators=(",", ":")) + "\n")
            last_chg[g] = ts
            st["chg"] += 1
    state.clear(); state.update(new)


if __name__ == "__main__":
    sys.exit(main())
