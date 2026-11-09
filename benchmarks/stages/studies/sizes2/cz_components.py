#!/usr/bin/env python3
"""cz_components.py <store root> <out.json> [--md out.md]: on-disk components of a Gnomon store, read only (opens files for reading, never the engine).

Every file under the store root is counted once. A segment file (.tgs, crates/gnomon-core/src/segment.rs: magic "TGSG" | format u32 |
header_len u32 | header JSON | pad to 64 | column extents, each padded to 64 | string table | footer JSON | footer_len u32 | "TGSE") is split
into: preamble, header JSON (and, inside it, the bytes each top-level field takes when re-serialized compactly: the per-segment run tables,
block maxima and bounds that serve as the segment's zone maps), alignment padding, each column extent by column name, the string table,
the footer. Other files are grouped by their directory under the store root and their extension. Sums are checked against the file sizes.
"""
import json
import os
import struct
import sys
from collections import defaultdict
from pathlib import Path


def seg(p: Path):
    b = p.read_bytes()
    n = len(b)
    assert b[:4] == b"TGSG", f"{p}: no segment magic"
    fmt, hl = struct.unpack_from("<II", b, 4)
    hj = b[12:12 + hl]
    h = json.loads(hj)
    data_start = (12 + hl + 63) // 64 * 64
    assert b[-4:] == b"TGSE", f"{p}: no complete marker"
    fl = struct.unpack_from("<I", b, n - 8)[0]
    footer = fl + 8
    parts = {"preamble": 12, "header JSON": hl, "pad after header": data_start - 12 - hl}
    cols = {}
    end = data_start
    for c in h.get("columns", []):
        cols[c["name"]] = cols.get(c["name"], 0) + int(c["bytes"])
        end = max(end, data_start + int(c["offset"]) + int(c["bytes"]))
    so, sb = int(h.get("strings_offset", 0)), int(h.get("strings_bytes", 0))
    s_abs = data_start + so
    parts["string table"] = sb
    parts["footer"] = footer
    accounted = sum(parts.values()) + sum(cols.values())
    parts["column padding and gaps"] = n - accounted
    fields = {k: len(json.dumps(v, separators=(",", ":"))) for k, v in h.items()}
    return {"bytes": n, "format": fmt, "lane": str(h.get("lane")), "kind": str(h.get("kind")), "rows": h.get("rows"),
            "parts": parts, "columns": cols, "header_fields": fields}


def main():
    root, out = Path(sys.argv[1]), Path(sys.argv[2])
    md = Path(sys.argv[sys.argv.index("--md") + 1]) if "--md" in sys.argv else None
    total = 0
    other = defaultdict(int)
    nseg = 0
    by_lane = defaultdict(lambda: {"segments": 0, "rows": 0, "bytes": 0, "parts": defaultdict(int), "columns": defaultdict(int), "header_fields": defaultdict(int)})
    for dp, dn, fn in os.walk(root):
        for f in fn:
            p = Path(dp) / f
            sz = p.stat().st_size
            total += sz
            if f.endswith(".tgs"):
                s = seg(p); nseg += 1
                L = by_lane[f"{s['kind']}/{s['lane']}"]
                L["segments"] += 1; L["rows"] += s["rows"] or 0; L["bytes"] += s["bytes"]
                for k, v in s["parts"].items():
                    L["parts"][k] += v
                for k, v in s["columns"].items():
                    L["columns"][k] += v
                for k, v in s["header_fields"].items():
                    L["header_fields"][k] += v
            else:
                rel = p.relative_to(root)
                d = str(rel.parent) if str(rel.parent) != "." else "(root)"
                ext = p.suffix or "(none)"
                other[f"{d} *{ext}"] += sz
    seg_total = sum(L["bytes"] for L in by_lane.values())
    assert seg_total + sum(other.values()) == total
    doc = {"store": str(root), "total_bytes": total, "segments": nseg, "segment_bytes": seg_total,
           "by_lane": {k: {**v, "parts": dict(v["parts"]), "columns": dict(v["columns"]), "header_fields": dict(v["header_fields"])} for k, v in by_lane.items()},
           "other_files": dict(sorted(other.items(), key=lambda x: -x[1]))}
    out.write_text(json.dumps(doc, indent=1) + "\n")
    if md:
        mb = lambda x: f"{x / 1e6:,.1f}"
        pct = lambda x: f"{100 * x / total:.1f}"
        L = [f"# Components of {root}", "", f"Total {mb(total)} MB (10^6 bytes) in {sum(1 for _ in root.rglob('*') if _.is_file())} files; {nseg} segment files, {mb(seg_total)} MB.", "",
             "| component | MB | % of store |", "|---|---:|---:|"]
        rows = []
        for lane, v in sorted(by_lane.items()):
            for k, x in sorted(v["columns"].items(), key=lambda t: -t[1]):
                rows.append((f"segments {lane}: column {k}", x))
            for k, x in v["parts"].items():
                rows.append((f"segments {lane}: {k}", x))
        for k, x in other.items():
            rows.append((f"file {k}", x))
        for name, x in sorted(rows, key=lambda t: -t[1]):
            L.append(f"| {name} | {mb(x)} | {pct(x)} |")
        L += ["", "Header JSON by top-level field (re-serialized compactly; sums can differ from the stored header by whitespace):", "",
              "| lane | field | MB |", "|---|---|---:|"]
        for lane, v in sorted(by_lane.items()):
            for k, x in sorted(v["header_fields"].items(), key=lambda t: -t[1])[:12]:
                L.append(f"| {lane} | {k} | {mb(x)} |")
        L += ["", "| lane | segments | rows | MB |", "|---|---:|---:|---:|"] + [f"| {k} | {v['segments']} | {v['rows']:,} | {mb(v['bytes'])} |" for k, v in sorted(by_lane.items())]
        md.write_text("\n".join(L) + "\n")
    print(json.dumps({"total_bytes": total, "segments": nseg, "lanes": {k: v["bytes"] for k, v in by_lane.items()}, "other": dict(list(doc["other_files"].items())[:10])}))


if __name__ == "__main__":
    main()
