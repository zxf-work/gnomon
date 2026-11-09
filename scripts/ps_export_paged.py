#!/usr/bin/env python3
"""Paged version-row export of a replayed Gnomon store: the W3 and W4 exports at
SF10 without the whole-store Python list.

    ps_export_paged.py w3 --repo REPO --store S --out ROWS [--log L]        nv.parquet, ev.parquet, export.json
    ps_export_paged.py w4 --repo REPO --store S --out DIR  [--log L]        node_v / edge_v parquet, export.json,
                                                                            commit_tts.json

**Why.** `w3_snb_sql.py export` and `w4_check.py export` read `adapter.all_node_versions()` and
`all_edge_versions()`, which convert the engine's whole version table into one Python list of NodeVersion /
EdgeVersion objects. At SF10 that exceeds a shared node: W4 SF10 rho = 0 was OOM-killed at 46 GB;
W3 SF10 holds 38 M node and about 230 M edge versions. The engine's own `all_versions_paged(kind, chunk)` yields
the same objects (the same `_nodes` / `_edges` converters) a chunk at a time; only the row order (the engine's
walk order) may differ, and no consumer of the rows depends on it (the history tables and the W4 loads and
parameters are order-independent).

w4-finbench/repo): its `gnomon` and `scripts/eval_harness.py` are imported, not this worktree's. With --log and no
store yet, the store is replayed first with `eval_harness.load_store`, exactly as the two exports do. The columns
and conversions are those of the two exports (canonical_json props for W3; the typed tables of w4_check for W4).
Validated on the persisted SF1 stores against the SF1 exports (equal multisets of rows) before any SF10 use.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

CHUNK = 1_000_000
PAGE = 4_000_000
OPEN = 2 ** 62


class _V:
    """One version row of a keyed page (the attribute names of NodeVersion / EdgeVersion that the exports read)."""
    __slots__ = ("vid", "eid", "uid", "label", "src", "dst", "rel_type", "disc", "vt_s", "vt_e", "tt_s", "tt_e", "props")


def pages(ad, kind: str, mode: str):
    """The store's version rows of one kind, a page at a time.
    mode "paged": `all_versions_paged` (the engine caches the whole version table of the kind between pages:
    about 640 bytes per W3 edge version, 147 GB at SF10). mode "keys": `versions_page` over every belief and the
    whole valid-time line (the engine holds 32-byte keys of the population, not the rows), and the page's props from
    `props_for_vids`; the same rows, in (tt_s, vid) order."""
    if mode == "paged":
        yield from ad.all_versions_paged(kind, CHUNK)
        return
    off = 0
    while True:
        cols, total = ad.versions_page(kind, as_of=OPEN, t_a=-(2 ** 63), t_b=2 ** 63 - 1, belief="all",
                                       offset=off, limit=PAGE)
        n = len(cols["vid"])
        if n == 0:
            return
        props = ad.props_for_vids(kind, list(cols["vid"]))
        out = []
        for i in range(n):
            v = _V()
            for c, x in cols.items():
                setattr(v, c, x[i])
            if kind == "edge":
                v.disc = v.disc or ""          # as the engine's _edges converter does
            for c in ("vt_s", "vt_e", "tt_s", "tt_e"):
                setattr(v, c, int(getattr(v, c)))
            v.props = props[v.vid]
            out.append(v)
        yield out
        off += n
        if off >= total:
            return


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kind", choices=["w3", "w4"])
    ap.add_argument("--repo", type=Path, required=True)
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--log", type=Path)
    ap.add_argument("--mode", choices=["keys", "paged"], default="keys",
                    help="keys: versions_page + props_for_vids (bounded); paged: all_versions_paged (whole-kind cache)")
    ap.add_argument("--kinds", default="node,edge", help="w3: export only these kinds (one process per kind keeps the "
                    "peak to one kind's engine-side cache; export.json is then export-<kind>.json)")
    a = ap.parse_args()
    sys.path.insert(0, str(a.repo))
    sys.path.insert(0, str(a.repo / "scripts"))
    import gnomon
    a.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rec = {"store": str(a.store), "repo": str(a.repo), "gnomon": gnomon.__file__, "mode": a.mode,
           "page_rows": PAGE if a.mode == "keys" else CHUNK}
    if a.log is not None and not (a.store / "eventlog.jsonl").exists():
        import eval_harness as H
        H.load_store(a.store, "native", a.log)
        rec["replay_s"] = round(time.time() - t0, 1)
    if a.kind == "w3":
        export_w3(a, rec, t0)
    else:
        export_w4(a, rec, t0)
    return 0


def export_w3(a, rec, t0) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import gnomon
    from gnomon.core.model import canonical_json
    st = gnomon.open(a.store, backend="native", read_only=True)
    ad = st.adapter

    def dump(kind, cols, fn, path):
        w, n = None, 0
        for page in pages(ad, kind, a.mode):
            buf = {c: [] for c in cols}
            for v in page:
                for c, x in zip(cols, fn(v)):
                    buf[c].append(x)
            n += len(page)
            t = pa.table(buf)
            w = w or pq.ParquetWriter(path, t.schema)
            w.write_table(t)
        if w is None:
            t = pa.table({c: [] for c in cols}); w = pq.ParquetWriter(path, t.schema); w.write_table(t)
        w.close()
        return n
    kinds = a.kinds.split(",")
    nn = ne = None
    if "node" in kinds:
        nn = dump("node", ["uid", "label", "props", "vt_s", "vt_e", "tt_s", "tt_e", "vid"],
              lambda v: (int(v.uid), v.label, canonical_json(v.props), v.vt_s, v.vt_e, v.tt_s, v.tt_e, v.vid),
              str(a.out / "nv.parquet"))
    if "edge" in kinds:
        ne = dump("edge", ["src", "dst", "rel", "disc", "props", "vt_s", "vt_e", "tt_s", "tt_e", "vid"],
              lambda v: (int(v.src), int(v.dst), v.rel_type, v.disc, canonical_json(v.props), v.vt_s, v.vt_e,
                         v.tt_s, v.tt_e, v.vid), str(a.out / "ev.parquet"))
    st.close()
    rec.update(node_versions=nn, edge_versions=ne, wall_s=round(time.time() - t0, 1))
    name = "export.json" if len(kinds) == 2 else f"export-{kinds[0]}.json"
    (a.out / name).write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps(rec))


def export_w4(a, rec, t0) -> None:
    import duckdb
    import pandas as pd
    import gnomon
    from w4_check import DDL_EDGE, DDL_NODE, EDGE_COLS, NODE_COLS
    st = gnomon.open(a.store)
    ad = st.adapter
    con = duckdb.connect()
    con.execute(DDL_NODE)
    con.execute(DDL_EDGE)
    n = m = 0
    for page in pages(ad, "node", a.mode):
        rows = []
        for v in page:
            p = v.props
            rows.append((v.uid, v.label, v.vt_s, v.vt_e, v.tt_s, v.tt_e, p.get("isBlocked"), p.get("accountType"),
                         p.get("createTime"), p.get("mediumType"), p.get("loanAmount"), p.get("balance")))
        df = pd.DataFrame(rows, columns=NODE_COLS, dtype=object)  # noqa: F841
        con.execute("INSERT INTO node_v SELECT * FROM df")
        n += len(rows)
    for page in pages(ad, "edge", a.mode):
        rows = [(v.src, v.dst, v.rel_type, v.disc, v.vt_s, v.vt_e, v.tt_s, v.tt_e, v.props.get("amount")) for v in page]
        df = pd.DataFrame(rows, columns=EDGE_COLS, dtype=object)  # noqa: F841
        con.execute("INSERT INTO edge_v SELECT * FROM df")
        m += len(rows)
    st.close()
    con.execute(f"COPY node_v TO '{a.out}/node_v.parquet' (FORMAT parquet)")
    con.execute(f"COPY edge_v TO '{a.out}/edge_v.parquet' (FORMAT parquet)")
    tts = [r[0] for r in con.execute("SELECT DISTINCT tt_s FROM edge_v UNION SELECT DISTINCT tt_s FROM node_v "
                                     "ORDER BY 1").fetchall()]
    rec.update(node_versions=n, edge_versions=m, export_s=round(time.time() - t0, 1), commit_tts=len(tts),
               tt_first=tts[0], tt_last=tts[-1])
    (a.out / "export.json").write_text(json.dumps(rec, indent=1))
    (a.out / "commit_tts.json").write_text(json.dumps(tts))
    print(json.dumps(rec))


if __name__ == "__main__":
    raise SystemExit(main())
