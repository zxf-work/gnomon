#!/usr/bin/env python3
"""Out-of-order logs (coverage audit item 5): re-order the edge events of a frozen log, deterministically.

    ooo_shuffle.py write --in <log> --out <log> --mode lag|full|identity [--lag N] --seed S --meta <json>
    ooo_shuffle.py check --orig <log> --new <log> --meta <json>

What moves: only the events inside `ingest_events` ops. Each batch keeps its transaction time, its size and its position; every other op
(the node assert, corrections, retractions) stays byte for byte where it was. So belief time keeps the order the engine enforces (replay
refuses a falling tt), and what breaks is the order of valid time against arrival.

An event's discriminator is written out explicitly: without it the engine derives the discriminator from the position in the stream, and
a moved event would become a different edge. Identity, and so every later correction's reference, stays the same.
  full: a uniform permutation of the events.
  lag:  arrival key = position + U(0, N): each event arrives up to about N positions late; rough order is kept.
  identity: no move and no explicit discriminator; the output must equal the input byte for byte (a test of the writer).
--explicit-nodes (a test of the node rule): the first ingest op also carries a `nodes` record for every endpoint uid, vt_s = that uid's smallest valid start over the whole log, so no bare version depends on arrival. With mode identity it gives the original log in the explicit-node form.
batch_id is recomputed exactly as EventLog.append computes it, since replay checks it. Deterministic in (input, mode, lag, seed, numpy)."""
import argparse
import hashlib
import json
import sys

import numpy as np

from gnomon.core.model import canonical_json, sha256_hex


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def record(tt, ops):
    bid = sha256_hex(canonical_json({"tt": tt, "ops": ops}))[:16]
    return (canonical_json({"batch_id": bid, "tt": tt, "ops": ops}) + "\n").encode("utf-8")


def read(path):
    with open(path, "rb") as f:
        head = f.readline()
        return head, [json.loads(line) for line in f]


def ingest_ops(batches):
    for b in batches:
        for op in b["ops"]:
            if op.get("op") == "ingest_events" and op.get("events"):
                yield op


def order(n, mode, lag, seed):
    rng = np.random.default_rng(seed)
    if mode == "full":
        return rng.permutation(n)
    if mode == "lag":
        return np.argsort(np.arange(n, dtype=np.float64) + rng.uniform(0.0, float(lag), n), kind="stable")
    return np.arange(n)


def write(a):
    head, batches = read(a.inp)
    events, sizes = [], []
    for op in ingest_ops(batches):
        off = op.get("offset", 0)
        for i, ev in enumerate(op["events"]):
            if a.mode != "identity" and "disc" not in ev:
                ev["disc"] = f"#{off + i}"
            events.append(ev)
        sizes.append(len(op["events"]))
    n = len(events)
    perm = order(n, a.mode, a.lag, a.seed)
    disp = np.abs(perm - np.arange(n))
    vt = np.fromiter((events[int(i)]["vt_s"] for i in perm), dtype=np.int64, count=n)
    rank = np.argsort(np.argsort(vt, kind="stable"), kind="stable").astype(np.float64)
    rho = float(np.corrcoef(rank, np.arange(n, dtype=np.float64))[0, 1]) if n > 1 else 1.0
    pos = 0
    spans = []
    for op, m in zip(ingest_ops(batches), sizes):
        op["events"] = [events[int(i)] for i in perm[pos:pos + m]]
        spans.append(int(vt[pos:pos + m].max() - vt[pos:pos + m].min()))
        pos += m
    n_explicit = 0
    if a.explicit_nodes:
        low = {}
        for e in events:
            v = e["vt_s"]
            for u in (e["src"], e["dst"]):
                if u not in low or v < low[u]:
                    low[u] = v
        first = next(ingest_ops(batches))
        label = first.get("node_label", "Node")
        first["nodes"] = [{"label": label, "uid": u, "vt_s": low[u]} for u in sorted(low)]
        n_explicit = len(low)
    with open(a.out, "wb") as f:
        f.write(head)
        for b in batches:
            f.write(record(b["tt"], b["ops"]))
    meta = {"tool": "ooo_shuffle.py", "mode": a.mode, "lag": a.lag, "seed": a.seed, "numpy": np.__version__,
            "input": a.inp, "input_sha256": sha256(a.inp), "output": a.out, "output_sha256": sha256(a.out),
            "batches": len(batches), "ingest_batches": len(sizes), "events": n, "explicit_nodes": n_explicit,
            "max_displacement": int(disp.max()), "mean_displacement": float(disp.mean()),
            "spearman_valid_start_vs_arrival": rho, "median_vt_span_of_a_batch": float(np.median(spans))}
    with open(a.meta, "w") as f:
        json.dump(meta, f, indent=1)
    print(json.dumps(meta, indent=1))
    return 0


def check(a):
    meta = json.load(open(a.meta))
    bad = []
    ho, bo = read(a.orig)
    hn, bn = read(a.new)
    if ho != hn:
        bad.append("header differs")
    if len(bo) != len(bn):
        bad.append(f"batch count {len(bo)} against {len(bn)}")
    ev_o, ev_n = {}, {}
    for x, y in zip(bo, bn):
        if x["tt"] != y["tt"]:
            bad.append(f"tt differs at {x['tt']}")
        if y["batch_id"] != sha256_hex(canonical_json({"tt": y["tt"], "ops": y["ops"]}))[:16]:
            bad.append(f"batch_id wrong at tt {y['tt']}")
        if len(x["ops"]) != len(y["ops"]):
            bad.append(f"op count differs at tt {x['tt']}")
        for p, q in zip(x["ops"], y["ops"]):
            if p.get("op") != "ingest_events":
                if p != q:
                    bad.append(f"a non-ingest op changed at tt {x['tt']}")
                continue
            if len(p["events"]) != len(q["events"]):
                bad.append(f"batch size differs at tt {x['tt']}")
            off = p.get("offset", 0)
            for i, e in enumerate(p["events"]):
                d = e.get("disc", f"#{off + i}")
                ev_o[d] = (e["src"], e["dst"], e["rel_type"], e["vt_s"], e.get("vt_e"), json.dumps(e.get("props"), sort_keys=True))
            for i, e in enumerate(q["events"]):
                if "disc" not in e and meta["mode"] != "identity":
                    bad.append(f"event without a discriminator at tt {y['tt']}")
                    break
                d = e.get("disc", f"#{off + i}")
                ev_n[d] = (e["src"], e["dst"], e["rel_type"], e["vt_s"], e.get("vt_e"), json.dumps(e.get("props"), sort_keys=True))
    if len(ev_o) != meta["events"] or len(ev_n) != meta["events"] or ev_o != ev_n:
        bad.append(f"the event sets differ: {len(ev_o)} against {len(ev_n)}, expected {meta['events']}")
    if sha256(a.new) != meta["output_sha256"]:
        bad.append("output sha256 is not the recorded one")
    if meta.get("explicit_nodes"):
        low = {}
        for x in ev_n.values():
            for u in (x[0], x[1]):
                if u not in low or x[3] < low[u]:
                    low[u] = x[3]
        first = next(ingest_ops(bn))
        got = {r["uid"]: r["vt_s"] for r in first.get("nodes", [])}
        if got != low:
            bad.append(f"the nodes record is not the per-uid minimum: {len(got)} records, {len(low)} uids")
        if sum(1 for b in bn for op in b["ops"] if op.get("nodes")) != 1:
            bad.append("nodes records outside the first ingest op")
    if meta["mode"] == "identity" and not meta.get("explicit_nodes") and sha256(a.orig) != sha256(a.new):
        bad.append("identity is not byte-identical")
    if meta["mode"] == "lag" and meta["max_displacement"] > 2 * meta["lag"]:
        bad.append(f"displacement {meta['max_displacement']} above 2 x lag {meta['lag']}")
    print("CHECK", "FAILED: " + "; ".join(bad[:10]) if bad else "ok", a.new)
    return 1 if bad else 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write")
    w.add_argument("--in", dest="inp", required=True)
    w.add_argument("--out", required=True)
    w.add_argument("--mode", choices=["lag", "full", "identity"], required=True)
    w.add_argument("--lag", type=int, default=0)
    w.add_argument("--seed", type=int, required=True)
    w.add_argument("--meta", required=True)
    w.add_argument("--explicit-nodes", action="store_true")
    c = sub.add_parser("check")
    c.add_argument("--orig", required=True)
    c.add_argument("--new", required=True)
    c.add_argument("--meta", required=True)
    a = ap.parse_args()
    return write(a) if a.cmd == "write" else check(a)


if __name__ == "__main__":
    sys.exit(main())
