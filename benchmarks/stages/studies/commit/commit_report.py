#!/usr/bin/env python3
"""Durable commit cost: the table, from the job's records. commit_report.py --dir D --out report.md. One pass per configuration on one node; descriptive, no verdict."""
import argparse
import json
import os
import re


def j(d, n):
    p = os.path.join(d, n)
    return json.load(open(p)) if os.path.exists(p) else None


def fs(d, n):
    p = os.path.join(d, n)
    if not os.path.exists(p):
        return None
    for line in open(p):
        x = line.split()
        if x and x[-1] == "total":     # % time, seconds, usecs/call, calls, [errors], total
            return int(x[3])
    return None


def verdict(c):
    if not c:
        return "not run"
    v = c.get("summary", {})
    ok = sum(1 for x in v.values() if x == "agrees")
    na = sum(1 for x in v.values() if x == "not expressible")
    bad = {k: x for k, x in v.items() if x not in ("agrees", "not expressible")}
    return f"{ok} of {len(v) - na} agree" + (f"; {bad}" if bad else "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    d = a.dir
    L = ["# Durable commit cost on C4: T-mgBench write stream, per operation", "",
         "Per transaction, client side, whole transaction (statements and commit). `total` includes everything the load does in its history phase; Gnomon's commit figures leave out compaction, which runs between transactions and is in `total`. One pass per configuration, no strace in these runs. Descriptive.", "",
         "| system | configuration | transactions | total s | total per tx, ms | median ms | p95 ms | p99 ms | fsyncs | fsyncs per tx (pass) | store bytes | answers against the oracle | digest |",
         "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|"]
    rep = j(d, "load-gnomon-replay.json") or {}
    dg0 = (rep.get("content_digest") or {}).get("digest")
    tags = ["off"] + sorted([f[len("load-gnomon-live-n"):-5] for f in os.listdir(d) if f.startswith("load-gnomon-live-n")], key=int)
    for t in tags:
        tag = "off" if t == "off" else "n" + t
        r = j(d, f"load-gnomon-live-{tag}.json")
        if not r:
            continue
        c = r.get("commit_ms", {})
        f = fs(d, f"strace-gnomon-{tag}.txt")
        s = j(d, f"load-gnomon-strace-{tag}.json") or {}
        tx = r.get("transactions")
        dg = (r.get("content_digest") or {}).get("digest")
        L.append(f"| Gnomon | {'one generation per commit' if t == 'off' else 'small commit N = ' + t} | {tx} | {r['load_s']:.0f} | {1e3 * r['load_s'] / tx:.3g} | {c.get('median')} | {c.get('p95')} | {c.get('p99')} | {f} | {f / s['transactions']:.3g} | {r.get('store_bytes')} | digest against replay | {'same as replay' if dg and dg == dg0 else dg} |" if f is not None and s.get("transactions") else
                 f"| Gnomon | {'one generation per commit' if t == 'off' else 'small commit N = ' + t} | {tx} | {r['load_s']:.0f} | {1e3 * r['load_s'] / tx:.3g} | {c.get('median')} | {c.get('p95')} | {c.get('p99')} | - | - | {r.get('store_bytes')} | digest against replay | {'same as replay' if dg and dg == dg0 else dg} |")
    for name, lf, sf, slf, cf, cfg in (("AeonG", "load-aeong-native.json", "strace-aeong-native.txt", "load-aeong-native-strace.json", "check-aeong-native.json", "native, durable (WAL fsync per commit)"),
                                       ("Neo4j", "load-neo4j.json", "strace-neo4j-small.txt", "load-neo4j-small-strace.json", "check-neo4j.json", "defaults, log forced at commit; fsyncs on the small log"),
                                       ("DuckDB (SQL)", "load-duckdbsql.json", "strace-duckdbsql-small.txt", "load-duckdbsql-small-strace.json", "check-duckdbsql.json", "file database, WAL, 16 threads; fsyncs on the small log")):
        r = j(d, lf)
        if not r or "load" not in r:
            continue
        l = r["load"]
        ph = l.get("phases", {})
        hist = ph.get("history_s")
        tx = l.get("history_transactions") or l.get("tx_ms", {}).get("n") or l.get("commits")
        c = l.get("tx_ms", {})
        f = fs(d, sf)
        sl = (j(d, slf) or {}).get("load", {})
        stx = sl.get("commits") or sl.get("history_transactions")
        L.append(f"| {name} | {cfg} | {tx} | {hist:.0f} | {1e3 * hist / tx:.3g} | {c.get('median')} | {c.get('p95')} | {c.get('p99')} | {f if f is not None else '-'} | {f / stx:.3g} |" if f and stx else
                 f"| {name} | {cfg} | {tx} | {hist:.0f} | {1e3 * hist / tx:.3g} | {c.get('median')} | {c.get('p95')} | {c.get('p99')} | - | - |")
        sp = j(d, {"AeonG": "space-aeong-native.json", "Neo4j": "space-neo4j.json"}.get(name, "-")) or {}
        L[-1] += f" {sp or l.get('db_bytes') or '-'} | {verdict(j(d, cf))} | - |"
    L += ["", f"Replay on the same log (no log write): load {rep.get('load_s')} s, digest {dg0}, answers: {verdict(j(d, 'check-gnomon.json'))}.", ""]
    with open(a.out, "w") as f:
        f.write("\n".join(L) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
