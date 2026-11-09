#!/usr/bin/env python3
"""w4_store_compare.py --a DIR --b DIR --out PREFIX: two stores built by replay of one frozen log. Prints and writes: the store digest of each (Store.digest_streaming), stats,
and every file by relative path, size and sha256: identical, differing, or only on one side. Exit 0 always when both ran; the verdict is in the record."""
import argparse
import hashlib
import json
import os
import sys


def manifest(root):
    out = {}
    for d, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(d, f)
            h = hashlib.sha256()
            with open(p, "rb") as fh:
                for b in iter(lambda: fh.read(1 << 22), b""):
                    h.update(b)
            out[os.path.relpath(p, root)] = (os.path.getsize(p), h.hexdigest())
    return out


def compare(a, b):
    same, differ, only_a, only_b = [], [], [], []
    for k in sorted(set(a) | set(b)):
        if k not in b:
            only_a.append((k, a[k][0]))
        elif k not in a:
            only_b.append((k, b[k][0]))
        elif a[k] == b[k]:
            same.append(k)
        else:
            differ.append((k, a[k][0], b[k][0]))
    # content present on both sides under other names (a run-specific file name would show here)
    ca, cb = {v[1] for v in a.values()}, {v[1] for v in b.values()}
    return {"files_a": len(a), "files_b": len(b), "bytes_a": sum(v[0] for v in a.values()), "bytes_b": sum(v[0] for v in b.values()),
            "identical": len(same), "differ": differ, "only_a": only_a, "only_b": only_b,
            "contents_only_a": len(ca - cb), "contents_only_b": len(cb - ca)}


def digest(path):
    sys.path.insert(0, os.environ.get("EXTCMP_REPO", "."))
    import gnomon
    s = gnomon.open(path, backend="native", read_only=True)
    st = s.stats()
    d = s.digest_streaming()
    s.close()
    return d, {k: st.get(k) for k in ("n_entities", "n_node_versions", "n_edge_versions")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    ap.add_argument("--a-name", default="a")
    ap.add_argument("--b-name", default="b")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    r = {"a": a.a_name, "b": a.b_name}
    r["files"] = compare(manifest(a.a), manifest(a.b))
    print("files", {k: (len(v) if isinstance(v, list) else v) for k, v in r["files"].items()}, flush=True)
    json.dump(r, open(a.out + ".json", "w"), indent=1)
    da, sa = digest(a.a)
    db, sb = digest(a.b)
    r.update(digest_a=da, digest_b=db, stats_a=sa, stats_b=sb, digest_equal=da == db)
    print(a.a_name, da, sa, flush=True)
    print(a.b_name, db, sb, flush=True)
    print("DIGEST", "IDENTICAL" if da == db else "DIFFERENT", flush=True)
    json.dump(r, open(a.out + ".json", "w"), indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
