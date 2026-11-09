import json, collections, sys, os
d = sys.argv[1]
print(open(os.path.join(d, "verdicts.txt")).read().split("DIFFERS:")[0])
for v in ("reg", "bel"):
    rs = json.load(open(f"{d}/verify-native-{v}.json"))["results"]
    H = {(r["query"], r["instance"]): (r["hash"], r.get("proj_hash")) for r in rs if r.get("ok")}
    emp = collections.Counter(r["query"] for r in rs if r.get("ok") and not r.get("rows"))
    print(v, "native", len(rs), "ok", len(H), "empty answers", dict(emp))
    for s in ("duckdbsql", "postgres", "neo4j"):
        o = json.load(open(f"{d}/verify-{s}-{v}.json"))["results"]; c = collections.Counter(); ex = collections.Counter()
        for r in o:
            k = (r["query"], r["instance"])
            if not r.get("ok"): c["not ok"] += 1; ex[(k[0], str(r.get("error"))[:80])] += 1
            elif k not in H: c["no ref"] += 1
            elif r["hash"] == H[k][0]: c["same"] += 1
            elif r.get("proj_hash") and r["proj_hash"] == H[k][1]: c["same projected"] += 1
            else: c["DIFF"] += 1; ex[(k[0], "DIFF")] += 1
        print(" ", v, s, dict(c), dict(ex))
    walls = collections.defaultdict(list)
    for s in ("native", "duckdbsql", "postgres", "neo4j"):
        for r in json.load(open(f"{d}/verify-{s}-{v}.json"))["results"]:
            if r.get("wall_ms") is not None: walls[s].append(r["wall_ms"])
    print(" ", v, "verify wall s (sum, single call each):", {s: round(sum(x) / 1000, 1) for s, x in walls.items()})
for s in ("native", "duckdbsql", "postgres", "neo4j"):
    l = json.load(open(f"{d}/load-{s}.json")); l = l.get("load", l)
    print("load", s, l.get("load_s"), (l.get("counts") or {k: l.get("stats", {}).get(k) for k in ("n_edge_versions",)}))
for l in open(f"{d}/match-check.jsonl"):
    r = json.loads(l); print("match", r["id"], r["agree"], r["native"].get("rows"), r["native"].get("s"), r["reference"].get("s"))
