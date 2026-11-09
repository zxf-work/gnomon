#!/usr/bin/env python3
"""mem_table.py --res R --wl W --out mem.md: peak memory per system and phase from rss-samples.jsonl over marks.txt. A system's block runs from sys_start to sys_end. open: sys_start to the driver's first open_done, plus any later open_start to open_done (a worker restart). prime: prime_start to prime_done.
query: the rest of the block. A sample counts for a phase if it falls inside it, or is the first sample after a phase shorter than the interval. RSS sum, largest VmHWM and PSS sum of the system's category; for Gnomon and DuckDB the category is the client (python) processes, for a server its own processes, with the client printed beside it."""
import argparse
import json
from pathlib import Path

CAT = {"native": "client", "gnomon": "client", "duckdbsql": "client", "duckdb": "client", "postgres": "postgres", "neo4j": "neo4j", "memgraph": "memgraph"}
NAME = {"native": "Gnomon", "gnomon": "Gnomon", "duckdbsql": "DuckDB (SQL)", "duckdb": "DuckDB (SQL)", "postgres": "PostgreSQL", "neo4j": "Neo4j", "memgraph": "Memgraph"}


def main():
    ap = argparse.ArgumentParser()
    for k in ("res", "wl", "out"):
        ap.add_argument("--" + k, required=True)
    a = ap.parse_args()
    res = Path(a.res)
    hdr, samples = None, []
    for x in open(res / "rss-samples.jsonl"):
        r = json.loads(x)
        if r.get("header"):
            hdr = r
        else:
            samples.append(r)
    marks = []
    for x in open(res / "marks.txt"):
        p = x.split()
        marks.append((float(p[0]), p[2], p[3:]))
    blocks, cur = [], None
    for t, what, rest in marks:
        if what == "sys_start":
            cur = {"point": rest[0], "system": rest[1], "start": t, "end": None, "open": [], "prime": [], "ostart": t, "pstart": None, "first_open": False}
            blocks.append(cur)
        elif cur is None:
            continue
        elif what == "sys_end":
            cur["end"] = t
        elif what == "open_start":
            if cur["first_open"]:
                cur["ostart"] = t
        elif what == "open_done":
            cur["open"].append((cur["ostart"], t))
            cur["first_open"] = True
        elif what == "prime_start":
            cur["pstart"] = t
        elif what == "prime_done" and cur["pstart"]:
            cur["prime"].append((cur["pstart"], t))
    L = [f"# Peak memory by system and phase, {a.wl}", "", f"Sampler: 2 s, cgroup filter {hdr.get('cgroup_filter') if hdr else '?'}, host {hdr.get('host') if hdr else '?'}. GB = 2^30 bytes. RSS sum over the category's processes (a server's shared pages counted once per process); VmHWM is the largest single process's high-water mark so far, so it does not fall between phases; PSS is one sample in ten, shared pages counted once.", "",
         "| point | system | phase | duration s | samples | peak RSS GB | VmHWM in the phase GB | peak PSS GB | client RSS GB (servers) |", "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    gb = lambda kb: round(kb / 1048576, 2)
    for b in blocks:
        end = b["end"] or (samples[-1]["t"] if samples else b["start"])
        c = CAT.get(b["system"], "client")
        inside = [s for s in samples if b["start"] <= s["t"] <= end + 2.5]

        def in_iv(t, ivs):
            return any(x <= t <= y for x, y in ivs)
        opens = b["open"] or [(b["start"], end)]
        firsts = []
        for x, y in opens + b["prime"]:
            nxt = next((s["t"] for s in inside if s["t"] >= y), None)
            firsts.append(nxt)
        ph = {"open": [], "prime": [], "query": []}
        for s in inside:
            t = s["t"]
            if in_iv(t, opens):
                ph["open"].append(s)
            elif in_iv(t, b["prime"]):
                ph["prime"].append(s)
            else:
                ph["query"].append(s)
        for (x, y), nxt in zip(opens + b["prime"], firsts):
            name = "open" if (x, y) in opens else "prime"
            if nxt is not None and not any(x <= s["t"] <= y for s in ph[name]):
                ph[name] += [s for s in inside if s["t"] == nxt]
        dur = {"open": sum(y - x for x, y in opens), "prime": sum(y - x for x, y in b["prime"])}
        dur["query"] = max(0.0, end - b["start"] - dur["open"] - dur["prime"])
        for name in ("open", "prime", "query"):
            ss = ph[name]
            rss = [s["by"].get(c, {}).get("rss_kb", 0) for s in ss]
            hwm = [s["by"].get(c, {}).get("hwm_kb", 0) for s in ss]
            pss = [s["by"].get(c, {}).get("pss_kb", 0) for s in ss if s.get("pss")]
            cl = [s["by"].get("client", {}).get("rss_kb", 0) for s in ss]
            L.append(f"| {b['point']} | {NAME.get(b['system'], b['system'])} | {name} | {round(dur[name], 1)} | {len(ss)} | {gb(max(rss)) if rss else ''} | {gb(max(hwm)) if hwm else ''} | {gb(max(pss)) if pss else ''} | {gb(max(cl)) if (cl and c != 'client') else ''} |")
    L += ["", "Cells answered per system in this pass are in each block's record. What the sampler can miss: a peak between two samples for the RSS sum (VmHWM catches it for a process alive at the next sample); the VmHWM of a process that ends between samples (a worker killed at the ceiling); memory outside RSS (page cache, kernel); cells censored in the recorded run, which are not in this pass."]
    Path(a.out).write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
