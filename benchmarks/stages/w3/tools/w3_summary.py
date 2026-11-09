#!/usr/bin/env python3
"""W3 timed stage: summaries and scoring of the timed records (end-to-end time primary).

    w3_summary.py --res <w3/results> --gate <w3/gate> --params <params dir> --sacct <sacct.psv> --out <dir>

Inputs (relative to --res; all read-only):
  Gate  gate-*/ , rerun-*/ , gateext-*/ (the dirs of the verdicts) re-read with tools/w3_gate.py's own classifier to give each
        excluded cell its reason; checked against <gate>/exclusions-<point>.json.
Rules:
  * ONE rotation: an instance value is the median of that rotation's timed calls (p50_ms, e2e); there is no rotation mean;
  * DuckDB (SQL) = the better of its 64-thread and 32-thread configurations per instance;
  * censored cells ("> 10 s") count as Gnomon faster when Gnomon answered; unreached, error, refused, not-expressible,
    differing and gate-excluded cells of the other system are not compared;
  * a Gnomon censored, refused or error cell where the other system answered is a LOSS for Gnomon (listed);
  * ratio = Gnomon / other (below 1 = Gnomon faster); a cell's ratio = median over the instances both answered of Gnomon's
    value / median of the other's value over the same instances ("paired"); the best baseline of a cell is the competitor with
    the largest such ratio (the fastest one);
  * Gnomon refusals (CostError) are "refused (estimated > 10 s)"; StateError cells the runner did not mark censored are errors.
Writes tables.md, scoring.md, summary.json.
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import math
import re
import statistics as st
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import w3_gate as G  # noqa: E402  (the gate's own loader/classifier, to re-derive exclusion reasons per cell)

OK, CENS, REF, ERR, NA = "ok", "cens", "refused", "error", "na"
UNR, GUNR, DIFF, XERR, MISS = "unreached", "gunreached", "differs", "xerror", "missing"
NAME = {"gnomon": "Gnomon", "duckdbsql": "DuckDB (SQL)", "duckdb64": "DuckDB (SQL) 64 thr", "duckdb32": "DuckDB (SQL) 32 thr",
        "postgres": "PostgreSQL", "neo4j": "Neo4j", "memgraph": "Memgraph", "ladybug": "LadybugDB",
        "aeong": "AeonG (native TT)", "aeong-prop": "AeonG (property form)"}
COMP = ["duckdbsql", "postgres", "neo4j", "memgraph", "ladybug"]
SYS_SF1 = ["gnomon", "duckdbsql", "duckdb64", "duckdb32", "postgres", "neo4j", "memgraph", "ladybug"]
SYS_SF10 = ["gnomon", "duckdbsql", "duckdb64", "duckdb32", "postgres", "memgraph", "ladybug"]
TOKENS = ["gnomon", "duckdb64", "duckdb32", "postgres", "neo4j", "memgraph", "ladybug"]
GATE_NAME = {"duckdb64": "duckdb", "duckdb32": "duckdb", "duckdbsql": "duckdb"}
SETTINGS = ["current", "asof", "window"]
REASON_CLS = {"unreached": UNR, "Gnomon cell unreached": GUNR, "notexpr": NA, "error": XERR, "refused": REF,
              "differs from Gnomon": DIFF}
SUBSET12 = ["IS1", "IS2", "IC2", "IC6", "IC9", "IC13", "BI2", "BI8", "BI14", "BIH-VER", "BIH-R3", "TLDBC-IS3"]
CEIL_MS = 10000.0


def fam(q):
    if q.startswith("TLDBC"):
        return "T-LDBC"
    if q.startswith("BIH"):
        return "BIH"
    return q.rstrip("0123456789")


FAMS = ["IS", "T-LDBC", "IC", "BI", "BIH"]


def qkey(q):
    m = re.match(r"([A-Za-z\-]+?)(\d+)$", q)
    return (FAMS.index(fam(q)), int(m.group(2)) if m else 0, q)


def num(v):
    return isinstance(v, (int, float))


def fmt(v):
    if v is None:
        return "–"
    if not num(v):
        return str(v)
    if abs(v) < 100:
        return f"{v:.3g}"
    return f"{v:,.0f}"


def gm(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def sig2(x):
    if x is None or x <= 0:
        return None
    return round(x, -math.floor(math.log10(x)) + 1)


def med(xs):
    return st.median(xs) if xs else None


def md_table(head, rows):
    return ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)] + ["| " + " | ".join(map(str, r)) + " |" for r in rows]


# ---------------------------------------------------------------------------------------------------------------- loading
def cell_class(c):
    if c.get("ok"):
        return OK
    e = str(c.get("error", ""))
    if "CostError" in e:
        return REF
    if c.get("not_expressible"):
        return NA
    if "StateError" in e and not c.get("censored"):
        return ERR
    if c.get("censored") or c.get("aborted") or e.startswith(">10"):
        return CENS
    return ERR


def load_blocks(d: Path):
    """{token: {(q, s, i): record}}"""
    out: dict = defaultdict(dict)
    for f in sorted(d.glob("*.json")):
        if "-" not in f.stem:
            continue
        tok = f.stem.split("-", 1)[1]
        rec = json.loads(f.read_text())
        if "results" not in rec:
            continue
        for c in rec["results"]:
            out[tok][(c["query"], c["setting"], c["instance"])] = c
    return out


def derive_reasons(point, dirs, gnomon_ref=None, systems=None):
    """The gate's exclusion reasons per system and cell (the logic of tools/w3_gate.py main), from the gate dirs."""
    data: dict = {}
    for d in dirs:
        for sd in sorted(glob.glob(f"{d}/{point}/*/")):
            s = Path(sd).name
            if systems and s not in systems:
                continue
            recs, exp = G.load_dir(Path(sd))
            if s in data:
                data[s][0].update(recs)
                data[s][1].update(exp)
            else:
                data[s] = (recs, exp)
    if gnomon_ref:
        Gm = {}
        for line in open(gnomon_ref):
            if line.strip():
                r = json.loads(line)
                Gm[(r["qid"], r["i"], r["setting"])] = r
    else:
        Gm = data["gnomon"][0]
    excl: dict = {s: {} for s in data}
    for s, (recs, exp) in data.items():
        for key in sorted(exp):
            q, i, stt = key
            o = recs.get(key)
            co = G.classify(o)
            b = Gm.get(key)
            cb = G.classify(b)
            if s == "gnomon":
                if co == "unreached":
                    excl[s][(q, stt, i)] = "unreached"
                continue
            if cb == "unreached":
                excl[s][(q, stt, i)] = "Gnomon cell unreached"
                continue
            if co == "unreached":
                excl[s][(q, stt, i)] = "unreached"
            elif co in ("error", "notexpr", "refused"):
                excl[s][(q, stt, i)] = co
            elif co == "ok" and cb == "ok":
                if o["hash"] != b["hash"] and o.get("sorted_hash") != b.get("sorted_hash"):
                    excl[s][(q, stt, i)] = "differs from Gnomon"
    return excl


class Point:
    def __init__(self, name, res, blockdir, gate_dirs, exclusions_json, params_file, systems, n_big, n_small, ladybug_settings):
        self.name = name
        self.systems = systems
        self.params = defaultdict(dict)       # qid -> {i: stratum}
        for line in open(params_file):
            if line.strip():
                p = json.loads(line)
                self.params[p["qid"]][p["i"]] = p["stratum"]
        self.queries = sorted(self.params, key=qkey)
        self.nbig, self.nsmall = n_big, n_small
        self.lbs = ladybug_settings
        blocks = load_blocks(blockdir)
        self.records = blocks
        excl = derive_reasons(name, gate_dirs)
        ref = json.loads(Path(exclusions_json).read_text())["excluded"]
        self.gate_check = {}
        for s, v in excl.items():
            self.gate_check[s] = (set(f"{q}|{i}|{t}" for (q, t, i) in v) == set(ref.get(s, [])))
        for s in ref:
            if s not in excl and ref[s]:
                self.gate_check[s] = False
        self.excl = excl
        self.cells: dict = {}
        self.state_error_cells: list = []
        for s in systems:
            self.cells[s] = {}
        for s in TOKENS:
            if s not in systems:
                continue
            recs, ex = blocks.get(s, {}), excl.get(GATE_NAME.get(s, s), {})
            for q in self.queries:
                for stt in self.settings(s):
                    for i in range(self.n(s, q)):
                        k = (q, stt, i)
                        if k in recs:
                            c = recs[k]
                            cl = cell_class(c)
                            if (s == "gnomon" and self.name == "SF10" and q == "IC4" and stt == "window" and i in (0, 5, 6, 7)
                                    and "StateError" in str(c.get("error", ""))):
                                cl = ERR
                            self.cells[s][k] = (cl, c["p50_ms"] if c.get("ok") else None)
                            if s == "gnomon" and "StateError" in str(c.get("error", "")):
                                self.state_error_cells.append((q, stt, i, cl))
                        elif k in ex:
                            self.cells[s][k] = (REASON_CLS[ex[k]], None)
                        else:
                            self.cells[s][k] = (MISS, None)
        if "duckdbsql" in systems:
            for k in self.cells["duckdb64"]:
                a, b = self.cells["duckdb64"][k], self.cells["duckdb32"][k]
                cand = [x for x in (a, b) if x[0] == OK]
                if cand:
                    self.cells.setdefault("duckdbsql", {})[k] = min(cand, key=lambda t: t[1])
                elif a[0] == CENS or b[0] == CENS:
                    self.cells.setdefault("duckdbsql", {})[k] = (CENS, None)
                else:
                    self.cells.setdefault("duckdbsql", {})[k] = a

    def settings(self, s):
        return ["current", "asof"] if (s == "ladybug" and self.name == "SF10") else SETTINGS

    def n(self, s, q):
        base = self.nbig if s in ("gnomon", "duckdb64", "duckdb32", "duckdbsql") else self.nsmall
        return min(base, len(self.params[q]))

    # --- comparisons
    def cmp_inst(self, k, x, g="gnomon"):
        gc, gv = self.cells[g][k]
        xc, xv = self.cells[x][k]
        if gc == OK:
            if xc == OK:
                return ("win" if gv < xv else "loss"), gv / xv
            if xc == CENS:
                return "win_c", None
            return "nc", None
        if gc in (CENS, REF, ERR) and xc == OK:
            return "loss_g", None
        return "nc", None

    def cell_cmp(self, x, q, s, g="gnomon", insts=None):
        """Paired comparison of one (query, setting) cell. Returns a dict."""
        n = self.n(x, q)
        gv, xv = [], []
        cens_x = gun = nc = 0
        for i in range(n):
            if insts is not None and i not in insts:
                continue
            k = (q, s, i)
            if k not in self.cells[x] or k not in self.cells[g]:
                continue
            gc, g_v = self.cells[g][k]
            xc, x_v = self.cells[x][k]
            if gc == OK and xc == OK:
                gv.append(g_v)
                xv.append(x_v)
            elif gc == OK and xc == CENS:
                cens_x += 1
            elif gc in (CENS, REF, ERR) and xc == OK:
                gun += 1
            else:
                nc += 1
        ratio = med(gv) / med(xv) if gv else None
        if ratio is not None:
            verdict = "faster" if ratio < 1 else "slower"
        elif cens_x and not gun:
            verdict = "faster (other > 10 s)"
        elif gun:
            verdict = "slower (Gnomon unanswered)"
        else:
            verdict = None
        return {"ratio": ratio, "n_both": len(gv), "cens_x": cens_x, "g_unanswered": gun, "nc": nc, "verdict": verdict,
                "g_med": med(gv), "x_med": med(xv)}

    def best_ratio(self, q, s, cands, insts=None):
        """(ratio, system) against the fastest candidate that has a paired ratio; Gnomon ÷ best."""
        best = None
        for x in cands:
            if x not in self.cells:
                continue
            r = self.cell_cmp(x, q, s, insts=insts)["ratio"]
            if r is not None and (best is None or r > best[0]):
                best = (r, x)
        return best

    def vals(self, s, q=None, stt=None):
        return [v for (qq, ss, i), (c, v) in self.cells[s].items() if c == OK and (q is None or qq == q) and (stt is None or ss == stt)]

    def tally(self, s, pred=lambda k: True):
        return Counter(c for k, (c, v) in self.cells[s].items() if pred(k))


def cell_text(P, s, q, stt):
    n = P.n(s, q)
    cs = [P.cells[s].get((q, stt, i)) for i in range(n)]
    cs = [c for c in cs if c is not None]
    if not cs:
        return "–"
    ok = [v for c, v in cs if c == OK]
    cnt = Counter(c for c, v in cs)
    if ok:
        extra = []
        if len(ok) < len(cs):
            extra.append(f"{len(ok)}/{len(cs)}")
        t = fmt(med(ok))
        return t + (f" [{', '.join(extra)}]" if extra else "")
    top = cnt.most_common(1)[0][0]
    return {CENS: "> 10 s", REF: "refused", ERR: "error", NA: "n/a", UNR: "unreached", GUNR: "unreached (Gnomon)",
            DIFF: "differs", XERR: "excluded (error)", MISS: "missing"}[top]


# ---------------------------------------------------------------------------------------------------------------- engine split
def engine_rows(recs_by_tok, toks, label_of):
    """{(token, family|'all'): stats} from records with engine_time summaries."""
    out = {}
    for tok in toks:
        grp: dict = defaultdict(lambda: {"sh": [], "dl": [], "e2": [], "fr": [], "ex": []})
        for (q, s, i), c in recs_by_tok.get(tok, {}).items():
            if not c.get("ok"):
                continue
            es = (c.get("engine_time") or {}).get("summary")
            if not es or es.get("engine_ms_p50") is None:
                continue
            e2e, eng = c["p50_ms"], es["engine_ms_p50"]
            for f in (fam(q), "all"):
                g = grp[f]
                g["sh"].append(eng / e2e)
                g["dl"].append(e2e - eng)
                g["e2"].append(e2e)
                if es.get("front_ms_p50") is not None:
                    g["fr"].append(es["front_ms_p50"])
                    g["ex"].append(eng - es["front_ms_p50"])
        for f, g in grp.items():
            out[(tok, f)] = {"cells": len(g["sh"]), "share": med(g["sh"]), "p10": sorted(g["sh"])[len(g["sh"]) // 10],
                             "e2e": med(g["e2"]), "front": med(g["fr"]), "exec": med(g["ex"]), "delivery": med(g["dl"])}
    return out


# ---------------------------------------------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", type=Path, required=True)
    ap.add_argument("--gate", type=Path, required=True)
    ap.add_argument("--params", type=Path, required=True)
    ap.add_argument("--sacct", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    R = a.res
    sf1_dirs = [R / "gate-J26", R / "gate-J27", R / "rerun-J33"]
    sf10_dirs = [R / "gate-J34", R / "gate-J28", R / "gate-J36", R / "gate-J30", R / "gateext-J37",
                 R / "gateext-J39"]
    P1 = Point("SF1", R, R / "timed-J31/rot1", sf1_dirs, a.gate / "exclusions-SF1.json", a.params / "W3-SF1.timed.params.jsonl",
               SYS_SF1, 20, 10, None)
    P10 = Point("SF10", R, R / "timed-J40/rot1", sf10_dirs, a.gate / "exclusions-SF10.json", a.params / "W3-SF10.timed.params.jsonl",
                SYS_SF10, 8, 8, None)
    PTS = {"SF1": P1, "SF10": P10}
    S: dict = {"gate_reasons_match_exclusions": {p: P.gate_check for p, P in PTS.items()}}
    md = ["# W3 timed stage: summaries (end-to-end time primary; ONE rotation)", "",
          "Basis: SF1 = rotation 1 of job J31 (rotation 2 is partial and not scored); SF10 = rotation 1 of job "
          "J40; engine-time passes: SF1 job J41, SF10 job J40 (rotE). An instance value is the median of that instance's "
          "timed calls (ms, end to end); with one rotation there is no mean over rotations. DuckDB (SQL) is the better of the 64-thread and "
          "32-thread configurations per instance. Ratios are Gnomon ÷ other (below 1: Gnomon faster).", "",
          "Gnomon faster = instances where Gnomon's value is below the other system's, out of comparable instances (the other system "
          "censored at 10 s counts as Gnomon faster when Gnomon answered; unreached, error, refused, not-expressible, differing and "
          "gate-excluded cells of the other system are not compared). A Gnomon cell that is censored, refused or an error while the "
          "other system answered is a Gnomon loss and is counted as comparable and not faster. A cell ratio is paired: the median "
          "of Gnomon's instance values over the instances both systems answered ÷ the median of the other's over the same instances.", "",
          "Re-derived gate exclusion reasons equal the gate's exclusions-<point>.json for every system: " +
          "; ".join(f"{p}: {'yes' if all(P.gate_check.values()) else 'NO ' + str(P.gate_check)}" for p, P in PTS.items()), ""]

    # ---- headline per point -------------------------------------------------------------------------------------------
    for pn, P in PTS.items():
        md += [f"## {pn}: instance cells by class, median end-to-end time and Gnomon-faster counts", ""]
        rows = []
        S.setdefault("headline", {})[pn] = {}
        for s in P.systems:
            t = P.tally(s)
            ans = P.vals(s)
            fast = comp = loss_g = 0
            ratios = []
            if s != "gnomon":
                for k in P.cells[s]:
                    r, x = P.cmp_inst(k, s)
                    if r in ("win", "win_c"):
                        fast += 1
                        comp += 1
                        if x:
                            ratios.append(x)
                    elif r in ("loss", "loss_g"):
                        comp += 1
                        loss_g += r == "loss_g"
                        if x:
                            ratios.append(x)
            S["headline"][pn][s] = {"cells": len(P.cells[s]), "answered": t[OK], "cens": t[CENS], "refused": t[REF], "error": t[ERR],
                                    "median_ms": med(ans), "faster": fast if s != "gnomon" else None, "comparable": comp if s != "gnomon" else None,
                                    "gnomon_unanswered_losses": loss_g if s != "gnomon" else None}
            rows.append([NAME[s], len(P.cells[s]), t[OK], t[CENS], t[REF], t[ERR], t[UNR] + t[GUNR], t[NA], t[DIFF] + t[XERR], t[MISS],
                         fmt(med(ans)), "" if s == "gnomon" else f"{fast} of {comp}" + (f" ({loss_g} Gnomon unanswered)" if loss_g else ""),
                         "" if s == "gnomon" else fmt(med(ratios))])
        md += md_table(["system", "instance cells", "answered", "> 10 s", "refused", "error", "unreached (gate)", "n/a", "excluded (differs or error)",
                        "missing", "median e2e ms", "Gnomon faster", "median ratio (both answered)"], rows)
        md += ["", "Median e2e is over the cells the system answered (a slow system's median describes the cells it could answer; across systems it "
               "is not like for like). 'unreached (gate)' covers the answer-check window cut of the system itself and cells whose Gnomon "
               "answer was unreached (removed for every system).", ""]
        # per setting
        rows = []
        for stt in SETTINGS:
            for s in P.systems:
                if s in ("gnomon", "duckdb64", "duckdb32"):
                    continue
                if stt not in P.settings(s):
                    continue
                fast = comp = 0
                for k in P.cells[s]:
                    if k[1] != stt:
                        continue
                    r, _ = P.cmp_inst(k, s)
                    if r in ("win", "win_c"):
                        fast += 1
                        comp += 1
                    elif r in ("loss", "loss_g"):
                        comp += 1
                rows.append([stt, NAME[s], f"{fast} of {comp}"])
        md += ["Gnomon-faster counts by setting:", ""] + md_table(["setting", "system", "Gnomon faster"], rows) + [""]
        # per family
        rows = []
        for f in FAMS:
            for stt in SETTINGS:
                row = [f, stt]
                for s in COMP:
                    if s not in P.systems or stt not in P.settings(s):
                        row.append("–")
                        continue
                    rs, fast, comp = [], 0, 0
                    for q in [q for q in P.queries if fam(q) == f]:
                        cc = P.cell_cmp(s, q, stt)
                        if cc["ratio"] is not None:
                            rs.append(cc["ratio"])
                        if cc["verdict"] and cc["verdict"].startswith("faster"):
                            fast += 1
                            comp += 1
                        elif cc["verdict"]:
                            comp += 1
                    row.append(f"{fmt(gm(rs))} ({fast}/{comp})" if comp else "–")
                rows.append(row)
        md += [f"Per query family and setting, {pn}: geometric mean over the family's queries of the paired cell ratio Gnomon ÷ system, "
               "then (queries where Gnomon is faster / queries compared):", ""]
        md += md_table(["family", "setting"] + [NAME[s] for s in COMP], rows) + [""]

    # ---- answer hashes of the timed calls ---------------------------------------------------------------------------
    md += ["## Answer hashes in the timed records (Gnomon vs each system, instances both answered)", "",
           "Each timed record carries the hash of its answer rows. A mismatch is an answer that differs from Gnomon's in row content or row order "
           "(the gate also accepts same-rows-other-order ties, listed as 'row-order ties' there: none occurred).", ""]
    rows = []
    S["hash_mismatch"] = {}
    for pn, P in PTS.items():
        for tok in TOKENS:
            if tok == "gnomon" or tok not in P.systems:
                continue
            both = bad = 0
            ex = []
            for k, c in P.records.get(tok, {}).items():
                g = P.records["gnomon"].get(k)
                if c.get("ok") and g and g.get("ok"):
                    both += 1
                    if c.get("hash") != g.get("hash"):
                        bad += 1
                        ex.append(f"{k[0]}|{k[1]}|{k[2]}")
            S["hash_mismatch"][f"{pn}|{tok}"] = bad
            rows.append([pn, NAME[tok], both, bad, ", ".join(ex[:8]) or "–"])
    md += md_table(["point", "system", "instances both answered", "hash differs from Gnomon's", "first cells"], rows) + [""]

    # ---- per query tables ---------------------------------------------------------------------------------------------
    for pn, P in PTS.items():
        for stt in SETTINGS:
            md += [f"## {pn}, setting {stt}: median e2e ms per query over the instances each system answered", ""]
            md += ["A bracket gives answered / timed instances when not all were answered; '> 10 s' = every timed instance censored; "
                   "'Gnomon ÷ best' is the paired ratio against the fastest competitor (DuckDB (SQL), PostgreSQL, Neo4j, Memgraph, LadybugDB).", ""]
            head = ["query", "n"] + [NAME[s] for s in ["gnomon", "duckdbsql", "postgres", "neo4j", "memgraph", "ladybug"] if s in P.systems] + \
                   ["best competitor", "Gnomon ÷ best"]
            body = []
            for q in P.queries:
                row = [q, P.n("gnomon", q)]
                for s in ["gnomon", "duckdbsql", "postgres", "neo4j", "memgraph", "ladybug"]:
                    if s in P.systems:
                        row.append(cell_text(P, s, q, stt) if stt in P.settings(s) else "–")
                b = P.best_ratio(q, stt, [c for c in COMP if c in P.systems and stt in P.settings(c)])
                row += [NAME[b[1]] if b else "none", fmt(b[0]) if b else "–"]
                body.append(row)
            md += md_table(head, body) + [""]

    # ---- where Gnomon is slower ---------------------------------------------------------------------------------------
    losses_all: dict = {}
    md += ["## Where Gnomon is not faster than another system (query cells; paired ratio above 1, or Gnomon unanswered where the other answered)", ""]
    for pn, P in PTS.items():
        rows = []
        for s in COMP:
            if s not in P.systems:
                continue
            for q in P.queries:
                for stt in P.settings(s):
                    cc = P.cell_cmp(s, q, stt)
                    if cc["verdict"] in ("slower", "slower (Gnomon unanswered)") or (cc["verdict"] == "faster" and cc["g_unanswered"]):
                        rows.append([NAME[s], q, stt, fmt(cc["ratio"]), cc["n_both"], cc["g_unanswered"],
                                     fmt(cc["g_med"]), fmt(cc["x_med"]), cc["verdict"]])
                        losses_all.setdefault(pn, []).append((s, q, stt, cc))
        md += [f"### {pn}: {len(rows)} (system, query, setting) cells", ""]
        md += md_table(["system", "query", "setting", "Gnomon ÷ system", "instances both answered", "Gnomon unanswered where system answered",
                        "Gnomon ms", "system ms", "verdict"], rows or [["none"] * 9]) + [""]
    S["n_loss_cells"] = {p: len(v) for p, v in losses_all.items()}
    md += ["### Summary of the cells where Gnomon is not faster, by point, system and family", "",
           "Cells = (query, setting) pairs compared (a pair is compared when the paired ratio exists, or one side is censored/unanswered while the other answered). "
           "Ratios are the paired cell ratios Gnomon ÷ system over the slower cells (median and maximum).", ""]
    rows = []
    for pn, P in PTS.items():
        for s_ in COMP:
            if s_ not in P.systems:
                continue
            for f in FAMS:
                comp = slow = 0
                rr = []
                for q in [q for q in P.queries if fam(q) == f]:
                    for stt in P.settings(s_):
                        cc = P.cell_cmp(s_, q, stt)
                        if cc["verdict"] is None:
                            continue
                        comp += 1
                        if cc["verdict"] in ("slower", "slower (Gnomon unanswered)") or (cc["verdict"] == "faster" and cc["g_unanswered"]):
                            slow += 1
                            if cc["ratio"] is not None and cc["ratio"] > 1:
                                rr.append(cc["ratio"])
                if comp:
                    rows.append([pn, NAME[s_], f, f"{slow} of {comp}", fmt(med(rr)), fmt(max(rr) if rr else None)])
    md += md_table(["point", "system", "family", "cells where Gnomon is not faster", "ratio median (slower cells)", "ratio max"], rows) + [""]

    # ---- Gnomon errors / refusals ---------------------------------------------------------------------------------------
    md += ["## Gnomon cells that are not answered (censored, refused, error) and what the other systems did", ""]
    for pn, P in PTS.items():
        rows = []
        gq = defaultdict(list)
        for (q, stt, i), (c, v) in P.cells["gnomon"].items():
            if c in (CENS, REF, ERR):
                gq[(q, stt, c)].append(i)
        for (q, stt, c), ii in sorted(gq.items(), key=lambda kv: (qkey(kv[0][0]), kv[0][1])):
            ans = []
            for x in COMP + ["postgres"]:
                pass
            oth = []
            for x in COMP:
                if x not in P.systems or stt not in P.settings(x):
                    continue
                cnt = Counter(P.cells[x].get((q, stt, i), (MISS,))[0] for i in ii if i < P.n(x, q))
                if cnt:
                    oth.append(f"{NAME[x]} " + "/".join(f"{k}:{v}" for k, v in sorted(cnt.items())))
            rows.append([q, stt, {CENS: "> 10 s", REF: "refused (estimated > 10 s)", ERR: "error"}[c], len(ii), ",".join(map(str, ii)), "; ".join(oth)])
        md += [f"### {pn}", ""] + md_table(["query", "setting", "Gnomon", "instances", "which", "other systems on the same instances (class:count)"],
                                          rows or [["none"] * 6]) + [""]

    md += ["### Gnomon cells whose error text is the binding-table StateError", "",
           "The stage names SF10 IC4 window i = 0, 5, 6, 7 (recorded as errors). The runner also recorded the following cells with the same "
           "StateError text but as censored (> 10 s: the call had already passed the ceiling); they are counted as censored.", ""]
    md += md_table(["point", "query", "setting", "instance", "recorded as"],
                   [[pn, q, stt, i, {ERR: "error (named)", CENS: "> 10 s (StateError text)"}.get(cl, cl)]
                    for pn, P in PTS.items() for (q, stt, i, cl) in sorted(P.state_error_cells)] or [["none"] * 5]) + [""]

    # ---- engine split --------------------------------------------------------------------------------------------------
    E1 = load_blocks(R / "etime-J41/rotE")
    E10 = load_blocks(R / "timed-J40/rotE")
    ES = {}
    for pn, P, E in (("SF1", P1, E1), ("SF10", P10, E10)):
        eng = engine_rows(E, ["gnomon", "duckdb64", "postgres"], None)
        eng.update(engine_rows(P.records, ["neo4j", "memgraph"], None))
        ES[pn] = eng
    S["engine"] = {pn: {f"{t}|{f}": v for (t, f), v in e.items()} for pn, e in ES.items()}
    md += ["## Engine, front-end and delivery split (median over cells of each instance's median call)", "",
           "Gnomon, DuckDB (SQL) 64 threads and PostgreSQL: the engine-time pass (instances 0-2, 1 warm-up + 3 timed); Neo4j and Memgraph: the "
           "timed rotation with probes on. Engine share = engine time ÷ e2e; delivery = e2e - engine time. LadybugDB and AeonG have no "
           "engine-time probe.", ""]
    for pn in PTS:
        rows = []
        for f in ["all"] + FAMS:
            for tok in ["gnomon", "duckdb64", "postgres", "neo4j", "memgraph"]:
                v = ES[pn].get((tok, f))
                if v:
                    rows.append([f, NAME[tok], v["cells"], fmt(v["e2e"]), f"{v['share']:.2f} ({v['p10']:.2f})", fmt(v["front"]), fmt(v["exec"]),
                                 fmt(v["delivery"])])
        md += [f"### {pn}", ""] + md_table(["family", "system", "cells", "e2e ms", "engine share (p10)", "front ms", "execution ms", "delivery ms"], rows) + [""]

    # ---- AeonG -----------------------------------------------------------------------------------------------------------
    AE = {}
    for rot in ("rot1", "rot2"):
        AE[rot] = load_blocks(R / "aeong-J38" / rot)
    ae_reasons = derive_reasons("SF1", [R / "aeong-J38/gate"], gnomon_ref=a.gate / "ref-SF1-gnomon.jsonl", systems=["aeong", "aeong-prop"])
    ae_ref = json.loads((R / "aeong-J38/gate-verdict/exclusions-SF1.json").read_text())
    S["aeong_gate_reasons_match"] = {s: set(f"{q}|{i}|{t}" for (q, t, i) in v) == set(ae_ref["excluded"][s]) for s, v in ae_reasons.items()}
    AEV = {}
    for rot in ("rot1", "rot2"):
        for f in ("aeong", "aeong-prop"):
            AEV[(rot, f)] = {k: (cell_class(c), c["p50_ms"] if c.get("ok") else None) for k, c in AE[rot].get(f, {}).items()}
    md += ["## AeonG (SF1, job J38; two rotations were completed; rotation 1 is scored for consistency with the other systems, rotation 2 is a check)", "",
           f"Re-derived AeonG gate reasons equal exclusions-SF1.json: {dict(sorted(S['aeong_gate_reasons_match'].items()))}. Gate reasons per form: " +
           "; ".join(f"{f}: {dict(sorted(Counter(ae_reasons[f].values()).items()))}" for f in sorted(ae_reasons)), ""]
    rows = []
    ae_cmp = {}
    for rot in ("rot1", "rot2"):
        for f in ("aeong", "aeong-prop"):
            vv = AEV[(rot, f)]
            cnt = Counter(c for c, v in vv.values())
            rows.append([rot, NAME[f], len(vv), cnt[OK], cnt[CENS], cnt[ERR]])
    md += md_table(["rotation", "form", "timed cells", "answered", "> 10 s", "error"], rows) + [""]
    qs_ae = [q for q in P1.queries if fam(q) in ("IS", "T-LDBC")]
    body = []
    for f in ("aeong", "aeong-prop"):
        for rot in ("rot1", "rot2"):
            for q in qs_ae:
                for stt in SETTINGS:
                    gv, av = [], []
                    cx = 0
                    for i in range(10):
                        k = (q, stt, i)
                        g = P1.cells["gnomon"].get(k)
                        x = AEV[(rot, f)].get(k)
                        if g and x and g[0] == OK and x[0] == OK:
                            gv.append(g[1])
                            av.append(x[1])
                        elif g and x and g[0] == OK and x[0] == CENS:
                            cx += 1
                    if gv:
                        ae_cmp[(f, rot, q, stt)] = {"ratio": med(gv) / med(av), "n": len(gv), "g": med(gv), "x": med(av)}
                    elif cx:
                        ae_cmp[(f, rot, q, stt)] = {"ratio": None, "n": 0, "cens": cx}
    for f in ("aeong", "aeong-prop"):
        for q in qs_ae:
            for stt in SETTINGS:
                r1, r2 = ae_cmp.get((f, "rot1", q, stt)), ae_cmp.get((f, "rot2", q, stt))
                if r1 or r2:
                    body.append([NAME[f], q, stt, r1["n"] if r1 else "–", fmt(r1["g"]) if r1 and r1.get("g") else "–", fmt(r1["x"]) if r1 and r1.get("x") else ("> 10 s" if r1 else "–"),
                                 fmt(r1["ratio"]) if r1 else "–", fmt(r2["ratio"]) if r2 else "–"])
    md += ["Per query cell, Gnomon (this rotation-1 run of the SF1 job) vs AeonG, paired on the instances both answered: Gnomon ms, AeonG ms (rotation 1), "
           "ratio Gnomon ÷ AeonG (rotation 1), ratio with AeonG's rotation 2:", ""]
    md += md_table(["AeonG form", "query", "setting", "instances", "Gnomon ms", "AeonG ms", "ratio rot1", "ratio rot2 (check)"], body) + [""]

    # ---- W3-D selection --------------------------------------------------------------------------------------------------
    sel_rows, sel = [], {}
    for s in ["duckdbsql", "duckdb64", "duckdb32", "postgres", "neo4j", "memgraph", "ladybug"]:
        per_q, miss, pooled = {}, [], []
        for q in SUBSET12:
            vs = []
            for i in range(P1.n(s, q)):
                c, v = P1.cells[s].get((q, "asof", i), (MISS, None))
                if c == OK:
                    vs.append(v)
                elif c == CENS:
                    vs.append(CEIL_MS)
            if vs:
                per_q[q] = med(vs)
                pooled += vs
            else:
                miss.append(q)
        a_ = med(list(per_q.values())) if per_q else None
        sel[s] = {"median_of_query_medians": a_, "pooled_median": med(pooled), "queries_without_value": miss,
                  "censored_as_10s": sum(1 for q in SUBSET12 for i in range(P1.n(s, q)) if P1.cells[s].get((q, "asof", i), (MISS,))[0] == CENS)}
        sel_rows.append([NAME[s], fmt(a_), fmt(med(pooled)), len(per_q), ", ".join(miss) or "–", sel[s]["censored_as_10s"]])
    md += ["## W3-D systems by the stage rule (lowest SF1 median e2e over the 12-query subset at asof)", "",
           "Subset: " + ", ".join(SUBSET12) + ". Value per system: the median over the 12 queries of each query's median instance value "
           "(a censored instance counts as 10,000 ms, a lower bound), and, as a second reading, the median of all instance values pooled over the 12 "
           "queries. DuckDB (SQL) is the better of 32/64 threads per instance. Queries with no value (every instance excluded or n/a) are left out.", ""]
    md += md_table(["system", "median of query medians ms", "pooled median ms", "queries with a value", "queries without", "censored instances (as 10 s)"], sel_rows) + [""]
    S["w3d"] = sel

    # ---- SU table ---------------------------------------------------------------------------------------------------------
    rows, tot = [], 0.0
    purpose = {}
    for line in open(a.sacct):
        f = line.rstrip("\n").split("|")
        if f[0] == "JobID" or len(f) < 8:
            continue
        su = int(f[7]) / 3600.0
        tot += su
        rows.append([f[0], f[1], f[2], f[3], f[4], f[5], f[6], f"{su:,.1f}"])
    md += ["## Jobs and service units (sacct CPUTimeRAW / 3600 for every job in submitted.txt)", ""]
    md += md_table(["job", "name", "partition", "cores", "state", "exit", "elapsed", "SU"], rows)
    md += ["", f"Total: {tot:,.0f} SU", ""]
    S["su_total"] = tot

    (a.out / "tables.md").write_text("\n".join(md) + "\n")

    (a.out / "summary.json").write_text(json.dumps(S, indent=1, default=str) + "\n")
    print(f"wrote {a.out}: {npass} of {npass + nmiss} pass; total SU {tot:,.0f}")


if __name__ == "__main__":
    main()
