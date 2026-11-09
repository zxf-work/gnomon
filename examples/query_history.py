"""History: versions, the belief log, and the diff between two beliefs.

    python examples/query_history.py

Operators: entity_history (one node's versions as believed at a time),
version_history (the belief log, with both clocks on every row), and
diff_belief (what changed in belief between two belief times).
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


def fmt(t):
    return "open" if t == OPEN_END else t


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "history")
        call = lambda op, args: call_operator(store.adapter, op, args)

        # Three belief times, T1 < T2 < T3 (the data-model page's example).
        store.assert_node("alice", "Person", {"name": "Alice"}, vt_s=0)
        store.assert_node("acme", "Company", {"name": "Acme"}, vt_s=0)
        T1 = store.assert_edge("alice", "acme", "WORKS_AT", {"role": "engineer"},
                               vt_s=100, vt_e=500)
        job = EntityRef("edge", src="alice", dst="acme", rel_type="WORKS_AT")
        T2 = store.correct(job, {"role": "manager"}, vt_s=300, vt_e=500)
        T3 = store.retract(job, 400)

        # --- entity_history: alice's node, with her edges ---------------
        env = call("entity_history", {"uid": "alice", "include_edges": True})
        print("alice's edges now:",
              [(e["rel_type"], e["dst"], e["vt_s"], e["vt_e"]) for e in env["edges"]])

        # --- version_history: one identity's belief log ------------------
        env = call("version_history", {"kind": "edge", "window": {"t_a": 0, "t_b": 1000},
                                       "ref": {"src": "alice", "dst": "acme",
                                               "rel_type": "WORKS_AT"},
                                       "belief": "all", "include_props": True})
        names = {T1: "T1", T2: "T2", T3: "T3", OPEN_END: "open"}
        print("the edge's version rows (vt, role, tt):")
        # (rows come in belief-time order; ties are sorted here by valid time)
        for r in sorted(env["rows"], key=lambda r: (r["tt_s"], r["vt_s"])):
            print(f"   [{r['vt_s']}, {r['vt_e']})  {r['props']['role']:<9}"
                  f"  [{names[r['tt_s']]}, {names[r['tt_e']]})")

        # Belief modes: current, superseded, all; each as of a belief time.
        for as_of in (T1, T2, OPEN_END):
            counts = {b: call("version_history", {"kind": "edge",
                                                  "window": {"t_a": 0, "t_b": 1000},
                                                  "belief": b, "as_of_tt": as_of})["rows_total"]
                      for b in ("current", "superseded", "all")}
            print(f"as of {names[as_of]}: {counts}")

        # --- the same question asked at three belief times ---------------
        for as_of in (T1, T2, T3):
            env = call("version_history", {"kind": "edge", "window": {"t_a": 0, "t_b": 1000},
                                           "ref": {"src": "alice", "dst": "acme",
                                                   "rel_type": "WORKS_AT"},
                                           "include_props": True, "as_of_tt": as_of})
            print(f"believed at {names[as_of]}:",
                  sorted((r["vt_s"], r["vt_e"], r["props"]["role"]) for r in env["rows"]))

        # --- diff_belief: what changed between two belief times ----------
        env = call("diff_belief", {"t1": T1, "t2": OPEN_END,
                                   "window": {"t_a": 0, "t_b": 1000}})
        print("since T1:", env["added_total"], "added,", env["retracted_total"], "retracted")
        for r in sorted(env["rows"], key=lambda r: (r["tt"], r["change"], r["vt_s"])):
            print(f"   {r['change']:<9} [{r['vt_s']}, {fmt(r['vt_e'])})  at {names[r['tt']]}")
        store.close()


if __name__ == "__main__":
    main()
