"""Snapshots: the graph at one valid instant, and how it changes.

    python examples/query_snapshots.py

Operators: snapshot_subgraph (k hops around seeds), diff_snapshots (what
changed between two valid instants), neighborhood_evolution (one node's
neighbours gained and lost), and entity_history with a valid-time filter.
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


def build(store):
    """Five people and who works with whom, over valid time 0..1000."""
    for uid, role in [("ann", "lead"), ("ben", "dev"), ("cal", "dev"),
                      ("dan", "dev"), ("eve", "ops")]:
        store.assert_node(uid, "Person", {"name": uid.title(), "role": role}, vt_s=0)
    t_loaded = store.ingest_events([
        {"src": "ann", "dst": "ben", "rel_type": "WORKS_WITH", "vt_s": 0, "vt_e": 1000},
        {"src": "ben", "dst": "cal", "rel_type": "WORKS_WITH", "vt_s": 200, "vt_e": 800},
        {"src": "cal", "dst": "dan", "rel_type": "WORKS_WITH", "vt_s": 400, "vt_e": OPEN_END},
        {"src": "ann", "dst": "eve", "rel_type": "WORKS_WITH", "vt_s": 600, "vt_e": OPEN_END},
    ])
    # Later we learn: ben stopped working with cal at 500, not 800 ...
    store.retract(EntityRef("edge", src="ben", dst="cal", rel_type="WORKS_WITH",
                            disc="#1"), 500)
    # ... and dan became a lead at 700.
    store.assert_node("dan", "Person", {"name": "Dan", "role": "lead"}, vt_s=700)
    return t_loaded


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "team")
        t_loaded = build(store)
        call = lambda op, args: call_operator(store.adapter, op, args)

        # --- snapshot_subgraph: k hops around seeds at one valid instant ---
        for label, as_of in [("now", OPEN_END), ("as loaded", t_loaded)]:
            env = call("snapshot_subgraph", {"seeds": ["ann"], "t_valid": 650, "hops": 3,
                                             "as_of_tt": as_of})
            nodes = [(n["uid"], n["hop"]) for n in env["nodes"]]
            edges = sorted((r["src"], r["dst"]) for r in env["rows"])
            print(f"snapshot at 650, {label}:")
            print("   nodes (uid, hop):", nodes)
            print("   edges:", edges)

        # --- diff_snapshots: what changed between two valid instants ----
        env = call("diff_snapshots", {"t1": 300, "t2": 900})
        print("diff 300 -> 900:")
        print("   edges added:  ", sorted((e["src"], e["dst"]) for e in env["edges_added"]))
        print("   edges removed:", sorted((e["src"], e["dst"]) for e in env["edges_removed"]))
        print("   props changed:", [(p["id"], p["from"]["props"], p["to"]["props"])
                                   for p in env["props_changed"]])

        # --- neighborhood_evolution: one node's neighbours over time ----
        env = call("neighborhood_evolution", {"uid": "cal", "t1": 300, "t2": 900,
                                              "stride": 150})
        print("cal, 300 -> 900: gained", env["neighbors_gained"],
              "lost", env["neighbors_lost"])
        print("   degree series:", [(p["t"], p["degree"]) for p in env["degree_series"]])

        # --- entity_history restricted to a valid-time window -----------
        env = call("entity_history", {"uid": "dan", "window": {"t_a": 650, "t_b": 750}})
        print("dan's versions overlapping [650, 750):",
              [(r["vt_s"], r["props"]["role"]) for r in env["rows"]])
        store.close()


if __name__ == "__main__":
    main()
