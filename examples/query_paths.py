"""Paths and reachability: who can reach whom, and how.

    python examples/query_paths.py

Operators: temporal_reachability (earliest arrival over time-respecting
paths), temporal_paths (up to k time-respecting paths between two nodes),
and paths (hop and weight distances, k shortest paths, a window form).
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()

# Messages between people: (src, dst, valid time). Each is an instant.
MESSAGES = [
    ("ann", "ben", 10), ("ben", "cal", 20), ("cal", "dan", 15),
    ("ann", "cal", 30), ("cal", "dan", 40), ("dan", "eve", 50),
    ("ben", "eve", 60),
]
# Roads with a length, valid for a period: (src, dst, km, vt_s, vt_e).
ROADS = [
    ("A", "B", 5, 0, OPEN_END), ("B", "C", 5, 0, OPEN_END),
    ("A", "C", 20, 0, OPEN_END), ("C", "D", 1, 0, 500), ("B", "D", 30, 0, OPEN_END),
]


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "paths")
        t_msgs = store.ingest_events(
            [{"src": s, "dst": d, "rel_type": "MSG", "vt_s": t} for s, d, t in MESSAGES])
        store.ingest_events(
            [{"src": s, "dst": d, "rel_type": "ROAD", "vt_s": a, "vt_e": b,
              "props": {"km": km}} for s, d, km, a, b in ROADS])
        call = lambda op, args: call_operator(store.adapter, op, args)
        window = {"t_a": 0, "t_b": 100}

        # --- temporal_reachability: earliest arrival from ann ------------
        env = call("temporal_reachability", {"src": "ann", "window": window})
        print("reached from ann:", [(r["uid"], r["earliest_arrival"]) for r in env["rows"]])
        env = call("temporal_reachability", {"src": "ann", "window": {"t_a": 25, "t_b": 100}})
        print("reached from ann, from 25 on:",
              [(r["uid"], r["earliest_arrival"]) for r in env["rows"]])

        # --- temporal_paths: the paths themselves -------------------------
        env = call("temporal_paths", {"src": "ann", "dst": "dan", "window": window, "k": 3})
        print("ann -> dan, time-respecting:")
        for p in env["rows"]:
            hops = " ".join(f"{e['src']}>{e['dst']}@{e['t']}" for e in p["edges"])
            print(f"   arrival {p['arrival']}, {p['hops']} hops: {hops}")

        # A correction removes a message; the old answer stays reproducible.
        store.retract(EntityRef("edge", src="ann", dst="cal", rel_type="MSG", disc="#3"), 30)
        env = call("temporal_paths", {"src": "ann", "dst": "dan", "window": window, "k": 3})
        print("after retracting ann>cal@30:", len(env["rows"]), "path(s)")
        env = call("temporal_paths", {"src": "ann", "dst": "dan", "window": window, "k": 3,
                                      "as_of_tt": t_msgs})
        print("as believed before:          ", len(env["rows"]), "path(s)")

        # --- paths: distances on the road graph ---------------------------
        roads = {"rel_types": ["ROAD"]}
        env = call("paths", {"sources": {"uids": ["A"]}, "targets": {"uids": ["D"]},
                             "t_valid": 100, **roads})
        print("hops A -> D at 100:", [(r["dst"], r["distance"]) for r in env["rows"]])
        env = call("paths", {"sources": {"uids": ["A"]}, "targets": {"uids": ["D"]},
                             "t_valid": 100, "metric": "weight",
                             "weight": {"prop": "km"}, "result": "one_path", **roads})
        print("shortest km A -> D at 100:",
              [(r["distance"], r["path"]) for r in env["rows"]])
        env = call("paths", {"sources": {"uids": ["A"]}, "targets": {"uids": ["D"]},
                             "t_valid": 600, "metric": "weight",
                             "weight": {"prop": "km"}, "result": "one_path", **roads})
        print("shortest km A -> D at 600:",
              [(r["distance"], r["path"]) for r in env["rows"]])
        env = call("paths", {"sources": {"uids": ["A"]}, "targets": {"uids": ["D"]},
                             "t_valid": 100, "result": "k_paths", "k": 3,
                             "max_hops": 4, **roads})
        print("3 shortest A -> D by hops at 100:", [r["path"] for r in env["rows"]])
        store.close()


if __name__ == "__main__":
    main()
