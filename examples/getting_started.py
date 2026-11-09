"""Getting started: open a store, write facts, correct one, ask twice.

Run it with the package installed:

    python examples/getting_started.py

The store is written under a temporary directory and removed at the end.
Times are int64 microseconds; this example uses small numbers (0, 100,
300, ...) as valid times so the output is easy to read.
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()  # import every operator module once per process


def city_history(store, as_of_tt=OPEN_END):
    """alice's versions as (vt_s, vt_e, city), believed at as_of_tt."""
    env = call_operator(store.adapter, "entity_history",
                        {"uid": "alice", "as_of_tt": as_of_tt})
    return [(r["vt_s"], "open" if r["vt_e"] == OPEN_END else r["vt_e"],
             r["props"]["city"]) for r in env["rows"]]


def friends(store, t_valid, as_of_tt=OPEN_END):
    """Who alice is linked to in the graph as it was at valid time t_valid."""
    env = call_operator(store.adapter, "snapshot_subgraph",
                        {"seeds": ["alice"], "t_valid": t_valid, "hops": 1,
                         "as_of_tt": as_of_tt})
    return sorted(n["uid"] for n in env["nodes"] if n["uid"] != "alice")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "people"
        store = gnomon.open(path)  # a directory; created if absent

        # Each write is one batch. It returns the batch's belief time.
        t1 = store.assert_node("alice", "Person", {"name": "Alice", "city": "Lyon"}, vt_s=0)
        store.assert_node("bob", "Person", {"name": "Bob", "city": "Lyon"}, vt_s=0)
        t2 = store.assert_edge("alice", "bob", "KNOWS", {"since": 2019}, vt_s=100)

        # A correction: from valid time 300 on, alice lived in Paris.
        # The new version carries exactly the props given here.
        t3 = store.correct(EntityRef("node", uid="alice"),
                           {"name": "Alice", "city": "Paris"}, vt_s=300)
        print("belief times increase:", t1 < t2 < t3)

        print("alice now:           ", city_history(store))
        print("alice as believed t2:", city_history(store, as_of_tt=t2))

        # One version, at one valid instant.
        env = call_operator(store.adapter, "entity_history",
                            {"uid": "alice", "t_valid": 200})
        print("city at valid time 200:", env["rows"][0]["props"]["city"])

        # A retraction: the KNOWS edge ended at valid time 500.
        t4 = store.retract(EntityRef("edge", src="alice", dst="bob", rel_type="KNOWS"), 500)
        print("friends at 600, now:           ", friends(store, 600))
        print("friends at 600, as believed t3:", friends(store, 600, as_of_tt=t3))
        print("friends at 400, now:           ", friends(store, 400))
        store.close()

        # Any number of reader processes may open the store read-only.
        reader = gnomon.open(path, read_only=True)
        print("reader sees the retraction:", reader.frontier_tt() >= t4)
        print("reader stats:", {k: reader.stats()[k]
                                for k in ("n_node_versions", "n_edge_versions")})
        reader.close()


if __name__ == "__main__":
    main()
