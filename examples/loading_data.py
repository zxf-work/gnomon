"""Loading data: single writes, bulk batches, corrections and retractions.

    python examples/loading_data.py

Shows what each write call stores, how bulk ingest names edges, how to
address an edge later for a correction, and what a node that only appears
as an edge endpoint looks like.
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.core.errors import GnomonError
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


def edge_rows(store, as_of_tt=OPEN_END, rel_types=None):
    """Every believed edge version: (src, dst, disc, vt_s, vt_e, props)."""
    env = call_operator(store.adapter, "version_history",
                        {"kind": "edge", "window": {"t_a": 0, "t_b": OPEN_END},
                         "rel_types": rel_types, "as_of_tt": as_of_tt, "limit": 1000})
    out = []
    for r in env["rows"]:
        props = call_operator(store.adapter, "version_history", {
            "kind": "edge", "window": {"t_a": 0, "t_b": OPEN_END},
            "ref": {"eid": r["eid"]}, "include_props": True, "as_of_tt": as_of_tt,
        })["rows"]
        p = next(x["props"] for x in props if x["vid"] == r["vid"])
        out.append((r["src"], r["dst"], r["disc"], r["vt_s"],
                    "open" if r["vt_e"] == OPEN_END else r["vt_e"], p))
    return sorted(out)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "bank")

        # --- single writes: one batch, one belief time each -------------
        store.assert_node("acct-1", "Account", {"owner": "Ann"}, vt_s=0)
        store.assert_node("acct-2", "Account", {"owner": "Ben"}, vt_s=0)
        store.assert_edge("acct-1", "acct-2", "TRUSTS", {"level": "low"}, vt_s=10, vt_e=500)

        # --- a bulk batch: labelled nodes first, then events -------------
        # Events are instantaneous unless vt_e is given (vt_e = vt_s + 1).
        # An event without "disc" gets "#<position in the stream>".
        t_bulk = store.ingest_events(
            [
                {"src": "acct-1", "dst": "acct-3", "rel_type": "PAYS", "vt_s": 100,
                 "props": {"amount": 250}, "disc": "tx-1"},
                {"src": "acct-3", "dst": "acct-4", "rel_type": "PAYS", "vt_s": 120,
                 "props": {"amount": 240}, "disc": "tx-2"},
                {"src": "acct-4", "dst": "acct-1", "rel_type": "PAYS", "vt_s": 130,
                 "props": {"amount": 230}},
            ],
            nodes=[{"uid": "acct-3", "label": "Account", "props": {"owner": "Cal"}, "vt_s": 0}],
        )
        print("after the bulk batch:")
        for row in edge_rows(store, rel_types=["PAYS"]):
            print("  ", row)

        # acct-4 was never described: bulk ingest created a bare node for it,
        # labelled "Node", valid from its first event.
        v = call_operator(store.adapter, "entity_history", {"uid": "acct-4"})["rows"][0]
        print("acct-4:", v["label"], v["vt_s"], v["props"])

        # --- a correction on an edge, addressed by its disc --------------
        tx1 = EntityRef("edge", src="acct-1", dst="acct-3", rel_type="PAYS", disc="tx-1")
        store.correct(tx1, {"amount": 205}, vt_s=100, vt_e=101)

        # --- a retraction: the TRUSTS edge ended at valid time 300 -------
        store.retract(EntityRef("edge", src="acct-1", dst="acct-2", rel_type="TRUSTS"), 300)

        print("now:")
        for row in edge_rows(store):
            print("  ", row)
        print("as believed after the bulk batch:")
        for row in edge_rows(store, as_of_tt=t_bulk):
            print("  ", row)

        # --- writes that refuse --------------------------------------------
        try:
            store.retract(EntityRef("node", uid="acct-9"), 50)
        except GnomonError as e:
            print("refused:", e.code, e.message)
        try:
            store.ingest_events([], nodes=[{"uid": "acct-1", "label": "Account", "vt_s": 0}])
        except GnomonError as e:
            print("refused:", e.code)

        # --- an edge written by assert_edge does not create its endpoints --
        store.assert_edge("acct-5", "acct-6", "TRUSTS", vt_s=0)
        snap = call_operator(store.adapter, "snapshot_subgraph",
                             {"seeds": ["acct-5"], "t_valid": 10})
        print("snapshot from acct-5 before its nodes exist:", snap["nodes_total"], "nodes")
        store.assert_node("acct-5", "Account", {}, vt_s=0)
        store.assert_node("acct-6", "Account", {}, vt_s=0)
        snap = call_operator(store.adapter, "snapshot_subgraph",
                             {"seeds": ["acct-5"], "t_valid": 10})
        print("after asserting both nodes:", snap["nodes_total"], "nodes,",
              snap["rows_total"], "edge")

        print("stats:", {k: store.stats()[k] for k in
                         ("n_entities", "n_node_versions", "n_edge_versions")})
        store.close()


if __name__ == "__main__":
    main()
