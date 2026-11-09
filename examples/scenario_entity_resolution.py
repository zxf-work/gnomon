"""Scenario: entity resolution. Two records turn out to be one person.

    python examples/scenario_entity_resolution.py

Two customer records, cust-1001 ("J. Smith") and cust-2002 ("John
Smith"), each hold a card. A matching step decides they are the same
person and records that belief as a SAME_AS edge. Later the match is
found to be wrong and the edge is withdrawn. Every question can be asked
before the merge, while it was believed, and after it was withdrawn.
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()

WINDOW = {"t_a": 0, "t_b": 1000}


def person_cards(customer):
    """A match argument: the cards of `customer` and of any record believed
    to be the same person (zero or one SAME_AS step, either direction)."""
    return {
        "pattern": {"nodes": [{"var": "c", "uids": [customer]}, {"var": "same"},
                              {"var": "card", "labels": ["Card"]}],
                    "steps": [{"from": "c", "to": "same", "rel_type": "SAME_AS",
                               "dir": "both", "hops": {"min": 0, "max": 1}},
                              {"from": "same", "to": "card", "rel_type": "HOLDS"}]},
        "return": {"rows": [{"as": "card", "expr": {"col": "card.uid"}}],
                   "order_by": [{"expr": {"col": "card"}}]},
    }


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "customers")
        store.ingest_events(
            [{"src": "cust-1001", "dst": "card-1", "rel_type": "HOLDS", "disc": "h1",
              "vt_s": 0, "vt_e": OPEN_END},
             {"src": "cust-2002", "dst": "card-2", "rel_type": "HOLDS", "disc": "h2",
              "vt_s": 0, "vt_e": OPEN_END},
             {"src": "card-1", "dst": "shop-A", "rel_type": "PAID", "vt_s": 100},
             {"src": "card-2", "dst": "shop-B", "rel_type": "PAID", "vt_s": 200},
             {"src": "card-2", "dst": "shop-C", "rel_type": "PAID", "vt_s": 300}],
            nodes=[{"uid": "cust-1001", "label": "Customer",
                    "props": {"name": "J. Smith", "city": "Leeds"}, "vt_s": 0},
                   {"uid": "cust-2002", "label": "Customer",
                    "props": {"name": "John Smith", "city": "Leeds"}, "vt_s": 0},
                   {"uid": "card-1", "label": "Card", "props": {}, "vt_s": 0},
                   {"uid": "card-2", "label": "Card", "props": {}, "vt_s": 0}])
        t_before = store.frontier_tt()

        # The matcher decides the two records are one person, for all time.
        t_merged = store.assert_edge("cust-2002", "cust-1001", "SAME_AS",
                                     {"score": 0.93}, vt_s=0)

        # A reviewer finds the match wrong. Retracting at the edge's own
        # vt_s withdraws it over its whole valid interval.
        same = EntityRef("edge", src="cust-2002", dst="cust-1001", rel_type="SAME_AS")
        t_split = store.retract(same, 0)

        call = lambda op, args: call_operator(store.adapter, op, args)
        states = [("before the merge", t_before), ("merged", t_merged),
                  ("after the split", t_split)]

        env = call("resolve_entities", {"query": "smith", "label": "Customer"})
        print("records matching 'smith':", [(r["uid"], r["name"]) for r in env["rows"]])

        for label, as_of in states:
            env = call("match", {**person_cards("cust-1001"),
                                 "scope": {"vt": {"instant": 500}, "as_of_tt": as_of}})
            print(f"cards of cust-1001's person, {label}:", [r["card"] for r in env["rows"]])

        # Shops the person paid at, through either record's cards.
        shops = person_cards("cust-1001")
        shops["pattern"]["nodes"].append({"var": "shop"})
        shops["pattern"]["steps"].append({"from": "card", "to": "shop", "rel_type": "PAID"})
        shops["return"] = {"group": {"by": [], "aggs": [{"as": "shops", "fn": "count"}]}}
        for label, as_of in states:
            env = call("match", {**shops, "scope": {"vt": {"window": WINDOW},
                                                    "co_valid": ["valid"], "as_of_tt": as_of}})
            print(f"payments by cust-1001's person, {label}:", env["rows"][0]["shops"])

        # The SAME_AS edge's own belief log: written, then withdrawn.
        env = call("version_history", {"kind": "edge", "window": WINDOW, "belief": "all",
                                       "ref": {"src": "cust-2002", "dst": "cust-1001",
                                               "rel_type": "SAME_AS"},
                                       "include_props": True})
        for r in env["rows"]:
            believed = "still believed" if r["tt_e"] == OPEN_END else (
                "withdrawn at t_split" if r["tt_e"] == t_split else r["tt_e"])
            print("SAME_AS version:", r["props"], "written at t_merged:",
                  r["tt_s"] == t_merged, "|", believed)
        store.close()


if __name__ == "__main__":
    main()
