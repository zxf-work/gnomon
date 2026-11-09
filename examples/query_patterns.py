"""Name search, typed patterns, follow-up computation and pagination.

    python examples/query_patterns.py

Operators: resolve_entities (find uids by uid or name), the engine's name
index (adapter.lookup_name), match (typed pattern matching), compute
(arithmetic over rows an earlier call returned), and cursors.
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()

PEOPLE = [("p1", "Ann Lee", "Lyon"), ("p2", "Ben Ode", "Paris"),
          ("p3", "Cal Lee", "Lyon"), ("p4", "Dan Roy", "Paris")]
# Orders: (person, shop, valid time, amount)
ORDERS = [("p1", "s1", 100, 30), ("p1", "s2", 120, 50), ("p2", "s1", 130, 20),
          ("p3", "s1", 140, 70), ("p3", "s2", 150, 10), ("p4", "s2", 160, 90),
          ("p1", "s1", 400, 25)]


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "shop")
        store.ingest_events(
            [{"src": p, "dst": s, "rel_type": "ORDERED", "vt_s": t, "disc": f"o{i}",
              "props": {"amount": a}} for i, (p, s, t, a) in enumerate(ORDERS)],
            nodes=[{"uid": uid, "label": "Person", "props": {"name": n, "city": c}, "vt_s": 0}
                   for uid, n, c in PEOPLE]
            + [{"uid": "s1", "label": "Shop", "props": {"name": "Corner"}, "vt_s": 0},
               {"uid": "s2", "label": "Shop", "props": {"name": "Market"}, "vt_s": 0}])
        t_loaded = store.frontier_tt()
        # A correction: p2's city was recorded wrong; it was Lyon all along.
        store.correct(EntityRef("node", uid="p2"), {"name": "Ben Ode", "city": "Lyon"},
                      vt_s=0)
        call = lambda op, args: call_operator(store.adapter, op, args)

        # --- resolve_entities and the name index ---------------------------
        env = call("resolve_entities", {"query": "lee"})
        print("resolve 'lee':", [(r["uid"], r["name"]) for r in env["rows"]])
        env = call("resolve_entities", {"query": "market", "label": "Shop"})
        print("resolve 'market' (Shop):", [r["uid"] for r in env["rows"]])
        hits = store.adapter.lookup_name("ann", "prefix")
        print("name index, prefix 'ann':", [(uid, label) for uid, _, label, _ in hits])

        # --- match: a projection at one valid instant ----------------------
        lyon_buyers = {
            "pattern": {"nodes": [{"var": "p", "labels": ["Person"]},
                                  {"var": "s", "labels": ["Shop"]}],
                        "steps": [{"var": "o", "from": "p", "to": "s",
                                   "rel_type": "ORDERED"}],
                        "where": [{"cmp": "=", "l": {"prop": ["p", "city"]},
                                   "r": {"lit": "Lyon"}}]},
            "return": {"rows": [{"as": "who", "expr": {"col": "p.uid"}},
                                {"as": "shop", "expr": {"prop": ["s", "name"]}}],
                       "order_by": [{"expr": {"col": "who"}}, {"expr": {"col": "shop"}}]},
        }
        env = call("match", {**lyon_buyers, "scope": {"vt": {"instant": 400}}})
        print("Lyon buyers, order valid at 400:", [(r["who"], r["shop"]) for r in env["rows"]])

        # --- match: a group over a window ----------------------------------
        per_city = {
            "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "s"}],
                        "steps": [{"var": "o", "from": "p", "to": "s",
                                   "rel_type": "ORDERED"}]},
            "return": {"group": {"by": [{"as": "city", "expr": {"prop": ["p", "city"]}}],
                                 "aggs": [{"as": "orders", "fn": "count"},
                                          {"as": "total", "fn": "sum",
                                           "of": {"tprop": ["o", "amount"], "tau": "int"}}]},
                       "order_by": [{"expr": {"col": "city"}}]},
        }
        for label, as_of in [("now", OPEN_END), ("as loaded", t_loaded)]:
            env = call("match", {**per_city,
                                 "scope": {"vt": {"window": {"t_a": 0, "t_b": 1000}},
                                           "co_valid": ["valid"], "as_of_tt": as_of}})
            print(f"orders per city, {label}:",
                  [(r["city"], r["orders"], r["total"]) for r in env["rows"]])

        # --- compute: arithmetic over rows an earlier call returned --------
        env = call("aggregate_events", {"group_by": [{"dim": "endpoint", "role": "src"}],
                                        "aggregates": [{"agg": "sum", "of": "prop",
                                                        "prop": "amount"}],
                                        "window": {"t_a": 0, "t_b": 1000}})
        spend = env["rows"]
        top = call("compute", {"fn": "topk", "input": spend, "field": "sum_prop_amount",
                               "k": 2})
        print("top 2 spenders:", [(r["src"], r["sum_prop_amount"]) for r in top["rows"]])
        mean = call("compute", {"fn": "mean", "input": spend, "field": "sum_prop_amount"})
        print("mean spend:", mean["value"])

        # --- pagination: follow the cursor ---------------------------------
        args = {"kind": "edge", "window": {"t_a": 0, "t_b": 1000}, "limit": 3}
        page = call("version_history", args)
        pages = [len(page["rows"])]
        while page["cursor"] is not None:
            page = call("version_history", {**args, "cursor": page["cursor"]})
            pages.append(len(page["rows"]))
        print("rows_total", page["rows_total"], "read in pages of", pages)
        store.close()


if __name__ == "__main__":
    main()
