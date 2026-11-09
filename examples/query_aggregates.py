"""Counts, series and windows over valid time.

    python examples/query_aggregates.py

Operators: aggregate_events (grouped aggregation of edge events),
graph_metric_timeseries (one metric per time bucket), state_series (an
aggregate over the versions alive in each bucket, on either clock),
burst_detection, count_temporal_motifs / find_temporal_motif_instances,
and co_active (an interval join).
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()

# Payments: (src, dst, valid time, amount). A burst of payments at 500..540.
PAYMENTS = [
    ("ann", "ben", 100, 20), ("ben", "ann", 130, 5), ("ann", "cal", 250, 40),
    ("cal", "dan", 300, 15), ("dan", "ann", 320, 15), ("ben", "cal", 410, 10),
    ("ann", "ben", 500, 70), ("ann", "ben", 510, 70), ("ann", "ben", 520, 70),
    ("ann", "ben", 530, 70), ("ann", "ben", 540, 70), ("cal", "ann", 700, 25),
]
# Logins: ann logs in every 10 time units; ben adds a burst at 500..549.
LOGINS = [("ann", t) for t in range(0, 1000, 10)] + [("ben", t) for t in range(500, 550, 5)]
# Memberships with a valid interval: (person, club, vt_s, vt_e).
MEMBERS = [("ann", "chess", 0, 600), ("ben", "chess", 200, 900),
           ("cal", "chess", 650, 800), ("dan", "golf", 100, 300)]


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "agg")
        store.ingest_events(
            [{"src": s, "dst": d, "rel_type": "PAYS", "vt_s": t, "disc": f"p{i}",
              "props": {"amount": a}} for i, (s, d, t, a) in enumerate(PAYMENTS)])
        store.ingest_events(
            [{"src": p, "dst": c, "rel_type": "MEMBER_OF", "vt_s": a, "vt_e": b,
              "props": {"fee": 10}} for p, c, a, b in MEMBERS])
        store.ingest_events(
            [{"src": u, "dst": "site", "rel_type": "LOGIN", "vt_s": t} for u, t in LOGINS])
        t_before = store.frontier_tt()
        # A correction: payment p2 was 400, not 40.
        store.correct(EntityRef("edge", src="ann", dst="cal", rel_type="PAYS", disc="p2"),
                      {"amount": 400}, vt_s=250, vt_e=251)
        call = lambda op, args: call_operator(store.adapter, op, args)
        window = {"t_a": 0, "t_b": 1000}

        # --- aggregate_events: group, count, exact sum ---------------------
        args = {"group_by": [{"dim": "endpoint", "role": "src"}],
                "aggregates": [{"agg": "count"},
                               {"agg": "sum", "of": "prop", "prop": "amount"}],
                "window": window, "rel_types": ["PAYS"]}
        for label, as_of in [("now", OPEN_END), ("before the correction", t_before)]:
            env = call("aggregate_events", {**args, "as_of_tt": as_of})
            print(f"paid by sender, {label}:", [(r["src"], r["count"], r["sum_prop_amount"])
                                                for r in env["rows"]])
        env = call("aggregate_events", {"group_by": [{"dim": "time_bucket"}], "stride": 250,
                                        "aggregates": [{"agg": "count_distinct", "of": "src"}],
                                        "window": window, "rel_types": ["PAYS"]})
        print("distinct payers per 250:", [(r["t_a"], r["distinct_src"])
                                           for r in env["rows"]])

        # --- graph_metric_timeseries: one metric per bucket ----------------
        env = call("graph_metric_timeseries", {"metric": "edge_event_count",
                                               "window": window, "stride": 100})
        print("edge events (all types) per 100:", [r["value"] for r in env["rows"]])

        # --- state_series: versions alive in each bucket -------------------
        env = call("state_series", {"kind": "edge", "rel_types": ["MEMBER_OF"],
                                    "window": window, "stride": 250,
                                    "agg": {"fn": "count"}})
        print("memberships alive at each bucket start:",
              [(r["t_a"], r["value"]) for r in env["rows"]])
        env = call("state_series", {"kind": "edge", "rel_types": ["MEMBER_OF"],
                                    "window": window, "stride": 500, "at": "time_weighted",
                                    "agg": {"fn": "count"}})
        print("time-weighted over each 500:", [(r["t_a"], r["value"]) for r in env["rows"]])

        # --- burst_detection: bursty buckets of an event-rate series -------
        env = call("burst_detection", {"target": {"kind": "edge_event_rate",
                                                  "rel_type": "LOGIN"},
                                       "window": window, "stride": 50, "method": "ratio",
                                       "params": {"w": 4, "r": 2.0}})
        print("login bursts (bucket start, events):",
              [(r["t_a"], r["value"]) for r in env["rows"]])

        # --- temporal motifs: money that comes back (u->v, v->w, w->u) ------
        env = call("count_temporal_motifs", {"motif": "M_triangle_cyclic", "delta": 100,
                                             "window": window})
        print("payment cycles within 100:", env["count"])
        env = call("find_temporal_motif_instances", {"motif": "M_triangle_cyclic",
                                                     "delta": 100, "window": window})
        for inst in env["rows"]:
            print("   ", [(e["src"], e["dst"], e["t"]) for e in inst["edges"]])

        # --- co_active: memberships that overlap in valid time -------------
        env = call("co_active", {"a_spec": {"rel_type": "MEMBER_OF", "dst": "chess"},
                                 "b_spec": {"rel_type": "MEMBER_OF", "dst": "chess"},
                                 "allen_relation": {"relation": "overlaps"}})
        print("chess memberships that overlap:",
              sorted((r["a"]["src"], r["b"]["src"]) for r in env["rows"]))
        store.close()


if __name__ == "__main__":
    main()
