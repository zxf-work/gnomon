"""Scenario: a payment audit. What was known when the alert was cleared?

    python examples/scenario_payment_audit.py

An analyst cleared an alert on account acct-M. Later, a delayed feed
reported one more transfer, a wrong amount was corrected, and an ownership
link was found. The audit asks two questions:

  1. What did the graph look like, as believed when the alert was cleared?
  2. What was learned after that, and what does the graph show now?

Valid times are real dates (int64 microseconds, UTC). Belief times are
assigned by the store at each write; the example keeps the one returned by
the write that cleared the alert.
"""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


def us(iso):
    """ISO date or date-time (UTC) -> int64 microseconds since the epoch."""
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp()) * 1_000_000


DAY = {"t_a": us("2021-03-02"), "t_b": us("2021-03-03")}


def load_before_clearing(store):
    store.ingest_events([], nodes=[
        {"uid": "p-olsen", "label": "Person", "props": {"name": "K. Olsen"},
         "vt_s": us("2020-01-01")},
        {"uid": "p-xu", "label": "Person", "props": {"name": "L. Xu"},
         "vt_s": us("2020-01-01")},
        {"uid": "acct-V", "label": "Account", "props": {}, "vt_s": us("2020-01-01")},
        {"uid": "acct-M", "label": "Account", "props": {}, "vt_s": us("2020-01-01")},
        {"uid": "acct-X", "label": "Account", "props": {}, "vt_s": us("2020-01-01")},
        {"uid": "acct-Z", "label": "Account", "props": {}, "vt_s": us("2020-01-01")},
    ])
    store.ingest_events([
        {"src": "p-olsen", "dst": "acct-V", "rel_type": "OWNS", "disc": "own-1",
         "vt_s": us("2020-01-01"), "vt_e": OPEN_END},
        {"src": "p-xu", "dst": "acct-X", "rel_type": "OWNS", "disc": "own-2",
         "vt_s": us("2020-01-01"), "vt_e": OPEN_END},
        {"src": "acct-V", "dst": "acct-M", "rel_type": "PAYS", "disc": "tx-1",
         "vt_s": us("2021-03-02T09:00"), "props": {"amount": 4900}},
        # entered wrongly as 240; the real amount was 2400
        {"src": "acct-M", "dst": "acct-X", "rel_type": "PAYS", "disc": "tx-2",
         "vt_s": us("2021-03-02T09:30"), "props": {"amount": 240}},
    ])
    store.assert_node("alert-17", "Alert", {"account": "acct-M", "status": "open"},
                      vt_s=us("2021-03-02T10:00"))
    # The analyst clears the alert at 16:00. Keep this write's belief time.
    return store.assert_node("alert-17", "Alert", {"account": "acct-M", "status": "cleared"},
                             vt_s=us("2021-03-02T16:00"))


def learn_after_clearing(store):
    # A delayed feed reports a transfer made at 09:45 that day.
    store.ingest_events([
        {"src": "acct-M", "dst": "acct-Z", "rel_type": "PAYS", "disc": "tx-3",
         "vt_s": us("2021-03-02T09:45"), "props": {"amount": 2400}}])
    # The amount of tx-2 is corrected.
    store.correct(EntityRef("edge", src="acct-M", dst="acct-X", rel_type="PAYS", disc="tx-2"),
                  {"amount": 2400}, vt_s=us("2021-03-02T09:30"), vt_e=us("2021-03-02T09:30") + 1)
    # K. Olsen, who sent the money, has owned acct-Z since 2020.
    store.assert_edge("p-olsen", "acct-Z", "OWNS", vt_s=us("2020-01-01"), disc="own-3")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "payments")
        t_cleared = load_before_clearing(store)
        call = lambda op, args: call_operator(store.adapter, op, args)

        trail_args = {"src": "acct-V", "window": DAY, "as_of_tt": t_cleared}
        trail_then = call("temporal_reachability", trail_args)

        learn_after_clearing(store)

        # --- 1. the graph as believed when the alert was cleared -------------
        alert = call("entity_history", {"uid": "alert-17", "as_of_tt": t_cleared})
        print("alert-17 as believed at clearing:",
              [v["props"]["status"] for v in alert["rows"]])
        for label, as_of in [("at clearing", t_cleared), ("now", OPEN_END)]:
            env = call("temporal_reachability", {**trail_args, "as_of_tt": as_of})
            print(f"money trail from acct-V, {label}:",
                  [(r["uid"], datetime.fromtimestamp(r["earliest_arrival"] / 1e6, UTC)
                    .strftime("%H:%M")) for r in env["rows"]])
        # An answer pinned to a past belief time does not change.
        again = call("temporal_reachability", trail_args)
        print("pinned answer unchanged after later writes:",
              again["result_digest"] == trail_then["result_digest"], "| pinned:", again["pinned"])

        out_args = {"group_by": [{"dim": "endpoint", "role": "dst"}],
                    "aggregates": [{"agg": "sum", "of": "prop", "prop": "amount"}],
                    "window": DAY, "rel_types": ["PAYS"],
                    "endpoint_filter": {"role": "src", "uids": ["acct-M"]}}
        for label, as_of in [("at clearing", t_cleared), ("now", OPEN_END)]:
            env = call("aggregate_events", {**out_args, "as_of_tt": as_of})
            print(f"paid out by acct-M, {label}:",
                  [(r["dst"], r["sum_prop_amount"]) for r in env["rows"]])

        # --- 2. what was learned after the clearing -----------------------------
        names = {r["eid"]: f"{r['src']}->{r['dst']} {r['rel_type']}" for r in call(
            "version_history", {"kind": "edge", "window": {"t_a": 0, "t_b": OPEN_END},
                                "belief": "all", "limit": 1000})["rows"]}
        diff = call("diff_belief", {"t1": t_cleared, "t2": OPEN_END, "window": DAY})
        print("belief changes about that day since clearing:",
              diff["added_total"], "added,", diff["retracted_total"], "retracted")
        for r in sorted(diff["rows"], key=lambda r: (r["tt"], r["change"])):
            print(f"   {r['change']:<9} {names.get(r['key'], r['key'])}")

        # Who owns the accounts acct-M paid that day?
        owners = {
            "pattern": {"nodes": [{"var": "m", "uids": ["acct-M"]}, {"var": "a"},
                                  {"var": "o", "labels": ["Person"]}],
                        "steps": [{"from": "m", "to": "a", "rel_type": "PAYS"},
                                  {"from": "o", "to": "a", "rel_type": "OWNS"}]},
            "return": {"rows": [{"as": "account", "expr": {"col": "a.uid"}},
                                {"as": "owner", "expr": {"prop": ["o", "name"]}}],
                       "order_by": [{"expr": {"col": "account"}}]},
        }
        for label, as_of in [("at clearing", t_cleared), ("now", OPEN_END)]:
            env = call("match", {**owners, "scope": {"vt": {"window": DAY},
                                                     "co_valid": ["valid"],
                                                     "as_of_tt": as_of}})
            print(f"owners of accounts paid by acct-M, {label}:",
                  [(r["account"], r["owner"]) for r in env["rows"]])
        store.close()


if __name__ == "__main__":
    main()
