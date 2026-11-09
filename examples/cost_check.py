"""The cost check: what a refusal looks like, and how to change the budget.

    python examples/cost_check.py

Before an operator runs, call_operator estimates its work from the store's
statistics and refuses a call above the budget with a CostError (code
E_COST). The default budget is 10 seconds of estimated time. This example
lowers the budget to provoke a refusal on a small store, then shows the
three ways forward: narrow the call, raise the budget, or skip the check.
"""

import tempfile
from pathlib import Path

import gnomon
from gnomon.core.errors import CostError
from gnomon.temporal.algebra import call_operator, ensure_all_registered
from gnomon.temporal.guardrails import DEFAULT_CEILINGS

ensure_all_registered()


def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = gnomon.open(Path(tmp) / "cost")
        # 20,000 events over valid time 0 .. 2,000,000.
        store.ingest_events(
            {"src": f"n{i % 500}", "dst": f"n{(i * 7 + 1) % 500}", "rel_type": "LINK",
             "vt_s": i * 100} for i in range(20_000))
        print("default budget:", DEFAULT_CEILINGS)

        # Up to 5 time-respecting paths of at most 4 hops, over the whole window.
        args = {"src": "n1", "dst": "n57", "window": {"t_a": 0, "t_b": 2_000_000}}
        budget = {"time_est_ms": 100}  # a deliberately small budget for this example

        try:
            call_operator(store.adapter, "temporal_paths", args, cost_ceilings=budget)
        except CostError as e:
            payload = e.to_payload()
            print("refused:", payload["error"], "-", payload["message"])
            print("   estimate:", payload["details"]["estimate"])
            print("   over:    ", payload["details"]["ceilings"])
            print("   advice:  ", payload["details"]["suggestions"])

        # 1. Narrow the call: fewer hops, or a smaller valid-time window.
        for label, narrow in [("max_hops 2", {**args, "max_hops": 2}),
                              ("window 1/10", {**args, "window": {"t_a": 0, "t_b": 200_000}})]:
            try:
                env = call_operator(store.adapter, "temporal_paths", narrow,
                                    cost_ceilings=budget)
                print(f"{label}: admitted, {len(env['rows'])} of {env['rows_total']}"
                      " paths returned")
            except CostError as e:
                print(f"{label}: refused, estimate {e.details['estimate']['time_est_ms']} ms")

        # 2. Raise the budget for this call (the default budget admits it too).
        env = call_operator(store.adapter, "temporal_paths", args,
                            cost_ceilings={"time_est_ms": 60_000})
        print("raised budget:", len(env["rows"]), "of", env["rows_total"], "paths returned")

        # 3. Skip the check entirely.
        env = call_operator(store.adapter, "temporal_paths", args, skip_cost_check=True)
        print("skip_cost_check:", len(env["rows"]), "of", env["rows_total"], "paths returned")
        store.close()


if __name__ == "__main__":
    main()
