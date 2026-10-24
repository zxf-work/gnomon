"""The admission guardrail's refusal, pinned by a test rather than a frozen digest.

A frozen-digest case meant to pin the refusal point of `enforce_cost` -- the per-operator `cost_fn` estimate, checked
before the operator runs -- but on its store the estimate passes and the refusal it
froze was the enumeration's runtime budget. Here a `temporal_paths` call whose
estimate is past the ceilings is refused at admission, on whichever backend the suite
selects, and the operator itself never runs.
"""

from __future__ import annotations

import pytest

from gnomon.core.errors import CostError
from gnomon.core.model import canonical_json
from gnomon.temporal import ops_paths
from gnomon.temporal.algebra import call_operator, ensure_all_registered
from gnomon.temporal.guardrails import DEFAULT_CEILINGS

from .conftest import fresh_adapter

ensure_all_registered()


def _dense_store():
    """25 nodes, 2,000 instantaneous events: a mean windowed out-degree of 80, so
    six hops estimate 80 + 80^2 + ... + 80^6 expansions, past the ceiling."""
    a = fresh_adapter()
    events = [{"src": f"u{i % 25}", "dst": f"u{(i * 7 + 3) % 25}", "rel_type": "R",
               "vt_s": i, "vt_e": i + 1} for i in range(2000)]
    a.begin()
    a.apply_ops([{"op": "ingest_events", "events": events, "offset": 0,
                  "node_label": "Node"}], 1)
    a.commit()
    return a


def test_an_estimate_past_the_ceilings_is_refused_before_the_operator_runs(monkeypatch):
    a = _dense_store()

    def executed(*_a, **_k):
        raise AssertionError("the operator ran: the refusal was not at admission")

    monkeypatch.setattr(ops_paths, "_engine_paths", executed)
    monkeypatch.setattr(ops_paths, "_csr_for", executed)
    args = {"src": "u1", "dst": "u2", "window": {"t_a": 0, "t_b": 2000}, "k": 3,
            "max_hops": 6}
    est = ops_paths._paths_cost(args, a.stats())
    assert est["expansions_est"] > DEFAULT_CEILINGS["expansions_est"]
    with pytest.raises(CostError, match="estimated cost for temporal_paths exceeds ceilings"):
        call_operator(a, "temporal_paths", args)


def test_an_estimate_inside_the_ceilings_runs(monkeypatch):
    """The same store, two hops: admitted, and answered."""
    a = _dense_store()
    out = call_operator(a, "temporal_paths", {"src": "u1", "dst": "u2",
                                              "window": {"t_a": 0, "t_b": 2000},
                                              "k": 3, "max_hops": 2})
    assert out["rows_total"] >= 0
    assert canonical_json(out["rows"])  # an answer, not a refusal
