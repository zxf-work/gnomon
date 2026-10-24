"""The two hub kernels answer exactly what the operators answered,
and answer what the old budgets refused.

* `temporal_paths` counts its paths and keeps the first `k` in the
  engine (`crate::paths`); `GNOMON_PATHS_KERNEL=python` keeps the depth-first
  enumeration. Every answer here is the enumeration's, at current and past
  belief, over windows that read by rounds and that read the window; a hub
  query the enumeration's budget refuses is answered, and equals the
  enumeration with its budget lifted.
* `co_active` counts its pairs and cuts its page in the engine
  (`interval_page`) with no cap. Every answer here is the oracle's, page by
  page, including joins far past the old 50,000-pair cap.

The stores are small skewed graphs: two hubs hold a third or more of the
endpoints, and corrections and retractions give them a past belief state.
The co_active store's intervals are long, so its hub joins run far past the
old cap.
"""

from __future__ import annotations

import random
from typing import Any

import pytest

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

import gnomon  # noqa: E402
from gnomon.core.errors import CostError  # noqa: E402
from gnomon.core.model import OPEN_END, EntityRef, canonical_json  # noqa: E402
from gnomon.temporal import ops_paths, ops_series  # noqa: E402
from gnomon.temporal.algebra import call_operator, ensure_all_registered, paginate  # noqa: E402
from gnomon.temporal.oracle import Oracle  # noqa: E402

from .conftest import ENVELOPE_META_KEYS  # noqa: E402

ensure_all_registered()

N_EVENTS = 6000
N_NODES = 300


def _events(p0: float, p1: float, max_life: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)

    def pick() -> int:
        r = rng.random()
        return 0 if r < p0 else 1 if r < p0 + p1 else rng.randrange(2, N_NODES)

    out = []
    for i in range(N_EVENTS):
        life = 1 if i % 3 == 0 else 1 + rng.randrange(max_life)
        out.append({"src": f"n{pick()}", "dst": f"n{pick()}",
                    "rel_type": "R" if i % 4 else "S", "vt_s": i, "vt_e": i + life})
    return out


def _ref(e: dict[str, Any], i: int) -> EntityRef:
    return EntityRef(kind="edge", src=e["src"], dst=e["dst"], rel_type=e["rel_type"],
                     disc=f"#{i}")


def _build(path, events: list[dict[str, Any]]) -> int:
    """The store, with corrections and retractions after the ingest; returns
    a belief time at which the ingest is believed and they are not."""
    st = gnomon.open(path, backend="native")
    epoch = st.ingest_events(events)
    for i in range(0, N_EVENTS, 37):
        e = events[i]
        st.correct(_ref(e, i), {"w": 2}, vt_s=e["vt_s"], vt_e=e["vt_e"])
    for i in range(5, N_EVENTS, 53):
        e = events[i]
        if e["vt_e"] - e["vt_s"] > 2:
            st.retract(_ref(e, i), e["vt_s"] + (e["vt_e"] - e["vt_s"]) // 2)
    st.close()
    return epoch


@pytest.fixture(scope="module")
def hub(tmp_path_factory):
    """`(kernel adapter, enumeration adapter, oracle, a past belief time)`
    over one store, two read-only handles opened under the two settings."""
    path = tmp_path_factory.mktemp("hub") / "store"
    with pytest.MonkeyPatch.context() as mp:
        for k in ("GNOMON_PATHS_KERNEL", "GNOMON_PATHS_ROUNDS"):
            mp.delenv(k, raising=False)
        epoch = _build(path, _events(0.22, 0.10, 400, 7))
        kern = gnomon.open(path, backend="native", read_only=True)
        mp.setenv("GNOMON_PATHS_KERNEL", "python")
        enum = gnomon.open(path, backend="native", read_only=True)
    assert kern.adapter.designs()["paths_kernel"] is True
    assert enum.adapter.designs()["paths_kernel"] is False
    oracle = Oracle(list(kern.adapter.all_node_versions()),
                    list(kern.adapter.all_edge_versions()))
    yield kern.adapter, enum.adapter, oracle, epoch
    kern.close()
    enum.close()


@pytest.fixture(scope="module")
def dense(tmp_path_factory):
    """`(adapter, oracle, a past belief time)`: heavier hubs, long intervals."""
    path = tmp_path_factory.mktemp("dense") / "store"
    epoch = _build(path, _events(0.30, 0.15, 2400, 8))
    st = gnomon.open(path, backend="native", read_only=True)
    oracle = Oracle(list(st.adapter.all_node_versions()), list(st.adapter.all_edge_versions()))
    yield st.adapter, oracle, epoch
    st.close()


def _payload(out: dict[str, Any]) -> str:
    return canonical_json({k: v for k, v in out.items() if k not in ENVELOPE_META_KEYS})


def _paths_cases() -> list[dict[str, Any]]:
    rng = random.Random(11)
    windows = [(0, N_EVENTS // 4), (N_EVENTS // 2, N_EVENTS // 2 + 300), (1000, 1010),
               (2000, 2600), (0, 40)]
    nodes = ["n0", "n1"] + [f"n{i}" for i in range(2, 14)]
    cases = []
    for _ in range(90):
        s, d = rng.choice(nodes), rng.choice(nodes)
        t_a, t_b = rng.choice(windows)
        hubs = s in ("n0", "n1") or d in ("n0", "n1")
        cases.append({"src": s, "dst": d, "window": {"t_a": t_a, "t_b": t_b},
                      "k": rng.choice([1, 3, 20]),
                      "max_hops": rng.choice([1, 2, 3] if hubs else [1, 2, 3, 4])})
    cases += [{"src": "n0", "dst": "n0", "window": {"t_a": 0, "t_b": 900}, "k": 3, "max_hops": 3},
              {"src": "n0", "dst": "n1", "window": {"t_a": 0, "t_b": 1500}, "k": 3, "max_hops": 3},
              {"src": "n1", "dst": "n0", "window": {"t_a": 0, "t_b": 1500}, "k": 20, "max_hops": 3}]
    return cases


@pytest.mark.parametrize("past", [False, True])
def test_paths_answers_are_the_enumerations(hub, monkeypatch, past):
    kern, enum, _, epoch = hub
    monkeypatch.setattr(ops_paths, "MAX_EXPANSIONS", 10**9)
    answered = 0
    for args in _paths_cases():
        if past:
            args = {**args, "as_of_tt": epoch}
        got = call_operator(kern, "temporal_paths", dict(args))
        want = call_operator(enum, "temporal_paths", dict(args))
        assert _payload(got) == _payload(want), args
        answered += got["rows_total"] > 0
    assert answered > 20  # the cases are not vacuous


def test_paths_small_windows_are_the_oracles(hub):
    kern, _, oracle, epoch = hub
    for s, d in [("n0", "n1"), ("n2", "n0"), ("n1", "n3"), ("n4", "n5")]:
        for t_a, t_b, h in [(1000, 1030, 3), (3000, 3060, 2), (0, 25, 4)]:
            for as_of in (OPEN_END, epoch):
                args = {"src": s, "dst": d, "window": {"t_a": t_a, "t_b": t_b},
                        "k": 5, "max_hops": h, "as_of_tt": as_of}
                got = call_operator(kern, "temporal_paths", args)
                want = oracle.temporal_paths(got["args_echo"])
                assert _payload(got) == canonical_json(want), args


def test_a_hub_query_past_the_enumerations_budget_is_answered(hub, monkeypatch):
    kern, enum, _, _ = hub
    args = {"src": "n0", "dst": "n1", "window": {"t_a": 0, "t_b": 1500}, "k": 3, "max_hops": 3}
    monkeypatch.setattr(ops_paths, "MAX_EXPANSIONS", 2000)
    with pytest.raises(CostError, match="temporal_paths expansion budget exceeded"):
        call_operator(enum, "temporal_paths", dict(args))
    got = call_operator(kern, "temporal_paths", dict(args))
    monkeypatch.setattr(ops_paths, "MAX_EXPANSIONS", 10**9)
    want = call_operator(enum, "temporal_paths", dict(args))
    assert _payload(got) == _payload(want)
    assert got["rows_total"] > 100 and got["truncated"] and len(got["rows"]) == 3


def test_the_kernels_own_budget_refuses_with_the_operators_error(hub, monkeypatch):
    kern, _, _, _ = hub
    monkeypatch.setattr(ops_paths, "KERNEL_MAX_EXPANSIONS", 5)
    with pytest.raises(CostError, match="temporal_paths expansion budget exceeded"):
        call_operator(kern, "temporal_paths",
                      {"src": "n0", "dst": "n1", "window": {"t_a": 0, "t_b": 1500},
                       "k": 3, "max_hops": 3})


def test_rounds_and_the_window_read_give_one_answer(hub):
    kern, _, _, epoch = hub
    ids = {u: int(x) for u, x in zip(["n0", "n1", "n2", "n7", "n9"],
                                      kern.dense_ids(["n0", "n1", "n2", "n7", "n9"]))}
    modes = set()
    for s, d in [("n2", "n7"), ("n7", "n9"), ("n0", "n1"), ("n9", "n0"), ("n2", "n2")]:
        for t_a, t_b in [(0, 40), (500, 900), (0, N_EVENTS)]:
            for h in (2, 3, 4):
                for as_of in (OPEN_END, epoch):
                    r = kern.paths_topk(ids[s], ids[d], t_a, t_b, as_of, 5, h, 10**9, rounds=True)
                    w = kern.paths_topk(ids[s], ids[d], t_a, t_b, as_of, 5, h, 10**9, rounds=False)
                    modes.add(r["by_rounds"])
                    assert not w["by_rounds"] or s == d
                    for key in ("total", "arrival", "hops", "src_id", "dst_id", "vt_s",
                                "eid", "rel_type", "exceeded"):
                        assert r[key] == w[key], (s, d, t_a, t_b, h, key)
    assert modes == {True, False}  # both readings were exercised


def test_an_open_write_batch_keeps_the_enumeration(tmp_path):
    st = gnomon.open(tmp_path / "s", backend="native")
    a = st.adapter
    a.begin()
    a.apply_ops([{"op": "ingest_events", "offset": 0, "node_label": "Node", "events": [
        {"src": "a", "dst": "b", "rel_type": "R", "vt_s": 1, "vt_e": 50},
        {"src": "b", "dst": "c", "rel_type": "R", "vt_s": 2, "vt_e": 50}]}], 1)
    a.commit()
    args = {"src": "a", "dst": "c", "window": {"t_a": 0, "t_b": 100}, "k": 5, "max_hops": 3}
    assert call_operator(a, "temporal_paths", dict(args))["rows_total"] == 1
    sid, did = (int(x) for x in a.dense_ids(["a", "c"]))
    assert a.paths_topk(sid, did, 0, 100, OPEN_END, 5, 3, 10**6)["total"] == 1
    a.begin()
    try:
        a.apply_ops([{"op": "assert_edge", "src": "a", "dst": "c", "rel_type": "R",
                      "props": {}, "vt_s": 3, "vt_e": 9, "disc": ""}], 2)
        # the engine's scan does not see staged rows: the operator keeps its
        # enumeration inside a batch
        assert a.paths_topk(sid, did, 0, 100, OPEN_END, 5, 3, 10**6) is None
        assert call_operator(a, "temporal_paths", dict(args))["rows_total"] >= 1
    finally:
        a.rollback()
    st.close()


# --------------------------------------------------------------------- #
# co_active                                                             #
# --------------------------------------------------------------------- #

def _co_cases(epoch: int) -> list[dict[str, Any]]:
    specs = [({"src": "n0"}, {"src": "n1"}), ({"src": "n1"}, {"src": "n0"}),
             ({"dst": "n0", "rel_type": "R"}, {"src": "n0"}),
             ({"src": "n0"}, {"src": "n0"}), ({"src": "n5"}, {"dst": "n1"})]
    rels = [{"relation": "overlaps"}, {"relation": "during"}, {"relation": "meets"},
            {"relation": "before", "gap": 30}]
    def case(i: int, rel: dict[str, Any], as_of: int = OPEN_END) -> dict[str, Any]:
        return {"a_spec": specs[i][0], "b_spec": specs[i][1], "allen_relation": rel,
                "as_of_tt": as_of}

    out = [case(i, rel) for i in range(4) for rel in rels[:2]]      # the hub joins
    out += [case(i, rel) for i in (0, 4) for rel in rels[2:]]
    out += [case(i, rels[0], epoch) for i in (0, 1)]
    out += [case(4, rel) for rel in rels[:2]] + [case(4, rel, epoch) for rel in rels]
    return out


def test_co_active_pages_are_the_oracles(dense):
    adapter, oracle, epoch = dense
    past_cap = 0
    for args in _co_cases(epoch):
        first = call_operator(adapter, "co_active", dict(args))
        total = first["rows_total"]
        past_cap += total > ops_series.PAIR_CAP
        # the oracle's whole join once, then its pages
        full = oracle.co_active({**first["args_echo"], "limit": 10**9, "cursor": None})
        assert full["rows_total"] == total, args
        for cursor, limit in [(None, 100), ("100", 7), (str(max(0, total - 3)), 10),
                              (str(total), 5), (str(total + 4), 5), ("0", 1)]:
            call = {**args, "cursor": cursor, "limit": limit}
            got = call_operator(adapter, "co_active", dict(call))
            want = paginate(full["rows"], limit, cursor)
            assert _payload(got) == canonical_json(want), call
    assert past_cap >= 4  # joins past the old cap were answered


def test_co_active_a_negative_cursor_keeps_slice_semantics(dense):
    adapter, oracle, _ = dense
    args = {"a_spec": {"src": "n5"}, "b_spec": {"dst": "n1"},
            "allen_relation": {"relation": "overlaps"}, "cursor": "-3", "limit": 2}
    got = call_operator(adapter, "co_active", dict(args))
    assert _payload(got) == canonical_json(oracle.co_active(got["args_echo"]))
