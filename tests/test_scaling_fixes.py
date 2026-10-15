"""Two queries whose work could follow the store's size follow the answer's.

The registry generator keeps the average
degree constant as the input grows, so a node's whole history is ~200 edge
versions at every size; only the instant edge set, the node set and the rows
per segment grow.

* **Fix B (engine)**: an incidence-filtered scan over a few nodes
  (`touching_ids`) takes each segment's adjacency postings and examines only
  the rows they name. `scan_edges` reports `rows_examined`,
  `adj_rows_walked` (posting rows walked) and `adj_segments` (segments that
  took postings). Forecast: `nbr.evolution` examines at most 1,000 rows.
* **Fix A (operator)**: `snapshot_subgraph` expands from its seeds, one
  filtered scan per hop and one both-endpoint scan for the induced edges,
  with node validity by identity point reads. It makes no unfiltered edge
  scan and no node scan. Forecast shape: its work grows at most 1.5x from
  the small store to one five times larger.

Switches (read at open, recorded by `designs()`): `GNOMON_SEED_EXPANSION=0`
turns fix A off, `GNOMON_ADJ_FILTERED_SCAN=0` fix B. Every answer is the same
either way; `tests/test_ablation_switches.py` holds the whole registry to
that on corrected stores.

Native engine only: the fixes are engine and native-adapter paths.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

import gnomon  # noqa: E402
from gnomon.core.model import OPEN_END  # noqa: E402
from gnomon.temporal.algebra import call_operator, ensure_all_registered  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
#: Two input sizes five times apart, both past the generator's floors
#: (`n_nodes`, `edge_life`), so the average degree is the same in both.
SMALL, LARGE = 20_000, 100_000
#: Every variable that changes which path these queries take.
VARS = ("GNOMON_SEED_EXPANSION", "GNOMON_ADJ_FILTERED_SCAN", "GNOMON_TCSR",
        "GNOMON_RANGE_VISIBILITY", "GNOMON_WINDOW_FIRST", "GNOMON_SEGMENT_PRUNE",
        "GNOMON_VIDFILTER", "GNOMON_EXISTENCE_PROBE", "GNOMON_TIME_CODEC")


def _harness():
    scripts_dir = str(REPO / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "eval_harness" in sys.modules:
        return sys.modules["eval_harness"]
    spec = importlib.util.spec_from_file_location("eval_harness",
                                                  REPO / "scripts" / "eval_harness.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def stores(tmp_path_factory) -> dict[int, tuple[Path, Any]]:
    """`{size: (store root, registry dataset)}`, size-tiered as the
    evaluation loads them, built with every variable unset."""
    H = _harness()
    out: dict[int, tuple[Path, Any]] = {}
    with pytest.MonkeyPatch.context() as mp:
        for k in VARS:
            mp.delenv(k, raising=False)
        base = tmp_path_factory.mktemp("w30")
        for n in (SMALL, LARGE):
            data = H.build_dataset(n)
            root = base / f"s{n}"
            H.load_store(root, "native", data.log)
            out[n] = (root, data)
    return out


class Spy:
    """Records every engine call an operator makes, with the counters the
    engine returns."""

    def __init__(self, inner: Any) -> None:
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "calls", [])

    def __getattr__(self, name: str) -> Any:
        f = getattr(self._inner, name)
        if not callable(f):
            return f

        def wrapped(*a: Any, **k: Any) -> Any:
            r = f(*a, **k)
            rec: dict[str, Any] = {"op": name, "kw": dict(k)}
            if isinstance(r, dict):
                for c in ("rows_examined", "adj_rows_walked", "adj_segments"):
                    if c in r:
                        rec[c] = int(r[c])
            self.calls.append(rec)
            return r

        return wrapped


def _open(path: Path, mp, env: dict[str, str] | None = None):
    for k in VARS:
        mp.delenv(k, raising=False)
    for k, v in (env or {}).items():
        mp.setenv(k, v)
    return gnomon.open(path, backend="native")


def _spied(store, op: str, args: dict[str, Any]) -> tuple[Any, list[dict[str, Any]]]:
    ensure_all_registered()
    ad = store.adapter
    real = ad._store
    spy = Spy(real)
    ad._store = spy
    try:
        out = call_operator(ad, op, dict(args), skip_cost_check=True)
    finally:
        ad._store = real
    return out, spy.calls


def _query(data, qid: str):
    H = _harness()
    reg = H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    return next(q for q in reg if q.id == qid)


def _work(calls: list[dict[str, Any]]) -> dict[str, int]:
    scans = [c for c in calls if c["op"] == "scan_edges"]
    out = {"scans": len(scans),
           "rows_examined": sum(c["rows_examined"] for c in scans),
           "adj_rows_walked": sum(c["adj_rows_walked"] for c in scans)}
    out["total"] = out["rows_examined"] + out["adj_rows_walked"]
    return out


# --------------------------------------------------------------------- #
# fix B: nbr.evolution's three filtered scans                           #
# --------------------------------------------------------------------- #


def test_neighborhood_evolution_examines_the_nodes_rows_not_the_stores(stores, monkeypatch):
    work = {}
    for n, (path, data) in stores.items():
        q = _query(data, "nbr.evolution")
        store = _open(path, monkeypatch)
        try:
            _, calls = _spied(store, q.op, q.args)
        finally:
            store.close()
        scans = [c for c in calls if c["op"] == "scan_edges"]
        assert len(scans) == 3, calls
        for c in scans:
            assert c["kw"].get("touching_ids"), c
            # falsifier: a filtered scan over one node that walked no postings
            assert c["adj_rows_walked"] > 0 and c["adj_segments"] > 0, (n, c)
        work[n] = _work(calls)
        # forecast: at most 1,000 rows examined over the three scans
        assert work[n]["rows_examined"] <= 1_000, (n, work[n])
    # forecast shape: the work follows the answer (flat), not the store (5x)
    for k in ("rows_examined", "adj_rows_walked"):
        assert work[LARGE][k] <= 1.5 * work[SMALL][k], (k, work)


# --------------------------------------------------------------------- #
# fix A: snap.hop2 expands from its seed                                #
# --------------------------------------------------------------------- #


def test_snapshot_subgraph_expands_from_its_seeds(stores, monkeypatch):
    work = {}
    for n, (path, data) in stores.items():
        q = _query(data, "snap.hop2")
        store = _open(path, monkeypatch)
        try:
            out, calls = _spied(store, q.op, q.args)
        finally:
            store.close()
        assert out["nodes_total"] > 10 and out["rows_total"] > 10, out
        ops = [c["op"] for c in calls]
        # falsifier: the whole graph's nodes, or an edge scan with no filter
        assert "scan_nodes" not in ops, ops
        scans = [c for c in calls if c["op"] == "scan_edges"]
        assert scans and all(c["kw"].get("touching_ids") for c in scans), calls
        # one scan per hop plus the induced edges
        assert len(scans) == q.args["hops"] + 1, calls
        assert all(c["adj_segments"] > 0 for c in scans), scans
        work[n] = _work(calls)
    # falsifier (design note): rows examined, or the scans' whole work, grow
    # more than 1.5x from one size to the next.
    #
    # The whole work (rows examined + adjacency rows walked), not
    # adjacency rows alone. A segment takes its postings only when they are
    # fewer than the rows the scan would examine there (`PostingsSource`'s
    # budget), and the derived event-lane staircase made that budget smaller:
    # on the small store some segments now scan their (cut) range instead of
    # walking postings, so adjacency rows alone fell from 10,623 to 262 there
    # while the whole work fell from 15,815 to 3,911 (large store: 4,470 to
    # 2,664). Adjacency rows alone then measure which of two substitutable
    # paths the cost rule chose, not whether the work follows the store.
    for k in ("rows_examined", "total"):
        assert work[LARGE][k] <= 1.5 * work[SMALL][k], (k, work)


def test_a_frontier_past_the_cap_takes_the_whole_snapshot_and_answers_the_same(
        stores, monkeypatch):
    from gnomon.temporal import ops_snapshot

    H = _harness()
    path, data = stores[SMALL]
    q = _query(data, "snap.hop2")
    store = _open(path, monkeypatch)
    try:
        ref, _ = _spied(store, q.op, q.args)
        monkeypatch.setattr(ops_snapshot, "SEED_EXPANSION_MAX_IDS", 5)
        got, calls = _spied(store, q.op, q.args)
    finally:
        store.close()
    assert H.canonical_hash(got) == H.canonical_hash(ref)
    # the fallback: the instant's whole edge set and node set
    assert any(c["op"] == "scan_edges" and not c["kw"].get("touching_ids") for c in calls)
    assert any(c["op"] == "scan_nodes" for c in calls)


def test_the_operator_cap_is_the_engines(stores):
    import gnomon._engine as e
    from gnomon.temporal import ops_snapshot

    assert ops_snapshot.SEED_EXPANSION_MAX_IDS == e.ADJ_FILTER_MAX_IDS


# --------------------------------------------------------------------- #
# answers: probes the registry lacks, under every configuration          #
# --------------------------------------------------------------------- #

CONFIGS: dict[str, dict[str, str]] = {
    "seed expansion off": {"GNOMON_SEED_EXPANSION": "0"},
    "filtered scan off": {"GNOMON_ADJ_FILTERED_SCAN": "0"},
    "both off": {"GNOMON_SEED_EXPANSION": "0", "GNOMON_ADJ_FILTERED_SCAN": "0"},
    "adjacency index off": {"GNOMON_TCSR": "scan"},
    "range visibility off": {"GNOMON_RANGE_VISIBILITY": "0"},
    "segment pruning off": {"GNOMON_SEGMENT_PRUNE": "0"},
}


def _probes(data) -> list[tuple[str, str, dict[str, Any]]]:
    mid = data.t0 + (data.t1 - data.t0) // 2
    span = data.t1 - data.t0
    out: list[tuple[str, str, dict[str, Any]]] = []
    for as_label, as_of in (("now", OPEN_END), ("epoch1", data.tt_epoch1)):
        for hops in (0, 1, 2, 3):
            out.append((f"snap n1 h{hops} {as_label}", "snapshot_subgraph",
                        {"seeds": ["n1"], "hops": hops, "t_valid": mid, "as_of_tt": as_of}))
        out.append((f"snap three seeds {as_label}", "snapshot_subgraph",
                    {"seeds": ["n1", "n2", "n7"], "hops": 2, "t_valid": mid + 17,
                     "as_of_tt": as_of}))
        out.append((f"snap rel S {as_label}", "snapshot_subgraph",
                    {"seeds": ["n1"], "hops": 2, "t_valid": mid, "rel_types": ["S"],
                     "as_of_tt": as_of}))
        out.append((f"snap page {as_label}", "snapshot_subgraph",
                    {"seeds": ["n3"], "hops": 2, "t_valid": mid, "limit": 7,
                     "as_of_tt": as_of}))
        out.append((f"snap past the extent {as_label}", "snapshot_subgraph",
                    {"seeds": ["n1"], "hops": 2, "t_valid": data.t1 + 10 * span,
                     "as_of_tt": as_of}))
        out.append((f"snap at the start {as_label}", "snapshot_subgraph",
                    {"seeds": ["n1", "n5"], "hops": 1, "t_valid": data.t0,
                     "as_of_tt": as_of}))
        for uid in ("n1", "n2"):
            for t1, t2 in ((mid, mid + span // 10), (data.t0, data.t0 + 3), (mid, data.t1)):
                out.append((f"nbr {uid} [{t1},{t2}) {as_label}", "neighborhood_evolution",
                            {"uid": uid, "t1": t1, "t2": t2, "as_of_tt": as_of}))
        out.append((f"hist n1 edges {as_label}", "entity_history",
                    {"uid": "n1", "include_edges": True, "as_of_tt": as_of}))
    return out


def _hashes(path: Path, data, env: dict[str, str], mp) -> tuple[dict[str, str], dict]:
    H = _harness()
    ensure_all_registered()
    store = _open(path, mp, env)
    try:
        ad = store.adapter
        out: dict[str, str] = {}
        for name, op, args in _probes(data):
            try:
                out[name] = H.canonical_hash(call_operator(ad, op, dict(args),
                                                           skip_cost_check=True))
            except Exception as ex:  # noqa: BLE001 -- an error is an answer too
                out[name] = f"error {type(ex).__name__}: {ex}"[:200]
        designs = ad.designs()
    finally:
        store.close()
    return out, designs


@pytest.mark.parametrize("config", sorted(CONFIGS))
def test_every_probe_answers_the_same_under_each_configuration(config, stores, monkeypatch):
    path, data = stores[SMALL]
    ref, designs = _hashes(path, data, {}, monkeypatch)
    assert designs["seed_expansion"] and designs["adjacency_filtered_scan"], designs
    assert sum(not h.startswith("error") for h in ref.values()) == len(ref), ref
    got, designs = _hashes(path, data, CONFIGS[config], monkeypatch)
    if "GNOMON_SEED_EXPANSION" in CONFIGS[config]:
        assert designs["seed_expansion"] is False, designs
    if "GNOMON_ADJ_FILTERED_SCAN" in CONFIGS[config]:
        assert designs["adjacency_filtered_scan"] is False, designs
    moved = {k: (ref[k], got[k]) for k in ref if ref[k] != got[k]}
    assert not moved, f"{config}: {len(moved)} answer(s) moved: {moved}"
