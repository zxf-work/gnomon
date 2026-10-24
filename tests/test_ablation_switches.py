"""Every ablation switch keeps every registry answer, and does work.

One switch turns one engine design off:

=====================  ==========================  =========================
design                 switch                      read or write time
=====================  ==========================  =========================
range visibility       GNOMON_RANGE_VISIBILITY=0     read, fixed at open
window-first           GNOMON_WINDOW_FIRST=0         read, fixed at open
segment pruning        GNOMON_SEGMENT_PRUNE=0        read, fixed at open
adjacency index        GNOMON_TCSR=scan              read, fixed at open
identity filter        GNOMON_VIDFILTER=ignore       read, fixed at open
believed-node count    GNOMON_EXISTENCE_PROBE=walk   ingest, fixed at open
time-column encoding   GNOMON_TIME_CODEC=raw|runs    write, per segment
seed-driven snapshot   GNOMON_SEED_EXPANSION=0       read, fixed at open
filtered scan postings GNOMON_ADJ_FILTERED_SCAN=0    read, fixed at open
count by range         GNOMON_AGG_RANGE_COUNT=0      read, fixed at open
reachability kernel    GNOMON_REACH_KERNEL=python    read, fixed at open
snapshot-diff kernel   GNOMON_DIFF_KERNEL=python     read, fixed at open
=====================  ==========================  =========================

For each switch this file asserts two things on real stores:

* **identity** -- every query of the evaluation registry, plus probes the
  registry lacks (windowed version history in three belief modes and at a
  past belief time, a whole-extent and a historical belief diff, aggregates
  at a past belief time), hashes exactly as it does with every design on,
  hashed the way `scripts/eval_harness.py` hashes;
* **work** -- an engine counter read through `phase_stats()` moves the way
  the switch says. A switch that silently does nothing fails here.

Each configuration opens a fresh handle with the environment set, because
the read-time switches are read once, at open; the handle's `designs()` is
asserted to say what was set.

The default run covers the reference store (the registry dataset's own
corrections) and the same store with 10 % of its edges corrected, size-
tiered and settled. `GNOMON_ABLATION_FULL=1` adds 0 % and 1 %, the
settled-before-corrections configuration, and every pair of switches.

Native engine only: the switches are engine knobs.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

import gnomon  # noqa: E402
from gnomon.core.model import OPEN_END  # noqa: E402
from gnomon.temporal.algebra import call_operator, ensure_all_registered  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
FULL = os.environ.get("GNOMON_ABLATION_FULL") == "1"
SCALE = 1_500

#: Every variable any switch reads; cleared before each configuration so the
#: caller's environment cannot leak into the reference.
ALL_VARS = ("GNOMON_RANGE_VISIBILITY", "GNOMON_WINDOW_FIRST", "GNOMON_SEGMENT_PRUNE",
            "GNOMON_TCSR", "GNOMON_VIDFILTER", "GNOMON_EXISTENCE_PROBE",
            "GNOMON_TIME_CODEC", "GNOMON_SEED_EXPANSION", "GNOMON_ADJ_FILTERED_SCAN",
            "GNOMON_AGG_RANGE_COUNT", "GNOMON_REACH_KERNEL", "GNOMON_DIFF_KERNEL",
            "GNOMON_MATCH_KERNEL")

#: switch name -> (environment, the `designs()` flag it turns off)
SWITCHES: dict[str, tuple[dict[str, str], str]] = {
    "range_visibility": ({"GNOMON_RANGE_VISIBILITY": "0"}, "range_visibility"),
    "window_first": ({"GNOMON_WINDOW_FIRST": "0"}, "window_first"),
    "segment_prune": ({"GNOMON_SEGMENT_PRUNE": "0"}, "segment_prune"),
    "adjacency_index": ({"GNOMON_TCSR": "scan"}, "adjacency_index"),
    "identity_filter": ({"GNOMON_VIDFILTER": "ignore"}, "identity_filter"),
    "seed_expansion": ({"GNOMON_SEED_EXPANSION": "0"}, "seed_expansion"),
    "adj_filtered_scan": ({"GNOMON_ADJ_FILTERED_SCAN": "0"}, "adjacency_filtered_scan"),
    "agg_range_count": ({"GNOMON_AGG_RANGE_COUNT": "0"}, "agg_range_count"),
    "reach_kernel": ({"GNOMON_REACH_KERNEL": "python"}, "reach_kernel"),
    "diff_kernel": ({"GNOMON_DIFF_KERNEL": "python"}, "diff_kernel"),
    "match_kernel": ({"GNOMON_MATCH_KERNEL": "python"}, "match_kernel"),
}


def _load(name: str):
    scripts_dir = str(REPO / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, REPO / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------- #
# stores                                                                #
# --------------------------------------------------------------------- #


def _settle(path: Path) -> None:
    store = gnomon.open(path, backend="native")
    try:
        store.adapter.settle()
    finally:
        store.close()


def _inject(path: Path, pct: float) -> None:
    _load("inject_corrections").inject(path, pct, seed=0, chunk_rows=50_000,
                                       progress_every=0, dry_run=False, batch=50)


@pytest.fixture(scope="module")
def stores(tmp_path_factory) -> dict[str, tuple[Path, Any]]:
    """`{name: (store root, registry dataset)}`, built with every variable
    unset."""
    H = _load("eval_harness")
    out: dict[str, tuple[Path, Any]] = {}
    with pytest.MonkeyPatch.context() as mp:
        for k in ALL_VARS:
            mp.delenv(k, raising=False)
        base = tmp_path_factory.mktemp("ablation")
        data = H.build_dataset(SCALE)
        ref = base / "ref-tiered"
        H.load_store(ref, "native", data.log)
        out["ref-tiered"] = (ref, data)
        for pct in ((1, 10) if FULL else (10,)):
            tiered = base / f"c{pct}-tiered"
            shutil.copytree(ref, tiered)
            _inject(tiered, pct)
            out[tiered.name] = (tiered, data)
            settled = base / f"c{pct}-settled"
            shutil.copytree(tiered, settled)
            _settle(settled)
            out[settled.name] = (settled, data)
            if FULL:
                before = base / f"c{pct}-settled-before"
                shutil.copytree(ref, before)
                _settle(before)
                _inject(before, pct)
                out[before.name] = (before, data)
        if FULL:
            # no correction at all: the dataset's events and n1's name only
            c0 = base / "c0-tiered"
            s = gnomon.open(c0, backend="native")
            s.ingest_events([H._event(i, SCALE) for i in range(SCALE)])
            s.assert_node("n1", "Node", {"name": "alpha"}, vt_s=0, vt_e=SCALE)
            s.close()
            out["c0-tiered"] = (c0, H.dataset_from_log(c0 / "eventlog.jsonl"))
    return out


# --------------------------------------------------------------------- #
# the instrument                                                        #
# --------------------------------------------------------------------- #


def _queries(H, data) -> list[tuple[str, Any]]:
    """The registry, plus the probes the registry lacks."""
    reg = H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    out: list[tuple[str, Any]] = []
    for q in reg:
        if q.op == "lookup_name":
            out.append((q.id, lambda ad, q=q: H._lookup_name_dispatch(ad, q.args)))
        elif q.op == "diff_belief":
            out.append((q.id, lambda ad, q=q: H._diff_belief_dispatch(ad, q.args)))
        else:
            out.append((q.id, lambda ad, q=q: call_operator(
                ad, q.op, dict(q.args), skip_cost_check=True)))
    w10 = H._fixed_window(data.t0, data.t1)
    whole = {"t_a": data.t0, "t_b": data.t1}
    for belief in ("current", "superseded", "all"):
        for label, as_of in (("now", OPEN_END), ("epoch1", data.tt_epoch1)):
            for wl, w in (("w10", w10), ("whole", whole)):
                out.append((f"vh.{belief}.{label}.{wl}", lambda ad, b=belief, a=as_of, w=w: (
                    call_operator(ad, "version_history",
                                  {"kind": "edge", "window": dict(w), "belief": b,
                                   "as_of_tt": a, "limit": 10_000},
                                  skip_cost_check=True))))
    for label, t2 in (("now", OPEN_END), ("epoch1+", data.tt_epoch1 + 1)):
        out.append((f"diff.belief.whole.{label}", lambda ad, t2=t2: H._diff_belief_dispatch(
            ad, {"t1": 0, "t2": t2, "window": dict(whole), "limit": 100_000})))
    by_id = {q.id: q for q in reg}
    for qid in ("series.count", "agg.rel_bucket", "series.count.w10", "burst.zscore"):
        q = by_id[qid]
        args = {**q.args, "as_of_tt": data.tt_epoch1}
        out.append((f"{qid}.epoch1", lambda ad, args=args, q=q: call_operator(
            ad, q.op, dict(args), skip_cost_check=True)))
    # the shapes the reachability and snapshot-diff kernels answer
    # beyond the registry's own calls -- both other directions, a later
    # page, a past belief time; the diff backwards, paged small, in the past
    rq, dq = by_id["reach.window"], by_id["diff.global"]
    for label, extra in (("in", {"direction": "in"}), ("both", {"direction": "both"}),
                         ("page2", {"limit": 7, "cursor": "5"}),
                         ("epoch1", {"as_of_tt": data.tt_epoch1})):
        args = {**rq.args, **extra}
        out.append((f"reach.window.{label}", lambda ad, args=args: call_operator(
            ad, "temporal_reachability", dict(args), skip_cost_check=True)))
    for label, extra in (("backwards", {"t1": dq.args["t2"], "t2": dq.args["t1"]}),
                         ("limit3", {"limit": 3}),
                         ("epoch1", {"as_of_tt": data.tt_epoch1})):
        args = {**dq.args, **extra}
        out.append((f"diff.global.{label}", lambda ad, args=args: call_operator(
            ad, "diff_snapshots", dict(args), skip_cost_check=True)))
    return out


def _answers(path: Path, data, env: dict[str, str], mp) -> dict[str, Any]:
    """Every query once on a fresh handle opened under `env`: hashes, the
    handle's designs, and the engine counters over the whole pass."""
    H = _load("eval_harness")
    ensure_all_registered()
    for k in ALL_VARS:
        mp.delenv(k, raising=False)
    for k, v in env.items():
        mp.setenv(k, v)
    store = gnomon.open(path, backend="native")
    ad = store.adapter
    queries = _queries(H, data)

    def run_all() -> dict[str, str]:
        hashes: dict[str, str] = {}
        for qid, run in queries:
            try:
                hashes[qid] = H.canonical_hash(run(ad))
            except Exception as e:  # noqa: BLE001 -- an error is an answer too
                hashes[qid] = f"error {type(e).__name__}: {e}"[:200]
        return hashes

    try:
        designs = ad.designs()
        # The first pass opens every segment; an open folds a run-list
        # segment's tt_s bounds, which the run counter rightly records, but it
        # is not a belief test. The counters are read over the second pass,
        # on a warm handle, which must also give the first pass's answers --
        # except adjacency, which `NativeTCSR` caches per handle, so the
        # traversal's own work is read off the first pass.
        ad.reset_phase_stats()
        hashes = run_all()
        first = ad.phase_stats()
        ad.reset_phase_stats()
        again = run_all()
        assert again == hashes, "a second pass on the same handle answered differently"
        phase = ad.phase_stats()
        tcsr = ad.tcsr_stats()
        vidfilter = ad.vidfilter_stats()
    finally:
        store.close()
    return {"hashes": hashes, "designs": designs, "phase": phase, "first": first, "tcsr": tcsr,
            "vidfilter": vidfilter}


_REFERENCE: dict[str, dict[str, Any]] = {}


def _reference(name: str, stores, mp) -> dict[str, Any]:
    if name not in _REFERENCE:
        path, data = stores[name]
        got = _answers(path, data, {}, mp)
        assert all(got["designs"].values()), f"unset variables turned a design off: {got['designs']}"
        answered = [q for q, h in got["hashes"].items() if not h.startswith("error")]
        assert len(answered) >= 30, f"{name}: only {len(answered)} queries answered: {got['hashes']}"
        _REFERENCE[name] = got
    return _REFERENCE[name]


def _store_names(stores) -> list[str]:
    return sorted(stores)


def _range_items(phase: dict[str, int]) -> int:
    return phase["vis_codes"] + phase["vis_runs"] + phase["vis_closes"]


def _assert_identical(name: str, label: str, ref: dict, got: dict) -> None:
    assert set(got["hashes"]) == set(ref["hashes"])
    moved = {q: (ref["hashes"][q], got["hashes"][q]) for q in ref["hashes"]
             if ref["hashes"][q] != got["hashes"][q]}
    assert not moved, f"{name} under {label}: {len(moved)} answer(s) moved: {moved}"


# --------------------------------------------------------------------- #
# one switch at a time                                                  #
# --------------------------------------------------------------------- #


@pytest.mark.parametrize("switch", sorted(SWITCHES))
def test_each_switch_keeps_every_answer_and_changes_the_work(switch, stores, monkeypatch):
    env, flag = SWITCHES[switch]
    for name in _store_names(stores):
        path, data = stores[name]
        ref = _reference(name, stores, monkeypatch)
        got = _answers(path, data, env, monkeypatch)
        assert got["designs"][flag] is False, f"{env} did not turn {flag} off: {got['designs']}"
        assert sum(not v for v in got["designs"].values()) == 1, got["designs"]
        _assert_identical(name, str(env), ref, got)

        p_on, p_off = ref["phase"], got["phase"]
        if switch == "range_visibility":
            # falsifier: no row tested one by one, or any range item read
            assert p_on["vis_rows"] == 0, p_on
            assert p_off["vis_rows"] > 0, f"{name}: no per-row belief test ran: {p_off}"
            assert _range_items(p_off) == 0, f"{name}: range-domain items read: {p_off}"
        elif switch == "window_first":
            # falsifier: the whole-segment path read no more than window-first
            assert _range_items(p_off) >= _range_items(p_on), (name, p_on, p_off)
            if "settled" in name:
                assert _range_items(p_off) > _range_items(p_on), (
                    f"{name}: GNOMON_WINDOW_FIRST=0 read {_range_items(p_off)} items, "
                    f"window-first {_range_items(p_on)}")
        elif switch == "segment_prune":
            # falsifier: anything pruned with the switch set
            assert p_off["seg_pruned"] == 0, f"{name}: pruned {p_off['seg_pruned']}"
            if "tiered" in name:
                assert p_on["seg_pruned"] > 0, f"{name}: nothing to prune: {p_on}"
        elif switch == "adjacency_index":
            from gnomon.storage.native.adapter import tcsr_backend
            assert tcsr_backend() == "scan", "the adapter must report the backend it ran"
            # falsifier: an index loaded or built, or no endpoint row scanned
            assert got["tcsr"]["built"] + got["tcsr"]["loaded"] == 0, got["tcsr"]
            f_on, f_off = ref["first"], got["first"]
            assert f_off["adj_scan_rows"] > 0, f"{name}: no adjacency scan: {f_off}"
            assert f_on["adj_scan_rows"] == 0, f_on
            # the filtered scans take the scanned adjacency instead
            assert p_off["sel_adj_rows"] > 0, f"{name}: no filtered scan took postings: {p_off}"
        elif switch == "identity_filter":
            # falsifier: a filter probed with the switch set
            assert ref["vidfilter"]["probes"] > 0, f"{name}: the filter is never consulted"
            assert got["vidfilter"]["probes"] == 0, f"{name}: probed {got['vidfilter']}"
        elif switch == "seed_expansion":
            # falsifier: the whole-snapshot path examined no more rows
            assert p_off["sel_rows_examined"] > p_on["sel_rows_examined"], (name, p_on, p_off)
        elif switch == "adj_filtered_scan":
            # falsifier: postings walked with the switch set, or none without
            assert p_on["sel_adj_rows"] > 0, f"{name}: no filtered scan took postings: {p_on}"
            assert p_off["sel_adj_rows"] == 0, f"{name}: postings walked: {p_off}"
            assert p_off["sel_rows_examined"] > p_on["sel_rows_examined"], (name, p_on, p_off)
        elif switch == "agg_range_count":
            # falsifier: a count answered by ranges with the switch set, or
            # none without it
            assert p_on["agg_range_counts"] > 0, f"{name}: no count took ranges: {p_on}"
            assert p_off["agg_range_counts"] == 0, f"{name}: ranges used: {p_off}"
        elif switch == "reach_kernel":
            assert p_on["reach_search_ns"] > 0, f"{name}: the kernel never ran: {p_on}"
            assert p_off["reach_search_ns"] == 0, f"{name}: the kernel ran: {p_off}"
        elif switch == "diff_kernel":
            assert p_on["snap_edge_plan_ns"] > 0, f"{name}: the kernel never ran: {p_on}"
            assert p_off["snap_edge_plan_ns"] == 0, f"{name}: the kernel ran: {p_off}"


@pytest.mark.skipif(not FULL, reason="the cross product runs under GNOMON_ABLATION_FULL=1")
def test_every_pair_of_switches_and_all_together_keep_every_answer(stores, monkeypatch):
    names = sorted(SWITCHES)
    combos = [(a, b) for i, a in enumerate(names) for b in names[i + 1:]]
    combos.append(tuple(names))
    for name in _store_names(stores):
        path, data = stores[name]
        ref = _reference(name, stores, monkeypatch)
        for combo in combos:
            env: dict[str, str] = {}
            for s in combo:
                env.update(SWITCHES[s][0])
            got = _answers(path, data, env, monkeypatch)
            assert not any(got["designs"][SWITCHES[s][1]] for s in combo), got["designs"]
            _assert_identical(name, str(env), ref, got)


# --------------------------------------------------------------------- #
# the write-time switch: GNOMON_TIME_CODEC                                #
# --------------------------------------------------------------------- #


def _segment_formats(root: Path) -> list[int]:
    """The preamble format of every live and superseded segment file."""
    return [int.from_bytes(p.read_bytes()[4:8], "little")
            for p in sorted((root / "native").rglob("*.tgs"))]


@pytest.mark.parametrize("codec", ["raw", "runs"])
def test_a_store_written_under_another_time_codec_answers_identically(
        codec, stores, tmp_path, monkeypatch):
    """The same event log replayed (replay keeps transaction times) with
    `GNOMON_TIME_CODEC` set, size-tiered and then settled under the same
    setting, answers every query exactly as the stores written with it
    unset. Raw segments are format 2; with the variable unset none is."""
    pairs = [("c10-tiered", False), ("c10-settled", True)]
    if FULL:
        pairs += [("c1-tiered", False), ("c1-settled", True)]
    for name, settle in pairs:
        src, data = stores[name]
        ref = _reference(name, stores, monkeypatch)
        src_formats = _segment_formats(src)
        assert src_formats and 2 not in src_formats, f"{name}: formats {set(src_formats)}"
        root = tmp_path / f"{name}-{codec}"
        for k in ALL_VARS:
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("GNOMON_TIME_CODEC", codec)
        # the registry log, then the store's own log (the injected
        # corrections; the registry log was replayed, so it is not in it)
        _replay(root, [data.log, src / "eventlog.jsonl"])
        if settle:
            _settle(root)
        if codec == "raw":
            live = _live_formats(root)
            assert live and set(live) == {2}, f"{name}: live segment formats {live}"
        else:
            assert set(_segment_formats(root)) == {1}
        monkeypatch.delenv("GNOMON_TIME_CODEC")
        got = _answers(root, data, {}, monkeypatch)
        assert all(got["designs"].values())
        _assert_identical(name, f"GNOMON_TIME_CODEC={codec}", ref, got)
        if codec == "raw":
            # a raw column has no block bounds: where a belief time splits a
            # segment (the settled store's mixed tt_s) rows are read one by
            # one, and no dictionary code is ever read
            assert got["phase"]["vis_codes"] == 0, got["phase"]
            if settle:
                assert got["phase"]["vis_rows"] > 0, f"{name}: no raw tt_s value read"


def _replay(root: Path, logs: list[Path]) -> None:
    """Replay `logs` in order into a new store, preserving transaction times,
    with compaction armed as `eval_harness.load_store` arms it."""
    from gnomon.storage.eventlog import replay

    store = gnomon.open(root, backend="native")
    try:
        for log in logs:
            replay(log, store.adapter, compact_trigger=True)
    finally:
        store.close()


def _live_formats(root: Path) -> list[int]:
    """Formats of the segments the current generation names."""
    store = gnomon.open(root, backend="native", read_only=True)
    try:
        files = [e["file"] for e in store.adapter.segment_layout()]
    finally:
        store.close()
    return [int.from_bytes((root / "native" / f).read_bytes()[4:8], "little") for f in files]


# --------------------------------------------------------------------- #
# the ingest-time switch: GNOMON_EXISTENCE_PROBE                          #
# --------------------------------------------------------------------- #


def test_ingest_with_the_existence_walk_writes_the_same_content(tmp_path, monkeypatch):
    """Bulk ingest asks which endpoints already have a believed node
    version; `GNOMON_EXISTENCE_PROBE=walk` answers by the walk instead of the
    believed-node count. Two ingests of the same events (transaction times
    are wall-clock, so they are compared on the layout- and clock-free
    content digest) must store the same content."""
    H = _load("eval_harness")
    events = [H._event(i, SCALE) for i in range(SCALE)]
    digests = {}
    for label, env in (("count", {}), ("walk", {"GNOMON_EXISTENCE_PROBE": "walk"})):
        for k in ALL_VARS:
            monkeypatch.delenv(k, raising=False)
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        s = gnomon.open(tmp_path / label, backend="native")
        try:
            assert s.adapter.designs()["existence_count"] is (label == "count")
            for i in range(0, SCALE, 250):
                s.ingest_events(events[i:i + 250])
            digests[label] = s.adapter.content_digest()
        finally:
            s.close()
    assert digests["count"] == digests["walk"]
