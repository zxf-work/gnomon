"""The belief diff, through the adapter and against the brute-force oracle.

`diff_belief` answers the dual of `diff_snapshots`: two belief times and one
valid-time window, rather than one belief time and two valid-time instants.
The engine computes it per segment in the *range* domain — the `added` set is
the difference of two `tt_s`-run verdict lists, the `retracted` set a filter
of the segment's folded close slice — and never looks at a version. Every
check here is therefore written against `gnomon.temporal.oracle.Oracle`, which
restates the definition one predicate at a time over the full version
population: the two share no code, so an agreement between them is evidence
rather than a tautology.

The `crates/gnomon-core/src/beliefdiff.rs` unit tests cover the range
algebra itself (that the one-pass `added_ranges` really is the difference of
the two verdict lists, and that neither `settle` nor `compact_incremental`
moves an answer). This file covers everything above the PyO3 boundary: the
adapter contract, the unsupported verdict on the other backends, the harness
registry entry, and agreement at scale.

    GNOMON_DIFF_BELIEF_SCALE=100000 uv run pytest tests/test_diff_belief.py

is the measurement-host form of the scaled agreement check; the default is
small so the everyday suite stays fast (the `GNOMON_HYP_EXAMPLES` convention).
"""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

import gnomon
from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END, EntityRef
from gnomon.temporal.oracle import Oracle

ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, relpath: str) -> Any:
    # Same shape as the synth-store builder's own tests: the scripts do
    # unqualified `import <other_script>` internally, so scripts/ has to be on
    # sys.path for that to resolve the way it does under `python scripts/<name>.py`.
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


B = _load_module("build_synth_store", "scripts/build_synth_store.py")
H = _load_module("eval_harness", "scripts/eval_harness.py")

#: The scaled agreement check's size. 100_000 is the figure the design note
#: names and what the measurement host runs; the default is small because a
#: 100k build commits one write batch per correction in its epoch-2 phases and
#: takes tens of seconds, which does not belong in the everyday suite.
SCALE = int(os.environ.get("GNOMON_DIFF_BELIEF_SCALE", "3000"))

#: Enough to hold every version of the fixtures below, so a test that is not
#: about the window is never silently filtered by one.
WIDE = (-(1 << 60), 1 << 60)

LIMIT = 1_000_000


def _oracle(adapter: Any) -> Oracle:
    return Oracle(list(adapter.all_node_versions()), list(adapter.all_edge_versions()))


def _agree(adapter: Any, t1: int, t2: int, t_a: int, t_b: int,
           limit: int = LIMIT) -> dict[str, Any]:
    """Assert the engine and the oracle give the identical answer; return it."""
    got = adapter.diff_belief(t1, t2, t_a, t_b, limit)
    want = _oracle(adapter).diff_belief(
        {"t1": t1, "t2": t2, "t_a": t_a, "t_b": t_b, "limit": limit})
    if got != want:                                  # a readable failure
        g = [tuple(sorted(r.items())) for r in got["rows"]]
        w = [tuple(sorted(r.items())) for r in want["rows"]]
        raise AssertionError(
            f"diff_belief({t1}, {t2}, [{t_a}, {t_b})) disagrees with the oracle\n"
            f"  totals engine={got['added_total']}/{got['retracted_total']} "
            f"oracle={want['added_total']}/{want['retracted_total']}\n"
            f"  only in engine: {[x for x in g if x not in w][:4]}\n"
            f"  only in oracle: {[x for x in w if x not in g][:4]}")
    return got


@pytest.fixture(scope="module")
def corrected_store():
    """A small store carrying every correction shape the engine can record.

    Bulk ingest, a node assertion that fixes a belief boundary, whole-interval
    corrections, a *partial* correction (which splits one interval into two
    versions with different props — the shape a whole-interval correction
    cannot produce), a retraction, and one version written and corrected in
    consecutive transactions, which is the *ephemeral* case: believed over so
    short a span that a belief window can contain its whole life.
    """
    path = Path(tempfile.mkdtemp()) / "corrected"
    store = gnomon.open(path, backend="native")
    n = 60
    store.ingest_events([
        {"src": f"n{i % 9}", "dst": f"n{(i + 4) % 9}", "rel_type": "R",
         "disc": f"#{i}", "vt_s": i, "vt_e": i + 6, "props": {"w": i}}
        for i in range(n)])
    epoch1 = store.assert_node("n1", "Node", {"name": "alpha"}, vt_s=0, vt_e=n)

    def ref(i: int) -> EntityRef:
        return EntityRef(kind="edge", src=f"n{i % 9}", dst=f"n{(i + 4) % 9}",
                         rel_type="R", disc=f"#{i}")

    for i in range(0, n, 7):                          # whole-interval
        store.correct(ref(i), {"w": 900 + i}, vt_s=i, vt_e=i + 6)
    epoch2 = store.assert_node("n1", "Node", {"name": "beta"}, vt_s=0, vt_e=n)
    for i in range(1, n, 11):                         # partial: splits the interval
        store.correct(ref(i), {"w": 700 + i}, vt_s=i + 3, vt_e=i + 6)
    for i in range(3, n, 17):
        store.retract(ref(i), i + 2)

    # The ephemeral pair. The two markers bracket it in transaction time:
    # `before_ephem` is stamped *before* the row is written and `after_ephem`
    # after it is corrected, so the belief window `(before_ephem, after_ephem]`
    # contains that version's whole believed life — both its `tt_s` and its
    # `tt_e`. Stamping the first marker after the write instead would put the
    # arrival outside the window and test nothing.
    before_ephem = store.assert_node("marker", "Node", {}, vt_s=0, vt_e=n)
    store.ingest_events([{"src": "e1", "dst": "e2", "rel_type": "R",
                          "disc": "#ephem", "vt_s": 5, "vt_e": 9,
                          "props": {"w": 1}}])
    ephem_ref = EntityRef(kind="edge", src="e1", dst="e2", rel_type="R",
                          disc="#ephem")
    store.correct(ephem_ref, {"w": 2}, vt_s=5, vt_e=9)
    after_ephem = store.assert_node("marker2", "Node", {}, vt_s=0, vt_e=n)

    yield {"store": store, "adapter": store.adapter, "n": n,
           "epoch1": epoch1, "epoch2": epoch2,
           "before_ephem": before_ephem, "after_ephem": after_ephem}
    store.close()


# --------------------------------------------------------------------------- #
# agreement with the brute-force reference                                     #
# --------------------------------------------------------------------------- #

def test_every_belief_pair_agrees_with_the_brute_force_reference(corrected_store):
    """The engine's range-domain answer equals the oracle's per-version one.

    Swept over pairs that bracket every belief boundary the fixture has — the
    two node-assertion epochs and the ephemeral pair's two sides — because the
    range algebra's short-circuits (`tt_max <= t1`, `tt_min > t2`, and the
    fully-inside case) are selected by exactly those comparisons, so a sweep
    that never straddles a boundary would take one branch every time.
    """
    f = corrected_store
    boundaries = [0, f["epoch1"], f["epoch2"], f["before_ephem"],
                  f["after_ephem"], OPEN_END]
    for i, t1 in enumerate(boundaries):
        for t2 in boundaries[i:]:
            _agree(f["adapter"], t1, t2, *WIDE)


def test_the_valid_time_window_agrees_too(corrected_store):
    f = corrected_store
    n = f["n"]
    for t_a, t_b in [(0, n), (0, 1), (n // 3, n // 3 + 5), (5, 9), (n, n + 50),
                     (-100, 0), (7, 7)]:
        _agree(f["adapter"], 0, OPEN_END, t_a, t_b)


def test_a_scaled_store_agrees_with_the_reference():
    """Agreement on a store built by the campaign's own builder.

    `--named-fraction 0.1`, seed 0 and the trigger-driven compaction cadence
    are this project's measurement-store settings, so this is the same corpus the
    latency numbers are taken on, at whatever scale `GNOMON_DIFF_BELIEF_SCALE`
    selects. The builder's own epoch-2 phases supply the corrections; nothing
    here injects any, because the point is to check the engine against the
    reference on a store nobody shaped for the test.
    """
    out = Path(tempfile.mkdtemp()) / f"synth-{SCALE}"
    B.build(out, SCALE, 0, 250, 0, "native", False, None, "none",
            tick_us=1, interval_fraction=0.0, interval_length_dist="pow2",
            short_life_us=None, named_fraction=0.1)
    store = gnomon.open(out, backend="native")
    try:
        a = store.adapter
        # The whole belief history, then a window narrow enough that the
        # answer is a handful of rows: the wide case exercises the output
        # path, the narrow one the per-segment arithmetic that survives it.
        wide = _agree(a, 0, OPEN_END, *WIDE)
        assert wide["added_total"] > 0 and wide["retracted_total"] > 0, (
            "the builder's own corrections must reach this store, or the "
            "comparison is between two empty answers")
        _agree(a, 0, OPEN_END, SCALE // 4, SCALE // 4 + 50)
        _agree(a, 0, OPEN_END, 0, SCALE)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the semantics a set difference cannot express                                #
# --------------------------------------------------------------------------- #

def test_an_ephemeral_version_is_reported_under_both_tags(corrected_store):
    """A version written and corrected inside one belief window changed state
    twice, and both changes are reported.

    It is in neither `believed(t1)` nor `believed(t2)`, so a set difference of
    the two populations drops it silently. This is the one place where the
    operator's contract is *not* that difference, and it is pinned here as
    well as in the Rust unit tests because it is the thing a reimplementation
    would get wrong.
    """
    f = corrected_store
    out = _agree(f["adapter"], f["before_ephem"], f["after_ephem"], *WIDE)
    ephem = [r for r in out["rows"]
             if r["kind"] == "edge" and r["vt_s"] == 5 and r["vt_e"] == 9]
    assert ephem, "the ephemeral fixture produced no rows at all"
    # exactly one *version* carries both tags: the one superseded inside the
    # window. Its replacement was only added, and is the control.
    by_vid: dict[str, set[str]] = {}
    for r in ephem:
        by_vid.setdefault(r["vid"], set()).add(r["change"])
    both = [v for v, c in by_vid.items() if c == {"added", "retracted"}]
    assert len(both) == 1, (
        f"exactly one version should have changed state twice, got {by_vid}")
    # and it is invisible to a set difference of the two believed populations
    for as_of in (f["before_ephem"], f["after_ephem"]):
        live = {v.vid for v in f["adapter"].all_edge_versions()
                if v.believed_at(as_of)}
        assert both[0] not in live, (
            "the ephemeral version must be in neither believed population — "
            "that is what makes a set difference unable to report it")
    # the arrival is reported before the retraction, and at different tt
    pair = sorted((r["tt"], r["change"]) for r in ephem if r["vid"] == both[0])
    assert [c for _, c in pair] == ["added", "retracted"]
    assert pair[0][0] < pair[1][0]


def test_an_empty_belief_window_is_empty_not_an_error(corrected_store):
    f = corrected_store
    for t in (0, f["epoch1"], f["epoch2"], OPEN_END):
        out = f["adapter"].diff_belief(t, t, *WIDE, LIMIT)
        assert out == {"rows": [], "added_total": 0, "retracted_total": 0,
                       "truncated": False}


def test_the_arguments_are_validated(corrected_store):
    a = corrected_store["adapter"]
    with pytest.raises(InvalidArgError):
        a.diff_belief(100, 1, *WIDE, LIMIT)
    with pytest.raises(InvalidArgError):
        a.diff_belief(0, 100, 50, 10, LIMIT)


def test_paging_caps_the_rows_and_never_the_totals(corrected_store):
    f = corrected_store
    whole = f["adapter"].diff_belief(0, OPEN_END, *WIDE, LIMIT)
    assert not whole["truncated"]
    for limit in (0, 1, 5, len(whole["rows"]) - 1):
        page = f["adapter"].diff_belief(0, OPEN_END, *WIDE, limit)
        assert page["rows"] == whole["rows"][:limit]
        assert page["truncated"] is True
        assert page["added_total"] == whole["added_total"]
        assert page["retracted_total"] == whole["retracted_total"]


def test_the_answer_is_totally_ordered(corrected_store):
    rows = corrected_store["adapter"].diff_belief(0, OPEN_END, *WIDE, LIMIT)["rows"]
    assert len(rows) > 4
    keys = [(0 if r["kind"] == "node" else 1, r["key"], r["tt"], r["change"],
             r["vid"]) for r in rows]
    assert keys == sorted(keys)
    assert len(set(keys)) == len(keys), "the order must be total, not merely sorted"


# --------------------------------------------------------------------------- #
# layout independence, through the adapter                                     #
# --------------------------------------------------------------------------- #

def test_settle_does_not_move_the_answer():
    """A settled store answers identically to the tiered one it was built as.

    The Rust tests make this claim about the engine's own `settle`; this one
    makes it about the whole stack the measurement runs through, because the
    this test compares tiered against settled latency and a
    latency comparison between two different *answers* would be meaningless.
    """
    path = Path(tempfile.mkdtemp()) / "settle-me"
    store = gnomon.open(path, backend="native")
    store.ingest_events([
        {"src": f"n{i % 5}", "dst": f"n{(i + 2) % 5}", "rel_type": "R",
         "disc": f"#{i}", "vt_s": i, "vt_e": i + 3, "props": {"w": i}}
        for i in range(120)])
    for i in range(0, 120, 9):
        store.correct(EntityRef(kind="edge", src=f"n{i % 5}",
                                dst=f"n{(i + 2) % 5}", rel_type="R",
                                disc=f"#{i}"), {"w": -i}, vt_s=i, vt_e=i + 3)
    a = store.adapter
    probes = [(0, OPEN_END, *WIDE), (0, OPEN_END, 10, 20)]
    before = [a.diff_belief(*p, LIMIT) for p in probes]
    assert any(b["added_total"] for b in before)
    a.settle(None)
    assert [a.diff_belief(*p, LIMIT) for p in probes] == before
    a.compact_incremental(None)
    assert [a.diff_belief(*p, LIMIT) for p in probes] == before
    store.close()


# --------------------------------------------------------------------------- #
# the other backends, and the harness                                          #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("backend", ["duckdb"])
def test_other_backends_report_unsupported_rather_than_emulating(backend):
    """the evaluation notes' **unsupported** verdict, not a slow number.

    A four-column emulation of the belief diff is two belief-filtered scans
    and an anti-join over a version table that grows with every correction —
    a different query with a different cost. Answering with it would make the
    comparison a comparison of two questions, so the base adapter raises and
    the harness records the refusal.
    """
    pytest.importorskip(backend)
    store = gnomon.open(Path(tempfile.mkdtemp()) / backend, backend=backend)
    try:
        with pytest.raises(NotImplementedError) as e:
            store.adapter.diff_belief(0, 10, 0, 10, 10)
        assert "diff_belief" in str(e.value)
        assert "unsupported" in str(e.value)
    finally:
        store.close()


def test_the_harness_registers_diff_belief_and_dispatches_it(corrected_store):
    """`diff.belief` is in the benchmark registry and runs through the
    harness-level bypass, with the fixed parameters the id promises."""
    queries = H.registry(0, 100, 42, ("n1",))
    ids = [q.id for q in queries]
    assert "diff.belief" in ids
    # appended after every pre-existing id, so prior receipts stay comparable
    assert ids.index("diff.belief") == len(ids) - 1
    q = queries[ids.index("diff.belief")]
    assert q.op == "diff_belief", "it is not a REGISTRY operator; see its note"
    assert q.args["t1"] == 42 and q.args["t2"] == OPEN_END
    assert q.args["window"] == H._w10_window(0, 100)

    f = corrected_store
    payload = H._diff_belief_dispatch(
        f["adapter"], {"t1": 0, "t2": OPEN_END,
                       "window": {"t_a": WIDE[0], "t_b": WIDE[1]},
                       "limit": LIMIT})
    assert payload == f["adapter"].diff_belief(0, OPEN_END, *WIDE, LIMIT)
    # `_answer_size` reads the two totals through its generic `*_total` branch
    assert H._answer_size(payload) == (payload["added_total"]
                                       + payload["retracted_total"])
    # and the answer hashes, so two systems could be compared on it
    assert H.canonical_hash(payload) == H.canonical_hash(
        f["adapter"].diff_belief(0, OPEN_END, *WIDE, LIMIT))


def test_diff_belief_is_not_a_registered_operator():
    """`diff_belief` is an engine capability, not part of the operator
    algebra: it is not in `REGISTRY` and runs through the harness bypass,
    exactly like `lookup_name`.
    """
    from gnomon.temporal.algebra import REGISTRY, ensure_all_registered
    ensure_all_registered()
    assert "diff_belief" not in REGISTRY
    assert "diff_snapshots" in REGISTRY, "the valid-time diff is still an operator"


# --------------------------------------------------------------------------- #
# the cost model, made observable                                              #
# --------------------------------------------------------------------------- #

def test_settle_trades_segment_count_for_tt_s_runs():
    """`segment_tt_profile` exposes the term `diff_belief`'s cost actually has.

    The operator is `O(segments x (runs + closes))`, and `settle` does not
    reduce both terms — it trades them. A tiered segment is one commit's rows,
    so its `tt_s` is a single run and `added_ranges`' folded `(tt_min, tt_max)`
    bounds decide the whole segment in two comparisons. `settle` merges
    everything into one global `(vt_s, vid)` order, which interleaves
    transaction times that were contiguous per commit and shatters those runs
    (an effect `read.rs` records). The bounds then say "mixed" for
    every segment and every run has to be walked.

    Measured on the development machine at 1M with 10 % corrections: the settled store answers
    from 3 segments in 0.799 ms while the tiered one answers from 139 in
    0.064 ms — fewer segments, twelve times slower. This test is the mechanism
    behind that number, at unit scale, and `runs_per_row` is the ratio a
    block-bounds `tt_s` representation would be scored against.
    """
    path = Path(tempfile.mkdtemp()) / "runs"
    store = gnomon.open(path, backend="native")
    # many small commits: each becomes its own segment with a single tt_s run
    for batch in range(24):
        store.ingest_events([
            {"src": f"n{(batch * 5 + i) % 11}", "dst": f"n{(batch * 5 + i + 3) % 11}",
             "rel_type": "R", "disc": f"#{batch}-{i}",
             "vt_s": (i * 24 + batch), "vt_e": (i * 24 + batch) + 2,
             "props": {"w": i}}
            for i in range(20)])
    a = store.adapter
    try:
        before = a._store.segment_tt_profile()
        edge_before = [r for r in before if r["lane"].startswith("edge")]
        assert len(edge_before) > 4, "the fixture must leave several edge segments"
        assert all(r["tt_s_runs"] == 1 for r in edge_before), (
            "a tiered segment is one commit, so it carries exactly one tt_s run: "
            f"{[(r['rows'], r['tt_s_runs']) for r in edge_before]}")

        a.settle(None)
        after = a._store.segment_tt_profile()
        edge_after = [r for r in after if r["lane"].startswith("edge")]
        assert len(edge_after) < len(edge_before), "settle must reduce the segment count"
        # ... and pay for it in runs
        assert max(r["tt_s_runs"] for r in edge_after) > max(
            r["tt_s_runs"] for r in edge_before), (
            "settle must shatter the tt_s runs it merged across — that is the "
            "term the segment reduction is traded against")
        assert max(r["runs_per_row"] for r in edge_after) > 0.0
        # the profile is self-consistent
        for r in before + after:
            assert 0 <= r["tt_s_runs"] <= max(1, r["rows"])
            assert r["tt_min"] <= r["tt_max"]
            assert r["runs_per_row"] == pytest.approx(
                r["tt_s_runs"] / r["rows"] if r["rows"] else 0.0)
    finally:
        store.close()
