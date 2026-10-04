"""`settle` — the quiescent read-optimising merge, end to end.

**What this file is for.** `crates/gnomon-core/src/settle.rs`'s own unit
tests pin the settle planner (byte floor, single pass, group bounds, lane routing,
idempotence) against the engine's in-process fixtures. This file pins the
things that only exist above the engine: the PyO3 surface and its dict shape,
the adapter's index-rebuild default, the CLI action, the digest invariance a
settled campaign store's provenance rests on, gc's ability to reclaim what
settle superseded, the crash-point test, and the cross-process reader test.

**The claim under test, in one sentence.** A settle changes what segments
*are* and nothing about what the store *says*: every row survives, transaction
time does not advance, no identity is recomputed, and therefore
`store_digest()` is byte-identical either side. That invariance is not a
convenience — it is the provenance proof for settling a store in place at a
later commit, because it is what distinguishes "the same store, relaid out"
from "a second build".

These run against the **native** engine specifically, whatever
`GNOMON_TEST_BACKEND` says: settle is a native-engine operation with no
portable-backend equivalent, and a DuckDB run would pass every test here
vacuously.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# Settle is a native-engine operation with no portable-backend equivalent, so
# every test here opens the native backend explicitly regardless of
# GNOMON_TEST_BACKEND -- and skips outright where the compiled engine is absent.
pytest.importorskip("gnomon.storage.native",
                    reason="settle is a native-engine path")

#: Spawn, so a child never inherits an open store handle (or this process's
#: pin table) — the same discipline `test_reader_generation_pinning.py`
#: and `test_concurrency.py` use, and for the same reason.
MP = mp.get_context("spawn")

#: Commits in the fixture. Each one appends a segment per lane it touches, so
#: this is also the straggler count settle has to collapse — the shape a
#: tiered build leaves behind, at unit scale.
BATCHES = 12
ROWS_PER_BATCH = 8


def _events(tag: str, lo: int, n: int) -> list[dict]:
    return [
        {"src": f"{tag}{i}", "dst": f"{tag}{i + 1}", "rel_type": "R",
         "vt_s": i, "vt_e": i + 1}
        for i in range(lo, lo + n)
    ]


def _build(root: Path, batches: int = BATCHES, arm: str = "treatment"):
    """Ingest in `batches` commits under one compaction configuration.

    `treatment` consults the engine's own cluster trigger, which is what a
    `--compact-every 0` build does and what leaves the unfilled tier windows
    settle exists to pay off. `control` runs the whole-store compactor once at
    the end, which is the configuration whose live segment count *is* the byte floor.
    `none` compacts not at all, so the live segment count is exactly the
    commit count -- the shape a settle-planner test needs when it wants segments
    rather than a realistic layout.
    """
    import gnomon

    s = gnomon.open(root, backend="native")
    for b in range(batches):
        s.ingest_events(_events("e", b * ROWS_PER_BATCH, ROWS_PER_BATCH))
        if arm == "treatment" and s.adapter.needs_compaction():
            s.adapter.compact_incremental()
    if arm == "control":
        s.adapter.compact()
    return s


def _lanes(store) -> dict:
    """Per-lane live segment counts, through `verify(mode="fast")`.

    Not by reading `manifests/<generation>.json`: since the delta-manifest
    change a generation's record is a checkpoint *or* a delta against one, so
    the tip file is a full document only sometimes. `verify` walks the
    reconstructed manifest and is the accessor `eval_harness.py`'s own store
    receipt uses for exactly this number.
    """
    return store.adapter.verify(mode="fast")["closes_by_lane"]


def _live(store) -> int:
    return sum(v["segments"] for v in _lanes(store).values())


# --------------------------------------------------------------------- #
# 1. the invariance the provenance argument rests on                     #
# --------------------------------------------------------------------- #


def test_store_digest_is_identical_either_side_of_a_settle(tmp_path: Path) -> None:
    """The single most important assertion in this file.

    `store_digest()` hashes `vid`, `tt_s` and `tt_e` along with every field.
    Settle neither advances transaction time nor recomputes any identity —
    `vid` is copied verbatim as a raw `Id96` by the merge writer — so the
    digest must not move by one byte. If it ever does, a settled campaign
    store stops being the store it was built as, and the whole
    "settle in place, keep one build commit" argument goes with it.
    """
    s = _build(tmp_path / "s")
    try:
        before = s.adapter.store_digest()
        rows_before = len(s.adapter.all_edge_versions())
        report = s.adapter.settle()
        assert report["rows_rewritten"] > 0, (
            "the fixture left nothing to settle — every assertion here would "
            "be vacuous"
        )
        assert s.adapter.store_digest() == before, "settle changed the store digest"
        assert len(s.adapter.all_edge_versions()) == rows_before
        # and the streaming digest, which is what the campaign actually uses
        # at 30M/100M, agrees with the in-memory one on the settled store
        assert s.adapter.store_digest_streaming() == before
    finally:
        s.close()


def test_settle_publishes_exactly_one_generation_and_nothing_logical_moves(
    tmp_path: Path,
) -> None:
    """The observable half of I5 from above the engine.

    Transaction-time invariance itself is asserted where the manifest is
    actually reachable — `settle.rs`'s
    `settle_does_not_advance_transaction_time`, which reads `created_tt` off
    the successor directly. From here the manifest document is not readable in
    general: since the delta-manifest change a generation's record is a
    checkpoint *or* a delta against one, so the tip file is a full document
    only sometimes, and a test that happened to read it would be asserting
    about the checkpoint cadence rather than about settle. What *is* visible
    here is the consequence: exactly one generation is published, the store
    still verifies, and nothing logical moved.
    """
    s = _build(tmp_path / "s")
    try:
        gen_before = s.adapter.generation
        digest_before = s.adapter.store_digest()
        report = s.adapter.settle()
        assert report["generation_before"] == gen_before
        assert report["generation_after"] == gen_before + 1
        assert s.adapter.generation == gen_before + 1
        assert s.adapter.store_digest() == digest_before
        assert s.adapter.verify()["healthy"]
    finally:
        s.close()


# --------------------------------------------------------------------- #
# 2. the operation reaches the byte floor                                #
# --------------------------------------------------------------------- #


def test_settle_reaches_the_control_arms_segment_count(tmp_path: Path) -> None:
    """The whole-store control's live segment count *is* the byte floor —
    it rewrote every row into fresh segments at the byte target, which is what
    settle does too, for `n` rows of rewriting instead of the control's
    cumulative `n**2/(2P)`. Both builds ingest the same op sequence, so the
    comparison is layout against layout.

    Stated as "at most the control's + 2" rather than "equal": a settle may
    end on one partial per merge group per writer, and the node slot runs two
    writers. The byte floor is "plus lane partials", never a fixed number.
    """
    control = _build(tmp_path / "c", arm="control")
    control_segments = _live(control)
    control.close()

    t = _build(tmp_path / "t", arm="treatment")
    try:
        before = _live(t)
        assert before > control_segments, (
            f"the treatment must start above the control's floor "
            f"({before} vs {control_segments}) or this gate is vacuous"
        )
        report = t.adapter.settle()
        after = _live(t)
        assert after == report["segments_after"]
        assert after <= control_segments + 2, (
            f"settle left {after} segments against the whole-store control's "
            f"{control_segments}"
        )
    finally:
        t.close()


def test_a_second_settle_rewrites_nothing(tmp_path: Path) -> None:
    """Idempotence is not a nicety here: settle merges *every*
    live segment of an eligible lane, so without a byte-floor test a second
    call would rewrite the whole store to save nothing. The report must say so
    with an exact counter, and no generation may be published."""
    s = _build(tmp_path / "s")
    try:
        first = s.adapter.settle()
        assert first["rows_rewritten"] > 0
        gen = s.adapter.generation
        digest = s.adapter.store_digest()

        second = s.adapter.settle()
        assert second["rows_rewritten"] == 0, "a second settle rewrote rows"
        assert second["rounds"] == 0
        assert second["groups"] == 0
        assert second["segments_after"] == second["segments_before"]
        assert second["segments_before"] == first["segments_after"]
        assert s.adapter.generation == gen, (
            "a settle with nothing to do must publish no generation"
        )
        assert s.adapter.store_digest() == digest
        assert not s.adapter.needs_settle()
    finally:
        s.close()


def test_needs_settle_is_advisory_and_agrees_with_the_operation(tmp_path: Path) -> None:
    s = _build(tmp_path / "s")
    try:
        assert s.adapter.needs_settle(), "a tiered store must want a settle"
        s.adapter.settle()
        assert not s.adapter.needs_settle()
    finally:
        s.close()


# --------------------------------------------------------------------- #
# 3. memory: an exact counter, never an RSS reading                      #
# --------------------------------------------------------------------- #


def test_the_merge_fan_in_is_bounded_by_the_budget_not_by_store_size(
    tmp_path: Path,
) -> None:
    """Settle's boundedness claim, in counter form.

    The boundedness claim is that settle's residency is `fan-in x one segment`
    and flat in the store's size. An RSS reading cannot test that on a laptop
    — it is dominated by the interpreter, it is not reproducible, and it is
    exactly the kind of measurement the standing rule keeps off this machine.
    The counter that *does* test it is the fan-in: at a fixed byte budget, a
    store twice the size must split into more merge groups rather than open
    more segments at once.

    So: same budget, two store sizes; `groups` grows, `fan_in` does not.

    Both configurations build with no compaction at all, so the live segment count is
    exactly the commit count and the two stores really do differ by 4x in
    segments. Under the cluster trigger they would both converge to a handful
    and the comparison would be between two numbers that agree for a reason
    unrelated to the budget.
    """
    small = _build(tmp_path / "small", batches=BATCHES, arm="none")
    big = _build(tmp_path / "big", batches=BATCHES * 4, arm="none")
    try:
        # an 8-row edge segment models 400 B, so this takes three per group
        # and the lane splits several ways at both sizes
        budget = 1200
        r_small = small.adapter.settle(budget_bytes=budget)
        r_big = big.adapter.settle(budget_bytes=budget)

        assert r_small["budget_bytes"] == budget
        assert r_big["budget_bytes"] == budget
        assert r_big["segments_before"] > r_small["segments_before"], (
            "the bigger fixture must actually hold more segments"
        )
        assert r_big["groups"] > r_small["groups"], (
            "a bigger store at the same budget must split into more groups"
        )
        assert r_big["fan_in"] <= r_small["fan_in"] + 1, (
            f"the fan-in grew with the store ({r_small['fan_in']} -> "
            f"{r_big['fan_in']}) — the merge is not bounded by the budget"
        )
    finally:
        small.close()
        big.close()


def test_one_settle_rewrites_each_stored_row_exactly_once(tmp_path: Path) -> None:
    """One settle rewrites every row once, by the exact counter rather than
    a wall or an RSS."""
    s = _build(tmp_path / "s")
    try:
        n = (len(s.adapter.all_edge_versions())
             + len(s.adapter.all_node_versions()))
        r = s.adapter.settle()
        assert r["rounds"] == 1
        assert r["rows_rewritten"] == n, (
            f"settle rewrote {r['rows_rewritten']} rows against {n} stored"
        )
        # the below-target census travels on the same report
        assert r["inputs_total"] >= 2
        assert 0 <= r["inputs_below_full_rows"] <= r["inputs_total"]
    finally:
        s.close()


# --------------------------------------------------------------------- #
# 4. the indexes settle orphans, and the opt-out                         #
# --------------------------------------------------------------------- #


def _idx_files(root: Path) -> tuple[int, int]:
    idx = root / "native" / "idx"
    return (len(list((idx / "tcsr").glob("*"))) if (idx / "tcsr").exists() else 0,
            len(list((idx / "name").glob("*"))) if (idx / "name").exists() else 0)


def test_settle_rebuilds_the_indexes_it_orphans_by_default(tmp_path: Path) -> None:
    """A merged segment has a different `body_sha`, so every
    per-segment TCSR and name index the inputs carried is orphaned by
    construction — that is settle's one deliberate spend of the
    index-survival benefit, and it is stated rather than hidden. Leaving the
    rebuild to the first queries would make them pay a full-file sha256 plus
    an index build per segment, contaminating exactly the latency settle
    exists to improve, so the default here is **on** — against `compact()`'s
    off, deliberately.
    """
    root = tmp_path / "s"
    s = _build(root)
    try:
        s.adapter.build_indexes()
        s.adapter.build_name_indexes()
        before = _idx_files(root)
        assert before[0] > 0, "the fixture must have TCSR indexes to orphan"

        report = s.adapter.settle()
        rebuilt = report["indexes_rebuilt"]
        assert rebuilt is not None
        assert rebuilt["tcsr"]["built"] > 0, (
            "settle's outputs must have had their TCSR built, not loaded"
        )
        assert rebuilt["tcsr"]["segments"] == report["segments_after"] - _node_segments(s)
        assert "name" in rebuilt
    finally:
        s.close()


def _node_segments(store) -> int:
    return _lanes(store)["node"]["segments"]


def test_no_index_skips_the_rebuild(tmp_path: Path) -> None:
    """The ablation configuration. `--no-index` exists so the cost of a cold settled
    store can be measured against a warm one; it must genuinely skip."""
    root = tmp_path / "s"
    s = _build(root)
    try:
        report = s.adapter.settle(build_indexes=False)
        assert report["indexes_rebuilt"] is None
        assert report["rows_rewritten"] > 0
    finally:
        s.close()


# --------------------------------------------------------------------- #
# 5. gc, and the ~2x disk transient settle deliberately leaves           #
# --------------------------------------------------------------------- #


def test_gc_after_settle_reclaims_the_superseded_segments(tmp_path: Path) -> None:
    """Settle does not call gc — deletion stays an operator act, as it
    is for `compact` — so the store is transiently about twice its settled
    size until `gc --keep 2` runs. The report states the transient rather than
    leaving it to be discovered: `reclaimable_bytes` is what gc will free."""
    root = tmp_path / "s"
    s = _build(root)
    try:
        seg = root / "native" / "seg"
        before_files = {p.name for p in seg.glob("*.tgs")}
        report = s.adapter.settle()
        assert report["reclaimable_bytes"] > 0

        after_settle = {p.name for p in seg.glob("*.tgs")}
        assert before_files <= after_settle, (
            "settle must not unlink anything — an older generation still "
            "names those files and a reader holding it must keep working"
        )
        s.adapter.gc(keep_last=1)
        after_gc = {p.name for p in seg.glob("*.tgs")}
        assert after_gc < after_settle, "gc reclaimed nothing after a settle"
        assert s.adapter.verify()["healthy"]
    finally:
        s.close()


# --------------------------------------------------------------------- #
# 6. the CLI                                                             #
# --------------------------------------------------------------------- #


#: Keys the `gnomon store settle` JSON document promises. A receipt consumer
#: reads these by name, so dropping one is a breaking change and adding one
#: is not — hence a subset assertion, not an equality.
SETTLE_KEYS = {
    "segments_before", "segments_after", "closes_folded", "rows_rewritten",
    "rounds", "fan_in", "groups", "budget_bytes", "inputs_total",
    "inputs_below_full_rows", "reclaimable_bytes", "generation_before",
    "generation_after", "wall_us", "indexes_rebuilt",
}


def _cli(root: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": str(REPO)}
    return subprocess.run(
        [sys.executable, "-m", "gnomon.cli", "store", *args, "--store", str(root)],
        capture_output=True, text=True, env=env, cwd=REPO, timeout=600,
    )


def test_the_cli_action_exits_zero_and_prints_the_documented_json(
    tmp_path: Path,
) -> None:
    root = tmp_path / "s"
    _build(root).close()

    done = _cli(root, "settle")
    assert done.returncode == 0, done.stderr[-2000:]
    report = json.loads(done.stdout)
    assert SETTLE_KEYS <= set(report), SETTLE_KEYS - set(report)
    assert report["rows_rewritten"] > 0
    assert report["rounds"] == 1

    # idempotent from the CLI too
    again = _cli(root, "settle")
    assert again.returncode == 0, again.stderr[-2000:]
    assert json.loads(again.stdout)["rows_rewritten"] == 0

    # and gc afterwards, the documented follow-on
    gc = _cli(root, "gc", "--keep", "1")
    assert gc.returncode == 0, gc.stderr[-2000:]


def test_the_cli_honours_no_index_and_budget_bytes(tmp_path: Path) -> None:
    root = tmp_path / "s"
    _build(root).close()
    done = _cli(root, "settle", "--no-index", "--budget-bytes", "65536")
    assert done.returncode == 0, done.stderr[-2000:]
    report = json.loads(done.stdout)
    assert report["indexes_rebuilt"] is None
    assert report["budget_bytes"] == 65536


# --------------------------------------------------------------------- #
# 7. A crash before install leaves the pre-settle generation            #
# --------------------------------------------------------------------- #


_CRASH_CHILD = """
import sys
import gnomon
s = gnomon.open(sys.argv[1], backend="native")
s.adapter.settle()
print("SETTLE RETURNED", flush=True)
"""


def test_a_crash_before_install_leaves_the_pre_settle_generation(
    tmp_path: Path,
) -> None:
    """`GNOMON_CRASH_POINT=settle_before_install` aborts the
    process with no unwinding, so what is on disk is what a power cut at that
    line would leave: fresh segment files written but named by no manifest,
    `CURRENT` still pointing at the pre-settle generation, and that
    generation's event-log cursor unchanged. The next open must therefore
    recover exactly the un-applied suffix and nothing else, and the store's
    digest must be the one it had before the settle started.
    """
    root = tmp_path / "s"
    s = _build(root)
    before_digest = s.adapter.store_digest()
    before_gen = s.adapter.generation
    s.close()

    env = {**os.environ, "PYTHONPATH": str(REPO),
           "GNOMON_CRASH_POINT": "settle_before_install"}
    child = subprocess.run([sys.executable, "-c", _CRASH_CHILD, str(root)],
                           capture_output=True, text=True, env=env,
                           cwd=REPO, timeout=600)
    assert child.returncode != 0, (
        "the crash point did not fire — this test would pass vacuously"
    )
    assert "SETTLE RETURNED" not in child.stdout
    assert "GNOMON_CRASH_POINT hit: settle_before_install" in child.stderr

    import gnomon

    reopened = gnomon.open(root, backend="native")
    try:
        assert reopened.adapter.generation == before_gen, (
            "the store came back at a generation the settle never installed"
        )
        assert reopened.adapter.store_digest() == before_digest
        assert reopened.adapter.verify()["healthy"]
        # and the store is still settleable afterwards
        assert reopened.adapter.settle()["rows_rewritten"] > 0
        assert reopened.adapter.store_digest() == before_digest
    finally:
        reopened.close()


# --------------------------------------------------------------------- #
# 8. A cross-process reader survives settle followed by gc              #
# --------------------------------------------------------------------- #


def _child_delayed_reader(root: str, out: str, opened, proceed) -> None:
    """Open read-only, announce, query nothing until the writer has settled
    and collected this handle's own generation out from under it."""
    import gnomon

    s = gnomon.open(root, backend="native", read_only=True)
    generation = s.adapter.generation
    opened.set()
    proceed.wait(180)
    result: dict = {"generation": generation}
    try:
        result["count"] = len(s.adapter.all_edge_versions())
        result["ok"] = True
    except Exception as e:  # noqa: BLE001 — the failure itself is the result
        result["ok"] = False
        result["error_type"] = type(e).__name__
        result["message"] = str(e)[:500]
    result["reopen_on_enoent_total"] = s.adapter.reopen_on_enoent_total()
    Path(out).write_text(json.dumps(result))
    try:
        s.close()
    except Exception:  # noqa: BLE001 — best effort; the result is written
        pass


def test_a_cross_process_reader_survives_a_settle_and_a_gc(tmp_path: Path) -> None:
    """The settle-shaped sibling of
    `gc.rs::a_cross_process_reader_survives_gc_because_pin_at_open_maps_its_
    whole_generation`, run against a real second process rather than an
    emulated unpin.

    Settle supersedes *every* segment a reader's generation names — more than
    a compaction does — so it is the strongest available test of pin-at-open
   : a reader that opens and queries nothing has its whole generation
    mapped at open, and must still answer correctly after the writer has
    settled and gc'd those exact files away.
    """
    import gnomon

    root = tmp_path / "s"
    _build(root).close()

    native_dir = root / "native"
    pre_reader = sorted(p.name for p in (native_dir / "seg").glob("*.tgs"))
    assert pre_reader, "the fixture must have written segments"

    out = tmp_path / "reader.json"
    opened, proceed = MP.Event(), MP.Event()
    reader = MP.Process(target=_child_delayed_reader,
                        args=(str(root), str(out), opened, proceed))
    reader.start()
    try:
        assert opened.wait(180), "the reader never opened the store"
        w = gnomon.open(root, backend="native")
        expected = len(w.adapter.all_edge_versions())
        report = w.adapter.settle()
        assert report["rows_rewritten"] > 0
        w.adapter.gc(keep_last=1)
        w.close()
    finally:
        proceed.set()
        reader.join(180)
        if reader.is_alive():
            reader.terminate()
            reader.join(30)

    removed = [f for f in pre_reader if not (native_dir / "seg" / f).exists()]
    assert removed, (
        "gc must have collected at least one of the reader's own generation's "
        "segments for this test to mean anything; none were removed"
    )
    assert reader.exitcode == 0, f"the reader crashed: exit {reader.exitcode}"
    got = json.loads(out.read_text())
    assert got["ok"], f"the reader failed after settle+gc: {got}"
    assert got["count"] == expected
    assert got["reopen_on_enoent_total"] == 0, (
        "the adapter's ENOENT net fired, which means pin-at-open did not "
        "protect the reader's generation across the settle"
    )
