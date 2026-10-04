"""This module's gate: total compaction work grows near-linearly with
the store under the incremental compactor, and quadratically without it.

**The defect this guards.** `compact()` reads the whole logical store back
through the read path and reseals it (`crates/gnomon-core/src/compact.rs`).
Under a fixed op cadence `P`, a build of `n` rows therefore performs `n/P`
compactions of sizes `P, 2P, ..., n`, so total rows rewritten is `≈ n²/(2P)` —
the quadratic term, measured at 83.76 % of a 100M build's 28,372.9 s wall
and extrapolating to ~33 days at 1B. Incremental compaction rewrites `F`
same-size-tier segments at a time instead, so a row is rewritten
`O(log_F(n / rows_per_commit))` times and the total is `O(n · log_F n)`.

**What is measured, and why not the wall.** The gate states the
gate as a *compaction-wall* ratio. This gate measures **rows rewritten**
instead — the engine's own `rows_rewritten` counter.
It is the quantity the cost model actually predicts, and it is a counter rather than a clock: exactly reproducible
run to run, so the gate needs no medians, no warm-up and no host-speed
allowance. `test_the_measurement_is_exactly_reproducible` pins that.

**Both configurations run in every test**, which is this file's main anti-flakiness
mechanism: the bound is not a number remembered from a different machine but
one the control configuration re-fails on this machine, in this run, against the same
fixture. The rule — "a gate that has never seen the defect it guards is
an assertion about nothing" — is satisfied by construction rather than by a
separate fixture-reality test, and `test_the_control_arm_is_quadratic_here`
states it as its own assertion anyway.

These run against the **native** engine specifically, whatever
GNOMON_TEST_BACKEND says: `compact_incremental` and the `rows_rewritten`
counter are native-engine concepts with no portable-backend equivalent, and a
DuckDB run would pass every test here vacuously.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest


# --------------------------------------------------------------------------- #
# thresholds, with their provenance
# --------------------------------------------------------------------------- #
#
# Measured on this machine (macOS, native engine, this file's own fixture),
# both configurations over the same op sequence, `rows_rewritten` summed over every
# compaction including the end-of-build fold:
#
#             n = 8,000                    n = 64,000
#   arm       rewritten  per stored row    rewritten  per stored row   growth
#   control      28,000      3.50           1,120,000     17.50         5.00x
#   treatment     8,000      1.00             128,000      2.00         2.00x
#
# The treatment's figures are the model, not a coincidence: at
# ROWS_PER_BATCH = 200 every commit writes one tier-2 segment, `F` = 8 of them
# merge into one tier-3 segment, and 8,000 rows is exactly one such pass while
# 64,000 rows is exactly two. The control's are `n²/(2P)` with P = CADENCE_OPS.
#
# MAX_GROWTH bounds how much the per-stored-row rewrite cost may grow across
# an 8x size step. 3.0 sits 1.5x above the treatment's measured 2.00x and
# 1.67x below the control's 5.00x, so a loaded or differently-tuned host has
# room in both directions before either a false failure or a false pass.
MAX_GROWTH = 3.0

# A second, independent signal: the absolute amplification at the larger size.
# The treatment measured 2.00 rewritten rows per stored row; the control 17.50.
# 5.0 sits 2.5x above the healthy figure and 3.5x below the defect — a wider
# margin against noise than against the defect, which is why MAX_GROWTH above
# (size-independent) is the primary signal and this backs it up.
MAX_AMPLIFICATION = 5.0

N_SMALL = 8_000
N_BIG = 64_000
ROWS_PER_BATCH = 200
#: The control configuration's `--compact-every`, in rows. Ten commits per compaction.
CADENCE_OPS = 2_000


def _native_adapter(tmp: Path):
    """The native engine, regardless of GNOMON_TEST_BACKEND (see module doc)."""
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="incremental compaction is a native-engine path",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


def _ops(vts) -> list[dict]:
    return [
        {"op": "assert_node", "uid": f"n{vt}", "label": "N", "props": {"v": vt},
         "vt_s": vt, "vt_e": vt + 1}
        for vt in vts
    ]


def _build(tmp: Path, n_rows: int, arm: str) -> dict:
    """Ingest `n_rows` in ROWS_PER_BATCH commits, compacting under one configuration.

    `control` mirrors `build_synth_store.py --compact-every CADENCE_OPS`: the
    whole-store compactor on an op counter. `treatment` mirrors
    `--compact-every 0`: the engine's own cluster trigger consulted after
    every batch, and the incremental compactor when it fires. Both end with
    the fold the driver's finalisation phase does — one call for the control,
    which needs no more, and a drain for the treatment, which merges one tier
    per call.

    `gc()` is deliberately not called: it deletes superseded files, which
    changes disk bytes but not one row of compaction work, and it would make
    the gate's wall depend on the filesystem.
    """
    a = _native_adapter(tmp)
    rewritten, compactions, since, tt = 0, 0, 0, 1000
    for lo in range(0, n_rows, ROWS_PER_BATCH):
        tt += 1
        a.begin()
        a.apply_ops(_ops(range(lo, min(lo + ROWS_PER_BATCH, n_rows))), tt)
        a.commit()
        since += ROWS_PER_BATCH
        if arm == "control":
            if since >= CADENCE_OPS:
                rewritten += a.compact()["rows_rewritten"]
                compactions += 1
                since = 0
        elif a.needs_compaction():
            rewritten += a.compact_incremental()["rows_rewritten"]
            compactions += 1
    if arm == "control":
        rewritten += a.compact()["rows_rewritten"]
        compactions += 1
    else:
        for _ in range(64):
            if not a.needs_compaction():
                break
            rewritten += a.compact_incremental()["rows_rewritten"]
            compactions += 1
        assert not a.needs_compaction(), (
            "the treatment's trigger did not converge in 64 rounds — the "
            "compactor and the predicate it plans against disagree"
        )
    report = a.verify()
    assert report["healthy"], report["problems"]
    rows = len(a.all_node_versions())
    a.close()
    assert rows == n_rows, f"the fixture lost rows: {rows} of {n_rows}"
    return {"rewritten": rewritten, "compactions": compactions,
            "per_row": rewritten / n_rows}


# --- 1. the fixture is real: the control configuration is quadratic on this host --- #


def test_the_control_arm_is_quadratic_here(tmp_path):
    """The gate must fail on the defect it guards, on this machine.

    Not a restatement of the bound below — this asserts the *shape*. Under a
    fixed cadence the total is `n²/(2P)`, so an 8x size step must cost ~8x
    more per stored row, which is what makes the whole-store compactor
    unreachable at 300M-1B.
    """
    small = _build(tmp_path / "cs", N_SMALL, "control")
    big = _build(tmp_path / "cb", N_BIG, "control")
    growth = big["per_row"] / small["per_row"]

    assert growth > MAX_GROWTH, (
        f"the control configuration did not reproduce the quadratic term on this host "
        f"(per-stored-row rewrite cost grew only {growth:.2f}x from "
        f"{N_SMALL} to {N_BIG} rows, against the {MAX_GROWTH}x bound the "
        f"treatment is held to). Either the cadence is no longer driving the "
        f"whole-store compactor, or `compact()` stopped rewriting the whole "
        f"store — in both cases every bound in this file is vacuous."
    )
    assert big["per_row"] > MAX_AMPLIFICATION, (
        f"the control rewrote only {big['per_row']:.2f} rows per stored row "
        f"at {N_BIG}, under the {MAX_AMPLIFICATION} bound the treatment is "
        f"held to — the fixture is not exercising the defect"
    )


# --- 2. the gate ----------------------------------------------------------- #


def test_total_rows_rewritten_grows_near_linearly(tmp_path):
    small = _build(tmp_path / "ts", N_SMALL, "treatment")
    big = _build(tmp_path / "tb", N_BIG, "treatment")
    growth = big["per_row"] / small["per_row"]

    assert growth < MAX_GROWTH, (
        f"total compaction work per stored row grew {growth:.2f}x from a "
        f"{N_SMALL}-row build to a {N_BIG}-row one (limit {MAX_GROWTH}x): "
        f"{small['per_row']:.2f} -> {big['per_row']:.2f} rewritten rows per "
        f"stored row, {small['rewritten']:,} -> {big['rewritten']:,} rows "
        f"over {small['compactions']} and {big['compactions']} compactions. "
        f"The bound is O(n · log_F n): an 8x size step is one more merge "
        f"tier, not 8x the work per row. A failure here means the merge sets "
        f"are no longer size-tiered — that is the shape that makes the "
        f"treatment rewrite a large segment alongside small ones on every "
        f"round."
    )
    assert big["per_row"] < MAX_AMPLIFICATION, (
        f"the {N_BIG}-row build rewrote {big['per_row']:.2f} rows per stored "
        f"row (limit {MAX_AMPLIFICATION}), against the model's "
        f"log_8(64000/200) = 2.0"
    )


def test_the_measurement_is_exactly_reproducible(tmp_path):
    """`rows_rewritten` is a counter, not a clock.

    Two independent builds of the same op sequence must rewrite the same
    number of rows, in the same number of compactions. That is what lets this
    file assert against fixed bounds with no medians, no warm-up and no
    host-speed allowance — and it would break the moment the merge planner's merge
    sets depended on anything but the manifest.
    """
    first = _build(tmp_path / "r1", N_SMALL, "treatment")
    second = _build(tmp_path / "r2", N_SMALL, "treatment")
    assert first == second, (
        f"the incremental compactor is not deterministic: {first} vs {second}"
    )


# --- 3. logical equivalence between the two configurations --------------- #


def test_the_two_arms_agree_on_the_logical_store(tmp_path):
    """The same store compacted either way lists identically.

    One store is built, copied on disk, and then compacted two ways, so the
    comparison is over identical `vid`/`tt_s` values — two *independent*
    ingests could not be compared at all, because `tt_s` is wall-clock
    derived and every derived id with it. Version dataclass equality
    covers every field the store digest hashes, so this is the digest claim
    without needing the digest's sort.
    """
    src = tmp_path / "src"
    a = _native_adapter(src)
    tt = 1000
    for lo in range(0, N_SMALL, ROWS_PER_BATCH):
        tt += 1
        a.begin()
        a.apply_ops(_ops(range(lo, lo + ROWS_PER_BATCH)), tt)
        a.commit()
    # a correction, so both configurations have a close run to fold rather than only
    # segments to merge
    tt += 1
    a.begin()
    a.apply_ops([{"op": "correct", "ref": {"kind": "node", "uid": "n7"},
                  "props": {"v": -7}, "vt_s": 7, "vt_e": 8}], tt)
    a.commit()
    before = sorted(a.all_node_versions(), key=lambda v: v.vid)
    a.close()

    whole, incr = tmp_path / "whole", tmp_path / "incr"
    shutil.copytree(src, whole)
    shutil.copytree(src, incr)

    w = _native_adapter(whole)
    w.compact()
    whole_rows = sorted(w.all_node_versions(), key=lambda v: v.vid)
    assert w.verify()["healthy"]
    w.close()

    i = _native_adapter(incr)
    rounds = 0
    while i.needs_compaction() and rounds < 64:
        i.compact_incremental()
        rounds += 1
    incr_rows = sorted(i.all_node_versions(), key=lambda v: v.vid)
    assert i.verify()["healthy"]
    i.close()

    assert rounds > 0, "the incremental configuration did no work — the test is vacuous"
    assert whole_rows == before, "the whole-store configuration changed the listing"
    assert incr_rows == before, "the incremental configuration changed the listing"
    assert incr_rows == whole_rows


# --- 4. untouched segments keep their indexes ----------------------- #


def _idx_shas(root: Path) -> dict[str, str]:
    """`sha256` of every file under the store's rebuildable index directory.

    `root` is the adapter's own directory — `NativeAdapter(path)` puts
    `idx/`, `seg/` and `manifests/` directly under it. (A `Store` opened
    through `gnomon.open` nests the same layout under `native/`.)
    """
    idx = root / "idx"
    if not idx.exists():
        return {}
    return {
        str(p.relative_to(idx)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(idx.rglob("*"))
        if p.is_file()
    }


def test_untouched_segments_keep_their_indexes(tmp_path):
    """The first-class benefit.

    The `idx/tcsr/*.tci` and `idx/name/*.tnx` indexes are keyed on their
    segment's own `body_sha`, so a segment a compaction did not rewrite keeps
    its index. A whole-store compaction replaces every segment and therefore
    orphans every index file on every cycle; an incremental one orphans only
    the tier it merged. Both configurations run, so the assertion is against measured
    behaviour on this host rather than against a remembered number.
    """
    src = tmp_path / "src"
    a = _native_adapter(src)
    tt = 1000
    for lo in range(0, N_SMALL, ROWS_PER_BATCH):
        tt += 1
        a.begin()
        a.apply_ops(
            [{"op": "assert_edge", "src": f"s{v}", "dst": f"d{v}", "rel_type": "R",
              "props": {"v": v}, "vt_s": v, "vt_e": v + 1}
             for v in range(lo, lo + ROWS_PER_BATCH)],
            tt,
        )
        a.commit()
    a.build_indexes()
    a.close()

    before = _idx_shas(src)
    assert before, "the fixture built no index files — the test is vacuous"

    whole, incr = tmp_path / "whole", tmp_path / "incr"
    shutil.copytree(src, whole)
    shutil.copytree(src, incr)

    # gc, because compaction does not delete anything: an older generation
    # still names the superseded segments, and sweeping the index files whose
    # segment is gone is gc's pass 4. `keep_last=1` drops the pre-compaction
    # generation, which is what makes the sweep visible.
    w = _native_adapter(whole)
    w.compact()
    w.gc(keep_last=1)
    w.close()
    after_whole = _idx_shas(whole)

    i = _native_adapter(incr)
    assert i.needs_compaction()
    # One merge set only. Without the budget the merge planner would take every
    # eligible window in one call, and this fixture's segments are all the
    # same size and so all one tier — there would be nothing left untouched
    # to make an assertion about.
    i.compact_incremental(budget_rows=1)
    i.gc(keep_last=1)
    i.close()
    after_incr = _idx_shas(incr)

    kept_incr = {k for k, v in before.items() if after_incr.get(k) == v}
    kept_whole = {k for k, v in before.items() if after_whole.get(k) == v}

    assert kept_incr, (
        "an incremental compaction left no index file intact: every one of "
        f"{sorted(before)} was rewritten or removed"
    )
    assert len(kept_whole) == 0, (
        f"the whole-store configuration kept {len(kept_whole)} of {len(before)} index "
        f"files — it replaces every segment, so gc should have swept every "
        f"one of them, and the comparison below is not measuring what it "
        f"claims"
    )
    assert len(kept_incr) > len(kept_whole), (
        f"an incremental compaction kept {len(kept_incr)} of {len(before)} "
        f"index files against the whole-store configuration's {len(kept_whole)} — the "
        f"incremental benefit is that untouched segments keep their `body_sha` and "
        f"therefore their indexes, and it is not showing up here"
    )
