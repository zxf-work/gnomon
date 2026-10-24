"""Incremental compaction's memory gate: a build's peak RSS stops growing with the store.

**The defect this guards.** The whole-store `compact()` materialises the
whole logical store twice — once as `EdgeVersionOut`/`NodeVersionOut` (seven
owned `String`s plus a re-derived 64-char `eid` per row) and once as staging
rows — before it writes anything. At 100M that was a measured **+94 GB
transient** inside a 28,372.9 s build whose peak RSS reached 186.42 GB. It is
`O(store)` per call, so it grows with the build no matter how the cadence is
set. The incremental k-way merge holds `fan-in × one segment` plus one output
builder instead, which is flat in `n`.

**How it is measured.** Each configuration (the whole-store baseline and
the incremental compactor) runs in a **subprocess**, and reports its peak
resident set as a delta over a baseline taken after imports and before the
store is opened. A subprocess is not a convenience: a peak high-water mark
never falls, so a second measurement in the same process would inherit the
first's and measure nothing.

The peak itself is read one of two ways, and the difference was a real
defect, not a portability nicety:

* **Linux — `clear_refs` + `VmHWM`.** The probe writes `5` to
  `/proc/self/clear_refs` (proc(5): resets the peak to the current RSS) and
  then reads `VmHWM` from `/proc/self/status`. Without the reset this gate
  measured **nothing at all** on a Linux workstation: `ru_maxrss` is a
  *lifetime* high-water mark, the post-import baseline there was already
  403 MB in one run and 1.46 GB in another — more than a 20k build's entire
  working set — so every delta came out exactly 0. A zero delta means the
  measurement failed, not that the build used no memory, and `_measure` says
  so.
* **macOS — `ru_maxrss`.** There is no `/proc` and no resettable peak, but
  the post-import baseline (~35 MB) sits well below any build's peak, so the
  lifetime counter is already the right answer. `ru_maxrss` is bytes here and
  kibibytes on Linux; the probe scales accordingly.

If neither is available — Linux where `clear_refs` cannot be written — the
probe reports `mechanism: "unavailable"` and the tests **skip with that
reason** rather than passing on a delta they cannot trust.

The gate is a *ratio between two configurations measured the same way on the
same machine in the same run*, so neither a unit mistake nor a host
difference can turn a failure into a pass.

**Both configurations run in every test.** No bound here is a number
remembered from another machine: the baseline re-establishes the defect in
this run, against the same op sequence — a gate which has never seen its
defect is an assertion about nothing. It does so with the engine's own
`rows_rewritten` counter rather than with an RSS ratio, for the reason the
comment above `MIN_CONTROL_WORK_RATIO` sets out at length.

Native engine only, whatever GNOMON_TEST_BACKEND says: `compact_incremental` is
a native-engine path and a DuckDB run would pass vacuously.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


# --------------------------------------------------------------------------- #
# thresholds, with their provenance
# --------------------------------------------------------------------------- #
#
# Measured on a development laptop (macOS, native engine, this file's own
# probe), peak RSS as a delta over the baseline taken after imports and
# before the store is opened:
#
#   n         baseline      incremental  incremental / baseline
#   20,000    119.8 MB       62.0 MB          0.52
#   80,000   1013.5 MB      218.9 MB          0.22
#   growth       8.46x         3.53x
#
# The baseline's 8.46x over a 4x size step is the O(store) transient plus the
# rising floor. The incremental 3.53x is **not** its compaction transient,
# which is bounded by the fan-in — it is the store's own resident growth
# (segments, identity postings, dictionary), which is genuinely O(n) and
# which no compaction strategy removes. That is why the *share* below is the
# primary signal and the growth bound is the looser, secondary one.
#
# MAX_SHARE bounds the incremental peak as a fraction of the baseline's at the
# larger size — the claim this file exists to make, and the quantity least
# sensitive to host, allocator and measurement mechanism, since both
# configurations are measured the same way on the same machine in the same run.
#
# **The bound moved once, because a separate fix removed a term from the
# baseline's peak.** It was 0.5, set against a measured 0.22 on macOS. It then
# failed on a Linux workstation at 0.52 and 0.54 in two consecutive runs — not
# because the incremental path regressed (its peak was 165 MB in both) but
# because the baseline's fell to 307–318 MB from the ~900 MB it used to reach.
#
# What left it is a leak in the identity postings, and the exact counters say
# how much. This fixture calls `compact()` 40 times at `CADENCE_OPS` and
# **never calls `gc()`**, so `Postings::retain_segments` — then the postings'
# only prune — never fired, and each whole-store compaction's reseal left the
# previous cycle's entries behind. Running this file's own op sequence and
# reading `postings_index_stats("node")["entries"]` at `N_BIG`:
#
#   configuration   postings entries   at 12 B/row   at 389 B/row (before)
#   baseline               1,632,000       19.8 MB              ~635 MB
#   incremental              225,200        2.7 MB               ~88 MB
#
# — 1,632,000 entries for 80,000 rows ever written is a multiplicative leak,
# and it was 7.2x larger in the baseline than in the incremental run.
# 389 B/row was the postings' cost before that fix
# (`tests/test_postings_bound.py`). Reconstructing the earlier peaks:
# baseline 307 + 615 = ~922 MB, incremental 165 + 85 = ~250 MB, share 0.27 —
# which is where the old 0.22-0.27 readings came from. So roughly half of the
# old ratio's headroom was a second defect being measured, not this one.
#
# What remains is a purer reading of the transient this file guards, and it
# is still a real result: 165 MB against 307 MB, a 1.86x separation, with the
# `rows_rewritten` counter (the fixture-reality check below) at 10.97x
# untouched. Measured after that fix: **0.52 and 0.54 on Linux
# (clear_refs+VmHWM), 0.235 on macOS (ru_maxrss)** — the two mechanisms
# disagree because a lifetime `ru_maxrss` on macOS is dominated by an earlier
# spike that the postings fix does not lower.
#
# 0.75 sits 1.39x above the worst reading (0.54) and 1.33x below the
# no-improvement line at 1.0. The defect it guards — the merge materialising
# more than one merge set at a time — would push the incremental peak toward
# the baseline, i.e. toward 1.0, so the bound still has to be crossed by a
# large margin to be crossed at all.
MAX_SHARE = 0.75

# A loose ceiling on how much a build's own peak may grow across the 4x size
# step. This is a sanity bound, **not** a discriminating one: see the
# fixture-reality discussion below for why an RSS-growth ratio cannot
# separate the configurations reliably. The treatment measured 3.53x.
MAX_GROWTH = 5.0

# --- the fixture-reality check ------------------------------------ #
#
# "A gate that has never seen the defect it guards is an assertion about
# nothing." The question is what to measure to establish that.
#
# The first version of this check asserted that the *baseline's peak RSS*
# grew faster across the size step than the incremental path is allowed to.
# That is the right claim and the wrong instrument. A peak-RSS ratio depends
# on the allocator, the page cache, and whatever else the machine was doing:
#
#   host / run                                     baseline 20k -> 80k   growth
#   laptop A (macOS, ru_maxrss) — the outlier       119.8 -> 1013.5 MB    8.46x
#   laptop B (macOS)                                  118 ->   562 MB     4.75x
#   Linux workstation, run 1 (clear_refs+VmHWM)       100 ->   476 MB     4.75x
#   Linux workstation, run 2 — same host, same code   101 ->   430 MB     4.26x
#
# Three of the four cluster at 4.26-4.75x and the 8.46x is the outlier; two
# runs on one host disagreeing by 11 % is the measurement moving, not the
# engine. The bound the first version used was 5.0, which three of these four
# runs sit under. So the fixture-reality check now uses the **exact counter**:
# `rows_rewritten`, which the engine reports per compaction and which
# is identical run to run and host to host — `test_the_measurement_is_exactly_
# reproducible` in the scaling gate pins that directly.
#
# On this fixture the baseline rewrites 1,720,000 rows at 80k against the
# incremental path's 156,800 — **10.97x**. 3.0 leaves a 3.7x margin and is
# still far above anything a healthy incremental path could produce: a 1M/10M
# comparison at a much laxer cadence (`--compact-every 1000000` against this
# fixture's 2000) measured 3.29x at 10M and 2.52x at 1M, so 3.0 is comfortably
# inside the range this fixture's own cadence produces while not being a
# number only this fixture could reach.
MIN_CONTROL_WORK_RATIO = 3.0

# The RSS-growth floor is kept, demoted to a secondary sanity check: the
# control must at least grow *some* multiple across the step, or it is not
# reading the whole store back at all. 2.0 sits below every figure in the
# table above, including the 4.26x low-water mark.
MIN_CONTROL_RSS_GROWTH = 2.0

N_SMALL = 20_000
N_BIG = 80_000
ROWS_PER_BATCH = 200
#: The control configuration's `--compact-every`, in rows.
CADENCE_OPS = 2_000

#: The probe, written to a temp file and run as a subprocess.
#:
#: `root` arrives as **argv**, not through `str.format`, so nothing in this
#: source has to escape a brace — a dict literal in a `.format`-ed template is
#: a silent syntax error waiting to happen.
_PROBE = r"""
# Build one configuration and report the peak RSS it reached. Run as a subprocess by
# tests/test_incremental_compaction_memory.py, never imported.
import json, resource, sys, tempfile
from pathlib import Path

ROOT, ARM = sys.argv[1], sys.argv[2]
N_ROWS, ROWS_PER_BATCH, CADENCE = (int(a) for a in sys.argv[3:6])
sys.path.insert(0, ROOT)
from gnomon.storage.native import NativeAdapter


def vmhwm_kb():
    # Linux's peak resident set, which clear_refs can reset. None elsewhere.
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def ru_maxrss_kb():
    # The process's *lifetime* peak. Bytes on macOS, kibibytes on Linux.
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru // 1024 if sys.platform == "darwin" else ru


def reset_peak():
    # Reset VmHWM to the current VmRSS (proc(5): write 5 to clear_refs).
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        return False
    return True


# Which mechanism can measure *this section's* peak rather than the
# process's lifetime one. On Linux the post-import baseline already exceeds a
# small build's whole working set -- measured at 403 MB and at 1.46 GB on
# a Linux workstation -- so an unreset `ru_maxrss` yields a delta of exactly 0 and the
# comparison silently measures nothing. Resetting VmHWM is the fix; macOS has
# neither /proc nor an equivalent, but there the baseline (~35 MB) sits well
# below any build's peak, so the lifetime counter is already the right answer.
if vmhwm_kb() is not None and reset_peak():
    MECHANISM, read_peak = "clear_refs+VmHWM", vmhwm_kb
elif sys.platform == "darwin":
    MECHANISM, read_peak = "ru_maxrss", ru_maxrss_kb
else:
    MECHANISM, read_peak = "unavailable", ru_maxrss_kb


def ops(vts):
    # a props payload wide enough that the materialised form of a row is
    # meaningfully larger than its stored columns, which is the whole shape
    # of the transient under test
    return [{"op": "assert_node", "uid": "n%d" % v, "label": "N",
             "props": {"v": v, "pad": "x" * 200}, "vt_s": v, "vt_e": v + 1}
            for v in vts]


def main():
    base = read_peak()
    with tempfile.TemporaryDirectory() as td:
        a = NativeAdapter(Path(td) / "s")
        a.paranoid = False
        tt, since, rewritten = 1000, 0, 0
        for lo in range(0, N_ROWS, ROWS_PER_BATCH):
            tt += 1
            a.begin()
            a.apply_ops(ops(range(lo, min(lo + ROWS_PER_BATCH, N_ROWS))), tt)
            a.commit()
            since += ROWS_PER_BATCH
            if ARM == "control":
                if since >= CADENCE:
                    rewritten += a.compact()["rows_rewritten"]
                    since = 0
            elif a.needs_compaction():
                rewritten += a.compact_incremental()["rows_rewritten"]
        if ARM == "control":
            rewritten += a.compact()["rows_rewritten"]
        else:
            for _ in range(64):
                if not a.needs_compaction():
                    break
                rewritten += a.compact_incremental()["rows_rewritten"]
        rows = len(a.all_node_versions())
        a.close()
    peak = read_peak()
    print(json.dumps({"arm": ARM, "n": N_ROWS, "mechanism": MECHANISM,
                      "base_kb": base, "peak_kb": peak,
                      "delta_kb": peak - base,
                      "rewritten": rewritten, "rows": rows}))


main()
"""


@pytest.fixture(scope="module")
def probe(tmp_path_factory) -> Path:
    pytest.importorskip(
        "gnomon.storage.native",
        reason="incremental compaction is a native-engine path",
    )
    p = tmp_path_factory.mktemp("probe") / "rss_probe.py"
    p.write_text(_PROBE)
    return p


def _measure(probe: Path, arm: str, n_rows: int) -> dict:
    root = str(Path(__file__).resolve().parents[1])
    out = subprocess.run(
        [sys.executable, str(probe), root, arm, str(n_rows),
         str(ROWS_PER_BATCH), str(CADENCE_OPS)],
        capture_output=True, text=True, timeout=1800,
    )
    assert out.returncode == 0, (
        f"the {arm} probe at n={n_rows} failed:\n{out.stdout}\n{out.stderr}"
    )
    result = json.loads(out.stdout.strip().splitlines()[-1])
    assert result["rows"] == n_rows, f"the probe lost rows: {result}"
    if result["mechanism"] == "unavailable":
        pytest.skip(
            "no way to measure this section's peak RSS on this host: "
            "/proc/self/status has VmHWM but /proc/self/clear_refs could not "
            "be written, so the peak cannot be reset, and `ru_maxrss` is a "
            "lifetime high-water mark that the post-import baseline already "
            "exceeds at these sizes. Re-run where clear_refs is writable "
            f"(Linux) or on macOS. Probe said: {result}"
        )
    assert result["delta_kb"] > 0, (
        f"no RSS growth measured at all via {result['mechanism']}: {result}. "
        f"A zero delta means the measurement failed, not that the build used "
        f"no memory."
    )
    return result


@pytest.fixture(scope="module")
def measured(probe) -> dict:
    """All four measurements, once — each is a subprocess and a real build."""
    return {
        (arm, n): _measure(probe, arm, n)
        for arm in ("control", "treatment")
        for n in (N_SMALL, N_BIG)
    }


def test_the_control_arm_is_really_the_defect_here(measured):
    """The gate must fail on the defect it guards, on this machine.

    Measured with the engine's own `rows_rewritten` counter rather than with
    peak RSS. The claim — the whole-store compactor rewrites `O(store)` rows
    per call, so its total work grows with the build — was never in doubt;
    the instrument was. A peak-RSS ratio for the same configuration, same code, came
    out 8.46x, 4.75x, 4.75x and 4.26x across three hosts and four runs, twice
    of those on the same machine. The counter does not move.
    """
    c_big = measured[("control", N_BIG)]
    t_big = measured[("treatment", N_BIG)]
    work_ratio = c_big["rewritten"] / max(1, t_big["rewritten"])

    assert work_ratio >= MIN_CONTROL_WORK_RATIO, (
        f"the control rewrote {c_big['rewritten']:,} rows at {N_BIG} against "
        f"the treatment's {t_big['rewritten']:,} — only {work_ratio:.2f}x, "
        f"under the {MIN_CONTROL_WORK_RATIO}x this fixture must reproduce "
        f"(10.97x when the bound was set). Either the cadence stopped driving "
        f"the whole-store compactor, or `compact()` stopped rewriting the "
        f"whole store; in both cases every bound in this file is vacuous."
    )

    # secondary: the control must at least grow across the size step, or it
    # is not materialising the store at all
    growth = c_big["delta_kb"] / measured[("control", N_SMALL)]["delta_kb"]
    assert growth > MIN_CONTROL_RSS_GROWTH, (
        f"the control configuration's peak RSS grew only {growth:.2f}x from {N_SMALL} "
        f"to {N_BIG} rows "
        f"({measured[('control', N_SMALL)]['delta_kb'] / 1024:.0f} MB -> "
        f"{c_big['delta_kb'] / 1024:.0f} MB), under the "
        f"{MIN_CONTROL_RSS_GROWTH}x floor. This is the loose check; the "
        f"work-ratio assertion above is the one that discriminates."
    )


def test_the_incremental_build_peaks_far_below_the_whole_store_one(measured):
    small = measured[("treatment", N_SMALL)]
    big = measured[("treatment", N_BIG)]
    control_big = measured[("control", N_BIG)]

    share = big["delta_kb"] / control_big["delta_kb"]
    assert share < MAX_SHARE, (
        f"the trigger-driven build peaked at {big['delta_kb'] / 1024:.0f} MB "
        f"against the whole-store cadence's "
        f"{control_big['delta_kb'] / 1024:.0f} MB at {N_BIG} rows — "
        f"{share:.2f} of it, over the {MAX_SHARE} bound. The transient this "
        f"removes is `compact()` holding every row twice in expanded form "
        f"; the merge holds fan-in x one segment instead, "
        f"so a failure here means the merge is materialising more than one "
        f"merge set at a time."
    )

    growth = big["delta_kb"] / small["delta_kb"]
    assert growth < MAX_GROWTH, (
        f"the trigger-driven build's peak RSS grew {growth:.2f}x from "
        f"{N_SMALL} to {N_BIG} rows ({small['delta_kb'] / 1024:.0f} MB -> "
        f"{big['delta_kb'] / 1024:.0f} MB), over the {MAX_GROWTH}x bound. "
        f"The compaction transient is meant to be flat in n — what is left "
        f"growing is the store's own resident set, not the rewrite."
    )
