"""The per-phase ingest accounting (`gnomon/eval/ingest_phases.py`, recorded by
`scripts/build_synth_store.py` as `build_info.ingest_phase_totals`).

What must hold, and the value each check has to exclude:

* **The phases account for ingest time.** On a small build the residual --
  the part of ingest time no phase claims -- is at most 5 %. A driver whose
  brackets missed a step (say the per-batch compaction trigger) would put
  that step in the residual; the smoke build this was written against had a
  residual of 0.06 %, so 5 % is a bound with room, not a coin flip.
* **The phases are exclusive.** Phase seconds plus the residual equal ingest
  time to within rounding, and ingest time equals the driver's own
  `wall_s`. A composer that double counted an engine body (charging it both
  to ``pyo3.boundary`` and to its steps) would overshoot ingest time.
* **The engine side is decomposed**, not left opaque: seal, commit and
  compaction steps carry time, and the PyO3 boundary is a small fraction.
  If the engine totals were never read, every engine call would land in
  ``engine.*`` and ``seal.fsync`` would be absent.
* **Observation only.** The content digest of a build is the same with the
  accounting on and off.
"""

from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load(name: str, rel: str) -> Any:
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


B = _load("build_synth_store", "scripts/build_synth_store.py")

from gnomon.eval import ingest_phases as IP  # noqa: E402

N = 4_000


def _engine_timers_compiled_in() -> bool:
    try:
        from gnomon.storage.native import NativeAdapter
    except ImportError:
        return False
    return bool(NativeAdapter.build_info().get("ingest_phase_timers"))


def _build(out: Path, compact_every: int, accounting: bool = True) -> dict[str, Any]:
    return B.build(out, N, seed=0, batch=500, compact_every=compact_every,
                   backend="native", resume=False, stop_at_ops=None,
                   digest_mode="full", tick_us=round(52 * B.PARTITION_WIDTH_US / N),
                   interval_fraction=0.5, named_fraction=0.1,
                   phase_accounting=accounting)


@pytest.fixture(scope="module")
def tiered(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return _build(tmp_path_factory.mktemp("tiered") / "s", compact_every=0)


@pytest.mark.parametrize("compact_every", [0, 1_000])
def test_the_phases_account_for_at_least_95_percent_of_ingest_time(
        tmp_path: Path, compact_every: int, tiered: dict[str, Any]) -> None:
    result = tiered if compact_every == 0 else _build(tmp_path / "s", compact_every)
    t = result["record"]["build_info"]["ingest_phase_totals"]
    assert t is not None
    assert t["residual_fraction"] <= 0.05, t["groups"]
    # exclusive: phases + residual == ingest time (rounding only)
    phase_sum = sum(row["s"] for row in t["phases"].values())
    assert phase_sum == pytest.approx(t["ingest_s"], abs=1e-3)
    assert t["accounted_s"] + t["residual_s"] == pytest.approx(t["ingest_s"], abs=1e-5)
    # the clock spans the driver's own ingest timer
    assert t["ingest_s"] == pytest.approx(result["wall_s"], rel=0.01, abs=0.01)
    groups = sum(row["s"] for row in t["groups"].values())
    assert groups == pytest.approx(t["ingest_s"], abs=1e-3)


def test_engine_calls_are_decomposed_not_opaque(tiered: dict[str, Any]) -> None:
    if not _engine_timers_compiled_in():
        pytest.skip("extension built with the ingest timers compiled out")
    t = tiered["record"]["build_info"]["ingest_phase_totals"]
    phases = t["phases"]
    assert t["engine_timers"] is True
    assert not any(k.startswith("engine.") for k in phases)
    for leaf in ("seal.fsync", "seal.encode", "commit.manifest_write_fsync",
                 "compaction.merge_rows", "staging.row_build_push",
                 "eventlog.fsync", "python.row_build.identity_hashing"):
        assert phases.get(leaf, {}).get("s", 0) > 0, leaf
    # the boundary is the conversion cost around engine bodies, not the bodies
    assert phases["pyo3.boundary"]["fraction"] < 0.25
    # the lazily built indexes write nothing during a build: TCSR and the
    # name index are built at first query
    side = t["sidecar_index_files"]
    assert sum(v for k, v in side.items() if k != "vidfilter") == 0, side
    # The identity filters are the deliberate exception -- built at seal,
    # because for that index the first use is the whole cost it removes --
    # and their build time must be *attributed*, not left in the residual
    if side.get("vidfilter", 0):
        built = (phases.get("commit.identity_filter", {}).get("s", 0)
                 + phases.get("compaction.identity_filter", {}).get("s", 0))
        assert built > 0, (
            f"{side['vidfilter']} identity-filter files were written but no "
            f"phase accounts for building them")


def test_the_accounting_does_not_change_what_is_built(
        tmp_path: Path, tiered: dict[str, Any]) -> None:
    off = _build(tmp_path / "off", compact_every=0, accounting=False)
    assert off["record"]["build_info"]["ingest_phase_totals"] is None
    assert off["content_digest"] == tiered["content_digest"]


def test_the_clock_is_exclusive_and_sums_to_its_elapsed_time() -> None:
    c = IP.PhaseClock()
    c.start()
    with c.section("a"):
        time.sleep(0.01)
        with c.section("b"):
            time.sleep(0.02)
        mid = c.snapshot_excl()
        assert sum(mid.values()) == pytest.approx(c.elapsed_ns(), rel=0.01)
    wrapped = c.wrap(lambda: time.sleep(0.005), "c")
    wrapped()
    c.stop()
    assert sum(c.excl.values()) == c.elapsed_ns()
    assert c.excl["b"] >= 20_000_000
    assert 10_000_000 <= c.excl["a"] < 20_000_000  # b is not charged to a
    assert c.calls["c"] == 1


def test_without_engine_timers_each_engine_call_stays_one_opaque_phase() -> None:
    leaves = IP.compose({"unattributed": 5, "pyo3.commit": 100, "python.row_build": 7}, {})
    assert leaves == {"unattributed": 5, "engine.commit": 100, "python.row_build": 7}


def test_a_disabled_clock_installs_nothing(tmp_path: Path) -> None:
    import gnomon

    store = gnomon.open(tmp_path / "s", backend="native")
    try:
        before = dict(store.__dict__)
        undo = IP.instrument(store, IP.PhaseClock(enabled=False))
        assert store.__dict__ == before
        undo()
    finally:
        store.close()
