"""Crash injection on the single-operation commit path.

A child process writes a deterministic stream of one-operation transactions
through `Store` — a small base graph, then updates, creates and deletes —
consulting the incremental-compaction trigger after every write and running
gc every few compactions, the way a single-op load does. It arms
`GNOMON_CRASH_POINT` after a seed prefix and dies at the next hit. The parent
reopens the store (recovery replays the unapplied log suffix), writes the rest
of the stream, and the content and store digests must equal those of the same
stream written without a crash.

Transaction time comes from the log (`tt = last + 1`), not the wall clock, so
the uninterrupted run and every crashed-and-recovered run assign identical
`tt`s and their store digests are comparable at all.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import gnomon

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

N_OPS = 360
SEED_OPS = 120

_DRIVER = r'''
import json, os, random, sys
import gnomon
from gnomon.core.clock import HybridLogicalClock
from gnomon.core.errors import GnomonError

OPEN_END = 2 ** 62
NV, NE = 120, 360


def stream(n):
    rng = random.Random(38)
    ops = [[{"op": "ingest_events", "events": [], "node_label": "P", "source": "ingest",
             "provenance_ref": None,
             "nodes": [{"uid": f"v{i}", "label": "P", "props": {"p": 0}, "vt_s": 0}
                       for i in range(NV)]}]]
    edges = [{"src": f"v{rng.randrange(NV)}", "dst": f"v{rng.randrange(NV)}", "rel_type": "K",
              "vt_s": 0, "vt_e": OPEN_END} for _ in range(NE)]
    ops.append([{"op": "ingest_events", "events": edges, "offset": 0, "node_label": "P",
                 "source": "ingest", "provenance_ref": None}])
    alive = [(e["src"], e["dst"], f"#{j}") for j, e in enumerate(edges)]
    for i in range(n):
        r = rng.random()
        if i % 41 == 40:
            # a transaction that fails: it rolls back, and on the
            # small-commit path its unpublished batch-mates are re-applied
            op = {"op": "retract", "ref": {"kind": "edge", "src": "v0", "dst": "v0",
                                           "rel_type": "K", "disc": "nosuch"}, "t": 0,
                  "source": "ingest", "provenance_ref": None}
        elif r < 0.8:
            v = min(int(rng.paretovariate(1.1)) - 1, NV - 1)
            op = {"op": "assert_node", "uid": f"v{v}", "label": "P", "props": {"p": i},
                  "vt_s": 0, "vt_e": OPEN_END, "source": "ingest", "provenance_ref": None}
        elif r < 0.9:
            s, d = rng.randrange(NV), rng.randrange(NV)
            op = {"op": "assert_edge", "src": f"v{s}", "dst": f"v{d}", "rel_type": "K",
                  "props": {"w": i}, "vt_s": 0, "vt_e": OPEN_END, "disc": f"c{i}",
                  "source": "ingest", "provenance_ref": None}
            alive.append((f"v{s}", f"v{d}", f"c{i}"))
        else:
            k = rng.randrange(len(alive))
            s, d, disc = alive[k]
            alive[k] = alive[-1]
            alive.pop()
            op = {"op": "retract", "ref": {"kind": "edge", "src": s, "dst": d, "rel_type": "K",
                                           "disc": disc}, "t": 0, "source": "ingest",
                  "provenance_ref": None}
        ops.append([op])
    return ops


def run(path, start, stop, arm_at, point):
    store = gnomon.open(path, backend="native")
    ad = store.adapter
    compactions = 0
    for i, ops in enumerate(stream(stop)[start:stop], start=start):
        if i == arm_at and point:
            os.environ["GNOMON_CRASH_POINT"] = point
        store.clock = HybridLogicalClock(store.clock.last_tt, now_fn=lambda: 0)
        try:
            store._write(ops)
        except GnomonError:
            pass
        while ad.needs_compaction():
            ad.compact_incremental()
            compactions += 1
            if compactions % 4 == 0:
                ad.gc(keep_last=2)
    store.close()


if __name__ == "__main__":
    path, start, stop, arm_at, point = sys.argv[1], *map(int, sys.argv[2:5]), sys.argv[5]
    run(path, start, stop, arm_at, point)
    print("DONE", flush=True)
'''


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(gnomon.__file__).resolve().parent.parent) + \
        os.pathsep + env.get("PYTHONPATH", "")
    env["GNOMON_CRASH_POINT"] = ""
    return env


def _drive(tmp: Path, store: Path, start: int, stop: int, arm_at: int = -1,
           point: str = "", extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    script = tmp / "driver.py"
    if not script.exists():
        script.write_text(_DRIVER)
    env = _env()
    env.update(extra_env or {})
    return subprocess.run([sys.executable, str(script), str(store), str(start), str(stop),
                           str(arm_at), point], capture_output=True, text=True, timeout=900,
                          env=env)


SMALL = {"GNOMON_SMALL_COMMIT": "1", "GNOMON_SMALL_COMMIT_N": "16",
         "GNOMON_SMALL_COMMIT_MS": "100000"}


def _applied(store: Path) -> int:
    """Records in the store's own log, header excluded: how far the stream got."""
    with open(store / "eventlog.jsonl", "rb") as f:
        return sum(1 for line in f if line.strip()) - 1


def _digests(store: Path) -> tuple[str, str]:
    s = gnomon.open(store, backend="native", read_only=True)
    try:
        return s.digest(), s.adapter.content_digest()
    finally:
        s.close()


@pytest.fixture(scope="module")
def reference(tmp_path_factory) -> tuple[str, str]:
    tmp = tmp_path_factory.mktemp("ref")
    proc = _drive(tmp, tmp / "s", 0, N_OPS + 2)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return _digests(tmp / "s")


def crash_and_recover(tmp: Path, point: str, extra_env: dict[str, str] | None = None) -> Path:
    store = tmp / "s"
    total = N_OPS + 2
    proc = _drive(tmp, store, 0, total, arm_at=SEED_OPS, point=point, extra_env=extra_env)
    assert proc.returncode in (134, 137, -6), (
        f"child did not die at {point}: exit={proc.returncode} "
        f"stdout={proc.stdout[-500:]!r} stderr={proc.stderr[-2000:]!r}")
    assert f"GNOMON_CRASH_POINT hit: {point}" in proc.stderr or point.startswith("py_"), \
        proc.stderr[-2000:]
    # recovery is the reopen; then the rest of the stream, unarmed
    done = _applied(store)
    proc = _drive(tmp, store, done, total, extra_env=extra_env)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return store


ENGINE_POINTS = [
    "compact_after_close_merge",  # merged close run durable, manifest not yet
    "compact_before_install",
    "after_checkpoint",  # a full checkpoint durable, CURRENT still on its parent
    "gc_mid_delete",
    "after_seal",
    "after_close_runs",
    "after_manifest",
    "after_current",
]


@pytest.mark.parametrize("point", ENGINE_POINTS)
def test_engine_crash_points_recover_to_the_uninterrupted_digests(tmp_path, reference, point):
    store = crash_and_recover(tmp_path, point)
    assert _digests(store) == reference
    s = gnomon.open(store, backend="native", read_only=True)
    try:
        v = s.adapter.verify()
        assert not v.get("problems"), v["problems"]
    finally:
        s.close()


@pytest.mark.parametrize("point", ["py_after_wal_fsync", "py_before_engine_commit"])
def test_log_append_to_publish_window_recovers_to_the_uninterrupted_digests(
        tmp_path, reference, point):
    store = crash_and_recover(tmp_path, point)
    assert _digests(store) == reference


def test_the_close_run_list_stays_bounded_over_a_single_op_stream(tmp_path):
    proc = _drive(tmp_path, tmp_path / "s", 0, N_OPS + 2)
    assert proc.returncode == 0, proc.stderr[-2000:]
    s = gnomon.open(tmp_path / "s", backend="native", read_only=True)
    try:
        md = tmp_path / "s" / "native" / "manifests"
        ckpts = [json.loads(p.read_text()) for p in sorted(md.glob("*.json"))]
        ckpts = [d for d in ckpts if d.get("kind") == "checkpoint"]
        assert ckpts, "a compacting single-op stream writes checkpoints"
        assert max(len(d["close_runs"]) for d in ckpts) < 64
    finally:
        s.close()


# --------------------------------------------------------------------- #
# the small-commit path (GNOMON_SMALL_COMMIT=1)                            #
# --------------------------------------------------------------------- #


def _log_sha(store: Path) -> str:
    import hashlib
    return hashlib.sha256((store / "eventlog.jsonl").read_bytes()).hexdigest()


def test_the_small_commit_path_writes_the_same_log_and_digests(tmp_path, reference):
    on, off = tmp_path / "on", tmp_path / "off"
    for d, env in ((on, SMALL), (off, None)):
        d.mkdir()
        proc = _drive(d, d / "s", 0, N_OPS + 2, extra_env=env)
        assert proc.returncode == 0, proc.stderr[-2000:]
    assert _digests(on / "s") == _digests(off / "s") == reference
    assert _log_sha(on / "s") == _log_sha(off / "s")
    gens = []
    for d in (on, off):
        s = gnomon.open(d / "s", backend="native", read_only=True)
        gens.append(s.adapter.generation)
        s.close()
    assert gens[0] < gens[1], f"the small-commit path published as many generations: {gens}"


SMALL_POINTS = [
    "py_small_commit_deferred",  # acknowledged: the record is fsynced, not yet published
    "py_small_commit_publish",  # a publish about to start
    "after_seal",  # mid-publish: segments durable, nothing names them
    "after_manifest",  # mid-publish: manifest durable, CURRENT not flipped
    "py_small_commit_reapply",  # re-applying batch-mates after a failed transaction
    "compact_after_close_merge",
]


@pytest.mark.parametrize("point", SMALL_POINTS)
def test_small_commit_crash_points_recover_to_the_digests_of_the_default_path(
        tmp_path, reference, point):
    store = crash_and_recover(tmp_path, point, extra_env=SMALL)
    assert _digests(store) == reference

