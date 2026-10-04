"""A store opened read-only writes nothing under its directory.

Found on the measurement host: September stores opened with
`read_only=True` and asked `diff.belief` / `hist.asof` gained identity-filter
sidecars under `native/idx/vidfilter/` (13 files in one store, 2 in each of
two others) and moved the mtimes of `native/idx`. The lazy filter build
inside the fold path persisted its file with no read-only check, and the
engine itself had no notion of a read-only handle at all — `NativeAdapter`
took no `read_only` parameter, so the only thing that made a handle a
reader was `Store` refusing its own write API.

The property tested here is the strong one: after a read-only handle has
answered **every query in the benchmark registry**, run the harness-bypass
operators (`lookup_name`, `diff_belief`), folded identity postings, traversed
the adjacency index (both the per-segment Rust index and the
`GNOMON_TCSR=python` opt-out), built the sorted name index, asked for every
explicit index build, digested, verified and reported stats, the store
directory's file list, sizes and mtimes are **identical** to what they were
before the handle opened — every file and every directory, walked
recursively.

Every store starts with **no sidecars at all** (built with
`GNOMON_VIDFILTER=0` and never queried), which is the shape of a legacy store
and the one in which every lazy build has something to build. Each test
copies a pristine store first, so one surface's writes can never satisfy
another surface's test by making its lazy build a load.

The read-write control at the bottom is what keeps this suite honest: the
same workload on a read-write handle *does* persist sidecars, so a tree
that does not move under a read-only handle is the guard working, not a
workload that never reached the lazy paths.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest

import gnomon
from gnomon.core.model import OPEN_END, edge_eid
from gnomon.storage.eventlog import EventLog

ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, relpath: str) -> Any:
    # same shape as tests/test_diff_belief.py: scripts import their siblings
    # unqualified, so scripts/ has to be on sys.path
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


H = _load_module("eval_harness", "scripts/eval_harness.py")

#: Small enough for the everyday suite, large enough that the store has many
#: segments (one per write batch) and every registry query has rows to read.
SCALE = int(os.environ.get("GNOMON_READONLY_SCALE", "1200"))


# --------------------------------------------------------------------------- #
# the oracle: a recursive (path, kind, size, mtime) listing                    #
# --------------------------------------------------------------------------- #

def _tree(root: Path) -> dict[str, tuple[str, int, int]]:
    """Every entry under `root` (directories included, `root` itself as
    `.`), as `relpath -> (kind, size, mtime_ns)`. A created, removed,
    resized or rewritten file shows up here, and so does a directory whose
    entry list changed (its mtime moves) — the September evidence was
    exactly a new directory plus the moved mtime of its parent."""
    out: dict[str, tuple[str, int, int]] = {}
    st = root.stat()
    out["."] = ("d", st.st_size, st.st_mtime_ns)
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            p = Path(dirpath) / name
            st = p.lstat()
            kind = "d" if p.is_dir() and not p.is_symlink() else "f"
            out[str(p.relative_to(root))] = (kind, st.st_size, st.st_mtime_ns)
    return out


def _diff(before: dict, after: dict) -> list[str]:
    lines = []
    for k in sorted(set(before) | set(after)):
        a, b = before.get(k), after.get(k)
        if a != b:
            lines.append(f"{k}: {a} -> {b}")
    return lines


def _assert_unchanged(before: dict, after: dict, what: str) -> None:
    d = _diff(before, after)
    assert not d, (f"{what} wrote under the store directory "
                   f"({len(d)} entries changed):\n  " + "\n  ".join(d[:40]))


@contextmanager
def _env(**kv: str | None) -> Iterator[None]:
    old = {k: os.environ.get(k) for k in kv}
    try:
        for k, v in kv.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# --------------------------------------------------------------------------- #
# fixtures: two pristine stores, never opened after they were written          #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def corrected() -> Any:
    """The harness's own reference store: bulk events, then a second belief
    epoch of corrections, partial corrections and retractions. Built with
    the seal-time filter build off, so no `idx/` sidecar exists."""
    with _env(GNOMON_VIDFILTER="0"):
        data = H.build_dataset(SCALE)
    store_dir = data.log.parent
    assert not any((store_dir / "native" / "idx").rglob("*.*")), \
        "the fixture must start with no sidecars"
    return data


@pytest.fixture(scope="module")
def plain() -> Any:
    """A correction-free store: the same bulk events and nothing else, so
    every row is believed and every fold runs with no close to apply."""
    path = Path(tempfile.mkdtemp()) / "plain"
    with _env(GNOMON_VIDFILTER="0"):
        store = gnomon.open(path, backend="native")
        store.ingest_events([H._event(i, SCALE) for i in range(SCALE)])
        store.close()
    last_tt = EventLog(path / "eventlog.jsonl").last_tt()
    return H.Dataset(path / "eventlog.jsonl", SCALE, last_tt, t0=0, t1=SCALE,
                     filter_uids=tuple(f"n{i}" for i in range(H.MOTIF_FILTER)))


def _copy(data: Any, tmp_path: Path) -> Path:
    dst = tmp_path / "store"
    shutil.copytree(data.log.parent, dst)
    return dst


def _stores(request: pytest.FixtureRequest, which: str) -> Any:
    return request.getfixturevalue(which)


# --------------------------------------------------------------------------- #
# the workload                                                                 #
# --------------------------------------------------------------------------- #

def _registry_and_extras(adapter: Any, data: Any) -> dict[str, str | None]:
    """Run the whole benchmark registry the way `run_system` does, plus
    every read the native engine has a lazy sidecar for. Returns
    `query id -> error or None`, so a query that cannot run on a backend is
    recorded rather than masking the write check."""
    from gnomon.temporal.algebra import call_operator, ensure_all_registered

    ensure_all_registered()
    out: dict[str, str | None] = {}
    queries = H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    for q in queries:
        try:
            if q.op == "lookup_name":
                H._lookup_name_dispatch(adapter, q.args)
            elif q.op == "diff_belief":
                H._diff_belief_dispatch(adapter, q.args)
            else:
                call_operator(adapter, q.op, dict(q.args), skip_cost_check=True)
            out[q.id] = None
        except Exception as e:  # noqa: BLE001 - recorded, asserted on below
            out[q.id] = f"{type(e).__name__}: {e}"
    return out


def _edge_ids(n: int = 3) -> list[str]:
    out = []
    for i in range(1, 1 + n):
        r = H._edge_ref(i * 7, SCALE)
        out.append(edge_eid(r.src, r.dst, r.rel_type, r.disc))
    return out


def _identity_folds(engine: Any) -> None:
    """Point identity lookups on both row kinds: each one folds the
    postings of the segments it has to search, which is where the lazy
    filter step fires."""
    for uid in ("n1", "n2", "n3"):
        engine.believed("node", uid, OPEN_END)
    for eid in _edge_ids():
        engine.believed("edge", eid, OPEN_END)


def _native_extras(adapter: Any, data: Any) -> None:
    """The native-only reads that reach a lazy sidecar or an engine path a
    registry query may not: identity folds on both kinds (the lazy filter
    step), belief diffs at several windows, every name-index mode, the
    explicit index builds, digest, verify and stats."""
    _identity_folds(adapter._store)
    w = H._w10_window(data.t0, data.t1)
    adapter.diff_belief(0, OPEN_END, w["t_a"], w["t_b"], 1000)
    adapter.diff_belief(0, OPEN_END, data.t0, data.t1, 100_000)
    for mode, q in (("exact", "n1"), ("prefix", "alp"), ("prefix", "n")):
        list(adapter.lookup_name(q, mode))
    adapter.stats()
    adapter.store_digest()
    adapter.verify("fast")
    adapter.verify("full")
    adapter.build_indexes(tcsr=True, name=True, vidfilter=True)


def _assert_lazy_paths_ran(adapter: Any) -> None:
    """The workload must actually have reached the lazy builds, or an
    unchanged tree proves nothing."""
    vf = adapter.vidfilter_stats()
    assert vf["lazy_built"] + vf["built"] > 0, f"no identity filter was built: {vf}"
    tc = adapter.tcsr_stats()
    assert tc["built"] > 0, f"no adjacency index was built: {tc}"


# --------------------------------------------------------------------------- #
# read-only surfaces                                                           #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("which", ["corrected", "plain"])
@pytest.mark.parametrize("opener", ["gnomon.open", "Store", "gnomon.open+backend"])
def test_a_read_only_store_writes_nothing(request, tmp_path, which, opener):
    data = _stores(request, which)
    path = _copy(data, tmp_path)
    before = _tree(path)
    if opener == "gnomon.open":
        store = gnomon.open(path, read_only=True)
    elif opener == "Store":
        from gnomon.store import Store
        store = Store(path, read_only=True)
    else:
        store = gnomon.open(path, backend="native", read_only=True)
    try:
        errors = _registry_and_extras(store.adapter, data)
        _native_extras(store.adapter, data)
        _assert_lazy_paths_ran(store.adapter)
        store.store_identity
        store.digest()
        store.stats()
    finally:
        store.close()
    _assert_unchanged(before, _tree(path), f"{opener}(read_only=True) on {which}")
    failed = {k: v for k, v in errors.items() if v}
    assert not failed, f"registry queries failed on a read-only handle: {failed}"


@pytest.mark.parametrize("which", ["corrected", "plain"])
def test_a_read_only_native_adapter_writes_nothing(request, tmp_path, which):
    """`NativeAdapter` constructed directly (what `gnomon store verify` and
    the measurement scripts do), with `read_only=True`."""
    from gnomon.storage.native import NativeAdapter

    data = _stores(request, which)
    path = _copy(data, tmp_path)
    before = _tree(path)
    adapter = NativeAdapter(path / "native", read_only=True)
    try:
        errors = _registry_and_extras(adapter, data)
        _native_extras(adapter, data)
        _assert_lazy_paths_ran(adapter)
    finally:
        adapter.close()
    _assert_unchanged(before, _tree(path), f"NativeAdapter(read_only=True) on {which}")
    assert not {k: v for k, v in errors.items() if v}


def test_a_read_only_engine_handle_writes_nothing(corrected, tmp_path):
    """The extension type itself, `_engine.NativeStore(path, read_only=True)`:
    the lazy filter (identity fold), the adjacency index (traversal), the
    name index (lookup) and the explicit builds of all three."""
    from gnomon import _engine

    path = _copy(corrected, tmp_path)
    before = _tree(path)
    s = _engine.NativeStore(str(path / "native"), read_only=True)
    assert s.is_read_only()
    w = H._w10_window(corrected.t0, corrected.t1)
    s.diff_belief(0, OPEN_END, w["t_a"], w["t_b"], 1000)
    _identity_folds(s)
    s.neighbors_of(0, "out")
    s.lookup_name("n1", "exact")
    s.lookup_name("alp", "prefix")
    s.build_indexes()
    s.build_vid_filters()
    s.build_name_indexes()
    assert s.vidfilter_stats()["lazy_built"] + s.vidfilter_stats()["built"] > 0
    assert s.tcsr_stats()["built"] > 0
    s.close()
    del s
    _assert_unchanged(before, _tree(path), "_engine.NativeStore(read_only=True)")


def test_the_python_tcsr_opt_out_writes_nothing_on_a_read_only_handle(corrected, tmp_path):
    """`GNOMON_TCSR=python` persists a whole-store permutation under
    `native/index/tcsr.npz` on a read-write handle; a reader must keep it in
    memory."""
    path = _copy(corrected, tmp_path)
    before = _tree(path)
    with _env(GNOMON_TCSR="python"):
        store = gnomon.open(path, read_only=True)
        try:
            errors = _registry_and_extras(store.adapter, corrected)
            assert store.adapter._tcsr is not None, "the opt-out index was never built"
        finally:
            store.close()
    _assert_unchanged(before, _tree(path), "GNOMON_TCSR=python read-only handle")
    assert not {k: v for k, v in errors.items() if v}


def test_the_read_only_cli_actions_write_nothing(corrected, tmp_path, capsys):
    """`gnomon store digest` and `gnomon store verify` (both modes) are the
    CLI's read-only actions."""
    from gnomon.cli import main

    path = _copy(corrected, tmp_path)
    before = _tree(path)
    for argv in (["store", "digest", "--store", str(path)],
                 ["store", "verify", "--store", str(path)],
                 ["store", "verify", "--full", "--store", str(path)]):
        try:
            main(argv)
        except SystemExit:
            pass
    capsys.readouterr()
    _assert_unchanged(before, _tree(path), "the read-only CLI actions")


def test_a_read_only_store_with_no_event_log_creates_none(corrected, tmp_path):
    """A native store whose `eventlog.jsonl` is absent (a copy of the engine
    directory alone): a read-only open answers from the engine and does not
    create the log."""
    path = _copy(corrected, tmp_path)
    (path / "eventlog.jsonl").unlink()
    (path / "writer.lock").unlink(missing_ok=True)
    before = _tree(path)
    store = gnomon.open(path, backend="native", read_only=True)
    try:
        assert store.adapter.stats()
        H._diff_belief_dispatch(store.adapter, {
            "t1": 0, "t2": OPEN_END, "window": H._w10_window(0, SCALE), "limit": 10})
    finally:
        store.close()
    _assert_unchanged(before, _tree(path), "read-only open without an event log")


def test_read_only_on_a_directory_with_no_store_writes_nothing(tmp_path):
    """An existing, empty directory opened read-only: refused, and nothing
    created — not `native/`, not a genesis manifest, not an event log."""
    path = tmp_path / "empty"
    path.mkdir()
    before = _tree(path)
    with pytest.raises(Exception):
        store = gnomon.open(path, backend="native", read_only=True)
        store.close()
    _assert_unchanged(before, _tree(path), "read-only open of an empty directory")


def test_a_read_only_engine_handle_refuses_every_writer_entry_point(corrected, tmp_path):
    """Defence in depth below `Store`'s own write refusal: the maintenance
    entry points callers reach on the adapter directly (compaction, settle,
    gc, the manifest upgrade) are refused by the engine, and refused
    *before* they write anything — a compaction that wrote its new segments
    and then failed at publication would leave orphans."""
    from gnomon.storage.native import NativeAdapter

    path = _copy(corrected, tmp_path)
    before = _tree(path)
    adapter = NativeAdapter(path / "native", read_only=True)
    try:
        calls = {
            "compact": lambda: adapter.compact(),
            "compact_incremental": lambda: adapter.compact_incremental(),
            "compact_current_only": lambda: adapter.compact_current_only(),
            "settle": lambda: adapter.settle(),
            "gc": lambda: adapter.gc(keep_last=1),
            "upgrade_manifests": lambda: adapter.upgrade_manifests(),
            "begin": lambda: adapter._store.begin(1 << 62),
        }
        for name, call in calls.items():
            with pytest.raises(Exception) as e:
                call()
            assert "read-only" in str(e.value) or "read only" in str(e.value), \
                f"{name}: {e.value}"
    finally:
        adapter.close()
    _assert_unchanged(before, _tree(path), "refused writer entry points")


@pytest.mark.parametrize("backend", ["duckdb"])
def test_a_read_only_portable_backend_writes_nothing(tmp_path, corrected, backend):
    """The same registry on a read-only DuckDB store."""
    pytest.importorskip(backend)
    path = tmp_path / backend
    H.load_store(path, backend, corrected.log)
    before = _tree(path)
    store = gnomon.open(path, backend=backend, read_only=True)
    try:
        _registry_and_extras(store.adapter, corrected)
        store.stats()
    finally:
        store.close()
    _assert_unchanged(before, _tree(path), f"read-only {backend} store")


# --------------------------------------------------------------------------- #
# the control: a read-write handle keeps today's behaviour                      #
# --------------------------------------------------------------------------- #

def test_a_read_write_handle_still_persists_its_sidecars(corrected, tmp_path):
    """Same workload, read-write handle: the lazy filters, the adjacency
    index and the name index are written under `native/idx/`, exactly as
    before. Without this the read-only tests could pass on a workload that
    never reached a lazy build."""
    path = _copy(corrected, tmp_path)
    before = _tree(path)
    store = gnomon.open(path)
    try:
        _registry_and_extras(store.adapter, corrected)
        assert store.adapter.vidfilter_stats()["lazy_built"] > 0
    finally:
        store.close()
    after = _tree(path)
    assert _diff(before, after), "a read-write handle persisted nothing"
    idx = path / "native" / "idx"
    for sub in ("vidfilter", "tcsr"):
        assert any((idx / sub).iterdir()), f"no {sub} sidecar persisted"
    assert any(p.suffix == ".tnx" for p in idx.rglob("*")), "no name index persisted"
