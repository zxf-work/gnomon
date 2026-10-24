"""The per-segment Rust TCSR: structure, lifecycle, and the index rule.

The design move is
that the index is built over **all stored rows of one immutable segment**
rather than over the store's current beliefs. Everything this file pins
follows from that one property:

* a commit cannot invalidate an existing segment's index, because it does not
  change any existing segment — it only adds new ones;
* the stamp is therefore the segment's own `body_sha`, not the manifest
  generation, so an index can never be used against bytes it was not computed
  from;
* and because belief filtering moved to traversal time, a *correction* also
  leaves the index alone — the closed row is still in it, and is filtered out
  when the slice is read.

The index rule (stated at `gnomon/storage/tcsr.py:131`)
is the other half: an index is rebuildable and never authoritative, so every
way a `.tci` file can be wrong must degrade to a rebuild, and a failure to
*write* one must never fail a query.

These run against the **native** engine regardless of `GNOMON_TEST_BACKEND` —
`idx/tcsr/` is a native concept with no portable-backend equivalent, and a
DuckDB run would pass every test here vacuously.

This file pins the **default**: an unset `GNOMON_TCSR` selects this index, and
`GNOMON_TCSR=python` selects the NumPy whole-store one, which remains the
portable backends' implementation and is never overridden for them. Those
three tests are the single executable statement of which side is the default;
everything above them sets the flag explicitly and is indifferent to it.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

from gnomon.storage.native import NativeAdapter  # noqa: E402
from gnomon.storage.native.adapter import NativeTCSR  # noqa: E402

RING = 7


def _events(n: int, start: int = 0) -> list[dict]:
    return [
        {"src": f"n{i % RING}", "dst": f"n{(i + 1) % RING}", "rel_type": "R",
         "vt_s": i, "vt_e": i + 5, "props": {"w": i}}
        for i in range(start, start + n)
    ]


def _store(root: Path, n: int = 60):
    import gnomon

    store = gnomon.open(root, backend="native")
    store.ingest_events(_events(n))
    return store


def _rust(monkeypatch, adapter) -> None:
    """Point this adapter's shared index at the Rust engine and drop whatever
    it was holding, so the next `tcsr()` rebuilds on the other side."""
    monkeypatch.setenv("GNOMON_TCSR", "rust")
    adapter._tcsr = None


def _idx_dir(adapter) -> Path:
    return adapter.path / "idx" / "tcsr"


def _tci_files(adapter) -> list[str]:
    d = _idx_dir(adapter)
    return sorted(p.name for p in d.glob("*.tci")) if d.exists() else []


def _out(adapter, uid: str, direction: str = "out", t_max: int | None = None):
    """`(nbr, vt_s, vt_e)` for one uid through whatever index is configured —
    the `row` column is deliberately left out, because its values are index
    positions and the two backends number them differently by construction."""
    u = int(adapter.dense_ids([uid])[0])
    csr, _cols = adapter.tcsr()
    nbr, vt_s, vt_e, _row = csr.neighbors(u, direction, t_max=t_max)
    return [np.asarray(a).tolist() for a in (nbr, vt_s, vt_e)]


# --- 1. the file appears, is loaded, and is stamped to its segment -------- #


def test_a_traversal_builds_the_index_and_a_reopen_loads_it(tmp_path, monkeypatch):
    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    assert not _tci_files(a), "nothing is built until something traverses"

    want = _out(a, "n0")
    assert want[0], "the fixture graph must have out-edges"
    files = _tci_files(a)
    assert files, "the first traversal must persist the index it built"
    assert a.tcsr_stats()["built"] >= 1
    assert a.tcsr_stats()["loaded"] == 0

    fresh = NativeAdapter(a.path)
    got = _out(fresh, "n0")
    assert got == want
    assert fresh.tcsr_stats()["built"] == 0, "a second process must not rebuild"
    assert fresh.tcsr_stats()["loaded"] == len(files)
    fresh.close()
    store.close()


def test_the_cli_pre_builds_every_segment_and_is_idempotent(tmp_path, capsys):
    from gnomon.cli import main

    store = _store(tmp_path)
    segments = len(store.adapter._store.stats()["edge_segments"]) \
        if isinstance(store.adapter._store.stats().get("edge_segments"), list) else None
    store.close()

    assert main(["store", "build-indexes", "--store", str(tmp_path)]) == 0
    first = json.loads(capsys.readouterr().out)["tcsr"]
    assert first["segments"] >= 1
    assert first["built"] == first["segments"], "nothing on disk yet"
    assert first["loaded"] == 0
    if segments is not None:
        assert first["segments"] == segments

    assert main(["store", "build-indexes", "--store", str(tmp_path), "--tcsr"]) == 0
    second = json.loads(capsys.readouterr().out)["tcsr"]
    assert second["segments"] == first["segments"]
    assert second["built"] == 0, "an index already on disk is loaded, not rebuilt"
    assert second["loaded"] == second["segments"]


# --- 2. a commit does not invalidate an existing segment's index ---------- #


def test_a_commit_rebuilds_only_the_segments_it_added(tmp_path, monkeypatch):
    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    before = a.build_indexes()["tcsr"]
    assert before["built"] == before["segments"] >= 1

    store.ingest_events(_events(20, start=1000))
    after = NativeAdapter(a.path).build_indexes()["tcsr"]
    assert after["segments"] > before["segments"], "the fixture must add a segment"
    assert after["built"] == after["segments"] - before["segments"], (
        f"a commit must invalidate nothing: {before['segments']} segments had "
        f"indexes, the store now has {after['segments']}, and "
        f"{after['built']} were built — the index is over all stored rows of "
        f"an immutable segment, so only the new ones can need building"
    )
    assert after["loaded"] == before["segments"]
    store.close()


def test_a_correction_leaves_the_existing_indexes_alone(tmp_path, monkeypatch):
    """Belief filtering is at traversal time, so a correction changes what
    comes *out* of an index without changing the index. The segments the
    correction's own commit adds get indexes of their own; the ones that were
    already there must be byte-identical afterwards."""
    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    before = _out(a, "n0")
    assert before[2][0] == 5, "the first n0 edge is believed over [0, 5)"
    a.build_indexes()
    was = {p.name: p.read_bytes() for p in _idx_dir(a).glob("*.tci")}
    assert was

    # `ingest_events` discriminates each event (`disc="#<i>"`), so this names
    # one logical edge: the n0 -> n1 version the first event wrote. Retracting
    # from vt 3 closes it and writes the carved remainder [0, 3).
    from gnomon.core.model import EntityRef

    store.retract(
        EntityRef(kind="edge", src="n0", dst="n1", rel_type="R", disc="#0"), t=3)
    a._tcsr = None
    after = _out(a, "n0")
    assert after != before, "the correction must be visible to the traversal"
    assert after[2][0] == 3, f"the carved version ends at 3, got {after[2][0]}"

    now = {p.name: p.read_bytes() for p in _idx_dir(a).glob("*.tci")}
    for name, raw in was.items():
        assert now.get(name) == raw, (
            f"{name} changed across a correction — the index is over all "
            f"stored rows of an immutable segment, so a close cannot touch it"
        )
    fresh = NativeAdapter(a.path)
    rebuilt = fresh.build_indexes()["tcsr"]
    assert rebuilt["loaded"] >= len(was), "every pre-existing index was reused"
    fresh.close()
    store.close()


# --- 3. every way the file can be wrong degrades to a rebuild ------------- #


@pytest.mark.parametrize("damage", ["truncate", "garbage", "flip", "empty"])
def test_a_damaged_index_degrades_to_a_rebuild_never_an_error(
        tmp_path, monkeypatch, damage):
    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    want = _out(a, "n2")
    a.build_indexes()
    victim = _idx_dir(a) / _tci_files(a)[0]
    raw = victim.read_bytes()

    if damage == "truncate":
        victim.write_bytes(raw[: len(raw) // 2])
    elif damage == "garbage":
        victim.write_bytes(b"not a Gnomon index file at all")
    elif damage == "empty":
        victim.write_bytes(b"")
    else:
        mid = bytearray(raw)
        mid[len(mid) // 2] ^= 0xFF
        victim.write_bytes(bytes(mid))

    fresh = NativeAdapter(a.path)
    assert _out(fresh, "n2") == want, "damage means rebuild, never a wrong answer"
    assert fresh.tcsr_stats()["built"] >= 1
    # the rebuild replaced the damaged bytes, and reproduced the original
    # exactly — the index is a pure function of the segment, so a rebuild is
    # byte-identical to the build it replaces
    assert victim.read_bytes() == raw
    assert NativeAdapter(a.path).build_indexes()["tcsr"]["built"] == 0
    fresh.close()
    store.close()


def test_an_index_stamped_for_another_segment_is_never_trusted(tmp_path, monkeypatch):
    """The store-restored-from-backup case: two segments' index files
    swapped. Each file is internally perfect — right magic, right CRCs — and
    each is stamped with the *other* segment's body sha, which is the only
    thing that can catch it."""
    store = _store(tmp_path)
    store.ingest_events(_events(20, start=1000))
    a = store.adapter
    _rust(monkeypatch, a)
    want = {uid: _out(a, uid) for uid in ("n0", "n3")}
    a.build_indexes()
    files = _tci_files(a)
    assert len(files) >= 2, "the fixture must hold at least two segments"

    first, second = (_idx_dir(a) / f for f in files[:2])
    first_raw, second_raw = first.read_bytes(), second.read_bytes()
    first.write_bytes(second_raw)
    second.write_bytes(first_raw)

    fresh = NativeAdapter(a.path)
    assert {uid: _out(fresh, uid) for uid in ("n0", "n3")} == want
    assert fresh.tcsr_stats()["built"] >= 2, "both swapped files were rebuilt"
    fresh.close()
    store.close()


def test_a_missing_index_directory_is_not_an_error(tmp_path, monkeypatch):
    import shutil

    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    want = _out(a, "n1")
    a.build_indexes()
    shutil.rmtree(_idx_dir(a))

    fresh = NativeAdapter(a.path)
    assert _out(fresh, "n1") == want
    assert _tci_files(fresh), "the rebuild recreated the directory"
    fresh.close()
    store.close()


def test_an_unwritable_index_path_answers_the_query_anyway(tmp_path, monkeypatch):
    """A store that cannot take the write — read-only mount, disk full —
    still answers; it just rebuilds next time (`tcsr.py:131-135`)."""
    import shutil

    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    want = _out(a, "n1")
    shutil.rmtree(_idx_dir(a), ignore_errors=True)
    # a *file* where the directory has to be: create_dir_all and the write
    # both fail, and the query must not notice
    _idx_dir(a).parent.mkdir(parents=True, exist_ok=True)
    _idx_dir(a).write_bytes(b"not a directory")

    fresh = NativeAdapter(a.path)
    assert _out(fresh, "n1") == want
    assert fresh.tcsr_stats()["built"] >= 1
    fresh.close()
    store.close()


# --- 4. gc collects an index exactly when it collected its segment -------- #


def test_gc_collects_the_indexes_of_the_segments_it_collected(tmp_path, monkeypatch):
    store = _store(tmp_path)
    store.ingest_events(_events(20, start=1000))
    a = store.adapter
    _rust(monkeypatch, a)
    a.build_indexes()
    before = _tci_files(a)
    assert len(before) >= 2

    a.compact()
    assert _tci_files(a) == before, "compact() is not a sweep"
    report = a.gc(keep_last=1)
    assert report["segments_removed"] >= 2

    after = _tci_files(a)
    assert len(after) < len(before), (
        f"the indexes of collected segments must go with them: {before} -> {after}"
    )
    live = {p.name for p in (a.path / "seg").glob("*.tgs")}
    assert all(f"{name[:-4]}.tgs" in live for name in after), (
        f"every surviving index must name a surviving segment: {after} vs {live}"
    )
    assert a.verify()["healthy"]
    store.close()


def test_compact_can_pre_build_the_indexes_it_just_invalidated(tmp_path, monkeypatch):
    """`compact(build_indexes=True)` — the escape hatch for benchmark
    stores, so a measured traversal is steady-state rather than including a
    first-query build. Off by default, because compaction time is a published
    number."""
    store = _store(tmp_path)
    store.ingest_events(_events(20, start=1000))
    a = store.adapter
    _rust(monkeypatch, a)

    a.compact()
    assert not _tci_files(a), "off by default: compaction builds nothing"

    a.compact(build_indexes=True)
    built = _tci_files(a)
    assert built, "build_indexes=True must leave the compacted segments indexed"
    assert NativeAdapter(a.path).build_indexes()["tcsr"]["built"] == 0
    assert a.verify()["healthy"]
    store.close()


def test_the_compact_flag_covers_the_name_index_too(tmp_path, monkeypatch):
    """`compact(build_indexes=True)` means *both* index kinds, as settle's
    flag already did.

    Compaction rewrites segments, so it orphans the per-segment TCSR **and**
    the sorted name index by exactly the same mechanism: a rewritten segment
    is a new file with a new id and a new `body_sha`, and both indexes are
    stamped with it. Before this change this flag rebuilt only the TCSR, so the next
    `lookup_name` paid a build the caller had already asked to have paid —
    a silent asymmetry with settle, whose `build_indexes(name=True)` covered
    both.

    "Loads rather than builds" is asserted on the counters that mean exactly
    that, from a **fresh handle** so nothing in-process can answer from a
    cache: `build_indexes()["tcsr"]["built"]` and `build_name_indexes()` both
    report how many segments needed (re)building, so zero from both is the
    statement that a first query of either kind finds its index already on
    disk. The fixture-is-real half is the configuration above it: the same store, the
    same compaction, with the flag off, where both counters are non-zero.
    """
    from gnomon.core.model import OPEN_END

    def counters(path):
        """(tcsr segments needing a build, name segments needing a build),
        from a handle that has never traversed anything."""
        h = NativeAdapter(path)
        try:
            return h.build_indexes()["tcsr"]["built"], h.build_name_indexes()
        finally:
            h.close()

    store = _store(tmp_path)
    store.ingest_events(_events(20, start=1000))
    a = store.adapter
    _rust(monkeypatch, a)
    assert a.lookup_name("n0", "exact", OPEN_END), "fixture is real: n0 resolves"

    # the configuration the flag exists to avoid: compact without it, and both kinds
    # of first query have to build.
    a.compact()
    cold_tcsr, cold_name = counters(a.path)
    assert cold_tcsr > 0 and cold_name > 0, (
        f"fixture is not real: a compaction with the flag off should leave "
        f"both kinds to build, got tcsr={cold_tcsr} name={cold_name}")

    # and with it, neither does. The name half is the regression this test
    # exists for; the tcsr half is here so a future change cannot fix one by
    # breaking the other.
    store.ingest_events(_events(20, start=2000))
    a.compact(build_indexes=True)
    warm_tcsr, warm_name = counters(a.path)
    assert warm_tcsr == 0, f"{warm_tcsr} TCSR index(es) still needed building"
    assert warm_name == 0, (
        f"{warm_name} name index(es) still needed building — "
        f"compact(build_indexes=True) must cover both kinds, as settle does")

    # the answers are unchanged by any of it
    assert a.lookup_name("n0", "exact", OPEN_END), "n0 still resolves"
    assert _out(a, "n0")[0], "n0 still has out-neighbours"
    assert a.verify()["healthy"]
    store.close()


def test_the_incremental_compactor_keeps_the_same_flag_meaning(tmp_path,
                                                               monkeypatch):
    """`compact_incremental`'s docstring says its flag mirrors `compact()`'s.
    The mirror is kept literal: both kinds, or the docstring is a lie the
    next reader inherits. Cheap here by construction — this compactor leaves
    untouched segments their files, their `body_sha` and therefore their
    indexes, so the rebuild loads almost everything."""
    store = _store(tmp_path)
    for start in (1000, 2000, 3000):
        store.ingest_events(_events(20, start=start))
    a = store.adapter
    _rust(monkeypatch, a)
    a.compact_incremental(build_indexes=True)

    fresh = NativeAdapter(a.path)
    try:
        assert fresh.build_indexes()["tcsr"]["built"] == 0
        assert fresh.build_name_indexes() == 0
    finally:
        fresh.close()
    store.close()


# --- 5. residency: the shape the memory model predicts ------------------- #


def test_resident_bytes_are_o_active_vertices_plus_rows(tmp_path, monkeypatch):
    """The index holds `u32` offsets and a `u32` row permutation and
    nothing else — no gathered `nbr/vt_s/vt_e/row` int64 columns, which is
    where the 18 % resident tax lived. Dense costs `4*(|V|+1+n)` per
    direction, sparse `4*(m + m+1 + n)`; both are bounded by
    `4 * (2*(|V|+1) + 2*n)` for this store's id space."""
    store = _store(tmp_path, n=200)
    a = store.adapter
    _rust(monkeypatch, a)
    _out(a, "n0")
    a.build_indexes()
    stats = a.tcsr_stats()

    rows = len(a.edges_columnar(columns=("vt_s",))["vt_s"])
    n_entities = a.num_entities()
    ceiling = 4 * 2 * (rows + n_entities + 1) * stats["segments_cached"]
    assert 0 < stats["resident_bytes"] <= ceiling, (
        f"{stats['resident_bytes']} B over {rows} rows / {n_entities} entities "
        f"in {stats['segments_cached']} segments exceeds the "
        f"4*(|V|+1+n) per direction bound of {ceiling} B"
    )
    # the gathered form the Python index keeps is 4 int64 columns per
    # direction; the whole point is that this is far under it
    assert stats["resident_bytes"] < 2 * 4 * 8 * rows or rows < 64
    store.close()


def test_release_drops_residency_without_changing_answers(tmp_path, monkeypatch):
    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    want = _out(a, "n4")
    assert a.tcsr_stats()["resident_bytes"] > 0

    a.release_tcsr()
    assert a.tcsr_stats()["resident_bytes"] == 0
    assert a.tcsr_stats()["segments_cached"] == 0
    assert _out(a, "n4") == want
    store.close()


# --- 6. the shim's own contract ------------------------------------------ #


def test_the_shim_accumulator_is_chunked_and_budgeted(tmp_path, monkeypatch):
    """The accumulator holds the engine's own `int64` arrays and concatenates
    a column once, lazily. It is also bounded: `tcsr()` hands out a fresh
    accumulator once the held one is over `SHIM_ROW_BUDGET`, at an operator
    boundary, so a long-lived reader process cannot grow it without bound."""
    from gnomon.storage.native.adapter import SHIM_ROW_BUDGET, _ShimCols

    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    csr, cols = a.tcsr()
    assert isinstance(cols, _ShimCols)
    for uid in ("n0", "n1", "n2"):
        csr.neighbors(int(a.dense_ids([uid])[0]), "out")

    rows = cols.rows_accumulated()
    assert rows > 0
    assert len(cols["vt_s"]) == rows
    assert cols["vt_s"] is cols["vt_s"], "a materialized column is cached"
    assert cols.resident_bytes() > 0
    # 6 int64 columns, held as chunks and once flattened
    assert cols.resident_bytes() <= 2 * 6 * 8 * rows

    # under budget, the same pair comes back
    assert a.tcsr()[1] is cols
    # over budget, the next operator gets a fresh one — and the old object is
    # still valid for whoever holds it
    cols._n = SHIM_ROW_BUDGET + 1
    fresh_csr, fresh_cols = a.tcsr()
    assert fresh_cols is not cols
    assert fresh_cols.rows_accumulated() == 0
    assert len(cols["vt_s"]) == rows, "the old accumulator stays consistent"
    assert len(fresh_csr.neighbors(int(a.dense_ids(["n0"])[0]), "out")[0]) > 0
    store.close()




def test_the_shim_serves_the_columns_its_callers_index_by_row(tmp_path, monkeypatch):
    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    u = int(a.dense_ids(["n0"])[0])
    csr, cols = a.tcsr()
    nbr, vt_s, vt_e, row = csr.neighbors(u, "out")
    assert len(row) == len(nbr) > 0

    assert "eid" not in cols, (
        "`temporal_paths` branches on this: the native path must take the "
        "`edge_idents_at` route, not carry a sha256 per scanned row"
    )
    assert "vid" not in cols, (
        "`Adjacency.step`'s `both` merge must break ties on `row`, exactly as "
        "it does over the Python index built from TCSR_COLS"
    )
    for name in a.TCSR_COLS:
        assert name in cols
        assert cols[name].dtype == np.int64

    assert cols["src_id"][row].tolist() == [u] * len(row)
    assert cols["dst_id"][row].tolist() == nbr.tolist()
    assert cols["vt_s"][row].tolist() == vt_s.tolist()
    assert cols["vt_e"][row].tolist() == vt_e.tolist()
    # the physical addresses are what `edge_idents_at` is called with
    eids, rels = a.edge_idents_at(cols["seg_id"][row], cols["seg_row"][row])
    assert len(eids) == len(row)
    assert set(rels) == {"R"}
    store.close()


def test_the_shim_refuses_a_direction_it_cannot_reconstruct(tmp_path, monkeypatch):
    from gnomon.core.errors import InvalidArgError

    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    csr, _ = a.tcsr()
    with pytest.raises(InvalidArgError):
        csr.neighbors(0, "both")
    store.close()


def test_t_max_cuts_the_slice_the_way_the_python_index_does(tmp_path, monkeypatch):
    store = _store(tmp_path)
    a = store.adapter
    py = _out(a, "n0", t_max=20)
    _rust(monkeypatch, a)
    assert _out(a, "n0", t_max=20) == py
    assert all(t < 20 for t in py[1]), "t_max is `vt_s < t_max`, half-open"
    assert _out(a, "n0", t_max=0) == [[], [], []]
    store.close()


def test_inside_a_batch_the_flag_falls_back_to_the_live_scan(tmp_path, monkeypatch):
    """A batch's rows are in no segment until it commits, so a per-segment
    index cannot be the store's answer mid-batch. The flag is therefore
    ignored inside a batch rather than honoured with an index built from a
    different set of rows than the Python path would use — which is also why
    the base `tcsr()` refuses to *persist* anything mid-batch."""
    from gnomon.storage.tcsr import TemporalCSR

    store = _store(tmp_path)
    a = store.adapter
    _rust(monkeypatch, a)
    a.begin()
    # a batch's tt must be after the last generation's, which `ingest_events`
    # took from the wall clock
    tt = a._store.stats()["frontier_tt"] + 1 if "frontier_tt" in a._store.stats() \
        else 2_000_000_000_000_000
    a.apply_ops([{"op": "assert_edge", "src": "n0", "dst": "n5", "rel_type": "R",
                  "props": {}, "vt_s": 900, "vt_e": 901}], tt)
    a._tcsr = None
    csr, cols = a.tcsr()
    assert isinstance(csr, TemporalCSR), "a batch takes the Python build"
    assert isinstance(cols, dict), "and its plain columnar dict, not the shim"
    assert not _tci_files(a), "and it persists nothing mid-batch"
    a.commit()

    # after the commit the flag applies again, and the new row is there
    a._tcsr = None
    assert 900 in _out(a, "n0")[1]
    store.close()


# --- 7. The default, and the opt-out that survives it ---------------- #


def test_the_flag_unset_path_is_the_rust_index_and_writes_no_npz(tmp_path,
                                                                 monkeypatch):
    """Unset means **rust**, and the NumPy permutation is not written.

    This is the test that used to pin the opposite (the shadow period,
    `…_writes_no_index_and_keeps_the_npz`). It is kept as the same on-disk
    assertion with the two sides swapped, so the default lives in exactly one
    executable place and a silent flip back fails here rather than only in a
    latency table: `idx/tcsr/*.tci` appears, `index/tcsr.npz` does not.
    """
    monkeypatch.delenv("GNOMON_TCSR", raising=False)
    from gnomon.storage.native.adapter import TCSR_DEFAULT_BACKEND, tcsr_backend

    assert TCSR_DEFAULT_BACKEND == "rust"
    assert tcsr_backend() == "rust", "an unset GNOMON_TCSR is the Rust index"
    store = _store(tmp_path)
    a = store.adapter

    csr, cols = a.tcsr()
    assert isinstance(csr, NativeTCSR)
    assert set(a.TCSR_COLS) <= set(cols)
    assert _out(a, "n0")[0], "fixture is real: n0 has out-neighbours"
    assert _tci_files(a), "the default path persists per-segment indexes"
    assert not (a.path / "index" / "tcsr.npz").exists(), (
        "the default path must not write the whole-store NumPy permutation")
    store.close()


def test_the_python_opt_out_still_writes_the_npz_and_no_tci(tmp_path,
                                                            monkeypatch):
    """The other half of the same pin: `GNOMON_TCSR=python` is a real opt-out,
    not an alias for the default. It gives the NumPy whole-store permutation
    under `index/` and touches nothing under `idx/tcsr/` — the behaviour the
    portable backends get unconditionally, and the one `verify --full`'s tcsr
    layer and the `tcsr_file` corruption class are written against."""
    monkeypatch.setenv("GNOMON_TCSR", "python")
    from gnomon.storage.tcsr import TemporalCSR

    store = _store(tmp_path)
    a = store.adapter
    csr, cols = a.tcsr()
    assert isinstance(csr, TemporalCSR)
    assert set(a.TCSR_COLS) <= set(cols)
    assert (a.path / "index" / "tcsr.npz").exists()
    assert not _tci_files(a), "the opt-out must not build per-segment indexes"
    store.close()


def test_an_unrecognised_flag_value_falls_back_to_the_default(monkeypatch):
    """A measurement knob must never make a read fail, and it must never
    silently select the *other* side either: anything that is not one of
    `TCSR_BACKENDS` resolves to the default, which is now rust."""
    from gnomon.storage.native.adapter import TCSR_BACKENDS, tcsr_backend

    for value in ("", "  ", "numpy", "Rust ", "PYTHON", "1", "yes"):
        monkeypatch.setenv("GNOMON_TCSR", value)
        got = tcsr_backend()
        assert got in TCSR_BACKENDS
        assert got == (value.strip().lower()
                       if value.strip().lower() in TCSR_BACKENDS else "rust")


def test_portable_backends_never_see_any_of_this(tmp_path, monkeypatch):
    import gnomon

    monkeypatch.setenv("GNOMON_TCSR", "rust")
    store = gnomon.open(tmp_path, backend="duckdb")
    store.ingest_events(_events(30))
    from gnomon.storage.tcsr import TemporalCSR

    csr, _ = store.adapter.tcsr()
    assert isinstance(csr, TemporalCSR), (
        "GNOMON_TCSR is a native-adapter knob; `gnomon/storage/tcsr.py` remains "
        "the portable backends' implementation and is never overridden"
    )
    assert not (Path(tmp_path) / "native").exists()
    store.close()
