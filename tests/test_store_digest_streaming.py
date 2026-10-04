"""`StorageAdapter.store_digest_streaming` — the bounded-memory digest pass
behind `scripts/build_synth_store.py`'s `--digest streaming`.

`store_digest()` (`gnomon/storage/base.py`) sorts and canonical-JSON-encodes
*every* node and edge version in one shot — a 17.4M-edge store already
showed a 25 GB RSS spike from this pass alone, and at
100M+ rows the same materialization would exceed a 93 GB host. The streaming
variant reproduces the identical byte stream via an external merge sort over
disk-spilled chunks, so this module's whole job is to prove that claim: same
digest, small `chunk_rows` forcing several spilled chunks and a real k-way
merge, still equal.
"""

from __future__ import annotations

import gnomon
from gnomon.core.model import OPEN_END, EntityRef


def _small_workload(store: "gnomon.Store") -> None:
    """A handful of node/edge asserts plus a correction and a retraction, so
    both node and edge version populations have more than one row and are
    not merely append-only (the carve arms exercise `_remainder`, giving
    rows with distinct `tt_s`/`vt_s`, which is what the digest sort key
    actually orders by)."""
    for i in range(12):
        store.assert_node(f"n{i}", "N", {"i": i}, vt_s=0, vt_e=1000)
    for i in range(30):
        u, v = f"n{i % 12}", f"n{(i + 1) % 12}"
        store.assert_edge(u, v, "R", {"w": i}, vt_s=i * 10, vt_e=i * 10 + 50)
    store.correct(EntityRef(kind="node", uid="n0"),
                  {"i": 0, "corrected": True}, vt_s=0, vt_e=1000)
    store.retract(EntityRef(kind="edge", src="n1", dst="n2", rel_type="R"), t=15)
    store.ingest_events([
        {"src": f"n{i % 12}", "dst": f"n{(i + 3) % 12}", "rel_type": "S",
         "vt_s": 2000 + i} for i in range(40)])


def test_streaming_digest_matches_full_digest(tmp_path):
    store = gnomon.open(tmp_path / "s", backend="native")
    _small_workload(store)

    full = store.adapter.store_digest()
    # chunk_rows well below the row count on both sides, so this forces
    # several spilled chunks per kind and a real k-way merge, not a
    # single-chunk degenerate case.
    streaming = store.adapter.store_digest_streaming(chunk_rows=3)
    store.close()

    assert streaming == full


def test_streaming_digest_matches_full_digest_various_chunk_sizes(tmp_path):
    """Equivalence must not depend on how the rows happen to be chunked —
    proving it at one `chunk_rows` value (e.g. exactly the row count, a
    single-chunk degenerate case) would not touch the merge path at all."""
    store = gnomon.open(tmp_path / "s", backend="native")
    _small_workload(store)
    full = store.adapter.store_digest()

    for chunk_rows in (1, 2, 7, 1_000_000):
        got = store.adapter.store_digest_streaming(chunk_rows=chunk_rows)
        assert got == full, f"mismatch at chunk_rows={chunk_rows}"
    store.close()


def test_streaming_digest_empty_store(tmp_path):
    """No node or edge versions at all — the degenerate case where both
    `_spill_sorted_rows` calls return no files and `_merge_sorted_spill` is a
    no-op, still producing the same digest as `{"edges":[],"nodes":[]}`."""
    store = gnomon.open(tmp_path / "s", backend="native")
    full = store.adapter.store_digest()
    streaming = store.adapter.store_digest_streaming(chunk_rows=10)
    store.close()
    assert streaming == full


def test_store_digest_streaming_via_store_wrapper(tmp_path):
    """`Store.digest_streaming()` (the public-facing wrapper, mirroring
    `Store.digest()`) agrees with the adapter-level methods directly."""
    store = gnomon.open(tmp_path / "s", backend="native")
    _small_workload(store)
    full = store.digest()
    streaming = store.digest_streaming(chunk_rows=5)
    store.close()
    assert streaming == full


def test_stats_streaming_matches_expected_over_multiple_segments_with_closes(tmp_path):
    """The sibling fix to the digest streaming above: `NativeStore::stats_accum`
    (`crates/gnomon-core/src/read.rs`) now opens each edge segment via
    `open_segment_uncached` instead of the session's cached `open_segment`,
    so folding stats never holds more than one segment's decoded columns at
    a time. This is the Python-visible half of that proof: `store.stats()`
    must still be exactly right over a store with several segments (each
    top-level `assert_*`/`correct`/`retract` call below is its own commit,
    hence its own segment) and a real closed version (the `correct` carves
    the original row, closing it) -- the same shape the 17.4M-edge
    finalisation spike involved."""
    store = gnomon.open(tmp_path / "s", backend="native")
    for i in range(12):
        store.assert_node(f"n{i}", "N", {"i": i}, vt_s=0, vt_e=1000)
    for i in range(5):
        store.assert_edge(f"n{i}", f"n{i + 1}", "R", {"w": i}, vt_s=i * 10, vt_e=i * 10 + 50)
    # a real closed version: this carves and closes the original n0->n1 row
    store.correct(EntityRef(kind="edge", src="n0", dst="n1", rel_type="R"),
                  {"w": 99}, vt_s=5, vt_e=40)
    store.retract(EntityRef(kind="edge", src="n1", dst="n2", rel_type="R"), t=15)

    seg_dir = store.adapter.path / "seg"
    n_segments = len(list(seg_dir.glob("*.tgs")))
    assert n_segments >= 3, f"test needs a multi-segment store, got {n_segments}"

    got = store.stats()

    # Ground truth, independent of stats_accum's segment fold: walk every
    # version directly through the materializing accessors.
    edges = store.adapter.all_edge_versions()
    nodes = store.adapter.all_node_versions()
    want_rel_counts: dict[str, int] = {}
    want_out_degree: dict[str, int] = {}
    vt_min = vt_max = None
    for e in edges:
        want_rel_counts[e.rel_type] = want_rel_counts.get(e.rel_type, 0) + 1
        want_out_degree[e.src] = want_out_degree.get(e.src, 0) + 1
        ve = e.vt_s + 1 if e.vt_e >= OPEN_END else e.vt_e
        vt_min = e.vt_s if vt_min is None else min(vt_min, e.vt_s)
        vt_max = ve if vt_max is None else max(vt_max, ve)

    assert got["n_edge_versions"] == len(edges)
    assert got["n_node_versions"] == len(nodes)
    assert got["vt_min"] == vt_min
    assert got["vt_max"] == vt_max
    assert got["rel_type_counts"] == want_rel_counts
    assert got["max_out_degree"] == max(want_out_degree.values())
    store.close()


# --------------------------------------------------------------------- #
# the paged digest producer                                             #
# --------------------------------------------------------------------- #

def test_all_versions_page_reconstructs_all_versions_with_corrections(tmp_path):
    """`NativeAdapter.all_versions_page` drawn to exhaustion, at a page size
    well below the row count, must return exactly the same rows as the
    one-shot `all_edge_versions`/`all_node_versions` -- same identity set,
    same field values -- on a store with corrections and a retraction (not
    merely an append-only one, so closed `tt_e` values are actually
    exercised). Row order is unspecified (the digest sorts), so this
    compares as sets of `(vid, tt_s)` pairs plus a full-row equality
    keyed the same way, rather than by position.
    """
    store = gnomon.open(tmp_path / "s", backend="native")
    _small_workload(store)

    for chunk_rows in (1, 2, 7, 1_000_000):
        for kind, want in (("edge", list(store.adapter.all_edge_versions())),
                          ("node", list(store.adapter.all_node_versions()))):
            got = []
            offset = 0
            total_reported = None
            pages = 0
            while True:
                cols, total = store.adapter.all_versions_page(kind, offset, chunk_rows)
                total_reported = total
                convert = (store.adapter._nodes if kind == "node"
                          else store.adapter._edges)
                page = convert(cols)
                assert len(page) <= chunk_rows
                got.extend(page)
                pages += 1
                offset += chunk_rows
                if offset >= total:
                    break
                assert pages < 10_000, "runaway pagination loop"

            assert total_reported == len(want), (kind, chunk_rows)
            key = ((lambda v: (v.vid, v.tt_s)) if kind == "node"
                  else (lambda v: (v.vid, v.tt_s)))
            assert sorted(got, key=key) == sorted(want, key=key), (kind, chunk_rows)
    store.close()


def test_all_versions_paged_matches_all_versions_with_corrections(tmp_path):
    """`NativeAdapter.all_versions_paged` (the generator `store_digest_
    streaming` now consumes) must yield, across all its chunks, exactly the
    rows `all_edge_versions`/`all_node_versions` return in one shot."""
    store = gnomon.open(tmp_path / "s", backend="native")
    _small_workload(store)

    for chunk_rows in (1, 3, 1_000_000):
        for kind, want in (("edge", list(store.adapter.all_edge_versions())),
                          ("node", list(store.adapter.all_node_versions()))):
            got = []
            for page in store.adapter.all_versions_paged(kind, chunk_rows):
                assert 0 < len(page) <= chunk_rows
                got.extend(page)
            key = lambda v: (v.vid, v.tt_s)  # noqa: E731
            assert sorted(got, key=key) == sorted(want, key=key), (kind, chunk_rows)
    store.close()


def test_paged_digest_producer_equals_unpaged_on_a_store_with_corrections(tmp_path):
    """The headline equality proof: `store_digest_streaming`
    pulls its rows through `all_versions_paged` (backed by the paged
    `all_versions_page` PyO3 method) instead of materializing
    `all_edge_versions()`/`all_node_versions()` up front, on a store whose
    version population is not merely append-only (`_small_workload` has a
    correction and a retraction, so `tt_e` is genuinely closed on some
    rows -- the case a naive reimplementation of the producer boundary
    could get wrong). `chunk_rows` values below, at, and above the row
    count all force a different pagination shape through the same
    producer path."""
    store = gnomon.open(tmp_path / "s", backend="native")
    _small_workload(store)
    full = store.adapter.store_digest()

    for chunk_rows in (1, 2, 5, 1_000_000):
        got = store.adapter.store_digest_streaming(chunk_rows=chunk_rows)
        assert got == full, f"mismatch at chunk_rows={chunk_rows}"
    store.close()
