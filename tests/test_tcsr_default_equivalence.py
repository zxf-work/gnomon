"""The Rust per-segment TCSR as the *default* traversal index.

A traversal comparison established that the Rust and NumPy indexes agree on
one corrected store, vertex by vertex: the equivalence that *permitted* a
default flip. This file pins the equivalence the flip *requires*, which is a
different and larger set:

1. **Every registry answer**, not just the traversal primitives. `paths.k` is
   the only registry query that consumes the index directly and `snap.hop2`
   and `reach.window` consume none of it — but "the default moved and no answer moved" is a
   claim about all of them, including the ones that must *not* change, and a
   claim worth making executable rather than inferring from which files were
   edited.

2. **A store compacted since its indexes were built.** Compaction writes new
   segment ids and never rewrites an old file, so the keying — the segment's
   own `body_sha` — has to make the superseded indexes simply irrelevant
   rather than stale-but-consulted.

3. **A settled store.** Settle rebuilds inline by default, through the
   same `build_indexes` call a lazy first traversal would reach, so the first
   query after a settle must *load* its indexes and build nothing. If that
   ever inverts, the first query pays a full-file sha256 plus a build per
   segment — the exact cost settle exists to remove, and invisible to any
   correctness test.

4. **A store recovered from a truncated log** (suffix recovery). The
   recovered generation's segments are new files; the existing ones are
   untouched. The adversarial half is the id collision: two stores replaying
   different logs mint the same segment ids, so an index file *can* end up
   next to a segment it was not computed from with a matching id in its own
   header. Only the `body_sha` stamp separates them, and this asserts it does.

Equivalence here means the answers, not the plans: `row` values are positions
in whatever accumulator answered and the two backends number them differently
by construction (see `_ShimCols`), so the comparison is over the payload a
registry query returns, hashed exactly as `scripts/eval_harness.py` hashes it
— volatile freshness metadata excluded, everything else in.

Native engine only, whatever `GNOMON_TEST_BACKEND` says: `GNOMON_TCSR` is a
native-adapter knob and a DuckDB run would compare the NumPy index against
itself.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

import gnomon  # noqa: E402
from gnomon.storage.native import NativeAdapter  # noqa: E402

REPO = Path(__file__).resolve().parents[1]

#: Commits in the fixtures. Enough that compaction and settle both have
#: several live segments to merge, and that a suffix replay lands in a
#: generation of its own.
BATCHES = 10
ROWS_PER_BATCH = 12
RING = 9


def _load_eval_harness():
    """`scripts/eval_harness.py` does unqualified `import <sibling>` when run
    as a script, so `scripts/` has to be on `sys.path` before it loads —
    same loader `tests/test_name_index.py` uses, for the same reason."""
    scripts_dir = str(REPO / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(
        "eval_harness", REPO / "scripts" / "eval_harness.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _events(lo: int, n: int) -> list[dict]:
    return [
        {"src": f"n{i % RING}", "dst": f"n{(i + 1) % RING}", "rel_type": "R",
         "vt_s": i, "vt_e": i + 6, "props": {"w": i}}
        for i in range(lo, lo + n)
    ]


def _build(root: Path, batches: int = BATCHES):
    """A tiered store: the engine's own cluster trigger decides compaction,
    which is what a `--compact-every 0` campaign build does and what leaves
    the unmerged tier windows settle exists to pay off."""
    store = gnomon.open(root, backend="native")
    for b in range(batches):
        store.ingest_events(_events(b * ROWS_PER_BATCH, ROWS_PER_BATCH))
        if store.adapter.needs_compaction():
            store.adapter.compact_incremental()
    return store


def _tci(root: Path) -> dict[str, bytes]:
    d = root / "native" / "idx" / "tcsr"
    return {p.name: p.read_bytes() for p in sorted(d.glob("*.tci"))} \
        if d.exists() else {}


def _npz(root: Path) -> Path:
    return root / "native" / "index" / "tcsr.npz"


# --------------------------------------------------------------------- #
# the instrument: one store, two arms, the whole registry                #
# --------------------------------------------------------------------- #


def _registry_answers(root: Path, backend: str,
                      monkeypatch) -> dict[str, tuple[bool, str | None]]:
    """`{query id: (ok, answer hash)}` for the whole registry on `root`,
    answered with `GNOMON_TCSR=<backend>`.

    Goes through `eval_harness.run_system` rather than re-deriving the
    registry here, so the queries, the dispatch (including `lookup_name`'s
    harness-level bypass) and the hash are literally the campaign's — a
    disagreement found here is a disagreement the campaign would report, and
    a query the campaign cannot answer is reported here as unanswerable
    rather than silently skipped.
    """
    H = _load_eval_harness()
    monkeypatch.setenv("GNOMON_TCSR", backend)
    data = H.dataset_from_log(root / "eventlog.jsonl")
    queries = H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    results = H.run_system("native", root, queries)
    return {r.query: (r.ok, r.hash) for r in results}


def _slices(adapter, direction: str = "out") -> list[list]:
    """Every vertex's neighbour slice, values only.

    `row` is deliberately absent: its values are positions in whichever
    accumulator answered, and the two backends number them differently by
    construction. Everything a caller can *observe* about a neighbour — the
    endpoint and its valid-time bounds — is here.
    """
    import numpy as np

    csr, _cols = adapter.tcsr()
    out = []
    for u in range(adapter.num_entities()):
        nbr, vt_s, vt_e, _row = csr.neighbors(u, direction)
        out.append([np.asarray(a).tolist() for a in (nbr, vt_s, vt_e)])
    return out


def _slices_both_arms(root: Path, monkeypatch) -> tuple[list, list]:
    """The same store read twice, one fresh handle per arm."""
    got = []
    for backend in ("python", "rust"):
        monkeypatch.setenv("GNOMON_TCSR", backend)
        a = NativeAdapter(root / "native")
        try:
            got.append(_slices(a))
        finally:
            a.close()
    return got[0], got[1]


# --------------------------------------------------------------------- #
# 1. every registry answer                                              #
# --------------------------------------------------------------------- #


def test_every_registry_answer_is_identical_under_both_backends(
        tmp_path, monkeypatch):
    """The whole registry, one store, two flag values, answer hashes compared.

    The campaign scores this at 1M and 10M on the measurement host; this runs
    the same instrument at unit scale so a divergence is caught by the suite
    rather than by a sweep. It also asserts the fixture is real in the one way
    that matters: `paths.k` must actually answer, and the Rust arm must
    actually have built per-segment indexes — an A/B between two identical
    Python paths would agree perfectly and prove nothing.
    """
    root = tmp_path / "s"
    store = _build(root)
    store.close()

    python = _registry_answers(root, "python", monkeypatch)
    assert python["paths.k"][0], f"fixture is not real: {python['paths.k']}"
    assert python["paths.k"][1], "paths.k produced no answer hash"

    rust = _registry_answers(root, "rust", monkeypatch)
    assert _tci(root), (
        "the rust arm built no per-segment index — the two arms just ran the "
        "same code and this comparison is vacuous")

    assert set(python) == set(rust)
    disagreements = {q: (python[q], rust[q]) for q in python
                     if python[q] != rust[q]}
    assert not disagreements, (
        f"the default flip moved {len(disagreements)} registry answer(s): "
        f"{disagreements}")
    assert len(python) >= 18, (
        f"the registry shrank to {len(python)} queries; this test is supposed "
        f"to cover all of them")


def test_the_two_arms_agree_on_every_vertex_slice_in_both_directions(
        tmp_path, monkeypatch):
    """Below the operators: the index's own surface, every vertex, both
    directions. `paths.k` reaches only the vertices its DFS happens to visit,
    so a slice the registry never asks for could disagree unnoticed."""
    root = tmp_path / "s"
    store = _build(root)
    store.close()

    for direction in ("out", "in"):
        got = []
        for backend in ("python", "rust"):
            monkeypatch.setenv("GNOMON_TCSR", backend)
            a = NativeAdapter(root / "native")
            try:
                got.append(_slices(a, direction))
            finally:
                a.close()
        assert any(s[0] for s in got[0]), f"no {direction} edges in the fixture"
        assert got[0] == got[1], f"the {direction} slices disagree"


# --------------------------------------------------------------------- #
# 2. a store compacted since its indexes were built                     #
# --------------------------------------------------------------------- #


def test_a_compaction_since_the_index_was_built_invalidates_by_body_sha(
        tmp_path, monkeypatch):
    """Compaction writes new segment ids and rewrites no existing file, so
    the indexes it supersedes are not stale — they are for segments that no
    longer exist. The new segments must therefore be *built* (never loaded
    off a file that happens to sit at the same path), and the answers must not
    move."""
    root = tmp_path / "s"
    store = _build(root)
    monkeypatch.setenv("GNOMON_TCSR", "rust")
    store.adapter.build_indexes()
    before_files = _tci(root)
    assert before_files, "the fixture must have indexes to supersede"
    before = _slices(store.adapter)

    store.adapter.compact()          # build_indexes=False: the default
    store.close()

    fresh = NativeAdapter(root / "native")
    try:
        after = _slices(fresh)
        stats = fresh.tcsr_stats()
        assert stats["built"] > 0, (
            "the compacted segments are new files and must have been built")
        assert stats["loaded"] == 0, (
            f"a compacted store must load none of the superseded indexes, "
            f"loaded={stats['loaded']}")
    finally:
        fresh.close()
    assert after == before, "compaction moved a traversal answer"

    py, rs = _slices_both_arms(root, monkeypatch)
    assert py == rs, "the two arms disagree on the compacted store"


def test_the_superseded_index_files_are_never_consulted_for_new_segments(
        tmp_path, monkeypatch):
    """The same claim aimed at the file rather than at the counters: an index
    from a segment compaction superseded, planted at a live segment's path —
    the shape a restore, or a crash between `compact` and `gc`, can leave
    behind. Two independent things reject it (the id in its own header and
    the `body_sha` it was computed from), which is the point: a store cannot
    be made to serve it by fixing one of them. The id-matching case, where
    only the sha is left to separate them, is the last test here."""
    monkeypatch.setenv("GNOMON_TCSR", "rust")
    root = tmp_path / "s"
    store = _build(root)
    store.adapter.build_indexes()
    donor_name, donor = sorted(_tci(root).items())[0]
    want = _slices(store.adapter)
    store.adapter.compact()
    store.adapter.build_indexes()
    store.close()

    # `idx/tcsr/` still holds the superseded segments' indexes — compaction
    # writes new segment ids and gc is a separate operator act — so the
    # victim has to be a *live* segment's index, chosen by the ids a
    # traversal actually consults rather than by globbing the directory.
    probe = NativeAdapter(root / "native")
    try:
        _slices(probe)
        live = [int(e["seg_id"]) for e in probe.tcsr_stats()["per_segment"]]
    finally:
        probe.close()
    idx_dir = root / "native" / "idx" / "tcsr"
    victims = [idx_dir / f"{sid:012}.tci" for sid in live]
    victims = [p for p in victims if p.exists() and p.name != donor_name]
    assert victims, (
        f"fixture is not real: no live segment index to overwrite "
        f"(live={live}, donor={donor_name})")
    victims[0].write_bytes(donor)

    fresh = NativeAdapter(root / "native")
    try:
        assert _slices(fresh) == want, (
            "an index from a superseded segment was served as if it were the "
            "new segment's")
        assert fresh.tcsr_stats()["built"] >= 1, (
            f"the planted file at {victims[0].name} must have been rejected "
            f"and rebuilt")
    finally:
        fresh.close()


# --------------------------------------------------------------------- #
# 3. a settled store                                                    #
# --------------------------------------------------------------------- #


def test_a_settled_store_loads_its_indexes_and_builds_none(tmp_path,
                                                           monkeypatch):
    """Settle's inline rebuild is `build_indexes(name=True)` — the same
    load-or-build a first traversal reaches, so the bytes it writes are the
    bytes a first traversal would have written. The observable consequence,
    and the one this asserts, is that the first traversal after a settle
    *loads*: `built == 0`, `loaded == the settled segment count`.
    """
    monkeypatch.setenv("GNOMON_TCSR", "rust")
    root = tmp_path / "s"
    store = _build(root)
    before = _slices(store.adapter)
    report = store.adapter.settle()
    store.close()

    rebuilt = report["indexes_rebuilt"]
    assert rebuilt is not None and rebuilt["tcsr"]["built"] > 0, (
        f"settle must rebuild what it orphaned: {rebuilt}")
    assert "name" in rebuilt, "settle rebuilds the name index in the same call"

    fresh = NativeAdapter(root / "native")
    try:
        after = _slices(fresh)
        stats = fresh.tcsr_stats()
        assert stats["built"] == 0, (
            f"the first traversal after a settle rebuilt {stats['built']} "
            f"index(es); settle's inline rebuild should have left nothing to "
            f"build")
        assert stats["loaded"] > 0, "it must have loaded something"
    finally:
        fresh.close()
    assert after == before, "settle moved a traversal answer"

    py, rs = _slices_both_arms(root, monkeypatch)
    assert py == rs, "the two arms disagree on the settled store"


def test_the_registry_agrees_on_a_settled_store(tmp_path, monkeypatch):
    """The operator-level form of the above. Settle is where the campaign's
    settled arm is measured, so its equivalence is not optional."""
    root = tmp_path / "s"
    store = _build(root)
    store.adapter.settle()
    store.close()

    python = _registry_answers(root, "python", monkeypatch)
    rust = _registry_answers(root, "rust", monkeypatch)
    assert python["paths.k"][0], "fixture is not real after a settle"
    assert python == rust, (
        {q: (python[q], rust[q]) for q in python if python[q] != rust[q]})


# --------------------------------------------------------------------- #
# 4. recovery from a truncated log                              #
# --------------------------------------------------------------------- #


def test_suffix_recovery_leaves_existing_indexes_alone_and_agrees(
        tmp_path, monkeypatch):
    """The real suffix-recovery shape: a batch reaches the durable log and the backend
    never sees it, so the next open replays the suffix and publishes a
    generation of its own. Recovery rewrites no existing segment, so every
    index built before it must still be byte-identical afterwards — and the
    recovered rows must be visible to the traversal under both arms."""
    from gnomon.storage.eventlog import EventLog

    monkeypatch.setenv("GNOMON_TCSR", "rust")
    root = tmp_path / "s"
    store = _build(root)
    store.adapter.build_indexes()
    before_files = _tci(root)
    assert before_files
    tt = store.clock.last_tt
    store.close()

    # the crash: appended to the log directly, never applied by the backend
    EventLog(root / "eventlog.jsonl").append(
        tt + 10, [{"op": "assert_edge", "src": "n0", "dst": "n4",
                   "rel_type": "R", "props": {"w": 9999},
                   "vt_s": 5000, "vt_e": 5010, "disc": "#recovered",
                   "source": "ingest", "provenance_ref": None}])

    recovered = gnomon.open(root, backend="native")
    try:
        edges = _slices(recovered.adapter)
    finally:
        recovered.close()
    assert any(5000 in s[1] for s in edges), (
        "fixture is not real: the replayed batch is not in the traversal")

    now = _tci(root)
    for name, raw in before_files.items():
        assert now.get(name) == raw, (
            f"{name} changed across a suffix replay — recovery adds segments, "
            f"it does not rewrite them")

    py, rs = _slices_both_arms(root, monkeypatch)
    assert py == rs, "the two arms disagree on the recovered store"


def test_an_index_for_a_re_written_segment_of_the_same_id_is_not_reused(
        tmp_path, monkeypatch):
    """The adversarial half, and the one the keying exists for.

    Segment ids come from the manifest's own counter, so two stores built
    from two different logs mint the *same* ids over different bytes. An index
    file carried across — by a restore, or by a recovery that re-mints an id
    whose file was orphaned by a crash — therefore passes an id check and a
    path check. Only the segment's `body_sha` separates them.

    This plants exactly that file and asserts the answer is right and the
    index was rebuilt. It is the same rule `test_native_tcsr.py::
    test_an_index_stamped_for_another_segment_is_never_trusted` pins for a
    *foreign* id; this is the case where the id matches.
    """
    monkeypatch.setenv("GNOMON_TCSR", "rust")
    a_root, b_root = tmp_path / "a", tmp_path / "b"
    a = _build(a_root, batches=4)
    a.adapter.build_indexes()
    want = _slices(a.adapter)
    a.close()

    # a different log over the same id space
    b = gnomon.open(b_root, backend="native")
    for k in range(4):
        b.ingest_events([
            {"src": f"n{(i + 3) % RING}", "dst": f"n{(i + 5) % RING}",
             "rel_type": "R", "vt_s": 900 + i, "vt_e": 950 + i,
             "props": {"w": -i}}
            for i in range(k * ROWS_PER_BATCH, (k + 1) * ROWS_PER_BATCH)])
    b.adapter.build_indexes()
    b.close()

    # Only the ids a traversal on A actually consults are worth planting on:
    # `_build` may compact, and a superseded segment's index is an orphan that
    # nothing would have read either way.
    probe = NativeAdapter(a_root / "native")
    try:
        _slices(probe)
        live = {f"{int(e['seg_id']):012}.tci"
                for e in probe.tcsr_stats()["per_segment"]}
    finally:
        probe.close()

    planted = 0
    for name, raw in _tci(b_root).items():
        target = a_root / "native" / "idx" / "tcsr" / name
        if name in live and target.exists():
            target.write_bytes(raw)
            planted += 1
    assert planted, (
        f"fixture is not real: the two stores minted no live segment id in "
        f"common, so nothing was planted (A live={sorted(live)}, "
        f"B={sorted(_tci(b_root))})")

    fresh = NativeAdapter(a_root / "native")
    try:
        assert _slices(fresh) == want, (
            "an index computed over another store's bytes was served")
        assert fresh.tcsr_stats()["built"] >= planted, (
            f"{planted} planted index(es), only "
            f"{fresh.tcsr_stats()['built']} rebuilt — a matching segment id "
            f"is not evidence of matching bytes")
    finally:
        fresh.close()


# --------------------------------------------------------------------- #
# 5. the opt-out keeps working                                          #
# --------------------------------------------------------------------- #


def test_the_opt_out_is_a_whole_working_path_not_a_fallback_stub(
        tmp_path, monkeypatch):
    """`GNOMON_TCSR=python` has to stay a first-class path, not a switch that
    happens to still compile: it is what every portable backend runs, and it
    is the control arm of every future A/B. On the same store it must build
    and persist its own whole-store permutation, answer the registry, and
    leave `idx/tcsr/` untouched."""
    root = tmp_path / "s"
    store = _build(root)
    store.close()

    monkeypatch.setenv("GNOMON_TCSR", "python")
    answers = _registry_answers(root, "python", monkeypatch)
    assert answers["paths.k"][0]
    assert _npz(root).exists(), "the opt-out must persist its permutation"
    assert not _tci(root), "the opt-out must not build per-segment indexes"

    # and the default, on the same store, is the other one
    monkeypatch.delenv("GNOMON_TCSR", raising=False)
    a = NativeAdapter(root / "native")
    try:
        _slices(a)
    finally:
        a.close()
    assert _tci(root), "the default must build per-segment indexes"
