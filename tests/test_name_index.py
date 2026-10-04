"""The on-disk sorted name index — its own gate.

`resolve_entities` (`crates/gnomon-core/src/read.rs`) answers the
shipped query by opening every node segment and visiting every row: correct,
because the shipped query is substring on both uid and name, and no sorted
index accelerates `contains`. `lookup_name` is the new API next to
it: exact and prefix match through a per-segment sorted `(casefolded name,
row)` array, built lazily and persisted at `idx/name/<seg_id>.tnx`, stamped
by the segment's own `body_sha` so it is never authoritative and always
rebuildable. `resolve_entities` itself is byte-for-byte
unchanged — it is not exercised through the index at all.

**The two claims this file has to hold up, together.** (1) The index does
not change what a query already answered: `resolve_entities` gives the same
result whether or not an index happens to exist on disk, and `lookup_name`'s
*canonical state* — phase two of the two-phase resolve — agrees with
what the old single-sweep `upsert` closure would compute for the same uid,
on a fixture that actually exercises corrections (so there is more than one
candidate version to pick the latest of) and casefold-colliding names (so
"is this the same key" is answered by the pinned casefold, not by identity).
(2) The mechanism is the point: `lookup_name`'s cost does not grow with
store size the way `resolve_entities`'s does. Both are gated here, not just
asserted in a docstring — the same discipline
`tests/test_compacted_layout_scaling.py` sets, whose four anti-flakiness
mechanisms this file copies verbatim (the gate template): (i) one untimed
warm-up then `statistics.median` over several reps; (ii) a host-independent
ratio as the primary signal plus an absolute bound as a belt; (iii) a
fixture-is-real assertion before the timed section; (iv)
`pytest.importorskip("gnomon.storage.native", ...)`, `adapter.paranoid =
False`, a fresh store per configuration.

**Threshold provenance (measured on this machine, 2026-09-19, this file's
own fixture and statistic, native engine built from this worktree):**

    n_rows   lookup_name (exact)   resolve_entities (substr)   speedup
      5,000         2.4-2.8 us            1.29-1.30 ms           ~500x
     50,000         2.0-2.3 us           13.5-13.7 ms          ~6,000x

`lookup_name`'s own cost did not move outside noise across a 10x store-size
increase (ratio 0.7x-1.2x across repeated runs — occasionally *faster* at
the larger size, which is within measurement noise for a call this cheap);
`resolve_entities`'s cost scaled ~10x, exactly the O(rows) sweep the design
doc predicts and exactly the contrast
that makes "O(log n)" a falsifiable claim rather than an assertion. `MAX_
INDEX_RATIO = 3.0` sits well above the ~1x observed (room for a loaded host)
and far below what an O(rows) mechanism would show at this size ratio (10x);
`MAX_INDEX_ABS_US = 50.0` sits ~20x above the ~2-3us observed;
`MIN_SWEEP_RATIO = 3.0` is the belt-and-braces check that the *fixture*
still produces the contrast this file exists to measure — a fixture that
stopped growing the sweep's cost would make every ratio here vacuously pass.
"""

from __future__ import annotations

import json
import statistics
import time

import pytest


def _native_adapter(tmp):
    """The native engine, regardless of `GNOMON_TEST_BACKEND` — the sorted
    name index is a native-engine mechanism with no portable-backend
    equivalent. `lookup_name` exists only on `NativeAdapter`; a system
    without it is `unsupported` in the bench harness (see the tests at the
    bottom of this file), not given a fallback here."""
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="the sorted name index gate measures the native lookup_name path",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


def _node_op(uid: str, name: str | None, vt_s: int = 0, vt_e: int = 100,
            label: str = "Person") -> dict:
    props = {} if name is None else {"name": name}
    return {"op": "assert_node", "uid": uid, "label": label, "props": props,
            "vt_s": vt_s, "vt_e": vt_e}


def _correct_name_op(uid: str, name: str, vt_s: int = 0, vt_e: int = 100) -> dict:
    return {"op": "correct", "ref": {"kind": "node", "uid": uid},
            "props": {"name": name}, "vt_s": vt_s, "vt_e": vt_e}


def _props_name(props_json: str) -> str | None:
    return json.loads(props_json).get("name")


# --- 1. correctness: index presence must never change resolve_entities --- #


def test_resolve_entities_is_unchanged_by_the_index_existing_on_disk(tmp_path):
    """`resolve_entities` is a new-API-alongside, not a
    new-API-instead-of. Calling `lookup_name` first (which builds and
    persists the index) must not perturb what `resolve_entities` answers."""
    a = _native_adapter(tmp_path / "s")
    a.begin()
    a.apply_ops([_node_op("n1", "Alice"), _node_op("n2", "Bob")], 1000)
    a.commit()

    before = a.resolve_entities("alice")
    a.lookup_name("alice", "exact")  # builds + persists idx/name/...
    after = a.resolve_entities("alice")
    assert before == after, "resolve_entities moved after the index was built"


def test_fixture_actually_persists_an_index_file(tmp_path):
    """Fixture-is-real, mechanism 1: an index file must actually land on
    disk, or every gate below would be exercising nothing."""
    root = tmp_path / "s"
    a = _native_adapter(root)
    a.begin()
    a.apply_ops([_node_op("n1", "Alice")], 1000)
    a.commit()
    built = a.build_name_indexes()
    assert built == 1, f"expected exactly one node segment to build, got {built}"
    idx_dir = root / "idx" / "name"
    assert idx_dir.is_dir(), "no idx/name/ directory after build_name_indexes()"
    files = list(idx_dir.glob("*.tnx"))
    assert len(files) == 1, f"expected one .tnx file, found {files}"
    assert a.build_name_indexes() == 0, "an already-fresh index must not rebuild"


# --- 2. exact / prefix correctness, including casefold collisions -------- #


def test_exact_match_finds_every_casefold_colliding_uid(tmp_path):
    a = _native_adapter(tmp_path / "s")
    a.begin()
    a.apply_ops([
        _node_op("n1", "Xanadu"),
        _node_op("n2", "XANADU"),  # casefolds identically to n1's name
        _node_op("n3", "Bob"),
        _node_op("n4", None),      # no name at all — must never match
    ], 1000)
    a.commit()

    got = {(uid, score, label, _props_name(props))
          for uid, score, label, props in a.lookup_name("xanadu", "exact")}
    assert got == {("n1", 2, "Person", "Xanadu"), ("n2", 2, "Person", "XANADU")}

    # and the exact-uid O(1) path (score 0, distinct from the name path)
    got_uid = a.lookup_name("n3", "exact")
    assert [(u, s, la) for u, s, la, _ in got_uid] == [("n3", 0, "Person")]


def test_prefix_match_is_contiguous_and_excludes_non_matching_names(tmp_path):
    a = _native_adapter(tmp_path / "s")
    a.begin()
    a.apply_ops([
        _node_op("n1", "ann"),
        _node_op("n2", "Anna"),
        _node_op("n3", "Annabel"),
        _node_op("n4", "Anne"),
        _node_op("n5", "Bob"),
        _node_op("n6", "an"),  # too short to match the 3-char prefix below
    ], 1000)
    a.commit()

    got = {uid for uid, *_ in a.lookup_name("ann", "prefix")}
    assert got == {"n1", "n2", "n3", "n4"}


def test_a_query_matching_nothing_returns_an_empty_list_not_an_error(tmp_path):
    a = _native_adapter(tmp_path / "s")
    a.begin()
    a.apply_ops([_node_op("n1", "Alice")], 1000)
    a.commit()
    assert a.lookup_name("nobody", "exact") == []
    assert a.lookup_name("zzz", "prefix") == []


# --- 3. the two-phase resolve against the old sweep, with corrections ---- #
#        and casefold-colliding names (the required differential test)     #


def test_two_phase_resolve_matches_the_sweep_through_corrections(tmp_path):
    """Phase two (canonical state via `believed_node_versions`,
    taking the latest by `(vt_s, vid)`) must agree with what the old
    single-sweep `upsert` closure computes — on a uid that has actually been
    corrected more than once, so there is a real "latest of several" choice
    to get right, and on casefold-colliding names, so "same key" is answered
    by the pinned casefold rather than by accident.
    """
    a = _native_adapter(tmp_path / "s")
    a.begin()
    a.apply_ops([
        _node_op("n1", "First"),
        _node_op("n2", "Xanadu"),
        _node_op("n3", "XANADU"),
        _node_op("n4", "Bob"),
    ], 1000)
    a.commit()
    a.begin()
    a.apply_ops([_correct_name_op("n1", "Middle")], 2000)
    a.commit()
    a.begin()
    a.apply_ops([_correct_name_op("n1", "Latest")], 3000)
    a.commit()

    # (a) exact-uid path: same canonical state as the sweep's, after two
    # corrections landed — this is where a wrong tie-break would show up.
    swept = a.resolve_entities("n1")
    indexed = a.lookup_name("n1", "exact")
    assert swept == indexed
    assert _props_name(swept[0][3]) == "Latest"

    # (b) name path, casefold-colliding: same *set* of matches, same
    # canonical state per uid, regardless of which mechanism found it.
    swept_xanadu = {(uid, score, label, _props_name(props))
                   for uid, score, label, props in a.resolve_entities("xanadu")}
    indexed_xanadu = {(uid, score, label, _props_name(props))
                      for uid, score, label, props in a.lookup_name("xanadu", "exact")}
    assert swept_xanadu == indexed_xanadu == {
        ("n2", 2, "Person", "Xanadu"), ("n3", 2, "Person", "XANADU"),
    }

    # (c) every uid's canonical state agrees between the two mechanisms,
    # queried by its own uid (score 0 either way) — the general form of (a).
    for uid in ("n1", "n2", "n3", "n4"):
        assert a.resolve_entities(uid) == a.lookup_name(uid, "exact"), (
            f"canonical state for {uid} diverged between resolve_entities "
            f"and lookup_name"
        )


def test_historical_as_of_sees_the_name_believed_then(tmp_path):
    """The payoff for indexing every stored row, not just current-
    canonical ones: a name superseded by a later correction is still
    findable — and resolves to the state that *was* canonical then, not to
    today's — at a belief time before the correction."""
    a = _native_adapter(tmp_path / "s")
    a.begin()
    a.apply_ops([_node_op("n1", "First")], 1000)
    a.commit()
    a.begin()
    a.apply_ops([_correct_name_op("n1", "Latest")], 3000)
    a.commit()

    # currently: "First" is superseded and must not match at OPEN_END
    assert a.lookup_name("first", "exact") == []
    current = a.lookup_name("n1", "exact")
    assert _props_name(current[0][3]) == "Latest"

    # at a belief time between the two batches: "First" is what was believed
    as_of_between = 2000
    historical = a.lookup_name("first", "exact", as_of_tt=as_of_between)
    assert len(historical) == 1
    assert historical[0][0] == "n1"
    assert _props_name(historical[0][3]) == "First"
    # and the sweep must agree — this is the same belief predicate either way
    assert a.resolve_entities("first", as_of_tt=as_of_between) == historical


# --- 4. rebuild rule: missing / stale / corrupt never surfaces a wrong --- #
#        answer                                                             #


def test_a_deleted_index_file_does_not_corrupt_the_live_answer(tmp_path):
    """`load_or_build`'s process-local cache (`nameidx.rs`) means a live
    process that already built the index in memory keeps answering
    correctly even if the on-disk copy is deleted out from under it — it
    just does not have to touch disk again this session. The rebuild-and-
    repersist half of the index rule, for a *cold* process, is
    `test_a_missing_index_file_is_rebuilt_and_repersisted_by_a_fresh_process`
    below (a fresh process is required because the cache is a Rust process
    static, so a second `NativeAdapter` in the *same* interpreter would
    still hit it)."""
    root = tmp_path / "s"
    a = _native_adapter(root)
    a.begin()
    a.apply_ops([_node_op("n1", "Alice")], 1000)
    a.commit()
    before = a.lookup_name("alice", "exact")

    idx_dir = root / "idx" / "name"
    for f in idx_dir.glob("*.tnx"):
        f.unlink()
    assert list(idx_dir.glob("*.tnx")) == []

    after = a.lookup_name("alice", "exact")
    assert after == before, "a missing index file must never surface a wrong answer"


def test_a_corrupted_index_file_does_not_corrupt_the_live_answer(tmp_path):
    """See the docstring above: this is the live-process half of the index
    rule for a corrupted file. The cold-process half is the same
    `_lookup_name_in_a_fresh_process` helper used below, exercised against
    a corrupted rather than a missing file."""
    root = tmp_path / "s"
    a = _native_adapter(root)
    a.begin()
    a.apply_ops([_node_op("n1", "Alice")], 1000)
    a.commit()
    before = a.lookup_name("alice", "exact")

    idx_dir = root / "idx" / "name"
    [f] = list(idx_dir.glob("*.tnx"))
    data = bytearray(f.read_bytes())
    data[len(data) // 2] ^= 0xFF  # flip a byte deep inside the file
    f.write_bytes(bytes(data))

    after = a.lookup_name("alice", "exact")
    assert after == before, "a corrupted index file must never surface a wrong answer"


def _lookup_name_in_a_fresh_process(store_root, query: str, mode: str) -> list:
    """Run `lookup_name` in a brand-new Python process against an
    already-built store, so `nameidx.rs`'s process-global cache starts
    empty and a missing/corrupt/stale index file is genuinely read (or not
    found) from disk rather than served from a warm in-process cache — the
    scenario the rebuild rule is actually about."""
    import json as _json
    import subprocess
    import sys
    from pathlib import Path as _Path

    worktree_root = _Path(__file__).resolve().parents[1]
    script = (
        "import sys, json; "
        f"sys.path.insert(0, {str(worktree_root)!r}); "
        "from gnomon.storage.native import NativeAdapter; "
        f"a = NativeAdapter({str(store_root)!r}); "
        f"print(json.dumps(a.lookup_name({query!r}, {mode!r})))"
    )
    out = subprocess.run([sys.executable, "-c", script], capture_output=True,
                         text=True, check=True, cwd=str(worktree_root))
    return _json.loads(out.stdout)


def test_a_missing_index_file_is_rebuilt_and_repersisted_by_a_fresh_process(tmp_path):
    root = tmp_path / "s"
    a = _native_adapter(root)
    a.begin()
    a.apply_ops([_node_op("n1", "Alice")], 1000)
    a.commit()
    before = a.lookup_name("alice", "exact")
    a.close()

    idx_dir = root / "idx" / "name"
    for f in idx_dir.glob("*.tnx"):
        f.unlink()
    assert list(idx_dir.glob("*.tnx")) == []

    after = _lookup_name_in_a_fresh_process(root, "alice", "exact")
    assert [tuple(row) for row in after] == before, (
        "a fresh process must rebuild a missing index and answer correctly"
    )
    assert list(idx_dir.glob("*.tnx")), (
        "the fresh process's rebuild must be persisted back to disk"
    )


def test_a_corrupted_index_file_is_rebuilt_and_repersisted_by_a_fresh_process(tmp_path):
    root = tmp_path / "s"
    a = _native_adapter(root)
    a.begin()
    a.apply_ops([_node_op("n1", "Alice")], 1000)
    a.commit()
    before = a.lookup_name("alice", "exact")
    a.close()

    idx_dir = root / "idx" / "name"
    [f] = list(idx_dir.glob("*.tnx"))
    original_size = f.stat().st_size
    data = bytearray(f.read_bytes())
    data[len(data) // 2] ^= 0xFF
    f.write_bytes(bytes(data))

    after = _lookup_name_in_a_fresh_process(root, "alice", "exact")
    assert [tuple(row) for row in after] == before, (
        "a fresh process must rebuild a corrupted index and answer correctly, "
        "never trust the corrupted bytes"
    )
    # repersisted (not just answered from memory): the file is whole again,
    # not still carrying the flipped byte at the same size
    assert f.stat().st_size == original_size
    assert bytes(f.read_bytes()) != bytes(data), "the corrupted bytes must not survive a rebuild"


def test_a_stale_index_is_rebuilt_after_compaction_reshapes_segments(tmp_path):
    """Compaction writes fresh segment ids with fresh `body_sha`s
    (`ENGINE_IMPLEMENTATION_SPEC.md:196`; segment ids never reused,
    `manifest.rs:560-563`), so a persisted index keyed by the old id is
    simply orphaned, never served against the new segment — this checks
    the answer survives compaction, which is the everyday version of the
    stamp-mismatch rule."""
    root = tmp_path / "s"
    a = _native_adapter(root)
    a.begin()
    a.apply_ops([_node_op("n1", "Alice"), _node_op("n2", "Bob")], 1000)
    a.commit()
    before = a.lookup_name("alice", "exact")
    a.compact()
    after = a.lookup_name("alice", "exact")
    assert after == before, "compaction must not change lookup_name's answer"


# --- 5. the performance gate: O(log n), not O(rows) ----------------------- #
#
# See the module docstring for the measured numbers and threshold
# provenance. `MAX_INDEX_RATIO` gates the mechanism this file exists to
# prove; `MIN_SWEEP_RATIO` is the fixture-is-real check that the comparison
# is not vacuous.

MAX_INDEX_RATIO = 3.0
MAX_INDEX_ABS_US = 50.0
MIN_SWEEP_RATIO = 3.0

N_SMALL = 5_000
N_BIG = 50_000


def _build_single_named(tmp, n_rows: int):
    """`n_rows` node versions in one batch, exactly one of them named — the
    name index has one entry regardless of `n_rows`, so any cost growth in
    `lookup_name` comes from store size, not from the answer being bigger."""
    a = _native_adapter(tmp)
    a.begin()
    ops = [_node_op(f"n{i}", "Target" if i == n_rows // 2 else None)
          for i in range(n_rows)]
    a.apply_ops(ops, 1000)
    a.commit()
    return a


def _median_seconds(fn, reps: int) -> float:
    fn()  # warm-up: builds and caches the index, not timed
    times = [None] * reps
    for i in range(reps):
        t0 = time.perf_counter()
        fn()
        times[i] = time.perf_counter() - t0
    return statistics.median(times)


def test_lookup_name_cost_does_not_grow_with_store_size(tmp_path):
    small = _build_single_named(tmp_path / "small", N_SMALL)
    small_report = small.stats()
    assert small_report["n_node_versions"] == N_SMALL

    small_idx = _median_seconds(lambda: small.lookup_name("target", "exact"), reps=7)
    small_sweep = _median_seconds(lambda: small.resolve_entities("target"), reps=5)

    big = _build_single_named(tmp_path / "big", N_BIG)
    big_idx = _median_seconds(lambda: big.lookup_name("target", "exact"), reps=7)
    big_sweep = _median_seconds(lambda: big.resolve_entities("target"), reps=5)

    idx_ratio = big_idx / small_idx
    sweep_ratio = big_sweep / small_sweep

    assert sweep_ratio > MIN_SWEEP_RATIO, (
        f"the fixture did not exercise the O(rows) sweep cost: resolve_entities "
        f"grew only {sweep_ratio:.2f}x from {N_SMALL} to {N_BIG} rows "
        f"({small_sweep * 1e3:.4f}ms -> {big_sweep * 1e3:.4f}ms) — a ratio this "
        f"far under the {N_BIG // N_SMALL}x row-count increase means this test "
        f"would pass vacuously regardless of whether lookup_name's own cost is flat"
    )
    assert idx_ratio < MAX_INDEX_RATIO, (
        f"lookup_name's cost grew {idx_ratio:.2f}x from a {N_SMALL}-row store to "
        f"a {N_BIG}-row one (limit {MAX_INDEX_RATIO}x): "
        f"{small_idx * 1e6:.3f}us -> {big_idx * 1e6:.3f}us, while the resolve_"
        f"entities sweep over the same stores grew {sweep_ratio:.2f}x "
        f"({small_sweep * 1e3:.4f}ms -> {big_sweep * 1e3:.4f}ms) — the sorted "
        f"index is supposed to be the mechanism that does *not* do this"
    )
    assert big_idx * 1e6 < MAX_INDEX_ABS_US, (
        f"lookup_name on the {N_BIG}-row store cost {big_idx * 1e6:.3f}us, over "
        f"the {MAX_INDEX_ABS_US}us bound measured against this file's own "
        f"fixture (module docstring has the provenance)"
    )


# --- 6. the bench-harness dispatch: a bypass, not a plan-layer operator --- #
#
# `lookup_name` is deliberately unregistered in the plan-layer operator registry
# (it is an engine capability, not part of the operator algebra). `resolve.exact` /
# `resolve.prefix` still have to be *runnable* in `scripts/eval_harness.py`,
# so `run_system` special-cases `q.op == "lookup_name"` to call
# `adapter.lookup_name` directly instead of `call_operator` — the same shape
# `run_postgres`/`run_clickhouse` already use for their own drivers. These
# tests exercise that dispatch end to end: a real native answer with a
# stable hash, and a clean `unsupported` verdict (never a crash) for a
# backend with no `lookup_name`.


def _load_eval_harness():
    """`scripts/eval_harness.py` does unqualified `import <sibling>` when run
    as a script, so `scripts/` has to be on `sys.path` before it loads —
    mirrors `tests/test_build_synth_store.py::_load_module`."""
    import importlib.util
    import sys as _sys
    from pathlib import Path as _Path

    root = _Path(__file__).resolve().parent.parent
    scripts_dir = str(root / "scripts")
    if scripts_dir not in _sys.path:
        _sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location("eval_harness", root / "scripts/eval_harness.py")
    mod = importlib.util.module_from_spec(spec)
    _sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


#: Only the ids this test cares about — `run_system` times every query with
#: `_measure`'s full warmup+repetition protocol, and the registry's motif/
#: aggregate queries are the expensive ones; the dispatch this test checks
#: doesn't need them exercised too.
_DISPATCH_CHECK_IDS = ("resolve.exact", "resolve.prefix", "resolve.substr", "hist.single")


def test_resolve_exact_and_prefix_run_end_to_end_through_the_native_harness_driver(tmp_path):
    eh = _load_eval_harness()
    ds = eh.build_dataset(500)
    store_path = tmp_path / "store"
    eh.load_store(store_path, "native", ds.log)
    queries = [q for q in eh.registry(ds.t0, ds.t1, ds.tt_epoch1, ds.filter_uids)
              if q.id in _DISPATCH_CHECK_IDS]
    results = {r.query: r for r in eh.run_system("native", store_path, queries)}

    for qid in ("resolve.exact", "resolve.prefix"):
        r = results[qid]
        assert r.ok, f"{qid} failed on native: {r.error}"
        assert r.hash is not None and len(r.hash) > 0
        assert r.rows == 1, f"{qid} should match exactly n1 on this fixture, got {r.rows} rows"

    # every other query is unaffected by the dispatch branch
    assert results["resolve.substr"].ok
    assert results["hist.single"].ok


def test_resolve_exact_and_prefix_report_unsupported_not_a_crash_off_native(tmp_path):
    eh = _load_eval_harness()
    ds = eh.build_dataset(500)
    store_path = tmp_path / "store"
    eh.load_store(store_path, "duckdb", ds.log)
    queries = [q for q in eh.registry(ds.t0, ds.t1, ds.tt_epoch1, ds.filter_uids)
              if q.id in _DISPATCH_CHECK_IDS]
    results = {r.query: r for r in eh.run_system("duckdb", store_path, queries)}

    for qid in ("resolve.exact", "resolve.prefix"):
        r = results[qid]
        assert not r.ok, f"{qid} unexpectedly succeeded on duckdb"
        assert r.error is not None and "unsupported" in r.error.lower(), (
            f"{qid}'s failure on duckdb should read as unsupported, got: {r.error!r}"
        )

    # duckdb answers everything else through call_operator as always,
    # including resolve_entities's own portable substring fallback
    assert results["resolve.substr"].ok
    assert results["hist.single"].ok
