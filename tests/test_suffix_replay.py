"""Fault injection for the event-log replay cursor (suffix recovery).

The write path is write-ahead: a batch is fsynced to the event log before the
backend applies it, so a crash in between leaves a durable record with no
store state. The native manifest now records a replay cursor (offset past the
last applied record, rolling chain over that prefix); reopening replays only
the un-applied suffix. The crash-safety rules under test:

  * a crash between log append and backend commit recovers to exactly the
    state a full replay of the same log produces;
  * a cursor the log cannot account for — log truncated below it, or the
    applied prefix rewritten — is corruption and refuses loudly, never
    guesses;
  * a missing cursor (legacy store) degrades to *not replaying* — it cannot
    know what was applied — and never to skipping batches silently on a
    cursor-bearing store;
  * failed batches re-fail identically during recovery and are skipped,
    exactly as the live path and full replay treat them.

Mirrors tests/test_native_faults.py conventions: native engine specific,
independent of GNOMON_TEST_BACKEND.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gnomon.core.errors import StateError

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

import gnomon  # noqa: E402
from gnomon.storage.eventlog import EventLog, replay  # noqa: E402
from gnomon.storage.native import NativeAdapter  # noqa: E402


def build(root: Path, batches: int = 3):
    store = gnomon.open(root, backend="native")
    for b in range(batches):
        store.assert_edge(f"n{b}", f"n{b + 1}", "R", {"w": b},
                          vt_s=b, vt_e=b + 10, disc=f"#{b}")
    return store


def log_path(root: Path) -> Path:
    return root / "eventlog.jsonl"


def replay_reference_digest(root: Path, tmp_path: Path) -> str:
    """What a full replay of the store's log produces — the recovery gold."""
    fresh = NativeAdapter(tmp_path / "replay-reference" / "native")
    replay(log_path(root), fresh)
    digest = fresh.store_digest()
    fresh.close()
    return digest


# --- the happy path: cursor tracks the log ------------------------------- #


def test_cursor_tracks_the_log_through_writes_and_reopen(tmp_path):
    root = tmp_path / "s"
    store = build(root)
    offset, chain = store.adapter.event_cursor()
    assert offset == log_path(root).stat().st_size, (
        "after a clean write the cursor sits at the end of the log"
    )
    assert chain not in ("", None)
    digest = store.digest()
    store.close()

    re = gnomon.open(root)
    assert re.adapter.event_cursor() == (offset, chain), (
        "a clean reopen replays nothing and moves nothing"
    )
    assert re.digest() == digest
    re.close()


# --- crash between log append and backend commit -------------------------- #


def test_unapplied_suffix_is_replayed_on_open(tmp_path):
    root = tmp_path / "s"
    store = build(root)
    tt = store.clock.last_tt
    store.close()

    # the crash: two batches reach the durable log, the backend never sees
    # them (appended directly, no adapter call)
    log = EventLog(log_path(root))
    for i, t in enumerate((tt + 10, tt + 20)):
        log.append(t, [{"op": "assert_node", "uid": f"crash{i}", "label": "N",
                        "props": {"p": i}, "vt_s": 0, "vt_e": 50,
                        "source": "ingest", "provenance_ref": None}])

    re = gnomon.open(root)
    assert re.adapter.event_cursor()[0] == log_path(root).stat().st_size
    for i in range(2):
        assert re.adapter.believed_node_versions(f"crash{i}"), (
            f"the un-applied batch for crash{i} must be recovered"
        )
    # recovery must equal a from-scratch replay of the same log, bit for bit
    assert re.digest() == replay_reference_digest(root, tmp_path)
    digest = re.digest()
    re.close()

    # and recovery is idempotent: nothing replays twice
    again = gnomon.open(root)
    assert again.digest() == digest
    again.close()


def test_recovered_store_accepts_new_writes(tmp_path):
    root = tmp_path / "s"
    store = build(root)
    tt = store.clock.last_tt
    store.close()
    EventLog(log_path(root)).append(
        tt + 10, [{"op": "assert_node", "uid": "crash0", "label": "N",
                   "props": {}, "vt_s": 0, "vt_e": 50,
                   "source": "ingest", "provenance_ref": None}])

    re = gnomon.open(root)
    re.assert_node("after", "N", {"q": 1}, vt_s=0, vt_e=50)
    assert re.adapter.event_cursor()[0] == log_path(root).stat().st_size
    assert re.digest() == replay_reference_digest(root, tmp_path)
    re.close()


def test_failed_batch_in_the_suffix_is_skipped_deterministically(tmp_path):
    root = tmp_path / "s"
    store = build(root)
    tt = store.clock.last_tt
    digest_before = store.digest()
    store.close()

    # a batch that fails on apply (retracting a node that does not exist)
    # sits in the log suffix, exactly as the live path would have left it
    EventLog(log_path(root)).append(
        tt + 10, [{"op": "retract", "ref": {"kind": "node", "uid": "ghost"},
                   "t": 5, "source": "ingest", "provenance_ref": None}])

    re = gnomon.open(root)
    assert re.digest() == digest_before, (
        "a failed batch must change nothing during recovery"
    )
    # The single-writer lock (`Store.__init__`'s `fcntl.flock`) now
    # refuses a second concurrent writer on the same path, so `re` is
    # closed before the next open rather than left live alongside it — the
    # thing under test (every open retries the failed record deterministically,
    # publishing nothing) does not depend on the two handles overlapping.
    re.close()
    reopened = gnomon.open(root)
    assert reopened.digest() == digest_before
    # a later successful write advances the cursor past the failed record
    reopened.assert_node("after", "N", {}, vt_s=0, vt_e=50)
    assert reopened.adapter.event_cursor()[0] == \
        log_path(root).stat().st_size
    assert reopened.digest() == replay_reference_digest(root, tmp_path)
    reopened.close()


# --- corruption must refuse loudly ---------------------------------------- #


def test_log_truncated_below_the_cursor_refuses_to_open(tmp_path):
    root = tmp_path / "s"
    build(root).close()
    p = log_path(root)
    p.write_bytes(p.read_bytes()[:-20])

    with pytest.raises(StateError, match="ahead of the event log|boundary"):
        gnomon.open(root)


def test_rewritten_applied_prefix_refuses_to_open(tmp_path):
    root = tmp_path / "s"
    build(root).close()
    p = log_path(root)
    raw = bytearray(p.read_bytes())
    # flip one byte inside the first applied record (past the header line),
    # keeping the length: only the chain can notice
    header_end = raw.index(b"\n") + 1
    i = header_end + 30
    raw[i] = raw[i] ^ 0x01
    p.write_bytes(bytes(raw))

    with pytest.raises(StateError, match="chain mismatch|not readable"):
        gnomon.open(root)


def test_a_torn_tail_past_the_cursor_recovers_instead_of_refusing(tmp_path):
    """The contract changed deliberately. This test used to assert
    that a torn final record refuses to open — injected-crash trials showed
    that turns a routine power cut mid-append into an outage. The torn
    record was never acknowledged (append fsyncs, then returns), so the
    right recovery is: truncate the tail, replay the sound suffix, stay
    writable. Damage that is NOT the tail still refuses — that case is
    pinned by tests/test_torn_wal.py alongside the three torn signatures."""
    root = tmp_path / "s"
    store = build(root)
    before = store.digest()
    store.close()
    with open(log_path(root), "ab") as f:
        f.write(b'{"batch_id": "torn", "tt": 9')

    store = gnomon.open(root)
    assert store.digest() == before, "recovery must not change applied state"
    with open(log_path(root), "rb") as f:
        assert b'"torn"' not in f.read(), "the torn bytes must be truncated"
    store.close()


# --- missing cursor: degrade to not-replaying, never to guessing ----------- #


def test_legacy_store_without_cursor_recovers_nothing(tmp_path, monkeypatch):
    """A store written before cursors existed reports chain "". Recovery
    must not replay (it cannot know what was applied — replaying would
    double-apply) and the store must keep opening exactly as before."""
    root = tmp_path / "s"
    store = build(root)
    digest = store.digest()
    store.close()

    monkeypatch.setattr(NativeAdapter, "event_cursor", lambda self: (0, ""))
    legacy = gnomon.open(root)
    assert legacy.digest() == digest, (
        "a cursorless store must not re-apply its own history"
    )
    legacy.close()


def test_first_write_upgrades_a_legacy_store(tmp_path, monkeypatch):
    """The upgrade path: a legacy store's next write starts the chain by
    hashing the whole applied prefix once, and from then on the cursor is
    live — a subsequent crash recovers by suffix."""
    root = tmp_path / "s"
    build(root).close()

    # open as legacy (no cursor), then write once
    monkeypatch.setattr(NativeAdapter, "event_cursor", lambda self: (0, ""))
    legacy = gnomon.open(root)
    monkeypatch.undo()
    legacy.assert_node("upgraded", "N", {}, vt_s=0, vt_e=50)
    tt = legacy.clock.last_tt
    offset, chain = legacy.adapter.event_cursor()
    assert offset == log_path(root).stat().st_size
    assert chain not in ("", None)
    legacy.close()

    # and the freshly started chain supports real suffix recovery
    EventLog(log_path(root)).append(
        tt + 10, [{"op": "assert_node", "uid": "crash0", "label": "N",
                   "props": {}, "vt_s": 0, "vt_e": 50,
                   "source": "ingest", "provenance_ref": None}])
    re = gnomon.open(root)
    assert re.adapter.believed_node_versions("crash0")
    assert re.digest() == replay_reference_digest(root, tmp_path)
    re.close()


# --- `gnomon replay` threads the cursor into rebuilt stores ------------------ #


def test_cli_replay_leaves_a_valid_cursor(tmp_path):
    from gnomon.cli import main

    src_root = tmp_path / "src"
    store = build(src_root)
    digest = store.digest()
    store.close()

    dst = tmp_path / "rebuilt"
    assert main(["replay", str(log_path(src_root)), "--store", str(dst),
                 "--backend", "native"]) == 0
    re = gnomon.open(dst)
    assert re.digest() == digest
    offset, chain = re.adapter.event_cursor()
    assert offset == log_path(dst).stat().st_size
    assert chain not in ("", None)
    re.close()


# --- `gnomon store verify` --------------------------------------------------- #


def test_store_verify_cli_reports_health_and_exit_codes(tmp_path, capsys):
    from gnomon.cli import main

    root = tmp_path / "s"
    build(root).close()
    assert main(["store", "verify", "--store", str(root)]) == 0
    assert "healthy" in capsys.readouterr().out

    seg = sorted((root / "native" / "seg").glob("*.tgs"))[0]
    seg.write_bytes(seg.read_bytes()[:-32])  # lose the completion marker
    assert main(["store", "verify", "--store", str(root)]) == 1
    out = capsys.readouterr().out
    assert "PROBLEMS" in out and "gnomon replay" in out


# --- segment-cache budget: residency must not change answers ------- #


def _cached_entries(store) -> int:
    return store.adapter._store.segment_cache_stats()[0]


def test_tiny_segment_cache_budget_is_answer_invariant(tmp_path, monkeypatch):
    """Residency must not change answers, under any budget.

    Residency is driven by **point reads**, not by the bulk walk. A change
    made every whole-store path
    — `all_edge_versions` among them — open its segments uncached, so the
    walk no longer touches the cache at all and a budget it never consults
    can never evict. The answer-invariance claim this test exists for is
    unchanged, and is in fact stronger now: the walk's answers cannot depend
    on a budget it does not read.
    """
    root = tmp_path / "s"
    store = build(root, batches=6)
    digest = store.digest()
    edges = sorted(v.vid for v in store.adapter.all_edge_versions())
    eids = sorted({v.eid for v in store.adapter.all_edge_versions()})
    store.close()
    assert len(eids) == 6, f"the fixture must span several segments, got {eids}"

    # 1 byte of budget: every cached open evicts everything else
    monkeypatch.setenv("GNOMON_SEGMENT_CACHE_BYTES", "1")
    capped = gnomon.open(root)
    assert sorted(v.vid for v in capped.adapter.all_edge_versions()) == edges
    assert capped.digest() == digest

    # the path that still caches: a point read holds exact (file, row) pairs
    # and will come back to the same segment, so it keeps the cache
    for eid in eids:
        assert capped.adapter.believed_edge_versions(eid), f"{eid} did not resolve"

    entries, _bytes, budget, evictions = \
        capped.adapter._store.segment_cache_stats()
    assert budget == 1
    assert evictions > 0, "a 1-byte budget must evict"
    assert entries <= 1
    capped.close()


def test_a_bulk_walk_leaves_the_segment_cache_alone(tmp_path, monkeypatch):
    """The other half of that change, as a feature rather than as a side effect.

    A walk that touches every segment exactly once must not leave their
    decoded columns resident: that is what made a 5.2 GB store's worth of
    cache part of the 100M build's 107.3 GB resident floor. A point read, which
    will come back to the same segment, still caches — and this asserts both,
    so a future change that quietly restored the cached bulk open would fail
    here rather than only showing up as memory on a benchmark node.
    """
    root = tmp_path / "s"
    store = build(root, batches=6)
    eids = sorted({v.eid for v in store.adapter.all_edge_versions()})
    store.close()

    monkeypatch.setenv("GNOMON_SEGMENT_CACHE_BYTES", "0")  # unbounded
    s = gnomon.open(root)
    # Commit-time activity may already have cached a segment or two; what is
    # under test is that the walk adds nothing *beyond* whatever that left.
    before = _cached_entries(s)
    assert before < 6, (
        f"test is only meaningful with segments left to fold: {before} "
        f"cached of 6"
    )

    s.adapter.all_edge_versions()
    s.adapter.all_node_versions()
    assert _cached_entries(s) == before, (
        "a whole-store walk must not add a segment to the session cache"
    )

    assert s.adapter.believed_edge_versions(eids[0])
    assert _cached_entries(s) > before, (
        "a selective read must still populate the session cache"
    )
    s.close()
