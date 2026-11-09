"""StorageAdapter over the native Rust engine.

This module is deliberately mechanical: it converts between the dataclasses
`gnomon.core.model` defines and the columnar dicts `gnomon._engine` speaks, and
translates error types. There is no storage logic here at all — if something
needs deciding, it belongs in `crates/gnomon-core`. The bi-temporal
semantics (interval carving, disjointness, replay determinism) live once in
`StorageAdapter` and are inherited unchanged, which is exactly why both
backends can be held to the same oracle.

Every row-moving call crosses the boundary once, as a dict of columns rather
than a list of records, so an ingest chunk of 50,000 events is one crossing.
"""

from __future__ import annotations

import errno
import json
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence, TypeVar

import numpy as np

from gnomon.core.errors import InvalidArgError, NotFoundError, StateError
from gnomon.core.model import OPEN_END, EdgeVersion, NodeVersion, canonical_json, clamp_tt
from gnomon.storage.base import StorageAdapter
from gnomon.storage.crashpoint import crash_point

try:
    from gnomon import _engine
except ImportError as e:  # pragma: no cover - packaging failure, not a code path
    raise ImportError(
        "the native backend needs the compiled gnomon._engine extension. "
        "Install a wheel (`pip install gnomon`), or build from source with a "
        "Rust toolchain (`uv sync --reinstall-package gnomon`)."
    ) from e


_T = TypeVar("_T")


def _translate(e: Exception) -> Exception:
    """Map an engine exception onto the Gnomon error taxonomy.

    The Rust layer keeps its category as the message prefix precisely so this
    can be exact rather than a guess at the prose.
    """
    msg = str(e).strip("'\"")
    if isinstance(e, KeyError) or msg.startswith("not_found:"):
        return NotFoundError(msg)
    if isinstance(e, OverflowError) or msg.startswith("capacity:"):
        return InvalidArgError(msg)
    return StateError(msg)


class NativeAdapter(StorageAdapter):
    """Bi-temporal store backed by the native engine.

    Unlike the DuckDB adapter there is no in-memory mode: a store is a
    directory, because immutable segment files plus an atomic manifest swap
    are what give snapshot isolation and crash recovery.
    """

    #: The TCSR path scans integer columns plus row addresses — never eid or
    #: rel_type, which cost a sha256 / a string per *scanned* row. Traversal
    #: fetches identities afterwards for the rows that survive it, via
    #: `edge_idents_at`. Portable backends keep the base class columns.
    TCSR_COLS = ("src_id", "dst_id", "vt_s", "vt_e", "seg_id", "seg_row")
    #: `aggregate_events_columnar` takes the extended fold's arguments
    #: (properties, a property filter, a cohort) and may decline (`None`).
    AGGREGATE_EXT = True

    #: `apply_ops` takes the batch's event-log record as `raw=`, so
    #: an `ingest_events` op can be staged from it by the engine
    #: (`stage_ingest_events_raw`). Callers holding the record (replay, the
    #: live write path) pass it only to adapters that declare this.
    accepts_raw_record = True

    #: `(record bytes, {id(op): index in ops})` while `apply_ops` runs with a
    #: record; `None` otherwise.
    _raw_batch: tuple[bytes, dict[int, int]] | None = None

    #: Wall-clock microseconds the last `commit()` call spent outside the
    #: Rust engine's own `NativeStore::commit` (pyo3 argument marshalling,
    #: GIL bookkeeping, this method's own frame) -- the pyo3-boundary slice
    #: of the residual a commit-cost diagnosis found: engine-side
    #: `total_us` is now fully accounted for by `last_commit_phases()`,
    #: but the harness's own `write_us` still wraps a call into
    #: compiled code, and that call is not free. 0 before any commit.
    _last_commit_python_wrap_us = 0

    def __init__(self, path: str | Path, read_only: bool = False) -> None:
        self.path = Path(path)
        #: A reader that never writes under the store directory: the
        #: engine refuses every writer entry point and keeps the lazily built
        #: sidecars in memory; this adapter creates no directory and never
        #: saves the `GNOMON_TCSR=python` permutation.
        self.read_only = read_only
        if not read_only:
            self.path.mkdir(parents=True, exist_ok=True)
        try:
            self._store = _engine.NativeStore(str(self.path), read_only=read_only)
        except Exception as e:
            raise _translate(e) from None
        # Reopen-on-ENOENT net counter (see `_read_op`). Per-adapter,
        # not per native handle, because a reopen replaces `self._store`
        # entirely -- a soak reading this across many periodic reopens needs
        # it to keep counting across all of them.
        self._reopen_on_enoent_total = 0
        self._small_commit_init()

    def close(self) -> None:
        if self._sc_on:
            self.publish_pending()
        self._store.close()

    # --- reopen-on-ENOENT net ----------------------------------------------- #
    #
    # Pin-at-open (`crates/gnomon-core/src/store.rs::pin_generation_
    # files`) maps every segment a reader's generation names, so a
    # cross-process reader keeps answering through a writer's `gc(keep_last=
    # ...)` unlinks. The residual left open is a reader that resolves
    # `CURRENT` microseconds before gc's unlink pass, which can still miss a
    # file pinning would otherwise have caught -- and so can a store over
    # `GNOMON_PIN_MAX_SEGMENTS`, where pinning is disabled outright. Both
    # surface as a *detected* `OSError` naming the missing file, never
    # silently wrong data -- this is the net that turns that detected error
    # into a transparent retry instead of a failed read.

    def reopen_on_enoent_total(self) -> int:
        """How many times a read operator hit the reopen-on-ENOENT net: an engine
        `OSError` with `errno == ENOENT`, healed by reopening the native
        handle against the latest `CURRENT` and retrying the whole operator
        once. 0 across a healthy run; nonzero-but-still-succeeding is the net
        doing its job, which is why soaks track it as its own counter rather
        than folding it into a general error count."""
        return self._reopen_on_enoent_total

    def _reopen(self) -> None:
        """Re-resolve `CURRENT`: open a fresh native handle at whatever
        generation is live now, and let the stale one go.

        `NativeStore.close()` is presently a Rust no-op (the mmaps and the
        gc pin release when CPython drops the object), so dropping the old
        reference here -- rather than keeping both alive -- is what keeps
        this handle from pinning two generations' files at once across a
        long-running reader's many reopens.
        """
        stale = self._store
        self._store = _engine.NativeStore(str(self.path), read_only=self.read_only)
        try:
            stale.close()
        except Exception:
            pass

    def _read_op(self, fn: Callable[[Any], _T]) -> _T:
        """Run one read-only engine call, with the reopen-on-ENOENT net: `fn(self._store)`,
        and on an `OSError` whose `errno` is exactly `ENOENT`, reopen and
        retry `fn` against the new handle exactly once before giving up.

        Every other exception -- including any other `OSError`, which is a
        real I/O problem to surface, not paper over -- translates and raises
        immediately, as every read method here already did before this net
        existed. The retry re-runs the *whole* operator against the new
        generation rather than resuming mid-flight, which is what keeps one
        answer from ever mixing rows out of two generations.
        """
        try:
            return fn(self._store)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise _translate(e) from None
            self._reopen_on_enoent_total += 1
            self._reopen()
        except Exception as e:
            raise _translate(e) from None
        try:
            return fn(self._store)
        except Exception as e:
            raise _translate(e) from None

    @staticmethod
    def build_info() -> dict[str, Any]:
        """What the loaded `_engine.so` was built as.

        No store needed — it is a property of the extension module, not of
        an open handle — so this is a `staticmethod`: `NativeAdapter.build_info()`
        works without constructing one. Every timing record (`open_phase_us`,
        `last_commit_phases`) should be paired with this: a diagnosis of the
        engine's commit cost found the untimed residual absorbing all
        of the treatment's decile growth, and a debug-assertions build (which
        runs `store::publish`'s O(segments) `debug_assert_eq!` every commit)
        was the top candidate.
        """
        return dict(_engine.build_info())

    def tcsr(self):
        """This handle's traversal index: `NativeTCSR` by default.

        The default is the per-segment Rust TCSR: one index per
        immutable segment file, keyed by that segment's own `body_sha`, built
        lazily at first traversal and persisted under `native/idx/tcsr/`. A
        commit invalidates nothing — it adds segments that have no index yet —
        and belief filtering happens at traversal time against the believed
        row ranges rather than being baked into the index.

        **`GNOMON_TCSR=python`** is the opt-out, and takes the NumPy path below:
        one whole-store index over current beliefs, its permutation saved
        under `index/` (a directory neither gc nor verify walks) stamped with
        `(generation, manifest_sha)`; a stamp match on open replaces the two
        argsorts with a file read, and any mismatch — including a store
        rebuilt in place, which reuses generation numbers with different
        content — falls back to a rebuild and re-save. That path is
        also what `StorageAdapter.tcsr` gives the portable backends, so it is
        exercised whether or not anyone sets the flag here.

        The flag is read on each build, not once per process, so one process
        can still run both configurations against one store — the A/B that moved the
        default stays runnable, and the fallback stays free.

        Inside an open batch the flag is ignored and the NumPy build runs
        whatever it says: staged rows are visible to reads before the
        generation advances, so they are in no segment yet and a per-segment
        index cannot see them. Nothing is loaded or saved mid-batch either,
        because a stamp written then would lie.
        """
        if self._tcsr is not None:
            held = self._tcsr[1]
            if not (isinstance(held, _ShimCols)
                    and held.rows_accumulated() > SHIM_ROW_BUDGET):
                return self._tcsr
            # over budget: hand the next operator a fresh accumulator. This
            # point is an operator boundary, so a caller already holding the
            # old pair keeps a consistent one until it drops it.
            self._tcsr = None
        from gnomon.storage.tcsr import TemporalCSR, load_permutation, save_permutation

        # Inside an open batch the rows staged so far are visible to reads
        # but are in no segment yet, so a per-segment index cannot see them.
        # The Python build from a live scan can, and does — so the batch is
        # the one case where the flag is ignored rather than honoured with a
        # wrong answer.
        if tcsr_backend() in ("rust", "scan") and not self._store.in_batch():
            cols = _ShimCols()
            self._tcsr = (NativeTCSR(self, cols), cols)
            return self._tcsr

        cols = self.edges_columnar(columns=self.TCSR_COLS)
        n = self.num_entities()
        if self._store.in_batch():
            self._tcsr = (TemporalCSR.build(cols, n), cols)
            return self._tcsr
        path = self.path / "index" / "tcsr.npz"
        gen, sha = self._store.generation(), self._store.manifest_sha()
        csr = load_permutation(path, cols, n, gen, sha)
        if csr is None:
            csr = TemporalCSR.build(cols, n)
            if not self.read_only:  # a reader keeps it in memory
                save_permutation(path, csr, gen, sha)
        self._tcsr = (csr, cols)
        return self._tcsr

    # --- batch transactions ---------------------------------------------- #
    #
    # `Store._write` brackets every batch, and the engine needs the same
    # bracket because a batch is exactly one published generation. The
    # transaction time is taken from the first row written in the batch:
    # `apply_ops` stamps every row it produces with the batch's tt.

    def apply_ops(self, ops: Sequence[dict[str, Any]], tt: int,
                  raw: bytes | None = None) -> None:
        """Open the engine batch at the authoritative transaction time.

        `apply_ops` is the only caller that knows the batch's `tt` up front —
        individual writes only carry it per row, and `ensure_entities` does
        not carry it at all. Everything else is the inherited bi-temporal
        semantics, unchanged.

        `raw` is this batch's event-log record, whose `ops` are
        `ops`. With it, each `ingest_events` op is offered to the engine's
        raw-record fast path first (`_ingest_events` below).
        """
        if not self._sc_on:
            self._apply_ops_once(ops, tt, raw)
            return
        self._sc_txn = (ops, tt, raw)
        self._sc_cursor = None
        self._sc_applying += 1
        try:
            self._apply_ops_once(ops, tt, raw)
        finally:
            self._sc_applying -= 1

    def _apply_ops_once(self, ops: Sequence[dict[str, Any]], tt: int,
                        raw: bytes | None) -> None:
        fresh = not self._store.in_batch()
        self._ensure_batch(tt)
        if raw is None:
            super().apply_ops(ops, tt)
            return
        # A transaction with per-operation ops is applied by the
        # engine from its record (`crates/gnomon-core/src/apply_raw.rs`),
        # which stages exactly what the loop below stages. Only on a batch
        # this transaction opened, so a refusal can roll back this
        # transaction and nothing else -- never on the small-commit path,
        # whose batch carries unpublished batch-mates, nor under `paranoid`.
        if (fresh and not self._sc_on and not self.paranoid
                and any(op.get("op") != "ingest_events" for op in ops)):
            try:
                verdict = self._store.apply_ops_raw(raw, tt)
            except Exception as e:
                raise _translate(e) from None
            if verdict == "applied":
                self._tcsr = None  # as `apply_ops` does after its loop
                self.note_frontier_tt(tt)
                return
            if verdict == "refused":
                # an op the Python path raises on: undo the ops staged before
                # it, then let that path apply the record again and raise there
                try:
                    self._store.rollback()
                except Exception as e:
                    raise _translate(e) from None
                self._ensure_batch(tt)
        self._raw_batch = (raw, {id(op): i for i, op in enumerate(ops)})
        try:
            super().apply_ops(ops, tt)
        finally:
            self._raw_batch = None

    def _ingest_events(self, op: dict[str, Any], tt: int) -> None:
        """Stage the op from the batch's event-log record in the
        engine when it is in the shape the engine reproduces exactly (no
        `nodes`, no event `props`; see `crates/gnomon-core/src/
        ingest_raw.rs`) and `GNOMON_INGEST_RAW` is not `0`. Otherwise — or
        without a record — the Python row build, unchanged."""
        rb = self._raw_batch
        if rb is not None:
            i = rb[1].get(id(op))
            if i is not None:
                try:
                    done = self._store.stage_ingest_events_raw(rb[0], i, tt)
                except Exception as e:
                    raise _translate(e) from None
                if done is not None:
                    return
        super()._ingest_events(op, tt)

    def begin(self) -> None:
        # the engine batch opens at the first apply_ops/write, when tt is known
        pass

    def commit(self) -> None:
        if self._sc_on:
            self._small_commit_end_txn()
            return
        self._publish()

    def _publish(self) -> None:
        if not self._store.in_batch():
            return  # a batch that wrote nothing publishes nothing
        t = time.perf_counter()
        try:
            self._store.commit()
        except Exception as e:
            raise _translate(e) from None
        wall_us = int((time.perf_counter() - t) * 1e6)
        # `last_commit_phases()["total_us"]` is the Rust engine's own
        # `commit_start.elapsed()` (which fully accounts for it internally);
        # what's left over here is the pyo3 call boundary this method pays on
        # top of that, never negative by construction but clamped anyway
        # against clock-source jitter between the two timers.
        engine_us = int(self._store.last_commit_phases()["total_us"])
        self._last_commit_python_wrap_us = max(0, wall_us - engine_us)

    def last_commit_python_wrap_us(self) -> int:
        """See `_last_commit_python_wrap_us`. 0 before any commit."""
        return self._last_commit_python_wrap_us

    def rollback(self) -> None:
        if self._sc_on:
            self._small_commit_rollback()
            return
        if not self._store.in_batch():
            return
        try:
            self._store.rollback()
        except Exception as e:
            raise _translate(e) from None

    # --- the small-commit path (GNOMON_SMALL_COMMIT=1, opt-in) --------------- #
    #
    # A transaction is durable once `Store._write` has fsynced its event-log
    # record: that record is the source of truth, and a crash before the
    # generation that contains it is published is recovered by the
    # suffix replay (`Store._recover`). The per-transaction generation is a
    # materialized view of the log, so the small-commit path keeps the engine
    # batch open across transactions and publishes one generation per
    # `small_commit_n` transactions, or once the oldest unpublished one is
    # `small_commit_ms` old, or before any read on this handle, or on close.
    #
    # What that changes is cross-process visibility and nothing else: a
    # reader in another process (a `read_only` handle) sees published
    # generations, so it may lag this writer by up to N transactions or T
    # ms. This handle always reads its own commits — every read method
    # publishes first (`_small_commit_barrier`). Durability, the event-log
    # bytes and both digests are unchanged: each transaction keeps its own
    # log record and its rows their own `tt_s`; only how many transactions
    # one generation carries differs.
    #
    # A transaction that fails rolls the engine batch back — which discards
    # its unpublished batch-mates too — and the batch-mates are applied
    # again from their records. Apply is deterministic (replay rests on
    # that), so they stage exactly what they staged the first time.

    #: Methods the read barrier leaves alone: the write path itself, the
    #: dictionary (staged entities are visible either way), what describes
    #: the handle, and the compaction trigger, which is a question about
    #: published state that the replay driver asks after every transaction.
    _SC_NO_BARRIER = frozenset({
        "apply_ops", "begin", "commit", "rollback", "close", "publish_pending",
        "small_commit", "small_commit_pending", "insert_node_versions",
        "insert_edge_versions", "close_node_versions", "close_edge_versions",
        "retire_node_versions", "retire_edge_versions", "ensure_entities",
        "dense_ids", "uids_for", "num_entities", "note_event_cursor",
        "note_frontier_tt", "frontier_tt", "designs", "build_info",
        "last_commit_python_wrap_us", "reopen_on_enoent_total",
        "needs_compaction", "needs_whole_store_compaction", "needs_settle",
    })

    def _small_commit_init(self) -> None:
        on, n, ms = (False, 1, 0) if self.read_only else self._store.small_commit()
        self._sc_on = bool(on)
        self._sc_n = max(1, int(n))
        self._sc_ms = max(0, int(ms))
        #: unpublished transactions, oldest first: `(ops, tt, raw, cursor)`
        self._sc_pending: list[tuple[Sequence[dict[str, Any]], int, bytes | None,
                                     tuple[int, str] | None]] = []
        self._sc_txn: tuple[Sequence[dict[str, Any]], int, bytes | None] | None = None
        self._sc_cursor: tuple[int, str] | None = None
        self._sc_first = 0.0
        self._sc_tt = 0
        self._sc_applying = 0
        #: serializes a publish with this handle's own reads: the publish may
        #: come from `Store`'s timer thread (the `small_commit_ms` bound)
        import threading
        self._sc_lock = threading.RLock()
        if self._sc_on:
            self._small_commit_barrier()

    def small_commit(self) -> dict[str, Any]:
        """The small-commit path as this handle runs it: `{"enabled", "n",
        "ms"}`, read when the handle opened (`GNOMON_SMALL_COMMIT`,
        `GNOMON_SMALL_COMMIT_N`, `GNOMON_SMALL_COMMIT_MS`)."""
        return {"enabled": self._sc_on, "n": self._sc_n, "ms": self._sc_ms}

    def small_commit_pending(self) -> tuple[int, float]:
        """`(transactions acknowledged but unpublished, seconds the oldest
        has waited)`; `(0, 0.0)` when the path is off or nothing is pending."""
        if not self._sc_pending:
            return 0, 0.0
        return len(self._sc_pending), time.monotonic() - self._sc_first

    def publish_pending(self) -> None:
        """Publish the open batch now, if the small-commit path holds one."""
        if not self._sc_on or self._sc_applying:
            return
        with self._sc_lock:
            if self._sc_pending:
                crash_point("py_small_commit_publish")
                self._publish()
            self._sc_pending.clear()

    def _small_commit_end_txn(self) -> None:
        if self._sc_txn is not None:
            ops, tt, raw = self._sc_txn
            self._sc_txn = None
            if not self._sc_pending:
                self._sc_first = time.monotonic()
            self._sc_pending.append((ops, tt, raw, self._sc_cursor))
            self._sc_cursor = None
        if not self._store.in_batch():
            # nothing staged by any pending transaction: nothing to publish,
            # as on the default path
            self._sc_pending.clear()
            return
        if (len(self._sc_pending) >= self._sc_n
                or (time.monotonic() - self._sc_first) * 1e3 >= self._sc_ms):
            self.publish_pending()
            return
        # acknowledged (the record is fsynced) but not yet published
        crash_point("py_small_commit_deferred")

    def _small_commit_rollback(self) -> None:
        self._sc_txn = None
        self._sc_cursor = None
        if not self._store.in_batch():
            return
        try:
            self._store.rollback()
        except Exception as e:
            raise _translate(e) from None
        self._sc_tt = 0
        mates = list(self._sc_pending)
        if not mates:
            return
        self._sc_applying += 1
        try:
            cursor = None
            for i, (ops, tt, raw, cur) in enumerate(mates):
                self._apply_ops_once(ops, tt, raw)
                if cur is not None:
                    cursor = cur
                if i == 0:
                    crash_point("py_small_commit_reapply")
            if cursor is not None:
                self._store.set_event_cursor(int(cursor[0]), cursor[1])
        finally:
            self._sc_applying -= 1

    def _small_commit_barrier(self) -> None:
        """Shadow every read method of this instance with one that first
        publishes what the handle holds unpublished, so a read here always
        sees this writer's own commits through the ordinary (batch-free)
        read paths. Installed only when the path is on: off, nothing on any
        path changes."""
        import inspect

        def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
            def read(*args: Any, **kwargs: Any) -> Any:
                if self._sc_applying:
                    return fn(*args, **kwargs)
                with self._sc_lock:
                    if self._sc_pending:
                        self.publish_pending()
                    return fn(*args, **kwargs)
            read.__name__ = getattr(fn, "__name__", "read")
            read.__doc__ = getattr(fn, "__doc__", None)
            return read

        for name in dir(type(self)):
            if name.startswith("_") or name in self._SC_NO_BARRIER:
                continue
            static = inspect.getattr_static(type(self), name)
            if isinstance(static, (property, staticmethod, classmethod)) or not callable(static):
                continue
            setattr(self, name, wrap(getattr(self, name)))

    # --- replay cursor (suffix recovery) ------------------------------------ #

    def event_cursor(self) -> tuple[int, str]:
        """`(offset, chain)` the current generation recorded.

        `offset` points immediately past the newline of the last applied
        event-log record; `chain` is the rolling hash over that prefix. A
        store written before cursors existed reports chain `""` — the
        caller must treat that as "no cursor", never as "nothing applied".
        """
        offset, chain = self._store.event_cursor()
        return int(offset), chain

    def note_event_cursor(self, offset: int, chain: str) -> None:
        """Stage the cursor the next commit records in its manifest."""
        self._store.set_event_cursor(int(offset), chain)
        if self._sc_on:
            self._sc_cursor = (int(offset), chain)

    def _ensure_batch(self, tt: int) -> None:
        """Open the engine batch lazily; `in_batch` is the source of truth.
        On the small-commit path a later transaction joins the batch its
        unpublished predecessors left open, which moves the batch's
        transaction time forward instead."""
        if not self._store.in_batch():
            try:
                self._store.begin(tt)
            except Exception as e:
                raise _translate(e) from None
            self._sc_tt = tt
        elif self._sc_on and tt > self._sc_tt:
            try:
                self._store.advance_batch(tt)
            except Exception as e:
                raise _translate(e) from None
            self._sc_tt = tt

    # --- entities ---------------------------------------------------------- #

    def ensure_entities(self, uid_labels: Iterable[tuple[str, str]]) -> None:
        uids: list[str] = []
        labels: list[str] = []
        seen: set[str] = set()
        for uid, label in uid_labels:
            if uid not in seen:
                seen.add(uid)
                uids.append(uid)
                labels.append(label)
        if not uids:
            return
        try:
            self._store.ensure_entities(uids, labels)
        except Exception as e:
            raise _translate(e) from None

    def dense_ids(self, uids: Sequence[str]) -> np.ndarray:
        try:
            return self._store.dense_ids(list(uids))
        except Exception as e:
            raise _translate(e) from None

    def uids_for(self, ids: Sequence[int]) -> list[str]:
        try:
            return self._store.uids_for([int(i) for i in ids])
        except Exception as e:
            raise _translate(e) from None

    def num_entities(self) -> int:
        return int(self._store.num_entities())

    # --- version writes ----------------------------------------------------- #

    def insert_node_versions(self, rows: Sequence[NodeVersion]) -> None:
        if not rows:
            return
        self._ensure_batch(rows[0].tt_s)
        try:
            self._store.stage_nodes(
                {
                    "vid": [r.vid for r in rows],
                    "uid": [r.uid for r in rows],
                    "label": [r.label for r in rows],
                    "vt_s": [r.vt_s for r in rows],
                    "vt_e": [r.vt_e for r in rows],
                    "tt_s": [r.tt_s for r in rows],
                    "props": [canonical_json(r.props) for r in rows],
                    "source": [r.source for r in rows],
                    "provenance_ref": [r.provenance_ref for r in rows],
                }
            )
        except Exception as e:
            raise _translate(e) from None

    def insert_edge_versions(self, rows: Sequence[EdgeVersion]) -> None:
        if not rows:
            return
        self._ensure_batch(rows[0].tt_s)
        try:
            self._store.stage_edges(
                {
                    "vid": [r.vid for r in rows],
                    "src": [r.src for r in rows],
                    "dst": [r.dst for r in rows],
                    "rel_type": [r.rel_type for r in rows],
                    "disc": [r.disc for r in rows],
                    "vt_s": [r.vt_s for r in rows],
                    "vt_e": [r.vt_e for r in rows],
                    "tt_s": [r.tt_s for r in rows],
                    "props": [canonical_json(r.props) for r in rows],
                    "source": [r.source for r in rows],
                    "provenance_ref": [r.provenance_ref for r in rows],
                }
            )
        except Exception as e:
            raise _translate(e) from None

    def close_node_versions(self, vids: Sequence[str], tt_e: int,
                           vt_s: Sequence[int] | None = None) -> None:
        self._close("node", vids, tt_e, vt_s)

    def close_edge_versions(self, vids: Sequence[str], tt_e: int,
                           vt_s: Sequence[int] | None = None) -> None:
        self._close("edge", vids, tt_e, vt_s)

    def _close(self, kind: str, vids: Sequence[str], tt_e: int,
              vt_s: Sequence[int] | None = None) -> None:
        """Record closes, passing the rows' `vt_s` when the caller has it.

        `vt_s` is the first half of the physical sort key `(vt_s, vid)`, so
        the engine can bound which segments could hold the row from the
        manifest's `key_lo`/`key_hi` and binary-search only those -- instead
        of the store-wide vid postings map that used to be built by the
        first correction of a build and never released. Omitting it is
        correct and slower, never wrong.
        """
        if not vids:
            return
        self._ensure_batch(tt_e)
        try:
            self._store.stage_closes(kind, list(vids), tt_e,
                                    None if vt_s is None else list(vt_s))
        except Exception as e:
            raise _translate(e) from None

    def retire_node_versions(self, vids: Sequence[str]) -> None:
        self._retire("node", vids)

    def retire_edge_versions(self, vids: Sequence[str]) -> None:
        self._retire("edge", vids)

    def _retire(self, kind: str, vids: Sequence[str]) -> None:
        """Drop rows the open batch staged and has since replaced. They only
        ever exist in staging — a committed row belongs to an earlier
        transaction time and is closed, never retired — so this cannot reach
        a sealed segment."""
        if not vids:
            return
        try:
            self._store.stage_retires(kind, list(vids))
        except Exception as e:
            raise _translate(e) from None

    # --- version reads -------------------------------------------------------- #

    @staticmethod
    def _nodes(cols: dict[str, Any]) -> list[NodeVersion]:
        return [
            NodeVersion(
                vid=cols["vid"][i],
                uid=cols["uid"][i],
                label=cols["label"][i],
                vt_s=cols["vt_s"][i],
                vt_e=cols["vt_e"][i],
                tt_s=cols["tt_s"][i],
                tt_e=cols["tt_e"][i],
                props=json.loads(cols["props"][i]),
                source=cols["source"][i] or "ingest",
                provenance_ref=cols["provenance_ref"][i],
            )
            for i in range(len(cols["vid"]))
        ]

    @staticmethod
    def _edges(cols: dict[str, Any]) -> list[EdgeVersion]:
        return [
            EdgeVersion(
                eid=cols["eid"][i],
                vid=cols["vid"][i],
                src=cols["src"][i],
                dst=cols["dst"][i],
                rel_type=cols["rel_type"][i],
                disc=cols["disc"][i] or "",
                vt_s=cols["vt_s"][i],
                vt_e=cols["vt_e"][i],
                tt_s=cols["tt_s"][i],
                tt_e=cols["tt_e"][i],
                props=json.loads(cols["props"][i]),
                source=cols["source"][i] or "ingest",
                provenance_ref=cols["provenance_ref"][i],
            )
            for i in range(len(cols["vid"]))
        ]

    def believed_node_versions(self, uid: str, as_of_tt: int = OPEN_END) -> list[NodeVersion]:
        return self._nodes(self._read_op(
            lambda s: s.believed("node", uid, clamp_tt(as_of_tt))))

    def nodes_valid_at(self, ids: Sequence[int], t: int,
                       as_of_tt: int = OPEN_END) -> dict[int, tuple[str, str]]:
        """`{dense id: (uid, label)}` for the ids among `ids` that have a
        node version valid at instant `t` (`vt_s <= t < vt_e`) believed at
        `as_of_tt` -- by one identity point read per id, not a node scan.

        For the seed-driven `snapshot_subgraph`: exactly what
        `nodes_columnar(as_of_tt, t, t + 1)` says about these ids. Where an
        id has several such versions the label is the one that scan lists
        last, the version greatest in `(vt_s, vid)`, which is the scan's
        order (`read.rs::scan_node_versions`).
        """
        ids = [int(i) for i in ids]
        if not ids:
            return {}
        as_of = clamp_tt(as_of_tt)
        uids = self.uids_for(ids)
        if os.environ.get("GNOMON_NODES_VALID_BATCH", "1") != "0" and \
                hasattr(self._store, "nodes_valid_at"):
            # The loop below in one engine call
            return {int(i): (u, lab) for i, u, lab in self._read_op(
                lambda s: s.nodes_valid_at(ids, list(uids), int(t), as_of))}

        def run(s: Any) -> dict[int, tuple[str, str]]:
            out: dict[int, tuple[str, str]] = {}
            for i, uid in zip(ids, uids):
                c = s.believed("node", uid, as_of)
                best: tuple[tuple[int, str], str] | None = None
                for vt_s, vt_e, vid, label in zip(c["vt_s"], c["vt_e"], c["vid"], c["label"]):
                    if vt_s <= t < vt_e and (best is None or (vt_s, vid) > best[0]):
                        best = ((vt_s, vid), label)
                if best is not None:
                    out[i] = (uid, best[1])
            return out

        return self._read_op(run)

    def believed_edge_versions(self, eid: str, as_of_tt: int = OPEN_END, *,
                               src: str | None = None, dst: str | None = None) -> list[EdgeVersion]:
        # src/dst are performance hints for backends that can anchor on them;
        # the native engine's `eid` lookup is already a hash-map hit, so there
        # is nothing to anchor and the hints are ignored.
        return self._edges(self._read_op(
            lambda s: s.believed("edge", eid, clamp_tt(as_of_tt))))

    def believed_node_versions_overlapping(self, uid: str, vt_s: int, vt_e: int,
                                           as_of_tt: int = OPEN_END) -> list[NodeVersion]:
        """The engine's windowed point read (`pointread.rs`): seeks to the
        versions overlapping `[vt_s, vt_e)` instead of materializing the
        uid's whole believed history and filtering it here. Same answer as
        the base class's filter, in the same order."""
        return self._nodes(self._read_op(
            lambda s: s.believed_window("node", uid, int(vt_s), int(vt_e), clamp_tt(as_of_tt))))

    def believed_edge_versions_overlapping(self, eid: str, vt_s: int, vt_e: int,
                                           as_of_tt: int = OPEN_END, *,
                                           src: str | None = None,
                                           dst: str | None = None) -> list[EdgeVersion]:
        # src/dst: ignored, as in `believed_edge_versions`
        return self._edges(self._read_op(
            lambda s: s.believed_window("edge", eid, int(vt_s), int(vt_e), clamp_tt(as_of_tt))))

    def identity_versions(self, kind: str, key: str) -> list[NodeVersion] | list[EdgeVersion]:
        """The engine's identity log (`read.rs::*_versions_of`): the
        identity's rows located through its postings, every belief, closes
        applied, open batch included, in `(tt_s, vid)` order. An edge key
        that is not an eid's hex is no identity: empty."""
        if kind == "edge":
            try:
                int(key, 16)
            except ValueError:
                return []
            if len(key) != 24:
                return []
            return self._edges(self._read_op(lambda s: s.versions_of("edge", key)))
        return self._nodes(self._read_op(lambda s: s.versions_of("node", key)))

    def identity_version_cols(self, kind: str, key: str) -> dict[str, list[Any]] | None:
        """`identity_versions` as the engine's columns, without building a
        version object or parsing props per row; `None`
        where `identity_versions` returns no rows for a key that is no
        identity."""
        if kind == "edge":
            try:
                int(key, 16)
            except ValueError:
                return None
            if len(key) != 24:
                return None
            return self._read_op(lambda s: s.versions_of("edge", key))
        return self._read_op(lambda s: s.versions_of("node", key))

    def node_intervals(self, ids: Sequence[int], vt_min: int, vt_max: int,
                       as_of_tt: int = OPEN_END) -> dict[int, list[tuple[int, int]]]:
        """`{dense id: [(vt_s, vt_e), ...]}` of the node versions believed at
        `as_of_tt` overlapping `[vt_min, vt_max)`, by one point read per id
        (the windowed read of `believed_node_versions_overlapping`): what
        `nodes_columnar(as_of_tt, vt_min, vt_max)` says about these ids."""
        ids = [int(i) for i in ids]
        if not ids:
            return {}
        a, s_, e_ = self._read_op(lambda s: s.node_intervals(ids, int(vt_min), int(vt_max),
                                                             clamp_tt(as_of_tt)))
        out: dict[int, list[tuple[int, int]]] = {}
        for i, x, y in zip(a, s_, e_):
            out.setdefault(int(i), []).append((int(x), int(y)))
        return out

    def nodes_with_believed_versions(
        self, uids: Sequence[str], as_of_tt: int = OPEN_END
    ) -> set[str]:
        if not uids:
            return set()
        return set(self._read_op(
            lambda s: s.believed_any(list(uids), clamp_tt(as_of_tt))))

    def all_node_versions(self) -> Iterable[NodeVersion]:
        return self._nodes(self._read_op(lambda s: s.all_versions("node")))

    def all_edge_versions(self) -> Iterable[EdgeVersion]:
        return self._edges(self._read_op(lambda s: s.all_versions("edge")))

    def all_versions_page(
        self, kind: str, offset: int, limit: int
    ) -> tuple[dict[str, Any], int]:
        """One page of the whole-store version population, every
        `to_json()` field included (`props`, `source`, `provenance_ref`) —
        the engine-side half of the paged digest producer. `(offset, limit)` follow `versions_page`'s own convention,
        but the row *order* is the engine's whole-store walk order
        (unspecified — the digest sorts either way), not `versions_page`'s
        `(tt_s, vid)` belief order, and every field lands in the page
        (`versions_page` above deliberately drops `props`/`source`/
        `provenance_ref` for `version_history`'s cheaper shape) —
        the two exist for different callers and neither can serve the
        other's.
        """
        cols, total = self._read_op(lambda s: s.all_versions_page(kind, offset, limit))
        return cols, int(total)

    def all_versions_paged(
        self, kind: str, chunk_rows: int
    ) -> Iterator[list[NodeVersion] | list[EdgeVersion]]:
        """Override of `StorageAdapter`'s O(store) default (`base.py`):
        pages through `all_versions_page` above instead of materializing
        every version of `kind` into one Python list up front, so
        `store_digest_streaming` never holds more than `chunk_rows`
        `NodeVersion`/`EdgeVersion` objects (each with `props` already
        `json.loads`-parsed) at once — the fix for the ~80 GB digest-pass
        transient measured at 100M rows (a Python
        list of every version, on top of the Rust `Vec` that fed it).
        """
        convert = self._nodes if kind == "node" else self._edges
        offset = 0
        while True:
            cols, total = self.all_versions_page(kind, offset, chunk_rows)
            page = convert(cols)
            if page:
                yield page
            offset += chunk_rows
            if offset >= total:
                return

    def versions_page(
        self,
        kind: str,
        *,
        as_of: int = OPEN_END,
        t_a: int,
        t_b: int,
        belief: str = "current",
        rel_types: Sequence[str] | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[dict[str, Any], int]:
        """The bounded override of the ABC default, following the same compatibility
        pattern as the operator it replaces.

        Everything the default does over ten object arrays covering the whole
        population — mask, `(tt_s, vid)` order, count, slice — the engine does
        over 32-byte integer keys, and only the page crosses the boundary.
        `props` is not among the columns asked for, so no row is parsed out of
        JSON on the way back either; the default reached `versions_columnar`,
        which reached `all_*_versions`, which built one `props` dict per
        version of the store to answer a 50-row question.

        Peak memory is therefore the page plus the key array plus the segment
        cache budget, not ~1,060 bytes per stored version.
        """
        got = self._read_op(lambda s: s.version_page(
            kind, clamp_tt(as_of), t_a, t_b, belief,
            list(rel_types) if rel_types is not None else None,
            offset, limit,
        ))
        # int columns arrive as int64 arrays, string columns as lists of str
        # — the same two shapes the ABC default's object arrays present at
        # `cols[c][i]`, so the row build above them is unchanged.
        cols = {c: got[c] for c in self.VERSION_COLS[kind]}
        return cols, int(got["rows_total"])

    def props_for_vids(self, kind: str, vids: Sequence[str]) -> dict[str, dict]:
        if not vids:
            return {}
        raw = self._read_op(lambda s: s.props_for_vids(kind, list(vids)))
        return {vid: json.loads(text) for vid, text in raw.items()}

    # --- columnar read path ----------------------------------------------------- #

    def edges_columnar(
        self,
        as_of_tt: int = OPEN_END,
        vt_min: int | None = None,
        vt_max: int | None = None,
        rel_types: Sequence[str] | None = None,
        columns: Sequence[str] | None = None,
        touching_ids: Sequence[int] | None = None,
        touching_both: bool = False,
    ) -> dict[str, np.ndarray]:
        got = self._read_op(lambda s: s.scan_edges(
            as_of_tt=clamp_tt(as_of_tt),
            vt_min=vt_min,
            vt_max=vt_max,
            rel_types=list(rel_types) if rel_types is not None else None,
            touching_ids=[int(i) for i in touching_ids]
            if touching_ids is not None
            else None,
            touching_both=touching_both,
            limit=None,
            # a real pushdown: unprojected string columns are never built
            columns=list(columns) if columns is not None else None,
        ))
        out: dict[str, np.ndarray] = {}
        for c in self.EDGE_INT_COLS:
            if columns is None or c in columns:
                out[c] = np.asarray(got[c], dtype=np.int64)
        for c in self.EDGE_STR_COLS:
            if columns is None or c in columns:
                out[c] = np.asarray(got[c], dtype=object)
        # opt-in only; the engine already knows how to project these
        for c in self.EDGE_OPT_COLS:
            if columns is not None and c in columns:
                out[c] = np.asarray(got[c], dtype=object)
        # physical row addresses (native only, explicit request only): the
        # ticket for coming back to the engine for derived fields of a few
        # surviving rows without materializing them for the whole scan
        for c in ("seg_id", "seg_row"):
            if columns is not None and c in columns:
                out[c] = np.asarray(got[c], dtype=np.int64)
        return out

    def neighborhood_evolution_parts(self, uid_id: int, t1: int, t2: int, as_of_tt: int,
                                     stride: int) -> tuple[list[str], list[str], list[dict[str, int]]] | None:
        """`neighborhood_evolution`'s one-read form in one engine call
: the gained and the lost neighbours' uids, each
        sorted, and the degree series, from the read `edges_columnar` makes
        over `[t1, t2 + 1)` touching `uid_id`. `None` without the engine
        function, or when the handle is not the engine's own (a wrapper
        around it): the operator then runs its NumPy form."""
        fn = getattr(_engine, "neighborhood_evolution_parts", None)
        if fn is None or type(self._store) is not _engine.NativeStore:
            return None
        return self._read_op(lambda s: fn(s, int(uid_id), int(t1), int(t2),
                                          clamp_tt(as_of_tt), int(stride)))

    def aggregate_events_columnar(
        self,
        as_of_tt: int,
        t_a: int,
        t_b: int,
        rel_types: Sequence[str] | None,
        stride: int | None,
        group_by: Sequence[tuple[str, str | None]],
        aggregates: Sequence[tuple[str, str | None]],
        max_groups: int,
        prop_keys: Sequence[str] | None = None,
        agg_props: Sequence[int | None] | None = None,
        prop_filter: tuple[int, str, Any] | None = None,
        cohort: tuple[Sequence[int], str] | None = None,
        valid_at: int | None = None,
    ) -> dict[str, Any] | None:
        """fast path: the whole grouped aggregation in one engine call
        (aggregate.rs — two-phase parallel partials over the scan's
        selections, fixed-width group codes, canonical order). Returns key
        code columns plus rehydration tables; the operator pages in Python.

        With any of the last four arguments the engine runs its extended
        fold and returns `None` when it declines the call; the answer then
        carries `prop_skipped`, the per-key `prop_coercion` counts.
        """
        ext: dict[str, Any] = {}
        if prop_keys is not None:
            ext["prop_keys"] = list(prop_keys)
            ext["agg_props"] = list(agg_props) if agg_props is not None else None
        if prop_filter is not None:
            ext["prop_filter"] = (int(prop_filter[0]), str(prop_filter[1]), prop_filter[2])
        if cohort is not None:
            ext["cohort"] = ([int(i) for i in cohort[0]], str(cohort[1]))
        if valid_at is not None:
            ext["valid_at"] = int(valid_at)
        got = self._read_op(lambda s: s.aggregate_edges(
            as_of_tt=clamp_tt(as_of_tt),
            t_a=int(t_a),
            t_b=int(t_b),
            rel_types=list(rel_types) if rel_types is not None else None,
            stride=None if stride is None else int(stride),
            group_by=[(d, r) for d, r in group_by],
            aggregates=[(a, o) for a, o in aggregates],
            max_groups=int(max_groups),
            **ext,
        ))
        if got is None:
            return None
        return {
            **({"prop_skipped": list(got["prop_skipped"])}
               if "prop_skipped" in got else {}),
            "rows_total": int(got["rows_total"]),
            "keys": [np.asarray(k, dtype=np.int64) for k in got["keys"]],
            "aggs": got["aggs"],
            "rel_names": got["rel_names"],
            "label_names": got["label_names"],
            # How much of the manifest the call had to open. Additive --
            # `ops_aggregate` reads the answer columns by name; this is here
            # so window selectivity is observable without a stopwatch
            # (`tests/test_aggregate_window_selectivity.py`).
            "segments_total": int(got["segments_total"]),
            "segments_pruned": int(got["segments_pruned"]),
            "segments_opened": int(got["segments_opened"]),
            # Rows the verdict ranges handed to the fold. `segments_opened`
            # alone cannot say whether two layouts of the same store differ in
            # reach or only in how many files that reach is spread over.
            "rows_selected": int(got["rows_selected"]),
        }

    def edge_idents_at(self, seg_ids: Sequence[int],
                       seg_rows: Sequence[int]) -> tuple[np.ndarray, np.ndarray]:
        """`(eid, rel_type)` for scan addresses, in input order.

        Addresses come from a scan projecting `seg_id`/`seg_row` and are
        valid only against the generation that produced them — the engine
        refuses ids the current generation does not name.
        """
        eids, rels = self._read_op(lambda s: s.edge_idents_at(
            [int(i) for i in seg_ids], [int(r) for r in seg_rows]))
        return np.asarray(eids, dtype=object), np.asarray(rels, dtype=object)

    def props_at(self, kind: str, seg_ids: Sequence[int],
                 seg_rows: Sequence[int]) -> list[dict]:
        """Props of explicit scan addresses, in input order.

        `props_for_vids` for rows whose addresses a scan projecting
        `seg_id`/`seg_row` already returned: the engine reads exactly those
        rows rather than sweeping every row of the kind for a vid. Valid only
        against the generation that produced the addresses.
        """
        raw = self._read_op(lambda s: s.props_at(
            kind, [int(i) for i in seg_ids], [int(r) for r in seg_rows]))
        return [json.loads(text) for text in raw]

    def nodes_columnar(
        self,
        as_of_tt: int = OPEN_END,
        vt_min: int | None = None,
        vt_max: int | None = None,
    ) -> dict[str, np.ndarray]:
        got = self._read_op(lambda s: s.scan_nodes(
            as_of_tt=clamp_tt(as_of_tt), vt_min=vt_min, vt_max=vt_max
        ))
        out = {c: np.asarray(got[c], dtype=np.int64) for c in ("uid_id", "vt_s", "vt_e")}
        out.update({c: np.asarray(got[c], dtype=object) for c in ("uid", "vid", "label")})
        return out

    # --- statistics and maintenance ------------------------------------------------ #

    def stats(self) -> dict[str, Any]:
        s = self._store.stats()
        return {
            "n_entities": int(s["n_entities"]),
            "n_node_versions": int(s["n_node_versions"]),
            "n_edge_versions": int(s["n_edge_versions"]),
            "vt_min": s["vt_min"],
            "vt_max": s["vt_max"],
            "rel_type_counts": s["rel_type_counts"],
            "max_out_degree": int(s["max_out_degree"]),
        }

    def compact(self, build_indexes: bool = False) -> dict[str, int]:
        """Fold close runs into segment sidecars and merge runs.

        `build_indexes=True` pre-builds `native/idx/` for the segments
        compaction just wrote — **both** kinds, the per-segment
        TCSR and the sorted name index, which is what settle already does.
        Compaction orphans both by construction: a rewritten segment is a new
        file with a new `body_sha`, and both indexes are stamped with it. So
        rebuilding one and not the other left the next `lookup_name` paying
        exactly the build this flag exists to have paid already. This removed
        that asymmetry; before it, the flag meant "the TCSR" and the name
        index was silently excluded.

        **Off by default**, unchanged: compaction time is a published number
        (the "maintain vs strip" comparison) and taxing it to
        serve a query that may never run is the wrong trade. It is on for
        benchmark stores, so the numbers that get measured are steady-state
        rather than including a first-query build.

        The build happens here rather than inside the engine's `compact`
        deliberately: it is exactly `build_indexes(name=True)` run afterwards
        — the same load-or-build a first query takes — it cannot fail a
        compaction that already succeeded, and it keeps the engine's
        compaction path untouched this cycle.
        """
        try:
            report = self._store.compact()
        except Exception as e:
            raise _translate(e) from None
        if build_indexes:
            self.build_indexes(name=True, vidfilter=True)
        return report

    def compact_current_only(self) -> dict[str, int]:
        """Strip the store to the currently believed rows.

        Experimental configuration for the current-vs-bi-temporal overhead
        measurement: superseded versions, close runs, and sidecars are
        physically dropped and the store is stamped ``CURRENT_ONLY``, after
        which it refuses past-belief queries and corrections. Current-belief
        answers are unchanged — that equivalence is the experiment's gate.
        """
        try:
            return self._store.compact_current_only()
        except Exception as e:
            raise _translate(e) from None

    def current_only(self) -> bool:
        """Whether this store is the stripped-down configuration."""
        return bool(self._store.current_only())

    def build_indexes(self, tcsr: bool = True,
                      name: bool = False,
                      vidfilter: bool = False) -> dict[str, Any]:
        """Pre-build the rebuildable indexes under `native/idx/`.

        Indexes are built lazily at first use: taxing the write
        path — a published 43k ev/s number — to serve a query that may never
        run is the wrong trade. This is the escape hatch for the case where
        it *is* the right trade: a latency-sensitive long-running process, or a
        benchmark store that must be measured at steady state.

        **One code path with the lazy build, by construction.** This does not
        re-implement anything: the engine's `build_tcsr_indexes` walks the
        edge segments the current manifest names and calls `tcsr_index(file)`
        on each — the *same* load-or-build a first traversal calls, cache,
        `body_sha` stamp check, silent-rebuild rule and `built`/`loaded`
        counters included. So a pre-built index and a lazily built one are the
        same bytes, and there is no second place for the two to diverge. The
        name index (`build_name_indexes`) has the same shape one level down.

        `name=True` folds the sorted name index into the same report — it is
        what settle and `gnomon store build-indexes` both want, and having it
        here means neither of them assembles its own report shape.

        `vidfilter=True` folds in the per-segment identity filter, and
        it is the one index this adapter does *not* leave to the lazy path.
        The other two are built on first use because the first use is cheap;
        the filter's first use is the whole cost it exists to remove: at scale,
        the 1B correction ramp is one operation, a single
        whole-store identity probe — so a filter written lazily would be
        written during a pass that has already happened. The engine
        therefore builds it at seal, and this call is the migration path for
        a store that predates the change, not the normal way it is built.

        Rebuildable and never authoritative, so this is always safe to
        run, always safe to skip, and idempotent: a segment whose index is
        already on disk is loaded, not rebuilt.
        """
        out: dict[str, Any] = {}
        if tcsr:
            out["tcsr"] = dict(self._read_op(lambda s: s.build_indexes()))
        if name:
            out["name"] = {"segments_built": self.build_name_indexes()}
        if vidfilter:
            out["vidfilter"] = dict(
                self._read_op(lambda s: s.build_vid_filters()))
        return out

    def postings_stats(self, kind: str = "edge") -> tuple[int, int, int]:
        """`(identity_entries, vid_entries, resident_segments)` of one row
        kind's identity postings. The middle element is always 0
        since the vid map was removed; it is kept so the existing
        receipts keep their shape."""
        return tuple(self._store.postings_stats(kind))

    def open_path_stats(self) -> dict[str, int]:
        """How this handle's segment opens resolved: `cached_hits`,
        `cached_misses`, `uncached_opens`, `decoded_bytes`, `touched_bytes`,
        `selected_rows`, `first_touch_verifies`, `pinned_source_hits`,
        `fresh_map_loads`. Taken as a delta around one query,
        `touched_bytes` and `selected_rows` are that query's read
        amplification.

        `segment_cache_stats` reports residency, this reports traffic; the
        pair is what separates "the query path never caches" from "it caches
        and something evicts". Read off `self._store` on every call rather
        than off a handle captured once, because the reopen-on-ENOENT
        net replaces the native handle and a captured one would stay frozen.
        """
        return dict(self._store.open_path_stats())

    def designs(self) -> dict[str, Any]:
        """Which engine designs this handle runs with: `name -> in
        force`. Each ablation switch (`GNOMON_RANGE_VISIBILITY=0`,
        `GNOMON_WINDOW_FIRST=0`, `GNOMON_SEGMENT_PRUNE=0`, `GNOMON_TCSR=scan`,
        `GNOMON_VIDFILTER=ignore`, `GNOMON_EXISTENCE_PROBE=walk`, and
        `GNOMON_SEED_EXPANSION=0`, `GNOMON_ADJ_FILTERED_SCAN=0`) turns one
        design off and keeps every answer; the engine reads them once, when
        the handle opens, so this -- not the current environment -- is what
        a receipt records. All `True` is the shipped engine. One entry is a
        number, not a switch: `match_threads`, the threads the read kernels
        split a large frontier or join over (`GNOMON_MATCH_THREADS`; by default
        the available cores), which changes no answer and no row order."""
        return dict(self._store.designs())

    def reach_earliest(self, src: int, t_a: int, t_b: int, as_of_tt: int,
                       direction: str, offset: int, limit: int) -> dict[str, Any] | None:
        """`temporal_reachability`'s single-label earliest-arrival
        search in the engine -- one planned scan of the window's believed
        rows into a by-source CSR, a label-setting search, and the
        `(arrival, uid)`-ordered page `[offset, offset + limit)` with the
        total. `None` inside an open write batch (the engine's scan does not
        see staged rows; the operator's own path handles that case)."""
        return self._read_op(lambda s: s.reach_earliest(
            int(src), int(t_a), int(t_b), clamp_tt(as_of_tt), direction,
            int(offset), int(limit)))

    def paths_topk(self, src: int, dst: int, t_a: int, t_b: int, as_of_tt: int,
                   k: int, max_hops: int, budget: int,
                   rounds: bool | None = None) -> dict[str, Any] | None:
        """`temporal_paths` in the engine -- the graph read from both
        ends in rounds (or one window read), latest-start pruning, every path
        counted with its last edge by binary search, the first `k` kept as
        they are found. `{"total", "arrival", "hops", "src_id", "dst_id",
        "vt_s", "eid", "rel_type", "expansions", "exceeded", ...}`, the edge
        lists path after path. `None` inside an open write batch, and under
        `GNOMON_TCSR=python`, whose opt-out is the whole traversal path through
        the NumPy index (as for `match_kernel` and `snapshot_kernel`). `rounds`
        forces the reading by rounds on or off (None: `GNOMON_PATHS_ROUNDS`)."""
        if tcsr_backend() == "python":
            return None
        return self._read_op(lambda s: s.temporal_paths(
            int(src), int(dst), int(t_a), int(t_b), clamp_tt(as_of_tt),
            int(k), int(max_hops), int(budget), rounds))

    def diff_snapshots_kernel(self, t1: int, t2: int, as_of_tt: int,
                              limit: int) -> dict[str, Any] | None:
        """`diff_snapshots` without `scope`, in the engine -- two
        point-state plans, rows common to both retired by a merge, the rest
        joined on `(src_id, dst_id, rel_type, disc)` / the dense uid, eids
        only for added and removed rows, strings only for the pages. Content
        candidates come back with both stored texts for the operator to
        compare. `None` inside an open write batch."""
        return self._read_op(lambda s: s.diff_snapshots(
            int(t1), int(t2), clamp_tt(as_of_tt), int(limit)))

    def match_kernel(self, plan_json: str, offset: int, limit: int) -> dict[str, Any] | None:
        """`match` in the engine: execute one compiled plan
        (`ops_match.plan_json`) on binding tables of dense ids, hops through
        the per-segment adjacency index, typed property columns, and return
        the page `[offset, offset + limit)` as `{"rows", "rows_total"}` with
        each row a dict. `None` sends the operator to its reference route."""
        if tcsr_backend() == "python":
            return None             # the opt-out builds no per-segment index
        got = self._read_op(lambda s: s.match_plan(plan_json, int(offset), int(limit)))
        if got is None:
            return None
        cols = got["columns"]
        rows = [{c: (json.loads(v[0]) if isinstance(v, tuple) else v)
                 for c, v in zip(cols, r)} for r in got["rows"]]
        return {"rows": rows, "rows_total": int(got["rows_total"])}

    def match_pairs(self, plan_json: str) -> tuple | None:
        """A weight pattern's plan in the engine, column-wise: `(src, dst,
        w_kind, w_int, w_f64)` arrays (dense ids; 0 null, 1 integer, 2
        float), or None for the rows route."""
        if tcsr_backend() == "python":
            return None
        return self._read_op(lambda s: s.match_plan_pairs(plan_json))

    def snapshot_kernel(self, seed_ids: Sequence[int], t: int, as_of_tt: int, hops: int,
                        rel_types: list[str] | None) -> dict[str, Any] | None:
        """`snapshot_subgraph` from its seeds in the engine: an undirected
        breadth-first search with one adjacency call per level for the whole
        frontier, node validity by identity point reads, and the induced
        edges in `(vt_s, vid)` order. `None` inside an open write batch, and
        under `GNOMON_TCSR=python`, which builds no per-segment index."""
        if tcsr_backend() == "python":
            return None
        return self._read_op(lambda s: s.snapshot_expand(
            [int(i) for i in seed_ids], int(t), clamp_tt(as_of_tt), int(hops),
            list(rel_types) if rel_types else None))

    def snapshot_scan_page(self, seed_ids: Sequence[int], t: int, as_of_tt: int, hops: int,
                           rel_types: list[str] | None, cap: int, offset: int,
                           limit: int) -> dict[str, Any] | None:
        """`ops_snapshot._expand_from_seeds` and the operator's page in one
        engine call; `None` past `cap`, as there."""
        return self._read_op(lambda s: s.snapshot_scan_page(
            [int(i) for i in seed_ids], int(t), clamp_tt(as_of_tt), int(hops), int(cap),
            int(offset), int(limit), list(rel_types) if rel_types is not None else None))

    def snapshot_kernel_page(self, seed_ids: Sequence[int], t: int, as_of_tt: int, hops: int,
                             rel_types: list[str] | None, offset: int, limit: int,
                             node_limit: int) -> dict[str, Any] | None:
        """`snapshot_kernel` as a page: the totals, the nodes
        `[0, node_limit)` by `(hop, uid)` and the edges `[offset, offset +
        limit)` in `(vt_s, vid)` order, built for the page only. `None` where
        `snapshot_kernel` is."""
        if tcsr_backend() == "python":
            return None
        return self._read_op(lambda s: s.snapshot_expand_page(
            [int(i) for i in seed_ids], int(t), clamp_tt(as_of_tt), int(hops),
            int(offset), int(limit), int(node_limit),
            list(rel_types) if rel_types else None))

    def match_last_decline(self) -> str:
        """Why the engine's `match` executor last declined a plan."""
        return str(self._store.match_last_decline())

    def match_label_counts(self) -> dict[str, int]:
        """Node versions written per label (every version: an estimate for
        the `match` compiler's step order), cached per generation."""
        return {str(k): int(v) for k, v in self._read_op(lambda s: s.match_label_counts())}

    def match_rel_ends(self) -> list[tuple[str, int, int, int]]:
        """Per relationship type, its edge versions and the distinct
        identities at its source and target ends (every version: the
        `match` compiler's second-anchor estimates), cached per generation."""
        return [(str(r), int(c), int(s), int(d)) for r, c, s, d in self._read_op(lambda s: s.match_rel_ends())]

    def match_adjacency_rows(self, uids: list[str], direction: str) -> int:
        """The adjacency-index rows of `uids` in `direction` (every type and
        version: an upper bound of a first hop, the `match` compiler's
        second-anchor probe)."""
        return int(self._read_op(lambda s: s.match_adjacency_rows([str(u) for u in uids], direction)))

    def phase_stats(self) -> dict[str, int]:
        """Per-phase read-path timers: `agg_*` for the aggregate path's own
        phases, `sel_*` for the scan select stage, `fold_*` for the partial
        fold. Nanosecond totals, plus a few plain counts.

        Process-global rather than per handle, deliberately: the counters
        beside this one are per handle, and a harness that opens several
        handles and reads the idle one gets zeros and calls them a finding.
        Bracket one query between :meth:`reset_phase_stats` and this call to
        get that query's split; `*_max_ns` is a running maximum and means
        "the slowest worker" only inside such a bracket.

        `sel_busy_ns / (agg_select_wall_ns * width)` and the same ratio for
        the fold are the two stages' parallel efficiency — the quantity a
        size-skewed segment layout costs an equal-count chunking.
        """
        return dict(self._store.phase_stats())

    def reset_phase_stats(self) -> None:
        """Zero the per-phase timers. Instrumentation only."""
        self._store.reset_phase_stats()

    def ingest_phase_stats(self) -> dict[str, int]:
        """Process-wide ingest-path timers (`crates/gnomon-core/src/
        ingest_phase.rs`): nanosecond totals and counts for every engine-side
        step between a batch arriving and it being durable and compacted --
        the PyO3 method bodies, staging, the endpoint-existence probe,
        correction resolution, each seal and commit step, both compactors
        and gc. Context-indexed slots read `<context>.<slot>`.

        Empty when the extension was built with the timers compiled out
        (`build_info()["ingest_phase_timers"]` is then False), or by an engine
        that predates them. Observation only; nothing reads these back."""
        fn = getattr(self._store, "ingest_phase_stats", None)
        return {k: int(v) for k, v in fn().items()} if callable(fn) else {}

    def reset_ingest_phase_stats(self) -> None:
        """Zero the ingest-path timers. Instrumentation only."""
        fn = getattr(self._store, "reset_ingest_phase_stats", None)
        if callable(fn):
            fn()

    def segment_layout(self) -> list[dict[str, Any]]:
        """The current generation's segments with their composite key
        boundaries, straight off the manifest: `lane`, `file`, `rows`,
        `key_lo`, `key_hi`, `vt_min`, `vt_max`. Opens nothing."""
        return [dict(r) for r in self._store.segment_layout()]

    def segment_cache_stats(self) -> tuple[int, int, int | None, int]:
        """`(entries, accounted_bytes, budget, evictions)` of the segment
        cache, read off the live handle for the reason above."""
        return tuple(self._store.segment_cache_stats())

    def postings_index_stats(self, kind: str = "edge") -> dict[str, Any]:
        """The identity postings' byte-budget receipt:
        `entries`, `resident_segments`, `resident_bytes`, `budget`,
        `evictions`, `rebuilds`, `folds`, `rows_folded`, `transient_folds`.

        Mirrors `segment_cache_stats` for the segment cache. `folds` and
        `rows_folded` are what attribute a build's postings cost to a call:
        a whole-store fold shows up as `rows_folded` reaching the store's
        row count on the first point read."""
        return dict(self._store.postings_index_stats(kind))

    def postings_bytes(self) -> int:
        """Resident postings bytes across both row kinds -- the quantity
        `GNOMON_POSTINGS_BYTES` bounds, for a build receipt."""
        return sum(self.postings_index_stats(k)["resident_bytes"]
                   for k in ("edge", "node"))

    def memory_accounting(self) -> dict[str, dict[str, int]]:
        """Bytes held by each long-lived in-process structure of the native
        handle, as `{"current": {...}, "peak": {...}}`.

        Observation only -- no read or write path behaves differently because
        this was called. `peak` is a running maximum over the samples taken
        through *this* accessor, exactly as `postings_index_stats` backs
        `postings_peak_bytes`: the call costs nothing between samples, so the
        caller picks the cadence.

        Read off `self._store` on every call, never a captured handle, for
        the reason above `segment_cache_stats`: reopen-on-ENOENT replaces it.

        The components are documented on the Rust accessor. Two totals
        summarise them: `anonymous_total` is heap the engine
        allocated, and is the only class the segment-cache and postings
        budgets bound; `file_mapped_total` is the store's own segment bytes
        mapped in, which is not charged to any budget and equals the store's
        segment bytes once every page has been touched. Compare the first
        against `RssAnon` and the second against `RssFile + RssShmem` --
        which of those two carries it is a property of the filesystem (tmpfs
        uses `RssShmem`), not of the engine."""
        got = self._store.memory_accounting()
        return {k: dict(v) for k, v in got.items()}

    def set_postings_budget(self, budget: int | None) -> None:
        """Override `GNOMON_POSTINGS_BYTES` for this handle (`None` =
        unbounded), as `set_segment_cache_budget` does for the cache --
        so a test can exercise eviction without mutating the process
        environment, which races other threads."""
        self._store.set_postings_budget(budget)

    def tcsr_stats(self) -> dict[str, Any]:
        """Residency of this handle's per-segment TCSR indexes.

        How many segments are cached, how many were built versus loaded, the
        bytes they hold, and — for the memory model, which compares TCSR memory
        against `4·(|V| + 1 + n)·2` — the `rows_indexed` and `active_vertices`
        that model is computed from, pre-evaluated as `forecast_bytes`.
        `per_segment` carries the same figures per cached segment with each
        direction's dense/sparse election, so a miss can be attributed rather
        than merely observed.

        These are the *default* path's counters, so a zeroed report
        now means one of three things rather than "the flag is unset": no
        traversal has run on this handle yet (the build is lazy), every
        segment was released (`release_tcsr`), or the handle is running the
        `GNOMON_TCSR=python` opt-out, which never touches these indexes.
        `tcsr_backend()` distinguishes the last from the first two, and
        `segments_cached` plus `built`/`loaded` distinguish the first two from
        each other: `built`/`loaded` are monotonic per handle and survive a
        release."""
        return dict(self._store.tcsr_stats())

    def vidfilter_stats(self) -> dict[str, Any]:
        """Residency, provenance and probe receipt for this handle's
        per-segment identity filters.

        `mapped_bytes` belongs under **`file_mapped_total`, never under
        `anonymous_total`**: a filter is a file mapping, which is the
        property that makes it reclaimable where the anonymous postings heap
        it prunes is not, and the memory-class split stops being honest if it is
        charged to the wrong side.

        `probes`/`positives`/`negatives` are the mechanism receipt, and
        `epsilon` (`empty_positives / positives`) measures the false-positive
        rate in situ rather than assuming the construction's nominal one.
        `lazy_built` counts the legacy-store fallback — filters written on
        the way out of a fold the probe had to do anyway — so that "nothing
        relies on the lazy build path" stays a reading rather than a claim.

        A zeroed report means no identity probe has run on this handle, or
        the store has no `idx/vidfilter/` at all; both are the same prior
        behaviour and neither is an error.
        """
        return dict(self._store.vidfilter_stats())

    def set_vidfilter_budget(self, budget: int | None) -> None:
        """Override `GNOMON_VIDFILTER_BYTES` for this handle (`None` =
        unbounded), as `set_postings_budget` does for the postings."""
        self._store.set_vidfilter_budget(budget)

    def release_vid_filters(self) -> None:
        """Drop this handle's mapped filters. The `idx/vidfilter/` files
        stay, so the next identity probe maps them again."""
        self._store.release_vid_filters()

    def release_tcsr(self) -> None:
        """Drop every resident index — the engine's per-segment ones and this
        adapter's shared handle. The `idx/` files stay, so the next traversal
        loads rather than rebuilds. This is the "nothing resident" condition
        of the four-condition residency probe."""
        self._tcsr = None
        self._store.release_tcsr()

    def gc(self, keep_last: int = 2) -> dict[str, int]:
        """Collect superseded generations (`gnomon store gc`).

        Removes manifests older than the last `keep_last` generations, then
        any segment or close-run file no retained manifest references. The
        generation `CURRENT` names and generations pinned by in-process
        readers are never touched, so this is safe to run any time no batch
        is open.
        """
        try:
            return self._store.gc(keep_last)
        except Exception as e:
            raise _translate(e) from None

    def manifest_format(self) -> int:
        """On-disk manifest format: 3 for a store this build wrote, 1 or 2
        for one an earlier engine wrote, which opens read-only. Format 3
        differs from 2 only in what ``manifest_sha`` is — a Merkle root over
        the ordered segment set rather than the sha of the whole document."""
        return int(self._store.manifest_format())

    def upgrade_manifests(self) -> dict[str, Any]:
        """Convert a format-1 or format-2 store to format 3 (`gnomon store
        upgrade-manifests`).

        Republishes the current generation's content as a format-3
        checkpoint and flips `CURRENT`: one manifest written, nothing else
        touched. Idempotent — `upgraded` is False if the store is already
        format 3. The generation advances by one and `manifest_sha` changes
        (the format field is inside its preimage, and from format 3 the
        digest is a different function entirely), so a persisted TCSR stamped
        against the old generation rebuilds on next use, which is exactly
        what that stamp is for.
        """
        try:
            return self._store.upgrade_manifests()
        except Exception as e:
            raise _translate(e) from None

    def resolve_entities(self, query: str, as_of_tt: int = OPEN_END):
        """Engine-side entity resolution.

        Present only on this backend; `ops_snapshot.resolve_entities` uses it
        when available and otherwise falls back to its portable scan.
        """
        return self._read_op(lambda s: s.resolve_entities(query, clamp_tt(as_of_tt)))

    def resolve_entities_page(self, query: str, as_of_tt: int, label: str | None,
                              offset: int, limit: int):
        """`resolve_entities` with the operator's label filter, order and
        pagination done in the engine: `(rows_total, page)`, the
        page being `(uid, score, label, props)` tuples for rows
        `[offset, offset + limit)` of the `(score, uid)` order. Present only
        on this backend; `ops_snapshot.resolve_entities` prefers it.
        """
        return self._read_op(lambda s: s.resolve_entities_page(
            query, clamp_tt(as_of_tt), label, offset, limit))

    def lookup_name(self, query: str, mode: str, as_of_tt: int = OPEN_END,
                    limit: int | None = None):
        """Sorted-name-index lookup: exact or prefix
        match on the casefolded name, plus the O(1) exact-uid path.

        A new API next to `resolve_entities`, not a replacement for it:
        `resolve_entities` answers substring (on both uid and name) and no
        sorted index accelerates `contains`, so it is unchanged. `mode` is
        `"exact"` or `"prefix"`. Present only on this backend.
        """
        return self._read_op(lambda s: s.lookup_name(query, mode, clamp_tt(as_of_tt), limit))

    def diff_belief(self, t1: int, t2: int, t_a: int, t_b: int,
                    limit: int) -> dict[str, Any]:
        """Belief diff: every version whose belief state changed between
        transaction times `t1` and `t2`, inside the valid-time window
        `[t_a, t_b)`. See `StorageAdapter.diff_belief` for the contract and
        `crates/gnomon-core/src/beliefdiff.rs` for the semantics.

        Computed per segment from the header's `tt_s` runs and the segment's
        effective close set, restricted to the window's rows, so the cost is
        `O(segments x log rows)` plus the runs and closes inside the window's
        rows plus the answer, and follows neither the store's size nor the
        size of the segment the window lands in. Present only on this backend.

        `t1` and `t2` are clamped exactly as every other belief-time argument
        is (`clamp_tt`), so `OPEN_END` means "current beliefs" here too and a
        caller sweeping a sequence of steps up to the open end gets the same answer the
        rest of the read path would give it.
        """
        if t1 > t2:
            raise InvalidArgError(
                f"diff_belief requires t1 <= t2, got t1={t1} t2={t2}")
        if t_a > t_b:
            raise InvalidArgError(
                f"diff_belief requires t_a <= t_b, got t_a={t_a} t_b={t_b}")
        return self._read_op(
            lambda s: s.diff_belief(clamp_tt(t1), clamp_tt(t2), t_a, t_b, limit))

    def build_name_indexes(self) -> int:
        """Force-build (and persist) the sorted name index for every node
        segment the current manifest names (`gnomon store build-indexes
        --name`). `lookup_name` builds the same index lazily on first use;
        this is the deployment answer for a latency-sensitive process that
        wants the cost paid up front. Returns the number of segments
        actually (re)built — already-fresh segments are skipped.
        """
        try:
            return int(self._store.build_name_indexes())
        except Exception as e:
            raise _translate(e) from None

    def verify(self, mode: str = "fast") -> dict[str, Any]:
        """Check this store's integrity. Read-only, in both modes.

        Corruption has to be *detected* before bad data reaches a query — the
        engine's durability objective is that an inconsistent generation is
        never silently exposed, which is only true if something checks.

        `mode="fast"` is the engine's file walk: every segment, close run,
        dictionary and manifest this generation names, checksummed and
        cross-checked against what the manifest claims. It is bounded by the
        bytes on disk and is what `gnomon store verify` has always run.

        `mode="full"` adds everything that spans files or lives outside the
        engine's own directory:

        * the engine's own extra passes — the manifest parent chain across
          every retained generation, dictionary-code reference validity, and
          the bitemporal row invariants (`integrity.rs`);
        * the **event log**: per-record framing, tt monotonicity, and the
          rolling chain over the applied prefix against the `(offset, chain)`
          cursor this generation recorded. The store is a deterministic
          materialization of that log, so a log the store cannot account for
          is a defect in the store even when every segment checksums;
        * the **persisted TCSR permutation**, when one exists: its stamp, its
          shape against the live scan, and a content spot-check that rebuilds
          the index and compares;
        * the **artifact registry**: record digests, the per-name generation
          chain, and the blobs those records point at.

        **Nothing is repaired.** A torn event-log tail is a finding, not a
        trim; a stale index is a finding, not a rebuild. That is what makes
        this usable as an oracle: a checker that quietly fixed what it found
        would make every corruption sweep a tautology. It is also why a
        caller who wants the truth should open the store read-only —
        `Store(..., read_only=True)` — since a writer handle runs crash
        recovery on open and will have trimmed a torn tail before verify is
        ever called.

        The report is the engine's, plus `mode`, `store`, and a `findings`
        list of `{layer, kind, path, generation, detail, severity}` covering
        every layer. `problems` is the prose of the `error` findings and
        `healthy` is whether there are none; `advisory` findings are reported
        and do not make a store unhealthy.
        """
        if mode not in ("fast", "full"):
            raise InvalidArgError(f"verify mode must be 'fast' or 'full', got {mode!r}")
        try:
            report = self._store.verify(full=(mode == "full"))
        except Exception as e:
            raise _translate(e) from None
        findings: list[dict[str, Any]] = list(report.get("findings") or ())
        if mode == "full":
            findings += self._verify_eventlog()
            # The index check rebuilds the index from a live scan, so it can
            # only run over segments the engine has just certified. Against a
            # damaged one it would raise on the scan and take the whole
            # report down with it, hiding the findings that actually matter.
            findings += self._verify_tcsr(
                scan_is_trustworthy=not any(f["severity"] == "error" for f in findings))
            findings += self._verify_registry()
        report["findings"] = findings
        report["problems"] = [f["detail"] for f in findings if f["severity"] == "error"]
        report["healthy"] = not report["problems"]
        report["mode"] = mode
        report["store"] = str(self.path)
        return report

    # --- the full-mode layers outside the engine --------------------------- #
    #
    # The engine directory is `<store>/native`; the event log and the artifact
    # registry are its siblings, one level up. An adapter constructed directly
    # on a bare directory (as the engine's own unit tests do) has neither, and
    # their absence is not a defect — only a store that *claims* an applied
    # log prefix and cannot produce the log is.

    def _finding(self, layer: str, kind: str, path: str, detail: str,
                 severity: str = "error") -> dict[str, Any]:
        return {"layer": layer, "kind": kind, "path": path,
                "generation": self.generation, "detail": detail,
                "severity": severity}

    def _verify_eventlog(self) -> list[dict[str, Any]]:
        from gnomon.storage.eventlog import EventLog

        rel = "eventlog.jsonl"
        path = self.path.parent / rel
        offset, chain = self.event_cursor()
        if not path.exists():
            if offset:
                return [self._finding(
                    "eventlog", "log-missing", rel,
                    f"generation {self.generation} records an applied prefix of "
                    f"{offset} bytes, but there is no event log at {path}")]
            return []
        try:
            # `EventLog(path)` creates the file when it is absent; the guard
            # above is what keeps this constructor read-only.
            log = EventLog(path)
        except Exception as e:
            return [self._finding("eventlog", "log-unreadable", rel,
                                  f"the event log cannot be opened: {e}")]
        try:
            defects = log.check_chain(offset, chain or None)
        except OSError as e:
            return [self._finding("eventlog", "log-unreadable", rel,
                                  f"the event log cannot be read: {e}")]
        return [self._finding("eventlog", d["kind"], rel, d["detail"])
                for d in defects]

    def _verify_registry(self) -> list[dict[str, Any]]:
        from gnomon.artifact import registry as artifact_registry

        rel = artifact_registry.FILE_NAME
        try:
            defects = artifact_registry.verify(self.path.parent)
        except OSError as e:
            return [self._finding("registry", "registry-unreadable", rel,
                                  f"the artifact registry cannot be read: {e}")]
        layer_of = {"blob-missing": "blob", "blob-digest-mismatch": "blob"}
        return [self._finding(layer_of.get(d["kind"], "registry"), d["kind"], rel,
                              d["detail"])
                for d in defects]

    #: Vertices the TCSR content spot-check compares in full. The permutation
    #: is checked whole through its offsets (cheap, exact); this samples the
    #: gathered neighbour arrays, which is where a corrupt `row` column that
    #: still had the right shape would hide.
    TCSR_SAMPLE = 32

    def _verify_tcsr(self, scan_is_trustworthy: bool = True) -> list[dict[str, Any]]:
        import numpy as np

        from gnomon.storage.tcsr import (PERM_FORMAT, TemporalCSR,
                                       load_permutation)

        rel = "index/tcsr.npz"
        path = self.path / "index" / "tcsr.npz"
        if not path.exists():
            return []
        if not scan_is_trustworthy:
            return [self._finding(
                "tcsr", "tcsr-not-checked", rel,
                "the persisted TCSR permutation was not checked: it can only be "
                "compared against a rebuild from the live rows, and this store has "
                "findings against the rows themselves",
                severity="advisory")]
        gen, sha = self._store.generation(), self._store.manifest_sha()
        try:
            with np.load(path) as z:
                stamp = (int(z["format"]), int(z["generation"]), str(z["manifest_sha"]))
        except Exception as e:
            return [self._finding(
                "tcsr", "tcsr-unreadable", rel,
                f"the persisted TCSR permutation exists but cannot be read: {e}")]
        if stamp != (PERM_FORMAT, int(gen), sha):
            # Not a defect in the store. A permutation is meaningless against
            # rows it was not computed from, and `load_permutation` already
            # ignores a mismatched stamp and rebuilds — so every store that
            # has written since its last index build would otherwise report
            # as corrupt, which would make this useless as a sweep oracle.
            return [self._finding(
                "tcsr", "tcsr-stale", rel,
                f"the persisted TCSR permutation is stamped "
                f"(format {stamp[0]}, generation {stamp[1]}, manifest {stamp[2]}) "
                f"and this store is at (format {PERM_FORMAT}, generation {gen}, "
                f"manifest {sha}); it will be ignored and rebuilt",
                severity="advisory")]

        try:
            cols = self.edges_columnar(columns=self.TCSR_COLS)
            n = self.num_entities()
        except Exception as e:  # a scan the engine's own walk did not condemn
            return [self._finding(
                "tcsr", "tcsr-not-checked", rel,
                f"the persisted TCSR permutation could not be compared against a "
                f"rebuild: the base scan failed with {e}",
                severity="advisory")]
        loaded = load_permutation(path, cols, n, gen, sha)
        if loaded is None:
            return [self._finding(
                "tcsr", "tcsr-shape", rel,
                "the persisted TCSR permutation carries this generation's stamp but "
                "does not fit the rows it names")]
        built = TemporalCSR.build(cols, n)
        for direction in ("out", "in"):
            a = loaded.out if direction == "out" else loaded.inn
            b = built.out if direction == "out" else built.inn
            if not np.array_equal(a.offsets, b.offsets):
                return [self._finding(
                    "tcsr", "tcsr-content-mismatch", rel,
                    f"the persisted TCSR permutation's {direction} offsets do not "
                    f"match the index rebuilt from this generation's rows")]
        # Content: rebuild a slice per sampled vertex and compare what the
        # traversal would actually read.
        degrees = np.diff(built.out.offsets) + np.diff(built.inn.offsets)
        sample = np.nonzero(degrees)[0][: self.TCSR_SAMPLE]
        for u in sample:
            for direction in ("out", "in"):
                got = loaded.neighbors(int(u), direction)
                want = built.neighbors(int(u), direction)
                if any(not np.array_equal(g, w) for g, w in zip(got, want)):
                    return [self._finding(
                        "tcsr", "tcsr-content-mismatch", rel,
                        f"the persisted TCSR permutation disagrees with the index "
                        f"rebuilt from this generation's rows at vertex {int(u)} "
                        f"({direction})")]
        return []

    def needs_compaction(self) -> bool:
        """Is any key-range cluster eligible for an incremental rewrite?

        This is exactly the predicate `compact_incremental` plans
        against, so a driver can loop on it. It used to be the deliberately
        coarse `any close run, or more than four segments` hint, which
        over-triggered by design and which no driver could use — see
        `needs_whole_store_compaction` for that one, kept as the A/B
        control.
        """
        return bool(self._store.needs_compaction())

    def needs_whole_store_compaction(self) -> bool:
        """The earlier hint, paired with `compact()` (the control configuration)."""
        return bool(self._store.needs_whole_store_compaction())

    def compact_incremental(self, budget_rows: int | None = None,
                            build_indexes: bool = False) -> dict[str, Any]:
        """Rewrite only the eligible key-range clusters.

        The whole-store `compact()` above reads the entire logical store
        back and reseals it, which is a per-call cost proportional to the
        store rather than to what changed. This rewrites one size
        tier of one cluster at a time; untouched segments keep their files,
        their `body_sha`, and therefore their `idx/tcsr` and `idx/name`
        indexes.

        `budget_rows` caps one call; `None` takes every eligible cluster.
        The logical store, and so every digest, is unchanged either way.
        `build_indexes` mirrors `compact()`'s flag — both kinds, off by
        default — and the mirror is kept literal rather than approximate:
        this call's whole point is that untouched segments keep their
        indexes, so the rebuild is cheap here by construction (an index
        already on disk is loaded, not rebuilt) and there is no reason for
        the two compactors to disagree about which kinds they cover.
        """
        try:
            report = self._store.compact_incremental(budget_rows)
        except Exception as e:
            raise _translate(e) from None
        if build_indexes:
            self.build_indexes(name=True, vidfilter=True)
        return report

    def needs_settle(self) -> bool:
        """Advisory: is any lane above its byte floor?

        Opens no segment and touches no file — row counts and key ranges from
        the manifest are everything the plan needs. It schedules nothing; a
        driver polls it at quiescence and decides for itself, the same
        contract `needs_compaction` carries.
        """
        return bool(self._store.needs_settle())

    def settle(self, budget_bytes: int | None = None,
               build_indexes: bool = True) -> dict[str, Any]:
        """Merge every live segment of every lane above its byte floor.

        A major compaction, invoked explicitly at quiescence: one
        bounded-fan-in k-way merge per lane over every live segment, rewriting
        each row exactly once and ending at the store's byte floor. The tiered
        build path buys its write saving by leaving partially-filled tier
        windows unmerged, and those same unmerged windows are the segments a
        whole-extent aggregate must open; settle pays that residue off once,
        at a moment nobody is waiting.

        `budget_bytes` overrides `GNOMON_SETTLE_BYTES` and bounds the input
        bytes one merge group spans, so peak memory is flat in store size.
        The logical store is unchanged and transaction time does not advance,
        so `store_digest` and `content_digest` are invariant.

        `build_indexes` defaults to **True**, against `compact()`'s False, and
        the divergence is intentional: settle orphans every per-segment
        TCSR and name index by construction, because a merged segment has a
        different `body_sha`. Leaving the rebuild to the first queries would
        make them pay a full-file sha256 plus an index build per segment —
        contaminating exactly the latency measurement settle exists to
        improve. That matters more now that the Rust TCSR is the default
        traversal index: the first query after a settle is now on the default
        path, not behind a flag.

        As with `compact`, the rebuild happens here rather than inside the
        engine: it is exactly `build_indexes(name=True)` run afterwards — one
        call, the same load-or-build the lazy first-use path takes, so a
        settled store's indexes are the same bytes a first traversal would
        have produced. It cannot fail a settle that already succeeded, and it
        leaves the engine's settle path with one job.

        Settle does not call gc. `reclaimable_bytes` in the report is what a
        following `gnomon store gc --keep 2` frees; until it runs the store is
        transiently about twice its settled size on disk.
        """
        try:
            report = dict(self._store.settle(budget_bytes))
        except Exception as e:
            raise _translate(e) from None
        report["indexes_rebuilt"] = (
            self.build_indexes(name=True, vidfilter=True)
            if build_indexes else None)
        return report

    def content_digest_streaming(
        self, budget_bytes: int | None = None
    ) -> dict[str, Any]:
        """`content_digest()` in bounded memory, computed inside the engine.

        The posthoc pass materialises every logical row into sorted Python
        lists — the ~80 GB digest transient measured at 100M,
        and an OOM at 300M. This one fills a byte-bounded buffer of canonical
        row bytes, spills it sorted, and finishes with a bounded-fan-in k-way
        merge, so peak memory follows `budget_bytes` and the fan-in rather
        than the row count.

        Byte-identical to `content_digest()` for the same store — the point of
        the exercise, and what `tests/test_content_digest_streaming.py` proves
        against `scripts/build_synth_store.py::content_digest` itself.

        `budget_bytes` overrides `GNOMON_DIGEST_BYTES`; `0` means unbounded.
        Returns the engine's whole report — `digest` plus the counters the
        memory claim is scored on (`rows_held_max`, `runs_spilled`,
        `spill_bytes`, `merge_rounds`, `max_fan_in`, `ambiguous_ties`).
        """
        return dict(self._read_op(lambda s: s.content_digest(budget_bytes)))

    def content_digest(self) -> str:
        """The ABC's whole-population pass, kept as the reference.

        Deliberately **not** overridden to call the streaming path: the two
        must remain independently computable or the equality test proves
        nothing.
        """
        return super().content_digest()

    @property
    def generation(self) -> int:
        """Current manifest generation — the store's publication counter."""
        return int(self._store.generation())


# --------------------------------------------------------------------- #
# The Rust per-segment TCSR, the default traversal index           #
# --------------------------------------------------------------------- #
#
# The Rust index shipped beside the NumPy one, off by
# default in 0.8.x, and an A/B between the two flag values was run as the
# evidence that flips the default. That A/B ran at 1M and 10M, tiered and
# settled, across compaction and suffix recovery, and the default is now
# `rust`; `GNOMON_TCSR=python` is the opt-out.
#
# `gnomon/storage/tcsr.py` is still never touched — it remains the portable
# backends' implementation (`StorageAdapter.tcsr`, DuckDB) and the
# opt-out's implementation here, so it cannot bit-rot behind the flag.

#: Values of `GNOMON_TCSR`. Anything unrecognised is treated as unset, for the
#: same reason `GNOMON_SCAN_THREADS` is: a measurement knob must never make a
#: read fail.
#:
#: `scan` is the ablation of the per-segment index: the adapter takes
#: the `rust` path, and the engine, which reads the same variable once when
#: the handle opens (`designs()["adjacency_index"]`), finds each traversal
#: call's adjacency by scanning the segments' endpoint columns instead of
#: loading an index. Answers are identical; only the work differs.
TCSR_BACKENDS = ("python", "rust", "scan")

#: What an unset (or unrecognised) `GNOMON_TCSR` means. `rust` now; it was
#: `python` for the whole 0.8.x shadow period. One name, so a test or a script
#: can assert the default rather than hard-coding the winning string.
TCSR_DEFAULT_BACKEND = "rust"

#: Rows the shim accumulator may hold before `tcsr()` hands out a fresh one.
#:
#: The accumulator grows only with rows a traversal actually *touched*, which
#: one operator call bounds by `MAX_EXPANSIONS` — but the shared handle
#: outlives any one call, so a long-running process would otherwise grow
#: it without bound. The swap happens at `tcsr()` entry, which is an operator
#: boundary: whoever already holds a `(csr, cols)` pair keeps a consistent one
#: (the old object lives as long as it is referenced) and only the next
#: operator gets the fresh pair. 1M rows is ~48 MB across the six `int64`
#: columns — under the ~112 MB of gathered columns the Python index holds for
#: a 1M-row store *unconditionally*, and reached only by a process that
#: has traversed a million edges without writing.
SHIM_ROW_BUDGET = 1_000_000


def tcsr_backend() -> str:
    """`"python"` or `"scan"` only when `GNOMON_TCSR` says so, else `"rust"`
    (the default). Read per build, not per process, so one process
    can run both sides of the A/B against one store. `"scan"` routes exactly
    as `"rust"` does here; whether the engine then uses its index is fixed
    when the handle opens and reported by `NativeAdapter.designs()`.

    Every consumer that wants to *record* which index answered must call this
    rather than read the environment variable: an unset variable is the
    default, not `python`, and a receipt that says otherwise is wrong."""
    v = (os.environ.get("GNOMON_TCSR") or "").strip().lower()
    return v if v in TCSR_BACKENDS else TCSR_DEFAULT_BACKEND


class _ShimCols(Mapping):
    """The columnar arrays the `row` values of `NativeTCSR.neighbors` index.

    The Python TCSR's `row` is a position in a whole-store `edges_columnar`
    scan, and its callers reach back through it for `vt_s`, the endpoint ids
    and the physical addresses. The Rust index deliberately never materializes
    that scan — that is where the 18 % resident tax went — so the shim
    keeps instead an **append-only accumulator of the rows a traversal
    actually touched**, and hands out positions in it.

    The keys are exactly `NativeAdapter.TCSR_COLS`, so `"eid" in cols` is
    False and `temporal_paths` takes its declared identity-fetch path through
    `edge_idents_at` (`ops_paths.py`), and `"vid" in cols` is False so
    `Adjacency.step`'s `direction="both"` merge breaks ties on `row`, exactly
    as it does over the Python index.

    A physical row touched by two different `neighbors` calls gets two
    positions. That is harmless everywhere it can happen: every consumer reads
    values *through* the position and compares values, never positions —
    `temporal_paths` sorts paths by `(arrival, hops, (vt_s, eid))` and
    `edge_idents_at` is keyed by the address, not by the position. The
    per-`(u, direction, t_max)` cache in `NativeTCSR` means a DFS revisiting a
    vertex does not even pay it.

    Chunked, not list-appended: each `neighbors_of` result is kept as the
    NumPy arrays the engine already returned, and a column is concatenated
    once, lazily, the first time a caller indexes it. `temporal_paths` reads
    `cols["vt_s"][r]` once per row of every found path, so materializing on
    each read would be quadratic, and a Python list of boxed ints would cost
    several times the bytes of the `int64` array it stands in for.
    """

    __slots__ = ("_chunks", "_flat", "_n")

    def __init__(self) -> None:
        self._chunks: dict[str, list[np.ndarray]] = {
            c: [] for c in NativeAdapter.TCSR_COLS}
        self._flat: dict[str, np.ndarray] = {}
        self._n = 0

    def _append(self, got: dict[str, np.ndarray], u: int, direction: str) -> int:
        """Record one `neighbors_of` result; return the first position it got.

        The engine returns the *other* endpoint (`nbr`); which side that is
        depends on the direction, and both endpoint columns have to be here
        because `temporal_paths` reads `src_id` and `dst_id` per trail row.
        """
        base = self._n
        n = len(got["nbr"])
        if n == 0:
            return base
        mine = np.full(n, u, dtype=np.int64)
        if direction == "out":
            src, dst = mine, got["nbr"]
        elif direction == "in":
            src, dst = got["nbr"], mine
        else:  # pragma: no cover - `neighbors` refuses it before we get here
            raise InvalidArgError(
                f"the shim accumulator cannot recover both endpoints for "
                f"direction {direction!r}"
            )
        self._chunks["src_id"].append(src)
        self._chunks["dst_id"].append(dst)
        for c in ("vt_s", "vt_e", "seg_id", "seg_row"):
            self._chunks[c].append(got[c])
        self._flat.clear()
        self._n += n
        return base

    def __getitem__(self, key: str) -> np.ndarray:
        hit = self._flat.get(key)
        if hit is None:
            chunks = self._chunks[key]
            hit = np.concatenate(chunks) if chunks else np.empty(0, dtype=np.int64)
            hit = hit.astype(np.int64, copy=False)
            self._flat[key] = hit
        return hit

    def __contains__(self, key: object) -> bool:
        return key in self._chunks

    def __iter__(self):
        return iter(self._chunks)

    def __len__(self) -> int:
        return len(self._chunks)

    def rows_accumulated(self) -> int:
        """Rows this traversal has pulled across the boundary. The shim's own
        residency, which a probe must count alongside the index's."""
        return self._n

    def resident_bytes(self) -> int:
        """Bytes the accumulator holds, counting a materialized column twice —
        once as chunks and once flattened — because that is what it costs."""
        return int(
            sum(a.nbytes for chunks in self._chunks.values() for a in chunks)
            + sum(a.nbytes for a in self._flat.values())
        )


class NativeTCSR:
    """`TemporalCSR.neighbors` over the engine's per-segment index.

    Only the one method matters: `neighbors(u, direction, t_max)` is the
    entire primitive `temporal_paths`' DFS (`ops_paths.py`) consumes, and
    it returns the same four arrays in the same `(vt_s, vid)` order.

    `t_max` is the engine's `vt_max` — `vt_s < t_max`, the same half-open cut
    `TemporalCSR.neighbors` makes with its `searchsorted`. `as_of_tt` is
    current belief, because this object only ever stands in for the *shared*
    current-belief index: `ops_paths._csr_for` and `AdjacencyCache._build`
    both take a per-call windowed `TemporalCSR.build` for anything else, and
    that dispatch is left unchanged here.
    """

    __slots__ = ("_adapter", "_cols", "_cache")

    def __init__(self, adapter: "NativeAdapter", cols: _ShimCols) -> None:
        self._adapter = adapter
        self._cols = cols
        #: per `(u, direction, t_max)`, so a DFS that revisits a vertex pays
        #: one boundary crossing for it, not one per visit
        self._cache: dict[tuple[int, str, int | None], tuple[np.ndarray, ...]] = {}

    def neighbors(self, u: int, direction: str = "out",
                  t_max: int | None = None) -> tuple[np.ndarray, ...]:
        if direction not in ("out", "in"):
            raise InvalidArgError(
                f"direction must be 'out' or 'in', got {direction!r}; the "
                f"engine's merged 'both' slice is reached through "
                f"`store.neighbors_of`, not through this shim, because the "
                f"accumulator cannot recover both endpoints from a merged row"
            )
        key = (int(u), direction, None if t_max is None else int(t_max))
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        got = self._adapter._read_op(lambda s: s.neighbors_of(
            int(u), direction,
            as_of_tt=clamp_tt(OPEN_END),
            vt_min=None,
            vt_max=None if t_max is None else int(t_max),
            rel_types=None,
        ))
        base = self._cols._append(got, int(u), direction)
        rows = np.arange(base, base + len(got["nbr"]), dtype=np.int64)
        out = (got["nbr"], got["vt_s"], got["vt_e"], rows)
        self._cache[key] = out
        return out

    def stats(self) -> dict[str, Any]:
        """Residency of this index: the engine's own per-segment counters plus
        the shim's accumulator, both in `resident_bytes`."""
        got = dict(self._adapter._store.tcsr_stats())
        got["shim_rows"] = self._cols.rows_accumulated()
        got["shim_bytes"] = self._cols.resident_bytes()
        got["shim_slices_cached"] = len(self._cache)
        return got
