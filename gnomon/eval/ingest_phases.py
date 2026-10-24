"""Per-phase ingest-time accounting for a whole build.

A build driver (`scripts/build_synth_store.py`)
creates one :class:`PhaseClock`, starts it where its own ingest timer starts,
brackets its own steps with :meth:`PhaseClock.section`, and calls
:func:`instrument` on the open store. At the end :func:`report` turns the
clock and the engine's own timers into ``ingest_phase_totals``: seconds and
share of ingest time per phase, a coarser grouping, and the residual that no
phase claims.

**Two timer sources, one tree.** Python-side time is measured here with an
exclusive-time stack: every bracketed call charges its elapsed time to its own
name minus whatever its bracketed children took, so the exclusive times of
all names plus the root's own exclusive time (the *residual*) add up to the
root's elapsed time exactly. Every call into the native engine is bracketed
too (``pyo3.<method>``), and the engine reports, per method, how long its own
body ran and how that body divides into steps
(`crates/gnomon-core/src/ingest_phase.rs`). :func:`compose` replaces each
``pyo3.<method>`` bracket by *boundary* (the bracket minus the engine body:
argument conversion into Rust types, result conversion back, call dispatch)
plus the engine's own steps. The decomposition therefore never double counts:
a step is either Python time outside every engine call or a slice of one
engine call's body.

**Observation only.** :func:`instrument` shadows bound methods on *this*
store's instances and a few module-level names with wrappers that call the
original unchanged, and :func:`uninstrument` removes them. Nothing about what
is written, in what order, or how, changes; the content digest of a build is
the same with and without it (the test suite checks this).

**Two per-row splits are estimates, and are labelled as such.** Python's
identity hashing (``edge_eid``/``_vid``, sha256 per row) runs inside the bulk
row loop, where a timer per row would cost more than it measures; its share
is estimated from a calibration run after ingest (:func:`calibrate_hashing`).
The engine's split of staging into dictionary lookup vs row build is sampled
(one row in 64 timed in two halves).
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from typing import Any, Callable, Iterable

_now = time.perf_counter_ns


# --------------------------------------------------------------------- #
# the clock                                                              #
# --------------------------------------------------------------------- #

class _Section:
    __slots__ = ("_clock", "_name")

    def __init__(self, clock: "PhaseClock", name: str) -> None:
        self._clock = clock
        self._name = name

    def __enter__(self) -> None:
        self._clock.push(self._name)

    def __exit__(self, *exc: Any) -> None:
        self._clock.pop()


class _NullSection:
    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: Any) -> None:
        return None


_NULL = _NullSection()


class PhaseClock:
    """Exclusive-time accounting over a stack of named sections.

    ``excl[name]`` is the time spent in `name` and not in any section opened
    inside it; the root section's exclusive time is the residual.
    """

    ROOT = "unattributed"

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.excl: dict[str, int] = defaultdict(int)
        self.calls: dict[str, int] = defaultdict(int)
        self._stack: list[list[Any]] = []
        self.t_start: int | None = None
        self.t_stop: int | None = None
        self._marks: dict[str, tuple[int, dict[str, int], dict[str, int]]] = {}
        self.samples: list[dict[str, Any]] = []

    # -- stack -------------------------------------------------------- #

    def push(self, name: str) -> None:
        self._stack.append([name, _now(), 0])

    def pop(self, now: int | None = None) -> None:
        name, start, child = self._stack.pop()
        el = (now if now is not None else _now()) - start
        self.excl[name] += el - child
        self.calls[name] += 1
        if self._stack:
            self._stack[-1][2] += el

    def section(self, name: str) -> Any:
        return _Section(self, name) if self.enabled else _NULL

    def wrap(self, fn: Callable[..., Any], name: str) -> Callable[..., Any]:
        push, pop = self.push, self.pop

        def timed(*a: Any, **k: Any) -> Any:
            push(name)
            try:
                return fn(*a, **k)
            finally:
                pop()

        timed.__wrapped__ = fn  # type: ignore[attr-defined]
        return timed

    # -- root ---------------------------------------------------------- #

    def start(self) -> None:
        if not self.enabled:
            return
        self.t_start = _now()
        self._stack = [[self.ROOT, self.t_start, 0]]

    def stop(self) -> None:
        """Close the root: everything still open is closed with it."""
        if not self.enabled or self.t_stop is not None:
            return
        now = _now()
        while self._stack:
            self.pop(now)
        self.t_stop = now

    def elapsed_ns(self) -> int:
        if self.t_start is None:
            return 0
        return (self.t_stop or _now()) - self.t_start

    def snapshot_excl(self) -> dict[str, int]:
        """Exclusive times as of now, with sections still open charged for
        their elapsed-minus-children so far (the root included)."""
        out = dict(self.excl)
        now = _now()
        for name, start, child in self._stack:
            out[name] = out.get(name, 0) + (now - start - child)
        # each open frame's elapsed also counts as its parent's child: undo
        # the parent's charge for time already charged to the open child
        for i in range(1, len(self._stack)):
            name, start, _ = self._stack[i]
            parent = self._stack[i - 1][0]
            out[parent] -= now - start
        return out

    def mark(self, label: str, engine: dict[str, int]) -> None:
        """Remember the state at a window boundary (e.g. the start of the
        end-of-build finalisation) so the report can split by window."""
        if self.enabled:
            self._marks[label] = (self.elapsed_ns(), self.snapshot_excl(), dict(engine))


# --------------------------------------------------------------------- #
# instrumenting a store                                                  #
# --------------------------------------------------------------------- #

class _TimedNative:
    """Wraps the native handle: every method call is a ``pyo3.<name>``
    section. Attribute reads that are not calls pass straight through."""

    def __init__(self, inner: Any, clock: PhaseClock) -> None:
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_clock", clock)
        object.__setattr__(self, "_cache", {})

    def __getattr__(self, name: str) -> Any:
        cache = object.__getattribute__(self, "_cache")
        got = cache.get(name)
        if got is not None:
            return got
        attr = getattr(object.__getattribute__(self, "_inner"), name)
        if not callable(attr):
            return attr
        wrapped = object.__getattribute__(self, "_clock").wrap(attr, "pyo3." + name)
        cache[name] = wrapped
        return wrapped


class _TimedOs:
    """`os`, with `fsync` bracketed — installed as the event log module's
    `os` so the write-ahead fsync is its own phase."""

    def __init__(self, clock: PhaseClock) -> None:
        self._fsync = clock.wrap(os.fsync, "eventlog.fsync")

    def __getattr__(self, name: str) -> Any:
        if name == "fsync":
            return self._fsync
        return getattr(os, name)


#: (instance attribute, section) pairs on the Store.
_STORE_METHODS = (
    ("ingest_events", "python.store_write"),
    ("_write_locked", "python.store_write"),
    ("correct", "python.store_write"),
    ("retract", "python.store_write"),
    ("assert_node", "python.store_write"),
)

#: ... and on the storage adapter.
_ADAPTER_METHODS = (
    ("apply_ops", "python.apply_dispatch"),
    ("_ingest_events", "python.row_build"),
    ("_correct", "python.correction_logic"),
    ("_retract", "python.correction_logic"),
    ("_assert_node", "python.correction_logic"),
    ("_assert_edge", "python.correction_logic"),
    ("_supersede_nodes", "python.correction_logic"),
    ("_supersede_edges", "python.correction_logic"),
    ("insert_edge_versions", "python.batch_conversion"),
    ("insert_node_versions", "python.batch_conversion"),
    ("ensure_entities", "python.batch_conversion"),
    ("nodes_with_believed_versions", "python.batch_conversion"),
    ("close_edge_versions", "python.batch_conversion"),
    ("close_node_versions", "python.batch_conversion"),
    ("retire_edge_versions", "python.batch_conversion"),
    ("retire_node_versions", "python.batch_conversion"),
    ("believed_edge_versions", "python.batch_conversion"),
    ("believed_node_versions", "python.batch_conversion"),
    ("believed_edge_versions_overlapping", "python.batch_conversion"),
    ("believed_node_versions_overlapping", "python.batch_conversion"),
    ("note_event_cursor", "python.batch_conversion"),
    ("commit", "python.batch_conversion"),
)


def instrument(store: Any, clock: PhaseClock) -> Callable[[], None]:
    """Bracket this store's write path. Returns the function that undoes it.

    Only instance attributes and three module-level names are touched, each
    replaced by a wrapper that calls the original with the same arguments.
    """
    if not clock.enabled:
        return lambda: None
    undo: list[Callable[[], None]] = []

    def on_instance(obj: Any, attr: str, section: str) -> None:
        if not hasattr(obj, attr):
            return
        setattr(obj, attr, clock.wrap(getattr(obj, attr), section))
        undo.append(lambda o=obj, a=attr: o.__dict__.pop(a, None))

    def on_module(modname: str, attr: str, value: Any) -> None:
        mod = sys.modules.get(modname)
        if mod is None or not hasattr(mod, attr):
            return
        orig = getattr(mod, attr)
        setattr(mod, attr, value)
        undo.append(lambda m=mod, a=attr, o=orig: setattr(m, a, o))

    for attr, section in _STORE_METHODS:
        on_instance(store, attr, section)
    adapter = store.adapter
    for attr, section in _ADAPTER_METHODS:
        on_instance(adapter, attr, section)

    evmod = sys.modules.get("gnomon.storage.eventlog")
    if evmod is not None:
        on_module("gnomon.storage.eventlog", "canonical_json",
                  clock.wrap(evmod.canonical_json, "eventlog.serialize"))
        on_module("gnomon.storage.eventlog", "sha256_hex",
                  clock.wrap(evmod.sha256_hex, "eventlog.serialize"))
        on_module("gnomon.storage.eventlog", "os", _TimedOs(clock))
    # the event log's own append, so its non-serialise, non-fsync remainder
    # (open, write, flush, tell) is its own phase
    on_instance(store.eventlog, "append", "eventlog.write")
    stmod = sys.modules.get("gnomon.store")
    if stmod is not None and hasattr(stmod, "extend_chain"):
        on_module("gnomon.store", "extend_chain",
                  clock.wrap(stmod.extend_chain, "eventlog.chain_hash"))

    native = getattr(adapter, "_store", None)
    if native is not None:
        adapter._store = _TimedNative(native, clock)

        def restore(a: Any = adapter, n: Any = native) -> None:
            if isinstance(a.__dict__.get("_store"), _TimedNative):
                a._store = n
        undo.append(restore)

    def uninstrument() -> None:
        while undo:
            undo.pop()()
    return uninstrument


def engine_stats(store: Any) -> dict[str, int]:
    """The engine's ingest-phase totals, read *around* the timing wrapper so
    that reading them is not itself a timed call. Empty when the extension
    has the timers compiled out, or predates them."""
    adapter = getattr(store, "adapter", None)
    native = getattr(adapter, "_store", None)
    if isinstance(native, _TimedNative):
        native = object.__getattribute__(native, "_inner")
    fn = getattr(native, "ingest_phase_stats", None)
    if not callable(fn):
        return {}
    try:
        return {k: int(v) for k, v in fn().items()}
    except Exception:  # noqa: BLE001 -- a receipt must never fail a build
        return {}


def reset_engine_stats(store: Any) -> None:
    adapter = getattr(store, "adapter", None)
    native = getattr(adapter, "_store", None)
    if isinstance(native, _TimedNative):
        native = object.__getattribute__(native, "_inner")
    fn = getattr(native, "reset_ingest_phase_stats", None)
    if callable(fn):
        fn()


# --------------------------------------------------------------------- #
# composing the two sources                                              #
# --------------------------------------------------------------------- #

#: Which engine body slot each bracketed PyO3 method is measured against.
_BODY = {
    "begin": "py_begin_ns",
    "stage_edges": "py_stage_edges_ns",
    "stage_nodes": "py_stage_nodes_ns",
    "ensure_entities": "py_ensure_entities_ns",
    "believed_any": "py_believed_any_ns",
    "believed": "py_believed_ns",
    "believed_window": "py_believed_window_ns",
    "stage_closes": "py_stage_closes_ns",
    "stage_retires": "py_stage_retires_ns",
    "commit": "py_commit_ns",
    "needs_compaction": "py_needs_compaction_ns",
    "compact_incremental": "py_compact_incremental_ns",
    "compact": "py_compact_ns",
    "gc": "py_gc_ns",
    "stage_ingest_events_raw": "py_ingest_raw_ns",
    "apply_ops_raw": "py_apply_raw_ns",
}

_WRITE_SLOTS = ("column_fill", "encode", "string_heap", "header_checksum",
                "file_write", "fsync", "rename")


def compose(py: dict[str, int], r: dict[str, int],
            hashing_ns: int | None = None) -> dict[str, int]:
    """Leaf phases in nanoseconds. `py` is a clock's exclusive-time map,
    `r` the engine's totals over the same window (both as deltas when a
    window is not the whole build). `hashing_ns` is the calibrated estimate
    of Python identity hashing, carved out of ``python.row_build``."""
    out: dict[str, int] = defaultdict(int)

    def put(k: str, v: float) -> None:
        out[k] += max(0, int(v))

    def g(k: str) -> int:
        return int(r.get(k, 0))

    for k, v in py.items():
        if k.startswith("pyo3."):
            continue
        if k == "python.row_build" and hashing_ns is not None:
            h = min(v, hashing_ns)
            put("python.row_build.identity_hashing", h)
            put("python.row_build.objects", v - h)
        else:
            put(k, v)
    calls = {k[5:]: v for k, v in py.items() if k.startswith("pyo3.")}

    if not r:  # timers compiled out: each engine call stays opaque
        for m, v in calls.items():
            put(f"engine.{m}", v)
        return dict(out)

    boundary = 0
    for m, v in calls.items():
        if m in _BODY:
            boundary += max(0, v - g(_BODY[m]))
        else:  # in_batch, set_event_cursor, last_commit_phases, ...
            put("pyo3.other_calls", v)
    put("pyo3.boundary", boundary)

    # -- staging: sampled split of the stage_* bodies ------------------------
    stage = g("py_stage_edges_ns") + g("py_stage_nodes_ns")
    lk, bd = g("stage_sampled_lookup_ns"), g("stage_sampled_build_ns")
    frac = lk / (lk + bd) if lk + bd else 0.0
    put("staging.dictionary_lookup", stage * frac)
    put("staging.row_build_push", stage * (1 - frac))
    put("staging.dictionary_insert", g("py_ensure_entities_ns"))

    # -- endpoint existence probe (bulk path) --------------------------------
    p_locate, p_fold = g("probe.locate_ns"), g("probe.fold_ns")
    p_mat, p_staged, p_scan = (g("probe.rows_materialize_ns"),
                               g("probe.staged_ns"), g("probe_scan_ns"))
    put("probe.postings_fold", p_fold)
    put("probe.postings_lookup", p_locate - p_fold)
    put("probe.rows_materialize", p_mat)
    put("probe.staged_rows", p_staged)
    put("probe.full_scan", p_scan)
    # current belief answered from the believed-node count: reading new node
    # segments and diffing close runs, then one lookup per uid
    p_cnt_rec, p_cnt_ans = (g("probe_count_reconcile_ns"),
                            g("probe_count_answer_ns"))
    put("probe.count_reconcile", p_cnt_rec)
    put("probe.count_lookup", p_cnt_ans)
    # the probe runs from `believed_any` and, on the raw-record fast path, from
    # inside `stage_ingest_events_raw`, whose body counts it separately
    put("probe.other", g("py_believed_any_ns") + g("ingest_raw_probe_ns") - p_locate
        - p_mat - p_staged - p_scan - p_cnt_rec - p_cnt_ans)

    # -- the raw-record ingest fast path --------------------------------------
    r_parts = {
        "ingest_raw.parse_check": g("ingest_raw_parse_ns"),
        "ingest_raw.identity_hashing": g("ingest_raw_identity_ns"),
        "ingest_raw.stage_rows": g("ingest_raw_stage_ns"),
    }
    for k, v in r_parts.items():
        put(k, v)
    put("staging.dictionary_insert", g("ingest_raw_ensure_ns"))
    # an `ingest_events` op inside an engine-applied transaction runs the same
    # timers inside `apply_ops_raw`'s body, under `apply_raw_ingest_ns`
    put("ingest_raw.other", g("py_ingest_raw_ns") + g("apply_raw_ingest_ns")
        - sum(r_parts.values()) - g("ingest_raw_ensure_ns") - g("ingest_raw_probe_ns"))

    # -- A transaction applied by the engine from its record -----
    # Its reads and closes run the correction timers below and are summed into
    # those phases' bodies; ensure and staging join the staging phases; the
    # row build (vid hashing, the rows themselves) is what is left.
    put("apply_raw.parse_check", g("apply_raw_parse_ns"))
    put("staging.dictionary_insert", g("apply_raw_ensure_ns"))
    put("staging.row_build_push", g("apply_raw_stage_ns"))
    put("apply_raw.row_build", g("py_apply_raw_ns") - g("apply_raw_parse_ns")
        - g("apply_raw_ensure_ns") - g("apply_raw_read_ns") - g("apply_raw_supersede_ns")
        - g("apply_raw_stage_ns") - g("apply_raw_ingest_ns"))

    # -- correction resolution -----------------------------------------------
    c_locate, c_fold = g("correct.locate_ns"), g("correct.fold_ns")
    c_mat, c_staged = g("correct.rows_materialize_ns"), g("correct.staged_ns")
    put("correction.postings_fold", c_fold)
    put("correction.postings_lookup", c_locate - c_fold)
    put("correction.rows_materialize", c_mat)
    put("correction.staged_rows", c_staged)
    put("correction.read_other", g("py_believed_ns") + g("py_believed_window_ns")
        + g("apply_raw_read_ns") - c_locate - c_mat - c_staged)
    put("correction.close_locate", g("close_locate_ns"))
    put("correction.close_overlay", g("close_overlay_ns"))
    put("correction.close_other", g("py_stage_closes_ns") - g("close_locate_ns")
        - g("close_overlay_ns") + g("py_stage_retires_ns") + g("apply_raw_supersede_ns"))

    # -- commit: CommitPhases, with the seal split by the seal context --------
    seal_parts = {
        "seal.sort": g("seal.sort_ns"),
        "seal.split_lanes": g("seal.split_lanes_ns"),
        "seal.chunk_plan": g("seal.chunk_plan_ns"),
        "seal.row_copy": g("seal.row_copy_ns"),
        "seal.segment_entry": g("seal.segment_entry_ns"),
    }
    for s in _WRITE_SLOTS:
        seal_parts[f"seal.{s}"] = g(f"seal.{s}_ns")
    for k, v in seal_parts.items():
        put(k, v)
    put("seal.other", g("commit_seal_ns") - sum(seal_parts.values()))
    named = {
        "commit.capture": g("commit_capture_ns"),
        "commit.ratio_seed": g("commit_ratio_seed_ns"),
        "commit.close_run": g("commit_closes_ns"),
        "commit.stats": g("commit_stats_ns"),
        "commit.dictionary_tail": g("commit_dict_ns"),
        "commit.manifest_digest": g("commit_digest_ns"),
        "commit.manifest_build": g("commit_delta_build_ns"),
        "commit.manifest_write_fsync": g("commit_manifest_ns"),
        "commit.current_pointer": g("commit_current_ns"),
        "commit.pin_close_index": g("commit_pin_ns"),
        # The identity filters of the segments a commit sealed, built
        # after the publication point. Named rather than left in
        # `commit.other`, which would otherwise quietly grow by exactly this.
        "commit.identity_filter": g("commit_vidfilter_ns"),
    }
    for k, v in named.items():
        put(k, v)
    put("commit.other", g("py_commit_ns") - g("commit_seal_ns") - sum(named.values()))

    # -- compaction ------------------------------------------------------------
    put("compaction.trigger", g("py_needs_compaction_ns"))
    inc_rows, full_rows = g("inc_rows_rewritten"), g("full_rows_rewritten")
    tot = inc_rows + full_rows
    w_inc = inc_rows / tot if tot else (1.0 if g("inc_calls") else 0.0)
    w_full = 1.0 - w_inc if (tot or g("full_calls")) else 0.0
    opens = g("compact.open_uncached_ns") + g("compact.open_cached_ns")
    writes = {s: g(f"compact.{s}_ns") for s in _WRITE_SLOTS}
    write_sum = sum(writes.values())
    for s, v in writes.items():
        name = "compaction.fsync" if s in ("fsync", "rename") else f"compaction.{s}"
        put(name, v)

    # size-tiered
    put("compaction.plan", g("inc_plan_ns"))
    put("compaction.close_index", g("inc_close_index_ns"))
    put("compaction.read_decode", opens * w_inc)
    flush = g("inc_flush_ns")
    put("compaction.flush_other", flush - write_sum * w_inc)
    put("compaction.merge_rows", g("inc_merge_ns") - flush - opens * w_inc)
    put("compaction.splice", g("inc_splice_ns"))
    put("compaction.manifest_seal", g("inc_manifest_seal_ns") + g("full_manifest_seal_ns"))
    put("compaction.publish", g("inc_publish_ns") + g("full_publish_ns"))
    put("compaction.install_adopt", g("inc_adopt_ns") + g("full_adopt_ns"))
    # The identity filters of the segments a compaction wrote
    put("compaction.identity_filter", g("inc_vidfilter_ns") + g("full_vidfilter_ns"))
    # full-store
    put("compaction.read_decode", opens * w_full)
    put("compaction.full_read_rows", g("full_read_ns") - opens * w_full)
    put("compaction.restage", g("full_restage_ns"))
    seal_c = (g("compact.sort_ns") + g("compact.split_lanes_ns") + g("compact.chunk_plan_ns")
              + g("compact.row_copy_ns") + g("compact.segment_entry_ns"))
    put("compaction.sort_chunk", seal_c)
    put("compaction.seal_other", g("full_seal_ns") - seal_c - write_sum * w_full)
    inc_named = (g("inc_plan_ns") + g("inc_close_index_ns") + g("inc_merge_ns")
                 + g("inc_splice_ns") + g("inc_manifest_seal_ns") + g("inc_publish_ns")
                 + g("inc_adopt_ns") + g("inc_vidfilter_ns"))
    full_named = (g("full_read_ns") + g("full_restage_ns") + g("full_seal_ns")
                  + g("full_manifest_seal_ns") + g("full_publish_ns") + g("full_adopt_ns")
                  + g("full_vidfilter_ns"))
    put("compaction.other", g("py_compact_incremental_ns") - inc_named
        + g("py_compact_ns") - full_named)
    put("gc", g("py_gc_ns"))
    return dict(out)


#: Leaf-name prefix -> the group the summary table shows. First match wins.
GROUPS: tuple[tuple[str, str], ...] = (
    ("client.event_generation", "event generation (client)"),
    ("client.correction_ops", "event generation (client)"),
    ("client.driver", "driver bookkeeping (client)"),
    ("python.store_write", "event-log append"),
    ("eventlog.", "event-log append"),
    ("python.", "Python row build and batch conversion"),
    ("pyo3.", "PyO3 call boundary"),
    ("ingest_raw.", "raw-record ingest (engine row build)"),
    ("apply_raw.", "transaction applied from its record (engine row build)"),
    ("engine.", "engine (timers compiled out)"),
    ("staging.", "staging (dictionary, row build)"),
    ("probe.", "endpoint existence probe"),
    ("correction.", "correction resolution"),
    ("seal.", "seal (sort, encode, write, fsync)"),
    ("commit.", "commit publish (manifest, fsyncs)"),
    ("compaction.", "compaction"),
    ("gc", "compaction"),
    ("unattributed", "unattributed (residual)"),
)


def group_of(leaf: str) -> str:
    for prefix, group in GROUPS:
        if leaf.startswith(prefix):
            return group
    return "other"


def _grouped(leaves: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for k, v in leaves.items():
        out[group_of(k)] += v
    return dict(out)


def _diff(a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    keys = set(a) | set(b)
    return {k: int(a.get(k, 0)) - int(b.get(k, 0)) for k in keys}


def _table(leaves: dict[str, int], total_ns: int) -> dict[str, dict[str, float]]:
    return {k: {"s": round(v / 1e9, 6),
                "fraction": round(v / total_ns, 6) if total_ns else 0.0}
            for k, v in sorted(leaves.items(), key=lambda kv: -kv[1])}


# --------------------------------------------------------------------- #
# calibration: Python identity hashing                                   #
# --------------------------------------------------------------------- #

def calibrate_hashing(events: Iterable[dict[str, Any]], tt: int) -> dict[str, float]:
    """Nanoseconds per edge row and per node row of the identity hashing the
    bulk path runs in Python (`base.py::_ingest_events`: `edge_eid` and
    `_vid` per event, `_vid` per new node), measured on real events after
    ingest. Used only to split ``python.row_build``; see the module
    docstring."""
    from gnomon.core.model import edge_eid
    from gnomon.storage.base import _vid

    evs = list(events)
    if not evs:
        return {"edge_ns": 0.0, "node_ns": 0.0, "sample": 0}
    t0 = _now()
    for i, ev in enumerate(evs):
        disc = ev.get("disc", f"#{i}")
        eid = edge_eid(ev["src"], ev["dst"], ev["rel_type"], disc)
        _vid(eid, tt, ev["vt_s"])
    t1 = _now()
    for ev in evs:
        _vid(ev["src"], tt, ev["vt_s"])
    t2 = _now()
    return {"edge_ns": (t1 - t0) / len(evs), "node_ns": (t2 - t1) / len(evs),
            "sample": len(evs)}


# --------------------------------------------------------------------- #
# the record                                                             #
# --------------------------------------------------------------------- #

def progress_summary(clock: PhaseClock, engine: dict[str, int], top: int = 6) -> str:
    """One short `group pct` string for a progress line."""
    if not clock.enabled or clock.t_start is None:
        return ""
    total = clock.elapsed_ns()
    groups = _grouped(compose(clock.snapshot_excl(), engine))
    parts = sorted(groups.items(), key=lambda kv: -kv[1])[:top]
    return "  phases " + " ".join(
        f"{name.split(' (')[0].replace(' ', '_')}={100 * v / total:.0f}%"
        for name, v in parts if total)


def sample(clock: PhaseClock, engine: dict[str, int], ops: int) -> None:
    """Keep one cumulative observation per progress line, so the growth of
    each phase *within* one build is on record, not only its total."""
    if not clock.enabled or clock.t_start is None:
        return
    leaves = compose(clock.snapshot_excl(), engine)
    clock.samples.append({"ops": ops, "elapsed_s": round(clock.elapsed_ns() / 1e9, 3),
                          "groups_s": {k: round(v / 1e9, 4)
                                       for k, v in _grouped(leaves).items()},
                          "leaves_s": {k: round(v / 1e9, 4) for k, v in leaves.items()
                                       if v >= 1e6}})


def report(clock: PhaseClock, engine: dict[str, int], *,
           rows_hashed: tuple[int, int] = (0, 0),
           hashing: dict[str, float] | None = None,
           extra: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """``ingest_phase_totals`` for the build record, or None when the clock
    was off. `rows_hashed` is `(edge rows, node rows)` that went through the
    Python bulk row loop, for the hashing estimate."""
    if not clock.enabled or clock.t_start is None:
        return None
    total = clock.elapsed_ns()
    py = clock.snapshot_excl() if clock.t_stop is None else dict(clock.excl)
    h_ns = None
    if hashing:
        h_ns = int(hashing["edge_ns"] * rows_hashed[0] + hashing["node_ns"] * rows_hashed[1])
    leaves = compose(py, engine, h_ns)
    residual = leaves.get(PhaseClock.ROOT, 0)
    windows: dict[str, Any] = {}
    fin = clock._marks.get("finalisation")
    if fin is not None:
        t_mark, py_mark, eng_mark = fin
        h_build = h_ns  # all bulk hashing happens before finalisation
        build = compose(py_mark, eng_mark, h_build)
        final = compose(_diff(py, py_mark), _diff(engine, eng_mark))
        windows = {
            "build": {"s": round(t_mark / 1e9, 6), "phases": _table(build, t_mark)},
            "finalisation": {"s": round((total - t_mark) / 1e9, 6),
                             "phases": _table(final, total - t_mark)},
        }
    out = {
        "ingest_s": round(total / 1e9, 6),
        "accounted_s": round((total - residual) / 1e9, 6),
        "residual_s": round(residual / 1e9, 6),
        "residual_fraction": round(residual / total, 6) if total else 0.0,
        "engine_timers": bool(engine),
        "phases": _table(leaves, total),
        "groups": _table(_grouped(leaves), total),
        "windows": windows,
        "python_sections": {k: {"s": round(v / 1e9, 6), "calls": clock.calls.get(k, 0)}
                            for k, v in sorted(py.items(), key=lambda kv: -kv[1])},
        "engine_raw": dict(sorted(engine.items())),
        "hashing_calibration": dict(hashing or {}, rows_edge=rows_hashed[0],
                                    rows_node=rows_hashed[1],
                                    estimated_s=round((h_ns or 0) / 1e9, 6)),
        "samples": clock.samples,
        "notes": [
            "phases are exclusive: they sum to ingest_s minus residual_s",
            "python.row_build.identity_hashing is a calibrated estimate",
            "staging.dictionary_lookup vs staging.row_build_push is a sampled split",
            "pyo3.boundary = time inside each bracketed engine call minus that "
            "call's engine-measured body (argument/result conversion, dispatch)",
        ],
    }
    if extra:
        out.update(extra)
    return out
