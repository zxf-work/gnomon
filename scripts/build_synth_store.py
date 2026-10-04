"""Build a synthetic bi-temporal store of N entities, streamed, timing ingest per phase.

**The workload.** Deterministic from `(n_entities, seed)` alone: splitmix64
endpoints; constant average degree, because the node count and the edge
lifetime both scale with N; 50-node communities with 70% of edges inside a
community; one 10x burst band; and a second belief epoch. The op sequence is
bulk ingest -> a belief-epoch node assertion -> epoch-2 corrections and one
node correction -> partial mid-interval corrections -> retractions. It is the
same family `eval_harness.py::build_dataset` generates, but every event is
generated from its own index, on demand, in batches of `--batch`, so nothing
N-sized is ever held in memory.

**`--seed`.** The seed is XORed into the two per-event splitmix64 draws
before the finalizer. `--seed 0` reproduces `build_dataset` exactly; any
other seed draws an independent, still fully reproducible graph.

**Digests.** `store.digest()` hashes every version including its transaction
times, which come from the wall clock at commit, so two independent builds of
the same events never agree on it. `content_digest()` uses the same sort keys
minus `vid`/`tt_s`/`tt_e`, with each row's canonical bytes as a final
tie-break so the order is total; equal logical content gives equal content
digests. `--digest full` (the default below `DIGEST_AUTO_THRESHOLD` = 10M
entities) computes both; `none` (the default at and above it) records only
the O(1) manifest identity; `streaming` computes the store digest in bounded
memory.

**Output.** The store at `--out` (with its `eventlog.jsonl`), and
`<out>/build-record.json`: the parameters, wall times, the per-phase split of
ingest time (`ingest_phase_totals`, see `gnomon/eval/ingest_phases.py`), RSS
samples, one record per compaction, and the digests, conforming to
`benchmarks/schema/result_manifest.schema.json`. `--stop-at-ops` and
`--resume` split a long build across processes.

    python scripts/build_synth_store.py --n-entities 100000 --out runs/synth-100k
    python scripts/build_synth_store.py --n-entities 10000000 --batch 250 \\
        --compact-every 0 --out runs/synth-10m
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import platform
import resource
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import gnomon  # noqa: E402
from gnomon.core.model import EntityRef, canonical_json, digest  # noqa: E402
from gnomon.eval import ingest_phases  # noqa: E402

SCHEMA_VERSION = "1.0.0"

#: Frozen family shape, verbatim from `scripts/eval_harness.py` -- see the
#: module docstring's "Reused verbatim" paragraph. Never redefine these to
#: "fix" a scale-curve property; that would silently redefine every existing
#: synth-* record's comparability (mirrors `build_synth_iv_store.py`'s own
#: stated discipline for the same reason).
COMMUNITY = 50
INTRA_PCT = 70
_M64 = (1 << 64) - 1

#: The values used for the large builds; both are ordinary flags below;
#: these are only the argparse defaults.
DEFAULT_BATCH = 250
DEFAULT_COMPACT_EVERY = 1_000_000
#: Rounds the finalisation phase will call the incremental compactor before
#: giving up on draining its trigger (`build`). A handful should do; the cap
#: exists so a non-converging trigger shows up in the build record as
#: `finalisation_phases.compact.still_eligible` instead of as a hung job.
FINAL_COMPACT_ROUNDS = 64

#: Scale at which the final digest pass switches its default from `full` to
#: `none`. A store this size already showed the failure mode at hand: a
#: 17.4M-edge store held a flat ~2.4 GB RSS through streaming ingest and
#: then spiked to ~25 GB in its own finalization pass -- and per that
#: build's own analysis the spike is `store.stats()`'s full-segment walk
#: plus the forced end-of-build `compact()`, not a `store_digest()` call at
#: all (that build recorded only the manifest identity, which never touches
#: store content). `store_digest()` itself is worse: it
#: sorts and canonical-JSON-encodes one Python dict per version, which is
#: exactly the route `stats_accum()` (`crates/gnomon-core/src/read.rs`)
#: was rewritten *away* from after it caused a 2 GB-cap OOM at 10M rows. At
#: 100M+ rows `store_digest()` alone would exceed a 93 GB host, so a build at
#: or above this threshold defaults to skipping it rather than to inheriting
#: SF1's spike from a different pass.
DIGEST_AUTO_THRESHOLD = 10_000_000
DIGEST_MODES = ("none", "full", "streaming")


def default_digest_mode(n_entities: int) -> str:
    """`none` at or above `DIGEST_AUTO_THRESHOLD`, `full` below it -- the
    scale-conditioned default `--digest` resolves to when not given
    explicitly (module docstring "Digest equivalence"; see also
    `DIGEST_AUTO_THRESHOLD`'s own docstring for why `full`'s cost, not
    SF1's actual spike, is what this guards against)."""
    return "none" if n_entities >= DIGEST_AUTO_THRESHOLD else "full"

#: "a progress line every 500k ops" (task spec) -- not exposed as a flag,
#: since nothing about it is dataset- or scale-specific.
PROGRESS_EVERY_OPS = 500_000

#: The ordered phases of one build, exactly `build_dataset`'s own sequence.
#: A resumed build starts at `state["phase"]`; every phase strictly before it
#: is already fully committed to the store and is not re-entered.
PHASES = ("bulk", "n1_assert", "epoch2_edges", "epoch2_node", "partial", "retract", "done")


# --------------------------------------------------------------------- #
# generator math -- verbatim from eval_harness.py except _row_mix's seed
# --------------------------------------------------------------------- #

def _mix(x: int) -> int:
    """splitmix64 finalizer -- verbatim from `eval_harness.py::_mix`."""
    x = (x * 0x9E3779B97F4A7C15) & _M64
    x ^= x >> 30
    x = (x * 0xBF58476D1CE4E5B9) & _M64
    x ^= x >> 27
    x = (x * 0x94D049BB133111EB) & _M64
    return x ^ (x >> 31)


def n_nodes(n_entities: int) -> int:
    """Verbatim from `eval_harness.py::n_nodes`."""
    return max(200, n_entities // 100)


def edge_life(n_entities: int) -> int:
    """Verbatim from `eval_harness.py::edge_life`."""
    return max(40, n_entities // 20)


def _vt_s(i: int, n_entities: int) -> int:
    """Verbatim from `eval_harness.py::_vt_s` -- one deliberate burst band."""
    b0 = n_entities // 2
    b1 = b0 + max(1, n_entities // 20)
    if b0 <= i < b1:
        return b0 + (i - b0) // 10
    return i


def _row_mix(i: int, arm: int, seed: int) -> int:
    """`_mix(2*i + arm)`, seed-salted; `arm` selects an independent draw
    stream per row. At `seed=0` this is bit-identical to
    `eval_harness.py::_event`'s `_mix(2*i)` / `_mix(2*i+1)` (see the module
    docstring on `--seed`). Depends on nothing but `(i, arm, seed)`, so a
    row is reproducible from its own index alone -- no state crosses a batch
    boundary, which is a strictly stronger property than "per-batch
    reproducible"."""
    return _mix((2 * i + arm) ^ (seed << 1))


def _named_node_indices(n_entities: int, seed: int, fraction: float) -> list[int]:
    """Deterministic subset of node indices, at roughly `fraction` of the
    population, to give a real distinguishing `name` prop.

    Every node but `n1` reaches the store only as an `ingest_events`
    auto-created bare version (no `props` at all -- `gnomon/storage/base.py::
    _ingest_events`), because `n1`'s single `assert_node(..., {"name":
    "alpha"}, ...)` call below is the generator's only named node. The
    This makes the name index untestable at any
    scale: with one real name in the whole store, the index holds a
    constant 2 entries regardless of `n_entities`, so index bytes vs.
    key bytes, a ratio that should be scale-independent but needs more than
    one point to show it) has nothing to measure against.

    `fraction=0.0` (the default) selects nothing, so a build with this
    knob unused is byte-identical to one without it -- no digest changes.
    A nonzero fraction uses `_row_mix`'s draw stream 4 (streams 0 and 1
    pick the endpoints, 2 and 3 are the long-interval draws), seed-salted
    like every other draw here, so selection is deterministic and
    reproducible from `(n_entities, seed, fraction)` alone, and independent
    of which streams an unrelated knob already occupies.
    """
    if fraction <= 0.0:
        return []
    n = n_nodes(n_entities)
    return [k for k in range(n) if (_row_mix(k, 4, seed) / 2.0 ** 64) < fraction]


#: `PartitionMap::DEFAULT_WIDTH` (`crates/gnomon-core/src/staging.rs`) --
#: 7 days in microseconds. The li-family knobs below express interval
#: lengths as multiples of this, so they route to the interval lane
#: (`lane_for`, `LANE_MAX_PARTITION_CROSSINGS = 2`) regardless of scale,
#: without this script ever calling `set_layout` -- the store keeps the
#: engine's own default width, and the generator's math agrees with it by
#: construction rather than by a second copy of the constant drifting.
PARTITION_WIDTH_US = 7 * 24 * 60 * 60 * 1_000_000


def _event(i: int, n_entities: int, seed: int, *,
          tick_us: int = 1,
          interval_fraction: float = 0.0,
          interval_length_dist: str = "pow2",
          short_life_us: int | None = None,
          partition_width_us: int = PARTITION_WIDTH_US) -> dict[str, Any]:
    """Event `i`: same construction as `eval_harness.py::_event`, with
    `_row_mix` (seeded) standing in for the bare `_mix(2*i)`/`_mix(2*i+1)`.

    **Long-interval extension, additive only.** At the defaults (`tick_us=1`,
    `interval_fraction=0.0`) this computes *exactly* the base formula --
    `vt_s = t`, `vt_e = t + edge_life(n_entities)` -- which is the "hard
    requirement" that `--interval-fraction 0 --tick-us 1` reproduce
    `synth-1m`/`synth-10m` byte-for-byte. `tick_us` widens
    the tick into real microseconds (`vt_s = tick_index * tick_us`), so a
    52-week store can be built without changing the engine's partition
    width. `interval_fraction` of rows draw a "long" life instead of the
    short one: `L = partition_width_us * 2**U`, `U ~ Uniform{1,2,3}` (the
    `pow2` distribution -- crossings 3..9, always routes to the interval
    lane via `lane_for`); the rest keep `short_life_us` (default
    `edge_life(n_entities) * tick_us`, crossings <= 2, always the event
    lane). The long/short draw uses a *third* `_row_mix` stream (2) and the
    `U` draw a *fourth* (3), so neither disturbs the endpoint streams (0, 1)
    any existing record depends on.
    """
    n, t_ticks = n_nodes(n_entities), _vt_s(i, n_entities)
    vt_s = t_ticks * tick_us
    src = _row_mix(i, 0, seed) % n
    r = _row_mix(i, 1, seed)
    if r % 100 < INTRA_PCT:
        base = (src // COMMUNITY) * COMMUNITY
        dst = base + (r >> 8) % min(COMMUNITY, n - base)
    else:
        dst = (r >> 8) % n

    if interval_fraction > 0.0 and (_row_mix(i, 2, seed) / 2.0 ** 64) < interval_fraction:
        if interval_length_dist != "pow2":
            raise NotImplementedError(
                f"--interval-length-dist {interval_length_dist!r}: only 'pow2' is "
                f"implemented")
        u = 1 + (_row_mix(i, 3, seed) % 3)  # U ~ Uniform{1, 2, 3}
        life_us = partition_width_us * (2 ** u)
    elif short_life_us is not None:
        life_us = short_life_us
    elif tick_us == 1:
        # the exact base formula -- required for byte-for-byte
        # reproduction at the CLI defaults (tick_us=1, interval_fraction=0)
        life_us = edge_life(n_entities)
    else:
        # `tick_us != 1` means a `synth-li` build: `edge_life(n_entities) *
        # tick_us` would scale into the multi-partition range right along
        # with vt_s (e.g. at the `auto52w` calibration it is *larger*
        # than `partition_width_us`), silently promoting every "short" edge
        # into the interval lane too. The default for this family is
        # `0.25 * W_p` (crossings <= 2, always the event lane); this is that
        # default, applied automatically rather than requiring every
        # `synth-li` invocation to pass `--short-life` explicitly.
        life_us = round(0.25 * partition_width_us)

    return {"src": f"n{src}", "dst": f"n{dst}",
            "rel_type": "R" if i % 3 else "S",
            "vt_s": vt_s, "vt_e": vt_s + life_us,
            # Explicit, never left to `ingest_events`'s own offset-based
            # default: `Store.ingest_events` (`gnomon/store.py`) resets its
            # internal `offset` to 0 on *every top-level call*, accumulating
            # correctly only across the batches *within* one call. This
            # script commits the bulk phase as many separate calls (one per
            # `--batch`), so relying on the implicit default would silently
            # restart disc numbering at "#0" every batch, colliding discs
            # across batches and breaking every later `_edge_ref` lookup
            # (found by the equivalence smoke build below at --batch <
            # --n-entities). Stamping it here reproduces exactly what
            # `eval_harness.py::build_dataset`'s single, whole-list call gets
            # from the implicit default (`f"#{offset+i}"` with `offset`
            # accumulating over one call's own chunks), for any --batch.
            "disc": f"#{i}"}


def _edge_ref(i: int, n_entities: int, seed: int) -> EntityRef:
    """The identity `ingest_events` gave event `i` -- verbatim in spirit from
    `eval_harness.py::_edge_ref`, using `_event`'s own explicit `disc`."""
    e = _event(i, n_entities, seed)
    return EntityRef(kind="edge", src=e["src"], dst=e["dst"],
                     rel_type=e["rel_type"], disc=e["disc"])


# --------------------------------------------------------------------- #
# content digest -- tt-independent equivalence notion (see module docstring)
# --------------------------------------------------------------------- #

def _strip_tt(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row.pop("vid", None)
    row.pop("tt_s", None)
    row.pop("tt_e", None)
    return row


def _digest_reference_key(id_field: str, r: dict[str, Any]) -> tuple[Any, Any, Any, str]:
    """The reference's four-component sort key: identity, `vt_s`,
    `vt_e`, and the props text `json.dumps(props, sort_keys=True)` renders --
    Python's *default* separators and `ensure_ascii=True`, not the canonical
    encoding. Shared by the digest and by
    `_count_ambiguous_ties` so the two can never disagree about what a tie
    is."""
    return (r[id_field], r["vt_s"], r["vt_e"], json.dumps(r["props"], sort_keys=True))


def _digest_sort_key(id_field: str):
    """The reference key, made total by appending the row's
    own canonical bytes as a fifth, tie-breaking component -- exactly the
    tie-break `crates/gnomon-core/src/digest.rs::cmp_rec` uses
    (`a.json.cmp(&b.json)`, the row's full canonical-JSON text, compared
    raw). Two distinct rows are never byte-identical, so this key is total:
    the reference's sort -- and so `content_digest()` -- no longer depends on
    `all_*_versions()`'s incoming order."""
    def key(r: dict[str, Any]) -> tuple[Any, Any, Any, str, str]:
        return (*_digest_reference_key(id_field, r), canonical_json(r))
    return key


def _count_ambiguous_ties(rows_sorted: list[dict[str, Any]], id_field: str) -> int:
    """Adjacent rows in `rows_sorted` (already ordered by `_digest_sort_key`)
    that tie on the reference's *four*-component key yet differ in content --
    counted once per row against its immediate predecessor, exactly as
    `digest.rs::finish`'s `ambiguous_ties` counter does (not once per pair
    within a tie group of size > 2)."""
    ties = 0
    prev_key: tuple[Any, Any, Any, str] | None = None
    prev_json: str | None = None
    for r in rows_sorted:
        key = _digest_reference_key(id_field, r)
        rj = canonical_json(r)
        if prev_key is not None and prev_key == key and prev_json != rj:
            ties += 1
        prev_key, prev_json = key, rj
    return ties


def content_digest_report(store: Any, *, kinds: tuple[str, ...] = ("nodes", "edges")) -> dict[str, Any]:
    """`content_digest()`'s digest plus `ambiguous_ties`: the
    count of adjacent sorted rows that tie on the reference's four-component
    key yet differ in content. The fifth, total-order-making sort component
    (each row's own canonical bytes -- `_digest_sort_key`) is what makes the
    returned `digest` layout-independent even when such ties exist;
    `ambiguous_ties` reports how many pairs needed it, the same quantity
    `crates/gnomon-core/src/digest.rs` reports under the same name. Zero
    on every store the ingest path builds and on every campaign corpus
    measured so far; `content_digest()` itself stays a thin
    wrapper around this for callers that only want the digest string."""
    payload: dict[str, Any] = {}
    ambiguous_ties = 0
    if "nodes" in kinds:
        node_rows = sorted(
            (_strip_tt(v.to_json()) for v in store.adapter.all_node_versions()),
            key=_digest_sort_key("uid"))
        ambiguous_ties += _count_ambiguous_ties(node_rows, "uid")
        payload["nodes"] = node_rows
    if "edges" in kinds:
        edge_rows = sorted(
            (_strip_tt(v.to_json()) for v in store.adapter.all_edge_versions()),
            key=_digest_sort_key("eid"))
        ambiguous_ties += _count_ambiguous_ties(edge_rows, "eid")
        payload["edges"] = edge_rows
    return {"digest": digest(payload), "ambiguous_ties": ambiguous_ties}


def content_digest(store: Any, *, kinds: tuple[str, ...] = ("nodes", "edges")) -> str:
    """A digest over logical content only: every field `store_digest()`
    hashes, minus `vid`/`tt_s`/`tt_e` (all three derived from the wall-clock
    transaction clock -- see module docstring, "Digest equivalence"). Two
    independent builds of the same logical events, corrections and
    retractions, in the same order, produce the same `content_digest()` even
    though their `store.digest()` never will.

    `kinds` restricts to `("edges",)` to compare only edge-version content,
    which is fully batch-size invariant (unlike an auto-created node
    version's `vt_s` -- see module docstring's discussion of `--batch`).

    The sort is total: rows that tie on the four keyed
    components are broken by their own canonical bytes, so this value no
    longer depends on `all_*_versions()`'s incoming order even in that case
    -- see `content_digest_report` for the accompanying `ambiguous_ties`
    count.
    """
    return content_digest_report(store, kinds=kinds)["digest"]


# --------------------------------------------------------------------- #
# resource / bytes accounting
# --------------------------------------------------------------------- #

def _rss_kb() -> dict[str, int]:
    """Cross-platform peak-RSS snapshot (`ru_maxrss` alone is already the
    lifetime peak, kB on Linux / bytes on macOS) -- same shape as
    the other measurement drivers use.

    `RssAnon`/`RssFile`/`RssShmem` are added where the kernel reports them
    (Linux), as additive keys -- the shape callers pin is `maxrss_kb`, and
    `_timed`'s own pinned shape is the three keys of its entry, not these.
    They are what `memory_accounting` has to be read against: every component
    that estimates a `Vec`/`HashMap` capacity is anonymous heap and belongs
    against `RssAnon`, while `segment_cache_mapped` and `pinned_mapped` are
    mapped segment files whose *touched* pages land in `RssFile` **or
    `RssShmem`**. Summing the accounting against `VmRSS` mixes them and can
    attribute none.

    **`RssShmem` is not optional here.** A store built on `/dev/shm` -- which
    is where a development build often puts it, and `/tmp` on a cluster node is often
    tmpfs too -- is shmem-backed, so its mapped pages are accounted in
    `RssShmem` and `RssFile` stays near zero. One 30M build read
    `RssFile` = 20,204 kB against a 770,392,788 B store and looked as though
    the mappings were not resident at all; `VmRSS - RssAnon - RssFile` was
    752,420 kB, i.e. the whole store, and all of it was shmem. Reading only
    the first two fields would have attributed the store's own bytes to
    nothing."""
    out: dict[str, int] = {}
    status = Path("/proc/self/status")
    if status.exists():  # Linux
        for line in status.read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:", "RssAnon:",
                                "RssFile:", "RssShmem:")):
                k, v = line.split(":")
                out[k.strip().lower()] = int(v.split()[0])
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    out["maxrss_kb"] = ru // 1024 if sys.platform == "darwin" else ru
    return out


#: Running maximum of the identity postings' resident bytes, and the budget
#: they are held under. Resident bytes only fall when the postings are
#: evicted or gc prunes them, so a peak has to be *sampled* -- the value at
#: the end of a build is a floor on what the build actually held. Every place
#: that already samples RSS samples this too (`Builder._tick`'s progress line
#: and each `_timed` finalisation phase), which is the same cadence the RSS
#: series is read at.
_POSTINGS_PEAK: dict[str, Any] = {"bytes": 0, "budget": None, "sampled": 0,
                                  "detail": {}}


#: The last full fold/filter receipt, and where to leave it on disk.
#:
#: **An abandoned build writes no receipt**, which was an instrumentation gap:
#: in a postings-budget sweep, the build at 0.88x of the needed budget hit a
#: 45-minute stopping rule, so `postings_detail`'s fold counters -- which
#: exist only in process and are written once, at the end -- were lost for
#: exactly the build whose mechanism was in question. The sweep could only
#: quote that build's wall (>=27x slower, abandoned) and nothing about *why*,
#: and an earlier "~12 whole-store re-folds" figure had to be retracted
#: because it had been read from `/proc/<pid>/io` `rchar`, which does not
#: count mmap page faults.
#:
#: So the counters are also emitted *periodically*, on the same cadence the
#: RSS series is sampled at, and on `SIGUSR1`. A build killed by a stopping
#: rule then still leaves a fold count behind, and the next sweep below the
#: knee produces a mechanism reading whether or not it finishes.
_INSTRUMENTS: dict[str, Any] = {"out": None, "last": None, "samples": 0}


def _instruments_path(out: Path) -> Path:
    """Deliberately **not** `_progress_path`: that file is the resume state
    and `--resume` refuses a build whose shape does not match it. These are
    observations, they are never read back by the driver, and a damaged one
    must not be able to stop a restart."""
    return out / ".build_instruments.json"


def _fold_receipt(store: Any) -> dict[str, Any] | None:
    """`postings_index_stats` for both row kinds plus `vidfilter_stats`, or
    None on an adapter that carries neither (DuckDB, and any native engine
    without these counters)."""
    adapter = getattr(store, "adapter", None)
    fn = getattr(adapter, "postings_index_stats", None)
    if not callable(fn):
        return None
    out: dict[str, Any] = {"postings_detail": {}}
    try:
        for kind in ("edge", "node"):
            out["postings_detail"][kind] = dict(fn(kind))
        vf = getattr(adapter, "vidfilter_stats", None)
        out["vidfilter_stats"] = dict(vf()) if callable(vf) else None
    except Exception:  # noqa: BLE001 -- a receipt must never fail a build
        return None
    return out


def _emit_instruments(*, reason: str = "sample") -> None:
    """Write the last receipt to disk, atomically. Never raises: this is an
    observation, and a full disk or a read-only mount must cost a
    measurement rather than the build that was producing it."""
    out, last = _INSTRUMENTS["out"], _INSTRUMENTS["last"]
    if out is None or last is None:
        return
    try:
        payload = dict(last)
        payload["reason"] = reason
        payload["at"] = time.time()
        payload["samples"] = _INSTRUMENTS["samples"]
        payload["postings_peak_bytes"] = _POSTINGS_PEAK["bytes"]
        payload["postings_budget"] = _POSTINGS_PEAK["budget"]
        payload["rss"] = _rss_kb()
        tmp = _instruments_path(out).with_suffix(f".json.tmp{os.getpid()}")
        tmp.write_text(json.dumps(payload, sort_keys=True))
        tmp.replace(_instruments_path(out))
    except Exception:  # noqa: BLE001
        return


def _arm_instruments(store: Any, out: Path) -> None:
    """Point the periodic dump at `out` and make `SIGUSR1` trigger one.

    A signal is the only way to ask a build that is inside one long
    operation -- and the correction ramp *is* one operation -- for a reading before its stopping rule fires. Best effort:
    a platform without `SIGUSR1` simply keeps the periodic dump."""
    _INSTRUMENTS["out"] = out
    try:
        import signal

        def _dump(_signum: int, _frame: Any) -> None:
            _sample_postings(store)
            _emit_instruments(reason="signal")

        signal.signal(signal.SIGUSR1, _dump)
    except (AttributeError, ValueError, OSError):  # no SIGUSR1, or not main thread
        return


def _sample_postings(store: Any) -> None:
    """Fold one observation of the postings' resident bytes into the peak,
    and leave the full fold/filter receipt where an abandoned build's killer
    cannot take it with the process.

    Silent on any adapter that does not carry the receipt (DuckDB, and any
    native engine without the receipt), so a build against one still produces
    a record -- with `postings_peak_bytes` left at 0 and `postings_budget`
    at None, which reads as "not measured" rather than as "held nothing"."""
    receipt = _fold_receipt(store)
    if receipt is None:
        return
    try:
        total, budget = 0, None
        detail: dict[str, int] = {}
        for st in receipt["postings_detail"].values():
            total += st["resident_bytes"]
            budget = st["budget"]
            # The fold counters, summed over both kinds. `rows_folded`
            # divided by the store's row count is the number of whole-store
            # re-folds a budget costs -- the quantity the postings-budget
            # sweep prices against wall.
            for k in ("folds", "rows_folded", "evictions", "rebuilds",
                      "transient_folds", "entries", "resident_segments"):
                detail[k] = detail.get(k, 0) + int(st.get(k, 0))
    except Exception:  # noqa: BLE001 -- a receipt must never fail a build
        return
    _POSTINGS_PEAK["bytes"] = max(_POSTINGS_PEAK["bytes"], total)
    _POSTINGS_PEAK["budget"] = budget
    for k, v in detail.items():
        # monotone counters: the last sample is the total, but take a max so
        # a handle replaced by reopen-on-ENOENT cannot walk one backwards
        _POSTINGS_PEAK["detail"][k] = max(_POSTINGS_PEAK["detail"].get(k, 0), v)
    _POSTINGS_PEAK["sampled"] += 1
    _INSTRUMENTS["last"] = receipt
    _INSTRUMENTS["samples"] += 1
    _emit_instruments()


#: Running maximum, per component, of `adapter.memory_accounting()` plus the
#: `RssAnon`/`RssFile` split at the same instant. Same discipline as
#: `_POSTINGS_PEAK` and for the same reason: nothing hooks an allocator, so a
#: peak has to be sampled, and the value at the end of a build is a floor on
#: what the build held.
#:
#: The cadence is *not* the progress line alone. The 1B build's whole RSS
#: ramp -- 3 GiB to 62 GiB -- happened inside the ~262 point `correct`/
#: `retract` ops that follow the bulk phase, which move the op counter by 262
#: and so fire **no** progress line at `PROGRESS_EVERY_OPS = 500_000`. That is
#: precisely why the existing series could not see it. `_tick` therefore also
#: samples once per op outside the bulk phase, which is a few hundred samples
#: per build at any scale.
_MEM_PEAK: dict[str, Any] = {"components": {}, "rss": {}, "sampled": 0}

#: Which `memory_accounting` components are anonymous heap, and so belong
#: against `RssAnon`. `segment_cache_mapped` and `pinned_mapped` are excluded
#: because they are file-backed (`RssFile`), and `segment_cache_accounted` is
#: excluded because it is the *sum* of `segment_cache_mapped` and
#: `segment_cache_decoded` -- adding it would double-count. The two `*_entries`
#: / `*_segments` / `*_rows` / `*_evictions` components are counts, not bytes,
#: and `segment_cache_budget` is a setting.
_ANON_COMPONENTS = (
    "segment_cache_decoded", "segment_cache_header", "postings", "tcsr",
    "name_index_cache", "close_index", "staging", "manifest", "dictionary",
    "stats_accum", "verified_set",
    # Identity-filter bytes held on the heap -- non-zero only when a
    # filter's write failed. The mapped ones (`vid_filter_mapped`) are
    # file-backed and deliberately absent here, like `segment_cache_mapped`.
    "vid_filter_heap", "node_belief",
)

#: The anonymous-memory formula, as a receipt. The memory claim is about **anonymous**
#: memory: it is bounded by the two budgets plus the terms nothing bounds,
#: with the slack measured rather than assumed.
#:
#:     anon <= segment_cache_budget + postings_budget
#:             + staging + dictionary + slack
#:
#: Mapped bytes are a separate term equal to the store's segment bytes, not
#: part of this inequality, because they are file-backed pages the kernel
#: manages rather than heap the engine allocated.
_ANON_UNBOUNDED_TERMS = ("staging", "dictionary", "stats_accum", "manifest",
                         "close_index", "verified_set", "tcsr",
                         "name_index_cache", "node_belief")


def _sample_open_path(store: Any) -> None:
    """Fold one observation of the segment-open counters into `_MEM_PEAK`.

    These are what attribute the terminal ramp to a *path* rather than to a
    structure. Measured around one correction on a 31-segment store:
    `uncached_opens` +30 (the whole-store postings fold, one uncached open
    per live segment) against `cached_misses` +1 (the single segment the
    located row actually lives in). Without them the two O(store) terms --
    postings, which is O(store) by construction, and decoded columns, which
    is O(1 segment) *per correction* -- are indistinguishable in a peak."""
    fn = getattr(getattr(store, "adapter", None), "open_path_stats", None)
    if not callable(fn):
        return
    try:
        got = fn()
    except Exception:  # noqa: BLE001 -- a receipt must never fail a build
        return
    slot = _MEM_PEAK.setdefault("open_path", {})
    for k, v in got.items():
        slot[k] = max(slot.get(k, 0), int(v))


def _sample_memory(store: Any) -> None:
    """Fold one observation of the native handle's memory accounting into
    `_MEM_PEAK`.

    Silent on any adapter without the accessor (DuckDB, and any native engine
    without the accessor), so a build against one still produces a record with
    `memory_accounting_peak` absent rather than wrong -- the same contract
    `_sample_postings` keeps."""
    fn = getattr(getattr(store, "adapter", None), "memory_accounting", None)
    if not callable(fn):
        return
    try:
        got = fn()["current"]
        rss = _rss_kb()
    except Exception:  # noqa: BLE001 -- a receipt must never fail a build
        return
    comp = _MEM_PEAK["components"]
    for k, v in got.items():
        comp[k] = max(comp.get(k, 0), int(v))
    peak_rss = _MEM_PEAK["rss"]
    for k, v in rss.items():
        peak_rss[k] = max(peak_rss.get(k, 0), int(v))
    _sample_open_path(store)
    _MEM_PEAK["sampled"] += 1


def _memory_peak_receipt() -> dict[str, Any] | None:
    """`memory_accounting_peak` for the build record, or None when nothing
    was sampled (a DuckDB build, or a native engine without the accessor).

    `components` are the per-component running maxima in bytes; `rss_kb` is
    the maximum of each `/proc/self/status` field over the same samples, so a
    reader can put the anonymous components against `rssanon` and the mapped
    ones against `rssfile` without joining two files. `accounted_anon_bytes`
    is the sum of the components that are anonymous heap -- the one number
    that is directly comparable to `rssanon`, with the difference being
    allocator slack the accounting cannot see. Additive: nothing existing in
    the record changes shape."""
    if not _MEM_PEAK["sampled"]:
        return None
    comp = dict(_MEM_PEAK["components"])
    anon = sum(comp.get(k, 0) for k in _ANON_COMPONENTS)
    mapped = comp.get("pinned_mapped", 0)
    rss = dict(_MEM_PEAK["rss"])
    # The three memory classes, each against the /proc field that carries it.
    # `rssfile + rssshmem` rather than `rssfile` alone -- which of the two a
    # mapped segment lands in is a property of the filesystem (tmpfs uses the
    # second), not of the engine.
    mapped_resident = rss.get("rssfile", 0) + rss.get("rssshmem", 0)
    formula = (comp.get("segment_cache_budget", 0)
               + (_POSTINGS_PEAK["budget"] or 0)
               + sum(comp.get(k, 0) for k in _ANON_UNBOUNDED_TERMS))
    return {"components": comp,
            "rss_kb": rss,
            "classes": {
                "anonymous_bytes": anon,
                "file_mapped_bytes": mapped,
                "rss_anon_kb": rss.get("rssanon", 0),
                "rss_file_kb": rss.get("rssfile", 0),
                "rss_shmem_kb": rss.get("rssshmem", 0),
                "rss_mapped_resident_kb": mapped_resident,
                # The formula the anonymous claim is made against, and
                # the slack the accounting cannot name (allocator retention;
                # attributed to glibc's dynamic mmap/trim thresholds).
                "anon_formula_bytes": formula,
                "anon_slack_bytes": max(0, rss.get("rssanon", 0) * 1024 - anon),
                "anon_within_formula": rss.get("rssanon", 0) * 1024 <= formula,
            },
            # kept for continuity with older records; this is the
            # TOTAL, not the anonymous claim
            "accounted_anon_bytes": anon,
            "accounted_mapped_bytes": mapped,
            "open_path": dict(_MEM_PEAK.get("open_path", {})),
            "sampled": _MEM_PEAK["sampled"]}


def _timed(fn: Callable[[], Any]) -> tuple[Any, dict[str, Any]]:
    """Run `fn`, sampling RSS immediately before and after and the wall time
    it took. One entry of `finalisation_phases` (see `build`): a 17.4M-edge
    build's flat-2.4-GB-to-25-GB finalisation spike was traced to the final `compact()`/`gc()`/`stats()`/digest
    sequence, and the existing 60 s-cadence RSS sampling (`_print_progress`)
    could not tell which of those four phases it was in -- they are all
    inside one call with nothing between them. This isolates each one."""
    before = _rss_kb()
    t0 = time.time()
    result = fn()
    wall_s = time.time() - t0
    after = _rss_kb()
    return result, {"rss_kb_before": before, "rss_kb_after": after, "wall_s": round(wall_s, 3)}


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _merge_reports(a: Any, b: Any) -> Any:
    """Fold two compaction reports into one, for the drain loop.

    Sums what accumulates (rows rewritten, closes folded, clusters touched,
    engine wall), keeps the first `segments_before` and the last
    `segments_after` -- the two ends of the whole drain -- and leaves
    anything it does not recognise as the later report's value, so a new
    engine field is carried through rather than dropped.
    """
    if not isinstance(a, dict) or not isinstance(b, dict):
        return b
    out = {**a, **b}
    for key in ("rows_rewritten", "closes_folded", "clusters_touched",
                "edge_rows", "node_rows", "wall_us"):
        if key in a and key in b:
            out[key] = a[key] + b[key]
    if "segments_before" in a:
        out["segments_before"] = a["segments_before"]
    return out


def _compaction_record(report: Any, *, ops: int, wall_s: float, gc_wall_s: float,
                       segment_bytes_before: int, segment_bytes_after: int,
                       phase: str) -> dict[str, Any]:
    """One entry of `build_info.compaction_records`.

    `report` is whatever the adapter's `compact()` returned -- a dict from
    the native engine's `CompactionReport`, or `None`/something else from a
    backend that does not report. Unknown keys are simply absent rather than
    invented, so a record never claims a number the engine did not produce;
    `rows_rewritten` falls back to `edge_rows + node_rows`, which is exactly
    what the whole-store path rewrites, for an engine built before the
    counter existed.

    `wall_s` is measured here and `engine_wall_us` inside the engine: the
    difference is the PyO3 boundary plus the adapter, and keeping both means
    a merge rows/s figure can be quoted against either without the reader
    having to guess which one was used.
    """
    r = report if isinstance(report, dict) else {}
    rows = r.get("rows_rewritten")
    if rows is None and ("edge_rows" in r or "node_rows" in r):
        rows = int(r.get("edge_rows", 0)) + int(r.get("node_rows", 0))
    rec: dict[str, Any] = {
        "phase": phase,
        "ops": ops,
        "wall_s": round(wall_s, 3),
        "gc_wall_s": round(gc_wall_s, 3),
        "segment_bytes_before": segment_bytes_before,
        "segment_bytes_after": segment_bytes_after,
    }
    if rows is not None:
        rec["rows_rewritten"] = int(rows)
        elapsed = wall_s if wall_s > 0 else None
        rec["rows_per_s"] = round(rows / elapsed) if elapsed else None
    for key in ("segments_before", "segments_after", "closes_folded",
                "clusters_touched", "incremental"):
        if key in r:
            rec[key] = r[key]
    if "wall_us" in r:
        rec["engine_wall_us"] = int(r["wall_us"])
    return rec


def _store_bytes(out: Path) -> dict[str, int]:
    native = out / "native"
    manifest_bytes = _dir_bytes(native / "manifests")
    segment_bytes = _dir_bytes(native / "seg")
    return {"manifest_bytes": manifest_bytes, "segment_bytes": segment_bytes,
            "total_bytes": _dir_bytes(out)}


def _index_files(out: Path) -> dict[str, int]:
    """Sidecar index files on disk under `native/idx/`, per kind.

    The TCSR and name indexes are built lazily at first query, so a build
    writes none of those, and their zero here is the evidence the phase
    report cites for their zero build time. The one exception is by design:
    The identity filters (`idx/vidfilter/`) are built **at seal**, because
    for that index the first use is the entire cost it removes. Their count
    is non-zero on any build that sealed a segment, and their build time is
    not residual -- it is attributed to `commit.identity_filter` and
    `compaction.identity_filter`."""
    idx = out / "native" / "idx"
    if not idx.exists():
        return {}
    return {d.name: sum(1 for p in d.rglob("*") if p.is_file())
            for d in sorted(idx.iterdir()) if d.is_dir()}


def _tcsr_counts(store: Any) -> dict[str, Any] | None:
    fn = getattr(getattr(store, "adapter", None), "tcsr_stats", None)
    if not callable(fn):
        return None
    try:
        got = fn()
    except Exception:  # noqa: BLE001 -- a receipt must never fail a build
        return None
    return {k: got[k] for k in ("built", "loaded", "segments_cached") if k in got}


def _git_commit() -> str:
    env = os.environ.get("GNOMON_COMMIT")
    if env:
        return env
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                               capture_output=True, text=True, check=True).stdout.strip()
        return f"{sha}-dirty" if dirty else sha
    except Exception:                                  # noqa: BLE001
        return "0" * 40


def _ram_gb() -> float:
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024 ** 3), 2)
    except (ValueError, OSError, AttributeError):       # pragma: no cover
        return 1.0


def _manifest_identity(store: Any) -> dict[str, Any]:
    """O(1) store identity -- generation plus the engine's own manifest sha
    (a pre-computed field, `crates/gnomon-py/src/lib.rs::manifest_sha`,
    never a store scan) -- available regardless of `digest_mode`.

    Mirrors `build_snb_store.py::_identity`'s `manifest` mode, which exists
    for exactly this reason there: a cheap, real identity for a store too
    big to digest in full. Used here as the `--digest none`/`streaming`
    fallback for the sidecar's schema-required `dataset.digest` (and, for
    `none`, `result_digest` too) -- see `_make_record`."""
    out: dict[str, Any] = {}
    try:
        out["generation"] = store.adapter.generation
    except Exception:                                   # noqa: BLE001
        pass
    try:
        out["manifest_sha"] = store.adapter._store.manifest_sha()  # noqa: SLF001
    except Exception:                                   # noqa: BLE001
        pass
    return out


def _engine_build_info() -> dict[str, Any] | None:
    try:
        from gnomon.storage.native import NativeAdapter
    except ImportError:
        return None
    return NativeAdapter.build_info()


# --------------------------------------------------------------------- #
# resumable, streaming build
# --------------------------------------------------------------------- #

def _progress_path(out: Path) -> Path:
    return out / ".build_progress.json"


def _load_progress(out: Path) -> dict[str, Any] | None:
    p = _progress_path(out)
    return json.loads(p.read_text()) if p.exists() else None


def _save_progress(out: Path, state: dict[str, Any]) -> None:
    _progress_path(out).write_text(json.dumps(state, sort_keys=True))


class _StopBuild(Exception):
    """Raised internally to unwind out of the phase loop at --stop-at-ops."""


class Builder:
    """Drives one (possibly resumed) build through `PHASES` in order,
    committing every op through the normal `Store` write API, compacting on
    cadence, and printing progress every `PROGRESS_EVERY_OPS` ops."""

    def __init__(self, store: Any, out: Path, n_entities: int, seed: int,
                batch: int, compact_every: int, stop_at_ops: int | None,
                ops_done: int, since_compact: int = 0,
                event: Callable[..., dict[str, Any]] = _event,
                clock: "ingest_phases.PhaseClock | None" = None) -> None:
        self.store = store
        # Per-phase ingest accounting (`gnomon/eval/ingest_phases.py`): the
        # driver brackets its own steps with `self.clock.section(...)`; a
        # disabled clock makes every bracket a no-op.
        self.clock = clock if clock is not None else ingest_phases.PhaseClock(enabled=False)
        self.out = out
        self.n_entities = n_entities
        self.seed = seed
        self.batch = batch
        # Long-interval knob: defaults to the module-level `_event`
        # itself, so a caller that never passes `event` gets exactly the
        # base bulk phase.
        self.event = event
        self.compact_every = compact_every
        # `--compact-every 0` stops
        # meaning "never compact" and starts meaning "consult the engine's
        # own trigger after every batch, and rewrite only the eligible
        # key-range clusters". A positive value keeps the op cadence with the
        # whole-store compactor, which is what a comparison baseline needs --
        # the two configurations differ in both the trigger and the compactor, because
        # a cadence-driven incremental compaction would measure neither.
        # `--no-compact` is the configuration `0` used to name.
        self.incremental = compact_every == 0
        self.stop_at_ops = stop_at_ops
        self.ops = ops_done
        # Persisted exactly (not recomputed as `ops_done % compact_every`):
        # a compaction resets this to 0 even when the triggering tick
        # overshot the threshold, so the true remainder can drift from a
        # clean multiple of compact_every after many compactions.
        self._since_compact = since_compact
        self._last_progress_mark = ops_done // PROGRESS_EVERY_OPS
        self.t0 = time.time()
        self.compactions = 0
        # One record per compaction, so merge rows/s and the total rows
        # rewritten over a build are read off the build record rather than
        # estimated from checkpoint deltas, which carry no per-compaction
        # timer. Kept in memory for this
        # process only: like `compactions` itself, a --resume starts the
        # list empty rather than claiming to know what an earlier process
        # rewrote.
        self.compaction_records: list[dict[str, Any]] = []

    # -- bookkeeping shared by every phase ------------------------------ #

    def _resume_state(self, phase: str, next_i: int | None) -> dict[str, Any]:
        return {"phase": phase, "next_i": next_i, "ops_done": self.ops,
                "since_compact": self._since_compact,
                "n_entities": self.n_entities, "seed": self.seed}

    def _tick(self, n: int, phase: str, next_i: int | None) -> None:
        """`phase`/`next_i` describe where a build should resume if it stops
        right after this call -- for an indexed/bulk phase that is always
        *this* phase at the next index (the range self-corrects to a no-op
        if it turns out to be exhausted); for a one-shot phase (`n1_assert`,
        `epoch2_node`) the op above already fully completed it, so the
        caller passes the *next* phase name instead (see `run_single`) —
        otherwise a resume would re-run an already-committed op."""
        self.ops += n
        self._since_compact += n
        if self.compact_every > 0 and self._since_compact >= self.compact_every:
            self._compact()
        elif self.incremental and self._needs_compaction():
            self._compact()
        mark = self.ops // PROGRESS_EVERY_OPS
        if mark > self._last_progress_mark:
            self._last_progress_mark = mark
            with self.clock.section("client.driver.progress"):
                self._print_progress()
                # Piggyback a checkpoint on the same cadence: an unclean exit
                # (OOM, kill -9, power loss) with no --stop-at-ops still leaves a
                # progress file no older than PROGRESS_EVERY_OPS ops behind,
                # instead of only ever checkpointing at phase boundaries.
                _save_progress(self.out, self._resume_state(phase, next_i))
        elif n < max(self.batch, 2):
            # A point op -- one of the ~262 `correct`/`retract` calls the
            # post-bulk phases make at any scale. Each resolves an identity
            # against the whole committed store, which is where the 1B
            # build's entire RSS ramp happened; at 500k ops per progress
            # line these 262 ops fire none, so the peak they produce was
            # invisible to the existing series. Sampled per op instead, and
            # bounded by construction: `_phase_ranges` makes the post-bulk
            # phases ~262 ops whatever `n_entities` is.
            with self.clock.section("client.driver.progress"):
                _sample_memory(self.store)
        if self.stop_at_ops is not None and self.ops >= self.stop_at_ops:
            _save_progress(self.out, self._resume_state(phase, next_i))
            raise _StopBuild()

    def _needs_compaction(self) -> bool:
        """The engine's own compaction trigger: is any key-range cluster eligible?

        Costs no segment open -- it is arithmetic over the manifest's key
        ranges and row counts plus the already-cached close index -- which
        is what makes it affordable once per batch. An adapter without the
        predicate (DuckDB) never triggers, exactly as it never compacts.
        """
        with self.clock.section("client.driver.trigger"):
            needs = getattr(self.store.adapter, "needs_compaction", None)
            return bool(needs()) if callable(needs) else False

    def _compactor(self) -> tuple[Callable[[], Any] | None, str]:
        """The compaction entry point this build uses, and its name
        (`incremental` or `whole-store`).

        Trigger-driven builds (`--compact-every 0`) call the incremental
        compactor; cadence-driven ones call the whole-store one. Falling
        back to `compact` when the adapter has no `compact_incremental`
        keeps an older engine (or the DuckDB adapter) working rather than
        silently skipping compaction -- but it also means a build record
        must be read for `incremental`, not for the flag, to know which compactor
        actually ran.
        """
        if self.incremental:
            fn = getattr(self.store.adapter, "compact_incremental", None)
            if fn is not None:
                return fn, "incremental"
        return getattr(self.store.adapter, "compact", None), "whole-store"

    def _compact(self) -> None:
        with self.clock.section("client.driver.compaction"):
            self._compact_inner()

    def _compact_inner(self) -> None:
        compact, arm = self._compactor()
        gc = getattr(self.store.adapter, "gc", None)
        if compact is not None and gc is not None:
            with self.clock.section("client.driver.dir_scan"):
                seg_before = _dir_bytes(self.out / "native" / "seg")
            t0 = time.time()
            report = compact()
            compact_s = time.time() - t0
            t1 = time.time()
            gc(keep_last=2)
            gc_s = time.time() - t1
            self.compactions += 1
            with self.clock.section("client.driver.dir_scan"):
                seg_after = _dir_bytes(self.out / "native" / "seg")
            self.compaction_records.append(_compaction_record(
                report, ops=self.ops, wall_s=compact_s, gc_wall_s=gc_s,
                segment_bytes_before=seg_before,
                segment_bytes_after=seg_after,
                phase=arm))
        self._since_compact = 0

    def _print_progress(self) -> None:
        elapsed = time.time() - self.t0
        rate = self.ops / elapsed if elapsed > 0 else 0.0
        rss = _rss_kb()
        nbytes = _store_bytes(self.out)
        postings_stats = getattr(self.store.adapter, "postings_stats", None)
        postings = postings_stats() if callable(postings_stats) else None
        _sample_postings(self.store)
        _sample_memory(self.store)
        phases = ""
        if self.clock.enabled:
            engine = ingest_phases.engine_stats(self.store)
            ingest_phases.sample(self.clock, engine, self.ops)
            phases = ingest_phases.progress_summary(self.clock, engine)
        print(f"  {self.ops:>14,} ops  {rate:>10,.0f} ops/s  elapsed {elapsed:>8.1f}s  "
              f"maxrss {rss.get('maxrss_kb', 0) / 1024:.0f} MB  "
              f"manifest {nbytes['manifest_bytes']:,} B  segment {nbytes['segment_bytes']:,} B  "
              f"compactions {self.compactions}"
              + (f"  postings {postings}" if postings is not None else "")
              + phases, flush=True)

    # -- phases ---------------------------------------------------------- #

    def run_bulk(self, next_i: int) -> None:
        n_entities, seed, batch = self.n_entities, self.seed, self.batch
        lo = next_i
        while lo < n_entities:
            hi = min(lo + batch, n_entities)
            with self.clock.section("client.event_generation"):
                rows = [self.event(i, n_entities, seed) for i in range(lo, hi)]
            self.store.ingest_events(rows)
            lo = hi
            self._tick(len(rows), "bulk", lo)

    def run_single(self, next_phase: str, next_i: int | None,
                  fn: Callable[[], Any]) -> None:
        """Run a one-shot op (`n1_assert`, `epoch2_node`). `next_phase`/
        `next_i` is where a build resumes if it stops right after -- always
        the *next* phase, since the single op above is unconditionally done
        once `fn()` returns and re-running it would duplicate a commit."""
        with self.clock.section("client.correction_ops"):
            fn()
        self._tick(1, next_phase, next_i)

    def run_indexed(self, phase: str, idx_range: range,
                    op_fn: Callable[[int], Any]) -> None:
        step = idx_range.step
        section = self.clock.section
        for i in idx_range:
            with section("client.correction_ops"):
                op_fn(i)
            self._tick(1, phase, i + step)


def _phase_ranges(n_entities: int) -> dict[str, range]:
    """Index domains for the range-based phases -- exactly `build_dataset`'s
    own loops (`scripts/eval_harness.py`)."""
    step = max(1, n_entities // 200)
    life, mid = edge_life(n_entities), n_entities // 2
    return {
        "epoch2_edges": range(0, n_entities, step),
        "partial": range(mid - life // 2, mid, max(1, life // 20)),
        "retract": range(0, n_entities, step * 4),
    }


def build(out: Path, n_entities: int, seed: int, batch: int, compact_every: int,
         backend: str, resume: bool, stop_at_ops: int | None,
         digest_mode: str = "full", *,
         tick_us: int = 1, interval_fraction: float = 0.0,
         interval_length_dist: str = "pow2",
         short_life_us: int | None = None,
         named_fraction: float = 0.0,
         phase_accounting: bool = True) -> dict[str, Any]:
    """Run (or resume) one build. Returns a result dict; `result["complete"]`
    is False if `--stop-at-ops` cut the build short.

    `digest_mode` picks the final digest pass: `full` computes both
    `store.digest()` and `content_digest()` exactly as before (the default
    here, matching every existing caller of this function that predates
    `--digest`); `streaming` computes `store.digest_streaming()` instead of
    `store.digest()`, in bounded memory; `none` skips both. `main()` resolves
    the CLI's scale-conditioned default (`default_digest_mode`) before
    calling this -- this parameter is never itself "auto".

    `tick_us`/`interval_fraction`/`interval_length_dist`/`short_life_us` are
    the long-interval `synth-li-*` knobs; at their defaults every call this
    function makes to `event(...)` below is byte-identical to the base
    `_event(i, n_entities, seed)` (see `_event`'s own docstring) -- this is
    what makes `--interval-fraction 0 --tick-us 1` (`main`'s CLI defaults)
    reproduce `synth-1m`/`synth-10m` exactly.

    `named_fraction` (default 0.0, a no-op -- see `_named_node_indices`)
    gives that fraction of nodes a real `name` prop, asserted once up
    front on a fresh build, before `run_bulk`'s own auto-created bare
    nodes exist for anything but `n1` -- so the name index has more than
    a constant 2 entries to hold at any scale.

    `phase_accounting` (default on) records `ingest_phase_totals` -- the
    per-phase split of ingest time (`gnomon/eval/ingest_phases.py`) -- into the
    build record. Off, no wrapper is installed and no bracket is timed; the
    engine's own timers are governed by how the extension was built
    (`build_info()["ingest_phase_timers"]`), not by this flag."""
    if digest_mode not in DIGEST_MODES:
        raise SystemExit(f"--digest must be one of {DIGEST_MODES}, got {digest_mode!r}")
    existing = (out / "eventlog.jsonl").exists()
    if existing and not resume:
        raise SystemExit(f"{out} already has a store -- pass --resume to continue it, "
                         f"or remove it deliberately and re-run.")
    state = _load_progress(out) if existing else None
    if resume and existing and state is None:
        raise SystemExit(f"{out} exists but has no {_progress_path(out).name} -- cannot "
                         f"safely resume (it may already be a finished build, or was not "
                         f"built by this script).")
    if state is not None:
        if state["n_entities"] != n_entities or state["seed"] != seed:
            raise SystemExit(f"resume parameter mismatch: in-progress build was "
                             f"--n-entities {state['n_entities']} --seed {state['seed']}, "
                             f"this invocation asked for --n-entities {n_entities} "
                             f"--seed {seed}")
        phase, next_i, ops_done = state["phase"], state.get("next_i"), state["ops_done"]
        since_compact = state.get("since_compact", 0)
    else:
        phase, next_i, ops_done, since_compact = "bulk", 0, 0, 0

    event = functools.partial(
        _event, tick_us=tick_us, interval_fraction=interval_fraction,
        interval_length_dist=interval_length_dist, short_life_us=short_life_us,
    )
    store = gnomon.open(out, backend=backend)
    # From here on, every RSS sample also leaves the fold and
    # filter counters on disk, and `kill -USR1 <pid>` takes a reading on
    # demand -- so a build a stopping rule kills mid-correction still yields
    # a fold count instead of a wall and no mechanism.
    _arm_instruments(store, out)
    clock = ingest_phases.PhaseClock(enabled=phase_accounting)
    uninstrument = ingest_phases.instrument(store, clock)
    if clock.enabled:
        ingest_phases.reset_engine_stats(store)
    b = Builder(store, out, n_entities, seed, batch, compact_every, stop_at_ops, ops_done,
               since_compact=since_compact, event=event, clock=clock)
    # Ingest time is `b.t0` .. `wall` below; the clock's root spans exactly
    # that interval, so its residual is what no bracket claimed.
    clock.start()
    ranges = _phase_ranges(n_entities)
    order = PHASES
    start_idx = order.index(phase)

    def resumed(idx_range: Any) -> Any:
        if next_i is None:
            return idx_range
        return range(max(idx_range.start, next_i), idx_range.stop, idx_range.step)

    # Named-node pre-pass: a one-shot step ahead of the phase pipeline
    # proper (not a resumable PHASES entry -- see `named_fraction`'s
    # docstring note), so it must run before `run_bulk` creates bare
    # versions for these same uids, and must run at most once per build --
    # a fresh build only (`phase == "bulk" and next_i in (0, None)`), never
    # on a resume that is already past its start.
    if named_fraction > 0.0 and phase == "bulk" and next_i in (0, None):
        # One chunked call, not one `assert_node` per node. The
        # per-node form writes **one batch, one manifest and one tiny
        # segment each** -- the cost `ingest_events`' own docstring warns
        # about in as many words ("`assert_node`-per-node, which writes one
        # batch and one manifest each and costs O(N^2) in both time and
        # bytes", measured there at 8,435 nodes producing 8,436 manifests
        # and 15 GB).
        #
        # It is also outside `Builder._tick`, so no compaction policy
        # is consulted while it runs: not the trigger under
        # `--compact-every 0`, and not the op counter under a positive
        # cadence. At `--named-fraction 0.1` that is `n_nodes // 10`
        # untracked commits -- ~1,000 at 1M and ~30,000 at 30M -- and the
        # 30M segment census caught exactly that: the first compaction in
        # *every* configuration saw ~29,900 live segments, past
        # `GNOMON_PIN_MAX_SEGMENTS` (16,384) where pin-at-open
        # degrades to lazy per-segment loading.
        # Reproduced at 1M before this change: 1,041 -> 138 -> 28 -> 16.
        #
        # Batching fixes it at the source rather than by compacting the mess
        # away: ~30,000 named nodes become one write batch and one segment,
        # so there is no transient for a trigger to have to catch up with.
        # These nodes are written before `run_bulk` precisely so they do not
        # supersede an auto-created bare version (`_ingest_node_rows`
        # refuses that), and batching does not change it.
        with clock.section("client.event_generation"):
            named = [
                {"uid": f"n{k}", "label": "Node", "props": {"name": f"entity{k}"},
                 "vt_s": 0, "vt_e": n_entities}
                # n1 gets its own name below ("alpha"/"alpha-corrected")
                for k in _named_node_indices(n_entities, seed, named_fraction)
                if k != 1
            ]
        if named:
            store.ingest_events([], nodes=named)

    try:
        if start_idx <= order.index("bulk"):
            b.run_bulk(next_i if phase == "bulk" else 0)
            with clock.section("client.driver.progress"):
                _save_progress(out, b._resume_state("n1_assert", None))
        if start_idx <= order.index("n1_assert"):
            b.run_single("epoch2_edges", ranges["epoch2_edges"].start, lambda: store.assert_node(
                "n1", "Node", {"name": "alpha"}, vt_s=0, vt_e=n_entities))
            with clock.section("client.driver.progress"):
                _save_progress(out, b._resume_state("epoch2_edges", ranges["epoch2_edges"].start))
        if start_idx <= order.index("epoch2_edges"):
            r = resumed(ranges["epoch2_edges"]) if phase == "epoch2_edges" else ranges["epoch2_edges"]
            b.run_indexed("epoch2_edges", r, lambda i: store.correct(
                _edge_ref(i, n_entities, seed), {"weight": 2},
                vt_s=event(i, n_entities, seed)["vt_s"],
                vt_e=event(i, n_entities, seed)["vt_e"]))
            with clock.section("client.driver.progress"):
                _save_progress(out, b._resume_state("epoch2_node", None))
        if start_idx <= order.index("epoch2_node"):
            b.run_single("partial", ranges["partial"].start, lambda: store.correct(
                EntityRef(kind="node", uid="n1"), {"name": "alpha-corrected"},
                vt_s=0, vt_e=n_entities))
            with clock.section("client.driver.progress"):
                _save_progress(out, b._resume_state("partial", ranges["partial"].start))
        if start_idx <= order.index("partial"):
            life = edge_life(n_entities)
            r = resumed(ranges["partial"]) if phase == "partial" else ranges["partial"]
            b.run_indexed("partial", r, lambda i: store.correct(
                _edge_ref(i, n_entities, seed), {"weight": 3},
                vt_s=event(i, n_entities, seed)["vt_s"] + life // 2,
                vt_e=event(i, n_entities, seed)["vt_e"]))
            with clock.section("client.driver.progress"):
                _save_progress(out, b._resume_state("retract", ranges["retract"].start))
        if start_idx <= order.index("retract"):
            r = resumed(ranges["retract"]) if phase == "retract" else ranges["retract"]
            b.run_indexed("retract", r, lambda i: store.retract(
                _edge_ref(i, n_entities, seed),
                event(i, n_entities, seed)["vt_s"] + edge_life(n_entities) // 2))
            with clock.section("client.driver.progress"):
                _save_progress(out, b._resume_state("done", None))
    except _StopBuild:
        clock.stop()
        wall = time.time() - b.t0
        uninstrument()
        stats = store.stats()
        result = {"complete": False, "ops": b.ops, "wall_s": round(wall, 3),
                  "stats": stats, "compactions": b.compactions,
                  "compaction_records": b.compaction_records,
                  "rss": _rss_kb(), "bytes": _store_bytes(out)}
        store.close()
        return result

    # done: fold the tail, final measurements. compact/gc/stats/digest are
    # timed and RSS-sampled individually into `finalisation_phases` -- see
    # `_timed` -- instead of going through `Builder._compact()` (which folds
    # compact+gc into one untimed call), so a build-record can show which of
    # the four actually spiked.
    finalisation_phases: dict[str, Any] = {}
    # the end-of-build window starts here: the phase report splits at it
    clock.mark("finalisation", ingest_phases.engine_stats(store) if clock.enabled else {})
    section = clock.section
    compact_fn, arm = b._compactor()
    gc_fn = getattr(store.adapter, "gc", None)
    if compact_fn is not None and gc_fn is not None:
        with section("client.driver.dir_scan"):
            seg_before = _dir_bytes(out / "native" / "seg")
        # The incremental compactor merges one size tier per eligible
        # cluster per call, so the *final* fold drains the trigger rather
        # than calling once: a build record whose store is still eligible
        # for compaction would make the segment-count and query-floor
        # columns of the two compactors incomparable. The whole-store compactor
        # needs exactly one call, by construction. The cap is a guard, not
        # a policy -- draining should take a handful of rounds, and
        # exhausting it means the trigger is not converging, which the
        # record should show rather than hide in an infinite loop.
        def _drain() -> Any:
            with section("client.driver.compaction"):
                acc = compact_fn()
                if arm != "incremental":
                    return acc
                for _ in range(FINAL_COMPACT_ROUNDS):
                    if not b._needs_compaction():
                        break
                    acc = _merge_reports(acc, compact_fn())
                return acc

        with section("client.driver.progress"):
            _sample_postings(store)
            _sample_memory(store)
        report, finalisation_phases["compact"] = _timed(_drain)
        with section("client.driver.progress"):
            _sample_postings(store)
            _sample_memory(store)
        _, finalisation_phases["gc"] = _timed(lambda: gc_fn(keep_last=2))
        with section("client.driver.progress"):
            _sample_postings(store)
            _sample_memory(store)
        # A sibling key, not a field of the `compact` phase: every entry of
        # `finalisation_phases` is a `_timed` record of exactly
        # {rss_kb_before, rss_kb_after, wall_s}, and
        # `tests/test_build_synth_store.py` pins that shape.
        finalisation_phases["compact_arm"] = {
            "arm": arm,
            "still_eligible": b._needs_compaction(),
        }
        b.compactions += 1
        with section("client.driver.dir_scan"):
            seg_after = _dir_bytes(out / "native" / "seg")
        b.compaction_records.append(_compaction_record(
            report, ops=b.ops,
            wall_s=finalisation_phases["compact"]["wall_s"],
            gc_wall_s=finalisation_phases["gc"]["wall_s"],
            segment_bytes_before=seg_before,
            segment_bytes_after=seg_after,
            phase="final"))
    b._since_compact = 0
    # End of ingest time. Everything after this line (stats, digest) is a
    # post-ingest measurement and is reported in `finalisation_phases` only.
    clock.stop()
    wall = time.time() - b.t0
    engine_phases = ingest_phases.engine_stats(store) if clock.enabled else {}
    uninstrument()
    stats, finalisation_phases["stats"] = _timed(store.stats)
    _sample_postings(store)
    _sample_memory(store)

    # The pass this driver's own `--digest` flag exists to make optional at
    # scale (module docstring "Digest equivalence"; `DIGEST_AUTO_THRESHOLD`).
    # `content_digest()` walks the whole store exactly as `store.digest()`
    # does (see its own docstring), so it is gated identically -- computing
    # it under `streaming` would defeat the point of choosing `streaming`.
    def _digest_pass() -> tuple[str | None, str | None, int | None]:
        if digest_mode == "full":
            cd_report = content_digest_report(store)
            return store.digest(), cd_report["digest"], cd_report["ambiguous_ties"]
        if digest_mode == "streaming":
            return store.digest_streaming(), None, None
        return None, None, None  # "none"

    (store_digest, cdigest, cdigest_ties), finalisation_phases["digest"] = _timed(_digest_pass)
    _sample_postings(store)
    _sample_memory(store)
    rss = _rss_kb()
    nbytes = _store_bytes(out)
    manifest_identity = _manifest_identity(store)
    phase_totals = None
    if clock.enabled:
        # after ingest time has stopped: calibrate the one per-row Python
        # step the report splits by estimate (identity hashing), on a sample
        # of this build's own events
        stride = max(1, n_entities // 20_000)
        hashing = ingest_phases.calibrate_hashing(
            (event(i, n_entities, seed) for i in range(0, n_entities, stride)),
            tt=int(time.time() * 1e6))
        phase_totals = ingest_phases.report(
            clock, engine_phases,
            rows_hashed=(n_entities, n_nodes(n_entities)),
            hashing=hashing,
            extra={"sidecar_index_files": _index_files(out),
                   "tcsr_stats_after_ingest": _tcsr_counts(store)})
    store.close()
    _progress_path(out).unlink(missing_ok=True)
    result = {"complete": True, "ops": b.ops, "wall_s": round(wall, 3), "stats": stats,
             "compactions": b.compactions, "compaction_records": b.compaction_records,
             "digest_mode": digest_mode,
             "store_digest": store_digest, "content_digest": cdigest,
             "content_digest_ambiguous_ties": cdigest_ties,
             "manifest_identity": manifest_identity, "rss": rss, "bytes": nbytes,
             "finalisation_phases": finalisation_phases,
             "postings_peak_bytes": _POSTINGS_PEAK["bytes"],
             "postings_budget": _POSTINGS_PEAK["budget"],
             "postings_detail": dict(_POSTINGS_PEAK["detail"]),
             "memory_accounting_peak": _memory_peak_receipt(),
             "postings_detail_by_kind":
                 (_INSTRUMENTS["last"] or {}).get("postings_detail"),
             "vidfilter_stats": (_INSTRUMENTS["last"] or {}).get("vidfilter_stats"),
             "ingest_phase_totals": phase_totals}
    record = _make_record(out, n_entities, seed, batch, compact_every, backend, result,
                          tick_us=tick_us, interval_fraction=interval_fraction,
                          interval_length_dist=interval_length_dist,
                          short_life_us=short_life_us, named_fraction=named_fraction)
    (out / "build-record.json").write_text(json.dumps(record, indent=1, sort_keys=True))
    result["record"] = record
    return result


# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #

def _safe_relative(p: Path) -> bool:
    try:
        p.relative_to(ROOT)
        return True
    except ValueError:
        return False


def _dataset_identity(result: dict[str, Any]) -> dict[str, Any]:
    """The sidecar's `dataset` block: a real `store_digest` when one was
    computed (`full` or `streaming`), else the O(1) manifest identity
    (`_manifest_identity`, always available) -- `digest_kind: "manifest"`,
    the same fallback `build_snb_store.py::_identity`'s own `manifest` mode
    uses for the same reason. Never null: the schema requires a non-empty
    `digest` string unconditionally, and a `none`-mode build still owes the
    sidecar *some* real, cheap identity for the store it built."""
    if result["store_digest"] is not None:
        return {"name": "synth", "digest": result["store_digest"],
                "digest_kind": "store_digest"}
    manifest_sha = (result.get("manifest_identity") or {}).get("manifest_sha")
    return {"name": "synth", "digest": manifest_sha or "unavailable",
            "digest_kind": "manifest"}


def _result_digest(result: dict[str, Any], dataset: dict[str, Any]) -> str:
    """The schema's top-level `result_digest` (non-empty, required
    unconditionally): `content_digest` when computed (`full`), else the same
    real digest already picked for `dataset.digest` above -- reusing it
    rather than inventing a second placeholder, since under `streaming` that
    is a real `store_digest` and under `none` it is the same manifest
    identity `dataset.digest` already fell back to."""
    return result["content_digest"] or dataset["digest"]


def _make_record(out: Path, n_entities: int, seed: int, batch: int, compact_every: int,
                 backend: str, result: dict[str, Any], *,
                 tick_us: int = 1, interval_fraction: float = 0.0,
                 interval_length_dist: str = "pow2",
                 short_life_us: int | None = None,
                 named_fraction: float = 0.0) -> dict[str, Any]:
    """`benchmarks/schema/result_manifest.schema.json`-conforming sidecar for
    one completed build, plus a `build_info` block with everything this
    driver measured. Written by `build()` itself (not `main()`) so a direct
    caller -- the test suite included -- gets the same sidecar a CLI run
    would, without going through `argparse`.

    `--digest none`/`streaming` (see `build`) leave `result["store_digest"]`
    and/or `result["content_digest"]` as `None` -- `_dataset_identity` and
    `_result_digest` supply the schema's non-null fallbacks; `build_info`
    below carries the literal `None`s (and `digest_mode` itself) so a reader
    can always tell which pass was actually skipped, rather than confusing a
    fallback identity for a real content digest."""
    sha = _git_commit()
    record_path = out / "build-record.json"
    dataset = _dataset_identity(result)
    return {
        "schema_version": SCHEMA_VERSION,
        "git_commit": sha,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": {"host": platform.node(), "platform": platform.platform(),
                   "cpus": os.cpu_count() or 1, "ram_gb": _ram_gb()},
        "config": {"n_entities": n_entities, "batch": batch,
                  "compact_every": compact_every, "backend": backend,
                  "digest_mode": result["digest_mode"],
                  # Provenance: at the defaults this is the plain
                  # `synth-*` family; non-defaults mark a `synth-li-*` build.
                  "tick_us": tick_us, "interval_fraction": interval_fraction,
                  "interval_length_dist": interval_length_dist,
                  "short_life_us": short_life_us, "named_fraction": named_fraction},
        "seed": {"value": seed},
        "dataset": dataset,
        "result_digest": _result_digest(result, dataset),
        "protocol": {"warmups": 0, "reps": 1, "ceilings": {}},
        "record": str(record_path.relative_to(ROOT)) if _safe_relative(record_path) else str(record_path),
        "build_info": {
            "engine": _engine_build_info(),
            "n_entities": n_entities,
            "seed": seed,
            "batch": batch,
            "compact_every": compact_every,
            "compactions": result["compactions"],
            # The per-compaction accumulator. `compactions` above
            # stays the integer count every existing reader expects; the
            # detail lands beside it rather than replacing it.
            "compaction_records": result.get("compaction_records", []),
            "compaction_rows_rewritten": sum(
                r.get("rows_rewritten", 0)
                for r in result.get("compaction_records", [])),
            "compaction_wall_s": round(sum(
                r.get("wall_s", 0.0)
                for r in result.get("compaction_records", [])), 3),
            "ops": result["ops"],
            "wall_s": result["wall_s"],
            # Kept under its historic name for continuity with earlier
            # records, but it is the process TOTAL --
            # anonymous heap plus the store's mapped pages plus everything
            # else. The claim about engine memory is `memory_accounting_peak.
            # classes.anonymous_bytes`, not this.
            "peak_rss": result["rss"],
            "peak_rss_is": "total",
            "store_bytes": result["bytes"],
            "stats": result["stats"],
            "digest_mode": result["digest_mode"],
            "store_digest": result["store_digest"],
            "content_digest": result["content_digest"],
            "content_digest_ambiguous_ties": result.get("content_digest_ambiguous_ties"),
            "manifest_identity": result["manifest_identity"],
            "finalisation_phases": result.get("finalisation_phases"),
            # Additive: absent (None) on a build whose adapter has no
            # `memory_accounting`, exactly as `postings_peak_bytes` reads 0
            # on one with no postings receipt.
            "memory_accounting_peak": result.get("memory_accounting_peak"),
            # The identity postings were the second O(store) holder
            # of a build's RSS -- 1.2-1.8 GB of ingest floor stepping to
            # 15.73 GB at 30M the moment the first correction ran, linear in
            # row count (design s13). These two say how much was actually
            # held and under what bound, so a peak-RSS number can be
            # attributed rather than argued about.
            "postings_peak_bytes": result.get("postings_peak_bytes", 0),
            "postings_budget": result.get("postings_budget"),
            "postings_detail": result.get("postings_detail"),
            # The same counters **per row kind**, with that kind's
            # identity-filter probe counters beside its folds, so
            # `folds / filter_probes` -- "a correction folds one segment
            # rather than the store" -- is one division inside one dict.
            # A separate key rather than a second meaning for
            # `postings_detail`, which is the sum over both kinds and stays
            # exactly what it was. `None` on an adapter that carries neither,
            # which is how an older record reads.
            "postings_detail_by_kind": result.get("postings_detail_by_kind"),
            "vidfilter_stats": result.get("vidfilter_stats"),
            # Additive: the per-phase split of `wall_s` (ingest time), or
            # None when the build ran with --phase-accounting off.
            # `gnomon/eval/ingest_phases.py` documents every phase name.
            "ingest_phase_totals": result.get("ingest_phase_totals"),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--n-entities", type=int, required=True,
                    help="event count -- the old generator's --scale, same math "
                        "(n_nodes = max(200, N//100), edge_life = max(40, N//20))")
    ap.add_argument("--seed", type=int, default=0,
                    help="0 reproduces eval_harness.build_dataset exactly (no --seed "
                        "flag exists there); any other value draws an independent, "
                        "still fully reproducible graph")
    ap.add_argument("--batch", type=int, default=DEFAULT_BATCH,
                    help=f"bulk-ingest commit size (default {DEFAULT_BATCH}, "
                        f"the value used for the large builds)")
    ap.add_argument("--compact-every", type=int, default=DEFAULT_COMPACT_EVERY,
                    help=f"positive: whole-store compact()+gc(keep_last=2) every "
                        f"this many ops (default {DEFAULT_COMPACT_EVERY}, "
                        f"the value used for the large builds), the comparison baseline. "
                        f"0: consult the engine's cluster trigger after every "
                        f"batch and compact incrementally "
                        f"-- the setting for scale builds. Negative: no "
                        f"cadence compaction at all; the end-of-build fold "
                        f"still runs in every "
                        f"mode")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--backend", default="native", choices=("native",),
                    help="only native is streaming-safe at this scale")
    ap.add_argument("--resume", action="store_true",
                    help="continue an in-progress build at --out (refused without "
                        "this flag if --out already has a store)")
    ap.add_argument("--stop-at-ops", type=int, default=None,
                    help="stop after this many total ops (A/B-style partial builds); "
                        "resumable with --resume")
    ap.add_argument("--digest", default=None, choices=DIGEST_MODES, dest="digest",
                    help="final digest pass: 'full' computes store.digest() and "
                        "content_digest() as before; 'streaming' computes "
                        "store.digest_streaming() instead, in bounded memory "
                        "(see gnomon/storage/base.py::store_digest_streaming); 'none' "
                        "skips both. Default is scale-conditioned -- 'none' at or "
                        f"above --n-entities {DIGEST_AUTO_THRESHOLD:,}, 'full' below "
                        "it (DIGEST_AUTO_THRESHOLD) -- pass this to override")
    # Long-interval workload.
    # The `synth-li-*` family this drives is a *new* family, never a
    # redefinition of `synth-*`: at the defaults below (tick-us=1,
    # interval-fraction=0) this reproduces the existing family byte-for-byte
    # (see `_event`'s docstring).
    ap.add_argument("--tick-us", default="1",
                    help="microseconds per event tick. '1' reproduces today's "
                        "generator exactly (default). 'auto52w' computes "
                        "tick_us = round(52 * partition_width_us / n_entities) so "
                        "the store spans 52 weeks of valid time "
                        "-- otherwise an integer literal")
    ap.add_argument("--interval-fraction", type=float, default=0.0,
                    help="fraction of edges drawn long-lived, routed to the "
                        "interval lane (default 0.0: no interval lane, matches "
                        "today's generator)")
    ap.add_argument("--interval-length-dist", default="pow2", choices=("pow2",),
                    help="long-edge length distribution: 'pow2' draws "
                        "L = partition_width_us * 2**U, U ~ Uniform{1,2,3} "
                        "(crossings 3..9, always the interval lane)")
    ap.add_argument("--short-life", type=int, default=None, dest="short_life_us",
                    help="short-edge length in microseconds (default: "
                        "edge_life(n_entities) * tick_us ticks, crossings <= 2, "
                        "always the event lane)")
    ap.add_argument("--named-fraction", type=float, default=0.0,
                    help="fraction of nodes given a real, deterministic `name` "
                        "prop up front (default 0.0: only n1 is named, matching "
                        "today's generator and digests -- the name index "
                        "otherwise holds a constant 2 entries at any scale)")
    ap.add_argument("--phase-accounting", default="on", choices=("on", "off"),
                    help="record ingest_phase_totals, the per-phase split of ingest "
                        "time, in the build record (default on); 'off' installs "
                        "no timing wrapper at all")
    args = ap.parse_args()

    if args.tick_us == "auto52w":
        tick_us = max(1, round(52 * PARTITION_WIDTH_US / args.n_entities))
    else:
        try:
            tick_us = int(args.tick_us)
        except ValueError:
            raise SystemExit(f"--tick-us must be an integer or 'auto52w', got {args.tick_us!r}")
    if tick_us <= 0:
        raise SystemExit(f"--tick-us must be positive, got {tick_us}")
    if not 0.0 <= args.interval_fraction <= 1.0:
        raise SystemExit(f"--interval-fraction must be in [0, 1], got {args.interval_fraction}")
    if not 0.0 <= args.named_fraction <= 1.0:
        raise SystemExit(f"--named-fraction must be in [0, 1], got {args.named_fraction}")

    digest_mode = args.digest if args.digest is not None else default_digest_mode(args.n_entities)
    if args.digest is None:
        print(f"digest_mode=auto -> {digest_mode!r} "
              f"(n_entities={args.n_entities:,}, threshold={DIGEST_AUTO_THRESHOLD:,})", flush=True)
    else:
        print(f"digest_mode={digest_mode!r} (explicit)", flush=True)

    sha = _git_commit()
    dataset_name = "synth" if (args.interval_fraction == 0.0 and tick_us == 1) else "synth-li"
    print(f"RUN_STARTED commit={sha} dataset={dataset_name} n_entities={args.n_entities} "
          f"seed={args.seed} batch={args.batch} compact_every={args.compact_every} "
          f"out={args.out} backend={args.backend} resume={args.resume} "
          f"digest={digest_mode} tick_us={tick_us} "
          f"interval_fraction={args.interval_fraction} "
          f"interval_length_dist={args.interval_length_dist} "
          f"short_life_us={args.short_life_us} named_fraction={args.named_fraction} "
          f"host={platform.node()}", flush=True)

    result = build(args.out, args.n_entities, args.seed, args.batch,
                   args.compact_every, args.backend, args.resume, args.stop_at_ops,
                   digest_mode, tick_us=tick_us, interval_fraction=args.interval_fraction,
                   interval_length_dist=args.interval_length_dist,
                   short_life_us=args.short_life_us, named_fraction=args.named_fraction,
                   phase_accounting=args.phase_accounting == "on")

    if not result["complete"]:
        print(f"\nSTOPPED at --stop-at-ops: {result['ops']:,} ops, "
             f"{result['wall_s']}s wall. Resume with the same command plus --resume.")
        return 0

    record_path = args.out / "build-record.json"
    print(f"\nDONE ops={result['ops']:,} wall={result['wall_s']}s "
         f"compactions={result['compactions']} "
         f"maxrss={result['rss'].get('maxrss_kb', 0) / 1024:.0f}MB "
         f"manifest={result['bytes']['manifest_bytes']:,}B "
         f"segment={result['bytes']['segment_bytes']:,}B")
    print(f"digest_mode={result['digest_mode']}")
    print(f"store_digest={result['store_digest']}")
    print(f"content_digest={result['content_digest']}")
    if result.get("content_digest_ambiguous_ties") is not None:
        print(f"content_digest_ambiguous_ties={result['content_digest_ambiguous_ties']}")
    totals = result.get("ingest_phase_totals")
    if totals:
        print(f"ingest_phase_totals: ingest {totals['ingest_s']:.1f}s, "
              f"residual {100 * totals['residual_fraction']:.2f}%")
        for name, row in totals["groups"].items():
            print(f"  {100 * row['fraction']:6.2f}%  {row['s']:10.2f}s  {name}")
    print(f"record: {record_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
