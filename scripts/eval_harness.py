"""Query-registry harness: a fixed set of named queries, a result
canonicalizer, and a run manifest.

* a **query registry** — one stable identifier per logical question, so a
  result can be attributed to a query rather than to a code path;
* a **result canonicalizer** — a hash over the logical answer, ignoring
  presentation, so "same answer" is decidable rather than eyeballed;
* a **run manifest** — enough environment to make a number reproducible, and
  to know when two numbers are not comparable.

It runs the registry on the native engine and, optionally, on the DuckDB
reference backend. The two have identical semantics, so any hash mismatch
between them is a bug.

    python scripts/eval_harness.py --scale 200000
    python scripts/eval_harness.py --scale 200000 --systems native,duckdb
    python scripts/eval_harness.py --store <store dir> --log <its eventlog.jsonl> --json out.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gnomon
from gnomon.core.model import OPEN_END, EntityRef, canonical_json, sha256_hex
from gnomon.temporal.algebra import (
    ENVELOPE_META_FIELDS,
    call_operator,
    ensure_all_registered,
)

ROOT = Path(__file__).resolve().parents[1]

def measurement_host() -> str:
    """The host whose numbers are comparable to each other.

    Reported numbers come from one host, so that two rows of a table are
    always comparable.

    Defaulting to the current host makes an un-configured run "on the
    measurement host" by definition. A deliberate cross-host comparison is
    pinned with `GNOMON_MEASUREMENT_HOST=<name>`.
    """
    return os.environ.get("GNOMON_MEASUREMENT_HOST",
                          platform.node().split(".")[0])


#: Computed once at import time.
MEASUREMENT_HOST = measurement_host()

sys.path.insert(0, str(Path(__file__).resolve().parent))
import provenance  # noqa: E402

#: Fields that describe the *call* rather than the answer. They legitimately
#: differ between systems and runs, so they are excluded from the hash: the
#: dataset extent moves as a store grows, and the digest is itself derived.
#: Derived from the operator layer's own `ENVELOPE_META_FIELDS`, so an
#: envelope key added there cannot reach this hash.
VOLATILE = tuple(dict.fromkeys(ENVELOPE_META_FIELDS + ("result_digest", "cursor")))


@dataclass(frozen=True)
class Query:
    """One logical question, identified independently of how it is answered."""

    id: str
    op: str
    args: dict[str, Any]
    note: str = ""
    #: Semantics a system must support to answer this at all. Recorded so a
    #: system that cannot express one is reported as such rather than as slow.
    requires: tuple[str, ...] = ()


#: Fixed, not scale-relative. Sized to stay inside the cost guardrail at every
#: scale in the sweep while still covering whole communities: a filter of
#: n_nodes/5 answered at 20k events and raised E_COST at 200k, which makes the
#: motif row measure the guardrail rather than the operator.
MOTIF_FILTER = 200

#: The fixed-window variants of three whole-extent queries.
#:
#: `series.count`, `burst.zscore` and `agg.rel_bucket` all use the store's
#: **whole valid-time extent** as their window (`{t_a: t0, t_b: t1}` below),
#: so their cost grows with the store by construction and a "fixed-window
#: p50 grows <= 1.3x from 30M to 1B" claim cannot be read off them at
#: all. Worse, a whole-extent window hides the very property such a claim
#: is about: a probe of segment count against latency found a 10-tick
#: `aggregate_events` had gone 0.85 -> 70.7 ms (83x) while those three
#: registry rows moved only ~3x, because the loss of window selectivity is
#: invisible to a query whose window is everything.
#:
#: The `*.w10` variants below are the same three operator calls with the
#: window replaced by a window of **fixed absolute width**, `W10_WIDTH`
#: ticks, at a fixed absolute offset `W10_OFFSET` past the extent's start.
#: The synthetic corpus puts exactly one event on every tick (`_vt_s`:
#: "event `i` starts at tick `i`"), so at every scale in the sweep this is
#: the same 10 ticks holding the same ~10 events, well clear of the burst
#: band at `scale // 2` — the window is identical from 30M to 1B, and
#: anything the p50 does across scales is the engine, not the query.
#:
#: `_w10_window` clamps for extents too small or too coarse to hold that
#: (unit fixtures, and real corpora such as CollegeMsg whose ticks are
#: microseconds), so the ids are always runnable; on those the window is a
#: fixed *fraction* of the extent instead and the absolute-width claim does
#: not apply. The fixed-width claim is measured on the synthetic sweep.
W10_WIDTH = 10
W10_OFFSET = 1_000


def _w10_window(t0: int, t1: int) -> dict[str, int]:
    """The fixed absolute-width window, placed inside `[t0, t1)`."""
    span = max(1, t1 - t0)
    t_a = t0 + min(W10_OFFSET, span // 4)
    t_b = min(t_a + W10_WIDTH, t1)
    if t_b <= t_a:                       # degenerate extent: never empty
        t_b = min(t_a + 1, t1)
    return {"t_a": int(t_a), "t_b": int(t_b)}


def registry(t0: int, t1: int, tt_epoch1: int,
             filter_uids: tuple[str, ...]) -> list[Query]:
    """The Phase 0 query set: one per operator family, plus belief-time probes.

    Parameters are derived from the dataset's valid-time extent `[t0, t1)`, so
    the same registry is meaningful on a synthetic log whose clock starts at
    zero and on CollegeMsg, whose clock starts in April 2004. `tt_epoch1` is a
    belief boundary that must come from the store: an as_of_tt that sees the
    first write batch but not everything after it — on the synthetic data
    that is pre-correction belief, on a replayed real dataset it is
    mid-ingestion belief. Either way both systems must agree on what was
    believed then, which is what the probe exists to check.
    """
    span = t1 - t0
    mid = t0 + span // 2
    return [
        Query("hist.single", "entity_history", {"uid": "n1"},
              "point lookup by identity"),
        Query("hist.asof", "entity_history", {"uid": "n1", "as_of_tt": tt_epoch1},
              "same lookup under the pre-correction belief",
              requires=("bitemporal",)),
        Query("snap.hop2", "snapshot_subgraph",
              {"seeds": ["n1"], "hops": 2, "t_valid": mid},
              "2-hop neighbourhood at an instant"),
        # t2 - t1 is half an edge lifetime, so the two instants can see
        # different versions of the same edge; a wider gap makes the edge sets
        # disjoint and props_changed empty whatever the data contains.
        Query("diff.global", "diff_snapshots", {"t1": mid, "t2": mid + span // 40},
              "global difference between two instants"),
        Query("reach.window", "temporal_reachability",
              {"src": "n1", "window": {"t_a": t0, "t_b": t0 + span // 10}},
              "time-respecting reachability"),
        Query("paths.k", "temporal_paths",
              {"src": "n1", "dst": "n2", "window": {"t_a": t0, "t_b": t0 + span // 4},
               "k": 3, "max_hops": 3},
              "k shortest time-respecting paths"),
        Query("series.count", "graph_metric_timeseries",
              {"metric": "edge_event_count", "window": {"t_a": t0, "t_b": t1},
               "stride": max(1, span // 100)},
              "bucketed event rate"),
        Query("burst.zscore", "burst_detection",
              {"target": {"kind": "edge_event_rate"}, "window": {"t_a": t0, "t_b": t1},
               "stride": max(1, span // 100), "method": "zscore", "params": {"z": 3.0}},
              "burst flags over the same buckets"),
        Query("nbr.evolution", "neighborhood_evolution",
              {"uid": "n1", "t1": mid, "t2": mid + span // 10},
              "neighbours gained and lost"),
        Query("coactive.narrow", "co_active",
              {"a_spec": {"src": "n1"}, "b_spec": {"src": "n2"},
               "allen_relation": {"relation": "overlaps"}},
              "interval join between two edge sets"),
        Query("resolve.substr", "resolve_entities", {"query": "n1"},
              "entity resolution by substring"),
        # The grouped-aggregation query: the query family a study of
        # analysts' questions found most common, and the shape columnar
        # engines are strongest at. Integer-valued
        # aggregates only, so the cross-system hash is exact. limit covers
        # every group (~2 rels x ~100 buckets), so the *full* answer is
        # what gets verified, not a page.
        Query("agg.rel_bucket", "aggregate_events",
              {"group_by": [{"dim": "rel_type"}, {"dim": "time_bucket"}],
               "aggregates": [{"agg": "count"},
                              {"agg": "count_distinct", "of": "dst"}],
               "window": {"t_a": t0, "t_b": t1},
               "stride": max(1, span // 100), "limit": 1000},
              "count + distinct-dst by rel_type x time-bucket, full window"),
        Query("motif.filtered", "count_temporal_motifs",
              {"motif": "M_triangle_cyclic", "delta": span // 50,
               "window": {"t_a": t0, "t_b": t1},
               "node_filter": list(filter_uids)},
              "delta-motif count, node-filtered to stay inside the guardrail"),
        # Added *after* the core thirteen so their
        # ids and the published query order are unchanged. `resolve.substr`
        # above answers the same uid ("n1") by substring sweep; these two
        # answer it (and, for the prefix case, its one corrected name) through
        # the sorted index instead — same target, different mechanism, which
        # is what makes an "N times faster" comparison apples to apples.
        # Both are native-only micro-benchmarks: the generator gives exactly one node ("n1") an
        # explicit `name` prop (`build_dataset` above), so a name-matching
        # query here exercises the index's *mechanism*, not a realistic
        # name-search workload — the registry has no such workload yet.
        #
        # op="lookup_name" is deliberately not a registered operator
        # (`REGISTRY`): it is an engine capability, not part of the operator
        # algebra. These two queries are instead runnable through a
        # **harness-level bypass**, `run_system`'s `q.op == "lookup_name"` branch, which
        # calls `adapter.lookup_name` directly for a system that has it
        # (native only, today) and reports `ok=False` with an explicit
        # "unsupported" `NotImplementedError` for one that does not
        # (duckdb — no adapter-level equivalent to call trivially; reported
        # as unsupported, never a crash).
        Query("resolve.exact", "lookup_name", {"query": "n1", "mode": "exact"},
              "entity resolution by exact uid, via the sorted name index's "
              "O(1) dictionary path"),
        Query("resolve.prefix", "lookup_name", {"query": "alp", "mode": "prefix"},
              "entity resolution by a 3-character name prefix ('alp' of "
              "n1's corrected name 'alpha-corrected'), via the sorted name "
              "index's binary search"),
        # The fixed-window variants. Appended *after* every
        # existing id, and no existing id's arguments are touched, so the
        # published query order and every prior receipt stay comparable.
        # Same three operators as `series.count` / `burst.zscore` /
        # `agg.rel_bucket` above and the same arguments, except that the
        # window is `_w10_window` — fixed at W10_WIDTH absolute ticks
        # instead of the whole extent — and the stride is 1, so the bucket
        # count is the window width rather than a fixed 100 buckets that
        # would grow with the store. These carry no `op` of their own, so
        # `run_system`, `run_store_mode` and `eval_bitemporal.run_queries`
        # dispatch them through `call_operator` exactly like the whole-
        # extent originals; the SQL/Cypher baselines report them as "no SQL
        # written yet (not a verdict)" the same way any id absent from
        # their own QUERIES map already is.
        Query("series.count.w10", "graph_metric_timeseries",
              {"metric": "edge_event_count", "window": _w10_window(t0, t1),
               "stride": 1},
              f"bucketed event rate over a fixed {W10_WIDTH}-tick window "
              f"(fixed-window variant)"),
        Query("agg.rel_bucket.w10", "aggregate_events",
              {"group_by": [{"dim": "rel_type"}, {"dim": "time_bucket"}],
               "aggregates": [{"agg": "count"},
                              {"agg": "count_distinct", "of": "dst"}],
               "window": _w10_window(t0, t1),
               "stride": 1, "limit": 1000},
              f"count + distinct-dst by rel_type x time-bucket over a fixed "
              f"{W10_WIDTH}-tick window (fixed-window variant)"),
        Query("burst.zscore.w10", "burst_detection",
              {"target": {"kind": "edge_event_rate"},
               "window": _w10_window(t0, t1),
               "stride": 1, "method": "zscore", "params": {"z": 3.0}},
              f"burst flags over the same fixed {W10_WIDTH}-tick window "
              f"(fixed-window variant)"),
        # The belief diff. Appended after
        # every existing id and touching none of their arguments, so the
        # published query order and every prior receipt stay comparable.
        #
        # **Belief times.** `tt_epoch1` is the boundary the whole registry
        # already uses — the first write batch is believed and nothing after
        # it is — and `OPEN_END` is current belief. `(tt_epoch1, OPEN_END]`
        # therefore spans *exactly* the store's correction history, which is
        # the one pair whose meaning is the same on the synthetic corpus, on
        # a replayed real dataset, and at every correction density: "what have
        # our beliefs changed about since the first epoch". A pair of
        # arbitrary literals would mean something different on each.
        #
        # **Window.** `_w10_window`, not the whole extent, for exactly the
        # reason W10_WIDTH's note above gives: a whole-extent window makes the
        # answer — and so the latency — grow with the store by construction,
        # which would hide the property this operator exists to demonstrate.
        # The claim under test is that the cost is `O(segments x (runs +
        # closes))` and therefore does *not* follow the row count or the
        # correction density; a query whose answer grows with both cannot
        # test it. Wider-window shapes belong beside this one, never in
        # place of it.
        #
        # op="diff_belief" is deliberately **not** a registered operator
        # (`REGISTRY`), for the same reason as `lookup_name` (see its note
        # above). It runs through the harness-level bypass
        # `_diff_belief_dispatch` below.
        Query("diff.belief", "diff_belief",
              {"t1": tt_epoch1, "t2": OPEN_END,
               "window": _w10_window(t0, t1), "limit": 1000},
              f"versions whose belief state changed between the first belief "
              f"epoch and current belief, over a fixed {W10_WIDTH}-tick "
              f"valid-time window",
              requires=("bitemporal",)),
    ]


def canonical_hash(payload: dict[str, Any]) -> str:
    """Hash the logical answer, ignoring presentation.

    Two systems agree when this matches. Volatile call metadata is stripped
    first; everything else — including row order, which the operator contract
    fixes — is part of the answer.
    """
    stable = {k: v for k, v in payload.items() if k not in VOLATILE}
    return sha256_hex(canonical_json(stable))[:32]


def _answer_size(payload: dict[str, Any]) -> int:
    """How big the answer is, for operators that do not all use `rows`.

    `diff_snapshots` and `neighborhood_evolution` carry several named lists
    and no `rows` key at all, so reading `len(payload["rows"])` reported them
    as empty — which is how a 999-edge difference showed up in the results
    table as a zero.
    """
    if "rows_total" in payload:
        return int(payload["rows_total"])
    totals = [v for k, v in payload.items()
              if k.endswith("_total") and isinstance(v, int)]
    if totals:
        return sum(totals)
    if "count" in payload:
        return int(payload["count"])
    return len(payload.get("rows", []))


#: Protocol: warmup runs, then 30 measured repetitions for sub-second
#: queries and 10 for slower ones. Earlier revisions reported min-of-3 in a
#: field named p50_ms; the tables produced then were mislabeled best cases.
WARMUP = 5
REPS_FAST = 30
REPS_SLOW = 10


def _measure(fn) -> tuple[Any, list[float]]:
    """Run `fn` per the repetition protocol; return (last payload, timings)."""
    for _ in range(WARMUP):
        payload = fn()
    t0 = time.perf_counter()
    payload = fn()
    first = (time.perf_counter() - t0) * 1e3
    timings = [first]
    for _ in range((REPS_FAST if first < 1000.0 else REPS_SLOW) - 1):
        t0 = time.perf_counter()
        payload = fn()
        timings.append((time.perf_counter() - t0) * 1e3)
    return payload, timings


def _p50_p95(timings: list[float]) -> tuple[float, float]:
    xs = sorted(timings)
    n = len(xs)
    p50 = statistics.median(xs)
    p95 = xs[min(n - 1, max(0, -(-95 * n // 100) - 1))]
    return round(p50, 3), round(p95, 3)


@dataclass
class Result:
    query: str
    ok: bool
    hash: str | None = None
    p50_ms: float | None = None
    p95_ms: float | None = None
    rows: int | None = None
    error: str | None = None
    timings_ms: list[float] | None = None


def _lookup_name_dispatch(adapter: Any, args: dict[str, Any]) -> dict[str, Any]:
    """`resolve.exact`/`resolve.prefix`'s system driver.

    A harness-level bypass of `call_operator`, by design:
    `lookup_name` is not registered in `REGISTRY` (see the note at its two
    `Query` entries above), so this is the only way these two ids run at
    all. Native answers through `NativeAdapter.lookup_name` directly, in the
    same `{rows, rows_total, truncated, cursor}` shape every operator's
    payload takes, so `canonical_hash`/`_answer_size` need no special case.
    Any other backend (duckdb) has no adapter-level equivalent to call
    trivially, so it raises rather than guesses — `run_system`'s existing
    `except Exception` turns that into `ok=False` with the reason in
    `Result.error`: an **unsupported** verdict ("cannot express it;
    reported as such, never as slow"), not a crash.
    """
    kernel = getattr(adapter, "lookup_name", None)
    if kernel is None:
        raise NotImplementedError(
            f"unsupported: {type(adapter).__name__} has no lookup_name driver"
        )
    extra = {k: v for k, v in args.items() if k not in ("query", "mode")}
    rows = []
    for uid, score, label, props_text in kernel(args["query"], args["mode"], **extra):
        rows.append({"uid": uid, "label": label,
                     "name": json.loads(props_text).get("name"),
                     "match": int(score)})
    rows.sort(key=lambda r: (r["match"], r["uid"]))
    return {"rows": rows, "rows_total": len(rows), "truncated": False, "cursor": None}


def _diff_belief_dispatch(adapter: Any, args: dict[str, Any]) -> dict[str, Any]:
    """`diff.belief`'s system driver.

    A harness-level bypass of `call_operator`, the same shape
    `_lookup_name_dispatch` above uses and for the same reason: `diff_belief`
    stays unregistered in `REGISTRY` (see the note at its `Query` entry), so
    this is the only way the id runs at all. Native answers through
    `NativeAdapter.diff_belief`; `StorageAdapter.diff_belief`'s base raises
    `NotImplementedError` for every other backend, which `run_system`'s
    `except Exception` turns into `ok=False` with the reason in
    `Result.error` — an **unsupported** verdict, not
    a crash, and deliberately not a four-column emulation, which is a
    different query with a different cost and would have to be scored as one.

    The payload is returned with the engine's own `{rows, added_total,
    retracted_total, truncated}` keys, so `canonical_hash` hashes the ordered
    answer as-is and `_answer_size` sums the two `*_total` fields through its
    existing generic branch — no special case in either.
    """
    kernel = getattr(adapter, "diff_belief", None)
    if kernel is None:
        raise NotImplementedError(
            f"unsupported: {type(adapter).__name__} has no diff_belief driver"
        )
    w = args["window"]
    return kernel(args["t1"], args["t2"], w["t_a"], w["t_b"], args["limit"])


#: Backends that load in-process; their `load` block reads `runtime: embedded`.
EMBEDDED_SYSTEMS = ("native", "duckdb")


def _dir_bytes(path: Path) -> int | None:
    """Best-effort on-disk size of a store directory for the `load` receipt.

    `None`, never a crash: this is instrumentation, not a query-timing path,
    and a store that vanished or a permissions error underneath it must not
    take the whole run down.
    """
    try:
        if not path.exists():
            return None
        return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
    except OSError:
        return None


def run_system(name: str, store_path: Path,
               queries: list[Query], *, skip_cost_check: bool = False,
               load_receipt: dict[str, Any] | None = None,
               load_s: float | None = None) -> list[Result]:
    """Answer every registry query on one system.

    `skip_cost_check`: bypass the cost guardrail -- `add_time_estimate` (`gnomon/temporal/guardrails.py`)
    computes `time_est_ms` linearly in *stored* row counts regardless of
    window selectivity, so at 100M+ scale it refuses window-bounded
    queries whose actual cost is O(window). Latency runs at that scale use
    this and report the refusals separately, as a finding about the
    estimator rather than a silent change to admission. The default is
    `False`.

    `load_receipt`/`load_s`: when `load_receipt` is given, this records `{load_s, rows, bytes,
    runtime}` under `load_receipt[name]` -- `load_s` is the caller's own
    wall-clock measurement around `load_store` (the embedded backends load
    entirely inside that call, so the caller's timer is already correct;
    nothing here re-times it). Purely additive: omitting `load_receipt`
    reproduces every prior call site exactly.
    """
    ensure_all_registered()
    store = gnomon.open(store_path, backend=name)
    adapter = store.adapter
    if load_receipt is not None:
        try:
            stats = store.stats()
            rows = stats.get("n_edge_versions", 0) + stats.get("n_node_versions", 0)
        except Exception:                                  # noqa: BLE001
            rows = None
        load_receipt[name] = {
            "load_s": load_s, "rows": rows, "bytes": _dir_bytes(store_path),
            "runtime": "embedded",
        }
    out: list[Result] = []
    for q in queries:
        try:
            if q.op == "lookup_name":  # harness-level bypass, see above
                payload, timings = _measure(
                    lambda: _lookup_name_dispatch(adapter, q.args))
            elif q.op == "diff_belief":  # harness-level bypass, see above
                payload, timings = _measure(
                    lambda: _diff_belief_dispatch(adapter, q.args))
            else:
                payload, timings = _measure(
                    lambda: call_operator(adapter, q.op, dict(q.args),
                                          skip_cost_check=skip_cost_check))
            p50, p95 = _p50_p95(timings)
            out.append(Result(q.id, True, canonical_hash(payload), p50, p95,
                              int(_answer_size(payload)),
                              timings_ms=[round(t, 3) for t in timings]))
        except Exception as e:  # a system that cannot answer is data, not a crash
            out.append(Result(q.id, False, error=f"{type(e).__name__}: {e}"[:160]))
    store.close()
    return out


def manifest(data: Dataset, systems: list[str]) -> dict[str, Any]:
    """Everything needed to say whether two runs are comparable.

    Commit and dirtiness come from `scripts/provenance.py`, which honours
    `GNOMON_COMMIT` before shelling out, so a machine without `git` (a batch
    node, say) still records the commit it ran.

    `dirty` can be `None`, meaning nobody could determine it, which is
    deliberately distinct from `false`.
    """
    def sh(*cmd: str) -> str:
        try:
            return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                                  check=True).stdout.strip()
        except Exception:
            return "unknown"

    return {
        "commit": provenance.commit(short=True, with_dirty=False),
        "commit_source": provenance.describe()["commit_source"],
        # -uno: untracked files on a runner are scratch, not provenance dirt
        "dirty": provenance.dirty(),
        "rustc": sh("rustc", "--version"),
        "python": platform.python_version(),
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
        "cpu_count": os.cpu_count(),
        "dataset": data.name,
        "scale_events": data.scale,
        "systems": systems,
        "protocol": {"warmup": WARMUP, "reps_fast": REPS_FAST,
                     "reps_slow": REPS_SLOW,
                     "stats": "median and p95 over measured reps"},
        "cache_state": "warm",  # queries repeat in-process
        "hostname": platform.node().split(".")[0],
        "measurement_host": MEASUREMENT_HOST,
        "on_measurement_host": platform.node().split(".")[0] == MEASUREMENT_HOST,
    }


#: The graph grows with `scale` at *constant average degree* — |V| and edge
#: lifetime both scale, so an instant snapshot has ~10 neighbours per node at
#: every scale. A fixed |V| would make 2-hop neighbourhoods swallow the whole
#: graph as scale rose; a fixed lifetime made them empty. Neither measures the
#: operator.
def n_nodes(scale: int) -> int:
    return max(200, scale // 100)


def edge_life(scale: int) -> int:
    return max(40, scale // 20)


_M64 = (1 << 64) - 1


def _mix(x: int) -> int:
    """splitmix64 finalizer — a deterministic stand-in for a PRNG.

    Deterministic matters: the corrections in `build_dataset` have to name
    edges by the identity ingestion gave them, which means every endpoint must
    be recomputable from the event index alone.
    """
    x = (x * 0x9E3779B97F4A7C15) & _M64
    x ^= x >> 30
    x = (x * 0xBF58476D1CE4E5B9) & _M64
    x ^= x >> 27
    x = (x * 0x94D049BB133111EB) & _M64
    return x ^ (x >> 31)


def _vt_s(i: int, scale: int) -> int:
    """Event `i` starts at tick `i`, except inside one deliberate burst.

    Without this the event rate is exactly flat, so `burst_detection` returns
    an empty list at every scale and two systems "agree" on having found
    nothing. A band of 5% of the events is compressed tenfold into one stretch
    of the timeline, which puts a bucket roughly six sigma above the mean.
    """
    b0 = scale // 2
    b1 = b0 + max(1, scale // 20)
    if b0 <= i < b1:
        return b0 + (i - b0) // 10
    return i


#: Nodes per community, and the share of edges that stay inside one.
#: A uniform random graph has almost no triangles — that is a property of the
#: model, not of the data it is standing in for. With a flat endpoint choice,
#: `count_temporal_motifs` returned zero cyclic triangles at every node-filter
#: size that stayed inside the cost guardrail, so the motif operator was being
#: compared on an answer of nothing. Real interaction graphs cluster; blocking
#: most edges inside a community reproduces enough of that for the operator to
#: have something to count.
COMMUNITY = 50
INTRA_PCT = 70


def _event(i: int, scale: int) -> dict[str, Any]:
    """Event `i`: a random edge with community structure, reproducible from `i`.

    Endpoints used to be `src = i mod |V|`, `dst = 7i + 3 mod |V|`. That is not
    a random graph — `dst` is a function of `src`, so the graph is one
    deterministic cycle in which every edge out of n1 goes to n10 and nowhere
    else. `neighborhood_evolution` reported zero neighbours gained and zero
    lost at every scale, and 2-hop neighbourhoods held four nodes, because
    there was nothing else to find. Mixing the index independently for each
    endpoint gives a random multigraph with Poisson degree instead.

    The degree distribution is deliberately uniform rather than power-law: a
    scale sweep should vary scale alone, and the real skew is measured on the
    frozen CollegeMsg replay, which has a genuine one.
    """
    n, t = n_nodes(scale), _vt_s(i, scale)
    src = _mix(2 * i) % n
    r = _mix(2 * i + 1)
    if r % 100 < INTRA_PCT:
        # Communities are contiguous id ranges so that a node_filter of the
        # first k nodes covers whole communities rather than slicing every one.
        base = (src // COMMUNITY) * COMMUNITY
        dst = base + (r >> 8) % min(COMMUNITY, n - base)
    else:
        dst = (r >> 8) % n
    return {"src": f"n{src}", "dst": f"n{dst}",
            "rel_type": "R" if i % 3 else "S",
            "vt_s": t, "vt_e": t + edge_life(scale)}


def _edge_ref(i: int, scale: int) -> EntityRef:
    """The identity `ingest_events` gave event `i`.

    Ingestion discriminates by batch offset (`storage/base.py:360`), so the
    endpoints repeat every 6000 events but each occurrence is a *distinct*
    logical edge, not a new version of one. A ref built without the disc names
    an edge that was never written, and correcting it raises NotFound.
    """
    e = _event(i, scale)
    return EntityRef(kind="edge", src=e["src"], dst=e["dst"],
                     rel_type=e["rel_type"], disc=f"#{i}")


@dataclass(frozen=True)
class Dataset:
    """A reference event log plus everything the registry derives from it."""

    log: Path
    scale: int
    #: A transaction time at which the first write batch is believed and
    #: later ones are not. Without it there is no way to ask a belief
    #: question, because tt values are wall-clock and not known in advance.
    tt_epoch1: int
    #: Valid-time extent [t0, t1) the registry windows are placed in.
    t0: int
    t1: int
    #: Node population for the motif filter (parameters are curated per
    #: dataset). Uids that do not exist would make the native
    #: backend raise E_NOT_FOUND while a SQL baseline silently matches
    #: nothing — a divergence in the *harness*, not the systems.
    filter_uids: tuple[str, ...] = ()
    name: str = "synth"


def dataset_from_log(path: Path) -> Dataset:
    """Registry parameters for a pre-recorded event log (e.g. CollegeMsg).

    The extent comes from scanning the log's events, and the belief boundary
    is the first batch's tt — an as_of_tt under which mid-ingestion state is
    visible and everything later is not. That probes the transaction clock
    even on a dataset with no corrections: a system that ignores it returns
    the whole store and the hashes split.
    """
    vt_lo, vt_hi, n = None, None, 0
    first_tt = None
    srcs: set[str] = set()
    with open(path, encoding="utf-8") as f:
        f.readline()  # header
        for line in f:
            rec = json.loads(line)
            if first_tt is None:
                first_tt = rec["tt"]
            for op in rec["ops"]:
                for ev in op.get("events", []):
                    t = ev["vt_s"]
                    vt_lo = t if vt_lo is None else min(vt_lo, t)
                    vt_hi = t if vt_hi is None else max(vt_hi, t)
                    srcs.add(ev["src"])
                    n += 1
    if not n:
        raise SystemExit(f"{path}: no events found")
    return Dataset(log=path, scale=n, tt_epoch1=first_tt,
                   t0=vt_lo, t1=vt_hi + 1,
                   filter_uids=tuple(sorted(srcs)[:MOTIF_FILTER]),
                   name=path.stem)


def build_dataset(scale: int) -> Dataset:
    """Write the reference event log once. Every system replays *this*.

    Building a store per system would look equivalent and is not: transaction
    times come from a clock at write time, so independently built stores of
    the same data differ in tt, and therefore in every version id derived from
    it. The first run of this harness reported two systems disagreeing for
    exactly that reason.

    Two properties of the generated data are deliberate, and both were added
    after measuring that their absence made the comparison vacuous:

    **The graph scales at constant average degree.** Lifetime used to be a
    constant 40 ticks while one edge starts per tick, so exactly ~40 edges were
    ever valid at any instant, on 2000 nodes — every instant operator was
    answering over an essentially empty graph no matter the scale.
    `snapshot_subgraph` at 2 hops returned its seed and nothing else at 20k
    events and at 200k alike. Two systems agreeing on that is not evidence of
    anything. Scaling |V| and lifetime together keeps degree near 10
    throughout, so the same query stays meaningful and stays bounded.

    **There is a second belief epoch.** The generator previously produced no
    corrections at all, so every row had an open `tt_e` and no query could
    distinguish one belief state from another — which is exactly the bar
    for calling two systems equivalent on bi-temporal semantics. The corrections
    below close `tt_e` on real rows, so `hist.asof` finally tests the clock.
    """
    path = Path(tempfile.mkdtemp()) / "reference"
    store = gnomon.open(path, backend="native")
    store.ingest_events([_event(i, scale) for i in range(scale)])
    tt_epoch1 = store.assert_node("n1", "Node", {"name": "alpha"},
                                  vt_s=0, vt_e=scale)

    # Epoch 2: corrections and a retraction. These supersede believed rows,
    # so their tt_e closes and the store stops being uniformly "current".
    step = max(1, scale // 200)
    for i in range(0, scale, step):
        e = _event(i, scale)
        store.correct(_edge_ref(i, scale), {"weight": 2},
                      vt_s=e["vt_s"], vt_e=e["vt_e"])
    store.correct(EntityRef(kind="node", uid="n1"), {"name": "alpha-corrected"},
                  vt_s=0, vt_e=scale)

    # Partial corrections: rewrite only the tail half of an edge's interval,
    # which splits it into two versions carrying different props. Whole-
    # interval corrections cannot produce that, and `diff_snapshots` only
    # reports a props change when the two instants see *different* vids — so
    # without a split its props_changed list is empty by construction and that
    # branch of any reimplementation goes untested.
    life, mid = edge_life(scale), scale // 2
    for i in range(mid - life // 2, mid, max(1, life // 20)):
        e = _event(i, scale)
        store.correct(_edge_ref(i, scale), {"weight": 3},
                      vt_s=e["vt_s"] + life // 2, vt_e=e["vt_e"])
    for i in range(0, scale, step * 4):
        store.retract(_edge_ref(i, scale),
                      _event(i, scale)["vt_s"] + edge_life(scale) // 2)
    store.close()
    return Dataset(path / "eventlog.jsonl", scale, tt_epoch1, t0=0, t1=scale,
                   filter_uids=tuple(f"n{i}" for i in range(MOTIF_FILTER)))


def load_store(path: Path, backend: str, log: Path) -> None:
    """Materialize the reference log into one system, preserving its tt.

    Compaction is armed where the adapter supports it. Without it
    replay never compacts, so the store this function builds holds **one
    segment per applied batch** — 283 over 282 generations at 1M, censused
    in the segment-count/latency design note, and tens of
    thousands at 30M, past `GNOMON_PIN_MAX_SEGMENTS`. That is the store every
    latency number in the campaign is measured on, so its layout should be
    one a real store reaches, not one no writer would ever leave behind.

    `compact_trigger` rather than a batch cadence: the cadence form calls
    the whole-store compactor, which would put an O(store) rewrite inside
    the loop and change what the wall measures. The trigger rewrites one
    eligible tier, which is what a live writer under `--compact-every 0`
    does. Backends without the API (DuckDB) are unaffected — they have
    no segments to bound.
    """
    from gnomon.storage.eventlog import replay

    store = gnomon.open(path, backend=backend)
    trigger = all(hasattr(store.adapter, m) for m in
                  ("needs_compaction", "compact_incremental", "gc"))
    replay(log, store.adapter, compact_trigger=trigger)
    store.close()


def measure_window_start(store_path: Path, positions: list[float],
                         json_path: Path | None) -> int:
    """Sweep window-start fractions of an existing store's own valid-time
    extent and report the interval-lane staircase's block-skip fraction
    straight from `scan_edges`' stats (`blocks_skipped` / `blocks_considered`,
    `rows_examined`) -- the same counters
    `tests/test_interval_staircase.py` gates.

    Deliberately bypasses `registry()` / `run_system()` entirely: running
    the full query registry has nothing to do with what this measures (a
    single scan-stats counter on one backend).
    """
    store = gnomon.open(store_path, backend="native")
    try:
        cols = store.adapter.edges_columnar(columns=["vt_s", "vt_e"])
        if len(cols["vt_s"]) == 0:
            raise SystemExit(f"{store_path}: no edges to measure")
        t0, t1 = int(cols["vt_s"].min()), int(cols["vt_e"].max())
        print(f"store={store_path} extent=[{t0}, {t1}) span={t1 - t0}")
        rows = []
        for p in positions:
            vt_min = t0 + int(p * (t1 - t0))
            stats = store.adapter._read_op(lambda st, vt_min=vt_min: st.scan_edges(
                as_of_tt=OPEN_END, vt_min=vt_min, vt_max=None, rel_types=None,
                touching_ids=None, touching_both=False, limit=None,
                columns=["vt_s"]))
            skipped = stats["blocks_skipped"]
            considered = stats["blocks_considered"]
            ratio = skipped / considered if considered else 0.0
            rows.append({
                "window_start": p, "vt_min": vt_min,
                "blocks_skipped": skipped, "blocks_considered": considered,
                "skip_ratio": ratio, "rows_examined": stats["rows_examined"],
            })
            print(f"  p={p:<5} vt_min={vt_min:<20} blocks_skipped={skipped:<8} "
                 f"blocks_considered={considered:<8} skip_ratio={ratio:.3f} "
                 f"rows_examined={stats['rows_examined']}")
        if json_path:
            json_path.write_text(json.dumps(
                {"store": str(store_path), "extent": [t0, t1], "positions": rows},
                indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"  wrote {json_path}")
        return 0
    finally:
        store.close()


# --------------------------------------------------------------------- #
# Store mode: measure the BUILT store,                                  #
# not a freshly replayed, never-compacted one.                    #
# --------------------------------------------------------------------- #

def store_receipt(store_path: Path, skip_cost_check: bool) -> dict[str, Any]:
    """Everything a receipt needs about an EXISTING
    store being measured *in place*: which store this is, whether it was
    compacted, and under what cache/guardrail settings — distinct from
    `manifest()` above, which describes a run over a freshly built or
    replayed dataset and knows nothing about a store's own layout.

    There is no boolean "was this compacted" flag in the manifest, so this
    reports the two numbers that answer
    it directly: `generation` (how many commits produced this store) and
    `segment_count` (how many segments the current manifest names). A store
    nobody ever compacted has one segment per commit-that-created-a-
    segment, so `segment_count` tracks `generation`; a compacted store has
    far fewer. The receipt reports both, plus a `compacted` guess
    (`segment_count < generation`), rather than asserting a fact the
    manifest does not carry.
    """
    store = gnomon.open(store_path, backend="native", read_only=True)
    try:
        v = store.adapter.verify(mode="fast")
        lanes = v["closes_by_lane"]
        segment_count = (lanes["edge_event"]["segments"] + lanes["edge_interval"]["segments"]
                        + lanes["node"]["segments"])
        generation = store.adapter.generation
        return {
            "store_path": str(store_path),
            "manifest_sha": store.adapter._store.manifest_sha(),  # noqa: SLF001
            "generation": generation,
            "segment_count": segment_count,
            "compacted": segment_count < generation,
            # Large runs set this explicitly and the receipt records it,
            # rather than trusting `cache_budget`'s RAM-derived default.
            "segment_cache_bytes": (int(os.environ["GNOMON_SEGMENT_CACHE_BYTES"])
                                    if "GNOMON_SEGMENT_CACHE_BYTES" in os.environ else None),
            # Recorded on the receipt, never silently assumed.
            "skip_cost_check": skip_cost_check,
        }
    finally:
        store.close()


def run_store_mode(store_path: Path, log_path: Path, skip_cost_check: bool,
                   json_path: Path | None) -> int:
    """Answer the query registry
    against the EXISTING native store `build_synth_store.py` produced —
    compacted state, segment layout and all — instead of replaying the log
    into a fresh, never-compacted temp store the way `--scale`/bare
    `--log` above do (`load_store`, `build_dataset`). A replay into a fresh
    store measures a different physical layout (one uncompacted segment per
    commit) from the store a large build actually produces, so latency
    numbers for a built store must come from this mode.

    Registry parameters come from `--log` via `dataset_from_log` —
    the store's own event log, not a freshly generated one, so `n1`, the
    valid-time extent and `tt_epoch1` all match what is actually in the
    store being measured.
    """
    ensure_all_registered()
    data = dataset_from_log(log_path)
    queries = registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    meta = manifest(data, ["native"])
    meta["store"] = store_receipt(store_path, skip_cost_check)

    print(f"query registry (store mode) — {len(queries)} queries "
          f"on {store_path}")
    print(f"  commit {meta['commit']}{' (dirty)' if meta['dirty'] else ''} | "
          f"{meta['platform']} | {meta['cpu_count']} cores")
    print(f"  store: generation={meta['store']['generation']} "
          f"segments={meta['store']['segment_count']} "
          f"compacted={meta['store']['compacted']} "
          f"cache_bytes={meta['store']['segment_cache_bytes']} "
          f"skip_cost_check={skip_cost_check}")
    if not meta["on_measurement_host"]:
        print(f"  WARNING: not {MEASUREMENT_HOST} — development run, timings "
              f"are not comparable with reported numbers")
    print()

    results = {"native": run_system("native", store_path, queries,
                                    skip_cost_check=skip_cost_check)}
    return report_results(["native"], results, queries, meta, json_path)


def _batch_tts_from_log(path: Path) -> list[int]:
    """Every batch's own `tt`, in the log's own (commit/generation) order —
    one JSONL line is one applied write batch is one generation
    (`gnomon/storage/eventlog.py`), so this is "the store's own generation
    count" made concrete: `len()` of the result is
    the generation this log produced, and index `g` back from the end is
    the tt as of `g` generations before the log's tail.
    """
    tts: list[int] = []
    with open(path, encoding="utf-8") as f:
        f.readline()  # header
        for line in f:
            tts.append(json.loads(line)["tt"])
    if not tts:
        raise SystemExit(f"{path}: no batches found")
    return tts


def measure_as_of_sweep(store_path: Path, log_path: Path, distances: list[int],
                        skip_cost_check: bool, json_path: Path | None) -> int:
    """Stress axis 3: `hist.asof` in the registry always
    probes `tt_epoch1` (the *first* belief), which is a single, shallow
    point regardless of scale — `believed_ranges`' mixed case (a distant
    as-of that is neither all-visible nor none-visible) never runs. This
    sweeps `as_of_tt` back from the store's own tail by `--as-of-distance`
    generations (not wall-clock time — the store's own commit count, which
    is what "10,000 generations back" means on a log with 400,363 of
    them), and times `entity_history` at each point the same way the main
    registry does.

    `n1` is guaranteed present: `build_synth_store.py` (and
    `eval_harness.build_dataset`) both `assert_node("n1", ...)` explicitly.
    """
    ensure_all_registered()
    tts = _batch_tts_from_log(log_path)
    n_generations = len(tts)
    store = gnomon.open(store_path, backend="native", read_only=True)
    try:
        adapter = store.adapter
        rows = []
        for g in distances:
            idx = max(0, n_generations - 1 - g)
            as_of_tt = tts[idx]
            payload, timings = _measure(
                lambda as_of_tt=as_of_tt: call_operator(
                    adapter, "entity_history", {"uid": "n1", "as_of_tt": as_of_tt},
                    skip_cost_check=skip_cost_check))
            p50, p95 = _p50_p95(timings)
            rows.append({
                "distance_generations": g, "resolved_index": idx, "as_of_tt": as_of_tt,
                "p50_ms": p50, "p95_ms": p95, "rows": _answer_size(payload),
            })
            print(f"  distance={g:<8} idx={idx:<10} as_of_tt={as_of_tt:<20} "
                 f"p50_ms={p50:<10} rows={_answer_size(payload)}")
        if json_path:
            json_path.write_text(json.dumps(
                {"store": str(store_path), "log": str(log_path),
                 "n_generations": n_generations, "skip_cost_check": skip_cost_check,
                 "points": rows},
                indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"  wrote {json_path}")
        return 0
    finally:
        store.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=200_000, help="events to generate")
    ap.add_argument("--systems", default="native",
                    help="comma-separated: native,duckdb (duckdb is the "
                         "optional reference backend)")
    ap.add_argument("--log", type=Path,
                    help="replay this pre-recorded event log instead of "
                         "generating synthetic data (e.g. "
                         "a log frozen by scripts/freeze_synth_log.py)")
    ap.add_argument("--json", type=Path, help="write the full run record here")
    # A separate
    # measurement mode, dispatched before the query-registry path below and
    # never touching `registry()` itself -- see
    # `measure_window_start`'s docstring.
    ap.add_argument("--store", type=Path,
                    help="open this existing native store directly, instead of "
                        "building (--scale) or replaying (--log) one -- required by "
                        "--window-start")
    ap.add_argument("--window-start", default=None,
                    help="comma-separated window-start fractions of --store's own "
                        "valid-time extent (e.g. '0.1,0.25,0.5,0.75,0.9'); measures "
                        "the interval-lane staircase's block-skip fraction directly "
                        "instead of running the query-registry comparison. Requires "
                        "--store")
    # `--store` given WITHOUT `--window-start` answers the registry against that store
    # directly instead of requiring one of the two dispatches above --
    # `run_store_mode`'s own docstring has the full story. `--as-of-distance`
    # is the sibling belief-time instrument, sharing `--store`/`--log`.
    ap.add_argument("--as-of-distance", default=None,
                    help="comma-separated generation counts back from --store's own "
                        "log tail (e.g. '1,10,100,1000,10000'); sweeps hist.asof's "
                        "as_of_tt across them instead of running the query-registry "
                        "comparison. Requires --store and --log")
    ap.add_argument("--skip-cost-check", action="store_true",
                    help="bypass the cost guardrail "
                        "(gnomon.temporal.algebra.call_operator's skip_cost_check) so "
                        "a window-bounded query is not refused on the estimator's "
                        "store-size-linear time_est_ms; refusals under this "
                        "flag are recorded as a finding about the estimator, not "
                        "run without it silently. Applies to --store mode and to "
                        "--as-of-distance; recorded on the run's receipt either way")
    args = ap.parse_args()

    if args.window_start is not None:
        if args.store is None:
            raise SystemExit("--window-start requires --store")
        positions = [float(p) for p in args.window_start.split(",") if p.strip()]
        return measure_window_start(args.store, positions, args.json)

    if args.as_of_distance is not None:
        if args.store is None or args.log is None:
            raise SystemExit("--as-of-distance requires --store and --log")
        distances = [int(d) for d in args.as_of_distance.split(",") if d.strip()]
        return measure_as_of_sweep(args.store, args.log, distances,
                                   args.skip_cost_check, args.json)

    if args.store is not None:
        # Store mode: measure the store that was
        # actually built, not a fresh replay of its log. Requires
        # --log to derive the registry's parameters from.
        if args.log is None:
            raise SystemExit(
                "--store (without --window-start/--as-of-distance) requires --log, "
                "to derive the query registry's parameters from -- see "
                "run_store_mode's docstring")
        return run_store_mode(args.store, args.log, args.skip_cost_check, args.json)

    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    data = dataset_from_log(args.log) if args.log else build_dataset(args.scale)
    queries = registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    meta = manifest(data, systems)

    print(f"phase-0 harness — {len(queries)} queries x {len(systems)} systems "
          f"on {data.name} @ {data.scale:,} events")
    print(f"  commit {meta['commit']}{' (dirty)' if meta['dirty'] else ''} | "
          f"{meta['platform']} | {meta['cpu_count']} cores")
    if not meta["on_measurement_host"]:
        print(f"  WARNING: not {MEASUREMENT_HOST} — development run, timings "
              f"are not comparable with reported numbers")
    print()

    log = data.log
    results: dict[str, list[Result]] = {}
    # One `load` block per system in the receipt,
    # `{load_s, rows, bytes, runtime}` — see `run_system`'s docstring.
    load: dict[str, Any] = {}
    for name in systems:
        path = Path(tempfile.mkdtemp()) / "store"
        t0 = time.perf_counter()
        load_store(path, name, log)
        load_s = time.perf_counter() - t0
        results[name] = run_system(name, path, queries, load_receipt=load,
                                   load_s=round(load_s, 3))
        shown = load.get(name, {}).get("load_s", load_s)
        print(f"  {name}: loaded in {shown:.1f}s")

    meta["load"] = load
    return report_results(systems, results, queries, meta, args.json)


def report_results(systems: list[str], results: dict[str, list[Result]],
                   queries: list[Query], meta: dict[str, Any],
                   json_path: Path | None) -> int:
    """Print the agreement table, optionally write the full JSON record, and
    return the exit criterion (0 = every system's hashes agree).

    Factored out of `main()` so the store mode
    below — a single-system run with no cross-system comparison to make —
    can print and record results in exactly the same shape, rather than a
    second, drifting copy of this table.
    """
    base, *others = systems
    by_id = {s: {r.query: r for r in rs} for s, rs in results.items()}

    hdr = f"\n  {'query':<18}{'rows':>8}" + "".join(f"{s:>12}" for s in systems) + "  agree"
    print(hdr)
    print("  " + "-" * (len(hdr) - 3))
    mismatches, unsupported = [], []
    for q in queries:
        cells, hashes = [], []
        rows = None
        for s in systems:
            r = by_id[s][q.id]
            if r.ok:
                cells.append(f"{r.p50_ms:>11.1f}")
                hashes.append(r.hash)
                rows = r.rows if rows is None else rows
            else:
                cells.append(f"{'n/a':>11}")
                hashes.append(None)
                unsupported.append((s, q.id, by_id[s][q.id].error))
        ok = len({h for h in hashes if h}) <= 1 and all(hashes)
        if not ok and all(hashes):
            mismatches.append(q.id)
        mark = "yes" if ok else ("MISMATCH" if all(hashes) else "partial")
        print(f"  {q.id:<18}{rows if rows is not None else '-':>8}"
              + "".join(cells) + f"  {mark}")

    print()
    for s, qid, err in unsupported:
        print(f"  no answer: {s}/{qid} — {err}")
    if mismatches:
        print(f"\n  RESULT HASHES DIFFER on {len(mismatches)}: {', '.join(mismatches)}")
    else:
        print(f"  all {len(queries)} queries agree across {', '.join(systems)}")

    if json_path:
        json_path.write_text(json.dumps({
            "manifest": meta,
            "queries": [{"id": q.id, "op": q.op, "note": q.note,
                         "requires": list(q.requires)} for q in queries],
            "results": {s: [vars(r) for r in rs] for s, rs in results.items()},
            "agree": not mismatches,
        }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"  wrote {json_path}")

    # Exit criterion: identical hashes across the backends that ran.
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
