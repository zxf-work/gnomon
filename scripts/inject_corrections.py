#!/usr/bin/env python3
"""Post-build correction-density injector: apply a chosen density of
corrections to an EXISTING native store through the public write path, so
read costs can be measured at several correction densities without
rebuilding the underlying store for each one.

**Density semantics.** `--density D` targets D percent of the store's
currently-*believed* edges (`tt_e == OPEN_END` -- a version some later
correction has not already superseded; correcting an already-closed
version would not change what any live query sees, so it is excluded from
the denominator and never selected). Selection is a stable hash of
`(eid, seed)`, not a running RNG, so the sampled set is the same across
reruns of the SAME store at the SAME (seed, density).

**Corrections are additive; run this against a fresh COPY of the store per
density point**, not the same store several times (that would compound
densities instead of giving independent measurements):

    cp -R runs/synth-1m runs/synth-1m-d5
    python scripts/inject_corrections.py \\
        --store runs/synth-1m-d5 --density 5 --seed 0 \\
        --digest --record runs/inject-1m-d5.json

Streams through the store's edges with the adapter's paged accessor
(`all_versions_paged`) rather than `all_edge_versions()`, so it (a) never
holds the whole edge population in memory at once, and (b) reads a stable
snapshot fixed at the first page pull: corrections issued mid-scan do not
feed back into later pages of the same pass, so a row this script just
wrote is never re-selected within the same run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import gnomon  # noqa: E402
from gnomon.core.model import OPEN_END, EntityRef  # noqa: E402
from gnomon.store import _ref_json  # noqa: E402
from gnomon.storage.base import make_op  # noqa: E402


def _selected(eid: str, seed: int, density_pct: float) -> bool:
    """Deterministic: the same `(eid, seed, density_pct)` always answers the
    same way, on this run or the next -- a running RNG would not survive a
    resumed or re-run injection identically."""
    if density_pct <= 0:
        return False
    if density_pct >= 100:
        return True
    h = hashlib.sha256(f"{eid}:{seed}".encode()).digest()
    frac = int.from_bytes(h[:8], "big") / 2.0 ** 64
    return frac < density_pct / 100.0


def inject(store_path: Path, density_pct: float, seed: int, chunk_rows: int,
          progress_every: int, dry_run: bool, batch: int = 1) -> dict[str, Any]:
    """Apply the selected corrections, `batch` of them per transaction.

    **Why `batch` exists, and why its default is not 1.** This script used to
    call `Store.correct` once per correction, and `Store.correct` is one op in
    one transaction. So a 10 % injection was not "10 % of the edges corrected"
    but "10 % of the edges corrected in that many *separate commits*", and the
    store's shape recorded the second fact rather than the first. Measured
    on a 100k synthetic store: live segment count went 30 at c=0 to 948 at
    c=1 % to **9,939** at c=10 %, tracking the commit count almost exactly,
    while `settle` collapsed every one of those stores back to 3-5 segments.

    That inflation is an artefact of this script, not a property of correction
    density, and it silently distorts anything whose cost has a per-segment
    term -- which is most of the read path, and all of the belief operators.
    A real correction stream arrives in transactions, not one row at a time.

    Batching makes the segment count a function of the tier planner, which is
    the thing under study, instead of a function of how this script chose to
    commit. `--batch 1` keeps the one-commit-per-correction behaviour.

    The write path is unchanged: `Store._write` is the same call
    `Store.correct` makes -- write-ahead log first, then apply, then commit --
    and the only difference is how many ops share the transaction, exactly as
    `Store.ingest_events` already batches its own. `_invalidate_memories` is
    called once per batch over the hull of the batch's valid-time ranges,
    which is a superset of what the per-correction path would invalidate.
    """
    store = gnomon.open(store_path, backend="native", read_only=dry_run)
    t0 = time.time()
    scanned = eligible = corrected = 0
    commits = 0
    pending: list[dict[str, Any]] = []
    hull: list[int] = []

    def flush() -> None:
        nonlocal commits, pending, hull
        if not pending:
            return
        store._write(pending)
        # one invalidation over the batch's hull; a superset of the per-op
        # calls `Store.correct` would have made, never a subset
        store._invalidate_memories(min(hull), max(hull))
        commits += 1
        pending = []
        hull = []

    try:
        for page in store.adapter.all_versions_paged("edge", chunk_rows):
            for e in page:
                scanned += 1
                if e.tt_e != OPEN_END:
                    # not currently believed -- an earlier correction (this
                    # store's own build phases, or a previous injection run)
                    # already superseded it. Correcting it would add a row
                    # no live query ever sees, which is not what "inject a
                    # correction stream" means.
                    continue
                eligible += 1
                if not _selected(e.eid, seed, density_pct):
                    continue
                if not dry_run:
                    ref = EntityRef(kind="edge", src=e.src, dst=e.dst,
                                    rel_type=e.rel_type, disc=e.disc)
                    props = {**e.props, "correction_seed": seed,
                             "correction_density_pct": density_pct}
                    if batch <= 1:
                        store.correct(ref, props, vt_s=e.vt_s, vt_e=e.vt_e)
                        commits += 1
                    else:
                        pending.append(make_op(
                            "correct", ref=_ref_json(ref), props=props,
                            vt_s=e.vt_s, vt_e=e.vt_e,
                            source="ingest", provenance_ref=None))
                        hull.extend((e.vt_s, e.vt_e))
                        if len(pending) >= batch:
                            flush()
                corrected += 1
                if progress_every and corrected % progress_every == 0:
                    print(f"  corrected={corrected:,} eligible={eligible:,} "
                         f"scanned={scanned:,} commits={commits:,}", flush=True)
        if not dry_run:
            flush()
    finally:
        store.close()
    wall = time.time() - t0
    return {
        "store": str(store_path),
        "density_pct_requested": density_pct,
        "seed": seed,
        "chunk_rows": chunk_rows,
        "batch": batch,
        "commits": commits,
        "dry_run": dry_run,
        "edges_scanned": scanned,
        "edges_eligible": eligible,
        "edges_corrected": corrected,
        "density_pct_actual": (100.0 * corrected / eligible) if eligible else 0.0,
        "wall_s": round(wall, 3),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store", required=True, type=Path,
                    help="existing native store to inject corrections into "
                        "(mutated in place -- see the module docstring for why "
                        "each density point wants its own copy)")
    ap.add_argument("--density", type=float, required=True,
                    help="target correction density, in percent of currently-"
                        "believed edges (e.g. 5 for 5%%)")
    ap.add_argument("--seed", type=int, default=0,
                    help="selection seed -- fixed for reproducibility (default 0)")
    ap.add_argument("--chunk-rows", type=int, default=200_000,
                    help="page size through the store's edges (default 200000, "
                        "matching store_digest_streaming's own default)")
    ap.add_argument("--batch", type=int, default=1000,
                    help="corrections per transaction (default 1000). One "
                        "commit per correction is --batch 1: it makes the store's "
                        "segment count track the commit count rather than the "
                        "tier planner (measured: 30 -> 9,939 live segments "
                        "from c=0 to c=10%% at 100k), which distorts anything "
                        "with a per-segment cost term")
    ap.add_argument("--progress-every", type=int, default=50_000,
                    help="print a progress line every N corrections (0 to disable)")
    ap.add_argument("--dry-run", action="store_true",
                    help="scan and report what would be corrected, write nothing "
                        "(opens the store read-only)")
    ap.add_argument("--digest", action="store_true",
                    help="record store.digest_streaming() before and after "
                        "injection. Expensive at scale -- opt-in")
    ap.add_argument("--digest-chunk-rows", type=int, default=200_000)
    ap.add_argument("--record", type=Path, help="write a JSON receipt here")
    args = ap.parse_args()

    if not 0.0 <= args.density <= 100.0:
        raise SystemExit(f"--density must be in [0, 100], got {args.density}")
    if args.batch < 1:
        raise SystemExit(f"--batch must be at least 1, got {args.batch}")

    digest_before = None
    if args.digest:
        s = gnomon.open(args.store, backend="native", read_only=True)
        digest_before = s.digest_streaming(chunk_rows=args.digest_chunk_rows)
        s.close()

    print(f"injecting density={args.density}% seed={args.seed} into {args.store} "
         f"(dry_run={args.dry_run}, batch={args.batch})")
    receipt = inject(args.store, args.density, args.seed, args.chunk_rows,
                     args.progress_every, args.dry_run, args.batch)
    receipt["digest_before"] = digest_before

    if args.digest and not args.dry_run:
        s = gnomon.open(args.store, backend="native", read_only=True)
        receipt["digest_after"] = s.digest_streaming(chunk_rows=args.digest_chunk_rows)
        s.close()
    else:
        receipt["digest_after"] = None

    print(f"  done: requested={receipt['density_pct_requested']}% "
         f"actual={receipt['density_pct_actual']:.4f}% "
         f"({receipt['edges_corrected']:,}/{receipt['edges_eligible']:,} eligible) "
         f"commits={receipt['commits']:,} batch={receipt['batch']} "
         f"wall_s={receipt['wall_s']}")

    if args.record:
        args.record.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                               encoding="utf-8")
        print(f"  wrote {args.record}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
