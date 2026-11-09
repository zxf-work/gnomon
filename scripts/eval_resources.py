"""Resource-axis evaluation: the four measurements the plan's resource
sections ask for, on the stores the phase-0 harness defines.

* **threads** — the thread-scaling curve of the parallel scan.
  `GNOMON_SCAN_THREADS` overrides the engine's worker count (scan.rs); each
  point runs in a fresh subprocess with the variable in its environment,
  so propagation into the Rust extension is by inheritance, not by
  `putenv` timing. DuckDB gets a column because its knob is one flag away
  (`SET threads`). Every thread count must produce the same result hash —
  the run fails otherwise, which measures the engine's "byte-identical
  output to the serial loop by construction" claim, not just its speed.

* **coldwarm** — three cache states per query, coldest last:
  `warm` (in-process repetition, the published protocol), `process_cold`
  (fresh process, OS page cache warm: what a new client pays), and
  `cold` (fresh process *and* page cache evicted: first query ever).
  Eviction is user-space — `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)`
  over every store file — because the measurement host offers no root for
  `drop_caches`. DONTNEED drops clean pages, and a read-only store's pages
  are clean, but the kernel is free to keep pages another process holds;
  the record therefore states the method rather than claiming a sterile
  cold state.

* **memcap** — the query suite under a hard memory ceiling, to
  find where the unbounded segment cache (a known roadmap item) actually
  hurts. Preferred enforcement is a rootless-Docker cgroup
  (`--memory=CAP --memory-swap=CAP`, so the cap is memory, not
  memory+swap); the store and venv are bind-mounted at their host paths
  so the editable install resolves unchanged. Where the host's cgroup
  setup cannot enforce limits rootless (cgroup v1), the fallback is
  `RLIMIT_AS`, which caps *address space* — mmapped store files count
  against it even when resident bytes do not, so it is a strictly harsher
  approximation and is labeled as such in the record.

* **readers** — N reader processes over one store, each looping
  the same query mix inside a common wall-clock window (barrier start).
  Reports per-reader per-query medians and aggregate throughput. This is
  the measurement behind the "lock-free reads off immutable segments"
  claim: per-reader latency should hold roughly flat until the readers'
  own scan threads oversubscribe the cores.

Stores are built once per scale by the phase-0 generator (same reference
plus a content fingerprint of the format-relevant code (see FORMAT_PATHS),
so every mode measures the same bytes and a stale artifact left by an
older commit is rebuilt automatically rather than silently reused.

    uv run python scripts/eval_resources.py threads  --scale 1000000 --json out.json
    uv run python scripts/eval_resources.py coldwarm --scale 10000000 --json out.json
    uv run python scripts/eval_resources.py memcap   --scale 10000000 --caps 2g,4g,8g --json out.json
    uv run python scripts/eval_resources.py readers  --scale 10000000 --readers 1,2,4,8,16 --json out.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import eval_harness as H  # noqa: E402  (query registry, dataset generator, protocol)

ROOT = Path(__file__).resolve().parents[1]

#: scan-heavy queries — the ones eval_phase0 attributes to that stage.
THREAD_QUERIES = ("series.count", "coactive.narrow", "motif.filtered")

#: traversal, two full-window scans, and the materialization-heavy diff.
REPR_QUERIES = ("hist.single", "snap.hop2", "series.count",
                "coactive.narrow", "diff.global", "motif.filtered")

#: The IR's mix: point + traversal + two scan shapes, looped as one unit.
READER_MIX = ("hist.single", "snap.hop2", "series.count", "coactive.narrow")


# --------------------------------------------------------------------------- #
# store cache: one reference log per scale, one replayed store per backend
# --------------------------------------------------------------------------- #

def default_workdir() -> Path:
    return Path(os.environ.get("TMPDIR", "/tmp")) / "gnomon-eval-resources"


#: Repo-relative paths whose content can change the on-disk store/eventlog
#: format, or the generated reference log itself. The cache identity below
#: is a *content* hash of exactly these paths, not the commit SHA, so a
#: docs-only or query-operator-only commit does not invalidate a 10M store
#: native store cached six weeks earlier silently kept serving after
#: writer trial in eval_concurrency.py failed with a cursor/record-boundary
#: StateError.
FORMAT_PATHS: tuple[str, ...] = (
    "gnomon/core", "gnomon/storage", "gnomon/store.py", "gnomon/write.py",
    "crates", "Cargo.toml", "Cargo.lock", "scripts/eval_harness.py",
)

_fingerprint_cache: dict[Path, str] = {}


def _format_files(root: Path) -> list[str]:
    """Repo-relative paths of tracked files under FORMAT_PATHS. Prefers
    `git ls-files` (works inside git worktrees); falls back to os.walk if
    git is unavailable or fails, skipping __pycache__/target/.pyc/.so."""
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--"] + list(FORMAT_PATHS),
            capture_output=True, check=True)
        names = out.stdout.decode("utf-8", "surrogateescape").split("\0")
        return sorted(n for n in names if n and (root / n).is_file())
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        pass
    found: list[str] = []
    for rel in FORMAT_PATHS:
        p = root / rel
        if p.is_file():
            found.append(rel)
            continue
        if not p.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(p):
            dirnames[:] = [dn for dn in dirnames if dn not in ("__pycache__", "target")]
            for fn in filenames:
                if fn.endswith((".pyc", ".so")):
                    continue
                found.append(str((Path(dirpath) / fn).relative_to(root)))
    return sorted(found)


def format_fingerprint(root: Path = ROOT) -> str:
    """sha256 over the sorted (relative path, file bytes) pairs for every
    tracked file under FORMAT_PATHS, first 16 hex chars. Hashes working-tree
    bytes (so uncommitted edits count), not git blob ids; the path is fed
    into the hash before the bytes so a rename changes the fingerprint.
    Deterministic across runs; cached per `root` within this process."""
    cached = _fingerprint_cache.get(root)
    if cached is not None:
        return cached
    h = hashlib.sha256()
    for rel in _format_files(root):
        h.update(rel.encode("utf-8"))
        h.update((root / rel).read_bytes())
    fp = h.hexdigest()[:16]
    _fingerprint_cache[root] = fp
    return fp


def build_identity(root: Path = ROOT) -> dict[str, Any]:
    """The record embedded in cache markers. Only `fingerprint` is used for
    the hit check; `commit`/`dirty`/`built_at` are for a human reading the
    marker after the fact."""
    commit = "unknown"
    dirty = False
    try:
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        pass
    try:
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "-uno"],
            capture_output=True, text=True, check=True)
        dirty = bool(status.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        pass
    return {"fingerprint": format_fingerprint(root), "commit": commit or "unknown",
            "dirty": dirty, "built_at": datetime.now(timezone.utc).isoformat()}


def _read_marker(path: Path) -> dict[str, Any] | None:
    """The parsed JSON object at `path`, or None if missing / not JSON / not
    an object — a legacy text marker like "replayed in 12.3s" yields None,
    i.e. is treated as stale."""
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return None
    return obj if isinstance(obj, dict) else None


def _cache_state(marker: dict[str, Any] | None, current_fp: str) -> str:
    """"hit" when marker's fingerprint matches; "legacy" when there is no
    (parseable) marker at all; "stale" otherwise."""
    if marker is None:
        return "legacy"
    if marker.get("fingerprint") == current_fp:
        return "hit"
    return "stale"


def _cache_miss_reason(state: str, marker: dict[str, Any] | None,
                       current_fp: str, label: str) -> str:
    if state == "legacy":
        return "  cache marker is pre-fingerprint (legacy) — rebuilding"
    commit = (marker or {}).get("commit", "unknown")
    fp = (marker or {}).get("fingerprint", "?")
    return (f"  cache stale: {label} built at commit {commit} (fp {fp}) "
            f"!= current fp {current_fp} — rebuilding")


def ensure_dataset(workdir: Path, scale: int) -> tuple[H.Dataset, Path]:
    """The reference event log for `scale`, built once and cached.

    Cache key: `scale` plus `format_fingerprint()`, recorded as meta.json's
    "identity". A legacy meta.json (no "identity") or one whose fingerprint
    no longer matches the working tree is stale and triggers a rebuild —
    along with every store-* directory/marker under `d`, since a store
    replayed from a different reference log is wrong regardless of its own
    marker."""
    d = workdir / f"scale-{scale}"
    d.mkdir(parents=True, exist_ok=True)
    meta_p, log_p = d / "meta.json", d / "eventlog.jsonl"
    current_fp = format_fingerprint()

    meta = _read_marker(meta_p)
    identity = meta.get("identity") if meta is not None else None
    state = _cache_state(identity, current_fp)
    if meta is not None and log_p.exists() and state == "hit":
        m = meta
        return H.Dataset(log=log_p, scale=m["scale"], tt_epoch1=m["tt_epoch1"],
                         t0=m["t0"], t1=m["t1"],
                         filter_uids=tuple(m["filter_uids"]), name=m["name"]), d

    existed = meta_p.exists() or log_p.exists()
    if existed:
        if state == "hit":
            print("  eventlog.jsonl missing — rebuilding", flush=True)
        else:
            print(_cache_miss_reason(state, identity, current_fp,
                                     f"scale-{scale} dataset"), flush=True)
        wiped = []
        for p in (meta_p, log_p):
            if p.exists():
                p.unlink()
                wiped.append(p.name)
        for p in sorted(d.iterdir()):
            if p.name.startswith("store-"):
                wiped.append(p.name)
                shutil.rmtree(p) if p.is_dir() else p.unlink()
        if wiped:
            print(f"  wiped stale cache entries: {', '.join(wiped)}", flush=True)

    print(f"  building reference log at {scale:,} events ...", flush=True)
    data = H.build_dataset(scale)
    shutil.copy2(data.log, log_p)
    meta_p.write_text(json.dumps({
        "scale": data.scale, "tt_epoch1": data.tt_epoch1, "t0": data.t0,
        "t1": data.t1, "filter_uids": list(data.filter_uids),
        "name": data.name, "identity": build_identity()}) + "\n")
    return H.Dataset(log=log_p, scale=data.scale, tt_epoch1=data.tt_epoch1,
                     t0=data.t0, t1=data.t1, filter_uids=data.filter_uids,
                     name=data.name), d


def ensure_store(d: Path, backend: str, log: Path) -> Path:
    """Replay the reference log into `backend` once. The `.ok` marker is a
    JSON object — build_identity() plus "backend", "replay_s" (wall time of
    the replay), and "dataset_fingerprint" (the fingerprint recorded in
    d/meta.json's identity, or None) — written only after a complete
    replay, so an interrupted build is rebuilt rather than trusted. It is a
    cache hit only when its "fingerprint" matches format_fingerprint() for
    the current working tree; otherwise the marker and store directory are
    FORMAT_PATHS above: a stale store silently served a superseded format)."""
    store, ok = d / f"store-{backend}", d / f"store-{backend}.ok"
    current_fp = format_fingerprint()
    marker = _read_marker(ok)
    state = _cache_state(marker, current_fp)
    if state == "hit" and store.exists():
        return store

    if ok.exists() or store.exists():
        if state == "hit":
            print(f"  store-{backend} directory missing — rebuilding", flush=True)
        else:
            print(_cache_miss_reason(state, marker, current_fp,
                                     f"store-{backend}"), flush=True)
    if ok.exists():
        ok.unlink()
    if store.exists():
        shutil.rmtree(store)

    print(f"  replaying into {backend} store ...", flush=True)
    t0 = time.perf_counter()
    H.load_store(store, backend, log)
    replay_s = time.perf_counter() - t0

    dataset_meta = _read_marker(d / "meta.json")
    dataset_fp = None
    if dataset_meta is not None:
        dataset_fp = (dataset_meta.get("identity") or {}).get("fingerprint")

    marker_obj = {**build_identity(), "backend": backend,
                  "replay_s": round(replay_s, 3),
                  "dataset_fingerprint": dataset_fp}
    ok.write_text(json.dumps(marker_obj) + "\n")
    return store


def queries_for(data: H.Dataset, ids: tuple[str, ...]) -> list[dict[str, Any]]:
    reg = {q.id: q for q in H.registry(data.t0, data.t1, data.tt_epoch1,
                                       data.filter_uids)}
    return [{"id": i, "op": reg[i].op, "args": reg[i].args} for i in ids]


# --------------------------------------------------------------------------- #
# the worker: one process, one suite — every mode shells out to this
# --------------------------------------------------------------------------- #

def evict_store_pages(store: Path) -> int:
    """Ask the kernel to drop the page cache for every file under `store`.

    User-space eviction: DONTNEED
    drops clean pages, and a read-only store's pages are clean. Returns
    the number of files advised.
    """
    n = 0
    for p in sorted(store.rglob("*")):
        if not p.is_file():
            continue
        fd = os.open(p, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
        n += 1
    return n


def _vm_status() -> dict[str, int]:
    """VmRSS/VmHWM in kB — the numbers of record for memory (ru_maxrss is"""
    out: dict[str, int] = {}
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:", "VmSize:")):
                k, v = line.split(":", 1)
                out[k.lower() + "_kb"] = int(v.split()[0])
    except OSError:
        pass
    return out


def _open_adapter(cfg: dict[str, Any]):
    from gnomon.temporal.algebra import ensure_all_registered
    ensure_all_registered()
    backend = cfg["backend"]
    if backend == "duckdb" and cfg.get("duckdb_threads"):
        from gnomon.storage.duckdb_adapter import DuckDBAdapter
        return DuckDBAdapter(Path(cfg["store"]) / "store.duckdb",
                             threads=int(cfg["duckdb_threads"]))
    import gnomon
    return gnomon.open(Path(cfg["store"]), backend=backend).adapter


def run_suite(cfg: dict[str, Any]) -> dict[str, Any]:
    """Execute one suite per the config; see `_suite_config` for the shape."""
    from gnomon.temporal.algebra import call_operator

    if cfg.get("evict"):
        evict_store_pages(Path(cfg["store"]))
    t0 = time.perf_counter()
    adapter = _open_adapter(cfg)
    open_ms = (time.perf_counter() - t0) * 1e3

    queries = cfg["queries"]
    warmup = cfg.get("warmup", H.WARMUP)
    reps = cfg.get("reps")            # fixed count; None = fast/slow protocol
    duration = cfg.get("duration_s")  # loop the mix for this long instead

    results: list[dict[str, Any]] = []
    timings: dict[str, list[float]] = {q["id"]: [] for q in queries}
    errors: dict[str, str] = {}

    def one(q: dict[str, Any]):
        return call_operator(adapter, q["op"], dict(q["args"]))

    if duration is None:
        for q in queries:
            try:
                for _ in range(warmup):
                    payload = one(q)
                t = time.perf_counter()
                payload = one(q)
                first = (time.perf_counter() - t) * 1e3
                ts = [first]
                n = reps if reps is not None else (
                    H.REPS_FAST if first < 1000.0 else H.REPS_SLOW)
                for _ in range(n - 1):
                    t = time.perf_counter()
                    payload = one(q)
                    ts.append((time.perf_counter() - t) * 1e3)
                p50, p95 = H._p50_p95(ts)
                results.append({
                    "query": q["id"], "ok": True,
                    "hash": H.canonical_hash(payload),
                    "p50_ms": p50, "p95_ms": p95, "first_ms": round(ts[0], 3),
                    "rows": H._answer_size(payload),
                    "timings_ms": [round(x, 3) for x in ts]})
            except Exception as e:  # a refusal is data, not a crash
                results.append({"query": q["id"], "ok": False,
                                "error": f"{type(e).__name__}: {e}"[:160]})
        wall_s = None
        done = sum(len(r.get("timings_ms", [])) for r in results)
    else:
        for _ in range(warmup):     # one warm pass over the whole mix
            for q in queries:
                try:
                    one(q)
                except Exception:
                    pass
        if cfg.get("start_at"):     # barrier: readers overlap by wall clock
            while time.time() < cfg["start_at"]:
                time.sleep(0.01)
        t_begin = time.perf_counter()
        deadline = t_begin + duration
        done = 0
        while time.perf_counter() < deadline:
            for q in queries:
                try:
                    t = time.perf_counter()
                    one(q)
                    timings[q["id"]].append((time.perf_counter() - t) * 1e3)
                    done += 1
                except Exception as e:
                    errors[q["id"]] = f"{type(e).__name__}: {e}"[:160]
        wall_s = time.perf_counter() - t_begin
        for q in queries:
            ts = timings[q["id"]]
            if not ts:
                results.append({"query": q["id"], "ok": False,
                                "error": errors.get(q["id"], "no completions")})
                continue
            p50, p95 = H._p50_p95(ts)
            results.append({"query": q["id"], "ok": True, "p50_ms": p50,
                            "p95_ms": p95, "count": len(ts),
                            "timings_ms": [round(x, 3) for x in ts[:2000]]})

    cache = None
    try:  # receipts for the byte-budget segment cache
        entries, cbytes, budget, evictions = adapter._store.segment_cache_stats()
        cache = {"entries": entries, "bytes": cbytes, "budget": budget,
                 "evictions": evictions}
    except AttributeError:
        pass
    return {"open_ms": round(open_ms, 3), "results": results,
            "wall_s": round(wall_s, 3) if wall_s is not None else None,
            "queries_done": done, "segment_cache": cache, **_vm_status()}


def _suite_config(store: Path, backend: str, queries: list[dict[str, Any]],
                  **kw: Any) -> dict[str, Any]:
    return {"store": str(store), "backend": backend, "queries": queries, **kw}


def spawn_suite(cfg: dict[str, Any], env_extra: dict[str, str] | None = None,
                argv: list[str] | None = None) -> subprocess.Popen:
    cmd = argv or [sys.executable, str(Path(__file__).resolve()), "_suite"]
    env = {**os.environ, **(env_extra or {})}
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, env=env)
    assert p.stdin is not None
    p.stdin.write(json.dumps(cfg))
    p.stdin.close()
    p.stdin = None  # communicate() must not flush a closed pipe
    return p


def collect(p: subprocess.Popen, timeout_s: float | None = None,
            docker_name: str | None = None) -> dict[str, Any]:
    try:
        out, err = p.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        # killing the docker *client* leaves the container running; kill it
        # by name so a thrashing capped run cannot outlive its budget
        if docker_name:
            subprocess.run(["docker", "kill", docker_name],
                           capture_output=True, timeout=60)
        p.kill()
        out, err = p.communicate()
        return {"failed": True, "timed_out": True, "timeout_s": timeout_s,
                "stderr": (err or "").strip()[-2000:]}
    if p.returncode != 0:
        return {"failed": True, "returncode": p.returncode,
                "stderr": err.strip()[-2000:]}
    return json.loads(out)


# --------------------------------------------------------------------------- #
# mode: threads
# --------------------------------------------------------------------------- #

def cmd_threads(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    data, d = ensure_dataset(args.workdir, args.scale)
    queries = queries_for(data, THREAD_QUERIES)
    counts = [int(x) for x in args.threads.split(",")]
    systems = [s.strip() for s in args.systems.split(",") if s.strip()]

    records, mismatches = [], []
    hashes: dict[tuple[str, str], set[str]] = {}
    for backend in systems:
        store = ensure_store(d, backend, data.log)
        for t in counts:
            env = {"GNOMON_SCAN_THREADS": str(t)} if backend == "native" else None
            cfg = _suite_config(store, backend, queries,
                                duckdb_threads=t if backend == "duckdb" else None)
            r = collect(spawn_suite(cfg, env_extra=env))
            records.append({"backend": backend, "threads": t, **r})
            for res in r.get("results", []):
                if res.get("ok"):
                    hashes.setdefault((backend, res["query"]), set()).add(res["hash"])
                    print(f"  {backend:>7} t={t:<3} {res['query']:<18} "
                          f"p50 {res['p50_ms']:>9.1f} ms", flush=True)
                else:
                    print(f"  {backend:>7} t={t:<3} {res['query']:<18} "
                          f"FAILED {res.get('error')}", flush=True)
    for (backend, qid), hs in sorted(hashes.items()):
        if len(hs) > 1:
            mismatches.append(f"{backend}/{qid}")
    if mismatches:
        print(f"\n  HASHES DIFFER ACROSS THREAD COUNTS: {', '.join(mismatches)}")
    else:
        print("\n  all thread counts agree on every result hash")
    return (1 if mismatches else 0), {
        "mode": "threads", "thread_counts": counts, "systems": systems,
        "queries": [q["id"] for q in queries], "records": records,
        "hash_agree_across_counts": not mismatches}


# --------------------------------------------------------------------------- #
# mode: coldwarm
# --------------------------------------------------------------------------- #

def cmd_coldwarm(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    data, d = ensure_dataset(args.workdir, args.scale)
    queries = queries_for(data, REPR_QUERIES)
    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    if not hasattr(os, "posix_fadvise"):
        print("  WARNING: no posix_fadvise on this platform — the 'cold' "
              "state cannot be produced here (Linux only)")
    records = []
    for backend in systems:
        store = ensure_store(d, backend, data.log)
        # warm: the published protocol, in one process
        warm = collect(spawn_suite(_suite_config(store, backend, queries)))
        records.append({"backend": backend, "state": "warm", **warm})
        for res in warm.get("results", []):
            if res.get("ok"):
                print(f"  {backend:>7} warm         {res['query']:<18} "
                      f"p50 {res['p50_ms']:>9.1f} ms", flush=True)
        # colder states: fresh process per trial, one query, one shot
        for state, evict in (("process_cold", False), ("cold", True)):
            for q in queries:
                trials = []
                for _ in range(args.trials):
                    cfg = _suite_config(store, backend, [q], warmup=0, reps=1,
                                        evict=evict)
                    trials.append(collect(spawn_suite(cfg)))
                firsts = [t["results"][0]["first_ms"] for t in trials
                          if t.get("results") and t["results"][0].get("ok")]
                opens = [t["open_ms"] for t in trials if "open_ms" in t]
                rec = {"backend": backend, "state": state, "query": q["id"],
                       "first_ms": sorted(firsts), "open_ms": sorted(opens),
                       "median_first_ms":
                           round(statistics.median(firsts), 3) if firsts else None,
                       "median_open_ms":
                           round(statistics.median(opens), 3) if opens else None,
                       "trials": trials}
                records.append(rec)
                print(f"  {backend:>7} {state:<12} {q['id']:<18} "
                      f"first {rec['median_first_ms']} ms "
                      f"(open {rec['median_open_ms']} ms)", flush=True)
    return 0, {"mode": "coldwarm", "systems": systems, "trials": args.trials,
               "eviction": "posix_fadvise(fd,0,0,POSIX_FADV_DONTNEED) per store "
                           "file (user-space; no root on the host)",
               "queries": [q["id"] for q in queries], "records": records}


# --------------------------------------------------------------------------- #
# mode: memcap
# --------------------------------------------------------------------------- #

def docker_memory_enforced() -> bool:
    """Can Docker actually enforce --memory here? Read the limit back from
    the container's own cgroup — an allocation canary is wrong on a host
    with swap but no swap-limit support (cgroup v1: memory is capped, the
    overflow swaps, so the canary survives while the cap is real)."""
    code = ("import pathlib\n"
            "for p in ('/sys/fs/cgroup/memory/memory.limit_in_bytes',"
            " '/sys/fs/cgroup/memory.max'):\n"
            "    q = pathlib.Path(p)\n"
            "    if q.exists():\n"
            "        print(q.read_text().strip()); break\n")
    try:
        r = subprocess.run(
            ["docker", "run", "--rm", "--memory", "256m",
             "python:3.12-slim", "python", "-c", code],
            capture_output=True, text=True, timeout=300)
        return int(r.stdout.strip()) <= 512 * 1024 * 1024
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return False


#: cgroup v1's "no limit" sentinel is `LONG_MAX` rounded down to a page
#: boundary (`9223372036854771712` on a 4 KiB-page x86_64 host) -- an
#: 8-exabyte "cap" is not a cap. Anything at or above this floor is treated
#: as unlimited, the same as v2's literal `"max"`.
_V1_UNLIMITED_FLOOR = 1 << 62


def _read_int_file(path: Path) -> int | None:
    """One cgroup counter file: an int, `None` for `"max"`/empty/unreadable.
    Never raises -- a missing or unreadable file is "not exposed", not a
    fatal error, since this is a diagnostic probe, not the query-timing path.
    """
    try:
        text = path.read_text().strip()
    except OSError:
        return None
    if text in ("max", ""):
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_self_cgroup_memory(cgroup_text: str, sys_fs_cgroup: Path) -> dict[str, Any]:
    """Parse `/proc/self/cgroup`'s content and read this process's own
    memory-controller files under `sys_fs_cgroup`.

    The Slurm-cgroup analogue of `docker_memory_enforced()`'s container-side
    a Slurm job submitted with `--mem=CAP` still runs this process inside a
    cgroup the Slurm cgroup plugin created, with a real memory limit on it --
    this reads that limit (and current/peak usage) directly, instead of
    spinning up a container to ask the same question. `cgroup_text` and
    `sys_fs_cgroup` are both parameters (not read from fixed paths inside
    this function) so a test can hand it a fixture tree instead of a real
    cgroup, per the campaign brief's own instruction.

    **v2** (single unified hierarchy: one line, `0::<path>`): `memory.max`
    (literal `"max"` means uncapped -> `None`), `memory.current`,
    `memory.peak` (kernel >=5.19; absent on an older kernel, so `None` there
    means "not exposed", not "no peak yet").

    **v1** (one line per controller: `hierarchy-id:controller-list:path`):
    the line whose comma-separated controller list contains `"memory"`;
    `memory.limit_in_bytes` (the kernel's own unlimited sentinel is folded
    into `None`, same meaning as v2's `"max"`), `memory.usage_in_bytes`,
    `memory.max_usage_in_bytes` (peak since the cgroup was created).

    Returns `{"cgroup_version": "v2"|"v1"|None, "path": str|None,
    "max_bytes": int|None, "current_bytes": int|None, "peak_bytes": int|None}`
    -- `cgroup_version: None` means neither shape matched (no `/proc/self/
    cgroup`, or a hierarchy this parser does not recognize).
    """
    for line in cgroup_text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        hier_id, controllers, rel_path = parts
        rel = rel_path.lstrip("/")
        if hier_id == "0" and controllers == "":
            base = sys_fs_cgroup / rel if rel else sys_fs_cgroup
            return {"cgroup_version": "v2", "path": str(base),
                    "max_bytes": _read_int_file(base / "memory.max"),
                    "current_bytes": _read_int_file(base / "memory.current"),
                    "peak_bytes": _read_int_file(base / "memory.peak")}
        if "memory" in controllers.split(","):
            base = sys_fs_cgroup / "memory" / rel if rel else sys_fs_cgroup / "memory"
            limit = _read_int_file(base / "memory.limit_in_bytes")
            if limit is not None and limit >= _V1_UNLIMITED_FLOOR:
                limit = None
            return {"cgroup_version": "v1", "path": str(base),
                    "max_bytes": limit,
                    "current_bytes": _read_int_file(base / "memory.usage_in_bytes"),
                    "peak_bytes": _read_int_file(base / "memory.max_usage_in_bytes")}
    return {"cgroup_version": None, "path": None, "max_bytes": None,
            "current_bytes": None, "peak_bytes": None}


def cgroup_memory_probe(cgroup_file: Path = Path("/proc/self/cgroup"),
                        sys_fs_cgroup: Path = Path("/sys/fs/cgroup")) -> dict[str, Any]:
    """`parse_self_cgroup_memory` against the real filesystem, with the
    Docker-free fallback the campaign brief asks for: when no cgroup memory
    cap is in effect (no `/proc/self/cgroup` at all -- e.g. this dev laptop,
    which has no Linux cgroups -- or a cgroup with no limit configured),
    report `resource.getrusage` peak RSS instead, alongside
    `GNOMON_SEGMENT_CACHE_BYTES` -- the knob under test when there is no
    OS-level cap left to observe.
    """
    try:
        text = cgroup_file.read_text()
    except OSError:
        text = ""
    info = parse_self_cgroup_memory(text, sys_fs_cgroup)
    if info["max_bytes"] is None:
        ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        info["fallback"] = "rusage_maxrss"
        # Linux reports ru_maxrss in kB, macOS in bytes (same asymmetry
        # `_rss_kb`/`_vm_status`-style helpers elsewhere in this file work
        # around).
        info["peak_rss_bytes"] = ru if sys.platform == "darwin" else ru * 1024
        cache_env = os.environ.get("GNOMON_SEGMENT_CACHE_BYTES")
        info["segment_cache_bytes_env"] = (int(cache_env) if cache_env and
                                           cache_env.isdigit() else cache_env)
    return info


def _parse_cap(cap: str) -> int:
    units = {"g": 1 << 30, "m": 1 << 20, "k": 1 << 10}
    return int(float(cap[:-1]) * units[cap[-1].lower()]) if cap[-1].lower() in units \
        else int(cap)


def _site_packages() -> str:
    import sysconfig
    return sysconfig.get_paths()["purelib"]


def cmd_memcap(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    data, d = ensure_dataset(args.workdir, args.scale)
    queries = queries_for(data, REPR_QUERIES)
    store = ensure_store(d, "native", data.log)
    caps = [c.strip() for c in args.caps.split(",") if c.strip()]

    use_docker = args.enforce == "docker" or (
        args.enforce == "auto" and docker_memory_enforced())
    # `--enforce cgroup-self` skips both the docker and rlimit branches and
    # instead *reads* the cgroup Slurm's own `--mem` already put this process
    # in (`cgroup_memory_probe`), the same way the uncapped "none" reference
    # run below spawns a plain subprocess with no enforcement of its own.
    use_cgroup_self = args.enforce == "cgroup-self"
    method = ("cgroup-self" if use_cgroup_self else
             "docker" if use_docker else "rlimit_as")
    print(f"  enforcement: {method}", flush=True)

    # reduced repetitions: under a 2 GB cap a 10M scan can take minutes,
    # and the question is where the knee is, not a tight median
    proto = {"warmup": args.warmup, "reps": args.reps}
    records = []
    # uncapped reference under the identical reduced protocol
    ref = collect(spawn_suite(_suite_config(store, "native", queries, **proto)))
    records.append({"cap": None, "method": "none", **ref})
    for res in ref.get("results", []):
        if res.get("ok"):
            print(f"  uncapped     {res['query']:<18} p50 {res['p50_ms']:>9.1f} ms",
                  flush=True)

    def run_capped(cap: str, qs: list[dict[str, Any]], tag: str) -> dict[str, Any]:
        cfg = _suite_config(store, "native", qs, **proto)
        name = None
        if use_docker:
            name = f"gnomon-memcap-{tag}-{os.getpid()}"
            mounts = sorted({str(ROOT), str(store.parents[1]),
                             _site_packages().split("/lib/")[0]})
            cmd = ["docker", "run", "--rm", "-i", "--name", name,
                   "--network", "none",
                   "--memory", cap, "--memory-swap", cap]
            for m in mounts:
                cmd += ["-v", f"{m}:{m}:ro"]
            # site-packages via PYTHONPATH skips .pth processing, so the
            # editable install does not resolve from it — the repo root is
            # appended explicitly instead (mounted at its host path).
            cmd += ["-e", f"PYTHONPATH={_site_packages()}:{ROOT}"]
            # the cache budget must cross into the container explicitly —
            # inside a cgroup /proc/meminfo still shows host RAM, so the
            # engine's half-of-RAM default cannot see the cap
            if os.environ.get("GNOMON_SEGMENT_CACHE_BYTES"):
                cmd += ["-e", "GNOMON_SEGMENT_CACHE_BYTES="
                        f"{os.environ['GNOMON_SEGMENT_CACHE_BYTES']}"]
            cmd += ["python:3.12-slim", "python",
                    str(Path(__file__).resolve()), "_suite"]
            p = spawn_suite(cfg, argv=cmd)
        elif use_cgroup_self:
            # No enforcement here on purpose: the cap named by `cap` is a
            # record of what this job was *submitted* with (Slurm's own
            # `--mem`), not something this process can or should re-apply --
            # the cgroup plugin already bounds the whole job's process tree.
            p = spawn_suite(cfg)
        else:
            cfg["rlimit_as_bytes"] = _parse_cap(cap)
            p = spawn_suite(cfg)
        r = collect(p, timeout_s=args.cap_timeout, docker_name=name)
        r["oom"] = bool(r.get("failed")) and (
            r.get("returncode") in (137, -9)
            or "MemoryError" in r.get("stderr", ""))
        if use_cgroup_self:
            r["cgroup"] = cgroup_memory_probe()
        return r

    for cap in caps:
        if args.per_query:
            # one container per query: a whole-suite OOM says the *suite*
            # exceeds the cap; this says which queries do on their own
            for q in queries:
                r = run_capped(cap, [q], f"{cap}-{q['id'].replace('.', '-')}")
                records.append({"cap": cap, "method": method,
                                "query": q["id"], **r})
                if r.get("failed"):
                    what = ("timed out" if r.get("timed_out")
                            else "OOM-killed" if r["oom"] else "FAILED")
                    print(f"  cap {cap:<6} {q['id']:<18} {what} "
                          f"(rc={r.get('returncode')})", flush=True)
                else:
                    res = r["results"][0]
                    msg = (f"p50 {res['p50_ms']:>9.1f} ms  "
                           f"hwm {r.get('vmhwm_kb', 0) // 1024} MB"
                           if res.get("ok") else f"FAILED {res.get('error')}")
                    print(f"  cap {cap:<6} {q['id']:<18} {msg}", flush=True)
            continue
        r = run_capped(cap, queries, cap)
        records.append({"cap": cap, "method": method, **r})
        if r.get("failed"):
            what = ("timed out (thrash budget exceeded)" if r.get("timed_out")
                    else "OOM-killed" if r["oom"] else "FAILED")
            print(f"  cap {cap:<6} {what} (rc={r.get('returncode')})", flush=True)
        else:
            for res in r.get("results", []):
                msg = (f"p50 {res['p50_ms']:>9.1f} ms" if res.get("ok")
                       else f"FAILED {res.get('error')}")
                print(f"  cap {cap:<6} {res['query']:<18} {msg}", flush=True)
    swap_kb = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("SwapTotal:"):
                swap_kb = int(line.split()[1])
    except OSError:
        pass
    return 0, {"mode": "memcap", "caps": caps, "method": method,
               "per_query": bool(args.per_query),
               "host_swap_kb": swap_kb,
               "enforcement_note":
                   "docker --memory bounds residency; on a cgroup-v1 kernel "
                   "without swap-limit support the overflow may swap rather "
                   "than OOM, which is the working-set>RAM behavior the IR asks "
                   "about. RLIMIT_AS instead caps address space (mmapped store "
                   "files count), a strictly harsher approximation.",
               "protocol_note": f"reduced reps: warmup={args.warmup}, "
                                f"reps={args.reps} (feasibility under caps)",
               "queries": [q["id"] for q in queries], "records": records}


# --------------------------------------------------------------------------- #
# mode: readers
# --------------------------------------------------------------------------- #

def cmd_readers(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    data, d = ensure_dataset(args.workdir, args.scale)
    queries = queries_for(data, READER_MIX)
    store = ensure_store(d, "native", data.log)
    counts = [int(x) for x in args.readers.split(",")]

    # one warm pre-pass so every config starts from the same page cache
    collect(spawn_suite(_suite_config(store, "native", queries, warmup=1, reps=1)))

    records = []
    for n in counts:
        start_at = time.time() + args.barrier_s
        procs = [spawn_suite(_suite_config(
            store, "native", queries, warmup=1,
            duration_s=args.duration, start_at=start_at)) for _ in range(n)]
        readers = [collect(p) for p in procs]
        done = sum(r.get("queries_done", 0) for r in readers if not r.get("failed"))
        walls = [r["wall_s"] for r in readers if r.get("wall_s")]
        agg_qps = done / max(walls) if walls else 0.0
        per_query = {}
        for q in queries:
            p50s = [res["p50_ms"] for r in readers if not r.get("failed")
                    for res in r["results"]
                    if res["query"] == q["id"] and res.get("ok")]
            per_query[q["id"]] = {
                "reader_p50s_ms": [round(x, 3) for x in sorted(p50s)],
                "median_reader_p50_ms":
                    round(statistics.median(p50s), 3) if p50s else None}
        records.append({"readers": n, "aggregate_qps": round(agg_qps, 2),
                        "queries_done": done, "per_query": per_query,
                        "raw": readers})
        line = " ".join(f"{q['id']}={per_query[q['id']]['median_reader_p50_ms']}"
                        for q in queries)
        print(f"  n={n:<3} agg {agg_qps:8.2f} q/s | median reader p50: {line}",
              flush=True)
    return 0, {"mode": "readers", "reader_counts": counts,
               "duration_s": args.duration,
               "queries": [q["id"] for q in queries], "records": records}


# --------------------------------------------------------------------------- #

def manifest(args: argparse.Namespace, data: H.Dataset) -> dict[str, Any]:
    m = H.manifest(data, systems=["native"])
    m["protocol"]["resource_mode"] = args.mode
    return m


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "_suite":
        # the worker path must work under a bare container python: no argparse
        # niceties, and rlimit is applied before any large import
        cfg = json.load(sys.stdin)
        if cfg.get("rlimit_as_bytes"):
            import resource
            cap = int(cfg["rlimit_as_bytes"])
            resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
        # anything the store or an operator prints must not corrupt the
        # one-JSON-on-stdout contract
        real_stdout = sys.stdout
        sys.stdout = sys.stderr
        try:
            result = run_suite(cfg)
        finally:
            sys.stdout = real_stdout
        print(json.dumps(result))
        return 0

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("mode", choices=["threads", "coldwarm", "memcap", "readers"])
    ap.add_argument("--scale", type=int, default=1_000_000)
    ap.add_argument("--workdir", type=Path, default=default_workdir())
    ap.add_argument("--json", type=Path)
    ap.add_argument("--systems", default="native,duckdb",
                    help="threads/coldwarm only")
    ap.add_argument("--threads", default="1,2,4,8,16,32")
    ap.add_argument("--trials", type=int, default=5, help="cold trials per query")
    ap.add_argument("--caps", default="2g,4g,8g")
    ap.add_argument("--enforce",
                    choices=["auto", "docker", "rlimit", "cgroup-self"],
                    default="auto",
                    help="'cgroup-self' (S3 axis 5, no Docker on the host): "
                         "read the cgroup Slurm's own --mem already applied "
                         "to this job instead of enforcing a new one")
    ap.add_argument("--warmup", type=int, default=2, help="memcap warmups")
    ap.add_argument("--reps", type=int, default=5, help="memcap reps")
    ap.add_argument("--cap-timeout", type=float, default=3600.0,
                    help="wall budget per capped suite before it is killed")
    ap.add_argument("--per-query", action="store_true",
                    help="memcap: one container per query instead of one "
                         "per cap, to attribute an OOM to a query")
    ap.add_argument("--readers", default="1,2,4,8,16")
    ap.add_argument("--duration", type=float, default=45.0,
                    help="seconds each reader loops the mix")
    ap.add_argument("--barrier-s", type=float, default=20.0,
                    help="lead time for readers to open + warm before the window")
    args = ap.parse_args()

    args.workdir.mkdir(parents=True, exist_ok=True)
    data, _ = ensure_dataset(args.workdir, args.scale)
    meta = manifest(args, data)
    print(f"resource harness [{args.mode}] on {data.name} @ {data.scale:,} events")
    print(f"  commit {meta['commit']}{' (dirty)' if meta['dirty'] else ''} | "
          f"{meta['platform']} | {meta['cpu_count']} cores")
    if not meta["on_measurement_host"]:
        print(f"  WARNING: not {H.MEASUREMENT_HOST} — development run, timings "
              f"are not comparable with reported numbers")
    print()

    rc, payload = {"threads": cmd_threads, "coldwarm": cmd_coldwarm,
                   "memcap": cmd_memcap, "readers": cmd_readers}[args.mode](args)
    if args.json:
        args.json.write_text(json.dumps(
            {"manifest": meta, **payload}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        print(f"  wrote {args.json}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
