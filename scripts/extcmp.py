"""External system comparison driver.

One server, one pinned engine commit, ten systems, one frozen event log per
input size. Loading and answering are separate steps so that a loaded system
can be stopped and restarted between query blocks (the timed phase interleaves
systems by block rather than running one system's series at another time of
day):

    extcmp.py ref    --log L --store S              replay L into the native reference store S
    extcmp.py load   --system X --log L --ref S ... load X (server must be up) from S / from L
    extcmp.py query  --system X --log L --ref S --mode verify|timed --json out.json
    extcmp.py verdict --base native.json other.json ...   per-query agreement table

Every external system is loaded from the *same* replayed native store's
byte-for-byte), except XTDB, which receives the log's ops and performs its own
SQL:2011 supersession (its idiom, as in the August evaluation), and AeonG,
which receives the version history as a sequence of transactions so that its
own transaction-time axis carries belief time.

Verification compares `canonical_hash` (eval_harness) exactly. For systems that
cannot reproduce Gnomon version ids (XTDB, Raphtory) a second, *projected* hash
is compared: the payload with `vid`, `tt_s`, `tt_e` removed at every depth and
every list of records sorted canonically — the field-level comparison the
project uses wherever ids cannot match by construction. Which comparison
carried a verdict is recorded per cell.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import eval_harness as H  # noqa: E402
import engine_time as ET  # noqa: E402
import cov_mark as CM
import sysworker as SW  # noqa: E402

import gnomon  # noqa: E402
from gnomon.core.model import canonical_json, sha256_hex  # noqa: E402
from gnomon.temporal.algebra import call_operator, ensure_all_registered  # noqa: E402

#: The ids every system is asked: the thirteen of the August comparison plus
#: the three fixed-window (`*.w10`) variants, which are the same operator
#: calls with a different window and need no new code on any system.
CORE_IDS = (
    "hist.single", "hist.asof", "snap.hop2", "diff.global", "reach.window",
    "paths.k", "series.count", "burst.zscore", "nbr.evolution",
    "coactive.narrow", "resolve.substr", "agg.rel_bucket", "motif.filtered",
    "series.count.w10", "agg.rel_bucket.w10", "burst.zscore.w10",
)
#: Registry ids that are native-only by the registry's own definition: two
#: harness bypasses for the sorted name index and the W17 belief diff, whose
#: four-column emulation the base adapter contract says is a different query
#: (scored separately, in the belief-axis study).
NATIVE_ONLY_IDS = ("resolve.exact", "resolve.prefix", "diff.belief")

W10_ALIAS = {"series.count.w10": "series.count",
             "agg.rel_bucket.w10": "agg.rel_bucket",
             "burst.zscore.w10": "burst.zscore"}

#: Per-query wall cap in verification (server-side statement caps are set in
#: servers.sh / the session; this is the client-side backstop).
VERIFY_CAP_S = int(os.environ.get("EXTCMP_VERIFY_CAP_S", "1800"))

#: Server-side statement limit (PostgreSQL statement_timeout, ClickHouse
#: max_execution_time). Unset: the verification cap, as before. The P1 jobs
#: set it below the client-side alarm, so the server cancels first and the
#: session stays usable.
SERVER_LIMIT_S = int(os.environ.get("EXTCMP_SERVER_LIMIT_S", VERIFY_CAP_S))

PROJECT_DROP = ("vid", "tt_s", "tt_e")

#: Phase B measurement protocol.
#: A call slower than CAP_S switches the cell to 1 warmup + 3 repetitions; a
#: call that reaches CALL_CEILING_S is aborted and the cell recorded as
#: ">3600 s, aborted". Server-side statement limits are set slightly above the
#: ceiling (EXTCMP_VERIFY_CAP_S in the runner) as a backstop.
CAP_S = 300
CALL_CEILING_S = int(os.environ.get("EXTCMP_CALL_CEILING_S", "3600"))

#: Query families: one timed block = one family on
#: every system. Native-only ids ride in the family of their operator and are
#: timed for native only.
FAMILIES = {
    "P": ("hist.single", "hist.asof"),
    "T": ("snap.hop2", "reach.window", "paths.k"),
    "S": ("series.count", "burst.zscore", "agg.rel_bucket"),
    "W": ("series.count.w10", "burst.zscore.w10", "agg.rel_bucket.w10"),
    "D": ("diff.global",),
    "N": ("nbr.evolution", "coactive.narrow"),
    "R": ("resolve.substr",),
    "M": ("motif.filtered",),
}
NATIVE_ONLY_BY_FAMILY = {"D": ("diff.belief",), "R": ("resolve.exact", "resolve.prefix")}
#: The correction-density study's belief-time family (extcmp_beliefq); not
#: part of the shared-registry rehearsal schedule.
FAMILIES["B"] = ("b.snap.hop2@T", "b.snap.hop2@now", "b.reach@T", "b.reach@now",
                 "b.diff", "b.diff@ctl", "b.hist", "b.hist@now")

#: Call metadata the engine has added to every operator payload since the
#: August comparison: the `plan_ir` block (plan/node digests, canonical args,
#: provenance). It describes the call, not the answer, and no baseline
#: produces it, so it is removed before hashing exactly like the harness's
#: own `VOLATILE` fields. (`eval_harness.canonical_hash` at this commit still
#: hashes it, so a plain `eval_harness.py --systems native,postgres` run
#: reports every native cell as differing from every baseline.)
EXTRA_VOLATILE = ("plan_ir",)


def cell_key(cell: dict[str, Any]) -> str:
    """A cell's identity: the registry id, plus `#<instance>` in parameter-file
    mode (P1, `extcmp_p1.py`). Without an instance it is the registry id, so
    every record written with the mode off is keyed exactly as before."""
    inst = cell.get("instance")
    return cell["query"] if inst is None else f"{cell['query']}#{inst}"


def answer_hash(payload: dict[str, Any]) -> str:
    return H.canonical_hash({k: v for k, v in payload.items() if k not in EXTRA_VOLATILE})


# --------------------------------------------------------------------------- #
# comparison                                                                    #
# --------------------------------------------------------------------------- #

def _project(x: Any) -> Any:
    if isinstance(x, dict):
        return {k: _project(v) for k, v in x.items()
                if k not in PROJECT_DROP and k not in H.VOLATILE
                and k not in EXTRA_VOLATILE}
    if isinstance(x, list):
        items = [_project(v) for v in x]
        if items and all(isinstance(v, dict) for v in items):
            items.sort(key=canonical_json)
        return items
    return x


def projected_hash(payload: dict[str, Any]) -> str:
    return sha256_hex(canonical_json(_project(payload)))[:32]


# --------------------------------------------------------------------------- #
# dataset                                                                       #
# --------------------------------------------------------------------------- #

def dataset(log: Path) -> H.Dataset:
    """`dataset_from_log`, cached beside the log (a pure function of its bytes)."""
    side = log.with_suffix(".extcmp-params.json")
    st = log.stat()
    if side.exists():
        d = json.loads(side.read_text())
        if d.get("size") == st.st_size and d.get("mtime_ns") == st.st_mtime_ns:
            return H.Dataset(log=log, scale=d["scale"], tt_epoch1=d["tt_epoch1"],
                             t0=d["t0"], t1=d["t1"],
                             filter_uids=tuple(d["filter_uids"]), name=log.stem)
    data = H.dataset_from_log(log)
    side.write_text(json.dumps({
        "size": st.st_size, "mtime_ns": st.st_mtime_ns, "scale": data.scale,
        "tt_epoch1": data.tt_epoch1, "t0": data.t0, "t1": data.t1,
        "filter_uids": list(data.filter_uids)}))
    return data


def queries(data: H.Dataset, ids: list[str] | None) -> list[H.Query]:
    qs = H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)
    if ids and any(i.startswith("b.") for i in ids):
        import extcmp_beliefq as B      # the correction-density study's queries
        qs = qs + B.belief_queries(
            data, B.derive_params(data.log, data.scale, data.t0, data.t1))
    if ids:
        by = {q.id: q for q in qs}
        return [by[i] for i in ids]
    return qs


# --------------------------------------------------------------------------- #
# systems                                                                       #
# --------------------------------------------------------------------------- #

def _dir_bytes(p: Path) -> int | None:
    return H._dir_bytes(p)


def open_existing_store(path: Path, backend: str = "native", *, read_only: bool = True):
    """Open a store that must already exist and hold rows.

    `gnomon.open` creates a store at a missing path. Every comparison step reads
    a store some earlier window built (the reference for a loader, the native
    store for its own verify), so a missing one means that window did not run
    on this node; opening it would create an empty store, the loaders would copy
    zero rows and every system would agree on empty answers. Refuse instead:
    the path must be a non-empty directory, it is opened read-only, and it must
    hold at least one node or edge version."""
    p = Path(path)
    if not p.is_dir() or not any(p.iterdir()):
        raise SystemExit(f"store {p} ({backend}) is missing or empty: refusing to open it "
                         f"(gnomon.open would create it empty and every answer check would pass vacuously)")
    try:
        st = gnomon.open(p, backend=backend, read_only=read_only)
    except TypeError:                                       # a backend without read_only
        st = gnomon.open(p, backend=backend)
    require_rows(st, p)
    return st


def require_rows(st, p: Path) -> dict[str, Any]:
    try:
        stats = st.stats()
    except Exception as e:                                  # noqa: BLE001
        raise SystemExit(f"store {p}: cannot read its row counts ({type(e).__name__}: {e})")
    n = int(stats.get("n_edge_versions") or 0) + int(stats.get("n_node_versions") or 0)
    if n <= 0:
        raise SystemExit(f"store {p} holds no node or edge versions: refusing a vacuous comparison")
    return stats


_INPUT_KEYS = ("uid", "src", "dst")
_INPUT_LISTS = ("seeds", "node_filter")


def instance_inputs(args: dict[str, Any]) -> list[str]:
    """The node identities an instance names as its input (uid, src, dst, seeds,
    node_filter, a_spec/b_spec.src). Queries that name none (whole-graph
    aggregates, substring search) are covered by the store's own row count."""
    out = [args[k] for k in _INPUT_KEYS if isinstance(args.get(k), str)]
    for k in _INPUT_LISTS:
        if isinstance(args.get(k), list):
            out += [u for u in args[k] if isinstance(u, str)]
    for k in ("a_spec", "b_spec"):
        if isinstance(args.get(k), dict) and isinstance(args[k].get("src"), str):
            out.append(args[k]["src"])
    return out


def check_inputs(st, qs, key_of) -> None:
    """Verify only: every node an instance names must exist in the reference
    store (else the instance asks about nothing and passes vacuously)."""
    a = st.adapter
    seen: dict[str, bool] = {}
    bad = []
    for q in qs:
        miss = []
        for u in instance_inputs(q.args):
            if u not in seen:
                try:
                    seen[u] = bool(a.believed_node_versions(u))
                except Exception:                           # noqa: BLE001
                    seen[u] = False
            if not seen[u]:
                miss.append(u)
        if miss:
            bad.append(f"{key_of(q)}: {miss[:5]}")
    if bad:
        raise SystemExit(f"{len(bad)} instance(s) name nodes absent from the reference store "
                         f"(vacuous input): {bad[:10]}")


class System:
    name = "?"
    #: which comparison decides agreement: "exact" (canonical_hash) or
    #: "projected" (ids not reproducible by construction)
    compare = "exact"

    def __init__(self, ref: Path | None, workdir: Path, size_tag: str):
        self.ref, self.workdir, self.size_tag = ref, workdir, size_tag

    def ref_adapter(self):
        return open_existing_store(self.ref, "native").adapter

    def load(self, log: Path) -> dict[str, Any]:
        raise NotImplementedError

    def open(self) -> None:
        pass

    def fn_for(self, q: H.Query) -> Callable[[], dict[str, Any]] | None:
        raise NotImplementedError

    def close(self) -> None:
        pass

    def settings(self) -> dict[str, Any]:
        return {}


class Native(System):
    name, backend = "native", "native"

    @property
    def path(self) -> Path:
        return self.workdir / "stores" / f"{self.backend}-{self.size_tag}"

    def load(self, log: Path) -> dict[str, Any]:
        if self.path.exists():
            raise SystemExit(f"{self.path} exists; refusing to reload over it")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        t0 = time.perf_counter()
        H.load_store(self.path, self.backend, log)
        wall = time.perf_counter() - t0
        st = gnomon.open(self.path, backend=self.backend)
        try:
            stats = st.stats()
        except Exception:                                   # noqa: BLE001
            stats = {}
        st.close()
        return {"load_s": round(wall, 3), "phases": {"replay_with_compaction_trigger_s": round(wall, 3)},
                "bytes": _dir_bytes(self.path), "stats": stats}

    def open(self) -> None:
        ensure_all_registered()
        self.store = open_existing_store(self.path, self.backend, read_only=False)
        self.adapter = self.store.adapter

    def settings(self) -> dict[str, Any]:
        # the engine reads its design switches at open: record what the open
        # handle reports (designs()), not only the environment
        out: dict[str, Any] = {"gnomon_env": {k: v for k, v in sorted(os.environ.items())
                                            if k.startswith("GNOMON_")}}
        try:
            out["designs"] = self.adapter.designs()
        except Exception as e:                              # noqa: BLE001
            out["designs"] = f"unavailable: {type(e).__name__}: {e}"[:200]
        return out

    def fn_for(self, q):
        a = self.adapter
        if q.id.startswith("b."):
            import extcmp_beliefq as B
            f = B.engine_fn(a, q)
            if f is not None:
                return f
        if q.op == "lookup_name":
            return lambda: H._lookup_name_dispatch(a, q.args)
        if q.op == "diff_belief":
            return lambda: H._diff_belief_dispatch(a, q.args)
        return lambda: call_operator(a, q.op, dict(q.args))

    def close(self):
        self.store.close()


class DuckDB(Native):
    name, backend = "duckdb", "duckdb"

    def settings(self):
        import duckdb
        return {"duckdb": duckdb.__version__, "adapter": "gnomon DuckDB backend defaults"}


class _QueryModule(System):
    """A system answered by a `QUERIES` map of functions `fn(handle, **args)`."""

    module: Any = None

    def fn_for(self, q):
        qid = W10_ALIAS.get(q.id, q.id)
        if q.id.startswith("b."):
            import extcmp_beliefq as B
            f = B.system_fn(self.name, self.handle, q)
            if f is not None:
                return f
            qid = B.ALIAS.get(q.id, q.id)
        fn = self.module.QUERIES.get(qid)
        if fn is None:
            return None
        h = self.handle
        return lambda: fn(h, **q.args)


class Postgres(_QueryModule):
    name = "postgres"

    def load(self, log):
        import pg_baseline
        adapter = self.ref_adapter()
        pg_baseline.ensure_database()
        conn = pg_baseline.connect()
        pg_baseline.apply_tuning(conn)
        conn.execute(pg_baseline.SCHEMA)
        t0 = time.perf_counter()
        counts = pg_baseline.load(conn, adapter)
        t1 = time.perf_counter()
        conn.execute(pg_baseline.INDEXES)
        if os.environ.get("EXTCMP_BELIEFQ_INDEXES"):
            # correction-density study: the belief-axis indexes a competent
            # user adds for diffs over tt_s/tt_e (extcmp_beliefq.PG_INDEXES)
            import extcmp_beliefq as B
            for stmt in B.PG_INDEXES:
                conn.execute(stmt)
        t2 = time.perf_counter()
        conn.execute("ANALYZE edge_versions")
        conn.execute("ANALYZE node_versions")
        conn.execute("ANALYZE entities")
        conn.execute("CHECKPOINT")
        t3 = time.perf_counter()
        dbb = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        conn.close()
        return {"load_s": round(t3 - t0, 3),
                "phases": {"copy_s": round(t1 - t0, 3), "index_build_s": round(t2 - t1, 3),
                           "analyze_checkpoint_s": round(t3 - t2, 3)},
                "counts": counts, "pg_database_size": dbb}

    def open(self):
        import pg_baseline
        import pg_queries
        self.module = pg_queries
        self.handle = pg_baseline.connect()
        self.tuning = pg_baseline.apply_tuning(self.handle)
        self.handle.execute(f"SET statement_timeout = '{SERVER_LIMIT_S}s'")
        import prime_cap as PC
        PC.hook(lambda sec: self.handle.execute(f"SET statement_timeout = '{int(sec) if sec else SERVER_LIMIT_S}s'"))

    def cancel(self):
        self.handle.cancel_safe()

    def settings(self):
        return getattr(self, "tuning", {})

    def close(self):
        self.handle.close()


class ClickHouse(_QueryModule):
    name = "clickhouse"

    def load(self, log):
        import ch_baseline
        adapter = self.ref_adapter()
        client = ch_baseline.connect()
        ch_baseline.create_schema(client)
        t0 = time.perf_counter()
        counts = ch_baseline.load(client, adapter)
        t1 = time.perf_counter()
        parts = client.query("SELECT table, count(), sum(rows), sum(bytes_on_disk) "
                             "FROM system.parts WHERE active AND database = 'gnomon_eval' "
                             "GROUP BY table").result_rows
        return {"load_s": round(t1 - t0, 3), "phases": {"insert_s": round(t1 - t0, 3)},
                "counts": counts,
                "parts": [list(map(lambda v: v if isinstance(v, (int, str)) else int(v), r)) for r in parts],
                "bytes_on_disk_active_parts": int(sum(r[3] for r in parts))}

    def open(self):
        import ch_baseline
        import ch_queries
        self.module = ch_queries
        self.handle = ch_baseline.connect()
        self.handle.command(f"SET max_execution_time = {SERVER_LIMIT_S}")
        import prime_cap as PC
        PC.hook(lambda sec: self.handle.command(f"SET max_execution_time = {int(sec) if sec else SERVER_LIMIT_S}"))
        self.report = ch_baseline.settings_report(self.handle)

    def settings(self):
        return getattr(self, "report", {})


class Neo4j(_QueryModule):
    name = "neo4j"

    def _base(self):
        import neo4j_baseline
        return neo4j_baseline

    def _queries(self):
        import neo4j_queries
        return neo4j_queries

    def load(self, log):
        base = self._base()
        adapter = self.ref_adapter()
        drv = base.connect()
        base.create_schema(drv)
        t0 = time.perf_counter()
        counts = base.load(drv, adapter)
        t1 = time.perf_counter()
        extra = self._after_load(drv)
        t2 = time.perf_counter()
        drv.close()
        return {"load_s": round(t2 - t0, 3),
                "phases": {"unwind_create_s": round(t1 - t0, 3), **extra,
                           "post_load_s": round(t2 - t1, 3)},
                "counts": counts}

    def _after_load(self, drv) -> dict[str, Any]:
        with drv.session() as s:
            if os.environ.get("EXTCMP_BELIEFQ_INDEXES"):
                import extcmp_beliefq as B
                for stmt in B.NEO4J_INDEXES:
                    s.run(stmt).consume()
            s.run("CALL db.awaitIndexes(3600)").consume()
            idx = s.run("SHOW INDEXES YIELD name, state, populationPercent "
                        "RETURN name, state, populationPercent").data()
        return {"indexes": idx}

    def open(self):
        self.module = self._queries()
        self.handle = self._base().connect()

    def close(self):
        self.handle.close()


class Memgraph(Neo4j):
    name = "memgraph"

    def _base(self):
        import memgraph_baseline
        return memgraph_baseline

    def _queries(self):
        import memgraph_queries
        return memgraph_queries

    def _after_load(self, drv):
        with drv.session() as s:
            if os.environ.get("EXTCMP_BELIEFQ_INDEXES"):
                import extcmp_beliefq as B
                for stmt in B.MEMGRAPH_INDEXES:
                    s.run(stmt).consume()
            idx = s.run("SHOW INDEX INFO").data()
            s.run("CREATE SNAPSHOT").consume()
        return {"indexes": idx, "snapshot": "CREATE SNAPSHOT after load"}


class Raphtory(System):
    """Valid-time only: built from the currently-believed rows (see
    `eval_harness._raphtory_graph`), persisted with `save_to_file`."""

    name, compare = "raphtory", "projected-raphtory"

    @property
    def path(self) -> Path:
        return self.workdir / "stores" / f"raphtory-{self.size_tag}"

    def load(self, log):
        adapter = self.ref_adapter()
        t0 = time.perf_counter()
        g = self._graph(adapter)
        t1 = time.perf_counter()
        g.save_to_file(str(self.path))
        t2 = time.perf_counter()
        return {"load_s": round(t2 - t0, 3),
                "phases": {"build_graph_s": round(t1 - t0, 3), "save_to_file_s": round(t2 - t1, 3)},
                "edges": g.count_edges(), "nodes": g.count_nodes(),
                "bytes": _dir_bytes(self.path) if self.path.is_dir() else self.path.stat().st_size}

    @staticmethod
    def _graph(adapter, as_of_tt=None, versions=None):
        """`eval_harness._raphtory_graph`, with one change a Raphtory user
        needs: a `PersistentGraph` edge is identified by `(src, dst, layer)`,
        so two Gnomon edges between the same pair with the same type (different
        `disc`) are one Raphtory edge, and the first one's `delete_edge` at its
        `vt_e` ends the other's validity too. Measured at 1M: `snap.hop2` lost 7
        of 70 nodes and `nbr.evolution` 1 of 13 neighbour changes that way. The loader
        therefore coalesces the currently-believed intervals per `(src, dst,
        rel_type)` into maximal disjoint runs and adds/deletes each run once —
        the instant adjacency every expressible query reads is then exact.
        Nodes as in the harness (first believed `vt_s` per uid)."""
        import raphtory
        from gnomon.core.model import OPEN_END, clamp_tt

        # `as_of_tt` (correction-density study, the re-ingest emulation): admit
        # the versions believed at that time instead of the current ones —
        # Raphtory has no transaction-time axis, so a past belief state is a
        # graph rebuilt from the versions believed then.
        a = None if as_of_tt is None else clamp_tt(as_of_tt)

        def keep(v):
            return v.tt_e == OPEN_END if a is None else (v.tt_s <= a < v.tt_e)
        nodes, edges = versions if versions is not None else (
            adapter.all_node_versions(), adapter.all_edge_versions())
        g = raphtory.PersistentGraph()
        for v in nodes:
            if keep(v):
                g.add_node(v.vt_s, v.uid)
        runs: dict[tuple, list] = {}
        for v in edges:
            if keep(v):
                runs.setdefault((v.src, v.dst, v.rel_type), []).append((v.vt_s, v.vt_e))
        for (src, dst, rel), ivs in runs.items():
            ivs.sort()
            cur_s, cur_e = ivs[0]
            for s_, e_ in ivs[1:] + [(None, None)]:
                if s_ is not None and s_ <= cur_e:
                    cur_e = max(cur_e, e_)
                    continue
                g.add_edge(cur_s, src, dst, layer=rel)
                if cur_e < OPEN_END:
                    g.delete_edge(cur_e, src, dst, layer=rel)
                if s_ is not None:
                    cur_s, cur_e = s_, e_
        return g

    def open(self):
        import raphtory
        t0 = time.perf_counter()
        self.g = raphtory.PersistentGraph.load_from_file(str(self.path))
        self.open_s = time.perf_counter() - t0

    def fn_for(self, q):
        import extcmp_raphtory as R
        if q.id in ("b.snap.hop2@T", "b.reach@T"):
            # expressible only by re-ingest: rebuild the graph of the versions
            # believed at T inside the timed call (the version rows are held in
            # memory, read once outside timing), then answer on it
            if not hasattr(self, "_versions"):
                ad = self.ref_adapter()
                self._versions = (list(ad.all_node_versions()), list(ad.all_edge_versions()))
            fn = R.QUERIES["b.snap.hop2@now" if q.id == "b.snap.hop2@T" else "b.reach@now"]
            vs, args = self._versions, dict(q.args)
            T = args.pop("as_of_tt")

            def rebuild_and_answer():
                return fn(self._graph(None, T, vs), **args)
            return rebuild_and_answer
        fn = R.QUERIES.get(q.id)
        if fn is None:
            return None
        g = self.g
        return lambda: fn(g, **q.args)

    def settings(self):
        import raphtory
        return {"raphtory": raphtory.__version__, "open_from_file_s": round(self.open_s, 3)}


class NativeMatch(Native):
    """    `match` (pattern) path, its text in scripts/matchpath_texts.py; a query with no `match` form there is not
    written for this system. Its own system name, so its records stay apart from the dedicated operators'."""
    name, backend = "nativematch", "native"

    def fn_for(self, q):
        import matchpath_texts as MT
        return MT.fn_for(self.adapter, q.op, dict(q.args))


class RaphtoryEq(Raphtory):
    """The same stored graph as `Raphtory`, through extcmp_raphtory.QUERIES_EQ: the fuller output of the same call. Its own system name, so its records stay apart from the first form's."""
    name, compare = "raphtoryeq", "projected-raphtory-eq"

    def fn_for(self, q):
        import extcmp_raphtory as R
        fn = R.QUERIES_EQ.get(q.id)
        if fn is None:
            return None
        g = self.g
        return lambda: fn(g, **q.args)


def _system_class(name: str):
    if name == "ladybug":
        import extcmp_ladybug
        return extcmp_ladybug.Ladybug
    if name == "xtdb":
        import extcmp_xtdb
        return extcmp_xtdb.XTDB
    if name == "aeong":
        import extcmp_aeong
        return extcmp_aeong.AeonG
    if name == "duckdbsql":
        # DuckDB (SQL): every query as SQL inside DuckDB over the version
        # tables (a baseline like postgres/clickhouse), not the "duckdb"
        # system, which is the engine's operators over the DuckDB backend
        import extcmp_duckdbsql
        return extcmp_duckdbsql.DuckDBSQL
    return {"native": Native, "duckdb": DuckDB, "postgres": Postgres,
            "clickhouse": ClickHouse, "neo4j": Neo4j, "memgraph": Memgraph,
            "raphtory": Raphtory, "raphtoryeq": RaphtoryEq, "nativematch": NativeMatch}[name]


# --------------------------------------------------------------------------- #
# commands                                                                      #
# --------------------------------------------------------------------------- #

class _Timeout(Exception):
    pass


def _alarm(signum, frame):
    raise _Timeout("client-side limit reached")


def measure_capped(fn) -> tuple[Any, list[float], list[float], str]:
    """The timing protocol, with the 300 s cap and the 3600 s
    ceiling. Returns (last payload, measured timings ms, warmup timings ms,
    protocol label). A call reaching the ceiling raises `_Timeout`."""
    def call():
        if ET.ENABLED:
            ET.CURRENT.begin()
        signal.alarm(CALL_CEILING_S)
        t = time.perf_counter()
        try:
            payload = fn()
        finally:
            signal.alarm(0)
        ms = (time.perf_counter() - t) * 1e3
        if SW.ENABLED:
            ms = SW.take_into(ms)           # e2e measured in the worker; the hop is recorded outside e2e
        if ET.ENABLED:
            ET.note(ms)
        return payload, ms

    payload, w0 = call()
    warm = [w0]
    timings: list[float] = []
    if w0 > CAP_S * 1e3:
        protocol, reps = "capped: 1 warmup + 3 repetitions (a call exceeded 300 s)", 3
    else:
        for _ in range(H.WARMUP - 1):
            payload, w = call()
            warm.append(w)
        payload, first = call()
        timings.append(first)
        if first > CAP_S * 1e3:
            protocol, reps = "capped after warmups: 3 repetitions (a call exceeded 300 s)", 2
        else:
            protocol = f"standard: {H.WARMUP} warmups + " + (
                f"{H.REPS_FAST} reps" if first < 1000.0 else f"{H.REPS_SLOW} reps")
            reps = (H.REPS_FAST if first < 1000.0 else H.REPS_SLOW) - 1
    for _ in range(reps):
        payload, t = call()
        timings.append(t)
    return payload, timings, warm, protocol


def host_facts() -> dict[str, Any]:
    def sh(cmd: str) -> str:
        try:
            return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                                  timeout=20).stdout.strip()
        except Exception as e:                              # noqa: BLE001
            return f"error: {e}"
    return {"utc": sh("date -u +%FT%TZ"), "uptime": sh("uptime"),
            "containers": (sh("apptainer instance list") if os.environ.get("EXTCMP_BACKEND") == "apptainer"
                           else sh("docker ps --format '{{.Names}} {{.Image}} {{.Status}}'")),
            "free_g": sh("free -g | sed -n 2p"),
            "top": sh("ps -eo pid,pcpu,rss,etimes,comm --sort=-pcpu | head -6")}


def cmd_ref(args) -> int:
    sysobj = Native(None, args.workdir, args.size_tag)
    if args.store:
        sysobj = Native(None, args.store.parent.parent, args.size_tag)
    rec = sysobj.load(args.log)
    print(json.dumps(rec, indent=1, default=str))
    return 0


def cmd_load(args) -> int:
    cls = _system_class(args.system)
    sysobj = cls(args.ref, args.workdir, args.size_tag)
    facts = host_facts()
    rec = {"system": args.system, "log": str(args.log), "ref": str(args.ref),
           "manifest": H.manifest(dataset(args.log), [args.system]),
           "host_before": facts}
    if os.environ.get("EXTCMP_RECORD_LABEL"):
        rec["label"] = os.environ["EXTCMP_RECORD_LABEL"]
    rec["load"] = sysobj.load(args.log)
    rec["host_after"] = host_facts()
    if args.json:
        args.json.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n")
    print(json.dumps(rec["load"], indent=1, default=str)[:4000])
    return 0


#: is waiting, and yields the shared lock if so (exit 75, resumed later).
LOCK_SH = Path(__file__).resolve().parents[1] / (
    "benchmarks/compare/kit/lib/lock.sh")
YIELD_RC = 75
#: exit code of a query step whose server died inside the window: the cells
#: after the death are recorded as "server died", never skipped silently
SERVER_DIED_RC = 76


def _container_state(name: str) -> tuple[bool, str]:
    """(alive, state) of the window's server container (EXTCMP_CONTAINER).
    With EXTCMP_STATE_CMD (the kit's `servers.sh state <system> <tag>`, which
    serves the Docker and the Apptainer backend alike) its one-line answer is
    used: first word true/false, then the state. Otherwise docker inspect. A
    container that cannot be inspected counts as dead; a failing CLI (timeout,
    missing binary) counts as alive, with the failure as the state."""
    cmd = os.environ.get("EXTCMP_STATE_CMD")
    if cmd:
        import shlex
        try:
            r = subprocess.run(shlex.split(cmd), capture_output=True, text=True, timeout=60)
            out = (r.stdout.strip() or r.stderr.strip())[:300]
        except Exception as e:                              # noqa: BLE001
            return True, f"state command failed: {e}"[:300]
        return out.startswith("true"), out
    try:
        r = subprocess.run(["docker", "inspect", "-f",
                            "{{.State.Running}} status={{.State.Status}} exit={{.State.ExitCode}} "
                            "oom={{.State.OOMKilled}} finished={{.State.FinishedAt}}", name],
                           capture_output=True, text=True, timeout=60)
    except Exception as e:                                  # noqa: BLE001
        return True, f"inspect failed: {e}"[:300]
    if r.returncode != 0:
        return False, f"container missing: {r.stderr.strip()}"[:300]
    out = r.stdout.strip()
    return out.startswith("true"), out


def _server_death(name: str, state: str, after: str | None) -> dict:
    try:
        lc = os.environ.get("EXTCMP_LOGS_CMD")
        import shlex
        tail = subprocess.run(shlex.split(lc) if lc else ["docker", "logs", "--tail", "40", name],
                              capture_output=True, text=True, timeout=60)
        log_tail = (tail.stdout + tail.stderr)[-4000:]
    except Exception as e:                                  # noqa: BLE001
        log_tail = f"docker logs failed: {e}"
    return {"utc": host_facts()["utc"], "container": name, "state": state,
            "after_query": after, "log_tail": log_tail}


def _timed_pending() -> str:
    if not os.environ.get("EXTCMP_YIELD") or not LOCK_SH.exists():
        return ""
    r = subprocess.run(["bash", "-c", f'source "{LOCK_SH}"; pending_others "$PPID"'],
                       capture_output=True, text=True, timeout=60)
    return r.stdout.strip()


def _write_query_record(args, data, sysobj, open_s, out, yielded):
    rec = {"system": args.system, "compare": sysobj.compare, "mode": args.mode,
           "log": str(args.log), "ref": str(args.ref) if args.ref else None,
           "manifest": H.manifest(data, [args.system]), "open_s": round(open_s, 3),
           "settings": sysobj.settings() if not yielded else {},
           "params": {"t0": data.t0, "t1": data.t1, "tt_epoch1": data.tt_epoch1,
                      "n_filter_uids": len(data.filter_uids)},
           "host": host_facts(), "results": out, "yielded": yielded}
    if getattr(args, "params", None):
        # parameter-file mode only: which file supplied the instances (absent,
        # and the record unchanged, when the mode is off)
        import extcmp_p1 as P1
        rec["param_file"] = P1.file_facts(args.params)
        rec["protocol_p1"] = P1.protocol_facts()
        rec["window_prime"] = getattr(args, "p1_prime", None)
        r10 = _r10m(args.system) if args.mode == "timed" else None
        if r10 is not None:
            # R-10M: which (system, family) pairs this 10M record did not run, and from which 1M records
            rec["protocol_p1"]["r10m"] = {"rule": r10["rule"], "label": r10["label"], "source": r10["source"],
                                          "files": r10.get("files", []), "skipped": sorted(r10["skip"]),
                                          "skip": r10["skip"]}
    if os.environ.get("EXTCMP_RECORD_LABEL"):
        rec["label"] = os.environ["EXTCMP_RECORD_LABEL"]
    if args.json:
        args.json.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n")


def cmd_died(args) -> int:
    """Record every id of one family as "server died" without opening the
    system: used for the families an AeonG window could not reach."""
    data = dataset(args.log)
    ids = list(FAMILIES[args.family])
    rec = {"system": args.system, "mode": "timed", "log": str(args.log), "results": [
        {"query": i, "ok": False, "error": "server died", "server_died": True} for i in ids],
        "server_death": {"utc": host_facts()["utc"], "reason": args.reason},
        "params": {"t0": data.t0, "t1": data.t1, "tt_epoch1": data.tt_epoch1},
        "host": host_facts(), "yielded": False}
    if os.environ.get("EXTCMP_RECORD_LABEL"):
        rec["label"] = os.environ["EXTCMP_RECORD_LABEL"]
    args.json.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n")
    return 0


def _r10m(system: str) -> dict[str, Any] | None:
    """R-10M: the skip list of this system at a W2 10M point, built from its
    1M timed records (EXTCMP_R10M_SKIP = r10m.py's JSON). Unset: nothing skipped."""
    p = os.environ.get("EXTCMP_R10M_SKIP")
    if not p or not Path(p).exists():
        return None
    d = json.loads(Path(p).read_text())
    if d.get("system") not in (None, system):
        raise SystemExit(f"EXTCMP_R10M_SKIP {p} is the list of {d.get('system')}, not of {system}")
    return d


def _excluded(system: str) -> dict[str, str]:
    """Per-query timing exclusions from the answer-check gate (run_campaign.sh
    writes <tag>/excluded.json and sets EXTCMP_EXCLUDE to it): {system: {query:
    "excluded: DIFFERS" | "excluded: error"}}. A listed (system, query) is not
    timed; its cell carries the label. EXTCMP_EXCLUDE unset: nothing excluded."""
    p = os.environ.get("EXTCMP_EXCLUDE")
    if not p:
        return {}
    return dict(json.loads(Path(p).read_text()).get(system, {}))


def _log_cell(log, cell: dict[str, Any], system: str) -> None:
    """One verify cell, appended as it finishes (cmd_query's <record>.cells.jsonl)."""
    if log is not None:
        log.write(json.dumps(dict(cell, system=system), sort_keys=True, default=str) + "\n")
        log.flush()


def cmd_query(args) -> int:
    if args.mode == "timed" and args.system == "aeong" and \
            os.environ.get("EXTCMP_AEONG_ASOF", "property") != "property":
        sys.exit("AeonG TT AS forms of the as-of graph ids are not timed: returns wrong answers (AeonG TT AS defects 1 and 2, see repro)")
    data = dataset(args.log)
    if getattr(args, "family", None):
        ids = list(FAMILIES[args.family]) + (
            list(NATIVE_ONLY_BY_FAMILY.get(args.family, ())) if args.system in ("native", "nativematch") else [])
    elif args.ids:
        ids = args.ids.split(",")
    else:
        ids = list(CORE_IDS) + (list(NATIVE_ONLY_IDS) if args.system in ("native", "nativematch") else [])
    qs = queries(data, ids)
    # parameter-file mode: every registry id is asked once per curated
    # instance of the file, with the instance's arguments; off (no --params),
    # each id is asked once with the registry's own arguments, as before
    insts: dict[int, dict[str, Any]] = {}
    if getattr(args, "params", None):
        import extcmp_p1 as P1
        qs, inst_list = P1.expand(qs, P1.load(args.params))
        insts = {id(q): i for q, i in zip(qs, inst_list)}

    def key_of(q) -> str:
        i = insts.get(id(q))
        return q.id if i is None else f"{q.id}#{i['instance']}"
    cls = _system_class(args.system)
    sysobj = cls(args.ref, args.workdir, args.size_tag)
    sysobj.log = args.log
    if SW.ENABLED and args.system in SW.SYSTEMS:
        def _make(cls=cls, a=args):
            o = cls(a.ref, a.workdir, a.size_tag)
            o.log = a.log
            return o
        sysobj = SW.WorkerSystem(sysobj, _make)
    if args.mode == "verify":
        # P-7: the store the answers are compared against must exist, hold rows,
        # and contain every node an instance names
        refp = args.ref if args.ref else getattr(sysobj, "path", None)
        if refp is None:
            sys.exit("verify needs --ref (or an embedded store) to check its input is non-empty")
        _rs = open_existing_store(refp, "native" if args.ref else sysobj.backend)
        check_inputs(_rs, qs, key_of)
        _rs.close()
    CM.mark("open_start")
    t0 = time.perf_counter()
    sysobj.open()
    open_s = time.perf_counter() - t0
    CM.mark("open_done")
    if ET.ENABLED:
        ET.attach(args.system, sysobj)
    out = []
    if args.json and args.json.exists():          # resume a step that yielded the lock
        prev = json.loads(args.json.read_text())
        if prev.get("yielded"):
            out = prev["results"]
            done = {cell_key(c) for c in out}
            qs = [q for q in qs if key_of(q) not in done]
            print(f"  resuming: {len(done)} ids already verified", flush=True)
    answers = args.answers
    if answers:
        answers.mkdir(parents=True, exist_ok=True)
    # verify: every finished cell is also appended to <record>.cells.jsonl as it answers, so a window that a timeout
    # kills (EXTCMP_WINDOW_TIMEOUT_S; the record is written only at the end) still leaves comparable cells (hashes
    # included). A resumed step appends; a fresh one starts the file.
    cells_log = None
    if args.mode == "verify" and args.json:
        cells_log = open(str(args.json) + ".cells.jsonl", "a" if out else "w")
    old = signal.signal(signal.SIGALRM, _alarm)
    if insts and qs:
        # parameter-file mode: one untimed call before the first instance
        CM.mark("prime_start")
        args.p1_prime = P1.prime(sysobj, qs[0], VERIFY_CAP_S, _Timeout)
        CM.mark("prime_done")
        if hasattr(sysobj, "set_prime"):      # in-process worker: the prime is replayed after every restart
            sysobj.set_prime(qs[0])
    container = os.environ.get("EXTCMP_CONTAINER") or None
    death = None
    for qi, q in enumerate(qs):
        inst = insts.get(id(q))
        key = key_of(q)
        if death is not None:
            cell = {"query": q.id, "ok": False, "error": "server died", "server_died": True}
            if inst is not None:
                cell["instance"] = inst["instance"]
            out.append(cell); _log_cell(cells_log, cell, args.system)
            print(f"  {key:<20} server died", flush=True)
            continue
        waiting = _timed_pending()
        if waiting:
            print(f"  yielding to pending timed job(s) {waiting!r} before {key}", flush=True)
            signal.signal(signal.SIGALRM, old)
            _write_query_record(args, data, sysobj, open_s, out, yielded=True)
            sysobj.close()
            return YIELD_RC
        cell: dict[str, Any] = {"query": q.id}
        if inst is not None:
            cell.update(instance=inst["instance"], stratum=inst.get("stratum"))
        if ET.ENABLED:
            ET.CALLS.clear()
        if SW.ENABLED:
            SW.HOPS.clear()
        excl = _excluded(args.system).get(key) if args.mode == "timed" else None
        r10 = _r10m(args.system) if args.mode == "timed" else None
        if excl is None and r10 is not None and q.id in r10["skip"]:
            cell.update(ok=False, censored=True, aborted=True, r10m=True, error=r10["label"])
            out.append(cell); _log_cell(cells_log, cell, args.system)
            print(f"  {key:<20} {r10['label']}", flush=True)
            continue
        if excl is not None:
            cell.update(ok=False, excluded=True, error=excl)
            out.append(cell); _log_cell(cells_log, cell, args.system)
            print(f"  {key:<20} {excl}", flush=True)
            continue
        fn = sysobj.fn_for(q)
        if fn is None:
            cell.update(ok=False, error="not written for this system")
            out.append(cell); _log_cell(cells_log, cell, args.system)
            print(f"  {key:<20} not written", flush=True)
            continue
        try:
            if args.mode == "verify" and inst is not None:
                payload, wall, notes = P1.verify_call(fn, VERIFY_CAP_S, _Timeout, sysobj,
                                                      lambda q=q: sysobj.fn_for(q))
                cell.update(ok=True, wall_ms=round(wall, 3), **notes)
            elif args.mode == "verify":
                signal.alarm(VERIFY_CAP_S)
                t = time.perf_counter()
                payload = fn()
                wall = (time.perf_counter() - t) * 1e3
                if SW.ENABLED:
                    wall = SW.take_into(wall)
                signal.alarm(0)
                cell.update(ok=True, wall_ms=round(wall, 3))
            elif inst is not None:
                # P1 protocol (extcmp_p1.measure): 2 warmups + 10 repetitions,
                # with the slow-cell and ceiling rules
                if isinstance(sysobj, SW.WorkerSystem):    # the protocol runs in the worker (sysworker.py)
                    payload, timings, warm, protocol, calls, hop = sysobj.measure(
                        q, "p1", {"_Timeout": _Timeout, "CeilingReached": P1.CeilingReached})
                    ET.CALLS[:] = calls
                    cell["harness_hop_ms"] = round(hop * 1e3, 3)
                else:
                    payload, timings, warm, protocol = P1.measure(
                        fn, _Timeout, lambda q=q: (P1.recover(sysobj), sysobj.fn_for(q))[1])
                p50, p95 = H._p50_p95(timings)
                cell.update(ok=True, p50_ms=p50, p95_ms=p95, n=len(timings),
                            protocol=protocol, capped=protocol.startswith("slow"),
                            timings_ms=[round(x, 3) for x in timings],
                            warmup_ms=[round(x, 3) for x in warm])
            else:
                if isinstance(sysobj, SW.WorkerSystem):    # the protocol runs in the worker (sysworker.py)
                    payload, timings, warm, protocol, calls, hop = sysobj.measure(q, "capped", {"_Timeout": _Timeout})
                    ET.CALLS[:] = calls
                    cell["harness_hop_ms"] = round(hop * 1e3, 3)
                else:
                    payload, timings, warm, protocol = measure_capped(fn)
                p50, p95 = H._p50_p95(timings)
                cell.update(ok=True, p50_ms=p50, p95_ms=p95, n=len(timings),
                            protocol=protocol, capped=protocol.startswith("capped"),
                            timings_ms=[round(x, 3) for x in timings],
                            warmup_ms=[round(x, 3) for x in warm])
            cell.update(hash=answer_hash(payload), proj_hash=projected_hash(payload),
                        rows=int(H._answer_size(payload)))
            if answers and inst is None:
                (answers / f"{q.id}.json").write_text(
                    json.dumps(payload, sort_keys=True, default=str)[:2_000_000])
            elif answers and args.mode == "verify":
                # parameter-file mode: answers kept from the answer check only (the hashes carry every timed cell);
                # written whole
                (answers / f"{key}.json").write_text(
                    json.dumps(payload, sort_keys=True, default=str))
        except _Timeout as e:
            if args.mode == "timed":
                cell.update(ok=False, aborted=True,
                            error=f">{CALL_CEILING_S} s, aborted ({e})")
                cancel = getattr(sysobj, "cancel", None)
                if cancel is not None:
                    try:
                        cancel()
                        cell["cancel"] = "server-side cancel sent"
                    except Exception as ce:                 # noqa: BLE001
                        cell["cancel"] = f"cancel failed: {ce}"[:200]
            else:
                cell.update(ok=False, error=f"timeout: {e}", refused=True)
            if inst is not None:
                # parameter-file mode: a multi-statement emulation can pass the
                # server-side limit statement by statement; the client alarm
                # interrupted it mid-statement, so cancel and reconnect before
                # the next instance (the session would refuse every later call)
                cell["recover"] = P1.recover(sysobj)
        except Exception as e:                              # noqa: BLE001
            signal.alarm(0)
            msg = f"{type(e).__name__}: {e}"
            cell.update(ok=False, error=msg[:500],
                        refused=("E_COST" in msg or "guardrail" in msg.lower()))
            if inst is not None and P1.is_server_timeout(msg):
                # parameter-file mode: a server-side statement limit (set to the
                # P1 ceiling) is the ceiling reached, not an error
                cell.update(aborted=True, refused=False,
                            error=f">{P1.ceiling_s()} s, aborted ({msg[:200]})")
        finally:
            signal.alarm(0)
        if container:
            alive, state = _container_state(container)
            if not alive:
                # the server is gone: this cell (if it failed) and every later
                # one are "server died"; the state and the log tail are kept
                if not cell.get("ok"):
                    cell.update(server_died=True, error=f"server died ({cell.get('error', '')})"[:500])
                death = _server_death(container, state, key)
                print(f"  SERVER DIED after {key}: {state}", flush=True)
        if SW.ENABLED and isinstance(sysobj, SW.WorkerSystem):
            if SW.HOPS and "harness_hop_ms" not in cell:   # single calls (answer check): per-call hops
                cell["harness_hop_ms"] = [round(x, 4) for x in SW.HOPS]
            ev = sysobj.take_events()
            if ev:
                cell["worker_events"] = ev         # a restart: the next calls ran on a cold cache
        if ET.ENABLED and ET.CALLS and "timings_ms" in cell:
            # one record per call in call order (warm-ups first); the timed calls are the last n
            n = len(cell["timings_ms"])
            cell["engine_time"] = {"source": ET.CURRENT.source, "warmup": ET.CALLS[:-n] if n else list(ET.CALLS),
                                   "timed": ET.CALLS[-n:] if n else [],
                                   "summary": ET.summarize(ET.CALLS[-n:] if n else [])}
        out.append(cell)
        _log_cell(cells_log, cell, args.system)
        shown = cell.get("wall_ms", cell.get("p50_ms"))
        print(f"  {key:<20} ok={cell['ok']} {shown} {cell.get('hash', cell.get('error', ''))[:80]}",
              flush=True)
    signal.signal(signal.SIGALRM, old)
    if cells_log is not None:
        cells_log.close()
    _write_query_record(args, data, sysobj, open_s, out, yielded=False)
    if death is not None:
        if args.json:
            rec = json.loads(args.json.read_text())
            rec["server_death"] = death
            args.json.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n")
        try:
            sysobj.close()
        except Exception:                                   # noqa: BLE001
            pass
        return SERVER_DIED_RC
    sysobj.close()
    if getattr(args, "merge_into", None) and args.merge_into.exists() and args.json:
        # re-verification of a few ids: replace those cells in the full record,
        # keeping the replaced cells under `superseded` with this run's stamp
        full = json.loads(args.merge_into.read_text())
        new = {c["query"]: c for c in out}
        full.setdefault("superseded", []).extend(
            dict(c, superseded_utc=host_facts()["utc"]) for c in full["results"] if c["query"] in new)
        full["results"] = [new.get(c["query"], c) for c in full["results"]]
        full.setdefault("reverified", []).append({"ids": sorted(new), "record": str(args.json)})
        args.merge_into.write_text(json.dumps(full, indent=1, sort_keys=True, default=str) + "\n")
    return 0


def verdict(base: dict[str, Any], other: dict[str, Any],
            base_answers: Path | None = None) -> list[dict[str, Any]]:
    b = {cell_key(r): r for r in base["results"]}
    if other.get("compare") == "projected-raphtory":
        import extcmp_raphtory as R
        b = {k: dict(v) for k, v in b.items()}
        for qid, br in b.items():
            f = base_answers / f"{qid}.json" if base_answers else None
            proj = (R.project_native(qid, json.loads(f.read_text()))
                    if f is not None and f.exists() and br.get("ok") else None)
            br["hash"] = br["proj_hash"] = (
                sha256_hex(canonical_json(proj))[:32] if proj is not None else None)
    rows = []
    for r in other["results"]:
        qid = r["query"]
        br = b.get(cell_key(r))
        n0 = len(rows)
        _verdict_row(rows, r, br, qid, other)
        if r.get("instance") is not None and len(rows) > n0:
            rows[-1]["instance"] = r["instance"]      # parameter-file mode only
    return rows


def _verdict_row(rows, r, br, qid, other) -> None:
    """One verdict row for cell r against the native cell br (appended to rows)."""
    if not r.get("ok"):
        err = r.get("error", "")
        v = (err if r.get("excluded")
             else "not written" if err.startswith("not written")
             else "not expressible" if err.startswith(("unsupported", "NotImplemented"))
             else "refused" if r.get("refused") else "error")
        rows.append({"query": qid, "verdict": v, "detail": err[:200]})
        return
    if br is None or not br.get("ok"):
        rows.append({"query": qid, "verdict": "no native reference"})
        return
    if other.get("compare") == "projected-raphtory":
        if br["hash"] is None:
            rows.append({"query": qid, "verdict": "no projection"})
        elif r["hash"] == br["hash"]:
            rows.append({"query": qid, "verdict": "equivalent",
                         "by": "projected (Raphtory-expressible fields, see extcmp_raphtory)"})
        else:
            rows.append({"query": qid, "verdict": "DIFFERS", "by": "raphtory projection"})
        return
    if r["hash"] == br["hash"]:
        rows.append({"query": qid, "verdict": "equivalent", "by": "exact hash"})
    elif r["proj_hash"] == br["proj_hash"]:
        rows.append({"query": qid, "verdict": "equivalent", "by": "projected hash (vid/tt_s/tt_e removed)"})
    else:
        rows.append({"query": qid, "verdict": "DIFFERS",
                     "native_rows": br.get("rows"), "rows": r.get("rows")})


def _probe(system: str) -> None:
    if system == "postgres":
        import psycopg
        psycopg.connect("dbname=postgres", connect_timeout=5).close()
    elif system == "clickhouse":
        import ch_baseline
        ch_baseline.connect().command("SELECT 1")
    elif system in ("neo4j", "memgraph", "aeong"):
        import neo4j
        uri = {"neo4j": os.environ.get("GNOMON_NEO4J_URI"),
               "memgraph": os.environ.get("GNOMON_MEMGRAPH_URI"),
               "aeong": os.environ.get("GNOMON_AEONG_URI")}[system]
        with neo4j.GraphDatabase.driver(uri, auth=("neo4j", "neo4j")) as d:
            with d.session() as s:
                s.run("RETURN 1").consume()
    elif system == "xtdb":
        import psycopg
        port = os.environ.get("GNOMON_XTDB_PORT", "15433")
        with psycopg.connect(f"host=127.0.0.1 port={port} dbname=xtdb user=xtdb",
                             connect_timeout=5, autocommit=True) as c:
            c.execute("SELECT 1").fetchone()


def cmd_wait(args) -> int:
    t0 = time.time()
    last = None
    while time.time() - t0 < args.timeout:
        try:
            _probe(args.system)
            print(f"{args.system} ready after {time.time() - t0:.1f}s")
            return 0
        except Exception as e:                              # noqa: BLE001
            last = e
            time.sleep(2)
    print(f"{args.system} not ready after {args.timeout}s: {last}")
    return 1


def cmd_bverdict(args) -> int:
    """Phase B: per rotation directory, every system's timed cells against the
    native engine's cells from the same rotation. Writes `table.json` and
    prints a markdown table (median ms / p95 ms / verdict)."""
    cells: dict[str, dict[str, Any]] = {}
    compare: dict[str, str] = {}
    blocks: dict[str, list] = {}
    for f in sorted(args.dir.glob("*-*.json")):
        if f.name == "table.json" or f.name.startswith(("load-", "aeong-done")):
            continue
        rec = json.loads(f.read_text())
        if "results" not in rec or "system" not in rec:
            continue
        sysname = rec["system"]
        compare[sysname] = rec.get("compare", "exact")
        for c in rec["results"]:
            cells.setdefault(sysname, {})[cell_key(c)] = dict(c, block_record=f.name)
        blocks.setdefault(sysname, []).append(f.name)
    base = {"results": list(cells.get("native", {}).values())}
    table: dict[str, Any] = {}
    for sysname, m in cells.items():
        if sysname == "native":
            vs = {q: {"verdict": "reference"} for q in m}
        else:
            vs = {r["query"]: r for r in verdict(
                base, {"system": sysname, "compare": compare[sysname], "results": list(m.values())},
                args.dir / "answers-native")}
        table[sysname] = {q: {"p50_ms": c.get("p50_ms"), "p95_ms": c.get("p95_ms"), "n": c.get("n"),
                              "protocol": c.get("protocol"), "capped": c.get("capped"),
                              "aborted": c.get("aborted"), "error": c.get("error"),
                              "server_died": c.get("server_died"),
                              "verdict": vs.get(q, {}).get("verdict"), "by": vs.get(q, {}).get("by")}
                          for q, c in m.items()}
    # windows with CPU use outside the lock: flagged by the window protocol
    # (windows.jsonl, "flag_other_over_1_core"; a re-run window's flag replaces
    # the earlier one) or listed after the fact in <rot dir>/contention.json
    outside: dict[tuple, str] = {}
    wins = args.dir.parent / "windows.jsonl"
    rot = args.dir.name[3:] if args.dir.name.startswith("rot") else None
    if wins.exists() and rot:
        cur = None
        for line in wins.read_text().splitlines():
            try:
                w = json.loads(line)
            except ValueError:
                continue
            if "kind" in w:
                cur = (w.get("family"), w.get("system")) if w.get("rot") == rot else None
                if cur and w["kind"] == "aeong":
                    cur = ("*", "aeong")
                if cur:
                    outside.pop(cur, None)
            elif cur and w.get("cpu_outside_window") == "window" and w.get("flag_other_over_1_core"):
                outside[cur] = (f"processes outside the window used {w['other_cores_avg']:.2f} cores "
                                f"on average ({w['acquire']} to {w['release']})")
    cont = args.dir / "contention.json"
    cause = None
    if cont.exists():
        cdoc = json.loads(cont.read_text())
        cause = cdoc.get("cause")
        for w in cdoc.get("windows", []):
            outside[(w["family"], w["system"])] = f"{cause} (window {w['window_start']} to {w['window_end']})"
    for sysname, m in table.items():
        for q, c in m.items():
            fam = cells[sysname][q]["block_record"].split("-")[0]
            note = outside.get((fam, sysname)) or outside.get(("*", sysname))
            if note:
                c["cpu_outside_window"] = note
    # AeonG versions shown at TT AS x with a property value written after x
    # (recorded per load, extcmp_aeong.py); an AeonG cell that differs in such
    # a rotation is labelled with it, never corrected
    ae_hist = []
    ae_load = args.dir / "load-aeong.json"
    if ae_load.exists():
        try:
            _ld = json.loads(ae_load.read_text())
            ae_hist = _ld.get("history_mismatch") or (_ld.get("load") or {}).get("history_mismatch") or []
        except ValueError:
            ae_hist = []
    if ae_hist and "aeong" in table:
        for c in table["aeong"].values():
            c["aeong_history_mismatch_versions"] = len(ae_hist)
    label = os.environ.get("EXTCMP_RECORD_LABEL", "local run — not for publication")
    (args.dir / "table.json").write_text(json.dumps({"label": label, "table": table},
                                                    indent=1, sort_keys=True) + "\n")
    print(f"*{label}* — median ms / p95 ms; † capped protocol; (p) projected comparison; "
          f"‡ CPU use outside the window\n")
    order = [s_ for s_ in ("native", "duckdb", "duckdbsql", "postgres", "clickhouse", "neo4j", "memgraph", "xtdb",
                           "ladybug", "aeong", "raphtory") if s_ in table]
    qids = list(CORE_IDS) + list(NATIVE_ONLY_IDS) + list(FAMILIES["B"])
    present = {q for t in table.values() for q in t}
    qids = [q for q in qids if q in present]
    print("| query | " + " | ".join(order) + " |")
    print("|---|" + "---|" * len(order))
    for q in qids:
        row = []
        for s_ in order:
            c = table[s_].get(q)
            if c is None:
                row.append("—")
            elif c["p50_ms"] is None:
                row.append(">3600 s, aborted" if c.get("aborted") else
                           "server died" if c.get("server_died") else (c.get("verdict") or "error"))
            else:
                v = c["verdict"]
                tag = "" if v in ("equivalent", "reference") else f" **{v}**"
                if s_ == "aeong" and v not in ("equivalent", "reference") and ae_hist:
                    tag = f" **{v} (AeonG history)**"
                if v == "equivalent" and c.get("by") and "projected" in c["by"]:
                    tag = " (p)"
                cap = (" †" if c.get("capped") else "") + (" ‡" if c.get("cpu_outside_window") else "")
                ms = lambda x: f"{x:.3f}" if x < 1 else f"{x:.1f}"
                row.append(f"{ms(c['p50_ms'])} / {ms(c['p95_ms'])}{cap}{tag}")
        print(f"| {q} | " + " | ".join(row) + " |")
    if "aeong" in table and any(q in table["aeong"] for q in ("b.snap.hop2@T", "b.reach@T")):
        print("\nAeonG b.snap.hop2@T and b.reach@T: property form (AeonG's answer); its native "
              "TT AS forms are not timed: returns wrong answers (AeonG TT AS defects 1 and 2, see repro)")
    if ae_hist:
        print(f"\nAeonG history: the load of this rotation recorded {len(ae_hist)} version(s) shown "
              f"at TT AS x with a property value written after x (load-aeong.json, history_mismatch); "
              f"AeonG cells that differ are labelled (AeonG history)")
    flagged = sorted({(s_, c["cpu_outside_window"]) for s_, m in table.items() for c in m.values()
                      if c.get("cpu_outside_window")})
    if flagged:
        print("\n‡ CPU use outside the window:")
        if cause:
            print(f"- {cause}; the affected windows are listed in contention.json")
        for s_, note in flagged:
            if not (cause and note.startswith(cause)):
                print(f"- {s_}: {note}")
    return 0


def cmd_verdict(args) -> int:
    base = json.loads(args.base.read_text())
    report = {}
    for p in args.others:
        other = json.loads(p.read_text())
        rows = verdict(base, other, args.base_answers)
        report[other["system"]] = rows
        print(f"== {other['system']} ({p.name})")
        for row in rows:
            print(f"  {row['query']:<20} {row['verdict']:<16} {row.get('by', row.get('detail', ''))}")
    if args.json:
        args.json.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("ref", "load", "query"):
        p = sub.add_parser(name)
        p.add_argument("--log", type=Path, required=True)
        p.add_argument("--workdir", type=Path, default=Path(__import__("os").environ.get("EXTCMP_ROOT", "extcmp-work")))
        p.add_argument("--size-tag", required=True)
        p.add_argument("--json", type=Path)
        if name == "ref":
            p.add_argument("--store", type=Path)
        else:
            p.add_argument("--system", required=True)
            p.add_argument("--ref", type=Path)
        if name == "query":
            p.add_argument("--mode", choices=("verify", "timed"), default="verify")
            p.add_argument("--ids", default="")
            p.add_argument("--family", choices=sorted(FAMILIES),
                           help="Phase B: one query family's ids (plus native-only ids for native)")
            p.add_argument("--answers", type=Path)
            p.add_argument("--merge-into", type=Path,
                           help="after a run with --ids, replace those ids' cells in this record")
            p.add_argument("--params", type=Path,
                           help="parameter-file mode (P1, extcmp_p1.py): ask every selected "
                                "registry id once per instance of this file, with the "
                                "instance's arguments; off by default (registry arguments)")
    p = sub.add_parser("died")
    p.add_argument("--system", required=True)
    p.add_argument("--family", required=True, choices=sorted(FAMILIES))
    p.add_argument("--log", type=Path, required=True)
    p.add_argument("--json", type=Path, required=True)
    p.add_argument("--reason", default="server died earlier in this window")
    p = sub.add_parser("bverdict")
    p.add_argument("--dir", type=Path, required=True)
    p = sub.add_parser("wait")
    p.add_argument("--system", required=True)
    p.add_argument("--timeout", type=int, default=600)
    p = sub.add_parser("verdict")
    p.add_argument("--base", type=Path, required=True)
    p.add_argument("--base-answers", type=Path,
                   help="the native run's --answers dir (needed for the Raphtory projection)")
    p.add_argument("others", type=Path, nargs="+")
    p.add_argument("--json", type=Path)
    args = ap.parse_args()
    return {"ref": cmd_ref, "load": cmd_load, "query": cmd_query,
            "wait": cmd_wait, "verdict": cmd_verdict, "bverdict": cmd_bverdict,
            "died": cmd_died}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
