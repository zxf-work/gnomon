"""StorageAdapter ABC.

Bi-temporal update semantics are implemented *once* here, on top of a
small set of backend primitives, so the native and DuckDB adapters cannot diverge
semantically: an adapter only implements row insertion, tt-closing, and scans.

Semantics recap:
- assert_*  — new belief. Any currently-believed version of the same logical
  identity overlapping the asserted valid interval is bi-temporally replaced:
  its tt is closed, and the non-overlapping remainder of its valid interval is
  re-inserted (same props, new tt). Then the new version is inserted. Adjacent
  identical-props versions are NOT coalesced (Phase 1 rule).
- retract   — evolution: the believed version whose vt contains t is closed in
  tt and re-inserted with vt truncated to [vt_s, t).
- correct   — correction: believed versions overlapping vt are closed in tt;
  non-overlapping remainders are preserved (old props, new tt); a corrected
  version with the given vt and new props is inserted.

Version ids: the spec defines vid = hash(identity, tt_s), which collides when
one batch splits a version into two fragments at the same tt. We therefore use
vid = hash(identity, tt_s, vt_s) — a strict refinement (unique because believed
valid intervals of one identity are disjoint).

Superseding within one batch: all three ops above stop believing the
versions they replace, and *when* the replaced version was written decides
what that means. One an earlier batch wrote is closed — its tt interval ends,
and the row stays, because that is belief history. One this same batch wrote
is retired: it would be believed over [tt, tt), which is no transaction time
at all, so it is not a row. That is what makes a second op in a batch safe on
the vid above — the carve re-derives the vid of the version it replaces, and
the replaced version is gone rather than colliding.
"""

from __future__ import annotations

import hashlib
import heapq
import json as _json
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np

from gnomon.core.errors import InvalidArgError, NotFoundError, StateError
from gnomon.core.model import (
    OPEN_END,
    EdgeVersion,
    EntityRef,
    Interval,
    NodeVersion,
    Props,
    canonical_json,
    digest,
    edge_eid,
    sha256_hex,
)


def _vid(identity: str, tt_s: int, vt_s: int) -> str:
    return sha256_hex(f"{identity}:{tt_s}:{vt_s}")[:24]


def _interval(start: int, end: int) -> Interval:
    try:
        return Interval(start, end)
    except ValueError as e:
        raise InvalidArgError(str(e)) from None


class StorageAdapter(ABC):
    """Backend-agnostic bi-temporal store."""

    paranoid: bool = False  # re-check disjointness invariant after every batch
    _tcsr = None  # lazily built current-belief TemporalCSR + its columnar arrays

    #: Highest transaction time this adapter has **applied** — the belief-time
    #: frontier a read is served from (`tt_q`, FRESHNESS_SEMANTICS).
    #: Maintained by `apply_ops` below, so all three backends get it from one
    #: line and no Rust changes. It is deliberately *not* the event log's tail
    #: and not `Store.clock.last_tt`: the log is fsynced **before** the batch is
    #: applied, so both over-report what a reader is being served, and a `tt_q`
    #: rounded *up* is the false-freshness direction this semantics forbids by name.
    _frontier_tt: int = 0

    def frontier_tt(self) -> int:
        """The applied transaction-time frontier (0 on a store this process has
        neither written nor been told about — `Store` seeds it at open)."""
        return self._frontier_tt

    def note_frontier_tt(self, tt: int) -> None:
        """Advance the frontier. Monotone: it never moves backwards, so a
        seeded value cannot be lowered by a stale caller."""
        if tt > self._frontier_tt:
            self._frontier_tt = int(tt)

    def tcsr(self):
        """Current-belief TemporalCSR (+ the columnar arrays it was built
        from), built lazily and invalidated by apply_ops."""
        if self._tcsr is None:
            from gnomon.storage.tcsr import TemporalCSR
            # exactly what the CSR build and its callers read; vid, disc and
            # props were being materialized here and thrown away
            cols = self.edges_columnar(columns=self.TCSR_COLS)
            self._tcsr = (TemporalCSR.build(cols, self.num_entities()), cols)
        return self._tcsr

    # --- batch transactions (backends override; default = no-op) --------- #

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    # ------------------------------------------------------------------ #
    # backend primitives                                                  #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def ensure_entities(self, uid_labels: Iterable[tuple[str, str]]) -> None:
        """Register logical node identities and assign dense int64 ids."""

    @abstractmethod
    def insert_node_versions(self, rows: Sequence[NodeVersion]) -> None: ...

    @abstractmethod
    def insert_edge_versions(self, rows: Sequence[EdgeVersion]) -> None: ...

    @abstractmethod
    def close_node_versions(self, vids: Sequence[str], tt_e: int,
                           vt_s: Sequence[int] | None = None) -> None:
        """Stop believing `vids` from `tt_e` on.

        `vt_s`, when given, is the valid-time start of each vid in `vids`,
        positionally. It is a locating hint and never an answer: an adapter
        that does not need it ignores it, and omitting it finds the same
        rows. The native engine uses it to prune segments on the manifest's
        key ranges instead of holding a store-wide vid postings map.
        """

    @abstractmethod
    def close_edge_versions(self, vids: Sequence[str], tt_e: int,
                           vt_s: Sequence[int] | None = None) -> None:
        """Stop believing `vids` from `tt_e` on. See `close_node_versions`."""

    @abstractmethod
    def retire_node_versions(self, vids: Sequence[str]) -> None:
        """Discard versions the *current* batch inserted and has since
        replaced — never a version an earlier batch committed.

        A retired version was believed over an empty transaction interval, so
        no read at any as_of_tt could ever have returned it and nothing in the
        store may refer to it. Its vid becomes free again, which is what the
        replacing fragment needs."""

    @abstractmethod
    def retire_edge_versions(self, vids: Sequence[str]) -> None: ...

    @abstractmethod
    def believed_node_versions(self, uid: str, as_of_tt: int = OPEN_END) -> list[NodeVersion]: ...

    @abstractmethod
    def believed_edge_versions(self, eid: str, as_of_tt: int = OPEN_END, *,
                               src: str | None = None, dst: str | None = None) -> list[EdgeVersion]:
        """Versions of the logical edge `eid` believed at `as_of_tt`.

        `src`/`dst` are pure performance hints, not part of the query: a
        backend that can anchor on them (one where `eid` has no primary key
        to index) may use them to avoid a full-table scan, and a backend that
        cannot (DuckDB, native) is free to ignore them. Passing hints MUST
        NOT change the result. Callers must pass exactly the src/dst the eid
        was derived from (`edge_eid(src, dst, rel_type, disc)`); behavior
        with wrong hints is undefined."""

    def believed_node_versions_overlapping(self, uid: str, vt_s: int, vt_e: int,
                                           as_of_tt: int = OPEN_END) -> list[NodeVersion]:
        """The versions of `uid` believed at `as_of_tt` whose valid time
        overlaps `[vt_s, vt_e)`, in `believed_node_versions` order.

        This is the question every write op asks (the versions an assert,
        correct or retract carves). The default filters the whole believed
        history; the native engine overrides it with a point read that seeks
        to the window instead (`crates/gnomon-core/src/pointread.rs`),
        which is what keeps a key with a long valid-time history from costing
        that history on every write. Same answer either way."""
        return [v for v in self.believed_node_versions(uid, as_of_tt)
                if v.vt_s < vt_e and vt_s < v.vt_e]

    def believed_edge_versions_overlapping(self, eid: str, vt_s: int, vt_e: int,
                                           as_of_tt: int = OPEN_END, *,
                                           src: str | None = None,
                                           dst: str | None = None) -> list[EdgeVersion]:
        """Edge counterpart of `believed_node_versions_overlapping`; `src`
        and `dst` are the same performance hints `believed_edge_versions`
        takes."""
        return [v for v in self.believed_edge_versions(eid, as_of_tt, src=src, dst=dst)
                if v.vt_s < vt_e and vt_s < v.vt_e]

    def identity_versions(self, kind: str, key: str) -> list[NodeVersion] | list[EdgeVersion]:
        """Every version of ONE identity ever written (a node's uid, or an
        edge's eid), whatever its belief, with its stored `tt_e`, ordered by
        `(tt_s, vid)`: the identity's belief log, which `version_history`
        reads for a `ref`. Versions closed by a bare retraction are in it.

        The default filters the whole population and is O(store); backends
        with an identity index override it (DuckDB by its key column, the
        native engine by its identity postings). Same answer either way."""
        if kind == "node":
            out: list = [v for v in self.all_node_versions() if v.uid == key]
        else:
            out = [v for v in self.all_edge_versions() if v.eid == key]
        out.sort(key=lambda v: (v.tt_s, v.vid))
        return out

    @abstractmethod
    def all_node_versions(self) -> Iterable[NodeVersion]:
        """Every version row, deterministic order not required (digest sorts)."""

    @abstractmethod
    def all_edge_versions(self) -> Iterable[EdgeVersion]: ...

    def all_versions_paged(
        self, kind: str, chunk_rows: int
    ) -> Iterator[list[NodeVersion] | list[EdgeVersion]]:
        """`all_node_versions`/`all_edge_versions`, drawn in `chunk_rows`
        chunks instead of one shot — the producer side of a later change:
        `store_digest_streaming` below consumes this
        instead of `all_{node,edge}_versions()` directly, so a backend that
        can produce rows without materializing the whole store first (the
        native engine's paged `all_versions_page`, `gnomon/storage/native/
        adapter.py`) makes the digest pass's own memory O(chunk_rows)
        rather than O(store).

        This default keeps every other backend correct at today's cost: it
        materializes the whole population once via `all_node_versions`/
        `all_edge_versions` (exactly what `store_digest_streaming` did
        before) and slices it into `chunk_rows`-sized chunks — no backend
        is required to override this to stay correct, only to get the
        bounded-memory property.
        """
        rows = list(self.all_node_versions() if kind == "node"
                    else self.all_edge_versions())
        for i in range(0, len(rows), chunk_rows):
            yield rows[i:i + chunk_rows]

    def nodes_with_believed_versions(self, uids: Sequence[str],
                                     as_of_tt: int = OPEN_END) -> set[str]:
        """Subset of `uids` that have at least one believed version.
        Backends should override with a batched query (hot on bulk ingest)."""
        return {u for u in uids if self.believed_node_versions(u, as_of_tt)}

    # --- dense id dictionary ------------------------------------------- #

    @abstractmethod
    def dense_ids(self, uids: Sequence[str]) -> np.ndarray:
        """Map uid strings to dense int64 ids; raises NotFoundError on misses."""

    @abstractmethod
    def uids_for(self, ids: Sequence[int]) -> list[str]: ...

    @abstractmethod
    def num_entities(self) -> int: ...

    # --- columnar read path (operator kernels) ------------------------- #

    #: Columns the temporal-CSR build and its consumers need. Anything else
    #: is paid for and discarded.
    TCSR_COLS = ("src_id", "dst_id", "vt_s", "vt_e", "eid", "rel_type")

    EDGE_INT_COLS = ("src_id", "dst_id", "vt_s", "vt_e")
    EDGE_STR_COLS = ("eid", "vid", "rel_type")
    #: Columns materialized ONLY when named explicitly — never by a bare
    #: `columns=None` scan. `props` is the whole canonical-JSON blob per row,
    #: the single most expensive thing the scan can produce, and the
    #: projection-pushdown work exists precisely because it used to be built
    #: for every call that never asked for it.
    EDGE_OPT_COLS = ("props",)

    #: Exactly the columns `version_history` emits, per kind — which is
    #: `to_json()` minus the three it drops. `props` is the blob excluded
    #: from a whole-store scan; `source` and `provenance_ref` are
    #: dropped by the operator, so fetching them was pure waste.
    VERSION_COLS = {
        "node": ("vid", "uid", "label", "vt_s", "vt_e", "tt_s", "tt_e"),
        "edge": ("vid", "eid", "src", "dst", "rel_type", "disc",
                 "vt_s", "vt_e", "tt_s", "tt_e"),
    }
    VERSION_INT_COLS = ("vt_s", "vt_e", "tt_s", "tt_e")

    def versions_columnar(self, kind: str) -> dict[str, np.ndarray]:
        """Struct-of-arrays over EVERY version ever written, of one kind.

        This exists because `version_history` was materializing the whole
        population to return a page: one object and one dict per version,
        profiled at 72% of its runtime and 64 s of object construction alone
        at 10M, to hand back at most `limit` rows.

        A backend that can hand its columns over overrides this; the default
        below keeps every other backend correct at the old cost, so the
        operator has one path and no backend diverges semantically — which
        is the property this guarantees.
        """
        rows = (self.all_node_versions() if kind == "node"
                else self.all_edge_versions())
        names = self.VERSION_COLS[kind]
        cols: dict[str, list] = {c: [] for c in names}
        for v in rows:
            for c in names:
                cols[c].append(getattr(v, c))
        return {c: np.asarray(vals,
                              dtype=np.int64 if c in self.VERSION_INT_COLS
                              else object)
                for c, vals in cols.items()}

    def versions_page(
        self,
        kind: str,
        *,
        as_of: int,
        t_a: int,
        t_b: int,
        belief: str = "current",
        rel_types: Sequence[str] | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[dict[str, Any], int]:
        """`(page columns, exact population size)` for `version_history`.

        The page is `VERSION_COLS[kind]`, already ordered by `(tt_s, vid)` and
        already sliced to `[offset:offset + limit]`; the count is of *every*
        row the filter selected, not of the page. `tt_e` comes back raw — the
        `OPEN_END` censoring against `as_of` is part of the operator's
        contract and stays in the operator, so there is one place to read it.

        Why this exists, next to `versions_columnar` rather than replacing it:
        a backend that can filter, order and count without materializing the
        population overrides this and never builds one, while the default
        below keeps every other backend correct at the old cost. That is
        this module's own compatibility pattern, applied
        to the operator it left behind — `version_history` was measured at
        10.6 GB and 75.8 s over 10M edge versions to return at most `limit`
        rows, which is the last item binding
        this store's 100M gate.

        **This default is the definition.** It is the arithmetic the frozen
        digest `84e8853c…be78fbfd` was captured over, moved here verbatim from
        `ops_versions.version_history`, and an overriding backend agrees with
        it row for row and position for position or the digest moves.
        """
        cols = self.versions_columnar(kind)
        tt_s, tt_e, vt_s, vt_e = (cols["tt_s"], cols["tt_e"],
                                  cols["vt_s"], cols["vt_e"])
        superseded = tt_e <= as_of
        keep = (tt_s <= as_of) & (vt_s < t_b) & (t_a < vt_e)
        if belief == "current":
            keep &= ~superseded
        elif belief == "superseded":
            keep &= superseded
        if rel_types is not None:
            keep &= np.isin(cols["rel_type"], list(set(rel_types)))
        idx = np.flatnonzero(keep)
        # (tt_s, vid) — the last key to lexsort is the primary one
        idx = idx[np.lexsort((cols["vid"][idx], tt_s[idx]))]
        page = idx[offset:offset + limit]
        return {c: cols[c][page] for c in self.VERSION_COLS[kind]}, int(idx.size)

    @abstractmethod
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
        """Struct-of-arrays over believed edge versions whose vt overlaps
        [vt_min, vt_max). Keys: src_id, dst_id, vt_s, vt_e (int64),
        eid, vid, rel_type (object). Sorted by (vt_s, vid).

        `columns` restricts materialization (string columns are the expensive
        part at 1M+ rows); `touching_ids` pushes down an
        incidence filter (src_id or dst_id in the given dense ids), and
        `touching_both` narrows that to *both* endpoints. The and-form exists
        because expressing it above the scan means deriving `eid` — a sha256
        per row — for rows the caller then discards; it has no effect unless
        `touching_ids` is given."""

    @abstractmethod
    def nodes_columnar(
        self,
        as_of_tt: int = OPEN_END,
        vt_min: int | None = None,
        vt_max: int | None = None,
    ) -> dict[str, np.ndarray]:
        """Keys: uid_id, vt_s, vt_e (int64), uid, vid, label (object). Sorted by vt_s."""

    @abstractmethod
    def props_for_vids(self, kind: str, vids: Sequence[str]) -> dict[str, dict]:
        """Fetch props JSON for specific version ids. kind: "node" | "edge"."""

    @abstractmethod
    def stats(self) -> dict[str, Any]:
        """Lightweight statistics for cost estimation."""

    def diff_belief(self, t1: int, t2: int, t_a: int, t_b: int,
                    limit: int) -> dict[str, Any]:
        """Versions whose *belief state* changed between `t1` and `t2`, inside
        the valid-time window `[t_a, t_b)`.

        The dual of `diff_snapshots`, which takes one `as_of_tt` and two
        valid-time instants: this takes two belief times and one valid-time
        window. A version is tagged `"added"` when its `tt_s` falls in
        `(t1, t2]` and `"retracted"` when its effective `tt_e` does, so a
        version written and corrected inside the same window — which is in
        neither believed population, and which a set difference of the two
        therefore drops — changed state twice and is reported twice.

        Returns `{rows, added_total, retracted_total, truncated}`, `rows`
        ordered by `(kind, key, tt, change, vid)` and capped at `limit`, the
        totals counted over the whole surviving population.

        **Not abstract, and deliberately so.** The native engine answers this
        from the segment headers' `tt_s` runs and each segment's effective
        close set, at a cost proportional to the `runs + closes` that fall in
        the window's rows rather than to rows. The SQL/graph adapters have no equivalent to call trivially —
        emulating it means two belief-filtered scans and an anti-join over a
        version table that grows with every correction, which is a different
        query with a different cost, not this one. Reporting that honestly is
        the evaluation notes' **unsupported** verdict ("cannot express
        it; reported as such, never as slow"), so the base raises rather than
        guessing, exactly as the harness's `lookup_name` bypass does.
        """
        raise NotImplementedError(
            f"unsupported: {type(self).__name__} has no diff_belief driver — "
            "the belief diff is a native-engine operator; a four-column "
            "emulation of it is a different query and must be scored as one"
        )

    # ------------------------------------------------------------------ #
    # shared bi-temporal semantics                                        #
    # ------------------------------------------------------------------ #

    def apply_ops(self, ops: Sequence[dict[str, Any]], tt: int) -> None:
        """Apply one write batch at transaction time `tt`. Used by both the
        live write path and event-log replay — must stay deterministic."""
        touched_nodes: set[str] = set()
        touched_edges: dict[str, tuple[str, str]] = {}
        for op in ops:
            kind = op["op"]
            if kind == "assert_node":
                self._assert_node(op, tt)
                touched_nodes.add(op["uid"])
            elif kind == "assert_edge":
                eid = self._assert_edge(op, tt)
                touched_edges[eid] = (op["src"], op["dst"])
            elif kind == "retract":
                self._retract(op, tt)
            elif kind == "correct":
                self._correct(op, tt)
            elif kind == "ingest_events":
                self._ingest_events(op, tt)
            else:
                raise InvalidArgError(f"unknown op kind: {kind}")
        self._tcsr = None  # writes invalidate the current-belief index
        # The batch applied in full — every op above either landed or raised,
        # and a raising batch never reaches here, so the frontier advances only
        # over applied state. Replay maintains it identically, which is
        # what makes a rebuilt store report the same frontier as the original.
        self.note_frontier_tt(tt)
        if self.paranoid:
            for uid in touched_nodes:
                self._check_disjoint([v for v in self.believed_node_versions(uid)], f"node {uid}")
            for eid, (src, dst) in touched_edges.items():
                self._check_disjoint(
                    [v for v in self.believed_edge_versions(eid, src=src, dst=dst)],
                    f"edge {eid}")

    def _supersede_nodes(self, versions: list, tt: int) -> None:
        """Stop believing `versions`, which a write at `tt` is replacing.

        Written by an earlier batch: closed, and kept, because a belief that
        was held and then revised is exactly what this store is for. Written
        by this batch: retired, because [tt, tt) is no transaction time and
        the version was therefore never believed."""
        retired = [v.vid for v in versions if v.tt_s == tt]
        superseded = [v for v in versions if v.tt_s != tt]
        if retired:
            self.retire_node_versions(retired)
        if superseded:
            # `vt_s` alongside the vids: the physical sort key's first half,
            # which lets the native engine locate each row by a manifest
            # range prune rather than by a store-wide vid postings map.
            # Free here — we are holding the versions.
            self.close_node_versions([v.vid for v in superseded], tt,
                                    [v.vt_s for v in superseded])

    def _supersede_edges(self, versions: list, tt: int) -> None:
        retired = [v.vid for v in versions if v.tt_s == tt]
        superseded = [v for v in versions if v.tt_s != tt]
        if retired:
            self.retire_edge_versions(retired)
        if superseded:
            self.close_edge_versions([v.vid for v in superseded], tt,
                                    [v.vt_s for v in superseded])

    @staticmethod
    def _check_disjoint(versions: list, label: str) -> None:
        ivs = sorted((v.vt_s, v.vt_e) for v in versions)
        for (s1, e1), (s2, _) in zip(ivs, ivs[1:]):
            if s2 < e1:
                raise StateError(f"disjointness violated for {label}", intervals=ivs)

    # -- node ops -------------------------------------------------------- #

    def _assert_node(self, op: dict[str, Any], tt: int) -> None:
        uid, label, props = op["uid"], op["label"], op.get("props", {})
        vt = _interval(op["vt_s"], op.get("vt_e", OPEN_END))
        self.ensure_entities([(uid, label)])
        existing = [v for v in self.believed_node_versions_overlapping(uid, vt.start, vt.end)
                    if Interval(v.vt_s, v.vt_e).overlaps(vt)]
        fragments: list[NodeVersion] = []
        for v in existing:
            for fs, fe in _remainder(v.vt_s, v.vt_e, vt.start, vt.end):
                fragments.append(NodeVersion(
                    vid=_vid(uid, tt, fs), uid=uid, label=v.label,
                    vt_s=fs, vt_e=fe, tt_s=tt, tt_e=OPEN_END, props=v.props,
                    source=v.source, provenance_ref=v.provenance_ref))
        self._supersede_nodes(existing, tt)
        new = NodeVersion(vid=_vid(uid, tt, vt.start), uid=uid, label=label,
                          vt_s=vt.start, vt_e=vt.end, tt_s=tt, tt_e=OPEN_END, props=props,
                          source=op.get("source", "ingest"),
                          provenance_ref=op.get("provenance_ref"))
        self.insert_node_versions(fragments + [new])

    # -- edge ops -------------------------------------------------------- #

    def _assert_edge(self, op: dict[str, Any], tt: int) -> str:
        src, dst, rel_type = op["src"], op["dst"], op["rel_type"]
        disc, props = op.get("disc", ""), op.get("props", {})
        vt = _interval(op["vt_s"], op.get("vt_e", OPEN_END))
        eid = edge_eid(src, dst, rel_type, disc)
        self.ensure_entities([(src, op.get("src_label", "")), (dst, op.get("dst_label", ""))])
        existing = [v for v in self.believed_edge_versions_overlapping(
                        eid, vt.start, vt.end, src=src, dst=dst)
                    if Interval(v.vt_s, v.vt_e).overlaps(vt)]
        fragments: list[EdgeVersion] = []
        for v in existing:
            for fs, fe in _remainder(v.vt_s, v.vt_e, vt.start, vt.end):
                fragments.append(EdgeVersion(
                    eid=eid, vid=_vid(eid, tt, fs), src=src, dst=dst, rel_type=rel_type,
                    disc=disc, vt_s=fs, vt_e=fe, tt_s=tt, tt_e=OPEN_END, props=v.props,
                    source=v.source, provenance_ref=v.provenance_ref))
        self._supersede_edges(existing, tt)
        new = EdgeVersion(eid=eid, vid=_vid(eid, tt, vt.start), src=src, dst=dst,
                          rel_type=rel_type, disc=disc, vt_s=vt.start, vt_e=vt.end,
                          tt_s=tt, tt_e=OPEN_END, props=props,
                          source=op.get("source", "ingest"),
                          provenance_ref=op.get("provenance_ref"))
        self.insert_edge_versions(fragments + [new])
        return eid

    # -- retract / correct ------------------------------------------------ #

    def _retract(self, op: dict[str, Any], tt: int) -> None:
        ref = _ref_from_op(op)
        t = op["t"]
        if ref.kind == "node":
            hits = [v for v in self.believed_node_versions_overlapping(ref.identity, t, t + 1)
                    if v.valid_at(t)]
            if not hits:
                raise NotFoundError(f"no believed node version of {ref.identity} valid at {t}")
            self._supersede_nodes(hits, tt)
            repl = [NodeVersion(vid=_vid(v.uid, tt, v.vt_s), uid=v.uid, label=v.label,
                                vt_s=v.vt_s, vt_e=t, tt_s=tt, tt_e=OPEN_END, props=v.props,
                                source=v.source, provenance_ref=v.provenance_ref)
                    for v in hits if v.vt_s < t]
            self.insert_node_versions(repl)
        else:
            hits = [v for v in self.believed_edge_versions_overlapping(
                        ref.identity, t, t + 1, src=ref.src, dst=ref.dst)
                    if v.valid_at(t)]
            if not hits:
                raise NotFoundError(f"no believed edge version of {ref.identity} valid at {t}")
            self._supersede_edges(hits, tt)
            repl = [EdgeVersion(eid=v.eid, vid=_vid(v.eid, tt, v.vt_s), src=v.src, dst=v.dst,
                                rel_type=v.rel_type, disc=v.disc, vt_s=v.vt_s, vt_e=t,
                                tt_s=tt, tt_e=OPEN_END, props=v.props,
                                source=v.source, provenance_ref=v.provenance_ref)
                    for v in hits if v.vt_s < t]
            self.insert_edge_versions(repl)

    def _correct(self, op: dict[str, Any], tt: int) -> None:
        ref = _ref_from_op(op)
        new_props: Props = op["props"]
        vt = _interval(op["vt_s"], op.get("vt_e", OPEN_END))
        if ref.kind == "node":
            versions = self.believed_node_versions_overlapping(ref.identity, vt.start, vt.end)
            hits = [v for v in versions if Interval(v.vt_s, v.vt_e).overlaps(vt)]
            if not hits:
                raise NotFoundError(f"no believed node version of {ref.identity} overlaps vt")
            # `correct` carries no label argument, so the corrected version can only
            # inherit one. Disagreeing labels among the hits leave that choice to the
            # order of an unordered scan, so refuse — before any mutation.
            if len({v.label for v in hits}) > 1:
                raise InvalidArgError(
                    f"correct of {ref.identity} over [{vt.start}, {vt.end}) spans versions "
                    f"with disagreeing labels; split it at the label boundaries",
                    uid=ref.identity, vt_s=vt.start, vt_e=vt.end,
                    hits=[[v.label, v.vt_s, v.vt_e]
                          for v in sorted(hits, key=lambda v: v.vt_s)])
            self._supersede_nodes(hits, tt)
            rows: list[NodeVersion] = []
            for v in hits:
                for fs, fe in _remainder(v.vt_s, v.vt_e, vt.start, vt.end):
                    rows.append(NodeVersion(vid=_vid(v.uid, tt, fs), uid=v.uid, label=v.label,
                                            vt_s=fs, vt_e=fe, tt_s=tt, tt_e=OPEN_END,
                                            props=v.props, source=v.source,
                                            provenance_ref=v.provenance_ref))
            label = hits[0].label
            rows.append(NodeVersion(vid=_vid(ref.identity, tt, vt.start), uid=ref.identity,
                                    label=label, vt_s=vt.start, vt_e=vt.end,
                                    tt_s=tt, tt_e=OPEN_END, props=new_props,
                                    source=op.get("source", "ingest"),
                                    provenance_ref=op.get("provenance_ref")))
            self.insert_node_versions(rows)
        else:
            versions = self.believed_edge_versions_overlapping(ref.identity, vt.start, vt.end,
                                                               src=ref.src, dst=ref.dst)
            hits = [v for v in versions if Interval(v.vt_s, v.vt_e).overlaps(vt)]
            if not hits:
                raise NotFoundError(f"no believed edge version of {ref.identity} overlaps vt")
            self._supersede_edges(hits, tt)
            proto = hits[0]
            rows_e: list[EdgeVersion] = []
            for v in hits:
                for fs, fe in _remainder(v.vt_s, v.vt_e, vt.start, vt.end):
                    rows_e.append(EdgeVersion(eid=v.eid, vid=_vid(v.eid, tt, fs), src=v.src,
                                              dst=v.dst, rel_type=v.rel_type, disc=v.disc,
                                              vt_s=fs, vt_e=fe, tt_s=tt, tt_e=OPEN_END,
                                              props=v.props, source=v.source,
                                              provenance_ref=v.provenance_ref))
            rows_e.append(EdgeVersion(eid=proto.eid, vid=_vid(proto.eid, tt, vt.start),
                                      src=proto.src, dst=proto.dst, rel_type=proto.rel_type,
                                      disc=proto.disc, vt_s=vt.start, vt_e=vt.end,
                                      tt_s=tt, tt_e=OPEN_END, props=new_props,
                                      source=op.get("source", "ingest"),
                                      provenance_ref=op.get("provenance_ref")))
            self.insert_edge_versions(rows_e)

    # -- bulk event ingestion --------------------------------------------- #

    def _ingest_events(self, op: dict[str, Any], tt: int) -> None:
        """Bulk path for event-stream datasets. Events are instantaneous
        (vt_e defaults to vt_s + 1) and disjoint by construction: each event
        without an explicit disc gets its batch offset as discriminator, so
        every event is its own logical edge. Overlap checks are bypassed.

        **The optional `nodes` array** carries structured node versions —
        `[{uid, label, props?, vt_s, vt_e?}, ...]` — so a labelled, propertied
        bulk load rides this path instead of one `assert_node` batch per node.
        It is optional and additive: an op without it behaves **exactly** as
        before, which the frozen digest receipt is the proof of.

        Why it exists: `Store.assert_node` writes one batch per call, and a
        batch is a log append plus a manifest commit whose cost grows with the
        store. Loading N nodes that way costs O(N²) work *and* O(N²) bytes —
        measured at 8,435 nodes producing 8,436 manifests and 15 GB. This path
        writes one batch per chunk and inserts node versions as one bulk array,
        which is why it stays flat.

        **Explicit nodes may not supersede** (see `_ingest_node_rows`): a
        collision refuses. That is what keeps this op Class A in the op taxonomy
        — pure append, no carve — and therefore what lets its footprint keep
        emitting no carve arm.
        """
        events: list[dict[str, Any]] = op.get("events") or []
        node_first_seen: dict[str, int] = {}
        edge_rows: list[EdgeVersion] = []
        for i, ev in enumerate(events):
            src, dst, rel_type = ev["src"], ev["dst"], ev["rel_type"]
            vt_s = ev["vt_s"]
            vt_e = ev.get("vt_e") or vt_s + 1
            disc = ev.get("disc", f"#{op.get('offset', 0) + i}")
            eid = edge_eid(src, dst, rel_type, disc)
            edge_rows.append(EdgeVersion(
                eid=eid, vid=_vid(eid, tt, vt_s), src=src, dst=dst, rel_type=rel_type,
                disc=disc, vt_s=vt_s, vt_e=vt_e, tt_s=tt, tt_e=OPEN_END,
                props=ev.get("props", {}),
                source=op.get("source", "ingest"),
                provenance_ref=op.get("provenance_ref")))
            for u in (src, dst):
                if u not in node_first_seen or vt_s < node_first_seen[u]:
                    node_first_seen[u] = vt_s
        label = op.get("node_label", "Node")
        explicit = self._ingest_node_rows(op, tt)
        explicit_uids = {r.uid for r in explicit}
        known = self.nodes_with_believed_versions(list(node_first_seen))
        # a uid the op describes explicitly is not also auto-created from an
        # endpoint: the explicit record carries the label and props, and a
        # second version over the same interval would violate disjointness
        new_uids = [u for u in node_first_seen
                    if u not in known and u not in explicit_uids]
        self.ensure_entities(
            [(r.uid, r.label) for r in explicit]
            + [(u, label) for u in node_first_seen if u not in explicit_uids])
        self.insert_node_versions(explicit + [
            NodeVersion(vid=_vid(u, tt, node_first_seen[u]), uid=u, label=label,
                        vt_s=node_first_seen[u], vt_e=OPEN_END, tt_s=tt, tt_e=OPEN_END,
                        props={}, source=op.get("source", "ingest"),
                        provenance_ref=op.get("provenance_ref"))
            for u in sorted(new_uids)])
        self.insert_edge_versions(edge_rows)

    def _ingest_node_rows(self, op: dict[str, Any], tt: int) -> list[NodeVersion]:
        """The optional `nodes` array, as version rows — or `[]` when absent.

        **A collision refuses.** An explicit node whose uid already has a
        believed version is a loader bug, not a statement of intent: bulk
        loading is for building a store, and "supersede whatever was there"
        is what `assert_node` is *for*. Refusing is also what keeps the
        soundness story simple — an op that cannot supersede cannot carve, so
        this stays Class A and its footprint needs no carve arm.

        The check is one batched `nodes_with_believed_versions` call over the
        whole array, not one query per node, or the refusal would reintroduce
        the per-op term this path exists to avoid.
        """
        nodes = op.get("nodes")
        if not nodes:
            return []

        seen: dict[str, int] = {}
        for i, rec in enumerate(nodes):
            uid = rec["uid"]
            if uid in seen:
                raise InvalidArgError(
                    f"ingest_events: uid {uid!r} appears twice in one `nodes` "
                    f"array (positions {seen[uid]} and {i}); a bulk load "
                    f"states each node once",
                    uid=uid)
            seen[uid] = i

        collisions = sorted(self.nodes_with_believed_versions(list(seen)))
        if collisions:
            shown = collisions[:8]
            raise InvalidArgError(
                f"ingest_events: {len(collisions)} of {len(seen)} nodes already "
                f"have a believed version ({', '.join(shown)}"
                f"{', ...' if len(collisions) > len(shown) else ''}). Bulk node "
                f"ingest appends and never supersedes — use `correct` to revise "
                f"a belief, or `assert_node` to replace one.",
                colliding=shown, n_colliding=len(collisions))

        source = op.get("source", "ingest")
        provenance_ref = op.get("provenance_ref")
        rows: list[NodeVersion] = []
        for rec in nodes:
            vt = _interval(rec["vt_s"], rec.get("vt_e", OPEN_END))
            rows.append(NodeVersion(
                vid=_vid(rec["uid"], tt, vt.start), uid=rec["uid"],
                label=rec["label"], vt_s=vt.start, vt_e=vt.end,
                tt_s=tt, tt_e=OPEN_END, props=rec.get("props") or {},
                source=rec.get("source", source),
                provenance_ref=rec.get("provenance_ref", provenance_ref)))
        return rows

    # ------------------------------------------------------------------ #
    # digest (replay-equivalence check)                                   #
    # ------------------------------------------------------------------ #

    def store_digest(self) -> str:
        """Digest of the full logical store content, backend-independent."""
        node_rows = sorted(
            (v.to_json() for v in self.all_node_versions()),
            key=lambda r: (r["uid"], r["tt_s"], r["vt_s"], r["vid"]))
        edge_rows = sorted(
            (v.to_json() for v in self.all_edge_versions()),
            key=lambda r: (r["eid"], r["tt_s"], r["vt_s"], r["vid"]))
        return digest({"nodes": node_rows, "edges": edge_rows})

    def store_digest_streaming(self, chunk_rows: int = 200_000) -> str:
        """`store_digest()`, computed in bounded memory by an external merge
        sort — never holding more than `chunk_rows` rows (per kind) or one
        buffered row per spilled chunk at once, so this stays flat as the
        store grows instead of materializing every version as `store_digest`
        does (the 25 GB digest-pass spike a 17.4M-edge store already showed;
        at 100M+ rows the same pass alone would exceed a 93 GB host).

        **Byte-identical to `store_digest()` for the same store — proved,
        not assumed** (`tests/test_store_digest_streaming.py`). The reason it
        can be: `canonical_json` uses compact separators (`","`, `":"`, no
        whitespace), so a row's own JSON text never depends on where in the
        outer array it lands — nesting depth does not change a compact
        encoder's output. `store_digest()` builds
        `sha256_hex(canonical_json({"edges": edge_rows, "nodes": node_rows}))`
        (`digest()` sorts the top-level keys, and "edges" < "nodes"). This
        method reproduces exactly that byte stream —
        `{"edges":[<row>,<row>,...],"nodes":[<row>,...]}` — by feeding a
        running `sha256` the same bytes in the same order, without ever
        holding the whole string (or the whole row population) in memory:
        each kind's rows are spilled to disk in `chunk_rows`-sized,
        pre-sorted batches (`_spill_sorted_rows`), then reassembled into
        global sorted order by a k-way merge over the spilled files
        (`_merge_sorted_spill`) that reads one buffered line per chunk.

        **The row *producer* is also bounded now.** Earlier this fed `_spill_sorted_rows` a generator over
        `self.all_edge_versions()`/`all_node_versions()` directly — a call
        that, on the native backend, returns only once every version of
        that kind has been converted into a Python list (each with its
        `props` already `json.loads`-parsed), so the *input* to the
        "streaming" sort was itself a full materialization: the sort was
        streaming, the production was not (an ~80 GB digest-pass
        transient at 100M). `all_versions_paged` (above) is that
        production path chunked; a backend that can produce a page without
        building the whole population first (native's
        `all_versions_page`) makes this call's own peak memory
        O(chunk_rows), not O(store).

        Chunk files live in a `tempfile.TemporaryDirectory` cleaned up before
        this returns (including on an exception mid-merge).
        """
        edge_key = _EDGE_DIGEST_KEY
        node_key = _NODE_DIGEST_KEY
        with tempfile.TemporaryDirectory(prefix="gnomon-digest-") as tmp:
            tmp_path = Path(tmp)
            edge_files = _spill_sorted_rows(
                (v.to_json() for chunk in self.all_versions_paged("edge", chunk_rows)
                 for v in chunk),
                edge_key, chunk_rows, tmp_path, "edge")
            node_files = _spill_sorted_rows(
                (v.to_json() for chunk in self.all_versions_paged("node", chunk_rows)
                 for v in chunk),
                node_key, chunk_rows, tmp_path, "node")

            h = hashlib.sha256()
            h.update(b'{"edges":[')
            _merge_sorted_spill(edge_files, h)
            h.update(b'],"nodes":[')
            _merge_sorted_spill(node_files, h)
            h.update(b']}')
            return h.hexdigest()


    def content_digest(self) -> str:
        """`store_digest()`'s fields minus `vid`/`tt_s`/`tt_e` — the digest
        two independent ingests of the same logical events *can* share.

        All three dropped fields derive from the wall-clock transaction clock,
        so `store_digest()` equality between independent builds is
        structurally impossible and `content_digest()` is the only
        layout-independent store identity an A/B may be compared on.

        **This is the library home of a notion the campaign has been computing
        from a driver.** `scripts/build_synth_store.py::content_digest` is the
        definition of record and stays the reference this is tested against
        (`tests/test_content_digest_streaming.py`); it is transcribed here,
        not re-derived, so the two cannot drift. Like `store_digest()` it
        materializes the whole version population — that is the pass the
        streaming digest exists to replace at scale, and the native adapter's
        `content_digest_streaming()` overrides it with a bounded one.

        The sort is total (by construction): rows tying on the four keyed
        components are broken by their own canonical bytes, so this value no
        longer depends on `all_*_versions()`'s incoming order even in that
        case — see `content_digest_report` for the accompanying
        `ambiguous_ties` count.
        """
        return self.content_digest_report()["digest"]

    def content_digest_report(self) -> dict[str, Any]:
        """`content_digest()`'s digest plus `ambiguous_ties`: the count of
        adjacent sorted rows that tie on the
        reference's four-component key (identity, `vt_s`, `vt_e`, props text)
        yet differ in content. The fifth, total-order-making sort component —
        each row's own canonical bytes — is what makes the returned `digest`
        layout-independent even when such ties exist; `ambiguous_ties`
        reports how many pairs needed it, mirroring
        `crates/gnomon-core/src/digest.rs`'s `ambiguous_ties` field and
        transcribed from `scripts/build_synth_store.py::content_digest_report`
        the same way `content_digest` is transcribed from `content_digest`."""
        def strip(row: dict[str, Any]) -> dict[str, Any]:
            row = dict(row)
            row.pop("vid", None)
            row.pop("tt_s", None)
            row.pop("tt_e", None)
            return row

        def reference_key(id_field: str, r: dict[str, Any]) -> tuple[Any, Any, Any, str]:
            return (r[id_field], r["vt_s"], r["vt_e"],
                    _json.dumps(r["props"], sort_keys=True))

        def sort_key(id_field: str):
            def key(r: dict[str, Any]) -> tuple[Any, Any, Any, str, str]:
                return (*reference_key(id_field, r), canonical_json(r))
            return key

        def count_ties(rows_sorted: list[dict[str, Any]], id_field: str) -> int:
            ties = 0
            prev_key: tuple[Any, Any, Any, str] | None = None
            prev_json: str | None = None
            for r in rows_sorted:
                key = reference_key(id_field, r)
                rj = canonical_json(r)
                if prev_key is not None and prev_key == key and prev_json != rj:
                    ties += 1
                prev_key, prev_json = key, rj
            return ties

        node_rows = sorted(
            (strip(v.to_json()) for v in self.all_node_versions()),
            key=sort_key("uid"))
        edge_rows = sorted(
            (strip(v.to_json()) for v in self.all_edge_versions()),
            key=sort_key("eid"))
        ambiguous_ties = count_ties(node_rows, "uid") + count_ties(edge_rows, "eid")
        return {"digest": digest({"nodes": node_rows, "edges": edge_rows}),
                "ambiguous_ties": ambiguous_ties}


#: Sort keys `store_digest`/`store_digest_streaming` order rows by — kept as
#: named constants so both call sites are provably the same key.
def _NODE_DIGEST_KEY(r: dict[str, Any]) -> tuple[Any, ...]:
    return (r["uid"], r["tt_s"], r["vt_s"], r["vid"])


def _EDGE_DIGEST_KEY(r: dict[str, Any]) -> tuple[Any, ...]:
    return (r["eid"], r["tt_s"], r["vt_s"], r["vid"])


def _spill_sorted_rows(
    rows: Iterable[dict[str, Any]],
    key_fn: Any,
    chunk_rows: int,
    tmp_dir: Path,
    prefix: str,
) -> list[Path]:
    """Consume `rows` in batches of at most `chunk_rows`, sort each batch by
    `key_fn`, and spill it to its own file as `[key, canonical_json(row)]`
    JSON lines. Returns the spilled file paths in creation order (empty list
    if `rows` was empty) — never holds more than one batch in memory.

    The spilled `canonical_json(row)` text is the exact byte sequence
    `store_digest()` would have embedded for this row (see
    `store_digest_streaming`'s docstring); round-tripping it through
    `json.dumps`/`json.loads` here (to give it a safe home inside a line of
    its own alongside the sort key) preserves it exactly, because JSON string
    escaping is a lossless encoding of the original text.
    """
    paths: list[Path] = []
    batch: list[dict[str, Any]] = []

    def flush(batch: list[dict[str, Any]]) -> None:
        if not batch:
            return
        batch.sort(key=key_fn)
        fh = tempfile.NamedTemporaryFile(
            mode="w", dir=tmp_dir, prefix=f"{prefix}-", suffix=".jsonl",
            delete=False, encoding="utf-8")
        try:
            for row in batch:
                fh.write(_json.dumps([list(key_fn(row)), canonical_json(row)]))
                fh.write("\n")
        finally:
            fh.close()
        paths.append(Path(fh.name))

    for row in rows:
        batch.append(row)
        if len(batch) >= chunk_rows:
            flush(batch)
            batch = []
    flush(batch)
    return paths


def _merge_sorted_spill(paths: list[Path], hasher: Any) -> None:
    """K-way merge `paths` (each pre-sorted by the spilling key, one JSON
    line per row as `[key, row_text]`) in global key order, feeding each
    row's `row_text` bytes into `hasher` as a comma-separated JSON array
    element — exactly the array body `store_digest()`'s `json.dumps` would
    produce for the same, fully-materialized, sorted row list.

    Holds one buffered line per open file at once (the heap), never the
    whole of any file — the spilled files themselves are deleted as this
    returns, whether it completes normally or raises.
    """
    if not paths:
        return
    files = [p.open("r", encoding="utf-8") for p in paths]
    try:
        heap: list[tuple[list[Any], int, str]] = []
        for i in range(len(files)):
            line = files[i].readline()
            if line:
                key, text = _json.loads(line)
                heap.append((key, i, text))
        heapq.heapify(heap)
        first = True
        while heap:
            _key, i, text = heapq.heappop(heap)
            if not first:
                hasher.update(b",")
            first = False
            hasher.update(text.encode("utf-8"))
            line = files[i].readline()
            if line:
                key2, text2 = _json.loads(line)
                heapq.heappush(heap, (key2, i, text2))
    finally:
        for f in files:
            f.close()
        for p in paths:
            p.unlink(missing_ok=True)


def _remainder(vs: int, ve: int, cs: int, ce: int) -> list[tuple[int, int]]:
    """Parts of [vs, ve) not covered by [cs, ce)."""
    out = []
    if vs < cs:
        out.append((vs, min(ve, cs)))
    if ce < ve:
        out.append((max(vs, ce), ve))
    return out


def _ref_from_op(op: dict[str, Any]) -> EntityRef:
    r = op["ref"]
    return EntityRef(kind=r["kind"], uid=r.get("uid"), src=r.get("src"), dst=r.get("dst"),
                     rel_type=r.get("rel_type"), disc=r.get("disc", ""))


def make_op(kind: str, **kwargs: Any) -> dict[str, Any]:
    """Canonical op record for the event log."""
    op = {"op": kind, **kwargs}
    # round-trip through canonical JSON so the logged and applied forms agree
    return _json.loads(canonical_json(op))
