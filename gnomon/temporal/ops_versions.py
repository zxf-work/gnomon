"""`version_history`: the belief log as a queryable population.

Every operator before this one answers *what did we believe* at some
`as_of_tt`. None answered *what did we revise, and when* — so a store built
around keeping corrections could not count them.

This is `entity_history` without the uid: the same version rows, the same
bi-temporal censoring, over the whole store instead of one identity.

THE CONTRACT, and the part an implementation gets wrong first. Everything is
evaluated **as of `as_of_tt`**, including the question of what counts as
superseded:

  * a version whose `tt_s` is after `as_of_tt` **does not exist yet** and
    appears in no mode;
  * a version whose belief ended after `as_of_tt` had not ended yet, so it
    is *current* at that `as_of` and its `tt_e` is reported as `OPEN_END` —
    the same censoring applies, for the same reason;
  * therefore `belief: "superseded"` as of a transaction time before a
    correction is honestly an **empty page**.

Without that, a pinned result leaks knowledge the caller's belief state does
not have, and bi-temporal immutability stops being a property of the system.

WHAT "HOW MANY CORRECTIONS" MEANS HERE. A count of **superseded** versions:
beliefs that were revised. A store nobody has corrected answers 0; a
`correct` that carves one version into three closes exactly one belief and
so counts once. Counting the *replacements* instead would count the
fragments, which are the same belief re-expressed rather than a revision.

Rows are ordered by `(tt_s, vid)` — the belief log's own order, not valid
time. That ordering is the difference between this operator and a snapshot.
Props are deliberately not returned: this reports the *shape* of the log,
and a props column would put a JSON blob on every row of a whole-store scan.
"""

from __future__ import annotations

from typing import Any

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END, clamp_tt, edge_eid
from gnomon.storage.base import StorageAdapter
from gnomon.temporal.algebra import (
    AS_OF_TT,
    CURSOR,
    LIMIT,
    UID,
    WINDOW,
    check_window,
    operator,
    paginate,
    required,
)
from gnomon.temporal.guardrails import (
    ROUTES_KEY,
    coeff_scale,
    point_read_estimate,
    rel_rows,
)

#: Engine-level (proposal D2 of the operator-coverage study): the belief log
#: of ONE identity. `{uid}` for a node; `{src, dst, rel_type, disc?}` or
#: `{eid}` for an edge. Read through the identity postings, so the cost is
#: the identity's own history, not the store's. Neither property has a
#: default and neither is in the base schema (algebra.OperatorSpec).
REF = {
    "type": "object",
    "properties": {
        "uid": UID,
        "eid": {"type": "string", "minLength": 1},
        "src": UID,
        "dst": UID,
        "rel_type": {"type": "string", "minLength": 1},
        "disc": {"type": "string"},
    },
    "additionalProperties": False,
    "description": "one identity: {uid} (kind node), or {src, dst, rel_type, "
                   "disc?} or {eid} (kind edge)",
}
INCLUDE_PROPS = {"type": "boolean",
                 "description": "with `ref`: each row also carries its props"}


def _engine_schema(schema: dict[str, Any]) -> dict[str, Any]:
    schema["properties"]["ref"] = REF
    schema["properties"]["include_props"] = INCLUDE_PROPS
    return schema


def _ref_key(kind: str, ref: dict[str, Any]) -> str:
    """The identity a `ref` names: a uid, or an edge's eid."""
    if kind == "node":
        if set(ref) != {"uid"}:
            raise InvalidArgError("a node ref is {uid}")
        return ref["uid"]
    if set(ref) == {"eid"}:
        return ref["eid"]
    if "uid" in ref or "eid" in ref or not {"src", "dst", "rel_type"} <= set(ref):
        raise InvalidArgError("an edge ref is {src, dst, rel_type, disc?} or {eid}")
    return edge_eid(ref["src"], ref["dst"], ref["rel_type"], ref.get("disc", ""))

def _version_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """This operator is in a different cost class from the rest of the
    algebra, and the guardrail has to know it.

    `all_*_versions` materializes one Python object per version; every other
    operator reads packed columns. Measured at 10,000,000 versions: 153.9 s
    and 13.3 GB peak RSS, against 1.3 s and 2.3 GB for a columnar count over
    the same population — 116x the time, 54x the bytes per row.

    Two departures from `scan_estimate`, both of them the truth rather than
    a safety margin. **No window pruning**, because there is no pushdown:
    the window filters the output, not the work, so a narrow window must not
    make the call look cheap. And the per-row cost is charged against
    `expansions_est`, because per-row allocation is what that ceiling is
    for — which puts the refusal at around 5M versions, on the right side of
    a call that would otherwise run for two and a half minutes.
    """
    if args.get("ref") is not None:
        # one identity's log, through its postings: a point read
        return point_read_estimate(args, stats)
    if (stats.get(ROUTES_KEY) or {}).get("version_page"):
        return _page_cost(args, stats)
    n = int(stats.get("n_edge_versions", 0)) + int(stats.get("n_node_versions", 0))
    return {"rows_scanned_est": n, "expansions_est": n}


#: The engine's bounded page (`NativeAdapter.versions_page`): a sweep of the
#: kind's stored version keys under the filter, the matches sorted by
#: `(tt_s, vid)`, and only the page built in Python. Measured on the five
#: calibration stores: ~165 ms per million matching versions,
#: ~4 ms per million swept (every version of the kind) and ~9 us per page row.
VERSION_PAGE_MS_PER_M = 250.0
VERSION_SWEEP_MS_PER_M = 8.0
VERSION_PAGE_ROW_MS = 0.015
VERSION_PAGE_CALL_MS = 2.0


def _page_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """`version_history` without `ref` on a backend with its own bounded
    `versions_page`. The base rate (15,400 ms/M, one Python object per
    version) is the base's default route; the engine builds the page alone,
    so it was 800 to 49,000 times its time. The versions the sweep can match
    are the kind's (an edge call's `rel_types'), every one of them in the
    worst case: the window and the belief mode filter during the sweep, and
    no share is taken for them; the sweep itself reads every version of the
    kind."""
    if args.get("kind") == "node":
        n = swept = int(stats.get("n_node_versions", 0))
    else:
        n, swept = rel_rows(stats, args.get("rel_types")), int(stats.get("n_edge_versions", 0))
    page = min(int(args.get("limit") or 0), n)
    ms = (VERSION_PAGE_CALL_MS + n * VERSION_PAGE_MS_PER_M / 1e6
          + swept * VERSION_SWEEP_MS_PER_M / 1e6 + page * VERSION_PAGE_ROW_MS) * coeff_scale()
    return {"rows_scanned_est": n, "expansions_est": 0, "time_est_ms": int(ms)}


#: Columns dropped from the version row. `props` is a blob and this
#: operator is a whole-store scan; `source`/`provenance_ref` are reserved
#: for Phase-3 write-back and are constant for every row written so far.


def _version_validators(args: dict[str, Any]) -> None:
    check_window(args)
    if args["kind"] == "node" and args.get("rel_types") is not None:
        raise InvalidArgError(
            "'rel_types' is only meaningful with kind 'edge'; node versions "
            "have a label, not a relationship type")
    if args.get("ref") is not None:
        _ref_key(args["kind"], args["ref"])
    elif args.get("include_props"):
        raise InvalidArgError(
            "'include_props' needs a 'ref': props on a whole-log scan would "
            "put a JSON blob on every row of the store")


def _cursor_offset(cursor: str | None) -> int:
    """`paginate`'s own cursor arithmetic, needed *before* the page is read
    rather than after it.

    The offset has to reach the backend, because the whole point of
    `versions_page` is that nothing outside the page is ever built — so the
    slice cannot be taken here, on a list the old path materialized first.
    The spelling is `paginate`'s (`algebra.py`), including the error, because
    the cursor is a plaintext decimal offset fixed by the operator contract; `paginate` is
    still called below over `range(rows_total)`, so the two cannot drift
    without the length check firing.
    """
    try:
        return int(cursor) if cursor else 0
    except ValueError:
        raise InvalidArgError(f"bad cursor: {cursor!r}") from None


@operator(
    "version_history",
    {
        "kind": required({"type": "string", "enum": ["node", "edge"]}),
        "window": required(WINDOW),
        "belief": {"type": "string",
                   "enum": ["current", "superseded", "all"],
                   "default": "current",
                   "description": "which beliefs, AS OF as_of_tt: current = "
                                  "still believed then, superseded = revised "
                                  "by then, all = written by then"},
        "rel_types": {"type": ["array", "null"], "items": {"type": "string"},
                      "default": None},
        "as_of_tt": AS_OF_TT,
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "The belief log itself: version rows of nodes or edges whose valid-time "
    "interval overlaps `window`, with BOTH clocks on every row (vt_s, vt_e, "
    "tt_s, tt_e) and ordered by transaction time. `belief` selects which "
    "beliefs, always as of `as_of_tt`: 'current' = still believed then, "
    "'superseded' = already revised by then, 'all' = everything written by "
    "then. **How many corrections exist is a count of `superseded` rows.** "
    "A version written after as_of_tt does not appear, and a belief that "
    "ended after as_of_tt is reported as still open — so a pinned result "
    "never leaks a revision the caller's belief state has not seen. Props "
    "are not returned; use entity_history for one identity's full rows. "
    "COST: this materializes the whole version log — no columnar version "
    "scan exists — so it is refused by the cost guardrail on stores past a "
    "few million versions, and the window prunes the answer rather than the "
    "work.",
    cost_fn=_version_cost,
    validators=[_version_validators],
    output_fields=("rows", "rows_total", "truncated", "cursor"),
    engine_schema=_engine_schema,
)
def version_history(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    as_of = clamp_tt(args["as_of_tt"])
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    belief, kind = args["belief"], args["kind"]
    rel_types = args["rel_types"]
    if args.get("ref") is not None:
        return _identity_log(adapter, args, as_of)

    # The page, and the exact size of the population it came out of. The
    # filtering, the ordering and the count all happen below the adapter:
    # a backend that can do them over packed columns builds nothing but the
    # page, and one that cannot falls back to the columnar mask, which is
    # this arithmetic verbatim (`StorageAdapter.versions_page`). The old
    # path did the mask here, over a struct-of-arrays of the whole
    # population — 10.6 GB at 10M edge versions to return `limit` rows.
    # `rows_total` is still exact: the filter counts without materializing.
    offset = _cursor_offset(args["cursor"])
    cols, rows_total = adapter.versions_page(
        kind, as_of=as_of, t_a=t_a, t_b=t_b, belief=belief,
        rel_types=rel_types, offset=offset, limit=args["limit"])

    # The envelope from `paginate` itself, over a `range` standing in for the
    # ordering — it slices in O(1) and never holds a row, and using the frozen
    # function is what keeps `truncated` and the cursor spelled exactly as
    # the operator contract fixes them.
    page = paginate(range(rows_total), args["limit"], args["cursor"])
    n = len(cols["vid"])
    if n != len(page["rows"]):
        raise InvalidArgError(
            f"versions_page returned {n} rows where offset {offset} and limit "
            f"{args['limit']} over {rows_total} select {len(page['rows'])}")

    names = adapter.VERSION_COLS[kind]
    tt_e = cols["tt_e"]
    page["rows"] = [
        {**{c: (int(cols[c][i]) if c in adapter.VERSION_INT_COLS
                else cols[c][i]) for c in names},
         # a belief that ended after `as_of` had not ended yet
         "tt_e": int(tt_e[i]) if tt_e[i] <= as_of else OPEN_END}
        for i in range(n)
    ]
    return page


def _identity_log(adapter: StorageAdapter, args: dict[str, Any],
                  as_of: int) -> dict[str, Any]:
    """`version_history` restricted to one identity: the same rows, the same
    as-of censoring, the same `(tt_s, vid)` order, over the identity's own
    versions (`StorageAdapter.identity_versions`) instead of the whole log.
    Versions ended by a bare retraction are in it like any other closed
    version: the log is read, not reconstructed from believed states."""
    kind = args["kind"]
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    belief, rel_types = args["belief"], args["rel_types"]
    names = adapter.VERSION_COLS[kind]
    rows = []
    for v in adapter.identity_versions(kind, _ref_key(kind, args["ref"])):
        if v.tt_s > as_of:
            continue                    # not written yet, in any mode
        superseded = v.tt_e <= as_of
        if (belief == "current" and superseded) or \
                (belief == "superseded" and not superseded):
            continue
        if not (v.vt_s < t_b and t_a < v.vt_e):
            continue
        if rel_types is not None and v.rel_type not in rel_types:
            continue
        r = {c: getattr(v, c) for c in names}
        r["tt_e"] = int(v.tt_e) if superseded else OPEN_END
        if args.get("include_props"):
            r["props"] = v.props
        rows.append(r)
    rows.sort(key=lambda r: (r["tt_s"], r["vid"]))
    return paginate(rows, args["limit"], args["cursor"])
