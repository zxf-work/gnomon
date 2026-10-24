"""TPC-BiH-shaped synthetic bi-temporal graph generator.

**This is not a validated port of TPC-BiH** (the bitemporal extension of
TPC-H), not TPC data, and not a TPC benchmark: it is a small deterministic
generator with the *shape* of a TPC-H schema stored as a property graph, and
with update scenarios modelled on TPC-BiH's history generator.

Nodes: Customer, Orders, Lineitem, Part, Supplier, Nation, Region, with uid
`"<Table>:<pk>"` (`"Lineitem:<orderkey>-<linenumber>"`). Edges: PLACED_BY
(Orders->Customer), IN_ORDER (Lineitem->Orders), OF_PART (Lineitem->Part),
FROM_SUPPLIER (Lineitem->Supplier), IN_NATION (Customer|Supplier->Nation),
IN_REGION (Nation->Region) and SUPPLIES (Supplier->Part, the PARTSUPP row:
props `availqty`, `supplycost`). Money is integer cents.

Two application times: valid time is each entity's application time
(`[a, b)` or `[a, OPEN_END)`; Supplier, Nation and Region are `[0,
OPEN_END)`), and an order's receivable period is kept as the props
`receivable_s`/`receivable_e`. Transaction time is the batch order.

The write stream, in commit order (`tt` = 1, 2, ...):

1. the bulk load (version 0): every node, then every edge, split into batches
   of at most `LOAD_CHUNK` ops (one batch up to a few thousand customers);
2. 24 scenario batches, one scenario per batch, cycling through New Order,
   Cancel Order, Deliver, Receive Payment, Update Stock, Change Price, Update
   Supplier and Manipulate Order Data. Deliver and Change Price are
   application-time updates (an assert over `[t, OPEN_END)` that carves the
   current version); Receive Payment, Update Stock, Update Supplier and
   Manipulate Order Data are non-temporal (`correct`, keeping valid time);
   Cancel Order retracts the order, its lineitems and their edges at `t`;
3. when `corrections_pct > 0`, two batches of `correct` ops that change one
   numeric prop of that percentage of node versions.

`Part:55` always exists with four suppliers, and every Update Stock and
Change Price batch touches its SUPPLIES edges, so they change along both
clocks.

Size: `scale` is the number of customers; about 5 orders per customer, 4
lineitems per order, `max(60, scale)` parts and `max(5, scale // 10)`
suppliers: roughly 100 version rows per customer (about 10k rows at scale
100, 1M at scale 10,000).
"""

from __future__ import annotations

import calendar
import random
import shutil
from pathlib import Path
from typing import Any, Callable, Iterable

Op = dict[str, Any]
EdgeKey = tuple[str, str, str, str]

OPEN_END = 2 ** 62
HOUR = 3_600_000_000
DAY = 86_400_000_000
#: Largest number of ops in one write batch; the bulk load is split into
#: chunks of this size (nodes first, so every edge lands after its endpoints).
LOAD_CHUNK = 100_000
SOURCE = "tpcbih_shaped"


def _ts(y: int, m: int, d: int) -> int:
    return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0)) * 1_000_000


DATA_START = _ts(2020, 1, 1)
LOAD_END = _ts(2022, 1, 1)
#: The valid instant "now" reads use: inside the data (open-ended versions
#: run past it), and after every generated valid-time start.
NOW = _ts(2022, 7, 1)
SCENARIO_END = NOW - DAY

PART55 = "Part:55"


# --------------------------------------------------------------------------
# in-memory mirror of the store's op semantics
# --------------------------------------------------------------------------

def _remainder(vs: int, ve: int, cs: int, ce: int) -> list[tuple[int, int]]:
    """Parts of [vs, ve) not covered by [cs, ce)."""
    out = []
    if vs < cs:
        out.append((vs, min(ve, cs)))
    if ce < ve:
        out.append((max(vs, ce), ve))
    return out


def _start(v: list[Any]) -> int:
    return v[0]


def _distinct(seq: Iterable[Any]) -> list[Any]:
    """Order-preserving de-duplication."""
    return list(dict.fromkeys(seq))


class _Ledger:
    """The current belief, kept in memory while the ops are generated.

    Each op is applied with the semantics of `gnomon/storage/base.py`: an
    assert carves any overlapping believed version of the same identity, a
    retract truncates the one version valid at `t` to `[vt_s, t)` (dropping it
    when `vt_s == t`), and a correct replaces the props over an exact valid
    interval. The ledger records the ops into batches, refuses an assert that
    would overlap a believed version unless the caller asks for a carve, and
    checks at every batch boundary that each believed edge version lies
    inside the believed valid time of both endpoints (adjacent node versions
    merged, since a carve splits one interval into touching pieces).

    Node versions are `[vt_s, vt_e, label, props]`, edge versions
    `[vt_s, vt_e, props]`, each list sorted by `vt_s`.
    """

    def __init__(self, source: str, now: int) -> None:
        self.source = source
        self.now = now
        self.nodes: dict[str, list[list[Any]]] = {}
        self.edges: dict[EdgeKey, list[list[Any]]] = {}
        self.incident: dict[str, list[EdgeKey]] = {}
        self.batches: list[tuple[int, list[Op]]] = []
        self._ops: list[Op] = []
        self._dirty_nodes: list[str] = []
        self._dirty_edges: list[EdgeKey] = []

    # -- batches ----------------------------------------------------------

    def _emit(self, op: Op) -> None:
        self._ops.append(op)
        if len(self._ops) >= LOAD_CHUNK:
            self.commit()

    def commit(self) -> None:
        """Close the current batch (a no-op when it is empty)."""
        if not self._ops:
            return
        self._check(self._dirty_nodes, self._dirty_edges)
        self.batches.append((len(self.batches) + 1, self._ops))
        self._ops, self._dirty_nodes, self._dirty_edges = [], [], []

    # -- asserts ----------------------------------------------------------

    def _interval_ok(self, vt_s: int, vt_e: int) -> None:
        if not (0 <= vt_s < vt_e <= OPEN_END) or vt_s >= self.now:
            raise AssertionError(f"invalid valid interval [{vt_s}, {vt_e})")

    @staticmethod
    def _carve(versions: list[list[Any]], vt_s: int, vt_e: int, carve: bool,
               what: str) -> list[list[Any]]:
        out: list[list[Any]] = []
        for v in versions:
            if v[0] < vt_e and vt_s < v[1]:
                if not carve:
                    raise AssertionError(
                        f"{what} asserted over a believed interval "
                        f"[{v[0]}, {v[1]})")
                for fs, fe in _remainder(v[0], v[1], vt_s, vt_e):
                    out.append([fs, fe, *v[2:]])
            else:
                out.append(v)
        return out

    def assert_node(self, uid: str, label: str, props: dict[str, Any],
                    vt_s: int, vt_e: int = OPEN_END, *,
                    carve: bool = False) -> None:
        self._interval_ok(vt_s, vt_e)
        kept = self._carve(self.nodes.get(uid, []), vt_s, vt_e, carve,
                           f"node {uid}")
        kept.append([vt_s, vt_e, label, props])
        kept.sort(key=_start)
        self.nodes[uid] = kept
        self._dirty_nodes.append(uid)
        self._emit({"op": "assert_node", "uid": uid, "label": label,
                    "props": props, "vt_s": vt_s, "vt_e": vt_e,
                    "source": self.source, "provenance_ref": None})

    def assert_edge(self, src: str, dst: str, rel: str, props: dict[str, Any],
                    vt_s: int, vt_e: int = OPEN_END, disc: str = "", *,
                    carve: bool = False) -> None:
        self._interval_ok(vt_s, vt_e)
        key = (src, dst, rel, disc)
        old = self.edges.get(key)
        if old is None:
            self.incident.setdefault(src, []).append(key)
            if dst != src:
                self.incident.setdefault(dst, []).append(key)
            old = []
        kept = self._carve(old, vt_s, vt_e, carve, f"edge {key}")
        kept.append([vt_s, vt_e, props])
        kept.sort(key=_start)
        self.edges[key] = kept
        self._dirty_edges.append(key)
        self._emit({"op": "assert_edge", "src": src, "dst": dst,
                    "rel_type": rel, "props": props, "vt_s": vt_s,
                    "vt_e": vt_e, "disc": disc, "source": self.source,
                    "provenance_ref": None})

    # -- retract ----------------------------------------------------------

    @staticmethod
    def _truncate(versions: list[list[Any]], t: int, what: str) -> None:
        for i, v in enumerate(versions):
            if v[0] <= t < v[1]:
                if v[0] < t:
                    versions[i] = [v[0], t, *v[2:]]
                else:
                    del versions[i]
                return
        raise AssertionError(f"retract of {what} at {t}: no version valid then")

    def retract_node(self, uid: str, t: int) -> None:
        self._truncate(self.nodes.get(uid, []), t, f"node {uid}")
        self._dirty_nodes.append(uid)
        self._emit({"op": "retract", "ref": {"kind": "node", "uid": uid},
                    "t": t, "source": self.source, "provenance_ref": None})

    def retract_edge(self, key: EdgeKey, t: int) -> None:
        self._truncate(self.edges.get(key, []), t, f"edge {key}")
        self._dirty_edges.append(key)
        self._emit({"op": "retract",
                    "ref": {"kind": "edge", "src": key[0], "dst": key[1],
                            "rel_type": key[2], "disc": key[3]},
                    "t": t, "source": self.source, "provenance_ref": None})

    def delete_edge(self, key: EdgeKey, t: int) -> None:
        """Stop believing the edge anywhere at or after `t`.

        A retract only touches the version valid at its `t`, so a version
        starting at or after `t` is removed by its own retract at its start."""
        for s, e in [(v[0], v[1]) for v in self.edges.get(key, [])]:
            if e > t:
                self.retract_edge(key, t if s < t else s)

    def delete_node(self, uid: str, t: int) -> None:
        """Stop believing the node and every incident edge from `t` on;
        the edges go first, so the batch never holds a dangling edge."""
        for key in list(self.incident.get(uid, ())):
            self.delete_edge(key, t)
        for s, e in [(v[0], v[1]) for v in self.nodes.get(uid, [])]:
            if e > t:
                self.retract_node(uid, t if s < t else s)

    # -- correct ----------------------------------------------------------

    def correct_node(self, uid: str, i: int, props: dict[str, Any]) -> None:
        """Replace the props of version `i` (the whole dict), keeping its vt."""
        v = self.nodes[uid][i]
        v[3] = props
        self._emit({"op": "correct", "ref": {"kind": "node", "uid": uid},
                    "props": props, "vt_s": v[0], "vt_e": v[1],
                    "source": self.source, "provenance_ref": None})

    def correct_edge(self, key: EdgeKey, i: int, props: dict[str, Any]) -> None:
        v = self.edges[key][i]
        v[2] = props
        self._emit({"op": "correct",
                    "ref": {"kind": "edge", "src": key[0], "dst": key[1],
                            "rel_type": key[2], "disc": key[3]},
                    "props": props, "vt_s": v[0], "vt_e": v[1],
                    "source": self.source, "provenance_ref": None})

    # -- reads ------------------------------------------------------------

    def node_index_at(self, uid: str, t: int) -> int:
        for i, v in enumerate(self.nodes.get(uid, ())):
            if v[0] <= t < v[1]:
                return i
        return -1

    def node_at(self, uid: str, t: int) -> list[Any] | None:
        i = self.node_index_at(uid, t)
        return None if i < 0 else self.nodes[uid][i]

    def alive(self, uid: str, t: int) -> bool:
        return self.node_index_at(uid, t) >= 0

    def edge_index_at(self, key: EdgeKey, t: int) -> int:
        for i, v in enumerate(self.edges.get(key, ())):
            if v[0] <= t < v[1]:
                return i
        return -1

    # -- invariant --------------------------------------------------------

    def _covered(self, uid: str, s: int, e: int) -> bool:
        cur_s = cur_e = -1
        for v in self.nodes.get(uid, ()):
            if v[0] == cur_e:
                cur_e = v[1]
            else:
                cur_s, cur_e = v[0], v[1]
            if cur_s <= s and e <= cur_e:
                return True
        return False

    def _check(self, nodes: Iterable[str], edges: Iterable[EdgeKey]) -> None:
        keys = dict.fromkeys(edges)
        for uid in nodes:
            for key in self.incident.get(uid, ()):
                keys[key] = None
        for key in keys:
            for v in self.edges[key]:
                for end in (key[0], key[1]):
                    if not self._covered(end, v[0], v[1]):
                        raise AssertionError(
                            f"edge {key} valid over [{v[0]}, {v[1]}) is not "
                            f"inside the valid time of endpoint {end}")

    def check_all(self) -> None:
        self._check((), self.edges)


def _apply_corrections(led: _Ledger, rng: random.Random,
                       candidates: list[tuple[str, str]], pct: float,
                       mutate: Callable[[random.Random, str, Any], Any],
                       n_batches: int = 2) -> None:
    """Correct one numeric prop of `pct` percent of the believed versions of
    the candidate nodes, in `n_batches` batches. `candidates` is
    `(uid, prop name)` in a fixed order; every version of each uid that
    carries the prop is one slot."""
    slots = [(uid, i, field) for uid, field in candidates
             for i, v in enumerate(led.nodes.get(uid, ())) if field in v[3]]
    n = round(len(slots) * pct / 100.0)
    if n <= 0:
        return
    chosen = sorted(rng.sample(range(len(slots)), n))
    per = -(-n // n_batches)
    for j, k in enumerate(chosen):
        uid, i, field = slots[k]
        props = dict(led.nodes[uid][i][3])
        props[field] = mutate(rng, field, props[field])
        led.correct_node(uid, i, props)
        if (j + 1) % per == 0:
            led.commit()
    led.commit()


def _after(rng: random.Random, t: int, mean: int, hi: int) -> int:
    """A time strictly after `t`: an exponential delay of `mean`, kept below
    `hi` while there is room (otherwise just after `t`)."""
    d = 1 + int(rng.expovariate(1.0 / mean))
    if t + d < hi:
        return t + d
    return t + 1 if t + 1 >= hi else rng.randrange(t + 1, hi)


def _check_args(scale: int, corrections_pct: float) -> None:
    if scale < 1:
        raise ValueError(f"scale must be >= 1, got {scale}")
    if not 0.0 <= corrections_pct <= 100.0:
        raise ValueError(f"corrections_pct must be in [0, 100], got {corrections_pct}")


# --------------------------------------------------------------------------
# vocabulary
# --------------------------------------------------------------------------

REGIONS = ("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")
#: (nation, region key), in nation-key order
NATIONS = (
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1),
    ("EGYPT", 4), ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3),
    ("INDIA", 2), ("INDONESIA", 2), ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2),
    ("JORDAN", 4), ("KENYA", 0), ("MOROCCO", 0), ("MOZAMBIQUE", 0),
    ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3), ("SAUDI ARABIA", 4),
    ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
)
SEGMENTS = ("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
PART_WORDS = ("almond", "antique", "aquamarine", "azure", "beige", "bisque",
              "black", "blanched", "blue", "blush", "brown", "burlywood",
              "chartreuse", "chiffon", "chocolate", "coral", "cornflower",
              "cream", "cyan", "dark", "deep", "dim", "dodger", "drab",
              "firebrick", "floral", "forest", "frosted", "gainsboro",
              "ghost", "goldenrod", "green", "grey", "honeydew", "hot")


def _retailprice(pk: int) -> int:
    """TPC-H's P_RETAILPRICE formula, in cents."""
    return 90000 + (pk // 10) % 20001 + 100 * (pk % 1000)


def _phone(rng: random.Random, nation: int) -> str:
    return (f"{10 + nation}-{rng.randint(100, 999)}-{rng.randint(100, 999)}-"
            f"{rng.randint(1000, 9999)}")


def _mutate(rng: random.Random, field: str, old: Any) -> Any:
    return old + rng.randint(1, 10_000)


# --------------------------------------------------------------------------
# the generator
# --------------------------------------------------------------------------

def _generate(scale: int, seed: int,
              corrections_pct: float) -> tuple[_Ledger, dict[str, list[str]]]:
    _check_args(scale, corrections_pct)
    rng = random.Random(seed)
    led = _Ledger(SOURCE, NOW)

    pend_nodes: list[tuple[str, str, dict[str, Any], int, int]] = []
    pend_edges: list[tuple[str, str, str, dict[str, Any], int, int]] = []
    direct = [False]

    def node(uid: str, label: str, props: dict[str, Any], vt_s: int,
             vt_e: int = OPEN_END) -> None:
        if direct[0]:
            led.assert_node(uid, label, props, vt_s, vt_e)
        else:
            pend_nodes.append((uid, label, props, vt_s, vt_e))

    def edge(src: str, dst: str, rel: str, vt_s: int, vt_e: int = OPEN_END,
             props: dict[str, Any] | None = None) -> None:
        p = props if props is not None else {}
        if direct[0]:
            led.assert_edge(src, dst, rel, p, vt_s, vt_e)
        else:
            pend_edges.append((src, dst, rel, p, vt_s, vt_e))

    # ---- static: regions, nations, suppliers ---------------------------------
    for rk, name in enumerate(REGIONS):
        node(f"Region:{rk}", "Region", {"name": name}, 0)
    for nk, (name, rk) in enumerate(NATIONS):
        node(f"Nation:{nk}", "Nation", {"name": name}, 0)
        edge(f"Nation:{nk}", f"Region:{rk}", "IN_REGION", 0)
    n_supp = max(5, scale // 10)
    suppliers = [f"Supplier:{k}" for k in range(1, n_supp + 1)]
    for k, u in enumerate(suppliers, start=1):
        nk = rng.randrange(len(NATIONS))
        node(u, "Supplier", {"name": f"Supplier#{k:09d}",
                             "acctbal": rng.randint(-99_999, 999_999),
                             "phone": _phone(rng, nk)}, 0)
        edge(u, f"Nation:{nk}", "IN_NATION", 0)

    # ---- parts and PARTSUPP ----------------------------------------------------
    n_part = max(60, scale)
    parts = [f"Part:{k}" for k in range(1, n_part + 1)]
    part_of: list[list[int]] = [[]]          # pk -> supplier keys (1-based)
    supplies: list[EdgeKey] = []
    part55_supplies: list[EdgeKey] = []
    for pk, u in enumerate(parts, start=1):
        avail = rng.randrange(DATA_START, DATA_START + 90 * DAY)
        node(u, "Part", {"name": " ".join(rng.sample(PART_WORDS, 3)),
                         "brand": f"Brand#{rng.randint(1, 5)}{rng.randint(1, 5)}",
                         "size": rng.randint(1, 50),
                         "retailprice": _retailprice(pk)}, avail)
        sks = sorted(rng.sample(range(1, n_supp + 1), 4))
        part_of.append(sks)
        for sk in sks:
            # one to three consecutive validity periods, the last open-ended
            n_periods = 3 if u == PART55 else rng.randint(1, 3)
            starts = [avail] + sorted(rng.sample(range(avail + DAY, LOAD_END),
                                                 n_periods - 1))
            for j, s in enumerate(starts):
                e = starts[j + 1] if j + 1 < len(starts) else OPEN_END
                edge(f"Supplier:{sk}", u, "SUPPLIES", s, e,
                     {"availqty": rng.randint(1, 9999),
                      "supplycost": rng.randint(100, 100_000)})
            key = (f"Supplier:{sk}", u, "SUPPLIES", "")
            supplies.append(key)
            if u == PART55:
                part55_supplies.append(key)

    # ---- customers and orders ----------------------------------------------------
    customers = [f"Customer:{k}" for k in range(1, scale + 1)]
    C_since: list[int] = []
    for k, u in enumerate(customers, start=1):
        since = rng.randrange(DATA_START, DATA_START + 180 * DAY)
        nk = rng.randrange(len(NATIONS))
        node(u, "Customer", {"name": f"Customer#{k:09d}",
                             "acctbal": rng.randint(-99_999, 999_999),
                             "mktsegment": rng.choice(SEGMENTS)}, since)
        edge(u, f"Nation:{nk}", "IN_NATION", since)
        C_since.append(since)

    orders: list[str] = []
    O_lines: list[list[str]] = []
    lineitems: list[str] = []

    def add_order(c: int, odate: int, status: str) -> None:
        okey = len(orders) + 1
        ou = f"Orders:{okey}"
        specs = []
        total = 0
        for ln in range(1, rng.randint(1, 7) + 1):
            pk = rng.randint(1, n_part)
            sk = rng.choice(part_of[pk])
            qty = rng.randint(1, 50)
            ext = qty * _retailprice(pk)
            discount = rng.randint(0, 10)
            line_status = status if status != "P" else rng.choice("OF")
            end = (odate + rng.randint(1, 90) * DAY if line_status == "F"
                   else OPEN_END)
            total += ext * (100 - discount) // 100
            specs.append((ln, pk, sk, end, {"linenumber": ln, "quantity": qty,
                                            "extendedprice": ext,
                                            "discount": discount,
                                            "linestatus": line_status}))
        rec_s = odate + rng.randint(1, 30) * DAY
        node(ou, "Orders", {"orderstatus": status, "totalprice": total,
                            "receivable_s": rec_s,
                            "receivable_e": rec_s + 30 * DAY}, odate)
        edge(ou, customers[c], "PLACED_BY", odate)
        lines = []
        for ln, pk, sk, end, props in specs:
            lu = f"Lineitem:{okey}-{ln}"
            node(lu, "Lineitem", props, odate, end)
            edge(lu, ou, "IN_ORDER", odate, end)
            edge(lu, f"Part:{pk}", "OF_PART", odate, end)
            edge(lu, f"Supplier:{sk}", "FROM_SUPPLIER", odate, end)
            lines.append(lu)
            lineitems.append(lu)
        orders.append(ou)
        O_lines.append(lines)

    first_order = DATA_START + 90 * DAY      # every part is available by then
    for c in range(scale):
        for _ in range(rng.randint(2, 8)):
            odate = rng.randrange(max(C_since[c], first_order) + HOUR,
                                  LOAD_END - 30 * DAY)
            r = rng.random()
            if odate < LOAD_END - 180 * DAY:
                status = "F" if r < 0.8 else ("P" if r < 0.9 else "O")
            else:
                status = "O" if r < 0.7 else ("P" if r < 0.9 else "F")
            add_order(c, odate, status)

    # ---- 1. bulk load ----------------------------------------------------------
    for uid, label, props, vt_s, vt_e in pend_nodes:
        led.assert_node(uid, label, props, vt_s, vt_e)
    for src, dst, rel, props, vt_s, vt_e in pend_edges:
        led.assert_edge(src, dst, rel, props, vt_s, vt_e)
    led.commit()
    pend_nodes.clear()
    pend_edges.clear()
    direct[0] = True

    # ---- 2. scenarios ----------------------------------------------------------
    m = max(2, scale // 20)

    def pick_order(t: int, want: Callable[[list[Any]], bool]) -> int:
        for _ in range(20):
            o = rng.randrange(len(orders))
            v = led.node_at(orders[o], t)
            if v is not None and want(v):
                return o
        return -1

    def new_order(lo: int, hi: int) -> None:
        for _ in range(m):
            add_order(rng.randrange(scale), rng.randrange(lo, hi), "O")

    def cancel_order(lo: int, hi: int) -> None:
        for _ in range(m):
            t = rng.randrange(lo, hi)
            o = pick_order(t, lambda v: True)
            if o < 0:
                continue
            for lu in O_lines[o]:
                led.delete_node(lu, t)
            led.delete_node(orders[o], t)

    def deliver(lo: int, hi: int) -> None:
        for _ in range(m):
            t = rng.randrange(lo, hi)
            o = pick_order(t, lambda v: v[3]["orderstatus"] == "O")
            if o < 0:
                continue
            props = dict(led.node_at(orders[o], t)[3])
            props["orderstatus"] = "F"
            props["receivable_s"] = t
            props["receivable_e"] = t + 30 * DAY
            led.assert_node(orders[o], "Orders", props, t, OPEN_END, carve=True)

    def correct_node_at(uid: str, t: int, field: str, delta: int) -> None:
        i = led.node_index_at(uid, t)
        if i < 0:
            return
        props = dict(led.nodes[uid][i][3])
        props[field] = props[field] + delta
        led.correct_node(uid, i, props)

    def receive_payment(lo: int, hi: int) -> None:
        for _ in range(m):
            correct_node_at(customers[rng.randrange(scale)], rng.randrange(lo, hi),
                            "acctbal", rng.randint(100, 500_000))

    def some_supplies() -> list[EdgeKey]:
        return _distinct(part55_supplies
                         + [supplies[rng.randrange(len(supplies))]
                            for _ in range(m)])

    def update_stock(lo: int, hi: int) -> None:
        for key in some_supplies():
            i = led.edge_index_at(key, rng.randrange(lo, hi))
            if i < 0:
                continue
            props = dict(led.edges[key][i][2])
            props["availqty"] = max(0, props["availqty"] + rng.randint(-500, 500))
            led.correct_edge(key, i, props)

    def change_price(lo: int, hi: int) -> None:
        for key in some_supplies():
            t = rng.randrange(lo, hi)
            i = led.edge_index_at(key, t)
            if i < 0:
                continue
            old = led.edges[key][i][2]
            props = {"availqty": old["availqty"],
                     "supplycost": max(1, old["supplycost"]
                                       + rng.randint(-2_000, 2_000))}
            led.assert_edge(key[0], key[1], key[2], props, t, OPEN_END, key[3],
                            carve=True)

    def update_supplier(lo: int, hi: int) -> None:
        for _ in range(m):
            sk = rng.randint(1, n_supp)
            uid = f"Supplier:{sk}"
            i = led.node_index_at(uid, rng.randrange(lo, hi))
            if i < 0:
                continue
            props = dict(led.nodes[uid][i][3])
            props["acctbal"] = props["acctbal"] + rng.randint(-50_000, 50_000)
            props["phone"] = _phone(rng, rng.randrange(len(NATIONS)))
            led.correct_node(uid, i, props)

    def manipulate_order(lo: int, hi: int) -> None:
        for _ in range(m):
            uid = orders[rng.randrange(len(orders))]
            versions = led.nodes.get(uid, [])
            if not versions:
                continue
            i = rng.randrange(len(versions))
            props = dict(versions[i][3])
            props["totalprice"] = max(0, props["totalprice"]
                                      + rng.randint(-10_000, 10_000))
            led.correct_node(uid, i, props)

    scenarios = (new_order, cancel_order, deliver, receive_payment,
                 update_stock, change_price, update_supplier, manipulate_order)
    n_scen = 3 * len(scenarios)
    width = (SCENARIO_END - LOAD_END) // n_scen
    for b in range(n_scen):
        lo = LOAD_END + b * width
        scenarios[b % len(scenarios)](lo, lo + width)
        led.commit()

    # ---- 3. corrections ------------------------------------------------------
    candidates = ([(u, "acctbal") for u in customers]
                  + [(u, "totalprice") for u in orders]
                  + [(u, "extendedprice") for u in lineitems]
                  + [(u, "retailprice") for u in parts]
                  + [(u, "acctbal") for u in suppliers])
    _apply_corrections(led, rng, candidates, corrections_pct, _mutate)
    led.commit()
    led.check_all()

    meta = {"customers": customers, "orders": orders, "parts": parts,
            "suppliers": suppliers}
    return led, meta


# --------------------------------------------------------------------------
# public API
# --------------------------------------------------------------------------

def batches(scale: int, seed: int = 0,
            corrections_pct: float = 0.0) -> list[tuple[int, list[Op]]]:
    """Every write as `(tt, ops)` in commit order, tt = 1, 2, ...

    Deterministic in its arguments. The corrections are drawn last, so the
    batches before them do not depend on `corrections_pct`."""
    led, _ = _generate(scale, seed, corrections_pct)
    return led.batches


def names(scale: int, seed: int = 0) -> dict[str, list[str]]:
    """Parameter candidates, all alive at `NOW`, in key order (`Part:55` is
    always among the parts)."""
    led, meta = _generate(scale, seed, 0.0)
    return {key: [u for u in meta[key] if led.alive(u, NOW)]
            for key in ("customers", "orders", "parts", "suppliers")}


def build(out: Path, scale: int, seed: int = 0, corrections_pct: float = 0.0,
          backend: str = "native") -> None:
    """Write the store at `out`, replacing whatever is there."""
    import gnomon

    out = Path(out)
    plan = batches(scale, seed, corrections_pct)
    if out.exists():
        shutil.rmtree(out)
    store = gnomon.open(out, backend=backend)
    try:
        for _tt, ops in plan:
            # one Store write per batch: the store stamps its own tt
            store._write(ops)          # noqa: SLF001 - a builder is a writer
    finally:
        store.close()


__all__ = ["NOW", "OPEN_END", "PART55", "batches", "build", "names"]
