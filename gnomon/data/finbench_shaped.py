"""LDBC FinBench-shaped synthetic bi-temporal graph generator.

**This is not LDBC data**, not produced by the LDBC FinBench data generator,
and not an LDBC benchmark workload. It is a small deterministic generator with
the *shape* of the FinBench schema: Person, Company, Account, Loan and Medium
nodes (uid `"<Type>:<id>"`) and the OWN, INVEST, APPLY, GUARANTEE, TRANSFER,
WITHDRAW, DEPOSIT, REPAY and SIGN_IN relationships, with simple stand-in
distributions (Pareto-weighted account activity, repeated pairs, planted
transfer 3-cycles).

Multiplicity: TRANSFER, WITHDRAW, DEPOSIT, REPAY and SIGN_IN keep one edge per
occurrence. Each occurrence's `disc` is its ordinal, in time order, among the
occurrences of the same `(src, dst, rel_type)` (`"0"`, `"1"`, ...); every
other relationship uses `disc = ""`.

Valid time, in microseconds within 2022 UTC: a node is valid from its
creation time, a Loan from its APPLY time, an event edge from its timestamp,
all open-ended until a delete. Creation times are never copied into props.

The write stream, in commit order (`tt` = 1, 2, ...):

1. initial load: every node, then every edge (events in time order), split
   into batches of at most `LOAD_CHUNK` ops;
2. two block batches: an `assert_node` of an Account with `isBlocked = true`
   over `[t, OPEN_END)`, which carves the earlier version to `[created, t)`;
3. two delete batches: `retract` at `t` of an Account, every incident edge,
   and every Loan connected to it by DEPOSIT or REPAY (with the Loan's own
   incident edges);
4. when `corrections_pct > 0`, two batches of `correct` ops that change the
   `balance` of that percentage of Loan versions, keeping their valid time.

Size: `scale` is the number of accounts; about 75 transfers per account and
roughly 92 version rows per account in total (about 9k rows at scale 100,
0.9M at scale 10,000).
"""

from __future__ import annotations

import calendar
import random
import shutil
from itertools import accumulate
from pathlib import Path
from typing import Any, Callable, Iterable

Op = dict[str, Any]
EdgeKey = tuple[str, str, str, str]

OPEN_END = 2 ** 62
HOUR = 3_600_000_000
DAY = 86_400_000_000
#: Largest number of ops in one write batch; the initial load is split into
#: chunks of this size (nodes first, so every edge lands after its endpoints).
LOAD_CHUNK = 100_000
SOURCE = "finbench_shaped"


def _ts(y: int, m: int, d: int) -> int:
    return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0)) * 1_000_000


YEAR_START = _ts(2022, 1, 1)
PARTIES_END = _ts(2022, 2, 1)       # persons, companies, media created before
ACCOUNTS_END = _ts(2022, 4, 1)      # accounts, loans, OWN/INVEST/GUARANTEE/APPLY
EVENTS_END = _ts(2022, 10, 1)       # TRANSFER/WITHDRAW/DEPOSIT/REPAY/SIGN_IN
BLOCKS_END = _ts(2022, 12, 1)
DELETES_END = _ts(2022, 12, 31)
#: The valid instant "now" reads use: after every generated valid-time start.
NOW = _ts(2023, 1, 1)

MULTI_RELS = ("TRANSFER", "WITHDRAW", "DEPOSIT", "REPAY", "SIGN_IN")


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

FIRST_NAMES = ("Anna", "Ben", "Chen", "Dara", "Eli", "Fatima", "Goran", "Hana",
               "Ivan", "Jun", "Karim", "Lena", "Mateo", "Nadia", "Omar",
               "Priya", "Quinn", "Rosa", "Sven", "Tariq", "Uma", "Viktor",
               "Wei", "Yara", "Zoltan")
LAST_NAMES = ("Abara", "Becker", "Costa", "Dube", "Eriksen", "Fischer",
              "Garcia", "Hoang", "Ito", "Jensen", "Kowalski", "Lopez",
              "Mbeki", "Nguyen", "Okafor", "Petrov", "Rossi", "Silva",
              "Tanaka", "Urban", "Vargas", "Wong", "Yilmaz", "Zubiri")
COMPANY_SUFFIX = ("Holdings", "Trading", "Logistics", "Capital", "Retail",
                  "Energy", "Software")
ACCOUNT_TYPES = ("debit", "credit", "savings", "brokerage", "escrow",
                 "merchant")
MEDIUM_TYPES = ("POS", "ATM", "PHONE", "PAD", "WEB")


def _money(rng: random.Random, mu: float, sigma: float) -> float:
    return round(max(0.01, rng.lognormvariate(mu, sigma)), 2)


def _mutate(rng: random.Random, field: str, old: Any) -> Any:
    return round(old * rng.uniform(0.5, 0.99), 2)


# --------------------------------------------------------------------------
# the generator
# --------------------------------------------------------------------------

def _generate(scale: int, seed: int,
              corrections_pct: float) -> tuple[_Ledger, dict[str, list[str]]]:
    _check_args(scale, corrections_pct)
    if scale < 3:
        raise ValueError(f"scale (accounts) must be >= 3, got {scale}")
    rng = random.Random(seed)
    led = _Ledger(SOURCE, NOW)

    pend_nodes: list[tuple[str, str, dict[str, Any], int]] = []
    pend_edges: list[tuple[str, str, str, dict[str, Any], int, str]] = []

    # ---- parties and media -------------------------------------------------
    n_acc = scale
    persons = [f"Person:{k}" for k in range(1, max(2, scale // 3) + 1)]
    companies = [f"Company:{k}" for k in range(1, max(2, scale // 6) + 1)]
    media = [f"Medium:{k}" for k in range(1, max(2, scale // 5) + 1)]
    for u in persons:
        pend_nodes.append((u, "Person", {
            "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "isBlocked": rng.random() < 0.02,
        }, rng.randrange(YEAR_START, PARTIES_END)))
    for k, u in enumerate(companies):
        pend_nodes.append((u, "Company", {
            "name": f"{rng.choice(LAST_NAMES)} "
                    f"{COMPANY_SUFFIX[k % len(COMPANY_SUFFIX)]} {k + 1}",
            "isBlocked": rng.random() < 0.02,
        }, rng.randrange(YEAR_START, PARTIES_END)))
    for u in media:
        pend_nodes.append((u, "Medium", {
            "isBlocked": rng.random() < 0.02,
            "mediumType": rng.choice(MEDIUM_TYPES),
        }, rng.randrange(YEAR_START, PARTIES_END)))

    def party() -> str:
        if rng.random() < 0.6:
            return persons[rng.randrange(len(persons))]
        return companies[rng.randrange(len(companies))]

    # ---- accounts and ownership ---------------------------------------------
    accounts = [f"Account:{k}" for k in range(1, n_acc + 1)]
    for u in accounts:
        ct = rng.randrange(PARTIES_END, ACCOUNTS_END)
        pend_nodes.append((u, "Account", {
            "isBlocked": False, "accountType": rng.choice(ACCOUNT_TYPES),
        }, ct))
        pend_edges.append((party(), u, "OWN", {}, ct, ""))

    # ---- loans ----------------------------------------------------------------
    loans = [f"Loan:{k}" for k in range(1, max(2, scale // 4) + 1)]
    for u in loans:
        t = rng.randrange(PARTIES_END, ACCOUNTS_END)
        amount = round(rng.uniform(10_000, 1_000_000), 2)
        pend_nodes.append((u, "Loan", {
            "loanAmount": amount,
            "balance": round(amount * rng.uniform(0.1, 1.0), 2),
        }, t))
        pend_edges.append((party(), u, "APPLY", {}, t, ""))

    # ---- INVEST and GUARANTEE -------------------------------------------------
    seen: set[tuple[str, str, str]] = set()

    def once(src: str, dst: str, rel: str, props: dict[str, Any]) -> None:
        if src == dst or (src, dst, rel) in seen:
            return
        seen.add((src, dst, rel))
        pend_edges.append((src, dst, rel, props,
                           rng.randrange(PARTIES_END, ACCOUNTS_END), ""))

    for c in companies:
        for _ in range(rng.randint(0, 3)):
            once(party(), c, "INVEST", {"ratio": round(rng.uniform(0.01, 0.6), 4)})
    for _ in range(len(persons) // 4):
        once(rng.choice(persons), rng.choice(persons), "GUARANTEE", {})
    for _ in range(max(1, len(companies) // 4)):
        once(rng.choice(companies), rng.choice(companies), "GUARANTEE", {})

    # ---- events -----------------------------------------------------------------
    events: list[tuple[int, int, str, str, str, dict[str, Any]]] = []

    def event(t: int, rel: str, src: str, dst: str, props: dict[str, Any]) -> None:
        events.append((t, len(events), rel, src, dst, props))

    acc_cw = list(accumulate(rng.paretovariate(1.2) for _ in range(n_acc)))

    def busy_account() -> int:
        return rng.choices(range(n_acc), cum_weights=acc_cw)[0]

    def pooled(count: int, draw: Callable[[], tuple[str, str]]) -> list[tuple[str, str]]:
        """`count` pairs, half of them repeats of an earlier pair (multi-edges)."""
        pool: list[tuple[str, str]] = []
        out: list[tuple[str, str]] = []
        for _ in range(count):
            if pool and rng.random() < 0.5:
                out.append(pool[rng.randrange(len(pool))])
            else:
                pair = draw()
                pool.append(pair)
                out.append(pair)
        return out

    def account_pair() -> tuple[str, str]:
        a = busy_account()
        b = busy_account()
        if a == b:
            b = (a + 1 + rng.randrange(n_acc - 1)) % n_acc
        return accounts[a], accounts[b]

    for src, dst in pooled(75 * n_acc, account_pair):
        event(rng.randrange(ACCOUNTS_END, EVENTS_END), "TRANSFER", src, dst,
              {"amount": _money(rng, 6.0, 1.5)})
    planted: list[tuple[str, str, str]] = []
    for _ in range(max(1, n_acc // 20)):
        # a planted 3-cycle a -> b -> c -> a inside one day
        a, b, c = (accounts[i] for i in rng.sample(range(n_acc), 3))
        planted.append((a, b, c))
        t1 = rng.randrange(ACCOUNTS_END, EVENTS_END - DAY)
        t2 = t1 + rng.randint(1, HOUR)
        t3 = t2 + rng.randint(1, HOUR)
        amount = _money(rng, 8.0, 1.0)
        event(t1, "TRANSFER", a, b, {"amount": amount})
        event(t2, "TRANSFER", b, c, {"amount": round(amount * 0.97, 2)})
        event(t3, "TRANSFER", c, a, {"amount": round(amount * 0.94, 2)})
    for src, dst in pooled(8 * n_acc, account_pair):
        event(rng.randrange(ACCOUNTS_END, EVENTS_END), "WITHDRAW", src, dst,
              {"amount": _money(rng, 5.0, 1.2)})
    for src, dst in pooled(4 * n_acc, lambda: (media[rng.randrange(len(media))],
                                               accounts[busy_account()])):
        event(rng.randrange(ACCOUNTS_END, EVENTS_END), "SIGN_IN", src, dst, {})
    for loan in loans:
        for acc in _distinct(rng.randrange(n_acc) for _ in range(rng.randint(1, 2))):
            for _ in range(rng.randint(1, 3)):
                event(rng.randrange(ACCOUNTS_END, EVENTS_END), "DEPOSIT", loan,
                      accounts[acc], {"amount": _money(rng, 9.0, 1.0)})
        for acc in _distinct(rng.randrange(n_acc) for _ in range(rng.randint(1, 2))):
            for _ in range(rng.randint(1, 4)):
                event(rng.randrange(ACCOUNTS_END, EVENTS_END), "REPAY",
                      accounts[acc], loan, {"amount": _money(rng, 7.0, 1.0)})

    events.sort(key=lambda e: (e[0], e[1]))
    ordinal: dict[tuple[str, str, str], int] = {}
    transfers: dict[str, int] = {}
    for t, _seq, rel, src, dst, props in events:
        k = ordinal.get((src, dst, rel), 0)
        ordinal[(src, dst, rel)] = k + 1
        pend_edges.append((src, dst, rel, props, t, str(k)))
        if rel == "TRANSFER":
            transfers[src] = transfers.get(src, 0) + 1
            transfers[dst] = transfers.get(dst, 0) + 1

    # ---- 1. initial load -----------------------------------------------------
    for uid, label, props, vt_s in pend_nodes:
        led.assert_node(uid, label, props, vt_s)
    for src, dst, rel, props, vt_s, disc in pend_edges:
        led.assert_edge(src, dst, rel, props, vt_s, OPEN_END, disc)
    led.commit()
    pend_nodes.clear()
    pend_edges.clear()
    events.clear()

    # ---- 2. blocks: a new version from t on carves the old one ---------------
    n_blk = 2
    width = (BLOCKS_END - EVENTS_END) // n_blk
    for b in range(n_blk):
        lo, hi = EVENTS_END + b * width, EVENTS_END + (b + 1) * width
        for _ in range(max(1, scale // 50)):
            t = rng.randrange(lo, hi)
            u = accounts[rng.randrange(n_acc)]
            v = led.node_at(u, t)
            if v is None or v[3].get("isBlocked"):
                continue
            props = dict(v[3])
            props["isBlocked"] = True
            led.assert_node(u, "Account", props, t, OPEN_END, carve=True)
        led.commit()

    # ---- 3. deletes: the account, its edges, and its loans -------------------
    n_del = 2
    width = (DELETES_END - BLOCKS_END) // n_del
    for b in range(n_del):
        lo, hi = BLOCKS_END + b * width, BLOCKS_END + (b + 1) * width
        for _ in range(max(1, scale // 100)):
            t = rng.randrange(lo, hi)
            u = accounts[rng.randrange(n_acc)]
            if not led.alive(u, t):
                continue
            linked = _distinct(key[0] if key[2] == "DEPOSIT" else key[1]
                               for key in led.incident.get(u, ())
                               if key[2] in ("DEPOSIT", "REPAY"))
            for loan in linked:
                led.delete_node(loan, t)
            led.delete_node(u, t)
        led.commit()

    # ---- 4. corrections ------------------------------------------------------
    _apply_corrections(led, rng, [(u, "balance") for u in loans],
                       corrections_pct, _mutate)
    led.commit()
    led.check_all()

    busiest = sorted(range(n_acc), key=lambda i: (-transfers.get(accounts[i], 0), i))
    meta = {
        "accounts": [accounts[i] for i in busiest],
        "persons": persons,
        "companies": companies,
        "cycles": planted,
    }
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
    """Parameter candidates, all alive at `NOW`: accounts with the most
    transfers first, then persons and companies in id order, and the planted
    transfer cycles (`cycle_src[i] -> cycle_dst[i] -> ... -> cycle_src[i]`)."""
    led, meta = _generate(scale, seed, 0.0)
    out = {key: [u for u in meta[key] if led.alive(u, NOW)]
           for key in ("accounts", "persons", "companies")}
    # the planted transfer 3-cycles whose accounts are all alive at NOW, as
    # their first two accounts (a cycle query's source and destination)
    alive = [cy for cy in meta["cycles"] if all(led.alive(u, NOW) for u in cy)]
    out["cycle_src"] = [cy[0] for cy in alive]
    out["cycle_dst"] = [cy[1] for cy in alive]
    return out


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


__all__ = ["MULTI_RELS", "NOW", "OPEN_END", "batches", "build", "names"]
