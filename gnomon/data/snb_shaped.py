"""LDBC SNB-shaped synthetic bi-temporal graph generator.

**This is not LDBC data**, not produced by the LDBC data generator, and not an
LDBC benchmark workload. It is a small deterministic generator with the
*shape* of the LDBC Social Network Benchmark schema: its labels, relationship
types and directions, and the uid encoding of an LDBC loader
(`uid = id * 8 + hierarchy tag`, `snb_uid` below). The distributions are simple stand-ins
(Pareto-weighted friendship degree, Zipf tag popularity, exponential delays);
nothing about LDBC's correlations is reproduced.

Two deliberate differences from a loader of real LDBC data:

* `KNOWS` is stored **once** per friendship, with `src` id < `dst` id.
* Valid time closes: deletes are written as `retract` ops at a deletion date,
  cascading the way LDBC's delete operations do (a deleted Post takes its
  reply tree and every incident edge with it).

Valid time: dynamic entities start at their creation date (microseconds since
the epoch, 2010-01-01 .. 2012-12-31 UTC); static entities (Tag, TagClass,
places, organisations) start at 0. Creation dates are never copied into props.
Every edge starts at or after both endpoints' starts.

The write stream, in commit order (`tt` = 1, 2, ...):

1. initial load: every node, then every edge, split into batches of at most
   `LOAD_CHUNK` ops;
2. three insert batches (new persons, friendships, posts, comments, likes);
3. three delete batches (posts with their reply trees, comments with their
   subtrees, friendships);
4. when `corrections_pct > 0`, two batches of `correct` ops that change a
   numeric property (`birthday`, `length`) of that percentage of node
   versions, keeping each version's valid interval.

Size: `scale` is the number of persons; about 20 messages per person and
roughly 120 version rows per person in total (about 12k rows at scale 100,
1.2M at scale 10,000).
"""

from __future__ import annotations

import calendar
import random
import shutil
from itertools import accumulate
from pathlib import Path
from typing import Any, Callable, Iterable

#: The uid encoding: an LDBC id times eight plus the tag of the id space it
#: draws from, so ids of different hierarchies never collide.
HIERARCHY_TAG: dict[str, int] = {
    "Forum": 0,
    "Message": 1,
    "Organisation": 2,
    "Person": 3,
    "Place": 4,
    "Tag": 5,
    "TagClass": 6,
}
HIERARCHY_STRIDE = 8


def snb_uid(hierarchy: str, ldbc_id: str | int) -> str:
    return str(int(ldbc_id) * HIERARCHY_STRIDE + HIERARCHY_TAG[hierarchy])



Op = dict[str, Any]
EdgeKey = tuple[str, str, str, str]

OPEN_END = 2 ** 62
HOUR = 3_600_000_000
DAY = 86_400_000_000
#: Largest number of ops in one write batch; the initial load is split into
#: chunks of this size (nodes first, so every edge lands after its endpoints).
LOAD_CHUNK = 100_000
SOURCE = "snb_shaped"


def _ts(y: int, m: int, d: int) -> int:
    return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0)) * 1_000_000


LOAD_START = _ts(2010, 1, 1)
LOAD_END = _ts(2012, 7, 1)
INSERT_END = _ts(2012, 10, 1)
DELETE_END = _ts(2012, 12, 31)
#: The valid instant "now" reads use: after every generated valid-time start.
NOW = _ts(2013, 1, 1)


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

CONTINENTS = ("Africa", "Asia", "Europe", "North_America", "Oceania",
              "South_America")
#: (country name, continent index)
COUNTRIES = (
    ("India", 1), ("China", 1), ("Germany", 2), ("United_States", 3),
    ("Brazil", 5), ("Nigeria", 0), ("France", 2), ("Japan", 1),
    ("Mexico", 3), ("Australia", 4), ("Egypt", 0), ("Spain", 2),
    ("Argentina", 5), ("Canada", 3), ("Vietnam", 1), ("Kenya", 0),
    ("Poland", 2), ("Peru", 5), ("Indonesia", 1), ("Italy", 2),
    ("New_Zealand", 4), ("Turkey", 1), ("Chile", 5), ("Ghana", 0),
    ("Sweden", 2), ("Pakistan", 1), ("Colombia", 5), ("Morocco", 0),
    ("Netherlands", 2), ("Philippines", 1),
)
TAGCLASS_BASE = (
    "Thing", "Agent", "Person", "Artist", "MusicalArtist", "Writer",
    "Athlete", "Politician", "Organisation", "Company", "Band", "SportsTeam",
    "Place", "PopulatedPlace", "Country", "City", "Work", "Album", "Film",
    "Book", "Single", "Event", "Species", "Device",
)
SYLLABLES = ("ka", "lo", "mi", "ra", "su", "te", "no", "vi", "da", "pe",
             "zu", "ho", "ri", "ma", "ge", "ba", "ty", "fe", "wa", "qi")
FIRST_NAMES = ("Anna", "Ben", "Chen", "Dara", "Eli", "Fatima", "Goran", "Hana",
               "Ivan", "Jun", "Karim", "Lena", "Mateo", "Nadia", "Omar",
               "Priya", "Quinn", "Rosa", "Sven", "Tariq", "Uma", "Viktor",
               "Wei", "Yara", "Zoltan")
LAST_NAMES = ("Abara", "Becker", "Costa", "Dube", "Eriksen", "Fischer",
              "Garcia", "Hoang", "Ito", "Jensen", "Kowalski", "Lopez",
              "Mbeki", "Nguyen", "Okafor", "Petrov", "Rossi", "Silva",
              "Tanaka", "Urban", "Vargas", "Wong", "Yilmaz", "Zubiri")
COMPANY_SUFFIX = ("Air", "Bank", "Labs", "Motors", "Media", "Foods", "Steel")
BROWSERS = ("Firefox", "Chrome", "Internet Explorer", "Safari", "Opera")
LANGUAGES = ("en", "de", "es", "fr", "zh", "pt", "ru", "ar")
WORDS = ("about", "great", "music", "game", "city", "photo", "trip", "book",
         "film", "team", "news", "today", "friend", "love", "work", "food",
         "song", "match", "history", "science", "art", "river", "night")


def _ip(rng: random.Random) -> str:
    return (f"{rng.randint(1, 223)}.{rng.randrange(256)}."
            f"{rng.randrange(256)}.{rng.randint(1, 254)}")


def _text(rng: random.Random, lo: int, hi: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(rng.randint(lo, hi)))


def _mutate(rng: random.Random, field: str, old: Any) -> Any:
    if field == "birthday":
        return old + DAY * rng.randint(1, 365)
    return old + rng.randint(1, 100)


# --------------------------------------------------------------------------
# the generator
# --------------------------------------------------------------------------

def _generate(scale: int, seed: int,
              corrections_pct: float) -> tuple[_Ledger, dict[str, list[str]]]:
    _check_args(scale, corrections_pct)
    rng = random.Random(seed)
    led = _Ledger(SOURCE, NOW)
    ids = {h: 0 for h in ("Forum", "Message", "Organisation", "Person",
                          "Place", "Tag", "TagClass")}

    def new_uid(hierarchy: str) -> tuple[int, str]:
        ids[hierarchy] += 1
        return ids[hierarchy], snb_uid(hierarchy, ids[hierarchy])

    # During the initial load nodes and edges are buffered so that every node
    # is written before any edge; afterwards they go straight to the ledger.
    pend_nodes: list[tuple[str, str, dict[str, Any], int]] = []
    pend_edges: list[tuple[str, str, str, dict[str, Any], int]] = []
    direct = [False]

    def node(uid: str, label: str, props: dict[str, Any], vt_s: int) -> None:
        if direct[0]:
            led.assert_node(uid, label, props, vt_s)
        else:
            pend_nodes.append((uid, label, props, vt_s))

    def edge(src: str, dst: str, rel: str, vt_s: int,
             props: dict[str, Any] | None = None) -> None:
        p = props if props is not None else {}
        if direct[0]:
            led.assert_edge(src, dst, rel, p, vt_s)
        else:
            pend_edges.append((src, dst, rel, p, vt_s))

    # ---- static entities: places, tag classes, tags, organisations --------
    continents: list[str] = []
    for name in CONTINENTS:
        i, u = new_uid("Place")
        node(u, "Continent", {"name": name, "id": i}, 0)
        continents.append(u)
    countries: list[str] = []
    country_names: list[str] = []
    for name, cont in COUNTRIES[:min(len(COUNTRIES), 5 + scale // 20)]:
        i, u = new_uid("Place")
        node(u, "Country", {"name": name, "id": i}, 0)
        edge(u, continents[cont], "IS_PART_OF", 0)
        countries.append(u)
        country_names.append(name)
    cities: list[str] = []
    city_country: list[int] = []
    city_names: list[str] = []
    per_country = max(2, min(40, 2 + scale // 250))
    for c, cname in enumerate(country_names):
        for k in range(per_country):
            i, u = new_uid("Place")
            name = f"{cname}_City_{k + 1}"
            node(u, "City", {"name": name, "id": i}, 0)
            edge(u, countries[c], "IS_PART_OF", 0)
            cities.append(u)
            city_country.append(c)
            city_names.append(name)

    tagclasses: list[str] = []
    tagclass_names: list[str] = []
    for k in range(min(71, 6 + scale // 100)):
        base = TAGCLASS_BASE[k % len(TAGCLASS_BASE)]
        name = base if k < len(TAGCLASS_BASE) else f"{base}_{k // len(TAGCLASS_BASE)}"
        i, u = new_uid("TagClass")
        node(u, "TagClass", {"name": name, "id": i}, 0)
        if k:
            # biased towards early classes: a shallow, bushy hierarchy
            edge(u, tagclasses[int(rng.random() ** 2 * k)], "IS_SUBCLASS_OF", 0)
        tagclasses.append(u)
        tagclass_names.append(name)

    n_tags = max(20, scale // 2)
    tags: list[str] = []
    tag_names: list[str] = []
    ns = len(SYLLABLES)
    for k in range(n_tags):
        name = f"{SYLLABLES[k % ns].capitalize()}{SYLLABLES[(k // ns) % ns]}_{k + 1}"
        i, u = new_uid("Tag")
        node(u, "Tag", {"name": name, "id": i}, 0)
        edge(u, tagclasses[rng.randrange(len(tagclasses))], "HAS_TYPE", 0)
        tags.append(u)
        tag_names.append(name)
    tag_cw = list(accumulate(1.0 / (r + 1) for r in range(n_tags)))

    def pick_tags(k: int) -> list[int]:
        return _distinct(rng.choices(range(n_tags), cum_weights=tag_cw, k=k))

    study_orgs: list[str] = []
    for k in range(max(3, scale // 50)):
        c = rng.randrange(len(cities))
        i, u = new_uid("Organisation")
        node(u, "University",
             {"name": f"University_{k + 1}_of_{city_names[c]}", "id": i}, 0)
        edge(u, cities[c], "IS_LOCATED_IN", 0)
        study_orgs.append(u)
    companies: list[str] = []
    for k in range(max(5, scale // 25)):
        c = rng.randrange(len(countries))
        i, u = new_uid("Organisation")
        name = (f"{LAST_NAMES[k % len(LAST_NAMES)]}_"
                f"{COMPANY_SUFFIX[k % len(COMPANY_SUFFIX)]}_{k + 1}")
        node(u, "Company", {"name": name, "id": i}, 0)
        edge(u, countries[c], "IS_LOCATED_IN", 0)
        companies.append(u)

    # ---- persons and their forums ------------------------------------------
    P_uid: list[str] = []
    P_ct: list[int] = []
    P_country: list[int] = []
    P_forums: list[list[tuple[int, int]]] = []   # (forum, time it may post there)
    friends: list[list[tuple[int, int]]] = []     # (friend, since)
    wall_of: list[int] = []
    F_uid: list[str] = []
    F_ct: list[int] = []
    F_mod: list[int] = []
    F_tags: list[list[int]] = []
    F_album: list[bool] = []
    groups: list[int] = []

    def new_forum(mod: int, title: str, ct: int, tag_idx: list[int],
                  album: bool = False) -> int:
        i, u = new_uid("Forum")
        node(u, "Forum", {"title": title, "id": i}, ct)
        edge(u, P_uid[mod], "HAS_MODERATOR", ct)
        for t in tag_idx:
            edge(u, tags[t], "HAS_TAG", ct)
        F_uid.append(u)
        F_ct.append(ct)
        F_mod.append(mod)
        F_tags.append(tag_idx)
        F_album.append(album)
        f = len(F_uid) - 1
        P_forums[mod].append((f, ct))
        return f

    def new_person(ct: int) -> int:
        i, u = new_uid("Person")
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city = rng.randrange(len(cities))
        props = {"firstName": first, "lastName": last,
                 "gender": rng.choice(("female", "male")),
                 "birthday": _ts(1975, 1, 1) + rng.randrange(25 * 365) * DAY,
                 "browserUsed": rng.choice(BROWSERS), "locationIP": _ip(rng),
                 "id": i}
        node(u, "Person", props, ct)
        edge(u, cities[city], "IS_LOCATED_IN", ct)
        interests = pick_tags(rng.randint(1, 5))
        for t in interests:
            edge(u, tags[t], "HAS_INTEREST", ct)
        if rng.random() < 0.7:
            edge(u, study_orgs[rng.randrange(len(study_orgs))], "STUDY_AT",
                 ct, {"classYear": rng.randint(2000, 2012)})
        for c in _distinct(rng.choices(range(len(companies)),
                                       k=rng.choice((0, 1, 1, 2)))):
            edge(u, companies[c], "WORK_AT", ct,
                 {"workFrom": rng.randint(1995, 2012)})
        p = len(P_uid)
        P_uid.append(u)
        P_ct.append(ct)
        P_country.append(city_country[city])
        P_forums.append([])
        friends.append([])
        wall_of.append(new_forum(p, f"Wall of {first} {last}",
                                 _after(rng, ct, HOUR, ct + DAY), interests[:2]))
        if rng.random() < 0.3:
            new_forum(p, f"Album {rng.randint(1, 9)} of {first} {last}",
                      _after(rng, ct, 10 * DAY, ct + 30 * DAY), interests[:1],
                      album=True)
        return p

    known: set[tuple[int, int]] = set()
    knows_keys: list[EdgeKey] = []

    def add_knows(a: int, b: int, base: int, mean: int, hi: int) -> None:
        if a > b:
            a, b = b, a
        if a == b or (a, b) in known:
            return
        known.add((a, b))
        since = _after(rng, max(base, P_ct[a], P_ct[b]), mean, hi)
        # index order is id order, so src id < dst id
        edge(P_uid[a], P_uid[b], "KNOWS", since)
        friends[a].append((b, since))
        friends[b].append((a, since))
        knows_keys.append((P_uid[a], P_uid[b], "KNOWS", ""))

    members: set[tuple[int, int]] = set()

    def join(p: int, f: int, base: int, mean: int, hi: int) -> None:
        if F_mod[f] == p or (f, p) in members:
            return
        members.add((f, p))
        date = _after(rng, max(base, F_ct[f], P_ct[p]), mean, hi)
        edge(F_uid[f], P_uid[p], "HAS_MEMBER", date)
        P_forums[p].append((f, date))

    # ---- messages ----------------------------------------------------------
    M_uid: list[str] = []
    M_ct: list[int] = []
    M_children: list[list[int]] = []
    posts: list[int] = []
    comments: list[int] = []

    def message_country(p: int) -> str:
        if rng.random() < 0.9:
            return countries[P_country[p]]
        return countries[rng.randrange(len(countries))]

    def new_post(p: int, f: int, ct: int) -> None:
        i, u = new_uid("Message")
        common = {"browserUsed": rng.choice(BROWSERS), "locationIP": _ip(rng),
                  "id": i}
        if F_album[f] or rng.random() < 0.1:
            props = {"imageFile": f"photo{i}.jpg", "length": 0, **common}
        else:
            content = _text(rng, 4, 40)
            props = {"content": content, "language": rng.choice(LANGUAGES),
                     "length": len(content), **common}
        node(u, "Post", props, ct)
        edge(u, P_uid[p], "HAS_CREATOR", ct)
        edge(F_uid[f], u, "CONTAINER_OF", ct)
        edge(u, message_country(p), "IS_LOCATED_IN", ct)
        for t in _distinct(F_tags[f][:1] + pick_tags(rng.randint(0, 2))):
            edge(u, tags[t], "HAS_TAG", ct)
        M_uid.append(u)
        M_ct.append(ct)
        M_children.append([])
        posts.append(len(M_uid) - 1)

    def new_comment(p: int, parent: int, ct: int) -> None:
        i, u = new_uid("Message")
        content = _text(rng, 1, 15)
        props = {"content": content, "length": len(content),
                 "browserUsed": rng.choice(BROWSERS), "locationIP": _ip(rng),
                 "id": i}
        node(u, "Comment", props, ct)
        edge(u, P_uid[p], "HAS_CREATOR", ct)
        edge(u, message_country(p), "IS_LOCATED_IN", ct)
        edge(u, M_uid[parent], "REPLY_OF", ct)
        if rng.random() < 0.5:
            edge(u, tags[pick_tags(1)[0]], "HAS_TAG", ct)
        M_uid.append(u)
        M_ct.append(ct)
        M_children.append([])
        m = len(M_uid) - 1
        M_children[parent].append(m)
        comments.append(m)

    def pick_parent() -> int:
        if not comments or rng.random() < 0.55:
            return posts[rng.randrange(len(posts))]
        return comments[rng.randrange(len(comments))]

    liked: set[tuple[int, int]] = set()

    def like(p: int, m: int, base: int, mean: int, hi: int) -> None:
        if (p, m) in liked:
            return
        liked.add((p, m))
        edge(P_uid[p], M_uid[m], "LIKES",
             _after(rng, max(base, P_ct[p], M_ct[m]), mean, hi))

    # ---- 1. initial load -----------------------------------------------------
    span = (LOAD_END - LOAD_START) * 3 // 5
    for ct in sorted(LOAD_START + rng.randrange(span) for _ in range(scale)):
        new_person(ct)
    n = len(P_uid)
    weights = [rng.paretovariate(1.3) for _ in range(n)]
    draws = rng.choices(range(n), cum_weights=list(accumulate(weights)), k=9 * n)
    for j in range(0, len(draws) - 1, 2):
        add_knows(draws[j], draws[j + 1], 0, 60 * DAY, LOAD_END)
    for _ in range(max(2, scale // 10)):
        mod = rng.randrange(n)
        t = pick_tags(1)[0]
        c = rng.randrange(len(cities))
        groups.append(new_forum(
            mod, f"Group for {tag_names[t]} in {city_names[c]}",
            _after(rng, P_ct[mod], 60 * DAY, LOAD_END), [t]))
    for p in range(n):
        for _ in range(rng.randint(1, 4)):
            if friends[p] and rng.random() < 0.5:
                q, since = friends[p][rng.randrange(len(friends[p]))]
                join(p, wall_of[q], since, 20 * DAY, LOAD_END)
            else:
                join(p, groups[rng.randrange(len(groups))], 0, 20 * DAY, LOAD_END)
    for p in range(n):
        for _ in range(rng.randint(2, 10)):
            f, ready = P_forums[p][rng.randrange(len(P_forums[p]))]
            new_post(p, f, _after(rng, ready, 90 * DAY, LOAD_END))
    for _ in range(14 * n):
        parent = pick_parent()
        c = rng.randrange(n)
        new_comment(c, parent, _after(rng, max(M_ct[parent], P_ct[c]),
                                      2 * DAY, LOAD_END))
    for p in range(n):
        for _ in range(rng.randint(0, 10)):
            like(p, rng.randrange(len(M_uid)), 0, 10 * DAY, LOAD_END)

    for uid, label, props, vt_s in pend_nodes:
        led.assert_node(uid, label, props, vt_s)
    for src, dst, rel, props, vt_s in pend_edges:
        led.assert_edge(src, dst, rel, props, vt_s)
    led.commit()
    pend_nodes.clear()
    pend_edges.clear()
    direct[0] = True

    # ---- 2. inserts ------------------------------------------------------------
    n_ins = 3
    width = (INSERT_END - LOAD_END) // n_ins
    for b in range(n_ins):
        lo, hi = LOAD_END + b * width, LOAD_END + (b + 1) * width
        for _ in range(max(1, scale // 100)):
            p = new_person(rng.randrange(lo, lo + width // 2))
            for _ in range(rng.randint(1, 5)):
                add_knows(rng.randrange(len(P_uid) - 1), p, lo, 5 * DAY, hi)
            join(p, groups[rng.randrange(len(groups))], lo, 5 * DAY, hi)
        for _ in range(max(2, scale // 20)):
            p = rng.randrange(len(P_uid))
            f, ready = P_forums[p][rng.randrange(len(P_forums[p]))]
            new_post(p, f, _after(rng, max(lo, ready), 10 * DAY, hi))
        for _ in range(max(4, scale // 10)):
            parent = pick_parent()
            c = rng.randrange(len(P_uid))
            new_comment(c, parent, _after(rng, max(lo, M_ct[parent], P_ct[c]),
                                          2 * DAY, hi))
        for _ in range(max(4, scale // 10)):
            like(rng.randrange(len(P_uid)), rng.randrange(len(M_uid)), lo,
                 5 * DAY, hi)
        for _ in range(max(2, scale // 20)):
            add_knows(rng.randrange(len(P_uid)), rng.randrange(len(P_uid)), lo,
                      5 * DAY, hi)
        led.commit()

    # ---- 3. deletes (cascading, as retracts at the deletion date) -------------
    def delete_tree(m: int, t: int) -> None:
        stack, order = [m], []
        while stack:
            x = stack.pop()
            order.append(x)
            stack.extend(M_children[x])
        for x in order:
            led.delete_node(M_uid[x], t)

    def pick_alive(pool: list[int], t: int) -> int | None:
        for _ in range(20):
            m = pool[rng.randrange(len(pool))]
            if led.alive(M_uid[m], t):
                return m
        return None

    n_del = 3
    width = (DELETE_END - INSERT_END) // n_del
    for b in range(n_del):
        lo, hi = INSERT_END + b * width, INSERT_END + (b + 1) * width
        for pool in (posts, comments):
            for _ in range(max(1, scale // 50)):
                t = rng.randrange(lo, hi)
                m = pick_alive(pool, t)
                if m is not None:
                    delete_tree(m, t)
        if knows_keys:
            for _ in range(max(1, scale // 30)):
                t = rng.randrange(lo, hi)
                key = knows_keys[rng.randrange(len(knows_keys))]
                if led.edge_index_at(key, t) >= 0:
                    led.retract_edge(key, t)
        led.commit()

    # ---- 4. corrections ------------------------------------------------------
    candidates = ([(u, "birthday") for u in P_uid]
                  + [(u, "length") for u in M_uid])
    _apply_corrections(led, rng, candidates, corrections_pct, _mutate)
    led.commit()
    led.check_all()

    meta = {
        "persons": P_uid,
        "tags": tag_names,
        "tagclasses": tagclass_names,
        "countries": country_names,
        "forums": F_uid,
        "posts": [M_uid[m] for m in posts],
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
    """Parameter candidates: uids of persons, forums and posts alive at
    `NOW`, and the names of tags, tag classes and countries."""
    led, meta = _generate(scale, seed, 0.0)
    return {
        "persons": [u for u in meta["persons"] if led.alive(u, NOW)],
        "tags": list(meta["tags"]),
        "tagclasses": list(meta["tagclasses"]),
        "countries": list(meta["countries"]),
        "forums": [u for u in meta["forums"] if led.alive(u, NOW)],
        "posts": [u for u in meta["posts"] if led.alive(u, NOW)],
    }


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


__all__ = ["NOW", "OPEN_END", "batches", "build", "names"]
