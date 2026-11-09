"""Scenario: a revision-history study. Reproduce an analysis at a past revision.

    python examples/scenario_revision_history.py

A small knowledge base of cities is edited in revisions, each with its own
timestamp. Here the revisions are written as an event log whose belief
times are the revision times, and the log is replayed into a store with
`gnomon replay`. An analysis run "as of 2022-03-01" gives the same answer,
byte for byte, after later revisions corrected the data; run "now", it
shows the corrected state.
"""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.cli import main as gnomon_cli
from gnomon.storage.base import make_op
from gnomon.storage.eventlog import EventLog
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


def us(iso):
    """ISO date (UTC) -> int64 microseconds since the epoch."""
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp()) * 1_000_000


SINCE = us("2000-01-01")  # valid from: the facts hold from 2000 on


def city(uid, name, country, pop):
    return [make_op("assert_node", uid=uid, label="City",
                    props={"name": name, "population": pop}, vt_s=SINCE, vt_e=OPEN_END,
                    source="ingest", provenance_ref=None),
            make_op("assert_edge", src=uid, dst=country, rel_type="IN", props={},
                    vt_s=SINCE, vt_e=OPEN_END, disc="", source="ingest", provenance_ref=None)]


def country(uid, name):
    return [make_op("assert_node", uid=uid, label="Country", props={"name": name},
                    vt_s=SINCE, vt_e=OPEN_END, source="ingest", provenance_ref=None)]


# One batch per revision: (revision time, ops). Belief times must increase.
REVISIONS = [
    ("2022-01-10", country("fr", "France") + country("de", "Germany")
     + city("paris", "Paris", "fr", 2_100_000) + city("lyon", "Lyon", "fr", 510_000)
     + city("berlin", "Berlin", "de", 3_600_000)),
    ("2022-02-15", city("marseille", "Marseille", "fr", 870_000)
     + city("marseile", "Marseile", "fr", 870_000)),  # a duplicate with a typo
    ("2022-04-20", [make_op("correct", ref={"kind": "node", "uid": "lyon"},
                            props={"name": "Lyon", "population": 520_000},
                            vt_s=SINCE, vt_e=OPEN_END, source="ingest",
                            provenance_ref=None)]),
    ("2022-05-05", [make_op("retract", ref={"kind": "node", "uid": "marseile"}, t=SINCE,
                            source="ingest", provenance_ref=None),
                    make_op("retract", ref={"kind": "edge", "src": "marseile", "dst": "fr",
                                            "rel_type": "IN", "disc": ""}, t=SINCE,
                            source="ingest", provenance_ref=None)]),
    ("2022-06-01", city("hamburg", "Hamburg", "de", 1_800_000)),
]

# The analysis: cities and population per country, at valid time 2022-01-01.
ANALYSIS = {
    "pattern": {"nodes": [{"var": "c", "labels": ["City"]}, {"var": "k", "labels": ["Country"]}],
                "steps": [{"from": "c", "to": "k", "rel_type": "IN"}]},
    "return": {"group": {"by": [{"as": "country", "expr": {"prop": ["k", "name"]}}],
                         "aggs": [{"as": "cities", "fn": "count"},
                                  {"as": "population", "fn": "sum",
                                   "of": {"tprop": ["c", "population"], "tau": "int"}}]},
               "order_by": [{"expr": {"col": "country"}}]},
}


def analysis(store, as_of_tt):
    return call_operator(store.adapter, "match", {
        **ANALYSIS, "scope": {"vt": {"instant": us("2022-01-01")}, "as_of_tt": as_of_tt}})


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        log = EventLog(tmp / "revisions.jsonl")
        for day, ops in REVISIONS[:2]:
            log.append(us(day), ops)

        # Replay the first two revisions and run the study at 2022-03-01.
        gnomon_cli(["replay", str(tmp / "revisions.jsonl"), "--store", str(tmp / "kb-march")])
        store = gnomon.open(tmp / "kb-march")
        march = analysis(store, us("2022-03-01"))
        print("study at 2022-03-01, run in March:",
              [(r["country"], r["cities"], r["population"]) for r in march["rows"]])
        store.close()

        # Later revisions arrive. Replay the whole log into a fresh store.
        for day, ops in REVISIONS[2:]:
            log.append(us(day), ops)
        gnomon_cli(["replay", str(tmp / "revisions.jsonl"), "--store", str(tmp / "kb")])
        store = gnomon.open(tmp / "kb")

        again = analysis(store, us("2022-03-01"))
        print("study at 2022-03-01, rerun later: ",
              [(r["country"], r["cities"], r["population"]) for r in again["rows"]])
        print("same result digest:", again["result_digest"] == march["result_digest"])
        now = analysis(store, OPEN_END)
        print("study now:                        ",
              [(r["country"], r["cities"], r["population"]) for r in now["rows"]])

        # How the believed number of cities changed, revision by revision.
        env = call_operator(store.adapter, "state_series", {
            "kind": "node", "labels": ["City"], "axis": "transaction",
            "t_valid": us("2022-01-01"),
            "window": {"t_a": us("2022-01-01"), "t_b": us("2022-07-01")},
            "stride": 30 * 86_400_000_000, "agg": {"fn": "count"}})
        print("cities believed, every 30 days from 2022-01-01:",
              [r["value"] for r in env["rows"]])

        # Corrections are a count of superseded versions.
        env = call_operator(store.adapter, "version_history", {
            "kind": "node", "window": {"t_a": 0, "t_b": OPEN_END}, "belief": "superseded"})
        print("superseded node versions:", sorted(r["uid"] for r in env["rows"]))

        # What changed between two revisions.
        env = call_operator(store.adapter, "diff_belief", {
            "t1": us("2022-03-01"), "t2": us("2022-05-31"),
            "window": {"t_a": 0, "t_b": OPEN_END}})
        edges = {r["eid"]: f"{r['src']}->{r['dst']}" for r in call_operator(
            store.adapter, "version_history",
            {"kind": "edge", "window": {"t_a": 0, "t_b": OPEN_END}, "belief": "all"})["rows"]}
        print("changes from March to May:")
        for r in sorted(env["rows"], key=lambda r: (r["tt"], r["kind"], r["key"], r["change"])):
            print(f"   {r['change']:<9} {r['kind']} {edges.get(r['key'], r['key'])}")

        # New live writes continue after the replayed history.
        t = store.correct(EntityRef("node", uid="hamburg"),
                          {"name": "Hamburg", "population": 1_850_000}, vt_s=SINCE)
        print("a live write is believed after the last revision:", t > us("2022-06-01"))
        print("first log record:", json.dumps(next(EventLog(tmp / "kb" / "eventlog.jsonl")
                                                   .batches())["ops"][0], sort_keys=True))
        store.close()


if __name__ == "__main__":
    main()
