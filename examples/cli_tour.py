"""A tour of the `gnomon` command-line tool.

    python examples/cli_tour.py

Runs the main subcommands against a store in a temporary directory and
prints a short summary of each one's output. Belief times and digests
differ from run to run, so the summary leaves them out; the command-line
guide shows the full output of one run.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def gnomon_cmd():
    """The `gnomon` script installed next to this Python, else `python -m gnomon.cli`."""
    exe = Path(sys.executable).with_name("gnomon")
    if exe.exists():
        return [str(exe)]
    found = shutil.which("gnomon")
    return [found] if found else [sys.executable, "-m", "gnomon.cli"]


def run(*args, cwd):
    """Run one gnomon command; return (exit code, stdout)."""
    out = subprocess.run(gnomon_cmd() + list(args), check=False, cwd=cwd,
                         capture_output=True, text=True)
    return out.returncode, out.stdout


EVENTS = [
    {"src": "alice", "dst": "bob", "rel_type": "KNOWS", "vt_s": 100, "vt_e": 900},
    {"src": "bob", "dst": "carol", "rel_type": "KNOWS", "vt_s": 200, "vt_e": 900},
    {"src": "alice", "dst": "carol", "rel_type": "MSG", "vt_s": 250,
     "props": {"text": "hi"}},
]


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in EVENTS))

        rc, out = run("ingest", "events.jsonl", "--store", "mystore", cwd=tmp)
        stats = json.loads(out)["stats"]
        print("ingest:", rc, {k: stats[k] for k in ("n_node_versions", "n_edge_versions")})

        rc, out = run("call", "--store", "mystore", "temporal_reachability",
                      '{"src": "alice", "window": {"t_a": 0, "t_b": 1000}}', cwd=tmp)
        env = json.loads(out)
        print("call:", rc, [(r["uid"], r["earliest_arrival"]) for r in env["rows"]])

        rc, out = run("call", "--store", "mystore", "entity_history", '{"uid": 7}', cwd=tmp)
        print("call with a bad argument:", rc, json.loads(out)["error"])

        rc, out = run("store", "verify", "--store", "mystore", "--full", cwd=tmp)
        print("verify:", rc, out.strip().splitlines()[-1].split(" — ")[0])
        rc, out = run("check", "mystore", cwd=tmp)
        print("check:", rc)

        rc, out = run("store", "compact", "--store", "mystore", cwd=tmp)
        print("compact:", rc, sorted(json.loads(out)))
        rc, out = run("store", "settle", "--store", "mystore", cwd=tmp)
        print("settle:", rc)
        rc, out = run("store", "gc", "--store", "mystore", "--keep", "2", cwd=tmp)
        print("gc:", rc, sorted(json.loads(out)))
        rc, out = run("store", "build-indexes", "--store", "mystore", cwd=tmp)
        print("build-indexes:", rc)

        rc, out = run("store", "digest", "--store", "mystore", cwd=tmp)
        digest = json.loads(out)["digest"]
        rc, out = run("store", "digest", "--store", "mystore", "--streaming", cwd=tmp)
        print("digest, streaming equals in-memory:", json.loads(out)["digest"] == digest)

        rc, out = run("replay", "mystore/eventlog.jsonl", "--store", "copy", cwd=tmp)
        print("replay:", rc, json.loads(out)["batches"], "batch(es)")
        rc, out = run("store", "digest", "--store", "copy", cwd=tmp)
        print("replayed copy has the same content digest:", json.loads(out)["digest"] == digest)

        rc, out = run("store", "backup", "--store", "mystore", "--dest", "bak", cwd=tmp)
        print("backup:", rc, sorted(os.listdir(tmp / "bak")))
        rc, out = run("store", "restore", "--dest", "bak", "--store", "restored", cwd=tmp)
        print("restore:", rc, json.loads(out)["verdict"])

        print("store layout:", sorted(os.listdir(tmp / "mystore")),
              sorted(os.listdir(tmp / "mystore" / "native")))


if __name__ == "__main__":
    main()
