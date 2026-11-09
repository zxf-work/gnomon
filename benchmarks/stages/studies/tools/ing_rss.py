#!/usr/bin/env python3
"""ing_rss.py <out.jsonl> [interval_s]: memory of every process of this user on this whole node, one line per sample until stopped.
Each sample: time; per category (postgres, neo4j (java), memgraph, client (python), other) the sum of RSS from /proc/<pid>/statm, and the largest VmHWM from /proc/<pid>/status; every tenth
sample also the sum of PSS from /proc/<pid>/smaps_rollup, which counts shared pages once (PostgreSQL's shared buffers). The sampler leaves itself out. A process that ends between reads is skipped."""
import json
import os
import signal
import sys
import time

PAGE = os.sysconf("SC_PAGE_SIZE") // 1024
UID = os.getuid()
SELF = os.getpid()


def category(cmd):
    if "postgres" in cmd:
        return "postgres"
    if "memgraph" in cmd:
        return "memgraph"
    if "java" in cmd or "neo4j" in cmd:
        return "neo4j"
    if "python" in cmd:
        return "client"
    return "other"


def field(text, name):
    for line in text.splitlines():
        if line.startswith(name + ":"):
            return int(line.split()[1])
    return 0


def sample(pss):
    out = {}
    for d in os.listdir("/proc"):
        if not d.isdigit() or int(d) == SELF:
            continue
        p = "/proc/" + d
        try:
            if os.stat(p).st_uid != UID:
                continue
            with open(p + "/cmdline", "rb") as f:
                cmd = f.read(400).replace(b"\0", b" ").decode("utf8", "replace")
            with open(p + "/statm") as f:
                rss = int(f.read().split()[1]) * PAGE
            with open(p + "/status") as f:
                hwm = field(f.read(), "VmHWM")
            v = 0
            if pss:
                with open(p + "/smaps_rollup") as f:
                    v = field(f.read(), "Pss")
        except OSError:
            continue
        if rss == 0:
            continue
        c = out.setdefault(category(cmd), {"rss_kb": 0, "hwm_kb": 0, "pss_kb": 0})
        c["rss_kb"] += rss
        c["hwm_kb"] = max(c["hwm_kb"], hwm)
        c["pss_kb"] += v
    return out


def main():
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    iv = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
    n = 0
    with open(sys.argv[1], "a") as f:
        while not stop:
            s = sample(n % 10 == 0)
            f.write(json.dumps({"t": round(time.time(), 1), "pss": n % 10 == 0, "by": s}) + "\n")
            f.flush()
            n += 1
            time.sleep(iv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
