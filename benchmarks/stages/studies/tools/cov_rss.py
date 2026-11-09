#!/usr/bin/env python3
"""cov_rss.py <out.jsonl> [interval_s]: memory of this job's processes, one line per sample until stopped. A copy of ing_rss.py with three changes:
(1) only processes in this job's Slurm cgroup are counted, so a shared node's other jobs stay out (if no job_<id> cgroup is found, every process of this user is counted, and the header says so);
(2) a clickhouse category; (3) COV_RSS_PSS_EVERY (default 10; 0: no PSS, for jobs that time queries, since reading smaps_rollup takes the target's mm lock).
Each sample: time; per category the sum of RSS from /proc/<pid>/statm and the largest VmHWM from /proc/<pid>/status; on PSS samples also the sum of PSS. The sampler leaves itself out. A process
that ends between reads is skipped."""
import json
import os
import re
import signal
import socket
import sys
import time

PAGE = os.sysconf("SC_PAGE_SIZE") // 1024
UID = os.getuid()
SELF = os.getpid()
EVERY = int(os.environ.get("COV_RSS_PSS_EVERY", "10"))


def job_token():
    try:
        m = re.search(r"job_\d+", open("/proc/self/cgroup").read())
        return m.group(0) if m else None
    except OSError:
        return None


TOKEN = job_token()


def category(cmd):
    if "postgres" in cmd:
        return "postgres"
    if "memgraph" in cmd:
        return "memgraph"
    if "clickhouse" in cmd:
        return "clickhouse"
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
            if TOKEN:
                with open(p + "/cgroup") as f:
                    if TOKEN not in f.read():
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
        f.write(json.dumps({"header": True, "cgroup_filter": TOKEN or "none, every process of this user", "host": socket.gethostname(), "interval_s": iv, "pss_every": EVERY}) + "\n")
        while not stop:
            pss = EVERY > 0 and n % EVERY == 0
            s = sample(pss)
            f.write(json.dumps({"t": round(time.time(), 1), "pss": pss, "by": s}) + "\n")
            f.flush()
            n += 1
            time.sleep(iv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
