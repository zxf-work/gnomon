#!/usr/bin/env python3
"""CPU used on the host by processes outside one window.

  cpu_others.py snap   <out.json> <owner pid> <label>   at acquire and at release
  cpu_others.py window <acquire.json> <release.json>    one JSON line for the window

The exclusive gate lock orders only the work that takes it: a process outside
the protocol (an orphan of a closed session, a cron job, another user's shell)
is invisible to it. A snapshot reads every /proc/<pid>/stat twice, 1 s apart,
and prints the top CPU consumers at that moment that the window does not own.
The window owns its own process tree (the window script, its ancestors and its
threads and the container runtime (dockerd, containerd, containerd-shim) are
listed apart and not counted against the window: they also serve its own
containers.

`window` compares the cumulative CPU of every process alive at release with
its value at acquire (a process started inside the window counts in full) and
flags the window when the processes it does not own used more than one core on
average over the window. A process that started and exited between the two
snapshots is not seen by name; the host-wide busy and steal time (from
/proc/stat) bound what such processes and the window itself used together.
"""
import json
import os
import pwd
import subprocess
import sys
import time

TCK = os.sysconf("SC_CLK_TCK")
INFRA = ("dockerd", "containerd", "docker-proxy", "runc")
FLAG_CORES = 1.0


def utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_procs():
    out = {}
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            raw = open(f"/proc/{d}/stat").read()
            comm = raw[raw.index("(") + 1:raw.rindex(")")]
            f = raw[raw.rindex(")") + 2:].split()
            uid = os.stat(f"/proc/{d}").st_uid
            cmd = open(f"/proc/{d}/cmdline", "rb").read().replace(b"\0", b" ")
            cg = open(f"/proc/{d}/cgroup").read()
        except (OSError, ValueError):
            continue
        out[int(d)] = {"ppid": int(f[1]), "cpu": int(f[11]) + int(f[12]), "start": int(f[19]),
                       "uid": uid, "comm": comm,
                       "cmd": cmd.decode("utf-8", "replace").strip()[:200], "cg": cg}
    return out


def host_stat():
    v = [int(x) for x in open("/proc/stat").readline().split()[1:]]
    # user nice system idle iowait irq softirq steal
    return {"busy": v[0] + v[1] + v[2] + v[5] + v[6], "steal": v[7] if len(v) > 7 else 0}


def own_containers():
    try:
        r = subprocess.run(["docker", "ps", "-q", "--no-trunc", "--filter", "name=extcmp-"],
                           capture_output=True, text=True, timeout=30)
        return [x for x in r.stdout.split() if x]
    except Exception:
        return []


def classify(procs, owner, cids):
    owned = set()
    p = owner
    while p in procs and p not in owned:      # the window script and its ancestors
        owned.add(p)
        p = procs[p]["ppid"]
    kids = {}
    for q, r in procs.items():
        kids.setdefault(r["ppid"], []).append(q)
    stack = [owner]
    while stack:                               # its descendants
        q = stack.pop()
        for c in kids.get(q, []):
            if c not in owned:
                owned.add(c)
                stack.append(c)
    # Apptainer servers (no container cgroup): the instance and exec processes
    # carry the instance name `extcmp-…` on their command line; they and their
    # descendants belong to the campaign's own server
    roots = [q for q, r in procs.items()
             if "extcmp-" in r["cmd"] and ("pptainer" in r["cmd"] or "instance://" in r["cmd"])]
    stack = list(roots)
    while stack:
        q = stack.pop()
        if q in owned:
            continue
        owned.add(q)
        stack.extend(kids.get(q, []))
    cls = {}
    for q, r in procs.items():
        if q in owned or any(c in r["cg"] for c in cids):
            cls[q] = "window"
        elif q == 2 or r["ppid"] == 2:
            cls[q] = "kernel"
        elif r["comm"] in INFRA or r["comm"].startswith("containerd-shim"):
            cls[q] = "runtime"
        else:
            cls[q] = "other"
    return cls


def user(uid):
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def same(prev, r):
    return prev is not None and prev["start"] == r["start"]


def cmd_snap(path, owner, label):
    cids = own_containers()
    a, ha, ta = read_procs(), host_stat(), time.time()
    time.sleep(1.0)
    b, hb, tb = read_procs(), host_stat(), time.time()
    dt = tb - ta
    cls = classify(b, owner, cids)
    rows = []
    for q, r in b.items():
        if cls[q] == "window":
            continue
        prev = a.get(q)
        c = (r["cpu"] - prev["cpu"] if same(prev, r) else r["cpu"]) / TCK / dt
        if c >= 0.05:
            rows.append((cls[q], {"pid": q, "user": user(r["uid"]), "cores": round(c, 3),
                                  "comm": r["comm"], "cmd": r["cmd"][:160]}))
    rows.sort(key=lambda x: -x[1]["cores"])
    snap = {"utc": utc(), "t": tb, "label": label, "owner": owner, "containers": cids, "host": hb,
            "procs": {str(q): {"cpu": r["cpu"], "start": r["start"], "uid": r["uid"], "comm": r["comm"],
                               "cmd": r["cmd"], "cls": cls[q]} for q, r in b.items()}}
    with open(path, "w") as fh:
        json.dump(snap, fh)
    print(json.dumps({
        "utc": snap["utc"], "cpu_outside_window": "at " + label,
        "host_busy_cores_now": round((hb["busy"] - ha["busy"]) / TCK / dt, 2),
        "steal_cores_now": round((hb["steal"] - ha["steal"]) / TCK / dt, 2),
        "top_other": [e for c, e in rows if c == "other"][:5],
        "top_runtime_kernel": [dict(e, cls=c) for c, e in rows if c != "other"][:3]}))


def cmd_window(pa, pb):
    a, b = json.load(open(pa)), json.load(open(pb))
    wall = max(b["t"] - a["t"], 1e-6)
    sums = {"other": 0.0, "runtime": 0.0, "kernel": 0.0, "window": 0.0}
    per = []
    for q, r in b["procs"].items():
        prev = a["procs"].get(q)
        if not same(prev, r):
            prev = None
        cls = "window" if (prev and prev["cls"] == "window") else r["cls"]
        s = (r["cpu"] - prev["cpu"] if prev else r["cpu"]) / TCK
        sums[cls] += s
        if cls == "other" and s / wall >= 0.05:
            per.append({"pid": int(q), "user": user(r["uid"]), "cores_avg": round(s / wall, 3),
                        "cpu_s": round(s, 1), "comm": r["comm"], "cmd": r["cmd"][:160],
                        "started_in_window": prev is None})
    per.sort(key=lambda x: -x["cores_avg"])
    other = sums["other"] / wall
    print(json.dumps({
        "utc": utc(), "cpu_outside_window": "window", "acquire": a["utc"], "release": b["utc"],
        "wall_s": round(wall, 1),
        "other_cores_avg": round(other, 3),
        "flag_other_over_1_core": other > FLAG_CORES,
        "runtime_cores_avg": round(sums["runtime"] / wall, 3),
        "kernel_cores_avg": round(sums["kernel"] / wall, 3),
        "host_busy_cores_avg": round((b["host"]["busy"] - a["host"]["busy"]) / TCK / wall, 3),
        "steal_cores_avg": round((b["host"]["steal"] - a["host"]["steal"]) / TCK / wall, 3),
        "top_other": per[:5]}))
    return 1 if other > FLAG_CORES else 0


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "snap":
        cmd_snap(sys.argv[2], int(sys.argv[3]), sys.argv[4])
    elif len(sys.argv) == 4 and sys.argv[1] == "window":
        sys.exit(cmd_window(sys.argv[2], sys.argv[3]))
    else:
        sys.exit("usage: cpu_others.py snap <out> <owner pid> <label> | window <acquire> <release>")
