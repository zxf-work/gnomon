#!/usr/bin/env bash
# Host census: one JSON object per call, appended to $1. Recorded before and
# after every window, and once at campaign start and end.
#   census.sh <out.jsonl> <label>
out=$1; label=${2:-}
r() { tr '\n' ' ' | sed 's/\\/\\\\/g; s/"/\\"/g'; }
gov=$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c | r)
boost=$(cat /sys/devices/system/cpu/cpufreq/boost 2>/dev/null || cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null | sed 's/^/no_turbo=/')
smt=$(cat /sys/devices/system/cpu/smt/active 2>/dev/null)
thp=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null)
thpd=$(cat /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null)
clk=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null)
swap=$(swapon --noheadings --show=NAME,SIZE 2>/dev/null | r)
mnt=$(findmnt -no SOURCE,FSTYPE,OPTIONS -T "${EXTCMP_ROOT:-/}" 2>/dev/null | r)
dev=$(lsblk -dno NAME,MODEL,ROTA,SIZE 2>/dev/null | r)
cg=$(stat -fc %T /sys/fs/cgroup 2>/dev/null)
dkr=$(docker info --format '{{.ServerVersion}} cgroup={{.CgroupDriver}}/{{.CgroupVersion}} root={{.DockerRootDir}}' 2>/dev/null)
ctr=$(docker ps --format '{{.Names}}={{.Image}}' 2>/dev/null | r)
[ "${EXTCMP_BACKEND:-docker}" = apptainer ] && ctr=$(apptainer instance list 2>/dev/null | tail -n +2 | r)
mem=$(free -b | awk '/Mem:/ {print "total="$2" used="$3" free="$4" cache="$6" avail="$7}')
cpu=$(lscpu 2>/dev/null | awk -F: '/Model name|^CPU\(s\)|Thread|Socket|NUMA node\(s\)|MHz/ {gsub(/^ +/,"",$2); printf "%s=%s; ", $1, $2}' | r)
top=$(ps -eo pid,pcpu,rss,etimes,comm --sort=-pcpu | head -6 | r)
# batch host: the job, the node, its sharing setting, node-local disk, Apptainer
job=""; [ -n "${SLURM_JOB_ID:-}" ] && job=$(scontrol show job "$SLURM_JOB_ID" 2>/dev/null |
    grep -oE '(JobId|Partition|NodeList|NumCPUs|OverSubscribe|TimeLimit|RunTime)=[^ ]+' | r)
loc=""; [ -n "${LOCAL:-}" ] && loc=$(df -B1 --output=source,fstype,size,avail "${LOCAL:-${TMPDIR:-/tmp}}" 2>/dev/null | tail -1 | r)
appt=$(apptainer --version 2>/dev/null)
numa="$(numactl -H 2>/dev/null | grep -E '^(available|node [0-9]+ (cpus|size))' | r) policies: client=[${EXTCMP_NUMA_CLIENT:-none}] server=[${EXTCMP_NUMA_SERVER:-none}] dropped: client=[${EXTCMP_NUMA_CLIENT_DROPPED:-}] server=[${EXTCMP_NUMA_SERVER_DROPPED:-}]"
lscpu_full=$(lscpu 2>/dev/null | awk -F: '{gsub(/^ +/,"",$2); printf "%s=%s; ", $1, $2}' | r)
printf '{"utc": "%s", "label": "%s", "host": "%s", "kernel": "%s", "cpu": "%s", "governor": "%s", "boost": "%s", "smt_active": "%s", "thp": "%s", "thp_defrag": "%s", "clocksource": "%s", "swap": "%s", "mount": "%s", "block_devices": "%s", "cgroup_fs": "%s", "docker": "%s", "containers": "%s", "memory": "%s", "loadavg": "%s", "top": "%s", "slurm_job": "%s", "local_disk": "%s", "apptainer": "%s", "lscpu": "%s", "numa": "%s"}\n' \
  "$(date -u +%FT%TZ)" "$label" "$(hostname -s)" "$(uname -r)" "$cpu" "$gov" "$boost" "$smt" "$thp" "$thpd" \
  "$clk" "$swap" "$mnt" "$dev" "$cg" "$dkr" "$ctr" "$mem" "$(cut -d' ' -f1-3 /proc/loadavg)" "$top" \
  "$job" "$loc" "$appt" "$lscpu_full" "$numa" >> "$out"
