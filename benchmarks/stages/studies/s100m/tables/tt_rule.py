"""tt rule check: per log, the batch tt series (one tt per batch line; the header line has none). Pairs: frozen log vs this stage's regeneration."""
import re, statistics as st
E = __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + ""; S = E + "/studies/s100m-a"
TT = re.compile(rb'"tt":(\d+)')
def series(p):
    out = []
    with open(p, "rb") as f:
        for i, line in enumerate(f):
            m = TT.findall(line[-64:])
            if m: out.append(int(m[-1]))
            elif i: out.append(None)
    return out
def desc(name, s):
    d = [b - a for a, b in zip(s, s[1:])]
    mono = all(x > 0 for x in d)
    return s, d, f"{name}: batches {len(s)}, strictly increasing {mono}, span {(s[-1]-s[0])/1e6:.3f} s, step median {st.median(d)} us, min {min(d)}, max {max(d)}"
def pair(a, b, base_n=None):
    sa, da, ta = desc("frozen", series(a)); sb, db, tb = desc("regenerated", series(b))
    print(" ", ta); print(" ", tb)
    same_n = len(sa) == len(sb)
    print(f"  same batch count {same_n}; first-tt offset {(sb[0]-sa[0])/1e6/86400:.2f} days")
    if same_n:
        r = [y / x for x, y in zip(da, db) if x > 0]
        print(f"  step ratio regenerated/frozen: median {st.median(r):.3f}, p5 {sorted(r)[len(r)//20]:.3f}, p95 {sorted(r)[len(r)*19//20]:.3f}")
    if base_n:
        for nm, s in (("frozen", sa), ("regenerated", sb)):
            print(f"  {nm}: base batches {base_n}, correction batches {len(s)-base_n}, gap last base -> first correction {(s[base_n]-s[base_n-1])/1e6:.3f} s, corrections span {(s[-1]-s[base_n])/1e6:.3f} s")
print("W2 1M d0: powerlaw-1000000 vs smoke-W2-1M-d0"); pair(f"{E}/w2-powerlaw/eventlogs/powerlaw-1000000.eventlog.jsonl", f"{S}/smoke-gen/eventlogs/smoke-W2-1M-d0.eventlog.jsonl")
print("W2 1M d10: W2-1M-d10 vs smoke-W2-1M-d10"); pair(f"{E}/w2-powerlaw/eventlogs/W2-1M-d10.eventlog.jsonl", f"{S}/smoke-gen/eventlogs/smoke-W2-1M-d10.eventlog.jsonl", 282)
print("W1 1M base: synth-1000000 vs smoke-K1M-d10 (base batches)")
a = series(f"{E}/v3/eventlogs/synth-1000000.eventlog.jsonl"); b = series(f"{S}/smoke-gen/eventlogs/smoke-K1M-d10.eventlog.jsonl")
print("  base tt identical (base copied verbatim):", a == b[:len(a)], "; correction batches", len(b) - len(a))
_, _, t = desc("smoke-K1M-d10", b); print(" ", t, f"; gap base -> corrections {(b[len(a)]-b[len(a)-1])/1e6:.3f} s")
for nm, p, n in (("K10M-d10", f"{E}/v3/eventlogs/K10M-d10.eventlog.jsonl", None), ("W2-10M-d10", f"{E}/w2-powerlaw/eventlogs/W2-10M-d10.eventlog.jsonl", None)):
    s = series(p); _, _, t = desc(nm, s); print("10M reference", t)
