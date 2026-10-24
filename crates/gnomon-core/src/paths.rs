//! `temporal_paths` in the engine: the number of node-simple
//! time-respecting paths `src -> dst` of at most `max_hops` edges inside
//! `[t_a, t_b)`, and the `k` first of them, without materialising the paths
//! and without walking their last edges.
//!
//! # The question
//!
//! The operational definition is `gnomon/temporal/ops_paths.py`'s (the oracle
//! shares it): from `src` at arrival `t_a`, an edge version `u -> v` with
//! valid interval `[vt_s, vt_e)` is traversable at arrival `a` iff
//! `tau = max(a, vt_s) < min(vt_e, t_b)`, and the arrival at `v` is `tau`. A
//! path never revisits a node, the source included, and ends the first time
//! it reaches `dst`. The answer is how many such paths have at most
//! `max_hops` edges (`rows_total`) and the first `k` of them by `(arrival,
//! hops, ((vt_s, eid) per edge))`.
//!
//! # Why the reference refused hub queries
//!
//! The Python operator enumerates every path depth first, sorts them all and
//! keeps `k`, and its budget is 500,000 examined edges. One hop from a hub
//! that budget is gone before a single level is walked (28 of 588 instances
//! of a power-law benchmark graph). The total is part of the answer, so the paths must
//! be counted; what they need not be is walked, and only `k` of them need to
//! exist as paths.
//!
//! # The search
//!
//! * **The graph, from both ends.** Out-edges of the nodes the source reaches
//!   and in-edges of the nodes that reach the target, a round at a time, the
//!   cheaper side first, until the two sides' rounds add up to `max_hops`:
//!   then every edge of every path is among them (edge `i` of an `h`-edge
//!   path leaves a node the source reaches in `i - 1` hops, or enters one that
//!   reaches the target in `h - i` hops). Rounds read the window's believed
//!   ranges through the adjacency postings, as `temporal_reachability`'s
//!   frontier does; a round that would bring the rows read past half
//!   the window is replaced by one read of the window (the rule `paths`
//!   frontier loading uses).
//! * **Latest starts.** For each node and hop budget `r`, the latest arrival
//!   from which the target is still reachable in at most `r` edges, by `r`
//!   backward relaxations. Arriving later can only lose edges
//!   (`max(a, vt_s)` is monotone in `a`), so a prefix that arrives after its
//!   node's latest start has no completion and is not extended. The bound
//!   ignores node-simplicity, so it never prunes a path.
//! * **Prefixes, not paths.** The walk visits prefixes of at most `max_hops -
//!   1` edges. A prefix ending at `u` with arrival `a` completes with one
//!   edge `u -> dst` exactly when `min(vt_e, t_b) > a` (the target is never
//!   on the prefix), so its completions are counted by one binary search in
//!   `u`'s edges into the target, sorted by that bound. In `(vt_s, eid)`
//!   order those completions also come out in answer order (their arrival
//!   `max(a, vt_s)` does not decrease, and the prefix is shared), so offering
//!   them to the best `k` stops at the first one refused.
//! * **The best `k`.** A sorted list of at most `k` candidates. `eid` is a
//!   hash of the edge's identity, computed only when two candidates tie on
//!   every earlier key, and remembered.
//!
//! Work is the edges examined at prefixes of at most `max_hops - 2` edges,
//! a subset of the edges the reference examines, so every query the
//! reference answers inside its budget is answered here inside the same one;
//! the operator allows this search a larger budget.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::error::{EngineError, Result};
use crate::scan::{ScanRequest, ScanSet, SegPlan};
use crate::segment::SegmentSource;
use crate::store::NativeStore;

/// The operator's hop cap (`MAX_PATH_HOPS`); a candidate holds this many edges.
pub const PATHS_MAX_HOPS: usize = 6;

/// A round is replaced by one window read when the rows read would pass
/// `1 / WHOLE_FRACTION` of the window's believed rows, past
/// [`WHOLE_MIN_ROWS`] rows only.
const WHOLE_FRACTION: usize = 2;
const WHOLE_MIN_ROWS: usize = 1024;

const NONE: u32 = u32::MAX;

/// The answer: the total, and the first `k` paths, edge by edge.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PathsOut {
    /// Paths in all (`rows_total`).
    pub total: u64,
    /// Per returned path, in answer order.
    pub arrival: Vec<i64>,
    pub hops: Vec<u32>,
    /// Per edge of the returned paths, path after path.
    pub src_id: Vec<u32>,
    pub dst_id: Vec<u32>,
    pub vt_s: Vec<i64>,
    pub eid: Vec<String>,
    pub rel_type: Vec<String>,
    /// Edges the walk examined.
    pub expansions: u64,
    /// The walk stopped at its budget: nothing else here is meaningful.
    pub exceeded: bool,
    /// Rows the graph was read from, and whether by rounds (false: one
    /// window read).
    pub rows_read: u64,
    pub by_rounds: bool,
}

/// One edge version the search may use: `vt_s < m` and `m > t_a`, where
/// `m = min(vt_e, t_b)`; never a self-loop, never into the source, never out
/// of the target. `plan`/`row` address it for its identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Raw {
    pub(crate) from: u32,
    pub(crate) to: u32,
    pub(crate) vt_s: i64,
    pub(crate) m: i64,
    pub(crate) plan: u32,
    pub(crate) row: u32,
}

/// `Some(min(vt_e, t_b))` when some arrival `a >= t_a` can traverse the edge.
#[inline]
fn bound(vt_s: i64, vt_e: i64, t_a: i64, t_b: i64) -> Option<i64> {
    let m = vt_e.min(t_b);
    (m > t_a && vt_s < m).then_some(m)
}

/// An edge between two local nodes, as the walk reads it.
#[derive(Clone, Copy, Debug)]
struct Arc {
    to: u32,
    vt_s: i64,
    m: i64,
    raw: u32,
}

/// An edge into the target.
#[derive(Clone, Copy, Debug)]
struct DArc {
    vt_s: i64,
    m: i64,
    raw: u32,
}

/// One answer candidate: `hops` edges, as indices into the gathered rows.
#[derive(Clone, Copy, Debug)]
struct Cand {
    arrival: i64,
    hops: u8,
    arcs: [u32; PATHS_MAX_HOPS],
}

/// What the walk found.
pub(crate) struct Found {
    pub(crate) total: u64,
    /// The best `k`, in answer order: `(arrival, edges)`.
    pub(crate) best: Vec<(i64, Vec<u32>)>,
    pub(crate) expansions: u64,
    pub(crate) exceeded: bool,
}

/// The search over the gathered rows (`src`, `dst`, endpoints are dense ids
/// below `n`). `eid_of` gives a row's identity; it is called only for rows
/// whose order would otherwise be undecided.
#[allow(clippy::too_many_arguments)]
pub(crate) fn search(
    raws: &[Raw],
    n: usize,
    src: u32,
    dst: u32,
    t_a: i64,
    max_hops: usize,
    k: usize,
    budget: u64,
    eid_of: &mut dyn FnMut(&Raw) -> Result<String>,
) -> Result<Found> {
    let mut found = Found {
        total: 0,
        best: Vec::new(),
        expansions: 0,
        exceeded: false,
    };
    if src == dst || max_hops == 0 {
        return Ok(found);
    }
    if max_hops > PATHS_MAX_HOPS {
        return Err(EngineError::invariant(format!(
            "temporal_paths: max_hops {max_hops} is past {PATHS_MAX_HOPS}"
        )));
    }
    // local ids: the source first, every endpoint but the target
    let mut loc = vec![NONE; n];
    let mut nodes: Vec<u32> = Vec::new();
    let mut local = |g: u32, loc: &mut Vec<u32>| -> Result<u32> {
        let slot = loc.get_mut(g as usize).ok_or_else(|| {
            EngineError::corrupt(format!("edge endpoint {g} is past the dictionary ({n} entities)"))
        })?;
        if *slot == NONE {
            *slot = nodes.len() as u32;
            nodes.push(g);
        }
        Ok(*slot)
    };
    local(src, &mut loc)?;
    for r in raws {
        local(r.from, &mut loc)?;
        if r.to != dst {
            local(r.to, &mut loc)?;
        }
    }
    let nl = nodes.len();
    // by-source runs: edges between local nodes, and edges into the target
    let mut off = vec![0u32; nl + 1];
    let mut doff = vec![0u32; nl + 1];
    for r in raws {
        let u = loc[r.from as usize] as usize;
        if r.to == dst {
            doff[u + 1] += 1;
        } else {
            off[u + 1] += 1;
        }
    }
    for i in 0..nl {
        off[i + 1] += off[i];
        doff[i + 1] += doff[i];
    }
    let mut cur = off.clone();
    let mut dcur = doff.clone();
    let mut arcs = vec![Arc { to: 0, vt_s: 0, m: 0, raw: 0 }; off[nl] as usize];
    let mut darcs = vec![DArc { vt_s: 0, m: 0, raw: 0 }; doff[nl] as usize];
    for (i, r) in raws.iter().enumerate() {
        let u = loc[r.from as usize] as usize;
        if r.to == dst {
            darcs[dcur[u] as usize] = DArc { vt_s: r.vt_s, m: r.m, raw: i as u32 };
            dcur[u] += 1;
        } else {
            arcs[cur[u] as usize] = Arc { to: loc[r.to as usize], vt_s: r.vt_s, m: r.m, raw: i as u32 };
            cur[u] += 1;
        }
    }
    // edges into the target: completions in answer order, `(vt_s, eid)`,
    // with eids computed only for ties on vt_s
    let mut memo: HashMap<u32, String> = HashMap::new();
    for u in 0..nl {
        let run = &mut darcs[doff[u] as usize..doff[u + 1] as usize];
        if run.len() < 2 {
            continue;
        }
        run.sort_unstable_by_key(|e| (e.vt_s, e.raw));
        let mut i = 0;
        while i < run.len() {
            let mut j = i + 1;
            while j < run.len() && run[j].vt_s == run[i].vt_s {
                j += 1;
            }
            if j - i > 1 {
                for e in &run[i..j] {
                    remember(&mut memo, raws, e.raw, eid_of)?;
                }
                run[i..j].sort_by(|x, y| memo[&x.raw].cmp(&memo[&y.raw]));
            }
            i = j;
        }
    }
    let mut dm: Vec<i64> = darcs.iter().map(|e| e.m).collect();
    for u in 0..nl {
        dm[doff[u] as usize..doff[u + 1] as usize].sort_unstable();
    }
    // latest starts: ls[r][u], the latest arrival at u from which the target
    // is reachable in at most r edges (i64::MIN: never)
    let mut ls: Vec<Vec<i64>> = vec![Vec::new(); max_hops];
    if max_hops >= 2 {
        let mut l1 = vec![i64::MIN; nl];
        for (u, l) in l1.iter_mut().enumerate() {
            for e in &darcs[doff[u] as usize..doff[u + 1] as usize] {
                *l = (*l).max(e.m - 1);
            }
        }
        ls[1] = l1;
        for r in 2..max_hops {
            let prev = &ls[r - 1];
            let mut next = prev.clone();
            for (u, l) in next.iter_mut().enumerate() {
                for e in &arcs[off[u] as usize..off[u + 1] as usize] {
                    let c = (e.m - 1).min(prev[e.to as usize]);
                    if e.vt_s <= c && c > *l {
                        *l = c;
                    }
                }
            }
            ls[r] = next;
        }
    }
    let graph = Graph { off, arcs, doff, darcs, dm, ls };
    let mut walk = Walk {
        g: &graph,
        raws,
        h: max_hops,
        k,
        budget,
        total: 0,
        exp: 0,
        best: Vec::with_capacity(k + 1),
        stack: [0; PATHS_MAX_HOPS + 1],
        path: [0; PATHS_MAX_HOPS],
        memo,
        eid_of,
    };
    let done = walk.visit(0, t_a, 0)?;
    found.total = walk.total;
    found.expansions = walk.exp;
    found.exceeded = !done;
    found.best = walk
        .best
        .iter()
        .map(|c| (c.arrival, c.arcs[..c.hops as usize].to_vec()))
        .collect();
    Ok(found)
}

/// `raw`'s eid into `memo`, once.
fn remember(
    memo: &mut HashMap<u32, String>,
    raws: &[Raw],
    raw: u32,
    eid_of: &mut dyn FnMut(&Raw) -> Result<String>,
) -> Result<()> {
    if let std::collections::hash_map::Entry::Vacant(v) = memo.entry(raw) {
        v.insert(eid_of(&raws[raw as usize])?);
    }
    Ok(())
}

struct Graph {
    off: Vec<u32>,
    arcs: Vec<Arc>,
    doff: Vec<u32>,
    darcs: Vec<DArc>,
    /// `darcs`' bounds, sorted per node
    dm: Vec<i64>,
    ls: Vec<Vec<i64>>,
}

struct Walk<'g, 'e> {
    g: &'g Graph,
    raws: &'g [Raw],
    h: usize,
    k: usize,
    budget: u64,
    total: u64,
    exp: u64,
    best: Vec<Cand>,
    /// the prefix's nodes (local ids), the source at 0
    stack: [u32; PATHS_MAX_HOPS + 1],
    /// the prefix's edges (gathered rows)
    path: [u32; PATHS_MAX_HOPS],
    memo: HashMap<u32, String>,
    eid_of: &'e mut dyn FnMut(&Raw) -> Result<String>,
}

impl Walk<'_, '_> {
    fn eid(&mut self, raw: u32) -> Result<()> {
        remember(&mut self.memo, self.raws, raw, self.eid_of)
    }

    /// The answer order: `(arrival, hops)`, then edge by edge `(vt_s, eid)`.
    fn cmp(&mut self, x: &Cand, y: &Cand) -> Result<Ordering> {
        let o = (x.arrival, x.hops).cmp(&(y.arrival, y.hops));
        if o != Ordering::Equal {
            return Ok(o);
        }
        for i in 0..x.hops as usize {
            let (a, b) = (x.arcs[i], y.arcs[i]);
            if a == b {
                continue;
            }
            let o = self.raws[a as usize].vt_s.cmp(&self.raws[b as usize].vt_s);
            if o != Ordering::Equal {
                return Ok(o);
            }
            self.eid(a)?;
            self.eid(b)?;
            let o = self.memo[&a].cmp(&self.memo[&b]);
            if o != Ordering::Equal {
                return Ok(o);
            }
        }
        Ok(Ordering::Equal)
    }

    /// Offer a candidate to the best `k`; false when it is not among them.
    fn offer(&mut self, c: Cand) -> Result<bool> {
        if self.best.len() == self.k {
            let worst = self.best[self.k - 1];
            if c.arrival > worst.arrival || self.cmp(&c, &worst)? != Ordering::Less {
                return Ok(false);
            }
            self.best.pop();
        }
        let mut at = self.best.len();
        while at > 0 {
            let p = self.best[at - 1];
            if self.cmp(&c, &p)? == Ordering::Less {
                at -= 1;
            } else {
                break;
            }
        }
        self.best.insert(at, c);
        Ok(true)
    }

    /// The prefix ending at local node `u` with arrival `a` and `depth`
    /// edges: its one-edge completions, then its extensions. False once the
    /// budget is spent.
    fn visit(&mut self, u: u32, a: i64, depth: usize) -> Result<bool> {
        let g = self.g;
        let (lo, hi) = (g.doff[u as usize] as usize, g.doff[u as usize + 1] as usize);
        if lo < hi {
            let ms = &g.dm[lo..hi];
            let n = ms.len() - ms.partition_point(|&m| m <= a);
            if n > 0 {
                self.total += n as u64;
                if self.k > 0 && (self.best.len() < self.k || a <= self.best[self.k - 1].arrival) {
                    for e in &g.darcs[lo..hi] {
                        if e.m <= a {
                            continue;
                        }
                        let mut c = Cand {
                            arrival: a.max(e.vt_s),
                            hops: (depth + 1) as u8,
                            arcs: self.path,
                        };
                        c.arcs[depth] = e.raw;
                        if !self.offer(c)? {
                            break;
                        }
                    }
                }
            }
        }
        // a prefix has at most h - 1 edges
        if depth + 2 > self.h {
            return Ok(true);
        }
        let r = self.h - depth - 1;
        for e in &g.arcs[g.off[u as usize] as usize..g.off[u as usize + 1] as usize] {
            self.exp += 1;
            if self.exp > self.budget {
                return Ok(false);
            }
            let tau = a.max(e.vt_s);
            if tau >= e.m || tau > g.ls[r][e.to as usize] {
                continue;
            }
            if self.stack[..=depth].contains(&e.to) {
                continue;
            }
            self.stack[depth + 1] = e.to;
            self.path[depth] = e.raw;
            if !self.visit(e.to, tau, depth + 1)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// `(src_id, dst_id, vt_s, vt_e or elided)` of one planned segment.
type PlanCols<'a> = (&'a [u32], &'a [u32], &'a [i64], Option<&'a [i64]>);

fn plan_cols<'a, S: SegmentSource>(set: &ScanSet<'a, S>, p: &SegPlan<'a>) -> Result<PlanCols<'a>> {
    let seg = set.segment(p.idx);
    Ok((seg.u32_column("src_id")?, seg.u32_column("dst_id")?, seg.i64_column("vt_s")?, p.vt_e))
}

/// The usable rows of `[lo, hi)` of plan `k` into `out`, those `keep`
/// accepts; returns the rows walked.
#[allow(clippy::too_many_arguments)]
fn take_rows(
    cols: &PlanCols<'_>,
    k: usize,
    ranges: &[(usize, usize)],
    src: u32,
    dst: u32,
    t_a: i64,
    t_b: i64,
    keep: &dyn Fn(u32, u32) -> bool,
    out: &mut Vec<Raw>,
) -> usize {
    let (s, d, vs, ve) = cols;
    let mut walked = 0;
    for &(a, b) in ranges {
        walked += b - a;
        for i in a..b {
            let (u, v) = (s[i], d[i]);
            if u == v || v == src || u == dst || !keep(u, v) {
                continue;
            }
            let e = ve.map_or(vs[i] + 1, |c| c[i]);
            if let Some(m) = bound(vs[i], e, t_a, t_b) {
                out.push(Raw { from: u, to: v, vt_s: vs[i], m, plan: k as u32, row: i as u32 });
            }
        }
    }
    walked
}

/// The rows the search needs: rounds from both ends, or one window read.
#[allow(clippy::too_many_arguments)]
fn gather<'a, S: SegmentSource>(
    set: &ScanSet<'a, S>,
    plans: &[SegPlan<'a>],
    src: u32,
    dst: u32,
    t_a: i64,
    t_b: i64,
    max_hops: usize,
    rounds: bool,
) -> Result<(Vec<Raw>, u64, bool)> {
    let window: usize = plans.iter().map(|p| p.rows).sum();
    if rounds {
        if let Some((raws, read)) = gather_rounds(set, plans, src, dst, t_a, t_b, max_hops, window)? {
            return Ok((raws, read as u64, true));
        }
    }
    let mut raws = Vec::new();
    let mut read = 0;
    for (k, p) in plans.iter().enumerate() {
        let cols = plan_cols(set, p)?;
        read += take_rows(&cols, k, &p.ranges, src, dst, t_a, t_b, &|_, _| true, &mut raws);
    }
    Ok((raws, read as u64, false))
}

/// Rounds from both ends (see the module note); `None` when a round would
/// read past the window-read rule, or a segment carries no postings.
#[allow(clippy::too_many_arguments)]
fn gather_rounds<'a, S: SegmentSource>(
    set: &ScanSet<'a, S>,
    plans: &[SegPlan<'a>],
    src: u32,
    dst: u32,
    t_a: i64,
    t_b: i64,
    max_hops: usize,
    window: usize,
) -> Result<Option<(Vec<Raw>, usize)>> {
    let mut fwd: Vec<Raw> = Vec::new();
    let mut bwd: Vec<Raw> = Vec::new();
    let mut fwd_done: HashSet<u32> = HashSet::new();
    let mut bwd_done: HashSet<u32> = HashSet::new();
    // hop-bounded earliest arrivals from the source, latest starts to the target
    let mut ea: HashMap<u32, i64> = HashMap::from([(src, t_a)]);
    let mut ls: HashMap<u32, i64> = HashMap::from([(dst, i64::MAX)]);
    let mut ff: Vec<u32> = vec![src];
    let mut bf: Vec<u32> = vec![dst];
    let (mut f, mut b) = (0usize, 0usize);
    let (mut fpv, mut bpv): (Option<f64>, Option<f64>) = (None, None);
    let mut read = 0usize;
    let mut cols: Vec<Option<PlanCols<'a>>> = vec![None; plans.len()];
    while f + b < max_hops {
        let pv = |own: Option<f64>, other: Option<f64>| own.or(other).unwrap_or(1.0);
        let est_f = ff.len() as f64 * pv(fpv, bpv);
        let est_b = bf.len() as f64 * pv(bpv, fpv);
        // the target's in-edges are always needed; otherwise the cheaper side
        let backward = b == 0 || est_b < est_f;
        let (front, est) = if backward { (&mut bf, est_b) } else { (&mut ff, est_f) };
        if !front.is_empty() {
            let want = read + est.ceil() as usize;
            if want > WHOLE_MIN_ROWS && want * WHOLE_FRACTION > window {
                return Ok(None);
            }
            front.sort_unstable();
            let fset: HashSet<u32> = front.iter().copied().collect();
            let mut walked = 0usize;
            for (k, p) in plans.iter().enumerate() {
                let Some(ranges) = set.plan_incident_rows(p, &front[..], !backward)? else {
                    return Ok(None);
                };
                if ranges.is_empty() {
                    continue;
                }
                if cols[k].is_none() {
                    cols[k] = Some(plan_cols(set, p)?);
                }
                let c = cols[k].as_ref().expect("filled above");
                walked += if backward {
                    take_rows(c, k, &ranges, src, dst, t_a, t_b, &|_, v| fset.contains(&v), &mut bwd)
                } else {
                    take_rows(c, k, &ranges, src, dst, t_a, t_b, &|u, _| fset.contains(&u), &mut fwd)
                };
            }
            read += walked;
            let per = walked as f64 / front.len() as f64;
            if backward {
                bpv = Some(per);
                bwd_done.extend(front.drain(..));
            } else {
                fpv = Some(per);
                fwd_done.extend(front.drain(..));
            }
        }
        // one more hop of reachability over the edges known so far
        if backward {
            b += 1;
            let mut next = ls.clone();
            for r in &bwd {
                if let Some(&l) = ls.get(&r.to) {
                    let c = (r.m - 1).min(l);
                    if r.vt_s <= c {
                        let e = next.entry(r.from).or_insert(i64::MIN);
                        *e = (*e).max(c);
                    }
                }
            }
            ls = next;
            bf = ls.keys().copied().filter(|v| *v != src && !bwd_done.contains(v)).collect();
        } else {
            f += 1;
            let mut next = ea.clone();
            for r in &fwd {
                if let Some(&a) = ea.get(&r.from) {
                    let tau = a.max(r.vt_s);
                    if tau < r.m {
                        let e = next.entry(r.to).or_insert(i64::MAX);
                        *e = (*e).min(tau);
                    }
                }
            }
            ea = next;
            ff = ea.keys().copied().filter(|v| *v != dst && !fwd_done.contains(v)).collect();
        }
    }
    // an edge both sides read once: keep the forward copy
    fwd.extend(bwd.into_iter().filter(|r| !fwd_done.contains(&r.from)));
    Ok(Some((fwd, read)))
}

/// `GNOMON_PATHS_ROUNDS=0` reads the window for every search (the A/B control
/// of the rounds); read once per process.
fn rounds_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("GNOMON_PATHS_ROUNDS").map_or(true, |v| v.trim() != "0"))
}

impl NativeStore {
    /// `temporal_paths` from dense id `src` to `dst` inside `[t_a, t_b)` as
    /// believed at `as_of_tt`: the total and the first `k` paths of at most
    /// `max_hops` edges, or `exceeded` once the walk has examined `budget`
    /// edges. `None` inside an open write batch, whose staged rows the
    /// segment scan does not see -- the caller keeps its own path there.
    #[allow(clippy::too_many_arguments)]
    pub fn temporal_paths(
        &self,
        src: u32,
        dst: u32,
        t_a: i64,
        t_b: i64,
        as_of_tt: i64,
        k: usize,
        max_hops: usize,
        budget: u64,
    ) -> Result<Option<PathsOut>> {
        self.temporal_paths_via(src, dst, t_a, t_b, as_of_tt, k, max_hops, budget, rounds_enabled())
    }

    /// [`Self::temporal_paths`] with the rounds on or off: off reads the
    /// window once; both give one answer.
    #[allow(clippy::too_many_arguments)]
    pub fn temporal_paths_via(
        &self,
        src: u32,
        dst: u32,
        t_a: i64,
        t_b: i64,
        as_of_tt: i64,
        k: usize,
        max_hops: usize,
        budget: u64,
        rounds: bool,
    ) -> Result<Option<PathsOut>> {
        if self.in_batch() {
            return Ok(None);
        }
        if max_hops > PATHS_MAX_HOPS {
            return Err(EngineError::invariant(format!(
                "temporal_paths: max_hops {max_hops} is past {PATHS_MAX_HOPS}"
            )));
        }
        let n = self.dict().len() as usize;
        for v in [src, dst] {
            if v as usize >= n {
                return Err(EngineError::invariant(format!("dense id {v} is not in the dictionary")));
            }
        }
        if src == dst || max_hops == 0 || t_b <= t_a {
            return Ok(Some(PathsOut { by_rounds: true, ..Default::default() }));
        }
        let req = ScanRequest {
            as_of_tt: crate::clamp_tt(as_of_tt),
            vt_min: Some(t_a),
            vt_max: Some(t_b),
            ..Default::default()
        };
        // the rounds read the adjacency postings, which the scan set carries
        // when it is built for an incidence filter
        let outer = ScanRequest {
            touching_ids: Some(vec![src, dst]),
            ..req.clone()
        };
        let dict = self.dict();
        self.with_edge_scan(&outer, |set, _| {
            let (plans, _) = set.plan_believed(&req)?;
            let (raws, rows_read, by_rounds) = gather(set, &plans, src, dst, t_a, t_b, max_hops, rounds)?;
            let ident = |r: &Raw| -> Result<(String, String)> {
                let seg = set.segment(plans[r.plan as usize].idx);
                let row = r.row as usize;
                let rel = seg.u16_column("rel_code")?;
                let disc = seg.u32_column("disc_ref")?;
                let strings = seg.strings()?;
                let rel_type = seg.header().rel_types.get(rel[row] as usize).ok_or_else(|| {
                    EngineError::corrupt(format!("rel_code {} has no entry", rel[row])).at_row(r.row)
                })?;
                let uid = |v: u32| {
                    dict.uid(v)
                        .ok_or_else(|| EngineError::invariant(format!("dense id {v} is not in the dictionary")))
                };
                let eid = crate::derive::edge_eid(uid(r.from)?, uid(r.to)?, rel_type, strings.get(disc[row])?);
                Ok((eid.to_hex(), rel_type.clone()))
            };
            let mut eid_of = |r: &Raw| ident(r).map(|x| x.0);
            let found = search(&raws, n, src, dst, t_a, max_hops, k, budget, &mut eid_of)?;
            let mut out = PathsOut {
                total: found.total,
                expansions: found.expansions,
                exceeded: found.exceeded,
                rows_read,
                by_rounds,
                ..Default::default()
            };
            if found.exceeded {
                return Ok(out);
            }
            for (arrival, edges) in &found.best {
                out.arrival.push(*arrival);
                out.hops.push(edges.len() as u32);
                for &e in edges {
                    let r = &raws[e as usize];
                    let (eid, rel_type) = ident(r)?;
                    out.src_id.push(r.from);
                    out.dst_id.push(r.to);
                    out.vt_s.push(r.vt_s);
                    out.eid.push(eid);
                    out.rel_type.push(rel_type);
                }
            }
            Ok(out)
        })
        .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Python operator's search, transcribed: every node-simple path by
    /// depth-first search, all sorted, the first k kept. Edges are
    /// `(u, v, vt_s, vt_e)`; an edge's eid is its index, zero-padded.
    #[allow(clippy::type_complexity)]
    fn reference(
        edges: &[(u32, u32, i64, i64)],
        src: u32,
        dst: u32,
        t_a: i64,
        t_b: i64,
        k: usize,
        h: usize,
    ) -> (u64, Vec<(i64, Vec<usize>)>) {
        #[allow(clippy::too_many_arguments)]
        fn dfs(
            edges: &[(u32, u32, i64, i64)],
            node: u32,
            a: i64,
            dst: u32,
            t_b: i64,
            h: usize,
            visited: &mut Vec<u32>,
            trail: &mut Vec<usize>,
            found: &mut Vec<(i64, Vec<usize>)>,
        ) {
            if node == dst && !trail.is_empty() {
                found.push((a, trail.clone()));
                return;
            }
            if trail.len() == h {
                return;
            }
            for (i, &(u, v, vs, ve)) in edges.iter().enumerate() {
                if u != node || visited.contains(&v) || vs >= t_b {
                    continue;
                }
                let tau = a.max(vs);
                if tau >= ve || tau >= t_b {
                    continue;
                }
                visited.push(v);
                trail.push(i);
                dfs(edges, v, tau, dst, t_b, h, visited, trail, found);
                trail.pop();
                visited.pop();
            }
        }
        let mut found = Vec::new();
        dfs(edges, src, t_a, dst, t_b, h, &mut vec![src], &mut Vec::new(), &mut found);
        let key = |p: &(i64, Vec<usize>)| {
            (p.0, p.1.len(), p.1.iter().map(|&i| (edges[i].2, format!("{i:08}"))).collect::<Vec<_>>())
        };
        found.sort_by_key(key);
        let total = found.len() as u64;
        found.truncate(k);
        (total, found)
    }

    /// The kernel over the same edges: the window's rows as the window read
    /// gathers them (one plan, row = edge index).
    #[allow(clippy::too_many_arguments)]
    fn kernel(
        edges: &[(u32, u32, i64, i64)],
        n: usize,
        src: u32,
        dst: u32,
        t_a: i64,
        t_b: i64,
        k: usize,
        h: usize,
    ) -> Found {
        let mut raws = Vec::new();
        for (i, &(u, v, vs, ve)) in edges.iter().enumerate() {
            if u == v || v == src || u == dst {
                continue;
            }
            if let Some(m) = bound(vs, ve, t_a, t_b) {
                raws.push(Raw { from: u, to: v, vt_s: vs, m, plan: 0, row: i as u32 });
            }
        }
        let mut eid_of = |r: &Raw| Ok(format!("{:08}", r.row));
        search(&raws, n, src, dst, t_a, h, k, u64::MAX, &mut eid_of).unwrap()
    }

    fn random_edges(n: u32, m: usize, seed: u64, tie_heavy: bool) -> Vec<(u32, u32, i64, i64)> {
        let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        (0..m)
            .map(|_| {
                // a skewed endpoint law: node 0 and 1 are hubs
                let pick = |r: u64| -> u32 {
                    match r % 10 {
                        0..=2 => 0,
                        3 => 1,
                        _ => (r / 10 % n as u64) as u32,
                    }
                };
                let u = pick(next());
                let v = pick(next());
                let vs = if tie_heavy { (next() % 8) as i64 } else { (next() % 200) as i64 - 20 };
                let ve = if next() % 3 == 0 { vs + 1 } else { vs + 1 + (next() % 60) as i64 };
                (u, v, vs, ve)
            })
            .collect()
    }

    #[test]
    fn counts_and_the_best_k_equal_the_enumeration() {
        for seed in 0..60u64 {
            let n = 12 + (seed as u32 % 5) * 6;
            let edges = random_edges(n, 60 + seed as usize * 3, seed, seed % 3 == 0);
            let (t_a, t_b) = ((seed % 10) as i64, 60 + (seed % 90) as i64);
            for h in 1..=4usize {
                for &(s, d) in &[(0u32, 1u32), (1, 0), (2, 0), (0, 3), (3, 4), (5, 5)] {
                    for k in [1usize, 3, 20] {
                        let (total, best) = reference(&edges, s, d, t_a, t_b, k, h);
                        let got = kernel(&edges, n as usize, s, d, t_a, t_b, k, h);
                        assert_eq!(got.total, total, "seed {seed} h {h} {s}->{d} k {k}");
                        let got_best: Vec<(i64, Vec<usize>)> = got
                            .best
                            .iter()
                            .map(|(a, es)| (*a, es.iter().map(|&r| r as usize).collect()))
                            .collect();
                        // the kernel's rows index its gathered list; map back
                        let mut raws_rows = Vec::new();
                        for (i, &(u, v, vs, ve)) in edges.iter().enumerate() {
                            if u == v || v == s || u == d {
                                continue;
                            }
                            if bound(vs, ve, t_a, t_b).is_some() {
                                raws_rows.push(i);
                            }
                        }
                        let got_best: Vec<(i64, Vec<usize>)> = got_best
                            .into_iter()
                            .map(|(a, es)| (a, es.into_iter().map(|r| raws_rows[r]).collect()))
                            .collect();
                        assert_eq!(got_best, best, "seed {seed} h {h} {s}->{d} k {k}");
                    }
                }
            }
        }
    }

    #[test]
    fn the_budget_stops_the_walk_and_says_so() {
        let edges = random_edges(10, 400, 7, false);
        let mut raws = Vec::new();
        for (i, &(u, v, vs, ve)) in edges.iter().enumerate() {
            if u == v || v == 0 || u == 1 {
                continue;
            }
            if let Some(m) = bound(vs, ve, 0, 1000) {
                raws.push(Raw { from: u, to: v, vt_s: vs, m, plan: 0, row: i as u32 });
            }
        }
        let mut eid_of = |r: &Raw| Ok(format!("{:08}", r.row));
        let all = search(&raws, 10, 0, 1, 0, 4, 3, u64::MAX, &mut eid_of).unwrap();
        assert!(!all.exceeded && all.expansions > 10);
        let cut = search(&raws, 10, 0, 1, 0, 4, 3, all.expansions - 1, &mut eid_of).unwrap();
        assert!(cut.exceeded);
        let exact = search(&raws, 10, 0, 1, 0, 4, 3, all.expansions, &mut eid_of).unwrap();
        assert!(!exact.exceeded);
        assert_eq!(exact.total, all.total);
    }
}
