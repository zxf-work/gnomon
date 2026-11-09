//! `temporal_reachability`'s earliest-arrival search, in the engine.
//!
//! # The question
//!
//! From a source node, at time `t_a`, which nodes can a time-respecting path
//! reach inside `[t_a, t_b)`, and how early? An edge version `u -> v` with
//! valid interval `[vt_s, vt_e)` is traversable from `u` at arrival `a` iff
//! `tau = max(a, vt_s)` satisfies `tau < vt_e` and `tau < t_b`; the arrival at
//! `v` is then `tau` (`gnomon/temporal/ops_paths.py`, the operational
//! definition the oracle shares).
//!
//! # Why a label-setting search is exact
//!
//! The Python operator iterates a vectorised fixpoint: every round relaxes
//! every window edge from the previous round's arrivals until nothing moves.
//! Its answer is the least fixed point of
//! `arr[v] = min over traversable u -> v of max(arr[u], vt_s)`, with
//! `arr[src] = t_a`. Two properties of the edge function `f(a) = max(a, vt_s)`
//! (defined when `f(a) < min(vt_e, t_b)`) make Dijkstra's order exact for it:
//!
//! * **it never decreases an arrival**: `f(a) >= a`, so once the smallest
//!   tentative arrival is taken off the heap no later relaxation can undercut
//!   it — the label is final;
//! * **it is monotone, and so is its domain**: `a1 <= a2` gives `f(a1) <=
//!   f(a2)`, and an edge traversable at `a2` is traversable at `a1`. So the
//!   earliest arrival at a node is the only one worth expanding: any path
//!   leaving it later can be matched or beaten by one leaving earlier.
//!
//! The `delta_max_wait` case breaks the second property (a smaller arrival
//! can make a wait too long), which is why it keeps the Python multi-label
//! search and never reaches this module.
//!
//! # Cost
//!
//! One planned scan of the window's believed rows — the match plan's ranges
//! ([`crate::scan::ScanSet::plan_believed`]), never a selection vector — into
//! a by-source CSR, then a heap search that touches only the reached part of
//! it, then a selection of the requested page by `(arrival, uid)` and a sort
//! of that page alone. Strings are built for the page's rows only.

use crate::error::{EngineError, Result};
use crate::scan::ScanRequest;
use crate::store::NativeStore;
use crate::OPEN_END;

/// Which way edges are walked: `Out` follows `src -> dst`, `In` walks
/// `dst -> src` with the same non-decreasing times away from the source (so
/// it finds paths to the source whose times do not increase towards it, not
/// every time-respecting path to it), `Both` does either.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReachDir {
    Out,
    In,
    Both,
}

impl ReachDir {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "out" => Ok(ReachDir::Out),
            "in" => Ok(ReachDir::In),
            "both" => Ok(ReachDir::Both),
            other => Err(EngineError::invariant(format!(
                "reachability direction must be out|in|both, got {other:?}"
            ))),
        }
    }
}

/// One page of the answer, ordered by `(arrival, uid)`, and the total.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReachOut {
    pub ids: Vec<u32>,
    pub uids: Vec<String>,
    pub arrival: Vec<i64>,
    /// Reached nodes, the source excluded.
    pub total: usize,
}

/// One traversable edge direction, as the search reads it: everything one
/// relaxation needs in one place, so a node's edges are one contiguous run.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Arc {
    vt_s: i64,
    vt_e: i64,
    to: u32,
}

/// The window's traversable edges, keyed by the node they leave.
pub(crate) struct Csr {
    /// `off[u]..off[u + 1]` indexes `arcs` for node `u`.
    off: Vec<u32>,
    arcs: Vec<Arc>,
}

/// Where a CSR's edges come from: a number of chunks, each of which can be
/// walked any number of times and yields `(src, dst, vt_s, vt_e)` in the
/// same order every time. The build walks each chunk twice (count, fill).
pub(crate) trait EdgeSource: Sync {
    fn chunks(&self) -> usize;
    /// Rows the chunks hold in all (an upper bound on the edges `each`
    /// yields): what the build sizes its worker count by.
    fn rows(&self) -> usize;
    fn each<F: FnMut(u32, u32, i64, i64)>(&self, chunk: usize, f: F) -> Result<()>;
}

/// A raw pointer the fill pass shares between workers that write disjoint
/// indices of one buffer.
#[derive(Clone, Copy)]
struct Shared(*mut Arc);
// SAFETY: every index is written by exactly one worker (see `Csr::build`).
unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Shared {
    /// Through a method, so a closure captures the `Sync` wrapper and not
    /// (by edition-2021 disjoint capture) the raw pointer field inside it.
    #[inline]
    fn ptr(&self) -> *mut Arc {
        self.0
    }
}

/// Rows below which one more CSR worker does not pay: each
/// worker's count array is `n` entries however few rows it counts, and the
/// offsets pass walks every one of them, so a worker with a few thousand
/// rows costs more in that pass than it saves in the count and the fill.
const CSR_MIN_ROWS_PER_WORKER: usize = 1 << 16;

/// Nodes per tile of the offsets pass: one tile of every
/// worker's counts (16 x 4 KiB at most) plus its running bases stays in L1/L2
/// while the tile is summed, scanned and assigned.
const CSR_OFFSET_TILE: usize = 1024;

impl Csr {
    /// [`Csr::build_with`] at [`CSR_MIN_ROWS_PER_WORKER`].
    fn build<E: EdgeSource>(n: usize, src: &E, dir: ReachDir, width: usize) -> Result<Self> {
        Self::build_with(n, src, dir, width, CSR_MIN_ROWS_PER_WORKER)
    }

    /// A parallel counting sort of the source's edges by the node they leave
    /// (both endpoints for `Both`), over `n` nodes, on up to `width` workers.
    ///
    /// Chunks are dealt to workers in fixed contiguous blocks, each worker
    /// counts its own chunks, the per-worker counts give every worker a
    /// disjoint write cursor per node, and the fill walks the same chunks in
    /// the same order — so every slot of `arcs` is written exactly once, and
    /// the layout does not depend on scheduling. (The search's answer would
    /// not depend on the order of a node's edges anyway.)
    ///
    /// The layout is also independent of the worker count: chunks are dealt
    /// in order and a node's run takes worker 0's arcs, then worker 1's, and
    /// so on, so every node's arcs come out in chunk order, row order within
    /// a chunk — a stable counting sort by key, whatever `width` is. The
    /// build leans on that twice: workers are capped at one per
    /// `min_rows_per_worker` rows, and the offsets pass is computed in tiles
    /// instead of node by node across every worker's counts.
    fn build_with<E: EdgeSource>(
        n: usize,
        src: &E,
        dir: ReachDir,
        width: usize,
        min_rows_per_worker: usize,
    ) -> Result<Self> {
        let chunks = src.chunks();
        let workers = width
            .clamp(1, 16)
            .min(chunks.max(1))
            .min(src.rows().div_ceil(min_rows_per_worker.max(1)).max(1));
        let block = chunks.div_ceil(workers).max(1);
        let mine = |w: usize| (w * block).min(chunks)..((w + 1) * block).min(chunks);
        let keys = |s: u32, d: u32| -> [Option<u32>; 2] {
            match dir {
                ReachDir::Out => [Some(s), None],
                ReachDir::In => [Some(d), None],
                ReachDir::Both => [Some(s), Some(d)],
            }
        };
        // pass 1: per-worker counts
        let counts: Vec<Result<Vec<u32>>> = crate::pool::run(workers, |w| {
            let mut c = vec![0u32; n];
            let mut bad: Option<u32> = None;
            for k in mine(w) {
                src.each(k, |s, d, _, _| {
                    for key in keys(s, d).into_iter().flatten() {
                        match c.get_mut(key as usize) {
                            Some(x) => *x += 1,
                            None => bad = Some(key),
                        }
                    }
                })?;
            }
            match bad {
                Some(k) => Err(EngineError::corrupt(format!(
                    "edge endpoint {k} is past the dictionary ({n} entities)"
                ))),
                None => Ok(c),
            }
        });
        let mut counts: Vec<Vec<u32>> = counts.into_iter().collect::<Result<_>>()?;
        let (off, total) = offsets_and_cursors(&mut counts, n)?;
        // each worker's cursors, uncontended: worker `w` locks only its own
        let cursors: Vec<std::sync::Mutex<Vec<u32>>> =
            counts.into_iter().map(std::sync::Mutex::new).collect();
        let mut arcs = vec![Arc::default(); total as usize];
        let out = Shared(arcs.as_mut_ptr());
        let len = arcs.len();
        // pass 2: the fill, every worker through its own cursors
        let filled: Vec<Result<()>> = crate::pool::run(workers, |w| {
            let mut cur = cursors[w].lock().unwrap_or_else(|e| e.into_inner());
            for k in mine(w) {
                src.each(k, |s, d, vt_s, vt_e| {
                    let pairs: [(Option<u32>, u32); 2] = match dir {
                        ReachDir::Out => [(Some(s), d), (None, 0)],
                        ReachDir::In => [(Some(d), s), (None, 0)],
                        ReachDir::Both => [(Some(s), d), (Some(d), s)],
                    };
                    for (key, to) in pairs {
                        let Some(key) = key else { continue };
                        let at = cur[key as usize] as usize;
                        cur[key as usize] += 1;
                        assert!(at < len, "CSR cursor past its node's run");
                        // SAFETY: `at` lies in this worker's own run for
                        // `key`, `[base_w(key), base_w(key) + count_w(key))`,
                        // which no other worker's cursor ever enters, and the
                        // cursor advances past each slot once.
                        unsafe { out.ptr().add(at).write(Arc { vt_s, vt_e, to }) };
                    }
                })?;
            }
            Ok(())
        });
        for f in filled {
            f?;
        }
        Ok(Self { off, arcs })
    }

    /// Earliest arrival per node from `src` at `t_a`, `OPEN_END` where
    /// unreached. Label-setting over a monotone radix heap (arrivals never
    /// decrease, see the module note); stale entries are skipped on pop.
    fn earliest(&self, src: u32, t_a: i64, t_b: i64) -> Vec<i64> {
        let n = self.off.len() - 1;
        let mut arr = vec![OPEN_END; n];
        if (src as usize) >= n {
            return arr;
        }
        arr[src as usize] = t_a;
        let mut heap = RadixHeap::new();
        heap.push(0, src);
        while let Some((k, u)) = heap.pop() {
            let a = t_a + k as i64;
            if a > arr[u as usize] {
                continue; // a stale entry: a better label was set since
            }
            let run = &self.arcs[self.off[u as usize] as usize..self.off[u as usize + 1] as usize];
            for e in run {
                let tau = a.max(e.vt_s);
                if tau >= e.vt_e || tau >= t_b {
                    continue;
                }
                let v = e.to as usize;
                if tau < arr[v] {
                    arr[v] = tau;
                    heap.push((tau - t_a) as u64, e.to);
                }
            }
        }
        arr
    }
}

/// The CSR's offsets, and each worker's first slot per node in place of its
/// count: node `u`'s run starts at `off[u]`, and inside it worker `w`'s arcs
/// start after workers `0..w`'s. Returns `(off, total)`.
///
/// **Tiled, worker-major.** The natural loop — for each node,
/// for each worker — reads one entry from each of the per-worker count
/// arrays per node, i.e. up to 16 streams at once. When those arrays happen
/// to sit at the same address modulo 64 KiB, which depends on the process's
/// allocation history and not on the data, every stream maps to the same
/// cache sets and the pass ran 16-20 ms instead of 2-5. Here
/// each tile of [`CSR_OFFSET_TILE`] nodes is summed over the workers one
/// array at a time, prefix-scanned, then assigned one array at a time: at
/// most two streams at once, whatever the arrays' addresses. Every slot gets
/// the same value the node-by-node loop gave it.
fn offsets_and_cursors(counts: &mut [Vec<u32>], n: usize) -> Result<(Vec<u32>, u64)> {
    let mut off = vec![0u32; n + 1];
    let mut total: u64 = 0;
    let mut base = vec![0u64; CSR_OFFSET_TILE.min(n)];
    let mut lo = 0;
    while lo < n {
        let hi = (lo + CSR_OFFSET_TILE).min(n);
        let base = &mut base[..hi - lo];
        // per-node sums over the workers
        base.fill(0);
        for c in counts.iter() {
            for (b, &x) in base.iter_mut().zip(&c[lo..hi]) {
                *b += x as u64;
            }
        }
        // the scan: the node's offset, and its first worker's first slot
        for (i, b) in base.iter_mut().enumerate() {
            off[lo + i] = total as u32;
            let here = *b;
            *b = total;
            total += here;
        }
        // `total` only grows, so checking at the tile's end refuses exactly
        // the windows the per-node check refused
        if total >= u32::MAX as u64 {
            return Err(EngineError::capacity(format!(
                "reachability window holds over {} edge directions, past the CSR's u32 index",
                u32::MAX
            )));
        }
        // each worker's first slot per node, worker by worker
        for c in counts.iter_mut() {
            for (b, x) in base.iter_mut().zip(&mut c[lo..hi]) {
                let here = *x as u64;
                *x = *b as u32;
                *b += here;
            }
        }
        lo = hi;
    }
    off[n] = total as u32;
    Ok((off, total))
}

/// A monotone priority queue on `u64` keys (a radix heap): every pushed key
/// is at least the last popped one, which label-setting guarantees
/// (`tau >= a`). A key lives in the bucket of the highest bit in which it
/// differs from the last popped key, so a pop redistributes one bucket into
/// strictly lower ones and every element moves at most 64 times — in
/// practice a handful, against a binary heap's `log n` sift per operation.
struct RadixHeap {
    last: u64,
    len: usize,
    buckets: Vec<Vec<(u64, u32)>>,
}

impl RadixHeap {
    fn new() -> Self {
        Self {
            last: 0,
            len: 0,
            buckets: (0..65).map(|_| Vec::new()).collect(),
        }
    }

    #[inline]
    fn bucket(x: u64) -> usize {
        if x == 0 {
            0
        } else {
            64 - x.leading_zeros() as usize
        }
    }

    #[inline]
    fn push(&mut self, k: u64, v: u32) {
        debug_assert!(k >= self.last, "radix heap keys must not decrease");
        self.buckets[Self::bucket(k ^ self.last)].push((k, v));
        self.len += 1;
    }

    fn pop(&mut self) -> Option<(u64, u32)> {
        if self.len == 0 {
            return None;
        }
        if self.buckets[0].is_empty() {
            let i = (1..65).find(|&i| !self.buckets[i].is_empty()).expect("len > 0");
            let moved = std::mem::take(&mut self.buckets[i]);
            self.last = moved.iter().map(|e| e.0).min().expect("non-empty bucket");
            for (k, v) in &moved {
                self.buckets[Self::bucket(k ^ self.last)].push((*k, *v));
            }
            // keep the allocation for the next refill
            let mut moved = moved;
            moved.clear();
            if self.buckets[i].is_empty() {
                self.buckets[i] = moved;
            }
        }
        self.len -= 1;
        self.buckets[0].pop()
    }
}

/// The earliest arrivals by growing a frontier from the source instead of
/// building the window's CSR: each round fetches the window's
/// believed edges incident to the newly reached nodes from the window's
/// plan (its believed, window-clipped ranges) through the adjacency postings,
/// keeps the arcs leaving them, and reruns the label-setting search over
/// the arcs gathered so far. When a round reaches nothing new, every
/// reached node's arcs are known, and the search over them is the search
/// over the whole window restricted to what the source can reach -- the
/// same least fixed point, so the same answer. A source with no arc in
/// the window costs one filtered scan.
///
/// `None` once the search outgrows [`FRONTIER_MAX_NODES`] expanded nodes,
/// [`FRONTIER_MAX_ARCS`] arcs or [`FRONTIER_MAX_ROUNDS`] rounds: then the
/// CSR, built once over the window, is the cheaper path, and the caller
/// takes it.
#[allow(clippy::too_many_arguments)]
fn reach_frontier<S: crate::segment::SegmentSource>(
    set: &crate::scan::ScanSet<'_, S>,
    plans: &[crate::scan::SegPlan<'_>],
    src: u32,
    t_a: i64,
    t_b: i64,
    dir: ReachDir,
    n: usize,
    arc_budget: usize,
) -> Result<Option<Vec<i64>>> {
    use std::collections::{HashMap, HashSet};
    if (src as usize) >= n {
        return Ok(Some(vec![OPEN_END; n]));
    }
    let mut adj: HashMap<u32, Vec<Arc>> = HashMap::new();
    let mut expanded: HashSet<u32> = HashSet::new();
    let mut frontier: Vec<u32> = vec![src];
    let mut arcs_total = 0usize;
    let mut arr: HashMap<u32, i64> = HashMap::new();
    let (max_nodes, max_arcs) = frontier_budget();
    let max_arcs = max_arcs.min(arc_budget);
    // `Out` needs only the rows leaving the frontier; the others both ends
    let src_only = dir == ReachDir::Out;
    for _round in 0..FRONTIER_MAX_ROUNDS {
        if frontier.is_empty() {
            let mut out = vec![OPEN_END; n];
            for (&v, &a) in &arr {
                out[v as usize] = a;
            }
            out[src as usize] = t_a;
            return Ok(Some(out));
        }
        if expanded.len() + frontier.len() > max_nodes {
            return Ok(None);
        }
        frontier.sort_unstable();
        let fset: HashSet<u32> = frontier.iter().copied().collect();
        let t = std::time::Instant::now();
        for p in plans {
            // the plan's believed, window-clipped rows incident to the
            // frontier, through the adjacency postings; a segment without
            // postings would mean scanning it every round: take the CSR
            let Some(ranges) = set.plan_incident_rows(p, &frontier, src_only)? else {
                return Ok(None);
            };
            if ranges.is_empty() {
                continue;
            }
            let seg = set.segment(p.idx);
            let s = seg.u32_column("src_id")?;
            let d = seg.u32_column("dst_id")?;
            let vs = seg.i64_column("vt_s")?;
            for (a, b) in ranges {
                for i in a..b {
                    let (vs, ve) = (vs[i], p.vt_e.map_or(vs[i] + 1, |c| c[i]));
                    if ve <= t_a {
                        continue; // never traversable: tau >= t_a (as the CSR's source skips)
                    }
                    let mut add = |key: u32, to: u32| {
                        if fset.contains(&key) {
                            adj.entry(key).or_default().push(Arc { vt_s: vs, vt_e: ve, to });
                            arcs_total += 1;
                        }
                    };
                    match dir {
                        ReachDir::Out => add(s[i], d[i]),
                        ReachDir::In => add(d[i], s[i]),
                        ReachDir::Both => {
                            add(s[i], d[i]);
                            add(d[i], s[i]);
                        }
                    }
                }
            }
        }
        crate::phase::since(crate::phase::Phase::ReachEdges, t);
        if arcs_total > max_arcs {
            return Ok(None);
        }
        // a source that leaves on many arcs reaches far: its search outgrows
        // the budget after another round or two, each of them paid for
        // nothing, so it takes the CSR now
        if expanded.is_empty() && arcs_total > FRONTIER_FIRST_ARCS {
            return Ok(None);
        }
        expanded.extend(frontier.drain(..));
        arr = earliest_sparse(&adj, src, t_a, t_b);
        frontier = arr.keys().copied().filter(|v| !expanded.contains(v)).collect();
    }
    Ok(None)
}

/// The frontier budget: past any of these the window's CSR is cheaper than
/// another filtered scan per round. Arcs are also capped at one per
/// [`FRONTIER_ROWS_PER_ARC`] rows the window holds (at least
/// [`FRONTIER_MIN_ARCS`]): a fetched arc costs several times what one row
/// costs the CSR build, so an abandoned frontier stays a small fraction of
/// the build it falls back to.
const FRONTIER_ROWS_PER_ARC: usize = 64;
const FRONTIER_MIN_ARCS: usize = 256;
const FRONTIER_MAX_NODES: usize = 128;
const FRONTIER_MAX_ARCS: usize = 1 << 16;
const FRONTIER_MAX_ROUNDS: usize = 12;
/// Arcs leaving the source past which the frontier gives up after its first
/// round (`reach_frontier`).
const FRONTIER_FIRST_ARCS: usize = 4;

/// The budget in force: the constants above, or `GNOMON_REACH_FRONTIER_BUDGET=
/// nodes,arcs` (read once per process; for measuring the trade-off).
fn frontier_budget() -> (usize, usize) {
    static B: std::sync::OnceLock<(usize, usize)> = std::sync::OnceLock::new();
    *B.get_or_init(|| {
        std::env::var("GNOMON_REACH_FRONTIER_BUDGET")
            .ok()
            .and_then(|v| {
                let (a, b) = v.split_once(',')?;
                Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
            })
            .unwrap_or((FRONTIER_MAX_NODES, FRONTIER_MAX_ARCS))
    })
}

/// `GNOMON_REACH_FRONTIER=0` builds the window's CSR for every search, as
/// before the frontier search existed (the paired control); read once per
/// process.
fn frontier_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("GNOMON_REACH_FRONTIER").map_or(true, |v| v.trim() != "0"))
}

/// [`Csr::earliest`] over a sparse arc map: the same label-setting search,
/// the same traversal rule, over the arcs the frontier has gathered.
fn earliest_sparse(
    adj: &std::collections::HashMap<u32, Vec<Arc>>,
    src: u32,
    t_a: i64,
    t_b: i64,
) -> std::collections::HashMap<u32, i64> {
    // A binary heap, not the radix heap of the CSR search: this search is
    // small, and a second caller of `RadixHeap` changed how the compiler
    // inlined it into `Csr::earliest`, the hot loop of every search the
    // frontier hands over (that loop measured 8-11 % slower).
    use std::cmp::Reverse;
    let mut arr: std::collections::HashMap<u32, i64> = std::collections::HashMap::new();
    arr.insert(src, t_a);
    let mut heap = std::collections::BinaryHeap::new();
    heap.push(Reverse((t_a, src)));
    while let Some(Reverse((a, u))) = heap.pop() {
        if a > arr[&u] {
            continue;
        }
        let Some(run) = adj.get(&u) else { continue };
        for e in run {
            let tau = a.max(e.vt_s);
            if tau >= e.vt_e || tau >= t_b {
                continue;
            }
            let best = arr.get(&e.to).copied().unwrap_or(OPEN_END);
            if tau < best {
                arr.insert(e.to, tau);
                heap.push(Reverse((tau, e.to)));
            }
        }
    }
    arr
}

/// The planner's believed, window-clipped ranges as an edge source: no
/// intermediate copy of the window's edges is ever made. Interval rows that
/// end by `t_a` are skipped (`tau >= t_a` always, so `vt_e <= t_a` is never
/// traversable; the scan's overlap test drops exactly those too).
/// `(src_id, dst_id, vt_s, vt_e or elided)` of one planned segment.
type PlanCols<'a> = (&'a [u32], &'a [u32], &'a [i64], Option<&'a [i64]>);

struct PlannedEdges<'a> {
    cols: Vec<PlanCols<'a>>,
    /// `(plan, row lo, row hi)`, each at most `PLAN_CHUNK` rows
    pieces: Vec<(u32, usize, usize)>,
    /// rows over all pieces
    rows: usize,
    t_a: i64,
}

const PLAN_CHUNK: usize = 1 << 16;

impl EdgeSource for PlannedEdges<'_> {
    fn chunks(&self) -> usize {
        self.pieces.len()
    }
    fn rows(&self) -> usize {
        self.rows
    }
    fn each<F: FnMut(u32, u32, i64, i64)>(&self, chunk: usize, mut f: F) -> Result<()> {
        let (p, a, b) = self.pieces[chunk];
        let (src, dst, vt_s, vt_e) = self.cols[p as usize];
        match vt_e {
            None => {
                for i in a..b {
                    f(src[i], dst[i], vt_s[i], vt_s[i] + 1);
                }
            }
            Some(vt_e) => {
                for i in a..b {
                    if vt_e[i] > self.t_a {
                        f(src[i], dst[i], vt_s[i], vt_e[i]);
                    }
                }
            }
        }
        Ok(())
    }
}

impl NativeStore {
    /// Earliest-arrival reachability from dense id `src` inside `[t_a, t_b)`
    /// as believed at `as_of_tt`: the page `[offset, offset + limit)` of the
    /// reached nodes (the source excluded) ordered by `(arrival, uid)`, plus
    /// the total. `None` inside an open write batch, whose staged rows the
    /// segment scan below does not see — the caller keeps its own path there.
    #[allow(clippy::too_many_arguments)]
    pub fn reach_earliest(
        &self,
        src: u32,
        t_a: i64,
        t_b: i64,
        as_of_tt: i64,
        dir: ReachDir,
        offset: usize,
        limit: usize,
    ) -> Result<Option<ReachOut>> {
        self.reach_earliest_via(src, t_a, t_b, as_of_tt, dir, offset, limit, frontier_enabled())
    }

    /// [`Self::reach_earliest`] with the frontier start on or off:
    /// off builds the window's CSR first, as before; both give one answer.
    #[allow(clippy::too_many_arguments)]
    pub fn reach_earliest_via(
        &self,
        src: u32,
        t_a: i64,
        t_b: i64,
        as_of_tt: i64,
        dir: ReachDir,
        offset: usize,
        limit: usize,
        frontier: bool,
    ) -> Result<Option<ReachOut>> {
        use crate::phase::{since, Phase};
        if self.in_batch() {
            return Ok(None);
        }
        let n = self.dict().len() as usize;
        let req = ScanRequest {
            as_of_tt: crate::clamp_tt(as_of_tt),
            vt_min: Some(t_a),
            vt_max: Some(t_b),
            ..Default::default()
        };
        // the frontier's scans need the adjacency postings, which the scan
        // set carries when it is built for an incidence filter; the CSR's
        // plan below never reads them (it names no incidence)
        let outer = if frontier {
            ScanRequest {
                touching_ids: Some(vec![src]),
                ..req.clone()
            }
        } else {
            req.clone()
        };
        let arr = self.with_edge_scan(&outer, |set, _| {
            let t = std::time::Instant::now();
            let (plans, _) = set.plan_believed(&req)?;
            // Grow from the source first, within a budget scaled to the
            // window: a search that outgrows it pays for the window's CSR
            // after all, having spent at most a small fraction of it
            if frontier {
                let rows: usize = plans.iter().map(|p| p.rows).sum();
                let budget = (rows / FRONTIER_ROWS_PER_ARC).max(FRONTIER_MIN_ARCS);
                let t = std::time::Instant::now();
                let small = reach_frontier(set, &plans, src, t_a, t_b, dir, n, budget)?;
                since(Phase::ReachSearch, t);
                if let Some(arr) = small {
                    return Ok(arr);
                }
            }
            let mut source = PlannedEdges {
                cols: Vec::with_capacity(plans.len()),
                pieces: Vec::new(),
                rows: 0,
                t_a,
            };
            for (k, p) in plans.iter().enumerate() {
                let seg = set.segment(p.idx);
                source.cols.push((
                    seg.u32_column("src_id")?,
                    seg.u32_column("dst_id")?,
                    seg.i64_column("vt_s")?,
                    p.vt_e,
                ));
                for &(a, b) in &p.ranges {
                    let mut lo = a;
                    while lo < b {
                        let hi = (lo + PLAN_CHUNK).min(b);
                        source.pieces.push((k as u32, lo, hi));
                        source.rows += hi - lo;
                        lo = hi;
                    }
                }
            }
            since(Phase::ReachEdges, t);
            let t = std::time::Instant::now();
            let csr = Csr::build(n, &source, dir, crate::scan::scan_threads())?;
            since(Phase::ReachCsr, t);
            let t = std::time::Instant::now();
            let arr = csr.earliest(src, t_a, t_b);
            since(Phase::ReachSearch, t);
            Ok(arr)
        })?;
        let t = std::time::Instant::now();
        let page = self.reach_page(&arr, src, offset, limit)?;
        since(Phase::ReachPage, t);
        Ok(Some(page))
    }

    /// The `(arrival, uid)`-ordered page of the reached nodes: a selection to
    /// the page's end and a sort of the page, never of the whole answer.
    fn reach_page(&self, arr: &[i64], src: u32, offset: usize, limit: usize) -> Result<ReachOut> {
        let dict = self.dict();
        let uid = |v: u32| -> Result<&str> {
            dict.uid(v)
                .ok_or_else(|| EngineError::invariant(format!("dense id {v} is not in the dictionary")))
        };
        let mut reached: Vec<(i64, &str, u32)> = Vec::new();
        for (v, &a) in arr.iter().enumerate() {
            if a < OPEN_END && v as u32 != src {
                reached.push((a, uid(v as u32)?, v as u32));
            }
        }
        let total = reached.len();
        let end = offset.saturating_add(limit).min(total);
        if offset >= end {
            return Ok(ReachOut { total, ..Default::default() });
        }
        // `(arrival, uid)` is a total order: uids are unique per dense id
        let ord = |x: &(i64, &str, u32), y: &(i64, &str, u32)| (x.0, x.1).cmp(&(y.0, y.1));
        if end < total {
            reached.select_nth_unstable_by(end - 1, ord);
            reached.truncate(end);
        }
        reached.sort_unstable_by(ord);
        let page = &reached[offset..end];
        Ok(ReachOut {
            ids: page.iter().map(|r| r.2).collect(),
            uids: page.iter().map(|r| r.1.to_string()).collect(),
            arrival: page.iter().map(|r| r.0).collect(),
            total,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::manifest::EventLogRef;
    use crate::row::{EdgeRow, RowKind};
    use std::path::PathBuf;

    /// Edges as four parallel columns: the tests' source. Chunks are tiny on
    /// purpose, so every build below is a many-worker build (through
    /// `build_with(.., 1)`: the default build caps workers by rows).
    struct EdgeList {
        src: Vec<u32>,
        dst: Vec<u32>,
        vt_s: Vec<i64>,
        vt_e: Vec<i64>,
    }

    const LIST_CHUNK: usize = 37;

    impl EdgeSource for EdgeList {
        fn chunks(&self) -> usize {
            self.src.len().div_ceil(LIST_CHUNK)
        }
        fn rows(&self) -> usize {
            self.src.len()
        }
        fn each<F: FnMut(u32, u32, i64, i64)>(&self, chunk: usize, mut f: F) -> Result<()> {
            let lo = chunk * LIST_CHUNK;
            for i in lo..(lo + LIST_CHUNK).min(self.src.len()) {
                f(self.src[i], self.dst[i], self.vt_s[i], self.vt_e[i]);
            }
            Ok(())
        }
    }

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-reach-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// The Python operator's fixpoint, transcribed: every round relaxes every
    /// edge from the previous round's arrivals until nothing moves. The
    /// reference the label-setting search is held to.
    fn fixpoint(n: usize, edges: &[(u32, u32, i64, i64)], src: u32, t_a: i64, t_b: i64) -> Vec<i64> {
        let mut arr = vec![OPEN_END; n];
        arr[src as usize] = t_a;
        loop {
            let mut new = arr.clone();
            for &(u, v, vs, ve) in edges {
                let a = arr[u as usize];
                if a >= OPEN_END {
                    continue;
                }
                let tau = a.max(vs);
                if tau < ve && tau < t_b && tau < new[v as usize] {
                    new[v as usize] = tau;
                }
            }
            if new == arr {
                return arr;
            }
            arr = new;
        }
    }

    fn list(edges: &[(u32, u32, i64, i64)]) -> EdgeList {
        EdgeList {
            src: edges.iter().map(|e| e.0).collect(),
            dst: edges.iter().map(|e| e.1).collect(),
            vt_s: edges.iter().map(|e| e.2).collect(),
            vt_e: edges.iter().map(|e| e.3).collect(),
        }
    }

    /// A deterministic pseudo-random temporal graph: instants and intervals,
    /// self-loops, parallel edges, edges into the source, edges that open
    /// before `t_a` and close after `t_b`.
    fn random_edges(n: u32, m: usize, seed: u64) -> Vec<(u32, u32, i64, i64)> {
        let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        (0..m)
            .map(|_| {
                let u = (next() % n as u64) as u32;
                let v = (next() % n as u64) as u32;
                let vs = (next() % 200) as i64 - 20;
                let ve = if next() % 3 == 0 { vs + 1 } else { vs + 1 + (next() % 60) as i64 };
                (u, v, vs, ve)
            })
            .collect()
    }

    #[test]
    fn label_setting_equals_the_fixpoint_in_every_direction() {
        for seed in 0..40u64 {
            let n = 50 + (seed as u32 % 7) * 20;
            let edges = random_edges(n, 400 + seed as usize * 13, seed);
            let (t_a, t_b) = ((seed % 30) as i64, 100 + (seed % 70) as i64);
            for (dir, view) in [
                (ReachDir::Out, edges.clone()),
                (ReachDir::In, edges.iter().map(|&(u, v, s, e)| (v, u, s, e)).collect()),
                (
                    ReachDir::Both,
                    edges.iter().flat_map(|&(u, v, s, e)| [(u, v, s, e), (v, u, s, e)]).collect(),
                ),
            ] {
                let csr = Csr::build_with(n as usize, &list(&edges), dir, 1 + (seed as usize % 5), 1).unwrap();
                for src in [0u32, 1, n / 2, n - 1] {
                    let got = csr.earliest(src, t_a, t_b);
                    let want = fixpoint(n as usize, &view, src, t_a, t_b);
                    assert_eq!(got, want, "seed {seed} {dir:?} src {src} [{t_a},{t_b})");
                }
            }
        }
    }

    /// The serial reference layout — a stable counting sort of
    /// the edge directions by key, in edge order.
    fn serial_layout(n: usize, edges: &[(u32, u32, i64, i64)], dir: ReachDir) -> (Vec<u32>, Vec<Arc>) {
        let mut by: Vec<Vec<Arc>> = vec![Vec::new(); n];
        for &(s, d, vt_s, vt_e) in edges {
            let pairs: [(Option<u32>, u32); 2] = match dir {
                ReachDir::Out => [(Some(s), d), (None, 0)],
                ReachDir::In => [(Some(d), s), (None, 0)],
                ReachDir::Both => [(Some(s), d), (Some(d), s)],
            };
            for (key, to) in pairs {
                if let Some(key) = key {
                    by[key as usize].push(Arc { vt_s, vt_e, to });
                }
            }
        }
        let mut off = vec![0u32; n + 1];
        let mut arcs = Vec::new();
        for (u, run) in by.into_iter().enumerate() {
            off[u] = arcs.len() as u32;
            arcs.extend(run);
        }
        off[n] = arcs.len() as u32;
        (off, arcs)
    }

    /// The tiled offsets pass and the row cap leave the arc
    /// layout exactly the serial reference's, at every worker width, in every
    /// direction, across tile boundaries (n past `CSR_OFFSET_TILE`, not a
    /// multiple of it), with the cap off and on.
    #[test]
    fn the_arc_layout_equals_the_serial_reference_at_every_worker_width() {
        for (seed, n) in [(1u64, 1u32), (2, 7), (3, 1023), (4, 1024), (5, 1025), (6, 2 * 1024 + 517)] {
            let edges = random_edges(n, 6000 + seed as usize * 101, seed);
            for dir in [ReachDir::Out, ReachDir::In, ReachDir::Both] {
                let (off, arcs) = serial_layout(n as usize, &edges, dir);
                for width in [1usize, 2, 3, 5, 8, 16, 32] {
                    for min_rows in [1usize, 500, CSR_MIN_ROWS_PER_WORKER] {
                        let csr = Csr::build_with(n as usize, &list(&edges), dir, width, min_rows).unwrap();
                        assert_eq!(csr.off, off, "n={n} {dir:?} width={width} min_rows={min_rows}");
                        assert!(csr.arcs == arcs, "n={n} {dir:?} width={width} min_rows={min_rows}");
                    }
                }
            }
        }
        // no edges at all, over a node space that spans tiles
        let csr = Csr::build_with(3000, &list(&[]), ReachDir::Both, 16, 1).unwrap();
        assert_eq!(csr.off, vec![0u32; 3001]);
        assert!(csr.arcs.is_empty());
    }

    /// The tiled offsets pass gives every slot the value the
    /// node-by-node pass gave it, for any number of count arrays.
    #[test]
    fn tiled_offsets_equal_the_node_by_node_pass() {
        let mut x = 0x9e3779b97f4a7c15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for (n, workers) in [(0usize, 1usize), (1, 1), (5, 3), (1024, 16), (1025, 4), (4099, 16)] {
            let counts: Vec<Vec<u32>> =
                (0..workers).map(|_| (0..n).map(|_| (next() % 5) as u32).collect()).collect();
            // the node-by-node loop
            let mut want = counts.clone();
            let mut want_off = vec![0u32; n + 1];
            let mut t: u64 = 0;
            for u in 0..n {
                want_off[u] = t as u32;
                for c in want.iter_mut() {
                    let here = c[u];
                    c[u] = t as u32;
                    t += here as u64;
                }
            }
            want_off[n] = t as u32;
            let mut got = counts;
            let (off, total) = offsets_and_cursors(&mut got, n).unwrap();
            assert_eq!((off, total), (want_off, t), "n={n} workers={workers}");
            assert_eq!(got, want, "n={n} workers={workers}");
        }
    }

    #[test]
    fn the_traversal_rule_is_tau_against_vt_e_and_t_b() {
        // 0 -> 1 opens at 5; 1 -> 2 closed at 5 (an arrival at 5 misses it);
        // 1 -> 3 opens at 9 (wait, then go); 3 -> 4 opens at t_b (too late)
        let e = [(0, 1, 5, 6), (1, 2, 0, 5), (1, 3, 9, 10), (3, 4, 20, 21), (0, 5, -10, 0)];
        let csr = Csr::build_with(6, &list(&e), ReachDir::Out, 4, 1).unwrap();
        let arr = csr.earliest(0, 0, 20);
        assert_eq!(arr, vec![0, 5, OPEN_END, 9, OPEN_END, OPEN_END]);
        // an edge that closed exactly at t_a is not traversable
        let csr = Csr::build_with(2, &list(&[(0, 1, -5, 0)]), ReachDir::Out, 1, 1).unwrap();
        assert_eq!(csr.earliest(0, 0, 10), vec![0, OPEN_END]);
    }

    #[allow(clippy::too_many_arguments)]
    fn row(src_id: u32, dst_id: u32, su: &str, du: &str, i: u32, vt_s: i64, vt_e: i64, tt: i64) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid(su, du, "R", &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt, vt_s),
            src_id,
            dst_id,
            rel_type: "R".into(),
            disc,
            vt_s,
            vt_e,
            tt_s: tt,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// The store path end to end: the scan's believed rows (a correction
    /// closes an edge, so belief time matters), the CSR, the search, and the
    /// `(arrival, uid)` page — against the fixpoint over the same rows and
    /// every page boundary.
    #[test]
    fn the_store_answer_is_the_fixpoint_over_believed_rows_paged_by_arrival_then_uid() {
        let root = tmp_root("store");
        let mut s = NativeStore::open(&root).unwrap();
        let n = 60u32;
        let raw = random_edges(n, 900, 7);
        s.begin(100).unwrap();
        let ids: Vec<u32> = (0..n)
            // uids whose byte order is not their id order
            .map(|k| s.ensure_entity(&format!("u{}", (k * 37) % 101), "Node").unwrap())
            .collect();
        let mut rows = Vec::new();
        for (i, &(u, v, vs, ve)) in raw.iter().enumerate() {
            let (su, du) = (format!("u{}", (u * 37) % 101), format!("u{}", (v * 37) % 101));
            let r = row(ids[u as usize], ids[v as usize], &su, &du, i as u32, vs, ve, 100);
            rows.push(r.clone());
            s.stage_edge(r).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        // close every 4th edge at tt=200: believed at 150, not at 250
        s.begin(200).unwrap();
        for r in rows.iter().step_by(4) {
            s.close_version_at(RowKind::Edge, r.vid, Some(r.vt_s), 200).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();

        let (t_a, t_b) = (10, 150);
        for (as_of, keep) in [(150i64, None), (250, Some(4usize)), (OPEN_END, Some(4))] {
            let believed: Vec<(u32, u32, i64, i64)> = rows
                .iter()
                .enumerate()
                .filter(|(i, _)| keep.is_none_or(|k| i % k != 0))
                .map(|(_, r)| (r.src_id, r.dst_id, r.vt_s, r.vt_e))
                .filter(|&(_, _, vs, ve)| ve > t_a && vs < t_b)
                .collect();
            for dir in [ReachDir::Out, ReachDir::In, ReachDir::Both] {
                let view: Vec<_> = match dir {
                    ReachDir::Out => believed.clone(),
                    ReachDir::In => believed.iter().map(|&(u, v, a, b)| (v, u, a, b)).collect(),
                    ReachDir::Both => believed
                        .iter()
                        .flat_map(|&(u, v, a, b)| [(u, v, a, b), (v, u, a, b)])
                        .collect(),
                };
                let src = ids[3];
                let arr = fixpoint(s.dict().len() as usize, &view, src, t_a, t_b);
                let mut want: Vec<(i64, String)> = arr
                    .iter()
                    .enumerate()
                    .filter(|(v, a)| **a < OPEN_END && *v as u32 != src)
                    .map(|(v, a)| (*a, s.dict().uid(v as u32).unwrap().to_string()))
                    .collect();
                want.sort();
                assert!(want.len() > 10, "the probe must reach something ({})", want.len());
                let total = want.len();
                // both starts: the frontier, and the window's CSR
                for (frontier, (offset, limit)) in [false, true].into_iter().flat_map(|f| {
                    [(0, usize::MAX), (0, 1), (0, 7), (5, 3), (total - 1, 10), (total, 5),
                     (total + 3, 5)].map(|p| (f, p))
                }) {
                    let got = s
                        .reach_earliest_via(src, t_a, t_b, as_of, dir, offset, limit, frontier)
                        .unwrap()
                        .expect("no open batch");
                    let page: Vec<(i64, String)> =
                        got.arrival.iter().copied().zip(got.uids.iter().cloned()).collect();
                    let lo = offset.min(total);
                    let hi = offset.saturating_add(limit).min(total);
                    assert_eq!(page, want[lo..hi].to_vec(), "{as_of} {dir:?} {offset}+{limit}");
                    assert_eq!(got.total, total);
                    for (id, u) in got.ids.iter().zip(&got.uids) {
                        assert_eq!(s.dict().uid(*id), Some(u.as_str()));
                    }
                }
            }
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_open_batch_declines() {
        let root = tmp_root("batch");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(5).unwrap();
        let a = s.ensure_entity("a", "Node").unwrap();
        assert_eq!(s.reach_earliest(a, 0, 10, OPEN_END, ReachDir::Out, 0, 10).unwrap(), None);
        let _ = std::fs::remove_dir_all(&root);
    }
}
