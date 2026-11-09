//! The warm open (`GNOMON_STORE_WARM_OPEN`, on by default; `0` keeps the lazy
//! path).
//!
//! A read-only handle used to build every store-wide, query-independent
//! structure on the first query that needed it, one at a time and on one
//! thread: the decoded segment cache (each segment's checksum walk and column
//! decode), the store statistics (`stats_accum`, which decoded every edge
//! segment a second time, uncached), the `match` compiler's label counts, the
//! adjacency index per edge segment, the identity postings per node segment
//! (folded from a second, uncached decode), the `match` executor's
//! per-segment label and relationship row lists, and
//! on a fresh process's first call, against 1-3 s for its second.
//!
//! This builds the same structures once at open, over the segments in
//! parallel, from one decode of each segment: the segment is opened through
//! the session cache and every per-segment structure is derived from that
//! decoded copy while the worker holds it. The store-wide results (statistics,
//! label counts, end counts) are folded from the per-segment parts. The node
//! identity postings are left to the first point read, as on the lazy path
//! (see `warm_open_inner` for why).
//!
//! Nothing here is a query result. Every structure is one the lazy path
//! builds on first use, with the same contents, through the same derivation
//! (the shared functions below and in `read.rs` / `matcher.rs`); answers,
//! plans and store files are unchanged, and nothing is written (the handle is
//! read-only). Every cache keeps its own budget: a segment or postings entry
//! the budget cannot hold is evicted or dropped exactly as on the lazy path.
//! A failure here is swallowed: the lazy path builds (and reports) the same
//! structure at first use.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;

use crate::error::Result;
use crate::row::RowKind;
use crate::store::{NativeStore, StatsAccum, StatsSummary};
use crate::OPEN_END;

/// `GNOMON_STORE_WARM_OPEN`: anything but `0` (or `off`, `false`) warms a
/// read-only handle at open. Read at every open, so a process can open one
/// store each way.
pub fn warm_open_on() -> bool {
    match std::env::var("GNOMON_STORE_WARM_OPEN") {
        Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "off" | "false"),
        Err(_) => true,
    }
}

/// Where the warm open spent its time (microseconds), and what it built.
#[derive(Debug, Default, Clone, Copy)]
pub struct WarmPhases {
    /// Worker threads.
    pub threads: u64,
    /// Segments warmed (node + edge).
    pub segments: u64,
    /// The per-segment pass: open (checksum + decode), statistics part,
    /// adjacency index, postings, row lists.
    pub segments_us: u64,
    /// Summed per worker: segment open (checksum walk + decode).
    pub open_us: u64,
    /// Summed per worker: adjacency index builds/loads.
    pub tcsr_us: u64,
    /// Summed per worker: identity postings folds.
    pub postings_us: u64,
    /// Summed per worker: statistics parts, label and relationship rows.
    pub derive_us: u64,
    /// Folding the statistics parts into the accumulator.
    pub stats_us: u64,
    /// Folding the label counts.
    pub labels_us: u64,
    /// `match_rel_ends` over the cached segments.
    pub rel_ends_us: u64,
    /// The whole warm open.
    pub total_us: u64,
    /// Segment-cache evictions during the warm open (budget pressure).
    pub evictions: u64,
    /// Every step completed; `false` leaves the rest to the lazy path.
    pub complete: bool,
}

/// Whether and how the warm open builds the node identity postings
/// (`GNOMON_STORE_WARM_POSTINGS`: unset = no, `cached`, `uncached`).
#[derive(Clone, Copy)]
enum Postings {
    Off,
    Cached,
    Uncached,
}

/// One segment's share of `stats_accum`.
struct EdgeStatsPart {
    rows: u64,
    vt_min: Option<i64>,
    vt_max: Option<i64>,
    /// per relationship name, rows (only names with rows)
    rel_counts: Vec<(String, u64)>,
}

struct Shared {
    /// per source dense id, its out-degree (edge versions it is the source of)
    out_degree: Vec<AtomicU32>,
    /// sources outside the dense array (none on a well-formed store)
    out_extra: Mutex<HashMap<u32, u64>>,
    open_us: AtomicUsize,
    tcsr_us: AtomicUsize,
    postings_us: AtomicUsize,
    derive_us: AtomicUsize,
    failed: AtomicBool,
}

fn us_since(t: std::time::Instant) -> usize {
    t.elapsed().as_micros() as usize
}

impl NativeStore {
    /// Build the store-wide, query-independent structures now (see the
    /// module documentation). Never fails: an error leaves the remaining
    /// structures to the lazy path, and `complete` says so.
    pub fn warm_open(&self) -> WarmPhases {
        let t0 = std::time::Instant::now();
        let mut ph = WarmPhases::default();
        let ok = self.warm_open_inner(&mut ph).is_ok();
        ph.complete = ok && ph.complete;
        ph.total_us = t0.elapsed().as_micros() as u64;
        ph
    }

    fn warm_open_inner(&self, ph: &mut WarmPhases) -> Result<()> {
        if self.in_batch() {
            return Ok(());
        }
        let m = self.manifest();
        // (kind, file, id, rows), largest first so the tail is short
        let mut items: Vec<(RowKind, String, u64, u32)> = Vec::new();
        for e in m.edge_lanes.event.iter().chain(m.edge_lanes.interval.iter()) {
            items.push((RowKind::Edge, e.file.clone(), crate::store::segment_id_of(&e.file), e.rows));
        }
        for e in m.node_store.iter() {
            items.push((RowKind::Node, e.file.clone(), crate::store::segment_id_of(&e.file), e.rows));
        }
        let edge_rows: u64 = items.iter().filter(|i| i.0 == RowKind::Edge).map(|i| i.3 as u64).sum();
        // the dense out-degree array counts in u32: a store with more edge
        // versions than that leaves the statistics to the lazy path
        let stats_here = edge_rows < u32::MAX as u64;
        let mut order: Vec<usize> = (0..items.len()).collect();
        order.sort_by_key(|&i| std::cmp::Reverse(items[i].3));
        let threads = std::env::var("GNOMON_STORE_WARM_THREADS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(|| self.designs().match_threads())
            .clamp(1, items.len().max(1));
        ph.threads = threads as u64;
        ph.segments = items.len() as u64;
        let n_ids = if stats_here { self.dict().len() as usize } else { 0 };
        let shared = Shared {
            out_degree: (0..n_ids).map(|_| AtomicU32::new(0)).collect(),
            out_extra: Mutex::new(HashMap::new()),
            open_us: AtomicUsize::new(0),
            tcsr_us: AtomicUsize::new(0),
            postings_us: AtomicUsize::new(0),
            derive_us: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
        };
        let adjacency = self.designs().adjacency_index;
        // the node identity postings stay with the first point read, as on
        // the lazy path. Built here they left later warm calls of BI4 current
        // 9-19 % slower with 25-45 % more minor faults per call, whether
        // folded from the cached segment (`cached`) or from a second,
        // uncached decode (`uncached`) (rec/tm/dx4, dx5, dx8, dx10); the
        // price of leaving them is the first point read's fold (BI4 current
        // at SF10: about 25 s of its first call).
        let postings = match std::env::var("GNOMON_STORE_WARM_POSTINGS").as_deref().map(str::trim) {
            Ok("cached") => Postings::Cached,
            Ok("uncached") => Postings::Uncached,
            _ => Postings::Off,
        };
        let evictions_before = self.segment_cache_stats().3;
        let next = AtomicUsize::new(0);
        let t = std::time::Instant::now();
        // per item: its statistics part (edges) or its label rows (nodes)
        let parts: Vec<Mutex<Option<EdgeStatsPart>>> = (0..items.len()).map(|_| Mutex::new(None)).collect();
        let labels: Vec<Mutex<Option<crate::matcher::LabelRows>>> =
            (0..items.len()).map(|_| Mutex::new(None)).collect();
        std::thread::scope(|sc| {
            // the dictionary's uid index (a parallel load leaves it unbuilt),
            // beside the segment pass: nothing in the pass looks a uid up
            sc.spawn(|| self.dict().prepare_index());
            for _ in 0..threads {
                sc.spawn(|| loop {
                    let k = next.fetch_add(1, Relaxed);
                    if k >= order.len() || shared.failed.load(Relaxed) {
                        break;
                    }
                    let i = order[k];
                    let (kind, file, id, _) = &items[i];
                    let r = self.warm_segment(*kind, file, *id, stats_here, (adjacency, postings), &shared);
                    match r {
                        Ok((part, lab)) => {
                            *parts[i].lock().expect("warm part mutex") = part;
                            *labels[i].lock().expect("warm labels mutex") = lab;
                        }
                        Err(_) => shared.failed.store(true, Relaxed),
                    }
                });
            }
        });
        ph.segments_us = t.elapsed().as_micros() as u64;
        ph.open_us = shared.open_us.load(Relaxed) as u64;
        ph.tcsr_us = shared.tcsr_us.load(Relaxed) as u64;
        ph.postings_us = shared.postings_us.load(Relaxed) as u64;
        ph.derive_us = shared.derive_us.load(Relaxed) as u64;
        ph.evictions = self.segment_cache_stats().3 - evictions_before;
        if shared.failed.load(Relaxed) {
            return Ok(());
        }

        // the statistics `stats()` serves, as `stats_accum` folds them and
        // `StatsSummary::of` summarises them: kept as the generation's
        // summary. The per-source out-degree map itself is not built (its
        // maximum is all a summary reads, and a reader never folds a commit
        // into it); `stats_accum` still builds it on the lazy path if asked.
        let t = std::time::Instant::now();
        let gen = self.generation();
        let have = matches!(
            self.stats_summary_cell().lock().expect("stats summary mutex poisoned").as_ref(),
            Some((g, _)) if *g == gen
        ) || self.stats_cell().lock().expect("stats mutex poisoned").is_some();
        // a handle with staged rows (never a reader) keeps the lazy path
        let staged = self.staged_edges().into_iter().next().is_some() || !self.staged_nodes().is_empty();
        if stats_here && !have && !staged {
            let mut acc = StatsAccum::default();
            for p in parts.into_iter().filter_map(|p| p.into_inner().expect("warm part mutex")) {
                acc.n_edge_versions += p.rows;
                if let Some(v) = p.vt_min {
                    acc.vt_min = Some(acc.vt_min.map_or(v, |m| m.min(v)));
                }
                if let Some(v) = p.vt_max {
                    acc.vt_max = Some(acc.vt_max.map_or(v, |m| m.max(v)));
                }
                for (name, n) in p.rel_counts {
                    *acc.rel_type_counts.entry(name).or_default() += n;
                }
            }
            let extra = shared.out_extra.into_inner().expect("warm extra mutex");
            let dense_max = par_max(&shared.out_degree, ph.threads as usize);
            let max_out_degree = dense_max.max(extra.values().copied().max().unwrap_or(0));
            acc.n_node_versions = self.manifest().node_store.iter().map(|e| e.rows as u64).sum::<u64>();
            let summary = StatsSummary {
                n_node_versions: acc.n_node_versions,
                n_edge_versions: acc.n_edge_versions,
                vt_min: acc.vt_min,
                vt_max: acc.vt_max,
                rel_type_counts: acc.rel_type_counts,
                max_out_degree,
            };
            if !self.in_batch() {
                *self.stats_summary_cell().lock().expect("stats summary mutex poisoned") =
                    Some((gen, std::sync::Arc::new(summary)));
            }
        }
        ph.stats_us = t.elapsed().as_micros() as u64;

        // the label counts, from the label rows (the same per-label row
        // counts `match_label_counts` folds from the label column)
        let t = std::time::Instant::now();
        let mut counts: HashMap<String, u64> = HashMap::new();
        for l in labels.into_iter().filter_map(|l| l.into_inner().expect("warm labels mutex")) {
            for (label, rows) in l.iter() {
                if !rows.is_empty() {
                    *counts.entry(label.clone()).or_default() += rows.len() as u64;
                }
            }
        }
        self.warm_label_counts(counts);
        ph.labels_us = t.elapsed().as_micros() as u64;

        let t = std::time::Instant::now();
        self.match_rel_ends()?;
        ph.rel_ends_us = t.elapsed().as_micros() as u64;
        ph.complete = true;
        Ok(())
    }

    /// One segment: opened through the cache (checksum walk on first touch,
    /// column decode), then every per-segment structure derived from it.
    fn warm_segment(
        &self,
        kind: RowKind,
        file: &str,
        id: u64,
        stats_here: bool,
        (adjacency, postings): (bool, Postings),
        sh: &Shared,
    ) -> Result<(Option<EdgeStatsPart>, Option<crate::matcher::LabelRows>)> {
        let t = std::time::Instant::now();
        let seg = self.open_segment(file)?;
        sh.open_us.fetch_add(us_since(t), Relaxed);
        match kind {
            RowKind::Edge => {
                let t = std::time::Instant::now();
                let part = if stats_here { Some(edge_stats_part(&seg, sh)?) } else { None };
                self.warm_match_edge_segment(id, &seg)?;
                sh.derive_us.fetch_add(us_since(t), Relaxed);
                if adjacency && seg.rows() > 0 {
                    let t = std::time::Instant::now();
                    self.tcsr_index(file)?;
                    sh.tcsr_us.fetch_add(us_since(t), Relaxed);
                }
                Ok((part, None))
            }
            RowKind::Node => {
                let t = std::time::Instant::now();
                let lab = self.warm_match_node_segment(id, &seg)?;
                sh.derive_us.fetch_add(us_since(t), Relaxed);
                let t = std::time::Instant::now();
                match postings {
                    Postings::Off => {}
                    Postings::Cached => {
                        self.warm_segment_postings(RowKind::Node, id, file, &seg)?;
                    }
                    Postings::Uncached => {
                        drop(seg);
                        self.with_segment_postings(RowKind::Node, id, file, |_| ())?;
                    }
                }
                sh.postings_us.fetch_add(us_since(t), Relaxed);
                Ok((None, Some(lab)))
            }
        }
    }
}

/// The largest count in `v`, on `threads` threads.
fn par_max(v: &[AtomicU32], threads: usize) -> u64 {
    let width = v.len().div_ceil(threads.max(1)).max(1);
    std::thread::scope(|sc| {
        let hs: Vec<_> = v
            .chunks(width)
            .map(|c| sc.spawn(move || c.iter().map(|x| x.load(Relaxed)).max().unwrap_or(0)))
            .collect();
        hs.into_iter().map(|h| h.join().expect("warm max worker")).max().unwrap_or(0) as u64
    })
}

/// `stats_accum`'s per-segment fold, over the cached segment: the same
/// columns and the same arithmetic per row; out-degrees go to the shared
/// dense array.
fn edge_stats_part(seg: &crate::segment::Segment<crate::segment::MmapSource>, sh: &Shared) -> Result<EdgeStatsPart> {
    let h = seg.header();
    let vt_s = seg.i64_column("vt_s")?;
    let vt_e = if h.vt_e_elided { None } else { Some(seg.i64_column("vt_e")?) };
    let src = seg.u32_column("src_id")?;
    let rel = seg.u16_column("rel_code")?;
    let mut code_counts = vec![0u64; h.rel_types.len()];
    let (mut vt_min, mut vt_max): (Option<i64>, Option<i64>) = (None, None);
    let mut extra: Vec<u32> = Vec::new();
    for i in 0..vt_s.len() {
        let ve = vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1);
        vt_min = Some(vt_min.map_or(vt_s[i], |m| m.min(vt_s[i])));
        let ve = if ve >= OPEN_END { vt_s[i] + 1 } else { ve };
        vt_max = Some(vt_max.map_or(ve, |m| m.max(ve)));
        if let Some(c) = code_counts.get_mut(rel[i] as usize) {
            *c += 1;
        }
        match sh.out_degree.get(src[i] as usize) {
            Some(c) => {
                c.fetch_add(1, Relaxed);
            }
            None => extra.push(src[i]),
        }
    }
    if !extra.is_empty() {
        let mut m = sh.out_extra.lock().expect("warm extra mutex");
        for s in extra {
            *m.entry(s).or_default() += 1;
        }
    }
    let rel_counts = code_counts
        .into_iter()
        .enumerate()
        .filter(|&(_, n)| n > 0)
        .map(|(code, n)| (h.rel_types[code].clone(), n))
        .collect();
    Ok(EdgeStatsPart { rows: vt_s.len() as u64, vt_min, vt_max, rel_counts })
}
