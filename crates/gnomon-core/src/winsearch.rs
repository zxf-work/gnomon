//! The window form of `paths`' least-cost search: label setting over
//! (node, instant set) states, as `gnomon/temporal/ops_general_paths.py`'s
//! `_win_search` does it, statement for statement, so its pieces are that
//! function's. Costs are exact: integers scaled by a common power of two
//! (the caller's float weights are dyadic rationals), each with whether a
//! fractional weight is on its path (the Python side prints such a cost as
//! a float). Nodes are the caller's local indices `0..n`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Sorted, disjoint half-open intervals.
pub type Ivs = Vec<(i64, i64)>;

fn ivs_norm(mut v: Ivs) -> Ivs {
    v.retain(|x| x.0 < x.1);
    v.sort_unstable();
    let mut out: Ivs = Vec::with_capacity(v.len());
    for (s, e) in v {
        if let Some(last) = out.last_mut() {
            if s <= last.1 {
                if e > last.1 {
                    last.1 = e;
                }
                continue;
            }
        }
        out.push((s, e));
    }
    out
}

fn ivs_and(a: &[(i64, i64)], b: &[(i64, i64)]) -> Ivs {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let (s, e) = (a[i].0.max(b[j].0), a[i].1.min(b[j].1));
        if s < e {
            out.push((s, e));
        }
        if a[i].1 < b[j].1 {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn ivs_sub(a: &[(i64, i64)], b: &[(i64, i64)]) -> Ivs {
    let mut out = Vec::new();
    let mut j = 0;
    for &(s, e) in a {
        let mut cur = s;
        while j < b.len() && b[j].1 <= cur {
            j += 1;
        }
        let mut k = j;
        while k < b.len() && b[k].0 < e {
            if b[k].0 > cur {
                out.push((cur, b[k].0));
            }
            cur = cur.max(b[k].1);
            if cur >= e {
                break;
            }
            k += 1;
        }
        if cur < e {
            out.push((cur, e));
        }
    }
    out
}

fn ivs_or(a: &[(i64, i64)], b: &[(i64, i64)]) -> Ivs {
    let mut v = a.to_vec();
    v.extend_from_slice(b);
    ivs_norm(v)
}

/// The graph: CSR steps per node (to, lo, hi, weight, fractional), and per
/// node its window-clipped existence.
pub struct WinGraph<'a> {
    pub off: &'a [u32],
    pub to: &'a [u32],
    pub lo: &'a [i64],
    pub hi: &'a [i64],
    pub w: &'a [i128],
    pub frac: &'a [bool],
    pub ex_off: &'a [u32],
    pub ex_lo: &'a [i64],
    pub ex_hi: &'a [i64],
}

impl WinGraph<'_> {
    fn exists(&self, u: u32) -> Ivs {
        let (a, b) = (self.ex_off[u as usize] as usize, self.ex_off[u as usize + 1] as usize);
        (a..b).map(|i| (self.ex_lo[i], self.ex_hi[i])).collect()
    }
}

/// One settled piece: the node, the instants where its least cost is `cost`,
/// and whether a fractional weight is on that path.
pub struct Piece {
    pub node: u32,
    pub ivs: Ivs,
    pub cost: i128,
    pub frac: bool,
}

struct Entry {
    d: i128,
    seq: u64,
    u: u32,
    frac: bool,
    k: Ivs,
}

impl PartialEq for Entry {
    fn eq(&self, o: &Self) -> bool {
        self.d == o.d && self.seq == o.seq
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Entry {
    // a min-heap on (cost, push order), as Python's heapq on (d, seq, ...)
    fn cmp(&self, o: &Self) -> Ordering {
        (o.d, o.seq).cmp(&(self.d, self.seq))
    }
}

/// The search's options, as `_win_search` takes them.
pub struct WinSearch<'a> {
    pub source: u32,
    pub va: i64,
    pub vb: i64,
    pub max_hops: Option<i128>,
    pub targets: &'a [u32],
    pub bound: Option<i128>,
    pub first_only: bool,
    pub budget: usize,
}

/// The pieces in settling order, or `Err(expansions)` past the budget.
pub fn win_search(g: &WinGraph<'_>, o: &WinSearch<'_>) -> Result<Vec<Piece>, usize> {
    let n = g.off.len() - 1;
    let start = ivs_and(&[(o.va, o.vb)], &g.exists(o.source));
    let mut pieces = Vec::new();
    if start.is_empty() {
        return Ok(pieces);
    }
    let is_target = {
        let mut m = vec![false; n];
        for &t in o.targets {
            m[t as usize] = true;
        }
        m
    };
    let n_targets = {
        let mut t = o.targets.to_vec();
        t.sort_unstable();
        t.dedup();
        t.len()
    };
    let mut first: Vec<Option<i128>> = vec![None; n];
    let mut n_first = 0usize;
    let mut first_max: i128 = i128::MIN;
    let mut settled: Vec<Ivs> = vec![Vec::new(); n];
    let mut heap = BinaryHeap::new();
    heap.push(Entry { d: 0, seq: 0, u: o.source, frac: false, k: start });
    let mut seq: u64 = 1;
    let mut budget = 0usize;
    let mut bound = o.bound;
    let all_first = |n_first: usize| n_targets > 0 && n_first == n_targets;
    while let Some(top) = heap.peek() {
        if bound.is_some_and(|b| top.d > b) {
            break;
        }
        if all_first(n_first) && top.d > first_max {
            break;
        }
        let Entry { d, u, frac, k, .. } = heap.pop().expect("peeked");
        let k = ivs_sub(&k, &settled[u as usize]);
        if k.is_empty() {
            continue;
        }
        if is_target[u as usize] && first[u as usize].is_none() {
            first[u as usize] = Some(d);
            n_first += 1;
            first_max = first_max.max(d);
            if o.first_only && bound.is_none_or(|b| d < b) {
                bound = Some(d);
            }
        }
        budget += 1;
        if budget > o.budget {
            return Err(budget);
        }
        settled[u as usize] = ivs_or(&settled[u as usize], &k);
        pieces.push(Piece { node: u, ivs: k.clone(), cost: d, frac });
        if o.max_hops.is_some_and(|h| d >= h) {
            continue;
        }
        if bound.is_some_and(|b| d >= b) {
            continue;
        }
        if all_first(n_first) && d >= first_max {
            continue;
        }
        for e in g.off[u as usize] as usize..g.off[u as usize + 1] as usize {
            let k2 = ivs_and(&k, &[(g.lo[e], g.hi[e])]);
            if k2.is_empty() {
                continue;
            }
            let v = g.to[e];
            let k2 = ivs_sub(&ivs_and(&k2, &g.exists(v)), &settled[v as usize]);
            if !k2.is_empty() {
                heap.push(Entry { d: d + g.w[e], seq, u: v, frac: frac || g.frac[e], k: k2 });
                seq += 1;
            }
        }
    }
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_cheaper_only_part_of_the_window_splits_the_instants() {
        // 0 -> 1 -> 2 costs 2 over [0, 10); 0 -> 2 costs 5 over [0, 20)
        let off = [0u32, 2, 3, 3];
        let to = [1u32, 2, 2];
        let lo = [0i64, 0, 0];
        let hi = [10i64, 20, 10];
        let w = [1i128, 5, 1];
        let frac = [false, false, false];
        let ex_off = [0u32, 1, 2, 3];
        let ex_lo = [0i64, 0, 0];
        let ex_hi = [20i64, 20, 20];
        let g = WinGraph { off: &off, to: &to, lo: &lo, hi: &hi, w: &w, frac: &frac, ex_off: &ex_off, ex_lo: &ex_lo, ex_hi: &ex_hi };
        let o = WinSearch { source: 0, va: 0, vb: 20, max_hops: None, targets: &[], bound: None, first_only: false, budget: 1000 };
        let p = win_search(&g, &o).expect("within budget");
        let at2: Vec<(Ivs, i128)> = p.iter().filter(|x| x.node == 2).map(|x| (x.ivs.clone(), x.cost)).collect();
        assert_eq!(at2, vec![(vec![(0, 10)], 2), (vec![(10, 20)], 5)]);
    }
}
