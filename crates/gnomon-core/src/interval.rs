//! Interval-join pair enumeration — the inner loop of `co_active`.
//!
//! The candidate range for each `a` row is already a pair of binary searches
//! over `b`'s starts, which vectorizes fine. What did not was walking those
//! ranges: a Python double loop over ~5.3 s at 1M events, the hotspot the
//! scan probe ranked first. That loop is all this module is.
//!
//! Deliberately narrow. Allen-relation choice, the cost cap, and the
//! self-pair check stay on the Python side: the first two are cheap and
//! vectorized, and the third only has to look at pairs that survive — which
//! the cap already bounds — so none of them belongs in a hot loop, and
//! keeping them out means this kernel needs no identities and no strings.

use crate::error::{EngineError, Result};

/// One page of the interval join and the join's size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PairPage {
    /// Pairs in the whole join (`rows_total`).
    pub total: u64,
    /// The pairs at positions `[offset, offset + limit)` of the join's
    /// `(i, j)` order.
    pub pairs: Vec<(u32, u32)>,
}

/// The interval join's size and one page of it, with no candidate pair ever
/// materialised and no cap.
///
/// The join is [`interval_pairs`]' -- `lo[i]..hi[i]` is `a` row `i`'s
/// candidate range in `b` (clamped to `b`), `require_b_end_after_a_end` adds
/// `b.vt_e > a.vt_e` -- minus the pairs listed in `exclude` (the operator's
/// self pairs), in `(i, j)` order.
///
/// Sizes: without the end predicate a range's size is its length. With it,
/// the count of `j` in `[lo, hi)` with `b_vt_e[j] > a_vt_e[i]` is two
/// prefix counts, and every prefix count comes from one sweep over `b` in
/// row order with a Fenwick tree over the ranks of `b`'s ends:
/// `O((|a| + |b|) log |b|)`, whatever the number of pairs. The page then
/// walks only the ranges it lands in.
#[allow(clippy::too_many_arguments)]
pub fn interval_page(
    lo: &[i64],
    hi: &[i64],
    a_vt_e: &[i64],
    b_vt_e: &[i64],
    require_b_end_after_a_end: bool,
    exclude: &[(u32, u32)],
    offset: u64,
    limit: usize,
) -> Result<PairPage> {
    if lo.len() != hi.len() || lo.len() != a_vt_e.len() {
        return Err(EngineError::invariant(
            "interval join: lo, hi and a_vt_e must have one entry per a-row",
        ));
    }
    let nb = b_vt_e.len() as i64;
    let range = |i: usize| -> (usize, usize) {
        let (s, e) = (lo[i].clamp(0, nb), hi[i].clamp(0, nb));
        (s as usize, e.max(s) as usize)
    };
    let na = lo.len();
    let mut cnt: Vec<u64> = (0..na).map(|i| {
        let (s, e) = range(i);
        (e - s) as u64
    }).collect();
    if require_b_end_after_a_end {
        // #{j < p : b_vt_e[j] <= x} for the two ends of every range, in one
        // sweep of p over b's rows
        let mut ranks: Vec<i64> = b_vt_e.to_vec();
        ranks.sort_unstable();
        ranks.dedup();
        let mut at_pos: Vec<u32> = vec![0; b_vt_e.len() + 2];
        for i in 0..na {
            let (s, e) = range(i);
            if s < e {
                at_pos[s + 1] += 1;
                at_pos[e + 1] += 1;
            }
        }
        for p in 0..=b_vt_e.len() {
            at_pos[p + 1] += at_pos[p];
        }
        let mut queries: Vec<(u32, bool)> = vec![(0, false); at_pos[b_vt_e.len() + 1] as usize];
        let mut fill = at_pos.clone();
        for i in 0..na {
            let (s, e) = range(i);
            if s < e {
                queries[fill[s] as usize] = (i as u32, false);
                fill[s] += 1;
                queries[fill[e] as usize] = (i as u32, true);
                fill[e] += 1;
            }
        }
        let mut tree = vec![0u32; ranks.len() + 1];
        let le = |tree: &[u32], x: i64| -> u64 {
            // ranks <= x, as a Fenwick prefix
            let mut k = ranks.partition_point(|&r| r <= x);
            let mut sum = 0u64;
            while k > 0 {
                sum += tree[k] as u64;
                k &= k - 1;
            }
            sum
        };
        // per range: (rows below its end) - (rows below its start) that end
        // after a's end
        let mut over: Vec<i64> = vec![0; na];
        for p in 0..=b_vt_e.len() {
            for &(i, end) in &queries[at_pos[p] as usize..at_pos[p + 1] as usize] {
                let n_over = (p as u64 - le(&tree, a_vt_e[i as usize])) as i64;
                over[i as usize] += if end { n_over } else { -n_over };
            }
            if p < b_vt_e.len() {
                let mut k = ranks.partition_point(|&r| r < b_vt_e[p]) + 1;
                while k < tree.len() {
                    tree[k] += 1;
                    k += 1 << k.trailing_zeros();
                }
            }
        }
        for (i, c) in cnt.iter_mut().enumerate() {
            let (s, e) = range(i);
            *c = if s < e { over[i] as u64 } else { 0 };
        }
    }
    let keep = |i: usize, j: usize| !require_b_end_after_a_end || b_vt_e[j] > a_vt_e[i];
    let mut excluded: Vec<(u32, u32)> = Vec::new();
    for &(i, j) in exclude {
        let (iu, ju) = (i as usize, j as usize);
        if iu < na {
            let (s, e) = range(iu);
            if s <= ju && ju < e && keep(iu, ju) {
                excluded.push((i, j));
            }
        }
    }
    excluded.sort_unstable();
    excluded.dedup();
    for &(i, _) in &excluded {
        cnt[i as usize] -= 1;
    }
    let total: u64 = cnt.iter().sum();
    let mut pairs = Vec::new();
    let mut skip = offset;
    for (i, &n) in cnt.iter().enumerate() {
        if pairs.len() >= limit {
            break;
        }
        if skip >= n {
            skip -= n;
            continue;
        }
        let (s, e) = range(i);
        for j in s..e {
            if !keep(i, j) || excluded.binary_search(&(i as u32, j as u32)).is_ok() {
                continue;
            }
            if skip > 0 {
                skip -= 1;
                continue;
            }
            pairs.push((i as u32, j as u32));
            if pairs.len() >= limit {
                break;
            }
        }
    }
    Ok(PairPage { total, pairs })
}

/// Enumerate `(a_row, b_row)` candidate pairs.
///
/// `lo[i]..hi[i]` is the candidate range in `b` for `a` row `i`, as produced
/// by the relation's binary searches. `require_b_end_after_a_end` carries the
/// one predicate the range bounds cannot express — `overlaps` and `during`
/// both additionally need `b.vt_e > a.vt_e`.
///
/// Pairs come back in `(i, j)` order. Both inputs arrive in `(vt_s, vid)`
/// order from the columnar scan, so that is already the order the operator
/// contract promises, and no sort is needed.
pub fn interval_pairs(
    lo: &[u32],
    hi: &[u32],
    a_vt_e: &[i64],
    b_vt_e: &[i64],
    require_b_end_after_a_end: bool,
) -> Result<Vec<(u32, u32)>> {
    if lo.len() != hi.len() || lo.len() != a_vt_e.len() {
        return Err(EngineError::invariant(
            "interval join: lo, hi and a_vt_e must have one entry per a-row",
        ));
    }
    let nb = b_vt_e.len() as u32;
    let mut out = Vec::new();
    for i in 0..lo.len() {
        let (start, end) = (lo[i], hi[i].min(nb));
        if start >= end {
            continue;
        }
        let a_end = a_vt_e[i];
        if require_b_end_after_a_end {
            for j in start..end {
                if b_vt_e[j as usize] > a_end {
                    out.push((i as u32, j));
                }
            }
        } else {
            // the range bounds already decided every pair
            out.reserve((end - start) as usize);
            for j in start..end {
                out.push((i as u32, j));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_each_candidate_range() {
        let pairs = interval_pairs(&[0, 2], &[2, 4], &[10, 10], &[0, 0, 0, 0], false).unwrap();
        assert_eq!(pairs, vec![(0, 0), (0, 1), (1, 2), (1, 3)]);
    }

    #[test]
    fn empty_and_inverted_ranges_yield_nothing() {
        assert!(interval_pairs(&[3], &[3], &[1], &[0, 0, 0, 0], false).unwrap().is_empty());
        assert!(interval_pairs(&[3], &[1], &[1], &[0, 0, 0, 0], false).unwrap().is_empty());
        assert!(interval_pairs(&[], &[], &[], &[], false).unwrap().is_empty());
    }

    #[test]
    fn end_predicate_filters_only_when_requested() {
        // b ends: 5, 20 — only the second outlives a's end of 10
        let b_end = [5i64, 20];
        let filtered = interval_pairs(&[0], &[2], &[10], &b_end, true).unwrap();
        assert_eq!(filtered, vec![(0, 1)]);
        let unfiltered = interval_pairs(&[0], &[2], &[10], &b_end, false).unwrap();
        assert_eq!(unfiltered, vec![(0, 0), (0, 1)]);
    }

    #[test]
    fn a_range_running_past_b_is_clamped() {
        // a defensive clamp: a malformed hi must not read out of bounds
        let pairs = interval_pairs(&[0], &[99], &[1], &[0, 0], false).unwrap();
        assert_eq!(pairs, vec![(0, 0), (0, 1)]);
    }

    #[test]
    fn pairs_come_back_in_input_order() {
        let lo = [0u32, 0, 1];
        let hi = [1u32, 3, 3];
        let pairs = interval_pairs(&lo, &hi, &[0, 0, 0], &[0, 0, 0], false).unwrap();
        assert!(pairs.windows(2).all(|w| w[0] < w[1]), "must be ascending");
    }

    /// The page and the total equal the enumeration's, minus the
    /// excluded pairs, at every offset and limit, with and without the end
    /// predicate, over ranges that are empty, inverted, or run past `b`.
    #[test]
    fn the_page_and_the_total_equal_the_enumeration() {
        let mut x = 0x2545f4914f6cdd1du64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for case in 0..300u64 {
            let na = (next() % 40) as usize;
            let nb = (next() % 50) as usize;
            let b_vt_e: Vec<i64> = (0..nb).map(|_| (next() % 30) as i64).collect();
            let a_vt_e: Vec<i64> = (0..na).map(|_| (next() % 30) as i64).collect();
            let lo: Vec<i64> = (0..na).map(|_| (next() % (nb as u64 + 3)) as i64 - 1).collect();
            let hi: Vec<i64> = lo.iter().map(|&l| l + (next() % 25) as i64 - 3).collect();
            for require in [false, true] {
                let lo32: Vec<u32> = lo.iter().map(|&v| v.max(0) as u32).collect();
                let hi32: Vec<u32> = hi.iter().map(|&v| v.max(0) as u32).collect();
                let all = interval_pairs(&lo32, &hi32, &a_vt_e, &b_vt_e, require).unwrap();
                let exclude: Vec<(u32, u32)> = all.iter().copied().filter(|_| next() % 7 == 0)
                    .chain([(na as u32 + 2, 0), (0, nb as u32 + 5)]).collect();
                let want: Vec<(u32, u32)> = all.iter().copied().filter(|p| !exclude.contains(p)).collect();
                for (offset, limit) in [(0u64, 1usize), (0, 7), (3, 5), (want.len() as u64, 4),
                                        (want.len() as u64 + 3, 2), (0, 1000), (case % 11, 13)] {
                    let got = interval_page(&lo, &hi, &a_vt_e, &b_vt_e, require, &exclude, offset, limit).unwrap();
                    assert_eq!(got.total, want.len() as u64, "case {case} require {require}");
                    let lo_i = (offset as usize).min(want.len());
                    let hi_i = (lo_i + limit).min(want.len());
                    assert_eq!(got.pairs, want[lo_i..hi_i].to_vec(), "case {case} require {require} {offset} {limit}");
                }
            }
        }
    }

    #[test]
    fn mismatched_inputs_are_rejected() {
        assert!(interval_pairs(&[0, 1], &[1], &[1, 2], &[0], false).is_err());
        assert!(interval_pairs(&[0], &[1], &[1, 2], &[0], false).is_err());
    }
}
