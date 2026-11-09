//! hub and b.snap.hop2).
//!
//! `snapshot_expand` builds every induced edge as strings (and the binding
//! layer a Python dict per edge) and every reached node with its label, and
//! the operator then keeps one page: 100 rows and 100 nodes by default. On a
//! power-law hub that is 440k edges and 66k nodes built to return 100 of each
//! (`snapshot_expand` 1.47 s of a 1.86 s call). `snapshot_expand_page` runs
//! the same search and the same row keys, sorts them with the same call, and
//! then takes the counts and builds strings for the requested slice only:
//! edges `[offset, offset + limit)` in the `(vt_s, vid)` order and nodes
//! `[0, node_limit)` in `(hop, uid)` order.
//!
//! Exactness: the edge order is the full path's own (`sort_unstable_by_key`
//! on the same vector built in the same order), so a page boundary inside a
//! group of equal keys cuts it where the full path's slice would; the node
//! order is total (`(hop, uid)` is unique per node). The operator switch
//! `GNOMON_SNAPSHOT_PAGE=0` keeps the full materialisation.

use super::*;

/// A page of the seed-driven snapshot: the totals, the nodes `[0,
/// node_limit)` by `(hop, uid)` and the edges `[offset, offset + limit)` in
/// the instant scan's `(vt_s, vid)` order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapPage {
    pub nodes: Vec<(String, String, u32)>,
    pub nodes_total: usize,
    pub edges: Vec<SnapEdge>,
    pub edges_total: usize,
}

impl NativeStore {
    /// `snapshot_expand`, paged: `None` inside an open write batch, as there.
    #[allow(clippy::too_many_arguments)]
    pub fn snapshot_expand_page(
        &self,
        seeds: &[u32],
        t: i64,
        as_of_tt: i64,
        hops: u32,
        rel_types: Option<&[String]>,
        offset: usize,
        limit: usize,
        node_limit: usize,
    ) -> Result<Option<SnapPage>> {
        if self.in_batch() {
            return Ok(None);
        }
        let as_of = crate::clamp_tt(as_of_tt);
        self.assert_full_belief(as_of)?;
        let plan = Plan { nodes: Vec::new(), root: 0, t_a: t, t_b: t + 1, as_of, columns: Vec::new() };
        match self.with_ctx(&plan, |ctx| ctx.snapshot_page(seeds, hops, rel_types, offset, limit, node_limit)) {
            Ok(out) => Ok(Some(out)),
            Err(Decline::Engine(e)) => Err(e),
            Err(d) => {
                note_decline(&d);
                Ok(None)
            }
        }
    }
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn snapshot_page(
        &mut self,
        seeds: &[u32],
        hops: u32,
        rels: Option<&[String]>,
        offset: usize,
        limit: usize,
        node_limit: usize,
    ) -> DResult<SnapPage> {
        let t0 = std::time::Instant::now();
        let t = self.plan.t_a;
        // the search: `Ctx::snapshot`'s, line for line
        let mut seeds: Vec<u32> = seeds.to_vec();
        seeds.sort_unstable();
        seeds.dedup();
        let mut valid = self.valid_versions(&seeds)?;
        let mut dist: FxMap<u32, u32> = FxMap::default();
        let mut frontier: Vec<u32> = seeds.iter().copied().filter(|i| valid.contains_key(i)).collect();
        for &s in &frontier {
            dist.insert(s, 0);
        }
        for h in 1..=hops {
            if frontier.is_empty() {
                break;
            }
            let adj = self.neighbors(&frontier, NeighborDir::Both, t, rels)?;
            let mut cand: Vec<u32> = adj.iter().flatten().map(|&(v, _)| v).filter(|v| !dist.contains_key(v)).collect();
            cand.sort_unstable();
            cand.dedup();
            let ask: Vec<u32> = cand.iter().copied().filter(|v| !valid.contains_key(v)).collect();
            valid.extend(self.valid_versions(&ask)?);
            frontier = cand.into_iter().filter(|v| valid.contains_key(v)).collect();
            for &v in &frontier {
                dist.insert(v, h);
            }
        }
        let mut reached: Vec<u32> = dist.keys().copied().collect();
        reached.sort_unstable();
        let t_search = t0.elapsed();
        // nodes: every reached node ordered by (hop, uid), labels for the page only
        let mut keyed: Vec<(u32, &str, u32)> = Vec::with_capacity(reached.len());
        {
            let d = self.store.dict();
            for &id in &reached {
                let Some(u) = d.uid(id) else {
                    return refuse("a dense id outside the dictionary");
                };
                keyed.push((dist[&id], u, id));
            }
        }
        keyed.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let nodes_total = keyed.len();
        let page_nodes: Vec<(u32, String, u32)> =
            keyed.iter().take(node_limit).map(|&(h, u, id)| (h, u.to_string(), id)).collect();
        drop(keyed);
        let mut nodes = Vec::with_capacity(page_nodes.len());
        for (h, u, id) in page_nodes {
            let a = valid[&id];
            let label = self.label_code(a)?;
            nodes.push((u, self.labels.text(label).to_string(), h));
        }
        // edges: the full path's keys and sort, then strings for the slice only
        let adj = self.neighbors(&reached, NeighborDir::Out, t, rels)?;
        let mut rows: Vec<(i64, u64, u32, Addr, u32, u32)> = Vec::new();
        for (p, list) in adj.iter().enumerate() {
            for &(v, e) in list {
                if reached.binary_search(&v).is_ok() {
                    let s = &self.edges[seg_of(e)];
                    let r = row_of(e);
                    rows.push((s.vt_s[r], s.vid_hi[r], s.vid_lo[r], e, reached[p], v));
                }
            }
        }
        rows.sort_unstable_by_key(|x| (x.0, x.1, x.2));
        let edges_total = rows.len();
        let lo = offset.min(edges_total);
        let hi = lo.saturating_add(limit).min(edges_total);
        let mut edges = Vec::with_capacity(hi - lo);
        for &(vt_s, hi_, lo_, e, src, dst) in &rows[lo..hi] {
            edges.push(SnapEdge {
                eid: self.eid_hex(e)?,
                vid: Id96 { hi: hi_, lo: lo_ }.to_hex(),
                src: self.uid_text(src)?.to_string(),
                dst: self.uid_text(dst)?.to_string(),
                rel_type: self.rels.text(self.rel_code(e)).to_string(),
                vt_s,
                vt_e: self.edges[seg_of(e)].vt_e(row_of(e)),
            });
        }
        if trace() {
            eprintln!(
                "match trace:     snapshot page: {} nodes, {} edges, page {}..{}: search {:.1} ms, total {:.1} ms",
                nodes_total,
                edges_total,
                lo,
                hi,
                t_search.as_secs_f64() * 1e3,
                t0.elapsed().as_secs_f64() * 1e3
            );
        }
        Ok(SnapPage { nodes, nodes_total, edges, edges_total })
    }
}
