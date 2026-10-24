//! The shared-instant rule, one shared valid instant:
//! the variable-length step `CoValidExpand`, the `optional`/`not` join
//! `CoValidJoin`, the per-version-tuple `Collapse`, and the union of binding
//! tables a `dir: both` hop compiles to.
//!
//! Range-native as the rest of the executor: the search walks the window's
//! believed adjacency through `neighbors_batch` (TCSR, believed ranges, dense
//! ids) under current belief, or one pass over the version keys otherwise;
//! node existence comes from identity point reads for a few ids and one pass
//! over the node lane's believed ranges for many. Instant sets are sorted
//! lists of disjoint half-open intervals.

use super::*;

/// Instants: sorted, disjoint, non-empty half-open intervals.
pub(super) type Ivs = Vec<(i64, i64)>;

pub(super) fn ivs_norm(mut v: Ivs) -> Ivs {
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

pub(super) fn ivs_and(a: &[(i64, i64)], b: &[(i64, i64)]) -> Ivs {
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

pub(super) fn ivs_sub(a: &[(i64, i64)], b: &[(i64, i64)]) -> Ivs {
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

pub(super) fn ivs_or(a: &[(i64, i64)], b: &[(i64, i64)]) -> Ivs {
    let mut v = a.to_vec();
    v.extend_from_slice(b);
    ivs_norm(v)
}

/// Per node, its `(neighbour, window-clipped edge interval)` list.
type Adj = FxMap<u32, Vec<(u32, i64, i64)>>;
/// One search's rows: (target id, version, depth, fragment start, end).
type Found = Vec<(u32, Addr, u32, i64, i64)>;

/// `CoValidExpand`'s fields.
#[derive(Clone, Debug)]
pub(super) struct CoValid {
    pub from: String,
    pub into: String,
    pub lo: u32,
    pub hi: Option<u32>,
    pub rel_type: Option<String>,
    pub dir: NeighborDir,
    pub belief: BeliefMode,
    pub from_iv: Option<(String, String)>,
}

fn pair(n: &J, key: &str) -> DResult<(String, String)> {
    match n.get(key).and_then(J::as_array).map(Vec::as_slice) {
        Some([a, b]) => match (a.as_str(), b.as_str()) {
            (Some(a), Some(b)) => Ok((a.to_string(), b.to_string())),
            _ => unsupported(format!("{key} names two columns")),
        },
        _ => unsupported(format!("{key} names two columns")),
    }
}

/// The three shared-instant plan nodes, from their JSON.
pub(super) fn parse_op(op: &str, n: &J, inputs: usize) -> DResult<Option<Op>> {
    Ok(Some(match op {
        "CoValidExpand" => {
            let h = n.get("hops").map_or_else(|| unsupported("hops"), Ok)?;
            let (lo, hi) = if let Some(ab) = h.get("bounded").and_then(J::as_array) {
                match (ab.first().and_then(J::as_u64), ab.get(1).and_then(J::as_u64)) {
                    (Some(a), Some(b)) => (a as u32, Some(b as u32)),
                    _ => return unsupported("bounded hops"),
                }
            } else if let Some(a) = h.get("unbounded").and_then(J::as_u64) {
                (a as u32, None)
            } else {
                return unsupported("CoValidExpand hops");
            };
            let dir = match jstr(n, "dir")? {
                "out" => NeighborDir::Out,
                "in" => NeighborDir::In,
                "both" => NeighborDir::Both,
                _ => return unsupported("dir"),
            };
            let from_iv = match n.get("from_iv") {
                None | Some(J::Null) => None,
                Some(_) => Some(pair(n, "from_iv")?),
            };
            Op::CoValidExpand(Box::new(CoValid {
                from: jstr(n, "from")?.to_string(),
                into: jstr(n, "into")?.to_string(),
                lo,
                hi,
                rel_type: n.get("rel_type").and_then(J::as_str).map(str::to_string),
                dir,
                belief: parse_belief(jstr(n, "belief")?)?,
                from_iv,
            }))
        }
        "CoValidJoin" => {
            if inputs != 2 {
                return unsupported("CoValidJoin takes two inputs");
            }
            let mut on = Vec::new();
            for p in n.get("on").and_then(J::as_array).map_or_else(|| unsupported("on"), Ok)? {
                let a = p.as_array().filter(|a| a.len() == 2).map_or_else(|| unsupported("on"), Ok)?;
                match (a[0].as_str(), a[1].as_str()) {
                    (Some(l), Some(r)) => on.push((l.to_string(), r.to_string())),
                    _ => return unsupported("on"),
                }
            }
            let outer = match jstr(n, "type")? {
                "left_outer" => true,
                "anti" => false,
                _ => return unsupported("CoValidJoin type"),
            };
            Op::CoValidJoin(Box::new(CoJoin {
                on,
                outer,
                left_iv: pair(n, "left_iv")?,
                right_iv: pair(n, "right_iv")?,
                out_iv: pair(n, "out_iv")?,
            }))
        }
        "Collapse" => Op::Collapse {
            by: jstrs(n, "by")?.map_or_else(|| unsupported("Collapse by"), Ok)?,
            mins: jstrs(n, "mins")?.unwrap_or_default(),
        },
        _ => return Ok(None),
    }))
}

/// `CoValidJoin`'s fields.
#[derive(Clone, Debug)]
pub(super) struct CoJoin {
    pub on: Vec<(String, String)>,
    pub outer: bool,
    pub left_iv: (String, String),
    pub right_iv: (String, String),
    pub out_iv: (String, String),
}

/// The admitted versions overlapping the window, per requested node id,
/// each list in `(vt_s, vid)` order.
struct WinVersions {
    belief: BeliefMode,
    map: FxMap<u32, Vec<Addr>>,
    /// after a whole pass: every id's versions, by id (offsets, versions)
    all: Option<(Vec<u32>, Vec<Addr>)>,
    full: bool,
    asked: usize,
    exists: FxMap<u32, Ivs>,
}

impl WinVersions {
    /// `id`'s admitted versions, in `(vt_s, vid)` order (asked for already).
    fn of(&self, id: u32) -> &[Addr] {
        match &self.all {
            Some((off, v)) => match off.get(id as usize + 1) {
                Some(&hi) => &v[off[id as usize] as usize..hi as usize],
                None => &[],
            },
            None => self.map.get(&id).map_or(&[], Vec::as_slice),
        }
    }
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn win_versions(&mut self, wv: &mut WinVersions, ids: &[u32]) -> DResult<()> {
        if wv.full {
            return Ok(());
        }
        let mut missing: Vec<u32> = ids.iter().copied().filter(|i| *i != NO_ID && !wv.map.contains_key(i)).collect();
        missing.sort_unstable();
        missing.dedup();
        if missing.is_empty() {
            return Ok(());
        }
        wv.asked += missing.len();
        let n_ids = self.store.dict().len() as usize;
        let point_max = POINT_READS_MAX.max(n_ids / POINT_READS_FRACTION);
        if missing.len() > point_max && wv.asked <= (n_ids / BATCH_READS_FRACTION).max(POINT_READS_MAX) {
            // more than point reads serve, few of all: the id column against
            // a bitset of the wanted ids, belief and valid time on the hits
            let mut want = vec![false; n_ids];
            for &i in &missing {
                if (i as usize) < n_ids {
                    want[i as usize] = true;
                    wv.map.entry(i).or_default();
                }
            }
            for a in self.node_rows_where(wv.belief, VtMode::Overlap, |sc, r| {
                want.get(sc.a[r] as usize).copied().unwrap_or(false)
            })? {
                wv.map.entry(self.nodes[seg_of(a)].a[row_of(a)]).or_default().push(a);
            }
            for &i in &missing {
                if let Some(v) = wv.map.get_mut(&i) {
                    v.sort_unstable_by_key(|&a| self.version_key(a));
                    v.dedup();
                }
            }
            return Ok(());
        }
        if wv.asked > (n_ids / BATCH_READS_FRACTION).max(POINT_READS_MAX) {
            // many: one pass over the node lane's believed, window-clipped
            // ranges, grouped by id (counted, then filled) and each id's
            // versions sorted, in chunks of ids on the pool
            let addrs = self.scan_addrs(RowKind::Node, wv.belief, VtMode::Overlap, None)?;
            let id_of = |a: Addr| self.nodes[seg_of(a)].a[row_of(a)] as usize;
            let top = addrs.iter().map(|&a| id_of(a) + 1).max().unwrap_or(0).max(n_ids);
            let mut off = vec![0u32; top + 1];
            for &a in &addrs {
                off[id_of(a) + 1] += 1;
            }
            for i in 0..top {
                off[i + 1] += off[i];
            }
            let mut next = off.clone();
            let mut fill = vec![NO_ADDR; addrs.len()];
            for &a in &addrs {
                let i = id_of(a);
                fill[next[i] as usize] = a;
                next[i] += 1;
            }
            let this: &Self = self;
            let fill = par_concat(top, |r| {
                let mut out = Vec::new();
                for i in r {
                    let mut v = fill[off[i] as usize..off[i + 1] as usize].to_vec();
                    v.sort_unstable_by_key(|&a| this.version_key(a));
                    v.dedup();
                    out.extend(v);
                }
                out
            });
            // dedup never fires (a scan yields each row once), so the offsets hold
            debug_assert_eq!(fill.len(), addrs.len());
            wv.all = Some((off, fill));
            wv.map = FxMap::default();
            wv.full = true;
            return Ok(());
        }
        let uids: Vec<String> = missing.iter().filter_map(|&i| self.store.dict().uid(i).map(str::to_string)).collect();
        for &i in &missing {
            wv.map.entry(i).or_default();
        }
        for (id, a) in self.node_versions_of(&uids, wv.belief, VtMode::Overlap)? {
            wv.map.entry(id).or_default().push(a);
        }
        for &i in &missing {
            if let Some(v) = wv.map.get_mut(&i) {
                v.sort_unstable_by_key(|&a| self.version_key(a));
                v.dedup();
            }
        }
        Ok(())
    }

    /// The window-clipped interval of a node version.
    fn node_iv(&self, a: Addr) -> (i64, i64) {
        let s = &self.nodes[seg_of(a)];
        (s.vt_s[row_of(a)].max(self.plan.t_a), s.vt_e(row_of(a)).min(self.plan.t_b))
    }

    /// The instants at which node `id` has an admitted version (`win_versions`
    /// must have been asked for it).
    fn exists_of(&self, wv: &mut WinVersions, id: u32) -> Ivs {
        if let Some(x) = wv.exists.get(&id) {
            return x.clone();
        }
        let ivs = ivs_norm(wv.of(id).iter().map(|&a| self.node_iv(a)).collect());
        wv.exists.insert(id, ivs.clone());
        ivs
    }

    /// One level's adjacency for a sorted distinct frontier: per node, its
    /// `(neighbour, window-clipped edge interval)` list.
    fn win_neighbors(
        &mut self,
        all_adj: &mut Option<Adj>,
        frontier: &[u32],
        c: &CoValid,
    ) -> DResult<Vec<Vec<(u32, i64, i64)>>> {
        let (va, vb) = (self.plan.t_a, self.plan.t_b);
        let rels: Option<Vec<String>> = c.rel_type.as_ref().map(|r| vec![r.clone()]);
        let mut out: Vec<Vec<(u32, i64, i64)>> = vec![Vec::new(); frontier.len()];
        if frontier.is_empty() {
            return Ok(out);
        }
        if c.belief == BeliefMode::Current {
            let t0 = std::time::Instant::now();
            let nb = self.store.neighbors_batch(frontier, c.dir, self.plan.as_of, Some(va), Some(vb), rels.as_deref())?;
            crate::phase::add(crate::phase::Phase::MatchHopNs, t0.elapsed().as_nanos() as u64);
            for (p, slot) in out.iter_mut().enumerate() {
                for j in nb.offsets[p] as usize..nb.offsets[p + 1] as usize {
                    let (s, e) = (nb.vt_s[j].max(va), nb.vt_e[j].min(vb));
                    if s < e {
                        slot.push((nb.nbr[j], s, e));
                    }
                }
            }
            return Ok(out);
        }
        if all_adj.is_none() {
            // superseded / all: the version keys once, into an id-keyed adjacency
            let addrs = self.scan_addrs(RowKind::Edge, c.belief, VtMode::Overlap, rels.as_deref())?;
            let code = c.rel_type.as_ref().map(|r| self.rels.get(r));
            let mut m: Adj = FxMap::default();
            for a in addrs {
                if let Some(want) = code {
                    if want != Some(self.rel_code(a)) {
                        continue;
                    }
                }
                let sc = &self.edges[seg_of(a)];
                let r = row_of(a);
                let (s, e) = (sc.vt_s[r].max(va), sc.vt_e(r).min(vb));
                if s >= e {
                    continue;
                }
                let (src, dst) = (sc.a[r], sc.b[r]);
                if matches!(c.dir, NeighborDir::Out | NeighborDir::Both) {
                    m.entry(src).or_default().push((dst, s, e));
                }
                if matches!(c.dir, NeighborDir::In | NeighborDir::Both) {
                    m.entry(dst).or_default().push((src, s, e));
                }
            }
            *all_adj = Some(m);
        }
        let m = all_adj.as_ref().expect("built above");
        for (p, &u) in frontier.iter().enumerate() {
            if let Some(l) = m.get(&u) {
                out[p] = l.clone();
            }
        }
        Ok(out)
    }

    /// Integer values of a time column, `None` where null.
    fn i64s(&mut self, rel: &Rel, name: &str) -> DResult<Vec<Option<i64>>> {
        let c = self.eval(&Expr::Col(name.to_string()), rel)?;
        match &c.v {
            Vals::I64(x) => Ok((0..rel.n).map(|i| (!c.is_null(i)).then_some(x[i])).collect()),
            Vals::Null(n) => Ok(vec![None; *n]),
            _ => unsupported(format!("not an integer column: {name}")),
        }
    }

    /// `CoValidExpand`: per distinct (seed, start interval), a level-synchronous
    /// search over (node, instant set) states; every search of a level shares
    /// one adjacency call.
    pub(super) fn covalid_expand(&mut self, input: Rel, c: &CoValid) -> DResult<Rel> {
        let t0 = trace().then(std::time::Instant::now);
        let (mut t_adj, mut t_lvl) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
        let n = input.n;
        let seeds = self.uid_column(&input, &c.from)?;
        let (va, vb) = (self.plan.t_a, self.plan.t_b);
        let starts: Vec<Option<(i64, i64)>> = match &c.from_iv {
            Some((s, e)) => {
                let (ss, ee) = (self.i64s(&input, s)?, self.i64s(&input, e)?);
                (0..n).map(|i| match (ss[i], ee[i]) {
                    (Some(s), Some(e)) => Some((s.max(va), e.min(vb))),
                    _ => None,
                }).collect()
            }
            None => vec![Some((va, vb)); n],
        };
        let key_of = |i: usize| -> Option<(u32, i64, i64)> {
            let (s, e) = starts[i]?;
            (seeds[i] != NO_ID && s < e).then_some((seeds[i], s, e))
        };
        let mut keys: Vec<(u32, i64, i64)> = (0..n).filter_map(key_of).collect();
        keys.sort_unstable();
        keys.dedup();
        let mut wv = WinVersions {
            belief: c.belief,
            map: FxMap::default(),
            all: None,
            full: false,
            asked: 0,
            exists: FxMap::default(),
        };
        let seed_ids: Vec<u32> = keys.iter().map(|k| k.0).collect();
        self.win_versions(&mut wv, &seed_ids)?;
        // per search: covered instants per node, the frontier, and what it reached
        let mut covered: Vec<FxMap<u32, Ivs>> = Vec::with_capacity(keys.len());
        let mut frontiers: Vec<Vec<(u32, Ivs)>> = Vec::with_capacity(keys.len());
        let mut reach: Vec<Vec<(u32, Ivs, u32)>> = Vec::with_capacity(keys.len());
        for &(seed, s, e) in &keys {
            let ex = self.exists_of(&mut wv, seed);
            let first = ivs_and(&[(s, e)], &ex);
            let mut cov = FxMap::default();
            let (mut fr, mut re) = (Vec::new(), Vec::new());
            if !first.is_empty() {
                cov.insert(seed, first.clone());
                fr.push((seed, first.clone()));
                re.push((seed, first, 0u32));
            }
            covered.push(cov);
            frontiers.push(fr);
            reach.push(re);
        }
        let t_setup = lap(t0);
        let mut all_adj: Option<Adj> = None;
        let mut depth = 0u32;
        let mut states = keys.len();
        loop {
            if c.hi.is_some_and(|h| depth >= h) || frontiers.iter().all(Vec::is_empty) {
                break;
            }
            depth += 1;
            let mut all: Vec<u32> = frontiers.iter().flatten().map(|x| x.0).collect();
            all.sort_unstable();
            all.dedup();
            let t1 = trace().then(std::time::Instant::now);
            let adj = self.win_neighbors(&mut all_adj, &all, c)?;
            let mut nbrs: Vec<u32> = adj.iter().flatten().map(|x| x.0).collect();
            nbrs.sort_unstable();
            nbrs.dedup();
            self.win_versions(&mut wv, &nbrs)?;
            t_adj += lap(t1);
            let t1 = trace().then(std::time::Instant::now);
            for k in 0..keys.len() {
                let fr = std::mem::take(&mut frontiers[k]);
                let mut next: FxMap<u32, Ivs> = FxMap::default();
                for (u, got) in fr {
                    let p = all.binary_search(&u).expect("frontier node listed");
                    for &(v, s, e) in &adj[p] {
                        let kk = ivs_and(&got, &[(s, e)]);
                        if kk.is_empty() {
                            continue;
                        }
                        let ex = self.exists_of(&mut wv, v);
                        let kk = ivs_and(&kk, &ex);
                        let kk = match covered[k].get(&v) {
                            Some(cv) => ivs_sub(&kk, cv),
                            None => kk,
                        };
                        if kk.is_empty() {
                            continue;
                        }
                        let cv = covered[k].entry(v).or_default();
                        *cv = ivs_or(cv, &kk);
                        let nx = next.entry(v).or_default();
                        *nx = ivs_or(nx, &kk);
                    }
                }
                let mut nx: Vec<(u32, Ivs)> = next.into_iter().collect();
                nx.sort_unstable_by_key(|x| x.0);
                for (v, kk) in &nx {
                    reach[k].push((*v, kk.clone(), depth));
                }
                states += nx.len();
                frontiers[k] = nx;
            }
            t_lvl += lap(t1);
            check_rows(states)?;
        }
        let t_search = lap(t0);
        // per search, its rows: (target id, version, depth, fragment)
        // in chunks of searches on the pool, in search order
        let this: &Self = self;
        let wv_r = &wv;
        let found: Vec<Found> = par_concat_from(reach.len(), (par_min() / 16).max(1), |rg| {
            rg.map(|k| {
                let mut f = Vec::new();
                for (v, kk, d) in &reach[k] {
                    if *d < c.lo {
                        continue;
                    }
                    for &a in wv_r.of(*v) {
                        for (s, e) in ivs_and(kk, &[this.node_iv(a)]) {
                            f.push((*v, a, *d, s, e));
                        }
                    }
                }
                f.sort_unstable_by(|x, y| {
                    (x.2, x.0, this.version_key(x.1), x.3).cmp(&(y.2, y.0, this.version_key(y.1), y.3))
                });
                f
            })
            .collect()
        });
        // the budget as the row loop meets it: after each input row
        let mut total = 0usize;
        for i in 0..n {
            if let Some(k) = key_of(i) {
                total += found[keys.binary_search(&k).expect("key listed")].len();
                check_rows(total)?;
            }
        }
        type Out = (usize, u32, Addr, u32, i64, i64);
        let outs: Vec<Out> = par_concat(n, |rg| {
            let mut o = Vec::new();
            for i in rg {
                let Some(k) = key_of(i) else { continue };
                let p = keys.binary_search(&k).expect("key listed");
                for &(v, a, d, s, e) in &found[p] {
                    o.push((i, v, a, d, s, e));
                }
            }
            o
        });
        let rows: Vec<usize> = outs.iter().map(|o| o.0).collect();
        let ids: Vec<u32> = outs.iter().map(|o| o.1).collect();
        let ver: Vec<Addr> = outs.iter().map(|o| o.2).collect();
        let depths: Vec<u32> = outs.iter().map(|o| o.3).collect();
        let ps: Vec<i64> = outs.iter().map(|o| o.4).collect();
        let pe: Vec<i64> = outs.iter().map(|o| o.5).collect();
        if trace() {
            eprintln!(
                "match trace:   co-valid expand: {} rows, {} searches, {} levels, {} states; setup {:.1} ms, adjacency {:.1} ms, levels {:.1} ms, found and rows {:.1} ms",
                n, keys.len(), depth, states,
                t_setup.as_secs_f64() * 1e3, t_adj.as_secs_f64() * 1e3, t_lvl.as_secs_f64() * 1e3,
                (lap(t0) - t_search).as_secs_f64() * 1e3
            );
        }
        let mut out = input.take(&rows);
        out.vars.push((c.into.clone(), Var::Node { ids, ver, depth: Some(depths) }));
        out.cols.push((format!("{}.ps", c.into), Col { v: Vals::I64(ps), null: None }));
        out.cols.push((format!("{}.pe", c.into), Col { v: Vals::I64(pe), null: None }));
        Ok(out)
    }

    /// `CoValidJoin`: per left fragment, the overlapping extensions on the
    /// intersection (`left_outer`), then the uncovered remainder.
    pub(super) fn covalid_join(&mut self, left: Rel, right: Rel, j: &CoJoin) -> DResult<Rel> {
        let lk: Vec<Vec<KeyVal>> = j.on.iter().map(|(l, _)| self.keys_by_name(&left, l)).collect::<DResult<_>>()?;
        let rk: Vec<Vec<KeyVal>> = j.on.iter().map(|(_, r)| self.keys_by_name(&right, r)).collect::<DResult<_>>()?;
        if lk.iter().any(|k| k.contains(&KeyVal::Null)) || rk.iter().any(|k| k.contains(&KeyVal::Null)) {
            return refuse("a null join key");
        }
        let key = |ks: &[Vec<KeyVal>], i: usize| -> Vec<KeyVal> { ks.iter().map(|k| k[i].clone()).collect() };
        let mut table: HashMap<Vec<KeyVal>, Vec<usize>> = HashMap::new();
        for r in 0..right.n {
            table.entry(key(&rk, r)).or_default().push(r);
        }
        let (ls, le) = (self.i64s(&left, &j.left_iv.0)?, self.i64s(&left, &j.left_iv.1)?);
        let (rs, re) = (self.i64s(&right, &j.right_iv.0)?, self.i64s(&right, &j.right_iv.1)?);
        let (mut li, mut ri, mut os, mut oe) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for i in 0..left.n {
            let (Some(s), Some(e)) = (ls[i], le[i]) else { continue };
            if s >= e {
                continue;
            }
            let mut cover: Ivs = Vec::new();
            if let Some(js) = table.get(&key(&lk, i)) {
                for &r in js {
                    let (Some(a), Some(b)) = (rs[r], re[r]) else { continue };
                    let (a, b) = (a.max(s), b.min(e));
                    if a >= b {
                        continue;
                    }
                    cover.push((a, b));
                    if j.outer {
                        li.push(i);
                        ri.push(Some(r));
                        os.push(a);
                        oe.push(b);
                    }
                }
            }
            for (a, b) in ivs_sub(&[(s, e)], &ivs_norm(cover)) {
                li.push(i);
                ri.push(None);
                os.push(a);
                oe.push(b);
            }
            check_rows(li.len())?;
        }
        let mut out = left.take(&li);
        if j.outer {
            let r = right.take_opt(&ri);
            out.vars.extend(r.vars);
            out.cols.extend(r.cols);
        }
        out.cols.push((j.out_iv.0.clone(), Col { v: Vals::I64(os), null: None }));
        out.cols.push((j.out_iv.1.clone(), Col { v: Vals::I64(oe), null: None }));
        Ok(out)
    }

    /// `Collapse`: the first row of each distinct `by` tuple, each `mins`
    /// target depth its least over the group.
    pub(super) fn collapse(&mut self, input: Rel, by: &[String], mins: &[String]) -> DResult<Rel> {
        let n = input.n;
        let keys: Vec<Vec<KeyVal>> = by.iter().map(|b| self.keys_by_name(&input, b)).collect::<DResult<_>>()?;
        let mut first: HashMap<Vec<KeyVal>, usize> = HashMap::new();
        let mut order: Vec<usize> = Vec::new();
        let mut group = vec![0usize; n];
        for i in 0..n {
            let k: Vec<KeyVal> = keys.iter().map(|c| c[i].clone()).collect();
            let next = order.len();
            let g = *first.entry(k).or_insert(next);
            if g == next {
                order.push(i);
            }
            group[i] = g;
        }
        let mut out = input.take(&order);
        for m in mins {
            let Some(var) = m.strip_suffix(".depth") else { return unsupported("Collapse of a non-depth column") };
            let Some(Var::Node { ids, depth: Some(d), .. }) = input.var(var) else {
                return unsupported("Collapse of a depth the binding does not carry");
            };
            let mut best: Vec<Option<u32>> = vec![None; order.len()];
            for i in 0..n {
                if ids[i] == NO_ID {
                    continue;
                }
                let b = &mut best[group[i]];
                *b = Some(b.map_or(d[i], |x| x.min(d[i])));
            }
            if let Some((_, Var::Node { depth: Some(od), .. })) = out.vars.iter_mut().find(|(k, _)| k == var) {
                for (g, b) in best.into_iter().enumerate() {
                    if let Some(b) = b {
                        od[g] = b;
                    }
                }
            }
        }
        Ok(out)
    }

    /// A union of binding tables with one variable and column list (a
    /// `dir: both` hop's two orientations): each input's rows in order.
    pub(super) fn union_vars(&mut self, inputs: Vec<Rel>) -> DResult<Rel> {
        let first = &inputs[0];
        let var_names: Vec<&String> = first.vars.iter().map(|(k, _)| k).collect();
        let col_names: Vec<&String> = first.cols.iter().map(|(k, _)| k).collect();
        for r in &inputs[1..] {
            if r.vars.iter().map(|(k, _)| k).ne(var_names.iter().copied())
                || r.cols.iter().map(|(k, _)| k).ne(col_names.iter().copied())
            {
                return unsupported("a union of binding tables with different variables");
            }
        }
        let n: usize = inputs.iter().map(|r| r.n).sum();
        check_rows(n)?;
        let mut vars = Vec::with_capacity(var_names.len());
        for (j, name) in var_names.iter().enumerate() {
            let v = match &first.vars[j].1 {
                Var::Node { depth, .. } => {
                    let (mut ids, mut ver) = (Vec::with_capacity(n), Vec::with_capacity(n));
                    let mut dep: Option<Vec<u32>> = depth.as_ref().map(|_| Vec::with_capacity(n));
                    for r in &inputs {
                        let Var::Node { ids: i2, ver: v2, depth: d2 } = &r.vars[j].1 else {
                            return unsupported("a union variable of two kinds");
                        };
                        ids.extend_from_slice(i2);
                        ver.extend_from_slice(v2);
                        match (&mut dep, d2) {
                            (Some(d), Some(d2)) => d.extend_from_slice(d2),
                            (None, None) => {}
                            _ => return unsupported("a union of targets with and without depth"),
                        }
                    }
                    Var::Node { ids, ver, depth: dep }
                }
                Var::Edge { .. } => {
                    let mut ver = Vec::with_capacity(n);
                    for r in &inputs {
                        let Var::Edge { ver: v2 } = &r.vars[j].1 else {
                            return unsupported("a union variable of two kinds");
                        };
                        ver.extend_from_slice(v2);
                    }
                    Var::Edge { ver }
                }
            };
            vars.push(((*name).clone(), v));
        }
        let mut cols = Vec::with_capacity(col_names.len());
        for (j, name) in col_names.iter().enumerate() {
            let parts: Vec<&Col> = inputs.iter().map(|r| &r.cols[j].1).collect();
            cols.push(((*name).clone(), self.concat_cols(&parts, n)?));
        }
        Ok(Rel { n, vars, cols })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_sets() {
        assert_eq!(ivs_norm(vec![(5, 7), (1, 3), (3, 4), (8, 8)]), vec![(1, 4), (5, 7)]);
        assert_eq!(ivs_and(&[(0, 10), (20, 30)], &[(5, 25)]), vec![(5, 10), (20, 25)]);
        assert_eq!(ivs_sub(&[(0, 10)], &[(2, 3), (5, 7)]), vec![(0, 2), (3, 5), (7, 10)]);
        assert_eq!(ivs_sub(&[(0, 10)], &[(-5, 20)]), Vec::<(i64, i64)>::new());
        assert_eq!(ivs_sub(&[(0, 4), (6, 9)], &[(3, 7)]), vec![(0, 3), (7, 9)]);
        assert_eq!(ivs_or(&[(0, 2)], &[(2, 5), (7, 8)]), vec![(0, 5), (7, 8)]);
    }
}
