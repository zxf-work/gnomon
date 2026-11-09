//! `neighborhood_evolution`'s one-read form in one engine call
//!.
//!
//! `ops_snapshot.neighborhood_evolution` reads the incident
//! edges of `uid` once over `[t1, t2 + 1)` and derives the neighbourhoods at
//! `t1` and `t2` and the degree series from that read with NumPy. This is
//! the same read (`NativeStore::scan_edges` with the same request the
//! adapter's `edges_columnar` builds) and the same predicates over it, so
//! the strings and Python objects crossing the binding are the answer's.

use std::collections::BTreeSet;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use gnomon_core::scan::ScanRequest;

use crate::{err, NativeStore, Res};

/// `(gained, lost, degree_series)`: the uids of the neighbours valid at
/// `t2` and not at `t1` (and the reverse), each sorted, and
/// `[{"t": b, "degree": d}]` for `b` in `range(t1, t2, stride)`, `d` the
/// incident rows overlapping `[t1, t2)` started by `b` minus those ended
/// by `b` -- `neighborhood_evolution`'s values before the limit is applied.
/// `as_of_tt` is the clamped belief time the adapter's scan takes.
#[pyfunction]
pub(crate) fn neighborhood_evolution_parts<'py>(
    py: Python<'py>,
    store: PyRef<'py, NativeStore>,
    uid_id: i64,
    t1: i64,
    t2: i64,
    as_of_tt: i64,
    stride: i64,
) -> Res<(Vec<String>, Vec<String>, Bound<'py, PyList>)> {
    if stride < 1 || t2 == i64::MAX {
        return Err(pyo3::exceptions::PyValueError::new_err("bad stride or t2"));
    }
    let req = ScanRequest {
        as_of_tt,
        vt_min: Some(t1),
        vt_max: Some(t2 + 1),
        rel_types: None,
        // the adapter passes `int(uid_id)` and the binding narrows it so
        touching_ids: Some(vec![uid_id as u32]),
        touching_both: false,
        vt_s_min: None,
        limit: None,
        columns: Some(
            ["src_id", "dst_id", "vt_s", "vt_e"]
                .iter()
                .map(|c| c.to_string())
                .collect(),
        ),
    };
    let cols = store.inner.scan_edges(&req).map_err(err)?.cols;
    let n = cols.vt_s.len();
    if cols.vt_e.len() != n || cols.src_id.len() != n || cols.dst_id.len() != n {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "neighborhood_evolution_parts: projected columns missing",
        ));
    }
    let mut at_t1: BTreeSet<i64> = BTreeSet::new();
    let mut at_t2: BTreeSet<i64> = BTreeSet::new();
    let mut starts: Vec<i64> = Vec::new();
    let mut ends: Vec<i64> = Vec::new();
    for r in 0..n {
        let (src, dst) = (cols.src_id[r] as i64, cols.dst_id[r] as i64);
        let (vs, ve) = (cols.vt_s[r], cols.vt_e[r]);
        let out = src == uid_id;
        let other = if out { dst } else { src };
        // the other endpoint of an incident row; a self-loop is no neighbour
        if (out || dst == uid_id) && other != uid_id {
            if vs <= t1 && ve > t1 {
                at_t1.insert(other);
            }
            if vs <= t2 && ve > t2 {
                at_t2.insert(other);
            }
        }
        if vs < t2 && ve > t1 {
            starts.push(vs);
            ends.push(ve);
        }
    }
    let uids = |ids: Vec<i64>| -> Res<Vec<String>> {
        let mut out = ids
            .into_iter()
            .map(|i| {
                store
                    .inner
                    .dict()
                    .uid(i as u32)
                    .map(str::to_string)
                    .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("unknown dense id: {i}")))
            })
            .collect::<Res<Vec<String>>>()?;
        out.sort_unstable();
        Ok(out)
    };
    let gained = uids(at_t2.difference(&at_t1).copied().collect())?;
    let lost = uids(at_t1.difference(&at_t2).copied().collect())?;
    starts.sort_unstable();
    ends.sort_unstable();
    let series = PyList::empty(py);
    let mut b = t1;
    while b < t2 {
        let started = starts.partition_point(|&x| x <= b) as i64;
        let ended = ends.partition_point(|&x| x <= b) as i64;
        let d = PyDict::new(py);
        d.set_item("t", b)?;
        d.set_item("degree", started - ended)?;
        series.append(d)?;
        b = match b.checked_add(stride) {
            Some(x) => x,
            None => break,
        };
    }
    Ok((gained, lost, series))
}
