//! of `algebra.args_key`, and one pass that canonicalises a payload's floats
//! and writes its canonical JSON together.
//!
//! Every function here answers exactly what its Python counterpart answers,
//! or declines (`None`) and the caller runs the Python function.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

fn push_i64(v: i64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let neg = v < 0;
    let mut u = v.unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    if neg {
        out.push(b'-');
    }
    out.extend_from_slice(&buf[i..]);
}

/// A Python int as `str(int)` writes it.
fn push_int(o: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    match o.extract::<i64>() {
        Ok(v) => push_i64(v, out),
        Err(_) => out.extend_from_slice(o.str()?.to_str()?.as_bytes()),
    }
    Ok(())
}

/// One value of `canonical_json` (`json.dumps(sort_keys=True,
/// separators=(",", ":"), ensure_ascii=False)`) appended to `out`; `false`
/// for anything outside exact dicts with str keys, lists, tuples, str, int,
/// bool, None and finite floats, which the caller then serialises in Python.
pub(crate) fn cj(o: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<bool> {
    if o.is_none() {
        out.extend_from_slice(b"null");
    } else if o.is_exact_instance_of::<PyBool>() {
        out.extend_from_slice(if o.is_truthy()? { b"true" } else { b"false" });
    } else if o.is_exact_instance_of::<PyInt>() {
        push_int(o, out)?;
    } else if let Ok(s) = o.cast_exact::<PyString>() {
        let Ok(s) = s.to_str() else { return Ok(false) };
        cj_str(s, out);
    } else if o.is_exact_instance_of::<PyFloat>() {
        let f: f64 = o.extract()?;
        if !f.is_finite() {
            return Ok(false);
        }
        out.extend_from_slice(o.repr()?.to_str()?.as_bytes());
    } else if let Ok(d) = o.cast_exact::<PyDict>() {
        return cj_dict(d, out);
    } else if let Ok(l) = o.cast_exact::<PyList>() {
        out.push(b'[');
        for (i, v) in l.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if !cj(&v, out)? {
                return Ok(false);
            }
        }
        out.push(b']');
    } else if let Ok(t) = o.cast_exact::<PyTuple>() {
        out.push(b'[');
        for (i, v) in t.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if !cj(&v, out)? {
                return Ok(false);
            }
        }
        out.push(b']');
    } else {
        return Ok(false);
    }
    Ok(true)
}

/// A dict's members in key order.
fn cj_dict<'py>(d: &Bound<'py, PyDict>, out: &mut Vec<u8>) -> PyResult<bool> {
    let mut keys: Vec<Bound<'py, PyString>> = Vec::with_capacity(d.len());
    let mut vals: Vec<Bound<'py, PyAny>> = Vec::with_capacity(d.len());
    for (k, v) in d.iter() {
        let Ok(ks) = k.cast_into_exact::<PyString>() else { return Ok(false) };
        keys.push(ks);
        vals.push(v);
    }
    let mut order: Vec<(&str, usize)> = Vec::with_capacity(keys.len());
    for (i, k) in keys.iter().enumerate() {
        let Ok(s) = k.to_str() else { return Ok(false) };
        order.push((s, i));
    }
    order.sort_unstable_by(|a, b| a.0.cmp(b.0));
    out.push(b'{');
    for (n, (k, i)) in order.iter().enumerate() {
        if n > 0 {
            out.push(b',');
        }
        cj_str(k, out);
        out.push(b':');
        if !cj(&vals[*i], out)? {
            return Ok(false);
        }
    }
    out.push(b'}');
    Ok(true)
}

/// `json`'s string escaping with `ensure_ascii=False`: `"` and `\`, the
/// five short control escapes, `\u00xx` for the other code points below 0x20.
fn cj_str(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    let b = s.as_bytes();
    // every byte of a multi-byte UTF-8 sequence is >= 0x80, so a byte scan
    // finds exactly the characters that need an escape
    if !b.iter().any(|&c| c < 0x20 || c == b'"' || c == b'\\') {
        out.extend_from_slice(b);
        out.push(b'"');
        return;
    }
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            '\u{8}' => out.extend_from_slice(b"\\b"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            c if (c as u32) < 0x20 => out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes()),
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
}

/// `gnomon.core.model.canonical_json(obj).encode("utf-8")`,
/// or `None` where the value holds a type this writer leaves to Python.
#[pyfunction]
pub(crate) fn canonical_json_bytes<'py>(
    py: Python<'py>,
    obj: Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let mut out = Vec::with_capacity(256);
    if !cj(&obj, &mut out)? {
        return Ok(None);
    }
    Ok(Some(PyBytes::new(py, &out)))
}

fn plain(o: &Bound<'_, PyAny>) -> bool {
    o.is_exact_instance_of::<PyString>()
        || o.is_exact_instance_of::<PyInt>()
        || o.is_exact_instance_of::<PyBool>()
        || o.is_none()
}

/// `_canonicalize_floats` of `o` (the rules of `lib.rs::canon`) with the
/// canonical JSON of the result appended to `out`; `None` wherever `canon`
/// would hand a value to the Python walk or `cj` would decline, and for a
/// non-finite float (the caller then runs both functions, which answer or
/// raise as they do on their own).
fn canon_cj<'py>(
    o: &Bound<'py, PyAny>,
    round: &Bound<'py, PyAny>,
    out: &mut Vec<u8>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let py = o.py();
    if plain(o) {
        return Ok(if cj(o, out)? { Some(o.clone()) } else { None });
    }
    if o.is_instance_of::<PyFloat>() {
        let f: f64 = o.extract()?;
        if !f.is_finite() {
            return Ok(None);
        }
        let r = round.call1((o, 9))?;
        return Ok(if cj(&r, out)? { Some(r) } else { None });
    }
    if let Ok(d) = o.cast_exact::<PyDict>() {
        if d.iter().all(|(_, v)| plain(&v)) {
            // `canon` copies it; the copy writes as the original does
            let res = d.copy()?;
            return Ok(if cj(o, out)? { Some(res.into_any()) } else { None });
        }
        // `canon` builds a new dict in the dict's own order; each value's
        // JSON goes to `frag`, then the members are written in key order
        let res = PyDict::new(py);
        let mut keys: Vec<Bound<'py, PyString>> = Vec::with_capacity(d.len());
        let mut spans: Vec<(usize, usize)> = Vec::with_capacity(d.len());
        let mut frag: Vec<u8> = Vec::with_capacity(256);
        for (k, v) in d.iter() {
            let Ok(ks) = k.cast_exact::<PyString>() else { return Ok(None) };
            let start = frag.len();
            match canon_cj(&v, round, &mut frag)? {
                Some(c) => res.set_item(ks, c)?,
                None => return Ok(None),
            }
            keys.push(ks.clone());
            spans.push((start, frag.len()));
        }
        let mut order: Vec<(&str, usize)> = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            let Ok(s) = k.to_str() else { return Ok(None) };
            order.push((s, i));
        }
        order.sort_unstable_by(|a, b| a.0.cmp(b.0));
        out.push(b'{');
        for (n, (k, i)) in order.iter().enumerate() {
            if n > 0 {
                out.push(b',');
            }
            cj_str(k, out);
            out.push(b':');
            let (a, b) = spans[*i];
            out.extend_from_slice(&frag[a..b]);
        }
        out.push(b'}');
        return Ok(Some(res.into_any()));
    }
    if let Ok(l) = o.cast_exact::<PyList>() {
        let res = PyList::empty(py);
        out.push(b'[');
        for (i, v) in l.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            match canon_cj(&v, round, out)? {
                Some(c) => res.append(c)?,
                None => return Ok(None),
            }
        }
        out.push(b']');
        return Ok(Some(res.into_any()));
    }
    if o.is_instance_of::<PyDict>() || o.is_instance_of::<PyList>() {
        return Ok(None);
    }
    // any other value is returned as it is (`canon`'s last arm)
    Ok(if cj(o, out)? { Some(o.clone()) } else { None })
}

/// `(_canonicalize_floats(obj), canonical_json(that).encode())` in one walk
///, or `None`: the caller then runs the two functions.
#[pyfunction]
pub(crate) fn canonicalize_json<'py>(
    py: Python<'py>,
    obj: Bound<'py, PyAny>,
    round: Bound<'py, PyAny>,
) -> PyResult<Option<(Bound<'py, PyAny>, Bound<'py, PyBytes>)>> {
    let mut out = Vec::with_capacity(1024);
    match canon_cj(&obj, &round, &mut out)? {
        Some(c) => Ok(Some((c, PyBytes::new(py, &out)))),
        None => Ok(None),
    }
}

fn ak_str(s: &str, out: &mut Vec<u8>) {
    out.push(b's');
    out.extend_from_slice(&(s.len() as u64).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// `algebra.args_key` as bytes: two values get equal bytes exactly when
/// `args_key` gives them equal keys; `false` where `args_key` gives `None`
/// (and for a str that is not UTF-8 encodable, which the caller keys in
/// Python).
fn ak(o: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<bool> {
    if o.is_none() {
        out.push(b'n');
    } else if o.is_exact_instance_of::<PyBool>() {
        out.push(if o.is_truthy()? { b't' } else { b'f' });
    } else if o.is_exact_instance_of::<PyInt>() {
        match o.extract::<i64>() {
            Ok(v) => {
                out.push(b'i');
                out.extend_from_slice(&v.to_le_bytes());
            }
            Err(_) => {
                out.push(b'I');
                ak_str(o.str()?.to_str()?, out);
            }
        }
    } else if let Ok(s) = o.cast_exact::<PyString>() {
        let Ok(s) = s.to_str() else { return Ok(false) };
        ak_str(s, out);
    } else if o.is_exact_instance_of::<PyFloat>() {
        let f: f64 = o.extract()?;
        if f.is_nan() {
            return Ok(false);
        }
        // equal bits <=> equal repr for every float but NaN
        out.push(b'd');
        out.extend_from_slice(&f.to_bits().to_le_bytes());
    } else if let Ok(d) = o.cast_exact::<PyDict>() {
        out.push(b'{');
        for (k, v) in d.iter() {
            let Ok(ks) = k.cast_exact::<PyString>() else { return Ok(false) };
            let Ok(ks) = ks.to_str() else { return Ok(false) };
            ak_str(ks, out);
            if !ak(&v, out)? {
                return Ok(false);
            }
        }
        out.push(b'}');
    } else if let Ok(l) = o.cast_exact::<PyList>() {
        out.push(b'[');
        for v in l.iter() {
            if !ak(&v, out)? {
                return Ok(false);
            }
        }
        out.push(b']');
    } else {
        return Ok(false);
    }
    Ok(true)
}

/// `algebra.args_key(obj)` as bytes, or `None`.
#[pyfunction]
pub(crate) fn args_key_bytes<'py>(
    py: Python<'py>,
    obj: Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let mut out = Vec::with_capacity(128);
    if !ak(&obj, &mut out)? {
        return Ok(None);
    }
    Ok(Some(PyBytes::new(py, &out)))
}
