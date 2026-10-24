//! Typed property values for the aggregate kernel: the type-fit rule, natively.
//!
//! `aggregate_events` reads numeric properties (`of: "prop"`) and filters on
//! one (`prop_filter`). The rule is the one `gnomon/temporal/props.py` states
//! for the portable path and the oracle, and this module is a third
//! implementation of the same definition, not an approximation of it:
//!
//! * a value takes part in arithmetic iff it is a JSON number (a boolean is
//!   not one);
//! * a comparison is defined iff the value and the literal are in the same
//!   type class (number/number, string/string, bool/bool), and numbers
//!   compare exactly, an integer against a float included;
//! * text is never parsed into a number; a missing key, a JSON `null` and a
//!   wrong-typed value do not take part, and are counted.
//!
//! Numbers are read from the stored text with the standard library's
//! correctly rounded parser, which is what Python's `json.loads` does too.
//! A general JSON parser's fast float path is not correctly rounded on long
//! mantissas, and one ulp is a different `result_digest`.
//!
//! Where the portable path's answer cannot be reproduced exactly, the value
//! is [`PVal::Unsure`] and the kernel declines the call (the operator then
//! answers it on the portable path): an integer outside int64, a non-finite
//! float, a string the strict parser refuses, and the `NaN`/`Infinity`
//! literals `json.loads` accepts.

use std::cmp::Ordering;

/// A JSON number as Python holds it after `json.loads`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PNum {
    Int(i64),
    Float(f64),
}

/// One property's value, classified for the rule above.
#[derive(Clone, Debug, PartialEq)]
pub enum PVal {
    /// missing key or JSON `null`
    Absent,
    Num(PNum),
    Str(String),
    Bool(bool),
    /// an object or an array
    Other,
    /// not reproducible exactly; the call declines
    Unsure,
}

/// A filter literal, classified the same way (`None`: no type class, so
/// every comparison against it is undefined).
#[derive(Clone, Debug, PartialEq)]
pub enum PLit {
    Num(PNum),
    Str(String),
    Bool(bool),
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl PCmp {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "eq" => PCmp::Eq,
            "ne" => PCmp::Ne,
            "lt" => PCmp::Lt,
            "le" => PCmp::Le,
            "gt" => PCmp::Gt,
            "ge" => PCmp::Ge,
            _ => return None,
        })
    }

    fn holds(self, o: Ordering) -> bool {
        match self {
            PCmp::Eq => o == Ordering::Equal,
            PCmp::Ne => o != Ordering::Equal,
            PCmp::Lt => o == Ordering::Less,
            PCmp::Le => o != Ordering::Greater,
            PCmp::Gt => o == Ordering::Greater,
            PCmp::Ge => o != Ordering::Less,
        }
    }
}

/// A filter verdict: `Ok(None)` = the comparison is undefined (the row does
/// not take part and is counted), `Ok(Some(b))` = it is defined and holds or
/// not, `Err(())` = the value is [`PVal::Unsure`].
#[allow(clippy::result_unit_err)]
pub fn verdict(v: &PVal, cmp: PCmp, lit: &PLit) -> Result<Option<bool>, ()> {
    let o = match (v, lit) {
        (PVal::Unsure, _) => return Err(()),
        (PVal::Num(a), PLit::Num(b)) => cmp_num(*a, *b),
        (PVal::Str(a), PLit::Str(b)) => a.as_bytes().cmp(b.as_bytes()),
        (PVal::Bool(a), PLit::Bool(b)) => a.cmp(b),
        _ => return Ok(None),
    };
    Ok(Some(cmp.holds(o)))
}

/// Exact comparison of two finite JSON numbers, as Python compares an int
/// with a float: by value, never through a lossy conversion.
pub fn cmp_num(a: PNum, b: PNum) -> Ordering {
    match (a, b) {
        (PNum::Int(x), PNum::Int(y)) => x.cmp(&y),
        (PNum::Float(x), PNum::Float(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        (PNum::Int(x), PNum::Float(y)) => cmp_int_float(x, y),
        (PNum::Float(x), PNum::Int(y)) => cmp_int_float(y, x).reverse(),
    }
}

fn cmp_int_float(i: i64, f: f64) -> Ordering {
    // 2^63 is exact in f64, and every finite f64 below it in magnitude has
    // an integer part that i64 holds exactly
    if f >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    if f < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    let t = f.trunc();
    match i.cmp(&(t as i64)) {
        Ordering::Equal => {
            if f > t {
                Ordering::Less
            } else if f < t {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        o => o,
    }
}

/// Equal by value but distinguishable in the output (`1` against `1.0`,
/// `0.0` against `-0.0`): which one Python's `min`/`max` returns depends on
/// the order it met them in, which a parallel fold does not have.
pub fn distinguishable(a: PNum, b: PNum) -> bool {
    match (a, b) {
        (PNum::Int(x), PNum::Int(y)) => x != y,
        (PNum::Float(x), PNum::Float(y)) => x.to_bits() != y.to_bits(),
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// extraction: the values of a few top-level keys of one JSON object's text
// ---------------------------------------------------------------------------

/// Fill `out[k]` with the value of `keys[k]` in the JSON object `text`.
/// Text that is not one JSON object leaves every key [`PVal::Absent`],
/// which is what the portable path's `parse_props` makes of it (`{}`).
/// A repeated key keeps its last value, as `json.loads` does.
pub fn extract(text: Option<&str>, keys: &[&str], out: &mut [PVal]) {
    for v in out.iter_mut() {
        *v = PVal::Absent;
    }
    let Some(text) = text else { return };
    let mut found: Vec<PVal> = vec![PVal::Absent; keys.len()];
    if scan_object(text.as_bytes(), text, keys, &mut found).is_some() {
        out.clone_from_slice(&found);
    }
}

fn ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// End (exclusive) of the string token starting at a quote at `b[i]`, and
/// whether it holds an escape.
fn string_end(b: &[u8], mut i: usize) -> Option<(usize, bool)> {
    i += 1;
    let mut esc = false;
    while i < b.len() {
        match b[i] {
            b'"' => return Some((i + 1, esc)),
            b'\\' => {
                esc = true;
                i += 2;
            }
            _ => i += 1,
        }
    }
    None
}

fn number_end(b: &[u8], mut i: usize) -> Option<usize> {
    if i < b.len() && b[i] == b'-' {
        i += 1;
    }
    let d0 = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == d0 || (b[d0] == b'0' && i - d0 > 1) {
        return None;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let f0 = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == f0 {
            return None;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let e0 = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == e0 {
            return None;
        }
    }
    Some(i)
}

/// End of the value starting at `i`, and whether it is one of the
/// non-standard literals `json.loads` accepts (`NaN`, `Infinity`,
/// `-Infinity`).
fn value_end(b: &[u8], i: usize) -> Option<(usize, bool)> {
    let lit = |w: &[u8]| b.get(i..i + w.len()) == Some(w);
    match *b.get(i)? {
        b'"' => string_end(b, i).map(|(e, _)| (e, false)),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        j = string_end(b, j)?.0;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth = depth.checked_sub(1)?;
                        if depth == 0 {
                            return Some((j + 1, false));
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            None
        }
        b't' if lit(b"true") => Some((i + 4, false)),
        b'f' if lit(b"false") => Some((i + 5, false)),
        b'n' if lit(b"null") => Some((i + 4, false)),
        b'N' if lit(b"NaN") => Some((i + 3, true)),
        b'I' if lit(b"Infinity") => Some((i + 8, true)),
        b'-' if lit(b"-Infinity") => Some((i + 9, true)),
        _ => number_end(b, i).map(|e| (e, false)),
    }
}

fn classify(raw: &str, nonstandard: bool) -> PVal {
    if nonstandard {
        return PVal::Unsure;
    }
    match raw.as_bytes()[0] {
        b'"' => match serde_json::from_str::<String>(raw) {
            Ok(s) => PVal::Str(s),
            // a lone surrogate escape: Python keeps it, a Rust string cannot
            Err(_) => PVal::Unsure,
        },
        b'{' | b'[' => PVal::Other,
        b't' => PVal::Bool(true),
        b'f' => PVal::Bool(false),
        b'n' => PVal::Absent,
        _ => {
            if raw.bytes().any(|c| matches!(c, b'.' | b'e' | b'E')) {
                match raw.parse::<f64>() {
                    Ok(f) if f.is_finite() => PVal::Num(PNum::Float(f)),
                    _ => PVal::Unsure,
                }
            } else {
                match raw.parse::<i64>() {
                    Ok(i) => PVal::Num(PNum::Int(i)),
                    // outside int64: Python holds it exactly, i64 cannot
                    Err(_) => PVal::Unsure,
                }
            }
        }
    }
}

fn scan_object(b: &[u8], text: &str, keys: &[&str], found: &mut [PVal]) -> Option<()> {
    let mut i = ws(b, 0);
    if b.get(i) != Some(&b'{') {
        return None;
    }
    i = ws(b, i + 1);
    if b.get(i) == Some(&b'}') {
        return (ws(b, i + 1) == b.len()).then_some(());
    }
    loop {
        if b.get(i) != Some(&b'"') {
            return None;
        }
        let (ke, esc) = string_end(b, i)?;
        let decoded;
        let key: &str = if esc {
            decoded = serde_json::from_str::<String>(&text[i..ke]).ok()?;
            &decoded
        } else {
            &text[i + 1..ke - 1]
        };
        i = ws(b, ke);
        if b.get(i) != Some(&b':') {
            return None;
        }
        i = ws(b, i + 1);
        let (ve, nonstd) = value_end(b, i)?;
        if let Some(k) = keys.iter().position(|k| *k == key) {
            found[k] = classify(&text[i..ve], nonstd);
        }
        i = ws(b, ve);
        match b.get(i) {
            Some(b',') => i = ws(b, i + 1),
            Some(b'}') => return (ws(b, i + 1) == b.len()).then_some(()),
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(text: &str, key: &str) -> PVal {
        let mut out = vec![PVal::Absent];
        extract(Some(text), &[key], &mut out);
        out.pop().unwrap()
    }

    #[test]
    fn extracts_typed_values() {
        assert_eq!(one(r#"{"a":1,"b":2.5}"#, "a"), PVal::Num(PNum::Int(1)));
        assert_eq!(one(r#"{"a":1,"b":2.5}"#, "b"), PVal::Num(PNum::Float(2.5)));
        assert_eq!(one(r#"{"a":"3"}"#, "a"), PVal::Str("3".into()));
        assert_eq!(one(r#"{"a":true}"#, "a"), PVal::Bool(true));
        assert_eq!(one(r#"{"a":null}"#, "a"), PVal::Absent);
        assert_eq!(one(r#"{"a":[1,{"x":"}"}]}"#, "a"), PVal::Other);
        assert_eq!(one(r#"{"b":1}"#, "a"), PVal::Absent);
        assert_eq!(one(r#"[1]"#, "a"), PVal::Absent);
        assert_eq!(one(r#"{"a":1} x"#, "a"), PVal::Absent);
        assert_eq!(one(r#"{"a":1,"a":2}"#, "a"), PVal::Num(PNum::Int(2)));
        assert_eq!(one(r#"{"a":7}"#, "a"), PVal::Num(PNum::Int(7)));
        assert_eq!(one(r#"{"a":99999999999999999999}"#, "a"), PVal::Unsure);
        assert_eq!(one(r#"{"a":1e400}"#, "a"), PVal::Unsure);
        assert_eq!(one(r#"{"a":NaN}"#, "a"), PVal::Unsure);
        assert_eq!(one(r#"{"a":-0.0}"#, "a"), PVal::Num(PNum::Float(-0.0)));
        assert_eq!(
            one(r#"{ "x" : "a\"b" , "a" : 0.30000000000000004 }"#, "a"),
            PVal::Num(PNum::Float(0.300_000_000_000_000_04))
        );
    }

    #[test]
    fn int_float_comparison_is_exact() {
        assert_eq!(cmp_num(PNum::Int(1), PNum::Float(1.0)), Ordering::Equal);
        assert_eq!(
            cmp_num(PNum::Int(9_007_199_254_740_993), PNum::Float(9_007_199_254_740_992.0)),
            Ordering::Greater
        );
        assert_eq!(cmp_num(PNum::Int(i64::MAX), PNum::Float(9.3e18)), Ordering::Less);
        assert_eq!(cmp_num(PNum::Float(-0.5), PNum::Int(0)), Ordering::Less);
        assert_eq!(cmp_num(PNum::Float(-1.5), PNum::Int(-1)), Ordering::Less);
        assert!(distinguishable(PNum::Int(1), PNum::Float(1.0)));
        assert!(distinguishable(PNum::Float(0.0), PNum::Float(-0.0)));
        assert!(!distinguishable(PNum::Float(2.0), PNum::Float(2.0)));
    }

    #[test]
    fn verdicts_follow_type_classes() {
        let n = PVal::Num(PNum::Int(3));
        assert_eq!(verdict(&n, PCmp::Gt, &PLit::Num(PNum::Float(2.5))), Ok(Some(true)));
        assert_eq!(verdict(&n, PCmp::Eq, &PLit::Str("3".into())), Ok(None));
        assert_eq!(verdict(&PVal::Bool(true), PCmp::Gt, &PLit::Bool(false)), Ok(Some(true)));
        assert_eq!(verdict(&PVal::Absent, PCmp::Eq, &PLit::None), Ok(None));
        assert_eq!(verdict(&PVal::Unsure, PCmp::Eq, &PLit::None), Err(()));
    }
}
