//! JavaScript value semantics the CCTV and radio providers relied on:
//! `String(x)`, `Number(x)`, truthiness, `trim`, `toFixed`, number printing,
//! URI component coding and UTF-16 lengths, over `serde_json::Value`.

use serde_json::Value;

use crate::common::js;

/// `\s` / `String.prototype.trim` whitespace (WhiteSpace + LineTerminator).
pub fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

/// `String.prototype.trim`.
pub fn trim(s: &str) -> &str {
    s.trim_matches(is_js_space)
}

/// `s.replace(/\s+/g, ' ')`.
pub fn collapse_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_space = false;
    for c in s.chars() {
        if is_js_space(c) {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

/// JS truthiness; `None` is `undefined`.
pub fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|f| f != 0.0 && !f.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_)) | Some(Value::Object(_)) => true,
    }
}

/// `a || b || ...`: the first truthy value, else the last one.
pub fn or<'a>(values: &[Option<&'a Value>]) -> Option<&'a Value> {
    for v in values {
        if truthy(*v) {
            return *v;
        }
    }
    values.last().copied().flatten()
}

/// `a ?? b ?? ...`: the first value that is neither null nor undefined.
pub fn nullish<'a>(values: &[Option<&'a Value>]) -> Option<&'a Value> {
    values.iter().copied().flatten().find(|v| !v.is_null())
}

/// `v?.[key]` for an object (or array index key).
pub fn get<'a>(v: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    match v? {
        Value::Object(map) => map.get(key),
        Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => None,
    }
}

/// `v?.[index]`.
pub fn index(v: Option<&Value>, i: usize) -> Option<Value> {
    match v? {
        Value::Array(items) => items.get(i).cloned(),
        Value::Object(map) => map.get(&i.to_string()).cloned(),
        Value::String(s) => {
            let units: Vec<u16> = s.encode_utf16().collect();
            units
                .get(i)
                .map(|u| Value::String(String::from_utf16_lossy(&[*u])))
        }
        _ => None,
    }
}

/// `for (const x of (v || []))`: `Err` where JS would throw (not iterable).
pub fn iterate(v: Option<&Value>) -> Result<Vec<Value>, ()> {
    if !truthy(v) {
        return Ok(Vec::new());
    }
    match v {
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(Value::String(s)) => Ok(s.chars().map(|c| Value::String(c.to_string())).collect()),
        _ => Err(()),
    }
}

/// `String(v)`; `None` is `"undefined"`.
pub fn string(v: Option<&Value>) -> String {
    match v {
        None => "undefined".into(),
        Some(v) => value_string(v),
    }
}

fn value_string(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => number_string(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => value_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// `String(a || b || '')` style coercion: `String(or(values))` with `''`
/// when nothing is truthy.
pub fn string_or(values: &[Option<&Value>], fallback: &str) -> String {
    for v in values {
        if truthy(*v) {
            return string(*v);
        }
    }
    fallback.to_string()
}

/// `Number(v)`; `None` is `undefined` (NaN), `null` is 0.
pub fn number(v: Option<&Value>) -> f64 {
    match v {
        None => f64::NAN,
        Some(Value::Null) => 0.0,
        Some(Value::Bool(b)) => f64::from(u8::from(*b)),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => js::number(s),
        Some(v @ Value::Array(_)) => js::number(&value_string(v)),
        Some(Value::Object(_)) => f64::NAN,
    }
}

/// `toFiniteNumber(value, fallback)`.
pub fn finite_or(v: Option<&Value>, fallback: f64) -> f64 {
    let n = number(v);
    if n.is_finite() { n } else { fallback }
}

/// A JSON number (no coercion), like `typeof v === 'number' ? v : NaN`.
pub fn strict_number(v: Option<&Value>) -> f64 {
    match v {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}

/// `Number.prototype.toString()` (radix 10).
pub fn number_string(x: f64) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if x == 0.0 {
        return "0".into();
    }
    let abs = x.abs();
    if (1e-6..1e21).contains(&abs) {
        return format!("{x}");
    }
    let s = format!("{x:e}");
    match s.split_once('e') {
        Some((mantissa, exp)) if !exp.starts_with('-') => format!("{mantissa}e+{exp}"),
        _ => s,
    }
}

/// `Number.prototype.toFixed(digits)`, rounding exact ties up like JS.
pub fn to_fixed(x: f64, digits: usize) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.abs() >= 1e21 || x.is_infinite() {
        return number_string(x);
    }
    let negative = x < 0.0;
    // Exact decimal expansion: an f64 never needs more than 1074 fraction digits.
    let exact = format!("{:.1100}", x.abs());
    let (int_part, frac_part) = exact.split_once('.').unwrap_or((&exact, ""));
    let mut digits_vec: Vec<u8> = int_part.bytes().collect();
    digits_vec.extend(frac_part.bytes().take(digits));
    let round_up = frac_part.as_bytes().get(digits).is_some_and(|d| *d >= b'5');
    if round_up {
        let mut i = digits_vec.len();
        loop {
            if i == 0 {
                digits_vec.insert(0, b'1');
                break;
            }
            i -= 1;
            if digits_vec[i] == b'9' {
                digits_vec[i] = b'0';
            } else {
                digits_vec[i] += 1;
                break;
            }
        }
    }
    let split = digits_vec.len() - digits;
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(std::str::from_utf8(&digits_vec[..split]).unwrap_or("0"));
    if digits > 0 {
        out.push('.');
        out.push_str(std::str::from_utf8(&digits_vec[split..]).unwrap_or(""));
    }
    out
}

/// A number as `JSON.stringify` writes it: non-finite is `null`, integral
/// values carry no fraction.
pub fn num(x: f64) -> Value {
    if !x.is_finite() {
        return Value::Null;
    }
    if x.fract() == 0.0 && x.abs() < 9_007_199_254_740_992.0 {
        return Value::from(x as i64);
    }
    serde_json::Number::from_f64(x).map_or(Value::Null, Value::Number)
}

/// `encodeURIComponent`.
pub fn encode_uri_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            )
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `decodeURIComponent`: `None` where JS throws `URIError`.
pub fn decode_uri_component(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let text = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(text, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// `s.length` (UTF-16 code units).
pub fn utf16_len(s: &str) -> usize {
    s.encode_utf16().count()
}

/// `s.slice(start, end)` in UTF-16 code units.
pub fn utf16_slice(s: &str, start: usize, end: usize) -> String {
    let units: Vec<u16> = s.encode_utf16().collect();
    let end = end.min(units.len());
    let start = start.min(end);
    String::from_utf16_lossy(&units[start..end])
}

/// Whether `word` occurs in `text` delimited by non-word ASCII characters
/// (`/\bWORD\b/` without the `u` flag).
pub fn has_word(text: &str, word: &str) -> bool {
    let is_word = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
    let mut start = 0;
    while let Some(pos) = text[start..].find(word) {
        let at = start + pos;
        let before = text[..at].chars().next_back();
        let after = text[at + word.len()..].chars().next();
        if !is_word(before) && !is_word(after) {
            return true;
        }
        start = at + word.len().max(1);
        while start < text.len() && !text.is_char_boundary(start) {
            start += 1;
        }
        if start >= text.len() {
            break;
        }
    }
    false
}

/// `new Date(ms).toISOString()`.
pub fn iso_string(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// Strip a UTF-8 byte-order mark, as `Response.text()` / `TextDecoder` do.
pub fn strip_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

/// `JSON.parse(await response.text())` over raw bytes.
pub fn parse_json(bytes: &[u8]) -> Option<Value> {
    serde_json::from_str(&String::from_utf8_lossy(strip_bom(bytes))).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn to_fixed_matches_js() {
        assert_eq!(to_fixed(0.5, 0), "1");
        assert_eq!(to_fixed(2.5, 0), "3");
        assert_eq!(to_fixed(0.25, 1), "0.3");
        assert_eq!(to_fixed(1.005, 2), "1.00");
        assert_eq!(to_fixed(30.2672, 6), "30.267200");
        assert_eq!(to_fixed(-97.7431, 6), "-97.743100");
        assert_eq!(to_fixed(-0.0, 1), "0.0");
        assert_eq!(to_fixed(-0.04, 1), "-0.0");
        assert_eq!(to_fixed(9.99, 1), "10.0");
        assert_eq!(to_fixed(f64::NAN, 1), "NaN");
        assert_eq!(to_fixed(2.0, 3), "2.000");
    }

    #[test]
    fn number_strings_match_js() {
        assert_eq!(number_string(30.0), "30");
        assert_eq!(number_string(-97.7431), "-97.7431");
        assert_eq!(number_string(1e21), "1e+21");
        assert_eq!(number_string(1.5e-7), "1.5e-7");
        assert_eq!(number_string(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(string(Some(&json!([1, null, "a"]))), "1,,a");
        assert_eq!(string(Some(&json!({}))), "[object Object]");
        assert_eq!(string(None), "undefined");
    }

    #[test]
    fn coercions_match_js() {
        assert_eq!(number(Some(&Value::Null)), 0.0);
        assert!(number(None).is_nan());
        assert_eq!(number(Some(&json!(" 12 "))), 12.0);
        assert_eq!(number(Some(&json!(true))), 1.0);
        assert!(!truthy(Some(&json!(0))));
        assert!(!truthy(Some(&json!(""))));
        assert!(truthy(Some(&json!([]))));
        assert_eq!(num(30.0), json!(30));
        assert_eq!(num(f64::NAN), Value::Null);
    }

    #[test]
    fn uri_coding_matches_js() {
        assert_eq!(encode_uri_component("a b/ü!"), "a%20b%2F%C3%BC!");
        assert_eq!(
            decode_uri_component("a%20b%2F%C3%BC").as_deref(),
            Some("a b/ü")
        );
        assert_eq!(decode_uri_component("%E0%A4%A"), None);
        assert_eq!(decode_uri_component("%C3"), None);
        assert_eq!(decode_uri_component("%zz"), None);
    }

    #[test]
    fn words_match_ascii_boundaries() {
        assert!(has_word("US-290 EB", "EB"));
        assert!(!has_word("WEBB", "EB"));
        assert!(has_word("ÉNB", "NB"));
        assert!(!has_word("", "NB"));
    }
}
