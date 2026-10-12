//! JavaScript semantics the MCP output depends on: `JSON.stringify` and
//! `JSON.parse`, number formatting (`String(n)`, `toFixed`, `Math.round`),
//! UTF-16 string lengths, `trim`, `Number()`/`String()` coercion, dates and
//! a stable sort that tolerates inconsistent comparators.

use std::cmp::Ordering;

use serde_json::{Map, Value};

/// `String(number)`.
pub fn fmt_num(value: f64) -> String {
    let mut buffer = ryu_js::Buffer::new();
    buffer.format(value).to_string()
}

/// A JSON number for `value`; non-finite numbers become null, as
/// `JSON.stringify` writes them.
pub fn num(value: f64) -> Value {
    if !value.is_finite() {
        return Value::Null;
    }
    if value == value.trunc() && value.abs() < 9.007_199_254_740_992e15 {
        // Integral values are stored as integers so they also print as such
        // through serde, without changing what `stringify` writes.
        if value == 0.0 {
            return Value::from(0);
        }
        return Value::from(value as i64);
    }
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// `num`, or null for `None`.
pub fn opt_num(value: Option<f64>) -> Value {
    value.map_or(Value::Null, num)
}

/// A value's number when it is a JSON number (`typeof v === 'number'`).
pub fn as_f64(value: &Value) -> Option<f64> {
    value.as_f64()
}

/// `Number.isFinite(value) ? value : None`.
pub fn finite(value: &Value) -> Option<f64> {
    value.as_f64().filter(|v| v.is_finite())
}

/// `Number(value)`.
pub fn to_number(value: &Value) -> f64 {
    match value {
        Value::Null => 0.0,
        Value::Bool(b) => f64::from(u8::from(*b)),
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::String(s) => str_to_number(s),
        Value::Array(_) => str_to_number(&to_js_string(value)),
        Value::Object(_) => f64::NAN,
    }
}

/// `Number(string)`, with JavaScript's whitespace set.
pub fn str_to_number(value: &str) -> f64 {
    let trimmed = trim(value);
    if trimmed.is_empty() {
        return 0.0;
    }
    crate::common::js::number(trimmed)
}

/// `String(value)`.
pub fn to_js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => fmt_num(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => to_js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// JavaScript truthiness of a JSON value.
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|v| v != 0.0 && !v.is_nan()),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// JavaScript truthiness of an optional value (`undefined` is falsy).
pub fn truthy_opt(value: Option<&Value>) -> bool {
    value.is_some_and(truthy)
}

/// `value != null` for an optional property.
pub fn present(value: Option<&Value>) -> bool {
    value.is_some_and(|v| !v.is_null())
}

/// The string when `typeof value === 'string'`.
pub fn as_str(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str)
}

/// Whether a JS whitespace or line terminator (what `String#trim` removes).
pub fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\u{9}'..='\u{d}'
            | ' '
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

/// `String#trim`.
pub fn trim(value: &str) -> &str {
    value.trim_matches(is_js_space)
}

/// `String#trimStart`.
pub fn trim_start(value: &str) -> &str {
    value.trim_start_matches(is_js_space)
}

/// `String#length` (UTF-16 code units).
pub fn len16(value: &str) -> usize {
    value.chars().map(char::len_utf16).sum()
}

/// `String#slice(0, max)` in UTF-16 units. A cut through a surrogate pair
/// drops the half character (a Rust string cannot hold it).
pub fn slice16(value: &str, max: usize) -> String {
    let mut used = 0;
    let mut out = String::new();
    for c in value.chars() {
        used += c.len_utf16();
        if used > max {
            break;
        }
        out.push(c);
    }
    out
}

/// `Number.prototype.toFixed(digits)`: the exact value rounded half up.
pub fn to_fixed(value: f64, digits: usize) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.abs() >= 1e21 {
        return fmt_num(value);
    }
    let negative = value < 0.0;
    let magnitude = value.abs();
    // The exact decimal expansion: a double has at most 1074 fraction digits;
    // fewer are needed for most values.
    let bits = magnitude.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i64;
    let fraction_digits = if exponent == 0 {
        1074
    } else {
        (1075 - exponent).clamp(0, 1074) as usize
    };
    let exact = format!("{magnitude:.fraction_digits$}");
    let (int_part, frac_part) = exact.split_once('.').unwrap_or((exact.as_str(), ""));
    let mut digits_vec: Vec<u8> = int_part.bytes().map(|b| b - b'0').collect();
    let int_len = digits_vec.len();
    let frac: Vec<u8> = frac_part.bytes().map(|b| b - b'0').collect();
    for index in 0..digits {
        digits_vec.push(frac.get(index).copied().unwrap_or(0));
    }
    let round_up = frac.get(digits).is_some_and(|d| *d >= 5);
    if round_up {
        let mut index = digits_vec.len();
        loop {
            if index == 0 {
                digits_vec.insert(0, 1);
                break;
            }
            index -= 1;
            if digits_vec[index] == 9 {
                digits_vec[index] = 0;
            } else {
                digits_vec[index] += 1;
                break;
            }
        }
    }
    let int_len = int_len + (digits_vec.len() - int_len - digits);
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    for (index, digit) in digits_vec.iter().enumerate() {
        if index == int_len {
            out.push('.');
        }
        out.push(char::from(b'0' + digit));
    }
    out
}

/// `Number(value.toFixed(digits))`.
pub fn round_to(value: f64, digits: usize) -> f64 {
    to_fixed(value, digits).parse().unwrap_or(f64::NAN)
}

/// `Math.round`: halves round toward positive infinity.
pub fn math_round(value: f64) -> f64 {
    if !value.is_finite() || value == value.trunc() {
        return value;
    }
    let floor = value.floor();
    if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

/// `Math.max(...values)` (NaN when any is NaN, -Infinity when empty).
pub fn math_max(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut best = f64::NEG_INFINITY;
    for value in values {
        if value.is_nan() {
            return f64::NAN;
        }
        if value > best || (value == 0.0 && best == 0.0 && best.is_sign_negative()) {
            best = value;
        }
    }
    best
}

/// `Math.min(...values)`.
pub fn math_min(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut best = f64::INFINITY;
    for value in values {
        if value.is_nan() {
            return f64::NAN;
        }
        if value < best || (value == 0.0 && best == 0.0 && value.is_sign_negative()) {
            best = value;
        }
    }
    best
}

/// `Math.min(max, Math.max(min, value))`, NaN-propagating like JS.
pub fn clamp(value: f64, min: f64, max: f64) -> f64 {
    math_min([max, math_max([min, value])])
}

/// The JS `%` operator.
pub fn rem(a: f64, b: f64) -> f64 {
    a % b
}

// ---------------------------------------------------------------- JSON ----

/// Whether a property name is an array index, which JS objects list first.
fn is_index_key(key: &str) -> Option<u32> {
    if key.is_empty() || key.len() > 10 {
        return None;
    }
    if key.len() > 1 && key.starts_with('0') {
        return None;
    }
    if !key.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    key.parse::<u64>()
        .ok()
        .filter(|v| *v < u64::from(u32::MAX))
        .map(|v| v as u32)
}

/// An object's keys in JavaScript property order: array indices ascending,
/// then the rest in insertion order.
pub fn js_keys(map: &Map<String, Value>) -> Vec<&String> {
    let mut indices: Vec<(u32, &String)> = Vec::new();
    let mut rest = Vec::new();
    for key in map.keys() {
        match is_index_key(key) {
            Some(index) => indices.push((index, key)),
            None => rest.push(key),
        }
    }
    if indices.is_empty() {
        return rest;
    }
    indices.sort_by_key(|(index, _)| *index);
    indices
        .into_iter()
        .map(|(_, key)| key)
        .chain(rest)
        .collect()
}

fn write_string(out: &mut String, value: &str) {
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_value(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            let v = n.as_f64().unwrap_or(f64::NAN);
            if v.is_finite() {
                out.push_str(&fmt_num(v));
            } else {
                out.push_str("null");
            }
        }
        Value::String(s) => write_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, key) in js_keys(map).into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_string(out, key);
                out.push(':');
                write_value(out, &map[key.as_str()]);
            }
            out.push('}');
        }
    }
}

/// `JSON.stringify(value)`.
pub fn stringify(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value);
    out
}

/// `JSON.stringify(string)`.
pub fn quote(value: &str) -> String {
    let mut out = String::new();
    write_string(&mut out, value);
    out
}

/// `JSON.parse(text)`.
pub fn parse_json(text: &str) -> Option<Value> {
    serde_json::from_str(text).ok()
}

/// `JSON.parse(new TextDecoder().decode(bytes))`: lossy UTF-8 with the BOM
/// removed.
pub fn parse_json_bytes(bytes: &[u8]) -> Option<Value> {
    parse_json(&decode_text(bytes))
}

/// `new TextDecoder().decode(bytes)`.
pub fn decode_text(bytes: &[u8]) -> String {
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    String::from_utf8_lossy(bytes).into_owned()
}

// ---------------------------------------------------------------- dates ----

const MAX_TIME_MS: f64 = 8.64e15;

/// `new Date(ms).toISOString()`, or None where JS would throw.
pub fn iso_string(ms: f64) -> Option<String> {
    if !ms.is_finite() {
        return None;
    }
    let ms = ms.trunc();
    if ms.abs() > MAX_TIME_MS {
        return None;
    }
    let ms = ms as i64;
    let days = ms.div_euclid(86_400_000);
    let in_day = ms.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let hour = in_day / 3_600_000;
    let minute = in_day / 60_000 % 60;
    let second = in_day / 1000 % 60;
    let milli = in_day % 1000;
    let year_text = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else if year < 0 {
        format!("-{:06}", -year)
    } else {
        format!("+{year:06}")
    };
    Some(format!(
        "{year_text}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{milli:03}Z"
    ))
}

/// `isoTime(ms)` from results.js: ISO 8601 for a finite time, else null.
pub fn iso_time(ms: Option<f64>) -> Value {
    ms.and_then(iso_string).map_or(Value::Null, Value::String)
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    // MakeDay: months outside 1..=12 roll into the year.
    let year = year + (month - 1).div_euclid(12);
    let month = (month - 1).rem_euclid(12) + 1;
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `Date.UTC(year, month0, day, h, m, s, ms)`.
pub fn date_utc(year: f64, month0: f64, day: f64, h: f64, m: f64, s: f64, ms: f64) -> f64 {
    let parts = [year, month0, day, h, m, s, ms];
    if parts.iter().any(|v| !v.is_finite()) {
        return f64::NAN;
    }
    let year = year.trunc();
    let year = if (0.0..=99.0).contains(&year) {
        1900.0 + year
    } else {
        year
    };
    let days =
        days_from_civil(year as i64, month0.trunc() as i64 + 1, 1) as f64 + day.trunc() - 1.0;
    let time = h.trunc() * 3_600_000.0 + m.trunc() * 60_000.0 + s.trunc() * 1000.0 + ms.trunc();
    let value = days * 86_400_000.0 + time;
    if value.abs() > MAX_TIME_MS {
        f64::NAN
    } else {
        value
    }
}

/// `Date.parse(text)` for the formats upstream feeds use: ISO 8601 (date
/// only is UTC; a time without an offset is taken as UTC, the server's zone)
/// and RFC 2822-style dates. NaN when unrecognized.
pub fn date_parse(text: &str) -> f64 {
    let text = trim(text);
    if let Some(value) = parse_iso(text) {
        return value;
    }
    parse_legacy(text).unwrap_or(f64::NAN)
}

fn digits_at(bytes: &[u8], start: usize, count: usize) -> Option<i64> {
    let slice = bytes.get(start..start + count)?;
    if !slice.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(slice).ok()?.parse().ok()
}

fn parse_iso(text: &str) -> Option<f64> {
    let b = text.as_bytes();
    let mut i;
    let year = match b.first()? {
        b'+' | b'-' => {
            let v = digits_at(b, 1, 6)?;
            i = 7;
            if b[0] == b'-' {
                if v == 0 {
                    return None;
                }
                -v
            } else {
                v
            }
        }
        _ => {
            i = 4;
            digits_at(b, 0, 4)?
        }
    };
    let mut month = 1;
    let mut day = 1;
    if b.get(i) == Some(&b'-') {
        month = digits_at(b, i + 1, 2)?;
        i += 3;
        if b.get(i) == Some(&b'-') {
            day = digits_at(b, i + 1, 2)?;
            i += 3;
        }
    }
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let date_ms = days_from_civil(year, month, day) as f64 * 86_400_000.0;
    if i == b.len() {
        return Some(date_ms).filter(|v| v.abs() <= MAX_TIME_MS);
    }
    if b[i] != b'T' && b[i] != b't' && b[i] != b' ' {
        return None;
    }
    let hour = digits_at(b, i + 1, 2)?;
    if b.get(i + 3) != Some(&b':') {
        return None;
    }
    let minute = digits_at(b, i + 4, 2)?;
    i += 6;
    let mut second = 0;
    let mut milli = 0.0;
    if b.get(i) == Some(&b':') {
        second = digits_at(b, i + 1, 2)?;
        i += 3;
        if b.get(i) == Some(&b'.') || b.get(i) == Some(&b',') {
            let start = i + 1;
            let mut end = start;
            while end < b.len() && b[end].is_ascii_digit() {
                end += 1;
            }
            if end == start {
                return None;
            }
            let frac = &text[start..end.min(start + 3)];
            milli = format!("{frac:0<3}").parse::<f64>().ok()?;
            i = end;
        }
    }
    if hour > 24
        || minute > 59
        || second > 59
        || (hour == 24 && (minute > 0 || second > 0 || milli > 0.0))
    {
        return None;
    }
    let mut offset_min = 0;
    if i < b.len() {
        match b[i] {
            b'Z' | b'z' => i += 1,
            b'+' | b'-' => {
                let sign = if b[i] == b'-' { -1 } else { 1 };
                let oh = digits_at(b, i + 1, 2)?;
                let om = if b.get(i + 3) == Some(&b':') {
                    i += 6;
                    digits_at(b, i - 2, 2)?
                } else {
                    i += 5;
                    digits_at(b, i - 2, 2)?
                };
                if oh > 23 || om > 59 {
                    return None;
                }
                offset_min = sign * (oh * 60 + om);
            }
            _ => return None,
        }
    }
    if i != b.len() {
        return None;
    }
    let value = date_ms + (hour * 3_600_000 + minute * 60_000 + second * 1000) as f64 + milli
        - (offset_min * 60_000) as f64;
    Some(value).filter(|v| v.abs() <= MAX_TIME_MS)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The legacy forms V8 accepts that feeds use, such as
/// `Tue, 01 Jul 2025 10:00:00 GMT` and `2025/07/01 10:00:00`.
fn parse_legacy(text: &str) -> Option<f64> {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc2822(text) {
        return Some(parsed.timestamp_millis() as f64);
    }
    let cleaned = text
        .trim_end_matches(" GMT")
        .trim_end_matches(" UTC")
        .trim_end_matches(" Z");
    for format in [
        "%a, %d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%b %d %Y %H:%M:%S",
        "%a %b %d %Y %H:%M:%S",
    ] {
        if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(cleaned, format) {
            return Some(parsed.and_utc().timestamp_millis() as f64);
        }
    }
    for format in ["%Y/%m/%d", "%m/%d/%Y", "%b %d %Y", "%d %b %Y"] {
        if let Ok(parsed) = chrono::NaiveDate::parse_from_str(cleaned, format) {
            return Some(parsed.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis() as f64);
        }
    }
    None
}

/// `Date.now()` as a float.
pub fn now() -> f64 {
    crate::common::js::now_ms() as f64
}

// ---------------------------------------------------------------- sort ----

/// `Array#sort(comparator)`: a stable merge sort that reads the
/// comparator's number like JS (NaN and 0 keep order), so inconsistent
/// comparators cannot panic.
pub fn sort_by<T: Clone>(items: &mut [T], mut compare: impl FnMut(&T, &T) -> f64) {
    let len = items.len();
    if len < 2 {
        return;
    }
    let mut source: Vec<T> = items.to_vec();
    let mut width = 1;
    while width < len {
        merge_pass(&source, items, width, &mut compare);
        width *= 2;
        if width < len {
            source.clone_from_slice(items);
        }
    }
}

fn merge_pass<T: Clone>(
    source: &[T],
    target: &mut [T],
    width: usize,
    compare: &mut impl FnMut(&T, &T) -> f64,
) {
    let len = source.len();
    let mut start = 0;
    while start < len {
        let mid = (start + width).min(len);
        let end = (start + 2 * width).min(len);
        let (mut i, mut j, mut k) = (start, mid, start);
        while i < mid && j < end {
            // Take from the right only when it sorts strictly before.
            if compare(&source[j], &source[i]) < 0.0 {
                target[k] = source[j].clone();
                j += 1;
            } else {
                target[k] = source[i].clone();
                i += 1;
            }
            k += 1;
        }
        while i < mid {
            target[k] = source[i].clone();
            i += 1;
            k += 1;
        }
        while j < end {
            target[k] = source[j].clone();
            j += 1;
            k += 1;
        }
        start = end;
    }
}

/// An `f64` comparator result from an ordering.
pub fn ord(ordering: Ordering) -> f64 {
    match ordering {
        Ordering::Less => -1.0,
        Ordering::Equal => 0.0,
        Ordering::Greater => 1.0,
    }
}

/// `a.localeCompare(b)` with the default (root) collation.
pub fn locale_compare(a: &str, b: &str) -> f64 {
    use std::sync::OnceLock;
    static COLLATOR: OnceLock<icu_collator::CollatorBorrowed<'static>> = OnceLock::new();
    let collator = COLLATOR.get_or_init(|| {
        icu_collator::Collator::try_new(Default::default(), Default::default())
            .expect("root collation data is compiled in")
    });
    ord(collator.compare(a, b))
}

/// `String(a) < String(b)` style comparison by UTF-16 code units.
pub fn cmp16(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_print_like_js() {
        assert_eq!(fmt_num(1.0), "1");
        assert_eq!(fmt_num(-0.0), "0");
        assert_eq!(fmt_num(1e21), "1e+21");
        assert_eq!(fmt_num(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(fmt_num(1.5e-7), "1.5e-7");
        assert_eq!(fmt_num(f64::NAN), "NaN");
    }

    #[test]
    fn to_fixed_rounds_the_exact_value_half_up() {
        assert_eq!(to_fixed(0.125, 2), "0.13");
        assert_eq!(to_fixed(1.005, 2), "1.00");
        assert_eq!(to_fixed(2.5, 0), "3");
        assert_eq!(to_fixed(-2.5, 0), "-3");
        assert_eq!(to_fixed(-0.001, 2), "-0.00");
        assert_eq!(to_fixed(-0.0, 2), "0.00");
        assert_eq!(to_fixed(9.995, 2), "9.99");
        assert_eq!(to_fixed(99.5, 0), "100");
        assert_eq!(to_fixed(37.7749, 3), "37.775");
        assert_eq!(to_fixed(1e21, 2), "1e+21");
        assert_eq!(to_fixed(5e-324, 2), "0.00");
    }

    #[test]
    fn math_round_rounds_halves_up() {
        assert_eq!(math_round(2.5), 3.0);
        assert_eq!(math_round(-2.5), -2.0);
        assert_eq!(math_round(0.499_999_999_999_999_94), 0.0);
        assert_eq!(math_round(-0.4), 0.0);
    }

    #[test]
    fn stringify_matches_json_stringify() {
        let value = json!({"b": 1, "2": [1.5, null, "x\u{1}\n"], "1": true, "a": -0.0});
        assert_eq!(
            stringify(&value),
            r#"{"1":true,"2":[1.5,null,"x\u0001\n"],"b":1,"a":0}"#
        );
    }

    #[test]
    fn dates_round_trip() {
        assert_eq!(iso_string(0.0).unwrap(), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            iso_string(1_700_000_000_123.0).unwrap(),
            "2023-11-14T22:13:20.123Z"
        );
        assert_eq!(date_parse("2023-11-14T22:13:20.123Z"), 1_700_000_000_123.0);
        assert_eq!(date_parse("2023-11-14"), 1_699_920_000_000.0);
        assert_eq!(date_parse("2023-11-14T23:13:20+01:00"), 1_700_000_000_000.0);
        assert_eq!(
            date_parse("Tue, 14 Nov 2023 22:13:20 GMT"),
            1_700_000_000_000.0
        );
        assert!(date_parse("nope").is_nan());
        assert_eq!(
            iso_string(-62_198_755_200_000.0).unwrap(),
            "-000001-01-01T00:00:00.000Z"
        );
    }

    #[test]
    fn sort_is_stable_and_tolerates_nan() {
        let mut items = vec![(3, 1.0), (1, f64::NAN), (2, 1.0), (0, 0.0)];
        sort_by(&mut items, |a, b| a.1 - b.1);
        assert_eq!(items.len(), 4);
        let mut plain = vec![5, 3, 9, 1, 3, 7, 2];
        sort_by(&mut plain, |a, b| f64::from(*a - *b));
        assert_eq!(plain, vec![1, 2, 3, 3, 5, 7, 9]);
    }

    #[test]
    fn trims_js_whitespace() {
        assert_eq!(trim("\u{feff} a \u{3000}"), "a");
        assert_eq!(trim("\u{85}a"), "\u{85}a");
        assert_eq!(len16("a😀"), 3);
        assert_eq!(slice16("a😀b", 2), "a");
    }
}
