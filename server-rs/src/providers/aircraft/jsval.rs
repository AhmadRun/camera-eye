//! JavaScript value semantics over `serde_json::Value`, for the record
//! normalizers ported from `src/` (truthiness, `Number(...)`, `String(...)`,
//! `Math.round` and number-to-JSON output).

use serde_json::Value;

use crate::common::js;

/// JS truthiness of a JSON value (`None` is `undefined`).
pub fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|v| v != 0.0 && !v.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_)) | Some(Value::Object(_)) => true,
    }
}

/// `Number(value)`; `None` is `undefined` (NaN).
pub fn to_number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Null) => 0.0,
        Some(Value::Bool(b)) => f64::from(u8::from(*b)),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => js::number(s),
        Some(array @ Value::Array(_)) => js::number(&to_js_string(array)),
        Some(Value::Object(_)) => f64::NAN,
    }
}

/// `String(value)` for a JSON value.
pub fn to_js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => number_to_string(n.as_f64().unwrap_or(f64::NAN)),
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

/// `String(value ?? '')`.
pub fn string_or_empty(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(other) => to_js_string(other),
    }
}

/// `String(value || '')`.
pub fn string_or_empty_falsy(value: Option<&Value>) -> String {
    if truthy(value) {
        value.map(to_js_string).unwrap_or_default()
    } else {
        String::new()
    }
}

/// JS `Number.prototype.toString()` for the values these modules produce.
pub fn number_to_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if value == 0.0 {
        return "0".into();
    }
    let abs = value.abs();
    if value.fract() == 0.0 && abs < 1e21 {
        return format!("{}", value as i128);
    }
    if (1e-6..1e21).contains(&abs) {
        return format!("{value}");
    }
    let formatted = format!("{value:e}");
    match formatted.split_once('e') {
        Some((mantissa, exp)) if !exp.starts_with('-') => format!("{mantissa}e+{exp}"),
        _ => formatted,
    }
}

/// `Math.round`: halves round toward +Infinity.
pub fn round(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let floor = value.floor();
    if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

/// A number as `JSON.stringify` writes it: integers without a fraction,
/// non-finite values as `null`.
pub fn num(value: f64) -> Value {
    if !value.is_finite() {
        return Value::Null;
    }
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        return Value::from(value as i64);
    }
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// `num` for an optional number (`null` when absent).
pub fn opt_num(value: Option<f64>) -> Value {
    value.map_or(Value::Null, num)
}

/// `encodeURIComponent`.
pub fn encode_uri_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Standard base64 (`Buffer.from(...).toString('base64')`).
pub fn base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn js_conversions() {
        assert!(!truthy(Some(&json!(0))));
        assert!(!truthy(Some(&json!(""))));
        assert!(truthy(Some(&json!("0"))));
        assert!(to_number(None).is_nan());
        assert_eq!(to_number(Some(&Value::Null)), 0.0);
        assert_eq!(to_number(Some(&json!("12"))), 12.0);
        assert_eq!(to_number(Some(&json!([5]))), 5.0);
        assert_eq!(to_number(Some(&json!(true))), 1.0);
        assert_eq!(to_js_string(&json!(123456)), "123456");
        assert_eq!(to_js_string(&json!(1.5)), "1.5");
        assert_eq!(to_js_string(&json!({"a": 1})), "[object Object]");
        assert_eq!(number_to_string(-0.0), "0");
        assert_eq!(number_to_string(30.25), "30.25");
        assert_eq!(number_to_string(1e21), "1e+21");
        assert_eq!(number_to_string(1e-7), "1e-7");
        assert_eq!(round(-2.5), -2.0);
        assert_eq!(round(2.5), 3.0);
        assert_eq!(num(1600.0), json!(1600));
        assert_eq!(num(-0.0), json!(0));
        assert_eq!(num(f64::NAN), Value::Null);
        assert_eq!(encode_uri_component("a b&c"), "a%20b%26c");
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64(b"ab"), "YWI=");
    }
}
