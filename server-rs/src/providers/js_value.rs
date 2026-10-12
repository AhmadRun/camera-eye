//! JavaScript conversions applied to parsed JSON values (`Number(v)`,
//! `String(v)`, truthiness, `??`), for providers that port JS code reading
//! upstream payloads of uncertain shape.

use serde_json::Value;

use crate::common::js;

/// `Number(value)`; a missing value (`undefined`) is NaN.
pub fn to_number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Null) => 0.0,
        Some(Value::Bool(b)) => f64::from(u8::from(*b)),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => js::number(s),
        Some(Value::Array(items)) => match items.as_slice() {
            [] => 0.0,
            [one] => js::number(&to_js_string(one)),
            _ => f64::NAN,
        },
        Some(Value::Object(_)) => f64::NAN,
    }
}

/// `typeof value === 'number' && Number.isFinite(value)`.
pub fn finite_number(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64).filter(|n| n.is_finite())
}

/// `String(value)` for a present value.
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

/// `String(number)` for the values these payloads carry.
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
    if value.fract() == 0.0 && value.abs() < 1e21 {
        return format!("{value:.0}");
    }
    format!("{value}")
}

/// JS truthiness.
pub fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|f| f != 0.0 && !f.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

/// `a || b || ...`: the first truthy value.
pub fn or<'a>(values: &[Option<&'a Value>]) -> Option<&'a Value> {
    values.iter().copied().find(|v| truthy(*v)).flatten()
}

/// `a ?? b ?? ...`: the first value that is neither missing nor null.
pub fn nullish<'a>(values: &[Option<&'a Value>]) -> Option<&'a Value> {
    values
        .iter()
        .copied()
        .find(|v| !matches!(v, None | Some(Value::Null)))
        .flatten()
}

/// `value?.key`: only objects have properties worth reading here.
pub fn get<'a>(value: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    value
        .and_then(|v| v.as_object())
        .and_then(|map| map.get(key))
}

/// `Math.round(value)`.
pub fn round(value: f64) -> f64 {
    (value + 0.5).floor()
}

/// A JSON number the way `JSON.stringify` writes it (NaN and Infinity are null).
pub fn number_value(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        Value::from(value as i64)
    } else {
        serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn conversions_match_js() {
        assert_eq!(to_number(Some(&json!("12"))), 12.0);
        assert_eq!(to_number(Some(&json!(null))), 0.0);
        assert!(to_number(None).is_nan());
        assert_eq!(to_number(Some(&json!([" 3 "]))), 3.0);
        assert_eq!(to_js_string(&json!(244060365)), "244060365");
        assert_eq!(to_js_string(&json!(1.5)), "1.5");
        assert_eq!(to_js_string(&json!(-0.0)), "0");
        assert_eq!(to_js_string(&json!({})), "[object Object]");
        assert_eq!(to_js_string(&json!([1, null, "a"])), "1,,a");
        assert_eq!(round(2.5), 3.0);
        assert_eq!(round(-2.5), -2.0);
        assert_eq!(number_value(124.0), json!(124));
        assert_eq!(number_value(f64::NAN), json!(null));
        assert!(truthy(Some(&json!("x"))));
        assert!(!truthy(Some(&json!(0))));
        assert_eq!(
            or(&[Some(&json!("")), Some(&json!("b"))]),
            Some(&json!("b"))
        );
        assert_eq!(
            nullish(&[Some(&json!(null)), Some(&json!(""))]),
            Some(&json!(""))
        );
    }
}
