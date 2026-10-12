//! Helpers the tools share: `src/tools/results.js` and argument access.

use serde_json::{Map, Value, json};

use crate::mcp::catalog::Failure;
use crate::mcp::js;
use crate::mcp::services::Services;
use crate::mcp::view::{self, Follow, Frame};

pub const DEFAULT_LIMIT: f64 = 25.0;
pub const MAX_LIMIT: f64 = 200.0;

/// An argument that is present (not `undefined`).
pub fn arg<'a>(args: &'a Value, key: &str) -> Option<&'a Value> {
    args.get(key)
}

/// A numeric argument, or `default` (`args.x ?? default`).
pub fn num_arg(args: &Value, key: &str, default: f64) -> f64 {
    match args.get(key) {
        None | Some(Value::Null) => default,
        Some(value) => js::to_number(value),
    }
}

/// A string argument (`args.x`, when a string).
pub fn str_arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str)
}

/// `capRows(rows, args.limit)`: `{ total, returned, truncated, rows }`.
pub fn cap_rows(rows: Vec<Value>, limit: Option<&Value>) -> Map<String, Value> {
    let limit = match limit {
        None => DEFAULT_LIMIT,
        Some(value) => js::to_number(value),
    };
    let keep = js::math_min([limit, MAX_LIMIT]);
    let total = rows.len();
    let kept: Vec<Value> = if keep.is_nan() {
        Vec::new()
    } else {
        let count = if keep < 0.0 {
            (total as f64 + keep.ceil()).max(0.0) as usize
        } else {
            keep as usize
        };
        rows.into_iter().take(count).collect()
    };
    let mut out = Map::new();
    out.insert("total".into(), json!(total));
    out.insert("returned".into(), json!(kept.len()));
    out.insert("truncated".into(), json!(kept.len() < total));
    out.insert("rows".into(), Value::Array(kept));
    out
}

/// `countNoun(count, singular, plural)`.
pub fn count_noun(count: usize, singular: &str, plural: Option<&str>) -> String {
    if count == 1 {
        format!("{count} {singular}")
    } else {
        match plural {
            Some(plural) => format!("{count} {plural}"),
            None => format!("{count} {singular}s"),
        }
    }
}

/// `thinEvenly(items, max)`.
pub fn thin_evenly<T: Clone>(items: &[T], max: usize) -> Vec<T> {
    if items.len() <= max {
        return items.to_vec();
    }
    let step = (items.len() - 1) as f64 / (max - 1) as f64;
    (0..max)
        .map(|index| items[js::math_round(index as f64 * step) as usize].clone())
        .collect()
}

/// `suggestView(services, ...)`, a TypeError on an invalid view.
pub fn suggest(
    services: &Services,
    frame: Frame<'_>,
    layers: &[&str],
    follow: Option<Follow>,
) -> Result<Value, Failure> {
    view::suggest_view(services, frame, layers, follow).map_err(Failure::Type)
}

/// `{ ...a, ...b }` for objects.
pub fn spread(mut target: Map<String, Value>, source: Map<String, Value>) -> Map<String, Value> {
    for (key, value) in source {
        target.insert(key, value);
    }
    target
}

/// Build an object from (key, value) pairs, skipping `None` (undefined).
pub fn obj<const N: usize>(pairs: [(&str, Option<Value>); N]) -> Value {
    let mut out = Map::new();
    for (key, value) in pairs {
        if let Some(value) = value {
            out.insert(key.to_string(), value);
        }
    }
    Value::Object(out)
}

/// A record's property as an optional value (`record.key`).
pub fn prop(record: &Value, key: &str) -> Option<Value> {
    record.get(key).cloned()
}

/// `record.key ?? null`.
pub fn prop_or_null(record: &Value, key: &str) -> Value {
    record.get(key).cloned().unwrap_or(Value::Null)
}

/// A record's finite number (`Number.isFinite(record.key)`).
pub fn fnum(record: &Value, key: &str) -> Option<f64> {
    record.get(key).and_then(js::finite)
}

/// `Number.isFinite(value) ? Number(value.toFixed(digits)) : null`.
pub fn round_or_null(value: Option<f64>, digits: usize) -> Value {
    match value.filter(|v| v.is_finite()) {
        Some(v) => js::num(js::round_to(v, digits)),
        None => Value::Null,
    }
}

/// A string value when it is a non-empty string, else null.
pub fn text_or_null(value: Option<&Value>) -> Value {
    match value.and_then(Value::as_str) {
        Some(s) if !s.is_empty() => json!(s),
        _ => Value::Null,
    }
}
