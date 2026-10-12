//! Port of `src/tools/schema.js`: the JSON Schema subset tool definitions
//! use, checked when a tool is defined and enforced on every call.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use regex::Regex;
use serde_json::Value;

use super::js;

const KEYWORDS: [&str; 16] = [
    "type",
    "description",
    "title",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "enum",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "pattern",
    "minItems",
    "maxItems",
    "default",
];
const TYPES: [&str; 6] = ["object", "array", "string", "number", "integer", "boolean"];

/// A schema `pattern` as a Rust regex. JS `u`-mode `\d` is ASCII only.
fn compile(pattern: &str) -> Option<Regex> {
    static CACHE: OnceLock<Mutex<HashMap<String, Option<Regex>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let mut cache = cache.lock().unwrap_or_else(|e| e.into_inner());
    cache
        .entry(pattern.to_string())
        .or_insert_with(|| Regex::new(&pattern.replace("\\d", "[0-9]")).ok())
        .clone()
}

/// `assertSupportedSchema`: an error message when a schema uses a keyword
/// or type the validator does not check.
pub fn assert_supported(schema: &Value, path: &str) -> Result<(), String> {
    let Some(map) = schema.as_object() else {
        return Err(format!("{path} must be an object"));
    };
    for key in js::js_keys(map) {
        if !KEYWORDS.contains(&key.as_str()) {
            return Err(format!("{path} uses unsupported keyword {key}"));
        }
    }
    let kind = map.get("type");
    if !kind
        .and_then(Value::as_str)
        .is_some_and(|t| TYPES.contains(&t))
    {
        let shown = kind.map_or("undefined".into(), js::to_js_string);
        return Err(format!("{path} has unsupported type {shown}"));
    }
    if let Some(pattern) = map.get("pattern") {
        let text = js::to_js_string(pattern);
        if compile(&text).is_none() {
            return Err(format!("Invalid regular expression: /{text}/u"));
        }
    }
    if map
        .get("additionalProperties")
        .is_some_and(|v| !v.is_boolean())
    {
        return Err(format!("{path}.additionalProperties must be true or false"));
    }
    if let Some(Value::Object(properties)) = map.get("properties") {
        for name in js::js_keys(properties) {
            assert_supported(&properties[name.as_str()], &format!("{path}.{name}"))?;
        }
    }
    if let Some(items) = map.get("items").filter(|v| js::truthy(v)) {
        assert_supported(items, &format!("{path}[]"))?;
    }
    Ok(())
}

fn number_of(schema: &Value, key: &str) -> Option<f64> {
    schema.get(key).filter(|v| !v.is_null()).map(js::to_number)
}

/// `validateValue`: the problems with a value, empty when it conforms.
pub fn validate(schema: &Value, value: &Value, path: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let fail = |problems: &mut Vec<String>, message: String| {
        problems.push(format!("{path} {message}"));
    };
    match schema.get("type").and_then(Value::as_str) {
        Some("object") => match value {
            Value::Object(object) => {
                if let Some(Value::Array(required)) = schema.get("required") {
                    for name in required {
                        let name = js::to_js_string(name);
                        if !object.contains_key(&name) {
                            fail(&mut problems, format!("is missing {name}"));
                        }
                    }
                }
                let properties = schema.get("properties").and_then(Value::as_object);
                let closed = schema.get("additionalProperties") == Some(&Value::Bool(false));
                for name in js::js_keys(object) {
                    let child = &object[name.as_str()];
                    match properties.and_then(|p| p.get(name.as_str())) {
                        Some(child_schema) => problems.extend(validate(
                            child_schema,
                            child,
                            &format!("{path}.{name}"),
                        )),
                        None if closed => {
                            fail(&mut problems, format!("has unknown property {name}"))
                        }
                        None => {}
                    }
                }
            }
            _ => fail(&mut problems, "must be an object".into()),
        },
        Some("array") => match value {
            Value::Array(items) => {
                let length = items.len() as f64;
                if let Some(min) = number_of(schema, "minItems").filter(|min| length < *min) {
                    fail(
                        &mut problems,
                        format!("must have at least {} items", js::fmt_num(min)),
                    );
                }
                if let Some(max) = number_of(schema, "maxItems").filter(|max| length > *max) {
                    fail(
                        &mut problems,
                        format!("must have at most {} items", js::fmt_num(max)),
                    );
                }
                if let Some(item_schema) = schema.get("items").filter(|v| js::truthy(v)) {
                    for (index, item) in items.iter().enumerate() {
                        problems.extend(validate(item_schema, item, &format!("{path}[{index}]")));
                    }
                }
            }
            _ => fail(&mut problems, "must be an array".into()),
        },
        Some("string") => match value {
            Value::String(text) => {
                let length = js::len16(text) as f64;
                if let Some(min) = number_of(schema, "minLength").filter(|min| length < *min) {
                    fail(
                        &mut problems,
                        format!("must be at least {} characters", js::fmt_num(min)),
                    );
                }
                if let Some(max) = number_of(schema, "maxLength").filter(|max| length > *max) {
                    fail(
                        &mut problems,
                        format!("must be at most {} characters", js::fmt_num(max)),
                    );
                }
                if let Some(pattern) = schema.get("pattern") {
                    let pattern = js::to_js_string(pattern);
                    if !compile(&pattern).is_some_and(|re| re.is_match(text)) {
                        fail(&mut problems, format!("must match {pattern}"));
                    }
                }
            }
            _ => fail(&mut problems, "must be a string".into()),
        },
        Some(kind @ ("number" | "integer")) => match value.as_f64().filter(|v| v.is_finite()) {
            Some(number) => {
                if kind == "integer" && number.fract() != 0.0 {
                    fail(&mut problems, "must be an integer".into());
                }
                if let Some(min) = number_of(schema, "minimum").filter(|min| number < *min) {
                    fail(
                        &mut problems,
                        format!("must be at least {}", js::fmt_num(min)),
                    );
                }
                if let Some(max) = number_of(schema, "maximum").filter(|max| number > *max) {
                    fail(
                        &mut problems,
                        format!("must be at most {}", js::fmt_num(max)),
                    );
                }
            }
            None => fail(&mut problems, "must be a number".into()),
        },
        Some("boolean") if !value.is_boolean() => {
            fail(&mut problems, "must be true or false".into());
        }
        _ => {}
    }
    if let Some(Value::Array(options)) = schema.get("enum")
        && !options.iter().any(|option| same_value_zero(option, value))
    {
        let listed: Vec<String> = options.iter().map(js::stringify).collect();
        fail(
            &mut problems,
            format!("must be one of {}", listed.join(", ")),
        );
    }
    problems
}

/// `Array#includes` equality for JSON values: primitives by value, objects
/// and arrays never equal a freshly parsed argument.
fn same_value_zero(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reports_each_problem_with_its_path() {
        let schema = json!({
            "type": "object",
            "properties": {
                "n": {"type": "integer", "minimum": 1, "maximum": 5},
                "tags": {"type": "array", "items": {"type": "string", "enum": ["a", "b"]}, "maxItems": 2},
                "id": {"type": "string", "pattern": "^\\d{2}$"}
            },
            "required": ["n"],
            "additionalProperties": false
        });
        assert!(validate(&schema, &json!({"n": 3}), "arguments").is_empty());
        assert_eq!(
            validate(
                &schema,
                &json!({"n": 0.5, "x": 1, "tags": ["c", "a", "b"], "id": "٣٣"}),
                "arguments"
            ),
            vec![
                "arguments.n must be an integer",
                "arguments.n must be at least 1",
                "arguments has unknown property x",
                "arguments.tags must have at most 2 items",
                "arguments.tags[0] must be one of \"a\", \"b\"",
                "arguments.id must match ^\\d{2}$",
            ]
        );
        assert_eq!(
            validate(&schema, &json!([]), "arguments"),
            vec!["arguments must be an object"]
        );
        assert_eq!(
            validate(&schema, &json!({}), "arguments"),
            vec!["arguments is missing n"]
        );
    }

    #[test]
    fn rejects_unsupported_keywords() {
        assert!(assert_supported(&json!({"type": "object"}), "s").is_ok());
        assert_eq!(
            assert_supported(&json!({"type": "object", "oneOf": []}), "s"),
            Err("s uses unsupported keyword oneOf".into())
        );
        assert_eq!(
            assert_supported(&json!({"type": "date"}), "s"),
            Err("s has unsupported type date".into())
        );
    }
}
