//! Overpass QL validation (port of `server/providers/overpass/query.js` and
//! `stripOverpassNoise` from `src/sources/overpass.js`).

use std::collections::HashSet;

use once_cell::sync::Lazy;
use regex::Regex;

use super::{OVERPASS_MAX_AROUND_M, OVERPASS_MAX_BBOX_DEG, OVERPASS_MAX_QL_TIMEOUT};
use crate::common::js;
use crate::providers::mapdata_support::encode_uri_component;

/// Every Overpass element-type specifier, including the combined shortcuts.
const ELEMENT_TYPES: &str = "node|way|relation|nwr|nw|nr|wr|rel";

// JS `\b` and `\w` are ASCII-only without the `u` flag; `\d` is ASCII too.
static SELECTOR_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(r"(?-u:\b)(?:{ELEMENT_TYPES}|area)(?-u:\b)")).expect("selector regex")
});
static AREA_ELEMENT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        r"(?i)(?-u:\b)(?:{ELEMENT_TYPES})\s*\(\s*area(?-u:\b)"
    ))
    .expect("area element regex")
});
const BBOX_PATTERN: &str = r"\(\s*(-?[0-9]+(?:\.[0-9]+)?)\s*,\s*(-?[0-9]+(?:\.[0-9]+)?)\s*,\s*(-?[0-9]+(?:\.[0-9]+)?)\s*,\s*(-?[0-9]+(?:\.[0-9]+)?)\s*\)";
static BBOX_RE: Lazy<Regex> = Lazy::new(|| Regex::new(BBOX_PATTERN).expect("bbox regex"));
static AROUND_RADIUS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)around(?:\.(?-u:\w)+)?:\s*([0-9.eE+-]+)").expect("around regex"));
static CONTROL_FLOW_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?-u:\b)(?:foreach|complete|retro|compare|convert|make)(?-u:\b)")
        .expect("control-flow regex")
});
static POLY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)(?-u:\b)poly\s*:").expect("poly regex"));
static OUT_STMT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^out(?-u:\b)").expect("out regex"));
static OUT_SET_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"->\s*\.((?-u:\w)+)").expect("out-set regex"));
static TAG_FILTER_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\[[^\]]*\]").expect("tag regex"));
static INPUT_SET_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\.([a-z_](?-u:\w)*)").expect("input-set regex"));
static AROUND_BOUND_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"around:\s*[0-9]").expect("around bound regex"));
static IS_IN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"is_in\s*\(").expect("is_in regex"));
static AREA_CALL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?-u:\b)area\s*\(").expect("area regex"));
static TIMEOUT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[timeout:\s*([0-9]+)\s*\]").expect("timeout regex"));
static BOUNDARY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)is_in\s*\(|(?-u:\b)pivot(?-u:\b)").expect("boundary regex"));

/// Whether a normalized query is boundary-class (the 30-day disk TTL).
pub fn is_overpass_boundary_query(cache_key: &str) -> bool {
    BOUNDARY_RE.is_match(cache_key)
}

/// Single-pass lexer: blank out quoted literals and strip comments.
pub fn strip_overpass_noise(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c == '"' || c == '\'' {
            i += 1;
            while i < n {
                if chars[i] == '\\' {
                    i += 2;
                    continue;
                }
                if chars[i] == c {
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push(c);
            out.push(c);
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < n && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
            out.push(' ');
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            i += 2;
            while i < n && chars[i] != '\n' {
                i += 1;
            }
            out.push(' ');
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// `(?<!\d)\.([a-z_]\w*)` with the look-behind done by hand.
fn input_sets(probe: &str) -> Vec<String> {
    INPUT_SET_RE
        .captures_iter(probe)
        .filter(|caps| {
            let start = caps.get(0).map_or(0, |m| m.start());
            !probe[..start]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_digit())
        })
        .filter_map(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .collect()
}

/// `sanitizeOverpassBody`: `Ok(form body)` or `Err(error message)`.
pub fn sanitize_overpass_body(raw_body: &str) -> Result<String, &'static str> {
    // `new URLSearchParams(body)` drops one leading `?`.
    let raw_body = raw_body.strip_prefix('?').unwrap_or(raw_body);
    let all: Vec<String> = url::form_urlencoded::parse(raw_body.as_bytes())
        .filter(|(k, _)| k == "data")
        .map(|(_, v)| v.into_owned())
        .collect();
    if all.len() != 1 {
        return Err("Exactly one data query is required");
    }
    let data = &all[0];
    if data
        .trim_matches(crate::providers::mapdata_support::is_js_space)
        .is_empty()
    {
        return Err("Missing Overpass data query");
    }
    let stripped = strip_overpass_noise(data);

    for caps in AROUND_RADIUS_RE.captures_iter(&stripped) {
        let radius = js::number(caps.get(1).map_or("", |m| m.as_str()));
        if !radius.is_finite() || radius > OVERPASS_MAX_AROUND_M {
            return Err("Overpass around radius too large");
        }
    }
    for caps in BBOX_RE.captures_iter(&stripped) {
        let n = |i: usize| js::number(caps.get(i).map_or("", |m| m.as_str()));
        let (s, w, north, e) = (n(1), n(2), n(3), n(4));
        if (north - s).abs() > OVERPASS_MAX_BBOX_DEG || (e - w).abs() > OVERPASS_MAX_BBOX_DEG {
            return Err("Overpass bbox too large");
        }
    }
    if CONTROL_FLOW_RE.is_match(&stripped) {
        return Err("Unsupported Overpass construct");
    }
    if POLY_RE.is_match(&stripped) {
        return Err("Overpass poly filter not allowed");
    }

    let mut bounded_sets: HashSet<String> = HashSet::new();
    for stmt in stripped.split(';') {
        let stmt = crate::providers::mapdata_support::js_trim(stmt);
        if stmt.is_empty() || stmt.starts_with('[') || OUT_STMT_RE.is_match(stmt) {
            continue;
        }
        let mut out_sets = Vec::new();
        let body = OUT_SET_RE.replace_all(stmt, |caps: &regex::Captures| {
            out_sets.push(caps[1].to_string());
            " ".to_string()
        });
        let probe = TAG_FILTER_RE.replace_all(&body, " ");
        if AREA_ELEMENT_RE.is_match(&probe) {
            return Err("Overpass area-bounded element selector not allowed");
        }
        let has_selector = SELECTOR_RE.is_match(&probe);
        let direct_bound = AROUND_BOUND_RE.is_match(&probe)
            || BBOX_RE.is_match(&probe)
            || IS_IN_RE.is_match(&probe)
            || AREA_CALL_RE.is_match(&probe);
        let set_bound = input_sets(&probe)
            .iter()
            .any(|name| bounded_sets.contains(name));
        let bounded = direct_bound || set_bound;
        if has_selector && !bounded {
            return Err("Overpass query has an unbounded selector");
        }
        if bounded {
            bounded_sets.extend(out_sets);
        }
    }

    let clamped = TIMEOUT_RE.replace_all(data, |caps: &regex::Captures| {
        let requested = js::number(&caps[1]);
        let requested = if requested == 0.0 || requested.is_nan() {
            OVERPASS_MAX_QL_TIMEOUT
        } else {
            requested
        };
        let value = requested.min(OVERPASS_MAX_QL_TIMEOUT);
        format!(
            "[timeout:{}]",
            crate::providers::mapdata_support::js_number_string(value)
        )
    });
    Ok(format!("data={}", encode_uri_component(&clamped)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_class_queries_detected() {
        assert!(is_overpass_boundary_query(
            "[out:json][timeout:25];is_in(37.5,14.2)->.a;area.a[\"boundary\"=\"administrative\"][\"admin_level\"];out tags;"
        ));
        assert!(is_overpass_boundary_query(
            "[out:json][timeout:25];area(3600039152)->.x;rel(pivot.x);out geom;"
        ));
        assert!(!is_overpass_boundary_query(
            "[out:json][timeout:25];( way(around:1200,30.27,-97.74)[\"leisure\"][\"name\"]; );out geom;"
        ));
        assert!(!is_overpass_boundary_query(
            "[out:json][timeout:12];way[\"highway\"~\"motorway|trunk\"](30.1,-97.9,30.5,-97.5);out geom;"
        ));
    }

    fn form(query: &str) -> String {
        format!("data={}", encode_uri_component(query))
    }

    #[test]
    fn app_queries_validate_and_timeouts_clamp() {
        let ok = sanitize_overpass_body(&form(
            "[out:json][timeout:90];node(around:10,30.27,-97.74)[\"name\"=\"x\"];out;",
        ))
        .unwrap();
        assert_eq!(
            ok,
            form("[out:json][timeout:30];node(around:10,30.27,-97.74)[\"name\"=\"x\"];out;")
        );
        assert!(
            sanitize_overpass_body(&form(
                "[out:json];is_in(37.5,14.2)->.a;area.a[\"boundary\"];out tags;"
            ))
            .is_ok()
        );
        assert!(
            sanitize_overpass_body(&form(
                "[out:json];area(3600039152)->.x;rel(pivot.x);out geom;"
            ))
            .is_ok()
        );
        assert!(
            sanitize_overpass_body(&form(
                "[out:json];way[\"highway\"](30.1,-97.9,30.5,-97.5);out geom;"
            ))
            .is_ok()
        );
    }

    #[test]
    fn abusive_queries_are_refused() {
        let cases = [
            ("", "Exactly one data query is required"),
            ("data=a&data=b", "Exactly one data query is required"),
            ("data=%20", "Missing Overpass data query"),
        ];
        for (body, error) in cases {
            assert_eq!(sanitize_overpass_body(body), Err(error), "{body}");
        }
        let queries = [
            (
                "node(around:5e7,1,2);out;",
                "Overpass around radius too large",
            ),
            (
                "node(around.a:60000);out;",
                "Overpass around radius too large",
            ),
            ("way(0,0,20,1);out;", "Overpass bbox too large"),
            (
                "foreach{node(around:1,1,1);}",
                "Unsupported Overpass construct",
            ),
            (
                "node(poly:\"1 2 3 4\");out;",
                "Overpass poly filter not allowed",
            ),
            (
                "is_in(1,2)->.a;way[\"highway\"](area.a);out;",
                "Overpass area-bounded element selector not allowed",
            ),
            (
                "way[\"highway\"];out;",
                "Overpass query has an unbounded selector",
            ),
            (
                "way[highway]->.a;node(around:1,1,1);way.a;out;",
                "Overpass query has an unbounded selector",
            ),
            (
                "node[\"note\"=\"around:1\"];out;",
                "Overpass query has an unbounded selector",
            ),
            (
                "node(around:1,1,1);way[is_in];out;",
                "Overpass query has an unbounded selector",
            ),
        ];
        for (query, error) in queries {
            assert_eq!(sanitize_overpass_body(&form(query)), Err(error), "{query}");
        }
    }

    #[test]
    fn noise_stripper_respects_quotes() {
        assert_eq!(
            strip_overpass_noise("a\"//x\"b // c\nd /* e */f"),
            "a\"\"b  \nd  f"
        );
    }
}
