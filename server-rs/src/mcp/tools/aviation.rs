//! `src/tools/queries/aviation.js`: aviation queries over the live
//! aircraft sources.

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, round_or_null, spread, str_arg, suggest, thin_evenly};
use crate::mcp::area::{LatLon, area_center, area_contains, distance_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::live::{self, Aircraft, Snapshot};
use crate::mcp::view::{Follow, Frame};

const MAX_TRACK_POINTS: usize = 200;

fn text_or_null(value: &Option<String>) -> Value {
    match value {
        Some(text) if !text.is_empty() => json!(text),
        _ => Value::Null,
    }
}

/// `aircraftRow(record, from)`.
fn aircraft_row(record: &Aircraft, from: Option<LatLon>) -> Value {
    let mut row = Map::new();
    row.insert("id".into(), json!(record.id));
    row.insert(
        "callsign".into(),
        if record.callsign.is_empty() {
            Value::Null
        } else {
            json!(record.callsign)
        },
    );
    row.insert("lat".into(), round_or_null(Some(record.latitude), 5));
    row.insert("lon".into(), round_or_null(Some(record.longitude), 5));
    row.insert(
        "altitude_m".into(),
        round_or_null(record.baro_altitude_m.or(record.ellipsoid_altitude_m), 0),
    );
    row.insert("on_ground".into(), json!(record.on_ground));
    row.insert("speed_mps".into(), round_or_null(record.speed_mps, 1));
    row.insert("course_deg".into(), round_or_null(record.course_deg, 0));
    row.insert(
        "vertical_rate_mps".into(),
        round_or_null(record.vertical_rate_mps, 1),
    );
    row.insert("type_code".into(), text_or_null(&record.type_code));
    row.insert("registration".into(), text_or_null(&record.registration));
    row.insert("operator".into(), text_or_null(&record.operator));
    row.insert(
        "origin_country".into(),
        text_or_null(&record.origin_country),
    );
    row.insert(
        "last_contact".into(),
        js::iso_time(record.contact_time_ms.or(record.position_time_ms)),
    );
    if let Some(from) = from {
        let point = LatLon::new(record.latitude, record.longitude);
        row.insert(
            "distance_km".into(),
            round_or_null(Some(distance_km(from, point)), 1),
        );
    }
    Value::Object(row)
}

fn snapshot_info<R>(snapshot: &Snapshot<R>) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert(
        "source".into(),
        snapshot.source.clone().map_or(Value::Null, Value::String),
    );
    out.insert(
        "coverage".into(),
        snapshot.coverage.clone().map_or(Value::Null, Value::String),
    );
    out.insert("observed_at".into(), js::iso_time(snapshot.observed_at_ms));
    out.insert("freshness".into(), json!(snapshot.freshness));
    out
}

/// A comparison operand as JS reads `null >= n`: null is 0.
fn altitude_operand(record: &Aircraft) -> f64 {
    record
        .baro_altitude_m
        .or(record.ellipsoid_altitude_m)
        .unwrap_or(0.0)
}

pub async fn aircraft_in_area(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let center = area_center(&area);
    let military = args.get("military").is_some_and(js::truthy);
    let snapshot = if military {
        live::military_snapshot(&services.api).await?
    } else {
        live::flight_snapshot(&services.api, Some((center.lat, center.lon))).await?
    };
    let airborne_only = args.get("airborne_only").is_some_and(js::truthy);
    let min = args
        .get("min_altitude_m")
        .filter(|v| !v.is_null())
        .map(js::to_number);
    let max = args
        .get("max_altitude_m")
        .filter(|v| !v.is_null())
        .map(js::to_number);
    let mut rows: Vec<Value> = snapshot
        .records
        .iter()
        .filter(|record| {
            area_contains(&area, LatLon::new(record.latitude, record.longitude))
                && !(airborne_only && record.on_ground)
                && min.is_none_or(|min| altitude_operand(record) >= min)
                && max.is_none_or(|max| altitude_operand(record) <= max)
        })
        .map(|record| aircraft_row(record, Some(center)))
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&a["distance_km"]) - js::to_number(&b["distance_km"])
    });
    let kind = if military {
        "military aircraft"
    } else {
        "aircraft"
    };
    let coverage = snapshot.coverage.clone().unwrap_or_default();
    let regional = coverage.to_lowercase().contains("regional");
    let mut notes = Vec::new();
    if regional {
        notes.push(format!("regional feed: {coverage}"));
    }
    if snapshot.freshness == "stale" {
        notes.push("data may be stale".into());
    }
    let summary = format!(
        "{} {kind} in {}{}",
        rows.len(),
        area.label,
        if notes.is_empty() {
            ".".to_string()
        } else {
            format!(" ({}).", notes.join("; "))
        }
    );
    let view = suggest(
        services,
        Frame::Area(&area),
        &[if military { "military" } else { "flights" }],
        None,
    )?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let data = spread(
        spread(data, cap_rows(rows, args.get("limit"))),
        snapshot_info(&snapshot),
    );
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn find_aircraft(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let given: Vec<&str> = ["callsign", "icao24", "registration"]
        .into_iter()
        .filter(|key| args.get(*key).is_some_and(|v| !v.is_null()))
        .collect();
    if given.len() != 1 {
        return Err(Failure::tool(
            "invalid_arguments",
            "Give exactly one of callsign, icao24 or registration",
        ));
    }
    let key = given[0];
    let wanted = js::trim(&js::to_js_string(&args[key])).to_uppercase();
    let feeds: Vec<&str> = if key == "registration" {
        vec!["military"]
    } else {
        vec!["civil", "military"]
    };
    let api = &services.api;
    let settled: Vec<Result<Snapshot<Aircraft>, Failure>> = if key == "registration" {
        vec![live::military_snapshot(api).await]
    } else {
        let (civil, military) = tokio::join!(
            live::flight_snapshot(api, None),
            live::military_snapshot(api)
        );
        vec![civil, military]
    };
    let unavailable: Vec<&str> = feeds
        .iter()
        .zip(&settled)
        .filter(|(_, result)| result.is_err())
        .map(|(name, _)| *name)
        .collect();
    if unavailable.len() == feeds.len() {
        let first = settled.into_iter().next().and_then(Result::err);
        return Err(first.unwrap_or_else(|| Failure::other("no feed")));
    }
    let field = |record: &Aircraft| -> String {
        let value = match key {
            "callsign" => Some(record.callsign.clone()),
            "icao24" => Some(record.id.clone()),
            _ => record.registration.clone(),
        };
        js::trim(&value.unwrap_or_default()).to_uppercase()
    };
    let mut seen = std::collections::HashSet::new();
    let mut matches: Vec<(&Aircraft, &str)> = Vec::new();
    for (index, result) in settled.iter().enumerate() {
        if let Ok(snapshot) = result {
            for record in &snapshot.records {
                if field(record) == wanted && seen.insert(record.id.clone()) {
                    matches.push((record, feeds[index]));
                }
            }
        }
    }
    let rows: Vec<Value> = matches
        .iter()
        .map(|(record, _)| aircraft_row(record, None))
        .collect();
    let answered: Vec<Value> = feeds
        .iter()
        .zip(&settled)
        .filter_map(|(name, result)| {
            result.as_ref().ok().map(|snapshot| {
                let mut entry = Map::new();
                entry.insert("feed".into(), json!(name));
                Value::Object(spread(entry, snapshot_info(snapshot)))
            })
        })
        .collect();
    let stale: Vec<String> = answered
        .iter()
        .filter(|feed| feed["freshness"] == "stale")
        .map(|feed| js::to_js_string(&feed["feed"]))
        .collect();
    let mut summary = if rows.is_empty() {
        format!("No aircraft with {key} {wanted} is currently reported.")
    } else {
        format!(
            "Found {} with {key} {wanted}.",
            count_noun(rows.len(), "aircraft", Some("aircraft"))
        )
    };
    if !unavailable.is_empty() {
        summary.push_str(&format!(
            " The {} feed did not answer.",
            unavailable.join(" and ")
        ));
    }
    if !stale.is_empty() {
        summary.push_str(&format!(
            " The {} feed data may be stale.",
            stale.join(" and ")
        ));
    }
    let view = match matches.as_slice() {
        [(record, feed)] => suggest(
            services,
            Frame::point(record.latitude, record.longitude),
            &[],
            Some(Follow {
                kind: if *feed == "military" {
                    "military_aircraft"
                } else {
                    "aircraft"
                }
                .into(),
                id: record.id.clone(),
                cockpit: false,
            }),
        )?,
        _ => Value::Null,
    };
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("unavailable_feeds".into(), json!(unavailable));
    data.insert("stale_feeds".into(), json!(stale));
    data.insert("feeds".into(), Value::Array(answered));
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn get_aircraft_track(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let icao24 = str_arg(&args, "icao24").unwrap_or_default().to_lowercase();
    let military = args.get("military").is_some_and(js::truthy);
    let mut points = match live::aircraft_track(&services.api, military, &icao24).await {
        Ok(points) => points,
        Err(error) if error.status() == Some(404) => Vec::new(),
        Err(error) => return Err(error),
    };
    js::sort_by(&mut points, |a, b| {
        a.observed_at_ms.unwrap_or(f64::NAN) - b.observed_at_ms.unwrap_or(f64::NAN)
    });
    let kept = thin_evenly(&points, MAX_TRACK_POINTS);
    let summary = match (points.first(), points.last()) {
        (Some(first), Some(last)) => format!(
            "{} for {icao24} from {} to {}.",
            count_noun(points.len(), "position", None),
            js::to_js_string(&js::iso_time(first.observed_at_ms)),
            js::to_js_string(&js::iso_time(last.observed_at_ms))
        ),
        _ => format!("No recent track is available for {icao24}."),
    };
    let view = match points.last() {
        Some(last) => suggest(
            services,
            Frame::point(last.latitude, last.longitude),
            &[],
            Some(Follow {
                kind: if military {
                    "military_aircraft"
                } else {
                    "aircraft"
                }
                .into(),
                id: icao24.clone(),
                cockpit: false,
            }),
        )?,
        None => Value::Null,
    };
    let rows: Vec<Value> = kept
        .iter()
        .map(|point| {
            json!({
                "time": js::iso_time(point.observed_at_ms),
                "lat": round_or_null(Some(point.latitude), 5),
                "lon": round_or_null(Some(point.longitude), 5),
                "altitude_m": round_or_null(point.baro_altitude_m, 0),
                "on_ground": point.on_ground,
            })
        })
        .collect();
    Ok(ToolResult::new(
        summary,
        json!({
            "view": view,
            "icao24": icao24,
            "total": points.len(),
            "returned": kept.len(),
            "thinned": kept.len() < points.len(),
            "points": rows,
        }),
    ))
}

fn or_null(value: Option<&Value>) -> Value {
    match value {
        None | Some(Value::Null) => Value::Null,
        Some(value) => value.clone(),
    }
}

fn airport(value: Option<&Value>) -> Value {
    match value.filter(|v| js::truthy(v)) {
        Some(value) => {
            let text = |key: &str| match value.get(key) {
                Some(v) if js::truthy(v) => v.clone(),
                _ => Value::Null,
            };
            json!({
                "code": text("code"),
                "name": text("name"),
                "lat": or_null(value.get("lat")),
                "lon": or_null(value.get("lon")),
            })
        }
        None => Value::Null,
    }
}

/// `a || b` over JSON values, as text for a template literal.
fn either_text(a: &Value, b: &Value) -> String {
    let chosen = if js::truthy(a) { a } else { b };
    js::to_js_string(chosen)
}

pub async fn get_aircraft_info(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let present = |key: &str| args.get(key).is_some_and(|v| !v.is_null());
    if !present("icao24") && !present("callsign") {
        return Err(Failure::tool(
            "invalid_arguments",
            "Give icao24, callsign or both",
        ));
    }
    let icao24 = str_arg(&args, "icao24").map(str::to_lowercase);
    let callsign = str_arg(&args, "callsign").map(str::to_uppercase);
    let api = &services.api;
    let lookup = |kind: &'static str, id: Option<String>| async move {
        match id {
            None => Ok(Value::Null),
            Some(id) => live::enrichment(api, kind, &id).await,
        }
    };
    let (kind, route) = tokio::try_join!(
        lookup("type", icao24.clone()),
        lookup("route", callsign.clone())
    )?;
    let aircraft = if kind.get("found").is_some_and(js::truthy) {
        json!({
            "icao24": icao24,
            "type_code": or_null(kind.get("typeCode")),
            "type_name": or_null(kind.get("typeName")),
            "registration": or_null(kind.get("registration")),
        })
    } else {
        Value::Null
    };
    let flight = if route.get("found").is_some_and(js::truthy) {
        json!({
            "callsign": callsign,
            "airline": or_null(route.get("airline")),
            "origin": airport(route.get("origin")),
            "destination": airport(route.get("destination")),
        })
    } else {
        Value::Null
    };
    let mut parts = Vec::new();
    if let Some(icao24) = icao24.as_ref().filter(|s| !s.is_empty()) {
        parts.push(if aircraft.is_null() {
            format!("no type is known for {icao24}")
        } else {
            let name = if js::truthy(&aircraft["type_name"]) {
                js::to_js_string(&aircraft["type_name"])
            } else if js::truthy(&aircraft["type_code"]) {
                js::to_js_string(&aircraft["type_code"])
            } else {
                "an aircraft of unknown type".into()
            };
            let registration = if js::truthy(&aircraft["registration"]) {
                format!(" ({})", js::to_js_string(&aircraft["registration"]))
            } else {
                String::new()
            };
            format!("aircraft {icao24} is {name}{registration}")
        });
    }
    if let Some(callsign) = callsign.as_ref().filter(|s| !s.is_empty()) {
        parts.push(if flight.is_null() {
            format!("no route is known for {callsign}")
        } else {
            let end = |key: &str| {
                let place = &flight[key];
                if place.is_null() {
                    "undefined".to_string()
                } else {
                    either_text(&place["code"], &place["name"])
                }
            };
            let airline = if js::truthy(&flight["airline"]) {
                format!(" ({})", js::to_js_string(&flight["airline"]))
            } else {
                String::new()
            };
            format!(
                "flight {callsign} flies {} to {}{airline}",
                end("origin"),
                end("destination")
            )
        });
    }
    let summary = parts.join("; ");
    let mut chars = summary.chars();
    let summary = match chars.next() {
        Some(first) => format!("{}{}.", first.to_uppercase(), chars.as_str()),
        None => ".".to_string(),
    };
    Ok(ToolResult::new(
        summary,
        json!({"aircraft": aircraft, "route": flight}),
    ))
}
