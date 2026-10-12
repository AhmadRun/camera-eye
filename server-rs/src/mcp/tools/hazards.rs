//! `src/tools/queries/hazards.js`: earthquakes and active fires.

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, fnum, num_arg, spread, suggest};
use crate::mcp::area::{area_contains_value, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services;
use crate::mcp::view::Frame;

pub async fn get_earthquakes(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = match args.get("area").filter(|a| js::truthy(a)) {
        Some(area) => Some(resolve_area(area, Some(services)).await?),
        None => None,
    };
    let minimum = num_arg(&args, "min_magnitude", 2.5);
    let mut events: Vec<Value> = services::earthquake_snapshot(&services.api)
        .await?
        .into_iter()
        .filter(|event| {
            fnum(event, "mag").is_some_and(|mag| mag >= minimum)
                && area
                    .as_ref()
                    .is_none_or(|area| area_contains_value(area, event))
        })
        .collect();
    js::sort_by(&mut events, |a, b| {
        let by_mag = fnum(b, "mag").unwrap_or(f64::NAN) - fnum(a, "mag").unwrap_or(f64::NAN);
        if by_mag != 0.0 && !by_mag.is_nan() {
            return by_mag;
        }
        fnum(b, "time").unwrap_or(0.0) - fnum(a, "time").unwrap_or(0.0)
    });
    let where_ = match &area {
        Some(area) => format!(" in {}", area.label),
        None => " worldwide".into(),
    };
    let strongest = match events.first() {
        Some(event) => {
            let place = event
                .get("place")
                .and_then(Value::as_str)
                .filter(|p| !p.is_empty());
            format!(
                "; strongest M{}{}",
                js::to_fixed(fnum(event, "mag").unwrap_or(f64::NAN), 1),
                place.map(|p| format!(" {p}")).unwrap_or_default()
            )
        }
        None => String::new(),
    };
    let summary = format!(
        "{} of M{}+ in the last 24 hours{}{}.",
        count_noun(events.len(), "earthquake", None),
        js::fmt_num(minimum),
        where_,
        strongest
    );
    let view = match &area {
        Some(area) => suggest(services, Frame::Area(area), &["earthquakes"], None)?,
        None => suggest(
            services,
            Frame::Point {
                lat: 20.0,
                lon: 0.0,
                altitude_m: 15_000_000.0,
            },
            &["earthquakes"],
            None,
        )?,
    };
    let mut result = cap_rows(events, args.get("limit"));
    let rows: Vec<Value> = result["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|event| {
                    let id = match event.get("usgsId") {
                        Some(Value::Null) | None => {
                            event.get("stableId").cloned().unwrap_or(Value::Null)
                        }
                        Some(id) => id.clone(),
                    };
                    json!({
                        "id": id,
                        "magnitude": event["mag"],
                        "place": event["place"],
                        "time": js::iso_time(fnum(event, "time")),
                        "lat": event["lat"],
                        "lon": event["lon"],
                        "depth_km": event["depthKm"],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    result.insert("rows".into(), Value::Array(rows));
    let mut data = Map::new();
    data.insert("view".into(), view);
    let data = spread(data, result);
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn get_active_fires(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let Some(snapshot) = services::fire_snapshot(&services.api).await? else {
        return Err(Failure::tool(
            "unavailable",
            "Fire detections need a NASA FIRMS key configured for this server",
        ));
    };
    let mut fires: Vec<Value> = snapshot["fires"]
        .as_array()
        .map(|list| {
            list.iter()
                .filter(|fire| area_contains_value(&area, fire))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let frp = |fire: &Value| match fire.get("frp") {
        None | Some(Value::Null) => 0.0,
        Some(value) => js::to_number(value),
    };
    js::sort_by(&mut fires, |a, b| frp(b) - frp(a));
    let stale = snapshot.get("stale") == Some(&Value::Bool(true));
    let missing: Vec<Value> = snapshot
        .get("sources")
        .filter(|s| js::truthy(s))
        .and_then(Value::as_array)
        .map(|sources| {
            sources
                .iter()
                .filter(|source| source.get("ok") == Some(&Value::Bool(false)))
                .map(|source| source.get("source").cloned().unwrap_or(Value::Null))
                .collect()
        })
        .unwrap_or_default();
    let mut notes = Vec::new();
    if stale {
        notes.push("data may be stale".to_string());
    }
    if !missing.is_empty() {
        let names: Vec<String> = missing
            .iter()
            .map(|m| {
                if m.is_null() {
                    String::new()
                } else {
                    js::to_js_string(m)
                }
            })
            .collect();
        notes.push(format!("no data from {}", names.join(", ")));
    }
    let summary = format!(
        "{} in {} in the last 24 hours{}",
        count_noun(fires.len(), "fire detection", None),
        area.label,
        if notes.is_empty() {
            ".".to_string()
        } else {
            format!(" ({}).", notes.join("; "))
        }
    );
    let view = suggest(services, Frame::Area(&area), &["local-firms"], None)?;
    let mut result = cap_rows(fires, args.get("limit"));
    let or_null = |fire: &Value, key: &str| match fire.get(key) {
        None | Some(Value::Null) => Value::Null,
        Some(v) => v.clone(),
    };
    let rows: Vec<Value> = result["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|fire| {
                    let acquired: Vec<String> = ["acqDate", "acqTime"]
                        .iter()
                        .filter_map(|key| {
                            fire.get(*key)
                                .filter(|v| js::truthy(v))
                                .map(js::to_js_string)
                        })
                        .collect();
                    let acquired = acquired.join(" ");
                    let mut row = Map::new();
                    row.insert(
                        "lat".into(),
                        fire.get("lat").cloned().unwrap_or(Value::Null),
                    );
                    row.insert(
                        "lon".into(),
                        fire.get("lon").cloned().unwrap_or(Value::Null),
                    );
                    if fire.get("lat").is_none() {
                        row.remove("lat");
                    }
                    if fire.get("lon").is_none() {
                        row.remove("lon");
                    }
                    row.insert("frp_mw".into(), or_null(fire, "frp"));
                    row.insert("confidence".into(), or_null(fire, "confidence"));
                    row.insert(
                        "acquired".into(),
                        if acquired.is_empty() {
                            Value::Null
                        } else {
                            json!(acquired)
                        },
                    );
                    row.insert("satellite".into(), or_null(fire, "satellite"));
                    row.insert("day_night".into(), or_null(fire, "daynight"));
                    Value::Object(row)
                })
                .collect()
        })
        .unwrap_or_default();
    result.insert("rows".into(), Value::Array(rows));
    let mut data = Map::new();
    data.insert("view".into(), view);
    data.insert("stale".into(), json!(stale));
    data.insert("missing_sources".into(), Value::Array(missing));
    data.insert(
        "fetched_at".into(),
        js::iso_time(fnum(&snapshot, "fetchedAt")),
    );
    let data = spread(data, result);
    Ok(ToolResult::new(summary, Value::Object(data)))
}
