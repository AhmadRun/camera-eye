//! `src/tools/queries/space.js`: launches, satellite passes and satellites
//! overhead.

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, num_arg, round_or_null, spread, suggest};
use crate::mcp::area::{NamedPoint, resolve_point};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::satellites::{
    Satrec, find_next_satellite_pass, look_angles_at, parse_tle_text, read_group,
    tle_catalog_number, twoline2satrec,
};
use crate::mcp::services::{self, Services};
use crate::mcp::view::{Follow, Frame};

fn text(value: Option<&Value>) -> Value {
    match value.and_then(Value::as_str) {
        Some(s) if !s.is_empty() => json!(s),
        _ => Value::Null,
    }
}

fn path<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter()
        .try_fold(value, |current, key| current.get(*key))
}

pub async fn get_recent_launches(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let (payload, stale) = services::launch_snapshot(&services.api).await?;
    let launches = if payload.is_array() {
        &payload
    } else {
        &payload["results"]
    };
    let Some(launches) = launches.as_array() else {
        return Err(Failure::tool(
            "malformed",
            "The launch feed returned no launches",
        ));
    };
    let mut rows: Vec<Value> = launches
        .iter()
        .map(|launch| {
            let rocket = match path(launch, &["rocket", "configuration", "full_name"]) {
                None | Some(Value::Null) => path(launch, &["rocket", "configuration", "name"]),
                some => some,
            };
            json!({
                "id": text(launch.get("id")),
                "name": text(launch.get("name")),
                "time": text(launch.get("net")),
                "status": text(path(launch, &["status", "name"])),
                "provider": text(path(launch, &["launch_service_provider", "name"])),
                "rocket": text(rocket),
                "pad": text(path(launch, &["pad", "name"])),
                "location": text(path(launch, &["pad", "location", "name"])),
                "mission": text(path(launch, &["mission", "name"])),
                "orbit": text(path(launch, &["mission", "orbit", "name"])),
            })
        })
        .filter(|launch| js::truthy(&launch["name"]))
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::locale_compare(&js::to_js_string(&b["time"]), &js::to_js_string(&a["time"]))
    });
    let latest = rows
        .first()
        .map(|row| format!("; latest {}", js::to_js_string(&row["name"])))
        .unwrap_or_default();
    let summary = format!(
        "{} in the last 30 days{}{}",
        count_noun(rows.len(), "launch", Some("launches")),
        latest,
        if stale { " (data may be stale)." } else { "." }
    );
    let view = suggest(
        services,
        Frame::Point {
            lat: 20.0,
            lon: 0.0,
            altitude_m: 15_000_000.0,
        },
        &["rocket-launches"],
        None,
    )?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("stale".into(), json!(stale));
    Ok(ToolResult::new(summary, Value::Object(data)))
}

const ISS_NORAD: i64 = 25544;
const COMPASS: [&str; 16] = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW",
    "NNW",
];

fn compass(degrees: f64) -> Value {
    let index = js::math_round((((degrees % 360.0) + 360.0) % 360.0) / 22.5) % 16.0;
    if index.is_finite() && index >= 0.0 {
        json!(COMPASS[index as usize])
    } else {
        Value::Null
    }
}

struct Entry {
    name: String,
    norad: Option<i64>,
    satrec: Satrec,
}

async fn read_catalog(services: &Services, group: &str) -> Result<(Vec<Entry>, bool), Failure> {
    let result = read_group(&services.api, group).await?;
    if !result.ok {
        return Err(Failure::tool(
            "unavailable",
            format!(
                "The {group} satellite catalog is unavailable (HTTP {})",
                result.status
            ),
        ));
    }
    let entries = parse_tle_text(&result.text)
        .into_iter()
        .map(|entry| Entry {
            norad: tle_catalog_number(&entry.line1),
            satrec: twoline2satrec(&entry.line1, &entry.line2),
            name: entry.name,
        })
        .collect();
    Ok((entries, result.stale))
}

fn match_satellite<'a>(entries: &'a [Entry], wanted: Option<&Value>) -> Option<&'a Entry> {
    let Some(wanted) = wanted.filter(|w| !w.is_null()) else {
        return entries.iter().find(|e| e.norad == Some(ISS_NORAD));
    };
    let text = js::trim(&js::to_js_string(wanted)).to_uppercase();
    let number = if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        Some(js::str_to_number(&text))
    } else {
        None
    };
    entries
        .iter()
        .find(|e| number.is_some_and(|n| e.norad.is_some_and(|norad| norad as f64 == n)))
        .or_else(|| entries.iter().find(|e| e.name.to_uppercase() == text))
        .or_else(|| {
            entries
                .iter()
                .find(|e| e.name.to_uppercase().contains(&text))
        })
}

fn point_json(point: &NamedPoint) -> Value {
    json!({"label": point.label, "lat": js::num(point.lat), "lon": js::num(point.lon)})
}

fn follow_satellite(
    services: &Services,
    point: &NamedPoint,
    entry: &Entry,
) -> Result<Value, Failure> {
    match entry.norad.filter(|n| *n != 0) {
        Some(norad) => suggest(
            services,
            Frame::Point {
                lat: point.lat,
                lon: point.lon,
                altitude_m: 3_000_000.0,
            },
            &[],
            Some(Follow {
                kind: "satellite".into(),
                id: norad.to_string(),
                cockpit: false,
            }),
        ),
        None => Ok(Value::Null),
    }
}

fn norad_json(entry: &Entry) -> Value {
    entry.norad.map_or(Value::Null, |n| json!(n))
}

pub async fn next_satellite_pass(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let point = resolve_point(&args["location"], Some(services)).await?;
    let group = args
        .get("group")
        .and_then(Value::as_str)
        .unwrap_or("stations")
        .to_string();
    let (entries, stale) = read_catalog(services, &group).await?;
    let stale_note = if stale {
        " (orbit data may be stale)"
    } else {
        ""
    };
    let Some(satellite) = match_satellite(&entries, args.get("satellite")) else {
        let wanted = args
            .get("satellite")
            .filter(|v| !v.is_null())
            .map_or("ISS".to_string(), js::to_js_string);
        return Err(Failure::tool(
            "invalid_arguments",
            format!("No satellite matching \"{wanted}\" is in the {group} group"),
        ));
    };
    let hours = num_arg(&args, "hours", 24.0);
    let visible_only = args.get("visible_only") == Some(&Value::Bool(true));
    let pass = find_next_satellite_pass(
        &satellite.satrec,
        point.lat,
        point.lon,
        js::now(),
        num_arg(&args, "min_elevation_deg", 10.0),
        hours,
        visible_only,
    );
    let name = &satellite.name;
    let view = follow_satellite(services, &point, satellite)?;
    let Some(pass) = pass else {
        let visible = if args.get("visible_only").is_some_and(js::truthy) {
            "visible "
        } else {
            ""
        };
        return Ok(ToolResult::new(
            format!(
                "{name} has no {visible}pass over {} in the next {} hours{stale_note}.",
                point.label,
                js::fmt_num(hours)
            ),
            json!({
                "view": view,
                "stale": stale,
                "location": point_json(&point),
                "satellite": name,
                "norad": norad_json(satellite),
                "pass": null,
            }),
        ));
    };
    let rise = js::iso_time(Some(pass.rise_ms));
    let summary = format!(
        "{name} next rises over {} at {} in the {}, peaking at {}°{}{stale_note}.",
        point.label,
        js::to_js_string(&rise),
        js::to_js_string(&compass(pass.rise_az_deg)),
        js::fmt_num(js::math_round(pass.max_elev_deg)),
        if pass.visible {
            "; visible to the naked eye"
        } else {
            ""
        }
    );
    Ok(ToolResult::new(
        summary,
        json!({
            "view": view,
            "stale": stale,
            "location": point_json(&point),
            "satellite": name,
            "norad": norad_json(satellite),
            "pass": {
                "rise": rise,
                "peak": js::iso_time(Some(pass.max_elev_ms)),
                "set": js::iso_time(Some(pass.set_ms)),
                "max_elevation_deg": round_or_null(Some(pass.max_elev_deg), 1),
                "rise_azimuth_deg": round_or_null(Some(pass.rise_az_deg), 0),
                "rise_direction": compass(pass.rise_az_deg),
                "visible": pass.visible,
            },
        }),
    ))
}

pub async fn satellites_overhead(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let point = resolve_point(&args["location"], Some(services)).await?;
    let group = args
        .get("group")
        .and_then(Value::as_str)
        .unwrap_or("stations")
        .to_string();
    let minimum = num_arg(&args, "min_elevation_deg", 10.0);
    let at = js::now();
    let (entries, stale) = read_catalog(services, &group).await?;
    let mut rows: Vec<Value> = entries
        .iter()
        .filter_map(|entry| {
            let look = look_angles_at(&entry.satrec, at, point.lat, point.lon)?;
            (look.elev_deg >= minimum).then(|| {
                json!({
                    "name": entry.name,
                    "norad": norad_json(entry),
                    "elevation_deg": round_or_null(Some(look.elev_deg), 1),
                    "azimuth_deg": round_or_null(Some(look.az_deg), 0),
                    "direction": compass(look.az_deg),
                })
            })
        })
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&b["elevation_deg"]) - js::to_number(&a["elevation_deg"])
    });
    let at_text = js::to_js_string(&js::iso_time(Some(at)));
    let summary = format!(
        "{} from the {group} group {} at least {}° above {} at {at_text}{}",
        count_noun(rows.len(), "satellite", None),
        if rows.len() == 1 { "is" } else { "are" },
        js::fmt_num(minimum),
        point.label,
        if stale {
            " (orbit data may be stale)."
        } else {
            "."
        }
    );
    let view = suggest(
        services,
        Frame::Point {
            lat: point.lat,
            lon: point.lon,
            altitude_m: 3_000_000.0,
        },
        &["satellites"],
        None,
    )?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("stale".into(), json!(stale));
    data.insert("location".into(), point_json(&point));
    data.insert("group".into(), json!(group));
    data.insert("at".into(), js::iso_time(Some(at)));
    Ok(ToolResult::new(summary, Value::Object(data)))
}
