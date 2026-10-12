//! `src/tools/queries/maritime.js`: maritime queries over the live vessel
//! source.

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, round_or_null, spread, str_arg, suggest, thin_evenly};
use crate::mcp::area::{
    LatLon, area_center, area_contains, area_radius_km, distance_km, resolve_area,
};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::live::{self, Snapshot, Vessel};
use crate::mcp::view::Frame;

const MAX_TRACK_POINTS: usize = 200;

fn text(value: &str) -> Value {
    if value.is_empty() {
        Value::Null
    } else {
        json!(value)
    }
}

fn vessel_row(record: &Vessel, from: Option<LatLon>) -> Value {
    let mut row = Map::new();
    row.insert("mmsi".into(), json!(record.id));
    row.insert("name".into(), text(&record.name));
    row.insert("imo".into(), text(&record.imo));
    row.insert("type".into(), text(&record.kind));
    row.insert("destination".into(), text(&record.destination));
    row.insert("lat".into(), round_or_null(Some(record.latitude), 5));
    row.insert("lon".into(), round_or_null(Some(record.longitude), 5));
    row.insert("speed_mps".into(), round_or_null(record.speed_mps, 1));
    row.insert("course_deg".into(), round_or_null(record.course_deg, 0));
    row.insert("heading_deg".into(), round_or_null(record.heading_deg, 0));
    row.insert("observed_at".into(), js::iso_time(record.observed_at_ms));
    if let Some(from) = from {
        let point = LatLon::new(record.latitude, record.longitude);
        row.insert(
            "distance_km".into(),
            round_or_null(Some(distance_km(from, point)), 1),
        );
    }
    Value::Object(row)
}

fn snapshot_info(snapshot: &Snapshot<Vessel>) -> Map<String, Value> {
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

pub async fn vessels_in_area(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let center = area_center(&area);
    let snapshot = live::vessel_snapshot(
        &services.api,
        Some((center.lat, center.lon, area_radius_km(&area))),
    )
    .await?;
    let wanted = str_arg(&args, "type")
        .map(str::to_lowercase)
        .filter(|w| !w.is_empty());
    let mut rows: Vec<Value> = snapshot
        .records
        .iter()
        .filter(|record| {
            area_contains(&area, LatLon::new(record.latitude, record.longitude))
                && wanted
                    .as_ref()
                    .is_none_or(|w| record.kind.to_lowercase().contains(w.as_str()))
        })
        .map(|record| vessel_row(record, Some(center)))
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&a["distance_km"]) - js::to_number(&b["distance_km"])
    });
    let kind = match str_arg(&args, "type").filter(|t| !t.is_empty()) {
        Some(kind) => format!("{kind} vessel"),
        None => "vessel".into(),
    };
    let summary = format!(
        "{} in {}{}",
        count_noun(rows.len(), &kind, None),
        area.label,
        if snapshot.freshness == "stale" {
            " (data may be stale)."
        } else {
            "."
        }
    );
    let view = suggest(services, Frame::Area(&area), &["ais-live-vessels"], None)?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let data = spread(
        spread(data, cap_rows(rows, args.get("limit"))),
        snapshot_info(&snapshot),
    );
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn find_vessel(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let given: Vec<&str> = ["mmsi", "imo", "name"]
        .into_iter()
        .filter(|key| args.get(*key).is_some_and(|v| !v.is_null()))
        .collect();
    if given.len() != 1 {
        return Err(Failure::tool(
            "invalid_arguments",
            "Give exactly one of mmsi, imo or name",
        ));
    }
    let key = given[0];
    let raw = js::to_js_string(&args[key]);
    let wanted = js::trim(&raw).to_uppercase();
    let matches = |record: &Vessel| match key {
        "mmsi" => record.id == wanted,
        "imo" => {
            let imo = &record.imo;
            let stripped = if imo.len() >= 3 && imo.as_bytes()[..3].eq_ignore_ascii_case(b"IMO") {
                &imo[3..]
            } else {
                imo.as_str()
            };
            stripped == wanted
        }
        _ => record.name.to_uppercase().contains(&wanted),
    };
    let snapshot = live::vessel_snapshot(&services.api, None).await?;
    let rows: Vec<Value> = snapshot
        .records
        .iter()
        .filter(|record| matches(record))
        .map(|record| vessel_row(record, None))
        .collect();
    let summary = if rows.is_empty() {
        format!("No vessel with {key} {raw} is currently reported.")
    } else {
        format!(
            "Found {} with {key} {raw}.",
            count_noun(rows.len(), "vessel", None)
        )
    };
    let view = if rows.len() == 1 {
        suggest(
            services,
            Frame::Point {
                lat: js::to_number(&rows[0]["lat"]),
                lon: js::to_number(&rows[0]["lon"]),
                altitude_m: 5_000.0,
            },
            &["ais-live-vessels"],
            None,
        )?
    } else {
        Value::Null
    };
    let mut data = Map::new();
    data.insert("view".into(), view);
    let data = spread(
        spread(data, cap_rows(rows, args.get("limit"))),
        snapshot_info(&snapshot),
    );
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn get_vessel_track(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let mmsi = js::to_js_string(&args["mmsi"]);
    let mut points: Vec<(f64, f64, f64)> = live::vessel_track(&services.api, &mmsi)
        .await?
        .into_iter()
        .filter_map(|(lat, lon, at)| at.map(|at| (lat, lon, at)))
        .collect();
    js::sort_by(&mut points, |a, b| a.2 - b.2);
    let kept = thin_evenly(&points, MAX_TRACK_POINTS);
    let summary = match (points.first(), points.last()) {
        (Some(first), Some(last)) => format!(
            "{} for {mmsi} from {} to {}.",
            count_noun(points.len(), "position", None),
            js::to_js_string(&js::iso_time(Some(first.2))),
            js::to_js_string(&js::iso_time(Some(last.2)))
        ),
        _ => format!("No recent track is available for {mmsi}."),
    };
    let view = match kept.last() {
        Some(last) => suggest(
            services,
            Frame::Point {
                lat: last.0,
                lon: last.1,
                altitude_m: 20_000.0,
            },
            &["ais-live-vessels"],
            None,
        )?,
        None => Value::Null,
    };
    let rows: Vec<Value> = kept
        .iter()
        .map(|(lat, lon, at)| {
            json!({
                "time": js::iso_time(Some(*at)),
                "lat": round_or_null(Some(*lat), 5),
                "lon": round_or_null(Some(*lon), 5),
            })
        })
        .collect();
    Ok(ToolResult::new(
        summary,
        json!({
            "view": view,
            "mmsi": args["mmsi"],
            "total": points.len(),
            "returned": kept.len(),
            "thinned": kept.len() < points.len(),
            "points": rows,
        }),
    ))
}
