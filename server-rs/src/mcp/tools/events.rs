//! `src/tools/queries/events.js`: the bundled Bhote Koshi 2026 outburst
//! flood event pack, read once through `/events/<id>/event.json`
//! (`src/sources/eventPacks.js`).

use serde_json::{Map, Value, json};

use super::util::{round_or_null, suggest};
use crate::mcp::area::{Area, LatLon, distance_km};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::data;
use crate::mcp::js;
use crate::mcp::services::{Services, template};
use crate::mcp::view::Frame;

const EVENT_ID: &str = "bhote-koshi-2026";
const ATTRIBUTION: &str = "Imagery: Vantor Open Data; reconstruction: GeoPera (CC BY-NC 4.0). \
Linked witness posts and the GeoGeorgeShadrach geolocation map keep their owners' terms.";

/// `events.getEvent(id)`, cached per services.
async fn get_event(services: &Services, id: &str) -> Result<Value, Failure> {
    if let Some(event) = services.events.lock().expect("event cache").get(id) {
        return Ok(event.clone());
    }
    let response = services
        .api
        .get(&format!("/events/{id}/event.json"))
        .await?;
    if !response.ok() {
        return Err(Failure::other(format!(
            "Event pack {id} unavailable ({})",
            response.status
        )));
    }
    let event = response.json().await?;
    services
        .events
        .lock()
        .expect("event cache")
        .insert(id.to_string(), event.clone());
    Ok(event)
}

fn path() -> Vec<(f64, f64)> {
    data::bhote_koshi_flood_path()
        .as_array()
        .map(|points| {
            points
                .iter()
                .map(|p| (js::to_number(&p[0]), js::to_number(&p[1])))
                .collect()
        })
        .unwrap_or_default()
}

fn path_length_km(path: &[(f64, f64)]) -> f64 {
    let mut total = 0.0;
    for index in 1..path.len() {
        total += distance_km(
            LatLon::new(path[index - 1].1, path[index - 1].0),
            LatLon::new(path[index].1, path[index].0),
        );
    }
    total
}

fn or_null(value: Option<&Value>) -> Value {
    value.cloned().unwrap_or(Value::Null)
}

/// Insert a property unless it is undefined.
fn put(map: &mut Map<String, Value>, key: &str, value: Option<&Value>) {
    if let Some(value) = value {
        map.insert(key.into(), value.clone());
    }
}

fn evidence_row(record: &Value) -> Value {
    let get = |path: &[&str]| {
        let mut value = Some(record);
        for key in path {
            value = value.filter(|v| !v.is_null()).and_then(|v| v.get(*key));
        }
        value.filter(|v| !v.is_null())
    };
    let mut row = Map::new();
    row.insert("sequence".into(), or_null(get(&["sequence"])));
    put(&mut row, "id", record.get("id"));
    row.insert("title".into(), or_null(get(&["title"])));
    row.insert("summary".into(), or_null(get(&["summary"])));
    row.insert("phase".into(), or_null(get(&["phase"])));
    row.insert(
        "lat".into(),
        round_or_null(record.get("lat").and_then(js::finite), 5),
    );
    row.insert(
        "lon".into(),
        round_or_null(record.get("lon").and_then(js::finite), 5),
    );
    row.insert(
        "location_confidence".into(),
        or_null(get(&["confidence", "location"])),
    );
    row.insert(
        "time_confidence".into(),
        or_null(get(&["confidence", "time"])),
    );
    row.insert(
        "captured_at".into(),
        or_null(get(&["timing", "capturedAt"])),
    );
    row.insert(
        "in_imagery".into(),
        json!(record.get("imageryCoverage").and_then(Value::as_str) == Some("within")),
    );
    row.insert("source_url".into(), or_null(get(&["media", "sourceUrl"])));
    row.insert("platform".into(), or_null(get(&["media", "platform"])));
    row.insert("via".into(), or_null(get(&["provenance", "via"])));
    Value::Object(row)
}

fn imagery_side(side: Option<&Value>) -> Value {
    match side.filter(|s| js::truthy(s)) {
        Some(side) => {
            let mut out = Map::new();
            put(&mut out, "observed_at", side.get("observedAt"));
            put(&mut out, "source", side.get("source"));
            Value::Object(out)
        }
        None => Value::Null,
    }
}

pub async fn get_bhote_koshi_flood(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let event = get_event(services, EVENT_ID).await?;
    let Some(spine) = event.get("evidenceSpine").and_then(Value::as_array) else {
        return Err(Failure::tool("malformed", "The event pack has no evidence"));
    };
    let wanted = args
        .get("phase")
        .and_then(Value::as_str)
        .map(str::to_lowercase);
    let mut records: Vec<&Value> = spine
        .iter()
        .filter(|record| {
            wanted.as_ref().is_none_or(|w| {
                record
                    .get("phase")
                    .and_then(Value::as_str)
                    .map(str::to_lowercase)
                    .as_ref()
                    == Some(w)
            })
        })
        .collect();
    js::sort_by(&mut records, |a, b| {
        let seq = |r: &Value| r.get("sequence").map_or(f64::NAN, js::to_number);
        seq(a) - seq(b)
    });
    let evidence: Vec<Value> = records.into_iter().map(evidence_row).collect();
    let mut phases: Vec<Option<&Value>> = Vec::new();
    for record in spine {
        let phase = record.get("phase");
        if !phases.iter().any(|p| match (p, phase) {
            (None, None) => true,
            (Some(a), Some(b)) => a == &b,
            _ => false,
        }) {
            phases.push(phase);
        }
    }
    let phases: Vec<Value> = phases.into_iter().map(or_null).collect();
    let path = path();
    let length_km = round_or_null(Some(path_length_km(&path)), 1);
    let phase_note = match args.get("phase").filter(|p| js::truthy(p)) {
        Some(phase) => format!(" in the {} phase", js::to_js_string(phase)),
        None => String::new(),
    };
    let summary = format!(
        "{}, observed {}: {} evidence record{}{phase_note} along a {} km mapped flood path.",
        template(event.get("title")),
        template(event.get("observedDate")),
        evidence.len(),
        if evidence.len() == 1 { "" } else { "s" },
        js::to_js_string(&length_km),
    );
    let lons: Vec<f64> = path.iter().map(|p| p.0).collect();
    let lats: Vec<f64> = path.iter().map(|p| p.1).collect();
    let frame = Area::bbox(
        js::math_min(lons.clone()),
        js::math_min(lats.clone()),
        js::math_max(lons),
        js::math_max(lats),
    );
    let view = suggest(services, Frame::Area(&frame), &["bhote-koshi-2026"], None)?;
    let point = |p: Option<&(f64, f64)>| match p {
        Some((lon, lat)) => json!({ "lat": js::num(*lat), "lon": js::num(*lon) }),
        None => Value::Null,
    };
    let witnesses: Vec<Value> = event
        .get("witnessAnchors")
        .and_then(Value::as_array)
        .map(|anchors| {
            anchors
                .iter()
                .map(|anchor| {
                    json!({
                        "title": or_null(anchor.get("title").filter(|v| !v.is_null())),
                        "lat": round_or_null(anchor.get("lat").and_then(js::finite), 6),
                        "lon": round_or_null(anchor.get("lon").and_then(js::finite), 6),
                        "source_url": or_null(anchor.get("sourceUrl").filter(|v| !v.is_null())),
                        "provenance": or_null(anchor.get("provenance").filter(|v| !v.is_null())),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let imagery = event.get("imagery").filter(|i| !i.is_null());
    let mut data = Map::new();
    data.insert("view".into(), view);
    put(&mut data, "id", event.get("id"));
    put(&mut data, "title", event.get("title"));
    put(&mut data, "observed_date", event.get("observedDate"));
    data.insert(
        "caveat".into(),
        or_null(
            event
                .get("reconstruction")
                .filter(|r| !r.is_null())
                .and_then(|r| r.get("caveat"))
                .filter(|v| !v.is_null()),
        ),
    );
    data.insert("phases".into(), Value::Array(phases));
    data.insert("evidence".into(), Value::Array(evidence));
    data.insert("witnesses".into(), Value::Array(witnesses));
    data.insert(
        "flood_path".into(),
        json!({
            "start": point(path.first()),
            "end": point(path.last()),
            "length_km": length_km,
            "description": "Simplified river centerline from the debris-dammed lake to Trishuli Bazaar.",
        }),
    );
    data.insert(
        "imagery".into(),
        json!({
            "box": or_null(imagery.and_then(|i| i.get("rectangle")).filter(|v| !v.is_null())),
            "before": imagery_side(imagery.and_then(|i| i.get("before"))),
            "after": imagery_side(imagery.and_then(|i| i.get("after"))),
        }),
    );
    data.insert("attribution".into(), json!(ATTRIBUTION));
    Ok(ToolResult::new(summary, Value::Object(data)))
}
