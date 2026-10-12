//! `src/tools/queries/media.js`: public cameras (radio is not on MCP).

use base64::Engine;
use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, fnum, round_or_null, spread, suggest, text_or_null};
use crate::mcp::area::{Area, LatLon, area_center, area_contains_value, distance_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, Image, ToolResult};
use crate::mcp::js;
use crate::mcp::services::{cctv, template};
use crate::mcp::view::Frame;

const FRAME_TYPES: [&str; 3] = ["image/jpeg", "image/png", "image/webp"];
const MAX_FRAME_BYTES: usize = 3 * 1024 * 1024;

struct Catalog {
    cameras: Vec<Value>,
    trimmed: Vec<Value>,
}

async fn read_catalog(ctx: &Ctx) -> Result<Catalog, Failure> {
    let payload = cctv::catalog(&ctx.services.api).await?;
    let cameras = payload["sources"]
        .as_array()
        .map(|sources| {
            sources
                .iter()
                .filter(|camera| {
                    camera.get("id").is_some_and(js::truthy)
                        && fnum(camera, "lat").is_some()
                        && fnum(camera, "lon").is_some()
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let trimmed = payload
        .get("trimmedPacks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(Catalog { cameras, trimmed })
}

/// A relational operand as JS reads it: undefined is NaN.
fn operand(value: Option<&Value>) -> f64 {
    value.map_or(f64::NAN, js::to_number)
}

/// `boxesOverlap(area, region)`.
fn boxes_overlap(area: &Area, region: &Value) -> bool {
    let north = operand(region.get("north"));
    let south = operand(region.get("south"));
    let east = operand(region.get("east"));
    let west_r = operand(region.get("west"));
    if area.south > north || area.north < south {
        return false;
    }
    let spans: Vec<(f64, f64)> = if area.west <= area.east {
        vec![(area.west, area.east)]
    } else {
        vec![(area.west, 180.0), (-180.0, area.east)]
    };
    spans
        .iter()
        .any(|(west, east_s)| *west <= east && *east_s >= west_r)
}

/// `Set#has` over JSON values (SameValueZero for primitives).
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

pub async fn find_cctv_cameras(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let center = area_center(&area);
    let catalog = read_catalog(&ctx).await?;
    let found: Vec<&Value> = catalog
        .cameras
        .iter()
        .filter(|camera| area_contains_value(&area, camera))
        .collect();
    let packs: Vec<&Value> = found
        .iter()
        .filter_map(|camera| camera.get("pack").filter(|p| js::truthy(p)))
        .collect();
    let mut trimmed = Vec::new();
    for entry in &catalog.trimmed {
        if entry.is_null() {
            return Err(Failure::Type(
                "Cannot read properties of null (reading 'region')".into(),
            ));
        }
        let keep = match entry.get("region").filter(|r| js::truthy(r)) {
            Some(region) => boxes_overlap(&area, region),
            None => entry
                .get("pack")
                .is_some_and(|pack| packs.iter().any(|p| same_value(p, pack))),
        };
        if keep {
            trimmed.push(entry.clone());
        }
    }
    let mut rows: Vec<Value> = found
        .iter()
        .map(|camera| {
            let point = LatLon::new(
                fnum(camera, "lat").unwrap_or(f64::NAN),
                fnum(camera, "lon").unwrap_or(f64::NAN),
            );
            json!({
                "id": camera["id"],
                "name": text_or_null(camera.get("name")),
                "city": text_or_null(camera.get("city")),
                "provider": text_or_null(camera.get("provider")),
                "lat": round_or_null(fnum(camera, "lat"), 6),
                "lon": round_or_null(fnum(camera, "lon"), 6),
                "heading_deg": round_or_null(fnum(camera, "headingDeg"), 0),
                "feed_type": text_or_null(camera.get("feedType")),
                "credit": text_or_null(camera.get("credit")),
                "distance_km": round_or_null(Some(distance_km(center, point)), 2),
            })
        })
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&a["distance_km"]) - js::to_number(&b["distance_km"])
    });
    let lead = if !rows.is_empty() || !trimmed.is_empty() {
        format!(
            "{} in {}",
            count_noun(rows.len(), "public camera", None),
            area.label
        )
    } else {
        format!("The camera catalog has no cameras in {}", area.label)
    };
    let tail = if trimmed.is_empty() {
        ".".to_string()
    } else {
        let parts: Vec<String> = trimmed
            .iter()
            .map(|entry| {
                format!(
                    "{} of {} from {}",
                    template(entry.get("served")),
                    template(entry.get("available")),
                    template(entry.get("pack"))
                )
            })
            .collect();
        format!(
            " (the catalog serves only some cameras here: {}).",
            parts.join(", ")
        )
    };
    let view = suggest(services, Frame::Area(&area), &["cctv"], None)?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("complete".into(), json!(trimmed.is_empty()));
    data.insert("catalog_trimmed".into(), Value::Array(trimmed));
    Ok(ToolResult::new(
        format!("{lead}{tail}"),
        Value::Object(data),
    ))
}

pub async fn get_cctv_snapshot(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let wanted = &args["camera_id"];
    let catalog = read_catalog(&ctx).await?;
    let Some(camera) = catalog.cameras.iter().find(|candidate| {
        candidate
            .get("id")
            .is_some_and(|id| id.is_string() && id == wanted)
    }) else {
        return Err(Failure::tool(
            "invalid_arguments",
            format!(
                "No camera has id {}; use find_cctv_cameras first",
                template(Some(wanted))
            ),
        ));
    };
    let (kind, bytes) = cctv::frame(&services.api, camera).await?;
    if !FRAME_TYPES.contains(&kind.as_str()) {
        let name = match camera.get("name").filter(|v| js::truthy(v)) {
            Some(name) => js::to_js_string(name),
            None => template(camera.get("id")),
        };
        return Err(Failure::tool(
            "unavailable",
            format!("Camera {name} has no still image right now"),
        ));
    }
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(Failure::tool(
            "unavailable",
            "The camera image is too large to return",
        ));
    }
    let name = match text_or_null(camera.get("name")) {
        Value::Null => camera["id"].clone(),
        name => name,
    };
    let city = camera
        .get("city")
        .filter(|v| js::truthy(v))
        .map(|c| format!(" in {}", js::to_js_string(c)));
    let credit = camera
        .get("credit")
        .filter(|v| js::truthy(v))
        .map(|c| format!(", courtesy of {}", js::to_js_string(c)));
    let summary = format!(
        "Current view from {}{}{}.",
        js::to_js_string(&name),
        city.unwrap_or_default(),
        credit.unwrap_or_default()
    );
    let view = suggest(
        services,
        Frame::Point {
            lat: fnum(camera, "lat").unwrap_or(f64::NAN),
            lon: fnum(camera, "lon").unwrap_or(f64::NAN),
            altitude_m: 1_500.0,
        },
        &["cctv"],
        None,
    )?;
    let data = json!({
        "view": view,
        "id": camera["id"],
        "name": name,
        "city": text_or_null(camera.get("city")),
        "provider": text_or_null(camera.get("provider")),
        "credit": text_or_null(camera.get("credit")),
        "mime_type": kind,
        "bytes": bytes.len(),
    });
    let mut result = ToolResult::new(summary, data);
    result.images = Some(vec![Image {
        mime_type: kind,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes),
    }]);
    Ok(result)
}
