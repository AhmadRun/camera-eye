//! `src/tools/queries/surveillance.js`: license plate reader cameras
//! mapped in OpenStreetMap.

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, round_or_null, spread, suggest};
use crate::mcp::area::{LatLon, area_center, area_contains, distance_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::alpr;
use crate::mcp::services::vector_tiles::BBox;
use crate::mcp::view::Frame;

const MAX_SIDE_DEGREES: f64 = 3.0;

fn text(value: &Option<String>) -> Value {
    value.clone().map_or(Value::Null, Value::String)
}

pub async fn find_alpr_cameras(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    if area.west > area.east
        || area.north - area.south > MAX_SIDE_DEGREES
        || area.east - area.west > MAX_SIDE_DEGREES
    {
        return Err(Failure::tool(
            "invalid_arguments",
            "The area must be at most 3° on each side and not cross the antimeridian",
        ));
    }
    let bbox = BBox {
        west: area.west,
        south: area.south,
        east: area.east,
        north: area.north,
    };
    let result = services.alpr.fetch(&bbox).await?;
    if result.no_coverage {
        let mut data = cap_rows(Vec::new(), args.get("limit"));
        data.insert("coverage".into(), json!("US and Canada"));
        return Ok(ToolResult::new(
            format!(
                "{} is outside the US and Canadian camera coverage.",
                area.label
            ),
            Value::Object(data),
        ));
    }
    if result.zoom_in {
        return Err(Failure::tool(
            "invalid_arguments",
            "Use a smaller, city-sized area",
        ));
    }
    let center = area_center(&area);
    let mut rows: Vec<Value> = result
        .records
        .iter()
        .filter(|camera| area_contains(&area, LatLon::new(camera.latitude, camera.longitude)))
        .map(|camera| {
            let point = LatLon::new(camera.latitude, camera.longitude);
            json!({
                "id": camera.id,
                "lat": round_or_null(Some(point.lat), 6),
                "lon": round_or_null(Some(point.lon), 6),
                "operator": text(&camera.operator),
                "manufacturer": text(&camera.manufacturer),
                "camera_type": text(&camera.camera_type),
                "zone": text(&camera.zone),
                "direction_deg": js::opt_num(camera.direction_deg),
                "last_verified": text(&camera.last_verified),
                "distance_km": round_or_null(Some(distance_km(center, point)), 2),
            })
        })
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&a["distance_km"]) - js::to_number(&b["distance_km"])
    });
    let mut notes = Vec::new();
    if result.saturated {
        notes.push("partial: some cameras were not loaded; use a smaller area");
    }
    if result.stale {
        notes.push("data may be stale");
    }
    let summary = format!(
        "{} mapped in {}{}",
        count_noun(rows.len(), "license plate reader camera", None),
        area.label,
        if notes.is_empty() {
            ".".to_string()
        } else {
            format!(" ({}).", notes.join("; "))
        }
    );
    let view = suggest(services, Frame::Area(&area), &["alpr-cameras"], None)?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("complete".into(), json!(!result.saturated));
    data.insert("stale".into(), json!(result.stale));
    data.insert("source".into(), json!(alpr::LABEL));
    data.insert("attribution".into(), alpr::attribution());
    Ok(ToolResult::new(summary, Value::Object(data)))
}
