//! `src/tools/queries/atmosphere.js`: weather map imagery (wind is not on
//! MCP).

use base64::Engine;
use serde_json::{Value, json};

use super::util::suggest;
use crate::mcp::area::{Area, area_center, area_radius_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, Image, ToolResult};
use crate::mcp::js;
use crate::mcp::services::template;
use crate::mcp::services::vector_tiles::BBox;
use crate::mcp::services::weather_maps;
use crate::mcp::view::Frame;

const IMAGE_SIZE: (f64, f64) = (1024.0, 512.0);
const SNAP_DEGREES: f64 = 0.25;
const MIN_HALF_HEIGHT_DEGREES: f64 = 0.5;
const KM_PER_DEGREE: f64 = 111.2;
const MAX_IMAGE_BYTES: usize = 4 * 1024 * 1024;

fn product_for(map: &str) -> Option<(&'static str, &'static str)> {
    Some(match map {
        "radar" => ("radar", "weather-radar"),
        "satellite" => ("clouds-regional", "weather-satellite"),
        "global_satellite" => ("clouds", "weather-satellite"),
        "lightning" => ("lightning", "weather-lightning"),
        _ => return None,
    })
}

fn snap_down(value: f64) -> f64 {
    (value / SNAP_DEGREES).floor() * SNAP_DEGREES
}

fn snap_up(value: f64) -> f64 {
    (value / SNAP_DEGREES).ceil() * SNAP_DEGREES
}

/// `weatherWindow(area, bounds)`.
pub fn weather_window(area: &Area, bounds: &BBox) -> Option<BBox> {
    if area.west > area.east {
        return None;
    }
    let center = area_center(area);
    let half_height = js::math_max([
        MIN_HALF_HEIGHT_DEGREES,
        area_radius_km(area) / KM_PER_DEGREE,
    ]);
    let height = snap_up(half_height * 2.0);
    let width = height * 2.0;
    if width > bounds.east - bounds.west || height > bounds.north - bounds.south {
        return None;
    }
    let mut west = snap_down(center.lon - width / 2.0);
    let mut south = snap_down(center.lat - height / 2.0);
    west = js::math_min([
        js::math_max([west, snap_up(bounds.west)]),
        snap_down(bounds.east - width),
    ]);
    south = js::math_min([
        js::math_max([south, snap_up(bounds.south)]),
        snap_down(bounds.north - height),
    ]);
    let map_box = BBox {
        west,
        south,
        east: west + width,
        north: south + height,
    };
    let inside_product = map_box.west >= bounds.west
        && map_box.east <= bounds.east
        && map_box.south >= bounds.south
        && map_box.north <= bounds.north;
    let covers_area = map_box.west <= js::math_max([area.west, bounds.west])
        && map_box.east >= js::math_min([area.east, bounds.east])
        && map_box.south <= js::math_max([area.south, bounds.south])
        && map_box.north >= js::math_min([area.north, bounds.north]);
    (inside_product && covers_area).then_some(map_box)
}

fn box_json(b: &BBox) -> Value {
    json!({
        "west": js::num(b.west),
        "south": js::num(b.south),
        "east": js::num(b.east),
        "north": js::num(b.north),
    })
}

pub async fn get_weather_map(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let map = js::to_js_string(&args["map"]);
    let (product, layer) =
        product_for(&map).ok_or_else(|| Failure::other("Unknown weather product"))?;
    let snapshot = weather_maps::snapshot(&services.api, product).await?;
    if snapshot.get("unavailable").is_some_and(js::truthy) {
        return Err(Failure::tool(
            "unavailable",
            format!("The {map} map is unavailable right now"),
        ));
    }
    let bounds_value = &snapshot["bounds"];
    let edge = |key: &str| js::to_number(&bounds_value[key]);
    let bounds = BBox {
        west: edge("west"),
        south: edge("south"),
        east: edge("east"),
        north: edge("north"),
    };
    let area = match args.get("area").filter(|a| js::truthy(a)) {
        Some(area) => Some(resolve_area(area, Some(services)).await?),
        None => None,
    };
    let mut map_box = None;
    if let Some(area) = &area {
        let center = area_center(area);
        if center.lon < bounds.west
            || center.lon > bounds.east
            || center.lat < bounds.south
            || center.lat > bounds.north
        {
            let coverage = match snapshot.get("coverage").filter(|c| js::truthy(c)) {
                Some(coverage) => js::to_js_string(coverage),
                None => "see its bounds".into(),
            };
            return Err(Failure::tool(
                "invalid_arguments",
                format!(
                    "{} is outside the {map} map's coverage: {coverage}",
                    area.label
                ),
            ));
        }
        map_box = weather_window(area, &bounds);
    }
    let latest = snapshot["latest"].as_str().unwrap_or_default().to_string();
    let (kind, bytes) = weather_maps::image(
        &services.api,
        product,
        &latest,
        IMAGE_SIZE,
        map_box.as_ref(),
    )
    .await?;
    if kind != "image/png" && kind != "image/jpeg" {
        return Err(Failure::tool(
            "unavailable",
            "The weather map returned no image",
        ));
    }
    if bytes.len() > MAX_IMAGE_BYTES {
        return Err(Failure::tool(
            "unavailable",
            "The weather map image is too large",
        ));
    }
    let where_ = area
        .as_ref()
        .map(|a| format!(" over {}", a.label))
        .unwrap_or_default();
    let title = match snapshot.get("title").filter(|t| js::truthy(t)) {
        Some(title) => js::to_js_string(title),
        None => map.clone(),
    };
    let summary = format!("{title} at {}{where_}.", template(snapshot.get("latest")));
    let view = match &area {
        Some(area) => suggest(services, Frame::Area(area), &[layer], None)?,
        None => suggest(
            services,
            Frame::Point {
                lat: (bounds.south + bounds.north) / 2.0,
                lon: (bounds.west + bounds.east) / 2.0,
                altitude_m: 8_000_000.0,
            },
            &[layer],
            None,
        )?,
    };
    let or_null = |key: &str| {
        snapshot
            .get(key)
            .cloned()
            .filter(|v| !v.is_null())
            .unwrap_or(Value::Null)
    };
    let data = json!({
        "view": view,
        "map": map,
        "title": or_null("title"),
        "description": or_null("description"),
        "coverage": or_null("coverage"),
        "time": snapshot["latest"],
        "box": map_box.as_ref().map_or_else(|| bounds_value.clone(), box_json),
        "source": or_null("source"),
        "attribution": or_null("attribution"),
    });
    let mut result = ToolResult::new(summary, data);
    result.images = Some(vec![Image {
        mime_type: kind,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes),
    }]);
    Ok(result)
}
