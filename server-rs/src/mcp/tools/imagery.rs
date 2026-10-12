//! `src/tools/queries/imagery.js`: the most recent satellite image of an
//! area.

use base64::Engine;
use serde_json::{Value, json};

use super::util::suggest;
use crate::mcp::area::{area_radius_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, Image, ToolResult};
use crate::mcp::js;
use crate::mcp::services::imagery;
use crate::mcp::services::vector_tiles::BBox;
use crate::mcp::view::Frame;

const MAX_SIDE_KM: f64 = 1000.0;
const MAX_LATITUDE: f64 = 85.0;
const IMAGE_WIDTH: f64 = 1024.0;
const MAX_IMAGE_BYTES: usize = 6 * 1024 * 1024;

fn sensor_of(product: &str) -> String {
    match product {
        "S30" => "Sentinel-2 (30 m)".into(),
        "L30" => "Landsat 8/9 (30 m)".into(),
        "VIIRS" => "VIIRS daily overview (250 m)".into(),
        other => other.into(),
    }
}

pub async fn get_recent_imagery(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    if area.west > area.east {
        return Err(Failure::tool(
            "invalid_arguments",
            "The area must not cross the antimeridian",
        ));
    }
    if area.south.abs() > MAX_LATITUDE || area.north.abs() > MAX_LATITUDE {
        return Err(Failure::tool(
            "invalid_arguments",
            "Imagery stops at ±85° latitude",
        ));
    }
    if area_radius_km(&area) * 2.0 > MAX_SIDE_KM * std::f64::consts::SQRT_2 {
        return Err(Failure::tool(
            "invalid_arguments",
            "The area is too large for imagery; use one up to 1000 km across",
        ));
    }
    let b = BBox {
        west: area.west,
        south: area.south,
        east: area.east,
        north: area.north,
    };
    let latest = match imagery::latest(&services.api, &b).await {
        Ok(latest) => latest,
        Err(Failure::Type(message)) => return Err(Failure::tool("invalid_arguments", message)),
        Err(error) => return Err(error),
    };
    let (Some(candidate), Some(reason)) = (latest.candidate, latest.reason) else {
        return Ok(ToolResult::new(
            format!("No recent satellite image covers {}.", area.label),
            json!({ "area": area.label, "image": null }),
        ));
    };
    let height = js::math_max([
        64.0,
        js::math_min([
            1024.0,
            js::math_round(IMAGE_WIDTH * (b.north - b.south) / (b.east - b.west)),
        ]),
    ]);
    let (kind, bytes) = imagery::snapshot(
        &services.api,
        &candidate.product,
        &candidate.day,
        &b,
        IMAGE_WIDTH,
        height,
    )
    .await?;
    if kind != "image/png" && kind != "image/jpeg" {
        return Err(Failure::tool(
            "unavailable",
            "The imagery service returned no image",
        ));
    }
    if bytes.len() > MAX_IMAGE_BYTES {
        return Err(Failure::tool(
            "unavailable",
            "The image is too large to return",
        ));
    }
    let sensor = sensor_of(&candidate.product);
    let cloud = match candidate.cloud {
        Some((min, max)) => format!(
            "{}–{}% cloud",
            js::fmt_num(js::math_round(min)),
            js::fmt_num(js::math_round(max))
        ),
        None => "cloud cover unknown".into(),
    };
    let quality = match reason {
        "clear" => "clear",
        "cloudy" => "cloudy",
        "partial" => "partly covering the area",
        _ => "coarse overview",
    };
    let tail = if reason == "overview" {
        String::new()
    } else {
        format!(", {cloud}")
    };
    let summary = format!(
        "Most recent image of {}: {sensor}, {}, {quality}{tail}.",
        area.label, candidate.day
    );
    let view = suggest(services, Frame::Area(&area), &["recent-imagery"], None)?;
    let cloud_pct = candidate.cloud.map_or(
        Value::Null,
        |(min, max)| json!({ "min": js::num(min), "max": js::num(max) }),
    );
    let data = json!({
        "view": view,
        "area": area.label,
        "image": {
            "product": candidate.product,
            "sensor": sensor,
            "day": candidate.day,
            "quality": reason,
            "cloud_pct": cloud_pct,
            "coverage": candidate.coverage,
        },
        "search_errors": latest.errors,
    });
    let mut result = ToolResult::new(summary, data);
    result.images = Some(vec![Image {
        mime_type: kind,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes),
    }]);
    Ok(result)
}
