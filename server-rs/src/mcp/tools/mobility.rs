//! `src/tools/queries/mobility.js`: live traffic flow (bike share and
//! transit are not on MCP).

use serde_json::{Value, json};

use super::util::{round_or_null, suggest};
use crate::mcp::area::{Area, LatLon, area_contains, distance_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::mvt::Coord;
use crate::mcp::services::traffic::{Flow, TrafficSource};
use crate::mcp::services::vector_tiles::BBox;
use crate::mcp::view::Frame;

const FLOW_ZOOMS: [u32; 4] = [12, 11, 10, 9];
const CONGESTED_LEVEL: f64 = 0.5;

/// Length in kilometers of a `[lon, lat]` line.
fn line_km(coords: &[Coord]) -> f64 {
    let mut total = 0.0;
    for index in 1..coords.len() {
        total += distance_km(
            LatLon::new(coords[index - 1][1], coords[index - 1][0]),
            LatLon::new(coords[index][1], coords[index][0]),
        );
    }
    total
}

/// `insideParts(coords, area)`: the parts of a line inside a radius area.
pub fn inside_parts(coords: &[Coord], area: &Area) -> Vec<Vec<Coord>> {
    if area.center.is_none() {
        return vec![coords.to_vec()];
    }
    let inside = |p: &Coord| area_contains(area, LatLon::new(p[1], p[0]));
    let edge = |from: Coord, to: Coord| -> Coord {
        let (mut a, mut b) = (from, to);
        let from_in = inside(&from);
        for _ in 0..20 {
            let middle = [(a[0] + b[0]) / 2.0, (a[1] + b[1]) / 2.0];
            if inside(&middle) == from_in {
                a = middle;
            } else {
                b = middle;
            }
        }
        if from_in { a } else { b }
    };
    let mut parts: Vec<Vec<Coord>> = Vec::new();
    let mut current: Option<Vec<Coord>> = inside(&coords[0]).then(|| vec![coords[0]]);
    for index in 1..coords.len() {
        let from = coords[index - 1];
        let to = coords[index];
        let (from_in, to_in) = (inside(&from), inside(&to));
        if from_in && to_in {
            if let Some(current) = current.as_mut() {
                current.push(to);
            }
        } else if from_in {
            let mut done = current.take().unwrap_or_default();
            done.push(edge(from, to));
            parts.push(done);
        } else if to_in {
            current = Some(vec![edge(from, to), to]);
        } else {
            let middle = (0..15)
                .map(|step| {
                    let t = f64::from(step + 1) / 16.0;
                    [
                        from[0] + (to[0] - from[0]) * t,
                        from[1] + (to[1] - from[1]) * t,
                    ]
                })
                .find(|p| inside(p));
            if let Some(middle) = middle {
                parts.push(vec![edge(middle, from), edge(middle, to)]);
            }
        }
    }
    if let Some(current) = current {
        parts.push(current);
    }
    parts.into_iter().filter(|part| part.len() > 1).collect()
}

async fn read_flow(traffic: &TrafficSource, area: &Area) -> Result<Flow, Failure> {
    let bbox = BBox {
        west: area.west,
        south: area.south,
        east: area.east,
        north: area.north,
    };
    for zoom in FLOW_ZOOMS {
        match traffic.flow_detail(&bbox, zoom).await {
            Ok(flow) => return Ok(flow),
            Err(error) if error.code() == Some("TILE_VIEW_TOO_WIDE") => continue,
            Err(error) => return Err(error),
        }
    }
    Err(Failure::tool(
        "invalid_arguments",
        "The area is too large for traffic detail; use a city-sized area",
    ))
}

#[derive(Default)]
struct Category {
    km: f64,
    weighted: f64,
    congested_km: f64,
}

fn round(value: f64, digits: usize) -> Value {
    round_or_null(Some(value), digits)
}

pub async fn get_traffic_flow(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    if area.west > area.east {
        return Err(Failure::tool(
            "invalid_arguments",
            "The area must not cross the antimeridian",
        ));
    }
    if !services.traffic.has_key().await? {
        return Err(Failure::tool(
            "unavailable",
            "Live traffic needs a TomTom key configured for this server",
        ));
    }
    let flow = read_flow(&services.traffic, &area).await?;
    let segments: Vec<_> = flow
        .segments
        .iter()
        .filter(|segment| segment.coords.len() > 1)
        .flat_map(|segment| {
            inside_parts(&segment.coords, &area)
                .into_iter()
                .map(move |coords| (segment, coords))
        })
        .collect();
    let mut categories: Vec<(Value, Category)> = Vec::new();
    let (mut measured_km, mut weighted, mut congested_km, mut closed_km) = (0.0, 0.0, 0.0, 0.0);
    for (segment, coords) in &segments {
        let km = line_km(coords);
        let key = if js::truthy(&segment.road_category) {
            segment.road_category.clone()
        } else {
            json!("other")
        };
        let index = match categories.iter().position(|(k, _)| *k == key) {
            Some(index) => index,
            None => {
                categories.push((key, Category::default()));
                categories.len() - 1
            }
        };
        let entry = &mut categories[index].1;
        entry.km += km;
        if segment.closure {
            closed_km += km;
        }
        let level = segment.traffic_level;
        if level.is_finite() {
            measured_km += km;
            weighted += level * km;
            entry.weighted += level * km;
            if level < CONGESTED_LEVEL {
                congested_km += km;
                entry.congested_km += km;
            }
        }
    }
    let percent = |part: f64, whole: f64| {
        if whole > 0.0 {
            js::num(js::math_round(part / whole * 100.0))
        } else {
            Value::Null
        }
    };
    let speed_pct = percent(weighted, measured_km);
    let mut by_category: Vec<Value> = categories
        .iter()
        .map(|(category, entry)| {
            json!({
                "category": category,
                "road_km": round(entry.km, 1),
                "speed_pct_of_free_flow": percent(entry.weighted, entry.km),
                "congested_km": round(entry.congested_km, 1),
            })
        })
        .collect();
    js::sort_by(&mut by_category, |a, b| {
        js::to_number(&b["road_km"]) - js::to_number(&a["road_km"])
    });
    let text = |v: Value| js::to_js_string(&v);
    let mut summary = if segments.is_empty() {
        format!("No live traffic is reported in {}.", area.label)
    } else {
        let speed = if speed_pct.is_null() {
            "unknown".to_string()
        } else {
            text(speed_pct.clone())
        };
        let closed = if closed_km > 0.0 {
            format!("; {} km closed.", text(round(closed_km, 1)))
        } else {
            ".".to_string()
        };
        format!(
            "Traffic in {}: {speed}% of free-flow speed on average; {} km congested of {} km measured{closed}",
            area.label,
            text(round(congested_km, 1)),
            text(round(measured_km, 1)),
        )
    };
    if flow.partial {
        summary.push_str(" Some map tiles did not load, so these figures are partial.");
    }
    let view = suggest(services, Frame::Area(&area), &["traffic"], None)?;
    Ok(ToolResult::new(
        summary,
        json!({
            "view": view,
            "area": area.label,
            "partial": flow.partial,
            "speed_pct_of_free_flow": speed_pct,
            "measured_km": round(measured_km, 1),
            "congested_km": round(congested_km, 1),
            "closed_km": round(closed_km, 1),
            "congested_below_pct": js::num(CONGESTED_LEVEL * 100.0),
            "by_category": by_category,
        }),
    ))
}
