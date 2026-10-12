//! `src/tools/queries/brief.js`: composite queries that combine other
//! tools' answers for one area (the HUD caption is not on MCP).

use serde_json::{Map, Value, json};

use super::environment::point_json;
use super::util::suggest;
use crate::mcp::area::{Area, LatLon, area_center, resolve_area, resolve_point};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::view::Frame;

const SECTION_LIMIT: u32 = 5;
const AWARENESS_LIMIT: u32 = 10;
const AWARENESS_RADIUS_KM: f64 = 250.0;

/// A section: its key, the layer it shows, its tool and arguments.
struct Section {
    key: &'static str,
    layer: Option<&'static str>,
    tool: &'static str,
    args: fn(&Value, LatLon) -> Value,
}

fn with_limit(argument: &Value, limit: u32) -> Value {
    json!({ "area": argument, "limit": limit })
}

const SECTIONS: [Section; 6] = [
    Section {
        key: "weather",
        layer: None,
        tool: "get_weather",
        args: |argument, center| match argument.get("place") {
            Some(place) if js::truthy(place) => json!({ "location": { "place": place } }),
            _ => json!({ "location": { "lat": js::num(center.lat), "lon": js::num(center.lon) } }),
        },
    },
    Section {
        key: "earthquakes",
        layer: Some("earthquakes"),
        tool: "get_earthquakes",
        args: |argument, _| with_limit(argument, SECTION_LIMIT),
    },
    Section {
        key: "fires",
        layer: Some("local-firms"),
        tool: "get_active_fires",
        args: |argument, _| with_limit(argument, SECTION_LIMIT),
    },
    Section {
        key: "aircraft",
        layer: Some("flights"),
        tool: "aircraft_in_area",
        args: |argument, _| with_limit(argument, SECTION_LIMIT),
    },
    Section {
        key: "vessels",
        layer: Some("ais-live-vessels"),
        tool: "vessels_in_area",
        args: |argument, _| with_limit(argument, SECTION_LIMIT),
    },
    Section {
        key: "cyclones",
        layer: Some("weather-cyclones"),
        tool: "get_cyclones",
        args: |argument, _| json!({ "area": argument }),
    },
];

const AWARENESS_SECTIONS: [Section; 4] = [
    Section {
        key: "military_aircraft",
        layer: None,
        tool: "aircraft_in_area",
        args: |argument, _| json!({ "area": argument, "military": true, "limit": AWARENESS_LIMIT }),
    },
    Section {
        key: "aircraft",
        layer: None,
        tool: "aircraft_in_area",
        args: |argument, _| with_limit(argument, AWARENESS_LIMIT),
    },
    Section {
        key: "vessels",
        layer: None,
        tool: "vessels_in_area",
        args: |argument, _| with_limit(argument, AWARENESS_LIMIT),
    },
    Section {
        key: "installations",
        layer: None,
        tool: "find_military_installations",
        args: |argument, _| with_limit(argument, AWARENESS_LIMIT),
    },
];

/// `sectionArea(resolved, original)`: the argument sections receive.
fn section_argument(resolved: &Area, original: Option<&Value>) -> Value {
    if let Some(place) = original
        .and_then(|o| o.get("place"))
        .filter(|p| js::truthy(p))
    {
        return json!({ "place": place });
    }
    match resolved.center {
        Some(center) => json!({
            "lat": js::num(center.lat),
            "lon": js::num(center.lon),
            "radius_km": js::num(center.radius_km),
        }),
        None => json!({
            "bbox": [
                js::num(resolved.west),
                js::num(resolved.south),
                js::num(resolved.east),
                js::num(resolved.north),
            ]
        }),
    }
}

/// `runSections`: every offered section, a failure marked unavailable.
async fn run_sections(
    all: &[Section],
    area: &Area,
    argument: &Value,
    ctx: &Ctx,
) -> (Vec<&'static str>, Map<String, Value>, Vec<String>) {
    let center = area_center(area);
    let sections: Vec<&Section> = all.iter().filter(|s| ctx.tools.has(s.tool)).collect();
    let results = futures_util::future::join_all(sections.iter().map(|section| {
        ctx.tools
            .call(section.tool, (section.args)(argument, center))
    }))
    .await;
    let mut answers = Map::new();
    let mut lines = Vec::new();
    let mut ran = Vec::new();
    for (section, result) in sections.iter().zip(results) {
        ran.push(section.key);
        match result {
            Ok(result) => {
                answers.insert(
                    section.key.into(),
                    json!({ "summary": result.summary, "data": result.data }),
                );
                lines.push(result.summary);
            }
            Err(error) => {
                let reason = match &error {
                    Failure::Tool(known) => known.message.clone(),
                    _ => "unavailable right now".into(),
                };
                answers.insert(
                    section.key.into(),
                    json!({ "unavailable": true, "reason": reason }),
                );
            }
        }
    }
    (ran, answers, lines)
}

pub async fn situation_brief(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let argument = section_argument(&area, args.get("area"));
    let (ran, sections, lines) = run_sections(&SECTIONS, &area, &argument, &ctx).await;
    let layers: Vec<&str> = SECTIONS
        .iter()
        .filter(|s| ran.contains(&s.key))
        .filter_map(|s| s.layer)
        .collect();
    let view = suggest(services, Frame::Area(&area), &layers, None)?;
    Ok(ToolResult::new(
        format!("Situation in {}: {}", area.label, lines.join(" ")),
        json!({
            "view": view,
            "area": area.label,
            "sections": sections,
        }),
    ))
}

pub async fn military_awareness(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let point = resolve_point(&args["location"], Some(services)).await?;
    let radius = match args.get("radius_km") {
        None | Some(Value::Null) => js::num(AWARENESS_RADIUS_KM),
        Some(value) => value.clone(),
    };
    let area = resolve_area(
        &json!({ "lat": js::num(point.lat), "lon": js::num(point.lon), "radius_km": radius.clone() }),
        Some(services),
    )
    .await?;
    let argument = section_argument(&area, None);
    let (_, sections, lines) = run_sections(&AWARENESS_SECTIONS, &area, &argument, &ctx).await;
    let view = suggest(
        services,
        Frame::Area(&area),
        &["military", "ais-live-vessels", "military-installations"],
        None,
    )?;
    Ok(ToolResult::new(
        format!(
            "Military awareness within {} km of {}: {}",
            js::to_js_string(&radius),
            point.label,
            lines.join(" ")
        ),
        json!({
            "view": view,
            "location": point_json(&point),
            "radius_km": radius,
            "sections": sections,
        }),
    ))
}
