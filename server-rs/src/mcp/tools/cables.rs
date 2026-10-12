//! `src/tools/queries/cables.js`: submarine cables and their landing
//! points, from the bundled TeleGeography data
//! (`src/layers/submarineCables/bundledSource.js`).

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, spread, suggest};
use crate::mcp::area::{LatLon, area_contains, coord_pairs, line_touches_area, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::fetch::Api;
use crate::mcp::view::Frame;

const ATTRIBUTION: &str = "© TeleGeography — submarinecablemap.com (CC BY-NC-SA 3.0)";
const LAYER: &str = "telegeography-submarine-cables";

async fn read(api: &Api, relative: &str) -> Result<Value, Failure> {
    let url = api.bundled_url(relative);
    let response = api.get(&url).await?;
    if !response.ok() {
        return Err(Failure::other(format!(
            "HTTP {} for {url}",
            response.status
        )));
    }
    response.json().await
}

fn cable_lines(feature: &Value) -> Vec<&Value> {
    let geometry = &feature["geometry"];
    match geometry["type"].as_str() {
        Some("LineString") => vec![&geometry["coordinates"]],
        Some("MultiLineString") => geometry["coordinates"]
            .as_array()
            .map(|l| l.iter().collect())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn features(collection: &Value) -> Vec<&Value> {
    collection
        .get("features")
        .and_then(Value::as_array)
        .map(|f| f.iter().collect())
        .unwrap_or_default()
}

pub async fn find_submarine_cables(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area_arg = args.get("area").filter(|a| js::truthy(a));
    let name_arg = args.get("name").filter(|n| js::truthy(n));
    if area_arg.is_none() && name_arg.is_none() {
        return Err(Failure::tool(
            "invalid_arguments",
            "Give an area, a name or both",
        ));
    }
    let area = match area_arg {
        Some(area) => Some(resolve_area(area, Some(services)).await?),
        None => None,
    };
    let wanted = args
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_lowercase);
    let (cables, landing_points) = futures_util::try_join!(
        read(
            &services.api,
            "telegeography_submarine_cables/cable-geo.json"
        ),
        read(
            &services.api,
            "telegeography_submarine_cables/landing-point-geo.json"
        ),
    )?;
    let landing_rows: Vec<Value> = features(&landing_points)
        .into_iter()
        .filter_map(|feature| {
            let props = feature.get("properties");
            let name = props
                .and_then(|p| p.get("name"))
                .cloned()
                .unwrap_or(Value::Null);
            let coordinates = feature.get("geometry").and_then(|g| g.get("coordinates"));
            let lon = coordinates.and_then(|c| c.get(0)).cloned();
            let lat = coordinates.and_then(|c| c.get(1)).cloned();
            let lat_num = lat.as_ref().and_then(js::finite)?;
            if !js::truthy(&name) {
                return None;
            }
            if let Some(area) = &area {
                let lon_num = lon.as_ref().and_then(Value::as_f64).unwrap_or(f64::NAN);
                if !area_contains(area, LatLon::new(lat_num, lon_num)) {
                    return None;
                }
            }
            let mut row = Map::new();
            row.insert(
                "id".into(),
                props
                    .and_then(|p| p.get("id"))
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            row.insert("name".into(), name);
            if let Some(lon) = lon {
                row.insert("lon".into(), lon);
            }
            row.insert("lat".into(), lat.unwrap_or(Value::Null));
            Some(Value::Object(row))
        })
        .collect();
    let mut rows: Vec<Value> = features(&cables)
        .into_iter()
        .filter(|feature| {
            let raw = feature.get("properties").and_then(|p| p.get("name"));
            let name = match raw.filter(|v| js::truthy(v)) {
                Some(value) => js::to_js_string(value),
                None => String::new(),
            };
            if name.is_empty()
                || wanted
                    .as_ref()
                    .is_some_and(|w| !name.to_lowercase().contains(w.as_str()))
            {
                return false;
            }
            area.as_ref().is_none_or(|area| {
                cable_lines(feature)
                    .into_iter()
                    .any(|line| line_touches_area(&coord_pairs(line), area))
            })
        })
        .map(|feature| {
            let props = &feature["properties"];
            json!({
                "id": props.get("id").cloned().unwrap_or(Value::Null),
                "name": props["name"],
            })
        })
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::locale_compare(&js::to_js_string(&a["name"]), &js::to_js_string(&b["name"]))
    });
    let where_ = area
        .as_ref()
        .map(|a| format!(" in {}", a.label))
        .unwrap_or_default();
    let what = match &wanted {
        Some(_) => format!(" matching \"{}\"", js::to_js_string(&args["name"])),
        None => String::new(),
    };
    let tail = if area.is_some() {
        format!(
            ", with {}.",
            count_noun(landing_rows.len(), "landing point", None)
        )
    } else {
        ".".into()
    };
    let summary = format!(
        "{}{what}{where_}{tail}",
        count_noun(rows.len(), "submarine cable", None)
    );
    let view = match &area {
        Some(area) => suggest(services, Frame::Area(area), &[LAYER], None)?,
        None => suggest(
            services,
            Frame::Point {
                lat: 20.0,
                lon: 0.0,
                altitude_m: 15_000_000.0,
            },
            &[LAYER],
            None,
        )?,
    };
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert(
        "landing_points".into(),
        if area.is_some() {
            Value::Object(cap_rows(landing_rows, args.get("limit")))
        } else {
            Value::Null
        },
    );
    data.insert("attribution".into(), json!(ATTRIBUTION));
    Ok(ToolResult::new(summary, Value::Object(data)))
}
