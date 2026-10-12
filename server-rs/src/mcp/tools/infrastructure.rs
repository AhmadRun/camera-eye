//! `src/tools/queries/infrastructure.js`: bundled datacenters and dams.

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, round_or_null, spread, suggest};
use crate::mcp::area::{LatLon, area_center, area_contains, distance_km, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::view::Frame;

fn text(value: &Option<String>) -> Value {
    value.clone().map_or(Value::Null, Value::String)
}

pub async fn find_infrastructure(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let center = area_center(&area);
    let datacenters = args["kind"] == "datacenters";
    let (layer_id, noun, attribution) = if datacenters {
        (
            "local-datacenters",
            "datacenter",
            "© OpenStreetMap contributors (ODbL 1.0)",
        )
    } else {
        (
            "local-dams",
            "dam",
            "© OpenStreetMap contributors (ODbL 1.0), via Open Infrastructure Map",
        )
    };
    let records = services
        .infrastructure
        .records(&services.api, layer_id)
        .await?;
    let mut rows: Vec<Value> = records
        .iter()
        .filter(|record| area_contains(&area, LatLon::new(record.lat, record.lon)))
        .map(|record| {
            let mut row = Map::new();
            row.insert("name".into(), text(&record.name));
            row.insert("operator".into(), text(&record.operator));
            if datacenters {
                row.insert("capacity".into(), text(&record.capacity));
            } else {
                row.insert("river".into(), text(&record.river));
                row.insert("output".into(), text(&record.output));
            }
            row.insert("lat".into(), round_or_null(Some(record.lat), 5));
            row.insert("lon".into(), round_or_null(Some(record.lon), 5));
            let distance = distance_km(center, LatLon::new(record.lat, record.lon));
            row.insert("distance_km".into(), round_or_null(Some(distance), 1));
            Value::Object(row)
        })
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&a["distance_km"]) - js::to_number(&b["distance_km"])
    });
    let summary = format!(
        "{} in {}.",
        count_noun(rows.len(), &format!("mapped {noun}"), None),
        area.label
    );
    let view = suggest(services, Frame::Area(&area), &[layer_id], None)?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("attribution".into(), json!(attribution));
    Ok(ToolResult::new(summary, Value::Object(data)))
}
