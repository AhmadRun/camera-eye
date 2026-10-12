//! The `run` functions of Core's tools (`src/tools/queries/*`), by name.
//! Definitions (titles, descriptions, schemas) come from the exported data.

pub mod atmosphere;
pub mod aviation;
pub mod brief;
pub mod cables;
pub mod environment;
pub mod events;
pub mod hazards;
pub mod imagery;
pub mod infrastructure;
pub mod maritime;
pub mod media;
pub mod mobility;
pub mod panel_request;
pub mod share;
pub mod space;
pub mod surveillance;
pub mod util;

use std::sync::Arc;

use super::catalog::{Ctx, Failure, Run, ToolFuture};
use serde_json::Value;

fn run<F, Fut>(f: F) -> Run
where
    F: Fn(Value, Ctx) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<super::catalog::ToolResult, Failure>> + Send + 'static,
{
    Arc::new(move |args, ctx| -> ToolFuture { Box::pin(f(args, ctx)) })
}

/// Tools no MCP surface offers and no tool combines; they are defined for
/// the catalog's completeness but not ported.
pub const NOT_PORTED: [&str; 9] = [
    "get_hud_caption",
    "search_places",
    "places_nearby",
    "plan_route",
    "get_wind",
    "get_regional_brief",
    "find_radio_stations",
    "get_bike_share",
    "get_transit_vehicles",
];

/// The `run` function for a tool.
pub fn run_for(name: &str) -> Option<Run> {
    Some(match name {
        "get_earthquakes" => run(hazards::get_earthquakes),
        "get_active_fires" => run(hazards::get_active_fires),
        "get_recent_launches" => run(space::get_recent_launches),
        "next_satellite_pass" => run(space::next_satellite_pass),
        "satellites_overhead" => run(space::satellites_overhead),
        "aircraft_in_area" => run(aviation::aircraft_in_area),
        "find_aircraft" => run(aviation::find_aircraft),
        "get_aircraft_track" => run(aviation::get_aircraft_track),
        "get_aircraft_info" => run(aviation::get_aircraft_info),
        "vessels_in_area" => run(maritime::vessels_in_area),
        "find_vessel" => run(maritime::find_vessel),
        "get_vessel_track" => run(maritime::get_vessel_track),
        "find_cctv_cameras" => run(media::find_cctv_cameras),
        "get_cctv_snapshot" => run(media::get_cctv_snapshot),
        "find_alpr_cameras" => run(surveillance::find_alpr_cameras),
        "get_traffic_flow" => run(mobility::get_traffic_flow),
        "get_weather_map" => run(atmosphere::get_weather_map),
        "get_recent_imagery" => run(imagery::get_recent_imagery),
        "find_submarine_cables" => run(cables::find_submarine_cables),
        "find_infrastructure" => run(infrastructure::find_infrastructure),
        "get_bhote_koshi_flood" => run(events::get_bhote_koshi_flood),
        "get_weather" => run(environment::get_weather),
        "get_cyclones" => run(environment::get_cyclones),
        "get_fire_perimeters" => run(environment::get_fire_perimeters),
        "get_terrain_height" => run(environment::get_terrain_height),
        "find_military_installations" => run(environment::find_military_installations),
        "get_map_features" => run(environment::get_map_features),
        "situation_brief" => run(brief::situation_brief),
        "military_awareness" => run(brief::military_awareness),
        "show_in_camera_eye" => run(share::show_in_camera_eye),
        "panel_request" => run(panel_request::panel_request),
        _ if NOT_PORTED.contains(&name) => {
            let message = format!("No tool named {name} is available");
            run(move |_, _| {
                let message = message.clone();
                async move { Err(Failure::tool("unsupported", message)) }
            })
        }
        _ => return None,
    })
}
