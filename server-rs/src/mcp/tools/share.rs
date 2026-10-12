//! `src/tools/queries/share.js`: links that open Camera Eye at a view.

use serde_json::{Map, Value, json};
use url::Url;

use crate::mcp::area::{LatLon, resolve_area};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::view::{
    camera_for_area, camera_json, camera_looking_at, create_view_from, view_url,
};

const FOLLOW_ALTITUDE_M: f64 = 20_000.0;

fn object_of(value: Option<&Value>) -> Map<String, Value> {
    value
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

/// `followedAircraftPosition(follow, tools)`.
async fn followed_aircraft_position(follow: &Value, ctx: &Ctx) -> Option<(Value, Value)> {
    let kind = follow.get("kind").and_then(Value::as_str)?;
    if !["aircraft", "military_aircraft"].contains(&kind) || !ctx.tools.has("find_aircraft") {
        return None;
    }
    let id = follow.get("id").cloned().unwrap_or(Value::Null);
    let mut args = Map::new();
    if follow.get("id").is_some() {
        args.insert("icao24".into(), id);
    }
    let result = ctx
        .tools
        .call("find_aircraft", Value::Object(args))
        .await
        .ok()?;
    let row = result.data.get("rows")?.as_array()?.iter().find(|row| {
        row.get("lat").and_then(js::finite).is_some()
            && row.get("lon").and_then(js::finite).is_some()
    })?;
    Some((row["lat"].clone(), row["lon"].clone()))
}

/// `resolveViewArguments(args)`: the view and a label for it.
pub async fn resolve_view_arguments(args: &Value, ctx: &Ctx) -> Result<(Value, String), Failure> {
    let base = args.get("view").filter(|v| v.is_object());
    let base_field = |key: &str| base.and_then(|b| b.get(key));
    let area_arg = args.get("area").filter(|a| js::truthy(a));
    let mut camera = if area_arg.is_some() {
        object_of(args.get("camera"))
    } else {
        let mut camera = object_of(base_field("camera"));
        for (key, value) in object_of(args.get("camera")) {
            camera.insert(key, value);
        }
        camera
    };
    let mut label: Option<String> = None;
    if let Some(area_arg) = area_arg {
        let area = resolve_area(area_arg, Some(&ctx.services)).await?;
        label = Some(area.label.clone());
        let framed = camera_for_area(&area);
        camera = if camera.get("lat").is_none() && camera.get("lon").is_none() {
            let num = |key: &str, default: f64| match camera.get(key) {
                None | Some(Value::Null) => default,
                Some(value) => js::to_number(value),
            };
            let looking = camera_looking_at(
                LatLon::new(framed.lat, framed.lon),
                num("altitude_m", framed.altitude_m.unwrap_or(f64::NAN)),
                num("heading_deg", 0.0),
                num("pitch_deg", -90.0),
            );
            camera_json(&looking)
        } else {
            let mut merged = camera_json(&framed);
            for (key, value) in camera {
                merged.insert(key, value);
            }
            merged
        };
    }
    let follow = match args.get("follow").filter(|f| !f.is_null()) {
        Some(follow) => follow.clone(),
        None => base_field("follow")
            .filter(|f| !f.is_null())
            .cloned()
            .unwrap_or(Value::Null),
    };
    let placed = camera.get("lat").and_then(js::finite).is_some()
        && camera.get("lon").and_then(js::finite).is_some();
    if !placed {
        let Some((lat, lon)) = followed_aircraft_position(&follow, ctx).await else {
            let message = if js::truthy(&follow) {
                format!(
                    "{} is not reported now; give an area or a camera with lat and lon too",
                    follow
                        .get("id")
                        .map_or_else(|| "undefined".into(), js::to_js_string)
                )
            } else {
                "Give a view, an area, or a camera with lat and lon".into()
            };
            return Err(Failure::tool("invalid_arguments", message));
        };
        let mut placed = Map::new();
        placed.insert("altitude_m".into(), js::num(FOLLOW_ALTITUDE_M));
        for (key, value) in camera {
            placed.insert(key, value);
        }
        placed.insert("lat".into(), lat);
        placed.insert("lon".into(), lon);
        camera = placed;
        let kind = if follow.get("kind").and_then(Value::as_str) == Some("military_aircraft") {
            "military aircraft"
        } else {
            "aircraft"
        };
        let id = follow
            .get("id")
            .map_or_else(|| "undefined".into(), js::to_js_string);
        label = Some(format!("{kind} {id}"));
    }
    let pick = |key: &str| {
        args.get(key)
            .filter(|v| !v.is_null())
            .or_else(|| base_field(key).filter(|v| !v.is_null()))
    };
    let view = create_view_from(
        &camera,
        args.get("layers")
            .filter(|v| !v.is_null())
            .or_else(|| base_field("layers")),
        pick("style").unwrap_or(&Value::Null),
        pick("map").unwrap_or(&Value::Null),
        &follow,
        pick("annotations"),
    )
    .map_err(|message| Failure::tool("invalid_arguments", message))?;
    let label = label.unwrap_or_else(|| {
        let at = |key: &str| js::to_fixed(view["camera"][key].as_f64().unwrap_or(f64::NAN), 3);
        format!("{}, {}", at("lat"), at("lon"))
    });
    Ok((view, label))
}

pub async fn show_in_camera_eye(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let base = ctx
        .services
        .app_base_url
        .as_deref()
        .and_then(|b| Url::parse(b).ok())
        .ok_or_else(|| Failure::tool("unavailable", "The app's address is not configured"))?;
    let (view, label) = resolve_view_arguments(&args, &ctx).await?;
    let url = view_url(&base, &view);
    Ok(ToolResult::new(
        format!("Open {label} in Camera Eye: {url}"),
        json!({ "url": url, "view": view }),
    ))
}
