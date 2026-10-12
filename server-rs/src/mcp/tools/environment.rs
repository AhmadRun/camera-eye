//! `src/tools/queries/environment.js`: weather, cyclones, fire perimeters,
//! terrain height, mapped military sites and map features (the regional
//! brief is not on MCP).

use serde_json::{Map, Value, json};

use super::util::{cap_rows, count_noun, round_or_null, spread, suggest};
use crate::mcp::area::{
    LatLon, NamedPoint, area_center, area_contains, coord_pairs, distance_km, polygons_touch_area,
    resolve_area, resolve_point,
};
use crate::mcp::catalog::{Ctx, Failure, ToolResult};
use crate::mcp::js;
use crate::mcp::services::environment as env;
use crate::mcp::services::features::{Answer, Kind};
use crate::mcp::services::vector_tiles::BBox;
use crate::mcp::view::Frame;

const MAX_TERRAIN_POINTS: usize = 20;
const MAX_SITE_BOX_DEGREES: f64 = 10.0;

/// The location a point argument resolved to.
pub fn point_json(point: &NamedPoint) -> Value {
    json!({"label": point.label, "lat": js::num(point.lat), "lon": js::num(point.lon)})
}

/// WMO weather interpretation codes.
fn weather_code_text(code: &str) -> Option<&'static str> {
    Some(match code {
        "0" => "clear sky",
        "1" => "mainly clear",
        "2" => "partly cloudy",
        "3" => "overcast",
        "45" => "fog",
        "48" => "depositing rime fog",
        "51" => "light drizzle",
        "53" => "drizzle",
        "55" => "dense drizzle",
        "56" => "light freezing drizzle",
        "57" => "freezing drizzle",
        "61" => "light rain",
        "63" => "rain",
        "65" => "heavy rain",
        "66" => "light freezing rain",
        "67" => "freezing rain",
        "71" => "light snow",
        "73" => "snow",
        "75" => "heavy snow",
        "77" => "snow grains",
        "80" => "light rain showers",
        "81" => "rain showers",
        "82" => "violent rain showers",
        "85" => "light snow showers",
        "86" => "snow showers",
        "95" => "thunderstorm",
        "96" => "thunderstorm with light hail",
        "99" => "thunderstorm with heavy hail",
        _ => return None,
    })
}

fn or_null(value: Option<&Value>) -> Value {
    value
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or(Value::Null)
}

/// `weatherRow(weather)`.
pub fn weather_row(weather: Option<&Value>) -> Value {
    let Some(weather) = weather.filter(|w| js::truthy(w)) else {
        return Value::Null;
    };
    let field = |key: &str| or_null(weather.get(key));
    let conditions = match weather.get("weatherCode") {
        None => Value::Null,
        Some(code) => weather_code_text(&js::to_js_string(code)).map_or(Value::Null, |t| json!(t)),
    };
    json!({
        "observed_at": field("observedAt"),
        "conditions": conditions,
        "weather_code": field("weatherCode"),
        "temperature_c": field("temperatureC"),
        "feels_like_c": field("apparentTemperatureC"),
        "precipitation_mm": field("precipitationMm"),
        "cloud_cover_pct": field("cloudCoverPct"),
        "wind_kph": field("windKph"),
        "wind_direction_deg": field("windDirectionDeg"),
        "visibility_m": field("visibilityM"),
    })
}

/// `describeWeather(row)`.
pub fn describe_weather(row: &Value) -> String {
    if row.is_null() {
        return "no current weather".into();
    }
    let mut parts = Vec::new();
    if js::truthy(&row["conditions"]) {
        parts.push(js::to_js_string(&row["conditions"]));
    }
    if let Some(t) = js::finite(&row["temperature_c"]) {
        parts.push(format!("{}°C", js::fmt_num(js::math_round(t))));
    }
    if let Some(w) = js::finite(&row["wind_kph"]) {
        parts.push(format!("wind {} km/h", js::fmt_num(js::math_round(w))));
    }
    if parts.is_empty() {
        "conditions unknown".into()
    } else {
        parts.join(", ")
    }
}

pub async fn get_weather(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let point = resolve_point(&args["location"], Some(services)).await?;
    let payload = env::weather_conditions(&services.api, point.lat, point.lon).await?;
    let payload = Some(&payload).filter(|p| !p.is_null());
    let row = weather_row(payload.and_then(|p| p.get("weather")));
    let stale = payload
        .and_then(|p| p.get("status"))
        .and_then(Value::as_str)
        == Some("stale");
    let summary = format!(
        "Weather at {}: {}{}",
        point.label,
        describe_weather(&row),
        if stale { " (data may be stale)." } else { "." }
    );
    Ok(ToolResult::new(
        summary,
        json!({
            "location": point_json(&point),
            "weather": row,
            "stale": stale,
            "retrieved_at": or_null(payload.and_then(|p| p.get("retrievedAt"))),
        }),
    ))
}

pub async fn get_cyclones(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = match args.get("area").filter(|a| js::truthy(a)) {
        Some(area) => Some(resolve_area(area, Some(services)).await?),
        None => None,
    };
    let snapshot = env::cyclone_snapshot(&services.api).await?;
    if snapshot.unavailable {
        return Err(Failure::tool(
            "unavailable",
            "Cyclone advisories are unavailable right now",
        ));
    }
    let opt = |v: Option<f64>| js::opt_num(v);
    let mut storms: Vec<Value> = snapshot
        .storms
        .iter()
        .filter(|storm| {
            area.as_ref().is_none_or(|area| {
                area_contains(
                    area,
                    LatLon::new(
                        storm.latitude.unwrap_or(f64::NAN),
                        storm.longitude.unwrap_or(f64::NAN),
                    ),
                )
            })
        })
        .map(|storm| {
            json!({
                "id": storm.id,
                "name": storm.name,
                "classification": storm.classification,
                "basin": storm.basin,
                "lat": opt(storm.latitude),
                "lon": opt(storm.longitude),
                "position_at": storm.position_at,
                "wind_kt": opt(storm.wind_kt),
                "pressure_hpa": opt(storm.pressure_hpa),
                "movement": {
                    "directionDegrees": opt(storm.direction_degrees),
                    "speedKt": opt(storm.speed_kt),
                },
                "advisory": storm.advisory_number,
                "issued_at": storm.issued_at,
                "advisory_url": storm.advisory_url,
            })
        })
        .collect();
    let wind = |s: &Value| s["wind_kt"].as_f64().unwrap_or(0.0);
    js::sort_by(&mut storms, |a, b| wind(b) - wind(a));
    let where_ = area
        .as_ref()
        .map(|a| format!(" in {}", a.label))
        .unwrap_or_default();
    let strongest = match storms.first() {
        Some(storm) => format!(
            "; strongest {} at {} kt.",
            js::to_js_string(&storm["name"]),
            js::to_js_string(&storm["wind_kt"])
        ),
        None => ".".into(),
    };
    let summary = format!(
        "{}{where_}{strongest}",
        count_noun(storms.len(), "active tropical cyclone", None)
    );
    let view = match &area {
        Some(area) => suggest(services, Frame::Area(area), &["weather-cyclones"], None)?,
        None => suggest(
            services,
            Frame::Point {
                lat: 20.0,
                lon: 0.0,
                altitude_m: 15_000_000.0,
            },
            &["weather-cyclones"],
            None,
        )?,
    };
    Ok(ToolResult::new(
        summary,
        json!({
            "view": view,
            "storms": storms,
            "coverage": snapshot.coverage,
            "source": snapshot.source,
            "attribution": snapshot.attribution,
            "stale": snapshot.stale,
        }),
    ))
}

/// Polygons of `[lon, lat]` rings from a perimeter's JSON.
fn polygons_of(value: Option<&Value>) -> Vec<Vec<Vec<(f64, f64)>>> {
    value
        .and_then(Value::as_array)
        .map(|polygons| {
            polygons
                .iter()
                .map(|rings| {
                    rings
                        .as_array()
                        .map(|rings| rings.iter().map(coord_pairs).collect())
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `polygonCenter(polygons)`: the average outer-ring vertex.
fn polygon_center(polygons: Option<&Value>) -> Option<LatLon> {
    let (mut lat, mut lon, mut count) = (0.0, 0.0, 0usize);
    for polygon in polygons.and_then(Value::as_array).into_iter().flatten() {
        for point in polygon
            .get(0)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let (Some(x), Some(y)) = (
                point.get(0).and_then(js::finite),
                point.get(1).and_then(js::finite),
            ) else {
                continue;
            };
            lon += x;
            lat += y;
            count += 1;
        }
    }
    (count > 0).then(|| LatLon::new(lat / count as f64, lon / count as f64))
}

pub async fn get_fire_perimeters(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    let fires = env::fire_perimeters(&services.api).await?;
    let mut rows: Vec<Value> = fires
        .iter()
        .filter(|fire| polygons_touch_area(&polygons_of(fire.get("polygons")), &area))
        .filter_map(|fire| Some((fire, polygon_center(fire.get("polygons"))?)))
        .map(|(fire, point)| {
            let mut row = Map::new();
            let mut put = |key: &str, value: Option<&Value>| {
                if let Some(value) = value {
                    row.insert(key.into(), value.clone());
                }
            };
            put("id", fire.get("stableId"));
            put("name", fire.get("name"));
            let acres = round_or_null(fire.get("acres").and_then(js::finite), 0);
            let mut row = row;
            row.insert("acres".into(), acres);
            for (key, source) in [
                ("contained_pct", "containedPct"),
                ("state", "state"),
                ("county", "county"),
                ("category", "category"),
                ("cause", "cause"),
                ("behavior", "behavior"),
                ("personnel", "personnel"),
            ] {
                if let Some(value) = fire.get(source) {
                    row.insert(key.into(), value.clone());
                }
            }
            row.insert(
                "discovered".into(),
                js::iso_time(fire.get("discoveredTime").and_then(js::finite)),
            );
            row.insert(
                "updated".into(),
                js::iso_time(fire.get("updatedTime").and_then(js::finite)),
            );
            row.insert("lat".into(), round_or_null(Some(point.lat), 5));
            row.insert("lon".into(), round_or_null(Some(point.lon), 5));
            Value::Object(row)
        })
        .collect();
    let acres = |row: &Value| row["acres"].as_f64().unwrap_or(0.0);
    js::sort_by(&mut rows, |a, b| acres(b) - acres(a));
    let summary = format!(
        "{} in {}.",
        count_noun(rows.len(), "mapped wildfire perimeter", None),
        area.label
    );
    let view = suggest(services, Frame::Area(&area), &["fire-perimeters"], None)?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let data = spread(data, cap_rows(rows, args.get("limit")));
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn get_terrain_height(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let points = args["points"].as_array().cloned().unwrap_or_default();
    let coords: Vec<(f64, f64)> = points
        .iter()
        .map(|p| (js::to_number(&p["lat"]), js::to_number(&p["lon"])))
        .collect();
    let results = env::terrain_heights(&services.api, &coords).await?;
    let results = match results {
        Some(Value::Array(results)) if results.len() == points.len() => results,
        _ => {
            return Err(Failure::tool(
                "malformed",
                "Terrain heights were incomplete",
            ));
        }
    };
    let rows: Vec<Value> = points
        .iter()
        .zip(&results)
        .map(|(point, result)| {
            let height = |key: &str| round_or_null(result.get(key).and_then(js::finite), 1);
            json!({
                "lat": point["lat"],
                "lon": point["lon"],
                "elevation_m": height("elevation"),
                "geoid_m": height("geoid"),
                "ellipsoid_m": height("ellipsoid"),
            })
        })
        .collect();
    let summary = if rows.len() == 1 {
        let first = &rows[0];
        let elevation = if first["elevation_m"].is_null() {
            "unknown".to_string()
        } else {
            js::to_js_string(&first["elevation_m"])
        };
        format!(
            "Ground elevation is {elevation} m at {}, {}.",
            js::to_js_string(&first["lat"]),
            js::to_js_string(&first["lon"])
        )
    } else {
        format!(
            "Ground elevation at {}.",
            count_noun(rows.len(), "point", None)
        )
    };
    debug_assert!(rows.len() <= MAX_TERRAIN_POINTS);
    Ok(ToolResult::new(summary, json!({ "points": rows })))
}

pub async fn find_military_installations(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let area = resolve_area(&args["area"], Some(services)).await?;
    if area.west > area.east
        || area.north - area.south > MAX_SITE_BOX_DEGREES
        || area.east - area.west > MAX_SITE_BOX_DEGREES
    {
        return Err(Failure::tool(
            "invalid_arguments",
            "The area must be at most 10° on each side and not cross the antimeridian",
        ));
    }
    let center = area_center(&area);
    let bbox = BBox {
        west: area.west,
        south: area.south,
        east: area.east,
        north: area.north,
    };
    let result = services.installations.mapped_sites(&bbox).await?;
    let names_pending = result.names_pending;
    let complete = !result.saturated;
    let mut rows: Vec<Value> = result
        .records
        .iter()
        .filter(|site| area_contains(&area, LatLon::new(site.latitude, site.longitude)))
        .map(|site| {
            let point = LatLon::new(site.latitude, site.longitude);
            json!({
                "id": site.id,
                "name": or_null(site.name.as_ref()),
                "kind": site.kind.clone().map_or(Value::Null, Value::String),
                "class": or_null(site.class.as_ref()),
                "lat": round_or_null(Some(point.lat), 5),
                "lon": round_or_null(Some(point.lon), 5),
                "distance_km": round_or_null(Some(distance_km(center, point)), 1),
            })
        })
        .collect();
    js::sort_by(&mut rows, |a, b| {
        js::to_number(&a["distance_km"]) - js::to_number(&b["distance_km"])
    });
    let mut notes = Vec::new();
    if !complete {
        notes.push("partial: the source returned only some sites");
    }
    if names_pending {
        notes.push("site names are still loading");
    }
    let summary = format!(
        "{} in {}{}",
        count_noun(rows.len(), "mapped military installation", None),
        area.label,
        if notes.is_empty() {
            ".".to_string()
        } else {
            format!(" ({}).", notes.join("; "))
        }
    );
    let view = suggest(
        services,
        Frame::Area(&area),
        &["military-installations"],
        None,
    )?;
    let mut data = Map::new();
    data.insert("view".into(), view);
    let mut data = spread(data, cap_rows(rows, args.get("limit")));
    data.insert("complete".into(), json!(complete));
    data.insert("names_pending".into(), json!(names_pending));
    data.insert(
        "source".into(),
        result.source.map_or(Value::Null, Value::String),
    );
    Ok(ToolResult::new(summary, Value::Object(data)))
}

pub async fn get_map_features(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    let point = resolve_point(&args["location"], Some(services)).await?;
    let kind_arg = js::to_js_string(&args["kind"]);
    let kind = match kind_arg.as_str() {
        "administrative_areas" => Kind::AdministrativeAreas,
        "named_places" => Kind::NamedPlaces,
        _ => Kind::Monuments,
    };
    let features = match services
        .features
        .lookup(&services.api, kind, point.lat, point.lon)
        .await?
    {
        Answer::Unavailable => {
            return Err(Failure::tool(
                "unavailable",
                "Map feature lookups need an Overpass server configured for this app",
            ));
        }
        Answer::NoAnswer => {
            return Err(Failure::tool(
                "retry_later",
                "The map feature service did not answer",
            ));
        }
        Answer::Features(features) => features,
    };
    let mut rows: Vec<Value> = features
        .iter()
        .filter_map(|feature| {
            let name = [&feature.english, &feature.primary, &feature.official]
                .into_iter()
                .find_map(|v| v.clone().filter(js::truthy))?;
            let (lat, lon) = match &feature.point {
                Some((lat, lon)) => (
                    lat.as_ref().and_then(js::finite),
                    lon.as_ref().and_then(js::finite),
                ),
                None => (None, None),
            };
            let mut row = Map::new();
            if let Some(id) = &feature.id {
                row.insert("id".into(), id.clone());
            }
            row.insert("name".into(), name);
            row.insert("category".into(), json!(feature.category));
            row.insert(
                "admin_level".into(),
                if feature.level == 99.0 {
                    Value::Null
                } else {
                    js::num(feature.level)
                },
            );
            row.insert("lat".into(), round_or_null(lat, 5));
            row.insert("lon".into(), round_or_null(lon, 5));
            Some(Value::Object(row))
        })
        .collect();
    let level = |row: &Value| row["admin_level"].as_f64().unwrap_or(99.0);
    js::sort_by(&mut rows, |a, b| level(a) - level(b));
    let kind_text = kind_arg.replacen('_', " ", 1);
    let summary = format!(
        "{} ({kind_text}) at {}.",
        count_noun(rows.len(), "named feature", None),
        point.label
    );
    let mut data = Map::new();
    data.insert("location".into(), point_json(&point));
    let data = spread(data, cap_rows(rows, args.get("limit")));
    Ok(ToolResult::new(summary, Value::Object(data)))
}
