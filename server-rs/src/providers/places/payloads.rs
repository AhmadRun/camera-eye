//! Place and route payload projections (port of
//! `src/data/placeProviderPayloads.js`).

use std::collections::HashSet;

use serde_json::{Map, Value, json};

use super::super::js_value::{
    finite_number, get, nullish, number_value, or, round, to_js_string, to_number,
};

/// `Number.MAX_SAFE_INTEGER`.
pub const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

fn place_context_priority(types: &[Value]) -> i32 {
    let has = |t: &str| types.iter().any(|v| v.as_str() == Some(t));
    if has("historical_landmark") || has("monument") {
        return 100;
    }
    if has("tourist_attraction") || has("museum") {
        return 90;
    }
    if has("premise") || has("street_address") {
        return 75;
    }
    if has("point_of_interest") {
        return 60;
    }
    if has("public_bathroom") {
        return 10;
    }
    40
}

fn approximate_distance_m(
    lat_a: f64,
    lon_a: f64,
    lat_b: Option<&Value>,
    lon_b: Option<&Value>,
) -> f64 {
    let (Some(lat_b), Some(lon_b)) = (finite_number(lat_b), finite_number(lon_b)) else {
        return MAX_SAFE_INTEGER;
    };
    if !lat_a.is_finite() || !lon_a.is_finite() {
        return MAX_SAFE_INTEGER;
    }
    let latitude_scale = 111_320.0;
    let longitude_scale = latitude_scale * (lat_a * std::f64::consts::PI / 180.0).cos();
    round(((lat_b - lat_a) * latitude_scale).hypot((lon_b - lon_a) * longitude_scale))
}

fn or_null(value: Option<&Value>) -> Value {
    value.cloned().unwrap_or(Value::Null)
}

fn types_of(place: &Value) -> Vec<Value> {
    place
        .get("types")
        .and_then(Value::as_array)
        .map(|types| types.iter().take(8).cloned().collect())
        .unwrap_or_default()
}

/// A JS TypeError where the JS projection would throw (a `null` record).
fn places_of(data: &Value) -> Result<Option<&Vec<Value>>, String> {
    if data.is_null() {
        return Err("Cannot read properties of null (reading 'places')".into());
    }
    let places = get(Some(data), "places").and_then(Value::as_array);
    if places.is_some_and(|p| p.iter().any(Value::is_null)) {
        return Err("Cannot read properties of null (reading 'location')".into());
    }
    Ok(places)
}

/// `projectNearbyPlaces`.
pub fn project_nearby_places(
    data: &Value,
    latitude: f64,
    longitude: f64,
) -> Result<Vec<Value>, String> {
    let Some(places) = places_of(data)? else {
        return Ok(Vec::new());
    };
    let mut seen = HashSet::new();
    let mut rows: Vec<(i32, f64, Map<String, Value>)> = Vec::new();
    for place in places {
        let p = Some(place);
        let place_latitude = nullish(&[get(get(p, "location"), "latitude")]);
        let place_longitude = nullish(&[get(get(p, "location"), "longitude")]);
        let types = types_of(place);
        let distance = approximate_distance_m(latitude, longitude, place_latitude, place_longitude);
        let name = or_null(or(&[get(get(p, "displayName"), "text")]));
        let address = or_null(or(&[
            get(p, "shortFormattedAddress"),
            get(p, "formattedAddress"),
        ]));
        let priority = place_context_priority(&types);
        let mut row = Map::new();
        row.insert("id".into(), or_null(or(&[get(p, "id")])));
        row.insert("name".into(), name.clone());
        row.insert("address".into(), address.clone());
        row.insert("latitude".into(), or_null(place_latitude));
        row.insert("longitude".into(), or_null(place_longitude));
        row.insert("distanceM".into(), number_value(distance));
        row.insert(
            "primaryType".into(),
            or_null(or(&[
                get(get(p, "primaryTypeDisplayName"), "text"),
                get(p, "primaryType"),
            ])),
        );
        row.insert("types".into(), Value::Array(types));
        if name.is_null() {
            continue;
        }
        let address_text = if address.is_null() {
            String::new()
        } else {
            to_js_string(&address)
        };
        let key = format!("{}:{}", to_js_string(&name), address_text).to_lowercase();
        if !seen.insert(key) {
            continue;
        }
        rows.push((priority, distance, row));
    }
    rows.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });
    Ok(rows
        .into_iter()
        .take(20)
        .map(|(_, _, row)| Value::Object(row))
        .collect())
}

/// `projectTextSearchPlaces`.
pub fn project_text_search_places(
    data: &Value,
    latitude: f64,
    longitude: f64,
) -> Result<Vec<Value>, String> {
    let Some(places) = places_of(data)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for place in places {
        let p = Some(place);
        let place_latitude = nullish(&[get(get(p, "location"), "latitude")]);
        let place_longitude = nullish(&[get(get(p, "location"), "longitude")]);
        let vp = get(p, "viewport");
        let corner = |name: &str, axis: &str| finite_number(get(get(vp, name), axis));
        let viewport = match (
            corner("low", "latitude"),
            corner("low", "longitude"),
            corner("high", "latitude"),
            corner("high", "longitude"),
        ) {
            (Some(_), Some(_), Some(_), Some(_)) => json!({
                "low": {
                    "latitude": get(get(vp, "low"), "latitude"),
                    "longitude": get(get(vp, "low"), "longitude"),
                },
                "high": {
                    "latitude": get(get(vp, "high"), "latitude"),
                    "longitude": get(get(vp, "high"), "longitude"),
                },
            }),
            _ => Value::Null,
        };
        let name = or_null(or(&[get(get(p, "displayName"), "text")]));
        if name.is_null() {
            continue;
        }
        let mut row = Map::new();
        row.insert("id".into(), or_null(or(&[get(p, "id")])));
        row.insert("name".into(), name);
        row.insert("address".into(), or_null(or(&[get(p, "formattedAddress")])));
        row.insert("latitude".into(), or_null(place_latitude));
        row.insert("longitude".into(), or_null(place_longitude));
        row.insert(
            "distanceM".into(),
            number_value(approximate_distance_m(
                latitude,
                longitude,
                place_latitude,
                place_longitude,
            )),
        );
        row.insert("primaryType".into(), or_null(or(&[get(p, "primaryType")])));
        row.insert("types".into(), Value::Array(types_of(place)));
        row.insert("viewport".into(), viewport);
        out.push(Value::Object(row));
    }
    Ok(out)
}

/// `normalizeRouteProfile`.
pub fn normalize_route_profile(raw: &str) -> Option<&'static str> {
    match raw {
        "car" | "driving" => Some("car"),
        "bike" | "cycling" | "bicycle" => Some("bike"),
        "foot" | "walking" | "walk" => Some("foot"),
        _ => None,
    }
}

/// `projectRouteResult`.
pub fn project_route_result(route: &Value, profile: &str) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("ok".into(), Value::Bool(true));
    out.insert("profile".into(), Value::String(profile.into()));
    out.insert(
        "distanceM".into(),
        number_value(round(to_number(route.get("distance")))),
    );
    out.insert(
        "durationS".into(),
        number_value(round(to_number(route.get("duration")))),
    );
    out.insert(
        "geometry".into(),
        or_null(get(get(Some(route), "geometry"), "coordinates")),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn place(name: &str, types: &[&str], longitude: f64) -> Value {
        json!({
            "displayName": { "text": name },
            "types": types,
            "location": { "latitude": 0, "longitude": longitude },
            "formattedAddress": "Street",
        })
    }

    #[test]
    fn nearby_labels_rank_dedupe_and_bound() {
        let mut places = vec![
            place("Bathroom", &["public_bathroom"], 0.0),
            place("Monument", &["monument"], 0.1),
            place("MONUMENT", &["monument"], 0.0),
        ];
        for i in 0..25 {
            places.push(place(
                &format!("POI {i}"),
                &["point_of_interest"],
                i as f64 / 100.0,
            ));
        }
        places.push(json!({ "id": "unnamed" }));
        let result = project_nearby_places(&json!({ "places": places }), 0.0, 0.0).unwrap();
        assert_eq!(result.len(), 20);
        assert_eq!(result[0]["name"], "Monument");
        assert_eq!(result[0]["distanceM"], 11132);
        assert_eq!(
            result
                .iter()
                .filter(|p| p["name"].as_str().unwrap().to_lowercase() == "monument")
                .count(),
            1
        );
        assert!(result.iter().all(|p| p.get("contextPriority").is_none()));
        assert_eq!(
            project_nearby_places(&json!({}), 0.0, 0.0).unwrap(),
            Vec::<Value>::new()
        );
        assert!(project_nearby_places(&Value::Null, 0.0, 0.0).is_err());
    }

    #[test]
    fn text_search_bounds() {
        let viewport = json!({
            "low": { "latitude": 1, "longitude": 2 },
            "high": { "latitude": 3, "longitude": 4 },
        });
        let result = project_text_search_places(
            &json!({
                "places": [
                    { "displayName": { "text": "Museum" }, "viewport": viewport,
                      "types": vec!["museum"; 12] },
                    { "displayName": { "text": "Park" }, "viewport": { "low": viewport["low"] } },
                ],
            }),
            0.0,
            0.0,
        )
        .unwrap();
        assert_eq!(result[0]["viewport"], viewport);
        assert_eq!(result[0]["distanceM"], json!(9_007_199_254_740_991_i64));
        assert_eq!(result[0]["latitude"], Value::Null);
        assert_eq!(result[0]["types"].as_array().unwrap().len(), 8);
        assert_eq!(result[1]["viewport"], Value::Null);
    }

    #[test]
    fn route_profiles_and_projection() {
        assert_eq!(normalize_route_profile("driving"), Some("car"));
        assert_eq!(normalize_route_profile("bicycle"), Some("bike"));
        assert_eq!(normalize_route_profile("walk"), Some("foot"));
        assert_eq!(normalize_route_profile("plane"), None);
        let route = json!({ "distance": 123.6, "duration": 80.2,
                            "geometry": { "coordinates": [[-97, 30]] } });
        assert_eq!(
            Value::Object(project_route_result(&route, "car")),
            json!({ "ok": true, "profile": "car", "distanceM": 124, "durationS": 80,
                    "geometry": [[-97, 30]] })
        );
    }
}
