//! Nominatim hit mapping (port of `src/nominatimGeocode.js`).

use serde_json::{Value, json};

use crate::common::js;
use crate::providers::mapdata_support::{
    js_json_number, js_number_string, js_string_or_empty, js_trim,
};

fn address_types(key: &str) -> Option<&'static [&'static str]> {
    Some(match key {
        "country" | "nation" => &["country", "political"],
        "state" | "province" | "region" => &["administrative_area_level_1", "political"],
        "county" | "district" => &["administrative_area_level_2", "political"],
        "municipality" | "city" | "town" | "village" | "hamlet" => &["locality", "political"],
        "suburb" | "neighbourhood" | "neighborhood" | "quarter" => &["neighborhood", "political"],
        "postcode" => &["postal_code"],
        "road" | "highway" => &["route"],
        "peak" | "mountain_range" | "wood" | "forest" | "island" => &["natural_feature"],
        "park" => &["park"],
        "nature_reserve" => &["park", "natural_feature"],
        "aerodrome" | "airport" => &["airport"],
        "university" => &["university"],
        "stadium" => &["stadium"],
        _ => return None,
    })
}

/// `boundedNumber`: a string or number that parses into `[min, max]`.
fn bounded_number(value: Option<&Value>, min: f64, max: f64) -> Option<f64> {
    let parsed = match value? {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => {
            if js_trim(s).is_empty() {
                return None;
            }
            js::number(s)
        }
        _ => return None,
    };
    (parsed.is_finite() && parsed >= min && parsed <= max).then_some(parsed)
}

pub fn nominatim_latitude(value: Option<&Value>) -> Option<f64> {
    bounded_number(value, -90.0, 90.0)
}

pub fn nominatim_longitude(value: Option<&Value>) -> Option<f64> {
    bounded_number(value, -180.0, 180.0)
}

fn field_lower(hit: &Value, name: &str) -> String {
    js_string_or_empty(hit.get(name)).to_lowercase()
}

/// `typesFromNominatim`.
pub fn types_from_nominatim(hit: &Value) -> Vec<&'static str> {
    let address_type = field_lower(hit, "addresstype");
    let osm_type = field_lower(hit, "type");
    let osm_class = field_lower(hit, "class");
    let mapped: Option<&[&str]> = address_types(&address_type)
        .or_else(|| address_types(&osm_type))
        .or(match osm_class.as_str() {
            "highway" => Some(&["route"]),
            "leisure" => Some(&["park"]),
            "natural" => Some(&["natural_feature"]),
            "aeroway" => Some(&["airport"]),
            "amenity" if osm_type == "university" => Some(&["university"]),
            _ => None,
        });
    mapped.map_or_else(
        || vec!["point_of_interest", "establishment"],
        <[&str]>::to_vec,
    )
}

/// `viewportFromNominatim`.
pub fn viewport_from_nominatim(hit: &Value) -> Option<Value> {
    let bounds = hit.get("boundingbox")?.as_array()?;
    if bounds.len() != 4 {
        return None;
    }
    let south = nominatim_latitude(bounds.first())?;
    let north = nominatim_latitude(bounds.get(1))?;
    let west = nominatim_longitude(bounds.get(2))?;
    let east = nominatim_longitude(bounds.get(3))?;
    if south > north {
        return None;
    }
    Some(json!({
        "southwest": { "lat": js_json_number(south), "lng": js_json_number(west) },
        "northeast": { "lat": js_json_number(north), "lng": js_json_number(east) },
    }))
}

/// `nominatimToGeocodeResult`.
pub fn nominatim_to_geocode_result(hit: Option<&Value>) -> Option<Value> {
    let hit = hit?;
    let lat = nominatim_latitude(hit.get("lat"))?;
    let lng = nominatim_longitude(hit.get("lon"))?;
    let display = js_string_or_empty(hit.get("display_name"));
    let label_source = if display.is_empty() {
        js_string_or_empty(hit.get("name"))
    } else {
        display
    };
    let label = js_trim(&label_source).to_string();
    if label.is_empty() {
        return None;
    }
    let types = types_from_nominatim(hit);
    let canonical = js_trim(&js_string_or_empty(hit.get("name"))).to_string();
    let mut geometry = serde_json::Map::new();
    geometry.insert(
        "location".into(),
        json!({ "lat": js_json_number(lat), "lng": js_json_number(lng) }),
    );
    if let Some(viewport) = viewport_from_nominatim(hit) {
        geometry.insert("bounds".into(), viewport.clone());
        geometry.insert("viewport".into(), viewport);
    }
    let components = if canonical.is_empty() {
        json!([])
    } else {
        json!([{ "long_name": canonical, "short_name": canonical, "types": types }])
    };
    Some(json!({
        "geometry": Value::Object(geometry),
        "formatted_address": label,
        "types": types,
        "address_components": components,
    }))
}

/// `nominatimViewboxFromBounds`: `swLat,swLng|neLat,neLng` ->
/// `west,north,east,south`.
pub fn nominatim_viewbox_from_bounds(bounds: Option<&str>) -> Option<String> {
    let text = js_trim(bounds.unwrap_or(""));
    let (sw, ne) = text.split_once('|')?;
    let (sw_lat, sw_lng) = sw.split_once(',')?;
    let (ne_lat, ne_lng) = ne.split_once(',')?;
    let decimal = |s: &str| {
        let body = s.strip_prefix('-').unwrap_or(s);
        let (int, frac) = match body.split_once('.') {
            Some((i, f)) => (i, Some(f)),
            None => (body, None),
        };
        !int.is_empty()
            && int.bytes().all(|b| b.is_ascii_digit())
            && frac.is_none_or(|f| !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit()))
    };
    if ![sw_lat, sw_lng, ne_lat, ne_lng].iter().all(|s| decimal(s)) {
        return None;
    }
    let as_value = |s: &str| Value::String(s.to_string());
    let sw_lat = nominatim_latitude(Some(&as_value(sw_lat)))?;
    let sw_lng = nominatim_longitude(Some(&as_value(sw_lng)))?;
    let ne_lat = nominatim_latitude(Some(&as_value(ne_lat)))?;
    let ne_lng = nominatim_longitude(Some(&as_value(ne_lng)))?;
    if sw_lat >= ne_lat {
        return None;
    }
    Some(format!(
        "{},{},{},{}",
        js_number_string(sw_lng),
        js_number_string(ne_lat),
        js_number_string(ne_lng),
        js_number_string(sw_lat)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinates_are_numbers_in_range_or_nothing() {
        assert_eq!(nominatim_latitude(Some(&json!("43.1731"))), Some(43.1731));
        assert_eq!(nominatim_latitude(Some(&json!(-33.8568))), Some(-33.8568));
        assert_eq!(nominatim_latitude(Some(&json!("90"))), Some(90.0));
        assert_eq!(nominatim_longitude(Some(&json!("-180"))), Some(-180.0));
        for value in [
            json!(null),
            json!(""),
            json!("   "),
            json!("abc"),
            json!(true),
            json!(false),
            json!([]),
            json!(["43"]),
            json!({}),
            json!("91"),
            json!("-90.5"),
        ] {
            assert_eq!(nominatim_latitude(Some(&value)), None, "{value}");
        }
        assert_eq!(nominatim_latitude(None), None);
        for value in [
            json!(null),
            json!(""),
            json!("181"),
            json!("-180.5"),
            json!(true),
            json!([]),
        ] {
            assert_eq!(nominatim_longitude(Some(&value)), None, "{value}");
        }
    }

    #[test]
    fn hits_without_real_coordinates_are_not_results() {
        for hit in [
            json!(null),
            json!({}),
            json!({ "lat": null, "lon": null, "display_name": "Nowhere" }),
            json!({ "lat": "", "lon": "", "display_name": "Nowhere" }),
            json!({ "lat": "43.17", "lon": null, "display_name": "Half a place" }),
            json!({ "lat": null, "lon": "-79.03", "display_name": "Half a place" }),
            json!({ "lat": "91", "lon": "0", "display_name": "Off the pole" }),
            json!({ "lat": "0", "lon": "999", "display_name": "Off the world" }),
            json!({ "lat": "43.17", "lon": "-79.03" }),
            json!({ "lat": "43.17", "lon": "-79.03", "display_name": "   " }),
        ] {
            assert_eq!(nominatim_to_geocode_result(Some(&hit)), None, "{hit}");
        }
        assert_eq!(nominatim_to_geocode_result(None), None);
    }

    #[test]
    fn a_real_hit_carries_point_label_types_and_box() {
        let result = nominatim_to_geocode_result(Some(&json!({
            "lat": "43.1731", "lon": "-79.0384",
            "display_name": "Niagara Falls, Ontario, Canada", "name": "Niagara Falls",
            "addresstype": "city", "boundingbox": ["43.0500", "43.1500", "-79.1500", "-79.0000"]
        })))
        .unwrap();
        assert_eq!(
            result["geometry"]["location"],
            json!({ "lat": 43.1731, "lng": -79.0384 })
        );
        assert_eq!(
            result["formatted_address"],
            "Niagara Falls, Ontario, Canada"
        );
        assert_eq!(result["types"], json!(["locality", "political"]));
        assert_eq!(
            result["geometry"]["bounds"],
            json!({ "southwest": { "lat": 43.05, "lng": -79.15 }, "northeast": { "lat": 43.15, "lng": -79 } })
        );
        assert_eq!(
            result["address_components"][0]["long_name"],
            "Niagara Falls"
        );
        assert_eq!(
            serde_json::to_string(&result["geometry"]["bounds"]["northeast"]).unwrap(),
            "{\"lat\":43.15,\"lng\":-79}"
        );
    }

    #[test]
    fn impossible_boxes_are_dropped() {
        for bbox in [
            Value::Null,
            json!([]),
            json!(["43.05", "43.15", "-79.15"]),
            json!(["43.05", "43.15", "-79.15", "999"]),
            json!(["43.05", "43.15", "-999", "-79.0"]),
            json!(["-91", "43.15", "-79.15", "-79.0"]),
            json!(["43.05", "91", "-79.15", "-79.0"]),
            json!([null, "43.15", "-79.15", "-79.0"]),
            json!(["", "43.15", "-79.15", "-79.0"]),
            json!(["43.15", "43.05", "-79.15", "-79.0"]),
        ] {
            assert_eq!(
                viewport_from_nominatim(&json!({ "boundingbox": bbox })),
                None
            );
            let result = nominatim_to_geocode_result(Some(&json!({
                "lat": "43.1731", "lon": "-79.0384", "display_name": "Somewhere", "boundingbox": bbox
            })))
            .unwrap();
            assert!(result["geometry"].get("bounds").is_none());
        }
    }

    #[test]
    fn types_fall_back_to_a_place() {
        assert_eq!(
            types_from_nominatim(&json!({ "addresstype": "country" })),
            ["country", "political"]
        );
        assert_eq!(
            types_from_nominatim(&json!({ "class": "highway", "type": "primary" })),
            ["route"]
        );
        assert_eq!(
            types_from_nominatim(&json!({})),
            ["point_of_interest", "establishment"]
        );
        assert_eq!(
            types_from_nominatim(&Value::Null),
            ["point_of_interest", "establishment"]
        );
    }

    #[test]
    fn viewbox_only_from_real_edges() {
        assert_eq!(
            nominatim_viewbox_from_bounds(Some("30.1,-97.95|30.52,-97.55")).as_deref(),
            Some("-97.95,30.52,-97.55,30.1")
        );
        for bounds in [
            None,
            Some(""),
            Some("not bounds"),
            Some("30.1,-97.95"),
            Some("-91,-97.95|30.52,-97.55"),
            Some("30.1,-999|30.52,-97.55"),
            Some("30.1,-97.95|91,-97.55"),
            Some("30.1,-97.95|30.52,999"),
            Some("30.52,-97.95|30.1,-97.55"),
            Some("30.1,-97.95|30.1,-97.55"),
        ] {
            assert_eq!(nominatim_viewbox_from_bounds(bounds), None, "{bounds:?}");
        }
    }
}
