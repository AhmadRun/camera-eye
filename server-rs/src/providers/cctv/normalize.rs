//! Port of `server/providers/cctv/normalize.js`, `src/sources/cctvTypes.js`
//! and `src/data/directionText.js`.

use serde_json::{Map, Value, json};

use super::jsval::{self, num};
use crate::common::geo::haversine_km;

/// `normalizeFeedType`: 'image', 'mjpeg', 'mp4', 'webm', 'hls' or pass-through.
pub fn normalize_feed_type(value: &str) -> String {
    let raw = jsval::trim(value).to_lowercase();
    match raw.as_str() {
        "" | "jpeg" | "jpg" | "png" => "image".into(),
        "mjpg" => "mjpeg".into(),
        "video" => "mp4".into(),
        "stream" => "hls".into(),
        _ => raw,
    }
}

/// `isVideoFeedType`.
pub fn is_video_feed_type(feed_type: &str) -> bool {
    matches!(feed_type, "mp4" | "webm" | "hls")
}

/// `directionToHeading(value, allowBare)`: a compass heading, NaN when unknown.
pub fn direction_to_heading(value: &str, allow_bare: bool) -> f64 {
    let text = jsval::trim(value).to_uppercase();
    if text.is_empty() {
        return f64::NAN;
    }
    let any = |words: &[&str]| words.iter().any(|w| jsval::has_word(&text, w));
    if any(&["NORTHBOUND", "NB"]) {
        return 0.0;
    }
    if any(&["SOUTHBOUND", "SB"]) {
        return 180.0;
    }
    if any(&["EASTBOUND", "EB"]) {
        return 90.0;
    }
    if any(&["WESTBOUND", "WB"]) {
        return 270.0;
    }
    if any(&["NORTHEAST", "NE"]) {
        return 45.0;
    }
    if any(&["NORTHWEST", "NW"]) {
        return 315.0;
    }
    if any(&["SOUTHEAST", "SE"]) {
        return 135.0;
    }
    if any(&["SOUTHWEST", "SW"]) {
        return 225.0;
    }
    if allow_bare {
        if any(&["NORTH"]) {
            return 0.0;
        }
        if any(&["SOUTH"]) {
            return 180.0;
        }
        if any(&["EAST"]) {
            return 90.0;
        }
        if any(&["WEST"]) {
            return 270.0;
        }
    }
    f64::NAN
}

/// `directionToHeading(value, allowBare)` for a raw value (`String(value || '')`).
pub fn direction_value_to_heading(value: Option<&Value>, allow_bare: bool) -> f64 {
    direction_to_heading(&jsval::string_or(&[value], ""), allow_bare)
}

/// FNV-1a 32-bit over UTF-16 code units (`hashSeed`).
pub fn hash_seed(text: &str) -> u32 {
    let mut h: u32 = 2_166_136_261;
    for unit in text.encode_utf16() {
        h ^= u32::from(unit);
        h = h.wrapping_mul(16_777_619);
    }
    h
}

/// `escapeXml`.
pub fn escape_xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// `normalizeKey`: lowercase snake_case.
pub fn normalize_key(text: &str) -> String {
    let lower = jsval::trim(text).to_lowercase();
    let mut out = String::new();
    let mut in_sep = false;
    for c in lower.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            out.push(c);
            in_sep = false;
        } else if !in_sep {
            out.push('_');
            in_sep = true;
        }
    }
    out.trim_matches('_').to_string()
}

/// `parsePointString`: WKT `POINT(lon lat)`.
pub fn parse_point_string(value: &str) -> (f64, f64) {
    static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
        regex::Regex::new(
            r"(?i)POINT\s*\(\s*(-?[0-9]+(?:\.[0-9]+)?)\s+(-?[0-9]+(?:\.[0-9]+)?)\s*\)",
        )
        .expect("valid regex")
    });
    match RE.captures(value) {
        Some(caps) => {
            let lon = crate::common::js::number(&caps[1]);
            let lat = crate::common::js::number(&caps[2]);
            (lat, lon)
        }
        None => (f64::NAN, f64::NAN),
    }
}

/// `coerceLatLon`: `(lat, lon)` from WKT or a lat/lon object.
pub fn coerce_lat_lon(value: Option<&Value>) -> (f64, f64) {
    if !jsval::truthy(value) {
        return (f64::NAN, f64::NAN);
    }
    match value {
        Some(Value::String(s)) => parse_point_string(s),
        Some(v @ (Value::Object(_) | Value::Array(_))) => {
            let v = Some(v);
            let g = |k: &str| jsval::get(v, k);
            let lat = jsval::finite_or(
                jsval::nullish(&[g("latitude"), g("lat"), g("y"), g("Latitude"), g("Lat")]),
                f64::NAN,
            );
            let lon = jsval::finite_or(
                jsval::nullish(&[
                    g("longitude"),
                    g("lon"),
                    g("lng"),
                    g("x"),
                    g("Longitude"),
                    g("Lon"),
                ]),
                f64::NAN,
            );
            (lat, lon)
        }
        _ => (f64::NAN, f64::NAN),
    }
}

/// `extractAustinCoords`.
pub fn extract_austin_coords(record: &Map<String, Value>) -> (f64, f64) {
    for key in [
        "location",
        "coordinates",
        "the_geom",
        "point",
        "geocoded_column",
    ] {
        let (lat, lon) = coerce_lat_lon(record.get(key));
        if lat.is_finite() && lon.is_finite() {
            return (lat, lon);
        }
    }
    let g = |k: &str| record.get(k);
    let lat = jsval::finite_or(
        jsval::nullish(&[
            g("latitude"),
            g("lat"),
            g("camera_latitude"),
            g("location_latitude"),
        ]),
        f64::NAN,
    );
    let lon = jsval::finite_or(
        jsval::nullish(&[
            g("longitude"),
            g("lon"),
            g("lng"),
            g("camera_longitude"),
            g("location_longitude"),
        ]),
        f64::NAN,
    );
    (lat, lon)
}

fn all_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// `extractAustinCameraId`.
pub fn extract_austin_camera_id(record: &Map<String, Value>) -> String {
    for key in [
        "camera_id",
        "cameraid",
        "cam_id",
        "device_id",
        "intersection_id",
        "id",
    ] {
        let Some(value) = record.get(key).filter(|v| !v.is_null()) else {
            continue;
        };
        let text = jsval::string(Some(value));
        let text = jsval::trim(&text);
        if all_digits(text) {
            return text.to_string();
        }
    }
    for (key, value) in record {
        if !(key.contains("camera") || key.contains("cam") || key.contains("device")) {
            continue;
        }
        if !key.contains("id") {
            continue;
        }
        let text = jsval::string_or(&[Some(value)], "");
        let text = jsval::trim(&text);
        if all_digits(text) {
            return text.to_string();
        }
    }
    String::new()
}

/// `extractAustinName`.
pub fn extract_austin_name(record: &Map<String, Value>, camera_id: &str) -> String {
    for key in [
        "camera_name",
        "location_name",
        "intersection_name",
        "location",
        "cross_street",
        "description",
        "name",
    ] {
        if let Some(Value::String(s)) = record.get(key) {
            let text = jsval::trim(s);
            if !text.is_empty() {
                return text.to_string();
            }
        }
    }
    format!("Austin Camera {camera_id}")
}

/// `extractAustinHeading`: degrees in [0, 360), NaN when unknown.
pub fn extract_austin_heading(record: &Map<String, Value>) -> f64 {
    let g = |k: &str| record.get(k);
    let direct = jsval::finite_or(
        jsval::nullish(&[g("heading_deg"), g("heading"), g("bearing")]),
        f64::NAN,
    );
    if direct.is_finite() {
        return ((direct % 360.0) + 360.0) % 360.0;
    }
    for key in [
        "direction",
        "travel_direction",
        "facing",
        "facing_direction",
    ] {
        let heading = direction_value_to_heading(record.get(key), true);
        if heading.is_finite() {
            return heading;
        }
    }
    let probe: Vec<String> = [
        "camera_name",
        "location_name",
        "intersection_name",
        "location",
        "cross_street",
        "description",
        "name",
    ]
    .iter()
    .filter_map(|k| record.get(*k).filter(|v| jsval::truthy(Some(v))))
    .map(|v| jsval::string(Some(v)))
    .collect();
    let inferred = direction_to_heading(&probe.join(" "), false);
    if inferred.is_finite() {
        return inferred;
    }
    f64::NAN
}

pub fn is_likely_austin_coordinate(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (30.02..=30.58).contains(&lat)
        && (-98.12..=-97.4).contains(&lon)
}

/// Longest unselected camera label the HUD shows.
pub const CAMERA_CODE_MAX_CHARS: usize = 28;

/// `cameraDisplayCode`.
pub fn camera_display_code(text: &str) -> String {
    let clean = jsval::trim(&jsval::collapse_ws(text)).to_string();
    if jsval::utf16_len(&clean) <= CAMERA_CODE_MAX_CHARS {
        return clean;
    }
    let head = jsval::utf16_slice(&clean, 0, CAMERA_CODE_MAX_CHARS - 1);
    format!("{}…", head.trim_end_matches(jsval::is_js_space))
}

/// `isPlausibleLatLon`: finite, in range, and not null island.
pub fn is_plausible_lat_lon(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && lat.abs() <= 90.0
        && lon.abs() <= 180.0
        && !(lat == 0.0 && lon == 0.0)
}

pub fn is_likely_bc_coordinate(lat: f64, lon: f64) -> bool {
    is_plausible_lat_lon(lat, lon)
        && (48.0..=60.5).contains(&lat)
        && (-139.5..=-114.0).contains(&lon)
}

pub fn is_likely_texas_coordinate(lat: f64, lon: f64) -> bool {
    is_plausible_lat_lon(lat, lon)
        && (25.5..=36.7).contains(&lat)
        && (-107.0..=-93.4).contains(&lon)
}

pub fn is_likely_nsw_coordinate(lat: f64, lon: f64) -> bool {
    is_plausible_lat_lon(lat, lon)
        && (-38.0..=-28.0).contains(&lat)
        && (140.9..=159.2).contains(&lon)
}

pub fn is_likely_calgary_coordinate(lat: f64, lon: f64) -> bool {
    is_plausible_lat_lon(lat, lon)
        && (50.8..=51.25).contains(&lat)
        && (-114.4..=-113.8).contains(&lon)
}

pub fn is_likely_finland_coordinate(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (59.5..=70.5).contains(&lat)
        && (19.0..=32.0).contains(&lon)
}

/// `fintrafficCameraName`.
pub fn fintraffic_camera_name(station_name: &str, station_id: &str, preset_id: &str) -> String {
    let base = jsval::trim(&station_name.replace('_', " ")).to_string();
    let base = if base.is_empty() {
        format!("Fintraffic {station_id}")
    } else {
        base
    };
    let view = jsval::utf16_slice(preset_id, jsval::utf16_len(station_id), usize::MAX);
    if view.is_empty() {
        base
    } else {
        format!("{base} (view {view})")
    }
}

/// `fallbackHeadingFromId`: one of 16 compass directions.
pub fn fallback_heading_from_id(camera_id: &str) -> f64 {
    f64::from(hash_seed(camera_id) % 16) * 22.5
}

/// `rowArrayToObject`: a Socrata row keyed by its column names.
pub fn row_array_to_object(row: &[Value], columns: &[Value]) -> Map<String, Value> {
    let mut record = Map::new();
    for (idx, col) in columns.iter().enumerate() {
        let col = Some(col);
        let name = jsval::or(&[jsval::get(col, "fieldName"), jsval::get(col, "name")]);
        let name = if jsval::truthy(name) {
            jsval::string(name)
        } else {
            format!("col_{idx}")
        };
        let key = normalize_key(&name);
        if key.is_empty() {
            continue;
        }
        match row.get(idx) {
            Some(value) => {
                record.insert(key, value.clone());
            }
            None => {
                record.remove(&key);
            }
        }
    }
    record
}

/// The bounding box of cameras with coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Region {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

impl Region {
    pub fn to_json(self) -> Value {
        json!({
            "west": num(self.west),
            "south": num(self.south),
            "east": num(self.east),
            "north": num(self.north),
        })
    }
}

/// `cameraRegion` over raw items (`Number(camera.lat)`).
pub fn camera_region<'a>(points: impl IntoIterator<Item = (f64, f64)> + 'a) -> Option<Region> {
    let mut region: Option<Region> = None;
    for (lat, lon) in points {
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        region = Some(match region {
            Some(r) => Region {
                west: r.west.min(lon),
                south: r.south.min(lat),
                east: r.east.max(lon),
                north: r.north.max(lat),
            },
            None => Region {
                west: lon,
                south: lat,
                east: lon,
                north: lat,
            },
        });
    }
    region
}

fn item_lat_lon(item: &Value) -> (f64, f64) {
    (
        jsval::number(jsval::get(Some(item), "lat")),
        jsval::number(jsval::get(Some(item), "lon")),
    )
}

/// A loader's result: its cameras, and when it was prioritized, how many
/// it offered and where.
#[derive(Debug, Clone, Default)]
pub struct LoadedPack {
    pub items: Vec<Value>,
    pub available: Option<usize>,
    pub region: Option<Region>,
}

impl LoadedPack {
    pub fn plain(items: Vec<Value>) -> Self {
        Self {
            items,
            available: None,
            region: None,
        }
    }
}

/// `prioritizeSources`: the `max_count` cameras nearest to any anchor.
pub fn prioritize_sources(
    cameras: Vec<Value>,
    max_count: f64,
    anchors: &[(f64, f64)],
) -> LoadedPack {
    let anchors: Vec<(f64, f64)> = anchors
        .iter()
        .copied()
        .filter(|(lat, lon)| lat.is_finite() && lon.is_finite())
        .collect();
    if anchors.is_empty() {
        return LoadedPack::plain(cameras);
    }
    let cap = if max_count.is_finite() && max_count > 0.0 {
        (max_count as usize).min(cameras.len())
    } else {
        cameras.len()
    };
    let available = cameras.len();
    let region = camera_region(cameras.iter().map(item_lat_lon));
    let mut scored: Vec<(f64, usize, Value)> = cameras
        .into_iter()
        .enumerate()
        .map(|(idx, camera)| {
            let (lat, lon) = item_lat_lon(&camera);
            let dist = if lat.is_finite() && lon.is_finite() {
                anchors
                    .iter()
                    .map(|(alat, alon)| haversine_km(lat, lon, *alat, *alon))
                    .fold(f64::INFINITY, f64::min)
            } else {
                f64::INFINITY
            };
            (dist, idx, camera)
        })
        .collect();
    scored.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.cmp(&b.1))
    });
    scored.truncate(cap);
    LoadedPack {
        items: scored.into_iter().map(|(_, _, camera)| camera).collect(),
        available: Some(available),
        region,
    }
}

/// `new Map(items.map((c) => [c.id, c])).values()`: last value per id, at
/// the position of its first occurrence.
pub fn dedupe_by_id(items: Vec<Value>) -> Vec<Value> {
    let mut map: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
    for item in items {
        let id = jsval::string(jsval::get(Some(&item), "id"));
        map.insert(id, item);
    }
    map.into_values().collect()
}

/// One served camera, as `normalizeSourceItem` shapes it.
#[derive(Debug, Clone, PartialEq)]
pub struct CctvSource {
    pub id: String,
    pub name: String,
    pub city: String,
    pub city_id: String,
    pub provider: String,
    pub lat: f64,
    pub lon: f64,
    pub heading_deg: f64,
    pub heading_confidence: String,
    pub pitch_deg: f64,
    pub fov_deg: f64,
    pub range_m: f64,
    pub mount_height_m: f64,
    pub ground_elevation_m: f64,
    pub feed_type: String,
    pub url: String,
    pub snapshot_url: String,
    pub license: String,
    pub credit: String,
    pub code: String,
    pub source_kind: String,
    pub pose_source: Option<String>,
    pub pack: String,
    pub ground_heights: Option<Value>,
}

/// `normalizeSourceItem`.
pub fn normalize_source_item(item: &Value) -> CctvSource {
    let v = Some(item);
    let g = |k: &str| jsval::get(v, k);
    let s = |values: &[Option<&Value>], fallback: &str| jsval::string_or(values, fallback);
    let string_field = |k: &str| match g(k) {
        Some(Value::String(text)) => text.clone(),
        _ => String::new(),
    };
    let name_upper = s(&[g("name")], "").to_uppercase();
    let code_input = if jsval::truthy(g("code")) {
        jsval::string(g("code"))
    } else if !name_upper.is_empty() {
        name_upper
    } else {
        s(&[g("id")], "")
    };
    CctvSource {
        id: jsval::trim(&s(&[g("id")], "")).to_string(),
        name: jsval::trim(&s(&[g("name"), g("id")], "")).to_string(),
        city: s(&[g("city")], ""),
        city_id: s(&[g("cityId")], ""),
        provider: s(&[g("provider")], "Configured CCTV Source"),
        lat: jsval::finite_or(g("lat"), f64::NAN),
        lon: jsval::finite_or(g("lon"), f64::NAN),
        heading_deg: jsval::finite_or(g("headingDeg"), f64::NAN),
        heading_confidence: s(&[g("headingConfidence"), g("headingSource")], "").to_lowercase(),
        pitch_deg: jsval::finite_or(g("pitchDeg"), f64::NAN),
        fov_deg: jsval::finite_or(g("fovDeg"), f64::NAN),
        range_m: jsval::finite_or(g("rangeM"), f64::NAN),
        mount_height_m: jsval::finite_or(g("mountHeightM"), f64::NAN),
        ground_elevation_m: jsval::finite_or(g("groundElevationM"), f64::NAN),
        feed_type: normalize_feed_type(&s(&[g("feedType"), g("type")], "")),
        url: string_field("url"),
        snapshot_url: string_field("snapshotUrl"),
        license: s(&[g("license"), g("licenseNote")], ""),
        credit: jsval::trim(&s(&[g("credit")], "")).to_string(),
        code: camera_display_code(&code_input),
        source_kind: s(&[g("sourceKind"), g("kind")], "configured"),
        pose_source: (g("poseSource") == Some(&Value::String("curated".into())))
            .then(|| "curated".to_string()),
        pack: String::new(),
        ground_heights: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_code_trims_long_names_with_an_ellipsis() {
        assert_eq!(
            camera_display_code(" IH 35   at Cesar Chavez "),
            "IH 35 at Cesar Chavez"
        );
        let code = camera_display_code("City of Windsor – Goyeau Street and Park Street");
        assert_eq!(jsval::utf16_len(&code), CAMERA_CODE_MAX_CHARS);
        assert!(code.ends_with('…'));
        assert_eq!(camera_display_code(""), "");
    }

    #[test]
    fn every_pack_labels_by_its_feed_name() {
        let base = |extra: Value| {
            let mut item = json!({ "lat": 1, "lon": 1, "url": "https://x.test/a.jpg" });
            item.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            normalize_source_item(&item)
        };
        assert_eq!(
            base(json!({"id": "354", "name": "5TH ST / CONGRESS AVE", "sourceKind": "austin-open-data"})).code,
            "5TH ST / CONGRESS AVE"
        );
        assert_eq!(
            base(json!({"id": "on-860", "name": "Goyeau Street and Park Street"})).code,
            "GOYEAU STREET AND PARK STRE…"
        );
        assert_eq!(
            base(json!({"id": "nsw-46fa631a", "name": "Erskine Street looking west", "code": "ERSKINE ST (SYDNEY)"})).code,
            "ERSKINE ST (SYDNEY)"
        );
        assert_eq!(
            base(json!({"id": "fi-c1455101", "name": ""})).code,
            "fi-c1455101"
        );
    }

    #[test]
    fn normalize_keeps_js_coercions() {
        let source = normalize_source_item(&json!({
            "id": " cam ", "lat": null, "lon": "12.5", "feedType": "video",
            "headingSource": "HIGH", "poseSource": "curated", "url": 5
        }));
        assert_eq!(source.id, "cam");
        assert_eq!(source.name, "cam");
        assert_eq!(source.lat, 0.0, "Number(null) is 0");
        assert_eq!(source.lon, 12.5);
        assert!(source.heading_deg.is_nan());
        assert_eq!(source.feed_type, "mp4");
        assert_eq!(source.heading_confidence, "high");
        assert_eq!(source.pose_source.as_deref(), Some("curated"));
        assert_eq!(source.url, "");
        assert_eq!(source.provider, "Configured CCTV Source");
        assert_eq!(source.source_kind, "configured");
    }

    #[test]
    fn feed_types_and_directions() {
        assert_eq!(normalize_feed_type(" JPG "), "image");
        assert_eq!(normalize_feed_type("stream"), "hls");
        assert_eq!(normalize_feed_type("rtsp"), "rtsp");
        assert!(is_video_feed_type("hls") && !is_video_feed_type("mjpeg"));
        assert_eq!(direction_to_heading("US-290 EB", false), 90.0);
        assert!(direction_to_heading("West Ave @ 6th St", false).is_nan());
        assert_eq!(direction_to_heading("West", true), 270.0);
        assert_eq!(direction_to_heading("SW", true), 225.0);
        assert_eq!(
            direction_to_heading("Bow Trail / 37 Street SW", true),
            225.0
        );
        assert!(direction_to_heading("N Lamar Blvd", false).is_nan());
    }

    #[test]
    fn prioritized_pack_reports_what_it_offered() {
        let cameras: Vec<Value> = (0..5)
            .map(|i| json!({"id": format!("c{i}"), "lat": i, "lon": 0}))
            .collect();
        let kept = prioritize_sources(cameras, 2.0, &[(0.0, 0.0)]);
        let ids: Vec<String> = kept
            .items
            .iter()
            .map(|c| c["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["c0", "c1"]);
        assert_eq!(kept.available, Some(5));
        assert_eq!(
            kept.region,
            Some(Region {
                west: 0.0,
                south: 0.0,
                east: 0.0,
                north: 4.0
            })
        );
    }

    #[test]
    fn fintraffic_names_and_bounds() {
        assert_eq!(
            fintraffic_camera_name("vt3_Hyvinkää_Noppo", "C01234", "C0123402"),
            "vt3 Hyvinkää Noppo (view 02)"
        );
        assert_eq!(
            fintraffic_camera_name("", "C01234", "C0123409"),
            "Fintraffic C01234 (view 09)"
        );
        assert_eq!(
            fintraffic_camera_name("vt4_Mäntsälä", "C01234", "C01234"),
            "vt4 Mäntsälä"
        );
        assert!(is_likely_finland_coordinate(60.1699, 24.9384));
        assert!(is_likely_finland_coordinate(70.088512, 27.0));
        assert!(is_likely_finland_coordinate(59.856454, 19.618722));
        assert!(!is_likely_finland_coordinate(51.5074, -0.1278));
        assert!(!is_likely_finland_coordinate(0.0, 0.0));
        assert!(!is_likely_finland_coordinate(f64::NAN, 24.9384));
    }

    #[test]
    fn hash_seed_is_fnv1a_over_utf16() {
        assert_eq!(hash_seed(""), 2_166_136_261);
        assert_eq!(hash_seed("a"), 0xe40c292c);
    }
}
