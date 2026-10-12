//! Port of `server/providers/cctv/sources.js`: the live open-data packs and
//! the curated catalog files. Every loader fails soft to an empty pack.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Map, Value, json};

use super::constants::*;
use super::fetch::{FetchRequest, FetchResponse, Fetcher, Redirect};
use super::jsval::{self, get, num};
use super::normalize::{
    LoadedPack, camera_display_code, dedupe_by_id, direction_to_heading,
    direction_value_to_heading, extract_austin_camera_id, extract_austin_coords,
    extract_austin_heading, extract_austin_name, fallback_heading_from_id, fintraffic_camera_name,
    is_likely_austin_coordinate, is_likely_bc_coordinate, is_likely_calgary_coordinate,
    is_likely_finland_coordinate, is_likely_nsw_coordinate, is_likely_texas_coordinate,
    is_plausible_lat_lon, prioritize_sources, row_array_to_object,
};

/// Raw environment lookup (`process.env[name]`), read at call time.
pub type EnvFn = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// What every loader needs: configuration, the network and the source root.
#[derive(Clone)]
pub struct LoaderDeps {
    pub env: EnvFn,
    pub fetch: Fetcher,
    pub root: PathBuf,
}

impl LoaderDeps {
    pub fn env(&self, name: &str) -> Option<String> {
        (self.env)(name)
    }

    /// `process.env[name] || fallback`.
    pub fn env_or(&self, name: &str, fallback: &str) -> String {
        self.env(name)
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| fallback.to_string())
    }

    /// A per-pack cap: `Number(env || default)` floored into [8, max].
    pub fn pack_cap(&self, name: &str, default: f64, max: f64) -> f64 {
        let n = match self.env(name).filter(|v| !v.is_empty()) {
            Some(text) => crate::common::js::number(&text),
            None => default,
        };
        if n.is_finite() {
            n.floor().min(max).max(8.0)
        } else {
            default
        }
    }

    /// One catalog request under the per-provider deadline (headers and body).
    async fn load<T, F, Fut>(&self, request: FetchRequest, read: F) -> Result<T, LoadError>
    where
        F: FnOnce(FetchResponse) -> Fut,
        Fut: std::future::Future<Output = Result<T, LoadError>>,
    {
        let fetch = Arc::clone(&self.fetch);
        let work = async move {
            let response = fetch(request).await.map_err(LoadError::Network)?;
            read(response).await
        };
        tokio::time::timeout(Duration::from_millis(CCTV_SOURCE_FETCH_TIMEOUT_MS), work)
            .await
            .map_err(|_| LoadError::Network("The operation was aborted due to timeout".into()))?
    }

    fn resolve(&self, file: &str) -> PathBuf {
        self.root.join(file)
    }
}

#[derive(Debug)]
enum LoadError {
    Status(u16),
    Redirected,
    Network(String),
    Body,
}

fn catalog_request(url: &str, redirect: Redirect) -> FetchRequest {
    FetchRequest::new(url, redirect, true).header("Accept", "application/json")
}

/// `resp.ok ? await resp.json() : <status>`.
async fn ok_json(response: FetchResponse) -> Result<Value, LoadError> {
    if !response.ok() {
        return Err(LoadError::Status(response.status));
    }
    response.json().await.ok_or(LoadError::Body)
}

/// The two pose personalities every pack starts from.
fn personality(has_heading: bool) -> (f64, f64, f64, f64) {
    if has_heading {
        (-24.0, 56.0, 210.0, 10.0)
    } else {
        (-18.0, 44.0, 145.0, 8.0)
    }
}

fn warn_load(pack: &str, error: &LoadError) {
    match error {
        LoadError::Status(status) => tracing::warn!("[CCTV] {pack} download failed: {status}"),
        LoadError::Redirected => {
            tracing::warn!("[CCTV] {pack} redirected; redirects are not followed")
        }
        LoadError::Network(message) => tracing::warn!("[CCTV] {pack} download error: {message}"),
        LoadError::Body => tracing::warn!("[CCTV] {pack} download error: invalid body"),
    }
}

// ---------------------------------------------------------------- Austin

/// `loadAustinSourcesFromOpenData`.
pub async fn load_austin(d: &LoaderDeps) -> LoadedPack {
    let endpoint = d.env_or("CCTV_AUSTIN_ROWS_URL", DEFAULT_AUSTIN_ROWS_URL);
    match d
        .load(catalog_request(&endpoint, Redirect::Follow), ok_json)
        .await
    {
        Ok(payload) => parse_austin(
            &payload,
            d.pack_cap("CCTV_AUSTIN_MAX_SOURCES", DEFAULT_AUSTIN_MAX_SOURCES, 300.0),
        ),
        Err(error) => {
            warn_load("Austin source", &error);
            LoadedPack::default()
        }
    }
}

/// The Austin Socrata rows.json payload to cameras.
pub fn parse_austin(payload: &Value, max_count: f64) -> LoadedPack {
    let p = Some(payload);
    let columns = match get(get(get(p, "meta"), "view"), "columns") {
        Some(Value::Array(c)) => c.clone(),
        _ => Vec::new(),
    };
    let rows = match get(p, "data") {
        Some(Value::Array(r)) => r.clone(),
        _ => Vec::new(),
    };
    if columns.is_empty() || rows.is_empty() {
        return LoadedPack::default();
    }
    let mut cameras = Vec::new();
    for row in &rows {
        let Value::Array(row) = row else {
            continue;
        };
        let record = row_array_to_object(row, &columns);
        let camera_id = extract_austin_camera_id(&record);
        if camera_id.is_empty() {
            continue;
        }
        let status =
            jsval::trim(&jsval::string_or(&[record.get("camera_status")], "")).to_uppercase();
        if !status.is_empty() && status != "TURNED_ON" {
            continue;
        }
        let (lat, lon) = extract_austin_coords(&record);
        if !lat.is_finite() || !lon.is_finite() || !is_likely_austin_coordinate(lat, lon) {
            continue;
        }
        let extracted = extract_austin_heading(&record);
        let has_heading = extracted.is_finite();
        let heading = if has_heading {
            extracted
        } else {
            fallback_heading_from_id(&camera_id)
        };
        let (pitch, fov, range, mount) = personality(has_heading);
        let image = format!(
            "https://cctv.austinmobility.io/image/{}.jpg",
            jsval::encode_uri_component(&camera_id)
        );
        cameras.push(json!({
            "id": camera_id,
            "name": extract_austin_name(&record, &camera_id),
            "city": "Austin",
            "cityId": "austin",
            "provider": "Austin Transportation & Public Works",
            "lat": lat, "lon": lon,
            "headingDeg": heading,
            "headingConfidence": if has_heading { "high" } else { "low" },
            "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
            "groundElevationM": 150,
            "feedType": "image",
            "url": image, "snapshotUrl": image,
            "sourceKind": "austin-open-data",
            "license": "Public city traffic camera frame",
        }));
    }
    let unique = dedupe_by_id(cameras);
    let total = unique.len();
    let prioritized = prioritize_sources(unique, max_count, &[AUSTIN_DOWNTOWN]);
    if prioritized.items.len() < total {
        tracing::info!(
            "[CCTV] Loaded Austin camera sources: {total} (using nearest {})",
            prioritized.items.len()
        );
    } else {
        tracing::info!(
            "[CCTV] Loaded Austin camera sources: {}",
            prioritized.items.len()
        );
    }
    prioritized
}

// ---------------------------------------------------------------- Caltrans

/// `loadCaltransSourcesFromOpenData`: one feed per configured district.
pub async fn load_caltrans(d: &LoaderDeps) -> LoadedPack {
    let raw = d
        .env("CCTV_CALTRANS_DISTRICTS")
        .unwrap_or_else(|| DEFAULT_CALTRANS_DISTRICTS.to_string());
    let districts: Vec<i64> = raw
        .split(',')
        .map(|t| crate::common::js::number(jsval::trim(t)))
        .filter(|n| n.fract() == 0.0 && (1.0..=12.0).contains(n))
        .map(|n| n as i64)
        .collect();
    if districts.is_empty() {
        return LoadedPack::default();
    }
    let settled = futures_util::future::join_all(districts.iter().map(|district| {
        let url = caltrans_cctv_url(*district);
        async move {
            d.load(catalog_request(&url, Redirect::Follow), ok_json)
                .await
                .map(|payload| (*district, payload))
        }
    }))
    .await;
    let mut cameras: Vec<Value> = Vec::new();
    for result in settled {
        match result {
            Ok((district, payload)) => {
                let rows = match get(Some(&payload), "data") {
                    Some(Value::Array(rows)) => rows.clone(),
                    _ => Vec::new(),
                };
                for row in &rows {
                    if let Some(camera) = caltrans_row(row, district, cameras.len()) {
                        cameras.push(camera);
                    }
                }
            }
            Err(error) => warn_load("Caltrans district", &error),
        }
    }
    let total = cameras.len();
    let max = d.pack_cap(
        "CCTV_CALTRANS_MAX_SOURCES",
        DEFAULT_CALTRANS_MAX_SOURCES,
        600.0,
    );
    let prioritized = prioritize_sources(cameras, max, &CALTRANS_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded Caltrans camera sources: {total} inService (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

fn caltrans_row(row: &Value, district: i64, index: usize) -> Option<Value> {
    static CODE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^([A-Za-z0-9_-]+)\s*--").expect("regex"));
    static PREFIX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^([A-Za-z0-9_-]+)\s*--\s*").expect("regex"));
    let cctv = get(Some(row), "cctv").filter(|v| jsval::truthy(Some(v)))?;
    if jsval::string(get(Some(cctv), "inService")).to_lowercase() != "true" {
        return None;
    }
    let loc_value = get(Some(cctv), "location");
    let empty = json!({});
    let loc = if jsval::truthy(loc_value) {
        loc_value
    } else {
        Some(&empty)
    };
    let lat = jsval::finite_or(get(loc, "latitude"), f64::NAN);
    let lon = jsval::finite_or(get(loc, "longitude"), f64::NAN);
    if !lat.is_finite() || !lon.is_finite() {
        return None;
    }
    let image_url = jsval::string_or(
        &[get(
            get(get(Some(cctv), "imageData"), "static"),
            "currentImageURL",
        )],
        "",
    );
    if !image_url.starts_with("https://cwwp2.dot.ca.gov/") {
        return None;
    }
    let location_name = jsval::trim(&jsval::string_or(&[get(loc, "locationName")], "")).to_string();
    let code = match CODE.captures(&location_name) {
        Some(caps) => caps[1].to_string(),
        None => format!("x{index}"),
    }
    .to_lowercase();
    let camera_id = format!("ca-d{district}-{code}");
    let heading = direction_value_to_heading(get(loc, "direction"), true);
    let has_heading = heading.is_finite();
    let stripped = PREFIX.replace(&location_name, "").into_owned();
    let label = if stripped.is_empty() {
        format!("Caltrans D{district} {code}")
    } else {
        stripped
    };
    let nearby = get(loc, "nearbyPlace");
    let name = if jsval::truthy(nearby) {
        format!("{label} ({})", jsval::string(nearby))
    } else {
        label
    };
    let feet = jsval::finite_or(get(loc, "elevation"), f64::NAN);
    let ground = if feet.is_finite() {
        (feet * 0.3048).clamp(-100.0, 4000.0)
    } else {
        150.0
    };
    let (pitch, fov, range, mount) = personality(has_heading);
    Some(json!({
        "id": camera_id,
        "name": name,
        "city": jsval::string_or(&[nearby], &format!("Caltrans D{district}")),
        "cityId": format!("ca-d{district}"),
        "provider": "Caltrans",
        "lat": lat, "lon": lon,
        "headingDeg": if has_heading { heading } else { fallback_heading_from_id(&camera_id) },
        "headingConfidence": if has_heading { "high" } else { "low" },
        "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
        "groundElevationM": ground,
        "feedType": "image",
        "url": image_url, "snapshotUrl": image_url,
        "sourceKind": "caltrans-open-data",
        "license": "Public Caltrans highway camera frame",
    }))
}

// ---------------------------------------------------------------- TfL

/// `loadTflSourcesFromOpenData`: London JamCams.
pub async fn load_tfl(d: &LoaderDeps) -> LoadedPack {
    let app_key = jsval::trim(&d.env_or("TFL_APP_KEY", "")).to_string();
    let url = if app_key.is_empty() {
        TFL_JAMCAM_URL.to_string()
    } else {
        format!(
            "{TFL_JAMCAM_URL}?app_key={}",
            jsval::encode_uri_component(&app_key)
        )
    };
    match d
        .load(catalog_request(&url, Redirect::Follow), ok_json)
        .await
    {
        Ok(places) => parse_tfl(
            &places,
            d.pack_cap("CCTV_TFL_MAX_SOURCES", DEFAULT_TFL_MAX_SOURCES, 600.0),
        ),
        Err(error) => {
            warn_load("TfL JamCam", &error);
            LoadedPack::default()
        }
    }
}

fn parse_tfl(places: &Value, max_count: f64) -> LoadedPack {
    let Value::Array(places) = places else {
        return LoadedPack::default();
    };
    let mut cameras = Vec::new();
    for place in places {
        let place = Some(place);
        let mut props: Map<String, Value> = Map::new();
        let Ok(list) = jsval::iterate(get(place, "additionalProperties")) else {
            tracing::warn!(
                "[CCTV] TfL JamCam download error: additionalProperties is not iterable"
            );
            return LoadedPack::default();
        };
        for p in &list {
            let key = get(Some(p), "key");
            if jsval::truthy(key) {
                props.insert(
                    jsval::string(key),
                    get(Some(p), "value").cloned().unwrap_or(Value::Null),
                );
            }
        }
        if jsval::string(props.get("available")).to_lowercase() != "true" {
            continue;
        }
        let lat = jsval::finite_or(get(place, "lat"), f64::NAN);
        let lon = jsval::finite_or(get(place, "lon"), f64::NAN);
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        let image_url = jsval::string_or(&[props.get("imageUrl")], "");
        if !image_url.starts_with(TFL_IMAGE_ORIGIN) {
            continue;
        }
        let raw_id = jsval::string_or(&[get(place, "id")], "");
        let raw_id = raw_id
            .strip_prefix("JamCams_")
            .unwrap_or(&raw_id)
            .to_string();
        if raw_id.is_empty() {
            continue;
        }
        let camera_id = format!("tfl-{raw_id}");
        cameras.push(json!({
            "id": camera_id,
            "name": jsval::string_or(&[get(place, "commonName")], &format!("JamCam {raw_id}")),
            "city": "London",
            "cityId": "london",
            "provider": "Transport for London",
            "lat": lat, "lon": lon,
            "headingDeg": fallback_heading_from_id(&camera_id),
            "headingConfidence": "low",
            "pitchDeg": -18, "fovDeg": 44, "rangeM": 145, "mountHeightM": 8,
            "groundElevationM": 15,
            "feedType": "image",
            "url": image_url, "snapshotUrl": image_url,
            "sourceKind": "tfl-open-data",
            "license": "Powered by TfL Open Data",
        }));
    }
    let total = cameras.len();
    let prioritized = prioritize_sources(cameras, max_count, &[LONDON_CENTER]);
    tracing::info!(
        "[CCTV] Loaded TfL JamCam sources: {total} available (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- Ontario

fn is_likely_ontario_coordinate(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (41.0..=57.5).contains(&lat)
        && (-95.6..=-74.0).contains(&lon)
}

/// `normalizeOntarioCctvUrl`: pin a view URL to the official still host.
fn normalize_ontario_cctv_url(value: Option<&Value>) -> String {
    let text = jsval::string_or(&[value], "");
    let Ok(parsed) = url::Url::parse(jsval::trim(&text)) else {
        return String::new();
    };
    let Some(view) = parsed.path().strip_prefix("/map/Cctv/") else {
        return String::new();
    };
    if view.is_empty() || view.contains(['/', '?', '#']) {
        return String::new();
    }
    let host = parsed.host_str().unwrap_or("").to_lowercase();
    if parsed.scheme() != "https" || (host != "511on.ca" && !host.ends_with(".traveliq.co")) {
        return String::new();
    }
    let Some(view_id) = jsval::decode_uri_component(view) else {
        return String::new();
    };
    if view_id.is_empty()
        || !view_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
    {
        return String::new();
    }
    format!(
        "{ONTARIO_511_IMAGE_ORIGIN}{}",
        jsval::encode_uri_component(&view_id)
    )
}

fn mentions_down(text: &str) -> bool {
    jsval::has_word(&text.to_ascii_uppercase(), "DOWN")
}

/// `pickOntarioCctvView`: the first enabled view that is not marked down.
fn pick_ontario_cctv_view(views: Option<&Value>) -> Option<(String, String)> {
    let Some(Value::Array(views)) = views else {
        return None;
    };
    let enabled: Vec<(String, String)> = views
        .iter()
        .filter(|view| {
            let view = Some(*view);
            jsval::trim(&jsval::string_or(
                &[get(view, "Status"), get(view, "status")],
                "",
            ))
            .to_lowercase()
                == "enabled"
        })
        .map(|view| {
            let view = Some(view);
            (
                normalize_ontario_cctv_url(jsval::or(&[get(view, "Url"), get(view, "url")])),
                jsval::trim(&jsval::string_or(
                    &[get(view, "Description"), get(view, "description")],
                    "",
                ))
                .to_string(),
            )
        })
        .filter(|(url, _)| !url.is_empty())
        .collect();
    enabled
        .iter()
        .find(|(_, description)| !mentions_down(description))
        .or(enabled.first())
        .cloned()
}

/// `loadOntarioSourcesFromOpenData`.
pub async fn load_ontario(d: &LoaderDeps) -> LoadedPack {
    match d
        .load(
            catalog_request(ONTARIO_511_CAMERAS_URL, Redirect::Follow),
            ok_json,
        )
        .await
    {
        Ok(rows) => parse_ontario(
            &rows,
            d.pack_cap(
                "CCTV_ONTARIO_MAX_SOURCES",
                DEFAULT_ONTARIO_MAX_SOURCES,
                1000.0,
            ),
        ),
        Err(error) => {
            warn_load("Ontario 511 camera", &error);
            LoadedPack::default()
        }
    }
}

fn parse_ontario(rows: &Value, max_count: f64) -> LoadedPack {
    let Value::Array(rows) = rows else {
        return LoadedPack::default();
    };
    let mut cameras = Vec::new();
    for row in rows {
        let r = Some(row);
        let raw_id = jsval::trim(&{
            let v = jsval::nullish(&[get(r, "Id"), get(r, "id")]);
            if v.is_some() {
                jsval::string(v)
            } else {
                String::new()
            }
        })
        .to_string();
        if raw_id.is_empty() {
            continue;
        }
        let lat = jsval::finite_or(
            jsval::nullish(&[get(r, "Latitude"), get(r, "latitude")]),
            f64::NAN,
        );
        let lon = jsval::finite_or(
            jsval::nullish(&[get(r, "Longitude"), get(r, "longitude")]),
            f64::NAN,
        );
        if !is_likely_ontario_coordinate(lat, lon) {
            continue;
        }
        let Some((view_url, description)) =
            pick_ontario_cctv_view(jsval::or(&[get(r, "Views"), get(r, "views")]))
        else {
            continue;
        };
        let camera_id = format!("on-{raw_id}");
        let location = jsval::trim(&jsval::string_or(
            &[get(r, "Location"), get(r, "location")],
            "",
        ))
        .to_string();
        let roadway = jsval::trim(&jsval::string_or(
            &[get(r, "Roadway"), get(r, "roadway")],
            "",
        ))
        .to_string();
        let view_label = if !description.is_empty() && !mentions_down(&description) {
            description.clone()
        } else {
            String::new()
        };
        let head = if !location.is_empty() {
            location.clone()
        } else if !roadway.is_empty() {
            roadway.clone()
        } else {
            format!("Ontario 511 Camera {raw_id}")
        };
        let label = [head, view_label]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" - ");
        let direction = jsval::nullish(&[get(r, "Direction"), get(r, "direction")]);
        let mut heading = direction_value_to_heading(direction, true);
        if !heading.is_finite() {
            heading = direction_to_heading(&description, true);
        }
        let has_heading = heading.is_finite();
        let (pitch, fov, range, mount) = personality(has_heading);
        let city = if !location.is_empty() {
            location
        } else if !roadway.is_empty() {
            roadway
        } else {
            "Ontario".into()
        };
        cameras.push(json!({
            "id": camera_id,
            "name": label,
            "city": city,
            "cityId": "ontario",
            "provider": "Ontario 511",
            "lat": lat, "lon": lon,
            "headingDeg": if has_heading { heading } else { fallback_heading_from_id(&camera_id) },
            "headingConfidence": if has_heading { "high" } else { "low" },
            "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
            "groundElevationM": 200,
            "feedType": "image",
            "url": view_url, "snapshotUrl": view_url,
            "sourceKind": "ontario-511-open-data",
            "license": "Open Government Licence - Ontario",
        }));
    }
    let unique = dedupe_by_id(cameras);
    let total = unique.len();
    let prioritized = prioritize_sources(unique, max_count, &ONTARIO_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded Ontario 511 camera sources: {total} enabled (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- Fintraffic

/// `loadFintrafficSourcesFromOpenData`: one preset (fixed view) is one camera.
pub async fn load_fintraffic(d: &LoaderDeps) -> LoadedPack {
    let request = FetchRequest::new(FINTRAFFIC_STATIONS_URL, Redirect::Manual, true)
        .header("Accept", "application/json")
        .header("Accept-Encoding", "gzip")
        .header("Digitraffic-User", DIGITRAFFIC_USER);
    let result = d
        .load(request, |response| async move {
            if (300..400).contains(&response.status) {
                return Err(LoadError::Redirected);
            }
            ok_json(response).await
        })
        .await;
    match result {
        Ok(payload) => parse_fintraffic(
            &payload,
            d.pack_cap(
                "CCTV_FINTRAFFIC_MAX_SOURCES",
                DEFAULT_FINTRAFFIC_MAX_SOURCES,
                600.0,
            ),
        ),
        Err(error) => {
            warn_load("Fintraffic station list", &error);
            LoadedPack::default()
        }
    }
}

fn parse_fintraffic(payload: &Value, max_count: f64) -> LoadedPack {
    static PRESET: Lazy<Regex> = Lazy::new(|| Regex::new(r"^C[0-9]{7}$").expect("regex"));
    let features = match get(Some(payload), "features") {
        Some(Value::Array(f)) => f.clone(),
        _ => Vec::new(),
    };
    if features.is_empty() {
        return LoadedPack::default();
    }
    let mut cameras = Vec::new();
    let mut stations_seen = 0usize;
    let empty = json!({});
    for feature in &features {
        let props_value = get(Some(feature), "properties");
        let props = if jsval::truthy(props_value) {
            props_value
        } else {
            Some(&empty)
        };
        let station_id = jsval::trim(&jsval::string_or(&[get(props, "id")], "")).to_string();
        if station_id.is_empty() {
            continue;
        }
        if jsval::string_or(&[get(props, "collectionStatus")], "").to_uppercase() != "GATHERING" {
            continue;
        }
        let coords = get(get(Some(feature), "geometry"), "coordinates");
        let lon = jsval::finite_or(jsval::index(coords, 0).as_ref(), f64::NAN);
        let lat = jsval::finite_or(jsval::index(coords, 1).as_ref(), f64::NAN);
        if !is_likely_finland_coordinate(lat, lon) {
            continue;
        }
        let reported = jsval::finite_or(jsval::index(coords, 2).as_ref(), 0.0);
        let ground = if reported > 0.0 {
            reported.min(1400.0)
        } else {
            FINTRAFFIC_GROUND_ELEVATION_M
        };
        stations_seen += 1;
        let Ok(presets) = jsval::iterate(get(props, "presets")) else {
            tracing::warn!("[CCTV] Fintraffic station download error: presets is not iterable");
            return LoadedPack::default();
        };
        for preset in &presets {
            let preset = Some(preset);
            if get(preset, "inCollection") != Some(&Value::Bool(true)) {
                continue;
            }
            let preset_id = jsval::trim(&jsval::string_or(&[get(preset, "id")], "")).to_string();
            if !PRESET.is_match(&preset_id) || !preset_id.starts_with(&station_id) {
                continue;
            }
            let camera_id = format!("fi-{}", preset_id.to_lowercase());
            let image_url = format!("{FINTRAFFIC_IMAGE_ORIGIN}{preset_id}.jpg");
            let name = jsval::string_or(&[get(props, "name")], "");
            cameras.push(json!({
                "id": camera_id,
                "name": fintraffic_camera_name(&name, &station_id, &preset_id),
                "city": "Finland",
                "cityId": "finland",
                "provider": "Fintraffic",
                "lat": lat, "lon": lon,
                "headingDeg": fallback_heading_from_id(&camera_id),
                "headingConfidence": "low",
                "pitchDeg": -18, "fovDeg": 44, "rangeM": 145, "mountHeightM": 8,
                "groundElevationM": ground,
                "feedType": "image",
                "url": image_url, "snapshotUrl": image_url,
                "sourceKind": "fintraffic-open-data",
                "license": "Fintraffic / digitraffic.fi (CC BY 4.0)",
            }));
        }
    }
    let total = cameras.len();
    let prioritized = prioritize_sources(cameras, max_count, &FINLAND_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded Fintraffic camera sources: {total} live presets across {stations_seen} stations (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- DriveBC

/// `driveBcImageCredit`: keep partner attribution, HTML stripped.
pub fn drive_bc_image_credit(raw: Option<&Value>) -> String {
    static TAGS: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]*>").expect("regex"));
    static ATTRIBUTION: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)courtesy|provided by|presented in cooperation|city of|parks canada")
            .expect("regex")
    });
    let text = jsval::string_or(&[raw], "");
    let text = jsval::trim(&jsval::collapse_ws(&TAGS.replace_all(&text, ""))).to_string();
    if !text.is_empty() && ATTRIBUTION.is_match(&text) {
        text
    } else {
        String::new()
    }
}

fn drivebc_heading(orientation: &str) -> Option<f64> {
    Some(match orientation {
        "N" => 0.0,
        "NE" => 45.0,
        "E" => 90.0,
        "SE" => 135.0,
        "S" => 180.0,
        "SW" => 225.0,
        "W" => 270.0,
        "NW" => 315.0,
        _ => return None,
    })
}

/// `Number.isSafeInteger(v)` for a JSON value.
fn safe_integer(v: Option<&Value>) -> Option<f64> {
    let f = match v {
        Some(Value::Number(n)) => n.as_f64()?,
        _ => return None,
    };
    (f.is_finite() && f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_991.0).then_some(f)
}

/// `loadDriveBcSourcesFromOpenData`.
pub async fn load_drivebc(d: &LoaderDeps) -> LoadedPack {
    match d
        .load(
            catalog_request(DRIVEBC_WEBCAMS_URL, Redirect::Follow),
            ok_json,
        )
        .await
    {
        Ok(rows) => parse_drivebc(
            &rows,
            d.pack_cap(
                "CCTV_DRIVEBC_MAX_SOURCES",
                DEFAULT_DRIVEBC_MAX_SOURCES,
                1200.0,
            ),
        ),
        Err(error) => {
            warn_load("DriveBC camera", &error);
            LoadedPack::default()
        }
    }
}

fn parse_drivebc(rows: &Value, max_count: f64) -> LoadedPack {
    let Value::Array(rows) = rows else {
        return LoadedPack::default();
    };
    let mut cameras = Vec::new();
    for row in rows {
        let r = Some(row);
        if get(r, "is_on") != Some(&Value::Bool(true))
            || get(r, "should_appear") != Some(&Value::Bool(true))
        {
            continue;
        }
        let Some(id) = safe_integer(get(r, "id")).filter(|id| *id > 0.0) else {
            continue;
        };
        let coords = match get(get(r, "location"), "coordinates") {
            Some(Value::Array(c)) => c.clone(),
            _ => Vec::new(),
        };
        let lon = jsval::strict_number(coords.first());
        let lat = jsval::strict_number(coords.get(1));
        if !is_likely_bc_coordinate(lat, lon) {
            continue;
        }
        let id_text = jsval::number_string(id);
        let camera_id = format!("drivebc-{id_text}");
        let orientation =
            jsval::trim(&jsval::string_or(&[get(r, "orientation")], "")).to_uppercase();
        let heading = drivebc_heading(&orientation);
        let has_heading = heading.is_some();
        let region = jsval::trim(&jsval::string_or(&[get(r, "region_name")], "")).to_string();
        let image_url = drivebc_image_url(&id_text);
        let name = jsval::trim(&jsval::string_or(&[get(r, "name")], "")).to_string();
        let elevation = jsval::strict_number(get(r, "elevation"));
        let (pitch, fov, range, mount) = personality(has_heading);
        cameras.push(json!({
            "id": camera_id,
            "name": if name.is_empty() { format!("DriveBC camera {id_text}") } else { name },
            "city": if region == "Border Cams" {
                "BC Border".to_string()
            } else if region.is_empty() {
                "British Columbia".to_string()
            } else {
                region
            },
            "cityId": "british-columbia",
            "provider": "DriveBC",
            "lat": lat, "lon": lon,
            "headingDeg": heading.unwrap_or_else(|| fallback_heading_from_id(&camera_id)),
            "headingConfidence": if has_heading { "high" } else { "low" },
            "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
            "groundElevationM": if elevation.is_finite() { elevation.clamp(-100.0, 4000.0) } else { 0.0 },
            "feedType": "image",
            "url": image_url, "snapshotUrl": image_url,
            "sourceKind": "drivebc-open-data",
            "license": "DriveBC, Open Government Licence – British Columbia",
            "credit": drive_bc_image_credit(get(r, "credit")),
        }));
    }
    let total = cameras.len();
    let prioritized = prioritize_sources(cameras, max_count, &DRIVEBC_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded DriveBC camera sources: {total} published (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- TxDOT

/// `normalizeTxdotDistrictPayload`: one district catalog to cameras.
pub fn normalize_txdot_district_payload(payload: &Value, district: &str) -> Vec<Value> {
    let by_roadway = get(Some(payload), "roadwayCctvStatuses");
    let groups: Vec<&Value> = match by_roadway {
        Some(Value::Object(map)) => map.values().collect(),
        Some(Value::Array(items)) => items.iter().collect(),
        _ => return Vec::new(),
    };
    let code = district.to_uppercase();
    let ground = txdot_district_elevation_m(&code).unwrap_or(TXDOT_DEFAULT_ELEVATION_M);
    let mut cameras = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for rows in groups {
        let Value::Array(rows) = rows else {
            continue;
        };
        for row in rows {
            let r = Some(row);
            if jsval::string_or(&[get(r, "statusDescription")], "") != "Device Online" {
                continue;
            }
            if get(r, "hasSnapshot") == Some(&Value::Bool(false)) {
                continue;
            }
            let lat = jsval::strict_number(get(r, "latitude"));
            let lon = jsval::strict_number(get(r, "longitude"));
            if !is_likely_texas_coordinate(lat, lon) {
                continue;
            }
            let icd_id = jsval::trim(&jsval::string_or(&[get(r, "icd_Id")], "")).to_string();
            if icd_id.is_empty() || !seen.insert(icd_id.clone()) {
                continue;
            }
            let name = {
                let raw = get(r, "name");
                let text = if jsval::truthy(raw) {
                    jsval::string(raw)
                } else {
                    icd_id.clone()
                };
                jsval::trim(&text).to_string()
            };
            let heading = direction_to_heading(&name, false);
            let has_heading = heading.is_finite();
            let camera_id = format!(
                "txdot-{}-{}",
                code.to_lowercase(),
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(icd_id.as_bytes())
            );
            let mut snapshot = url::Url::parse(TXDOT_CCTV_SNAPSHOT_URL).expect("constant URL");
            snapshot
                .query_pairs_mut()
                .append_pair("icdId", &icd_id)
                .append_pair("districtCode", &code);
            let snapshot = snapshot.to_string();
            let (pitch, fov, range, _) = personality(has_heading);
            cameras.push(json!({
                "id": camera_id,
                "name": name,
                "city": jsval::string_or(&[get(get(r, "equipLoc"), "roadway")], &code),
                "cityId": format!("tx-{}", code.to_lowercase()),
                "provider": "TxDOT",
                "lat": lat, "lon": lon,
                "headingDeg": if has_heading { heading } else { fallback_heading_from_id(&camera_id) },
                "headingConfidence": if has_heading { "high" } else { "low" },
                "pitchDeg": pitch, "fovDeg": fov, "rangeM": range,
                "mountHeightM": if has_heading { 12 } else { 10 },
                "groundElevationM": ground,
                "feedType": "image",
                "url": snapshot, "snapshotUrl": snapshot,
                "sourceKind": "txdot-its",
                "license": "Public TxDOT traffic camera data",
                "code": camera_display_code(&icd_id.to_uppercase()),
            }));
        }
    }
    cameras
}

/// `loadTxdotSourcesFromOpenData`: one catalog per configured district.
pub async fn load_txdot(d: &LoaderDeps) -> LoadedPack {
    let raw = d
        .env("CCTV_TXDOT_DISTRICTS")
        .unwrap_or_else(|| DEFAULT_TXDOT_DISTRICTS.to_string());
    let mut districts: Vec<String> = Vec::new();
    for token in raw.split(',') {
        let code = jsval::trim(token).to_uppercase();
        if TXDOT_DISTRICTS.contains(&code.as_str()) && !districts.contains(&code) {
            districts.push(code);
        }
    }
    if districts.is_empty() {
        return LoadedPack::default();
    }
    let settled = futures_util::future::join_all(districts.iter().map(|district| {
        let request = catalog_request(&txdot_cctv_status_url(district), Redirect::Follow)
            .header("User-Agent", CCTV_USER_AGENT);
        async move {
            d.load(request, ok_json)
                .await
                .map(|payload| (district.clone(), payload))
        }
    }))
    .await;
    let mut cameras = Vec::new();
    for result in settled {
        match result {
            Ok((district, payload)) => {
                cameras.extend(normalize_txdot_district_payload(&payload, &district))
            }
            Err(error) => warn_load("TxDOT district", &error),
        }
    }
    let total = cameras.len();
    let max = d.pack_cap("CCTV_TXDOT_MAX_SOURCES", DEFAULT_TXDOT_MAX_SOURCES, 2000.0);
    let prioritized = prioritize_sources(cameras, max, &TXDOT_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded TxDOT camera sources: {total} online across {} (using nearest {})",
        districts.join(","),
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- catalog files

/// Read a curated catalog file: `None` when missing or unreadable.
fn read_catalog_file(
    d: &LoaderDeps,
    env_name: &str,
    default: &str,
    pack: &str,
) -> Option<Vec<Value>> {
    let resolved = d.resolve(&d.env_or(env_name, default));
    if !resolved.exists() {
        tracing::warn!("[CCTV] {pack} source file missing: {}", resolved.display());
        return None;
    }
    match std::fs::read(&resolved)
        .ok()
        .and_then(|bytes| jsval::parse_json(&bytes))
    {
        Some(Value::Array(rows)) => Some(rows),
        Some(_) => Some(Vec::new()),
        None => {
            tracing::warn!(
                "[CCTV] {pack} source file read error: {}",
                resolved.display()
            );
            None
        }
    }
}

/// `loadTallinnSourcesFromCatalog`: curated ristmikud stills.
pub fn load_tallinn(d: &LoaderDeps) -> LoadedPack {
    let Some(rows) = read_catalog_file(
        d,
        "CCTV_TALLINN_SOURCES_FILE",
        DEFAULT_TALLINN_SOURCE_FILE,
        "Tallinn",
    ) else {
        return LoadedPack::default();
    };
    let mut cameras = Vec::new();
    for item in &rows {
        if !item.is_object() {
            continue;
        }
        let i = Some(item);
        let camera_id = match get(i, "id") {
            Some(v @ (Value::String(_) | Value::Number(_))) => {
                jsval::trim(&jsval::string(Some(v))).to_string()
            }
            _ => String::new(),
        };
        if camera_id.is_empty() {
            continue;
        }
        let lat = jsval::strict_number(get(i, "lat"));
        let lon = jsval::strict_number(get(i, "lon"));
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        if !(59.2..=59.7).contains(&lat) || !(24.3..=25.4).contains(&lon) {
            continue;
        }
        let image_url = curated_url(item);
        if !image_url.starts_with(TALLINN_IMAGE_ORIGIN) {
            continue;
        }
        let extracted = jsval::finite_or(get(i, "headingDeg"), f64::NAN);
        let has_heading = extracted.is_finite();
        let heading = if has_heading {
            ((extracted % 360.0) + 360.0) % 360.0
        } else {
            fallback_heading_from_id(&camera_id)
        };
        let confidence = if has_heading
            && jsval::string_or(&[get(i, "headingConfidence")], "").to_lowercase() != "low"
        {
            "high"
        } else {
            "low"
        };
        let (pitch, fov, range, mount) = personality(confidence == "high");
        let mut camera = json!({
            "id": camera_id,
            "name": jsval::trim(&jsval::string_or(&[get(i, "name")], &camera_id)),
            "city": "Tallinn",
            "cityId": "tallinn",
            "provider": "City of Tallinn",
            "lat": lat, "lon": lon,
            "headingDeg": heading,
            "headingConfidence": confidence,
            "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
            "groundElevationM": jsval::finite_or(get(i, "groundElevationM"), 15.0),
            "feedType": "image",
            "url": image_url, "snapshotUrl": image_url,
            "sourceKind": "tallinn-ristmikud",
            "license": "Public City of Tallinn traffic camera data",
        });
        if has_heading {
            camera["poseSource"] = json!("curated");
        }
        cameras.push(camera);
    }
    let unique = dedupe_by_id(cameras);
    let total = unique.len();
    let max = d.pack_cap(
        "CCTV_TALLINN_MAX_SOURCES",
        DEFAULT_TALLINN_MAX_SOURCES,
        300.0,
    );
    let prioritized = prioritize_sources(unique, max, &[TALLINN_CENTER]);
    tracing::info!(
        "[CCTV] Loaded Tallinn camera sources: {total} (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

/// `typeof item.url === 'string' ? item.url.trim() : typeof item.snapshotUrl === 'string' ? ... : ''`.
fn curated_url(item: &Value) -> String {
    match (item.get("url"), item.get("snapshotUrl")) {
        (Some(Value::String(url)), _) => jsval::trim(url).to_string(),
        (_, Some(Value::String(url))) => jsval::trim(url).to_string(),
        _ => String::new(),
    }
}

/// `loadWarendorfSourcesFromCatalog`: the municipal webcams on their two hosts.
pub fn load_warendorf(d: &LoaderDeps) -> LoadedPack {
    let Some(rows) = read_catalog_file(
        d,
        "CCTV_WARENDORF_SOURCES_FILE",
        DEFAULT_WARENDORF_SOURCE_FILE,
        "Warendorf",
    ) else {
        return LoadedPack::default();
    };
    let mut cameras = Vec::new();
    for item in &rows {
        let Value::Object(map) = item else {
            continue;
        };
        let id = match map.get("id") {
            Some(Value::String(s)) => jsval::trim(s).to_string(),
            _ => String::new(),
        };
        let url = curated_url(item);
        if id.is_empty() || !WARENDORF_IMAGE_ORIGINS.iter().any(|o| url.starts_with(o)) {
            continue;
        }
        let lat = jsval::strict_number(map.get("lat"));
        let lon = jsval::strict_number(map.get("lon"));
        if !is_plausible_lat_lon(lat, lon) {
            continue;
        }
        let mut camera = map.clone();
        let city_id = jsval::string_or(&[map.get("cityId")], "warendorf");
        camera.insert("id".into(), json!(id));
        camera.insert("url".into(), json!(url));
        camera.insert("snapshotUrl".into(), json!(url));
        camera.insert("cityId".into(), json!(city_id));
        camera.insert("feedType".into(), json!("image"));
        camera.insert("sourceKind".into(), json!("municipal-webcam"));
        cameras.push(Value::Object(camera));
    }
    tracing::info!("[CCTV] Loaded Warendorf camera sources: {}", cameras.len());
    LoadedPack::plain(cameras)
}

// ---------------------------------------------------------------- Tarktee

/// `parseTarkteeDatexLocations`: location id to (name, lat, lon).
pub fn parse_tarktee_datex_locations(xml: &str) -> IndexMap<String, (String, f64, f64)> {
    static BLOCK: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"<predefinedLocation\s+id="([^"]+)"[^>]*>((?s:.)*?)</predefinedLocation>"#)
            .expect("regex")
    });
    static LAT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)<latitude>\s*(-?[0-9]+(?:\.[0-9]+)?)\s*</latitude>").expect("regex")
    });
    static LON: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)<longitude>\s*(-?[0-9]+(?:\.[0-9]+)?)\s*</longitude>").expect("regex")
    });
    static NAME: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)<value\b[^>]*>\s*([^<]+?)\s*</value>").expect("regex"));
    let mut out = IndexMap::new();
    for caps in BLOCK.captures_iter(xml) {
        let id = caps[1].to_string();
        let body = &caps[2];
        let (Some(lat), Some(lon)) = (LAT.captures(body), LON.captures(body)) else {
            continue;
        };
        let lat = crate::common::js::number(&lat[1]);
        let lon = crate::common::js::number(&lon[1]);
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        let name = NAME
            .captures(body)
            .map(|c| jsval::trim(&c[1]).to_string())
            .unwrap_or_else(|| id.clone());
        out.insert(id, (name, lat, lon));
    }
    out
}

/// `parseTarkteeDatexImages`: location id to official image URL.
pub fn parse_tarktee_datex_images(xml: &str) -> IndexMap<String, String> {
    static BLOCK: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"<trafficView\b[^>]*>((?s:.)*?)</trafficView>").expect("regex"));
    static REF: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)<linearPredefinedLocationReference\b[^>]*\bid="([^"]+)""#).expect("regex")
    });
    static URL: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)<urlLinkAddress>\s*([^<\s]+)\s*</urlLinkAddress>").expect("regex")
    });
    let mut out = IndexMap::new();
    for caps in BLOCK.captures_iter(xml) {
        let body = &caps[1];
        let (Some(reference), Some(url)) = (REF.captures(body), URL.captures(body)) else {
            continue;
        };
        let url = jsval::trim(&url[1]).to_string();
        if !url.starts_with(TARKTEE_IMAGE_ORIGIN) {
            continue;
        }
        out.insert(reference[1].to_string(), url);
    }
    out
}

/// `loadTarkteeSourcesFromDatex`: Estonian road-weather cameras.
pub async fn load_tarktee(d: &LoaderDeps) -> LoadedPack {
    let xml_request = |url: &str| {
        FetchRequest::new(url, Redirect::Follow, true)
            .header("Accept", "application/xml,text/xml,*/*")
    };
    let text = |response: FetchResponse| async move {
        if !response.ok() {
            return Err(LoadError::Status(response.status));
        }
        response.text().await.ok_or(LoadError::Body)
    };
    // Both requests go out together; a network failure of either fails the pack.
    let (loc, img) = futures_util::future::join(
        d.load(xml_request(TARKTEE_LOCATIONS_URL), text),
        d.load(xml_request(TARKTEE_IMAGES_URL), text),
    )
    .await;
    let (loc_xml, img_xml) = match (loc, img) {
        (Ok(l), Ok(i)) => (l, i),
        (Err(LoadError::Status(status)), _) => {
            tracing::warn!("[CCTV] Tarktee locations download failed: {status}");
            return LoadedPack::default();
        }
        (_, Err(LoadError::Status(status))) => {
            tracing::warn!("[CCTV] Tarktee images download failed: {status}");
            return LoadedPack::default();
        }
        (Err(error), _) | (_, Err(error)) => {
            warn_load("Tarktee DATEX", &error);
            return LoadedPack::default();
        }
    };
    let locations = parse_tarktee_datex_locations(&loc_xml);
    let images = parse_tarktee_datex_images(&img_xml);
    if locations.is_empty() || images.is_empty() {
        tracing::warn!(
            "[CCTV] Tarktee DATEX parse empty: locations {} images {}",
            locations.len(),
            images.len()
        );
        return LoadedPack::default();
    }
    static NUMBER: Lazy<Regex> = Lazy::new(|| Regex::new(r"/images/([0-9]+)/").expect("regex"));
    let mut cameras = Vec::new();
    for (location_id, (name, lat, lon)) in &locations {
        let Some(image_url) = images.get(location_id) else {
            continue;
        };
        if *lat < 57.4 || *lat > 59.9 || *lon < 21.5 || *lon > 28.4 {
            continue;
        }
        let camera_id = match NUMBER.captures(image_url) {
            Some(caps) => format!("ee-tarktee-{}", &caps[1]),
            None => format!("ee-tarktee-{location_id}"),
        };
        cameras.push(json!({
            "id": camera_id,
            "name": name,
            "city": name,
            "cityId": "estonia",
            "provider": "Transpordiamet (Tarktee)",
            "lat": lat, "lon": lon,
            "headingDeg": fallback_heading_from_id(&camera_id),
            "headingConfidence": "low",
            "pitchDeg": -18, "fovDeg": 44, "rangeM": 145, "mountHeightM": 8,
            "groundElevationM": 40,
            "feedType": "image",
            "url": image_url, "snapshotUrl": image_url,
            "sourceKind": "tarktee-datex",
            "license": "Public Transpordiamet / Tarktee road weather camera data",
        }));
    }
    let total = cameras.len();
    let max = d.pack_cap(
        "CCTV_TARKTEE_MAX_SOURCES",
        DEFAULT_TARKTEE_MAX_SOURCES,
        300.0,
    );
    let prioritized = prioritize_sources(cameras, max, &TARKTEE_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded Tarktee camera sources: {total} with images (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- NSW

/// `nswCameraLabel`: the `view` sentence when it is one, else the title.
pub fn nsw_camera_label(props: Option<&Value>) -> String {
    let view = jsval::trim(&jsval::string_or(&[get(props, "view")], "")).to_string();
    let title = jsval::trim(&jsval::string_or(&[get(props, "title")], "")).to_string();
    let is_label = !view.is_empty()
        && jsval::utf16_len(&view) <= NSW_MAX_VIEW_LABEL
        && !view.contains(['\r', '\n']);
    if is_label { view } else { title }
}

/// `nswCameraToSource`: one feature to one source.
pub fn nsw_camera_to_source(feature: &Value) -> Option<Value> {
    let f = Some(feature);
    let raw_id = jsval::trim(&jsval::string_or(&[get(f, "id")], "")).to_string();
    if raw_id.is_empty() {
        return None;
    }
    let coords = get(get(f, "geometry"), "coordinates");
    let lon = jsval::strict_number(jsval::index(coords, 0).as_ref());
    let lat = jsval::strict_number(jsval::index(coords, 1).as_ref());
    if !is_likely_nsw_coordinate(lat, lon) {
        return None;
    }
    let empty = json!({});
    let props_value = get(f, "properties");
    let props = if jsval::truthy(props_value) {
        props_value
    } else {
        Some(&empty)
    };
    let url = jsval::trim(&jsval::string_or(&[get(props, "href")], "")).to_string();
    if !url.starts_with(NSW_IMAGE_ORIGIN) {
        return None;
    }
    let direction = jsval::trim(&jsval::string_or(&[get(props, "direction")], ""))
        .to_uppercase()
        .replace('-', "");
    let heading = direction_to_heading(&direction, true);
    let has_heading = heading.is_finite();
    let camera_id = format!("nsw-{raw_id}");
    let label = nsw_camera_label(props);
    let title_upper = jsval::string_or(&[get(props, "title")], "").to_uppercase();
    let (pitch, fov, range, mount) = personality(has_heading);
    Some(json!({
        "id": camera_id,
        "name": if label.is_empty() { format!("NSW {raw_id}") } else { label },
        "city": jsval::string_or(&[get(props, "region")], "New South Wales").replace('_', " "),
        "cityId": "nsw",
        "provider": "Live Traffic NSW",
        "lat": lat, "lon": lon,
        "headingDeg": if has_heading { heading } else { fallback_heading_from_id(&camera_id) },
        "headingConfidence": if has_heading { "high" } else { "low" },
        "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
        "groundElevationM": 25,
        "feedType": "image",
        "url": url, "snapshotUrl": url,
        "sourceKind": "nsw-livetraffic",
        "license": "Live Traffic NSW — Transport for NSW, CC BY 4.0",
        "code": camera_display_code(if title_upper.is_empty() { &raw_id } else { &title_upper }),
    }))
}

/// `loadNswSourcesFromOpenData`.
pub async fn load_nsw(d: &LoaderDeps) -> LoadedPack {
    let request =
        catalog_request(NSW_CAMERAS_URL, Redirect::Follow).header("User-Agent", CCTV_USER_AGENT);
    match d.load(request, ok_json).await {
        Ok(body) => {
            let cameras: Vec<Value> = match get(Some(&body), "features") {
                Some(Value::Array(features)) => {
                    features.iter().filter_map(nsw_camera_to_source).collect()
                }
                _ => Vec::new(),
            };
            let total = cameras.len();
            let max = d.pack_cap("CCTV_NSW_MAX_SOURCES", DEFAULT_NSW_MAX_SOURCES, 900.0);
            let prioritized = prioritize_sources(cameras, max, &[SYDNEY_CENTER]);
            tracing::info!(
                "[CCTV] Loaded NSW camera sources: {total} (using nearest {})",
                prioritized.items.len()
            );
            prioritized
        }
        Err(error) => {
            warn_load("NSW camera", &error);
            LoadedPack::default()
        }
    }
}

// ---------------------------------------------------------------- Calgary

/// `normalizeCalgaryImageUrl`: upgrade to HTTPS and pin to the city host.
pub fn normalize_calgary_image_url(raw: Option<&Value>) -> Option<String> {
    let text = match raw {
        None | Some(Value::Null) => String::new(),
        Some(v) => jsval::string(Some(v)),
    };
    let text = jsval::trim(&text);
    if text.is_empty() {
        return None;
    }
    let mut parsed = url::Url::parse(text).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    parsed.set_scheme("https").ok()?;
    let upgraded = parsed.to_string();
    upgraded
        .starts_with(CALGARY_IMAGE_ORIGIN)
        .then_some(upgraded)
}

/// `calgaryCameraId`: `calgary-<n>` from `loc<n>.jpg`, else a path slug.
pub fn calgary_camera_id(image_url: &str) -> Option<String> {
    static NUMBERED: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)loc([0-9]+)\.jpg$").expect("regex"));
    static EDGES: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^/+|\.[a-z0-9]+$").expect("regex"));
    static NON_ALNUM: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^a-zA-Z0-9]+").expect("regex"));
    let text = jsval::trim(image_url);
    if text.is_empty() {
        return None;
    }
    let path = url::Url::parse(text).ok()?.path().to_string();
    if let Some(caps) = NUMBERED.captures(&path) {
        return Some(format!("calgary-{}", &caps[1]));
    }
    let slug = NON_ALNUM
        .replace_all(&EDGES.replace_all(&path, ""), "-")
        .to_lowercase();
    (!slug.is_empty()).then(|| format!("calgary-{slug}"))
}

/// `calgaryCameraName`.
pub fn calgary_camera_name(record: &Value, camera_id: &str) -> String {
    let text = |v: Option<&Value>| match v {
        None | Some(Value::Null) => String::new(),
        Some(v) => jsval::trim(&jsval::string(Some(v))).to_string(),
    };
    let r = Some(record);
    let location = text(get(r, "camera_location"));
    if !location.is_empty() {
        return location;
    }
    let described = text(get(get(r, "camera_url"), "description"));
    if !described.is_empty() {
        return described;
    }
    format!(
        "Calgary Camera {}",
        camera_id.strip_prefix("calgary-").unwrap_or(camera_id)
    )
}

/// `calgaryCameraToSource`. No heading is derived from the record: the
/// quadrant and the address suffix are Calgary's street grid, not a facing.
pub fn calgary_camera_to_source(record: &Value) -> Option<Value> {
    if !record.is_object() && !record.is_array() {
        return None;
    }
    let r = Some(record);
    let Some(Value::Array(coordinates)) = get(get(r, "point"), "coordinates") else {
        return None;
    };
    if coordinates.len() < 2 {
        return None;
    }
    let lon = jsval::finite_or(coordinates.first(), f64::NAN);
    let lat = jsval::finite_or(coordinates.get(1), f64::NAN);
    if !is_likely_calgary_coordinate(lat, lon) {
        return None;
    }
    let image_url = normalize_calgary_image_url(get(get(r, "camera_url"), "url"))?;
    let camera_id = calgary_camera_id(&image_url)?;
    let name = calgary_camera_name(record, &camera_id);
    Some(json!({
        "id": camera_id,
        "name": name,
        "city": "Calgary",
        "cityId": "calgary",
        "provider": "The City of Calgary",
        "lat": lat, "lon": lon,
        "headingDeg": fallback_heading_from_id(&camera_id),
        "headingConfidence": "low",
        "pitchDeg": -18, "fovDeg": 44, "rangeM": 145, "mountHeightM": 8,
        "groundElevationM": 1045,
        "feedType": "image",
        "url": image_url, "snapshotUrl": image_url,
        "sourceKind": "calgary-open-data",
        "license": "Contains information licensed under the Open Government Licence – City of Calgary",
        "code": camera_display_code(&name.to_uppercase()),
    }))
}

/// `loadCalgarySourcesFromOpenData`: refuses redirects and oversized bodies.
pub async fn load_calgary(d: &LoaderDeps) -> LoadedPack {
    let endpoint = d.env_or("CCTV_CALGARY_ROWS_URL", DEFAULT_CALGARY_ROWS_URL);
    let result = d
        .load(
            catalog_request(&endpoint, Redirect::Manual),
            |response| async move {
                if (300..400).contains(&response.status) {
                    return Err(LoadError::Redirected);
                }
                if !response.ok() {
                    return Err(LoadError::Status(response.status));
                }
                response
                    .json_capped(CALGARY_MAX_CATALOG_BYTES)
                    .await
                    .ok_or(LoadError::Body)
            },
        )
        .await;
    let rows = match result {
        Ok(Value::Array(rows)) => rows,
        Ok(_) => return LoadedPack::default(),
        Err(error) => {
            warn_load("Calgary camera", &error);
            return LoadedPack::default();
        }
    };
    let mut cameras = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for record in &rows {
        let Some(camera) = calgary_camera_to_source(record) else {
            continue;
        };
        let id = camera["id"].as_str().unwrap_or_default().to_string();
        if seen.insert(id) {
            cameras.push(camera);
        }
    }
    let total = cameras.len();
    let max = d.pack_cap(
        "CCTV_CALGARY_MAX_SOURCES",
        DEFAULT_CALGARY_MAX_SOURCES,
        400.0,
    );
    let prioritized = prioritize_sources(cameras, max, &[CALGARY_DOWNTOWN]);
    tracing::info!(
        "[CCTV] Loaded Calgary camera sources: {total} (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

// ---------------------------------------------------------------- DelDOT

/// `loadDelDOTSourcesFromOpenData`: HTTPS HLS cameras, redirects refused.
pub async fn load_deldot(d: &LoaderDeps) -> LoadedPack {
    let result = d
        .load(
            catalog_request(DELDOT_CCTV_URL, Redirect::Manual),
            |response| async move {
                if (300..400).contains(&response.status) {
                    return Err(LoadError::Redirected);
                }
                if !response.ok() {
                    return Err(LoadError::Status(response.status));
                }
                response
                    .json_capped(DELDOT_MAX_CATALOG_BYTES)
                    .await
                    .ok_or(LoadError::Body)
            },
        )
        .await;
    match result {
        Ok(payload) => parse_deldot(
            &payload,
            d.pack_cap("CCTV_DELDOT_MAX_SOURCES", DEFAULT_DELDOT_MAX_SOURCES, 400.0),
        ),
        Err(error) => {
            warn_load("DelDOT source", &error);
            LoadedPack::default()
        }
    }
}

fn parse_deldot(payload: &Value, max_count: f64) -> LoadedPack {
    static PATH: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^/live/[A-Za-z0-9_.-]+/playlist\.m3u8$").expect("regex"));
    static ID: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z0-9_-]{1,80}$").expect("regex"));
    let rows = match get(Some(payload), "videoCameras") {
        Some(Value::Array(rows)) => rows.clone(),
        _ => Vec::new(),
    };
    if rows.is_empty() {
        return LoadedPack::default();
    }
    let mut cameras = Vec::new();
    for row in &rows {
        let r = Some(row);
        if jsval::string(get(r, "status")).to_lowercase() != "active" {
            continue;
        }
        let lat = jsval::finite_or(get(r, "lat"), f64::NAN);
        let lon = jsval::finite_or(get(r, "lon"), f64::NAN);
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        if !(38.4..=39.9).contains(&lat) || !(-75.85..=-75.0).contains(&lon) {
            continue;
        }
        let Ok(stream) = url::Url::parse(&jsval::string_or(&[get(get(r, "urls"), "m3u8s")], ""))
        else {
            continue;
        };
        if stream.origin().ascii_serialization() != "https://video.deldot.gov"
            || !stream.username().is_empty()
            || stream.password().is_some()
            || !PATH.is_match(stream.path())
        {
            continue;
        }
        let stream_url = stream.to_string();
        let id = jsval::trim(&jsval::string_or(&[get(r, "id")], "")).to_string();
        if !ID.is_match(&id) {
            continue;
        }
        let camera_id = format!("deldot-{}", id.to_lowercase());
        let title = jsval::trim(&jsval::string_or(&[get(r, "title")], "")).to_string();
        let heading = direction_to_heading(&title, false);
        let has_heading = heading.is_finite();
        let county = get(r, "county");
        let city = if jsval::truthy(county) {
            format!("{} County", jsval::string(county))
        } else {
            "Delaware".into()
        };
        static SPACES: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").expect("regex"));
        let city_id = format!(
            "deldot-{}",
            SPACES.replace_all(&jsval::string_or(&[county], "de").to_lowercase(), "-")
        );
        let (pitch, fov, range, mount) = personality(has_heading);
        cameras.push(json!({
            "id": camera_id,
            "name": if title.is_empty() { format!("DelDOT {id}") } else { title },
            "city": city,
            "cityId": city_id,
            "provider": "DelDOT",
            "lat": lat, "lon": lon,
            "headingDeg": if has_heading { heading } else { fallback_heading_from_id(&camera_id) },
            "headingConfidence": if has_heading { "high" } else { "low" },
            "pitchDeg": pitch, "fovDeg": fov, "rangeM": range, "mountHeightM": mount,
            "groundElevationM": 10,
            "feedType": "hls",
            "url": stream_url,
            "snapshotUrl": "",
            "sourceKind": "deldot-open-data",
            "license": "Public DelDOT traffic camera",
        }));
    }
    let unique = dedupe_by_id(cameras);
    let total = unique.len();
    let prioritized = prioritize_sources(unique, max_count, &DELDOT_ANCHORS);
    tracing::info!(
        "[CCTV] Loaded DelDOT camera sources: {total} Active (using nearest {})",
        prioritized.items.len()
    );
    prioritized
}

/// Keep `num` reachable for callers building raw items by hand.
#[allow(dead_code)]
pub fn finite(x: f64) -> Value {
    num(x)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use futures_util::FutureExt;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    pub fn env_map(pairs: &[(&str, &str)]) -> EnvFn {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(move |k| map.get(k).cloned())
    }

    pub fn deps(env: EnvFn, fetch: Fetcher) -> LoaderDeps {
        LoaderDeps {
            env,
            fetch,
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."),
        }
    }

    pub fn json_fetch<F>(respond: F) -> (Fetcher, Arc<Mutex<Vec<FetchRequest>>>)
    where
        F: Fn(&FetchRequest) -> FetchResponse + Send + Sync + 'static,
    {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let fetch: Fetcher = Arc::new(move |request: FetchRequest| {
            let response = respond(&request);
            log.lock().push(request);
            async move { Ok(response) }.boxed()
        });
        (fetch, seen)
    }

    pub fn json_response(value: Value) -> FetchResponse {
        FetchResponse::new(
            200,
            &[("content-type", "application/json")],
            value.to_string(),
        )
    }

    fn ids(pack: &LoadedPack) -> Vec<String> {
        pack.items
            .iter()
            .map(|c| c["id"].as_str().unwrap().to_string())
            .collect()
    }

    // ---- TxDOT

    fn txdot_row(overrides: Value) -> Value {
        let mut row = json!({
            "icd_Id": "FM-734 @ US-290 EB", "name": "FM-734 @ US-290 EB",
            "latitude": 30.34396, "longitude": -97.57988,
            "statusDescription": "Device Online", "hasSnapshot": true,
            "dirDescription": "North", "equipLoc": { "roadway": "FM-734", "direction": "North" }
        });
        row.as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        row
    }

    fn district(rows: Vec<Value>) -> Value {
        json!({ "roadwayCctvStatuses": { "FM-734": rows } })
    }

    #[test]
    fn txdot_keeps_only_online_cameras_with_numeric_coordinates() {
        let cameras = normalize_txdot_district_payload(
            &district(vec![
                txdot_row(json!({"icd_Id": "a", "name": "a"})),
                txdot_row(
                    json!({"icd_Id": "b", "name": "b", "statusDescription": "Device Offline"}),
                ),
                txdot_row(json!({"icd_Id": "c", "name": "c", "statusDescription": "Device Error"})),
                txdot_row(json!({"icd_Id": "d", "name": "d", "latitude": null})),
                txdot_row(json!({"icd_Id": "e", "name": "e", "longitude": "not-a-number"})),
                txdot_row(json!({"icd_Id": "f", "name": "f", "latitude": 0, "longitude": 0})),
                txdot_row(
                    json!({"icd_Id": "g", "name": "g", "latitude": "30.1", "longitude": "-97.7"}),
                ),
                txdot_row(json!({"icd_Id": "h", "name": "h", "hasSnapshot": false})),
            ]),
            "AUS",
        );
        assert_eq!(cameras.len(), 1);
        let c = &cameras[0];
        assert_eq!(c["name"], "a");
        assert_eq!(c["provider"], "TxDOT");
        assert_eq!(c["sourceKind"], "txdot-its");
        assert_eq!(
            c["url"],
            "https://its.txdot.gov/its/DistrictIts/GetCctvSnapshotByIcdId?icdId=a&districtCode=AUS"
        );
        assert_eq!(c["id"], "txdot-aus-YQ");
    }

    #[test]
    fn txdot_ids_are_distinct_recoverable_and_district_scoped() {
        let cameras = normalize_txdot_district_payload(
            &district(vec![
                txdot_row(json!({"icd_Id": "costarring", "name": "costarring"})),
                txdot_row(json!({"icd_Id": "liquid", "name": "liquid"})),
                txdot_row(json!({"icd_Id": "abcDEfaBCDEFAbcdEFABCdEfAbcdeFaB", "name": "x"})),
                txdot_row(json!({"icd_Id": "ABcDeFABCDefaBCDeFAbCdeFabcdefAB", "name": "y"})),
            ]),
            "AUS",
        );
        let ids: std::collections::HashSet<_> =
            cameras.iter().map(|c| c["id"].as_str().unwrap()).collect();
        assert_eq!(ids.len(), 4);
        let first = cameras[0]["id"]
            .as_str()
            .unwrap()
            .strip_prefix("txdot-aus-")
            .unwrap();
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(first)
            .unwrap();
        assert_eq!(decoded, b"costarring");
        let id_for = |d: &str| {
            normalize_txdot_district_payload(&district(vec![txdot_row(json!({}))]), d)[0]["id"]
                .clone()
        };
        assert_eq!(id_for("AUS"), id_for("AUS"));
        assert!(id_for("HOU").as_str().unwrap().starts_with("txdot-hou-"));
    }

    #[test]
    fn txdot_dedupes_headings_elevation_and_malformed_payloads() {
        let shared = json!({ "roadwayCctvStatuses": {
            "IH-35": [txdot_row(json!({"icd_Id": "shared"}))],
            "SH-71": [txdot_row(json!({"icd_Id": "shared"}))],
        }});
        assert_eq!(normalize_txdot_district_payload(&shared, "AUS").len(), 1);
        let cameras = normalize_txdot_district_payload(
            &district(vec![
                txdot_row(json!({"icd_Id": "eb", "name": "FM-734 @ US-290 EB"})),
                txdot_row(json!({"icd_Id": "wb", "name": "FM-734 @ US-290 WB"})),
                txdot_row(json!({"icd_Id": "plain", "name": "FM-734 @ Bellingham Dr"})),
                txdot_row(json!({"icd_Id": "lamar", "name": "N Lamar Blvd @ Rundberg Ln"})),
                txdot_row(json!({"icd_Id": "west", "name": "West Ave @ 6th St"})),
            ]),
            "AUS",
        );
        assert_eq!(cameras[0]["headingDeg"], 90.0);
        assert_eq!(cameras[0]["headingConfidence"], "high");
        assert_eq!(cameras[1]["headingDeg"], 270.0);
        for c in &cameras[2..] {
            assert_eq!(c["headingConfidence"], "low");
            assert!(c["headingDeg"].as_f64().unwrap().is_finite());
        }
        let at = |d: &str| {
            normalize_txdot_district_payload(&district(vec![txdot_row(json!({}))]), d)[0]["groundElevationM"].as_f64()
        };
        assert_eq!(at("HOU"), Some(15.0));
        assert_eq!(at("ELP"), Some(1140.0));
        assert_eq!(at("AUS"), Some(149.0));
        assert_eq!(at("SAT"), Some(198.0));
        for bad in [
            Value::Null,
            json!({}),
            json!({"roadwayCctvStatuses": []}),
            json!({"roadwayCctvStatuses": {"IH-35": null}}),
        ] {
            assert!(normalize_txdot_district_payload(&bad, "AUS").is_empty());
        }
    }

    #[tokio::test]
    async fn txdot_loader_fetches_each_configured_district_once() {
        let (fetch, seen) = json_fetch(|request| {
            let url = url::Url::parse(&request.url).unwrap();
            let code = url
                .query_pairs()
                .find(|(k, _)| k == "districtCode")
                .unwrap()
                .1
                .to_string();
            json_response(district(vec![txdot_row(
                json!({"icd_Id": format!("{code}-1"), "name": format!("{code}-1")}),
            )]))
        });
        let d = deps(
            env_map(&[("CCTV_TXDOT_DISTRICTS", "aus, SAT, NOPE, aus")]),
            fetch,
        );
        let pack = load_txdot(&d).await;
        assert_eq!(
            seen.lock()
                .iter()
                .map(|r| r.url.clone())
                .collect::<Vec<_>>(),
            [txdot_cctv_status_url("AUS"), txdot_cctv_status_url("SAT")]
        );
        let mut cities: Vec<String> = pack
            .items
            .iter()
            .map(|c| c["cityId"].as_str().unwrap().to_string())
            .collect();
        cities.sort();
        assert_eq!(cities, ["tx-aus", "tx-sat"]);
    }

    // ---- NSW

    fn nsw_feature(overrides: Value, props: Value) -> Value {
        let mut properties = json!({
            "region": "SYD_SOUTH", "title": "5 Ways (Miranda)",
            "view": "5 Ways at The Boulevarde looking west towards Sutherland.",
            "direction": "W",
            "href": "https://webcams.transport.nsw.gov.au/livetraffic-webcams/cameras/5_ways_miranda.jpeg"
        });
        properties
            .as_object_mut()
            .unwrap()
            .extend(props.as_object().unwrap().clone());
        let mut feature = json!({
            "type": "Feature", "id": "5_ways_miranda",
            "geometry": { "type": "Point", "coordinates": [151.1036, -34.0337] },
            "properties": properties
        });
        feature
            .as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        feature
    }

    #[test]
    fn nsw_label_and_mapping() {
        assert_eq!(
            nsw_camera_label(Some(
                &json!({"title": "5 Ways (Miranda)", "view": "5 Ways looking west."})
            )),
            "5 Ways looking west."
        );
        assert_eq!(
            nsw_camera_label(Some(
                &json!({"title": "Victoria Pass", "view": "x".repeat(200)})
            )),
            "Victoria Pass"
        );
        assert_eq!(
            nsw_camera_label(Some(&json!({"title": "T", "view": "line one\nline two"}))),
            "T"
        );
        assert_eq!(nsw_camera_label(Some(&json!({}))), "");

        let source =
            nsw_camera_to_source(&nsw_feature(json!({}), json!({"direction": "N-E"}))).unwrap();
        assert_eq!(source["id"], "nsw-5_ways_miranda");
        assert_eq!(source["city"], "SYD SOUTH");
        assert_eq!(source["lat"], -34.0337);
        assert_eq!(source["headingDeg"], 45.0);
        assert_eq!(source["headingConfidence"], "high");
        assert!(source["license"].as_str().unwrap().contains("CC BY 4.0"));
        assert_eq!(source["url"], source["snapshotUrl"]);

        assert!(
            nsw_camera_to_source(&nsw_feature(
                json!({}),
                json!({"href": "https://evil.example/x.jpeg"})
            ))
            .is_none()
        );
        assert!(nsw_camera_to_source(&nsw_feature(json!({"geometry": null}), json!({}))).is_none());
        assert!(nsw_camera_to_source(&nsw_feature(json!({"id": ""}), json!({}))).is_none());
        let no_bearing =
            nsw_camera_to_source(&nsw_feature(json!({}), json!({"direction": ""}))).unwrap();
        assert_eq!(no_bearing["headingConfidence"], "low");
    }

    #[tokio::test]
    async fn nsw_loader_reads_the_feed() {
        let (fetch, seen) = json_fetch(|_| {
            json_response(json!({ "type": "FeatureCollection", "features": [
                nsw_feature(json!({}), json!({})),
                nsw_feature(json!({"id": "other"}), json!({"href": "https://evil.example/x.jpeg"})),
            ]}))
        });
        let pack = load_nsw(&deps(env_map(&[]), fetch)).await;
        assert_eq!(seen.lock()[0].url, NSW_CAMERAS_URL);
        assert_eq!(ids(&pack), ["nsw-5_ways_miranda"]);
    }

    // ---- Calgary

    fn calgary_row(overrides: Value) -> Value {
        let mut row = json!({
            "camera_url": { "url": "http://trafficcam.calgary.ca/loc142.jpg", "description": "Camera 143" },
            "quadrant": "SW",
            "camera_location": "Bow Trail / 37 Street SW",
            "point": { "type": "Point", "coordinates": [-114.1413616, 51.0453097] }
        });
        row.as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        row
    }

    #[test]
    fn calgary_rows_map_to_pinned_https_sources() {
        let source = calgary_camera_to_source(&calgary_row(json!({}))).unwrap();
        assert_eq!(source["id"], "calgary-142");
        assert_eq!(source["name"], "Bow Trail / 37 Street SW");
        assert_eq!(source["lat"], 51.0453097);
        assert_eq!(source["url"], "https://trafficcam.calgary.ca/loc142.jpg");
        assert_eq!(source["url"], source["snapshotUrl"]);
        assert_eq!(source["code"], "BOW TRAIL / 37 STREET SW");
        let base_heading = source["headingDeg"].clone();
        for quadrant in ["NE", "NW", "SE", "SW", "NW/NE", "S"] {
            let s = calgary_camera_to_source(&calgary_row(json!({"quadrant": quadrant}))).unwrap();
            assert_eq!(s["headingConfidence"], "low");
            assert_eq!(s["headingDeg"], base_heading);
        }
        for bad in [
            json!({"point": {"type": "Point", "coordinates": [-79.3832, 43.6532]}}),
            json!({"point": {"type": "Point", "coordinates": [0, 0]}}),
            json!({"point": null}),
            json!({"point": {"coordinates": [-114.14]}}),
            json!({"camera_url": {"url": "https://evil.example/loc1.jpg"}}),
            json!({"camera_url": {"url": "https://trafficcam.calgary.ca.evil.test/1.jpg"}}),
            json!({"camera_url": {"url": "file:///etc/passwd"}}),
            json!({"camera_url": null}),
        ] {
            assert!(
                calgary_camera_to_source(&calgary_row(bad.clone())).is_none(),
                "{bad}"
            );
        }
        assert!(calgary_camera_to_source(&Value::Null).is_none());
        let long = calgary_camera_to_source(&calgary_row(json!({
            "camera_location": "Bow Trail & Old Banff Coach Rd SW / Strathcona Blvd SW"
        })))
        .unwrap();
        let code = long["code"].as_str().unwrap();
        assert_eq!(
            jsval::utf16_len(code),
            super::super::normalize::CAMERA_CODE_MAX_CHARS
        );
        assert!(code.ends_with('…') && code.starts_with("BOW TRAIL & OLD BANFF"));
    }

    #[test]
    fn calgary_urls_ids_and_names() {
        let s = |v: &str| json!(v);
        assert_eq!(
            normalize_calgary_image_url(Some(&s("http://trafficcam.calgary.ca/loc86.jpg")))
                .as_deref(),
            Some("https://trafficcam.calgary.ca/loc86.jpg")
        );
        assert_eq!(
            normalize_calgary_image_url(Some(&s("https://trafficcam.calgary.ca/loc86.jpg")))
                .as_deref(),
            Some("https://trafficcam.calgary.ca/loc86.jpg")
        );
        assert_eq!(normalize_calgary_image_url(Some(&s(""))), None);
        assert_eq!(normalize_calgary_image_url(Some(&s("not a url"))), None);
        assert_eq!(
            calgary_camera_id("https://trafficcam.calgary.ca/loc86.jpg").as_deref(),
            Some("calgary-86")
        );
        assert_eq!(
            calgary_camera_id("https://trafficcam.calgary.ca/cams/deerfoot_16av.jpg").as_deref(),
            Some("calgary-cams-deerfoot-16av")
        );
        assert_eq!(calgary_camera_id(""), None);
        assert_eq!(
            calgary_camera_name(
                &json!({"camera_location": "  9 Avenue / 3 Street SE "}),
                "calgary-1"
            ),
            "9 Avenue / 3 Street SE"
        );
        assert_eq!(
            calgary_camera_name(
                &json!({"camera_url": {"description": "Camera 12"}}),
                "calgary-11"
            ),
            "Camera 12"
        );
        assert_eq!(
            calgary_camera_name(&json!({}), "calgary-11"),
            "Calgary Camera 11"
        );
    }

    #[tokio::test]
    async fn calgary_loader_collapses_duplicates_and_refuses_redirects_and_oversize() {
        let (fetch, seen) = json_fetch(|_| {
            json_response(json!([
                calgary_row(json!({})),
                calgary_row(json!({"camera_location": "Bow Trail / 37 Street SW (duplicate)"})),
                calgary_row(json!({
                    "camera_url": {"url": "http://trafficcam.calgary.ca/loc86.jpg"},
                    "camera_location": "Stoney Trail / Deerfoot Trail SE",
                    "point": {"type": "Point", "coordinates": [-113.9766063, 50.9007257]}
                })),
                calgary_row(json!({"camera_url": {"url": "https://evil.example/x.jpg"}})),
            ]))
        });
        let pack = load_calgary(&deps(env_map(&[]), fetch)).await;
        assert_eq!(seen.lock()[0].url, DEFAULT_CALGARY_ROWS_URL);
        assert_eq!(seen.lock()[0].redirect, Redirect::Manual);
        assert_eq!(ids(&pack), ["calgary-142", "calgary-86"]);
        assert_eq!(pack.items[0]["name"], "Bow Trail / 37 Street SW");

        for response in [
            || FetchResponse::new(302, &[("location", "https://evil.example/rows.json")], "["),
            || FetchResponse::new(503, &[], "["),
        ] {
            let (fetch, _) = json_fetch(move |_| response());
            assert!(
                load_calgary(&deps(env_map(&[]), fetch))
                    .await
                    .items
                    .is_empty()
            );
        }
        let declared = (CALGARY_MAX_CATALOG_BYTES + 1).to_string();
        let (fetch, _) = json_fetch(move |_| {
            FetchResponse::new(
                200,
                &[("content-length", &declared)],
                json!([calgary_row(json!({}))]).to_string(),
            )
        });
        assert!(
            load_calgary(&deps(env_map(&[]), fetch))
                .await
                .items
                .is_empty()
        );
        let (fetch, _) = json_fetch(|_| {
            FetchResponse::new(200, &[], "x".repeat(CALGARY_MAX_CATALOG_BYTES + 1024))
        });
        assert!(
            load_calgary(&deps(env_map(&[]), fetch))
                .await
                .items
                .is_empty()
        );
        let failing: Fetcher = Arc::new(|_| async { Err("network down".to_string()) }.boxed());
        assert!(
            load_calgary(&deps(env_map(&[]), failing))
                .await
                .items
                .is_empty()
        );
    }

    // ---- DriveBC

    fn drivebc_row(id: Value, lat: Value, lon: Value, overrides: Value) -> Value {
        let mut row = json!({
            "id": id, "name": format!("Camera {}", jsval::string(Some(&id))),
            "is_on": true, "should_appear": true, "region_name": "Lower Mainland",
            "orientation": "N", "elevation": 12,
            "location": { "type": "Point", "coordinates": [lon, lat] },
        });
        row.as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        row
    }

    #[tokio::test]
    async fn drivebc_keeps_published_cameras_and_builds_frame_urls() {
        let (fetch, seen) = json_fetch(|_| {
            json_response(json!([
                drivebc_row(
                    json!(13),
                    json!(49.001268),
                    json!(-122.756658),
                    json!({
                        "name": "Douglas (Peace Arch) Border Crossing", "region_name": "Border Cams", "elevation": 10,
                        "links": {"imageDisplay": "https://example.com/13.jpg"}
                    })
                ),
                drivebc_row(
                    json!(682),
                    json!(49.0579),
                    json!(-123.0569),
                    json!({
                        "orientation": "se", "elevation": null,
                        "credit": "Images courtesy of <a href=\"https://www.translink.ca/\" target=\"_blank\">TransLink</a>"
                    })
                ),
                drivebc_row(
                    json!(1),
                    json!(49.2),
                    json!(-123.1),
                    json!({"is_on": false})
                ),
                drivebc_row(
                    json!(2),
                    json!(49.2),
                    json!(-123.1),
                    json!({"should_appear": false})
                ),
                drivebc_row(json!("3"), json!(49.2), json!(-123.1), json!({})),
                drivebc_row(json!(4), Value::Null, json!(-123.1), json!({})),
                drivebc_row(
                    json!(5),
                    json!(49.2),
                    json!(-123.1),
                    json!({"location": null})
                ),
                drivebc_row(json!(6), json!(1000), json!(-123.1), json!({})),
                drivebc_row(json!(7), json!(34.05), json!(-118.24), json!({})),
            ]))
        });
        let pack = load_drivebc(&deps(env_map(&[]), fetch)).await;
        assert_eq!(seen.lock()[0].url, DRIVEBC_WEBCAMS_URL);
        assert_eq!(ids(&pack), ["drivebc-682", "drivebc-13"]);
        let border = &pack.items[1];
        let deltaport = &pack.items[0];
        assert_eq!(border["url"], "https://www.drivebc.ca/images/13.jpg");
        assert_eq!(border["city"], "BC Border");
        assert_eq!(border["headingDeg"], 0.0);
        assert_eq!(border["groundElevationM"], 10.0);
        assert_eq!(border["credit"], "");
        assert_eq!(deltaport["credit"], "Images courtesy of TransLink");
        assert_eq!(deltaport["city"], "Lower Mainland");
        assert_eq!(deltaport["headingDeg"], 135.0);
        assert_eq!(deltaport["groundElevationM"], 0.0);
    }

    #[tokio::test]
    async fn drivebc_caps_by_distance_and_fails_soft() {
        let mut rows = Vec::new();
        for (i, lat) in [58.8, 58.81, 58.82, 58.83].iter().enumerate() {
            rows.push(drivebc_row(
                json!(100 + i),
                json!(lat),
                json!(-122.7),
                json!({}),
            ));
        }
        for (i, lat) in [49.28, 49.27, 49.26, 49.25].iter().enumerate() {
            rows.push(drivebc_row(
                json!(200 + i),
                json!(lat),
                json!(-123.12),
                json!({}),
            ));
        }
        for (i, lat) in [48.43, 48.44, 48.45, 48.46].iter().enumerate() {
            rows.push(drivebc_row(
                json!(300 + i),
                json!(lat),
                json!(-123.37),
                json!({}),
            ));
        }
        let body = Value::Array(rows);
        let (fetch, _) = json_fetch(move |_| json_response(body.clone()));
        let pack = load_drivebc(&deps(env_map(&[("CCTV_DRIVEBC_MAX_SOURCES", "8")]), fetch)).await;
        assert_eq!(pack.items.len(), 8);
        assert!(ids(&pack).iter().all(|id| !id.starts_with("drivebc-10")));

        let (fetch, _) = json_fetch(|_| FetchResponse::new(503, &[], "unavailable"));
        assert!(
            load_drivebc(&deps(env_map(&[]), fetch))
                .await
                .items
                .is_empty()
        );
        let (fetch, _) = json_fetch(|_| json_response(json!({"webcams": []})));
        assert!(
            load_drivebc(&deps(env_map(&[]), fetch))
                .await
                .items
                .is_empty()
        );
        let failing: Fetcher = Arc::new(|_| async { Err("fetch failed".to_string()) }.boxed());
        assert!(
            load_drivebc(&deps(env_map(&[]), failing))
                .await
                .items
                .is_empty()
        );
    }

    #[test]
    fn drivebc_credit_keeps_partner_attribution_only() {
        let c = |s: &str| drive_bc_image_credit(Some(&json!(s)));
        assert_eq!(
            c("Images courtesy of <a href=\"https://x.test/\">TransLink</a>"),
            "Images courtesy of TransLink"
        );
        assert_eq!(
            c("Camera image provided by City of Vancouver"),
            "Camera image provided by City of Vancouver"
        );
        assert_eq!(
            c("BC HighwayCam presented in cooperation with AtkinsRéalis."),
            "BC HighwayCam presented in cooperation with AtkinsRéalis."
        );
        assert_eq!(
            c(
                "This camera is located in a remote area and relies on solar power. As a result, transmission delays may occur."
            ),
            ""
        );
        assert_eq!(drive_bc_image_credit(Some(&Value::Null)), "");
    }

    #[test]
    fn default_pack_caps_fit_the_catalog_cap() {
        let packs = DEFAULT_AUSTIN_MAX_SOURCES
            + DEFAULT_CALTRANS_MAX_SOURCES
            + DEFAULT_TFL_MAX_SOURCES
            + DEFAULT_ONTARIO_MAX_SOURCES
            + DEFAULT_FINTRAFFIC_MAX_SOURCES
            + DEFAULT_DRIVEBC_MAX_SOURCES
            + DEFAULT_TXDOT_MAX_SOURCES
            + DEFAULT_TALLINN_MAX_SOURCES
            + DEFAULT_TARKTEE_MAX_SOURCES
            + DEFAULT_NSW_MAX_SOURCES
            + 1.0;
        let (cap, ceiling) = (DEFAULT_CCTV_MAX_SOURCES, CCTV_MAX_SOURCES_CEILING);
        assert!(packs <= cap);
        assert!(cap <= ceiling);
        assert_eq!(cap, 4000.0);
    }

    // ---- Fintraffic

    fn station(id: &str, status: &str, lon: f64, lat: f64, altitude: f64, presets: Value) -> Value {
        json!({
            "type": "Feature", "id": id,
            "geometry": { "type": "Point", "coordinates": [lon, lat, altitude] },
            "properties": { "id": id, "name": format!("vt4_{id}"), "collectionStatus": status, "presets": presets }
        })
    }

    async fn load_fintraffic_with(
        features: Vec<Value>,
        env: &[(&str, &str)],
    ) -> (LoadedPack, FetchRequest) {
        let body = json!({ "type": "FeatureCollection", "features": features });
        let (fetch, seen) = json_fetch(move |_| json_response(body.clone()));
        let pack = load_fintraffic(&deps(env_map(env), fetch)).await;
        let request = seen.lock()[0].clone();
        (pack, request)
    }

    #[tokio::test]
    async fn fintraffic_turns_presets_into_cameras() {
        let mut s = station(
            "C01503",
            "GATHERING",
            23.99616,
            60.05374,
            0.0,
            json!([{"id": "C0150301", "inCollection": true}, {"id": "C0150302", "inCollection": true}]),
        );
        s["properties"]["name"] = json!("kt51_Inkoo");
        let (pack, request) = load_fintraffic_with(vec![s], &[]).await;
        assert_eq!(pack.items.len(), 2);
        assert_eq!(pack.items[0]["id"], "fi-c0150301");
        assert_eq!(pack.items[0]["name"], "kt51 Inkoo (view 01)");
        assert_eq!(pack.items[1]["name"], "kt51 Inkoo (view 02)");
        assert_eq!(
            pack.items[0]["url"],
            format!("{FINTRAFFIC_IMAGE_ORIGIN}C0150301.jpg")
        );
        assert_ne!(pack.items[0]["headingDeg"], pack.items[1]["headingDeg"]);
        assert_eq!(request.url, FINTRAFFIC_STATIONS_URL);
        assert_eq!(request.header_value("Digitraffic-User"), Some("camera-eye"));
        assert_eq!(request.header_value("Accept-Encoding"), Some("gzip"));
        assert_eq!(request.redirect, Redirect::Manual);
    }

    #[tokio::test]
    async fn fintraffic_drops_dead_stations_and_bad_ids() {
        let (pack, _) = load_fintraffic_with(
            vec![
                station(
                    "C01601",
                    "REMOVED_TEMPORARILY",
                    24.9384,
                    60.1699,
                    0.0,
                    json!([{"id": "C0160101", "inCollection": true}]),
                ),
                station(
                    "C01503",
                    "GATHERING",
                    24.9384,
                    60.1699,
                    0.0,
                    json!([
                        {"id": "C0150301", "inCollection": true},
                        {"id": "C0150302", "inCollection": false},
                        {"id": "C0150303"},
                        {"id": "../../etc/passwd", "inCollection": true},
                        {"id": "C9999901", "inCollection": true},
                    ]),
                ),
                station(
                    "C01504",
                    "GATHERING",
                    60.1699,
                    24.9384,
                    0.0,
                    json!([{"id": "C0150401", "inCollection": true}]),
                ),
            ],
            &[],
        )
        .await;
        assert_eq!(ids(&pack), ["fi-c0150301"]);
    }

    #[tokio::test]
    async fn fintraffic_elevation_cap_and_failures() {
        let (pack, _) = load_fintraffic_with(
            vec![
                station(
                    "C08508",
                    "GATHERING",
                    24.9384,
                    60.1699,
                    136.0,
                    json!([{"id": "C0850801", "inCollection": true}]),
                ),
                station(
                    "C01503",
                    "GATHERING",
                    24.9384,
                    60.1699,
                    0.0,
                    json!([{"id": "C0150301", "inCollection": true}]),
                ),
                station(
                    "C03506",
                    "GATHERING",
                    24.9384,
                    60.1699,
                    99_000.0,
                    json!([{"id": "C0350601", "inCollection": true}]),
                ),
            ],
            &[],
        )
        .await;
        let by_id: HashMap<String, f64> = pack
            .items
            .iter()
            .map(|c| {
                (
                    c["id"].as_str().unwrap().to_string(),
                    c["groundElevationM"].as_f64().unwrap(),
                )
            })
            .collect();
        assert_eq!(by_id["fi-c0850801"], 136.0);
        assert_eq!(by_id["fi-c0150301"], FINTRAFFIC_GROUND_ELEVATION_M);
        assert_eq!(by_id["fi-c0350601"], 1400.0);

        let presets: Vec<Value> = (0..40)
            .map(|i| json!({"id": format!("C01503{i:02}"), "inCollection": true}))
            .collect();
        let (capped, _) = load_fintraffic_with(
            vec![station(
                "C01503",
                "GATHERING",
                24.9384,
                60.1699,
                0.0,
                Value::Array(presets),
            )],
            &[("CCTV_FINTRAFFIC_MAX_SOURCES", "8")],
        )
        .await;
        assert_eq!(capped.items.len(), 8);

        let (fetch, _) = json_fetch(|_| {
            FetchResponse::new(302, &[("location", "https://example.com/stations")], "")
        });
        assert!(
            load_fintraffic(&deps(env_map(&[]), fetch))
                .await
                .items
                .is_empty()
        );
        let (fetch, _) = json_fetch(|_| FetchResponse::new(503, &[], "nope"));
        assert!(
            load_fintraffic(&deps(env_map(&[]), fetch))
                .await
                .items
                .is_empty()
        );
        let failing: Fetcher = Arc::new(|_| async { Err("network down".to_string()) }.boxed());
        assert!(
            load_fintraffic(&deps(env_map(&[]), failing))
                .await
                .items
                .is_empty()
        );
    }

    // ---- Estonia and Warendorf

    const LOCATIONS_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<d2LogicalModel xmlns="http://datex2.eu/schema/2/2_0">
  <payloadPublication>
    <predefinedLocationContainer id="TRAFFIC_CAMERAS" version="0">
      <predefinedLocation id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" version="0">
        <predefinedLocationName>
          <values><value lang="et">Test Site</value></values>
        </predefinedLocationName>
        <location>
          <pointByCoordinates>
            <pointCoordinates>
              <latitude>59.4370</latitude>
              <longitude>24.7530</longitude>
            </pointCoordinates>
          </pointByCoordinates>
        </location>
      </predefinedLocation>
    </predefinedLocationContainer>
  </payloadPublication>
</d2LogicalModel>"#;

    const IMAGES_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<d2LogicalModel xmlns="http://datex2.eu/schema/2/2_0">
  <payloadPublication>
    <trafficView id="1">
      <linearTrafficView id="1">
        <linearPredefinedLocationReference targetClass="PredefinedLocation" id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" version="0"/>
        <trafficViewRecord id="1-1">
          <urlLink>
            <urlLinkAddress>https://tarktee.transpordiamet.ee/images/42/42_202608251542.jpg</urlLinkAddress>
            <urlLinkType>image</urlLinkType>
          </urlLink>
        </trafficViewRecord>
      </linearTrafficView>
    </trafficView>
  </payloadPublication>
</d2LogicalModel>"#;

    #[test]
    fn tarktee_parsers_join_locations_to_images() {
        let locations = parse_tarktee_datex_locations(LOCATIONS_XML);
        let images = parse_tarktee_datex_images(IMAGES_XML);
        let id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[id], ("Test Site".to_string(), 59.437, 24.753));
        assert_eq!(
            images[id],
            "https://tarktee.transpordiamet.ee/images/42/42_202608251542.jpg"
        );
        let off_host = IMAGES_XML.replace(
            "https://tarktee.transpordiamet.ee/images/42/42_202608251542.jpg",
            "https://evil.example/images/42/42.jpg",
        );
        assert!(parse_tarktee_datex_images(&off_host).is_empty());
    }

    #[tokio::test]
    async fn tarktee_loader_builds_cameras() {
        let (fetch, _) = json_fetch(|request| {
            let body = if request.url == TARKTEE_LOCATIONS_URL {
                LOCATIONS_XML
            } else {
                IMAGES_XML
            };
            FetchResponse::new(200, &[], body)
        });
        let pack = load_tarktee(&deps(env_map(&[]), fetch)).await;
        assert_eq!(ids(&pack), ["ee-tarktee-42"]);
        assert_eq!(pack.items[0]["name"], "Test Site");
    }

    #[test]
    fn tallinn_catalog_loads_curated_stills_only() {
        let (fetch, _) = json_fetch(|_| unreachable!());
        let pack = load_tallinn(&deps(env_map(&[]), fetch));
        assert!(pack.items.len() >= 200, "{}", pack.items.len());
        for camera in pack.items.iter().take(25) {
            assert!(camera["id"].as_str().unwrap().starts_with("tln-"));
            assert_eq!(camera["cityId"], "tallinn");
            assert_eq!(camera["sourceKind"], "tallinn-ristmikud");
            assert!(
                camera["url"]
                    .as_str()
                    .unwrap()
                    .starts_with("https://ristmikud.tallinn.ee/")
            );
            assert_eq!(camera["url"], camera["snapshotUrl"]);
        }
    }

    #[test]
    fn warendorf_catalog_registers_official_hosts_only() {
        let (fetch, _) = json_fetch(|_| unreachable!());
        let pack = load_warendorf(&deps(env_map(&[]), fetch.clone()));
        assert_eq!(ids(&pack), ["warendorf-marktplatz-rathaus"]);
        let camera = &pack.items[0];
        assert_eq!(camera["snapshotUrl"], camera["url"]);
        assert_eq!(camera["sourceKind"], "municipal-webcam");
        assert_eq!(camera["poseSource"], "curated");
        assert!(
            camera["license"]
                .as_str()
                .unwrap()
                .starts_with("Public municipal webcam data")
        );

        let missing = LoaderDeps {
            root: PathBuf::from("/nonexistent"),
            ..deps(env_map(&[]), fetch.clone())
        };
        assert!(load_warendorf(&missing).items.is_empty());

        let dir = std::env::temp_dir().join(format!("ce-warendorf-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("config")).unwrap();
        std::fs::write(
            dir.join("config/cctv_sources.warendorf.json"),
            json!([
                {"id": {"toString": null}, "url": "https://www.kreis-warendorf.de/a.jpg", "lat": 51.9, "lon": 7.9},
                {"id": "ok", "url": "https://www.kreis-warendorf.de/a.jpg", "lat": 51.9, "lon": 7.9},
                {"id": "text-coords", "url": "https://www.kreis-warendorf.de/a.jpg", "lat": "51.9", "lon": "7.9"},
                {"id": "null-island", "url": "https://www.kreis-warendorf.de/a.jpg", "lat": 0, "lon": 0},
                {"id": "off-host", "url": "https://evil.example/a.jpg", "lat": 51.9, "lon": 7.9},
            ])
            .to_string(),
        )
        .unwrap();
        let custom = LoaderDeps {
            root: dir.clone(),
            ..deps(env_map(&[]), fetch)
        };
        assert_eq!(ids(&load_warendorf(&custom)), ["ok"]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
