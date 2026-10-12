//! The services Core's tools read (`src/tools/services.js`), backed by a
//! running Camera Eye server's `/api` routes through [`fetch::Api`].

pub mod alpr;
pub mod cctv;
pub mod environment;
pub mod features;
pub mod fetch;
pub mod imagery;
pub mod infrastructure;
pub mod installations;
pub mod live;
pub mod mvt;
pub mod places;
pub mod satellites;
pub mod traffic;
pub mod vector_tiles;
pub mod weather_maps;

use std::path::Path;

use serde_json::Value;

use self::fetch::{Api, Init};
use self::places::GeocodePlaces;
use crate::mcp::catalog::Failure;

/// Everything the tools read.
pub struct Services {
    pub api: Api,
    /// The address the app is served from (`services.app.baseUrl`).
    pub app_base_url: Option<String>,
    /// The panel's request key (`services.app.panelKey`).
    pub panel_key: Option<String>,
    pub places: GeocodePlaces,
    pub alpr: alpr::AlprSource,
    pub traffic: traffic::TrafficSource,
    pub infrastructure: infrastructure::InfrastructureSource,
    pub installations: installations::InstallationSource,
    pub features: features::FeatureSource,
    /// The panel's held responses and requests in flight.
    pub panel: crate::mcp::tools::panel_request::PanelState,
    /// Event packs read so far, by id.
    pub events: std::sync::Mutex<std::collections::HashMap<String, Value>>,
}

/// Service names `createToolServices` supplies; every Core tool's
/// `requires` is among them.
pub const SERVICE_NAMES: [&str; 31] = [
    "app",
    "earthquakes",
    "fires",
    "launches",
    "aircraft",
    "military",
    "vessels",
    "satellites",
    "cctv",
    "radio",
    "placeSearch",
    "routing",
    "bikeshare",
    "transit",
    "traffic",
    "weather",
    "weatherMaps",
    "wind",
    "imagery",
    "cables",
    "alpr",
    "infrastructure",
    "events",
    "regional",
    "terrain",
    "summary",
    "features",
    "cyclones",
    "perimeters",
    "installations",
    "places",
];

impl Services {
    /// `createLocalToolServices({ apiBase, panelKey })`.
    pub fn new(api_base: &str, root: &Path, panel_key: Option<String>) -> Result<Self, String> {
        let api = Api::new(api_base, root)?;
        Ok(Self {
            places: GeocodePlaces::new(api.clone()),
            alpr: alpr::AlprSource::new(&api),
            traffic: traffic::TrafficSource::new(&api),
            infrastructure: infrastructure::InfrastructureSource::default(),
            events: Default::default(),
            panel: Default::default(),
            installations: installations::InstallationSource::new(&api),
            features: features::FeatureSource::default(),
            api,
            app_base_url: Some(api_base.to_string()),
            panel_key,
        })
    }

    /// Whether a service exists (`services[key] != null`).
    pub fn has(&self, key: &str) -> bool {
        SERVICE_NAMES.contains(&key)
    }
}

/// `String(value)` for a property that may be undefined.
pub fn template(value: Option<&Value>) -> String {
    value.map_or_else(|| "undefined".to_string(), crate::mcp::js::to_js_string)
}

/// `fetch(path, { cache: 'no-store' })`: the request headers that cache
/// mode adds.
pub fn no_store() -> Init {
    Init::default()
        .header("pragma", "no-cache")
        .header("cache-control", "no-cache")
}

/// `earthquakes.getSnapshot()` (`src/layers/earthquakes/source.js`).
pub async fn earthquake_snapshot(api: &Api) -> Result<Vec<Value>, Failure> {
    let response = api
        .get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
        .await?;
    if !response.ok() {
        return Err(Failure::other(format!("USGS HTTP {}", response.status)));
    }
    let payload = response.json().await?;
    normalize_earthquake_snapshot(&payload).ok_or_else(|| Failure::other("Malformed USGS response"))
}

/// `normalizeEarthquakeSnapshot` (`src/layers/earthquakes/records.js`).
pub fn normalize_earthquake_snapshot(geojson: &Value) -> Option<Vec<Value>> {
    use crate::mcp::js;
    use serde_json::{Map, json};
    let features = geojson.get("features")?.as_array()?;
    let mut rows = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for (index, feature) in features.iter().enumerate() {
        let geometry = feature.get("geometry");
        let coordinates = geometry
            .and_then(|g| g.get("coordinates"))
            .and_then(Value::as_array)?;
        let properties = feature.get("properties").and_then(Value::as_object)?;
        if coordinates.len() < 2 {
            return None;
        }
        if let Some(kind) = geometry
            .and_then(|g| g.get("type"))
            .filter(|t| !t.is_null())
            && kind != "Point"
        {
            return None;
        }
        let lon = js::finite(&coordinates[0]).filter(|v| v.abs() <= 180.0)?;
        let lat = js::finite(&coordinates[1]).filter(|v| v.abs() <= 90.0)?;
        let depth = coordinates.get(2).filter(|v| !v.is_null());
        if depth.is_some_and(|d| js::finite(d).is_none()) {
            return None;
        }
        let mag = properties.get("mag").filter(|v| !v.is_null());
        if let Some(mag) = mag
            && js::finite(mag).is_none_or(|m| m > 10.0)
        {
            return None;
        }
        let Some(mag) = mag.and_then(js::finite).filter(|m| *m >= 2.5) else {
            continue;
        };
        let id = feature.get("id").filter(|v| !v.is_null());
        let stable_id = match id {
            None => format!("event-{}", index + 1),
            Some(Value::String(s)) if s.is_empty() => format!("event-{}", index + 1),
            Some(id) => js::to_js_string(id),
        };
        if !ids.insert(stable_id.clone()) {
            return None;
        }
        let mut row = Map::new();
        row.insert("stableId".into(), json!(stable_id));
        row.insert("usgsId".into(), id.cloned().unwrap_or(Value::Null));
        row.insert("lon".into(), js::num(lon));
        row.insert("lat".into(), js::num(lat));
        row.insert("depthKm".into(), depth.cloned().unwrap_or(Value::Null));
        row.insert("mag".into(), js::num(mag));
        row.insert(
            "place".into(),
            properties
                .get("place")
                .filter(|p| p.is_string())
                .cloned()
                .unwrap_or(Value::Null),
        );
        row.insert(
            "time".into(),
            properties
                .get("time")
                .and_then(js::finite)
                .map_or(Value::Null, js::num),
        );
        rows.push(Value::Object(row));
    }
    Some(rows)
}

/// `fires.getSnapshot()` (`src/layers/firms/source.js`): the payload, or
/// None when the server has no FIRMS key.
pub async fn fire_snapshot(api: &Api) -> Result<Option<Value>, Failure> {
    let response = api.fetch("/api/firms", no_store()).await?;
    let ok = response.ok();
    let status = response.status;
    let payload = response.json().await.ok();
    if !ok {
        if status == 503
            && payload.as_ref().and_then(|p| p.get("error")) == Some(&Value::from("no_key"))
        {
            return Ok(None);
        }
        return Err(Failure::other(format!("FIRMS HTTP {status}")));
    }
    match payload {
        Some(payload) if payload.get("fires").is_some_and(Value::is_array) => Ok(Some(payload)),
        _ => Err(Failure::other("Malformed fire snapshot")),
    }
}

/// `launches.getLaunchSnapshot()` (`src/layers/launches/source.js`).
pub async fn launch_snapshot(api: &Api) -> Result<(Value, bool), Failure> {
    let response = api.get("/api/launches").await?;
    if !response.ok() {
        return Err(Failure::other(format!("HTTP {}", response.status)));
    }
    let stale = response.header("x-ce-cache").as_deref() == Some("STALE-ERROR");
    let payload = response.json().await?;
    if !payload.is_array() && !payload.get("results").is_some_and(Value::is_array) {
        return Err(Failure::other("Malformed launch snapshot"));
    }
    Ok((payload, stale))
}
