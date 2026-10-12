//! License plate reader cameras from the OpenStreetMap hourly extract
//! tiles (`createAlprTileSource` in `src/layers/alpr/source.js`, with
//! `tileRecords.js` and `records.js`).

use std::sync::Mutex;

use indexmap::IndexMap;
use serde_json::{Value, json};

use super::fetch::Api;
use super::mvt::{self, Geometry};
use super::vector_tiles::{
    BBox, TemplateSource, VectorTileSource, tiles_for_bounds, valid_tile_bounds,
};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

const MAX_VIEWPORT_DEGREES: f64 = 3.0;
const QUERY_SNAP_DEGREES: f64 = 0.05;
const DETAIL_MIN_ZOOM: u32 = 9;
const DETAIL_MAX_ZOOM: u32 = 12;
const DETAIL_MAX_TILES: usize = 16;
const SOURCE_RECORD_LIMIT: usize = 6000;
const PRECISION_CACHE_LIMIT: usize = 20_000;

pub const LABEL: &str = "OpenStreetMap · community mapped";

/// The source's `attribution`.
pub fn attribution() -> Value {
    json!({
        "name": "OpenStreetMap",
        "description": "© OpenStreetMap contributors, ODbL",
        "text": "© OpenStreetMap contributors, ODbL",
        "href": "https://www.openstreetmap.org/copyright",
    })
}

/// One camera record.
#[derive(Debug, Clone, PartialEq)]
pub struct Camera {
    pub id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub operator: Option<String>,
    pub manufacturer: Option<String>,
    pub camera_type: Option<String>,
    pub zone: Option<String>,
    pub direction_deg: Option<f64>,
    pub last_verified: Option<String>,
}

/// `textTag(value)`.
fn text_tag(value: Option<&Value>) -> Option<String> {
    let text = match value {
        None | Some(Value::Null) => String::new(),
        Some(value) => js::to_js_string(value),
    };
    let trimmed = js::trim(&text);
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// `numTag(value)`.
fn num_tag(value: Option<&Value>) -> Option<f64> {
    let value = value.filter(|v| !v.is_null())?;
    let text = js::to_js_string(value);
    let trimmed = js::trim(&text);
    if trimmed.is_empty() {
        return None;
    }
    Some(js::str_to_number(trimmed)).filter(|n| n.is_finite())
}

/// `normalizeDirection(value)`.
fn normalize_direction(value: Option<&Value>) -> Option<f64> {
    num_tag(value)
        .filter(|d| (0.0..=360.0).contains(d))
        .map(|d| d % 360.0)
}

/// `normalizeAlprTileFeature(feature)`.
pub fn normalize_tile_feature(feature: &mvt::Feature) -> Option<Camera> {
    let p = &feature.properties;
    let Geometry::Point([lon, lat]) = feature.geometry else {
        return None;
    };
    if p.get("osmType").and_then(Value::as_str) != Some("node") {
        return None;
    }
    let id = p.get("osmId").map_or(f64::NAN, js::to_number);
    let safe = id.is_finite() && id.fract() == 0.0 && id.abs() <= 9_007_199_254_740_991.0;
    if !safe
        || id <= 0.0
        || !lat.is_finite()
        || lat.abs() > 90.0
        || !lon.is_finite()
        || lon.abs() > 180.0
    {
        return None;
    }
    Some(Camera {
        id: format!("alpr:{}", js::fmt_num(id)),
        latitude: lat,
        longitude: lon,
        operator: text_tag(p.get("operator")),
        manufacturer: text_tag(p.get("brand")),
        camera_type: text_tag(p.get("cameraType")),
        zone: text_tag(p.get("surveillanceZone")),
        direction_deg: normalize_direction(p.get("direction")),
        last_verified: text_tag(p.get("check_date")),
    })
}

/// `decodeAlprTile(bytes, z, x, y)`.
pub fn decode_tile(bytes: &[u8], z: u32, x: u32, y: u32) -> Result<Vec<Camera>, Failure> {
    let malformed = |e: mvt::DecodeError| Failure::other(e.0);
    let layers = mvt::read_tile(bytes).map_err(malformed)?;
    let Some(layer) = layers.get("cameras") else {
        return Ok(Vec::new());
    };
    if layer.len() > 40_000 {
        return Err(Failure::other("Camera tile feature limit exceeded"));
    }
    let mut records = Vec::new();
    for i in 0..layer.len() {
        let feature = layer
            .feature(i)
            .and_then(|f| f.to_geojson(x, y, z))
            .map_err(malformed)?;
        if let Some(record) = normalize_tile_feature(&feature) {
            records.push(record);
        }
    }
    Ok(records)
}

/// `alprDetailZoom(box)`.
pub fn detail_zoom(b: &BBox) -> Option<u32> {
    (DETAIL_MIN_ZOOM..=DETAIL_MAX_ZOOM)
        .rev()
        .find(|zoom| tiles_for_bounds(b, *zoom, DETAIL_MAX_TILES + 1).len() <= DETAIL_MAX_TILES)
}

struct Known {
    zoom: u32,
    latitude: f64,
    longitude: f64,
}

/// `createAlprPrecisionCache`.
#[derive(Default)]
struct Precision {
    known: IndexMap<String, Known>,
}

impl Precision {
    fn apply(&mut self, record: Camera, zoom: u32) -> Camera {
        if let Some(finer) = self.known.get(&record.id)
            && finer.zoom > zoom
        {
            return Camera {
                latitude: finer.latitude,
                longitude: finer.longitude,
                ..record
            };
        }
        self.known.shift_remove(&record.id);
        self.known.insert(
            record.id.clone(),
            Known {
                zoom,
                latitude: record.latitude,
                longitude: record.longitude,
            },
        );
        while self.known.len() > PRECISION_CACHE_LIMIT {
            self.known.shift_remove_index(0);
        }
        record
    }
}

/// What `fetch(box)` returns.
pub struct AlprResult {
    pub records: Vec<Camera>,
    pub stale: bool,
    pub saturated: bool,
    pub no_coverage: bool,
    pub zoom_in: bool,
}

/// The US and Canadian extract sources.
pub struct AlprSource {
    sources: [VectorTileSource<Camera>; 2],
    precision: Mutex<Precision>,
}

impl AlprSource {
    pub fn new(api: &Api) -> Self {
        let source = |country: &str| {
            VectorTileSource::new(
                api.clone(),
                TemplateSource::TileJson(format!(
                    "https://tiles.dontgetflocked.com/cameras-{country}-hourly.json"
                )),
                "https://tiles.dontgetflocked.com",
                decode_tile,
                DETAIL_MAX_TILES,
                60.0 * 60.0 * 1000.0,
            )
        };
        Self {
            sources: [source("us"), source("ca")],
            precision: Mutex::new(Precision::default()),
        }
    }

    /// `fetch(box)`.
    pub async fn fetch(&self, b: &BBox) -> Result<AlprResult, Failure> {
        let limit = MAX_VIEWPORT_DEGREES + 2.0 * QUERY_SNAP_DEGREES + 1e-9;
        if !valid_tile_bounds(b) || b.north - b.south > limit || b.east - b.west > limit {
            return Err(Failure::Type(
                "ALPR requires a bounded city viewport".into(),
            ));
        }
        let empty = |no_coverage, zoom_in| AlprResult {
            records: Vec::new(),
            stale: false,
            saturated: false,
            no_coverage,
            zoom_in,
        };
        if b.east < -180.0 || b.west > -50.0 || b.north < 17.0 || b.south > 84.0 {
            return Ok(empty(true, false));
        }
        let Some(zoom) = detail_zoom(b) else {
            return Ok(empty(false, true));
        };
        let mut records: IndexMap<String, Camera> = IndexMap::new();
        let mut covered = false;
        let mut partial = false;
        for (index, source) in self.sources.iter().enumerate() {
            if index == 1 && (b.north < 41.0 || b.south > 84.0 || b.east < -142.0 || b.west > -52.0)
            {
                continue;
            }
            let metadata = source.metadata().await?;
            let bounds: Vec<Option<f64>> = (0..4)
                .map(|i| {
                    metadata
                        .get("bounds")
                        .and_then(|v| v.get(i))
                        .and_then(Value::as_f64)
                })
                .collect();
            let [Some(west), Some(south), Some(east), Some(north)] = bounds[..] else {
                return Err(Failure::other("Camera coverage unavailable"));
            };
            if b.east < west || b.west > east || b.north < south || b.south > north {
                continue;
            }
            covered = true;
            let result = source.fetch_bounds(b, zoom, None).await?;
            partial |= result.partial;
            let mut precision = self.precision.lock().expect("precision cache");
            for tile in &result.tiles {
                for record in tile.iter() {
                    let record = precision.apply(record.clone(), zoom);
                    match records.get_mut(&record.id) {
                        Some(slot) => *slot = record,
                        None => {
                            records.insert(record.id.clone(), record);
                        }
                    }
                }
            }
        }
        let mut list: Vec<Camera> = records.into_values().collect();
        let truncated = list.len() > SOURCE_RECORD_LIMIT;
        if truncated {
            let lat = (b.south + b.north) / 2.0;
            let lon = (b.west + b.east) / 2.0;
            let scale = (lat * std::f64::consts::PI / 180.0).cos();
            let mut ranked: Vec<(f64, Camera)> = list
                .into_iter()
                .map(|record| {
                    let d = (record.latitude - lat).powi(2)
                        + ((record.longitude - lon) * scale).powi(2);
                    (d, record)
                })
                .collect();
            js::sort_by(&mut ranked, |a, b| a.0 - b.0);
            list = ranked
                .into_iter()
                .take(SOURCE_RECORD_LIMIT)
                .map(|(_, r)| r)
                .collect();
        }
        Ok(AlprResult {
            records: list,
            stale: false,
            saturated: partial || truncated,
            no_coverage: !covered,
            zoom_in: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::services::mvt::tests::{tile, zigzag};

    #[test]
    fn decodes_camera_points() {
        let bytes = tile(
            "cameras",
            &["osmType", "osmId", "operator", "direction", "brand"],
            &[
                json!("node"),
                json!(42),
                json!(" City PD "),
                json!("90"),
                json!("way"),
            ],
            &[
                (
                    1,
                    vec![9, zigzag(2048), zigzag(2048)],
                    vec![0, 0, 1, 1, 2, 2, 3, 3],
                ),
                (1, vec![9, zigzag(10), zigzag(10)], vec![0, 4, 1, 1]),
            ],
        );
        let records = decode_tile(&bytes, 0, 0, 0).unwrap();
        assert_eq!(records.len(), 1);
        let camera = &records[0];
        assert_eq!(camera.id, "alpr:42");
        assert_eq!(camera.operator.as_deref(), Some("City PD"));
        assert_eq!(camera.direction_deg, Some(90.0));
        assert_eq!(camera.manufacturer, None);
    }

    #[test]
    fn precision_keeps_finer_positions() {
        let mut cache = Precision::default();
        let camera = |lat| Camera {
            id: "alpr:1".into(),
            latitude: lat,
            longitude: 0.0,
            operator: None,
            manufacturer: None,
            camera_type: None,
            zone: None,
            direction_deg: None,
            last_verified: None,
        };
        cache.apply(camera(1.0), 12);
        assert_eq!(cache.apply(camera(1.1), 9).latitude, 1.0);
        assert_eq!(cache.apply(camera(1.2), 12).latitude, 1.2);
    }

    #[test]
    fn detail_zoom_fits_sixteen_tiles() {
        let city = BBox {
            west: -122.52,
            south: 37.7,
            east: -122.35,
            north: 37.83,
        };
        assert!(detail_zoom(&city).is_some());
        let wide = BBox {
            west: -123.0,
            south: 36.0,
            east: -120.0,
            north: 39.0,
        };
        assert_eq!(detail_zoom(&wide), None);
    }
}
