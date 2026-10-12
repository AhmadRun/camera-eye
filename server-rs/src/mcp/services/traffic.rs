//! TomTom traffic flow (`src/layers/traffic/source.js` `getStatus`,
//! `flowSource.js`, `flowDecode.js` and `isDrivableFlowSegment` from
//! `roadModes.js`).

use std::time::Duration;

use serde_json::Value;

use super::fetch::{Api, Init};
use super::mvt::{self, Coord, Geometry};
use super::vector_tiles::{BBox, TemplateSource, VectorTileSource, clip_tile_line, tile_to_bbox};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

const FLOW_LAYER_NAME: &str = "Traffic flow";
const FLOW_ROAD_TYPES: [&str; 8] = [
    "Motorway",
    "International road",
    "Major road",
    "Secondary road",
    "Connecting road",
    "Major local road",
    "Local road",
    "Minor local road",
];
const DRIVABLE_CATEGORIES: [&str; 6] = [
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
    "street",
];

/// One decoded flow line.
#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub coords: Vec<Coord>,
    pub traffic_level: f64,
    pub road_type: String,
    /// `props.road_category ?? null`.
    pub road_category: Value,
    pub closure: bool,
}

/// `isDrivableFlowSegment(segment)`.
fn is_drivable(segment: &Segment) -> bool {
    FLOW_ROAD_TYPES.contains(&segment.road_type.as_str())
        && (!js::truthy(&segment.road_category)
            || segment
                .road_category
                .as_str()
                .is_some_and(|c| DRIVABLE_CATEGORIES.contains(&c)))
}

fn is_true(value: Option<&Value>) -> bool {
    matches!(value, Some(Value::Bool(true))) || value.and_then(Value::as_str) == Some("true")
}

/// `decodeFlowTile(bytes, z, x, y, { strict: true })`.
pub fn decode_flow_tile(bytes: &[u8], z: u32, x: u32, y: u32) -> Result<Vec<Segment>, Failure> {
    let layers = mvt::read_tile(bytes).map_err(|_| Failure::other("Malformed TomTom flow tile"))?;
    let Some(layer) = layers.get(FLOW_LAYER_NAME) else {
        return Ok(Vec::new());
    };
    let mut segments = Vec::new();
    for i in 0..layer.len() {
        let Ok(feature) = layer.feature(i).and_then(|f| f.to_geojson(x, y, z)) else {
            continue;
        };
        let props = &feature.properties;
        let closure = is_true(props.get("road_closure"));
        let level = props
            .get("traffic_level")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite());
        if level.is_none() && !closure {
            continue;
        }
        let traffic_level = level.map_or(0.0, |l| js::math_max([0.0, js::math_min([1.0, l])]));
        let road_type = props
            .get("road_type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let lines = match feature.geometry {
            Geometry::LineString(line) => vec![line],
            Geometry::MultiLineString(lines) => lines,
            _ => Vec::new(),
        };
        for coords in lines {
            if coords.len() < 2 {
                continue;
            }
            segments.push(Segment {
                coords,
                traffic_level,
                road_type: road_type.clone(),
                road_category: props.get("road_category").cloned().unwrap_or(Value::Null),
                closure,
            });
        }
    }
    Ok(segments)
}

/// The flow source's decode: drivable segments clipped to their tile.
fn decode(bytes: &[u8], z: u32, x: u32, y: u32) -> Result<Vec<Segment>, Failure> {
    let tile = tile_to_bbox(z, x, y);
    Ok(decode_flow_tile(bytes, z, x, y)?
        .into_iter()
        .filter(is_drivable)
        .flat_map(|segment| {
            clip_tile_line(&segment.coords, &tile)
                .into_iter()
                .map(move |coords| Segment {
                    coords,
                    ..segment.clone()
                })
                .collect::<Vec<_>>()
        })
        .collect())
}

/// `{ segments, partial }`.
pub struct Flow {
    pub segments: Vec<Segment>,
    pub partial: bool,
}

/// The traffic service the tool reads.
pub struct TrafficSource {
    api: Api,
    tiles: VectorTileSource<Segment>,
}

impl TrafficSource {
    pub fn new(api: &Api) -> Self {
        Self {
            api: api.clone(),
            tiles: VectorTileSource::new(
                api.clone(),
                TemplateSource::Fixed("/api/tomtom/flow/{z}/{x}/{y}.pbf".into()),
                "http://localhost",
                decode,
                16,
                120_000.0,
            ),
        }
    }

    /// `getStatus()`: whether the server has a TomTom key.
    pub async fn has_key(&self) -> Result<bool, Failure> {
        let response = self
            .api
            .fetch(
                "/api/tomtom/status",
                Init::default().timeout(Duration::from_secs(8)),
            )
            .await?;
        if !response.ok() {
            return Err(Failure::Other {
                message: "TomTom status unavailable".into(),
                status: Some(response.status),
                code: None,
            });
        }
        let status = response.json().await?;
        status
            .get("hasKey")
            .and_then(Value::as_bool)
            .ok_or_else(|| Failure::other("Malformed traffic status"))
    }

    /// `fetchFlowDetail(bounds, { zoom })`.
    pub async fn flow_detail(&self, bounds: &BBox, zoom: u32) -> Result<Flow, Failure> {
        let result = self.tiles.fetch_bounds(bounds, zoom, None).await?;
        let segments = result
            .tiles
            .iter()
            .flat_map(|tile| tile.iter())
            .flat_map(|segment| {
                clip_tile_line(&segment.coords, bounds)
                    .into_iter()
                    .map(|coords| Segment {
                        coords,
                        ..segment.clone()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        Ok(Flow {
            segments,
            partial: result.partial,
        })
    }
}
