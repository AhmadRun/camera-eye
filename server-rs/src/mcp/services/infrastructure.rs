//! Bundled OpenStreetMap datacenters and dams
//! (`src/sources/infrastructureData.js`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::fetch::Api;
use crate::mcp::catalog::Failure;
use crate::mcp::js;

/// An analyst record (`mapAnalystRecord`).
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub name: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub operator: Option<String>,
    pub capacity: Option<String>,
    pub river: Option<String>,
    pub output: Option<String>,
}

fn data_path(layer_id: &str) -> Option<&'static str> {
    match layer_id {
        "local-datacenters" => Some("datacenters/datacenters.geojsonl"),
        "local-dams" => Some("dams/dams.geojsonl"),
        _ => None,
    }
}

/// `text(v)` in `mapAnalystRecord`.
fn text(value: Option<&Value>) -> Option<String> {
    let s = match value {
        None | Some(Value::Null) => String::new(),
        Some(value) => js::to_js_string(value),
    };
    let t = js::trim(&s);
    (!t.is_empty() && t != "undefined" && t != "null").then(|| t.to_string())
}

fn first(candidates: &[Option<&Value>]) -> Option<String> {
    candidates.iter().find_map(|value| text(*value))
}

/// `featurePoint(feature)`: a point, or an outline's average vertex.
pub fn feature_point(feature: &Value) -> Option<(f64, f64)> {
    let geometry = feature.get("geometry")?;
    let coordinates = geometry.get("coordinates");
    let single;
    let outline: Option<&Vec<Value>> = match geometry.get("type").and_then(Value::as_str) {
        Some("Point") => {
            single = vec![coordinates.cloned().unwrap_or(Value::Null)];
            Some(&single)
        }
        Some("LineString") => coordinates.and_then(Value::as_array),
        Some("Polygon") => coordinates.and_then(|c| c.get(0)).and_then(Value::as_array),
        Some("MultiPolygon") => coordinates
            .and_then(|c| c.get(0))
            .and_then(|c| c.get(0))
            .and_then(Value::as_array),
        _ => None,
    };
    let points: Vec<(f64, f64)> = outline?
        .iter()
        .filter_map(|point| {
            Some((
                point.get(0).and_then(js::finite)?,
                point.get(1).and_then(js::finite)?,
            ))
        })
        .collect();
    if points.is_empty() {
        return None;
    }
    let n = points.len() as f64;
    let lon = points.iter().fold(0.0, |sum, p| sum + p.0) / n;
    let lat = points.iter().fold(0.0, |sum, p| sum + p.1) / n;
    Some((lon, lat))
}

/// `mapAnalystRecord` for a feature at a point.
fn analyst_record(properties: Option<&Value>, lon: f64, lat: f64, layer_id: &str) -> Record {
    let props = properties.filter(|p| p.is_object());
    let prop = |key: &str| props.and_then(|p| p.get(key));
    let tags = prop("tags").filter(|t| t.is_object());
    let tag = |key: &str| tags.and_then(|t| t.get(key));
    let datacenters = layer_id == "local-datacenters";
    let dams = layer_id == "local-dams";
    Record {
        name: first(&[
            prop("name"),
            tag("name"),
            tag("name:en"),
            tag("official_name"),
        ]),
        lat,
        lon,
        operator: first(&[tag("operator"), prop("operator"), tag("operator:short")]),
        capacity: datacenters
            .then(|| {
                first(&[
                    tag("capacity:it_load"),
                    tag("it_load"),
                    tag("capacity"),
                    prop("capacity"),
                ])
            })
            .flatten(),
        river: dams
            .then(|| {
                first(&[
                    tag("associated_river"),
                    prop("associated_river"),
                    tag("river"),
                    prop("river"),
                    tag("river:name"),
                ])
            })
            .flatten(),
        output: dams
            .then(|| first(&[prop("output"), tag("plant:output:electricity")]))
            .flatten(),
    }
}

/// `createInfrastructureSource`: records per layer, read once.
#[derive(Default)]
pub struct InfrastructureSource {
    cache: Mutex<HashMap<String, Arc<Vec<Record>>>>,
}

impl InfrastructureSource {
    /// `getRecords(layerId)`.
    pub async fn records(&self, api: &Api, layer_id: &str) -> Result<Arc<Vec<Record>>, Failure> {
        let Some(path) = data_path(layer_id) else {
            return Err(Failure::Type(format!(
                "Unknown infrastructure layer: {layer_id}"
            )));
        };
        if let Some(records) = self
            .cache
            .lock()
            .expect("infrastructure cache")
            .get(layer_id)
        {
            return Ok(records.clone());
        }
        let response = api.get(&api.bundled_url(path)).await?;
        if !response.ok() {
            return Err(Failure::other(format!("HTTP {}", response.status)));
        }
        let text = response.text().await?;
        let mut records = Vec::new();
        for line in text.split('\n').filter(|line| !js::trim(line).is_empty()) {
            let feature = js::parse_json(line).ok_or_else(super::fetch::json_syntax_error)?;
            if let Some((lon, lat)) = feature_point(&feature) {
                records.push(analyst_record(
                    feature.get("properties"),
                    lon,
                    lat,
                    layer_id,
                ));
            }
        }
        let records = Arc::new(records);
        self.cache
            .lock()
            .expect("infrastructure cache")
            .insert(layer_id.to_string(), records.clone());
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_features_to_records() {
        let feature = json!({
            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2]]]},
            "properties": {"tags": {"name:en": " Dam ", "river": "Nile"}, "output": null}
        });
        let (lon, lat) = feature_point(&feature).unwrap();
        assert_eq!((lon, lat), (1.0, 1.0));
        let record = analyst_record(feature.get("properties"), lon, lat, "local-dams");
        assert_eq!(record.name.as_deref(), Some("Dam"));
        assert_eq!(record.river.as_deref(), Some("Nile"));
        assert_eq!(record.output, None);
        assert_eq!(record.capacity, None);
    }
}
