//! Named OpenStreetMap features through the server's Overpass route:
//! `boundaries.query` from `src/services/requests.js` and the lookups of
//! `src/sources/overpassFeatures.js` the map feature tool uses, with the
//! record fields of `normalizeOverpassFeatures` it reads.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;

use super::fetch::{Api, Init, Redirect};
use super::installations::is_unavailable_capability;
use super::live::encode_uri_component;
use crate::mcp::catalog::Failure;
use crate::mcp::js;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_RETRY_MS: f64 = 30_000.0;

/// A feature lookup's answer.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    /// A definitive list (possibly empty).
    Features(Vec<Feature>),
    /// No Overpass capability on this server.
    Unavailable,
    /// A retryable failure or an admission delay.
    NoAnswer,
}

/// The fields of a normalized feature the tool reads.
#[derive(Debug, Clone, PartialEq)]
pub struct Feature {
    /// `element.id` (absent when the element had none).
    pub id: Option<Value>,
    pub category: &'static str,
    pub english: Option<Value>,
    pub primary: Option<Value>,
    pub official: Option<Value>,
    pub level: f64,
    pub point: Option<(Option<Value>, Option<Value>)>,
}

/// The kinds `get_map_features` offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    AdministrativeAreas,
    NamedPlaces,
    Monuments,
}

/// `normalizeOverpassFeatures(elements)`.
pub fn normalize(elements: &[Value]) -> Vec<Feature> {
    elements
        .iter()
        .map(|element| {
            let tags = element.get("tags").filter(|t| js::truthy(t));
            let tag = |key: &str| tags.and_then(|t| t.get(key)).cloned();
            let level = tag("admin_level").map_or(f64::NAN, |v| js::to_number(&v));
            let center = element.get("center").filter(|c| js::truthy(c));
            let point = if element.get("lat").and_then(js::finite).is_some() {
                Some((element.get("lat").cloned(), element.get("lon").cloned()))
            } else {
                center.map(|c| (c.get("lat").cloned(), c.get("lon").cloned()))
            };
            Feature {
                id: element.get("id").cloned(),
                category: if element.get("type").and_then(Value::as_str) == Some("area") {
                    "administrative"
                } else {
                    "feature"
                },
                english: tag("name:en"),
                primary: tag("name"),
                official: tag("official_name"),
                level: if level == 0.0 || level.is_nan() {
                    99.0
                } else {
                    level
                },
                point,
            }
        })
        .collect()
}

#[derive(Default)]
struct Probe {
    /// The settled probe: Some(Some(configured)) or Some(None) for an older
    /// server.
    known: Option<Option<bool>>,
    retry_at: f64,
}

/// `createOverpassFeatureSource` over `boundaries.query`.
#[derive(Default)]
pub struct FeatureSource {
    probe: Mutex<Probe>,
    unavailable: AtomicBool,
}

enum Query {
    Elements(Vec<Value>),
    Unavailable,
    Other,
}

impl FeatureSource {
    /// `boundariesConfigured()`.
    async fn configured(&self, api: &Api) -> Option<bool> {
        {
            let probe = self.probe.lock().expect("probe");
            if let Some(known) = probe.known {
                return known;
            }
            if js::now() < probe.retry_at {
                return None;
            }
        }
        let attempt = tokio::time::timeout(PROBE_TIMEOUT, async {
            let response = api
                .fetch("/api/overpass/status", Init::redirect(Redirect::Error))
                .await?;
            let ok = response.ok();
            let data = response.json().await.unwrap_or(Value::Null);
            Ok::<_, Failure>(if ok {
                data.get("configured").and_then(Value::as_bool)
            } else {
                None
            })
        })
        .await;
        let mut probe = self.probe.lock().expect("probe");
        match attempt {
            Ok(Ok(configured)) => {
                probe.known = Some(configured);
                configured
            }
            _ => {
                probe.retry_at = js::now() + PROBE_RETRY_MS;
                None
            }
        }
    }

    /// `boundaries.query(text)`.
    async fn boundaries(&self, api: &Api, text: &str) -> Result<Query, Failure> {
        if self.configured(api).await == Some(false) {
            return Ok(Query::Unavailable);
        }
        let mut init = Init::redirect(Redirect::Error)
            .header("Content-Type", "application/x-www-form-urlencoded");
        init.method = "POST".into();
        init.body = Some(format!("data={}", encode_uri_component(text)).into());
        let response = api.fetch("/api/overpass", init).await?;
        let ok = response.ok();
        let status = response.status;
        let retry = response.header("Retry-After");
        let data = response.json().await.unwrap_or(Value::Null);
        if is_unavailable_capability(&data) {
            return Ok(Query::Unavailable);
        }
        if status == 406 || status == 429 || (status == 503 && retry.is_some()) || !ok {
            return Ok(Query::Other);
        }
        let remark = match data.get("remark").filter(|r| js::truthy(r)) {
            Some(remark) => js::to_js_string(remark).to_lowercase(),
            None => String::new(),
        };
        if ["runtime error", "timed out", "out of memory"]
            .iter()
            .any(|m| remark.contains(m))
        {
            return Ok(Query::Other);
        }
        Ok(match data.get("elements") {
            Some(Value::Array(elements)) => Query::Elements(elements.clone()),
            _ => Query::Other,
        })
    }

    /// `query(text, timeoutMs)`.
    async fn query(&self, api: &Api, text: &str, timeout_ms: u64) -> Answer {
        if self.unavailable.load(Ordering::Relaxed) {
            return Answer::Unavailable;
        }
        match tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            self.boundaries(api, text),
        )
        .await
        {
            Ok(Ok(Query::Elements(elements))) => Answer::Features(normalize(&elements)),
            Ok(Ok(Query::Unavailable)) => {
                self.unavailable.store(true, Ordering::Relaxed);
                Answer::Unavailable
            }
            _ => Answer::NoAnswer,
        }
    }

    /// `getAdministrativeAreas`, `getEnclosingAreas` or `getMonuments` at a point.
    pub async fn lookup(
        &self,
        api: &Api,
        kind: Kind,
        lat: f64,
        lon: f64,
    ) -> Result<Answer, Failure> {
        if !lat.is_finite() || !lon.is_finite() || lat.abs() > 90.0 || lon.abs() > 180.0 {
            return Err(Failure::Type(
                "Valid feature coordinates are required".into(),
            ));
        }
        let (lat, lon) = (js::fmt_num(lat), js::fmt_num(lon));
        Ok(match kind {
            Kind::AdministrativeAreas => {
                let text = format!(
                    "[out:json][timeout:25];is_in({lat},{lon})->.a;area.a[\"boundary\"=\"administrative\"][\"admin_level\"];out tags;"
                );
                self.query(api, &text, 14_000).await
            }
            Kind::NamedPlaces => {
                let around = |what: &str, filters: &str| {
                    format!("      {what}(around:600,{lat},{lon}){filters};\n")
                };
                let text = [
                    "\n    [out:json][timeout:25];\n    (\n".to_string(),
                    around("way", "[\"leisure\"][\"name\"]"),
                    around(
                        "relation",
                        "[\"leisure\"][\"name\"][\"type\"=\"multipolygon\"]",
                    ),
                    around("way", "[\"landuse\"][\"name\"]"),
                    around(
                        "relation",
                        "[\"landuse\"][\"name\"][\"type\"=\"multipolygon\"]",
                    ),
                    around("way", "[\"boundary\"][\"name\"]"),
                    around("relation", "[\"boundary\"][\"name\"]"),
                    around("way", "[\"amenity\"][\"name\"]"),
                    around(
                        "relation",
                        "[\"amenity\"][\"name\"][\"type\"=\"multipolygon\"]",
                    ),
                    around("way", "[\"natural\"=\"water\"][\"name\"]"),
                    around(
                        "relation",
                        "[\"natural\"=\"water\"][\"name\"][\"type\"=\"multipolygon\"]",
                    ),
                    "    );\n    out geom;\n  ".to_string(),
                ]
                .concat();
                self.query(api, &text, 12_000).await
            }
            Kind::Monuments => {
                let text = format!(
                    "\n        [out:json][timeout:20];\n        (\n          nwr(around:2500,{lat},{lon})[\"historic\"~\"memorial|monument|statue|tomb\"];\n          nwr(around:2500,{lat},{lon})[\"tourism\"=\"artwork\"][\"name\"];\n          nwr(around:2500,{lat},{lon})[\"memorial\"][\"name\"];\n        );\n        out center tags;\n      "
                );
                self.query(api, &text, 6_000).await
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalizes_feature_fields() {
        let features = normalize(&[
            json!({"type": "area", "id": 3600000001u64, "tags": {"name": "Texas", "admin_level": "4"}}),
            json!({"type": "node", "id": 7, "lat": 1.5, "lon": 2.5, "tags": {"name:en": "Statue"}}),
        ]);
        assert_eq!(features[0].category, "administrative");
        assert_eq!(features[0].level, 4.0);
        assert_eq!(features[1].level, 99.0);
        assert_eq!(
            features[1].point,
            Some((Some(json!(1.5)), Some(json!(2.5))))
        );
    }
}
