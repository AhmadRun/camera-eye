//! Offline regional context from the bundled Natural Earth polygons (the
//! `naturalRegionAtPoint` part of `src/data/naturalEarthRegions.js`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

const EARTH_RADIUS_KM: f64 = 6371.0;

#[derive(Deserialize)]
struct Pack {
    #[serde(default)]
    features: Vec<Feature>,
}

#[derive(Deserialize)]
struct Feature {
    name: Value,
    #[serde(default)]
    polygons: Vec<Vec<Vec<f64>>>,
}

/// One indexed region (`buildEntries`).
pub struct Region {
    pub name: Value,
    pub kind: &'static str,
    pub polygons: Vec<Vec<(f64, f64)>>,
    pub area_km2: f64,
    /// `[west, south, east, north]`.
    pub bbox: [f64; 4],
}

/// Spherical-excess ring area in km².
fn ring_area_km2(ring: &[(f64, f64)]) -> f64 {
    let n = ring.len();
    if n < 3 {
        return 0.0;
    }
    let mut sum = 0.0;
    for i in 0..n {
        let (lon1, lat1) = ring[i];
        let (lon2, lat2) = ring[(i + 1) % n];
        sum +=
            (lon2 - lon1).to_radians() * (2.0 + lat1.to_radians().sin() + lat2.to_radians().sin());
    }
    (sum * EARTH_RADIUS_KM * EARTH_RADIUS_KM / 2.0).abs()
}

/// Ray-cast (even-odd) point-in-ring test; rings are `[lon, lat]`.
pub fn point_in_ring(ring: &[(f64, f64)], lat: f64, lon: f64) -> bool {
    if ring.len() < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        if (yi > lat) != (yj > lat) && lon < (xj - xi) * (lat - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

fn build_entries(pack: Pack, kind: &'static str, out: &mut Vec<Region>) {
    for feature in pack.features {
        if feature.polygons.is_empty() {
            continue;
        }
        let polygons: Vec<Vec<(f64, f64)>> = feature
            .polygons
            .into_iter()
            .map(|ring| {
                ring.into_iter()
                    .map(|p| {
                        (
                            p.first().copied().unwrap_or(f64::NAN),
                            p.get(1).copied().unwrap_or(f64::NAN),
                        )
                    })
                    .collect()
            })
            .collect();
        let mut area = 0.0;
        let (mut min_lon, mut min_lat) = (f64::INFINITY, f64::INFINITY);
        let (mut max_lon, mut max_lat) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
        for ring in &polygons {
            area += ring_area_km2(ring);
            for &(lon, lat) in ring {
                if lon < min_lon {
                    min_lon = lon;
                }
                if lon > max_lon {
                    max_lon = lon;
                }
                if lat < min_lat {
                    min_lat = lat;
                }
                if lat > max_lat {
                    max_lat = lat;
                }
            }
        }
        out.push(Region {
            name: feature.name,
            kind,
            polygons,
            area_km2: area,
            bbox: [min_lon, min_lat, max_lon, max_lat],
        });
    }
}

fn load_pack(path: &Path) -> anyhow::Result<Pack> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

/// Both packs, loaded once; a failed load is retried on the next lookup.
pub struct NaturalEarth {
    dir: PathBuf,
    index: OnceCell<Arc<Vec<Region>>>,
}

impl NaturalEarth {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            index: OnceCell::new(),
        }
    }

    async fn entries(&self) -> anyhow::Result<Arc<Vec<Region>>> {
        let dir = self.dir.clone();
        self.index
            .get_or_try_init(|| async move {
                tokio::task::spawn_blocking(move || -> anyhow::Result<Arc<Vec<Region>>> {
                    let regions = load_pack(&dir.join("regions.json"))?;
                    let marine = load_pack(&dir.join("marine.json"))?;
                    let mut entries = Vec::new();
                    build_entries(regions, "natural", &mut entries);
                    build_entries(marine, "marine", &mut entries);
                    Ok(Arc::new(entries))
                })
                .await?
            })
            .await
            .cloned()
    }

    /// `naturalRegionAtPoint`: the smallest containing region as place
    /// context. `Err` stands for the JS rejection (an unreadable pack).
    pub async fn region_at_point(
        &self,
        latitude: f64,
        longitude: f64,
    ) -> anyhow::Result<Option<Value>> {
        if !latitude.is_finite()
            || !longitude.is_finite()
            || latitude.abs() > 90.0
            || longitude.abs() > 180.0
        {
            return Ok(None);
        }
        let entries = self.entries().await?;
        let mut best: Option<&Region> = None;
        for entry in entries.iter() {
            let [west, south, east, north] = entry.bbox;
            if longitude < west || longitude > east || latitude < south || latitude > north {
                continue;
            }
            if best.is_some_and(|b| entry.area_km2 >= b.area_km2) {
                continue;
            }
            if entry
                .polygons
                .iter()
                .any(|ring| point_in_ring(ring, latitude, longitude))
            {
                best = Some(entry);
            }
        }
        Ok(best.map(|best| {
            json!({
                "label": best.name,
                "locality": null,
                "region": best.name,
                "country": null,
                "countryCode": null,
                "source": "Natural Earth",
                "kind": best.kind,
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_in_ring_square() {
        let square = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)];
        assert!(point_in_ring(&square, 5.0, 5.0));
        assert!(!point_in_ring(&square, 15.0, 5.0));
        assert!(!point_in_ring(&[(0.0, 0.0), (1.0, 1.0)], 0.5, 0.5));
    }

    fn bundled() -> NaturalEarth {
        NaturalEarth::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../src/data/local_data/natural_earth"),
        )
    }

    #[tokio::test]
    async fn bundled_packs_resolve_points() {
        let packs = bundled();
        let alps = packs.region_at_point(46.5, 10.0).await.unwrap().unwrap();
        assert_eq!(alps["source"], "Natural Earth");
        assert_eq!(alps["locality"], Value::Null);
        assert!(alps["label"].is_string());
        let gulf = packs.region_at_point(25.0, -90.0).await.unwrap().unwrap();
        assert_eq!(gulf["kind"], "marine");
        assert_eq!(packs.region_at_point(91.0, 0.0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_missing_pack_is_an_error_and_retried() {
        let packs = NaturalEarth::new(PathBuf::from("/nonexistent/natural_earth"));
        assert!(packs.region_at_point(10.0, 10.0).await.is_err());
        assert!(packs.region_at_point(10.0, 10.0).await.is_err());
    }
}
