//! Port of `server/providers/cctv/groundHeights.js`: the precomputed
//! ground-height sidecar, joined onto served sources whose pose still matches.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use parking_lot::Mutex;
use serde_json::{Map, Value, json};

use super::footprint::{Pose, pose_hash};
use super::normalize::CctvSource;

/// Sidecar written by scripts/precompute-cctv-heights.mjs.
pub const DEFAULT_GROUND_HEIGHTS_FILE: &str =
    "src/data/local_data/cctv_ground_heights/cctv_ground_heights.json";

/// The parsed sidecar with the path and modification time it was read at.
type Cached = (PathBuf, SystemTime, Map<String, Value>);

/// Cached by path and modification time.
#[derive(Default)]
pub struct GroundHeightsCache {
    cache: Mutex<Option<Cached>>,
}

impl GroundHeightsCache {
    /// `loadGroundHeights`: camera id to sidecar entry; `{}` on any problem.
    pub fn load(&self, source_root: &Path, configured: Option<&str>) -> Map<String, Value> {
        let file = configured
            .filter(|f| !f.is_empty())
            .unwrap_or(DEFAULT_GROUND_HEIGHTS_FILE);
        let resolved = source_root.join(file);
        let mut cache = self.cache.lock();
        let Ok(modified) = std::fs::metadata(&resolved).and_then(|m| m.modified()) else {
            *cache = None;
            return Map::new();
        };
        if let Some((path, mtime, cameras)) = cache.as_ref()
            && *path == resolved
            && *mtime == modified
        {
            return cameras.clone();
        }
        let parsed = std::fs::read(&resolved)
            .ok()
            .and_then(|bytes| super::jsval::parse_json(&bytes));
        let Some(parsed) = parsed else {
            *cache = None;
            return Map::new();
        };
        let cameras = match parsed.get("cameras") {
            Some(Value::Object(map)) => map.clone(),
            _ => Map::new(),
        };
        *cache = Some((resolved, modified, cameras.clone()));
        cameras
    }
}

fn source_pose(source: &CctvSource) -> Pose {
    Pose {
        lat: source.lat,
        lon: source.lon,
        heading_deg: source.heading_deg,
        pitch_deg: source.pitch_deg,
        fov_deg: source.fov_deg,
        range_m: source.range_m,
        mount_height_m: source.mount_height_m,
    }
}

fn finite_number(v: Option<&Value>) -> Option<f64> {
    v.and_then(Value::as_f64).filter(|f| f.is_finite())
}

/// `joinGroundHeights`: attach a shipped entry to each source whose pose
/// still hashes to what the entry was sampled for.
pub fn join_ground_heights(sources: &mut [CctvSource], entries: &Map<String, Value>) {
    for source in sources.iter_mut() {
        let Some(entry) = entries.get(&source.id) else {
            continue;
        };
        if entry.get("status") != Some(&Value::String("ok".into())) {
            continue;
        }
        let Some(mount) = finite_number(entry.get("mountGroundM")) else {
            continue;
        };
        let hash = pose_hash(&source_pose(source));
        if entry.get("poseHash") != Some(&Value::String(hash)) {
            continue;
        }
        let mut supports = Map::new();
        match entry.get("supports") {
            Some(Value::Object(map)) => {
                for (key, value) in map {
                    if let Some(f) = finite_number(Some(value)) {
                        supports.insert(key.clone(), super::jsval::num(f));
                    }
                }
            }
            Some(Value::Array(items)) => {
                for (i, value) in items.iter().enumerate() {
                    if let Some(f) = finite_number(Some(value)) {
                        supports.insert(i.to_string(), super::jsval::num(f));
                    }
                }
            }
            _ => {}
        }
        source.ground_heights = Some(json!({
            "poseHash": entry["poseHash"].clone(),
            "mountGroundM": super::jsval::num(mount),
            "supports": supports,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cctv::normalize::normalize_source_item;

    fn source(lat: f64) -> CctvSource {
        normalize_source_item(&json!({
            "id": "cam-1", "lat": lat, "lon": -97.7431, "headingDeg": 90, "pitchDeg": -24,
            "fovDeg": 56, "rangeM": 210, "mountHeightM": 12
        }))
    }

    #[test]
    fn an_entry_attaches_only_while_the_pose_matches() {
        let s = source(30.2672);
        let hash = pose_hash(&source_pose(&s));
        let entry = json!({
            "status": "ok", "poseHash": hash, "mountGroundM": 151.2,
            "supports": { "bl": 150.1, "bm": 149.9, "br": 155.5, "tl": null, "tm": "x" }
        });
        let entries: Map<String, Value> =
            [("cam-1".to_string(), entry.clone())].into_iter().collect();
        let mut joined = vec![s];
        join_ground_heights(&mut joined, &entries);
        assert_eq!(
            joined[0].ground_heights,
            Some(json!({
                "poseHash": hash, "mountGroundM": 151.2,
                "supports": { "bl": 150.1, "bm": 149.9, "br": 155.5 }
            }))
        );
        let mut moved = vec![source(30.2673)];
        join_ground_heights(&mut moved, &entries);
        assert_eq!(moved[0].ground_heights, None);
        let mut miss_entry = entry.clone();
        miss_entry["status"] = json!("miss");
        miss_entry["mountGroundM"] = Value::Null;
        let misses: Map<String, Value> = [("cam-1".to_string(), miss_entry)].into_iter().collect();
        let mut missed = vec![source(30.2672)];
        join_ground_heights(&mut missed, &misses);
        assert_eq!(missed[0].ground_heights, None);
    }

    #[test]
    fn the_sidecar_tolerates_missing_and_malformed_files() {
        let dir = std::env::temp_dir().join(format!("ce-heights-{}", uuid::Uuid::new_v4()));
        let folder = dir.join("src/data/local_data/cctv_ground_heights");
        std::fs::create_dir_all(&folder).unwrap();
        let cache = GroundHeightsCache::default();
        assert!(cache.load(&dir, None).is_empty(), "missing file");
        let file = folder.join("cctv_ground_heights.json");
        std::fs::write(&file, "{not json").unwrap();
        assert!(cache.load(&dir, None).is_empty(), "malformed file");
        std::fs::write(
            &file,
            r#"{"schemaVersion":1,"cameras":{"a":{"status":"ok"}}}"#,
        )
        .unwrap();
        // A fresh cache, since the mtime can repeat within one clock tick.
        let loaded = GroundHeightsCache::default().load(&dir, None);
        assert_eq!(Value::Object(loaded), json!({ "a": { "status": "ok" } }));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_shipped_sidecar_parses() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let cameras = GroundHeightsCache::default().load(&root, None);
        assert!(!cameras.is_empty());
    }
}
