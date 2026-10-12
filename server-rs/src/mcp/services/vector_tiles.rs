//! Slippy tile math (`src/data/tomtomTiles.js`), line clipping
//! (`clipTileLine` from `src/sources/openFreeMap.js`) and the bounded
//! vector tile cache (`createVectorTileSource` in
//! `src/sources/vectorTiles.js`).
//!
//! The cache keeps the JS bounds (TTL, 64 entries, 24 MB, 4 workers, 12 s
//! per request, 4 MB per tile, 256 KB metadata, a 5 s metadata cooldown);
//! concurrent requests for one tile are not coalesced.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use tokio::sync::Semaphore;
use url::Url;

use super::fetch::{Api, Init, Redirect, Response};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

const MERCATOR_LAT_LIMIT: f64 = 85.05112878;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(12);
const METADATA_MAX_BYTES: usize = 256 * 1024;
const METADATA_COOLDOWN_MS: f64 = 5_000.0;

/// A tile coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tile {
    pub z: u32,
    pub x: u32,
    pub y: u32,
}

/// A geographic box.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBox {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

/// `lonLatToTile(lon, lat, z)`.
pub fn lon_lat_to_tile(lon: f64, lat: f64, z: u32) -> (f64, f64) {
    let n = 2f64.powi(z as i32);
    let clamped = MERCATOR_LAT_LIMIT.min(lat).max(-MERCATOR_LAT_LIMIT);
    let lat_rad = clamped * std::f64::consts::PI / 180.0;
    let x = ((lon + 180.0) / 360.0 * n).floor();
    let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / std::f64::consts::PI) / 2.0 * n)
        .floor();
    (js_clamp(x, n - 1.0), js_clamp(y, n - 1.0))
}

/// `Math.max(0, Math.min(max, v))` (NaN stays NaN).
fn js_clamp(v: f64, max: f64) -> f64 {
    js::math_max([0.0, js::math_min([max, v])])
}

/// `tileToBBox(z, x, y)`.
pub fn tile_to_bbox(z: u32, x: u32, y: u32) -> BBox {
    let n = 2f64.powi(z as i32);
    let lon_at = |col: f64| col / n * 360.0 - 180.0;
    let lat_at = |row: f64| {
        (std::f64::consts::PI * (1.0 - 2.0 * row / n)).sinh().atan() * 180.0 / std::f64::consts::PI
    };
    BBox {
        west: lon_at(f64::from(x)),
        east: lon_at(f64::from(x) + 1.0),
        north: lat_at(f64::from(y)),
        south: lat_at(f64::from(y) + 1.0),
    }
}

/// `tilesForBounds(bounds, zoom, { maxTiles })`, row-major from the
/// northwest corner.
pub fn tiles_for_bounds(bounds: &BBox, zoom: u32, max_tiles: usize) -> Vec<Tile> {
    let BBox {
        south,
        west,
        north,
        east,
    } = *bounds;
    if ![south, west, north, east].iter().all(|v| v.is_finite()) {
        return Vec::new();
    }
    let nw = lon_lat_to_tile(
        js::math_min([west, east]),
        js::math_max([south, north]),
        zoom,
    );
    let se = lon_lat_to_tile(
        js::math_max([west, east]),
        js::math_min([south, north]),
        zoom,
    );
    let mut tiles = Vec::new();
    let mut y = nw.1;
    while y <= se.1 {
        let mut x = nw.0;
        while x <= se.0 {
            if tiles.len() >= max_tiles {
                return tiles;
            }
            tiles.push(Tile {
                z: zoom,
                x: x as u32,
                y: y as u32,
            });
            x += 1.0;
        }
        y += 1.0;
    }
    tiles
}

/// `validTileBounds(box)`.
pub fn valid_tile_bounds(b: &BBox) -> bool {
    [b.south, b.west, b.north, b.east]
        .iter()
        .all(|v| v.is_finite())
        && b.south >= -90.0
        && b.north <= 90.0
        && b.west >= -180.0
        && b.east <= 180.0
        && b.north > b.south
        && b.east > b.west
}

/// V8's `Math.hypot(a, b)`.
pub fn hypot2(a: f64, b: f64) -> f64 {
    let values = [a.abs(), b.abs()];
    let mut max = 0.0f64;
    let mut nan = false;
    for v in values {
        if v.is_nan() {
            nan = true;
        }
        if v > max {
            max = v;
        }
    }
    if max == f64::INFINITY {
        return f64::INFINITY;
    }
    if nan {
        return f64::NAN;
    }
    if max == 0.0 {
        return 0.0;
    }
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for v in values {
        let n = v / max;
        let summand = n * n - compensation;
        let preliminary = sum + summand;
        compensation = (preliminary - sum) - summand;
        sum = preliminary;
    }
    sum.sqrt() * max
}

/// `clipTileLine(coords, box)`: the parts of a `[lon, lat]` line inside a
/// box, dropping parts shorter than 12 m.
pub fn clip_tile_line(coords: &[[f64; 2]], b: &BBox) -> Vec<Vec<[f64; 2]>> {
    let mut lines: Vec<Vec<[f64; 2]>> = Vec::new();
    let mut line: Vec<[f64; 2]> = Vec::new();
    for i in 1..coords.len() {
        let (a, bb) = (coords[i - 1], coords[i]);
        let dx = bb[0] - a[0];
        let dy = bb[1] - a[1];
        let (mut lo, mut hi) = (0.0f64, 1.0f64);
        let p = [-dx, dx, -dy, dy];
        let q = [a[0] - b.west, b.east - a[0], a[1] - b.south, b.north - a[1]];
        for k in 0..4 {
            if p[k] == 0.0 {
                if q[k] < 0.0 {
                    hi = -1.0;
                }
            } else if p[k] < 0.0 {
                lo = js::math_max([lo, q[k] / p[k]]);
            } else {
                hi = js::math_min([hi, q[k] / p[k]]);
            }
        }
        if lo > hi {
            if line.len() > 1 {
                lines.push(std::mem::take(&mut line));
            }
            line.clear();
            continue;
        }
        let start = [a[0] + lo * dx, a[1] + lo * dy];
        let end = [a[0] + hi * dx, a[1] + hi * dy];
        let continues = line.last().is_some_and(|last| {
            let gap = (last[0] - start[0]).abs() + (last[1] - start[1]).abs();
            // Not greater, so a NaN gap continues the line as in JS.
            !matches!(gap.partial_cmp(&1e-10), Some(std::cmp::Ordering::Greater))
        });
        if !continues {
            if line.len() > 1 {
                lines.push(std::mem::take(&mut line));
            }
            line = vec![start];
        }
        line.push(end);
    }
    if line.len() > 1 {
        lines.push(line);
    }
    lines
        .into_iter()
        .filter(|points| {
            let mut metres = 0.0;
            for i in 1..points.len() {
                metres += hypot2(
                    (points[i][0] - points[i - 1][0])
                        * (points[i][1] * std::f64::consts::PI / 180.0).cos(),
                    points[i][1] - points[i - 1][1],
                ) * 111_320.0;
            }
            metres >= 12.0
        })
        .collect()
}

/// A tile decoder: bytes and tile coordinate to records.
pub type Decode<T> = fn(&[u8], u32, u32, u32) -> Result<Vec<T>, Failure>;

/// Where the source reads its tile URL template.
pub enum TemplateSource {
    /// A TileJSON document's `tiles[0]`.
    TileJson(String),
    /// A fixed template.
    Fixed(String),
}

struct Entry<T> {
    key: String,
    at: f64,
    size: usize,
    value: Arc<Vec<T>>,
}

struct State<T> {
    metadata: Option<Arc<Value>>,
    metadata_error: Option<(Failure, bool)>,
    metadata_retry_at: f64,
    cache: VecDeque<Entry<T>>,
    cache_bytes: usize,
}

/// The `fetchBounds` result.
pub struct BoundsResult<T> {
    pub tiles: Vec<Arc<Vec<T>>>,
    pub loaded_tiles: Vec<Tile>,
    pub partial: bool,
}

/// `createVectorTileSource`.
pub struct VectorTileSource<T> {
    api: Api,
    template: TemplateSource,
    allowed_origin: String,
    decode: Decode<T>,
    max_tiles: usize,
    ttl_ms: f64,
    max_entries: usize,
    max_cache_bytes: usize,
    max_response_bytes: usize,
    workers: Semaphore,
    state: Mutex<State<T>>,
}

/// The `Error` with `code: 'TILE_VIEW_TOO_WIDE'` a too-wide view fails with.
pub fn too_wide() -> Failure {
    Failure::Other {
        message: "Zoom in for vector tile coverage".into(),
        status: None,
        code: Some("TILE_VIEW_TOO_WIDE".into()),
    }
}

fn read_capped_error() -> Failure {
    Failure::Other {
        message: "Upstream response too large".into(),
        status: None,
        code: Some("RESPONSE_TOO_LARGE".into()),
    }
}

/// `readResponseBytesCapped`.
pub async fn read_capped(response: Response, max: usize) -> Result<Bytes, Failure> {
    let declared = response
        .header("content-length")
        .map_or(f64::NAN, |v| js::str_to_number(&v));
    if declared.is_finite() && declared > max as f64 {
        return Err(read_capped_error());
    }
    response
        .bytes_capped(max)
        .await?
        .ok_or_else(read_capped_error)
}

impl<T: Send + Sync + 'static> VectorTileSource<T> {
    pub fn new(
        api: Api,
        template: TemplateSource,
        allowed_origin: &str,
        decode: Decode<T>,
        max_tiles: usize,
        ttl_ms: f64,
    ) -> Self {
        Self {
            api,
            template,
            allowed_origin: allowed_origin.to_string(),
            decode,
            max_tiles,
            ttl_ms,
            max_entries: 64,
            max_cache_bytes: 24 * 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            workers: Semaphore::new(4),
            state: Mutex::new(State {
                metadata: None,
                metadata_error: None,
                metadata_retry_at: 0.0,
                cache: VecDeque::new(),
                cache_bytes: 0,
            }),
        }
    }

    /// Override the decoded cache bounds (`maxEntries`, `maxCacheBytes`).
    pub fn with_cache(mut self, max_entries: usize, max_cache_bytes: usize) -> Self {
        self.max_entries = max_entries;
        self.max_cache_bytes = max_cache_bytes;
        self
    }

    /// `request(url, signal, read)`: a GET with a 12 s limit that refuses
    /// redirects and fails on a non-2xx status.
    async fn request(&self, url: &str, max: usize) -> Result<Bytes, Failure> {
        let work = async {
            let response = self
                .api
                .fetch(
                    url,
                    Init::redirect(Redirect::Error).timeout(REQUEST_TIMEOUT),
                )
                .await?;
            if !response.ok() {
                let status = response.status;
                return Err(Failure::Other {
                    message: format!("Vector tiles unavailable (HTTP {status})"),
                    status: Some(status),
                    code: None,
                });
            }
            read_capped(response, max).await
        };
        tokio::time::timeout(REQUEST_TIMEOUT, work)
            .await
            .map_err(|_| Failure::Other {
                message: "Vector tile request timed out".into(),
                status: None,
                code: Some("TimeoutError".into()),
            })?
    }

    /// `getMetadata(signal)`: the TileJSON with `template` set.
    pub async fn metadata(&self) -> Result<Arc<Value>, Failure> {
        {
            let state = self.state.lock().expect("tile state");
            if let Some(metadata) = &state.metadata {
                return Ok(metadata.clone());
            }
            if let Some((error, retryable)) = &state.metadata_error
                && (!retryable || js::now() < state.metadata_retry_at)
            {
                return Err(error.clone());
            }
        }
        let loaded = match &self.template {
            TemplateSource::Fixed(template) => Ok(serde_json::json!({ "tiles": [template] })),
            TemplateSource::TileJson(url) => match self.request(url, METADATA_MAX_BYTES).await {
                Ok(bytes) => js::parse_json(&js::decode_text(&bytes))
                    .ok_or_else(super::fetch::json_syntax_error),
                Err(error) => Err(error),
            },
        };
        let checked = loaded
            .map_err(|error| (error, false))
            .and_then(|value| self.check_metadata(value));
        let mut state = self.state.lock().expect("tile state");
        match checked {
            Ok(metadata) => {
                let metadata = Arc::new(metadata);
                state.metadata = Some(metadata.clone());
                state.metadata_error = None;
                Ok(metadata)
            }
            Err((error, non_retryable)) => {
                let syntax = error.code() == Some("SyntaxError");
                let timeout = error.code() == Some("TimeoutError");
                let wrapped = Failure::Other {
                    message: "Vector tile metadata unavailable".into(),
                    status: error.status(),
                    code: timeout.then(|| "TimeoutError".to_string()),
                };
                state.metadata_error = Some((wrapped.clone(), !non_retryable && !syntax));
                state.metadata_retry_at = js::now() + METADATA_COOLDOWN_MS;
                Err(wrapped)
            }
        }
    }

    /// The metadata checks; the bool marks a non-retryable failure.
    fn check_metadata(&self, value: Value) -> Result<Value, (Failure, bool)> {
        let Some(url) = value
            .get("tiles")
            .and_then(|t| t.get(0))
            .and_then(Value::as_str)
            .filter(|url| {
                ["{z}", "{x}", "{y}"]
                    .iter()
                    .all(|token| url.contains(token))
            })
            .map(str::to_string)
        else {
            return Err((Failure::other("Invalid vector tile metadata"), true));
        };
        let origin_error = || (Failure::other("Invalid vector tile origin"), true);
        let probe = url
            .replacen("{z}", "0", 1)
            .replacen("{x}", "0", 1)
            .replacen("{y}", "0", 1);
        let base = Url::parse(&self.allowed_origin).map_err(|_| origin_error())?;
        let parsed = base.join(&probe).map_err(|_| origin_error())?;
        if parsed.origin().ascii_serialization() != self.allowed_origin
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(origin_error());
        }
        let mut value = value;
        if let Value::Object(map) = &mut value {
            map.insert("template".into(), Value::String(url));
            Ok(value)
        } else {
            Err((Failure::other("Invalid vector tile metadata"), true))
        }
    }

    async fn tile(&self, tile: Tile, template: &str) -> Result<Arc<Vec<T>>, Failure> {
        let key = format!("{template}:{}/{}/{}", tile.z, tile.x, tile.y);
        {
            let mut state = self.state.lock().expect("tile state");
            if let Some(index) = state.cache.iter().position(|entry| entry.key == key)
                && js::now() - state.cache[index].at < self.ttl_ms
            {
                let entry = state.cache.remove(index).expect("index");
                let value = entry.value.clone();
                state.cache.push_back(entry);
                return Ok(value);
            }
        }
        let _permit = self
            .workers
            .acquire()
            .await
            .map_err(|_| Failure::other("Source cleared"))?;
        let url = template
            .replacen("{z}", &tile.z.to_string(), 1)
            .replacen("{x}", &tile.x.to_string(), 1)
            .replacen("{y}", &tile.y.to_string(), 1);
        let bytes = self.request(&url, self.max_response_bytes).await?;
        let value = Arc::new((self.decode)(&bytes, tile.z, tile.x, tile.y)?);
        let size = bytes.len() * 4;
        if size <= self.max_cache_bytes {
            let mut state = self.state.lock().expect("tile state");
            if let Some(index) = state.cache.iter().position(|entry| entry.key == key) {
                let old = state.cache.remove(index).expect("index");
                state.cache_bytes -= old.size;
            }
            state.cache.push_back(Entry {
                key,
                at: js::now(),
                size,
                value: value.clone(),
            });
            state.cache_bytes += size;
            while state.cache.len() > self.max_entries || state.cache_bytes > self.max_cache_bytes {
                let Some(oldest) = state.cache.pop_front() else {
                    break;
                };
                state.cache_bytes -= oldest.size;
            }
        }
        Ok(value)
    }

    /// `fetchBounds(box, { zoom, tiles })`.
    pub async fn fetch_bounds(
        &self,
        b: &BBox,
        zoom: u32,
        selected: Option<Vec<Tile>>,
    ) -> Result<BoundsResult<T>, Failure> {
        if !valid_tile_bounds(b) || zoom > 14 {
            return Err(Failure::Type("Invalid tile viewport".into()));
        }
        let metadata = self.metadata().await?;
        let template = metadata["template"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let given = selected.is_some();
        let mut candidates =
            selected.unwrap_or_else(|| tiles_for_bounds(b, zoom, self.max_tiles + 1));
        let n = 1u64 << zoom;
        if candidates
            .iter()
            .any(|t| t.z != zoom || u64::from(t.x) >= n || u64::from(t.y) >= n)
        {
            return Err(Failure::Type("Invalid tile selection".into()));
        }
        let count = candidates.len() as f64;
        let cx = candidates.iter().map(|t| f64::from(t.x)).sum::<f64>() / count;
        let cy = candidates.iter().map(|t| f64::from(t.y)).sum::<f64>() / count;
        if !given {
            js::sort_by(&mut candidates, |a, b| {
                hypot2(f64::from(a.x) - cx, f64::from(a.y) - cy)
                    - hypot2(f64::from(b.x) - cx, f64::from(b.y) - cy)
            });
        }
        if candidates.len() > self.max_tiles {
            return Err(too_wide());
        }
        let results = futures_util::future::join_all(
            candidates.iter().map(|tile| self.tile(*tile, &template)),
        )
        .await;
        let mut tiles = Vec::new();
        let mut loaded = Vec::new();
        let mut first_error = None;
        for (result, tile) in results.into_iter().zip(&candidates) {
            match result {
                Ok(value) => {
                    tiles.push(value);
                    loaded.push(*tile);
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        let partial = tiles.len() != candidates.len();
        if tiles.is_empty()
            && let Some(error) = first_error
        {
            return Err(error);
        }
        Ok(BoundsResult {
            tiles,
            loaded_tiles: loaded,
            partial,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_math_matches_js() {
        assert_eq!(lon_lat_to_tile(-122.4194, 37.7749, 12), (655.0, 1583.0));
        let b = tile_to_bbox(12, 655, 1583);
        assert!(b.west < -122.4194 && b.east > -122.4194);
        assert!(b.south < 37.7749 && b.north > 37.7749);
        let tiles = tiles_for_bounds(
            &BBox {
                west: -122.5,
                south: 37.7,
                east: -122.4,
                north: 37.8,
            },
            12,
            64,
        );
        assert_eq!(
            tiles.first(),
            Some(&Tile {
                z: 12,
                x: 654,
                y: 1582
            })
        );
        assert_eq!(tiles.len(), 2 * 3);
    }

    #[test]
    fn clipping_keeps_inside_parts() {
        let b = BBox {
            west: 0.0,
            south: 0.0,
            east: 1.0,
            north: 1.0,
        };
        let parts = clip_tile_line(&[[-1.0, 0.5], [2.0, 0.5]], &b);
        assert_eq!(parts, vec![vec![[0.0, 0.5], [1.0, 0.5]]]);
        assert!(clip_tile_line(&[[0.5, 0.5], [0.50001, 0.5]], &b).is_empty());
    }

    #[test]
    fn hypot_matches_v8() {
        assert_eq!(hypot2(3.0, 4.0), 5.0);
        assert_eq!(hypot2(0.0, -0.0), 0.0);
    }
}
