//! NOAA nowCOAST observed weather imagery (port of
//! `server/providers/weather.js`).
//!
//! - `GET /api/weather/manifest?product=…` → frame times, bounds and URLs
//! - `GET /api/weather/tile?product&time&z&x&y[&size]` → a geographic WMS tile
//! - `GET /api/weather/image?product&time[&size][&bbox]` → a whole-extent
//!   image or a 2:1 detail window
//!
//! Fixed NOAA endpoints only; no client-supplied destinations. Upstream work
//! is shared per key, runs in at most eight slots behind a 96-entry queue,
//! is bounded by one deadline per operation, and is cancelled when its last
//! consumer disconnects. PNGs are validated and kept in a 128-entry, 16 MiB
//! LRU for a day.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use regex::Regex;
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;

use super::hazard_support::{
    Clock, FetchError, FetchRequest, FetchResponse, Fetcher, Redirect, encode_uri_component, jnum,
    js_number_string, mount, parse_date, read_bytes_capped, read_text_capped, reqwest_fetcher,
    sub_url, system_clock, to_iso,
};
use crate::common::{js, reply};
use crate::state::AppState;

const BASE: &str = "https://nowcoast.noaa.gov/geoserver/observations/";
const HOUR: i64 = 3_600_000;

/// One observed product.
pub struct Product {
    pub key: &'static str,
    service: &'static str,
    layer: &'static str,
    style: &'static str,
    title: &'static str,
    coverage: &'static str,
    description: &'static str,
    attribution: Option<&'static str>,
    metadata_ttl_ms: Option<i64>,
    image: (u32, u32),
}

pub const PRODUCTS: [Product; 4] = [
    Product {
        key: "lightning",
        service: "lightning_detection",
        layer: "ldn_lightning_strike_density",
        style: "lightning_density",
        title: "Lightning density · 15 min",
        coverage: "Pacific and Americas: 110°E across the dateline to 0°, 25°S–80°N; not global coverage.",
        description: "Observed 15-minute lightning strike density on an approximately 8 km grid, scaled as strikes/km²/min ×10³. Ground-network density, not individual GLM flashes.",
        attribution: Some("NOAA/NWS nowCOAST; derived from Vaisala NLDN/GLD360"),
        metadata_ttl_ms: Some(600_000),
        image: (4096, 2048),
    },
    Product {
        key: "radar",
        service: "weather_radar",
        layer: "conus_base_reflectivity_mosaic",
        style: "weather_radar_base_reflectivity",
        title: "CONUS radar reflectivity",
        coverage: "Contiguous United States; gaps do not establish absence of precipitation.",
        description: "Observed MRMS radar base reflectivity (dBZ), approximately 1 km and 4-minute updates; not a rainfall forecast.",
        attribution: None,
        metadata_ttl_ms: None,
        image: (4096, 2048),
    },
    Product {
        key: "clouds",
        service: "satellite",
        layer: "global_longwave_imagery_mosaic",
        style: "reflectance",
        title: "Global satellite infrared",
        coverage: "Global mosaic with incomplete polar coverage; nominal coverage 60°S–60°N.",
        description: "Longwave infrared cloud and land/sea temperature patterns, approximately 3 km; hourly updates with 2–3 hour source latency. Not a cloud-only mask.",
        attribution: None,
        metadata_ttl_ms: None,
        image: (2048, 1024),
    },
    Product {
        key: "clouds-regional",
        service: "satellite",
        layer: "goes_longwave_imagery",
        style: "goes-lir",
        title: "GOES regional satellite infrared",
        coverage: "GOES East/West regional North American coverage; not a global image.",
        description: "GOES-19/18 longwave infrared Band 14 cloud and surface temperature patterns, approximately 2 km and 5-minute updates. Not a cloud-only mask.",
        attribution: None,
        metadata_ttl_ms: None,
        image: (4096, 2048),
    },
];

fn product(key: &str) -> Option<&'static Product> {
    PRODUCTS.iter().find(|p| p.key == key)
}

/// Whole-extent images: 2:1 sizes up to each product's largest.
const IMAGE_SIZES: [&str; 3] = ["1024x512", "2048x1024", "4096x2048"];
/// Detail windows: any product up to 4096×2048.
const DETAIL_IMAGE: (u32, u32) = (4096, 2048);
const BBOX_STEP: f64 = 0.25;
const MAX_IMAGE_BYTES: usize = 16 * 1024 * 1024;

/// A coded failure (`failure(code, status)` in the JS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    pub code: &'static str,
    pub status: u16,
}

const fn failure(code: &'static str) -> Failure {
    Failure { code, status: 503 }
}

const fn bad_request(code: &'static str) -> Failure {
    Failure { code, status: 400 }
}

const BUSY: Failure = Failure {
    code: "weather_busy",
    status: 429,
};
const CANCELLED: Failure = failure("weather_request_cancelled");
const UNAVAILABLE: Failure = failure("weather_upstream_unavailable");
const INVALID_METADATA: Failure = failure("invalid_weather_metadata");

static OBSERVATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$").expect("regex")
});
static DOCTYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)<!DOCTYPE|<!ENTITY").expect("regex"));
static LAYER_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"</?(?:[A-Za-z0-9_.-]+:)?Layer\b[^>]*>").expect("regex"));
static NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"<(?:[A-Za-z0-9_.-]+:)?Name\s*>([^<]+)</(?:[A-Za-z0-9_.-]+:)?Name>").expect("regex")
});
static BOX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"<(?:[A-Za-z0-9_.-]+:)?EX_GeographicBoundingBox\s*>([\s\S]*?)</(?:[A-Za-z0-9_.-]+:)?EX_GeographicBoundingBox>",
    )
    .expect("regex")
});
static DIMENSION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"<(?:[A-Za-z0-9_.-]+:)?Dimension\b([^>]*)>([^<]*)</(?:[A-Za-z0-9_.-]+:)?Dimension>")
        .expect("regex")
});
static TIME_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\bname\s*=\s*["']time["']"#).expect("regex"));
static ISO_UNITS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\bunits\s*=\s*["']ISO8601["']"#).expect("regex"));
static DEFAULT_ATTR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\bdefault\s*=\s*["']([^"']+)["']"#).expect("regex"));
static BBOX_PART: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{1,3}(?:\.\d{1,6})?$").expect("regex"));
static TILE_COORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:0|[1-9]\d{0,2})$").expect("regex"));
static PNG_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^image/png(?:;|$)").expect("regex"));

/// `observationTime`: canonicalize only explicit UTC observations.
fn observation_time(value: Option<&str>) -> Option<String> {
    let value = value?;
    if !OBSERVATION.is_match(value) {
        return None;
    }
    let canonical = to_iso(parse_date(value)?);
    (canonical.replace(".000Z", "Z") == value.replace(".000Z", "Z")).then_some(canonical)
}

/// Product extent in degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bounds {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

impl Bounds {
    fn to_json(self) -> Value {
        json!({
            "west": jnum(self.west),
            "south": jnum(self.south),
            "east": jnum(self.east),
            "north": jnum(self.north),
        })
    }
}

/// Parsed capabilities for one product.
#[derive(Debug, Clone, PartialEq)]
pub struct Capabilities {
    pub bounds: Bounds,
    pub times: Vec<String>,
    pub allowed_times: Vec<String>,
}

/// `parseWeatherCapabilities`: the named leaf from bounded capabilities,
/// without resolving XML entities.
pub fn parse_weather_capabilities(
    xml: &str,
    product_key: &str,
    now_ms: i64,
) -> Result<Capabilities, Failure> {
    let spec = product(product_key).ok_or(INVALID_METADATA)?;
    if xml.encode_utf16().count() > 512 * 1024 || DOCTYPE.is_match(xml) {
        return Err(INVALID_METADATA);
    }
    struct Open {
        start: usize,
        nested: bool,
    }
    let mut stack: Vec<Open> = Vec::new();
    let mut leaf: Option<&str> = None;
    for (tags, found) in LAYER_TAG.find_iter(xml).enumerate() {
        if tags + 1 > 2048 {
            return Err(INVALID_METADATA);
        }
        let tag = found.as_str();
        if !tag.starts_with("</") {
            if let Some(top) = stack.last_mut() {
                top.nested = true;
            }
            if stack.len() >= 16 || tag.ends_with("/>") {
                return Err(INVALID_METADATA);
            }
            stack.push(Open {
                start: found.end(),
                nested: false,
            });
        } else {
            let opened = stack.pop().ok_or(INVALID_METADATA)?;
            if opened.nested {
                continue;
            }
            let candidate = &xml[opened.start..found.start()];
            let name = NAME.captures(candidate).map(|c| c[1].trim().to_string());
            if name.as_deref() == Some(spec.layer) {
                if leaf.is_some() {
                    return Err(INVALID_METADATA);
                }
                leaf = Some(candidate);
            }
        }
    }
    let leaf = match leaf {
        Some(leaf) if stack.is_empty() => leaf,
        _ => return Err(INVALID_METADATA),
    };
    let bounding = BOX
        .captures(leaf)
        .map(|c| c.get(1).map_or("", |m| m.as_str()).to_string());
    let read = |tag: &str| -> f64 {
        let Some(bounding) = &bounding else {
            return f64::NAN;
        };
        let pattern = Regex::new(&format!(
            r"<(?:[A-Za-z0-9_.-]+:)?{tag}\s*>([^<]+)</(?:[A-Za-z0-9_.-]+:)?{tag}>"
        ))
        .expect("regex");
        match pattern.captures(bounding).map(|c| c[1].trim().to_string()) {
            Some(value) if !value.is_empty() => js::number(&value),
            _ => f64::NAN,
        }
    };
    let bounds = Bounds {
        west: read("westBoundLongitude"),
        south: read("southBoundLatitude"),
        east: read("eastBoundLongitude"),
        north: read("northBoundLatitude"),
    };
    if ![bounds.west, bounds.south, bounds.east, bounds.north]
        .iter()
        .all(|n| n.is_finite())
        || bounds.west < -180.0
        || bounds.east > 180.0
        || bounds.south < -90.0
        || bounds.north > 90.0
        || bounds.west >= bounds.east
        || bounds.south >= bounds.north
    {
        return Err(INVALID_METADATA);
    }
    let dimensions: Vec<(String, String)> = DIMENSION
        .captures_iter(leaf)
        .map(|c| (c[1].to_string(), c[2].to_string()))
        .filter(|(attrs, _)| TIME_NAME.is_match(attrs))
        .collect();
    if dimensions.len() != 1 || !ISO_UNITS.is_match(&dimensions[0].0) {
        return Err(INVALID_METADATA);
    }
    let (attrs, body) = &dimensions[0];
    let raw: Vec<&str> = body.trim().split(',').collect();
    if raw.is_empty() || raw.len() > 512 {
        return Err(INVALID_METADATA);
    }
    let times: Vec<String> = raw
        .iter()
        .map(|value| observation_time(Some(value.trim())))
        .collect::<Option<_>>()
        .ok_or(INVALID_METADATA)?;
    if times
        .iter()
        .any(|t| parse_date(t).is_none_or(|ms| ms > now_ms + 5 * 60_000))
    {
        return Err(INVALID_METADATA);
    }
    let default_time = observation_time(
        DEFAULT_ATTR
            .captures(attrs)
            .as_ref()
            .map(|c| c.get(1).map_or("", |m| m.as_str())),
    );
    if default_time.is_none_or(|d| !times.contains(&d)) {
        return Err(INVALID_METADATA);
    }
    let mut seen = HashSet::new();
    let mut recent: Vec<String> = times
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .filter(|t| parse_date(t).is_some_and(|ms| now_ms - ms <= 24 * HOUR))
        .collect();
    recent.sort();
    if recent.len() > 26 {
        recent.drain(..recent.len() - 26);
    }
    if recent.is_empty() {
        return Err(failure("weather_observations_expired"));
    }
    let shown = recent[recent.len().saturating_sub(13)..].to_vec();
    Ok(Capabilities {
        bounds,
        times: shown,
        allowed_times: recent,
    })
}

/// `Math.round`: halves round toward +∞.
fn js_round(x: f64) -> f64 {
    let floor = x.floor();
    if x - floor >= 0.5 { floor + 1.0 } else { floor }
}

/// `weatherImageBbox`: a 2:1 detail window rounded to 0.25°; `None` when
/// absent.
pub fn weather_image_bbox(value: Option<&str>) -> Result<Option<[f64; 4]>, Failure> {
    let Some(value) = value else {
        return Ok(None);
    };
    let invalid = bad_request("invalid_weather_bbox");
    let parts: Vec<&str> = value.split(',').collect();
    if parts.len() != 4 || parts.iter().any(|p| !BBOX_PART.is_match(p)) {
        return Err(invalid);
    }
    // `+ 0` turns a rounded -0 into 0 so equal windows share one key.
    let rounded: Vec<f64> = parts
        .iter()
        .map(|p| js_round(js::number(p) / BBOX_STEP) * BBOX_STEP + 0.0)
        .collect();
    let [west, south, east, north] = [rounded[0], rounded[1], rounded[2], rounded[3]];
    if west < -180.0
        || east > 180.0
        || south < -90.0
        || north > 90.0
        || west >= east
        || south >= north
        || ((east - west) / (north - south) / 2.0 - 1.0).abs() > 0.01
    {
        return Err(invalid);
    }
    Ok(Some([west, south, east, north]))
}

/// `weatherTileBounds`: GeographicTilingScheme with two root tiles.
pub fn weather_tile_bounds(z: f64, x: f64, y: f64) -> Result<[f64; 4], Failure> {
    let integer = |n: f64| n.is_finite() && n.fract() == 0.0;
    if !(integer(z) && integer(x) && integer(y))
        || !(0.0..=6.0).contains(&z)
        || x < 0.0
        || y < 0.0
        || x >= 2f64.powf(z + 1.0)
        || y >= 2f64.powf(z)
    {
        return Err(bad_request("invalid_weather_tile"));
    }
    let span = 180.0 / 2f64.powf(z);
    Ok([
        -180.0 + x * span,
        90.0 - (y + 1.0) * span,
        -180.0 + (x + 1.0) * span,
        90.0 - y * span,
    ])
}

fn validate_png(bytes: Bytes, width: u32, height: u32) -> Result<Bytes, Failure> {
    let u32_at =
        |i: usize| u32::from_be_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    if bytes.len() < 33
        || bytes[..8] != [137, 80, 78, 71, 13, 10, 26, 10]
        || u32_at(8) != 13
        || &bytes[12..16] != b"IHDR"
        || u32_at(16) != width
        || u32_at(20) != height
    {
        return Err(failure("invalid_weather_image"));
    }
    Ok(bytes)
}

// ─── Proxy state ─────────────────────────────────────────────

#[derive(Clone)]
pub struct Deps {
    pub fetch: Fetcher,
    pub now: Clock,
    pub timeout: Duration,
}

#[derive(Clone)]
struct Metadata {
    caps: Capabilities,
    fetched_at: i64,
}

#[derive(Clone)]
enum OpValue {
    Text(String),
    Image(Bytes),
}

type OpResult = Result<OpValue, Failure>;
type Work = Box<dyn FnOnce() -> BoxFuture<'static, OpResult> + Send>;

struct Operation {
    id: u64,
    shared: Shared<BoxFuture<'static, OpResult>>,
    abort: AbortHandle,
    waiters: usize,
}

/// Map-with-insertion-order, oldest first.
struct Ordered<V> {
    entries: HashMap<String, V>,
    order: VecDeque<String>,
}

impl<V> Default for Ordered<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

impl<V> Ordered<V> {
    fn remove(&mut self, key: &str) -> Option<V> {
        let value = self.entries.remove(key)?;
        self.order.retain(|k| k != key);
        Some(value)
    }

    fn insert(&mut self, key: &str, value: V) {
        self.remove(key);
        self.entries.insert(key.to_string(), value);
        self.order.push_back(key.to_string());
    }

    fn pop_oldest(&mut self) -> Option<V> {
        let key = self.order.pop_front()?;
        self.entries.remove(&key)
    }
}

struct Tile {
    bytes: Bytes,
    at: i64,
}

#[derive(Default)]
struct Inner {
    metadata: HashMap<&'static str, Metadata>,
    attempts: HashMap<&'static str, i64>,
    operations: HashMap<String, Operation>,
    pending: VecDeque<(u64, oneshot::Sender<Slot>)>,
    active: usize,
    tiles: Ordered<Tile>,
    tile_bytes: usize,
    tile_failures: Ordered<i64>,
    next_id: u64,
}

struct Weather {
    deps: Deps,
    inner: Mutex<Inner>,
}

/// One of the eight upstream slots; dropping it frees the slot.
struct Slot {
    proxy: Arc<Weather>,
    armed: bool,
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.armed {
            self.proxy.inner.lock().active -= 1;
            self.proxy.pump();
        }
    }
}

/// A queued operation; dropped before it starts, it leaves the queue.
struct Queued {
    proxy: Arc<Weather>,
    id: u64,
}

impl Drop for Queued {
    fn drop(&mut self) {
        self.proxy
            .inner
            .lock()
            .pending
            .retain(|(id, _)| *id != self.id);
    }
}

/// One consumer of a shared operation; the last to disconnect cancels it.
struct Waiter<'a> {
    proxy: &'a Weather,
    key: String,
    id: u64,
    completed: bool,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        let abort = {
            let mut inner = self.proxy.inner.lock();
            let Some(operation) = inner
                .operations
                .get_mut(&self.key)
                .filter(|op| op.id == self.id)
            else {
                return;
            };
            operation.waiters -= 1;
            if operation.waiters > 0 || self.completed {
                return;
            }
            inner.operations.remove(&self.key).map(|op| op.abort)
        };
        if let Some(abort) = abort {
            abort.abort();
        }
    }
}

/// Clears a metadata attempt when its consumer disconnects mid-acquisition.
struct Attempt<'a> {
    proxy: &'a Weather,
    product: &'static str,
    armed: bool,
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.proxy.inner.lock().attempts.remove(self.product);
        }
    }
}

pub fn router(_state: &AppState) -> Router<AppState> {
    router_with(Deps {
        fetch: reqwest_fetcher(),
        now: system_clock(),
        timeout: Duration::from_millis(12_000),
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let proxy = Arc::new(Weather {
        deps,
        inner: Mutex::new(Inner::default()),
    });
    mount("/api/weather", move |request: Request| {
        let proxy = Arc::clone(&proxy);
        async move { proxy.handle(request).await }
    })
}

fn json_reply(status: u16, value: &Value) -> Response {
    let response = reply::json(status, value, Some("no-store"));
    if status == 429 {
        reply::with_header(response, "Retry-After", "2")
    } else {
        response
    }
}

fn error_reply(error: Failure) -> Response {
    if error.status == 400 || error.status == 429 {
        json_reply(error.status, &json!({ "error": error.code }))
    } else {
        json_reply(503, &json!({ "error": "weather_upstream_unavailable" }))
    }
}

/// `describe(product, value)`: the manifest body.
fn describe(spec: &Product, value: Option<(&Metadata, bool)>) -> Value {
    let time = value.and_then(|(m, _)| m.caps.times.last().cloned());
    let stale = value.is_none_or(|(_, stale)| stale);
    json!({
        "schemaVersion": 1,
        "product": spec.key,
        "title": spec.title,
        "coverage": spec.coverage,
        "description": spec.description,
        "source": "NOAA nowCOAST",
        "attribution": spec.attribution.unwrap_or("NOAA/NWS/NESDIS nowCOAST"),
        "bounds": value.map(|(m, _)| m.caps.bounds.to_json()),
        "times": value.map_or_else(Vec::new, |(m, _)| m.caps.times.clone()),
        "latest": time,
        "time": time,
        "observedAt": time,
        "fetchedAt": value.map(|(m, _)| m.fetched_at),
        "stale": stale,
        "unavailable": value.is_none(),
        "reason": match value {
            None => Value::from("Weather imagery unavailable"),
            Some((_, true)) => Value::from("Cached weather metadata; upstream unavailable"),
            Some((_, false)) => Value::Null,
        },
        "tileSize": 256,
        "maxLevel": 6,
        "tilingScheme": "geographic",
        "tileTemplate": time.as_ref().map(|t| format!(
            "/api/weather/tile?product={}&time={}&z={{z}}&x={{x}}&y={{y}}",
            spec.key,
            encode_uri_component(t)
        )),
        "imageUrl": time.as_ref().map(|t| format!(
            "/api/weather/image?product={}&time={}",
            spec.key,
            encode_uri_component(t)
        )),
        "imageSize": { "width": spec.image.0, "height": spec.image.1 },
    })
}

struct ImageShape {
    width: u32,
    height: u32,
    max_bytes: usize,
}

impl Weather {
    /// Start queued operations while slots are free.
    fn pump(self: &Arc<Self>) {
        loop {
            let sender = {
                let mut inner = self.inner.lock();
                if inner.active >= 8 {
                    return;
                }
                let Some((_, sender)) = inner.pending.pop_front() else {
                    return;
                };
                inner.active += 1;
                sender
            };
            let slot = Slot {
                proxy: Arc::clone(self),
                armed: true,
            };
            if let Err(mut slot) = sender.send(slot) {
                slot.armed = false;
                drop(slot);
                self.inner.lock().active -= 1;
            }
        }
    }

    /// `shared(key, work, clientSignal)`: join or start the operation.
    async fn shared(self: &Arc<Self>, key: &str, work: Work) -> OpResult {
        let (id, shared) = {
            let mut inner = self.inner.lock();
            if !inner.operations.contains_key(key) {
                if inner.operations.len() >= 120 || inner.pending.len() >= 96 {
                    return Err(BUSY);
                }
                inner.next_id += 1;
                let id = inner.next_id;
                let start = if inner.active < 8 {
                    inner.active += 1;
                    Ok(Slot {
                        proxy: Arc::clone(self),
                        armed: true,
                    })
                } else {
                    let (sender, receiver) = oneshot::channel();
                    inner.pending.push_back((id, sender));
                    Err(receiver)
                };
                let this = Arc::clone(self);
                let owned_key = key.to_string();
                let deadline = self.deps.timeout;
                let task = tokio::spawn(async move {
                    let run = async {
                        let _slot = match start {
                            Ok(slot) => slot,
                            Err(receiver) => {
                                let _queued = Queued {
                                    proxy: Arc::clone(&this),
                                    id,
                                };
                                receiver.await.map_err(|_| CANCELLED)?
                            }
                        };
                        work().await
                    };
                    let result = tokio::time::timeout(deadline, run)
                        .await
                        .unwrap_or(Err(CANCELLED));
                    let mut inner = this.inner.lock();
                    if inner
                        .operations
                        .get(&owned_key)
                        .is_some_and(|op| op.id == id)
                    {
                        inner.operations.remove(&owned_key);
                    }
                    result
                });
                let abort = task.abort_handle();
                let shared = async move { task.await.unwrap_or(Err(CANCELLED)) }
                    .boxed()
                    .shared();
                inner.operations.insert(
                    key.to_string(),
                    Operation {
                        id,
                        shared,
                        abort,
                        waiters: 0,
                    },
                );
            }
            let operation = inner.operations.get_mut(key).expect("operation exists");
            operation.waiters += 1;
            (operation.id, operation.shared.clone())
        };
        let mut waiter = Waiter {
            proxy: self,
            key: key.to_string(),
            id,
            completed: false,
        };
        let result = shared.await;
        waiter.completed = true;
        result
    }

    /// One fixed upstream read: capabilities text, or a validated PNG.
    fn upstream(&self, url: String, image: Option<ImageShape>) -> Work {
        let fetch = self.deps.fetch.clone();
        Box::new(move || {
            async move {
                let accept = if image.is_some() {
                    "image/png"
                } else {
                    "application/xml,text/xml"
                };
                let request = FetchRequest::get(url)
                    .redirect(Redirect::Error)
                    .header("Accept", accept);
                let response: FetchResponse = fetch(request).await.map_err(|_| UNAVAILABLE)?;
                let png_ok = image.is_none()
                    || PNG_TYPE.is_match(response.header("content-type").unwrap_or(""));
                if !response.ok() || !png_ok {
                    return Err(UNAVAILABLE);
                }
                match image {
                    Some(shape) => {
                        let bytes = read_bytes_capped(response, shape.max_bytes)
                            .await
                            .map_err(|_: FetchError| UNAVAILABLE)?;
                        validate_png(bytes, shape.width, shape.height).map(OpValue::Image)
                    }
                    None => read_text_capped(response, 512 * 1024)
                        .await
                        .map(OpValue::Text)
                        .map_err(|_| UNAVAILABLE),
                }
            }
            .boxed()
        })
    }

    /// `getMetadata(product)`: fresh, re-acquired, or bounded last-good.
    async fn get_metadata(
        self: &Arc<Self>,
        spec: &'static Product,
    ) -> Result<(Metadata, bool), Failure> {
        let ttl = spec.metadata_ttl_ms.unwrap_or(120_000);
        let op_key = format!("metadata:{}", spec.service);
        let old = {
            let inner = self.inner.lock();
            inner.metadata.get(spec.key).cloned()
        };
        if let Some(old) = &old
            && (self.deps.now)() - old.fetched_at < ttl
        {
            return Ok((old.clone(), false));
        }
        let mut attempt = Attempt {
            proxy: self,
            product: spec.key,
            armed: false,
        };
        let result: Result<Metadata, Failure> = async {
            {
                let mut inner = self.inner.lock();
                let now = (self.deps.now)();
                let recent = inner
                    .attempts
                    .get(spec.key)
                    .is_some_and(|at| now - at < 30_000);
                if recent && !inner.operations.contains_key(&op_key) {
                    return Err(UNAVAILABLE);
                }
                inner.attempts.insert(spec.key, now);
            }
            attempt.armed = true;
            let url = format!(
                "{BASE}{}/ows?service=WMS&version=1.3.0&request=GetCapabilities",
                spec.service
            );
            let xml = match self.shared(&op_key, self.upstream(url, None)).await? {
                OpValue::Text(text) => text,
                OpValue::Image(_) => return Err(UNAVAILABLE),
            };
            let caps = parse_weather_capabilities(&xml, spec.key, (self.deps.now)())?;
            let value = Metadata {
                caps,
                fetched_at: (self.deps.now)(),
            };
            self.inner.lock().metadata.insert(spec.key, value.clone());
            Ok(value)
        }
        .await;
        attempt.armed = false;
        match result {
            Ok(value) => Ok((value, false)),
            Err(error) if error.status == 429 => Err(error),
            Err(_) => match old {
                Some(old) if (self.deps.now)() - old.fetched_at <= HOUR => Ok((old, true)),
                _ => Err(failure("weather_metadata_unavailable")),
            },
        }
    }

    fn remember_tile(&self, key: &str, bytes: Bytes) {
        let now = (self.deps.now)();
        let mut inner = self.inner.lock();
        if let Some(old) = inner.tiles.remove(key) {
            inner.tile_bytes -= old.bytes.len();
        }
        while inner.tiles.entries.len() >= 128 || inner.tile_bytes + bytes.len() > 16 * 1024 * 1024
        {
            match inner.tiles.pop_oldest() {
                Some(oldest) => inner.tile_bytes -= oldest.bytes.len(),
                None => break,
            }
        }
        inner.tile_bytes += bytes.len();
        inner.tiles.insert(key, Tile { bytes, at: now });
    }

    async fn handle(self: Arc<Self>, request: Request) -> Response {
        match self.route(request).await {
            Ok(response) => response,
            Err(error) => error_reply(error),
        }
    }

    async fn route(self: &Arc<Self>, request: Request) -> Result<Response, Failure> {
        let raw_url = sub_url(&request, "/api/weather");
        let base = url::Url::parse("http://localhost/").expect("base");
        let Ok(url) = base.join(&raw_url) else {
            return Err(UNAVAILABLE);
        };
        if request.method() != http::Method::GET {
            return Ok(json_reply(405, &json!({ "error": "method_not_allowed" })));
        }
        let pathname = url.path();
        if !["/manifest", "/tile", "/image"].contains(&pathname) {
            return Ok(json_reply(404, &json!({ "error": "not_found" })));
        }
        let allowed: &[&str] = match pathname {
            "/manifest" => &["product"],
            "/image" => &["product", "time", "size", "bbox"],
            _ => &["product", "time", "z", "x", "y", "size"],
        };
        let params: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let get = |key: &str| {
            params
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
        };
        if raw_url.encode_utf16().count() > 512
            || params.iter().any(|(key, _)| {
                !allowed.contains(&key.as_str())
                    || params.iter().filter(|(k, _)| k == key).count() != 1
            })
        {
            return Ok(json_reply(
                400,
                &json!({ "error": "invalid_weather_query" }),
            ));
        }
        let Some(spec) = get("product").and_then(product) else {
            return Ok(json_reply(
                400,
                &json!({ "error": "unknown_weather_product" }),
            ));
        };
        let whole_image = pathname == "/image";
        let detail_box = if whole_image {
            weather_image_bbox(get("bbox"))?
        } else {
            None
        };
        let largest = if detail_box.is_some() {
            DETAIL_IMAGE
        } else {
            spec.image
        };
        let default_size = if whole_image {
            format!("{}x{}", largest.0, largest.1)
        } else {
            "256".to_string()
        };
        let size = get("size").map_or(default_size, str::to_string);
        let size_ok = if whole_image {
            IMAGE_SIZES.contains(&size.as_str())
                && js::parse_int(&size).is_some_and(|w| w <= i64::from(largest.0))
        } else {
            ["256", "512", "1024"].contains(&size.as_str())
        };
        if !size_ok {
            return Err(bad_request(if whole_image {
                "invalid_weather_image_size"
            } else {
                "invalid_weather_tile_size"
            }));
        }
        if pathname == "/manifest" {
            return match self.get_metadata(spec).await {
                Ok((value, stale)) => Ok(json_reply(200, &describe(spec, Some((&value, stale))))),
                Err(error) if error.status == 429 => Err(error),
                Err(_) => Ok(json_reply(200, &describe(spec, None))),
            };
        }
        let coords: Option<[&str; 3]> = (!whole_image).then(|| {
            [
                get("z").unwrap_or(""),
                get("x").unwrap_or(""),
                get("y").unwrap_or(""),
            ]
        });
        if coords.is_some_and(|c| c.iter().any(|v| !TILE_COORD.is_match(v))) {
            return Err(bad_request("invalid_weather_tile"));
        }
        let tile_bounds = match coords {
            Some([z, x, y]) => Some(weather_tile_bounds(
                js::number(z),
                js::number(x),
                js::number(y),
            )?),
            None => None,
        };
        let time = get("time").unwrap_or("");
        if time.is_empty() || observation_time(Some(time)).as_deref() != Some(time) {
            return Err(bad_request("invalid_weather_time"));
        }
        let (value, stale) = self.get_metadata(spec).await?;
        let now = (self.deps.now)();
        if !value.caps.allowed_times.iter().any(|t| t == time)
            || parse_date(time).is_none_or(|ms| now - ms > 24 * HOUR)
        {
            return Err(bad_request("unknown_weather_time"));
        }
        let b = value.caps.bounds;
        if detail_box
            .is_some_and(|d| d[0] < b.west || d[1] < b.south || d[2] > b.east || d[3] > b.north)
        {
            return Err(bad_request("invalid_weather_bbox"));
        }
        // A single advertised extent keeps NOAA's global reflectance
        // contrast consistent; independently normalized tiles create seams.
        let bbox = detail_box.unwrap_or_else(|| {
            if whole_image {
                [b.west, b.south, b.east, b.north]
            } else {
                tile_bounds.expect("tile bounds")
            }
        });
        let (width, height) = if whole_image {
            let (w, h) = size.split_once('x').unwrap_or(("0", "0"));
            (w.parse::<u32>().unwrap_or(0), h.parse::<u32>().unwrap_or(0))
        } else {
            let n = size.parse::<u32>().unwrap_or(0);
            (n, n)
        };
        let shape = ImageShape {
            width,
            height,
            max_bytes: if whole_image {
                MAX_IMAGE_BYTES
            } else {
                (1024 * 1024).max(width as usize * width as usize * 4 + 65_536)
            },
        };
        let bbox_text = bbox
            .iter()
            .map(|n| js_number_string(*n))
            .collect::<Vec<_>>()
            .join(",");
        let key = match coords {
            None => format!("{}:{time}:image:{size}:{bbox_text}", spec.key),
            Some(c) => format!("{}:{time}:tile:{size}:{}", spec.key, c.join("/")),
        };
        let cached = {
            let mut inner = self.inner.lock();
            let fresh = inner
                .tiles
                .entries
                .get(&key)
                .is_some_and(|tile| now - tile.at <= 24 * HOUR);
            if fresh {
                // Bump to most recently used.
                let tile = inner.tiles.remove(&key).expect("fresh tile");
                let bytes = tile.bytes.clone();
                inner.tiles.insert(&key, tile);
                Some(bytes)
            } else {
                None
            }
        };
        let bytes = match cached {
            Some(bytes) => bytes,
            None => {
                // Never ask nearestValue=1 WMS to silently replace an expired frame.
                if stale {
                    return Err(failure("weather_metadata_stale"));
                }
                let failed_recently = self
                    .inner
                    .lock()
                    .tile_failures
                    .entries
                    .get(&key)
                    .is_some_and(|at| now - at < 30_000);
                if failed_recently {
                    return Err(UNAVAILABLE);
                }
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("service", "WMS")
                    .append_pair("version", "1.1.1")
                    .append_pair("request", "GetMap")
                    .append_pair("layers", spec.layer)
                    .append_pair("styles", spec.style)
                    .append_pair("srs", "EPSG:4326")
                    .append_pair("bbox", &bbox_text)
                    .append_pair("width", &shape.width.to_string())
                    .append_pair("height", &shape.height.to_string())
                    .append_pair("format", "image/png")
                    .append_pair("transparent", "true")
                    .append_pair("time", time)
                    .finish();
                let upstream_url = format!("{BASE}{}/ows?{query}", spec.service);
                match self
                    .shared(
                        &format!("image:{key}"),
                        self.upstream(upstream_url, Some(shape)),
                    )
                    .await
                {
                    Ok(OpValue::Image(bytes)) => {
                        self.remember_tile(&key, bytes.clone());
                        bytes
                    }
                    Ok(OpValue::Text(_)) => return Err(UNAVAILABLE),
                    Err(error) => {
                        if error.status != 429 {
                            let mut inner = self.inner.lock();
                            inner.tile_failures.insert(&key, (self.deps.now)());
                            while inner.tile_failures.entries.len() > 128 {
                                inner.tile_failures.pop_oldest();
                            }
                        }
                        return Err(error);
                    }
                }
            }
        };
        let response = reply::raw(
            200,
            "image/png",
            bytes,
            Some("public, max-age=86400, immutable"),
        );
        Ok(reply::with_header(
            response,
            "X-Content-Type-Options",
            "nosniff",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::super::hazard_support::testing::{self, Calls};
    use super::*;

    const NOW: i64 = 1_789_524_720_000; // 2026-09-16T02:12:00Z
    const TIME: &str = "2026-09-16T02:08:00.000Z";
    const NAMES: [&str; 4] = [
        "conus_base_reflectivity_mosaic",
        "global_longwave_imagery_mosaic",
        "goes_longwave_imagery",
        "ldn_lightning_strike_density",
    ];

    fn leaf_with(name: &str, times: &[&str], bounds: (f64, f64, f64, f64)) -> String {
        let (west, east, south, north) = bounds;
        let [w, e, s, n] = [west, east, south, north].map(js_number_string);
        format!(
            "<Layer><Name>{name}</Name><Title>Unused feed title</Title><EX_GeographicBoundingBox><westBoundLongitude>{w}</westBoundLongitude><eastBoundLongitude>{e}</eastBoundLongitude><southBoundLatitude>{s}</southBoundLatitude><northBoundLatitude>{n}</northBoundLatitude></EX_GeographicBoundingBox><Dimension name=\"time\" default=\"{}\" units=\"ISO8601\" nearestValue=\"1\">{}</Dimension></Layer>",
            times.last().unwrap(),
            times.join(",")
        )
    }

    fn leaf(name: &str, times: &[&str]) -> String {
        leaf_with(name, times, (-130.0, -60.0, 20.0, 55.0))
    }

    fn xml_times(times: &[&str]) -> String {
        format!(
            "<WMS_Capabilities><Capability><Layer><Title>Parent</Title>{}</Layer></Capability></WMS_Capabilities>",
            NAMES.iter().map(|n| leaf(n, times)).collect::<String>()
        )
    }

    fn xml() -> String {
        xml_times(&[TIME])
    }

    fn png(width: u32, height: u32, length: usize) -> Bytes {
        let mut bytes = vec![0u8; length];
        bytes[..8].copy_from_slice(&[137, 80, 78, 71, 13, 10, 26, 10]);
        bytes[8..12].copy_from_slice(&13u32.to_be_bytes());
        bytes[12..16].copy_from_slice(b"IHDR");
        bytes[16..20].copy_from_slice(&width.to_be_bytes());
        bytes[20..24].copy_from_slice(&height.to_be_bytes());
        Bytes::from(bytes)
    }

    fn image(bytes: Bytes) -> FetchResponse {
        FetchResponse::from_bytes(200, &[("content-type", "image/png")], bytes)
    }

    fn text(body: impl Into<Bytes>) -> FetchResponse {
        FetchResponse::from_bytes(200, &[], body)
    }

    fn tile_url(product: &str, time: &str, z: &str, x: &str, y: &str) -> String {
        format!(
            "/tile?product={product}&time={}&z={z}&x={x}&y={y}",
            encode_uri_component(time)
        )
    }

    fn tile() -> String {
        tile_url("radar", TIME, "0", "0", "0")
    }

    fn whole_image(time: &str, product: &str) -> String {
        format!(
            "/image?product={product}&time={}",
            encode_uri_component(time)
        )
    }

    fn param(request: &FetchRequest, key: &str) -> Option<String> {
        url::Url::parse(&request.url)
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
    }

    /// Capabilities for `GetCapabilities`, a PNG sized as requested otherwise.
    fn sized_fetch(calls: Calls) -> Fetcher {
        testing::fetcher(calls, |request| {
            if request.url.contains("GetCapabilities") {
                return Ok(text(xml()));
            }
            let w: u32 = param(request, "width").unwrap().parse().unwrap();
            let h: u32 = param(request, "height").unwrap().parse().unwrap();
            Ok(image(png(w, h, 33)))
        })
    }

    struct Harness {
        router: Router<AppState>,
        state: AppState,
    }

    type Reply = (u16, http::HeaderMap, Bytes);

    impl Harness {
        fn new(fetch: Fetcher, now: Clock) -> Self {
            Self::with_timeout(fetch, now, Duration::from_millis(12_000))
        }

        fn with_timeout(fetch: Fetcher, now: Clock, timeout: Duration) -> Self {
            Self {
                router: router_with(Deps {
                    fetch,
                    now,
                    timeout,
                }),
                state: testing::state(),
            }
        }

        async fn request(&self, path: &str, method: &str) -> Reply {
            testing::send(
                &self.router,
                &self.state,
                method,
                &format!("/api/weather{path}"),
                "127.0.0.1",
            )
            .await
        }

        async fn get(&self, path: &str) -> Reply {
            self.request(path, "GET").await
        }

        async fn json(&self, path: &str) -> Value {
            serde_json::from_slice(&self.get(path).await.2).unwrap()
        }
    }

    fn fixed(now: i64) -> Clock {
        Arc::new(move || now)
    }

    fn width_of(body: &Bytes) -> u32 {
        u32::from_be_bytes([body[16], body[17], body[18], body[19]])
    }

    fn iso_minutes_before(minutes: i64) -> String {
        to_iso(NOW - minutes * 60_000)
    }

    #[tokio::test]
    async fn lightning_density_uses_a_fixed_product_with_attribution_and_ten_minute_ttl() {
        let (clock, now) = testing::manual_clock(NOW);
        let calls: Calls = Default::default();
        let harness = Harness::new(sized_fetch(Arc::clone(&calls)), clock);
        let manifest = harness.json("/manifest?product=lightning").await;
        assert_eq!(manifest["product"], json!("lightning"));
        assert!(manifest["title"].as_str().unwrap().contains("density"));
        assert!(
            manifest["description"]
                .as_str()
                .unwrap()
                .contains("not individual GLM flashes")
        );
        assert!(
            manifest["coverage"]
                .as_str()
                .unwrap()
                .contains("not global")
        );
        assert!(
            manifest["attribution"]
                .as_str()
                .unwrap()
                .contains("Vaisala")
        );
        assert_eq!(manifest["observedAt"], json!(TIME));
        assert_eq!(
            manifest["imageUrl"],
            json!(format!("/api/weather{}", whole_image(TIME, "lightning")))
        );
        assert_eq!(
            manifest["imageSize"],
            json!({ "width": 4096, "height": 2048 })
        );
        assert_eq!(
            harness
                .get(&tile_url("lightning", TIME, "0", "0", "0"))
                .await
                .0,
            200
        );
        {
            let calls = calls.lock();
            let map = url::Url::parse(&calls[1].url).unwrap();
            assert_eq!(
                map.origin().ascii_serialization(),
                "https://nowcoast.noaa.gov"
            );
            assert_eq!(
                map.path(),
                "/geoserver/observations/lightning_detection/ows"
            );
            assert_eq!(param(&calls[1], "layers").as_deref(), Some(NAMES[3]));
            assert_eq!(
                param(&calls[1], "styles").as_deref(),
                Some("lightning_density")
            );
            assert_eq!(param(&calls[1], "time").as_deref(), Some(TIME));
            assert_eq!(param(&calls[1], "width").as_deref(), Some("256"));
        }
        now.fetch_add(599_000, Ordering::SeqCst);
        harness.get("/manifest?product=lightning").await;
        assert_eq!(calls.lock().len(), 2);
        now.fetch_add(2000, Ordering::SeqCst);
        harness.get("/manifest?product=lightning").await;
        assert_eq!(calls.lock().len(), 3);
    }

    #[test]
    fn capabilities_select_exact_leaves_and_preserve_irregular_observation_times() {
        let older = "2026-09-16T02:04:14.000Z";
        let text = format!(
            "<WMS_Capabilities><Layer>{}{}</Layer></WMS_Capabilities>",
            leaf(NAMES[1], &["2026-09-16T01:00:00.000Z"]),
            leaf(NAMES[0], &[TIME, older, older])
        );
        let result = parse_weather_capabilities(&text, "radar", NOW).unwrap();
        assert_eq!(result.times, vec![older.to_string(), TIME.to_string()]);
        assert_eq!(
            result.bounds,
            Bounds {
                west: -130.0,
                east: -60.0,
                south: 20.0,
                north: 55.0
            }
        );
        assert_eq!(
            parse_weather_capabilities(&text, "clouds", NOW)
                .unwrap()
                .times,
            vec!["2026-09-16T01:00:00.000Z".to_string()]
        );
    }

    #[test]
    fn metadata_caps_advertised_frames_without_fabricating_interval_timestamps() {
        let times: Vec<String> = (0..40).map(|i| iso_minutes_before(40 - i)).collect();
        let refs: Vec<&str> = times.iter().map(String::as_str).collect();
        let parsed = parse_weather_capabilities(&xml_times(&refs), "radar", NOW).unwrap();
        assert_eq!(parsed.times, times[27..].to_vec());
        assert_eq!(parsed.allowed_times, times[14..].to_vec());
        let bad = [
            xml_times(&["2026-09-16T01:00:00Z/2026-09-16T02:00:00Z/PT5M"]),
            xml_times(&["2026-02-30T00:00:00Z"]),
            xml_times(&["2026-09-17T00:00:00Z"]),
            xml_times(&["2026-09-14T00:00:00Z"]),
            xml().replace(
                &format!("default=\"{TIME}"),
                "default=\"2026-09-16T02:00:00.000Z",
            ),
            format!(
                "<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>{}",
                xml()
            ),
            xml().replace("<westBoundLongitude>-130", "<westBoundLongitude>-999"),
            xml().replace(NAMES[0], "some_other_product"),
            format!("{}{}", xml(), leaf(NAMES[0], &[TIME])),
            "x".repeat(512 * 1024 + 1),
        ];
        for text in bad {
            assert!(parse_weather_capabilities(&text, "radar", NOW).is_err());
        }
    }

    #[test]
    fn geographic_tiles_use_a_2x1_root_and_reject_out_of_range_input() {
        assert_eq!(
            weather_tile_bounds(0.0, 0.0, 0.0),
            Ok([-180.0, -90.0, 0.0, 90.0])
        );
        assert_eq!(
            weather_tile_bounds(0.0, 1.0, 0.0),
            Ok([0.0, -90.0, 180.0, 90.0])
        );
        assert_eq!(
            weather_tile_bounds(1.0, 2.0, 1.0),
            Ok([0.0, -90.0, 90.0, 0.0])
        );
        assert_eq!(
            weather_tile_bounds(6.0, 127.0, 63.0),
            Ok([177.1875, -90.0, 180.0, -87.1875])
        );
        for (z, x, y) in [
            (7.0, 0.0, 0.0),
            (0.0, 2.0, 0.0),
            (0.0, 0.0, 1.0),
            (-1.0, 0.0, 0.0),
            (0.0, -1.0, 0.0),
            (1.5, 0.0, 0.0),
        ] {
            assert!(weather_tile_bounds(z, x, y).is_err());
        }
    }

    #[tokio::test]
    async fn manifest_provides_honest_coverage_frame_times_and_a_same_origin_template() {
        let calls: Calls = Default::default();
        let harness = Harness::new(sized_fetch(Arc::clone(&calls)), fixed(NOW));
        for product in ["radar", "clouds", "clouds-regional"] {
            let (status, headers, body) =
                harness.get(&format!("/manifest?product={product}")).await;
            assert_eq!(status, 200);
            assert_eq!(headers["cache-control"], "no-store");
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["product"], json!(product));
            for key in ["latest", "time", "observedAt"] {
                assert_eq!(value[key], json!(TIME));
            }
            assert_eq!(value["fetchedAt"], json!(NOW));
            assert_eq!(value["unavailable"], json!(false));
            assert_eq!(value["stale"], json!(false));
            assert_eq!(value["times"], json!([TIME]));
            assert_eq!(value["tileSize"], json!(256));
            assert_eq!(value["maxLevel"], json!(6));
            assert_eq!(value["tilingScheme"], json!("geographic"));
            assert_eq!(
                value["tileTemplate"],
                json!(format!(
                    "/api/weather/tile?product={product}&time={}&z={{z}}&x={{x}}&y={{y}}",
                    encode_uri_component(TIME)
                ))
            );
            let description = value["description"].as_str().unwrap();
            assert!(description.contains(if product == "radar" {
                "not a rainfall forecast"
            } else {
                "Not a cloud-only mask"
            }));
            if product == "clouds" {
                assert!(description.contains("2–3 hour"));
            }
            assert_eq!(
                value["imageUrl"],
                json!(format!("/api/weather{}", whole_image(TIME, product)))
            );
            assert_eq!(
                value["imageSize"],
                if product == "clouds" {
                    json!({ "width": 2048, "height": 1024 })
                } else {
                    json!({ "width": 4096, "height": 2048 })
                }
            );
            assert_eq!(
                value["bounds"],
                json!({ "west": -130, "south": 20, "east": -60, "north": 55 })
            );
        }
        for call in calls.lock().iter() {
            assert_eq!(
                url::Url::parse(&call.url).unwrap().host_str(),
                Some("nowcoast.noaa.gov")
            );
            assert_eq!(call.redirect, Redirect::Error);
        }
    }

    #[tokio::test]
    async fn invalid_enums_destinations_and_tile_inputs_never_fetch_upstream() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Err(FetchError::Network("should not fetch".into()))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        let urls = [
            "/manifest".to_string(),
            "/manifest?product=constructor".into(),
            "/manifest?product=radar&product=clouds".into(),
            "/manifest?product=radar&url=https://127.0.0.1".into(),
            tile() + "&host=169.254.169.254",
            tile() + "&bbox=0,0,1,1",
            tile() + "&layers=other",
            tile_url("radar", TIME, "7", "0", "0"),
            tile_url("radar", TIME, "0", "2", "0"),
            tile_url("radar", TIME, "0", "0", "1"),
            tile_url("radar", TIME, "00", "0", "0"),
            tile_url("radar", TIME, "0", "-1", "0"),
            tile_url("radar", "latest", "0", "0", "0"),
            tile_url("radar", "2026-09-16T02:08:00Z", "0", "0", "0"),
        ];
        for url in urls {
            assert_eq!(harness.get(&url).await.0, 400, "{url}");
        }
        assert_eq!(
            harness.request("/manifest?product=radar", "POST").await.0,
            405
        );
        assert_eq!(harness.get("/unknown?product=radar").await.0, 404);
        assert_eq!(calls.lock().len(), 0);
    }

    #[tokio::test]
    async fn tile_fixes_upstream_parameters_and_caches_exact_frame_bytes() {
        let calls: Calls = Default::default();
        let harness = Harness::new(sized_fetch(Arc::clone(&calls)), fixed(NOW));
        let url = tile_url("radar", TIME, "1", "2", "1");
        let (status, headers, body) = harness.get(&url).await;
        assert_eq!(status, 200);
        assert_eq!(headers["content-type"], "image/png");
        assert_eq!(headers["x-content-type-options"], "nosniff");
        {
            let calls = calls.lock();
            let map = calls
                .iter()
                .find(|c| param(c, "request").as_deref() == Some("GetMap"))
                .unwrap();
            let parsed = url::Url::parse(&map.url).unwrap();
            assert_eq!(parsed.path(), "/geoserver/observations/weather_radar/ows");
            let pairs: Vec<(String, String)> = parsed.query_pairs().into_owned().collect();
            let expected: Vec<(String, String)> = [
                ("service", "WMS"),
                ("version", "1.1.1"),
                ("request", "GetMap"),
                ("layers", NAMES[0]),
                ("styles", "weather_radar_base_reflectivity"),
                ("srs", "EPSG:4326"),
                ("bbox", "0,-90,90,0"),
                ("width", "256"),
                ("height", "256"),
                ("format", "image/png"),
                ("transparent", "true"),
                ("time", TIME),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            assert_eq!(pairs, expected);
            assert_eq!(map.redirect, Redirect::Error);
        }
        assert_eq!(harness.get(&url).await.2, body);
        assert_eq!(calls.lock().len(), 2);
        let unknown = harness
            .get(&tile_url(
                "radar",
                "2026-09-16T02:07:00.000Z",
                "0",
                "0",
                "0",
            ))
            .await;
        assert_eq!(unknown.0, 400);
        assert_eq!(calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn capabilities_ttl_and_bounded_last_good_state() {
        let (clock, now) = testing::manual_clock(NOW);
        let fail = Arc::new(AtomicBool::new(false));
        let calls: Calls = Default::default();
        let flag = Arc::clone(&fail);
        let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
            if flag.load(Ordering::SeqCst) {
                return Err(FetchError::Network("secret upstream details".into()));
            }
            Ok(text(xml()))
        });
        let harness = Harness::new(fetch, clock);
        let first = harness.json("/manifest?product=radar").await;
        now.fetch_add(119_000, Ordering::SeqCst);
        assert_eq!(harness.json("/manifest?product=radar").await, first);
        assert_eq!(calls.lock().len(), 1);
        fail.store(true, Ordering::SeqCst);
        now.fetch_add(2000, Ordering::SeqCst);
        let stale = harness.json("/manifest?product=radar").await;
        assert_eq!(stale["stale"], json!(true));
        assert_eq!(stale["unavailable"], json!(false));
        assert_eq!(stale["observedAt"], json!(TIME));
        assert_eq!(stale["fetchedAt"], json!(NOW));
        assert!(!stale.to_string().contains("secret"));
        harness.get("/manifest?product=radar").await;
        assert_eq!(
            calls.lock().len(),
            2,
            "failed metadata retries are throttled"
        );
        now.store(NOW + 3_600_001, Ordering::SeqCst);
        let unavailable = harness.json("/manifest?product=radar").await;
        assert_eq!(unavailable["unavailable"], json!(true));
        assert_eq!(unavailable["times"], json!([]));
        assert_eq!(unavailable["observedAt"], Value::Null);
        assert_eq!(unavailable["imageUrl"], Value::Null);
    }

    #[tokio::test]
    async fn stale_metadata_serves_only_already_cached_exact_tiles() {
        let (clock, now) = testing::manual_clock(NOW);
        let fail = Arc::new(AtomicBool::new(false));
        let maps = Arc::new(AtomicUsize::new(0));
        let (flag, counter) = (Arc::clone(&fail), Arc::clone(&maps));
        let fetch = testing::fetcher(Default::default(), move |request| {
            if request.url.contains("GetCapabilities") {
                if flag.load(Ordering::SeqCst) {
                    return Err(FetchError::Network("down".into()));
                }
                return Ok(text(xml()));
            }
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(image(png(256, 256, 33)))
        });
        let harness = Harness::new(fetch, clock);
        assert_eq!(harness.get(&tile()).await.0, 200);
        now.fetch_add(120_001, Ordering::SeqCst);
        fail.store(true, Ordering::SeqCst);
        assert_eq!(harness.get(&tile()).await.0, 200);
        assert_eq!(
            harness.get(&tile_url("radar", TIME, "0", "1", "0")).await.0,
            503
        );
        assert_eq!(maps.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn simultaneous_metadata_coalesces_service_acquisition_per_product() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Fetcher = {
            let (gate, calls) = (Arc::clone(&gate), Arc::clone(&calls));
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                let gate = Arc::clone(&gate);
                Box::pin(async move {
                    let _ = gate.acquire().await;
                    Ok(text(xml()))
                })
            })
        };
        let harness = Harness::new(fetch, fixed(NOW));
        let release = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            gate.add_permits(10);
        };
        let (one, two, ()) = tokio::join!(
            harness.json("/manifest?product=clouds"),
            harness.json("/manifest?product=clouds-regional"),
            release
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(one["product"], json!("clouds"));
        assert_eq!(two["product"], json!("clouds-regional"));
        assert_ne!(one["title"], two["title"]);
    }

    #[tokio::test]
    async fn shared_tile_survives_one_disconnect_but_aborts_when_all_leave() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let fetch: Fetcher = {
            let (gate, dropped) = (Arc::clone(&gate), Arc::clone(&dropped));
            Arc::new(move |request: FetchRequest| {
                let (gate, dropped) = (Arc::clone(&gate), Arc::clone(&dropped));
                Box::pin(async move {
                    if request.url.contains("GetCapabilities") {
                        return Ok(text(xml()));
                    }
                    let guard = CountOnDrop(dropped);
                    gate.acquire().await.unwrap().forget();
                    std::mem::forget(guard);
                    Ok(image(png(256, 256, 33)))
                })
            })
        };
        let harness = Arc::new(Harness::new(fetch, fixed(NOW)));
        harness.get("/manifest?product=radar").await;
        let h = Arc::clone(&harness);
        let a = tokio::spawn(async move { h.get(&tile()).await });
        let h = Arc::clone(&harness);
        let b = tokio::spawn(async move { h.get(&tile()).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        a.abort();
        let _ = a.await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        gate.add_permits(1);
        assert_eq!(b.await.unwrap().0, 200);
        let h = Arc::clone(&harness);
        let c = tokio::spawn(async move { h.get(&tile_url("radar", TIME, "0", "1", "0")).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        c.abort();
        let _ = c.await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "the last consumer leaving cancels the fetch"
        );
    }

    struct CountOnDrop(Arc<AtomicUsize>);
    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn metadata_deadline_cancels_stalled_bodies_with_a_sanitized_manifest() {
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_stream(
                200,
                &[],
                testing::stalled(Arc::clone(&flag)),
            ))
        });
        let harness = Harness::with_timeout(fetch, fixed(NOW), Duration::from_millis(15));
        assert_eq!(
            harness.json("/manifest?product=radar").await["unavailable"],
            json!(true)
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn disconnecting_the_last_metadata_consumer_permits_an_immediate_new_acquisition() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Fetcher = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_| {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move {
                    if n == 1 {
                        std::future::pending::<()>().await;
                    }
                    Ok(text(xml()))
                })
            })
        };
        let harness = Arc::new(Harness::new(fetch, fixed(NOW)));
        let h = Arc::clone(&harness);
        let first = tokio::spawn(async move { h.get("/manifest?product=radar").await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        first.abort();
        let _ = first.await;
        let second = harness.json("/manifest?product=radar").await;
        assert_eq!(second["unavailable"], json!(false));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "cancellation starts no cooldown"
        );
    }

    #[tokio::test]
    async fn streamed_metadata_cap_cancels_oversized_capabilities() {
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_stream(
                200,
                &[],
                testing::endless(Bytes::from(vec![b' '; 300_000]), Arc::clone(&flag)),
            ))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        assert_eq!(
            harness.json("/manifest?product=radar").await["unavailable"],
            json!(true)
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn tile_rejects_invalid_mime_signature_dimensions_statuses_and_oversized_bodies() {
        let cases: Vec<Box<dyn Fn() -> FetchResponse + Send + Sync>> = vec![
            Box::new(|| {
                FetchResponse::from_bytes(
                    200,
                    &[("content-type", "text/xml")],
                    "<ServiceException/>",
                )
            }),
            Box::new(|| image(Bytes::from(vec![0u8; 33]))),
            Box::new(|| image(png(512, 256, 33))),
            Box::new(|| {
                FetchResponse::from_bytes(
                    302,
                    &[
                        ("location", "http://127.0.0.1/"),
                        ("content-type", "image/png"),
                    ],
                    png(256, 256, 33),
                )
            }),
            Box::new(|| {
                FetchResponse::from_bytes(503, &[("content-type", "image/png")], png(256, 256, 33))
            }),
            Box::new(|| image(png(256, 256, 1024 * 1024 + 1))),
            Box::new(|| {
                FetchResponse::from_bytes(
                    200,
                    &[
                        ("content-type", "image/png"),
                        ("content-length", &(1024 * 1024 + 1).to_string()),
                    ],
                    png(256, 256, 33),
                )
            }),
        ];
        for invalid in cases {
            let fetch = testing::fetcher(Default::default(), move |request| {
                Ok(if request.url.contains("GetCapabilities") {
                    text(xml())
                } else {
                    invalid()
                })
            });
            let harness = Harness::new(fetch, fixed(NOW));
            let (status, _, body) = harness.get(&tile()).await;
            assert_eq!(status, 503);
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                json!({ "error": "weather_upstream_unavailable" })
            );
        }
    }

    #[tokio::test]
    async fn tile_body_deadline_cancels_a_stream_after_valid_headers() {
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |request| {
            if request.url.contains("GetCapabilities") {
                return Ok(text(xml()));
            }
            let prefix = futures_util::stream::once(async { Ok(png(256, 256, 33)) });
            let body = futures_util::StreamExt::chain(prefix, testing::stalled(Arc::clone(&flag)));
            Ok(FetchResponse::from_stream(
                200,
                &[("content-type", "image/png")],
                futures_util::StreamExt::boxed(body),
            ))
        });
        let harness = Harness::with_timeout(fetch, fixed(NOW), Duration::from_millis(15));
        assert_eq!(harness.get(&tile()).await.0, 503);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_tile_retries_use_a_bounded_cooldown() {
        let (clock, now) = testing::manual_clock(NOW);
        let maps = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&maps);
        let fetch = testing::fetcher(Default::default(), move |request| {
            if request.url.contains("GetCapabilities") {
                return Ok(text(xml()));
            }
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(if n == 1 {
                FetchResponse::from_bytes(503, &[], "private upstream detail")
            } else {
                image(png(256, 256, 33))
            })
        });
        let harness = Harness::new(fetch, clock);
        assert_eq!(harness.get(&tile()).await.0, 503);
        assert_eq!(harness.get(&tile()).await.0, 503);
        assert_eq!(maps.load(Ordering::SeqCst), 1);
        now.fetch_add(30_001, Ordering::SeqCst);
        assert_eq!(harness.get(&tile()).await.0, 200);
        assert_eq!(maps.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn tile_lru_enforces_128_entries_and_a_16_mib_budget() {
        for (length, entries) in [(33usize, 129usize), (600_000, 29)] {
            let maps = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&maps);
            let fetch = testing::fetcher(Default::default(), move |request| {
                if request.url.contains("GetCapabilities") {
                    return Ok(text(xml()));
                }
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(image(png(256, 256, length)))
            });
            let harness = Harness::new(fetch, fixed(NOW));
            for i in 0..entries {
                let url = tile_url(
                    "radar",
                    TIME,
                    "6",
                    &(i % 128).to_string(),
                    &(i / 128).to_string(),
                );
                assert_eq!(harness.get(&url).await.0, 200);
            }
            assert_eq!(maps.load(Ordering::SeqCst), entries);
            harness.get(&tile_url("radar", TIME, "6", "0", "0")).await;
            assert_eq!(
                maps.load(Ordering::SeqCst),
                entries + 1,
                "oldest tile evicted"
            );
        }
    }

    #[tokio::test]
    async fn eight_upstream_slots_and_a_96_entry_queue_bound_concurrent_tile_work() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let fetch: Fetcher = {
            let (gate, active, peak) = (Arc::clone(&gate), Arc::clone(&active), Arc::clone(&peak));
            Arc::new(move |request: FetchRequest| {
                let (gate, active, peak) =
                    (Arc::clone(&gate), Arc::clone(&active), Arc::clone(&peak));
                Box::pin(async move {
                    if request.url.contains("GetCapabilities") {
                        return Ok(text(xml()));
                    }
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    let permit = gate.acquire().await.unwrap();
                    permit.forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(image(png(256, 256, 33)))
                })
            })
        };
        let harness = Arc::new(Harness::new(fetch, fixed(NOW)));
        harness.get("/manifest?product=radar").await;
        let requests: Vec<_> = (0..104)
            .map(|x| {
                let h = Arc::clone(&harness);
                tokio::spawn(async move {
                    h.get(&tile_url("radar", TIME, "6", &x.to_string(), "0"))
                        .await
                        .0
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(active.load(Ordering::SeqCst), 8);
        let overflow = harness.get(&tile_url("radar", TIME, "6", "104", "0")).await;
        assert_eq!(overflow.0, 429);
        assert_eq!(overflow.1["retry-after"], "2");
        gate.add_permits(104);
        for request in requests {
            assert_eq!(request.await.unwrap(), 200);
        }
        assert_eq!(peak.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn global_infrared_image_uses_one_fixed_extent_and_exact_shape() {
        let calls: Calls = Default::default();
        let bounds = (-179.99, 179.95, -72.74, 72.73);
        let fetch = testing::fetcher(Arc::clone(&calls), move |request| {
            Ok(if request.url.contains("GetCapabilities") {
                text(format!(
                    "<WMS_Capabilities>{}</WMS_Capabilities>",
                    leaf_with(NAMES[1], &[TIME], bounds)
                ))
            } else {
                image(png(2048, 1024, 33))
            })
        });
        let harness = Harness::new(fetch, fixed(NOW));
        let manifest = harness.json("/manifest?product=clouds").await;
        assert_eq!(
            manifest["bounds"],
            json!({ "west": -179.99, "south": -72.74, "east": 179.95, "north": 72.73 })
        );
        let image_url = manifest["imageUrl"]
            .as_str()
            .unwrap()
            .replace("/api/weather", "");
        let (status, headers, body) = harness.get(&image_url).await;
        assert_eq!(status, 200);
        assert_eq!(headers["content-type"], "image/png");
        {
            let calls = calls.lock();
            assert_eq!(
                param(&calls[1], "bbox").as_deref(),
                Some("-179.99,-72.74,179.95,72.73")
            );
            assert_eq!(param(&calls[1], "width").as_deref(), Some("2048"));
            assert_eq!(param(&calls[1], "height").as_deref(), Some("1024"));
            assert_eq!(param(&calls[1], "styles").as_deref(), Some("reflectance"));
            assert_eq!(
                url::Url::parse(&calls[1].url).unwrap().path(),
                "/geoserver/observations/satellite/ows"
            );
        }
        assert_eq!(harness.get(&whole_image(TIME, "clouds")).await.2, body);
        assert_eq!(calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn whole_image_route_rejects_bad_products_sizes_and_destinations() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Err(FetchError::Network("unexpected".into()))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        let w = whole_image(TIME, "clouds");
        let r = whole_image(TIME, "radar");
        let urls = [
            whole_image(TIME, "other"),
            format!("{w}&size=4096x2048"),
            format!("{}&size=8192x4096", whole_image(TIME, "lightning")),
            format!("{r}&size=8192x4096"),
            format!("{r}&size=2048x2048"),
            format!("{r}&size=512x256"),
            format!("{r}&size=2048X1024"),
            format!("{r}&size=02048x1024"),
            format!("{r}&size=2048"),
            format!("{r}&size=1024x512&size=2048x1024"),
            format!("{w}&width=4096"),
            format!("{w}&height=2048"),
            format!("{w}&z=0"),
            format!("{w}&bbox=-180,-90,180"),
            format!("{w}&host=http://127.0.0.1"),
            format!("{w}&layers=other"),
            format!("{w}&time={}", encode_uri_component(TIME)),
            whole_image("latest", "clouds"),
            "/image?product=clouds".into(),
        ];
        for url in urls {
            let (status, headers, _) = harness.get(&url).await;
            assert_eq!(status, 400, "{url}");
            assert_eq!(headers["cache-control"], "no-store");
        }
        assert_eq!(calls.lock().len(), 0);
    }

    #[tokio::test]
    async fn every_product_serves_a_whole_extent_image_sized_up_to_its_limit() {
        let calls: Calls = Default::default();
        let harness = Harness::new(sized_fetch(Arc::clone(&calls)), fixed(NOW));
        for (product, name, width) in [
            ("radar", NAMES[0], 4096u32),
            ("clouds-regional", NAMES[2], 4096),
            ("lightning", NAMES[3], 4096),
            ("clouds", NAMES[1], 2048),
        ] {
            let maps = || -> Vec<FetchRequest> {
                calls
                    .lock()
                    .iter()
                    .filter(|c| !c.url.contains("GetCapabilities"))
                    .cloned()
                    .collect()
            };
            let before = maps().len();
            let (status, headers, body) = harness.get(&whole_image(TIME, product)).await;
            assert_eq!(status, 200, "{product}");
            assert_eq!(headers["cache-control"], "public, max-age=86400, immutable");
            assert_eq!(width_of(&body), width);
            let latest = maps();
            assert_eq!(latest.len(), before + 1);
            let map = latest.last().unwrap();
            assert_eq!(param(map, "layers").as_deref(), Some(name));
            assert_eq!(param(map, "bbox").as_deref(), Some("-130,20,-60,55"));
            assert_eq!(param(map, "width"), Some(width.to_string()));
            assert_eq!(param(map, "height"), Some((width / 2).to_string()));
            let smaller = format!("{}&size=1024x512", whole_image(TIME, product));
            assert_eq!(harness.get(&smaller).await.0, 200);
            assert_eq!(maps().len(), before + 2);
            assert_eq!(width_of(&harness.get(&smaller).await.2), 1024);
            let explicit = format!("{}&size={width}x{}", whole_image(TIME, product), width / 2);
            assert_eq!(harness.get(&explicit).await.0, 200);
            assert_eq!(
                maps().len(),
                before + 2,
                "the explicit default shares the entry"
            );
        }
    }

    #[tokio::test]
    async fn detail_windows_round_and_cache_by_product_time_size_and_bbox() {
        let calls: Calls = Default::default();
        let harness = Harness::new(sized_fetch(Arc::clone(&calls)), fixed(NOW));
        let maps = || -> Vec<FetchRequest> {
            calls
                .lock()
                .iter()
                .filter(|c| !c.url.contains("GetCapabilities"))
                .cloned()
                .collect()
        };
        let detail = |product: &str, bbox: &str, extra: &str| {
            format!("{}&bbox={bbox}{extra}", whole_image(TIME, product))
        };
        for (product, name) in [
            ("radar", NAMES[0]),
            ("clouds-regional", NAMES[2]),
            ("lightning", NAMES[3]),
            ("clouds", NAMES[1]),
        ] {
            let base = maps().len();
            let (status, headers, body) = harness.get(&detail(product, "-102,34,-96,37", "")).await;
            assert_eq!(status, 200, "{product}");
            assert_eq!(headers["cache-control"], "public, max-age=86400, immutable");
            assert_eq!(width_of(&body), 4096);
            let first = maps()[base].clone();
            assert_eq!(param(&first, "layers").as_deref(), Some(name));
            assert_eq!(param(&first, "time").as_deref(), Some(TIME));
            assert_eq!(param(&first, "bbox").as_deref(), Some("-102,34,-96,37"));
            assert_eq!(param(&first, "width").as_deref(), Some("4096"));
            assert_eq!(param(&first, "height").as_deref(), Some("2048"));
            assert_eq!(
                harness
                    .get(&detail(product, "-102.1,34.05,-95.9,37.1", ""))
                    .await
                    .2,
                body
            );
            assert_eq!(
                harness
                    .get(&detail(product, "-102,34,-96,37", "&size=4096x2048"))
                    .await
                    .0,
                200
            );
            assert_eq!(maps().len(), base + 1);
            assert_eq!(
                harness
                    .get(&detail(product, "-102,34,-96,37", "&size=2048x1024"))
                    .await
                    .0,
                200
            );
            assert_eq!(maps().len(), base + 2);
            assert_eq!(param(&maps()[base + 1], "width").as_deref(), Some("2048"));
            assert_eq!(
                harness.get(&detail(product, "-97,34,-91,37", "")).await.0,
                200
            );
            assert_eq!(maps().len(), base + 3);
            assert_eq!(harness.get(&whole_image(TIME, product)).await.0, 200);
            assert_eq!(maps().len(), base + 4);
            assert_eq!(
                param(&maps()[base + 3], "bbox").as_deref(),
                Some("-130,20,-60,55")
            );
        }
        let base = maps().len();
        assert_eq!(
            harness
                .get(&detail("radar", "-120,25,-70,50.25", ""))
                .await
                .0,
            200
        );
        assert_eq!(
            param(&maps()[base], "bbox").as_deref(),
            Some("-120,25,-70,50.25")
        );
        for bbox in [
            "-120,25,-70,50.5",
            "-134,30,-122,36",
            "-70,40,-58,46",
            "-100,17,-94,20",
            "-100,53,-94,56",
        ] {
            let (status, headers, body) = harness.get(&detail("radar", bbox, "")).await;
            assert_eq!(status, 400, "{bbox}");
            assert_eq!(headers["cache-control"], "no-store");
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                json!({ "error": "invalid_weather_bbox" })
            );
        }
        assert_eq!(maps().len(), base + 1);
    }

    #[tokio::test]
    async fn malformed_detail_windows_fail_before_any_upstream_request() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Err(FetchError::Network("unexpected".into()))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        let r = whole_image(TIME, "radar");
        for bbox in [
            "",
            "-102,34,-96",
            "-102,34,-96,37,1",
            "a,b,c,d",
            "-102,34,-96,3e1",
            "-102,34,-96,%2B37",
            "-102,%2034,-96,37",
            "-1020,34,-96,37",
            "-102.1234567,34,-96,37",
            "-96,34,-102,37",
            "-102,37,-96,34",
            "-102,34,-96,40",
            "-102,34,-90,37",
            "-190,34,-178,40",
            "-102,85,-94,91",
            "-100,34,-100,34",
        ] {
            let (status, _, body) = harness.get(&format!("{r}&bbox={bbox}")).await;
            assert_eq!(status, 400, "{bbox}");
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                json!({ "error": "invalid_weather_bbox" })
            );
        }
        for url in [
            format!("{r}&bbox=-102,34,-96,37&size=8192x4096"),
            format!("{r}&bbox=-102,34,-96,37&bbox=-102,34,-96,37"),
            format!("{}&bbox=-102,34,-96,37", tile()),
            "/manifest?product=radar&bbox=-102,34,-96,37".into(),
        ] {
            assert_eq!(harness.get(&url).await.0, 400, "{url}");
        }
        assert_eq!(calls.lock().len(), 0);
        assert_eq!(weather_image_bbox(None), Ok(None));
        let rounded = weather_image_bbox(Some("-0.1,-0.05,5.9,2.95"))
            .unwrap()
            .unwrap();
        assert_eq!(rounded, [0.0, 0.0, 6.0, 3.0]);
        assert!(
            rounded[0].is_sign_positive(),
            "no negative zero in cache keys"
        );
    }

    #[tokio::test]
    async fn whole_images_and_tiles_share_a_byte_budget_without_sharing_identities() {
        let times: Vec<String> = (0..5).map(|i| iso_minutes_before(5 - i)).collect();
        let maps = Arc::new(AtomicUsize::new(0));
        let (counter, advertised) = (Arc::clone(&maps), times.clone());
        let fetch = testing::fetcher(Default::default(), move |request| {
            if request.url.contains("GetCapabilities") {
                let refs: Vec<&str> = advertised.iter().map(String::as_str).collect();
                return Ok(text(xml_times(&refs)));
            }
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(if param(request, "width").as_deref() == Some("2048") {
                image(png(2048, 1024, 4 * 1024 * 1024))
            } else {
                image(png(256, 256, 33))
            })
        });
        let harness = Harness::new(fetch, fixed(NOW));
        for time in &times[..4] {
            assert_eq!(harness.get(&whole_image(time, "clouds")).await.0, 200);
        }
        assert_eq!(maps.load(Ordering::SeqCst), 4);
        assert_eq!(
            harness
                .get(&tile_url("clouds", &times[3], "0", "0", "0"))
                .await
                .0,
            200
        );
        assert_eq!(maps.load(Ordering::SeqCst), 5);
        assert_eq!(harness.get(&whole_image(&times[0], "clouds")).await.0, 200);
        assert_eq!(
            maps.load(Ordering::SeqCst),
            6,
            "the tile evicted the oldest image"
        );
        assert_eq!(harness.get(&whole_image(&times[0], "clouds")).await.0, 200);
        assert_eq!(maps.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn whole_image_identity_includes_metadata_bounds() {
        let (clock, now) = testing::manual_clock(NOW);
        let south = Arc::new(Mutex::new(-60.0));
        let maps = Arc::new(AtomicUsize::new(0));
        let (bounds, counter) = (Arc::clone(&south), Arc::clone(&maps));
        let fetch = testing::fetcher(Default::default(), move |request| {
            if request.url.contains("GetCapabilities") {
                let leaf = leaf_with(NAMES[1], &[TIME], (-180.0, 180.0, *bounds.lock(), 60.0));
                return Ok(text(format!("<WMS_Capabilities>{leaf}</WMS_Capabilities>")));
            }
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(image(png(2048, 1024, 33)))
        });
        let harness = Harness::new(fetch, clock);
        harness.get(&whole_image(TIME, "clouds")).await;
        now.fetch_add(120_001, Ordering::SeqCst);
        *south.lock() = -72.0;
        harness.get(&whole_image(TIME, "clouds")).await;
        assert_eq!(maps.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn frames_are_immutable_for_a_day_while_manifests_and_errors_are_uncached() {
        let harness = Harness::new(sized_fetch(Default::default()), fixed(NOW));
        for url in [tile(), whole_image(TIME, "clouds")] {
            for _ in 0..2 {
                let (status, headers, _) = harness.get(&url).await;
                assert_eq!(status, 200);
                assert_eq!(headers["cache-control"], "public, max-age=86400, immutable");
            }
        }
        for product in ["radar", "clouds", "clouds-regional", "lightning"] {
            let (status, headers, _) = harness.get(&format!("/manifest?product={product}")).await;
            assert_eq!(status, 200);
            assert_eq!(headers["cache-control"], "no-store");
        }
        for url in [
            tile_url("radar", "latest", "0", "0", "0"),
            whole_image("latest", "clouds"),
            "/tile?product=radar&z=0&x=0&y=0".into(),
            "/image?product=clouds".into(),
        ] {
            let (status, headers, _) = harness.get(&url).await;
            assert_eq!(status, 400);
            assert_eq!(headers["cache-control"], "no-store");
        }
        let fetch = testing::fetcher(Default::default(), |request| {
            if request.url.contains("GetCapabilities") {
                return Ok(text(xml()));
            }
            Err(FetchError::Network("offline".into()))
        });
        let failed = Harness::new(fetch, fixed(NOW));
        for url in [tile(), whole_image(TIME, "clouds")] {
            let (status, headers, _) = failed.get(&url).await;
            assert_eq!(status, 503);
            assert_eq!(headers["cache-control"], "no-store");
        }
    }

    #[tokio::test]
    async fn tile_sizes_select_wms_dimensions_and_distinct_cache_entries() {
        for product in ["radar", "clouds-regional", "lightning"] {
            let calls: Calls = Default::default();
            let harness = Harness::new(sized_fetch(Arc::clone(&calls)), fixed(NOW));
            let base = tile_url(product, TIME, "3", "4", "2");
            for size in [256u32, 512, 1024] {
                let url = format!("{base}&size={size}");
                let first = harness.get(&url).await;
                assert_eq!(first.0, 200);
                assert_eq!(width_of(&first.2), size);
                assert_eq!(harness.get(&url).await.2, first.2);
            }
            assert_eq!(harness.get(&base).await.0, 200);
            let maps: Vec<FetchRequest> = calls
                .lock()
                .iter()
                .filter(|c| !c.url.contains("GetCapabilities"))
                .cloned()
                .collect();
            let sizes: Vec<(String, String)> = maps
                .iter()
                .map(|m| (param(m, "width").unwrap(), param(m, "height").unwrap()))
                .collect();
            assert_eq!(
                sizes,
                vec![
                    ("256".into(), "256".into()),
                    ("512".into(), "512".into()),
                    ("1024".into(), "1024".into())
                ]
            );
            let boxes: HashSet<Option<String>> = maps.iter().map(|m| param(m, "bbox")).collect();
            assert_eq!(boxes.len(), 1);
        }
    }

    #[tokio::test]
    async fn unsupported_or_duplicate_sizes_fail_before_upstream_acquisition() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Err(FetchError::Network(String::new()))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        for size in [
            "",
            "0",
            "257",
            "2048",
            "-256",
            "0256",
            "256.0",
            "1e3",
            "512&size=1024",
        ] {
            let (status, headers, _) = harness.get(&format!("{}&size={size}", tile())).await;
            assert_eq!(status, 400, "{size}");
            assert_eq!(headers["cache-control"], "no-store");
        }
        assert_eq!(calls.lock().len(), 0);
    }

    #[tokio::test]
    async fn large_tiles_check_requested_dimensions_and_bounded_bytes() {
        for bytes in [
            png(256, 256, 33),
            png(1024, 512, 33),
            png(1024, 1024, 4 * 1024 * 1024 + 65_537),
        ] {
            let fetch = testing::fetcher(Default::default(), move |request| {
                Ok(if request.url.contains("GetCapabilities") {
                    text(xml())
                } else {
                    image(bytes.clone())
                })
            });
            let harness = Harness::new(fetch, fixed(NOW));
            assert_eq!(harness.get(&format!("{}&size=1024", tile())).await.0, 503);
        }
        let fetch = testing::fetcher(Default::default(), |request| {
            Ok(if request.url.contains("GetCapabilities") {
                text(xml())
            } else {
                image(png(1024, 1024, 2 * 1024 * 1024))
            })
        });
        let harness = Harness::new(fetch, fixed(NOW));
        assert_eq!(harness.get(&format!("{}&size=1024", tile())).await.0, 200);
    }
}
