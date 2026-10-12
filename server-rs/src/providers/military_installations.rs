//! `/api/military-installations`: allow-listed mapped military context from
//! the configured Overpass chain, with a 5-minute memory tier, a 30-day disk
//! tier and serve-stale (port of `server/providers/military-installations.js`
//! and `server/providers/military-installations/*`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::any;
use parking_lot::Mutex;
use serde_json::{Value, json};

use crate::common::query::Params;
use crate::common::rate_limit::RateLimiter;
use crate::common::{disk, js, reply};
use crate::providers::mapdata_support::{
    InFlight, InsertionMap, encode_uri_component, iso_from_ms, js_number_string, now_iso, peer_key,
    sha1_hex, to_fixed,
};
use crate::providers::overpass::{
    OverpassTransport, overpass_not_configured, resolve_overpass_upstreams, shared_transport,
};
use crate::state::AppState;

pub const MILITARY_INSTALLATION_CACHE_MS: f64 = 5.0 * 60_000.0;
pub const MILITARY_INSTALLATION_STALE_MS: f64 = 60.0 * 60_000.0;
pub const MILITARY_INSTALLATION_MAX_CACHE: usize = 80;
pub const MILITARY_INSTALLATION_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Upstream element cap; a response that reaches it is saturated.
pub const MILITARY_INSTALLATION_ELEMENT_CAP: usize = 700;
/// Disk-cache TTL (30 days).
pub const MILITARY_INSTALLATION_DISK_TTL_MS: f64 = 30.0 * 86_400_000.0;
/// Cache-key grid step in degrees (~5.5 km).
pub const MILITARY_INSTALLATION_BBOX_STEP_DEG: f64 = 0.05;

/// A request bbox.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BBox {
    pub south: f64,
    pub west: f64,
    pub north: f64,
    pub east: f64,
}

/// One cached answer: `{payload, cachedAt}`.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub payload: Value,
    pub cached_at: f64,
}

impl Entry {
    fn to_json(&self) -> Value {
        json!({ "payload": self.payload, "cachedAt": crate::providers::mapdata_support::js_json_number(self.cached_at) })
    }
}

/// `quantizeMilitaryInstallationBox`: snap outward onto the cache grid.
pub fn quantize_box(box_: BBox, step: f64) -> BBox {
    let snap = |value: f64, grow: bool| {
        let cells = js::number(&to_fixed(value / step, 9));
        let snapped = if grow { cells.ceil() } else { cells.floor() } * step;
        js::number(&to_fixed(snapped, 6))
    };
    BBox {
        south: snap(box_.south, false).max(-90.0),
        west: snap(box_.west, false).max(-180.0),
        north: snap(box_.north, true).min(90.0),
        east: snap(box_.east, true).min(180.0),
    }
}

/// `militaryInstallationCacheKey`.
pub fn cache_key(box_: BBox, decimals: usize) -> String {
    [box_.south, box_.west, box_.north, box_.east]
        .iter()
        .map(|v| to_fixed(*v, decimals))
        .collect::<Vec<_>>()
        .join(",")
}

/// `validMilitaryInstallationBox`.
pub fn valid_box(params: &Params) -> Option<BBox> {
    let south = params.finite("south")?;
    let west = params.finite("west")?;
    let north = params.finite("north")?;
    let east = params.finite("east")?;
    if south < -90.0
        || north > 90.0
        || west < -180.0
        || east > 180.0
        || south >= north
        || west >= east
    {
        return None;
    }
    if north - south > 10.0 || east - west > 10.0 {
        return None;
    }
    Some(BBox {
        south,
        west,
        north,
        east,
    })
}

/// Why an upstream refresh failed (`installationReason`, `retryAfterMs`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InstallationError {
    pub reason: Option<&'static str>,
    pub timeout: bool,
    pub retry_after_ms: Option<f64>,
}

/// `militaryInstallationFailureReason`.
pub fn failure_reason(error: &InstallationError) -> &'static str {
    match error.reason {
        Some(reason @ ("rate_limited" | "timeout" | "query_failed")) => reason,
        _ if error.timeout => "timeout",
        _ => "unavailable",
    }
}

/// `migrateMilitaryInstallationEntry`: derive a missing `saturated` flag.
pub fn migrate_entry(mut entry: Entry) -> Entry {
    if entry
        .payload
        .get("saturated")
        .is_some_and(Value::is_boolean)
    {
        return entry;
    }
    let count = entry
        .payload
        .get("elements")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if let Some(map) = entry.payload.as_object_mut() {
        map.insert(
            "saturated".into(),
            Value::Bool(count >= MILITARY_INSTALLATION_ELEMENT_CAP),
        );
    }
    entry
}

/// `militaryInstallationDiskFresh` over a raw stored value.
pub fn disk_fresh(value: &Value, max_age_ms: f64, now: f64) -> Option<Entry> {
    let cached_at = value.get("cachedAt")?.as_f64().filter(|v| v.is_finite())?;
    let payload = value.get("payload")?;
    payload.get("elements")?.as_array()?;
    (now - cached_at <= max_age_ms).then(|| Entry {
        payload: payload.clone(),
        cached_at,
    })
}

/// Cache key -> disk-cache file path.
pub fn disk_path(dir: &Path, key: &str) -> PathBuf {
    dir.join(format!("{}.json", sha1_hex(key)))
}

/// `readMilitaryInstallationDisk`.
pub async fn read_disk(dir: &Path, key: &str, max_age_ms: f64) -> Option<Entry> {
    let value: Value = disk::read_json(&disk_path(dir, key)).await?;
    disk_fresh(&value, max_age_ms, js::now_ms() as f64).map(migrate_entry)
}

/// `writeMilitaryInstallationDisk`: atomic temp-then-rename.
pub async fn write_disk(dir: &Path, key: &str, entry: &Entry) -> bool {
    match disk::write_json(&disk_path(dir, key), &entry.to_json()).await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!("[Installations Proxy] disk cache write failed: {error}");
            false
        }
    }
}

/// Where a request's answer comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    Hit,
    Disk,
    Upstream,
}

/// `resolveMilitaryInstallationTier`.
pub async fn resolve_tier<D>(
    memory: Option<Entry>,
    in_flight: bool,
    read_disk: D,
    now: f64,
    cache_ms: f64,
) -> (Tier, Option<Entry>)
where
    D: std::future::Future<Output = Option<Entry>>,
{
    if let Some(cached) = memory
        && now - cached.cached_at <= cache_ms
    {
        return (Tier::Hit, Some(cached));
    }
    if in_flight {
        return (Tier::Upstream, None);
    }
    match read_disk.await {
        Some(disk) => (Tier::Disk, Some(disk)),
        None => (Tier::Upstream, None),
    }
}

/// `{...payload, ...extra}` with JS spread key order.
fn spread(payload: &Value, extra: &[(&str, Value)]) -> Value {
    let mut map = payload.as_object().cloned().unwrap_or_default();
    for (key, value) in extra {
        map.insert((*key).to_string(), value.clone());
    }
    Value::Object(map)
}

struct Installations {
    transport: Arc<OverpassTransport>,
    cache: Mutex<InsertionMap<Entry>>,
    in_flight: InFlight<String, Result<Value, InstallationError>>,
    limiter: RateLimiter,
    disk_dir: PathBuf,
}

impl Installations {
    fn remember(&self, key: &str, entry: Entry) {
        let mut cache = self.cache.lock();
        cache.set(key, entry);
        cache.trim(MILITARY_INSTALLATION_MAX_CACHE);
    }

    async fn refresh(
        self: Arc<Self>,
        box_: BBox,
        key: String,
        endpoints: Vec<String>,
    ) -> Result<Value, InstallationError> {
        let bbox = [box_.south, box_.west, box_.north, box_.east]
            .iter()
            .map(|v| js_number_string(*v))
            .collect::<Vec<_>>()
            .join(",");
        let ql = format!(
            "[out:json][timeout:20];(nwr[\"military\"~\"^(airfield|naval_base|range|barracks|base)$\"]({bbox});nwr[\"landuse\"=\"military\"]({bbox}););out center tags geom {MILITARY_INSTALLATION_ELEMENT_CAP};"
        );
        let upstream = self
            .transport
            .fetch(
                &format!("data={}", encode_uri_component(&ql)),
                MILITARY_INSTALLATION_MAX_RESPONSE_BYTES,
                &endpoints,
            )
            .await;
        if upstream.status >= 400.0 || upstream.rate_limited || upstream.runtime_error {
            return Err(InstallationError {
                reason: Some(if upstream.rate_limited {
                    "rate_limited"
                } else if upstream.status == 504.0 {
                    "timeout"
                } else if upstream.runtime_error {
                    "query_failed"
                } else {
                    "unavailable"
                }),
                timeout: false,
                retry_after_ms: upstream.retry_after_ms,
            });
        }
        let parsed: Value =
            serde_json::from_str(&upstream.body).map_err(|_| InstallationError::default())?;
        let elements: Vec<Value> = parsed
            .get("elements")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .take(MILITARY_INSTALLATION_ELEMENT_CAP)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let saturated = elements.len() >= MILITARY_INSTALLATION_ELEMENT_CAP;
        let payload = json!({
            "elements": elements,
            "saturated": saturated,
            "elementCap": MILITARY_INSTALLATION_ELEMENT_CAP,
            "retrievedAt": now_iso(),
            "status": "ready",
        });
        let entry = Entry {
            payload: payload.clone(),
            cached_at: js::now_ms() as f64,
        };
        self.remember(&key, entry.clone());
        let dir = self.disk_dir.clone();
        tokio::spawn(async move {
            write_disk(&dir, &key, &entry).await;
        });
        Ok(payload)
    }
}

fn json_body(status: u16, body: &Value, cache: Option<&str>, marker: Option<&str>) -> Response {
    let response = reply::json(status, body, cache);
    match marker {
        Some(marker) => reply::with_header(response, "X-Military-Installations", marker),
        None => response,
    }
}

async fn handle(svc: Arc<Installations>, state: AppState, request: Request) -> Response {
    let (parts, _body) = request.into_parts();
    if parts.method != http::Method::GET {
        return reply::json(405, &json!({ "error": "Method Not Allowed" }), None);
    }
    let params = Params::parse(parts.uri.query().unwrap_or(""));
    let Some(requested) = valid_box(&params) else {
        return reply::json(
            400,
            &json!({ "error": "A non-dateline bbox no larger than 10 degrees is required" }),
            None,
        );
    };
    let exact = params.get("exact") == Some("1");
    let box_ = if exact {
        requested
    } else {
        quantize_box(requested, MILITARY_INSTALLATION_BBOX_STEP_DEG)
    };
    let key = if exact {
        format!("exact:{}", cache_key(box_, 5))
    } else {
        cache_key(box_, 3)
    };
    let now = js::now_ms() as f64;
    let cached = svc.cache.lock().get(&key).cloned();
    let endpoints = resolve_overpass_upstreams(&state);
    if endpoints.is_empty() {
        let stale = match cached {
            Some(entry) => Some(entry),
            None => read_disk(&svc.disk_dir, &key, f64::INFINITY).await,
        };
        return match stale {
            Some(stale) => {
                let retrieved = match stale.payload.get("retrievedAt") {
                    Some(value) if crate::providers::mapdata_support::js_truthy(Some(value)) => {
                        value.clone()
                    }
                    _ => iso_from_ms(stale.cached_at).map_or(Value::Null, Value::String),
                };
                let body = spread(
                    &stale.payload,
                    &[("status", json!("stale")), ("retrievedAt", retrieved)],
                );
                json_body(200, &body, Some("no-store"), Some("STALE"))
            }
            None => {
                let response =
                    reply::json_text(200, overpass_not_configured().body, Some("no-store"));
                reply::with_header(response, "X-Military-Installations", "DISABLED")
            }
        };
    }
    if !svc.limiter.allow(&peer_key(&parts.extensions)) {
        let response = reply::json(429, &json!({ "error": "Rate limit exceeded" }), None);
        return reply::with_header(response, "Retry-After", "5");
    }
    let (tier, entry) = resolve_tier(
        cached.clone(),
        svc.in_flight.contains(&key),
        read_disk(&svc.disk_dir, &key, MILITARY_INSTALLATION_DISK_TTL_MS),
        now,
        MILITARY_INSTALLATION_CACHE_MS,
    )
    .await;
    if let (Tier::Hit | Tier::Disk, Some(entry)) = (tier, &entry) {
        if tier == Tier::Disk {
            svc.remember(&key, entry.clone());
        }
        let body = spread(&entry.payload, &[("status", json!("cached"))]);
        let marker = if tier == Tier::Hit { "HIT" } else { "DISK" };
        return json_body(200, &body, Some("public, max-age=60"), Some(marker));
    }
    let worker = Arc::clone(&svc);
    let refresh_key = key.clone();
    let (task, shared) = svc.in_flight.coalesce(key.clone(), move || {
        worker.refresh(box_, refresh_key, endpoints)
    });
    match task.await {
        Ok(payload) => json_body(
            200,
            &payload,
            Some("public, max-age=60"),
            Some(if shared { "INFLIGHT" } else { "MISS" }),
        ),
        Err(error) => {
            if let Some(cached) =
                cached.filter(|c| now - c.cached_at <= MILITARY_INSTALLATION_STALE_MS)
            {
                let body = spread(&cached.payload, &[("status", json!("stale"))]);
                return json_body(200, &body, Some("no-store"), Some("STALE"));
            }
            if let Some(stale) = read_disk(&svc.disk_dir, &key, f64::INFINITY).await {
                let body = spread(&stale.payload, &[("status", json!("stale"))]);
                return json_body(200, &body, Some("no-store"), Some("STALE-DISK"));
            }
            let mut body = serde_json::Map::new();
            body.insert(
                "error".into(),
                json!("Mapped installation context is temporarily unavailable"),
            );
            body.insert("reason".into(), json!(failure_reason(&error)));
            if let Some(retry) = error.retry_after_ms {
                body.insert(
                    "retryAfterMs".into(),
                    crate::providers::mapdata_support::js_json_number(retry),
                );
            }
            let mut response = reply::json(503, &Value::Object(body), Some("no-store"));
            if let Some(retry) = error.retry_after_ms.filter(|r| *r != 0.0 && !r.is_nan()) {
                response = reply::with_header(
                    response,
                    "Retry-After",
                    &js_number_string((retry / 1000.0).ceil()),
                );
            }
            response
        }
    }
}

/// `GET /api/military-installations`.
pub fn router(state: &AppState) -> Router<AppState> {
    let svc = Arc::new(Installations {
        transport: shared_transport(),
        cache: Mutex::new(InsertionMap::default()),
        in_flight: InFlight::default(),
        limiter: RateLimiter::new(60_000, 90, Some(300)),
        disk_dir: state.cache_dir.join("military-installations"),
    });
    let handler = move |State(state): State<AppState>, request: Request| {
        let svc = Arc::clone(&svc);
        async move { handle(svc, state, request).await }
    };
    Router::new()
        .route("/api/military-installations", any(handler.clone()))
        .route("/api/military-installations/{*rest}", any(handler))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY_MS: f64 = 86_400_000.0;
    const TTL_MS: f64 = 30.0 * DAY_MS;

    fn entry(cached_at: f64, elements: Value) -> Entry {
        Entry {
            payload: json!({
                "elements": elements,
                "saturated": false,
                "retrievedAt": iso_from_ms(cached_at),
                "status": "ready",
            }),
            cached_at,
        }
    }

    fn one() -> Value {
        json!([{ "type": "node", "id": 1 }])
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ce-installations-{tag}-{}-{}",
            std::process::id(),
            js::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn b(south: f64, west: f64, north: f64, east: f64) -> BBox {
        BBox {
            south,
            west,
            north,
            east,
        }
    }

    const STEP: f64 = MILITARY_INSTALLATION_BBOX_STEP_DEG;

    #[test]
    fn failure_reasons_disclose_nothing_raw() {
        assert_eq!(failure_reason(&InstallationError::default()), "unavailable");
        assert_eq!(
            failure_reason(&InstallationError {
                timeout: true,
                ..Default::default()
            }),
            "timeout"
        );
        for reason in ["rate_limited", "query_failed"] {
            assert_eq!(
                failure_reason(&InstallationError {
                    reason: Some(reason),
                    ..Default::default()
                }),
                reason
            );
        }
    }

    #[test]
    fn snapping_grows_outward_and_respects_the_grid() {
        let requested = b(30.2612, -97.7431, 30.2884, -97.7099);
        let snapped = quantize_box(requested, STEP);
        assert_eq!(snapped, b(30.25, -97.75, 30.3, -97.7));
        let on_grid = b(30.25, -97.75, 30.3, -97.7);
        assert_eq!(quantize_box(on_grid, STEP), on_grid);
        assert_eq!(
            quantize_box(b(29.9999, 0.0, 30.05, 0.1), STEP),
            b(29.95, 0.0, 30.05, 0.1)
        );
        let polar = quantize_box(b(-89.99, -179.99, 89.99, 179.99), STEP);
        assert!(polar.south >= -90.0 && polar.north <= 90.0);
        assert!(polar.west >= -180.0 && polar.east <= 180.0);
    }

    #[test]
    fn keys_share_nearby_views_and_keep_exact_precision() {
        let key = |x: BBox| cache_key(quantize_box(x, STEP), 3);
        let base = key(b(30.2612, -97.7431, 30.2884, -97.7099));
        assert_eq!(base, key(b(30.2634, -97.7402, 30.2871, -97.7104)));
        assert_ne!(base, key(b(31.2612, -96.7431, 31.2884, -96.7099)));
        assert_eq!(base, "30.250,-97.750,30.300,-97.700");
        let a = b(30.26120, -97.74310, 30.28840, -97.70990);
        let c = b(30.26140, -97.74290, 30.28860, -97.70970);
        assert_ne!(cache_key(a, 5), cache_key(c, 5));
        assert_eq!(cache_key(a, 5), "30.26120,-97.74310,30.28840,-97.70990");
        assert_eq!(
            cache_key(quantize_box(a, STEP), 3),
            cache_key(quantize_box(c, STEP), 3)
        );
        let snapped = cache_key(quantize_box(b(30.26, -97.74, 30.28, -97.71), STEP), 3);
        let exact = format!("exact:{}", cache_key(b(30.26, -97.74, 30.28, -97.71), 5));
        assert_ne!(
            disk_path(Path::new("."), &snapped),
            disk_path(Path::new("."), &exact)
        );
    }

    #[test]
    fn validator_requires_every_edge() {
        assert!(valid_box(&Params::parse("west=-1&north=1&east=1")).is_none());
        let requested = valid_box(&Params::parse(
            "south=30.26120&west=-97.74310&north=30.28840&east=-97.70990",
        ))
        .unwrap();
        let snapped = quantize_box(requested, STEP);
        assert!(snapped.south < snapped.north && snapped.west < snapped.east);
        assert!(valid_box(&Params::parse("south=0&west=0&north=11&east=1")).is_none());
        assert!(valid_box(&Params::parse("south=0&west=170&north=1&east=-170")).is_none());
    }

    #[tokio::test]
    async fn legacy_entries_derive_saturation() {
        let now = 1_700_000_000_000.0;
        let at_cap = Entry {
            payload: json!({ "elements": vec![json!({"type": "node"}); 700], "retrievedAt": "", "status": "ready" }),
            cached_at: now,
        };
        let under = Entry {
            payload: json!({ "elements": vec![json!({"type": "node"}); 699], "retrievedAt": "", "status": "ready" }),
            cached_at: now,
        };
        assert_eq!(migrate_entry(at_cap.clone()).payload["saturated"], true);
        assert_eq!(migrate_entry(under).payload["saturated"], false);
        let explicit = Entry {
            payload: json!({ "elements": vec![json!({}); 700], "saturated": false }),
            cached_at: now,
        };
        assert_eq!(migrate_entry(explicit).payload["saturated"], false);

        let dir = temp_dir("legacy");
        let legacy = Entry {
            cached_at: js::now_ms() as f64,
            ..at_cap
        };
        std::fs::write(disk_path(&dir, "legacy"), legacy.to_json().to_string()).unwrap();
        let read = read_disk(&dir, "legacy", TTL_MS).await.unwrap();
        assert_eq!(read.payload["saturated"], true);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn disk_freshness_window() {
        let now = 1_700_000_000_000.0;
        let fresh =
            |age: f64, max: f64| disk_fresh(&entry(now - age, one()).to_json(), max, now).is_some();
        assert!(fresh(DAY_MS, TTL_MS));
        assert!(fresh(29.0 * DAY_MS, TTL_MS));
        assert!(!fresh(31.0 * DAY_MS, TTL_MS));
        assert!(fresh(400.0 * DAY_MS, f64::INFINITY));
        for bad in [
            Value::Null,
            json!({ "payload": { "elements": [] } }),
            json!({ "cachedAt": now }),
            json!({ "payload": {}, "cachedAt": now }),
            json!({ "payload": { "elements": "nope" }, "cachedAt": now }),
        ] {
            assert!(disk_fresh(&bad, TTL_MS, now).is_none(), "{bad}");
        }
    }

    #[tokio::test]
    async fn miss_write_restart_round_trip() {
        let dir = temp_dir("roundtrip");
        let key = "30.250,-97.750,30.300,-97.700";
        let now = js::now_ms() as f64;
        let (tier, _) = resolve_tier(
            None,
            false,
            read_disk(&dir, key, TTL_MS),
            now,
            MILITARY_INSTALLATION_CACHE_MS,
        )
        .await;
        assert_eq!(tier, Tier::Upstream);
        let fresh = entry(now, one());
        assert!(write_disk(&dir, key, &fresh).await);
        assert!(disk_path(&dir, key).exists());
        let temps = std::fs::read_dir(&dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp")
            })
            .count();
        assert_eq!(temps, 0, "no temp file is left behind");
        let (tier, _) = resolve_tier(
            Some(fresh.clone()),
            false,
            read_disk(&dir, key, TTL_MS),
            now,
            MILITARY_INSTALLATION_CACHE_MS,
        )
        .await;
        assert_eq!(tier, Tier::Hit);
        let (tier, hit) = resolve_tier(
            None,
            false,
            read_disk(&dir, key, TTL_MS),
            now,
            MILITARY_INSTALLATION_CACHE_MS,
        )
        .await;
        assert_eq!(tier, Tier::Disk);
        assert_eq!(hit.unwrap().payload["elements"], fresh.payload["elements"]);
        let (tier, _) = resolve_tier(
            Some(entry(now - 6.0 * 60_000.0, one())),
            false,
            read_disk(&dir, key, TTL_MS),
            now,
            MILITARY_INSTALLATION_CACHE_MS,
        )
        .await;
        assert_eq!(tier, Tier::Disk);

        // An in-flight request is joined instead of paying for a disk read.
        let mut reads = 0;
        let (tier, _) = resolve_tier(
            None,
            true,
            async {
                reads += 1;
                None
            },
            now,
            MILITARY_INSTALLATION_CACHE_MS,
        )
        .await;
        assert_eq!(tier, Tier::Upstream);
        assert_eq!(reads, 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn expiry_stale_reads_and_torn_writes() {
        let dir = temp_dir("expiry");
        let now = js::now_ms() as f64;
        write_disk(&dir, "expiry-key", &entry(now - 31.0 * DAY_MS, one())).await;
        assert!(read_disk(&dir, "expiry-key", TTL_MS).await.is_none());
        let stale = read_disk(&dir, "expiry-key", f64::INFINITY).await.unwrap();
        assert_eq!(stale.payload["status"], "ready");

        let good = entry(now, json!([{ "type": "node", "id": "original" }]));
        write_disk(&dir, "atomic-key", &good).await;
        let target = disk_path(&dir, "atomic-key");
        std::fs::write(
            format!("{}.crashed.tmp", target.display()),
            "{\"payload\":{\"elements\":[{\"type\":\"nod",
        )
        .unwrap();
        let survivor = read_disk(&dir, "atomic-key", TTL_MS).await.unwrap();
        assert_eq!(
            survivor.payload["elements"],
            json!([{ "type": "node", "id": "original" }])
        );
        std::fs::write(&target, "{\"payload\":{\"elements\":[").unwrap();
        assert!(read_disk(&dir, "atomic-key", TTL_MS).await.is_none());

        let blocked = dir.join("blocked");
        std::fs::write(&blocked, "not a directory").unwrap();
        assert!(!write_disk(&blocked, "key", &entry(now, one())).await);
        assert!(read_disk(&blocked, "key", TTL_MS).await.is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn unconfigured_serves_stale_or_capability_refusal() {
        use axum::body::Body;
        let dir = temp_dir("route");
        let state = AppState::new(PathBuf::from("."), PathBuf::from("dist")).unwrap();
        state.env.set("OVERPASS_UPSTREAMS", "");
        let svc = Arc::new(Installations {
            transport: shared_transport(),
            cache: Mutex::new(InsertionMap::default()),
            in_flight: InFlight::default(),
            limiter: RateLimiter::new(60_000, 90, Some(300)),
            disk_dir: dir.clone(),
        });
        let call = |method: &str, uri: &str| {
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap()
        };
        let read = |response: Response| async move {
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            (
                status,
                headers,
                serde_json::from_slice::<Value>(&bytes).unwrap(),
            )
        };
        let (status, _, _) = read(
            handle(
                Arc::clone(&svc),
                state.clone(),
                call("DELETE", "/api/military-installations"),
            )
            .await,
        )
        .await;
        assert_eq!(status, 405);
        let (status, _, _) = read(
            handle(
                Arc::clone(&svc),
                state.clone(),
                call("GET", "/api/military-installations"),
            )
            .await,
        )
        .await;
        assert_eq!(status, 400);
        let uri = "/api/military-installations?south=31.2&west=-97.8&north=31.3&east=-97.7";
        let (status, headers, body) =
            read(handle(Arc::clone(&svc), state.clone(), call("GET", uri)).await).await;
        assert_eq!(status, 200);
        assert_eq!(headers["X-Military-Installations"], "DISABLED");
        assert_eq!(body["code"], "OVERPASS_NOT_CONFIGURED");

        let cached_at = js::now_ms() as f64 - 60.0 * DAY_MS;
        let key = cache_key(quantize_box(b(31.2, -97.8, 31.3, -97.7), STEP), 3);
        svc.remember(
            &key,
            Entry {
                payload: json!({ "elements": [], "retrievedAt": iso_from_ms(cached_at) }),
                cached_at,
            },
        );
        let (_, headers, body) =
            read(handle(Arc::clone(&svc), state.clone(), call("GET", uri)).await).await;
        assert_eq!(headers["X-Military-Installations"], "STALE");
        assert_eq!(body["status"], "stale");
        assert_eq!(body["retrievedAt"], json!(iso_from_ms(cached_at)));
        let _ = std::fs::remove_dir_all(dir);
    }
}
