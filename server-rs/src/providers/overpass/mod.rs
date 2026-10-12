//! `/api/overpass`: the Overpass API proxy with memory + disk caching,
//! request coalescing, serve-stale and per-upstream cooldowns (port of
//! `server/providers/overpass.js` and `server/providers/overpass/*`).
//!
//! The JS plugin also installed `/api/route` (`installRouteMiddleware` from
//! `server/providers/places/routes.js`); that route belongs to the places
//! provider and is not part of this module.

pub mod geometry;
pub mod query;
pub mod transport;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::any;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::json;

use crate::common::rate_limit::RateLimiter;
use crate::common::{disk, js, reply};
use crate::providers::mapdata_support::{
    BodyReadError, InFlight, InsertionMap, collapse_js_space, iso_from_ms, mounted_url, peer_key,
    read_body_capped, sha1_hex,
};
use crate::state::AppState;

pub use transport::{OverpassPayload, OverpassTransport, overpass_not_configured};

/// Stable application identity for operator-configured Overpass instances.
pub const OVERPASS_USER_AGENT: &str = "camera-eye/0.1 (+https://github.com/AhmadRun/camera-eye)";
/// Fresh-cache TTL in memory (24 h).
pub const OVERPASS_CACHE_MS: f64 = 86_400_000.0;
/// Disk-cache TTL (7 days).
pub const OVERPASS_DISK_TTL_MS: f64 = 7.0 * 86_400_000.0;
/// Disk-cache TTL for boundary-class queries (30 days).
pub const OVERPASS_BOUNDARY_DISK_TTL_MS: f64 = 30.0 * 86_400_000.0;
/// Per-upstream fetch timeout.
pub const OVERPASS_TIMEOUT_MS: u64 = 22_000;
/// Max entries in the memory cache (oldest evicted first).
pub const OVERPASS_CACHE_MAX_ENTRIES: usize = 120;
/// Max accepted POST body (24 KB).
pub const OVERPASS_MAX_BODY_BYTES: usize = 24 * 1024;
/// Hard cap on one buffered upstream response (32 MB).
pub const OVERPASS_MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
/// Payloads at least this large go through geometry simplification.
pub const OVERPASS_SIMPLIFY_MIN_BYTES: usize = 1_500_000;
/// Geometry arrays with at least this many points are simplified.
pub const OVERPASS_SIMPLIFY_MIN_POINTS: usize = 1200;
/// Douglas-Peucker tolerance in degrees (~44 m).
pub const OVERPASS_SIMPLIFY_TOLERANCE_DEG: f64 = 0.0004;
/// Max concurrent upstream fetches across all distinct queries.
pub const OVERPASS_MAX_CONCURRENT: usize = 6;
/// Server-side `[timeout:]` ceiling in seconds.
pub const OVERPASS_MAX_QL_TIMEOUT: f64 = 30.0;
/// Max `around:` radius in metres.
pub const OVERPASS_MAX_AROUND_M: f64 = 50_000.0;
/// Max bbox span in degrees.
pub const OVERPASS_MAX_BBOX_DEG: f64 = 12.0;

/// `parseOverpassUpstreams`: operator-supplied HTTP(S) endpoints, deduped,
/// at most eight.
pub fn parse_overpass_upstreams(raw: &str) -> Vec<String> {
    let mut endpoints: Vec<String> = Vec::new();
    for token in raw.split(',') {
        let Ok(url) = url::Url::parse(crate::providers::mapdata_support::js_trim(token)) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none_or(str::is_empty)
            || url.fragment().is_some_and(|f| !f.is_empty())
        {
            continue;
        }
        let href = url.to_string();
        if !endpoints.contains(&href) {
            endpoints.push(href);
        }
    }
    endpoints.truncate(8);
    endpoints
}

/// `resolveOverpassUpstreams`: read at request time.
pub fn resolve_overpass_upstreams(state: &AppState) -> Vec<String> {
    parse_overpass_upstreams(&state.env.raw("OVERPASS_UPSTREAMS").unwrap_or_default())
}

/// The process-wide transport: its cooldowns are shared by every consumer
/// (the JS module state), so the installations proxy uses it too.
pub fn shared_transport() -> Arc<OverpassTransport> {
    static TRANSPORT: Lazy<Arc<OverpassTransport>> = Lazy::new(|| {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Arc::new(OverpassTransport::new(Arc::new(
            transport::ReqwestOverpass { client },
        )))
    });
    Arc::clone(&TRANSPORT)
}

/// Disk TTL for a query: boundary geometry keeps for a month, the rest a week.
pub fn overpass_disk_ttl_ms(cache_key: &str) -> f64 {
    if query::is_overpass_boundary_query(cache_key) {
        OVERPASS_BOUNDARY_DISK_TTL_MS
    } else {
        OVERPASS_DISK_TTL_MS
    }
}

/// Normalized query -> disk-cache file path.
pub fn overpass_disk_path(dir: &Path, cache_key: &str) -> PathBuf {
    dir.join(format!("{}.json", sha1_hex(cache_key)))
}

/// `readOverpassDisk`: only data (never a stored refusal) younger than
/// `max_age_ms` (`f64::INFINITY` = any age).
pub async fn read_overpass_disk(
    dir: &Path,
    cache_key: &str,
    max_age_ms: f64,
) -> Option<OverpassPayload> {
    let value: serde_json::Value = disk::read_json(&overpass_disk_path(dir, cache_key)).await?;
    let payload = OverpassPayload::from_json(&value)?;
    let cached_at = payload.cached_at?;
    if !payload.is_data() {
        return None;
    }
    if js::now_ms() as f64 - cached_at > max_age_ms {
        return None;
    }
    Some(payload)
}

/// Where a preflight found its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preflight {
    Hit,
    Inflight,
    Disk,
    Upstream,
    RateLimited,
}

impl Preflight {
    fn header(self) -> &'static str {
        match self {
            Preflight::Hit => "HIT",
            Preflight::Inflight => "INFLIGHT",
            Preflight::Disk => "DISK",
            Preflight::Upstream => "UPSTREAM",
            Preflight::RateLimited => "RATE_LIMITED",
        }
    }
}

/// `resolveOverpassPreflight`: memory, then in-flight, then disk; only a
/// complete miss consults (and spends) the upstream limiter.
pub async fn resolve_overpass_preflight<P, D, A>(
    memory: Option<OverpassPayload>,
    pending: Option<P>,
    read_disk: D,
    allow_upstream: A,
    now: f64,
    cache_ms: f64,
) -> (Preflight, Option<OverpassPayload>)
where
    P: std::future::Future<Output = OverpassPayload>,
    D: std::future::Future<Output = Option<OverpassPayload>>,
    A: FnOnce() -> bool,
{
    if let Some(cached) = memory
        && cached.is_data()
        && cached.cached_at.is_some_and(|at| now - at <= cache_ms)
    {
        return (Preflight::Hit, Some(cached));
    }
    if let Some(pending) = pending {
        return (Preflight::Inflight, Some(pending.await));
    }
    if let Some(disk) = read_disk.await.filter(OverpassPayload::is_data) {
        return (Preflight::Disk, Some(disk));
    }
    if allow_upstream() {
        (Preflight::Upstream, None)
    } else {
        (Preflight::RateLimited, None)
    }
}

/// Module state of the JS proxy.
pub struct OverpassProxy {
    transport: Arc<OverpassTransport>,
    cache: Mutex<InsertionMap<OverpassPayload>>,
    in_flight: InFlight<String, OverpassPayload>,
    concurrent: AtomicUsize,
    limiter: RateLimiter,
    disk_dir: PathBuf,
}

impl OverpassProxy {
    pub fn new(transport: Arc<OverpassTransport>, disk_dir: PathBuf) -> Self {
        Self {
            transport,
            cache: Mutex::new(InsertionMap::default()),
            in_flight: InFlight::default(),
            concurrent: AtomicUsize::new(0),
            limiter: RateLimiter::new(60_000, 90, Some(300)),
            disk_dir,
        }
    }

    fn remember(&self, key: &str, payload: OverpassPayload) {
        let mut cache = self.cache.lock();
        cache.set(key, payload);
        cache.trim(OVERPASS_CACHE_MAX_ENTRIES);
    }

    /// `readStaleOverpass`: last-good data at any age.
    async fn read_stale(&self, key: &str) -> Option<OverpassPayload> {
        let cached = self.cache.lock().get(key).cloned();
        match cached {
            Some(payload) if payload.is_data() => Some(payload),
            _ => read_overpass_disk(&self.disk_dir, key, f64::INFINITY).await,
        }
    }

    fn write_disk(&self, key: &str, payload: &OverpassPayload) {
        let path = overpass_disk_path(&self.disk_dir, key);
        let value = payload.to_json();
        tokio::spawn(async move {
            if let Err(error) = disk::write_json(&path, &value).await {
                tracing::warn!("[Overpass Proxy] disk cache write failed: {error}");
            }
        });
    }
}

/// `sendOverpassResponse`.
fn send_payload(payload: &OverpassPayload, cache_status: &str) -> Response {
    let content_type = if payload.content_type.is_empty() {
        "application/json"
    } else {
        &payload.content_type
    };
    let cache_control = if payload.is_data() {
        "public, max-age=15"
    } else {
        "no-store"
    };
    let mut response = reply::raw(
        payload.status as u16,
        content_type,
        payload.body.clone(),
        Some(cache_control),
    );
    response = reply::with_header(response, "X-Overpass-Cache", cache_status);
    if let Some(at) = payload.cached_at.and_then(iso_from_ms) {
        response = reply::with_header(response, "X-Overpass-Cached-At", &at);
    }
    if let Some(retry) = payload.retry_after_ms.filter(|r| *r != 0.0 && !r.is_nan()) {
        response = reply::with_header(
            response,
            "Retry-After",
            &crate::providers::mapdata_support::js_number_string((retry / 1000.0).ceil()),
        );
    }
    response
}

fn json_error(status: u16, message: &str, retry_after: Option<&str>) -> Response {
    let response = reply::json(status, &json!({ "error": message }), None);
    match retry_after {
        Some(value) => reply::with_header(response, "Retry-After", value),
        None => response,
    }
}

async fn handle(proxy: Arc<OverpassProxy>, state: AppState, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let url = mounted_url(&parts.uri, "/api/overpass");
    if parts.method == http::Method::GET && url.split('?').next() == Some("/status") {
        return reply::json(
            200,
            &json!({ "configured": !resolve_overpass_upstreams(&state).is_empty() }),
            Some("no-store"),
        );
    }
    if parts.method != http::Method::POST {
        return json_error(405, "Method Not Allowed", None);
    }
    let body = match read_body_capped(body, OVERPASS_MAX_BODY_BYTES).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(BodyReadError::TooLarge) => return json_error(413, "Overpass query too large", None),
        Err(BodyReadError::Failed) => {
            tracing::error!("[Overpass Proxy] request failed");
            return json_error(502, "Overpass proxy error", None);
        }
    };
    if body.is_empty() {
        return json_error(400, "Missing Overpass query body", None);
    }
    let safe_body = match query::sanitize_overpass_body(&body) {
        Ok(safe) => safe,
        Err(error) => return json_error(400, error, None),
    };
    let cache_key =
        crate::providers::mapdata_support::js_trim(&collapse_js_space(&safe_body)).to_string();
    let endpoints = resolve_overpass_upstreams(&state);
    if endpoints.is_empty() {
        return match proxy.read_stale(&cache_key).await {
            Some(stale) => send_payload(&stale, "STALE"),
            None => send_payload(&overpass_not_configured(), "DISABLED"),
        };
    }

    let memory = proxy.cache.lock().get(&cache_key).cloned();
    let pending = proxy.in_flight.get(&cache_key);
    let peer = peer_key(&parts.extensions);
    let (source, payload) = resolve_overpass_preflight(
        memory,
        pending,
        read_overpass_disk(
            &proxy.disk_dir,
            &cache_key,
            overpass_disk_ttl_ms(&cache_key),
        ),
        || proxy.limiter.allow(&peer),
        js::now_ms() as f64,
        OVERPASS_CACHE_MS,
    )
    .await;
    if source == Preflight::RateLimited {
        return json_error(429, "Rate limit exceeded", Some("5"));
    }
    if source != Preflight::Upstream {
        let payload = payload.unwrap_or_else(overpass_not_configured);
        if !payload.is_data()
            && let Some(stale) = proxy.read_stale(&cache_key).await
        {
            return send_payload(&stale, "STALE");
        }
        if source == Preflight::Disk {
            proxy.remember(&cache_key, payload.clone());
        }
        return send_payload(&payload, source.header());
    }

    if proxy.concurrent.load(Ordering::SeqCst) >= OVERPASS_MAX_CONCURRENT {
        return json_error(503, "Overpass proxy busy — try again shortly", Some("2"));
    }
    proxy.concurrent.fetch_add(1, Ordering::SeqCst);
    let worker = Arc::clone(&proxy);
    let key = cache_key.clone();
    let task = proxy.in_flight.start(cache_key.clone(), async move {
        let payload = worker
            .transport
            .fetch(&safe_body, OVERPASS_MAX_RESPONSE_BYTES, &endpoints)
            .await;
        if payload.is_data() {
            let mut entry = payload.clone();
            entry.cached_at = Some(js::now_ms() as f64);
            worker.remember(&key, entry.clone());
            worker.write_disk(&key, &entry);
        }
        worker.concurrent.fetch_sub(1, Ordering::SeqCst);
        payload
    });
    let payload = task.await;
    if !payload.is_data()
        && let Some(stale) = proxy.read_stale(&cache_key).await
    {
        return send_payload(&stale, "STALE");
    }
    send_payload(&payload, "MISS")
}

/// `POST /api/overpass` (and `GET /api/overpass/status`).
pub fn router(state: &AppState) -> Router<AppState> {
    let proxy = Arc::new(OverpassProxy::new(
        shared_transport(),
        state.cache_dir.join("overpass"),
    ));
    let handler = move |State(state): State<AppState>, request: Request| {
        let proxy = Arc::clone(&proxy);
        async move { handle(proxy, state, request).await }
    };
    Router::new()
        .route("/api/overpass", any(handler.clone()))
        .route("/api/overpass/{*rest}", any(handler))
}

#[cfg(test)]
mod tests {
    use super::transport::tests::{Scripted, reply as raw_reply};
    use super::*;
    use crate::providers::mapdata_support::encode_uri_component;
    use axum::body::Body;
    use tower::ServiceExt;

    fn payload(status: f64, cached_at: f64) -> OverpassPayload {
        OverpassPayload {
            status,
            body: "{\"elements\":[]}".into(),
            content_type: "application/json".into(),
            endpoint: None,
            rate_limited: false,
            runtime_error: false,
            retry_after_ms: None,
            cached_at: Some(cached_at),
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ce-overpass-{tag}-{}-{}",
            std::process::id(),
            js::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn disk_cache_rejects_refusals_but_keeps_last_good_data() {
        let dir = temp_dir("disk");
        let key = "overpass-cache-regression";
        let file = overpass_disk_path(&dir, key);
        let now = js::now_ms() as f64;
        for refusal in [
            json!({"status": 406}),
            json!({"status": 429}),
            json!({"status": 503}),
            json!({"status": 200, "rateLimited": true}),
            json!({"status": 200, "runtimeError": true}),
        ] {
            let mut value = payload(200.0, now).to_json();
            for (k, v) in refusal.as_object().unwrap() {
                value[k] = v.clone();
            }
            std::fs::write(&file, value.to_string()).unwrap();
            assert!(read_overpass_disk(&dir, key, 60_000.0).await.is_none());
            assert!(read_overpass_disk(&dir, key, f64::INFINITY).await.is_none());
        }
        let good = payload(200.0, now - 120_000.0);
        std::fs::write(&file, good.to_json().to_string()).unwrap();
        assert!(read_overpass_disk(&dir, key, 60_000.0).await.is_none());
        assert_eq!(
            read_overpass_disk(&dir, key, f64::INFINITY).await,
            Some(good)
        );
        std::fs::write(&file, "{invalid").unwrap();
        assert!(read_overpass_disk(&dir, key, f64::INFINITY).await.is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn preflight_order_spends_quota_only_on_a_full_miss() {
        use std::cell::Cell;
        let fresh = payload(200.0, 900.0);
        let joined = payload(201.0, 950.0);
        let disk = payload(202.0, 975.0);
        let disk_reads = Cell::new(0);
        let limiter = Cell::new(0);
        let read = |value: Option<OverpassPayload>| {
            let disk_reads = &disk_reads;
            async move {
                disk_reads.set(disk_reads.get() + 1);
                value
            }
        };
        let allow = || {
            limiter.set(limiter.get() + 1);
            true
        };
        let (source, hit) = resolve_overpass_preflight(
            Some(fresh.clone()),
            Some(std::future::ready(joined.clone())),
            read(Some(disk.clone())),
            allow,
            1000.0,
            200.0,
        )
        .await;
        assert_eq!((source, hit), (Preflight::Hit, Some(fresh)));
        let (source, hit) = resolve_overpass_preflight(
            Some(payload(200.0, 0.0)),
            Some(std::future::ready(joined.clone())),
            read(Some(disk.clone())),
            allow,
            1000.0,
            200.0,
        )
        .await;
        assert_eq!((source, hit), (Preflight::Inflight, Some(joined)));
        assert_eq!(disk_reads.get(), 0);
        assert_eq!(limiter.get(), 0);
        let none: Option<std::future::Ready<OverpassPayload>> = None;
        let (source, hit) = resolve_overpass_preflight(
            None,
            none.clone(),
            read(Some(disk.clone())),
            allow,
            0.0,
            1.0,
        )
        .await;
        assert_eq!((source, hit), (Preflight::Disk, Some(disk)));
        assert_eq!(limiter.get(), 0);
        let (source, _) =
            resolve_overpass_preflight(None, none.clone(), read(None), allow, 0.0, 1.0).await;
        assert_eq!(source, Preflight::Upstream);
        assert_eq!(disk_reads.get(), 2);
        assert_eq!(limiter.get(), 1);
        let (source, _) =
            resolve_overpass_preflight(None, none.clone(), read(None), || false, 0.0, 1.0).await;
        assert_eq!(source, Preflight::RateLimited);

        // Cached refusals are misses that spend exactly one admission.
        for invalid in [payload(406.0, 0.0), {
            let mut p = payload(200.0, 0.0);
            p.runtime_error = true;
            p
        }] {
            let admissions = Cell::new(0);
            let (source, _) = resolve_overpass_preflight(
                Some(invalid.clone()),
                none.clone(),
                std::future::ready(Some(invalid)),
                || {
                    admissions.set(admissions.get() + 1);
                    true
                },
                0.0,
                OVERPASS_CACHE_MS,
            )
            .await;
            assert_eq!(source, Preflight::Upstream);
            assert_eq!(admissions.get(), 1);
        }
    }

    #[test]
    fn upstream_configuration_is_validated() {
        assert_eq!(
            parse_overpass_upstreams(
                "http://localhost:1234/api, https://paid.example/query?key=secret, http://localhost:1234/api"
            ),
            vec![
                "http://localhost:1234/api",
                "https://paid.example/query?key=secret"
            ]
        );
        assert_eq!(
            parse_overpass_upstreams("ftp://bad, nonsense, javascript:bad, http://10.0.0.1/query"),
            vec!["http://10.0.0.1/query"]
        );
        assert!(parse_overpass_upstreams("").is_empty());
    }

    fn proxy_with(http: Arc<Scripted>, dir: &Path) -> Arc<OverpassProxy> {
        Arc::new(OverpassProxy::new(
            Arc::new(OverpassTransport::new(http)),
            dir.to_path_buf(),
        ))
    }

    async fn post(
        proxy: &Arc<OverpassProxy>,
        state: &AppState,
        body: &str,
    ) -> (u16, http::HeaderMap, String) {
        let request = Request::builder()
            .method("POST")
            .uri("/api/overpass")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = handle(Arc::clone(proxy), state.clone(), request).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    fn test_state(upstreams: Option<&str>) -> AppState {
        let state = AppState::new(PathBuf::from("."), PathBuf::from("dist")).unwrap();
        state.env.set("OVERPASS_UPSTREAMS", upstreams.unwrap_or(""));
        state
    }

    #[tokio::test]
    async fn coalesced_outage_callers_both_get_last_good_data() {
        for status in [406u16, 503, 429] {
            let dir = temp_dir(&format!("outage-{status}"));
            let state = test_state(Some(&format!("https://outage-{status}.example/api")));
            let release = Arc::new(tokio::sync::Notify::new());
            let gate = Arc::clone(&release);
            let http = Arc::new(Scripted {
                seen: Mutex::new(Vec::new()),
                respond: Box::new(move |_, _| Ok(raw_reply(status, "upstream unavailable", None))),
            });
            let proxy = proxy_with(Arc::clone(&http), &dir);
            let query = format!(
                "[out:json][timeout:12];node(around:10,30.27,-97.74)[\"name\"=\"{}\"];out;",
                js::now_ms()
            );
            let body = format!("data={}", encode_uri_component(&query));
            let stale = payload(200.0, js::now_ms() as f64 - 40.0 * 86_400_000.0);
            let file = overpass_disk_path(&dir, &body);
            std::fs::write(&file, stale.to_json().to_string()).unwrap();
            // Hold the shared request open until both callers have arrived.
            proxy.concurrent.store(0, Ordering::SeqCst);
            let holder = proxy.in_flight.start(body.clone(), {
                let proxy = Arc::clone(&proxy);
                let safe = body.clone();
                let endpoints = resolve_overpass_upstreams(&state);
                async move {
                    gate.notified().await;
                    proxy.transport.fetch(&safe, 1024, &endpoints).await
                }
            });
            let first = tokio::spawn({
                let (proxy, state, body) = (Arc::clone(&proxy), state.clone(), body.clone());
                async move { post(&proxy, &state, &body).await }
            });
            let second = tokio::spawn({
                let (proxy, state, body) = (Arc::clone(&proxy), state.clone(), body.clone());
                async move { post(&proxy, &state, &body).await }
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
            release.notify_one();
            for result in [first.await.unwrap(), second.await.unwrap()] {
                assert_eq!(result.0, 200, "{status}");
                assert_eq!(result.2, "{\"elements\":[]}");
                assert_eq!(result.1["X-Overpass-Cache"], "STALE");
            }
            drop(holder);
            assert_eq!(http.seen.lock().len(), 1, "one shared configured request");
            let on_disk: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
            assert_eq!(on_disk, stale.to_json());
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[tokio::test]
    async fn guards_answer_without_upstream() {
        let dir = temp_dir("guards");
        let http = Arc::new(Scripted {
            seen: Mutex::new(Vec::new()),
            respond: Box::new(|_, _| panic!("invalid requests must not fetch")),
        });
        let proxy = proxy_with(http, &dir);
        let state = test_state(None);
        let app = Router::new()
            .route(
                "/api/overpass",
                any({
                    let proxy = Arc::clone(&proxy);
                    move |State(state): State<AppState>, request: Request| {
                        handle(Arc::clone(&proxy), state, request)
                    }
                }),
            )
            .route(
                "/api/overpass/{*rest}",
                any({
                    let proxy = Arc::clone(&proxy);
                    move |State(state): State<AppState>, request: Request| {
                        handle(Arc::clone(&proxy), state, request)
                    }
                }),
            )
            .with_state(state.clone());
        let call = |method: &str, uri: &str, body: &str| {
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let status = app
            .clone()
            .oneshot(call("GET", "/api/overpass/status", ""))
            .await
            .unwrap();
        assert_eq!(status.status(), 200);
        assert_eq!(status.headers()["cache-control"], "no-store");
        assert_eq!(
            axum::body::to_bytes(status.into_body(), 1024)
                .await
                .unwrap(),
            "{\"configured\":false}"
        );
        for (method, uri, body, code) in [
            ("GET", "/api/overpass", "", 405),
            ("DELETE", "/api/overpass", "", 405),
            ("POST", "/api/overpass", "", 400),
            ("POST", "/api/overpass", "data=way[highway];out;", 400),
        ] {
            let response = app.clone().oneshot(call(method, uri, body)).await.unwrap();
            assert_eq!(response.status(), code, "{method} {uri} {body}");
        }
        let big = "x".repeat(OVERPASS_MAX_BODY_BYTES + 1);
        let response = app
            .clone()
            .oneshot(call("POST", "/api/overpass", &big))
            .await
            .unwrap();
        assert_eq!(response.status(), 413);

        // Not configured: the capability refusal, or last-good data.
        let query = "data=".to_string()
            + &encode_uri_component("[out:json];node(around:10,30,-97)[\"name\"=\"q\"];out;");
        let (code, headers, text) = post(&proxy, &state, &query).await;
        assert_eq!(code, 503);
        assert_eq!(headers["X-Overpass-Cache"], "DISABLED");
        assert!(text.contains("OVERPASS_NOT_CONFIGURED"));
        let cached_at = js::now_ms() as f64 - 60.0 * 86_400_000.0;
        let mut entry = payload(200.0, cached_at);
        entry.endpoint = Some("https://secret.example/private?token=secret".into());
        proxy.remember(&query, entry);
        let (code, headers, _) = post(&proxy, &state, &query).await;
        assert_eq!(code, 200);
        assert_eq!(headers["X-Overpass-Cache"], "STALE");
        assert_eq!(
            headers["X-Overpass-Cached-At"],
            iso_from_ms(cached_at).unwrap().as_str()
        );
        assert!(!format!("{headers:?}").contains("secret"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn configured_refusal_is_safe_with_retry_after() {
        let dir = temp_dir("refusal");
        let state = test_state(Some(
            "https://configured-redact.example/private?token=secret",
        ));
        let http = Arc::new(Scripted {
            seen: Mutex::new(Vec::new()),
            respond: Box::new(|url, _| Ok(raw_reply(406, url, Some("45")))),
        });
        let proxy = proxy_with(http, &dir);
        let query = "data=".to_string()
            + &encode_uri_component(&format!(
                "[out:json];node(around:10,33,-97)[\"name\"=\"{}\"];out;",
                js::now_ms()
            ));
        let (code, headers, text) = post(&proxy, &state, &query).await;
        assert_eq!(code, 406);
        assert_eq!(headers["retry-after"], "45");
        assert!(!text.contains("secret") && !text.contains("https:"));
        let _ = std::fs::remove_dir_all(dir);
    }
}
