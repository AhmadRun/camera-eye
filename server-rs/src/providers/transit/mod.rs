//! GTFS-Realtime transit proxy for the Transit data layer (port of
//! `server/providers/transit.js` and `src/sources/transitService.js`).
//!
//!   GET /api/transit/feeds                     → public catalog
//!   GET /api/transit/vehicles/<feedId>         → decoded snapshot as JSON
//!   GET /api/transit/trail/<feedId>/<vehicle>  → retained position history
//!
//! Only URLs registered in [`feeds`] are ever fetched. Redirects are
//! resolved hop by hop and must stay on the feed's own https origin. Per
//! feed: 15 s memory cache, single-flight refresh, serve-stale-on-failure
//! for up to 10 minutes, a fixed-window admission limiter and a cooldown
//! ladder after consecutive failures.

pub mod feeds;
pub mod gtfs;
pub mod history;
pub mod proxy;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::json;

use self::feeds::{TransitFeed, public_transit_catalog};
use self::history::TransitHistory;
use self::proxy::*;
use crate::common::http::{UpstreamError, read_bytes_capped};
use crate::common::rate_limit::RateLimiter;
use crate::providers::mapdata_support::{InFlight, mounted_url};
use crate::state::AppState;

/// One upstream answer: its status, the headers the proxy reads, and a
/// capped body read that only starts when awaited.
pub struct UpstreamReply {
    pub status: u16,
    pub location: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub body: BoxFuture<'static, Result<Vec<u8>, UpstreamError>>,
}

/// The upstream fetch (`fetchImpl` with `redirect: 'manual'`).
pub trait TransitFetch: Send + Sync {
    fn get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
    ) -> BoxFuture<'static, Result<UpstreamReply, String>>;
}

/// The production fetcher: redirects are never followed by the client.
pub struct ReqwestTransit {
    client: reqwest::Client,
}

impl ReqwestTransit {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

impl TransitFetch for ReqwestTransit {
    fn get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
    ) -> BoxFuture<'static, Result<UpstreamReply, String>> {
        let mut request = self.client.get(url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        Box::pin(async move {
            let response = request.send().await.map_err(|e| e.to_string())?;
            let header = |name: &str| {
                response
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            };
            Ok(UpstreamReply {
                status: response.status().as_u16(),
                location: header("location"),
                etag: header("etag"),
                last_modified: header("last-modified"),
                body: Box::pin(async move {
                    read_bytes_capped(response, TRANSIT_PROXY_MAX_BODY_BYTES)
                        .await
                        .map(|b| b.to_vec())
                }),
            })
        })
    }
}

/// Why a refresh failed; decides 502 versus 504.
#[derive(Clone, Debug)]
enum RefreshError {
    Differential(f64),
    TooLarge,
    Status(u16),
    Other(String),
}

impl RefreshError {
    fn message(&self) -> String {
        match self {
            Self::Differential(n) => format!(
                "feed is differential (incrementality {})",
                crate::providers::mapdata_support::js_number_string(*n)
            ),
            Self::TooLarge => "Upstream response too large".into(),
            Self::Status(status) => format!("upstream HTTP {status}"),
            Self::Other(message) => message.clone(),
        }
    }
}

struct CacheEntry {
    at: f64,
    contacted_at: f64,
    body: Arc<str>,
    host: String,
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Clone, Copy)]
struct Cooldown {
    failures: f64,
    next_attempt_at: f64,
}

type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;
type Refresh = Result<Arc<CacheEntry>, RefreshError>;

/// `createTransitService`.
pub struct TransitService {
    fetch: Arc<dyn TransitFetch>,
    clock: Clock,
    cache: Mutex<HashMap<&'static str, Arc<CacheEntry>>>,
    history: Arc<TransitHistory>,
    admit_history: RateLimiter,
    admit: RateLimiter,
    in_flight: InFlight<&'static str, Refresh>,
    cooldown: Mutex<HashMap<&'static str, Cooldown>>,
    closed: AtomicBool,
}

/// `fetchTransitFeed`: each redirect hop is judged before it is requested.
async fn fetch_transit_feed(
    fetch: &dyn TransitFetch,
    feed: &TransitFeed,
    headers: Vec<(String, String)>,
) -> Result<(UpstreamReply, String), RefreshError> {
    let mut current = feed.url.to_string();
    for hop in 0..=TRANSIT_MAX_REDIRECTS {
        let reply = fetch
            .get(&current, headers.clone())
            .await
            .map_err(RefreshError::Other)?;
        if !is_transit_redirect_status(reply.status) {
            return Ok((reply, current));
        }
        let decision = transit_redirect_decision(feed.url, &current, reply.location.as_deref());
        drop(reply);
        match decision {
            Err(reason) => return Err(RefreshError::Other(format!("upstream {reason}"))),
            Ok(_) if hop == TRANSIT_MAX_REDIRECTS => {
                return Err(RefreshError::Other(
                    "upstream redirected too many times".into(),
                ));
            }
            Ok(next) => current = next,
        }
    }
    Err(RefreshError::Other(
        "upstream redirected too many times".into(),
    ))
}

fn reply(status: u16, body: impl Into<Body>, headers: &[(&str, String)]) -> Response {
    let mut response = Response::new(body.into());
    *response.status_mut() =
        http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(name.as_bytes()),
            http::HeaderValue::from_str(value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

fn json_error(status: u16, message: &str) -> Response {
    reply(
        status,
        json!({ "error": message }).to_string(),
        &[
            ("Content-Type", "application/json".into()),
            ("Cache-Control", "no-store".into()),
        ],
    )
}

fn seconds_until(at: f64, now: f64) -> i64 {
    ((at - now) / 1000.0).ceil().max(1.0) as i64
}

impl TransitService {
    pub fn new(fetch: Arc<dyn TransitFetch>, clock: Clock) -> Arc<Self> {
        let history = TransitHistory::new(Arc::clone(&clock));
        Arc::new(Self {
            fetch,
            clock,
            cache: Mutex::new(HashMap::new()),
            history,
            admit_history: RateLimiter::new(60_000, 30, Some(120)),
            admit: RateLimiter::new(
                TRANSIT_ADMISSION_WINDOW_MS,
                TRANSIT_ADMISSION_MAX_PER_FEED,
                Some(TRANSIT_ADMISSION_MAX_GLOBAL),
            ),
            in_flight: InFlight::default(),
            cooldown: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    /// `close`: refuse further requests and release every cache.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.history.clear();
        self.cache.lock().clear();
        self.cooldown.lock().clear();
    }

    fn note_failure(&self, feed_id: &'static str, now: f64) -> f64 {
        let mut cooldown = self.cooldown.lock();
        let failures = cooldown.get(feed_id).map_or(0.0, |c| c.failures) + 1.0;
        let wait = next_transit_backoff_ms(failures);
        cooldown.insert(
            feed_id,
            Cooldown {
                failures,
                next_attempt_at: now + wait,
            },
        );
        wait
    }

    async fn refresh(self: Arc<Self>, feed: &'static TransitFeed, now: f64) -> Refresh {
        let previous = self.cache.lock().get(feed.id).cloned();
        let attempt = async {
            let headers = transit_upstream_headers(
                Some(feed),
                previous.as_ref().and_then(|p| p.etag.as_deref()),
                previous.as_ref().and_then(|p| p.last_modified.as_deref()),
            );
            let (response, final_url) =
                fetch_transit_feed(self.fetch.as_ref(), feed, headers).await?;
            if self.closed.load(Ordering::SeqCst) {
                return Err(RefreshError::Other("Transit provider closed".into()));
            }
            if !is_acceptable_transit_upstream_url(&final_url) {
                return Err(RefreshError::Other("upstream is not https".into()));
            }
            if response.status == 304
                && let Some(previous) = &previous
            {
                // `at` restarts the freshness window; `contactedAt`
                // records that the operator answered just now.
                let revalidated = Arc::new(CacheEntry {
                    at: now,
                    contacted_at: now,
                    body: Arc::clone(&previous.body),
                    host: previous.host.clone(),
                    etag: previous.etag.clone(),
                    last_modified: previous.last_modified.clone(),
                });
                self.cache.lock().insert(feed.id, Arc::clone(&revalidated));
                self.cooldown.lock().remove(feed.id);
                return Ok(revalidated);
            }
            if !(200..300).contains(&response.status) {
                return Err(RefreshError::Status(response.status));
            }
            let bytes = response.body.await.map_err(|error| match error {
                UpstreamError::TooLarge => RefreshError::TooLarge,
                other => RefreshError::Other(other.to_string()),
            })?;
            let snapshot =
                build_transit_snapshot(feed, &bytes, now).map_err(|error| match error {
                    SnapshotError::Differential(n) => RefreshError::Differential(n),
                    SnapshotError::Malformed => {
                        RefreshError::Other("malformed GTFS-Realtime payload".into())
                    }
                })?;
            if self.closed.load(Ordering::SeqCst) {
                return Err(RefreshError::Other("Transit provider closed".into()));
            }
            self.history
                .ingest(feed, &snapshot.vehicles, (self.clock)());
            let entry = Arc::new(CacheEntry {
                at: snapshot.fetched_at,
                contacted_at: snapshot.fetched_at,
                body: Arc::from(snapshot.body.to_string()),
                host: url::Url::parse(&final_url)
                    .ok()
                    .and_then(|u| u.host_str().map(str::to_string))
                    .unwrap_or_default(),
                etag: response.etag,
                last_modified: response.last_modified,
            });
            self.cache.lock().insert(feed.id, Arc::clone(&entry));
            self.cooldown.lock().remove(feed.id);
            Ok(entry)
        };
        let result = tokio::time::timeout(Duration::from_millis(TRANSIT_PROXY_TIMEOUT_MS), attempt)
            .await
            .unwrap_or_else(|_| Err(RefreshError::Other("This operation was aborted".into())));
        if let Err(error) = &result {
            // A shape fault condemns what we already hold.
            if matches!(error, RefreshError::Differential(_)) {
                self.cache.lock().remove(feed.id);
            }
            // Once per failed refresh, however many callers shared it.
            let wait = self.note_failure(feed.id, (self.clock)());
            tracing::warn!(
                "[transit-proxy] {} unavailable: {} — next attempt in {}s",
                feed.id,
                error.message(),
                (wait / 1000.0).round()
            );
        }
        result
    }

    /// `handle`: one request under the `/api/transit` mount.
    pub async fn handle(self: &Arc<Self>, method: &http::Method, uri: &http::Uri) -> Response {
        let mounted = mounted_url(uri, "/api/transit");
        let Ok(url) = url::Url::parse(&format!("http://localhost/api/transit{mounted}")) else {
            return json_error(404, "Unknown transit feed");
        };
        let Some(rest) = url
            .path()
            .strip_prefix("/api/transit")
            .filter(|r| r.starts_with('/'))
        else {
            return json_error(404, "Unknown transit feed");
        };
        let search = url
            .query()
            .filter(|q| !q.is_empty())
            .map(|q| format!("?{q}"));
        let req_url = format!("{rest}{}", search.unwrap_or_default());
        if self.closed.load(Ordering::SeqCst) {
            return json_error(503, "Transit provider closed");
        }
        if method != http::Method::GET {
            return reply(
                405,
                json!({ "error": "Method Not Allowed" }).to_string(),
                &transit_response_headers("NONE", "", None),
            );
        }
        let Some(route) = resolve_transit_route(&req_url) else {
            return json_error(404, "Unknown transit feed");
        };
        let feed = match route {
            TransitRoute::Feeds => {
                return reply(
                    200,
                    json!({ "feeds": public_transit_catalog() }).to_string(),
                    &[
                        ("Content-Type", "application/json; charset=utf-8".into()),
                        ("Cache-Control", "public, max-age=3600".into()),
                    ],
                );
            }
            TransitRoute::Trail(feed, vehicle_id) => {
                let now = (self.clock)();
                if !self.admit_history.allow(feed.id) {
                    return reply(
                        429,
                        json!({ "error": "Transit history read limit reached" }).to_string(),
                        &[
                            ("Content-Type", "application/json".into()),
                            ("Cache-Control", "no-store".into()),
                            ("Retry-After", "60".into()),
                        ],
                    );
                }
                return reply(
                    200,
                    self.history.get(feed.id, &vehicle_id, now).to_string(),
                    &[
                        ("Content-Type", "application/json".into()),
                        ("Cache-Control", "no-store".into()),
                    ],
                );
            }
            TransitRoute::Vehicles(feed) => feed,
        };
        let now = (self.clock)();
        let ttl_seconds = (TRANSIT_PROXY_TTL_MS / 1000.0).ceil() as i64;
        let cached = self.cache.lock().get(feed.id).cloned();
        let state = transit_cache_state(cached.as_ref().map(|c| c.at), now);
        let serve = |entry: &CacheEntry, cache_state: &str| {
            reply(
                200,
                entry.body.to_string(),
                &transit_response_headers(cache_state, &entry.host, Some(entry.contacted_at)),
            )
        };
        if state == CacheState::Fresh
            && let Some(cached) = &cached
        {
            return serve(cached, "HIT");
        }
        // Answer without contacting the operator.
        let without_upstream = |reason: &str, retry_after_s: i64| {
            if let (CacheState::Stale, Some(cached)) = (state, &cached) {
                return serve(cached, "STALE-ERROR");
            }
            reply(
                503,
                json!({
                    "error": "Transit feed unavailable",
                    "feedId": feed.id,
                    "retryInSec": retry_after_s,
                })
                .to_string(),
                &[
                    ("Content-Type", "application/json".into()),
                    ("Cache-Control", "no-store".into()),
                    ("X-CE-Cache", "NONE".into()),
                    ("Retry-After", retry_after_s.to_string()),
                    ("X-Transit-Backoff", reason.into()),
                ],
            )
        };
        let waiting = self.cooldown.lock().get(feed.id).copied();
        if let Some(waiting) = waiting
            && waiting.next_attempt_at > now
            && !self.in_flight.contains(&feed.id)
        {
            return without_upstream("cooldown", seconds_until(waiting.next_attempt_at, now));
        }
        if !self.in_flight.contains(&feed.id) && !self.admit.allow(feed.id) {
            return without_upstream("rate-limited", ttl_seconds);
        }
        let service = Arc::clone(self);
        let (request, shared) = self
            .in_flight
            .coalesce(feed.id, move || service.refresh(feed, now));
        match request.await {
            Ok(fresh) => serve(&fresh, if shared { "INFLIGHT" } else { "MISS" }),
            Err(error) => {
                let differential = matches!(error, RefreshError::Differential(_));
                if let (CacheState::Stale, Some(cached), false) = (state, &cached, differential) {
                    return serve(cached, "STALE-ERROR");
                }
                let pending = self.cooldown.lock().get(feed.id).copied();
                let retry_after_s = pending.map_or(ttl_seconds, |p| {
                    seconds_until(p.next_attempt_at, (self.clock)())
                });
                let status = match error {
                    RefreshError::Differential(_)
                    | RefreshError::TooLarge
                    | RefreshError::Status(_) => 502,
                    RefreshError::Other(_) => 504,
                };
                reply(
                    status,
                    json!({
                        "error": if differential {
                            "Transit feed is differential and is not supported"
                        } else {
                            "Transit feed unavailable"
                        },
                        "feedId": feed.id,
                        "retryInSec": retry_after_s,
                    })
                    .to_string(),
                    &[
                        ("Content-Type", "application/json".into()),
                        ("Cache-Control", "no-store".into()),
                        ("X-CE-Cache", "NONE".into()),
                        ("Retry-After", retry_after_s.to_string()),
                    ],
                )
            }
        }
    }
}

/// Mount a service at `/api/transit` (and every path under it).
pub fn routes(service: Arc<TransitService>) -> Router<AppState> {
    let handler = move |request: Request| {
        let service = Arc::clone(&service);
        async move { service.handle(request.method(), request.uri()).await }
    };
    Router::new()
        .route("/api/transit", any(handler.clone()))
        .route("/api/transit/{*rest}", any(handler))
}

pub fn router(_state: &AppState) -> Router<AppState> {
    let clock: Clock = Arc::new(|| crate::common::js::now_ms() as f64);
    routes(TransitService::new(Arc::new(ReqwestTransit::new()), clock))
}

#[cfg(test)]
mod tests {
    use super::gtfs::tests::{Fixture, encode_feed};
    use super::*;
    use serde_json::Value;
    use tower::ServiceExt;

    const T0: f64 = 1_700_000_000_000.0;

    #[derive(Clone)]
    enum Step {
        Reply {
            status: u16,
            location: Option<&'static str>,
            etag: Option<&'static str>,
            bytes: Vec<u8>,
        },
    }

    fn ok(bytes: Vec<u8>) -> Step {
        Step::Reply {
            status: 200,
            location: None,
            etag: None,
            bytes,
        }
    }

    fn status(status: u16) -> Step {
        Step::Reply {
            status,
            location: None,
            etag: None,
            bytes: Vec::new(),
        }
    }

    type Call = (String, Vec<(String, String)>);

    #[derive(Default)]
    struct Scripted {
        steps: Vec<Step>,
        calls: Mutex<Vec<Call>>,
    }

    impl TransitFetch for Scripted {
        fn get(
            &self,
            url: &str,
            headers: Vec<(String, String)>,
        ) -> BoxFuture<'static, Result<UpstreamReply, String>> {
            let index = {
                let mut calls = self.calls.lock();
                calls.push((url.to_string(), headers));
                calls.len() - 1
            };
            let step = self
                .steps
                .get(index.min(self.steps.len().saturating_sub(1)))
                .cloned();
            Box::pin(async move {
                match step {
                    None => Err("unexpected upstream".into()),
                    Some(Step::Reply {
                        status,
                        location,
                        etag,
                        bytes,
                    }) => Ok(UpstreamReply {
                        status,
                        location: location.map(str::to_string),
                        etag: etag.map(str::to_string),
                        last_modified: None,
                        body: Box::pin(async move { Ok(bytes) }),
                    }),
                }
            })
        }
    }

    impl Scripted {
        fn count(&self) -> usize {
            self.calls.lock().len()
        }
    }

    struct Harness {
        app: Router,
        upstream: Arc<Scripted>,
        now: Arc<Mutex<f64>>,
        service: Arc<TransitService>,
    }

    fn harness(steps: Vec<Step>) -> Harness {
        let upstream = Arc::new(Scripted {
            steps,
            ..Default::default()
        });
        let now = Arc::new(Mutex::new(T0));
        let clock_now = Arc::clone(&now);
        let service = TransitService::new(upstream.clone(), Arc::new(move || *clock_now.lock()));
        let state = AppState::new(".".into(), "dist".into()).unwrap();
        Harness {
            app: routes(Arc::clone(&service)).with_state(state),
            upstream,
            now,
            service,
        }
    }

    impl Harness {
        fn advance(&self, ms: f64) {
            *self.now.lock() += ms;
        }

        async fn call(&self, path: &str) -> (u16, http::HeaderMap, String) {
            self.call_method("GET", path).await
        }

        async fn call_method(&self, method: &str, path: &str) -> (u16, http::HeaderMap, String) {
            let response = self
                .app
                .clone()
                .oneshot(
                    http::Request::builder()
                        .method(method)
                        .uri(format!("/api/transit{path}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            (status, headers, String::from_utf8(bytes.to_vec()).unwrap())
        }
    }

    fn feed_bytes(incrementality: u64) -> Vec<u8> {
        encode_feed(
            &[Fixture {
                entity_id: Some("bus-1".into()),
                vehicle_id: Some("bus-1".into()),
                lat: Some(42.36),
                lon: -71.06,
                timestamp: Some(1_700_000_000),
                ..Default::default()
            }],
            Some(incrementality),
            Some(1_700_000_000),
        )
    }

    fn header<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
        headers.get(name).and_then(|v| v.to_str().ok())
    }

    fn body(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[tokio::test]
    async fn service_confines_requests_to_registered_feeds_and_get() {
        let h = harness(Vec::new());
        for path in [
            "/other",
            "/vehicles/unknown",
            "/vehicles/https%3A%2F%2Fexample.test",
        ] {
            assert_eq!(h.call(path).await.0, 404, "{path}");
        }
        for method in ["POST", "HEAD", "TRACE"] {
            assert_eq!(h.call_method(method, "/feeds").await.0, 405);
        }
        let (status, headers, text) = h.call("/feeds").await;
        assert_eq!(status, 200);
        assert_eq!(
            header(&headers, "cache-control"),
            Some("public, max-age=3600")
        );
        assert!(
            body(&text)["feeds"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f["id"] == "mbta")
        );
        assert_eq!(h.upstream.count(), 0);
        h.service.close();
        assert_eq!(h.call("/feeds").await.0, 503);
    }

    #[tokio::test]
    async fn only_registered_feed_ids_reach_an_operator() {
        let h = harness(vec![ok(feed_bytes(0))]);
        for path in [
            "/vehicles/not-a-feed",
            "/vehicles/..%2F..%2Fetc",
            "/vehicles/",
            "/elsewhere",
            "",
        ] {
            assert_eq!(h.call(path).await.0, 404, "{path}");
        }
        assert_eq!(h.upstream.count(), 0);
        let catalog = body(&h.call("/feeds").await.2);
        for feed in catalog["feeds"].as_array().unwrap() {
            assert!(feed.get("url").is_none() && feed.get("headers").is_none());
        }
        let (status, headers, text) = h.call_method("PUT", "/vehicles/mbta").await;
        assert_eq!(status, 405);
        assert_eq!(header(&headers, "x-ce-cache"), Some("NONE"));
        assert_eq!(text, r#"{"error":"Method Not Allowed"}"#);
    }

    #[tokio::test]
    async fn a_304_revalidation_is_a_success_and_reports_contact() {
        let h = harness(vec![
            Step::Reply {
                status: 200,
                location: None,
                etag: Some("W/\"v1\""),
                bytes: feed_bytes(0),
            },
            Step::Reply {
                status: 304,
                location: None,
                etag: Some("W/\"v1\""),
                bytes: Vec::new(),
            },
        ]);
        let first = h.call("/vehicles/mbta").await;
        assert_eq!(first.0, 200);
        assert_eq!(header(&first.1, "x-ce-cache"), Some("MISS"));
        assert_eq!(header(&first.1, "x-transit-contact"), Some("1700000000000"));
        assert_eq!(header(&first.1, "x-transit-upstream"), Some("cdn.mbta.com"));
        assert_eq!(
            header(&first.1, "content-type"),
            Some("application/json; charset=utf-8")
        );
        let snapshot = body(&first.2);
        assert_eq!(snapshot["count"], 1);
        assert_eq!(snapshot["fetchedAt"], 1_700_000_000_000_u64);
        assert_eq!(snapshot["vehicles"][0]["timestampSource"], "vehicle");

        for _ in 0..2 {
            h.advance(TRANSIT_PROXY_TTL_MS + 1000.0);
            let revalidated_at = *h.now.lock();
            let again = h.call("/vehicles/mbta").await;
            assert_eq!(again.0, 200);
            assert_eq!(header(&again.1, "x-ce-cache"), Some("MISS"));
            assert_eq!(body(&again.2)["fetchedAt"], 1_700_000_000_000_u64);
            assert_eq!(
                header(&again.1, "x-transit-contact"),
                Some(revalidated_at.to_string().as_str())
            );
            assert!(
                again.1.get("retry-after").is_none() && again.1.get("x-transit-backoff").is_none()
            );
        }
        let calls = h.upstream.calls.lock().clone();
        assert_eq!(calls.len(), 3);
        assert!(
            calls[1]
                .1
                .iter()
                .any(|(n, v)| n == "If-None-Match" && v == "W/\"v1\"")
        );
        let hit = h.call("/vehicles/mbta").await;
        assert_eq!(header(&hit.1, "x-ce-cache"), Some("HIT"));
        assert_eq!(h.upstream.count(), 3);
    }

    #[tokio::test]
    async fn a_differential_feed_condemns_the_previous_snapshot() {
        let h = harness(vec![ok(feed_bytes(0)), ok(feed_bytes(1))]);
        assert_eq!(h.call("/vehicles/mbta").await.0, 200);
        h.advance(TRANSIT_PROXY_TTL_MS + 1000.0);
        let refused = h.call("/vehicles/mbta").await;
        assert_eq!(refused.0, 502);
        assert!(
            body(&refused.2)["error"]
                .as_str()
                .unwrap()
                .contains("differential")
        );
        h.advance(2000.0);
        let after = h.call("/vehicles/mbta").await;
        assert_ne!(after.0, 200, "{}", after.2);
        assert_eq!(header(&after.1, "x-ce-cache"), Some("NONE"));
        assert_eq!(header(&after.1, "x-transit-backoff"), Some("cooldown"));
    }

    #[tokio::test]
    async fn a_failing_operator_is_put_on_a_cooldown() {
        let h = harness(vec![status(500)]);
        let failed = h.call("/vehicles/mbta").await;
        assert_eq!(failed.0, 502);
        assert_eq!(header(&failed.1, "retry-after"), Some("5"));
        assert_eq!(
            failed.2,
            r#"{"error":"Transit feed unavailable","feedId":"mbta","retryInSec":5}"#
        );
        assert_eq!(h.upstream.count(), 1);
        h.advance(1000.0);
        let held = h.call("/vehicles/mbta").await;
        assert_eq!(held.0, 503);
        assert_eq!(header(&held.1, "x-transit-backoff"), Some("cooldown"));
        assert_eq!(header(&held.1, "retry-after"), Some("4"));
        assert_eq!(h.upstream.count(), 1);
        h.advance(TRANSIT_BACKOFF_LADDER_MS[0]);
        let retried = h.call("/vehicles/mbta").await;
        assert_eq!(h.upstream.count(), 2);
        assert_eq!(header(&retried.1, "retry-after"), Some("15"));
    }

    #[tokio::test]
    async fn redirects_stay_on_the_feed_origin_and_are_never_contacted_first() {
        let same = harness(vec![
            Step::Reply {
                status: 302,
                location: Some("/files/current.pb"),
                etag: None,
                bytes: Vec::new(),
            },
            ok(feed_bytes(0)),
        ]);
        assert_eq!(same.call("/vehicles/capmetro-austin").await.0, 200);
        let calls = same.upstream.calls.lock().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].0, "https://data.texas.gov/files/current.pb");

        let off = harness(vec![
            Step::Reply {
                status: 302,
                location: Some("https://cdn.elsewhere.test/current.pb"),
                etag: None,
                bytes: Vec::new(),
            },
            ok(feed_bytes(0)),
        ]);
        assert_eq!(off.call("/vehicles/capmetro-austin").await.0, 504);
        assert_eq!(off.upstream.count(), 1);
    }

    #[tokio::test]
    async fn served_from_cache_during_an_outage_keeps_the_last_contact() {
        let h = harness(vec![ok(feed_bytes(0)), status(500)]);
        h.call("/vehicles/mbta").await;
        h.advance(TRANSIT_PROXY_TTL_MS + 1000.0);
        let during = h.call("/vehicles/mbta").await;
        assert_eq!(during.0, 200);
        assert_eq!(header(&during.1, "x-ce-cache"), Some("STALE-ERROR"));
        assert_eq!(header(&during.1, "cache-control"), Some("no-store"));
        assert_eq!(
            header(&during.1, "x-transit-contact"),
            Some("1700000000000")
        );
    }

    #[tokio::test]
    async fn history_reads_never_fetch_and_replays_never_append() {
        let h = harness(vec![ok(feed_bytes(0)), status(304), status(503)]);
        let read = || h.call("/trail/mbta/bus-1");
        assert_eq!(body(&read().await.2)["fixes"].as_array().unwrap().len(), 0);
        assert_eq!(h.upstream.count(), 0);
        h.call("/vehicles/mbta").await;
        let initial = body(&read().await.2);
        assert_eq!(initial["fixes"].as_array().unwrap().len(), 1);
        h.call("/vehicles/mbta").await;
        assert_eq!(h.upstream.count(), 1);
        h.advance(TRANSIT_PROXY_TTL_MS + 1.0);
        h.call("/vehicles/mbta").await;
        assert_eq!(body(&read().await.2), initial);
        h.advance(TRANSIT_PROXY_TTL_MS + 1.0);
        h.call("/vehicles/mbta").await;
        assert_eq!(body(&read().await.2), initial);
        assert_eq!(h.upstream.count(), 3);
        let long = format!("/trail/mbta/{}", "a".repeat(257));
        for path in [
            "/trail/mbta/%2Fescape",
            "/trail/mbta/abc%00bad",
            "/trail/mbta/%E0%A4%A",
            long.as_str(),
            "/trail/capmetro-austin/bus-1",
        ] {
            assert_eq!(h.call(path).await.0, 404, "{path}");
        }
        assert_eq!(h.upstream.count(), 3);
        h.service.close();
    }

    #[tokio::test]
    async fn history_admission_is_separate_from_upstream_admission() {
        let h = harness(vec![ok(feed_bytes(0))]);
        for _ in 0..30 {
            assert_eq!(h.call("/trail/mbta/bus-1").await.0, 200);
        }
        let limited = h.call("/trail/mbta/bus-1").await;
        assert_eq!(limited.0, 429);
        assert_eq!(header(&limited.1, "retry-after"), Some("60"));
        assert_eq!(h.upstream.count(), 0);
        assert_eq!(h.call("/vehicles/mbta").await.0, 200);
        assert_eq!(h.upstream.count(), 1);
        h.service.close();
    }
}
