//! `GET /api/route` (port of `server/providers/places/routes.js`): real OSM
//! routing through the public FOSSGIS OSRM servers, with a response cache,
//! in-flight sharing and a one-per-second outbound gate.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::response::Response;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use serde_json::{Map, Value, json};

use super::super::js_value::{get, number_to_string, round};
use super::payloads::{normalize_route_profile, project_route_result};
use super::route_steps::normalize_osrm_steps;
use super::{Places, peer};
use crate::common::geo::haversine_km;
use crate::common::http::read_text_capped;
use crate::common::js;
use crate::common::query::Params;
use crate::common::rate_limit::{RateLimiter, client_key};
use crate::common::reply;

/// Cache lifetime for one route.
pub const ROUTE_CACHE_MS: i64 = 600_000;
/// Hard cap on the OSRM response we will buffer.
pub const ROUTE_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Minimum gap between two outbound route requests.
pub const ROUTE_UPSTREAM_MIN_INTERVAL_MS: i64 = 1000;
/// How many requests may wait for the outbound gate at once.
pub const ROUTE_UPSTREAM_QUEUE_MAX: usize = 8;
/// Overall budget for one upstream call, gate wait included.
const ROUTE_UPSTREAM_TIMEOUT: Duration = Duration::from_millis(12_000);
const ROUTE_MAX_LEG_KM: f64 = 600.0;
const ROUTE_MAX_TOTAL_KM: f64 = 2500.0;
const ROUTE_CACHE_MAX: usize = 200;

/// Why an upstream call produced no answer.
#[derive(Debug, Clone)]
enum RouteFailure {
    /// `RouteBusyError`: the outbound queue is full.
    Busy { retry_after_ms: i64 },
    /// `UpstreamRateLimitError`: the routing service rate-limited us.
    UpstreamRateLimit { retry_after_sec: f64 },
    /// Anything thrown: timeout, network, oversize, bad JSON, redirect.
    Error(String),
}

/// `{ payload, error }`.
type RouteOutcome = Result<(Option<Map<String, Value>>, Option<String>), RouteFailure>;

struct CacheEntry {
    payload: Map<String, Value>,
    has_steps: bool,
    cached_at: i64,
}

/// A `Map` with JS insertion-order semantics (re-setting keeps the position).
#[derive(Default)]
struct RouteCache {
    entries: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
}

impl RouteCache {
    fn set(&mut self, key: String, entry: CacheEntry) {
        if self.entries.insert(key.clone(), entry).is_none() {
            self.order.push_back(key);
        }
        if self.entries.len() > ROUTE_CACHE_MAX
            && let Some(oldest) = self.order.pop_front()
        {
            self.entries.remove(&oldest);
        }
    }
}

#[derive(Default)]
struct GateState {
    next_upstream_at: i64,
    queue_depth: usize,
}

type Pending = Shared<BoxFuture<'static, RouteOutcome>>;

pub struct RouteService {
    /// Per-route base URL overrides (`endpoints[profile]`).
    endpoints: HashMap<String, String>,
    interval_ms: i64,
    limiter: RateLimiter,
    cache: Mutex<RouteCache>,
    inflight: Arc<Mutex<HashMap<String, (u64, Pending)>>>,
    next_inflight_id: Mutex<u64>,
    gate: Arc<Mutex<GateState>>,
}

/// Holds one outbound slot; releases it however the call ends.
struct GateTicket(Arc<Mutex<GateState>>);

impl Drop for GateTicket {
    fn drop(&mut self) {
        self.0.lock().queue_depth -= 1;
    }
}

impl RouteService {
    pub fn new(endpoints: HashMap<String, String>, interval_ms: i64) -> Self {
        Self {
            endpoints,
            interval_ms,
            limiter: RateLimiter::new(60_000, 60, Some(200)),
            cache: Mutex::new(RouteCache::default()),
            inflight: Arc::default(),
            next_inflight_id: Mutex::new(0),
            gate: Arc::default(),
        }
    }

    #[cfg(test)]
    pub fn inflight_count(&self) -> usize {
        self.inflight.lock().len()
    }
}

/// `throughUpstreamGate`: wait for this request's outbound slot.
async fn through_upstream_gate(
    gate: Arc<Mutex<GateState>>,
    interval_ms: i64,
) -> Result<GateTicket, RouteFailure> {
    let wait = {
        let mut state = gate.lock();
        let now = js::now_ms();
        if state.queue_depth >= ROUTE_UPSTREAM_QUEUE_MAX {
            return Err(RouteFailure::Busy {
                retry_after_ms: (state.next_upstream_at - now).max(0) + interval_ms,
            });
        }
        state.queue_depth += 1;
        let slot = now.max(state.next_upstream_at);
        state.next_upstream_at = slot + interval_ms;
        slot - now
    };
    let ticket = GateTicket(gate);
    if wait > 0 {
        tokio::time::sleep(Duration::from_millis(wait as u64)).await;
    }
    Ok(ticket)
}

struct FetchRequest {
    http: reqwest::Client,
    gate: Arc<Mutex<GateState>>,
    interval_ms: i64,
    profile: &'static str,
    osrm_profile: &'static str,
    base: String,
    coords: String,
    with_steps: bool,
}

/// `fetchRoute`: one upstream call, projected.
async fn fetch_route(request: FetchRequest) -> RouteOutcome {
    let upstream = format!(
        "{}/route/v1/{}/{}?overview=full&geometries=geojson&alternatives=false&steps={}",
        request.base.strip_suffix('/').unwrap_or(&request.base),
        request.osrm_profile,
        request.coords,
        if request.with_steps { "true" } else { "false" }
    );
    let call = async {
        // The slot is held until the response headers arrive, as in the JS.
        let response = {
            let _ticket = through_upstream_gate(request.gate.clone(), request.interval_ms).await?;
            request
                .http
                .get(&upstream)
                .header("User-Agent", "camera-eye/dev (local)")
                .send()
                .await
                .map_err(|e| RouteFailure::Error(e.to_string()))?
        };
        let status = response.status();
        // `redirect: 'error'`: a redirect is a failed fetch, never followed.
        if status.is_redirection() {
            return Err(RouteFailure::Error("redirect refused".into()));
        }
        if status.as_u16() == 429 {
            let retry_after = response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .map_or(f64::NAN, js::number);
            return Err(RouteFailure::UpstreamRateLimit {
                retry_after_sec: if retry_after.is_finite() && retry_after > 0.0 {
                    retry_after
                } else {
                    5.0
                },
            });
        }
        if !status.is_success() {
            return Ok(None);
        }
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !content_type.contains("json") {
            return Ok(None);
        }
        let text = read_text_capped(response, ROUTE_MAX_RESPONSE_BYTES)
            .await
            .map_err(|e| RouteFailure::Error(e.to_string()))?;
        serde_json::from_str::<Value>(&text)
            .map(Some)
            .map_err(|e| RouteFailure::Error(e.to_string()))
    };
    let osrm = match tokio::time::timeout(ROUTE_UPSTREAM_TIMEOUT, call).await {
        Err(_) => return Err(RouteFailure::Error("This operation was aborted".into())),
        Ok(result) => result?,
    };
    let no_route = Ok((None, Some("no route found".to_string())));
    let Some(osrm) = osrm else {
        return no_route;
    };
    let route = get(Some(&osrm), "routes")
        .and_then(Value::as_array)
        .and_then(|routes| routes.first());
    let coordinates = get(get(route, "geometry"), "coordinates");
    let has_coordinates = match coordinates {
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::String(text)) => !text.is_empty(),
        _ => false,
    };
    if get(Some(&osrm), "code").and_then(Value::as_str) != Some("Ok") || !has_coordinates {
        return no_route;
    }
    let route = route.expect("coordinates imply a route");
    let mut payload = project_route_result(route, request.profile);
    if request.with_steps {
        let (steps, truncated) = normalize_osrm_steps(Some(route));
        payload.insert("steps".into(), Value::Array(steps));
        if truncated {
            payload.insert("stepsTruncated".into(), Value::Bool(true));
        }
    }
    Ok((Some(payload), None))
}

fn fail(message: &str) -> Response {
    reply::json(200, &json!({ "ok": false, "error": message }), None)
}

fn rate_limited(message: &str, retry_after_sec: f64) -> Response {
    let seconds = if retry_after_sec.is_nan() || retry_after_sec == 0.0 {
        5.0
    } else {
        retry_after_sec
    };
    let seconds = round(seconds).max(1.0);
    reply::with_header(
        reply::json(429, &json!({ "ok": false, "error": message }), None),
        "Retry-After",
        &number_to_string(seconds),
    )
}

/// A caller that did not ask for maneuvers never sees them.
fn shape_payload(payload: &Map<String, Value>, want_steps: bool) -> Value {
    let mut out = payload.clone();
    if !want_steps {
        out.remove("steps");
    }
    Value::Object(out)
}

pub async fn handle(svc: &Places, req: Request) -> Response {
    let routes = &svc.routes;
    if !routes.limiter.allow(&client_key(peer(&req))) {
        return rate_limited("rate limited", 5.0);
    }
    let params = Params::parse(req.uri().query().unwrap_or(""));
    let raw = params
        .get("profile")
        .filter(|p| !p.is_empty())
        .unwrap_or("foot")
        .to_lowercase();
    let Some(profile) = normalize_route_profile(&raw) else {
        return fail("invalid profile");
    };
    let osrm_profile = if profile == "car" { "driving" } else { profile };
    let pairs: Vec<&str> = params
        .get("coords")
        .unwrap_or("")
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if pairs.len() < 2 || pairs.len() > 12 {
        return fail("need 2-12 coordinates");
    }
    let mut clean = Vec::new();
    let mut points = Vec::new();
    for pair in pairs {
        let parts: Vec<&str> = pair.split(',').collect();
        if parts.len() != 2 {
            return fail("invalid coordinate");
        }
        let lon = js::number(parts[0]);
        let lat = js::number(parts[1]);
        if !lon.is_finite() || !lat.is_finite() || lat.abs() > 90.0 || lon.abs() > 180.0 {
            return fail("invalid coordinate");
        }
        clean.push(format!(
            "{},{}",
            number_to_string(lon),
            number_to_string(lat)
        ));
        points.push((lon, lat));
    }
    let mut total_km = 0.0;
    for pair in points.windows(2) {
        let leg_km = haversine_km(pair[0].1, pair[0].0, pair[1].1, pair[1].0);
        if leg_km > ROUTE_MAX_LEG_KM {
            return fail("route leg too long");
        }
        total_km += leg_km;
    }
    if total_km > ROUTE_MAX_TOTAL_KM {
        return fail("route too long");
    }
    let coords = clean.join(";");
    let cache_key = format!("{profile}|{coords}");
    let base = routes
        .endpoints
        .get(profile)
        .cloned()
        .unwrap_or_else(|| format!("https://routing.openstreetmap.de/routed-{profile}"));
    let want_steps = params.get("steps") == Some("1");

    {
        let cache = routes.cache.lock();
        if let Some(cached) = cache.entries.get(&cache_key)
            && js::now_ms() - cached.cached_at <= ROUTE_CACHE_MS
            && (!want_steps || cached.has_steps)
        {
            return reply::json(200, &shape_payload(&cached.payload, want_steps), None);
        }
    }

    let inflight_key = format!("{base}|{cache_key}|{}", if want_steps { "s" } else { "n" });
    let pending = {
        let mut inflight = routes.inflight.lock();
        // A stepless request can ride a stepful call already in flight.
        let existing = inflight.get(&inflight_key).cloned().or_else(|| {
            if want_steps {
                None
            } else {
                inflight.get(&format!("{base}|{cache_key}|s")).cloned()
            }
        });
        match existing {
            Some((_, pending)) => pending,
            None => {
                let id = {
                    let mut next = routes.next_inflight_id.lock();
                    *next += 1;
                    *next
                };
                let (tx, rx) = tokio::sync::oneshot::channel::<RouteOutcome>();
                let pending: Pending = async move {
                    rx.await
                        .unwrap_or_else(|_| Err(RouteFailure::Error("route task ended".into())))
                }
                .boxed()
                .shared();
                inflight.insert(inflight_key.clone(), (id, pending.clone()));
                let request = FetchRequest {
                    http: svc.http.clone(),
                    gate: routes.gate.clone(),
                    interval_ms: routes.interval_ms,
                    profile,
                    osrm_profile,
                    base: base.clone(),
                    coords: coords.clone(),
                    with_steps: want_steps,
                };
                let map = routes.inflight.clone();
                let key = inflight_key.clone();
                // Started now, like the JS promise, whoever awaits it.
                tokio::spawn(async move {
                    let outcome = fetch_route(request).await;
                    {
                        let mut map = map.lock();
                        if map.get(&key).is_some_and(|(entry, _)| *entry == id) {
                            map.remove(&key);
                        }
                    }
                    let _ = tx.send(outcome);
                });
                pending
            }
        }
    };

    match pending.await {
        Err(RouteFailure::Busy { retry_after_ms }) => rate_limited(
            "routing busy — too many routes at once",
            (retry_after_ms as f64 / 1000.0).ceil(),
        ),
        Err(RouteFailure::UpstreamRateLimit { retry_after_sec }) => {
            rate_limited("routing service is rate limited", retry_after_sec)
        }
        Err(RouteFailure::Error(message)) => {
            tracing::error!("[Route Proxy] {message}");
            fail("route proxy error")
        }
        Ok((Some(payload), None)) => {
            let has_steps = payload.get("steps").is_some_and(Value::is_array);
            let body = shape_payload(&payload, want_steps);
            routes.cache.lock().set(
                cache_key,
                CacheEntry {
                    payload,
                    has_steps,
                    cached_at: js::now_ms(),
                },
            );
            reply::json(200, &body, None)
        }
        Ok((_, error)) => fail(error.as_deref().unwrap_or("no route found")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::routing::get as get_route;
    use parking_lot::Mutex as PlMutex;
    use serde_json::json;

    use super::super::super::openai::testkit::{Call, call, serve, state, temp_dir};
    use super::super::{Config, router_with};
    use super::*;

    fn osrm_route_with_steps(count: usize) -> Value {
        let steps: Vec<Value> = (0..count)
            .map(|i| {
                json!({
                    "name": format!("Road {i}"),
                    "distance": 10,
                    "duration": 5,
                    "maneuver": {
                        "type": if i == 0 { "depart" } else { "turn" },
                        "modifier": "left",
                        "location": [-97.0 + i as f64 / 1000.0, 30],
                    },
                })
            })
            .collect();
        json!({
            "code": "Ok",
            "routes": [{
                "distance": 123.6,
                "duration": 80.2,
                "geometry": { "coordinates": [[-97, 30], [-97.01, 30.01]] },
                "legs": [{ "steps": steps }],
            }],
        })
    }

    #[derive(Clone, Default)]
    struct Seen {
        urls: Arc<PlMutex<Vec<(i64, String)>>>,
        calls: Arc<AtomicUsize>,
    }

    /// An OSRM stand-in answering every profile with `reply(call_number)`.
    async fn osrm<F>(seen: Seen, reply: F) -> String
    where
        F: Fn(usize) -> Response + Clone + Send + Sync + 'static,
    {
        let app = Router::new().route(
            "/{*path}",
            get_route(move |uri: http::Uri| {
                let seen = seen.clone();
                let reply = reply.clone();
                async move {
                    let n = seen.calls.fetch_add(1, Ordering::SeqCst);
                    seen.urls.lock().push((js::now_ms(), uri.to_string()));
                    reply(n)
                }
            }),
        );
        serve(app).await
    }

    fn json_reply(value: Value) -> Response {
        reply::json(200, &value, None)
    }

    fn app(base: &str, interval_ms: i64) -> Router {
        let state = state(temp_dir("routes"));
        let mut endpoints = HashMap::new();
        for profile in ["foot", "car", "bike"] {
            endpoints.insert(profile.to_string(), format!("{base}/routed-{profile}/"));
        }
        let config = Config {
            route_endpoints: endpoints,
            route_interval_ms: interval_ms,
            ..Config::default()
        };
        router_with(&state, config).with_state(state)
    }

    #[tokio::test]
    async fn aliases_cache_guards_and_failures() {
        let seen = Seen::default();
        let flip = Arc::new(AtomicUsize::new(0));
        let mode = flip.clone();
        let base = osrm(seen.clone(), move |_| match mode.load(Ordering::SeqCst) {
            0 => json_reply(json!({
                "code": "Ok",
                "routes": [{ "distance": 123.6, "duration": 80.2,
                             "geometry": { "coordinates": [[-97, 30], [-97.01, 30.01]] } }],
            })),
            1 => reply::raw(503, "text/plain", "offline", None),
            _ => reply::raw(200, "text/html", "x", None),
        })
        .await;
        let app = app(&base, 1);
        let query = "/api/route?profile=driving&coords=-97,30;-97.01,30.01";
        let result = call(&app, Call::default_at(query)).await;
        assert_eq!(
            result.json(),
            json!({ "ok": true, "profile": "car", "distanceM": 124, "durationS": 80,
                    "geometry": [[-97, 30], [-97.01, 30.01]] })
        );
        assert!(seen.urls.lock()[0].1.contains("/routed-car/route/v1/driving/-97,30;-97.01,30.01?overview=full&geometries=geojson&alternatives=false&steps=false"));
        let alias = call(&app, Call::default_at(&query.replace("driving", "car"))).await;
        assert_eq!(alias.json(), result.json());
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
        for (url, error) in [
            ("/api/route?profile=plane&coords=0,0;1,1", "invalid profile"),
            ("/api/route?coords=0,91;1,1", "invalid coordinate"),
            ("/api/route?coords=0,0;100,0", "route leg too long"),
            ("/api/route?coords=0,0", "need 2-12 coordinates"),
            ("/api/route?coords=0,0,1;1,1", "invalid coordinate"),
        ] {
            let reply = call(&app, Call::default_at(url)).await;
            assert_eq!(reply.status, 200);
            assert_eq!(reply.json()["error"], error, "{url}");
            assert_eq!(reply.json()["ok"], false);
        }
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);

        // Upstream failures read as "no route found" (fresh coordinates
        // bypass the cache entry above).
        flip.store(1, Ordering::SeqCst);
        let failed = call(
            &app,
            Call::default_at("/api/route?profile=car&coords=-97,30;-97.02,30.01"),
        )
        .await;
        assert_eq!(
            failed.json(),
            json!({ "ok": false, "error": "no route found" })
        );
        flip.store(2, Ordering::SeqCst);
        let html = call(
            &app,
            Call::default_at("/api/route?profile=car&coords=-97,30;-97.03,30.01"),
        )
        .await;
        assert_eq!(html.json()["error"], "no route found");
    }

    #[tokio::test]
    async fn steps_are_opt_in() {
        let seen = Seen::default();
        let base = osrm(seen.clone(), |_| json_reply(osrm_route_with_steps(3))).await;
        let app = app(&base, 1);
        let query = "/api/route?profile=car&coords=-97,30;-97.01,30.01";
        let plain = call(&app, Call::default_at(query)).await;
        assert_eq!(
            plain.json(),
            json!({ "ok": true, "profile": "car", "distanceM": 124, "durationS": 80,
                    "geometry": [[-97, 30], [-97.01, 30.01]] })
        );
        assert!(seen.urls.lock()[0].1.ends_with("steps=false"));
        let with_steps = call(&app, Call::default_at(&format!("{query}&steps=1"))).await;
        assert_eq!(seen.calls.load(Ordering::SeqCst), 2);
        assert!(seen.urls.lock()[1].1.ends_with("steps=true"));
        let body = with_steps.json();
        assert_eq!(body["steps"].as_array().unwrap().len(), 3);
        assert_eq!(body["steps"][0]["instruction"], "Head out on Road 0");
        assert_eq!(body["steps"][1]["instruction"], "Turn left onto Road 1");
        let reuse = call(&app, Call::default_at(query)).await;
        assert_eq!(seen.calls.load(Ordering::SeqCst), 2, "served from cache");
        assert_eq!(reuse.body, plain.body);
    }

    #[tokio::test]
    async fn identical_concurrent_requests_make_one_upstream_call() {
        let seen = Seen::default();
        let release = Arc::new(tokio::sync::Notify::new());
        let gate = release.clone();
        let app_router = Router::new().route(
            "/{*path}",
            get_route({
                let seen = seen.clone();
                move || {
                    let seen = seen.clone();
                    let gate = gate.clone();
                    async move {
                        seen.calls.fetch_add(1, Ordering::SeqCst);
                        gate.notified().await;
                        json_reply(osrm_route_with_steps(4))
                    }
                }
            }),
        );
        let base = serve(app_router).await;
        let state = state(temp_dir("routes-concurrent"));
        let mut endpoints = HashMap::new();
        endpoints.insert("car".to_string(), format!("{base}/routed-car"));
        let svc = Arc::new(Places::new(Config {
            route_endpoints: endpoints,
            route_interval_ms: 1,
            ..Config::default()
        }));
        let request = |uri: &str| {
            let svc = svc.clone();
            let req = http::Request::builder()
                .uri(uri)
                .body(axum::body::Body::empty())
                .unwrap();
            tokio::spawn(async move { handle(&svc, req).await })
        };
        let stepful = "/api/route?profile=car&coords=-97,30;-97.01,30.01&steps=1";
        let all = vec![
            request(stepful),
            request(stepful),
            request(stepful),
            request("/api/route?profile=car&coords=-97,30;-97.01,30.01"),
        ];
        for _ in 0..50 {
            if seen.calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(svc.routes.inflight_count(), 1);
        release.notify_waiters();
        let mut bodies = Vec::new();
        for handle in all {
            let response = handle.await.unwrap();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            bodies.push(serde_json::from_slice::<Value>(&bytes).unwrap());
        }
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
        assert_eq!(bodies[0]["steps"].as_array().unwrap().len(), 4);
        assert_eq!(bodies[0], bodies[1]);
        assert_eq!(bodies[0], bodies[2]);
        assert!(bodies[3].get("steps").is_none());
        assert_eq!(svc.routes.inflight_count(), 0);
        drop(state);
    }

    #[tokio::test]
    async fn outbound_requests_are_spaced_and_the_queue_is_bounded() {
        const { assert!(ROUTE_UPSTREAM_MIN_INTERVAL_MS >= 1000) };
        let seen = Seen::default();
        let base = osrm(seen.clone(), |_| json_reply(osrm_route_with_steps(2))).await;
        let app = app(&base, ROUTE_UPSTREAM_MIN_INTERVAL_MS);
        let first = call(
            &app,
            Call::default_at("/api/route?profile=car&coords=-97,30;-97.01,30.01"),
        )
        .await;
        assert_eq!(first.json()["ok"], true);
        let second = call(
            &app,
            Call::default_at("/api/route?profile=foot&coords=-97,30;-97.01,30.01"),
        )
        .await;
        assert_eq!(second.json()["ok"], true);
        let urls = seen.urls.lock().clone();
        assert!(
            urls[1].0 - urls[0].0 >= ROUTE_UPSTREAM_MIN_INTERVAL_MS - 20,
            "two outbound calls {} ms apart",
            urls[1].0 - urls[0].0
        );

        // Past the queue depth the answer is an honest 429.
        let app = app_fast(&base);
        let mut pending = Vec::new();
        for i in 0..ROUTE_UPSTREAM_QUEUE_MAX + 3 {
            let uri = format!(
                "/api/route?profile=car&coords=-97,{};-97.01,30.01",
                30.0 + i as f64 / 100.0
            );
            let app = app.clone();
            pending.push(tokio::spawn(async move {
                call(&app, Call::default_at(&uri)).await
            }));
        }
        let mut results = Vec::new();
        for handle in pending {
            results.push(handle.await.unwrap());
        }
        let refused: Vec<_> = results.iter().filter(|r| r.status == 429).collect();
        assert!(!refused.is_empty());
        assert!(
            refused[0].json()["error"]
                .as_str()
                .unwrap()
                .contains("routing busy")
        );
        assert!(
            refused[0]
                .header("retry-after")
                .unwrap()
                .parse::<u32>()
                .unwrap()
                >= 1
        );
        assert!(results.iter().any(|r| r.json()["ok"] == true));
    }

    fn app_fast(base: &str) -> Router {
        app(base, 50)
    }

    #[tokio::test]
    async fn an_upstream_rate_limit_is_reported_as_one() {
        let base = osrm(Seen::default(), |_| {
            reply::with_header(
                reply::raw(429, "text/plain", "slow down", None),
                "retry-after",
                "30",
            )
        })
        .await;
        let app = app(&base, 1);
        let result = call(
            &app,
            Call::default_at("/api/route?profile=car&coords=-97,30;-97.01,30.01"),
        )
        .await;
        assert_eq!(result.status, 429);
        assert!(
            result.json()["error"]
                .as_str()
                .unwrap()
                .contains("routing service is rate limited")
        );
        assert_eq!(result.header("retry-after"), Some("30"));
    }

    #[tokio::test]
    async fn a_redirect_is_refused_not_followed() {
        let seen = Seen::default();
        let base = osrm(seen.clone(), |_| {
            reply::with_header(
                reply::raw(302, "text/plain", "", None),
                "location",
                "http://example.com/",
            )
        })
        .await;
        let app = app(&base, 1);
        let result = call(
            &app,
            Call::default_at("/api/route?profile=car&coords=-97,30;-97.01,30.01"),
        )
        .await;
        assert_eq!(
            result.json(),
            json!({ "ok": false, "error": "route proxy error" })
        );
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_pathological_maneuver_count_is_bounded() {
        let base = osrm(Seen::default(), |_| json_reply(osrm_route_with_steps(5000))).await;
        let app = app(&base, 1);
        let result = call(
            &app,
            Call::default_at("/api/route?profile=car&coords=-97,30;-97.01,30.01&steps=1"),
        )
        .await;
        let body = result.json();
        assert_eq!(
            body["steps"].as_array().unwrap().len(),
            super::super::route_steps::ROUTE_STEPS_MAX
        );
        assert_eq!(body["stepsTruncated"], true);
    }
}
