//! The regional cockpit routes (port of `server/providers/regional.js` and
//! `server/providers/regional/*`):
//!
//! - `GET /api/regional-brief?latitude&longitude` (`regionalBriefProxy`)
//! - `GET /api/geocode?q[&bounds]` (`geocodeProxy`, last-resort Nominatim)
//! - `GET /api/weather-effects?latitude&longitude` (`weatherEffectsProxy`)

pub mod model;
pub mod natural_earth;
pub mod news;
pub mod nominatim;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::{Value, json};

use crate::common::http::{UpstreamError, read_bytes_capped};
use crate::common::query::Params;
use crate::common::rate_limit::RateLimiter;
use crate::common::{js, reply};
use crate::providers::mapdata_support::{
    InFlight, InsertionMap, js_json_number, js_round, js_trim, now_iso, peer_key, to_fixed,
    utf16_len,
};
use crate::state::AppState;

use natural_earth::NaturalEarth;

/// Default cap on a regional upstream body (2 MB).
pub const REGIONAL_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const REGIONAL_DEFAULT_TIMEOUT_MS: u64 = 9000;
const WEATHER_MAX_RESPONSE_BYTES: usize = 512 * 1024;

const REGIONAL_BRIEF_CACHE_MS: f64 = 5.0 * 60_000.0;
const REGIONAL_BRIEF_STALE_MS: f64 = 60.0 * 60_000.0;
const REGIONAL_BRIEF_MAX_CACHE: usize = 120;

const WEATHER_EFFECTS_CACHE_MS: f64 = 5.0 * 60_000.0;
const WEATHER_EFFECTS_STALE_MS: f64 = 30.0 * 60_000.0;
const WEATHER_EFFECTS_MAX_CACHE: usize = 180;

const NOMINATIM_ENDPOINT: &str = "https://nominatim.openstreetmap.org/search";
const NOMINATIM_USER_AGENT: &str = "camera-eye/0.1 (+https://github.com/AhmadRun/camera-eye)";
const NOMINATIM_REFERER: &str = "https://github.com/AhmadRun/camera-eye";
/// Minimum spacing between upstream Nominatim calls (policy: 1/s).
const NOMINATIM_MIN_SPACING_MS: i64 = 1100;
/// How many searches may wait for their turn.
pub const NOMINATIM_MAX_PENDING: usize = 4;
/// How long a queued search may wait before it is not worth sending.
const NOMINATIM_MAX_WAIT_MS: i64 = 10_000;
const NOMINATIM_SEARCH_CACHE_MS: f64 = 5.0 * 60_000.0;
const NOMINATIM_SEARCH_MAX_CACHE: usize = 80;
const NOMINATIM_SEARCH_MAX_QUERY: usize = 200;

type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;

fn system_clock() -> Clock {
    Arc::new(|| js::now_ms() as f64)
}

// ---------------------------------------------------------------------------
// Upstream reads (`fetchRegionalJson` / `fetchRegionalText`)
// ---------------------------------------------------------------------------

/// One bounded GET; a seam so tests never touch the network.
pub trait RegionalFetch: Send + Sync {
    /// GET `url`; non-2xx is an error, the deadline covers the body, and a
    /// redirect is followed only when `follow` is set (`redirect: 'error'`
    /// otherwise).
    fn get(
        &self,
        url: String,
        headers: Vec<(String, String)>,
        timeout_ms: u64,
        max_bytes: usize,
        follow: bool,
    ) -> BoxFuture<'static, Result<Bytes, UpstreamError>>;
}

struct ReqwestRegional {
    follow: reqwest::Client,
    no_redirect: reqwest::Client,
}

impl RegionalFetch for ReqwestRegional {
    fn get(
        &self,
        url: String,
        headers: Vec<(String, String)>,
        timeout_ms: u64,
        max_bytes: usize,
        follow: bool,
    ) -> BoxFuture<'static, Result<Bytes, UpstreamError>> {
        let client = if follow {
            self.follow.clone()
        } else {
            self.no_redirect.clone()
        };
        Box::pin(async move {
            let exchange = async {
                let mut request = client.get(&url);
                for (name, value) in &headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                let status = response.status().as_u16();
                if !follow && matches!(status, 301 | 302 | 303 | 307 | 308) {
                    return Err(UpstreamError::Network("redirect refused".into()));
                }
                if !response.status().is_success() {
                    return Err(UpstreamError::Status(status));
                }
                read_bytes_capped(response, max_bytes).await
            };
            tokio::time::timeout(Duration::from_millis(timeout_ms), exchange)
                .await
                .map_err(|_| UpstreamError::Timeout)?
        })
    }
}

/// `fetchRegionalJson` / `fetchRegionalText` over a [`RegionalFetch`].
#[derive(Clone)]
pub struct RegionalHttp(Arc<dyn RegionalFetch>);

impl RegionalHttp {
    fn production() -> Self {
        let build = |policy: reqwest::redirect::Policy| {
            reqwest::Client::builder()
                .redirect(policy)
                .connect_timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default()
        };
        Self(Arc::new(ReqwestRegional {
            follow: build(reqwest::redirect::Policy::limited(20)),
            no_redirect: build(reqwest::redirect::Policy::none()),
        }))
    }

    async fn bytes(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        timeout_ms: u64,
        max_bytes: usize,
        follow: bool,
    ) -> Result<Bytes, UpstreamError> {
        let headers = headers
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        self.0
            .get(url.to_string(), headers, timeout_ms, max_bytes, follow)
            .await
    }

    pub async fn json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        timeout_ms: u64,
        max_bytes: usize,
        follow: bool,
    ) -> Result<Value, UpstreamError> {
        let bytes = self
            .bytes(url, headers, timeout_ms, max_bytes, follow)
            .await?;
        serde_json::from_slice(&bytes).map_err(|e| UpstreamError::Json(e.to_string()))
    }

    pub async fn text(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        timeout_ms: u64,
        follow: bool,
    ) -> Result<String, UpstreamError> {
        let bytes = self
            .bytes(
                url,
                headers,
                timeout_ms,
                REGIONAL_MAX_RESPONSE_BYTES,
                follow,
            )
            .await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// `validRegionalPoint`.
pub fn valid_regional_point(params: &Params) -> Option<(f64, f64)> {
    let latitude = params.finite("latitude")?;
    let longitude = params.finite("longitude")?;
    if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
        return None;
    }
    Some((latitude, longitude))
}

fn point_key((latitude, longitude): (f64, f64)) -> String {
    format!(
        "{},{}",
        to_fixed(js_round(latitude * 10.0) / 10.0, 1),
        to_fixed(js_round(longitude * 10.0) / 10.0, 1)
    )
}

fn coordinates((latitude, longitude): (f64, f64)) -> Value {
    json!({ "latitude": js_json_number(latitude), "longitude": js_json_number(longitude) })
}

fn with_status(payload: &Value, status: &str) -> Value {
    let mut map = payload.as_object().cloned().unwrap_or_default();
    map.insert("status".into(), json!(status));
    Value::Object(map)
}

fn method_not_allowed() -> Response {
    reply::json(405, &json!({ "error": "Method Not Allowed" }), None)
}

fn rate_limited(retry_after: &str) -> Response {
    reply::with_header(
        reply::json(429, &json!({ "error": "Rate limit exceeded" }), None),
        "Retry-After",
        retry_after,
    )
}

fn bad_point() -> Response {
    reply::json(
        400,
        &json!({ "error": "Valid latitude and longitude are required" }),
        None,
    )
}

/// `fetchRegionalWeather`: Open-Meteo current conditions, or `None`.
pub async fn fetch_regional_weather(http: &RegionalHttp, (lat, lon): (f64, f64)) -> Option<Value> {
    let params = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("latitude", &to_fixed(lat, 5))
        .append_pair("longitude", &to_fixed(lon, 5))
        .append_pair(
            "current",
            "temperature_2m,apparent_temperature,precipitation,weather_code,cloud_cover,wind_speed_10m,wind_direction_10m,visibility",
        )
        .append_pair("timezone", "UTC")
        .finish();
    let payload = http
        .json(
            &format!("https://api.open-meteo.com/v1/forecast?{params}"),
            &[],
            REGIONAL_DEFAULT_TIMEOUT_MS,
            WEATHER_MAX_RESPONSE_BYTES,
            true,
        )
        .await
        .ok()?;
    model::normalize_regional_weather(&payload)
}

/// A cached payload: `{payload, cachedAt}`.
#[derive(Clone)]
struct Cached {
    payload: Value,
    cached_at: f64,
}

/// What a point route's cache, in-flight map and limiter look like.
struct PointRoute {
    cache: Mutex<InsertionMap<Cached>>,
    in_flight: InFlight<String, Result<Value, ()>>,
    limiter: RateLimiter,
    max_cache: usize,
    now: Clock,
}

impl PointRoute {
    fn new(max: usize, global: usize, max_cache: usize, now: Clock) -> Self {
        Self {
            cache: Mutex::new(InsertionMap::default()),
            in_flight: InFlight::default(),
            limiter: RateLimiter::new(60_000, max, Some(global)),
            max_cache,
            now,
        }
    }

    fn remember(&self, key: &str, payload: Value) {
        let mut cache = self.cache.lock();
        cache.set(
            key,
            Cached {
                payload,
                cached_at: (self.now)(),
            },
        );
        cache.trim(self.max_cache);
    }
}

/// The shared request flow of `/api/regional-brief` and
/// `/api/weather-effects`: limiter, point, fresh cache, coalesced refresh,
/// serve-stale.
#[allow(clippy::too_many_arguments)]
async fn point_route<F, Fut>(
    route: &Arc<PointRoute>,
    request: Request,
    header: &str,
    fresh_ms: f64,
    stale_ms: f64,
    unavailable: &str,
    refresh: F,
) -> Response
where
    F: FnOnce((f64, f64), String) -> Fut,
    Fut: std::future::Future<Output = Result<Value, ()>> + Send + 'static,
{
    let (parts, _body) = request.into_parts();
    if parts.method != http::Method::GET {
        return method_not_allowed();
    }
    if !route.limiter.allow(&peer_key(&parts.extensions)) {
        return rate_limited("10");
    }
    let params = Params::parse(parts.uri.query().unwrap_or(""));
    let Some(point) = valid_regional_point(&params) else {
        return bad_point();
    };
    let key = point_key(point);
    let now = (route.now)();
    let cached = route.cache.lock().get(&key).cloned();
    if let Some(cached) = cached.as_ref().filter(|c| now - c.cached_at <= fresh_ms) {
        let body = with_status(&cached.payload, "cached");
        return reply::with_header(
            reply::json(200, &body, Some("public, max-age=60")),
            header,
            "HIT",
        );
    }
    let (task, shared) = route
        .in_flight
        .coalesce(key.clone(), move || refresh(point, key));
    match task.await {
        Ok(payload) => reply::with_header(
            reply::json(200, &payload, Some("public, max-age=60")),
            header,
            if shared { "INFLIGHT" } else { "MISS" },
        ),
        Err(()) => {
            if let Some(cached) = cached.filter(|c| now - c.cached_at <= stale_ms) {
                let body = with_status(&cached.payload, "stale");
                return reply::with_header(
                    reply::json(200, &body, Some("no-store")),
                    header,
                    "STALE",
                );
            }
            reply::json(503, &json!({ "error": unavailable }), Some("no-store"))
        }
    }
}

/// `regionalBriefHasAnySource`.
pub fn regional_brief_has_any_source(
    place: Option<&Value>,
    weather: Option<&Value>,
    news_status: &str,
) -> bool {
    place.is_some() || weather.is_some() || news_status != "unavailable"
}

// ---------------------------------------------------------------------------
// Nominatim forward search
// ---------------------------------------------------------------------------

/// Why a forward search produced no answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchError {
    /// `NOMINATIM_QUEUE_FULL`.
    QueueFull,
    /// `NOMINATIM_ABANDONED`.
    Abandoned,
    Upstream,
}

/// The one-request-per-second budget for the whole application.
pub struct Pacer {
    turn: tokio::sync::Mutex<()>,
    last_request_at: Mutex<i64>,
    pending: AtomicUsize,
    spacing_ms: i64,
    max_wait_ms: i64,
}

impl Pacer {
    pub fn new(spacing_ms: i64, max_wait_ms: i64) -> Self {
        Self {
            turn: tokio::sync::Mutex::new(()),
            last_request_at: Mutex::new(0),
            pending: AtomicUsize::new(0),
            spacing_ms,
            max_wait_ms,
        }
    }

    fn global() -> Arc<Pacer> {
        static PACER: Lazy<Arc<Pacer>> =
            Lazy::new(|| Arc::new(Pacer::new(NOMINATIM_MIN_SPACING_MS, NOMINATIM_MAX_WAIT_MS)));
        Arc::clone(&PACER)
    }

    /// `enqueueNominatim(work, {bounded: true, signal})`.
    async fn run_bounded<T, Fut>(
        &self,
        abandoned: &AtomicBool,
        work: impl FnOnce() -> Fut,
    ) -> Result<T, SearchError>
    where
        Fut: std::future::Future<Output = T>,
    {
        struct Pending<'a>(&'a AtomicUsize);
        impl Drop for Pending<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        if self
            .pending
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < NOMINATIM_MAX_PENDING).then_some(n + 1)
            })
            .is_err()
        {
            return Err(SearchError::QueueFull);
        }
        let _pending = Pending(&self.pending);
        let queued_at = js::now_ms();
        let _turn = self.turn.lock().await;
        let wait = self.spacing_ms - (js::now_ms() - *self.last_request_at.lock());
        if wait > 0 {
            tokio::time::sleep(Duration::from_millis(wait as u64)).await;
        }
        if js::now_ms() - queued_at > self.max_wait_ms || abandoned.load(Ordering::SeqCst) {
            return Err(SearchError::Abandoned);
        }
        *self.last_request_at.lock() = js::now_ms();
        Ok(work().await)
    }
}

/// `createNominatimSearchProvider`: cached, coalesced, paced forward search.
pub struct NominatimSearch {
    endpoint: String,
    http: RegionalHttp,
    pacer: Arc<Pacer>,
    cache: Mutex<InsertionMap<Cached>>,
    in_flight: InFlight<String, Result<Value, SearchError>>,
}

impl NominatimSearch {
    pub fn new(endpoint: &str, http: RegionalHttp, pacer: Arc<Pacer>) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            http,
            pacer,
            cache: Mutex::new(InsertionMap::default()),
            in_flight: InFlight::default(),
        }
    }

    /// `fetchNominatimSearch(query, bounds, {signal})`: the payload and
    /// whether it came from the cache.
    pub async fn search(
        self: &Arc<Self>,
        query: &str,
        bounds: Option<&str>,
        abandoned: Arc<AtomicBool>,
    ) -> Result<(Value, bool), SearchError> {
        let cache_key = format!("{}|{}", query.to_lowercase(), bounds.unwrap_or(""));
        if let Some(cached) = self.cache.lock().get(&cache_key)
            && js::now_ms() as f64 - cached.cached_at <= NOMINATIM_SEARCH_CACHE_MS
        {
            return Ok((cached.payload.clone(), true));
        }
        let this = Arc::clone(self);
        let query = query.to_string();
        let viewbox = nominatim::nominatim_viewbox_from_bounds(bounds);
        let key = cache_key.clone();
        let (task, _) = self.in_flight.coalesce(cache_key, move || async move {
            let query_string = {
                let mut params = url::form_urlencoded::Serializer::new(String::new());
                params
                    .append_pair("format", "jsonv2")
                    .append_pair("q", &query)
                    .append_pair("addressdetails", "1")
                    .append_pair("limit", "1")
                    .append_pair("accept-language", "en");
                if let Some(viewbox) = &viewbox {
                    params.append_pair("viewbox", viewbox);
                }
                params.finish()
            };
            let url = format!("{}?{}", this.endpoint, query_string);
            let rows = this
                .pacer
                .run_bounded(&abandoned, || {
                    this.http.json(
                        &url,
                        &[
                            ("User-Agent", NOMINATIM_USER_AGENT),
                            ("Referer", NOMINATIM_REFERER),
                        ],
                        REGIONAL_DEFAULT_TIMEOUT_MS,
                        REGIONAL_MAX_RESPONSE_BYTES,
                        false,
                    )
                })
                .await?
                .map_err(|_| SearchError::Upstream)?;
            let first = rows.as_array().and_then(|rows| rows.first());
            let payload = match nominatim::nominatim_to_geocode_result(first) {
                Some(result) => json!({ "status": "OK", "results": [result] }),
                None => json!({ "status": "ZERO_RESULTS", "results": [] }),
            };
            let mut cache = this.cache.lock();
            cache.set(
                &key,
                Cached {
                    payload: payload.clone(),
                    cached_at: js::now_ms() as f64,
                },
            );
            cache.trim(NOMINATIM_SEARCH_MAX_CACHE);
            Ok(payload)
        });
        task.await.map(|payload| (payload, false))
    }
}

/// Marks a search abandoned when the request is dropped before it answers
/// (the browser gave up).
struct AbandonOnDrop(Arc<AtomicBool>, bool);

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        if !self.1 {
            self.0.store(true, Ordering::SeqCst);
        }
    }
}

async fn geocode(
    search: &Arc<NominatimSearch>,
    limiter: &RateLimiter,
    request: Request,
) -> Response {
    let (parts, _body) = request.into_parts();
    if parts.method != http::Method::GET {
        return method_not_allowed();
    }
    if !limiter.allow(&peer_key(&parts.extensions)) {
        return rate_limited("10");
    }
    let params = Params::parse(parts.uri.query().unwrap_or(""));
    let query = js_trim(params.get("q").unwrap_or("")).to_string();
    if query.is_empty() || utf16_len(&query) > NOMINATIM_SEARCH_MAX_QUERY {
        return reply::json(
            400,
            &json!({ "error": "A place query of 1-200 characters is required" }),
            None,
        );
    }
    let abandoned = Arc::new(AtomicBool::new(false));
    let mut guard = AbandonOnDrop(Arc::clone(&abandoned), false);
    let result = search.search(&query, params.get("bounds"), abandoned).await;
    guard.1 = true;
    match result {
        Ok((payload, cached)) => reply::json(
            200,
            &json!({ "status": payload["status"], "results": payload["results"] }),
            Some(if cached {
                "public, max-age=60"
            } else {
                "no-store"
            }),
        ),
        Err(error) => {
            let busy = matches!(error, SearchError::QueueFull | SearchError::Abandoned);
            let response = reply::json(
                if busy { 429 } else { 503 },
                &json!({
                    "error": if busy {
                        "Place search is busy"
                    } else {
                        "Place search is temporarily unavailable"
                    }
                }),
                Some("no-store"),
            );
            if busy {
                reply::with_header(response, "Retry-After", "5")
            } else {
                response
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Routers
// ---------------------------------------------------------------------------

struct Regional {
    http: RegionalHttp,
    natural_earth: Arc<NaturalEarth>,
    brief: Arc<PointRoute>,
    weather: Arc<PointRoute>,
    search: Arc<NominatimSearch>,
    geocode_limiter: RateLimiter,
}

impl Regional {
    fn new(http: RegionalHttp, natural_earth: NaturalEarth, pacer: Arc<Pacer>, now: Clock) -> Self {
        Self {
            search: Arc::new(NominatimSearch::new(
                NOMINATIM_ENDPOINT,
                http.clone(),
                pacer,
            )),
            http,
            natural_earth: Arc::new(natural_earth),
            brief: Arc::new(PointRoute::new(
                30,
                90,
                REGIONAL_BRIEF_MAX_CACHE,
                Arc::clone(&now),
            )),
            weather: Arc::new(PointRoute::new(45, 120, WEATHER_EFFECTS_MAX_CACHE, now)),
            geocode_limiter: RateLimiter::new(60_000, 30, Some(90)),
        }
    }

    async fn regional_brief(self: Arc<Self>, request: Request) -> Response {
        let this = Arc::clone(&self);
        point_route(
            &self.brief,
            request,
            "X-Regional-Brief",
            REGIONAL_BRIEF_CACHE_MS,
            REGIONAL_BRIEF_STALE_MS,
            "Regional briefing is temporarily unavailable",
            move |point, key| async move {
                let (place, weather) = tokio::join!(
                    this.natural_earth.region_at_point(point.0, point.1),
                    fetch_regional_weather(&this.http, point)
                );
                let place = place.ok().flatten();
                let news = news::fetch_regional_news(&this.http, place.as_ref()).await;
                if !regional_brief_has_any_source(place.as_ref(), weather.as_ref(), news.status) {
                    return Err(());
                }
                let status = if place.is_some() && weather.is_some() && news.status != "unavailable"
                {
                    "ready"
                } else {
                    "partial"
                };
                let payload = json!({
                    "status": status,
                    "retrievedAt": now_iso(),
                    "coordinates": coordinates(point),
                    "place": place,
                    "placeStatus": if place.is_some() { "ready" } else { "unavailable" },
                    "weather": weather,
                    "weatherStatus": if weather.is_some() { "ready" } else { "unavailable" },
                    "newsStatus": news.status,
                    "newsQuery": news.query,
                    "newsSource": news.source,
                    "articles": news.articles,
                });
                this.brief.remember(&key, payload.clone());
                Ok(payload)
            },
        )
        .await
    }

    async fn weather_effects(self: Arc<Self>, request: Request) -> Response {
        let this = Arc::clone(&self);
        point_route(
            &self.weather,
            request,
            "X-Weather-Effects",
            WEATHER_EFFECTS_CACHE_MS,
            WEATHER_EFFECTS_STALE_MS,
            "Weather effects are temporarily unavailable",
            move |point, key| async move {
                let weather = fetch_regional_weather(&this.http, point).await.ok_or(())?;
                let payload = json!({
                    "status": "ready",
                    "retrievedAt": now_iso(),
                    "coordinates": coordinates(point),
                    "weather": weather,
                });
                this.weather.remember(&key, payload.clone());
                Ok(payload)
            },
        )
        .await
    }
}

fn routes(regional: Arc<Regional>) -> Router<AppState> {
    let brief = {
        let regional = Arc::clone(&regional);
        move |request: Request| Arc::clone(&regional).regional_brief(request)
    };
    let weather = {
        let regional = Arc::clone(&regional);
        move |request: Request| Arc::clone(&regional).weather_effects(request)
    };
    let geocode_handler = {
        let regional = Arc::clone(&regional);
        move |request: Request| {
            let regional = Arc::clone(&regional);
            async move { geocode(&regional.search, &regional.geocode_limiter, request).await }
        }
    };
    Router::new()
        .route("/api/regional-brief", any(brief.clone()))
        .route("/api/regional-brief/{*rest}", any(brief))
        .route("/api/geocode", any(geocode_handler.clone()))
        .route("/api/geocode/{*rest}", any(geocode_handler))
        .route("/api/weather-effects", any(weather.clone()))
        .route("/api/weather-effects/{*rest}", any(weather))
}

/// `/api/regional-brief`, `/api/geocode` and `/api/weather-effects`.
pub fn router(state: &AppState) -> Router<AppState> {
    let regional = Arc::new(Regional::new(
        RegionalHttp::production(),
        NaturalEarth::new(state.root.join("src/data/local_data/natural_earth")),
        Pacer::global(),
        system_clock(),
    ));
    routes(regional)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use std::sync::atomic::AtomicU64;
    use tower::ServiceExt;

    type Responder = dyn Fn(&str) -> Result<Bytes, UpstreamError> + Send + Sync;
    type Call = (String, Vec<(String, String)>, bool);

    struct Scripted {
        calls: Mutex<Vec<Call>>,
        delay_ms: u64,
        respond: Box<Responder>,
    }

    impl RegionalFetch for Scripted {
        fn get(
            &self,
            url: String,
            headers: Vec<(String, String)>,
            _timeout_ms: u64,
            _max_bytes: usize,
            follow: bool,
        ) -> BoxFuture<'static, Result<Bytes, UpstreamError>> {
            self.calls.lock().push((url.clone(), headers, follow));
            let result = (self.respond)(&url);
            let delay = self.delay_ms;
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                result
            })
        }
    }

    fn scripted(
        delay_ms: u64,
        respond: impl Fn(&str) -> Result<Bytes, UpstreamError> + Send + Sync + 'static,
    ) -> Arc<Scripted> {
        Arc::new(Scripted {
            calls: Mutex::new(Vec::new()),
            delay_ms,
            respond: Box::new(respond),
        })
    }

    fn app(http: Arc<Scripted>, pacer: Arc<Pacer>, now: Clock) -> Router {
        let state = AppState::new(".".into(), "dist".into()).unwrap();
        let regional = Arc::new(Regional::new(
            RegionalHttp(http),
            NaturalEarth::new(
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../src/data/local_data/natural_earth"),
            ),
            pacer,
            now,
        ));
        routes(regional).with_state(state)
    }

    async fn call(app: &Router, method: &str, uri: &str) -> (u16, http::HeaderMap, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
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
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn fast_pacer() -> Arc<Pacer> {
        Arc::new(Pacer::new(50, 10_000))
    }

    #[test]
    fn points_require_both_coordinates() {
        assert_eq!(valid_regional_point(&Params::parse("longitude=12.5")), None);
        assert_eq!(valid_regional_point(&Params::parse("latitude=12.5")), None);
        assert_eq!(
            valid_regional_point(&Params::parse("latitude=&longitude=12.5")),
            None
        );
        assert_eq!(
            valid_regional_point(&Params::parse("latitude=0&longitude=0")),
            Some((0.0, 0.0))
        );
        assert_eq!(point_key((34.61, -112.43)), "34.6,-112.4");
        assert_eq!(point_key((-0.04, 0.05)), "0.0,0.1");
    }

    #[test]
    fn an_all_source_outage_is_total_failure() {
        assert!(!regional_brief_has_any_source(None, None, "unavailable"));
        assert!(regional_brief_has_any_source(
            Some(&json!({ "country": "United States" })),
            None,
            "unavailable"
        ));
    }

    #[tokio::test]
    async fn guards_run_without_upstream() {
        let http = scripted(0, |_| panic!("invalid requests must not fetch"));
        let app = app(http, fast_pacer(), system_clock());
        for route in [
            "/api/regional-brief",
            "/api/weather-effects",
            "/api/geocode",
        ] {
            assert_eq!(call(&app, "DELETE", route).await.0, 405, "{route}");
            assert_eq!(call(&app, "GET", route).await.0, 400, "{route}");
        }
        for uri in [
            "/api/geocode?q=",
            "/api/geocode?q=%20%20",
            &format!("/api/geocode?q={}", "x".repeat(201)),
        ] {
            assert_eq!(call(&app, "GET", uri).await.0, 400, "{uri}");
        }
    }

    #[tokio::test]
    async fn weather_effects_share_work_and_keep_fresh_and_stale() {
        let now = Arc::new(AtomicU64::new((js::now_ms() as f64).to_bits()));
        let reader = Arc::clone(&now);
        let clock: Clock = Arc::new(move || f64::from_bits(reader.load(Ordering::SeqCst)));
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let http = scripted(10, move |url| {
            assert_eq!(
                url::Url::parse(url).unwrap().host_str(),
                Some("api.open-meteo.com")
            );
            if counter.fetch_add(1, Ordering::SeqCst) > 0 {
                return Err(UpstreamError::Network("offline".into()));
            }
            Ok(Bytes::from(
                json!({ "current": { "time": "2026-09-12T12:00", "temperature_2m": 20, "weather_code": 0, "wind_speed_10m": 10 } })
                    .to_string(),
            ))
        });
        let app = app(http, fast_pacer(), clock);
        let uri = "/api/weather-effects?latitude=34.61&longitude=-112.43";
        let (a, b) = tokio::join!(call(&app, "GET", uri), call(&app, "GET", uri));
        let mut markers = vec![
            a.1["x-weather-effects"].to_str().unwrap().to_string(),
            b.1["x-weather-effects"].to_str().unwrap().to_string(),
        ];
        markers.sort();
        assert_eq!(markers, ["INFLIGHT", "MISS"]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(a.2["weather"]["observedAt"], "2026-09-12T12:00:00.000Z");
        assert_eq!(
            a.2["coordinates"],
            json!({ "latitude": 34.61, "longitude": -112.43 })
        );
        let hit = call(&app, "GET", uri).await;
        assert_eq!(hit.1["x-weather-effects"], "HIT");
        assert_eq!(hit.2["status"], "cached");
        let later = f64::from_bits(now.load(Ordering::SeqCst)) + 6.0 * 60_000.0;
        now.store(later.to_bits(), Ordering::SeqCst);
        let stale = call(&app, "GET", uri).await;
        assert_eq!(stale.1["x-weather-effects"], "STALE");
        assert_eq!(stale.1["cache-control"], "no-store");
    }

    #[tokio::test]
    async fn regional_brief_uses_natural_earth_and_never_nominatim() {
        let http = scripted(0, |url| {
            assert!(!url.contains("nominatim") && !url.contains("overpass"));
            Err(UpstreamError::Status(503))
        });
        let app = app(http.clone(), fast_pacer(), system_clock());
        let (status, headers, body) = call(
            &app,
            "GET",
            "/api/regional-brief?latitude=46.5&longitude=10",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(headers["x-regional-brief"], "MISS");
        assert_eq!(body["place"]["source"], "Natural Earth");
        assert_eq!(body["status"], "partial");
        assert_eq!(body["weatherStatus"], "unavailable");
        assert_eq!(body["newsStatus"], "unavailable");
        let keys: Vec<&str> = body
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "status",
                "retrievedAt",
                "coordinates",
                "place",
                "placeStatus",
                "weather",
                "weatherStatus",
                "newsStatus",
                "newsQuery",
                "newsSource",
                "articles"
            ]
        );
        // Open sea far from any named region with every upstream down.
        let (status, _, body) = call(
            &app,
            "GET",
            "/api/regional-brief?latitude=-60&longitude=-140",
        )
        .await;
        if status == 503 {
            assert_eq!(
                body["error"],
                "Regional briefing is temporarily unavailable"
            );
        }
    }

    const HIT: &str = r#"[{"lat":"43.1731","lon":"-79.0384","display_name":"Niagara Falls, Ontario, Canada","name":"Niagara Falls","addresstype":"city","boundingbox":["43.05","43.15","-79.15","-79.0"]}]"#;

    #[tokio::test]
    async fn search_is_identified_cached_and_coalesced() {
        let http = scripted(40, |_| Ok(Bytes::from_static(HIT.as_bytes())));
        let app = app(http.clone(), fast_pacer(), system_clock());
        let first = call(&app, "GET", "/api/geocode?q=niagara").await;
        assert_eq!(first.0, 200);
        assert_eq!(first.1["cache-control"], "no-store");
        assert_eq!(first.2["status"], "OK");
        assert_eq!(
            first.2["results"][0]["geometry"]["location"],
            json!({ "lat": 43.1731, "lng": -79.0384 })
        );
        {
            let calls = http.calls.lock();
            assert_eq!(calls.len(), 1);
            let (url, headers, follow) = &calls[0];
            assert!(url.starts_with(
                "https://nominatim.openstreetmap.org/search?format=jsonv2&q=niagara&addressdetails=1&limit=1&accept-language=en"
            ));
            assert!(
                headers
                    .iter()
                    .any(|(k, v)| k == "User-Agent" && v.starts_with("camera-eye/0"))
            );
            assert!(headers.iter().any(|(k, _)| k == "Referer"));
            assert!(
                !follow,
                "a fixed API endpoint may not be redirected elsewhere"
            );
        }
        let repeat = call(&app, "GET", "/api/geocode?q=niagara").await;
        assert_eq!(repeat.0, 200);
        assert_eq!(repeat.1["cache-control"], "public, max-age=60");
        assert_eq!(http.calls.lock().len(), 1);

        let uri = "/api/geocode?q=coalesce";
        let answers = tokio::join!(
            call(&app, "GET", uri),
            call(&app, "GET", uri),
            call(&app, "GET", uri)
        );
        for answer in [answers.0, answers.1, answers.2] {
            assert_eq!(answer.0, 200);
        }
        assert_eq!(http.calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn bursts_past_the_queue_are_refused_and_failures_are_answers() {
        let http = scripted(0, |_| Ok(Bytes::from_static(b"[]")));
        let app = app(http.clone(), fast_pacer(), system_clock());
        let requests: Vec<_> = (0..20)
            .map(|i| {
                let app = app.clone();
                tokio::spawn(async move {
                    call(&app, "GET", &format!("/api/geocode?q=burst-{i}")).await
                })
            })
            .collect();
        let mut refused = 0;
        for request in requests {
            let (status, headers, body) = request.await.unwrap();
            if status == 429 {
                refused += 1;
                assert_eq!(headers["retry-after"], "5");
                assert_eq!(body["error"], "Place search is busy");
            } else {
                assert_eq!(status, 200);
                assert_eq!(body["status"], "ZERO_RESULTS");
            }
        }
        assert!(refused > 0);
        assert!(http.calls.lock().len() <= NOMINATIM_MAX_PENDING + 1);

        let down = scripted(0, |_| Err(UpstreamError::Status(502)));
        let app = app_with(down);
        let (status, headers, body) = call(&app, "GET", "/api/geocode?q=down").await;
        assert_eq!(status, 503);
        assert_eq!(headers["cache-control"], "no-store");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("temporarily unavailable")
        );
    }

    fn app_with(http: Arc<Scripted>) -> Router {
        app(http, fast_pacer(), system_clock())
    }

    #[tokio::test]
    async fn an_abandoned_search_costs_no_slot() {
        let http = scripted(0, |_| Ok(Bytes::from_static(HIT.as_bytes())));
        let search = Arc::new(NominatimSearch::new(
            NOMINATIM_ENDPOINT,
            RegionalHttp(http.clone()),
            fast_pacer(),
        ));
        let result = search
            .search("cancelled", None, Arc::new(AtomicBool::new(true)))
            .await;
        assert_eq!(result.unwrap_err(), SearchError::Abandoned);
        assert!(http.calls.lock().is_empty());
    }
}
