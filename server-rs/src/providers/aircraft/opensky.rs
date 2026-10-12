//! OpenSky Network proxy (`server/providers/aircraft/opensky.js`):
//! `GET /api/flights` with multi-mode auth, an adaptive-TTL response cache,
//! a 429 cooldown, serve-stale, and a 250 nm adsb.lol regional fallback.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use serde_json::Value;

use super::jsval::{self, encode_uri_component};
use super::records::normalize_adsb_lol_point_response;
use crate::common::coalesce::Coalescer;
use crate::common::http::read_json_capped;
use crate::common::js::{self, now_ms};
use crate::common::query::Params;
use crate::common::reply;
use crate::state::AppState;

/// TTL for the OpenSky response cache (ms).
const OPENSKY_CACHE_MS: i64 = 9000;
/// Per-attempt limit for the global snapshot.
const OPENSKY_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(10_000);
pub const OPENSKY_STATES_URL: &str = "https://opensky-network.org/api/states/all?extended=1";
pub const OPENSKY_TOKEN_URL: &str =
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";
pub const ADSBLOL_API_BASE: &str = "https://api.adsb.lol";
const OPENSKY_AUTH_MODE_DEFAULT: &str = "oauth";
const ADSBLOL_POINT_CACHE_MS: i64 = 12_000;
const ADSBLOL_POINT_CACHE_MAX: usize = 80;
const ADSBLOL_POINT_RADIUS_NM: u32 = 250;
const ADSBLOL_POINT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const ADSBLOL_POINT_TIMEOUT: Duration = Duration::from_millis(10_000);
/// Past this age an OpenSky snapshot yields to the regional source.
const OPENSKY_SOURCE_STALE_MS: f64 = 120_000.0;

// ---------------------------------------------------------------------------
// OAuth2 token (shared with the track backfill proxy)
// ---------------------------------------------------------------------------

type TokenFuture = Shared<BoxFuture<'static, Option<String>>>;

#[derive(Default)]
struct TokenState {
    token: Option<String>,
    expiry: i64,
    in_flight: Option<TokenFuture>,
    warned: bool,
}

/// `getOpenSkyToken`: a cached client_credentials bearer token with one
/// coalesced refresh at a time.
pub struct TokenManager {
    state: Mutex<TokenState>,
    token_url: String,
}

impl TokenManager {
    pub fn new(token_url: impl Into<String>) -> Self {
        Self {
            state: Mutex::new(TokenState::default()),
            token_url: token_url.into(),
        }
    }

    pub async fn get(self: &Arc<Self>, app: &AppState) -> Option<String> {
        let future = {
            let mut state = self.state.lock();
            if let Some(token) = &state.token
                && now_ms() < state.expiry - 60_000
            {
                return Some(token.clone());
            }
            if let Some(in_flight) = &state.in_flight {
                in_flight.clone()
            } else {
                let client_id = app.env.raw("OPENSKY_CLIENT_ID").filter(|v| !v.is_empty())?;
                let secret = app
                    .env
                    .raw("OPENSKY_CLIENT_SECRET")
                    .filter(|v| !v.is_empty())?;
                let this = Arc::clone(self);
                let http = app.http.clone();
                let future = async move {
                    let token = this.refresh(&http, &client_id, &secret).await;
                    this.state.lock().in_flight = None;
                    token
                }
                .boxed()
                .shared();
                state.in_flight = Some(future.clone());
                future
            }
        };
        future.await
    }

    async fn refresh(&self, http: &reqwest::Client, id: &str, secret: &str) -> Option<String> {
        let body = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}",
            encode_uri_component(id),
            encode_uri_component(secret)
        );
        let result = async {
            let response = http
                .post(&self.token_url)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(OPENSKY_ATTEMPT_TIMEOUT)
                .body(body)
                .send()
                .await?;
            let status = response.status();
            let text = response.bytes().await.unwrap_or_default();
            Ok::<_, reqwest::Error>((status, serde_json::from_slice::<Value>(&text).ok()))
        }
        .await;
        let mut state = self.state.lock();
        match result {
            Ok((status, data)) => {
                let data = data.unwrap_or(Value::Null);
                let access_token = data.get("access_token");
                if !status.is_success() || !jsval::truthy(access_token) {
                    if !state.warned {
                        let detail = [data.get("error_description"), data.get("error")]
                            .into_iter()
                            .find(|v| jsval::truthy(*v))
                            .flatten()
                            .map(jsval::to_js_string)
                            .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
                        tracing::warn!("[OpenSky] OAuth client_credentials failed: {detail}");
                        state.warned = true;
                    }
                    state.token = None;
                    state.expiry = 0;
                    return None;
                }
                let token = access_token.map(jsval::to_js_string).unwrap_or_default();
                let expires_in = jsval::to_number(data.get("expires_in"));
                let seconds = if expires_in.is_finite() {
                    expires_in
                } else {
                    1800.0
                };
                state.token = Some(token.clone());
                state.expiry = now_ms() + (seconds * 1000.0) as i64;
                tracing::info!(
                    "[OpenSky] OAuth token refreshed, expires in {} s",
                    jsval::number_to_string(seconds)
                );
                state.warned = false;
                Some(token)
            }
            Err(error) => {
                if !state.warned {
                    tracing::warn!("[OpenSky] OAuth token request failed: {error}");
                    state.warned = true;
                }
                state.token = None;
                state.expiry = 0;
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Proxy state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Meta {
    requested_mode: String,
    used_mode: String,
    reason: String,
}

struct SnapshotCache {
    body: Option<Bytes>,
    status: u16,
    time: i64,
    meta: Option<Meta>,
    source_epoch_ms: Option<f64>,
    /// Current adaptive TTL (credit governor).
    ttl_ms: i64,
    /// Epoch-ms before which no upstream fetch is attempted.
    cooldown_until: i64,
}

#[derive(Clone)]
struct PointRecord {
    body: Bytes,
    cached_at: i64,
    count: usize,
}

pub struct OpenSky {
    pub tokens: Arc<TokenManager>,
    states_url: String,
    adsblol_base: String,
    cache: Mutex<SnapshotCache>,
    auth_mode_warned: AtomicBool,
    point_cache: Mutex<Vec<(String, PointRecord)>>,
    point_in_flight: Coalescer<String, Result<PointRecord, String>>,
}

impl OpenSky {
    pub fn new(
        tokens: Arc<TokenManager>,
        states_url: impl Into<String>,
        adsblol_base: impl Into<String>,
    ) -> Self {
        Self {
            tokens,
            states_url: states_url.into(),
            adsblol_base: adsblol_base.into(),
            cache: Mutex::new(SnapshotCache {
                body: None,
                status: 0,
                time: 0,
                meta: None,
                source_epoch_ms: None,
                ttl_ms: OPENSKY_CACHE_MS,
                cooldown_until: 0,
            }),
            auth_mode_warned: AtomicBool::new(false),
            point_cache: Mutex::new(Vec::new()),
            point_in_flight: Coalescer::new(),
        }
    }

    /// `normalizeOpenSkyAuthMode`.
    fn auth_mode(&self, value: Option<&str>) -> String {
        let raw = value.unwrap_or("").trim().to_lowercase();
        if raw.is_empty() {
            return OPENSKY_AUTH_MODE_DEFAULT.into();
        }
        if ["basic", "oauth", "auto", "anon"].contains(&raw.as_str()) {
            return raw;
        }
        if !self.auth_mode_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "[OpenSky] Invalid OPENSKY_AUTH_MODE=\"{raw}\", defaulting to \"{OPENSKY_AUTH_MODE_DEFAULT}\""
            );
        }
        OPENSKY_AUTH_MODE_DEFAULT.into()
    }
}

/// `openskyAdaptiveTtlMs`.
pub fn adaptive_ttl_ms(remaining: f64) -> i64 {
    if !remaining.is_finite() || remaining > 2400.0 {
        OPENSKY_CACHE_MS
    } else if remaining > 1200.0 {
        30_000
    } else if remaining > 400.0 {
        90_000
    } else {
        300_000
    }
}

/// `openSkySourceEpochMs`.
fn source_epoch_ms(body: &[u8]) -> Option<f64> {
    let parsed: Value = serde_json::from_slice(body).ok()?;
    let seconds = jsval::to_number(parsed.get("time"));
    (seconds.is_finite() && seconds > 0.0).then_some(seconds * 1000.0)
}

/// `openSkySourceIsStale`.
fn source_is_stale(source_epoch_ms: Option<f64>, now: i64) -> bool {
    source_epoch_ms.is_some_and(|epoch| now as f64 - epoch > OPENSKY_SOURCE_STALE_MS)
}

/// `adsbLolFallbackAnchor`: the `lat`/`lon` view anchor, when valid.
pub fn adsb_lol_fallback_anchor(params: &Params) -> Option<(f64, f64)> {
    let latitude = params
        .finite("lat")
        .filter(|v| (-90.0..=90.0).contains(v))?;
    let longitude = params
        .finite("lon")
        .filter(|v| (-180.0..=180.0).contains(v))?;
    Some((latitude, longitude))
}

struct Headers<'a> {
    cache_status: &'a str,
    requested_mode: &'a str,
    used_mode: &'a str,
    reason: &'a str,
    stale_seconds: Option<f64>,
    retry_after_seconds: Option<f64>,
}

/// `buildOpenSkyHeaders` applied to a response.
fn opensky_response(
    status: u16,
    headers: Headers<'_>,
    body: impl Into<axum::body::Body>,
) -> Response {
    let mut response = reply::raw(status, "application/json", body, Some("no-store"));
    let mut set = |name: &str, value: &str| {
        response = reply::with_header(std::mem::take(&mut response), name, value);
    };
    set("X-OpenSky-Cache", headers.cache_status);
    set("X-OpenSky-Auth", headers.used_mode);
    set("X-OpenSky-Auth-Mode-Requested", headers.requested_mode);
    set("X-OpenSky-Auth-Mode-Used", headers.used_mode);
    set("X-OpenSky-Auth-Reason", headers.reason);
    if let Some(stale) = headers.stale_seconds.filter(|v| v.is_finite()) {
        set(
            "X-OpenSky-Stale-Seconds",
            &jsval::number_to_string(jsval::round(stale)),
        );
    }
    if let Some(retry) = headers.retry_after_seconds.filter(|v| v.is_finite()) {
        set(
            "X-OpenSky-Retry-After-Seconds",
            &jsval::number_to_string(jsval::round(retry)),
        );
    }
    response
}

struct Upstream {
    status: u16,
    ok: bool,
    remaining: String,
    retry_after: String,
    body: Bytes,
}

impl OpenSky {
    /// `fetchOpenSkyStates`: one retry after a timed-out or failed attempt.
    async fn fetch_states(
        &self,
        http: &reqwest::Client,
        authorization: Option<&str>,
    ) -> Result<Upstream, reqwest::Error> {
        let mut attempt = 1;
        loop {
            let result = async {
                let mut request = http
                    .get(&self.states_url)
                    .header("Accept", "application/json")
                    .timeout(OPENSKY_ATTEMPT_TIMEOUT);
                if let Some(auth) = authorization {
                    request = request.header("Authorization", auth);
                }
                let response = request.send().await?;
                let header = |name: &str| {
                    response
                        .headers()
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string()
                };
                let status = response.status();
                let remaining = header("x-rate-limit-remaining");
                let retry_after = header("x-rate-limit-retry-after-seconds");
                let bytes = response.bytes().await?;
                // `upstream.text()` decodes as UTF-8, replacing invalid bytes.
                let body = match String::from_utf8_lossy(&bytes) {
                    std::borrow::Cow::Borrowed(_) => bytes,
                    std::borrow::Cow::Owned(text) => Bytes::from(text),
                };
                Ok(Upstream {
                    status: status.as_u16(),
                    ok: status.is_success(),
                    remaining,
                    retry_after,
                    body,
                })
            }
            .await;
            match result {
                Ok(upstream) => return Ok(upstream),
                Err(error) if attempt >= 2 => return Err(error),
                Err(error) => {
                    tracing::warn!("[OpenSky] global snapshot attempt failed, retrying: {error}");
                    attempt += 1;
                }
            }
        }
    }

    /// `fetchAdsbLolPointFallback`.
    async fn point_fallback(
        &self,
        http: &reqwest::Client,
        params: &Params,
    ) -> Option<(PointRecord, &'static str)> {
        let (latitude, longitude) = adsb_lol_fallback_anchor(params)?;
        let rounded_lat = jsval::round(latitude * 4.0) / 4.0 + 0.0;
        let rounded_lon = jsval::round(longitude * 4.0) / 4.0 + 0.0;
        let cache_key = format!("{rounded_lat:.2},{rounded_lon:.2}");
        let cached = self
            .point_cache
            .lock()
            .iter()
            .find(|(key, _)| key == &cache_key)
            .map(|(_, record)| record.clone());
        let now = now_ms();
        if let Some(record) = &cached
            && now - record.cached_at < ADSBLOL_POINT_CACHE_MS
        {
            return Some((record.clone(), "HIT"));
        }
        let url = format!(
            "{}/v2/lat/{}/lon/{}/dist/{}",
            self.adsblol_base,
            jsval::number_to_string(rounded_lat),
            jsval::number_to_string(rounded_lon),
            ADSBLOL_POINT_RADIUS_NM
        );
        let http = http.clone();
        let (result, shared) = self
            .point_in_flight
            .run(cache_key.clone(), move || async move {
                let response = http
                    .get(&url)
                    .header("Accept", "application/json")
                    .header("User-Agent", "camera-eye-adsblol-regional-fallback/1.0")
                    .timeout(ADSBLOL_POINT_TIMEOUT)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!("upstream HTTP {}", response.status().as_u16()));
                }
                let payload = read_json_capped(response, ADSBLOL_POINT_MAX_RESPONSE_BYTES)
                    .await
                    .map_err(|e| e.to_string())?;
                let normalized = normalize_adsb_lol_point_response(&payload, now_ms());
                let count = normalized["states"].as_array().map_or(0, Vec::len);
                Ok(PointRecord {
                    body: Bytes::from(serde_json::to_vec(&normalized).unwrap_or_default()),
                    cached_at: now_ms(),
                    count,
                })
            })
            .await;
        match result {
            Ok(record) => {
                if !shared {
                    let mut cache = self.point_cache.lock();
                    cache.retain(|(key, _)| key != &cache_key);
                    cache.push((cache_key, record.clone()));
                    while cache.len() > ADSBLOL_POINT_CACHE_MAX {
                        cache.remove(0);
                    }
                }
                Some((record, if shared { "INFLIGHT" } else { "MISS" }))
            }
            Err(error) => {
                if !shared {
                    tracing::warn!("[adsb.lol Flights Fallback] {error}");
                }
                cached.map(|record| (record, "STALE"))
            }
        }
    }

    /// `serveAdsbLolPointFallback`: the response, or `None` without an anchor
    /// or any data.
    async fn serve_point_fallback(
        &self,
        http: &reqwest::Client,
        params: &Params,
        requested_mode: &str,
        reason: &str,
    ) -> Option<Response> {
        let (record, cache_status) = self.point_fallback(http, params).await?;
        let response = opensky_response(
            200,
            Headers {
                cache_status,
                requested_mode,
                used_mode: "adsblol-regional",
                reason,
                stale_seconds: None,
                retry_after_seconds: None,
            },
            record.body,
        );
        let response = reply::with_header(response, "X-Flight-Source", "adsb.lol");
        let response = reply::with_header(
            response,
            "X-Flight-Coverage",
            &format!("{ADSBLOL_POINT_RADIUS_NM}nm regional fallback"),
        );
        Some(reply::with_header(
            response,
            "X-Flight-Count",
            &record.count.to_string(),
        ))
    }

    /// The `/api/flights` handler body (before `X-Feed-Source` is added).
    async fn handle(&self, app: &AppState, params: &Params) -> Response {
        let http = &app.http;
        let requested_mode = self.auth_mode(app.env.raw("OPENSKY_AUTH_MODE").as_deref());
        let now = now_ms();
        let (has_cache, fresh_or_cooling, in_cooldown, cached_source_epoch) = {
            let cache = self.cache.lock();
            let in_cooldown = now < cache.cooldown_until;
            (
                cache.body.is_some(),
                now - cache.time < cache.ttl_ms || in_cooldown,
                in_cooldown,
                cache.source_epoch_ms,
            )
        };
        if has_cache && fresh_or_cooling {
            if source_is_stale(cached_source_epoch, now)
                && let Some(response) = self
                    .serve_point_fallback(
                        http,
                        params,
                        &requested_mode,
                        "opensky_snapshot_stale_regional_fallback",
                    )
                    .await
            {
                return response;
            }
            let cache = self.cache.lock();
            let meta = cache.meta.clone().unwrap_or(Meta {
                requested_mode: requested_mode.clone(),
                used_mode: "unknown".into(),
                reason: "cached".into(),
            });
            let is_stale = now - cache.time >= cache.ttl_ms;
            let requested = if meta.requested_mode.is_empty() {
                requested_mode.clone()
            } else {
                meta.requested_mode.clone()
            };
            let used = if meta.used_mode.is_empty() {
                "unknown".to_string()
            } else {
                meta.used_mode.clone()
            };
            let reason = if is_stale {
                "rate_limited_serving_stale".to_string()
            } else if meta.reason.is_empty() {
                "cached".to_string()
            } else {
                meta.reason.clone()
            };
            let status = if cache.status == 0 { 200 } else { cache.status };
            return opensky_response(
                status,
                Headers {
                    cache_status: if is_stale { "STALE" } else { "HIT" },
                    requested_mode: &requested,
                    used_mode: &used,
                    reason: &reason,
                    stale_seconds: is_stale.then(|| (now - cache.time) as f64 / 1000.0),
                    retry_after_seconds: in_cooldown
                        .then(|| (cache.cooldown_until - now) as f64 / 1000.0),
                },
                cache.body.clone().unwrap_or_default(),
            );
        }
        if in_cooldown {
            if let Some(response) = self
                .serve_point_fallback(
                    http,
                    params,
                    &requested_mode,
                    "opensky_cooldown_regional_fallback",
                )
                .await
            {
                return response;
            }
            let cooldown_until = self.cache.lock().cooldown_until;
            return opensky_response(
                429,
                Headers {
                    cache_status: "COOLDOWN",
                    requested_mode: &requested_mode,
                    used_mode: "none",
                    reason: "rate_limited",
                    stale_seconds: None,
                    retry_after_seconds: Some((cooldown_until - now) as f64 / 1000.0),
                },
                r#"{"error":"OpenSky rate limited; proxy cooling down."}"#,
            );
        }

        match self
            .fetch_and_serve(app, params, &requested_mode, now)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::error!("[OpenSky Proxy] {error}");
                let cached = {
                    let cache = self.cache.lock();
                    cache
                        .body
                        .clone()
                        .map(|body| (body, cache.status, cache.meta.clone()))
                };
                if let Some((body, status, meta)) = cached {
                    let meta = meta.unwrap_or_else(|| Meta {
                        requested_mode: requested_mode.clone(),
                        used_mode: "unknown".into(),
                        reason: "cached_stale".into(),
                    });
                    let or = |value: &str, fallback: &str| {
                        if value.is_empty() {
                            fallback.to_string()
                        } else {
                            value.to_string()
                        }
                    };
                    return opensky_response(
                        if status == 0 { 200 } else { status },
                        Headers {
                            cache_status: "STALE",
                            requested_mode: &or(&meta.requested_mode, OPENSKY_AUTH_MODE_DEFAULT),
                            used_mode: &or(&meta.used_mode, "unknown"),
                            reason: &or(&meta.reason, "cached_stale"),
                            stale_seconds: None,
                            retry_after_seconds: None,
                        },
                        body,
                    );
                }
                if let Some(response) = self
                    .serve_point_fallback(
                        http,
                        params,
                        &requested_mode,
                        "opensky_proxy_error_regional_fallback",
                    )
                    .await
                {
                    return response;
                }
                opensky_response(
                    502,
                    Headers {
                        cache_status: "MISS",
                        requested_mode: &requested_mode,
                        used_mode: "error",
                        reason: "proxy_error",
                        stale_seconds: None,
                        retry_after_seconds: None,
                    },
                    r#"{"error":"OpenSky proxy error"}"#,
                )
            }
        }
    }

    async fn fetch_and_serve(
        &self,
        app: &AppState,
        params: &Params,
        requested_mode: &str,
        now: i64,
    ) -> Result<Response, reqwest::Error> {
        let http = &app.http;
        let basic_user = app.env.raw("OPENSKY_USERNAME").unwrap_or_default();
        let basic_pass = app.env.raw("OPENSKY_PASSWORD").unwrap_or_default();
        let has_basic_creds = !basic_user.is_empty() && !basic_pass.is_empty();
        let basic_auth = || {
            format!(
                "Basic {}",
                jsval::base64(format!("{basic_user}:{basic_pass}").as_bytes())
            )
        };
        let mut authorization: Option<String> = None;
        let mut used_mode = "anon";
        let mut reason = "forced_anonymous";
        match requested_mode {
            "basic" => {
                if has_basic_creds {
                    authorization = Some(basic_auth());
                    used_mode = "basic";
                    reason = "basic_credentials";
                } else {
                    reason = "missing_basic_creds";
                }
            }
            "oauth" => {
                if let Some(token) = self.tokens.get(app).await {
                    authorization = Some(format!("Bearer {token}"));
                    used_mode = "oauth";
                    reason = "oauth_token";
                } else {
                    reason = "oauth_invalid_or_missing";
                }
            }
            "auto" => {
                if let Some(token) = self.tokens.get(app).await {
                    authorization = Some(format!("Bearer {token}"));
                    used_mode = "oauth";
                    reason = "oauth_token";
                } else if has_basic_creds {
                    authorization = Some(basic_auth());
                    used_mode = "basic";
                    reason = "oauth_unavailable_fallback_basic";
                } else {
                    reason = "missing_oauth_and_basic_creds";
                }
            }
            _ => {}
        }

        let mut upstream = self.fetch_states(http, authorization.as_deref()).await?;
        // Auto-mode fallback: OAuth rejected, retry with Basic credentials.
        if (upstream.status == 401 || upstream.status == 403)
            && requested_mode == "auto"
            && used_mode == "oauth"
            && has_basic_creds
        {
            upstream = self.fetch_states(http, Some(&basic_auth())).await?;
            used_mode = "basic";
            reason = "oauth_rejected_fallback_basic";
        }
        let mut body = upstream.body.clone();

        let source_epoch = if upstream.ok {
            source_epoch_ms(&body)
        } else {
            None
        };
        if upstream.ok
            && source_is_stale(source_epoch, now)
            && let Some(response) = self
                .serve_point_fallback(
                    http,
                    params,
                    requested_mode,
                    "opensky_snapshot_stale_regional_fallback",
                )
                .await
        {
            // Keep the global snapshot as a fail-soft cache only.
            let mut cache = self.cache.lock();
            cache.body = Some(body);
            cache.status = upstream.status;
            cache.time = now;
            cache.source_epoch_ms = source_epoch;
            cache.meta = Some(Meta {
                requested_mode: requested_mode.into(),
                used_mode: used_mode.into(),
                reason: reason.into(),
            });
            return Ok(response);
        }
        if upstream.status == 429 {
            reason = "rate_limited";
            // Credit governor: honor the retry-after, bounded 30 s … 30 min.
            // An absent header reads as `Number(null)`, i.e. 0.
            let retry_after_sec = js::number(&upstream.retry_after);
            let cooldown_ms = (if retry_after_sec.is_finite() {
                retry_after_sec * 1000.0
            } else {
                120_000.0
            })
            .clamp(30_000.0, 30.0 * 60_000.0);
            let stale = {
                let mut cache = self.cache.lock();
                cache.cooldown_until = now + cooldown_ms as i64;
                if cache.status == 200 {
                    cache.body.clone().map(|body| (body, cache.time))
                } else {
                    None
                }
            };
            if let Some((stale_body, time)) = stale {
                return Ok(opensky_response(
                    200,
                    Headers {
                        cache_status: "STALE",
                        requested_mode,
                        used_mode,
                        reason: "rate_limited_serving_stale",
                        stale_seconds: Some((now - time) as f64 / 1000.0),
                        retry_after_seconds: Some(cooldown_ms / 1000.0),
                    },
                    stale_body,
                ));
            }
        }

        if !upstream.ok && self.cache.lock().body.is_none() {
            let fallback_reason = format!("opensky_http_{}_regional_fallback", upstream.status);
            if let Some(response) = self
                .serve_point_fallback(http, params, requested_mode, &fallback_reason)
                .await
            {
                return Ok(response);
            }
        }

        if upstream.status == 401 || upstream.status == 403 {
            let (message, why) = if requested_mode == "basic" && !has_basic_creds {
                (
                    "OpenSky auth missing. Basic mode requires OPENSKY_USERNAME and OPENSKY_PASSWORD.",
                    "missing_basic_creds",
                )
            } else if requested_mode == "oauth" && used_mode != "oauth" {
                (
                    "OpenSky auth invalid. OAuth mode requires valid OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET.",
                    "oauth_invalid_or_missing",
                )
            } else if used_mode == "basic" {
                (
                    "OpenSky auth invalid. Username/password were rejected.",
                    "basic_invalid_credentials",
                )
            } else if used_mode == "oauth" {
                (
                    "OpenSky auth invalid. OAuth client credentials were rejected.",
                    "oauth_invalid_credentials",
                )
            } else if requested_mode == "auto" && !has_basic_creds {
                (
                    "OpenSky auth missing. Provide basic credentials or valid OAuth client credentials.",
                    "missing_oauth_and_basic_creds",
                )
            } else {
                ("OpenSky auth required.", "auth_required")
            };
            body = Bytes::from(
                serde_json::to_vec(&serde_json::json!({ "error": message })).unwrap_or_default(),
            );
            reason = why;
        }

        if upstream.ok && reason == "forced_anonymous" {
            reason = "anonymous_ok";
        } else if upstream.ok && used_mode == "basic" && reason == "basic_credentials" {
            reason = "basic_ok";
        } else if upstream.ok && used_mode == "oauth" && reason == "oauth_token" {
            reason = "oauth_ok";
        }

        // Only successful responses are cached.
        if upstream.ok {
            let mut cache = self.cache.lock();
            cache.body = Some(body.clone());
            cache.status = upstream.status;
            cache.time = now;
            cache.source_epoch_ms = source_epoch;
            cache.meta = Some(Meta {
                requested_mode: requested_mode.into(),
                used_mode: used_mode.into(),
                reason: reason.into(),
            });
            // Absent header: `Number(null)` is 0, the tightest tier.
            cache.ttl_ms = adaptive_ttl_ms(js::number(&upstream.remaining));
            cache.cooldown_until = 0;
        }

        Ok(opensky_response(
            upstream.status,
            Headers {
                cache_status: "MISS",
                requested_mode,
                used_mode,
                reason,
                stale_seconds: None,
                retry_after_seconds: None,
            },
            body,
        ))
    }
}

pub fn router(opensky: Arc<OpenSky>) -> Router<AppState> {
    let handler = move |State(app): State<AppState>, params: Params| {
        let opensky = Arc::clone(&opensky);
        async move {
            let response = opensky.handle(&app, &params).await;
            reply::with_header(response, "X-Feed-Source", "OpenSky Network")
        }
    };
    Router::new()
        .route("/api/flights", any(handler.clone()))
        .route("/api/flights/", any(handler))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_ttl_tiers() {
        assert_eq!(adaptive_ttl_ms(f64::NAN), 9000);
        assert_eq!(adaptive_ttl_ms(3000.0), 9000);
        assert_eq!(adaptive_ttl_ms(2000.0), 30_000);
        assert_eq!(adaptive_ttl_ms(800.0), 90_000);
        assert_eq!(adaptive_ttl_ms(0.0), 300_000);
    }

    #[test]
    fn fallback_anchor_requires_both_coordinates() {
        assert_eq!(adsb_lol_fallback_anchor(&Params::parse("?lat=12.5")), None);
        assert_eq!(adsb_lol_fallback_anchor(&Params::parse("?lon=12.5")), None);
        assert_eq!(
            adsb_lol_fallback_anchor(&Params::parse("?lat=91&lon=0")),
            None
        );
        assert_eq!(
            adsb_lol_fallback_anchor(&Params::parse("?lat=&lon=0")),
            None
        );
        assert_eq!(
            adsb_lol_fallback_anchor(&Params::parse("?lat=1&lon=-2")),
            Some((1.0, -2.0))
        );
    }

    #[test]
    fn source_staleness() {
        assert_eq!(source_epoch_ms(br#"{"time":100}"#), Some(100_000.0));
        assert_eq!(source_epoch_ms(br#"{"time":0}"#), None);
        assert_eq!(source_epoch_ms(b"nope"), None);
        assert!(source_is_stale(Some(0.0), 120_001));
        assert!(!source_is_stale(Some(0.0), 120_000));
        assert!(!source_is_stale(None, 1_000_000));
    }
}
