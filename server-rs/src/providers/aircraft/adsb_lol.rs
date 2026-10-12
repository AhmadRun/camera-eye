//! adsb.lol military aircraft proxy (`server/providers/aircraft/adsb-lol.js`):
//! `GET /api/military` from `/v2/mil` with a 12 s cache, serve-stale on
//! upstream failure, and a bounded Retry-After cooldown.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use parking_lot::Mutex;

use super::jsval;
use crate::common::js::{self, now_ms};
use crate::common::reply;
use crate::state::AppState;

pub const ADSBLOL_MIL_URL: &str = "https://api.adsb.lol/v2/mil";
/// Response cache TTL (ms).
const CACHE_MS: i64 = 12_000;
/// Cooldown after a 429 without a usable Retry-After (ms).
const RATE_LIMIT_COOLDOWN_MS: i64 = 30_000;
/// Cooldown after a 5xx (ms).
const SERVER_ERROR_COOLDOWN_MS: i64 = 15_000;
/// Bounds for an upstream-supplied Retry-After (ms).
const COOLDOWN_MIN_MS: f64 = 5000.0;
const COOLDOWN_MAX_MS: f64 = 120_000.0;

#[derive(Default)]
struct MilState {
    cache: Option<Bytes>,
    cache_at: i64,
    cooldown_until: i64,
    cooldown_status: u16,
}

pub struct Military {
    url: String,
    state: Mutex<MilState>,
}

fn clamp_cooldown(ms: f64) -> i64 {
    ms.clamp(COOLDOWN_MIN_MS, COOLDOWN_MAX_MS) as i64
}

/// `cooldownFor`: honour Retry-After (seconds or HTTP date) when sane.
pub fn cooldown_for(status: u16, retry_after: Option<&str>, now: i64) -> i64 {
    if let Some(raw) = retry_after.filter(|r| !r.is_empty()) {
        let seconds = js::number(raw);
        if seconds.is_finite() && seconds > 0.0 {
            return clamp_cooldown(seconds * 1000.0);
        }
        let at = chrono::DateTime::parse_from_rfc2822(raw.trim())
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(raw.trim()))
            .map(|d| d.timestamp_millis());
        if let Ok(at) = at
            && at > now
        {
            return clamp_cooldown((at - now) as f64);
        }
    }
    if status == 429 {
        RATE_LIMIT_COOLDOWN_MS
    } else {
        SERVER_ERROR_COOLDOWN_MS
    }
}

fn retry_seconds(until: i64) -> String {
    jsval::number_to_string((((until - now_ms()) as f64) / 1000.0).ceil())
}

impl Military {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            state: Mutex::new(MilState::default()),
        }
    }

    fn serve(
        &self,
        status: u16,
        body: Bytes,
        cache_status: &str,
        extra: &[(&str, String)],
    ) -> Response {
        let mut response = reply::raw(status, "application/json", body, Some("no-store"));
        response = reply::with_header(response, "X-Feed-Source", "adsb.lol");
        response = reply::with_header(response, "X-Feed-Cache", cache_status);
        if cache_status == "HIT" || cache_status == "STALE" {
            let age = (now_ms() - self.state.lock().cache_at).max(0);
            response = reply::with_header(response, "X-Feed-Age-Ms", &age.to_string());
        }
        for (name, value) in extra {
            response = reply::with_header(response, name, value);
        }
        response
    }

    async fn handle(&self, http: &reqwest::Client) -> Response {
        let now = now_ms();
        {
            let state = self.state.lock();
            if let Some(cache) = &state.cache
                && now - state.cache_at < CACHE_MS
            {
                let body = cache.clone();
                drop(state);
                return self.serve(200, body, "HIT", &[]);
            }
            if now < state.cooldown_until {
                let retry_after = retry_seconds(state.cooldown_until);
                let cache = state.cache.clone();
                let cooldown_status = state.cooldown_status;
                drop(state);
                if let Some(cache) = cache {
                    return self.serve(
                        200,
                        cache,
                        "STALE",
                        &[
                            ("X-ADS-B-Upstream-Status", cooldown_status.to_string()),
                            ("X-ADS-B-Retry-After-Seconds", retry_after),
                        ],
                    );
                }
                return self.serve(
                    if cooldown_status == 0 {
                        503
                    } else {
                        cooldown_status
                    },
                    Bytes::from_static(br#"{"error":"adsb.lol upstream cooling down"}"#),
                    "NONE",
                    &[("Retry-After", retry_after)],
                );
            }
        }
        match self.fetch(http).await {
            Ok(response) => response,
            Err(error) => {
                tracing::error!("[adsb.lol Proxy] {error}");
                let cache = self.state.lock().cache.clone();
                if let Some(cache) = cache {
                    return self.serve(200, cache, "STALE", &[]);
                }
                reply::raw(
                    502,
                    "application/json",
                    r#"{"error":"ADS-B proxy error"}"#,
                    None,
                )
            }
        }
    }

    async fn fetch(&self, http: &reqwest::Client) -> Result<Response, reqwest::Error> {
        let upstream = http
            .get(&self.url)
            .header("User-Agent", "camera-eye-adsblol-proxy/1.0")
            .send()
            .await?;
        let status = upstream.status().as_u16();
        if upstream.status().is_success() {
            let body = upstream.bytes().await?;
            {
                let mut state = self.state.lock();
                state.cache = Some(body.clone());
                state.cache_at = now_ms();
                state.cooldown_until = 0;
                state.cooldown_status = 0;
            }
            return Ok(self.serve(200, body, "MISS", &[]));
        }
        if status == 429 || status >= 500 {
            let failed_at = now_ms();
            let retry_after = upstream
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let (cache, until) = {
                let mut state = self.state.lock();
                state.cooldown_until =
                    failed_at + cooldown_for(status, retry_after.as_deref(), failed_at);
                state.cooldown_status = status;
                (state.cache.clone(), state.cooldown_until)
            };
            tracing::warn!(
                "[adsb.lol Proxy] upstream {status}; cooling down {} s{}",
                jsval::round((until - now_ms()) as f64 / 1000.0),
                if cache.is_some() {
                    ", serving stale"
                } else {
                    ""
                }
            );
            if let Some(cache) = cache {
                // Do not wait for a failing response body.
                drop(upstream);
                return Ok(self.serve(
                    200,
                    cache,
                    "STALE",
                    &[
                        ("X-ADS-B-Upstream-Status", status.to_string()),
                        ("X-ADS-B-Retry-After-Seconds", retry_seconds(until)),
                    ],
                ));
            }
        }
        let body = upstream.bytes().await?;
        let until = self.state.lock().cooldown_until;
        let extra: Vec<(&str, String)> = if until > now_ms() {
            vec![("Retry-After", retry_seconds(until))]
        } else {
            Vec::new()
        };
        Ok(self.serve(status, body, "MISS", &extra))
    }
}

pub fn router(military: Arc<Military>) -> Router<AppState> {
    let handler = move |State(app): State<AppState>| {
        let military = Arc::clone(&military);
        async move { military.handle(&app.http).await }
    };
    Router::new()
        .route("/api/military", any(handler.clone()))
        .route("/api/military/", any(handler))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown_honours_retry_after_within_bounds() {
        assert_eq!(cooldown_for(429, None, 0), 30_000);
        assert_eq!(cooldown_for(503, None, 0), 15_000);
        assert_eq!(cooldown_for(429, Some("10"), 0), 10_000);
        assert_eq!(cooldown_for(429, Some("1"), 0), 5000);
        assert_eq!(cooldown_for(429, Some("9999"), 0), 120_000);
        assert_eq!(cooldown_for(500, Some("0"), 0), 15_000);
        let now = chrono::DateTime::parse_from_rfc2822("Wed, 21 Oct 2015 07:28:00 GMT")
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            cooldown_for(429, Some("Wed, 21 Oct 2015 07:28:20 GMT"), now),
            20_000
        );
        assert_eq!(
            cooldown_for(429, Some("Wed, 21 Oct 2015 07:27:00 GMT"), now),
            30_000
        );
    }
}
