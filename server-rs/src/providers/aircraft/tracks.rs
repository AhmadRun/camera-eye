//! Aircraft track-history backfill (`server/providers/aircraft/tracks.js`):
//! `/api/flights/track?icao24=` (OpenSky `/tracks/all`) and
//! `/api/military/track?hex=` (adsb.lol readsb trace).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use parking_lot::Mutex;

use super::opensky::TokenManager;
use crate::common::http::{UpstreamError, read_text_capped};
use crate::common::js::now_ms;
use crate::common::query::Params;
use crate::common::reply;
use crate::state::AppState;

const TRACK_CACHE_MS: i64 = 60_000;
const TRACK_CACHE_MAX: usize = 200;
const RESPONSE_CAP_BYTES: usize = 5 * 1024 * 1024;
const TRACK_TIMEOUT: Duration = Duration::from_millis(12_000);
pub const OPENSKY_API_BASE: &str = "https://opensky-network.org";
pub const ADSBLOL_SITE_BASE: &str = "https://adsb.lol";

#[derive(Clone)]
struct Entry {
    at: i64,
    status: u16,
    body: Bytes,
}

pub struct Tracks {
    tokens: Arc<TokenManager>,
    opensky_base: String,
    adsblol_base: String,
    cache: Mutex<HashMap<String, Entry>>,
}

fn track_response(status: u16, body: Bytes) -> Response {
    reply::raw(
        status,
        "application/json; charset=utf-8",
        body,
        Some("no-store"),
    )
}

fn plain_error(status: u16, message: &str) -> Response {
    reply::json(status, &serde_json::json!({ "error": message }), None)
}

/// `^[0-9a-f]{n,m}$` over `allowed` bytes.
fn matches(value: &str, min: usize, max: usize, extra: &[u8]) -> bool {
    (min..=max).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || extra.contains(&b))
}

/// `String(params.get(key) || '').trim().toLowerCase()`.
fn param(params: &Params, key: &str) -> String {
    params.get(key).unwrap_or("").trim().to_lowercase()
}

pub fn valid_icao24(value: &str) -> bool {
    matches(value, 6, 6, b"")
}

pub fn valid_trace_hex(value: &str) -> bool {
    matches(value, 6, 7, b"~")
}

impl Tracks {
    pub fn new(
        tokens: Arc<TokenManager>,
        opensky_base: impl Into<String>,
        adsblol_base: impl Into<String>,
    ) -> Self {
        Self {
            tokens,
            opensky_base: opensky_base.into(),
            adsblol_base: adsblol_base.into(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn cache_put(&self, key: String, entry: Entry) {
        let mut cache = self.cache.lock();
        cache.insert(key, entry);
        if cache.len() > TRACK_CACHE_MAX {
            let oldest = cache
                .iter()
                .min_by_key(|(_, e)| e.at)
                .map(|(k, _)| k.clone());
            if let Some(oldest) = oldest {
                cache.remove(&oldest);
            }
        }
    }

    /// `proxyJson`: `Err` when the upstream request itself failed.
    async fn proxy_json(
        &self,
        http: &reqwest::Client,
        key: String,
        url: &str,
        authorization: Option<String>,
    ) -> Result<Response, UpstreamError> {
        let cached = self.cache.lock().get(&key).cloned();
        if let Some(entry) = cached
            && now_ms() - entry.at < TRACK_CACHE_MS
        {
            return Ok(track_response(entry.status, entry.body));
        }
        let mut request = http.get(url).timeout(TRACK_TIMEOUT);
        if let Some(auth) = authorization {
            request = request.header("Authorization", auth);
        }
        let upstream = request.send().await?;
        let upstream_status = upstream.status();
        let read = read_text_capped(upstream, RESPONSE_CAP_BYTES).await;
        let (status, body) = match read {
            Err(UpstreamError::TooLarge) => (
                502,
                Bytes::from_static(br#"{"error":"Upstream track response too large"}"#),
            ),
            Err(error) => return Err(error),
            Ok(_) if !upstream_status.is_success() => (
                upstream_status.as_u16(),
                Bytes::from(format!(
                    r#"{{"error":"Track source HTTP {}"}}"#,
                    upstream_status.as_u16()
                )),
            ),
            Ok(text) => (upstream_status.as_u16(), Bytes::from(text)),
        };
        // The 502 is cached like any upstream error: /tracks/all costs credits.
        self.cache_put(
            key,
            Entry {
                at: now_ms(),
                status,
                body: body.clone(),
            },
        );
        Ok(track_response(status, body))
    }

    async fn flights_track(&self, app: &AppState, params: &Params) -> Response {
        let icao24 = param(params, "icao24");
        if !valid_icao24(&icao24) {
            return plain_error(400, "icao24 must be a 6-char hex string");
        }
        let token = self.tokens.get(app).await;
        let url = format!(
            "{}/api/tracks/all?icao24={icao24}&time=0",
            self.opensky_base
        );
        match self
            .proxy_json(
                &app.http,
                format!("osky:{icao24}"),
                &url,
                token.map(|t| format!("Bearer {t}")),
            )
            .await
        {
            Ok(response) => response,
            Err(_) => plain_error(502, "OpenSky track fetch failed"),
        }
    }

    async fn military_track(&self, app: &AppState, params: &Params) -> Response {
        let hex = param(params, "hex");
        if !valid_trace_hex(&hex) {
            return plain_error(400, "hex must be a 6-7 char hex string");
        }
        let url = format!(
            "{}/data/traces/{}/trace_full_{hex}.json",
            self.adsblol_base,
            &hex[hex.len() - 2..]
        );
        match self
            .proxy_json(&app.http, format!("lol:{hex}"), &url, None)
            .await
        {
            Ok(response) => response,
            Err(_) => plain_error(502, "adsb.lol trace fetch failed"),
        }
    }
}

pub fn router(tracks: Arc<Tracks>) -> Router<AppState> {
    let flights = {
        let tracks = Arc::clone(&tracks);
        move |State(app): State<AppState>, params: Params| {
            let tracks = Arc::clone(&tracks);
            async move { tracks.flights_track(&app, &params).await }
        }
    };
    let military = move |State(app): State<AppState>, params: Params| {
        let tracks = Arc::clone(&tracks);
        async move { tracks.military_track(&app, &params).await }
    };
    // Connect's mount matched any sub-path too; the handlers ignore it.
    Router::new()
        .route("/api/flights/track", any(flights.clone()))
        .route("/api/flights/track/", any(flights.clone()))
        .route("/api/flights/track/{*rest}", any(flights))
        .route("/api/military/track", any(military.clone()))
        .route("/api/military/track/", any(military.clone()))
        .route("/api/military/track/{*rest}", any(military))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_validated() {
        assert!(valid_icao24("abc123"));
        assert!(!valid_icao24("abc12"));
        assert!(!valid_icao24("abc12g"));
        assert!(valid_trace_hex("~abc123"));
        assert!(valid_trace_hex("abc123"));
        assert!(!valid_trace_hex("abc12"));
        assert!(!valid_trace_hex("abc12345"));
        assert_eq!(
            param(&Params::parse("icao24=%20ABC123%20"), "icao24"),
            "abc123"
        );
    }
}
