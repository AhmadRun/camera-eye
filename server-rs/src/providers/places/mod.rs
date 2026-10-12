//! Place search and routing (port of `server/providers/places.js` and
//! `server/providers/places/*`):
//!
//! - `GET /api/google/nearby-places?lat&lon[&radiusM]`: nearby place labels.
//! - `GET /api/google/text-search?q&lat&lon[&radiusM]`: view-biased text search.
//! - `GET /api/route?profile&coords[&steps=1]`: OSM routing (installed by
//!   `overpass.js` in the JS server; it lives here with its payload code).
//!
//! Both Google routes refuse cross-site requests first, answer a keyless
//! install with a successful empty capability, validate coordinates before
//! spending quota, and share one per-IP limiter (`CE_RATELIMIT_GOOGLE_PER_MIN`,
//! default 120/min, `0` disables). Every error body carries `places: []`.

mod payloads;
mod route_steps;
mod routes;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{ConnectInfo, Request, State};
use axum::response::Response;
use axum::routing::{MethodRouter, any};
use http::Method;
use once_cell::sync::OnceCell;
use serde_json::{Value, json};

use super::js_value::{get, or};
use crate::common::js;
use crate::common::query::Params;
use crate::common::rate_limit::{RateLimiter, client_key, cost_rate_limiter};
use crate::common::reply;
use crate::common::same_site::admit_same_site;
use crate::env::Env;
use crate::state::AppState;

/// Requests/min/IP when `CE_RATELIMIT_GOOGLE_PER_MIN` is unset.
pub const GOOGLE_DEFAULT_PER_MIN: u64 = 120;

const PLACES_FAILED: &str = "Google Places request failed";

#[derive(Clone)]
pub struct Config {
    pub nearby_endpoint: String,
    pub text_search_endpoint: String,
    /// `endpoints[profile]` overrides for `/api/route`.
    pub route_endpoints: HashMap<String, String>,
    /// The outbound route gate (only a test shortens it).
    pub route_interval_ms: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            nearby_endpoint: "https://places.googleapis.com/v1/places:searchNearby".into(),
            text_search_endpoint: "https://places.googleapis.com/v1/places:searchText".into(),
            route_endpoints: HashMap::new(),
            route_interval_ms: routes::ROUTE_UPSTREAM_MIN_INTERVAL_MS,
        }
    }
}

pub struct Places {
    config: Config,
    /// Redirects are refused, like `redirect: 'error'`.
    http: reqwest::Client,
    google_limiter: OnceCell<Option<RateLimiter>>,
    routes: routes::RouteService,
}

impl Places {
    fn new(config: Config) -> Self {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let routes =
            routes::RouteService::new(config.route_endpoints.clone(), config.route_interval_ms);
        Self {
            config,
            http,
            google_limiter: OnceCell::new(),
            routes,
        }
    }
}

/// `resolveGoogleServerKey(process.env)`: the server-only key, else the
/// shared browser key.
pub fn google_server_api_key(env: &Env) -> String {
    let value = |name: &str| env.raw(name).unwrap_or_default().trim().to_string();
    let server = value("GOOGLE_MAPS_SERVER_API_KEY");
    if server.is_empty() {
        value("GOOGLE_MAPS_API_KEY")
    } else {
        server
    }
}

/// `keylessGooglePlacesResponse`.
pub fn keyless_google_places_response(api_key: &str) -> Option<(u16, Value)> {
    if !api_key.trim().is_empty() {
        return None;
    }
    Some((
        200,
        json!({ "configured": false, "error": null, "places": [] }),
    ))
}

/// `validatePlacesCoordinates`: `Ok((lat, lon))` or the 400 message.
pub fn validate_places_coordinates(params: &Params) -> Result<(f64, f64), &'static str> {
    let (Some(raw_lat), Some(raw_lon)) = (params.get("lat"), params.get("lon")) else {
        return Err("lat and lon are required");
    };
    if raw_lat.trim().is_empty() || raw_lon.trim().is_empty() {
        return Err("lat and lon are required");
    }
    let latitude = js::number(raw_lat);
    let longitude = js::number(raw_lon);
    if !latitude.is_finite() || !longitude.is_finite() {
        return Err("Valid lat and lon are required");
    }
    if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
        return Err("lat must be within [-90, 90] and lon within [-180, 180]");
    }
    Ok((latitude, longitude))
}

/// `Math.max(min, Math.min(max, Number(raw) || fallback))`.
fn radius(raw: Option<&str>, min: f64, max: f64, fallback: f64) -> f64 {
    let number = raw.map_or(0.0, js::number);
    let number = if number.is_nan() || number == 0.0 {
        fallback
    } else {
        number
    };
    number.min(max).max(min)
}

pub fn router(state: &AppState) -> Router<AppState> {
    router_with(state, Config::default())
}

pub fn router_with(_state: &AppState, config: Config) -> Router<AppState> {
    let svc = Arc::new(Places::new(config));
    let mut router = Router::new();
    router = mount(
        router,
        "/api/google/nearby-places",
        handler(&svc, |svc, state, req| async move {
            google(&svc, &state, req, Search::Nearby).await
        }),
    );
    router = mount(
        router,
        "/api/google/text-search",
        handler(&svc, |svc, state, req| async move {
            google(&svc, &state, req, Search::Text).await
        }),
    );
    mount(
        router,
        "/api/route",
        handler(&svc, |svc, _state, req| async move {
            routes::handle(&svc, req).await
        }),
    )
}

fn handler<F, Fut>(svc: &Arc<Places>, run: F) -> MethodRouter<AppState>
where
    F: Fn(Arc<Places>, AppState, Request) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    let svc = svc.clone();
    any(move |State(state): State<AppState>, req: Request| {
        let svc = svc.clone();
        let run = run.clone();
        async move { run(svc, state, req).await }
    })
}

/// Connect's `use(path)` also answers every sub-path.
fn mount(
    router: Router<AppState>,
    path: &str,
    handler: MethodRouter<AppState>,
) -> Router<AppState> {
    router
        .route(path, handler.clone())
        .route(&format!("{path}/"), handler.clone())
        .route(&format!("{path}/{{*rest}}"), handler)
}

fn peer(req: &Request) -> Option<SocketAddr> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Search {
    Nearby,
    Text,
}

fn places_error(status: u16, message: &str) -> Response {
    reply::json(status, &json!({ "error": message, "places": [] }), None)
}

fn json_utf8(status: u16, value: &Value, cache_control: Option<&str>) -> Response {
    reply::raw(
        status,
        "application/json; charset=utf-8",
        serde_json::to_vec(value).unwrap_or_default(),
        cache_control,
    )
}

async fn google(svc: &Places, state: &AppState, req: Request, search: Search) -> Response {
    // Gate first: a cross-site caller learns nothing about the method surface.
    if let Some(refused) = admit_same_site(req.headers()) {
        return refused;
    }
    if req.method() != Method::GET {
        return places_error(405, "Method not allowed");
    }

    // Keyless context costs nothing, so it resolves before the limiter.
    let api_key = google_server_api_key(&state.env);
    if let Some((status, payload)) = keyless_google_places_response(&api_key) {
        return reply::json(status, &payload, Some("no-store"));
    }

    let params = Params::parse(req.uri().query().unwrap_or(""));
    let text_query = params.get("q").unwrap_or("").trim().to_string();
    if search == Search::Text && text_query.is_empty() {
        return places_error(400, "q, lat and lon are required");
    }
    let (latitude, longitude) = match validate_places_coordinates(&params) {
        Ok(point) => point,
        Err(message) => return places_error(400, message),
    };

    let limiter = svc.google_limiter.get_or_init(|| {
        cost_rate_limiter(
            state.env.raw("CE_RATELIMIT_GOOGLE_PER_MIN").as_deref(),
            GOOGLE_DEFAULT_PER_MIN,
        )
    });
    if let Some(limiter) = limiter
        && !limiter.allow(&client_key(peer(&req)))
    {
        return reply::with_header(places_error(429, "Rate limit exceeded"), "Retry-After", "5");
    }

    let circle = |radius_m: f64| {
        json!({
            "circle": {
                "center": { "latitude": latitude, "longitude": longitude },
                "radius": super::js_value::number_value(radius_m),
            }
        })
    };
    let (endpoint, field_mask, body) = match search {
        Search::Nearby => (
            &svc.config.nearby_endpoint,
            [
                "places.id",
                "places.displayName",
                "places.formattedAddress",
                "places.shortFormattedAddress",
                "places.location",
                "places.primaryType",
                "places.primaryTypeDisplayName",
                "places.types",
            ]
            .join(","),
            json!({
                "maxResultCount": 20,
                "rankPreference": "DISTANCE",
                "locationRestriction": circle(radius(params.get("radiusM"), 25.0, 5000.0, 250.0)),
            }),
        ),
        Search::Text => (
            &svc.config.text_search_endpoint,
            [
                "places.id",
                "places.displayName",
                "places.formattedAddress",
                "places.location",
                "places.viewport",
                "places.primaryType",
                "places.types",
            ]
            .join(","),
            json!({
                "textQuery": text_query,
                "locationBias": circle(radius(params.get("radiusM"), 50.0, 50000.0, 4000.0)),
                "maxResultCount": 5,
            }),
        ),
    };

    let result = async {
        let response = svc
            .http
            .post(endpoint)
            .header("Content-Type", "application/json")
            .header("X-Goog-Api-Key", &api_key)
            .header("X-Goog-FieldMask", field_mask)
            .body(serde_json::to_vec(&body).unwrap_or_default())
            .send()
            .await?;
        let status = response.status();
        // `response.json().catch(() => ({}))`
        let data = response
            .bytes()
            .await
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .unwrap_or_else(|| json!({}));
        Ok::<_, reqwest::Error>((status, data))
    }
    .await;

    // Node's fetch rejects every network fault and refused redirect with
    // this message; it is what the JS relayed.
    let fetch_failed = || json_utf8(502, &json!({ "error": "fetch failed", "places": [] }), None);
    let (status, data) = match result {
        Ok((status, _)) if status.is_redirection() => return fetch_failed(),
        Ok(answer) => answer,
        Err(_) => return fetch_failed(),
    };
    let projected = match search {
        Search::Nearby => payloads::project_nearby_places(&data, latitude, longitude),
        Search::Text => payloads::project_text_search_places(&data, latitude, longitude),
    };
    let places = match projected {
        Ok(places) => places,
        Err(message) => {
            return json_utf8(502, &json!({ "error": message, "places": [] }), None);
        }
    };
    let ok = status.is_success();
    let error = if ok {
        Value::Null
    } else {
        or(&[get(get(Some(&data), "error"), "message")])
            .cloned()
            .unwrap_or_else(|| Value::String(PLACES_FAILED.into()))
    };
    json_utf8(
        if ok { 200 } else { status.as_u16() },
        &json!({ "places": places, "error": error }),
        Some("private, max-age=300"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::routing::post;
    use parking_lot::Mutex;

    use super::super::openai::testkit::{Call, call, serve, state, temp_dir};
    use super::*;

    #[test]
    fn coordinates_validate_like_js() {
        let check = |q: &str| validate_places_coordinates(&Params::parse(q));
        assert_eq!(check("lat=1&lon=2"), Ok((1.0, 2.0)));
        assert_eq!(check("lon=2"), Err("lat and lon are required"));
        assert_eq!(check("lat=%20&lon=2"), Err("lat and lon are required"));
        assert_eq!(
            check("lat=abc&lon=2"),
            Err("Valid lat and lon are required")
        );
        assert_eq!(
            check("lat=91&lon=2"),
            Err("lat must be within [-90, 90] and lon within [-180, 180]")
        );
        assert_eq!(
            check("lat=1&lon=-181"),
            Err("lat must be within [-90, 90] and lon within [-180, 180]")
        );
    }

    #[test]
    fn server_key_prefers_the_server_only_key() {
        let env = Env::default();
        env.set("GOOGLE_MAPS_SERVER_API_KEY", " server ");
        env.set("GOOGLE_MAPS_API_KEY", "browser");
        assert_eq!(google_server_api_key(&env), "server");
        env.set("GOOGLE_MAPS_SERVER_API_KEY", "  ");
        assert_eq!(google_server_api_key(&env), "browser");
        env.set("GOOGLE_MAPS_API_KEY", "");
        assert_eq!(google_server_api_key(&env), "");
        assert!(keyless_google_places_response("").is_some());
        assert!(keyless_google_places_response("k").is_none());
    }

    #[test]
    fn radius_clamps_like_js() {
        assert_eq!(radius(None, 25.0, 5000.0, 250.0), 250.0);
        assert_eq!(radius(Some("0"), 25.0, 5000.0, 250.0), 250.0);
        assert_eq!(radius(Some("10"), 25.0, 5000.0, 250.0), 25.0);
        assert_eq!(radius(Some("1e9"), 25.0, 5000.0, 250.0), 5000.0);
        assert_eq!(radius(Some("x"), 50.0, 50000.0, 4000.0), 4000.0);
    }

    struct Google {
        base: String,
        calls: Arc<AtomicUsize>,
        sent: Arc<Mutex<Vec<(String, Value)>>>,
    }

    async fn google_stub(status: u16, reply: Value) -> Google {
        let calls = Arc::new(AtomicUsize::new(0));
        let sent: Arc<Mutex<Vec<(String, Value)>>> = Arc::default();
        let (c, s) = (calls.clone(), sent.clone());
        let app = Router::new().route(
            "/{*path}",
            post(move |headers: http::HeaderMap, body: String| {
                let (c, s, reply) = (c.clone(), s.clone(), reply.clone());
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    let key = headers
                        .get("x-goog-api-key")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    s.lock()
                        .push((key, serde_json::from_str(&body).unwrap_or(Value::Null)));
                    (
                        http::StatusCode::from_u16(status).unwrap(),
                        axum::Json(reply),
                    )
                }
            }),
        );
        Google {
            base: serve(app).await,
            calls,
            sent,
        }
    }

    fn app(base: &str, key: &str) -> Router {
        let state = state(temp_dir("places"));
        state.env.set("GOOGLE_MAPS_SERVER_API_KEY", key);
        state.env.set("GOOGLE_MAPS_API_KEY", "");
        state.env.set("CE_RATELIMIT_GOOGLE_PER_MIN", "");
        let config = Config {
            nearby_endpoint: format!("{base}/nearby"),
            text_search_endpoint: format!("{base}/text"),
            ..Config::default()
        };
        router_with(&state, config).with_state(state)
    }

    #[tokio::test]
    async fn nearby_and_text_search_preserve_responses_and_keep_the_key_server_side() {
        let stub = google_stub(
            200,
            json!({ "places": [{
                "id": "p1",
                "displayName": { "text": "Ferry Building" },
                "formattedAddress": "1 Ferry Building",
                "location": { "latitude": 37.7955, "longitude": -122.3937 },
                "types": ["tourist_attraction"],
            }] }),
        )
        .await;
        let app = app(&stub.base, "fixture-google-server-key");
        let nearby = call(
            &app,
            Call::default_at("/api/google/nearby-places?lat=37.77493&lon=-122.41942&radiusM=99999"),
        )
        .await;
        assert_eq!(nearby.status, 200);
        assert_eq!(nearby.header("cache-control"), Some("private, max-age=300"));
        assert_eq!(
            nearby.header("content-type"),
            Some("application/json; charset=utf-8")
        );
        let body = nearby.json();
        assert_eq!(body["error"], Value::Null);
        assert_eq!(body["places"][0]["name"], "Ferry Building");
        assert!(!nearby.body.contains("fixture-google-server-key"));
        {
            let sent = stub.sent.lock();
            assert_eq!(sent[0].0, "fixture-google-server-key");
            assert_eq!(sent[0].1["locationRestriction"]["circle"]["radius"], 5000);
            assert_eq!(sent[0].1["rankPreference"], "DISTANCE");
        }

        let text = call(
            &app,
            Call::default_at("/api/google/text-search?q=%20Ferry%20&lat=37.77&lon=-122.41"),
        )
        .await;
        assert_eq!(text.status, 200);
        assert_eq!(text.json()["places"][0]["viewport"], Value::Null);
        let sent = stub.sent.lock();
        assert_eq!(sent[1].1["textQuery"], "Ferry");
        assert_eq!(sent[1].1["locationBias"]["circle"]["radius"], 4000);
        assert_eq!(sent[1].1["maxResultCount"], 5);
    }

    #[tokio::test]
    async fn bad_input_is_refused_before_the_limiter_and_google() {
        let stub = google_stub(200, json!({ "places": [] })).await;
        let app = app(&stub.base, "k");
        for (uri, error) in [
            (
                "/api/google/nearby-places?lat=&lon=1",
                "lat and lon are required",
            ),
            (
                "/api/google/nearby-places?lat=x&lon=1",
                "Valid lat and lon are required",
            ),
            (
                "/api/google/text-search?lat=1&lon=1",
                "q, lat and lon are required",
            ),
            (
                "/api/google/text-search?q=a&lat=100&lon=1",
                "lat must be within [-90, 90] and lon within [-180, 180]",
            ),
        ] {
            let reply = call(&app, Call::default_at(uri)).await;
            assert_eq!(reply.status, 400, "{uri}");
            assert_eq!(reply.json(), json!({ "error": error, "places": [] }));
        }
        let wrong = call(
            &app,
            Call {
                method: "POST",
                ..Call::default_at("/api/google/nearby-places")
            },
        )
        .await;
        assert_eq!(wrong.status, 405);
        assert_eq!(
            wrong.json(),
            json!({ "error": "Method not allowed", "places": [] })
        );
        let mut cross = Call::default_at("/api/google/text-search?q=a&lat=1&lon=1");
        cross.headers.push(("sec-fetch-site", "cross-site"));
        assert_eq!(call(&app, cross).await.status, 403);
        assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn the_limiter_keeps_the_places_contract_and_keyless_stays_free() {
        let stub = google_stub(200, json!({ "places": [] })).await;
        let app = app(&stub.base, "fixture-google-server-key");
        let url = "/api/google/nearby-places?lat=37.77493&lon=-122.41942";
        for _ in 0..GOOGLE_DEFAULT_PER_MIN {
            assert_eq!(call(&app, Call::default_at(url)).await.status, 200);
        }
        let over = call(&app, Call::default_at(url)).await;
        assert_eq!(over.status, 429);
        assert_eq!(over.header("retry-after"), Some("5"));
        assert_eq!(
            over.json(),
            json!({ "error": "Rate limit exceeded", "places": [] })
        );

        let keyless = self::app(&stub.base, "");
        let free = call(&keyless, Call::default_at(url)).await;
        assert_eq!(free.status, 200);
        assert_eq!(free.header("cache-control"), Some("no-store"));
        assert_eq!(
            free.json(),
            json!({ "configured": false, "error": null, "places": [] })
        );
    }

    #[tokio::test]
    async fn upstream_errors_relay_the_google_message_and_network_faults_502() {
        let stub = google_stub(403, json!({ "error": { "message": "Denied" } })).await;
        let app = app(&stub.base, "k");
        let denied = call(
            &app,
            Call::default_at("/api/google/nearby-places?lat=1&lon=1"),
        )
        .await;
        assert_eq!(denied.status, 403);
        assert_eq!(denied.json(), json!({ "places": [], "error": "Denied" }));

        let offline = self::app("http://127.0.0.1:9", "k");
        let failed = call(
            &offline,
            Call::default_at("/api/google/text-search?q=a&lat=1&lon=1"),
        )
        .await;
        assert_eq!(failed.status, 502);
        assert_eq!(
            failed.json(),
            json!({ "error": "fetch failed", "places": [] })
        );
    }
}
