//! Route-level tests against a loopback fake of every upstream.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::get;
use http::{Request, StatusCode};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::{Upstreams, router_with};
use crate::common::reply;
use crate::env::Env;
use crate::state::{AppState, Inner};

/// What the fake upstream answers: status, body and extra headers.
#[derive(Clone)]
struct Script {
    status: u16,
    body: String,
    headers: Vec<(&'static str, String)>,
}

impl Script {
    fn ok(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            headers: Vec::new(),
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            body: format!("upstream detail {status}"),
            headers: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct Fake {
    states: Arc<Mutex<Script>>,
    mil: Arc<Mutex<Script>>,
    hits: Arc<Mutex<Vec<String>>>,
    count: Arc<AtomicUsize>,
}

fn respond(script: Script) -> Response {
    let mut response = reply::raw(script.status, "application/json", script.body, None);
    for (name, value) in script.headers {
        response = reply::with_header(response, name, &value);
    }
    response
}

async fn fake_upstream() -> (String, Fake) {
    let fake = Fake {
        states: Arc::new(Mutex::new(Script::status(500))),
        mil: Arc::new(Mutex::new(Script::status(500))),
        hits: Arc::default(),
        count: Arc::default(),
    };
    fn hit(fake: &Fake, what: String) {
        fake.count.fetch_add(1, Ordering::SeqCst);
        fake.hits.lock().push(what);
    }
    let app = Router::new()
        .route(
            "/api/states/all",
            get(|State(f): State<Fake>, headers: http::HeaderMap| async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("none")
                    .to_string();
                hit(&f, format!("states {auth}"));
                respond(f.states.lock().clone())
            }),
        )
        .route(
            "/v2/lat/{lat}/lon/{lon}/dist/{dist}",
            get(
                |State(f): State<Fake>, Path((lat, lon, dist)): Path<(String, String, String)>| async move {
                    hit(&f, format!("point {lat} {lon} {dist}"));
                    respond(Script::ok(json!({
                        "now": 1_700_000_000_000i64,
                        "ac": [{ "hex": "abc123", "lat": 30.1, "lon": -97.7, "alt_baro": 1000 }],
                    })))
                },
            ),
        )
        .route(
            "/v2/mil",
            get(|State(f): State<Fake>| async move {
                hit(&f, "mil".into());
                respond(f.mil.lock().clone())
            }),
        )
        .route(
            "/api/tracks/all",
            get(|State(f): State<Fake>| async move {
                hit(&f, "tracks".into());
                respond(Script::status(404))
            }),
        )
        .route(
            "/data/traces/{xx}/{file}",
            get(
                |State(f): State<Fake>, Path((xx, file)): Path<(String, String)>| async move {
                    hit(&f, format!("trace {xx} {file}"));
                    respond(Script::ok(json!({ "icao": "ae1234", "trace": [] })))
                },
            ),
        )
        .route(
            "/v0/callsign/{cs}",
            get(|State(f): State<Fake>, Path(cs): Path<String>| async move {
                hit(&f, format!("callsign {cs}"));
                respond(Script::ok(json!({ "response": { "flightroute": {
                    "airline": { "name": "United" },
                    "origin": { "iata_code": "AUS", "municipality": "Austin", "latitude": 30.19, "longitude": -97.67 },
                    "destination": { "icao_code": "KDEN", "name": "Denver", "latitude": 39.86, "longitude": -104.67 },
                }}})))
            }),
        )
        .route(
            "/v0/aircraft/{hex}",
            get(|State(f): State<Fake>, Path(hex): Path<String>| async move {
                hit(&f, format!("aircraft {hex}"));
                respond(Script::status(404))
            }),
        )
        .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, fake)
}

fn app_state(env: &[(&str, &str)]) -> AppState {
    let dir = std::env::temp_dir().join(format!(
        "ce-aircraft-test-{}-{}",
        std::process::id(),
        crate::common::js::now_ms()
    ));
    let state = AppState(Arc::new(Inner {
        http: reqwest::Client::builder().no_proxy().build().unwrap(),
        env: Env::default(),
        root: dir.clone(),
        dist: dir.clone(),
        cache_dir: dir,
    }));
    for (name, value) in env {
        state.env.set(name, value);
    }
    state
}

async fn setup(env: &[(&str, &str)]) -> (Router, Fake) {
    let (base, fake) = fake_upstream().await;
    let state = app_state(env);
    let upstreams = Upstreams {
        opensky_states: format!("{base}/api/states/all?extended=1"),
        opensky_token: format!("{base}/token"),
        opensky_api: base.clone(),
        adsblol_api: base.clone(),
        adsblol_mil: format!("{base}/v2/mil"),
        adsblol_site: base.clone(),
        adsbdb: base,
    };
    (router_with(&state, upstreams).with_state(state), fake)
}

struct Reply {
    status: StatusCode,
    headers: http::HeaderMap,
    body: String,
}

impl Reply {
    fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }
}

async fn call(app: &Router, uri: &str) -> Reply {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    Reply {
        status,
        headers,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

fn opensky_body() -> Value {
    json!({ "time": crate::common::js::now_ms() / 1000, "states": [["abc123"]] })
}

#[tokio::test]
async fn flights_cache_a_fresh_anonymous_snapshot() {
    let (app, fake) = setup(&[("OPENSKY_AUTH_MODE", "anon")]).await;
    let mut script = Script::ok(opensky_body());
    script
        .headers
        .push(("x-rate-limit-remaining", "3000".into()));
    *fake.states.lock() = script;
    let first = call(&app, "/api/flights").await;
    assert_eq!(first.status, 200);
    assert_eq!(first.header("x-feed-source"), "OpenSky Network");
    assert_eq!(first.header("x-opensky-cache"), "MISS");
    assert_eq!(first.header("x-opensky-auth"), "anon");
    assert_eq!(first.header("x-opensky-auth-mode-requested"), "anon");
    assert_eq!(first.header("x-opensky-auth-reason"), "anonymous_ok");
    assert_eq!(first.header("cache-control"), "no-store");
    assert_eq!(first.json()["states"], json!([["abc123"]]));
    let second = call(&app, "/api/flights/").await;
    assert_eq!(second.header("x-opensky-cache"), "HIT");
    assert_eq!(second.header("x-opensky-auth-reason"), "anonymous_ok");
    assert_eq!(fake.count.load(Ordering::SeqCst), 1);
    // A sub-path is not this route.
    assert_eq!(call(&app, "/api/flights/other").await.status, 404);
}

#[tokio::test]
async fn flights_rate_limit_falls_back_to_the_regional_snapshot_then_cools_down() {
    let (app, fake) = setup(&[("OPENSKY_AUTH_MODE", "anon")]).await;
    let mut script = Script::status(429);
    script
        .headers
        .push(("x-rate-limit-retry-after-seconds", "90".into()));
    *fake.states.lock() = script;
    let fallback = call(&app, "/api/flights?lat=30.13&lon=-97.6").await;
    assert_eq!(fallback.status, 200);
    assert_eq!(fallback.header("x-flight-source"), "adsb.lol");
    assert_eq!(
        fallback.header("x-flight-coverage"),
        "250nm regional fallback"
    );
    assert_eq!(fallback.header("x-flight-count"), "1");
    assert_eq!(fallback.header("x-opensky-auth"), "adsblol-regional");
    assert_eq!(
        fallback.header("x-opensky-auth-reason"),
        "opensky_http_429_regional_fallback"
    );
    assert_eq!(fallback.header("x-feed-source"), "OpenSky Network");
    let body = fallback.json();
    assert_eq!(body["time"], json!(1_700_000_000));
    assert_eq!(body["states"][0][0], "abc123");
    assert!(
        fake.hits
            .lock()
            .contains(&"point 30.25 -97.5 250".to_string())
    );

    // Inside the cooldown, the fallback is served from its cache.
    let cached = call(&app, "/api/flights?lat=30.2&lon=-97.55").await;
    assert_eq!(cached.header("x-opensky-cache"), "HIT");
    assert_eq!(
        cached.header("x-opensky-auth-reason"),
        "opensky_cooldown_regional_fallback"
    );
    // Without an anchor the cooldown is answered locally.
    let cooling = call(&app, "/api/flights").await;
    assert_eq!(cooling.status, 429);
    assert_eq!(cooling.header("x-opensky-cache"), "COOLDOWN");
    assert_eq!(cooling.header("x-opensky-auth"), "none");
    assert_eq!(cooling.header("x-opensky-retry-after-seconds"), "90");
    assert_eq!(
        cooling.json(),
        json!({ "error": "OpenSky rate limited; proxy cooling down." })
    );
    let states_calls = fake
        .hits
        .lock()
        .iter()
        .filter(|h| h.starts_with("states"))
        .count();
    assert_eq!(states_calls, 1, "no upstream attempt during the cooldown");
}

#[tokio::test]
async fn flights_basic_mode_without_credentials_explains_the_401() {
    let (app, fake) = setup(&[
        ("OPENSKY_AUTH_MODE", " BASIC "),
        ("OPENSKY_USERNAME", ""),
        ("OPENSKY_PASSWORD", ""),
    ])
    .await;
    *fake.states.lock() = Script::status(401);
    let reply = call(&app, "/api/flights").await;
    assert_eq!(reply.status, 401);
    assert_eq!(reply.header("x-opensky-auth-mode-requested"), "basic");
    assert_eq!(reply.header("x-opensky-auth-reason"), "missing_basic_creds");
    assert_eq!(
        reply.json(),
        json!({ "error": "OpenSky auth missing. Basic mode requires OPENSKY_USERNAME and OPENSKY_PASSWORD." })
    );
}

#[tokio::test]
async fn flights_basic_mode_sends_credentials() {
    let (app, fake) = setup(&[
        ("OPENSKY_AUTH_MODE", "basic"),
        ("OPENSKY_USERNAME", "user"),
        ("OPENSKY_PASSWORD", "pass"),
    ])
    .await;
    *fake.states.lock() = Script::ok(opensky_body());
    let reply = call(&app, "/api/flights").await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("x-opensky-auth-reason"), "basic_ok");
    assert_eq!(fake.hits.lock()[0], "states Basic dXNlcjpwYXNz");
}

#[tokio::test]
async fn flights_proxy_error_without_cache_or_anchor_is_502() {
    let (app, _fake) = setup(&[("OPENSKY_AUTH_MODE", "anon")]).await;
    let state = app_state(&[("OPENSKY_AUTH_MODE", "anon")]);
    let unreachable = Upstreams {
        opensky_states: "http://127.0.0.1:1/api/states/all".into(),
        ..Upstreams::default()
    };
    let broken = router_with(&state, unreachable).with_state(state);
    let reply = call(&broken, "/api/flights").await;
    assert_eq!(reply.status, 502);
    assert_eq!(reply.header("x-opensky-auth"), "error");
    assert_eq!(reply.header("x-opensky-auth-reason"), "proxy_error");
    assert_eq!(reply.json(), json!({ "error": "OpenSky proxy error" }));
    drop(app);
}

#[tokio::test]
async fn military_caches_and_serves_stale_through_a_rate_limit() {
    let (app, fake) = setup(&[]).await;
    *fake.mil.lock() = Script::ok(json!({ "ac": [] }));
    let first = call(&app, "/api/military").await;
    assert_eq!(first.status, 200);
    assert_eq!(first.header("x-feed-source"), "adsb.lol");
    assert_eq!(first.header("x-feed-cache"), "MISS");
    assert_eq!(first.header("cache-control"), "no-store");
    let second = call(&app, "/api/military/").await;
    assert_eq!(second.header("x-feed-cache"), "HIT");
    assert!(!second.header("x-feed-age-ms").is_empty());
    assert_eq!(fake.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn military_relays_a_failure_with_nothing_cached_then_cools_down() {
    let (app, fake) = setup(&[]).await;
    let mut script = Script::status(429);
    script.headers.push(("retry-after", "20".into()));
    *fake.mil.lock() = script;
    let first = call(&app, "/api/military").await;
    assert_eq!(first.status, 429);
    assert_eq!(first.header("x-feed-cache"), "MISS");
    assert_eq!(first.header("retry-after"), "20");
    assert_eq!(first.body, "upstream detail 429");
    let cooling = call(&app, "/api/military").await;
    assert_eq!(cooling.status, 429);
    assert_eq!(cooling.header("x-feed-cache"), "NONE");
    assert_eq!(
        cooling.json(),
        json!({ "error": "adsb.lol upstream cooling down" })
    );
    assert_eq!(fake.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn track_backfill_validates_relays_and_caches() {
    let (app, fake) = setup(&[]).await;
    let bad = call(&app, "/api/flights/track?icao24=xyz").await;
    assert_eq!(bad.status, 400);
    assert_eq!(bad.header("content-type"), "application/json");
    assert_eq!(
        bad.json(),
        json!({ "error": "icao24 must be a 6-char hex string" })
    );
    let missing = call(&app, "/api/flights/track?icao24=ABC123").await;
    assert_eq!(missing.status, 404);
    assert_eq!(
        missing.header("content-type"),
        "application/json; charset=utf-8"
    );
    assert_eq!(missing.json(), json!({ "error": "Track source HTTP 404" }));
    let again = call(&app, "/api/flights/track?icao24=abc123").await;
    assert_eq!(again.status, 404);
    assert_eq!(fake.count.load(Ordering::SeqCst), 1, "cached for 60 s");

    let bad_hex = call(&app, "/api/military/track?hex=12").await;
    assert_eq!(bad_hex.status, 400);
    assert_eq!(
        bad_hex.json(),
        json!({ "error": "hex must be a 6-7 char hex string" })
    );
    let trace = call(&app, "/api/military/track?hex=~AE1234").await;
    assert_eq!(trace.status, 200);
    assert_eq!(trace.json()["icao"], "ae1234");
    assert!(
        fake.hits
            .lock()
            .contains(&"trace 34 trace_full_~ae1234.json".to_string())
    );
}

#[tokio::test]
async fn adsbdb_keeps_validation_and_missing_aircraft_semantics() {
    let (app, fake) = setup(&[]).await;
    assert_eq!(call(&app, "/api/adsbdb/route/!").await.status, 400);
    let invalid = call(&app, "/api/adsbdb/type/nope").await;
    assert_eq!(invalid.status, 400);
    assert_eq!(invalid.json(), json!({ "error": "invalid hex" }));
    let unknown = call(&app, "/api/adsbdb/unknown").await;
    assert_eq!(unknown.status, 404);
    assert_eq!(unknown.json(), json!({ "error": "unknown endpoint" }));
    assert_eq!(call(&app, "/api/adsbdb").await.status, 404);
    let absent = call(&app, "/api/adsbdb/type/ABCDEF").await;
    assert_eq!(absent.status, 200);
    assert_eq!(absent.json(), json!({ "found": false }));
    let again = call(&app, "/api/adsbdb/type/abcdef").await;
    assert_eq!(again.json(), json!({ "found": false }));
    let route = call(&app, "/api/adsbdb/route/ual123").await;
    assert_eq!(
        route.json(),
        json!({
            "found": true,
            "airline": "United",
            "origin": { "code": "AUS", "name": "Austin", "lat": 30.19, "lon": -97.67 },
            "destination": { "code": "KDEN", "name": "Denver", "lat": 39.86, "lon": -104.67 },
        })
    );
    assert_eq!(
        *fake.hits.lock(),
        vec!["aircraft abcdef", "callsign UAL123"],
        "a 404 is negative-cached"
    );
}
