//! Port of `src/data/windProxy.test.mjs`, plus shared test helpers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

use bytes::Bytes;
use chrono::NaiveDate;
use http::{HeaderMap, Method, Uri};
use parking_lot::Mutex;
use serde_json::Value;

use super::body::tests::{FakeFetch, response};
use super::body::{DecodeFn, Fetch};
use super::grid::{Decoded, Grid, Overlay};
use super::*;

pub fn utc_ms(y: i32, m: u32, d: u32, h: u32) -> i64 {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

pub fn deps_with(fetch: Arc<dyn Fetch>, now_ms: i64, decode: DecodeFn, target_dx: f64) -> Deps {
    Deps {
        fetch,
        now: Arc::new(move || now_ms),
        decode,
        target_dx,
    }
}

const IDX: &str = "1:0:d=2026091400:UGRD:10 m above ground:anl:\n\
2:5:d=2026091400:VGRD:10 m above ground:anl:\n\
3:10:d=2026091400:HGT:surface:anl:";

fn decoded(values: impl IntoIterator<Item = u8>) -> Decoded {
    Decoded {
        ni: 4,
        nj: 3,
        lo1: 0.0,
        la1: 90.0,
        di: 90.0,
        dj: 90.0,
        values: values.into_iter().map(f64::from).collect(),
        units: None,
    }
}

fn decoded_u() -> Decoded {
    decoded(1..=12)
}

fn decoded_v() -> Decoded {
    decoded(21..=32)
}

/// Fetch double: `.idx` text, and range bodies of the requested length.
fn make_fetch(fail: Arc<AtomicBool>, counter: Arc<AtomicUsize>) -> Arc<dyn Fetch> {
    FakeFetch::new(move |url, range| {
        counter.fetch_add(1, Ordering::SeqCst);
        if fail.load(Ordering::SeqCst) {
            return Err(WindError::new("gfs upstream down"));
        }
        if url.ends_with(".idx") {
            return Ok(response(200, IDX));
        }
        let (start, end) = range
            .and_then(|r| r.strip_prefix("bytes="))
            .and_then(|r| r.split_once('-'))
            .map(|(a, b)| (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap()))
            .unwrap();
        Ok(response(206, vec![0u8; end - start + 1]))
    })
}

fn decode_always(value: Decoded) -> DecodeFn {
    Arc::new(move |_| {
        let value = value.clone();
        Box::pin(async move { Ok(value) })
    })
}

/// Test configuration: a settable clock, counters and injectable parts.
struct Setup {
    clock: Arc<AtomicI64>,
    fail: Arc<AtomicBool>,
    fetch_calls: Arc<AtomicUsize>,
    decode: DecodeFn,
    models: Option<HashMap<&'static str, ModelFn>>,
}

impl Setup {
    fn new() -> Self {
        Self {
            clock: Arc::new(AtomicI64::new(utc_ms(2026, 9, 14, 12))),
            fail: Arc::new(AtomicBool::new(false)),
            fetch_calls: Arc::new(AtomicUsize::new(0)),
            decode: decode_always(decoded_u()),
            models: None,
        }
    }

    fn models(mut self, models: Vec<(&'static str, ModelFn)>) -> Self {
        self.models = Some(models.into_iter().collect());
        self
    }

    fn build(&self) -> Arc<WindProxy> {
        let production = Options::production();
        let clock = Arc::clone(&self.clock);
        WindProxy::new(Options {
            deps: Deps {
                fetch: make_fetch(Arc::clone(&self.fail), Arc::clone(&self.fetch_calls)),
                now: Arc::new(move || clock.load(Ordering::SeqCst)),
                decode: self.decode.clone(),
                target_dx: 90.0,
            },
            models: self.models.clone().unwrap_or(production.models),
            ..production
        })
    }

    fn advance(&self, ms: i64) {
        self.clock.fetch_add(ms, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct Res {
    status: u16,
    headers: HeaderMap,
    body: Bytes,
}

impl Res {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }

    fn floats(&self) -> Vec<f32> {
        self.body
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect()
    }
}

fn uri(url: &str) -> Uri {
    let full = if url.starts_with("/api/wind") {
        url.to_string()
    } else {
        format!("/api/wind{url}")
    };
    full.parse().unwrap()
}

async fn request_with(proxy: &Arc<WindProxy>, url: &str, method: Method) -> Res {
    let response = Arc::clone(proxy).handle(method, uri(url)).await;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    Res {
        status,
        headers,
        body,
    }
}

async fn request(proxy: &Arc<WindProxy>, url: &str) -> Res {
    request_with(proxy, url, Method::GET).await
}

fn grid_of(u: Vec<f32>, v: Vec<f32>, scalar: Option<Vec<f32>>) -> Grid {
    Grid {
        nx: 4,
        ny: 3,
        lo1: 0.0,
        la1: 90.0,
        dx: 90.0,
        dy: 90.0,
        u,
        v,
        scalar,
    }
}

fn simple_model(hour: u32, forecast_hour: Option<u32>, u: Vec<f32>) -> ModelValue {
    ModelValue {
        cycle: Cycle {
            date: "20260914".into(),
            hour,
            forecast_hour,
            run_iso: None,
            valid_iso: None,
        },
        level: "x".into(),
        units: "m/s".into(),
        grid: grid_of(u, vec![0.0; 12], None),
        scalar: None,
        scalar_error: None,
    }
}

fn model_fn(
    f: impl Fn(Overlay) -> Result<ModelValue, WindError> + Send + Sync + 'static,
) -> ModelFn {
    Arc::new(move |_, overlay| {
        let result = f(overlay);
        Box::pin(async move { result })
    })
}

fn weather_model(overlay: Overlay, step: u32) -> ModelValue {
    let scalar = grid::weather_scalar_metadata(overlay);
    let fill = if overlay == Overlay::Temperature {
        20.0
    } else {
        1013.25
    };
    ModelValue {
        cycle: Cycle {
            date: "20260914".into(),
            hour: 6,
            forecast_hour: Some(step),
            run_iso: None,
            valid_iso: None,
        },
        level: "10 m above ground".into(),
        units: "m/s".into(),
        grid: grid_of(
            vec![1.0; 12],
            vec![2.0; 12],
            scalar.as_ref().map(|_| vec![fill; 12]),
        ),
        scalar,
        scalar_error: None,
    }
}

#[tokio::test]
async fn manifest_describes_the_gfs_cycle_and_resampled_grid() {
    let proxy = Setup::new().build();
    let res = request(&proxy, "/").await;
    assert_eq!(res.status, 200);
    assert_eq!(res.headers["content-type"], "application/json");
    assert_eq!(res.headers["cache-control"], "no-store");
    let body = res.json();
    assert_eq!(body["schemaVersion"], 1);
    assert_eq!(body["model"], "gfs");
    assert_eq!(body["cycle"]["hour"], 6);
    assert_eq!(body["cycle"]["date"], "20260914");
    assert_eq!(body["units"], "m/s");
    assert_eq!(
        body["grid"],
        serde_json::json!({ "nx": 4, "ny": 3, "lo1": 0, "la1": 90, "dx": 90, "dy": 90 })
    );
    assert_eq!(
        body["gridUrl"],
        "/api/wind/grid/gfs-20260914-6-f6-90.bin?model=gfs"
    );
    assert_eq!(body["stale"], false);
    // Key order and number formatting match `JSON.stringify`.
    let text = std::str::from_utf8(&res.body).unwrap();
    assert!(text.starts_with("{\"schemaVersion\":1,\"model\":\"gfs\",\"cycle\":{\"date\":\"20260914\",\"hour\":6,\"forecastHour\":6,\"runIso\":\"2026-09-14T06:00:00.000Z\",\"validIso\":\"2026-09-14T12:00:00.000Z\"},\"fetchedAt\":"));
    assert!(text.ends_with(",\"level\":\"10 m above ground\",\"units\":\"m/s\",\"grid\":{\"nx\":4,\"ny\":3,\"lo1\":0,\"la1\":90,\"dx\":90,\"dy\":90},\"stale\":false,\"unavailable\":false,\"reason\":null,\"gridUrl\":\"/api/wind/grid/gfs-20260914-6-f6-90.bin?model=gfs\"}"));
}

#[tokio::test]
async fn grid_route_returns_float32_u_then_v() {
    let mut setup = Setup::new();
    let count = Arc::new(AtomicUsize::new(0));
    setup.decode = Arc::new(move |_| {
        let value = if count.fetch_add(1, Ordering::SeqCst) == 0 {
            decoded_u()
        } else {
            decoded_v()
        };
        Box::pin(async move { Ok(value) })
    });
    let proxy = setup.build();
    let manifest = request(&proxy, "/").await.json();
    let res = request(&proxy, manifest["gridUrl"].as_str().unwrap()).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.headers["content-type"], "application/octet-stream");
    assert_eq!(
        res.headers["cache-control"],
        "public, max-age=3600, immutable"
    );
    assert_eq!(res.body.len(), 4 * 3 * 4 * 2);
    let floats = res.floats();
    let expect = |d: Decoded| d.values.iter().map(|v| *v as f32).collect::<Vec<_>>();
    assert_eq!(floats[..12], expect(decoded_u())[..]);
    assert_eq!(floats[12..], expect(decoded_v())[..]);
}

#[tokio::test]
async fn status_omits_the_grid_url() {
    let proxy = Setup::new().build();
    request(&proxy, "/").await;
    let body = request(&proxy, "/status").await.json();
    assert!(body.get("gridUrl").is_none());
    assert_eq!(body["model"], "gfs");
}

#[tokio::test]
async fn refresh_is_cached_within_the_ttl_and_refetches_after_it() {
    let setup = Setup::new();
    let proxy = setup.build();
    request(&proxy, "/").await;
    let after_first = setup.fetch_calls.load(Ordering::SeqCst);
    assert_eq!(after_first, 3, "one idx plus two range fetches");
    request(&proxy, "/").await;
    assert_eq!(setup.fetch_calls.load(Ordering::SeqCst), after_first);
    setup.advance(2 * 3_600_000);
    request(&proxy, "/").await;
    assert_eq!(setup.fetch_calls.load(Ordering::SeqCst), after_first + 3);
}

#[tokio::test]
async fn serves_last_good_with_stale_on_upstream_failure() {
    let setup = Setup::new();
    let proxy = setup.build();
    let good = request(&proxy, "/").await.json();
    setup.fail.store(true, Ordering::SeqCst);
    setup.advance(2 * 3_600_000);
    let degraded = request(&proxy, "/").await.json();
    assert_eq!(degraded["stale"], true);
    assert_eq!(degraded["reason"], "Wind upstream unavailable");
    let frame = request(&proxy, good["gridUrl"].as_str().unwrap()).await;
    assert_eq!(frame.status, 200, "last-good grid stays addressable");
}

#[tokio::test]
async fn returns_a_json_404_for_an_unknown_grid() {
    let proxy = Setup::new().build();
    request(&proxy, "/").await;
    let res = request(&proxy, "/grid/nope.bin").await;
    assert_eq!(res.status, 404);
    assert_eq!(res.json(), serde_json::json!({ "error": "unknown_grid" }));
}

#[tokio::test]
async fn grid_ids_and_urls_are_model_scoped() {
    let proxy = Setup::new()
        .models(vec![
            (
                "gfs",
                model_fn(|_| Ok(simple_model(6, None, vec![0.0; 12]))),
            ),
            (
                "ifs",
                model_fn(|_| Ok(simple_model(0, None, vec![0.0; 12]))),
            ),
        ])
        .build();
    let gfs = request(&proxy, "/").await.json();
    let ifs = request(&proxy, "/?model=ifs").await.json();
    let gfs_url = gfs["gridUrl"].as_str().unwrap();
    let ifs_url = ifs["gridUrl"].as_str().unwrap();
    assert!(gfs_url.ends_with("gfs-20260914-6-f0-90.bin?model=gfs"));
    assert!(ifs_url.ends_with("ifs-20260914-0-f0-90.bin?model=ifs"));
    assert_eq!(request(&proxy, gfs_url).await.status, 200);
    assert_eq!(request(&proxy, ifs_url).await.status, 200);
    // A grid id resolves only through its own model parameter.
    assert_eq!(
        request(&proxy, &gfs_url.replace("model=gfs", "model=ifs"))
            .await
            .status,
        404
    );
}

#[tokio::test]
async fn rejects_a_non_finite_grid_instead_of_caching_it() {
    let mut bad = vec![0.0; 12];
    bad[3] = f32::INFINITY;
    let proxy = Setup::new()
        .models(vec![(
            "gfs",
            model_fn(move |_| Ok(simple_model(6, None, bad.clone()))),
        )])
        .build();
    let body = request(&proxy, "/").await.json();
    assert_eq!(body["unavailable"], true);
    assert_eq!(body["reason"], "Wind upstream unavailable");
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        "{\"model\":\"gfs\",\"schemaVersion\":1,\"unavailable\":true,\"stale\":true,\"reason\":\"Wind upstream unavailable\"}"
    );
}

#[tokio::test]
async fn rejects_unknown_models_paths_and_methods_before_acquisition() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let proxy = Setup::new()
        .models(vec![(
            "gfs",
            model_fn(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(WindError::new(""))
            }),
        )])
        .build();
    let res = request(&proxy, "/?model=toString").await;
    assert_eq!(res.status, 400);
    assert_eq!(res.json(), serde_json::json!({ "error": "unknown_model" }));
    // A known model name without a configured loader is also unknown.
    assert_eq!(request(&proxy, "/?model=ifs").await.status, 400);
    let res = request(&proxy, "/secret").await;
    assert_eq!(res.status, 404);
    assert_eq!(res.json(), serde_json::json!({ "error": "not_found" }));
    let res = request_with(&proxy, "/manifest", Method::POST).await;
    assert_eq!(res.status, 405);
    assert_eq!(
        res.json(),
        serde_json::json!({ "error": "method_not_allowed" })
    );
    assert_eq!(
        request_with(&proxy, "/manifest", Method::HEAD).await.status,
        405
    );
    assert_eq!(request(&proxy, "/grid/unknown.bin").await.status, 404);
    assert_eq!(request(&proxy, "/grid/unknown").await.status, 404);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn caches_forecast_steps_separately_and_keeps_a_prior_grid_readable() {
    let step = Arc::new(AtomicUsize::new(6));
    let current = Arc::clone(&step);
    let setup = Setup::new().models(vec![(
        "gfs",
        model_fn(move |_| {
            let step = current.load(Ordering::SeqCst) as u32;
            Ok(simple_model(6, Some(step), vec![step as f32; 12]))
        }),
    )]);
    let proxy = setup.build();
    let first = request(&proxy, "/").await.json();
    setup.advance(3_600_000);
    step.store(7, Ordering::SeqCst);
    let second = request(&proxy, "/").await.json();
    assert_ne!(first["gridUrl"], second["gridUrl"]);
    let old = request(&proxy, first["gridUrl"].as_str().unwrap()).await;
    assert_eq!(old.status, 200);
    assert_eq!(old.floats()[0], 6.0);
}

#[tokio::test]
async fn failure_backoff_prevents_repeated_upstream_acquisition() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let proxy = Setup::new()
        .models(vec![(
            "gfs",
            model_fn(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(WindError::new("private upstream detail"))
            }),
        )])
        .build();
    let (a, b) = tokio::join!(request(&proxy, "/"), request(&proxy, "/"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(a.json()["reason"], "Wind upstream unavailable");
    assert_eq!(b.json(), a.json());
    request(&proxy, "/").await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Sets its flag when dropped: a dropped model future is an aborted signal.
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

type Pending =
    Arc<Mutex<HashMap<&'static str, (Arc<AtomicBool>, tokio::sync::oneshot::Sender<ModelValue>)>>>;

/// A model that waits for the test to resolve it, per overlay.
fn pending_model(loads: Arc<AtomicUsize>, pending: Pending) -> ModelFn {
    Arc::new(move |_, overlay| {
        loads.fetch_add(1, Ordering::SeqCst);
        let aborted = Arc::new(AtomicBool::new(false));
        let (tx, rx) = tokio::sync::oneshot::channel();
        pending
            .lock()
            .insert(overlay.as_str(), (Arc::clone(&aborted), tx));
        let guard = DropFlag(aborted);
        Box::pin(async move {
            let _guard = guard;
            rx.await.map_err(|_| WindError::new("never resolved"))
        })
    })
}

/// Let spawned handlers and refresh tasks run until `done` holds.
async fn settle(done: impl Fn() -> bool) {
    for _ in 0..1000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held");
}

fn waiters(proxy: &Arc<WindProxy>, key: &str) -> usize {
    proxy
        .store
        .lock()
        .loadings
        .get(key)
        .map_or(0, |op| op.control.waiters.load(Ordering::SeqCst))
}

#[tokio::test]
async fn shared_work_survives_one_disconnect_and_aborts_when_the_last_client_leaves() {
    let loads = Arc::new(AtomicUsize::new(0));
    let pending: Pending = Arc::default();
    let proxy = Setup::new()
        .models(vec![(
            "gfs",
            pending_model(Arc::clone(&loads), Arc::clone(&pending)),
        )])
        .build();
    let first = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        async move { request(&proxy, "/manifest").await }
    });
    let second = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        async move { request(&proxy, "/manifest").await }
    });
    settle(|| waiters(&proxy, "gfs:none") == 2 && pending.lock().contains_key("none")).await;
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    let aborted = Arc::clone(&pending.lock()["none"].0);
    first.abort();
    settle(|| waiters(&proxy, "gfs:none") == 1).await;
    assert!(!aborted.load(Ordering::SeqCst));
    second.abort();
    settle(|| aborted.load(Ordering::SeqCst)).await;
    settle(|| proxy.store.lock().loadings.is_empty()).await;
    // An abandoned acquisition does not start the failure backoff.
    assert!(proxy.store.lock().attempts.is_empty());
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(second.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn disconnecting_an_overlay_request_does_not_abort_a_different_variant() {
    let loads = Arc::new(AtomicUsize::new(0));
    let pending: Pending = Arc::default();
    let proxy = Setup::new()
        .models(vec![("gfs", pending_model(loads, Arc::clone(&pending)))])
        .build();
    let first = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        async move { request(&proxy, "/manifest?overlay=temperature").await }
    });
    let second = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        async move { request(&proxy, "/manifest?overlay=pressure").await }
    });
    settle(|| {
        let pending = pending.lock();
        pending.contains_key("temperature") && pending.contains_key("pressure")
    })
    .await;
    settle(|| waiters(&proxy, "gfs:temperature") == 1 && waiters(&proxy, "gfs:pressure") == 1)
        .await;
    first.abort();
    let temperature = Arc::clone(&pending.lock()["temperature"].0);
    settle(|| temperature.load(Ordering::SeqCst)).await;
    let (pressure_aborted, tx) = pending.lock().remove("pressure").unwrap();
    assert!(!pressure_aborted.load(Ordering::SeqCst));
    tx.send(weather_model(Overlay::Pressure, 6)).ok().unwrap();
    let res = second.await.unwrap();
    assert_eq!(res.json()["scalar"]["kind"], "pressure");
}

#[tokio::test]
async fn refresh_times_out_into_the_unavailable_manifest() {
    let loads = Arc::new(AtomicUsize::new(0));
    let pending: Pending = Arc::default();
    let setup = Setup::new().models(vec![("gfs", pending_model(loads, Arc::clone(&pending)))]);
    let production = Options::production();
    let clock = Arc::clone(&setup.clock);
    let proxy = WindProxy::new(Options {
        deps: Deps {
            now: Arc::new(move || clock.load(Ordering::SeqCst)),
            ..production.deps.clone()
        },
        timeout: Duration::from_millis(20),
        models: setup.models.clone().unwrap(),
        ..production
    });
    let body = request(&proxy, "/").await.json();
    assert_eq!(body["unavailable"], true);
    assert!(pending.lock()["none"].0.load(Ordering::SeqCst));
    // A timeout with a waiting client keeps the retry backoff.
    assert!(proxy.store.lock().attempts.contains_key("gfs:none"));
}

#[tokio::test]
async fn weather_variants_cache_independently_bind_urls_and_retain_two_generations() {
    let step = Arc::new(AtomicUsize::new(6));
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let model = |name: &'static str| -> ModelFn {
        let step = Arc::clone(&step);
        let calls = Arc::clone(&calls);
        model_fn(move |overlay| {
            calls.lock().push(format!("{name}:{}", overlay.as_str()));
            Ok(weather_model(overlay, step.load(Ordering::SeqCst) as u32))
        })
    };
    let setup = Setup::new().models(vec![("gfs", model("gfs")), ("ifs", model("ifs"))]);
    let proxy = setup.build();
    let mut slots = Vec::new();
    for name in ["gfs", "ifs"] {
        for overlay in [Overlay::None, Overlay::Temperature, Overlay::Pressure] {
            let query = format!("?model={name}&overlay={}", overlay.as_str());
            let url = format!("/manifest{query}");
            let (a, b) = tokio::join!(request(&proxy, &url), request(&proxy, &url));
            let manifest = a.json();
            assert_eq!(b.json(), manifest);
            if overlay == Overlay::None {
                assert!(manifest.get("scalar").is_none());
                assert!(manifest.get("overlay").is_none());
            } else {
                assert_eq!(manifest["scalar"]["kind"], overlay.as_str());
            }
            let grid_url = manifest["gridUrl"].as_str().unwrap().to_string();
            let binary = request(&proxy, &grid_url).await;
            let floats_per_point = if overlay == Overlay::None { 2 } else { 3 };
            assert_eq!(binary.body.len(), 12 * 4 * floats_per_point);
            let floats = binary.floats();
            assert_eq!(floats[..12], [1.0; 12]);
            assert_eq!(floats[12..24], [2.0; 12]);
            if overlay != Overlay::None {
                let fill = if overlay == Overlay::Temperature {
                    20.0
                } else {
                    1013.25
                };
                assert_eq!(floats[24..], [fill; 12]);
                let unbound = grid_url.replace(&format!("&overlay={}", overlay.as_str()), "");
                assert_eq!(request(&proxy, &unbound).await.status, 404);
            }
            slots.push((url, grid_url));
        }
    }
    assert_eq!(calls.lock().len(), 6, "one shared acquisition per variant");
    for (url, _) in &slots {
        request(&proxy, url).await;
    }
    assert_eq!(calls.lock().len(), 6);
    setup.advance(3_600_000);
    step.store(7, Ordering::SeqCst);
    let mut seconds = Vec::new();
    for (url, _) in &slots {
        seconds.push(
            request(&proxy, url).await.json()["gridUrl"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    for (_, first) in &slots {
        assert_eq!(request(&proxy, first).await.status, 200);
    }
    setup.advance(3_600_000);
    step.store(8, Ordering::SeqCst);
    for ((url, first), second) in slots.iter().zip(&seconds) {
        request(&proxy, url).await;
        assert_eq!(request(&proxy, first).await.status, 404);
        assert_eq!(request(&proxy, second).await.status, 200);
    }
}

#[tokio::test]
async fn overlay_failures_retain_only_their_own_last_good_and_unknown_overlays_never_acquire() {
    let failing = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let (f, c) = (Arc::clone(&failing), Arc::clone(&calls));
    let setup = Setup::new().models(vec![(
        "gfs",
        model_fn(move |overlay| {
            c.fetch_add(1, Ordering::SeqCst);
            if f.load(Ordering::SeqCst) && overlay != Overlay::None {
                return Err(WindError::new("private detail"));
            }
            Ok(weather_model(overlay, 6))
        }),
    )]);
    let proxy = setup.build();
    let first = request(&proxy, "/manifest?overlay=temperature")
        .await
        .json();
    request(&proxy, "/manifest").await;
    failing.store(true, Ordering::SeqCst);
    setup.advance(3_600_000);
    let stale = request(&proxy, "/manifest?overlay=temperature")
        .await
        .json();
    assert_eq!(stale["stale"], true);
    assert_eq!(stale["gridUrl"], first["gridUrl"]);
    assert_eq!(stale["scalar"]["kind"], "temperature");
    // The stale flag rewrites values in place without reordering keys.
    let keys: Vec<_> = stale.as_object().unwrap().keys().cloned().collect();
    let first_keys: Vec<_> = first.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, first_keys);
    assert_eq!(
        request(&proxy, "/manifest?overlay=pressure").await.json()["unavailable"],
        true
    );
    assert_eq!(request(&proxy, "/manifest").await.json()["stale"], false);
    let before = calls.load(Ordering::SeqCst);
    for overlay in ["toString", "humidity", "temperature,pressure"] {
        let res = request(&proxy, &format!("/manifest?overlay={overlay}")).await;
        assert_eq!(res.status, 400);
        assert_eq!(
            res.json(),
            serde_json::json!({ "error": "unknown_overlay" })
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn rejects_malformed_scalar_metadata_lengths_and_non_finite_values() {
    let mutations: Vec<fn(&mut ModelValue)> = vec![
        |value| value.scalar.as_mut().unwrap().units = "K".into(),
        |value| value.grid.scalar = Some(vec![0.0; 11]),
        |value| value.grid.scalar.as_mut().unwrap()[3] = f32::INFINITY,
        |value| value.grid.v = vec![0.0; 1],
        |value| value.scalar_error = Some("Temperature field unavailable".into()),
        |value| value.scalar = None,
    ];
    for mutate in mutations {
        let proxy = Setup::new()
            .models(vec![(
                "gfs",
                model_fn(move |overlay| {
                    let mut value = weather_model(overlay, 6);
                    mutate(&mut value);
                    Ok(value)
                }),
            )])
            .build();
        let manifest = request(&proxy, "/manifest?overlay=temperature")
            .await
            .json();
        assert_eq!(manifest["unavailable"], true);
        assert!(manifest.get("gridUrl").is_none());
    }
    // Wind-only results may not carry scalar data.
    let proxy = Setup::new()
        .models(vec![(
            "gfs",
            model_fn(|_| {
                let mut value = weather_model(Overlay::None, 6);
                value.grid.scalar = Some(vec![0.0; 12]);
                Ok(value)
            }),
        )])
        .build();
    assert_eq!(
        request(&proxy, "/manifest").await.json()["unavailable"],
        true
    );
}

#[tokio::test]
async fn optional_scalar_failure_serves_wind_and_recovery_has_a_distinct_url() {
    let missing = Arc::new(AtomicBool::new(true));
    let m = Arc::clone(&missing);
    let setup = Setup::new().models(vec![(
        "gfs",
        model_fn(move |overlay| {
            if !m.load(Ordering::SeqCst) {
                return Ok(weather_model(overlay, 6));
            }
            Ok(ModelValue {
                scalar_error: Some("Temperature field unavailable".into()),
                ..weather_model(Overlay::None, 6)
            })
        }),
    )]);
    let proxy = setup.build();
    let partial = request(&proxy, "/manifest?overlay=temperature")
        .await
        .json();
    assert_eq!(partial["overlay"], "temperature");
    assert_eq!(partial["unavailable"], false);
    assert_eq!(partial["stale"], false);
    assert!(partial.get("scalar").is_none());
    assert_eq!(partial["scalarError"], "Temperature field unavailable");
    let partial_url = partial["gridUrl"].as_str().unwrap().to_string();
    assert!(partial_url.contains("-temperature-wind-only.bin"));
    assert_eq!(request(&proxy, &partial_url).await.body.len(), 12 * 8);
    setup.advance(3_600_000);
    missing.store(false, Ordering::SeqCst);
    let complete = request(&proxy, "/manifest?overlay=temperature")
        .await
        .json();
    assert_eq!(complete["cycle"], partial["cycle"]);
    assert_eq!(complete["scalar"]["kind"], "temperature");
    assert!(complete.get("scalarError").is_none());
    let complete_url = complete["gridUrl"].as_str().unwrap();
    assert_ne!(complete_url, partial_url);
    assert_eq!(request(&proxy, complete_url).await.body.len(), 12 * 12);
    assert_eq!(request(&proxy, &partial_url).await.body.len(), 12 * 8);
}

#[tokio::test]
async fn router_mounts_every_wind_path() {
    use tower::ServiceExt;
    let setup = Setup::new().models(vec![(
        "gfs",
        model_fn(|_| Ok(simple_model(6, None, vec![0.0; 12]))),
    )]);
    let state = crate::state::AppState::new(".".into(), "dist".into()).unwrap();
    let app = router_with(Options {
        models: setup.models.clone().unwrap(),
        ..Options::production()
    })
    .with_state(state);
    for (path, status) in [
        ("/api/wind", 200),
        ("/api/wind/", 200),
        ("/api/wind?model=gfs", 200),
        ("/api/wind/manifest", 200),
        ("/api/wind/status", 200),
        ("/api/wind/nope", 404),
        ("/api/wind/grid/a/b.bin", 404),
    ] {
        let response = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status, "{path}");
    }
}
