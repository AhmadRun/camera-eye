//! `/api/wind`: bounded, single-flight, per-model/overlay forecast snapshots
//! from fixed providers (port of `server/providers/wind.js` and
//! `server/providers/wind/*`).
//!
//! - `GET /api/wind[/|/manifest]?model=gfs|ifs&overlay=none|temperature|pressure`
//!   returns the JSON manifest (`Cache-Control: no-store`).
//! - `GET /api/wind/status` returns the manifest without `gridUrl`.
//! - `GET /api/wind/grid/<id>.bin?model=..&overlay=..` returns little-endian
//!   Float32 U, then V, then the optional scalar
//!   (`Cache-Control: public, max-age=3600, immutable`).

pub mod body;
pub mod catalog;
pub mod decode;
pub mod gfs;
pub mod grid;
pub mod ifs;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::response::Response;
use axum::routing::any;
use bytes::{Bytes, BytesMut};
use futures_util::future::{BoxFuture, FutureExt, Shared};
use http::{Method, Uri};
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use tokio::sync::Notify;

use crate::common::query::Params;
use crate::common::{js, reply};
use crate::state::AppState;

use self::body::{
    BoxFut, ByteRange, DecodeFn, Fetch, HttpFetch, load_message, load_optional_scalar,
};
use self::catalog::{CycleRef, iso};
use self::grid::{
    Grid, Overlay, ScalarMeta, WeatherInput, resample_weather_snapshot, weather_scalar_error,
    weather_scalar_metadata,
};

/// A wind pipeline failure. The message never reaches clients: every
/// failure is reported as `Wind upstream unavailable`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindError(pub String);

impl WindError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for WindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WindError {}

/// What a model loader receives (`{ fetchImpl, now, decodeImpl, targetDx }`).
#[derive(Clone)]
pub struct Deps {
    pub fetch: Arc<dyn Fetch>,
    pub now: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub decode: DecodeFn,
    pub target_dx: f64,
}

/// `cycle` of a model result.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cycle {
    pub date: String,
    pub hour: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forecast_hour: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_iso: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_iso: Option<String>,
}

/// A model loader's result.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelValue {
    pub cycle: Cycle,
    pub level: String,
    pub units: String,
    pub grid: Grid,
    pub scalar: Option<ScalarMeta>,
    pub scalar_error: Option<String>,
}

/// A model loader (`models.gfs` / `models.ifs`). Dropping the future cancels it.
pub type ModelFn =
    Arc<dyn Fn(Deps, Overlay) -> BoxFut<Result<ModelValue, WindError>> + Send + Sync>;

/// Byte ranges of the selected messages in one GRIB object.
#[derive(Debug, Clone, PartialEq)]
pub struct Ranges {
    pub u: ByteRange,
    pub v: ByteRange,
    pub scalar: Option<ByteRange>,
}

/// Timeout of the optional scalar companion (`loadOptionalScalar`).
const SCALAR_TIMEOUT: Duration = Duration::from_millis(12_000);

/// The shared tail of `fetchGfsWind` / `fetchIfsWind`: fetch U, V and the
/// optional scalar from one object, then resample.
pub(crate) async fn load_snapshot(
    deps: &Deps,
    overlay: Overlay,
    cycle: CycleRef,
    run_ms: i64,
    forecast_hour: u32,
    url: &str,
    ranges: Ranges,
) -> Result<ModelValue, WindError> {
    let fetch = &*deps.fetch;
    let (u, v, scalar) = futures_util::try_join!(
        load_message(fetch, &deps.decode, url, ranges.u),
        load_message(fetch, &deps.decode, url, ranges.v),
        async {
            Ok(load_optional_scalar(ranges.scalar, url, fetch, &deps.decode, SCALAR_TIMEOUT).await)
        },
    )?;
    let fields = resample_weather_snapshot(WeatherInput {
        u: Some(&u),
        v: Some(&v),
        scalar: scalar.as_ref(),
        overlay,
        target_dx: deps.target_dx,
    })?;
    Ok(ModelValue {
        cycle: Cycle {
            date: cycle.date,
            hour: cycle.hour,
            forecast_hour: Some(forecast_hour),
            run_iso: Some(iso(run_ms)),
            valid_iso: Some(iso(run_ms + i64::from(forecast_hour) * 3_600_000)),
        },
        level: "10 m above ground".into(),
        units: "m/s".into(),
        grid: fields.grid,
        scalar: fields.scalar,
        scalar_error: fields.scalar_error.map(str::to_string),
    })
}

/// Decode on the blocking pool: a 0.25° message is about a million points.
fn blocking_decode() -> DecodeFn {
    Arc::new(|bytes: Bytes| {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || decode::decode_wind_grib_message(&bytes))
                .await
                .map_err(|_| WindError::new("Wind decode failed"))?
        })
    })
}

/// `windProxy({...})` options.
pub struct Options {
    pub deps: Deps,
    pub ttl_ms: i64,
    pub timeout: Duration,
    pub models: HashMap<&'static str, ModelFn>,
}

impl Options {
    /// The production configuration.
    pub fn production() -> Self {
        let gfs: ModelFn = Arc::new(|deps, overlay| Box::pin(gfs::fetch_gfs_wind(deps, overlay)));
        let ifs: ModelFn = Arc::new(|deps, overlay| Box::pin(ifs::fetch_ifs_wind(deps, overlay)));
        Self {
            deps: Deps {
                fetch: Arc::new(HttpFetch::new()),
                now: Arc::new(js::now_ms),
                decode: blocking_decode(),
                target_dx: 1.0,
            },
            ttl_ms: 3_600_000,
            timeout: Duration::from_millis(40_000),
            models: HashMap::from([("gfs", gfs), ("ifs", ifs)]),
        }
    }
}

/// One issued snapshot: its manifest and the binary grid payload.
struct Snapshot {
    id: String,
    manifest: Map<String, Value>,
    bytes: Bytes,
    fetched_at: i64,
}

/// Cancellation shared by an in-flight refresh and its waiting requests.
struct Control {
    waiters: AtomicUsize,
    aborted: AtomicBool,
    abort: Notify,
}

impl Control {
    fn abort(&self) {
        self.aborted.store(true, Ordering::SeqCst);
        self.abort.notify_one();
    }
}

/// `None` is the unavailable result.
type Outcome = Option<Arc<Snapshot>>;

struct Operation {
    control: Arc<Control>,
    result: Shared<BoxFuture<'static, Outcome>>,
}

#[derive(Default)]
struct Store {
    caches: HashMap<String, Arc<Snapshot>>,
    loadings: HashMap<String, Arc<Operation>>,
    /// The two most recent issued grids per key, oldest first.
    grids: HashMap<String, Vec<(String, Arc<Snapshot>)>>,
    attempts: HashMap<String, i64>,
}

/// Counts a request as waiting on a refresh; dropping it before completion
/// (the client went away) aborts the refresh once nobody else waits.
struct Waiter {
    control: Arc<Control>,
    done: bool,
}

impl Waiter {
    fn new(control: Arc<Control>) -> Self {
        control.waiters.fetch_add(1, Ordering::SeqCst);
        Self {
            control,
            done: false,
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let before = self.control.waiters.fetch_sub(1, Ordering::SeqCst);
        if !self.done && before == 1 {
            self.control.abort();
        }
    }
}

const UNAVAILABLE: &str = "Wind upstream unavailable";

/// A JSON number the way `JSON.stringify` writes it (`0`, not `0.0`).
fn number(value: f64) -> Value {
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        Value::from(value as i64)
    } else {
        Value::from(value)
    }
}

/// `String(number)` for the grid id.
fn number_string(value: f64) -> String {
    match number(value) {
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

fn unavailable(model: &str, overlay: Overlay) -> Map<String, Value> {
    let mut manifest = Map::new();
    manifest.insert("model".into(), json!(model));
    manifest.insert("schemaVersion".into(), json!(1));
    if overlay != Overlay::None {
        manifest.insert("overlay".into(), json!(overlay.as_str()));
    }
    manifest.insert("unavailable".into(), json!(true));
    manifest.insert("stale".into(), json!(true));
    manifest.insert("reason".into(), json!(UNAVAILABLE));
    manifest
}

fn send_json(status: u16, value: &impl serde::Serialize) -> Response {
    reply::json(status, value, Some("no-store"))
}

fn send_error(status: u16, error: &str) -> Response {
    send_json(status, &json!({ "error": error }))
}

/// The provider: caches, in-flight refreshes and the request handler.
pub struct WindProxy {
    options: Options,
    store: Mutex<Store>,
}

impl WindProxy {
    pub fn new(options: Options) -> Arc<Self> {
        Arc::new(Self {
            options,
            store: Mutex::new(Store::default()),
        })
    }

    fn now(&self) -> i64 {
        (self.options.deps.now)()
    }

    /// Check a model result and turn it into an issued snapshot.
    fn issue(
        &self,
        model: &str,
        overlay: Overlay,
        value: ModelValue,
    ) -> Result<Snapshot, WindError> {
        let invalid = || WindError::new("Invalid wind grid");
        let ModelValue {
            cycle,
            level,
            units,
            grid,
            scalar: value_scalar,
            scalar_error,
        } = value;
        let count = grid.nx.checked_mul(grid.ny).ok_or_else(invalid)?;
        let expected_scalar = weather_scalar_metadata(overlay);
        let expected_error = weather_scalar_error(overlay);
        let missing_scalar = expected_scalar.is_some()
            && value_scalar.is_none()
            && scalar_error.is_some()
            && scalar_error.as_deref() == expected_error;
        let scalar = if missing_scalar {
            None
        } else {
            expected_scalar.clone()
        };
        let scalar_ok = match &scalar {
            Some(scalar) => value_scalar.as_ref() == Some(scalar),
            None => value_scalar.is_none() && grid.scalar.is_none(),
        };
        if grid.nx < 1
            || grid.ny < 1
            || count > 1_000_000
            || ![grid.lo1, grid.la1, grid.dx, grid.dy]
                .iter()
                .all(|n| n.is_finite())
            || grid.dx <= 0.0
            || grid.dy <= 0.0
            || !scalar_ok
            || (expected_scalar.is_none() && scalar_error.is_some())
            || (scalar.is_some() && scalar_error.is_some())
        {
            return Err(invalid());
        }
        let mut components = vec![&grid.u, &grid.v];
        if scalar.is_some() {
            components.push(grid.scalar.as_ref().ok_or_else(invalid)?);
        }
        for values in &components {
            if values.len() != count || !values.iter().all(|n| n.is_finite()) {
                return Err(invalid());
            }
        }
        let target_dx = self.options.deps.target_dx;
        let suffix = match overlay {
            Overlay::None => String::new(),
            _ => format!(
                "-{}{}",
                overlay.as_str(),
                if missing_scalar { "-wind-only" } else { "" }
            ),
        };
        let query = match overlay {
            Overlay::None => format!("model={model}"),
            _ => format!("model={model}&overlay={}", overlay.as_str()),
        };
        let id = format!(
            "{model}-{}-{}-f{}-{}{suffix}",
            cycle.date,
            cycle.hour,
            cycle.forecast_hour.unwrap_or(0),
            number_string(target_dx)
        );
        let mut manifest = Map::new();
        manifest.insert("schemaVersion".into(), json!(1));
        manifest.insert("model".into(), json!(model));
        manifest.insert(
            "cycle".into(),
            serde_json::to_value(&cycle).unwrap_or_default(),
        );
        if expected_scalar.is_some() {
            manifest.insert("overlay".into(), json!(overlay.as_str()));
        }
        if let Some(scalar) = &scalar {
            manifest.insert(
                "scalar".into(),
                serde_json::to_value(scalar).unwrap_or_default(),
            );
        }
        if missing_scalar {
            manifest.insert("scalarError".into(), json!(expected_error));
        }
        manifest.insert("fetchedAt".into(), json!(self.now()));
        manifest.insert("level".into(), json!(level));
        manifest.insert("units".into(), json!(units));
        manifest.insert(
            "grid".into(),
            json!({
                "nx": grid.nx,
                "ny": grid.ny,
                "lo1": number(grid.lo1),
                "la1": number(grid.la1),
                "dx": number(grid.dx),
                "dy": number(grid.dy),
            }),
        );
        manifest.insert("stale".into(), json!(false));
        manifest.insert("unavailable".into(), json!(false));
        manifest.insert("reason".into(), Value::Null);
        manifest.insert(
            "gridUrl".into(),
            json!(format!("/api/wind/grid/{id}.bin?{query}")),
        );
        let mut bytes = BytesMut::with_capacity(components.len() * count * 4);
        for values in components {
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        Ok(Snapshot {
            id,
            manifest,
            bytes: bytes.freeze(),
            fetched_at: self.now(),
        })
    }

    /// Start a refresh for `key`; the caller holds the store lock.
    fn refresh(
        self: &Arc<Self>,
        store: &mut Store,
        model: &'static str,
        overlay: Overlay,
        key: &str,
    ) -> Arc<Operation> {
        let control = Arc::new(Control {
            waiters: AtomicUsize::new(0),
            aborted: AtomicBool::new(false),
            abort: Notify::new(),
        });
        store.attempts.insert(key.to_string(), self.now());
        let this = Arc::clone(self);
        let task_control = Arc::clone(&control);
        let task_key = key.to_string();
        let task = tokio::spawn(async move {
            let control = task_control;
            let key = task_key;
            let loader = this.options.models[model].clone();
            let load = loader(this.options.deps.clone(), overlay);
            let result = tokio::select! {
                value = load => value,
                () = control.abort.notified() => Err(WindError::new("aborted")),
                () = tokio::time::sleep(this.options.timeout) => {
                    control.aborted.store(true, Ordering::SeqCst);
                    Err(WindError::new("timeout"))
                }
            };
            let result = result.and_then(|value| {
                if control.aborted.load(Ordering::SeqCst) {
                    return Err(WindError::new("aborted"));
                }
                this.issue(model, overlay, value)
            });
            let mut store = this.store.lock();
            let outcome = match result {
                Ok(snapshot) => {
                    let snapshot = Arc::new(snapshot);
                    store.caches.insert(key.clone(), Arc::clone(&snapshot));
                    // Retain the previous issued grid as well to cover a
                    // manifest/grid rollover.
                    let history = store.grids.entry(key.clone()).or_default();
                    match history.iter_mut().find(|(id, _)| *id == snapshot.id) {
                        Some(entry) => entry.1 = Arc::clone(&snapshot),
                        None => history.push((snapshot.id.clone(), Arc::clone(&snapshot))),
                    }
                    while history.len() > 2 {
                        history.remove(0);
                    }
                    Some(snapshot)
                }
                Err(_)
                    if control.aborted.load(Ordering::SeqCst)
                        && control.waiters.load(Ordering::SeqCst) == 0 =>
                {
                    store.attempts.remove(&key);
                    None
                }
                Err(_) => match store.caches.get(&key).cloned() {
                    Some(old) => {
                        let mut manifest = old.manifest.clone();
                        manifest.insert("stale".into(), json!(true));
                        manifest.insert("reason".into(), json!(UNAVAILABLE));
                        let stale = Arc::new(Snapshot {
                            id: old.id.clone(),
                            manifest,
                            bytes: old.bytes.clone(),
                            fetched_at: old.fetched_at,
                        });
                        store.caches.insert(key.clone(), Arc::clone(&stale));
                        Some(stale)
                    }
                    None => None,
                },
            };
            if store
                .loadings
                .get(&key)
                .is_some_and(|op| Arc::ptr_eq(&op.control, &control))
            {
                store.loadings.remove(&key);
            }
            outcome
        });
        let operation = Arc::new(Operation {
            control,
            result: task.map(|joined| joined.ok().flatten()).boxed().shared(),
        });
        store
            .loadings
            .insert(key.to_string(), Arc::clone(&operation));
        operation
    }

    /// The request handler for everything under `/api/wind`.
    pub async fn handle(self: Arc<Self>, method: Method, uri: Uri) -> Response {
        let rest = uri.path().strip_prefix("/api/wind").unwrap_or(uri.path());
        let pathname = if rest.is_empty() { "/" } else { rest };
        let params = Params::parse(uri.query().unwrap_or(""));
        let model = params
            .get("model")
            .filter(|m| !m.is_empty())
            .unwrap_or("gfs");
        let overlay = params
            .get("overlay")
            .filter(|o| !o.is_empty())
            .unwrap_or("none");
        if method != Method::GET {
            return send_error(405, "method_not_allowed");
        }
        let Some((&model, _)) = self
            .options
            .models
            .get_key_value(model)
            .filter(|(name, _)| matches!(**name, "gfs" | "ifs"))
        else {
            return send_error(400, "unknown_model");
        };
        let Ok(overlay) = Overlay::parse(overlay) else {
            return send_error(400, "unknown_overlay");
        };
        let key = format!("{model}:{}", overlay.as_str());
        if let Some(path) = pathname.strip_prefix("/grid/") {
            let snapshot = path.strip_suffix(".bin").and_then(|id| {
                let store = self.store.lock();
                store
                    .grids
                    .get(&key)?
                    .iter()
                    .find(|(issued, _)| issued == id)
                    .map(|(_, snapshot)| Arc::clone(snapshot))
            });
            return match snapshot {
                Some(snapshot) => reply::raw(
                    200,
                    "application/octet-stream",
                    snapshot.bytes.clone(),
                    Some("public, max-age=3600, immutable"),
                ),
                None => send_error(404, "unknown_grid"),
            };
        }
        if !matches!(pathname, "/" | "/manifest" | "/status") {
            return send_error(404, "not_found");
        }
        let (mut state, operation) = {
            let mut store = self.store.lock();
            let state = store.caches.get(&key).cloned();
            let fresh = state
                .as_ref()
                .is_some_and(|s| self.now() - s.fetched_at < self.options.ttl_ms);
            let operation = if fresh {
                None
            } else {
                match store.loadings.get(&key).cloned() {
                    Some(operation) => Some(operation),
                    None if store
                        .attempts
                        .get(&key)
                        .is_none_or(|at| self.now() - at >= 60_000) =>
                    {
                        Some(self.refresh(&mut store, model, overlay, &key))
                    }
                    None => None,
                }
            };
            (state, operation)
        };
        if let Some(operation) = operation {
            let mut waiter = Waiter::new(Arc::clone(&operation.control));
            state = operation.result.clone().await;
            waiter.done = true;
        }
        let manifest = state.map_or_else(|| unavailable(model, overlay), |s| s.manifest.clone());
        if pathname == "/status" {
            let mut status = manifest;
            status.remove("gridUrl");
            return send_json(200, &status);
        }
        send_json(200, &manifest)
    }
}

/// Mount `/api/wind` with an explicit configuration.
pub fn router_with(options: Options) -> Router<AppState> {
    let proxy = WindProxy::new(options);
    let handler = move |method: Method, uri: Uri| {
        let proxy = Arc::clone(&proxy);
        async move { proxy.handle(method, uri).await }
    };
    Router::new()
        .route("/api/wind", any(handler.clone()))
        .route("/api/wind/", any(handler.clone()))
        .route("/api/wind/{*rest}", any(handler))
}

/// Mount `/api/wind` with the production GFS and IFS loaders.
pub fn router(_state: &AppState) -> Router<AppState> {
    router_with(Options::production())
}

#[cfg(test)]
pub(crate) mod tests;
