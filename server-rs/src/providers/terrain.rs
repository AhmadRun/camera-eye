//! Re:Earth terrain point-height proxy (port of `server/providers/terrain.js`
//! and `src/data/terrainHeightsProxy.js`).
//!
//! `/api/terrain/heights?points=lon,lat;…` → `{results}` in exact request
//! order. Keyless upstream: <https://terrain.reearth.land/heights.json>.
//! Results are cached per 5dp point in memory and on disk
//! (`.ce-cache/terrain-heights.json`, flushed every 15 s) for 30 days;
//! only missing or stale points go upstream, in sequential chunks of 64
//! with bounded jittered retries.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};

use super::hazard_support::{
    Clock, FetchRequest, Fetcher, encode_uri_component, finite_number, mount, parse_date,
    random_fraction, read_all, reqwest_fetcher, sub_url, system_clock, to_fixed,
};
use crate::common::coalesce::Coalescer;
use crate::common::{disk, js, reply};
use crate::state::AppState;

/// Decimal precision the terrain client uses when serializing lon/lat.
pub const TERRAIN_POINT_PRECISION: usize = 5;
/// Maximum time retries may add after the first upstream attempt settles.
pub const TERRAIN_RETRY_BUDGET_MS: i64 = 10_000;
/// One initial attempt plus three bounded retries.
pub const TERRAIN_MAX_ATTEMPTS: usize = 4;

const TTL_MS: i64 = 30 * 24 * 3_600_000;
/// Sized against measured upstream latency (87–186 ms/point): 64 points fit
/// the 30 s attempt timeout at both ends of that range.
const UPSTREAM_CHUNK: usize = 64;
const MAX_POINTS: usize = 2000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(15);

pub type Point = (f64, f64);

/// `parseTerrainPoints`: the `points=lon,lat;…` parameter, or `None`.
pub fn parse_terrain_points(raw: Option<&str>) -> Option<Vec<Point>> {
    let text = raw.unwrap_or("").trim();
    if text.is_empty() {
        return None;
    }
    let pairs: Vec<&str> = text
        .split(';')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    if pairs.is_empty() {
        return None;
    }
    pairs
        .into_iter()
        .map(|pair| {
            let parts: Vec<&str> = pair.split(',').collect();
            if parts.len() != 2 {
                return None;
            }
            let lon = js::number(parts[0]);
            let lat = js::number(parts[1]);
            (lon.is_finite() && lat.is_finite()).then_some((lon, lat))
        })
        .collect()
}

/// `terrainPointKey`: the canonical 5dp cache key.
pub fn terrain_point_key((lon, lat): Point) -> String {
    format!(
        "{},{}",
        to_fixed(lon, TERRAIN_POINT_PRECISION),
        to_fixed(lat, TERRAIN_POINT_PRECISION)
    )
}

fn canonical_terrain_point(point: Point) -> (String, Point) {
    let key = terrain_point_key(point);
    let (lon, lat) = key.split_once(',').unwrap_or(("", ""));
    let canonical = (js::number(lon), js::number(lat));
    (key, canonical)
}

/// Only a real numeric ellipsoid height is cacheable/servable.
pub fn valid_terrain_result(result: Option<&Value>) -> bool {
    result
        .and_then(Value::as_object)
        .is_some_and(|r| finite_number(r.get("ellipsoid")).is_some())
}

/// A row the upstream returned with no usable height (real `elevation`,
/// null `geoid`/`ellipsoid`): an absent datum, not a failed refresh.
pub fn absent_terrain_datum(result: Option<&Value>) -> bool {
    result.and_then(Value::as_object).is_some_and(|r| {
        r.get("ellipsoid") == Some(&Value::Null)
            && r.get("geoid") == Some(&Value::Null)
            && finite_number(r.get("elevation")).is_some()
    })
}

/// `terrainRetryAfterMs`: delta-seconds or HTTP-date → milliseconds.
pub fn terrain_retry_after_ms(value: Option<&str>, now_ms: i64) -> Option<i64> {
    let text = value.unwrap_or("").trim();
    if text.is_empty() {
        return None;
    }
    let seconds = js::number(text);
    if seconds.is_finite() && seconds >= 0.0 {
        return Some((seconds * 1000.0) as i64);
    }
    let date = parse_date(text)?;
    Some((date - now_ms).max(0))
}

/// One failed chunk attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkError {
    pub message: String,
    /// `None` (unknown — network and parse errors) counts as retryable.
    pub retryable: Option<bool>,
    pub retry_after: Option<String>,
}

impl ChunkError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: None,
            retry_after: None,
        }
    }
}

pub type Sleep = Arc<dyn Fn(u64) -> BoxFuture<'static, ()> + Send + Sync>;

/// Injectable dependencies of [`fetch_terrain_chunk_with_retry`].
#[derive(Clone)]
pub struct RetryOptions {
    pub fetch: Fetcher,
    pub sleep: Sleep,
    pub random: Arc<dyn Fn() -> f64 + Send + Sync>,
    pub now: Clock,
    pub attempt_timeout_ms: i64,
    pub retry_budget_ms: i64,
    pub max_attempts: usize,
}

impl RetryOptions {
    pub fn new(fetch: Fetcher, now: Clock) -> Self {
        Self {
            fetch,
            sleep: Arc::new(|ms| tokio::time::sleep(Duration::from_millis(ms)).boxed()),
            random: Arc::new(random_fraction),
            now,
            attempt_timeout_ms: 30_000,
            retry_budget_ms: TERRAIN_RETRY_BUDGET_MS,
            max_attempts: TERRAIN_MAX_ATTEMPTS,
        }
    }
}

/// `fetchTerrainChunkWithRetry`: network errors, 429 and 5xx retry with
/// jittered backoff (Retry-After honored) inside a 10 s added-time budget;
/// other HTTP failures and malformed successes fail fast.
pub async fn fetch_terrain_chunk_with_retry(
    points: &[Point],
    options: &RetryOptions,
) -> Result<Vec<Value>, ChunkError> {
    let points_param = points
        .iter()
        .map(|p| terrain_point_key(*p))
        .collect::<Vec<_>>()
        .join(";");
    let url = format!(
        "https://terrain.reearth.land/heights.json?points={}",
        encode_uri_component(&points_param)
    );
    let mut retry_started_at: Option<i64> = None;
    let mut last_error: Option<ChunkError> = None;

    for attempt in 0..options.max_attempts {
        let mut timeout_ms = options.attempt_timeout_ms;
        if attempt > 0 {
            let remaining =
                options.retry_budget_ms - ((options.now)() - retry_started_at.unwrap_or(0));
            if remaining <= 0 {
                break;
            }
            timeout_ms = remaining.min(options.attempt_timeout_ms).max(1);
        }
        let fetch = options.fetch.clone();
        let request = FetchRequest::get(url.clone());
        let outcome = tokio::time::timeout(Duration::from_millis(timeout_ms as u64), async move {
            let response = fetch(request)
                .await
                .map_err(|e| ChunkError::new(e.to_string()))?;
            if !response.ok() {
                return Err(ChunkError {
                    message: format!("HTTP {}", response.status),
                    retryable: Some(response.status == 429 || response.status >= 500),
                    retry_after: response.header("retry-after").map(str::to_string),
                });
            }
            let bytes = read_all(response)
                .await
                .map_err(|e| ChunkError::new(e.to_string()))?;
            let json: Value =
                serde_json::from_slice(&bytes).map_err(|e| ChunkError::new(e.to_string()))?;
            match json.get("results") {
                Some(Value::Array(results)) => Ok(results.clone()),
                _ => Err(ChunkError {
                    message: "malformed upstream response (no results array)".into(),
                    retryable: Some(false),
                    retry_after: None,
                }),
            }
        })
        .await
        .unwrap_or_else(|_| Err(ChunkError::new("The operation was aborted due to timeout")));
        let error = match outcome {
            Ok(results) => return Ok(results),
            Err(error) => error,
        };
        let retryable = error.retryable != Some(false);
        let retry_after = error.retry_after.clone();
        last_error = Some(error);
        if !retryable || attempt >= options.max_attempts - 1 {
            break;
        }
        let started = *retry_started_at.get_or_insert_with(|| (options.now)());
        let remaining = options.retry_budget_ms - ((options.now)() - started);
        if remaining <= 0 {
            break;
        }
        let retry_after_ms = terrain_retry_after_ms(retry_after.as_deref(), (options.now)());
        let backoff_ms =
            (350.0 * 2f64.powi(attempt as i32) * (0.75 + (options.random)() * 0.5)).round() as i64;
        let delay_ms = retry_after_ms.unwrap_or(backoff_ms).min(remaining);
        if delay_ms > 0 {
            (options.sleep)(delay_ms as u64).await;
        }
    }
    Err(last_error
        .unwrap_or_else(|| ChunkError::new("terrain heights upstream retry budget exhausted")))
}

/// A cached height.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    pub at: i64,
    pub result: Value,
}

pub type TerrainCache = Mutex<HashMap<String, CacheEntry>>;

/// What [`resolve_terrain_height_request`] decided.
#[derive(Debug)]
pub struct Outcome {
    pub status: u16,
    pub body: Value,
    pub cache_changed: bool,
    pub upstream_error: Option<String>,
    pub absent_points: usize,
    pub requested_points: usize,
}

/// `resolveTerrainHeightRequest`: rebuild a batch from the per-point cache
/// and a missing-only fetch, in exact request order (duplicates included).
/// Stale values serve only when the refresh fails; any position without a
/// real height makes the whole answer a 502 with no fabricated results.
pub async fn resolve_terrain_height_request<F, Fut>(
    points: &[Point],
    cache: &TerrainCache,
    fetch_missing: F,
    ttl_ms: i64,
    now: &Clock,
) -> Outcome
where
    F: FnOnce(Vec<Point>) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<Option<Value>>, String>>,
{
    let requested: Vec<(String, Point)> =
        points.iter().map(|p| canonical_terrain_point(*p)).collect();
    let mut seen = HashSet::new();
    let unique: Vec<&(String, Point)> = requested
        .iter()
        .filter(|(key, _)| seen.insert(key.clone()))
        .collect();

    let request_started_at = now();
    let missing: Vec<&(String, Point)> = {
        let cache = cache.lock();
        unique
            .iter()
            .copied()
            .filter(|(key, _)| {
                !cache.get(key).is_some_and(|entry| {
                    valid_terrain_result(Some(&entry.result))
                        && request_started_at - entry.at < ttl_ms
                })
            })
            .collect()
    };

    let mut cache_changed = false;
    let mut upstream_error = None;
    let mut absent_points = 0;
    if !missing.is_empty() {
        match fetch_missing(missing.iter().map(|(_, p)| *p).collect()).await {
            Ok(fetched) => {
                let fetched_at = now();
                let mut omitted = 0;
                let mut cache = cache.lock();
                for (i, (key, _)) in missing.iter().enumerate() {
                    let result = fetched.get(i).and_then(Option::as_ref);
                    if valid_terrain_result(result) {
                        cache.insert(
                            key.clone(),
                            CacheEntry {
                                at: fetched_at,
                                result: result.cloned().unwrap_or(Value::Null),
                            },
                        );
                        cache_changed = true;
                    } else if absent_terrain_datum(result) {
                        // Transient: deliberately left uncached so the next poll re-asks.
                        absent_points += 1;
                    } else {
                        omitted += 1;
                    }
                }
                if omitted > 0 || fetched.len() != missing.len() {
                    let dropped = if omitted > 0 {
                        omitted
                    } else {
                        fetched.len().abs_diff(missing.len())
                    };
                    upstream_error =
                        Some(format!("upstream omitted {dropped} terrain position(s)"));
                }
            }
            Err(error) => upstream_error = Some(error),
        }
    }

    let results: Vec<Option<Value>> = {
        let cache = cache.lock();
        requested
            .iter()
            .map(|(key, _)| {
                cache
                    .get(key)
                    .filter(|entry| valid_terrain_result(Some(&entry.result)))
                    .map(|entry| entry.result.clone())
            })
            .collect()
    };
    let requested_points = unique.len();
    if results.iter().any(Option::is_none) {
        return Outcome {
            status: 502,
            body: json!({ "error": "terrain heights fetch failed and no cache available for every point" }),
            cache_changed,
            upstream_error,
            absent_points,
            requested_points,
        };
    }
    Outcome {
        status: 200,
        body: json!({ "results": results }),
        cache_changed,
        upstream_error,
        absent_points,
        requested_points,
    }
}

// ─── Proxy ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct Deps {
    pub retry: RetryOptions,
    pub cache_dir: PathBuf,
    pub flush_interval: Duration,
}

type Fetched = Result<Vec<Option<Value>>, String>;

struct Terrain {
    deps: Deps,
    mem: TerrainCache,
    inflight: Coalescer<String, Fetched>,
    disk_loaded: tokio::sync::OnceCell<()>,
    disk_dirty: Mutex<bool>,
}

pub fn router(state: &AppState) -> Router<AppState> {
    router_with(Deps {
        retry: RetryOptions::new(reqwest_fetcher(), system_clock()),
        cache_dir: state.cache_dir.clone(),
        flush_interval: FLUSH_INTERVAL,
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let proxy = Arc::new(Terrain {
        deps,
        mem: Mutex::new(HashMap::new()),
        inflight: Coalescer::new(),
        disk_loaded: tokio::sync::OnceCell::new(),
        disk_dirty: Mutex::new(false),
    });
    mount("/api/terrain/heights", move |request: Request| {
        let proxy = Arc::clone(&proxy);
        async move { proxy.handle(request).await }
    })
}

fn entry_from(value: &Value) -> Option<CacheEntry> {
    let at = value
        .get("at")
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite())?;
    Some(CacheEntry {
        at: at as i64,
        result: value.get("result").cloned().unwrap_or(Value::Null),
    })
}

impl Terrain {
    fn cache_path(&self) -> PathBuf {
        self.deps.cache_dir.join("terrain-heights.json")
    }

    /// Load the disk cache once (first request), then flush it periodically.
    async fn load_disk_once(self: &Arc<Self>) {
        self.disk_loaded
            .get_or_init(|| async {
                if let Some(parsed) = disk::read_json::<Value>(&self.cache_path()).await {
                    self.load_parsed(&parsed);
                }
                let weak = Arc::downgrade(self);
                let interval = self.deps.flush_interval;
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(interval).await;
                        let Some(this) = weak.upgrade() else {
                            return;
                        };
                        this.flush().await;
                    }
                });
            })
            .await;
    }

    fn load_parsed(&self, parsed: &Value) {
        let mut mem = self.mem.lock();
        let points = (parsed.get("version") == Some(&json!(2)))
            .then(|| parsed.get("points").and_then(Value::as_object))
            .flatten();
        if let Some(points) = points {
            for (key, entry) in points {
                if let Some(entry) =
                    entry_from(entry).filter(|e| valid_terrain_result(Some(&e.result)))
                {
                    mem.insert(key.clone(), entry);
                }
            }
        } else if let Some(batches) = parsed.as_object() {
            // One-time migration from the former raw-batch cache: zip only
            // real, positionally present results.
            for (raw_points, entry) in batches {
                let Some(points) = parse_terrain_points(Some(raw_points)) else {
                    continue;
                };
                let Some(at) = entry
                    .get("at")
                    .and_then(Value::as_f64)
                    .filter(|n| n.is_finite())
                else {
                    continue;
                };
                let Some(results) = entry.get("results").and_then(Value::as_array) else {
                    continue;
                };
                for (i, point) in points.iter().enumerate() {
                    let result = results.get(i);
                    if !valid_terrain_result(result) {
                        continue;
                    }
                    let key = terrain_point_key(*point);
                    let newer = mem.get(&key).is_none_or(|existing| at as i64 > existing.at);
                    if newer {
                        mem.insert(
                            key,
                            CacheEntry {
                                at: at as i64,
                                result: result.cloned().unwrap_or(Value::Null),
                            },
                        );
                    }
                }
            }
            *self.disk_dirty.lock() = !mem.is_empty();
        }
    }

    async fn flush(&self) {
        {
            let mut dirty = self.disk_dirty.lock();
            if !*dirty {
                return;
            }
            *dirty = false;
        }
        let points: Map<String, Value> = self
            .mem
            .lock()
            .iter()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    json!({ "at": entry.at, "result": entry.result }),
                )
            })
            .collect();
        let body = json!({ "version": 2, "points": points });
        if disk::write_json(&self.cache_path(), &body).await.is_err() {
            *self.disk_dirty.lock() = true; // retry next tick
            tracing::warn!("[terrain-heights-proxy] cache write failed");
        }
    }

    /// Fetch every missing chunk sequentially; a failing chunk yields nulls
    /// for its own positions. Total failure is the first real error.
    async fn fetch_upstream_all(retry: &RetryOptions, points: &[Point]) -> Fetched {
        let mut results = Vec::with_capacity(points.len());
        let mut first_error: Option<String> = None;
        for chunk in points.chunks(UPSTREAM_CHUNK) {
            let chunk_results = match fetch_terrain_chunk_with_retry(chunk, retry).await {
                Ok(results) => results,
                Err(error) => {
                    first_error.get_or_insert(error.message);
                    Vec::new()
                }
            };
            for j in 0..chunk.len() {
                results.push(chunk_results.get(j).filter(|v| !v.is_null()).cloned());
            }
        }
        if let Some(error) = first_error
            && results.iter().all(Option::is_none)
        {
            return Err(error);
        }
        Ok(results)
    }

    async fn handle(self: Arc<Self>, request: Request) -> Response {
        self.load_disk_once().await;
        let url = sub_url(&request, "/api/terrain/heights");
        let raw_points = url::Url::parse("http://internal/")
            .and_then(|base| base.join(&url))
            .ok()
            .and_then(|u| {
                u.query_pairs()
                    .find(|(k, _)| k == "points")
                    .map(|(_, v)| v.into_owned())
            });
        let Some(points) = parse_terrain_points(raw_points.as_deref()) else {
            return reply::json(
                400,
                &json!({ "error": "invalid points parameter — expected \"lon,lat;lon,lat;…\" with finite numbers" }),
                None,
            );
        };
        if points.len() > MAX_POINTS {
            return reply::json(
                500,
                &json!({ "error": format!("too many points ({}); max {MAX_POINTS} per request", points.len()) }),
                None,
            );
        }
        let this = Arc::clone(&self);
        let fetch_missing = move |missing: Vec<Point>| async move {
            let key = missing
                .iter()
                .map(|p| terrain_point_key(*p))
                .collect::<Vec<_>>()
                .join(";");
            let retry = this.deps.retry.clone();
            let (result, _) = this
                .inflight
                .run(key, move || async move {
                    tokio::spawn(async move { Self::fetch_upstream_all(&retry, &missing).await })
                        .await
                        .unwrap_or_else(|_| Err("terrain heights proxy error".into()))
                })
                .await;
            result
        };
        let outcome = resolve_terrain_height_request(
            &points,
            &self.mem,
            fetch_missing,
            TTL_MS,
            &self.deps.retry.now,
        )
        .await;
        if outcome.cache_changed {
            *self.disk_dirty.lock() = true;
        }
        if outcome.upstream_error.is_some() {
            tracing::warn!(
                "[terrain-heights-proxy] refresh incomplete — serving stale points when available"
            );
        } else if outcome.absent_points > 0 {
            tracing::info!(
                "[terrain-heights-proxy] {} of {} position(s) had no upstream height this poll — retrying next cycle",
                outcome.absent_points,
                outcome.requested_points
            );
        }
        reply::json(outcome.status, &outcome.body, None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::super::hazard_support::FetchResponse;
    use super::super::hazard_support::testing::{self, Calls};
    use super::*;

    fn result(id: &str, ellipsoid: f64) -> Value {
        json!({ "id": id, "ellipsoid": ellipsoid, "elevation": ellipsoid - 10.0, "geoid": 10 })
    }

    fn cache_of(entries: Vec<(Point, i64, Value)>) -> TerrainCache {
        Mutex::new(
            entries
                .into_iter()
                .map(|(p, at, result)| (terrain_point_key(p), CacheEntry { at, result }))
                .collect(),
        )
    }

    fn fixed(now: i64) -> Clock {
        Arc::new(move || now)
    }

    fn ids(body: &Value) -> Vec<&str> {
        body["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn points_parse_and_key_like_the_client() {
        assert_eq!(
            parse_terrain_points(Some(" 1,2 ; 3,4;")),
            Some(vec![(1.0, 2.0), (3.0, 4.0)])
        );
        assert_eq!(parse_terrain_points(Some("invalid")), None);
        assert_eq!(parse_terrain_points(Some("1,2,3")), None);
        assert_eq!(parse_terrain_points(Some(";;")), None);
        assert_eq!(parse_terrain_points(None), None);
        assert_eq!(terrain_point_key((2.56, 1.0)), "2.56000,1.00000");
        assert_eq!(terrain_point_key((-97.7431, 30.2672)), "-97.74310,30.26720");
        assert_eq!(terrain_retry_after_ms(Some("3"), 0), Some(3000));
        assert_eq!(terrain_retry_after_ms(Some(""), 0), None);
        assert_eq!(
            terrain_retry_after_ms(Some("Wed, 21 Oct 2015 07:28:00 GMT"), 1_445_412_470_000),
            Some(10_000)
        );
    }

    #[tokio::test]
    async fn cache_reconstruction_preserves_request_order_with_hits_misses_and_duplicates() {
        let now = 50_000;
        let cache = cache_of(vec![
            ((10.0, 1.0), now, result("cached-a", 101.0)),
            ((30.0, 3.0), now, result("cached-c", 303.0)),
        ]);
        let points = [
            (30.0, 3.0),
            (20.0, 2.0),
            (10.0, 1.0),
            (40.0, 4.0),
            (20.0, 2.0),
        ];
        let fetched_points = Mutex::new(None);
        let outcome = resolve_terrain_height_request(
            &points,
            &cache,
            |missing| {
                *fetched_points.lock() = Some(missing);
                async {
                    Ok(vec![
                        Some(result("fresh-b", 202.0)),
                        Some(result("fresh-d", 404.0)),
                    ])
                }
            },
            10_000,
            &fixed(now),
        )
        .await;
        assert_eq!(outcome.status, 200);
        assert_eq!(*fetched_points.lock(), Some(vec![(20.0, 2.0), (40.0, 4.0)]));
        assert_eq!(
            ids(&outcome.body),
            vec!["cached-c", "fresh-b", "cached-a", "fresh-d", "fresh-b"]
        );
    }

    fn retry_options(
        fetch: Fetcher,
        clock: Arc<AtomicI64>,
        sleeps: Arc<Mutex<Vec<u64>>>,
    ) -> RetryOptions {
        let now_clock = Arc::clone(&clock);
        RetryOptions {
            fetch,
            sleep: Arc::new(move |ms| {
                sleeps.lock().push(ms);
                clock.fetch_add(ms as i64, Ordering::SeqCst);
                async {}.boxed()
            }),
            random: Arc::new(|| 0.5),
            now: Arc::new(move || now_clock.load(Ordering::SeqCst)),
            attempt_timeout_ms: 30_000,
            retry_budget_ms: TERRAIN_RETRY_BUDGET_MS,
            max_attempts: TERRAIN_MAX_ATTEMPTS,
        }
    }

    #[tokio::test]
    async fn retry_429_honors_retry_after_before_the_next_attempt() {
        let calls: Calls = Default::default();
        let counter = Arc::clone(&calls);
        let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
            Ok(if counter.lock().len() == 1 {
                FetchResponse::from_bytes(429, &[("retry-after", "3")], "")
            } else {
                FetchResponse::json(&json!({ "results": [result("recovered", 88.0)] }))
            })
        });
        let sleeps = Arc::new(Mutex::new(Vec::new()));
        let options = retry_options(
            fetch,
            Arc::new(AtomicI64::new(1_000_000)),
            Arc::clone(&sleeps),
        );
        let results = fetch_terrain_chunk_with_retry(&[(12.345678, 45.678912)], &options)
            .await
            .unwrap();
        assert_eq!(calls.lock().len(), 2);
        assert_eq!(*sleeps.lock(), vec![3000]);
        assert_eq!(results[0]["id"], json!("recovered"));
        assert!(calls.lock()[0].url.ends_with("points=12.34568%2C45.67891"));
    }

    #[tokio::test]
    async fn permanently_failing_upstream_exhausts_retries_and_returns_502() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Ok(FetchResponse::from_bytes(503, &[], ""))
        });
        let clock = Arc::new(AtomicI64::new(2_000_000));
        let options = retry_options(fetch, Arc::clone(&clock), Default::default());
        let now: Clock = {
            let clock = Arc::clone(&clock);
            Arc::new(move || clock.load(Ordering::SeqCst))
        };
        let outcome = resolve_terrain_height_request(
            &[(-97.7431, 30.2672)],
            &Mutex::new(HashMap::new()),
            |missing| async move {
                fetch_terrain_chunk_with_retry(&missing, &options)
                    .await
                    .map(|r| r.into_iter().map(Some).collect())
                    .map_err(|e| e.message)
            },
            10_000,
            &now,
        )
        .await;
        assert_eq!(
            calls.lock().len(),
            4,
            "one initial attempt plus three retries"
        );
        assert_eq!(outcome.status, 502);
        assert!(outcome.body.get("results").is_none());
    }

    #[tokio::test]
    async fn stale_entries_serve_through_a_failed_refresh_only_when_every_point_is_real() {
        let cache = cache_of(vec![
            ((1.0, 1.0), 0, result("stale-a", 11.0)),
            ((2.0, 2.0), 0, result("stale-b", 22.0)),
        ]);
        let outcome = resolve_terrain_height_request(
            &[(2.0, 2.0), (1.0, 1.0)],
            &cache,
            |_| async { Err("offline".to_string()) },
            100,
            &fixed(1000),
        )
        .await;
        assert_eq!(outcome.status, 200);
        assert_eq!(ids(&outcome.body), vec!["stale-b", "stale-a"]);
    }

    #[tokio::test]
    async fn a_null_height_row_is_absent_not_a_failure_and_is_re_asked() {
        let cache = Mutex::new(HashMap::new());
        let calls = AtomicUsize::new(0);
        let points = [(-97.428, 40.194), (139.729, 35.693)];
        let fetch_missing = |missing: Vec<Point>| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(missing
                    .iter()
                    .map(|(lon, lat)| {
                        Some(if *lon == -97.428 {
                            json!({ "lon": lon, "lat": lat, "elevation": 443.855, "geoid": null, "ellipsoid": null })
                        } else {
                            result("real", 69.93)
                        })
                    })
                    .collect())
            }
        };
        let first = resolve_terrain_height_request(
            &points,
            &cache,
            fetch_missing,
            TTL_MS,
            &fixed(1_000_000),
        )
        .await;
        assert_eq!(first.upstream_error, None);
        assert_eq!(first.absent_points, 1);
        assert_eq!(first.requested_points, 2);
        assert_eq!(first.status, 502);
        assert!(
            !cache
                .lock()
                .contains_key(&terrain_point_key((-97.428, 40.194)))
        );
        assert_eq!(
            cache.lock()[&terrain_point_key((139.729, 35.693))].result["id"],
            json!("real")
        );
        resolve_terrain_height_request(&points, &cache, fetch_missing, TTL_MS, &fixed(1_000_000))
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_dropped_position_remains_a_genuine_upstream_fault() {
        let outcome = resolve_terrain_height_request(
            &[(1.0, 1.0), (2.0, 2.0)],
            &Mutex::new(HashMap::new()),
            |_| async { Ok(vec![Some(result("ok-a", 11.0))]) },
            10_000,
            &fixed(5_000),
        )
        .await;
        assert!(outcome.upstream_error.unwrap().contains("omitted"));
        assert_eq!(outcome.absent_points, 0);
    }

    #[tokio::test]
    async fn malformed_height_objects_remain_refresh_failures() {
        for row in [
            json!({}),
            json!([]),
            json!({ "ellipsoid": "0" }),
            json!({ "ellipsoid": null }),
        ] {
            let outcome = resolve_terrain_height_request(
                &[(1.0, 2.0)],
                &Mutex::new(HashMap::new()),
                |_| async move { Ok(vec![Some(row)]) },
                10_000,
                &system_clock(),
            )
            .await;
            assert_eq!(outcome.status, 502);
            assert!(outcome.upstream_error.is_some());
            assert_eq!(outcome.absent_points, 0);
            assert!(!outcome.cache_changed);
        }
    }

    fn upstream_points(request: &FetchRequest) -> Vec<Point> {
        let url = url::Url::parse(&request.url).unwrap();
        assert_eq!(
            url.origin().ascii_serialization(),
            "https://terrain.reearth.land"
        );
        let raw = url
            .query_pairs()
            .find(|(k, _)| k == "points")
            .unwrap()
            .1
            .into_owned();
        parse_terrain_points(Some(&raw)).unwrap()
    }

    fn harness(fetch: Fetcher, dir: &str) -> (Router<AppState>, PathBuf) {
        let cache_dir = testing::temp_dir(dir);
        let router = router_with(Deps {
            retry: RetryOptions::new(fetch, system_clock()),
            cache_dir: cache_dir.clone(),
            flush_interval: Duration::from_secs(3600),
        });
        (router, cache_dir)
    }

    async fn get(router: &Router<AppState>, query: &str) -> (u16, Value) {
        let (status, headers, body) = testing::send(
            router,
            &testing::state(),
            "GET",
            &format!("/api/terrain/heights{query}"),
            "127.0.0.1",
        )
        .await;
        assert_eq!(headers["content-type"], "application/json");
        assert!(headers.get("cache-control").is_none());
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn middleware_chunks_missing_points_and_reconstructs_reordered_requests() {
        let calls: Calls = Default::default();
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&sizes);
        let fetch = testing::fetcher(Arc::clone(&calls), move |request| {
            let points = upstream_points(request);
            seen.lock().push(points.len());
            Ok(FetchResponse::json(&json!({
                "results": points.iter().map(|(lon, _)| json!({ "ellipsoid": lon + 100.0 })).collect::<Vec<_>>()
            })))
        });
        let (router, _) = harness(fetch, "terrain-chunks");
        let points: Vec<String> = (0..257)
            .map(|i| format!("{},1", i as f64 / 100.0))
            .collect();
        assert_eq!(
            get(&router, &format!("/?points={}", points.join(";")))
                .await
                .0,
            200
        );
        assert_eq!(*sizes.lock(), vec![64, 64, 64, 64, 1]);
        let (_, reordered) = get(&router, "/?points=2.56,1;0,1;2.56,1").await;
        assert_eq!(
            reordered,
            json!({ "results": [{ "ellipsoid": 102.56 }, { "ellipsoid": 100.0 }, { "ellipsoid": 102.56 }] })
        );
        assert_eq!(calls.lock().len(), 5);
        let (status, body) = get(&router, "/?points=invalid").await;
        assert_eq!(status, 400);
        assert_eq!(
            body["error"],
            json!("invalid points parameter — expected \"lon,lat;lon,lat;…\" with finite numbers")
        );
        let many = vec!["0,1"; 2001].join(";");
        let (status, body) = get(&router, &format!("/?points={many}")).await;
        assert_eq!(status, 500);
        assert_eq!(
            body["error"],
            json!("too many points (2001); max 2000 per request")
        );
        assert_eq!(calls.lock().len(), 5);
    }

    #[tokio::test]
    async fn middleware_migrates_valid_legacy_disk_points_without_fabricating_heights() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |request| {
            let url = url::Url::parse(&request.url).unwrap();
            assert_eq!(
                url.query_pairs().find(|(k, _)| k == "points").unwrap().1,
                "3.00000,4.00000"
            );
            Ok(FetchResponse::json(
                &json!({ "results": [{ "ellipsoid": 88 }] }),
            ))
        });
        let (router, dir) = harness(fetch, "terrain-legacy");
        disk::write_json(
            &dir.join("terrain-heights.json"),
            &json!({ "1,2;3,4": { "at": crate::common::js::now_ms(), "results": [{ "ellipsoid": 77 }] } }),
        )
        .await
        .unwrap();
        let (status, body) = get(&router, "/?points=3,4;1,2").await;
        assert_eq!(status, 200);
        assert_eq!(
            body,
            json!({ "results": [{ "ellipsoid": 88 }, { "ellipsoid": 77 }] })
        );
        assert_eq!(calls.lock().len(), 1);
    }

    #[tokio::test]
    async fn middleware_retains_successful_chunks_around_a_failure_and_retries_only_missing_points()
    {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (seen, flag) = (Arc::clone(&requests), Arc::clone(&fail));
        let fetch = testing::fetcher(Default::default(), move |request| {
            let points = upstream_points(request);
            seen.lock().push(points.clone());
            if flag.load(Ordering::SeqCst) && points[0].0 == 0.64 {
                return Ok(FetchResponse::from_bytes(400, &[], ""));
            }
            Ok(FetchResponse::json(&json!({
                "results": points.iter().map(|(lon, _)| json!({ "ellipsoid": lon + 100.0 })).collect::<Vec<_>>()
            })))
        });
        let (router, _) = harness(fetch, "terrain-partial");
        let points: Vec<String> = (0..130)
            .map(|i| format!("{},1", i as f64 / 100.0))
            .collect();
        let url = format!("/?points={}", points.join(";"));
        assert_eq!(get(&router, &url).await.0, 502);
        let sizes: Vec<usize> = requests.lock().iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![64, 64, 2]);
        fail.store(false, Ordering::SeqCst);
        let (status, body) = get(&router, &url).await;
        assert_eq!(status, 200);
        let expected: Vec<Point> = (0..64).map(|i| ((i + 64) as f64 / 100.0, 1.0)).collect();
        assert_eq!(requests.lock()[3], expected);
        let results: Vec<Value> = (0..130)
            .map(|i| json!({ "ellipsoid": i as f64 / 100.0 + 100.0 }))
            .collect();
        assert_eq!(body["results"], Value::Array(results));
        assert_eq!(requests.lock().len(), 4);
    }
}
