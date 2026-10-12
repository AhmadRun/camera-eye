//! TomTom traffic-flow vector-tile proxy with a daily budget governor
//! (port of `server/providers/traffic.js` and the server half of
//! `src/data/tomtomTiles.js`).
//!
//! - `/api/tomtom/flow/{z}/{x}/{y}.pbf` → the uncompressed Mapbox Vector
//!   Tile (`relative` style), keyed server-side by `TOMTOM_API_KEY`
//! - `/api/tomtom/status` → `{hasKey, dailyCount, budget, date}`
//!
//! Memory + disk cache (`.ce-cache/tomtom/`), 120 s TTL, single-flight per
//! tile, serve-stale on failure. Upstream attempts count against a
//! persistent UTC-day budget (`TOMTOM_DAILY_TILE_BUDGET`, default 6000);
//! over it, stale tiles are served, else 429 `{error:'budget'}`.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use bytes::Bytes;
use parking_lot::Mutex;
use regex::Regex;
use serde_json::{Value, json};

use super::hazard_support::{
    Clock, FetchError, FetchRequest, Fetcher, encode_uri_component, jnum, js_number_string, mount,
    read_all, reqwest_fetcher, sub_url, system_clock, to_iso, with_deadline,
};
use crate::common::coalesce::Coalescer;
use crate::common::{disk, js, reply};
use crate::state::AppState;

/// Min supported TomTom flow-tile zoom.
pub const MIN_TILE_ZOOM: f64 = 8.0;
/// Max supported TomTom flow-tile zoom.
pub const MAX_TILE_ZOOM: f64 = 16.0;

const TILE_TTL_MS: i64 = 120_000;
/// 200,000/month free tier / 31 days = 6,451, rounded down for headroom.
const DEFAULT_DAILY_BUDGET: i64 = 6000;
const MEM_MAX_ENTRIES: usize = 256;
const UPSTREAM_TIMEOUT: Duration = Duration::from_millis(15_000);

static FLOW_PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^/flow/(\d+)/(\d+)/(\d+)\.pbf$").expect("regex"));

/// `isValidTileCoord(z, x, y)` with the default zoom range.
pub fn is_valid_tile_coord(z: f64, x: f64, y: f64) -> bool {
    let integer = |n: f64| n.is_finite() && n.fract() == 0.0;
    if !integer(z) || !integer(x) || !integer(y) {
        return false;
    }
    if !(MIN_TILE_ZOOM..=MAX_TILE_ZOOM).contains(&z) {
        return false;
    }
    let n = 2f64.powf(z);
    x >= 0.0 && x < n && y >= 0.0 && y < n
}

/// `utcDayKey(epochMs)`: `YYYY-MM-DD` in UTC.
pub fn utc_day_key(epoch_ms: i64) -> String {
    to_iso(epoch_ms).chars().take(10).collect()
}

/// The persisted daily counter.
#[derive(Debug, Clone, PartialEq)]
pub struct Budget {
    pub date: String,
    pub count: f64,
}

/// `normalizeBudget`: roll to zero on a new day; replace corrupt state.
pub fn normalize_budget(state: Option<Budget>, day_key: &str) -> Budget {
    match state {
        Some(state) if state.date == day_key && state.count.is_finite() && state.count >= 0.0 => {
            state
        }
        _ => Budget {
            date: day_key.to_string(),
            count: 0.0,
        },
    }
}

/// `isOverBudget`: a non-positive or invalid limit never blocks.
pub fn is_over_budget(state: &Budget, limit: f64) -> bool {
    if !limit.is_finite() || limit <= 0.0 {
        return false;
    }
    state.count >= limit
}

#[derive(Clone)]
pub struct Deps {
    pub fetch: Fetcher,
    pub now: Clock,
    pub cache_dir: PathBuf,
}

#[derive(Clone)]
struct TileEntry {
    at: i64,
    buf: Bytes,
}

/// Insertion-ordered tile memory (re-setting a key keeps its position,
/// like `Map.set`).
#[derive(Default)]
struct Memory {
    entries: HashMap<String, TileEntry>,
    order: VecDeque<String>,
}

impl Memory {
    fn set(&mut self, key: &str, entry: TileEntry) {
        if !self.entries.contains_key(key) {
            if self.entries.len() >= MEM_MAX_ENTRIES
                && let Some(oldest) = self.order.pop_front()
            {
                self.entries.remove(&oldest);
            }
            self.order.push_back(key.to_string());
        }
        self.entries.insert(key.to_string(), entry);
    }
}

struct Traffic {
    deps: Deps,
    mem: Mutex<Memory>,
    inflight: Coalescer<String, Option<TileEntry>>,
    budget: Mutex<Option<Budget>>,
    budget_loaded: tokio::sync::OnceCell<()>,
}

pub fn router(state: &AppState) -> Router<AppState> {
    router_with(Deps {
        fetch: reqwest_fetcher(),
        now: system_clock(),
        cache_dir: state.cache_dir.join("tomtom"),
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let proxy = Arc::new(Traffic {
        deps,
        mem: Mutex::new(Memory::default()),
        inflight: Coalescer::new(),
        budget: Mutex::new(None),
        budget_loaded: tokio::sync::OnceCell::new(),
    });
    mount(
        "/api/tomtom",
        move |State(state): State<AppState>, request: Request| {
            let proxy = Arc::clone(&proxy);
            async move { proxy.handle(&state, request).await }
        },
    )
}

fn send_json(status: u16, value: &Value) -> Response {
    reply::json(status, value, Some("no-store"))
}

fn send_tile(buf: Bytes, cache_status: &str) -> Response {
    reply::with_header(
        reply::raw(200, "application/x-protobuf", buf, Some("no-store")),
        "x-tomtom-cache",
        cache_status,
    )
}

fn daily_budget_limit(state: &AppState) -> i64 {
    js::parse_int(
        &state
            .env
            .raw("TOMTOM_DAILY_TILE_BUDGET")
            .unwrap_or_default(),
    )
    .filter(|n| *n > 0)
    .unwrap_or(DEFAULT_DAILY_BUDGET)
}

impl Traffic {
    fn budget_path(&self) -> PathBuf {
        self.deps.cache_dir.join("budget.json")
    }

    fn tile_path(&self, key: &str) -> PathBuf {
        self.deps
            .cache_dir
            .join(format!("flow-{}.pbf", key.replace('/', "-")))
    }

    async fn load_budget_once(&self) {
        self.budget_loaded
            .get_or_init(|| async {
                let Some(parsed) = disk::read_json::<Value>(&self.budget_path()).await else {
                    return;
                };
                let date = parsed.get("date").and_then(Value::as_str);
                let count = parsed.get("count").and_then(Value::as_f64);
                if let (Some(date), Some(count)) = (date, count) {
                    *self.budget.lock() = Some(Budget {
                        date: date.to_string(),
                        count,
                    });
                }
            })
            .await;
    }

    /// Roll the counter to today (UTC) and return it.
    fn current_budget(&self) -> Budget {
        let mut budget = self.budget.lock();
        let current = normalize_budget(budget.take(), &utc_day_key((self.deps.now)()));
        *budget = Some(current.clone());
        current
    }

    /// Count one upstream fetch attempt against today's budget.
    fn record_upstream_fetch(&self) {
        let snapshot = {
            self.current_budget();
            let mut budget = self.budget.lock();
            let current = budget.as_mut().expect("current budget");
            current.count += 1.0;
            json!({ "date": current.date, "count": jnum(current.count) })
        };
        let path = self.budget_path();
        tokio::spawn(async move {
            if let Err(error) = disk::write_json(&path, &snapshot).await {
                tracing::warn!("[tomtom-proxy] budget write failed: {error}");
            }
        });
    }

    async fn read_disk_tile(&self, key: &str) -> Option<TileEntry> {
        let path = self.tile_path(key);
        let (meta, buf) = tokio::join!(tokio::fs::metadata(&path), tokio::fs::read(&path));
        let at = meta
            .ok()?
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis() as i64;
        Some(TileEntry {
            at,
            buf: Bytes::from(buf.ok()?),
        })
    }

    async fn fetch_upstream(&self, api_key: &str, key: &str) -> Result<Bytes, FetchError> {
        let url = format!(
            "https://api.tomtom.com/traffic/map/4/tile/flow/relative/{key}.pbf?key={}",
            encode_uri_component(api_key)
        );
        // Attempts count — upstream bills the request either way.
        self.record_upstream_fetch();
        let buf = with_deadline(UPSTREAM_TIMEOUT, async {
            let response = (self.deps.fetch)(FetchRequest::get(url)).await?;
            if !response.ok() {
                return Err(FetchError::Network(format!("HTTP {}", response.status)));
            }
            read_all(response).await
        })
        .await?;
        if buf.is_empty() {
            return Err(FetchError::Network("empty tile body".into()));
        }
        Ok(buf)
    }

    async fn handle(self: Arc<Self>, state: &AppState, request: Request) -> Response {
        self.load_budget_once().await;
        let url = sub_url(&request, "/api/tomtom");
        let url_path = url.split('?').next().unwrap_or("");
        let api_key = state.env.raw("TOMTOM_API_KEY").unwrap_or_default();

        if url_path == "/status" {
            let b = self.current_budget();
            return send_json(
                200,
                &json!({
                    "hasKey": !api_key.is_empty(),
                    "dailyCount": jnum(b.count),
                    "budget": daily_budget_limit(state),
                    "date": b.date,
                }),
            );
        }

        let Some(m) = FLOW_PATH.captures(url_path) else {
            return send_json(404, &json!({ "error": "not_found" }));
        };
        let (z, x, y) = (js::number(&m[1]), js::number(&m[2]), js::number(&m[3]));
        if !is_valid_tile_coord(z, x, y) {
            return send_json(400, &json!({ "error": "invalid_tile" }));
        }
        if api_key.is_empty() {
            return send_json(503, &json!({ "error": "no_key" }));
        }

        let key = format!(
            "{}/{}/{}",
            js_number_string(z),
            js_number_string(x),
            js_number_string(y)
        );
        let now = (self.deps.now)();
        let cached = self.mem.lock().entries.get(&key).cloned();
        let entry = match cached {
            Some(entry) => Some(entry),
            None => {
                let disk = self.read_disk_tile(&key).await;
                if let Some(entry) = &disk {
                    self.mem.lock().set(&key, entry.clone());
                }
                disk
            }
        };
        // Fresh cache hit — never counts against the budget.
        if let Some(entry) = &entry
            && now - entry.at < TILE_TTL_MS
        {
            return send_tile(entry.buf.clone(), "HIT");
        }
        // Over the soft cap, last-good data beats a dead layer.
        if is_over_budget(&self.current_budget(), daily_budget_limit(state) as f64) {
            return match entry {
                Some(entry) => send_tile(entry.buf, "STALE-BUDGET"),
                None => send_json(429, &json!({ "error": "budget" })),
            };
        }
        // Stale or missing → refresh, single-flight per tile.
        let this = Arc::clone(&self);
        let tile_key = key.clone();
        let (fresh, _) = self
            .inflight
            .run(key.clone(), move || async move {
                tokio::spawn(async move {
                    match this.fetch_upstream(&api_key, &tile_key).await {
                        Ok(buf) => {
                            let fresh = TileEntry {
                                at: (this.deps.now)(),
                                buf,
                            };
                            this.mem.lock().set(&tile_key, fresh.clone());
                            if let Err(error) = disk::write_bytes(&this.tile_path(&tile_key), &fresh.buf).await {
                                tracing::warn!("[tomtom-proxy] tile cache write failed for {tile_key}: {error}");
                            }
                            Some(fresh)
                        }
                        Err(error) => {
                            tracing::warn!("[tomtom-proxy] {tile_key} fetch failed ({error}) — serving stale if any");
                            None
                        }
                    }
                })
                .await
                .ok()
                .flatten()
            })
            .await;
        match (fresh, entry) {
            (Some(fresh), _) => send_tile(fresh.buf, "MISS"),
            // Upstream down — stale beats empty.
            (None, Some(entry)) => send_tile(entry.buf, "STALE-ERROR"),
            (None, None) => send_json(502, &json!({ "error": "upstream" })),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::super::hazard_support::FetchResponse;
    use super::super::hazard_support::testing::{self, Calls};
    use super::*;

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, s)
            .unwrap()
            .and_utc()
            .timestamp_millis()
    }

    #[test]
    fn tile_coordinates_validate_zoom_range_and_bounds() {
        assert!(is_valid_tile_coord(8.0, 0.0, 0.0));
        assert!(is_valid_tile_coord(12.0, 935.0, 1686.0));
        assert!(is_valid_tile_coord(16.0, 65535.0, 65535.0));
        for z in [7.0, 17.0, -1.0] {
            assert!(!is_valid_tile_coord(z, 0.0, 0.0));
        }
        assert!(!is_valid_tile_coord(12.0, -1.0, 0.0));
        assert!(!is_valid_tile_coord(12.0, 0.0, -1.0));
        assert!(!is_valid_tile_coord(12.0, 4096.0, 0.0));
        assert!(!is_valid_tile_coord(12.0, 0.0, 4096.0));
        assert!(is_valid_tile_coord(12.0, 4095.0, 4095.0));
        assert!(!is_valid_tile_coord(12.5, 100.0, 100.0));
        assert!(!is_valid_tile_coord(12.0, 100.2, 100.0));
        assert!(!is_valid_tile_coord(12.0, 100.0, f64::NAN));
    }

    #[test]
    fn budget_accounting_rolls_and_caps() {
        assert_eq!(utc_day_key(utc(2026, 7, 16, 23, 59, 59)), "2026-07-16");
        assert_eq!(utc_day_key(utc(2026, 7, 17, 0, 0, 1)), "2026-07-17");
        let state = Budget {
            date: "2026-07-16".into(),
            count: 123.0,
        };
        assert_eq!(normalize_budget(Some(state.clone()), "2026-07-16"), state);
        let fresh = |d: &str| Budget {
            date: d.into(),
            count: 0.0,
        };
        assert_eq!(
            normalize_budget(
                Some(Budget {
                    date: "2026-07-16".into(),
                    count: 39999.0
                }),
                "2026-07-17"
            ),
            fresh("2026-07-17")
        );
        assert_eq!(normalize_budget(None, "2026-07-16"), fresh("2026-07-16"));
        assert_eq!(
            normalize_budget(
                Some(Budget {
                    date: "2026-07-16".into(),
                    count: f64::NAN
                }),
                "2026-07-16"
            ),
            fresh("2026-07-16")
        );
        assert_eq!(
            normalize_budget(
                Some(Budget {
                    date: "2026-07-16".into(),
                    count: -3.0
                }),
                "2026-07-16"
            ),
            fresh("2026-07-16")
        );
        let at = |count: f64| Budget {
            date: "x".into(),
            count,
        };
        assert!(!is_over_budget(&at(39999.0), 40000.0));
        assert!(is_over_budget(&at(40000.0), 40000.0));
        assert!(is_over_budget(&at(40001.0), 40000.0));
        assert!(!is_over_budget(&at(1e9), 0.0));
        assert!(!is_over_budget(&at(1e9), f64::NAN));
    }

    #[tokio::test]
    async fn middleware_preserves_keyless_mode_caching_stale_budget_and_utc_rollover() {
        let (clock, now) = testing::manual_clock(utc(2026, 9, 12, 12, 0, 0));
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |request| {
            let url = url::Url::parse(&request.url).unwrap();
            assert_eq!(
                url.query_pairs().find(|(k, _)| k == "key").unwrap().1,
                "fixture-key"
            );
            Ok(FetchResponse::from_bytes(200, &[], vec![1u8, 2, 3]))
        });
        let router = router_with(Deps {
            fetch,
            now: clock,
            cache_dir: testing::temp_dir("tomtom"),
        });
        let state = testing::state();
        state.env.set("TOMTOM_API_KEY", "");
        state.env.set("TOMTOM_DAILY_TILE_BUDGET", "1");
        let get = |uri: &'static str| testing::send(&router, &state, "GET", uri, "127.0.0.1");
        let json_of = |body: &Bytes| serde_json::from_slice::<Value>(body).unwrap();
        assert_eq!(
            json_of(&get("/api/tomtom/status").await.2)["hasKey"],
            json!(false)
        );
        let keyless = get("/api/tomtom/flow/8/1/1.pbf").await;
        assert_eq!(keyless.0, 503);
        assert_eq!(json_of(&keyless.2), json!({ "error": "no_key" }));
        assert_eq!(get("/api/tomtom/flow/7/1/1.pbf").await.0, 400);
        assert_eq!(get("/api/tomtom/elsewhere").await.0, 404);
        assert_eq!(calls.lock().len(), 0);
        state.env.set("TOMTOM_API_KEY", "fixture-key");
        let miss = get("/api/tomtom/flow/8/1/1.pbf").await;
        assert_eq!(miss.1["x-tomtom-cache"], "MISS");
        assert_eq!(miss.1["content-type"], "application/x-protobuf");
        assert_eq!(miss.1["cache-control"], "no-store");
        assert_eq!(miss.2, Bytes::from_static(&[1, 2, 3]));
        assert_eq!(
            get("/api/tomtom/flow/8/1/1.pbf").await.1["x-tomtom-cache"],
            "HIT"
        );
        let status = json_of(&get("/api/tomtom/status").await.2);
        assert_eq!(status["dailyCount"], json!(1));
        assert_eq!(status["budget"], json!(1));
        assert_eq!(status["date"], json!("2026-09-12"));
        assert_eq!(calls.lock().len(), 1);
        now.fetch_add(120_001, Ordering::SeqCst);
        assert_eq!(
            get("/api/tomtom/flow/8/1/1.pbf").await.1["x-tomtom-cache"],
            "STALE-BUDGET"
        );
        let over = get("/api/tomtom/flow/8/2/1.pbf").await;
        assert_eq!(over.0, 429);
        assert_eq!(json_of(&over.2), json!({ "error": "budget" }));
        assert_eq!(calls.lock().len(), 1);
        now.fetch_add(86_400_000, Ordering::SeqCst);
        assert_eq!(
            json_of(&get("/api/tomtom/status").await.2)["dailyCount"],
            json!(0)
        );
        assert_eq!(
            get("/api/tomtom/flow/8/1/1.pbf").await.1["x-tomtom-cache"],
            "MISS"
        );
        assert_eq!(calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn failed_refresh_serves_stale_tiles_or_a_sanitized_502() {
        let (clock, now) = testing::manual_clock(utc(2026, 9, 12, 12, 0, 0));
        let calls: Calls = Default::default();
        let counter = Arc::clone(&calls);
        let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
            if counter.lock().len() > 1 {
                return Ok(FetchResponse::from_bytes(500, &[], "secret"));
            }
            Ok(FetchResponse::from_bytes(200, &[], vec![9u8]))
        });
        let router = router_with(Deps {
            fetch,
            now: clock,
            cache_dir: testing::temp_dir("tomtom-stale"),
        });
        let state = testing::state();
        state.env.set("TOMTOM_API_KEY", "k");
        state.env.set("TOMTOM_DAILY_TILE_BUDGET", "");
        let get = |uri: &'static str| testing::send(&router, &state, "GET", uri, "127.0.0.1");
        assert_eq!(
            get("/api/tomtom/flow/10/1/1.pbf").await.1["x-tomtom-cache"],
            "MISS"
        );
        now.fetch_add(120_001, Ordering::SeqCst);
        assert_eq!(
            get("/api/tomtom/flow/10/1/1.pbf").await.1["x-tomtom-cache"],
            "STALE-ERROR"
        );
        let missing = get("/api/tomtom/flow/10/2/1.pbf").await;
        assert_eq!(missing.0, 502);
        assert_eq!(missing.2, Bytes::from_static(br#"{"error":"upstream"}"#));
        let status = serde_json::from_slice::<Value>(&get("/api/tomtom/status").await.2).unwrap();
        assert_eq!(status["budget"], json!(6000));
        assert_eq!(status["dailyCount"], json!(3));
    }
}
