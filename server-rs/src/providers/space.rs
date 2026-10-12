//! Satellite and launch feeds (port of `server/providers/space.js`,
//! `server/providers/space/*` and `src/data/spaceProviderRequests.js`).
//!
//! - `/api/celestrak/<group>`: CelesTrak GP/TLE text with a 6 h memory +
//!   disk cache, single-flight refresh and serve-stale on failure.
//! - `/api/launches`: the Launch Library 2 recent-launch feed with a 15 min
//!   memory + disk cache, single-flight refresh and serve-stale on failure.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::hazard_support::{
    Clock, FetchRequest, Fetcher, mount, read_all, read_text_capped, reqwest_fetcher, sub_url,
    system_clock, to_iso, with_deadline,
};
use crate::common::coalesce::Coalescer;
use crate::common::{disk, reply};
use crate::state::AppState;

const TLE_TTL_MS: i64 = 6 * 3_600_000;
const CELESTRAK_TIMEOUT: Duration = Duration::from_secs(20);
const CELESTRAK_USER_AGENT: &str =
    "camera-eye-celestrak-proxy/1.0 (+https://github.com/AhmadRun/camera-eye)";

/// `LL2_CACHE_TTL_MS`.
pub const LL2_CACHE_TTL_MS: i64 = 15 * 60_000;
const LL2_MAX_RESPONSE_BYTES: usize = 12 * 1024 * 1024;
const LL2_MAX_DISK_CACHE_BYTES: u64 = 24 * 1024 * 1024;
const LL2_TIMEOUT: Duration = Duration::from_secs(20);

/// `celestrakTleUrl(group)`.
pub fn celestrak_tle_url(group: &str) -> url::Url {
    let mut url =
        url::Url::parse("https://celestrak.org/NORAD/elements/gp.php").expect("fixed URL");
    url.query_pairs_mut()
        .append_pair("GROUP", group)
        .append_pair("FORMAT", "tle");
    url
}

/// `launchLibraryRecentUrl(end)`: the 30 days up to `end_ms`.
pub fn launch_library_recent_url(end_ms: i64) -> url::Url {
    let start = end_ms - 30 * 86_400_000;
    let mut url =
        url::Url::parse("https://ll.thespacedevs.com/2.3.0/launches/").expect("fixed URL");
    url.query_pairs_mut()
        .append_pair("net__gte", &to_iso(start))
        .append_pair("net__lte", &to_iso(end_ms))
        .append_pair("limit", "100")
        .append_pair("mode", "detailed");
    url
}

/// `launchLibraryRequestHeaders(token)`: never exposed client-side.
pub fn launch_library_request_headers(token: Option<&str>) -> Vec<(String, String)> {
    let mut headers = vec![("Accept".to_string(), "application/json".to_string())];
    let normalized = token.unwrap_or("").trim();
    if !normalized.is_empty() {
        headers.push(("Authorization".to_string(), format!("Token {normalized}")));
    }
    headers
}

/// A cached text body (`{ at, body }` on disk).
#[derive(Clone, Debug, PartialEq)]
struct TextEntry {
    at: i64,
    body: String,
}

impl TextEntry {
    fn to_json(&self) -> Value {
        json!({ "at": self.at, "body": self.body })
    }

    fn from_json(value: &Value) -> Option<Self> {
        let at = value.get("at")?.as_f64().filter(|n| n.is_finite())?;
        let body = value.get("body")?.as_str()?.to_string();
        Some(Self {
            at: at as i64,
            body,
        })
    }
}

/// Injectable dependencies (the JS tests swapped `fetch`, `Date` and `fsp`).
#[derive(Clone)]
pub struct Deps {
    pub fetch: Fetcher,
    pub now: Clock,
    pub cache_dir: PathBuf,
}

pub fn router(state: &AppState) -> Router<AppState> {
    router_with(Deps {
        fetch: reqwest_fetcher(),
        now: system_clock(),
        cache_dir: state.cache_dir.clone(),
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let celestrak = Arc::new(Celestrak {
        deps: deps.clone(),
        mem: Mutex::new(HashMap::new()),
        inflight: Coalescer::new(),
    });
    let launches = Arc::new(Launches {
        deps,
        cache: Mutex::new(None),
        disk_loaded: tokio::sync::OnceCell::new(),
        inflight: Coalescer::new(),
    });
    Router::new()
        .merge(mount("/api/celestrak", move |request: Request| {
            let celestrak = Arc::clone(&celestrak);
            async move { celestrak.handle(request).await }
        }))
        .merge(mount(
            "/api/launches",
            move |State(state): State<AppState>, request: Request| {
                let launches = Arc::clone(&launches);
                async move { launches.handle(&state, request).await }
            },
        ))
}

// ─── CelesTrak ───────────────────────────────────────────────

struct Celestrak {
    deps: Deps,
    mem: Mutex<HashMap<String, TextEntry>>,
    inflight: Coalescer<String, Option<TextEntry>>,
}

fn valid_group(group: &str) -> bool {
    !group.is_empty()
        && group
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// `/^1 /m`: some line starts with a TLE line-1 marker.
fn has_tle_line(body: &str) -> bool {
    body.starts_with("1 ")
        || ["\n1 ", "\r1 ", "\u{2028}1 ", "\u{2029}1 "]
            .iter()
            .any(|marker| body.contains(marker))
}

fn tle_reply(status: u16, body: String, cache_status: &str) -> Response {
    reply::with_header(
        reply::raw(status, "text/plain", body, None),
        "x-tle-cache",
        cache_status,
    )
}

impl Celestrak {
    fn disk_path(&self, group: &str) -> PathBuf {
        self.deps.cache_dir.join(format!("celestrak-{group}.json"))
    }

    async fn read_disk(&self, group: &str) -> Option<TextEntry> {
        let value: Value = disk::read_json(&self.disk_path(group)).await?;
        TextEntry::from_json(&value)
    }

    async fn handle(self: Arc<Self>, request: Request) -> Response {
        let url = sub_url(&request, "/api/celestrak");
        let without_slash = url.strip_prefix('/').unwrap_or(&url);
        let group = without_slash.split('?').next().unwrap_or("").to_string();
        if !valid_group(&group) {
            return reply::raw(400, "text/plain", "invalid group", None);
        }
        let now = (self.deps.now)();
        let cached = self.mem.lock().get(&group).cloned();
        let entry = match cached {
            Some(entry) => Some(entry),
            None => {
                let disk = self.read_disk(&group).await;
                if let Some(entry) = &disk {
                    self.mem.lock().insert(group.clone(), entry.clone());
                }
                disk
            }
        };
        if let Some(entry) = &entry
            && now - entry.at < TLE_TTL_MS
        {
            return tle_reply(200, entry.body.clone(), "HIT");
        }
        // Stale or missing → refresh, single-flight per group.
        let this = Arc::clone(&self);
        let key = group.clone();
        let (fresh, _) = self
            .inflight
            .run(group.clone(), move || async move {
                tokio::spawn(async move { this.refresh(&key).await })
                    .await
                    .ok()
                    .flatten()
            })
            .await;
        match (fresh, entry) {
            (Some(fresh), _) => tle_reply(200, fresh.body, "MISS"),
            // Upstream down — stale beats empty.
            (None, Some(entry)) => tle_reply(200, entry.body, "STALE-ERROR"),
            (None, None) => tle_reply(
                502,
                "celestrak fetch failed and no cache available".into(),
                "NONE",
            ),
        }
    }

    async fn refresh(&self, group: &str) -> Option<TextEntry> {
        match self.fetch_upstream(group).await {
            Ok(fresh) => {
                self.mem.lock().insert(group.to_string(), fresh.clone());
                if disk::write_json(&self.disk_path(group), &fresh.to_json())
                    .await
                    .is_err()
                {
                    tracing::warn!("[celestrak-proxy] cache write failed");
                }
                Some(fresh)
            }
            Err(()) => {
                tracing::warn!("[celestrak-proxy] refresh failed — serving cache if any");
                None
            }
        }
    }

    async fn fetch_upstream(&self, group: &str) -> Result<TextEntry, ()> {
        let request = FetchRequest::get(celestrak_tle_url(group).to_string())
            // CelesTrak 403s bulk groups unless the request carries a
            // descriptive User-Agent with a contact point.
            .header("User-Agent", CELESTRAK_USER_AGENT);
        let body = with_deadline(CELESTRAK_TIMEOUT, async {
            let response = (self.deps.fetch)(request).await?;
            if !response.ok() {
                return Err(super::hazard_support::FetchError::Network(format!(
                    "HTTP {}",
                    response.status
                )));
            }
            read_all(response).await
        })
        .await
        .map_err(|_| ())?;
        let body = String::from_utf8_lossy(&body).into_owned();
        // An upstream error page parses to zero TLEs — treat as failure.
        if !has_tle_line(&body) {
            return Err(());
        }
        Ok(TextEntry {
            at: (self.deps.now)(),
            body,
        })
    }
}

// ─── Launch Library 2 ────────────────────────────────────────

struct Launches {
    deps: Deps,
    cache: Mutex<Option<TextEntry>>,
    disk_loaded: tokio::sync::OnceCell<()>,
    inflight: Coalescer<&'static str, Result<TextEntry, u16>>,
}

fn launches_reply(status: u16, body: String, cache_state: &str) -> Response {
    let cache = if status == 200 {
        "public, max-age=900"
    } else {
        "no-store"
    };
    reply::with_header(
        reply::json_text(status, body, Some(cache)),
        "X-CE-Cache",
        cache_state,
    )
}

impl Launches {
    fn cache_path(&self) -> PathBuf {
        self.deps.cache_dir.join("launch-library-2-v2.3.json")
    }

    async fn load_disk_cache(&self) {
        self.disk_loaded
            .get_or_init(|| async {
                let path = self.cache_path();
                let Ok(meta) = tokio::fs::metadata(&path).await else {
                    return;
                };
                if meta.len() > LL2_MAX_DISK_CACHE_BYTES {
                    return;
                }
                let Some(value) = disk::read_json::<Value>(&path).await else {
                    return;
                };
                let Some(entry) = TextEntry::from_json(&value) else {
                    return;
                };
                let valid = serde_json::from_str::<Value>(&entry.body)
                    .ok()
                    .is_some_and(|body| body.get("results").is_some_and(Value::is_array));
                if valid {
                    *self.cache.lock() = Some(entry);
                }
            })
            .await;
    }

    async fn handle(self: Arc<Self>, state: &AppState, request: Request) -> Response {
        if request.method() != http::Method::GET {
            return launches_reply(
                405,
                json!({ "error": "Method Not Allowed" }).to_string(),
                "NONE",
            );
        }
        self.load_disk_cache().await;
        let now = (self.deps.now)();
        let stale = self.cache.lock().clone();
        if let Some(cache) = &stale
            && now - cache.at < LL2_CACHE_TTL_MS
        {
            return launches_reply(200, cache.body.clone(), "HIT");
        }
        let token = state.env.raw("LL2_API_TOKEN");
        let this = Arc::clone(&self);
        let (result, shared) = self
            .inflight
            .run("recent-launches", move || async move {
                tokio::spawn(async move { this.refresh_upstream(token.as_deref()).await })
                    .await
                    .unwrap_or(Err(502))
            })
            .await;
        match result {
            Ok(fresh) => launches_reply(200, fresh.body, if shared { "INFLIGHT" } else { "MISS" }),
            Err(status) => {
                // Log only a bounded status, never upstream bodies, URLs, or credentials.
                if !shared {
                    tracing::warn!(
                        "[launch-library-proxy] refresh failed (HTTP {status}){}",
                        if stale.is_some() {
                            " — serving stale cache"
                        } else {
                            ""
                        }
                    );
                }
                if let Some(stale) = stale {
                    return launches_reply(200, stale.body, "STALE-ERROR");
                }
                launches_reply(
                    status,
                    json!({ "error": "Launch Library 2 unavailable" }).to_string(),
                    "NONE",
                )
            }
        }
    }

    /// The fresh entry, or the status to answer with (the upstream's own
    /// HTTP status, else 502).
    async fn refresh_upstream(&self, token: Option<&str>) -> Result<TextEntry, u16> {
        let url = launch_library_recent_url((self.deps.now)());
        let mut request = FetchRequest::get(url.to_string());
        request.headers = launch_library_request_headers(token);
        let (status, body) = with_deadline(LL2_TIMEOUT, async {
            let response = (self.deps.fetch)(request).await?;
            let status = response.status;
            let ok = response.ok();
            let body = read_text_capped(response, LL2_MAX_RESPONSE_BYTES).await?;
            Ok((if ok { None } else { Some(status) }, body))
        })
        .await
        .map_err(|_| 502u16)?;
        if let Some(status) = status {
            return Err(status);
        }
        let parsed: Value = serde_json::from_str(&body).map_err(|_| 502u16)?;
        if !parsed.get("results").is_some_and(Value::is_array) {
            return Err(502);
        }
        let fresh = TextEntry {
            at: (self.deps.now)(),
            body,
        };
        *self.cache.lock() = Some(fresh.clone());
        let path = self.cache_path();
        let saved = fresh.to_json();
        tokio::spawn(async move {
            if disk::write_json(&path, &saved).await.is_err() {
                tracing::warn!("[launch-library-proxy] cache write failed");
            }
        });
        Ok(fresh)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::super::hazard_support::testing::{self, Calls};
    use super::super::hazard_support::{FetchError, FetchResponse};
    use super::*;

    const DETAIL: &str = "fixture-secret-token /internal/example <html>";

    fn deps(fetch: Fetcher, now: Clock, dir: &str) -> Deps {
        Deps {
            fetch,
            now,
            cache_dir: testing::temp_dir(dir),
        }
    }

    #[test]
    fn portable_requests_keep_fixed_origins_and_the_30_day_utc_window() {
        let tle = celestrak_tle_url("stations&FORMAT=json");
        assert_eq!(tle.origin().ascii_serialization(), "https://celestrak.org");
        assert_eq!(tle.path(), "/NORAD/elements/gp.php");
        let pairs: Vec<(String, String)> = tle.query_pairs().into_owned().collect();
        assert_eq!(
            pairs,
            vec![
                ("GROUP".into(), "stations&FORMAT=json".into()),
                ("FORMAT".into(), "tle".into())
            ]
        );
        let end = super::super::hazard_support::parse_date("2026-03-01T12:34:56.000Z").unwrap();
        let url = launch_library_recent_url(end);
        assert_eq!(
            url.origin().ascii_serialization(),
            "https://ll.thespacedevs.com"
        );
        assert_eq!(url.path(), "/2.3.0/launches/");
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        assert_eq!(
            pairs,
            vec![
                ("net__gte".into(), "2026-01-30T12:34:56.000Z".into()),
                ("net__lte".into(), "2026-03-01T12:34:56.000Z".into()),
                ("limit".into(), "100".into()),
                ("mode".into(), "detailed".into()),
            ]
        );
    }

    #[test]
    fn ll2_headers_carry_an_optional_trimmed_token() {
        assert_eq!(
            launch_library_request_headers(Some(" fixture-token ")),
            vec![
                ("Accept".to_string(), "application/json".to_string()),
                (
                    "Authorization".to_string(),
                    "Token fixture-token".to_string()
                )
            ]
        );
        assert_eq!(launch_library_request_headers(Some("  ")).len(), 1);
        assert_eq!(launch_library_request_headers(None).len(), 1);
        assert_eq!(LL2_CACHE_TTL_MS, 15 * 60_000);
    }

    #[tokio::test]
    async fn celestrak_coalesces_retains_stale_tles_and_reads_disk_in_a_new_instance() {
        let tle = "ISS\n1 25544U fixture\n2 25544 fixture";
        let (clock, now) = testing::manual_clock(1_000_000_000);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let calls: Calls = Default::default();
        let mode = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let fetch: Fetcher = {
            let gate = Arc::clone(&gate);
            let calls = Arc::clone(&calls);
            let mode = Arc::clone(&mode);
            Arc::new(move |request: FetchRequest| {
                calls.lock().push(request.clone());
                let gate = Arc::clone(&gate);
                let mode = mode.load(Ordering::SeqCst);
                Box::pin(async move {
                    let url = url::Url::parse(&request.url).unwrap();
                    assert_eq!(
                        url.query_pairs().find(|(k, _)| k == "GROUP").unwrap().1,
                        "stations"
                    );
                    assert_eq!(
                        request.header_value("User-Agent"),
                        Some(CELESTRAK_USER_AGENT)
                    );
                    match mode {
                        0 => {
                            let _ = gate.acquire().await;
                            Ok(FetchResponse::from_bytes(200, &[], tle))
                        }
                        1 => Ok(FetchResponse::from_bytes(200, &[], "not a TLE")),
                        _ => Err(FetchError::Network("fresh disk must prevent fetch".into())),
                    }
                })
            })
        };
        let d = deps(fetch, clock, "celestrak");
        let router = router_with(d.clone());
        let state = testing::state();
        let (status, _, _) =
            testing::send(&router, &state, "GET", "/api/celestrak/..bad", "127.0.0.1").await;
        assert_eq!(status, 400);
        assert_eq!(calls.lock().len(), 0);
        let first = testing::send(
            &router,
            &state,
            "GET",
            "/api/celestrak/stations",
            "127.0.0.1",
        );
        let second = testing::send(
            &router,
            &state,
            "GET",
            "/api/celestrak/stations",
            "127.0.0.1",
        );
        let release = async {
            while calls.lock().is_empty() {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            gate.add_permits(100);
        };
        let (a, b, ()) = tokio::join!(first, second, release);
        assert_eq!(a.2, tle);
        assert_eq!(b.2, tle);
        assert_eq!(calls.lock().len(), 1);
        let hit = testing::send(
            &router,
            &state,
            "GET",
            "/api/celestrak/stations",
            "127.0.0.1",
        )
        .await;
        assert_eq!(hit.1["x-tle-cache"], "HIT");
        assert_eq!(hit.1["content-type"], "text/plain");
        now.fetch_add(6 * 3_600_000, Ordering::SeqCst);
        mode.store(1, Ordering::SeqCst);
        let stale = testing::send(
            &router,
            &state,
            "GET",
            "/api/celestrak/stations",
            "127.0.0.1",
        )
        .await;
        assert_eq!(stale.0, 200);
        assert_eq!(stale.2, tle);
        assert_eq!(stale.1["x-tle-cache"], "STALE-ERROR");
        // A new instance reads a fresh disk entry without fetching.
        let disk_entry = TextEntry {
            at: now.load(Ordering::SeqCst),
            body: tle.into(),
        };
        disk::write_json(
            &d.cache_dir.join("celestrak-stations.json"),
            &disk_entry.to_json(),
        )
        .await
        .unwrap();
        mode.store(2, Ordering::SeqCst);
        let fresh_router = router_with(d.clone());
        let disk_hit = testing::send(
            &fresh_router,
            &state,
            "GET",
            "/api/celestrak/stations",
            "127.0.0.1",
        )
        .await;
        assert_eq!(disk_hit.1["x-tle-cache"], "HIT");
        assert_eq!(disk_hit.2, tle);
    }

    #[tokio::test]
    async fn celestrak_retains_invalid_group_and_unavailable_responses() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(calls, |_| Err(FetchError::Network(DETAIL.into())));
        let router = router_with(deps(fetch, system_clock(), "celestrak-down"));
        let state = testing::state();
        let invalid =
            testing::send(&router, &state, "GET", "/api/celestrak/a.b", "127.0.0.1").await;
        assert_eq!(invalid.0, 400);
        assert_eq!(invalid.2, "invalid group");
        let res = testing::send(&router, &state, "GET", "/api/celestrak/active", "127.0.0.1").await;
        assert_eq!(res.0, 502);
        assert_eq!(res.1["x-tle-cache"], "NONE");
        assert_eq!(res.2, "celestrak fetch failed and no cache available");
    }

    #[tokio::test]
    async fn celestrak_retains_fresh_and_stale_caches() {
        let (clock, now) = testing::manual_clock(1_000_000_000);
        let calls: Calls = Default::default();
        let counter = Arc::clone(&calls);
        let fetch = testing::fetcher(calls, move |_| {
            if counter.lock().len() > 1 {
                return Err(FetchError::Network(DETAIL.into()));
            }
            Ok(FetchResponse::from_bytes(200, &[], "1 valid-fixture-TLE"))
        });
        let router = router_with(deps(fetch, clock, "celestrak-stale"));
        let state = testing::state();
        let get = || testing::send(&router, &state, "GET", "/api/celestrak/active", "127.0.0.1");
        assert_eq!(get().await.1["x-tle-cache"], "MISS");
        assert_eq!(get().await.1["x-tle-cache"], "HIT");
        now.fetch_add(7 * 3_600_000, Ordering::SeqCst);
        let stale = get().await;
        assert_eq!(stale.0, 200);
        assert_eq!(stale.2, "1 valid-fixture-TLE");
        assert_eq!(stale.1["x-tle-cache"], "STALE-ERROR");
    }

    fn launch_router(fetch: Fetcher, now: Clock, dir: &str) -> Router<AppState> {
        router_with(deps(fetch, now, dir))
    }

    #[tokio::test]
    async fn launch_library_error_statuses_stay_generic() {
        for status in [401u16, 429, 500] {
            let fetch = testing::fetcher(Default::default(), move |_| {
                Ok(FetchResponse::from_bytes(status, &[], DETAIL.repeat(1000)))
            });
            let router = launch_router(fetch, system_clock(), "ll2-status");
            let res = testing::send(
                &router,
                &testing::state(),
                "GET",
                "/api/launches",
                "127.0.0.1",
            )
            .await;
            assert_eq!(res.0, status);
            assert_eq!(
                serde_json::from_slice::<Value>(&res.2).unwrap(),
                json!({ "error": "Launch Library 2 unavailable" })
            );
            assert_eq!(res.1["cache-control"], "no-store");
            assert_eq!(res.1["x-ce-cache"], "NONE");
        }
    }

    #[tokio::test]
    async fn launch_library_failures_return_502() {
        #[allow(clippy::type_complexity)]
        let cases: Vec<
            Box<dyn Fn(&FetchRequest) -> Result<FetchResponse, FetchError> + Send + Sync>,
        > = vec![
            Box::new(|_| Err(FetchError::Network(DETAIL.into()))),
            Box::new(|_| Ok(FetchResponse::from_bytes(200, &[], DETAIL))),
            Box::new(|_| Ok(FetchResponse::from_bytes(200, &[], "{}"))),
            Box::new(|_| {
                Ok(FetchResponse::from_bytes(
                    200,
                    &[("content-length", &(13 * 1024 * 1024).to_string())],
                    DETAIL,
                ))
            }),
        ];
        for case in cases {
            let fetch = testing::fetcher(Default::default(), case);
            let router = launch_router(fetch, system_clock(), "ll2-fail");
            let res = testing::send(
                &router,
                &testing::state(),
                "GET",
                "/api/launches",
                "127.0.0.1",
            )
            .await;
            assert_eq!(res.0, 502);
            assert!(!String::from_utf8_lossy(&res.2).contains("fixture-secret-token"));
        }
    }

    #[tokio::test]
    async fn launch_library_single_flight_fresh_cache_stale_fallback_and_method_guard() {
        let (clock, now) = testing::manual_clock(1_000_000_000);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch: Fetcher = {
            let gate = Arc::clone(&gate);
            let calls = Arc::clone(&calls);
            Arc::new(move |_request: FetchRequest| {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let gate = Arc::clone(&gate);
                Box::pin(async move {
                    if n == 1 {
                        let _ = gate.acquire().await;
                        Ok(FetchResponse::from_bytes(200, &[], r#"{"results":[]}"#))
                    } else {
                        Err(FetchError::Network(DETAIL.into()))
                    }
                })
            })
        };
        let router = launch_router(fetch, clock, "ll2-flight");
        let state = testing::state();
        assert_eq!(
            testing::send(&router, &state, "POST", "/api/launches", "127.0.0.1")
                .await
                .0,
            405
        );
        let first = testing::send(&router, &state, "GET", "/api/launches", "127.0.0.1");
        let second = testing::send(&router, &state, "GET", "/api/launches", "127.0.0.1");
        let release = async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            gate.add_permits(100);
        };
        let (a, b, ()) = tokio::join!(first, second, release);
        let mut states = vec![
            a.1["x-ce-cache"].to_str().unwrap().to_string(),
            b.1["x-ce-cache"].to_str().unwrap().to_string(),
        ];
        states.sort();
        assert_eq!(states, vec!["INFLIGHT", "MISS"]);
        assert_eq!(a.1["cache-control"], "public, max-age=900");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let hit = testing::send(&router, &state, "GET", "/api/launches", "127.0.0.1").await;
        assert_eq!(hit.1["x-ce-cache"], "HIT");
        now.fetch_add(16 * 60_000, Ordering::SeqCst);
        let stale = testing::send(&router, &state, "GET", "/api/launches", "127.0.0.1").await;
        assert_eq!(stale.0, 200);
        assert_eq!(stale.2, r#"{"results":[]}"#);
        assert_eq!(stale.1["x-ce-cache"], "STALE-ERROR");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn launch_plugin_preserves_optional_server_auth_and_cache() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |request| {
            let url = url::Url::parse(&request.url).unwrap();
            assert_eq!(
                url.query_pairs().find(|(k, _)| k == "limit").unwrap().1,
                "100"
            );
            assert_eq!(
                request.header_value("Authorization"),
                Some("Token fixture-token")
            );
            Ok(FetchResponse::json(
                &json!({ "results": [{ "id": "launch-fixture" }] }),
            ))
        });
        let router = launch_router(fetch, system_clock(), "ll2-auth");
        let state = testing::state();
        state.env.set("LL2_API_TOKEN", " fixture-token ");
        assert_eq!(
            testing::send(&router, &state, "POST", "/api/launches", "127.0.0.1")
                .await
                .0,
            405
        );
        let first = testing::send(&router, &state, "GET", "/api/launches", "127.0.0.1").await;
        assert_eq!(first.1["x-ce-cache"], "MISS");
        assert!(!String::from_utf8_lossy(&first.2).contains("fixture-token"));
        let hit = testing::send(&router, &state, "GET", "/api/launches", "127.0.0.1").await;
        assert_eq!(hit.1["x-ce-cache"], "HIT");
        assert_eq!(hit.2, first.2);
        assert_eq!(calls.lock().len(), 1);
    }
}
