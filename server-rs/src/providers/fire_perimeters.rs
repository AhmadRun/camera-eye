//! NIFC WFIGS fire perimeters and InciWeb incident context (port of
//! `server/providers/firePerimeters.js` and `src/layers/perimeters/records.js`).
//!
//! - `GET /api/fire-perimeters` → `{fetchedAt, rows}` (5 min TTL)
//! - `GET /api/fire-perimeters/inciweb/index` → InciWeb rows (1 h TTL)
//! - `GET /api/fire-perimeters/inciweb/publication/<id>` →
//!   `{createdMs, changedMs}` (30 min TTL, 256 retained)
//!
//! Fixed origins, bounded bodies, shared in-flight work and serve-stale on
//! upstream failure.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::Router;
use axum::extract::{ConnectInfo, Request};
use axum::response::Response;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use regex::Regex;
use serde_json::{Map, Value, json};

use super::hazard_support::{
    Clock, FetchError, FetchRequest, Fetcher, Redirect, finite_number, jnum, js_value_string,
    mount, parse_date, read_bytes_capped, read_text_capped, reqwest_fetcher, sub_url, system_clock,
    with_deadline,
};
use crate::common::rate_limit::{RateLimiter, client_key};
use crate::common::reply;
use crate::state::AppState;

const MIB: usize = 1024 * 1024;
/// The service caps one response at 2000 records; follow
/// `exceededTransferLimit` with offset pages, at most this many.
const MAX_PAGES: usize = 5;
const OUT_FIELDS: [&str; 15] = [
    "poly_IncidentName",
    "attr_UniqueFireIdentifier",
    "attr_IncidentSize",
    "attr_PercentContained",
    "attr_POOState",
    "attr_IncidentTypeCategory",
    "attr_FireDiscoveryDateTime",
    "poly_DateCurrent",
    "attr_FireCause",
    "attr_FireBehaviorGeneral",
    "attr_TotalIncidentPersonnel",
    "attr_POOCounty",
    "attr_EstimatedCostToDate",
    "attr_IncidentComplexityLevel",
    "attr_CpxName",
];

/// NIFC WFIGS current interagency fire perimeters (public, keyless).
static API_URL: LazyLock<String> = LazyLock::new(|| {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("where", "1=1")
        .append_pair("outFields", &OUT_FIELDS.join(","))
        .append_pair("maxAllowableOffset", "0.001")
        .append_pair("outSR", "4326")
        .append_pair("f", "geojson")
        .finish();
    format!(
        "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_Current/FeatureServer/0/query?{query}"
    )
});
const INCIWEB_INDEX_URL: &str = "https://inciweb.wildfire.gov/api/single-publication/";

static UPDATED_TIME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"<meta\s+property="og:updated_time"\s+content="([^"]+)""#).expect("regex")
});
static ORIGIN_TIME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"Date of Origin</th>\s*<td[^>]*>\s*<time\s+datetime="([^"]+)""#).expect("regex")
});
static REDIRECT_PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^/[a-z0-9-]+(?:/[a-z0-9-]+)?$").expect("regex"));

// ─── Normalization (src/layers/perimeters/records.js) ───────

fn valid_ring(ring: &Value) -> bool {
    let Some(ring) = ring.as_array() else {
        return false;
    };
    if ring.len() < 4 {
        return false;
    }
    ring.iter().all(|position| {
        let Some(position) = position.as_array().filter(|p| p.len() >= 2) else {
            return false;
        };
        let lon = finite_number(position.first());
        let lat = finite_number(position.get(1));
        lon.is_some_and(|lon| lon.abs() <= 180.0) && lat.is_some_and(|lat| lat.abs() <= 90.0)
    })
}

/// Polygon/MultiPolygon → polygons (each an array of rings); `None` on
/// malformed geometry, empty when there is nothing to draw.
fn normalize_polygons(geometry: Option<&Value>) -> Option<Vec<Value>> {
    let geometry = geometry?.as_object()?;
    let polygons: Vec<&Value> = match geometry.get("type").and_then(Value::as_str) {
        Some("Polygon") => vec![geometry.get("coordinates").unwrap_or(&Value::Null)],
        Some("MultiPolygon") => geometry
            .get("coordinates")
            .and_then(Value::as_array)?
            .iter()
            .collect(),
        _ => return None,
    };
    let mut result = Vec::new();
    for rings in polygons {
        let list = rings.as_array()?;
        if list.is_empty() {
            continue;
        }
        if !list.iter().all(valid_ring) {
            return None;
        }
        result.push(rings.clone());
    }
    Some(result)
}

/// `normalizeFirePerimeterSnapshot`: rows, skipping individually invalid
/// features; `None` only when the payload is not a feature collection.
pub fn normalize_fire_perimeter_snapshot(geojson: &Value) -> Option<Vec<Value>> {
    let features = geojson.get("features")?.as_array()?;
    let mut rows = Vec::new();
    let mut ids = HashSet::new();
    for feature in features {
        let Some(properties) = feature.get("properties").and_then(Value::as_object) else {
            continue;
        };
        let Some(polygons) = normalize_polygons(feature.get("geometry")) else {
            continue;
        };
        if polygons.is_empty() {
            continue;
        }
        let stable_id = match properties.get("attr_UniqueFireIdentifier") {
            Some(Value::String(id)) if !id.is_empty() => Some(id.clone()),
            _ => match feature.get("id") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) if s.is_empty() => None,
                Some(id) => Some(js_value_string(id)),
            },
        };
        let Some(stable_id) = stable_id else {
            continue;
        };
        if !ids.insert(stable_id.clone()) {
            continue;
        }
        let text = |key: &str| {
            properties
                .get(key)
                .and_then(Value::as_str)
                .map_or(Value::Null, Value::from)
        };
        let number = |key: &str| finite_number(properties.get(key)).map_or(Value::Null, jnum);
        let mut row = Map::new();
        row.insert("stableId".into(), Value::from(stable_id));
        row.insert("name".into(), text("poly_IncidentName"));
        row.insert("acres".into(), number("attr_IncidentSize"));
        row.insert("containedPct".into(), number("attr_PercentContained"));
        row.insert("state".into(), text("attr_POOState"));
        row.insert("category".into(), text("attr_IncidentTypeCategory"));
        row.insert(
            "discoveredTime".into(),
            number("attr_FireDiscoveryDateTime"),
        );
        row.insert("updatedTime".into(), number("poly_DateCurrent"));
        row.insert("cause".into(), text("attr_FireCause"));
        row.insert("behavior".into(), text("attr_FireBehaviorGeneral"));
        row.insert("personnel".into(), number("attr_TotalIncidentPersonnel"));
        row.insert("county".into(), text("attr_POOCounty"));
        row.insert("costToDate".into(), number("attr_EstimatedCostToDate"));
        row.insert("complexity".into(), text("attr_IncidentComplexityLevel"));
        row.insert("complexName".into(), text("attr_CpxName"));
        row.insert("polygons".into(), Value::Array(polygons));
        rows.push(Value::Object(row));
    }
    Some(rows)
}

// ─── Proxy ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct Deps {
    pub fetch: Fetcher,
    pub now: Clock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Busy,
    Unavailable,
}

/// A retained value per key, oldest first (the JS `Map` insertion order).
#[derive(Default)]
struct Store {
    entries: HashMap<String, (Value, i64)>,
    order: VecDeque<String>,
}

impl Store {
    fn get(&self, key: &str) -> Option<(Value, i64)> {
        self.entries.get(key).cloned()
    }

    fn set(&mut self, key: &str, value: Value, saved_at: i64) {
        if self.entries.remove(key).is_some() {
            self.order.retain(|k| k != key);
        }
        self.entries.insert(key.to_string(), (value, saved_at));
        self.order.push_back(key.to_string());
    }

    fn evict_oldest(&mut self) {
        if let Some(oldest) = self.order.pop_front() {
            self.entries.remove(&oldest);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StoreKind {
    Cache,
    Publications,
}

type SharedLoad = Shared<BoxFuture<'static, Result<Value, ()>>>;

struct Perimeters {
    deps: Deps,
    cache: Mutex<Store>,
    publications: Mutex<Store>,
    in_flight: Mutex<HashMap<String, SharedLoad>>,
    limiter: RateLimiter,
}

pub fn router(_state: &AppState) -> Router<AppState> {
    router_with(Deps {
        fetch: reqwest_fetcher(),
        now: system_clock(),
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let proxy = Arc::new(Perimeters {
        deps,
        cache: Mutex::new(Store::default()),
        publications: Mutex::new(Store::default()),
        in_flight: Mutex::new(HashMap::new()),
        limiter: RateLimiter::new(60_000, 60, Some(1200)),
    });
    mount("/api/fire-perimeters", move |request: Request| {
        let proxy = Arc::clone(&proxy);
        async move { proxy.handle(request).await }
    })
}

fn json_reply(status: u16, value: &Value, stale: bool) -> Response {
    let mut response = reply::json(status, value, Some("no-store"));
    if status == 405 {
        response = reply::with_header(response, "Allow", "GET");
    }
    if status == 429 {
        response = reply::with_header(response, "Retry-After", "60");
    }
    if stale {
        response = reply::with_header(response, "X-Data-Stale", "true");
    }
    response
}

#[derive(Clone)]
enum Route {
    Perimeters,
    Index,
    Publication(String),
}

impl Perimeters {
    async fn handle(self: Arc<Self>, request: Request) -> Response {
        if request.method() != http::Method::GET {
            return json_reply(405, &json!({ "error": "method_not_allowed" }), false);
        }
        let url = sub_url(&request, "/api/fire-perimeters");
        let path = url.split('?').next().unwrap_or("");
        let route = if path == "/" || path.is_empty() {
            Route::Perimeters
        } else if path == "/inciweb/index" {
            Route::Index
        } else if let Some(id) = path.strip_prefix("/inciweb/publication/") {
            if id.is_empty() || id.len() > 9 || !id.bytes().all(|b| b.is_ascii_digit()) {
                return json_reply(400, &json!({ "error": "invalid_publication_id" }), false);
            }
            Route::Publication(id.to_string())
        } else {
            return json_reply(404, &json!({ "error": "unknown_route" }), false);
        };
        let peer = request
            .extensions()
            .get::<ConnectInfo<std::net::SocketAddr>>()
            .map(|c| c.0);
        if !self.limiter.allow(&client_key(peer)) {
            return json_reply(429, &json!({ "error": "rate_limited" }), false);
        }
        let (key, ttl, kind) = match &route {
            Route::Perimeters => ("perimeters".to_string(), 300_000, StoreKind::Cache),
            Route::Index => ("index".to_string(), 3_600_000, StoreKind::Cache),
            Route::Publication(id) => (
                format!("publication:{id}"),
                1_800_000,
                StoreKind::Publications,
            ),
        };
        match self.acquire(&key, ttl, route.clone(), kind).await {
            Ok((value, stale)) => {
                let value = match (&route, stale, value) {
                    (Route::Perimeters, true, Value::Object(mut object)) => {
                        object.insert("stale".into(), Value::Bool(true));
                        Value::Object(object)
                    }
                    (_, _, value) => value,
                };
                json_reply(200, &value, stale)
            }
            Err(failure) => json_reply(
                if failure == Failure::Busy { 429 } else { 502 },
                &json!({ "error": "fire_perimeters_unavailable" }),
                false,
            ),
        }
    }

    fn store(&self, kind: StoreKind) -> &Mutex<Store> {
        match kind {
            StoreKind::Cache => &self.cache,
            StoreKind::Publications => &self.publications,
        }
    }

    async fn acquire(
        self: &Arc<Self>,
        key: &str,
        ttl: i64,
        route: Route,
        kind: StoreKind,
    ) -> Result<(Value, bool), Failure> {
        let previous = self.store(kind).lock().get(key);
        if let Some((value, saved_at)) = &previous
            && (self.deps.now)() - saved_at < ttl
        {
            return Ok((value.clone(), false));
        }
        let shared = {
            let mut in_flight = self.in_flight.lock();
            if let Some(existing) = in_flight.get(key) {
                existing.clone()
            } else {
                // Bound distinct concurrent requests as well as retained entries.
                if in_flight.len() >= 256 {
                    return Err(Failure::Busy);
                }
                let this = Arc::clone(self);
                let owned_key = key.to_string();
                let task = tokio::spawn(async move {
                    let result = this.load(&route).await;
                    if let Ok(value) = &result {
                        let mut store = this.store(kind).lock();
                        store.set(&owned_key, value.clone(), (this.deps.now)());
                        if kind == StoreKind::Publications && store.entries.len() > 256 {
                            store.evict_oldest();
                        }
                    }
                    this.in_flight.lock().remove(&owned_key);
                    result
                });
                let shared = async move { task.await.unwrap_or(Err(())) }
                    .boxed()
                    .shared();
                in_flight.insert(key.to_string(), shared.clone());
                shared
            }
        };
        match shared.await {
            Ok(value) => Ok((value, false)),
            Err(()) => previous
                .map(|(value, _)| (value, true))
                .ok_or(Failure::Unavailable),
        }
    }

    async fn load(&self, route: &Route) -> Result<Value, ()> {
        match route {
            Route::Perimeters => self.fetch_perimeters().await,
            Route::Index => self.fetch_index().await,
            Route::Publication(id) => self.fetch_publication(id).await,
        }
    }

    /// A bounded JSON read from a fixed URL that must not redirect.
    async fn upstream_json(
        &self,
        request: FetchRequest,
        cap: usize,
        timeout: Duration,
    ) -> Result<Value, ()> {
        let request = request.redirect(Redirect::Error);
        let bytes = with_deadline(timeout, async {
            let response = (self.deps.fetch)(request).await?;
            if !response.ok() {
                return Err(FetchError::Network("upstream_unavailable".into()));
            }
            read_bytes_capped(response, cap).await
        })
        .await
        .map_err(|_| ())?;
        serde_json::from_slice(&bytes).map_err(|_| ())
    }

    async fn fetch_perimeters(&self) -> Result<Value, ()> {
        let mut features: Vec<Value> = Vec::new();
        for page in 0..MAX_PAGES {
            let url = if page == 0 {
                API_URL.clone()
            } else {
                format!("{}&resultOffset={}", *API_URL, features.len())
            };
            let payload = self
                .upstream_json(FetchRequest::get(url), 16 * MIB, Duration::from_secs(30))
                .await?;
            let Some(page_features) = payload.get("features").and_then(Value::as_array) else {
                return Err(());
            };
            let exceeded = payload.get("exceededTransferLimit") == Some(&Value::Bool(true))
                || payload.pointer("/properties/exceededTransferLimit") == Some(&Value::Bool(true));
            let empty = page_features.is_empty();
            features.extend(page_features.iter().cloned());
            if !exceeded || empty {
                break;
            }
        }
        let rows =
            normalize_fire_perimeter_snapshot(&json!({ "features": features })).unwrap_or_default();
        Ok(json!({ "fetchedAt": (self.deps.now)(), "rows": rows }))
    }

    async fn fetch_index(&self) -> Result<Value, ()> {
        let mut request =
            FetchRequest::get(INCIWEB_INDEX_URL).header("Content-Type", "application/json");
        request.method = http::Method::POST;
        request.body = Some(br#"{"title":""}"#.to_vec());
        let rows = self
            .upstream_json(request, 4 * MIB, Duration::from_secs(20))
            .await?;
        if rows.is_array() { Ok(rows) } else { Err(()) }
    }

    /// InciWeb's publication API was retired; incident pages carry origin
    /// and update times.
    async fn fetch_publication(&self, id: &str) -> Result<Value, ()> {
        let fetch = self.deps.fetch.clone();
        let url = format!("https://inciweb.wildfire.gov/node/{id}");
        // One 15 s signal covers the redirect, the page and its body.
        let html = with_deadline(Duration::from_secs(15), async move {
            let mut response = fetch(FetchRequest::get(url).redirect(Redirect::Manual)).await?;
            if [301, 302, 303, 307, 308].contains(&response.status) {
                let location = response.header("location").unwrap_or("").to_string();
                drop(response);
                let target = safe_redirect(&location)
                    .ok_or_else(|| FetchError::Network("invalid_redirect".into()))?;
                response = fetch(FetchRequest::get(target).redirect(Redirect::Error)).await?;
            }
            if response.status != 200 {
                return Err(FetchError::Network("upstream_unavailable".into()));
            }
            read_text_capped(response, 2 * MIB).await
        })
        .await
        .map_err(|_| ())?;
        let changed = UPDATED_TIME.captures(&html).and_then(|c| parse_date(&c[1]));
        let created = ORIGIN_TIME.captures(&html).and_then(|c| parse_date(&c[1]));
        if created.is_none() && changed.is_none() {
            return Err(());
        }
        Ok(json!({ "createdMs": created, "changedMs": changed }))
    }
}

/// Validate an InciWeb redirect `Location`, returning the https URL to
/// follow; `None` for anything off-site or unusual.
fn safe_redirect(location: &str) -> Option<String> {
    if location.is_empty() || location.contains("..") || location.contains(['?', '%', '\\']) {
        return None;
    }
    let base = url::Url::parse("https://inciweb.wildfire.gov/").ok()?;
    let mut url = base.join(location).ok()?;
    if url.host_str() != Some("inciweb.wildfire.gov")
        || !REDIRECT_PATH.is_match(url.path())
        || !matches!(url.scheme(), "http" | "https")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    url.set_scheme("https").ok()?;
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::hazard_support::testing::{self, Calls};
    use super::super::hazard_support::{FetchResponse, testing::send};
    use super::*;

    const INCIDENT_HTML: &str = "<meta property=\"og:updated_time\" content=\"2026-09-23T11:32:31-04:00\" />\n<th>Date of Origin</th>\n<td class=\"text-tabular text-left\"><time datetime=\"2026-07-15T20:20:00Z\">Origin</time></td>";

    fn incident_times() -> Value {
        json!({
            "createdMs": parse_date("2026-07-15T20:20:00Z"),
            "changedMs": parse_date("2026-09-23T11:32:31-04:00"),
        })
    }

    fn incident_redirect() -> FetchResponse {
        FetchResponse::from_bytes(
            301,
            &[("location", "http://inciweb.wildfire.gov/test-fire")],
            "redirect",
        )
    }

    fn feature(id: &str) -> Value {
        json!({
            "properties": { "attr_UniqueFireIdentifier": id, "poly_DateCurrent": 123 },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-108, 35], [-107, 35], [-107, 36], [-108, 35]]],
            },
        })
    }

    struct Harness {
        router: Router<AppState>,
        state: AppState,
    }

    impl Harness {
        fn new(fetch: Fetcher, now: Clock) -> Self {
            Self {
                router: router_with(Deps { fetch, now }),
                state: testing::state(),
            }
        }

        async fn request(
            &self,
            path: &str,
            method: &str,
            peer: &str,
        ) -> (u16, http::HeaderMap, Value) {
            let (status, headers, body) = send(
                &self.router,
                &self.state,
                method,
                &format!("/api/fire-perimeters{path}"),
                peer,
            )
            .await;
            (
                status,
                headers,
                serde_json::from_slice(&body).unwrap_or(Value::Null),
            )
        }

        async fn get(&self, path: &str) -> (u16, http::HeaderMap, Value) {
            self.request(path, "GET", "127.0.0.1").await
        }
    }

    #[test]
    fn normalization_skips_invalid_features_but_keeps_the_rest() {
        let mut bad_ring = feature("bad");
        bad_ring["geometry"]["coordinates"][0][1] = json!([200, 0]);
        let snapshot = json!({ "features": [
            feature("a"),
            { "properties": { "attr_UniqueFireIdentifier": "null-geometry" }, "geometry": null },
            bad_ring,
            feature("a"),
            { "id": 7, "properties": {}, "geometry": feature("x")["geometry"] },
            { "properties": [], "geometry": feature("x")["geometry"] },
        ]});
        let rows = normalize_fire_perimeter_snapshot(&snapshot).unwrap();
        let ids: Vec<&str> = rows
            .iter()
            .map(|r| r["stableId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["a", "7"]);
        assert_eq!(rows[0]["updatedTime"], json!(123));
        assert_eq!(rows[0]["name"], Value::Null);
        assert_eq!(rows[0]["polygons"].as_array().unwrap().len(), 1);
        assert_eq!(normalize_fire_perimeter_snapshot(&json!({})), None);
    }

    #[tokio::test]
    async fn paging_stops_at_five_pages_and_normalizes_the_combined_snapshot() {
        let calls: Calls = Default::default();
        let counter = Arc::clone(&calls);
        let fetch = testing::fetcher(Arc::clone(&calls), move |request| {
            assert_eq!(request.redirect, Redirect::Error);
            let n = counter.lock().len();
            Ok(FetchResponse::json(&json!({
                "features": [feature(&n.to_string())],
                "exceededTransferLimit": true,
            })))
        });
        let harness = Harness::new(fetch, Arc::new(|| 1234));
        let (status, _, body) = harness
            .get("/?url=https://invalid.example&resultOffset=999")
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["fetchedAt"], json!(1234));
        assert_eq!(body["rows"].as_array().unwrap().len(), 5);
        let urls: Vec<url::Url> = calls
            .lock()
            .iter()
            .map(|r| url::Url::parse(&r.url).unwrap())
            .collect();
        let offsets: Vec<Option<String>> = urls
            .iter()
            .map(|u| {
                u.query_pairs()
                    .find(|(k, _)| k == "resultOffset")
                    .map(|(_, v)| v.into_owned())
            })
            .collect();
        assert_eq!(
            offsets,
            vec![
                None,
                Some("1".into()),
                Some("2".into()),
                Some("3".into()),
                Some("4".into())
            ]
        );
        assert!(
            urls.iter()
                .all(|u| u.host_str() == Some("services3.arcgis.com"))
        );
        assert_eq!(
            urls[0]
                .query_pairs()
                .find(|(k, _)| k == "maxAllowableOffset")
                .unwrap()
                .1,
            "0.001"
        );
    }

    #[tokio::test]
    async fn explicit_transfer_limit_false_stops_paging_and_fresh_cache_does_not_fetch() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Ok(FetchResponse::json(
                &json!({ "features": [feature("fire")], "exceededTransferLimit": false }),
            ))
        });
        let harness = Harness::new(fetch, system_clock());
        let first = harness.get("/").await;
        assert_eq!(harness.get("/").await.2, first.2);
        assert_eq!(calls.lock().len(), 1);
    }

    #[tokio::test]
    async fn failed_refresh_serves_the_last_good_snapshot_as_stale() {
        let (clock, now) = testing::manual_clock(0);
        let calls: Calls = Default::default();
        let counter = Arc::clone(&calls);
        let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
            if counter.lock().len() > 1 {
                return Err(FetchError::Network("private upstream details".into()));
            }
            Ok(FetchResponse::json(
                &json!({ "features": [feature("fire")] }),
            ))
        });
        let harness = Harness::new(fetch, clock);
        harness.get("/").await;
        now.store(300_000, Ordering::SeqCst);
        let (status, headers, body) = harness.get("/").await;
        assert_eq!(status, 200);
        assert_eq!(body["stale"], json!(true));
        assert_eq!(body["fetchedAt"], json!(0));
        assert_eq!(body["rows"][0]["stableId"], json!("fire"));
        assert_eq!(headers["x-data-stale"], "true");
    }

    #[tokio::test]
    async fn oversized_streamed_and_declared_bodies_produce_sanitized_502s() {
        for declared in [true, false] {
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = Arc::clone(&dropped);
            let fetch = testing::fetcher(Default::default(), move |_| {
                let length = (17 * MIB).to_string();
                let headers: Vec<(&str, &str)> = if declared {
                    vec![("content-length", length.as_str())]
                } else {
                    vec![]
                };
                Ok(FetchResponse::from_stream(
                    200,
                    &headers,
                    testing::endless(bytes::Bytes::from(vec![0u8; MIB]), Arc::clone(&flag)),
                ))
            });
            let harness = Harness::new(fetch, system_clock());
            let (status, _, body) = harness.get("/").await;
            assert_eq!(status, 502);
            assert_eq!(body, json!({ "error": "fire_perimeters_unavailable" }));
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn malformed_wfigs_data_is_not_cached_as_an_empty_snapshot() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Ok(FetchResponse::json(&json!({ "error": "upstream detail" })))
        });
        let harness = Harness::new(fetch, system_clock());
        assert_eq!(harness.get("/").await.0, 502);
        assert_eq!(harness.get("/").await.0, 502);
        assert_eq!(calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_upstream_operation_for_every_route() {
        for (path, payload) in [
            ("/", json!({ "features": [] })),
            ("/inciweb/index", json!([])),
            ("/inciweb/publication/42", json!({})),
        ] {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let calls = Arc::new(AtomicUsize::new(0));
            let fetch: Fetcher = {
                let gate = Arc::clone(&gate);
                let calls = Arc::clone(&calls);
                Arc::new(move |_request: FetchRequest| {
                    let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let gate = Arc::clone(&gate);
                    let payload = payload.clone();
                    Box::pin(async move {
                        let _ = gate.acquire().await;
                        if path.contains("/publication/") {
                            return Ok(if n == 1 {
                                incident_redirect()
                            } else {
                                FetchResponse::from_bytes(200, &[], INCIDENT_HTML)
                            });
                        }
                        Ok(FetchResponse::json(&payload))
                    })
                })
            };
            let harness = Harness::new(fetch, system_clock());
            let first = harness.get(path);
            let second = harness.get(path);
            let release = async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                gate.add_permits(100);
            };
            let (a, b, ()) = tokio::join!(first, second, release);
            assert_eq!((a.0, b.0), (200, 200));
            let expected = if path.contains("/publication/") { 2 } else { 1 };
            assert_eq!(calls.load(Ordering::SeqCst), expected);
        }
    }

    #[tokio::test]
    async fn bad_publication_ids_and_non_get_requests_never_fetch_upstream() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Err(FetchError::Network("unexpected".into()))
        });
        let harness = Harness::new(fetch, system_clock());
        for id in ["", "abc", "-1", "1234567890", "1/2", "%31", "1.0"] {
            assert_eq!(
                harness.get(&format!("/inciweb/publication/{id}")).await.0,
                400,
                "{id}"
            );
        }
        for path in ["/", "/inciweb/index", "/inciweb/publication/123"] {
            for method in ["POST", "HEAD", "PUT", "DELETE"] {
                let (status, headers, _) = harness.request(path, method, "127.0.0.1").await;
                assert_eq!(status, 405);
                assert_eq!(headers["allow"], "GET");
            }
        }
        assert_eq!(harness.get("/elsewhere").await.0, 404);
        assert_eq!(calls.lock().len(), 0);
    }

    #[tokio::test]
    async fn inciweb_index_posts_caches_for_an_hour_and_serves_stale_arrays() {
        let (clock, now) = testing::manual_clock(0);
        let rows = json!([{ "incident_id": "42", "incident_title": "Fire" }]);
        let calls: Calls = Default::default();
        let counter = Arc::clone(&calls);
        let answer = rows.clone();
        let fetch = testing::fetcher(Arc::clone(&calls), move |request| {
            assert_eq!(request.url, INCIWEB_INDEX_URL);
            assert_eq!(request.method, http::Method::POST);
            assert_eq!(
                request.header_value("Content-Type"),
                Some("application/json")
            );
            let body: Value = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
            assert_eq!(body, json!({ "title": "" }));
            if counter.lock().len() > 1 {
                return Err(FetchError::Network("offline".into()));
            }
            Ok(FetchResponse::json(&answer))
        });
        let harness = Harness::new(fetch, clock);
        assert_eq!(harness.get("/inciweb/index").await.2, rows);
        now.store(3_599_999, Ordering::SeqCst);
        assert_eq!(harness.get("/inciweb/index").await.2, rows);
        assert_eq!(calls.lock().len(), 1);
        now.store(3_600_000, Ordering::SeqCst);
        let stale = harness.get("/inciweb/index").await;
        assert_eq!(stale.2, rows);
        assert_eq!(stale.1["x-data-stale"], "true");
    }

    #[tokio::test]
    async fn publication_timestamps_thirty_minute_ttl_and_oldest_entry_eviction() {
        let (clock, now) = testing::manual_clock(0);
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |request| {
            Ok(if request.url.contains("/node/") {
                incident_redirect()
            } else {
                FetchResponse::from_bytes(200, &[], INCIDENT_HTML)
            })
        });
        let harness = Harness::new(fetch, clock);
        let get = |id: u32| {
            let peer = format!("10.0.{}.{}", id / 256, id % 256);
            let harness = &harness;
            async move {
                harness
                    .request(&format!("/inciweb/publication/{id}"), "GET", &peer)
                    .await
            }
        };
        assert_eq!(get(1).await.2, incident_times());
        now.store(1_799_999, Ordering::SeqCst);
        get(1).await;
        assert_eq!(calls.lock().len(), 2);
        now.store(1_800_000, Ordering::SeqCst);
        get(1).await;
        assert_eq!(calls.lock().len(), 4);
        for id in 2..=257 {
            assert_eq!(get(id).await.0, 200);
        }
        get(2).await;
        assert_eq!(calls.lock().len(), 516);
        get(1).await;
        assert_eq!(calls.lock().len(), 518);
        let urls: Vec<String> = calls.lock().iter().map(|r| r.url.clone()).collect();
        assert_eq!(urls[urls.len() - 2], "https://inciweb.wildfire.gov/node/1");
    }

    #[tokio::test]
    async fn index_and_publication_reads_enforce_their_smaller_caps() {
        for (path, cap) in [
            ("/inciweb/index", 4 * MIB),
            ("/inciweb/publication/1", 2 * MIB),
        ] {
            let fetch = testing::fetcher(Default::default(), move |_| {
                Ok(FetchResponse::from_bytes(
                    200,
                    &[("content-length", &(cap + 1).to_string())],
                    "{}",
                ))
            });
            let harness = Harness::new(fetch, system_clock());
            assert_eq!(harness.get(path).await.0, 502);
        }
    }

    #[tokio::test]
    async fn the_per_client_limit_stops_a_loop_while_another_client_reads_the_cache() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Ok(FetchResponse::json(&json!({ "features": [] })))
        });
        let harness = Harness::new(fetch, system_clock());
        for _ in 0..60 {
            assert_eq!(harness.get("/").await.0, 200);
        }
        let limited = harness.get("/").await;
        assert_eq!(limited.0, 429);
        assert_eq!(limited.1["retry-after"], "60");
        assert_eq!(limited.2, json!({ "error": "rate_limited" }));
        assert_eq!(harness.request("/", "GET", "10.9.9.9").await.0, 200);
        assert_eq!(calls.lock().len(), 1);
    }

    #[tokio::test]
    async fn publication_redirects_force_https_without_query_or_fragment() {
        for status in [301u16, 302, 303, 307, 308] {
            let calls: Calls = Default::default();
            let counter = Arc::clone(&calls);
            let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
                Ok(if counter.lock().len() == 1 {
                    FetchResponse::from_bytes(
                        status,
                        &[(
                            "location",
                            "http://inciweb.wildfire.gov/incident/test-fire#details",
                        )],
                        "redirect",
                    )
                } else {
                    FetchResponse::from_bytes(200, &[], INCIDENT_HTML)
                })
            });
            let harness = Harness::new(fetch, system_clock());
            assert_eq!(
                harness.get("/inciweb/publication/42").await.2,
                incident_times()
            );
            let calls = calls.lock();
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].url, "https://inciweb.wildfire.gov/node/42");
            assert_eq!(calls[0].redirect, Redirect::Manual);
            assert_eq!(
                calls[1].url,
                "https://inciweb.wildfire.gov/incident/test-fire"
            );
            assert_eq!(calls[1].redirect, Redirect::Error);
        }
    }

    #[tokio::test]
    async fn unsafe_publication_redirect_locations_fail_before_a_second_fetch() {
        for location in [
            "https://evil.example/x",
            "/a/../x",
            "/x?q=1",
            "/x?",
            "/%2e%2e/x",
            "/a/b/c",
            "",
        ] {
            let calls: Calls = Default::default();
            let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
                Ok(FetchResponse::from_bytes(
                    301,
                    &[("location", location)],
                    "redirect",
                ))
            });
            let harness = Harness::new(fetch, system_clock());
            assert_eq!(
                harness.get("/inciweb/publication/42").await.0,
                502,
                "{location}"
            );
            assert_eq!(calls.lock().len(), 1);
        }
    }

    #[tokio::test]
    async fn incident_pages_without_valid_timestamps_fail_closed_and_are_not_cached() {
        let invalid = Regex::new(r#"2026-[^"]+"#)
            .unwrap()
            .replace_all(INCIDENT_HTML, "invalid")
            .into_owned();
        for html in ["<html>No markers</html>".to_string(), invalid] {
            let calls: Calls = Default::default();
            let counter = Arc::clone(&calls);
            let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
                Ok(if counter.lock().len() % 2 == 1 {
                    incident_redirect()
                } else {
                    FetchResponse::from_bytes(200, &[], html.clone())
                })
            });
            let harness = Harness::new(fetch, system_clock());
            assert_eq!(harness.get("/inciweb/publication/42").await.0, 502);
            assert_eq!(harness.get("/inciweb/publication/42").await.0, 502);
            assert_eq!(calls.lock().len(), 4);
        }
    }

    #[tokio::test]
    async fn direct_200_incident_pages_work_including_one_missing_timestamp() {
        let mut no_changed = incident_times();
        no_changed["changedMs"] = Value::Null;
        let mut no_created = incident_times();
        no_created["createdMs"] = Value::Null;
        for (html, expected) in [
            (INCIDENT_HTML.to_string(), incident_times()),
            (
                INCIDENT_HTML.replace("og:updated_time", "unrelated"),
                no_changed,
            ),
            (
                INCIDENT_HTML.replace("Date of Origin", "Unrelated"),
                no_created,
            ),
        ] {
            let calls: Calls = Default::default();
            let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
                Ok(FetchResponse::from_bytes(200, &[], html.clone()))
            });
            let harness = Harness::new(fetch, system_clock());
            assert_eq!(harness.get("/inciweb/publication/42").await.2, expected);
            assert_eq!(calls.lock().len(), 1);
        }
    }

    #[tokio::test]
    async fn publication_html_streams_are_capped_at_2_mib() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_stream(
                200,
                &[],
                testing::endless(bytes::Bytes::from(vec![0u8; MIB]), Arc::clone(&flag)),
            ))
        });
        let harness = Harness::new(fetch, system_clock());
        assert_eq!(harness.get("/inciweb/publication/42").await.0, 502);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
