//! NOAA NHC / CPHC active tropical cyclones (port of
//! `server/providers/cyclones.js`).
//!
//! `GET /api/cyclones` → a validated snapshot of active storms with their
//! current advisory geometry. Fixed official endpoints, one shared bounded
//! refresh (12 s), a 5 min TTL, a 60 s failure cooldown, and age-limited
//! last-good fallback. No user-supplied destinations.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use regex::Regex;
use serde_json::{Map, Value, json};
use tokio::task::AbortHandle;

use super::hazard_support::{
    Clock, FetchError, FetchRequest, Fetcher, Redirect, jnum, mount, parse_date, read_text_capped,
    reqwest_fetcher, sub_url, system_clock, to_iso,
};
use crate::common::{js, reply};
use crate::state::AppState;

const STATUS_URL: &str = "https://www.nhc.noaa.gov/CurrentStorms.json";
const GIS: &str = "https://mapservices.weather.noaa.gov/tropical/rest/services/tropical/NHC_tropical_weather_summary/MapServer";
const HOUR: i64 = 3_600_000;
const COVERAGE: &str =
    "Atlantic and eastern/central North Pacific; not worldwide cyclone coverage.";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Points,
    Track,
    Cone,
}

struct LayerSpec {
    id: u8,
    cap: usize,
    count: usize,
    kind: Kind,
}

const LAYERS: [LayerSpec; 3] = [
    LayerSpec {
        id: 5,
        cap: 512 * 1024,
        count: 500,
        kind: Kind::Points,
    },
    LayerSpec {
        id: 6,
        cap: 512 * 1024,
        count: 32,
        kind: Kind::Track,
    },
    LayerSpec {
        id: 7,
        cap: 2 * 1024 * 1024,
        count: 32,
        kind: Kind::Cone,
    },
];

/// `invalid_cyclone_data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Invalid;

type Checked<T> = Result<T, Invalid>;

static ISO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{3})?Z$").expect("regex"));
static ADVISORY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^\d{1,3}[A-Z]?$").expect("regex"));
static STORM_ID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:al|ep|cp)\d{6}$").expect("regex"));
static OFFICIAL_PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^/text/[A-Z0-9]+\.shtml$").expect("regex"));
static IDP_SOURCE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^((?:al|ep|cp)\d{6})-(\d{1,3}[a-z]?)_5day_(pts|lin|pgn)$").expect("regex")
});

fn text(value: Option<&Value>, max: usize) -> Checked<String> {
    let Some(Value::String(s)) = value else {
        return Err(Invalid);
    };
    if s.trim().is_empty()
        || s.encode_utf16().count() > max
        || s.chars()
            .any(|c| (c as u32) <= 0x1f || c == '<' || c == '>')
    {
        return Err(Invalid);
    }
    Ok(s.trim().to_string())
}

fn advisory(value: Option<&Value>) -> Checked<String> {
    let Some(Value::String(s)) = value else {
        return Err(Invalid);
    };
    if !ADVISORY.is_match(s) {
        return Err(Invalid);
    }
    // `replace(/^0+(?=\d)/, '')`: drop leading zeros that precede a digit.
    let bytes = s.as_bytes();
    let mut start = 0;
    while start + 1 < bytes.len() && bytes[start] == b'0' && bytes[start + 1].is_ascii_digit() {
        start += 1;
    }
    Ok(s[start..].to_uppercase())
}

/// `number(value, min, max)`: strings and numbers in range, else null.
fn number(value: Option<&Value>, min: f64, max: f64) -> Option<f64> {
    let n = match value? {
        Value::String(s) if s.is_empty() => return None,
        Value::String(s) => js::number(s),
        Value::Number(n) => n.as_f64()?,
        _ => return None,
    };
    (n.is_finite() && n >= min && n <= max).then_some(n)
}

fn num_or_null(value: Option<f64>) -> Value {
    value.map_or(Value::Null, jnum)
}

fn iso(value: Option<&Value>, now: i64) -> Checked<String> {
    let Some(Value::String(s)) = value else {
        return Err(Invalid);
    };
    if !ISO.is_match(s) {
        return Err(Invalid);
    }
    let n = parse_date(s).ok_or(Invalid)?;
    let canonical = to_iso(n);
    if n > now + 5 * 60_000
        || now - n > 12 * HOUR
        || canonical.replace(".000Z", "Z") != s.replace(".000Z", "Z")
    {
        return Err(Invalid);
    }
    Ok(canonical)
}

fn position(coordinates: Option<&Value>) -> Checked<(f64, f64)> {
    let Some(Value::Array(items)) = coordinates else {
        return Err(Invalid);
    };
    if items.len() != 2 {
        return Err(Invalid);
    }
    let lon = items[0].as_f64().ok_or(Invalid)?;
    let lat = items[1].as_f64().ok_or(Invalid)?;
    if !lon.is_finite() || !lat.is_finite() || lon.abs() > 180.0 || lat.abs() > 90.0 {
        return Err(Invalid);
    }
    Ok((lon, lat))
}

fn position_value((lon, lat): (f64, f64)) -> Value {
    json!({ "longitude": jnum(lon), "latitude": jnum(lat) })
}

fn official_link(value: Option<&Value>) -> Value {
    let Some(Value::String(s)) = value else {
        return Value::Null;
    };
    if s.encode_utf16().count() > 256 {
        return Value::Null;
    }
    let Ok(url) = url::Url::parse(s) else {
        return Value::Null;
    };
    let ok = url.origin().ascii_serialization() == "https://www.nhc.noaa.gov"
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none_or(str::is_empty)
        && url.fragment().is_none_or(str::is_empty)
        && OFFICIAL_PATH.is_match(url.path());
    if ok {
        Value::from(url.as_str())
    } else {
        Value::Null
    }
}

/// `parseCycloneStatus`: every active storm, or [`Invalid`] — never a
/// silently shortened list.
pub fn parse_cyclone_status(payload: &Value, now: i64) -> Checked<Vec<Value>> {
    let storms = payload
        .get("activeStorms")
        .and_then(Value::as_array)
        .ok_or(Invalid)?;
    if storms.len() > 32 {
        return Err(Invalid);
    }
    let mut seen = HashSet::new();
    storms
        .iter()
        .map(|raw| {
            let id = raw.get("id").and_then(Value::as_str).ok_or(Invalid)?;
            if !STORM_ID.is_match(id) || !seen.insert(id.to_string()) {
                return Err(Invalid);
            }
            let forecast = raw.get("forecastAdvisory");
            let issued_at = iso(forecast.and_then(|f| f.get("issuance")), now)?;
            let advisory_number = advisory(forecast.and_then(|f| f.get("advNum")))?;
            let position_at = iso(raw.get("lastUpdate"), now)?;
            let mut storm = Map::new();
            storm.insert("id".into(), Value::from(id));
            storm.insert("name".into(), Value::from(text(raw.get("name"), 80)?));
            storm.insert(
                "classification".into(),
                Value::from(text(raw.get("classification"), 16)?),
            );
            storm.insert("basin".into(), Value::from(id[..2].to_uppercase()));
            let coordinates = json!([
                raw.get("longitudeNumeric").cloned().unwrap_or(Value::Null),
                raw.get("latitudeNumeric").cloned().unwrap_or(Value::Null)
            ]);
            storm.insert(
                "position".into(),
                position_value(position(Some(&coordinates))?),
            );
            storm.insert("positionAt".into(), Value::from(position_at));
            storm.insert("advisoryNumber".into(), Value::from(advisory_number));
            storm.insert("issuedAt".into(), Value::from(issued_at));
            storm.insert(
                "windKt".into(),
                num_or_null(number(raw.get("intensity"), 0.0, 300.0)),
            );
            storm.insert(
                "pressureHpa".into(),
                num_or_null(number(raw.get("pressure"), 800.0, 1100.0)),
            );
            storm.insert(
                "movement".into(),
                json!({
                    "directionDegrees": num_or_null(number(raw.get("movementDir"), 0.0, 360.0)),
                    "speedKt": num_or_null(number(raw.get("movementSpeed"), 0.0, 200.0)),
                }),
            );
            storm.insert(
                "advisoryUrl".into(),
                official_link(forecast.and_then(|f| f.get("url"))),
            );
            let basin = if id.starts_with("al") {
                "atlc"
            } else if id.starts_with("cp") {
                "cpac"
            } else {
                "epac"
            };
            storm.insert(
                "outlookUrl".into(),
                Value::from(format!(
                    "https://www.nhc.noaa.gov/gtwo.php?basin={basin}&fdays=7"
                )),
            );
            storm.insert("geometryStatus".into(), Value::from("pending"));
            storm.insert("geometryAdvisoryNumber".into(), Value::Null);
            storm.insert("forecastPoints".into(), json!([]));
            storm.insert("track".into(), Value::Null);
            storm.insert("cone".into(), Value::Null);
            Ok(Value::Object(storm))
        })
        .collect()
}

struct Budget {
    count: usize,
}

fn geometry(raw: Option<&Value>, kind: Kind, budget: &mut Budget) -> Checked<Value> {
    let raw = raw.ok_or(Invalid)?;
    let kind_name = raw.get("type").and_then(Value::as_str).ok_or(Invalid)?;
    let allowed: &[&str] = match kind {
        Kind::Points => &["Point"],
        Kind::Track => &["LineString", "MultiLineString"],
        Kind::Cone => &["Polygon", "MultiPolygon"],
    };
    if !allowed.contains(&kind_name) {
        return Err(Invalid);
    }
    fn point(value: &Value, budget: &mut Budget) -> Checked<Value> {
        let (lon, lat) = position(Some(value))?;
        budget.count += 1;
        if budget.count > 25_000 {
            return Err(Invalid);
        }
        Ok(json!([jnum(lon), jnum(lat)]))
    }
    fn line(value: &Value, ring: bool, budget: &mut Budget) -> Checked<Value> {
        let items = value.as_array().ok_or(Invalid)?;
        if items.len() < if ring { 4 } else { 2 } || items.len() > 10_000 {
            return Err(Invalid);
        }
        let result: Vec<Value> = items
            .iter()
            .map(|item| point(item, budget))
            .collect::<Checked<_>>()?;
        if ring
            && (result[0][0] != result[result.len() - 1][0]
                || result[0][1] != result[result.len() - 1][1])
        {
            return Err(Invalid);
        }
        Ok(Value::Array(result))
    }
    fn list(
        value: &Value,
        budget: &mut Budget,
        read: &dyn Fn(&Value, &mut Budget) -> Checked<Value>,
    ) -> Checked<Value> {
        let items = value.as_array().ok_or(Invalid)?;
        if items.is_empty() || items.len() > 128 {
            return Err(Invalid);
        }
        Ok(Value::Array(
            items
                .iter()
                .map(|item| read(item, budget))
                .collect::<Checked<_>>()?,
        ))
    }
    fn polygon(value: &Value, budget: &mut Budget) -> Checked<Value> {
        list(value, budget, &|ring, budget| line(ring, true, budget))
    }
    let coordinates = raw.get("coordinates").unwrap_or(&Value::Null);
    let coordinates = match kind_name {
        "Point" => point(coordinates, budget)?,
        "LineString" => line(coordinates, false, budget)?,
        "MultiLineString" => list(coordinates, budget, &|part, budget| {
            line(part, false, budget)
        })?,
        "Polygon" => polygon(coordinates, budget)?,
        _ => list(coordinates, budget, &polygon)?,
    };
    Ok(json!({ "type": kind_name, "coordinates": coordinates }))
}

/// JavaScript truthiness of a JSON value.
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|n| n != 0.0),
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

struct GisItem {
    id: String,
    advisory_number: String,
    geometry: Value,
    tau_hours: Option<f64>,
    wind_kt: Option<f64>,
    gust_kt: Option<f64>,
}

/// `attachCycloneGeometry`: never relabel earlier GIS geometry with the
/// newer status advisory.
pub fn attach_cyclone_geometry(storms: &[Value], collections: &[Value]) -> Checked<Vec<Value>> {
    let mut budget = Budget { count: 0 };
    let mut parsed: Vec<Vec<GisItem>> = Vec::new();
    for (index, spec) in LAYERS.iter().enumerate() {
        let payload = collections.get(index).ok_or(Invalid)?;
        let features = payload.get("features").and_then(Value::as_array);
        if payload.get("type").and_then(Value::as_str) != Some("FeatureCollection")
            || truthy(payload.get("exceededTransferLimit"))
            || features.is_none_or(|f| f.len() > spec.count)
        {
            return Err(Invalid);
        }
        let mut items = Vec::new();
        for feature in features.into_iter().flatten() {
            if feature.get("type").and_then(Value::as_str) != Some("Feature") {
                return Err(Invalid);
            }
            let p = feature.get("properties");
            let field = |key: &str| p.and_then(|p| p.get(key));
            let source = field("idp_source")
                .and_then(Value::as_str)
                .and_then(|s| IDP_SOURCE.captures(s))
                .ok_or(Invalid)?;
            if source[3].to_lowercase() != ["pts", "lin", "pgn"][index] {
                return Err(Invalid);
            }
            let adv = advisory(field("advisnum"))?;
            if adv != advisory(Some(&Value::from(&source[2])))? {
                return Err(Invalid);
            }
            items.push(GisItem {
                id: source[1].to_lowercase(),
                advisory_number: adv,
                geometry: geometry(feature.get("geometry"), spec.kind, &mut budget)?,
                tau_hours: number(field("tau"), 0.0, 168.0),
                wind_kt: number(field("maxwind"), 0.0, 300.0),
                gust_kt: number(field("gust"), 0.0, 350.0),
            });
        }
        parsed.push(items);
    }
    storms
        .iter()
        .map(|storm| {
            let id = storm["id"].as_str().unwrap_or("");
            let adv = storm["advisoryNumber"].as_str().unwrap_or("");
            let groups: Vec<Vec<&GisItem>> = parsed
                .iter()
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| item.id == id && item.advisory_number == adv)
                        .collect()
                })
                .collect();
            if groups[1].len() > 1 || groups[2].len() > 1 {
                return Err(Invalid);
            }
            if groups[0].is_empty() || groups[1].len() != 1 || groups[2].len() != 1 {
                return Ok(storm.clone());
            }
            let mut taus: Vec<f64> = Vec::new();
            let mut points: Vec<(f64, Value)> = groups[0]
                .iter()
                .map(|item| {
                    let tau = item.tau_hours.ok_or(Invalid)?;
                    if taus.contains(&tau) {
                        return Err(Invalid);
                    }
                    taus.push(tau);
                    Ok((
                        tau,
                        json!({
                            "position": position_value(position(item.geometry.get("coordinates"))?),
                            "tauHours": jnum(tau),
                            "windKt": num_or_null(item.wind_kt),
                            "gustKt": num_or_null(item.gust_kt),
                        }),
                    ))
                })
                .collect::<Checked<_>>()?;
            points.sort_by(|a, b| a.0.total_cmp(&b.0));
            let mut out = storm.clone();
            out["geometryStatus"] = Value::from("current");
            out["geometryAdvisoryNumber"] = Value::from(adv);
            out["forecastPoints"] = Value::Array(points.into_iter().map(|(_, p)| p).collect());
            out["track"] = groups[1][0].geometry.clone();
            out["cone"] = groups[2][0].geometry.clone();
            Ok(out)
        })
        .collect()
}

// ─── Proxy ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct Deps {
    pub fetch: Fetcher,
    pub now: Clock,
    pub timeout: Duration,
}

#[derive(Clone)]
struct Snapshot {
    storms: Vec<Value>,
    fetched_at: i64,
}

type SharedRefresh = Shared<BoxFuture<'static, Result<Snapshot, ()>>>;

struct Operation {
    id: u64,
    shared: SharedRefresh,
    abort: AbortHandle,
    waiters: usize,
}

#[derive(Default)]
struct Inner {
    cache: Option<Snapshot>,
    operation: Option<Operation>,
    attempted_at: Option<i64>,
    next_id: u64,
}

struct Cyclones {
    deps: Deps,
    inner: Mutex<Inner>,
}

enum AcquireError {
    Busy,
    Failed,
}

/// One waiting consumer of a shared refresh. Dropped without completing,
/// it is a disconnected client: the last one cancels the refresh and
/// lifts the failure cooldown.
struct Waiter<'a> {
    proxy: &'a Cyclones,
    id: u64,
    completed: bool,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        let mut inner = self.proxy.inner.lock();
        let Some(operation) = inner.operation.as_mut().filter(|op| op.id == self.id) else {
            return;
        };
        operation.waiters -= 1;
        if operation.waiters == 0 {
            operation.abort.abort();
            inner.operation = None;
            if !self.completed {
                inner.attempted_at = None;
            }
        }
    }
}

pub fn router(_state: &AppState) -> Router<AppState> {
    router_with(Deps {
        fetch: reqwest_fetcher(),
        now: system_clock(),
        timeout: Duration::from_millis(12_000),
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let proxy = Arc::new(Cyclones {
        deps,
        inner: Mutex::new(Inner::default()),
    });
    mount("/api/cyclones", move |request: Request| {
        let proxy = Arc::clone(&proxy);
        async move { proxy.handle(request).await }
    })
}

fn json_reply(status: u16, value: &Value) -> Response {
    let response = reply::json(status, value, Some("no-store"));
    if status == 429 {
        reply::with_header(response, "Retry-After", "2")
    } else {
        response
    }
}

fn describe(value: Option<&Snapshot>, stale: bool) -> Value {
    json!({
        "schemaVersion": 1,
        "source": "NOAA NHC / CPHC",
        "attribution": "NOAA/NWS National Hurricane Center / Central Pacific Hurricane Center",
        "coverage": COVERAGE,
        "fetchedAt": value.map(|v| v.fetched_at),
        "stale": stale || value.is_none(),
        "unavailable": value.is_none(),
        "reason": match (value, stale) {
            (None, _) => Value::from("Cyclone data unavailable"),
            (Some(_), true) => Value::from("Cached cyclone advisory; upstream unavailable"),
            (Some(_), false) => Value::Null,
        },
        "storms": value.map_or_else(Vec::new, |v| v.storms.clone()),
    })
}

impl Cyclones {
    async fn handle(self: Arc<Self>, request: Request) -> Response {
        if request.method() != http::Method::GET {
            return json_reply(405, &json!({ "error": "method_not_allowed" }));
        }
        if sub_url(&request, "/api/cyclones") != "/" {
            return json_reply(400, &json!({ "error": "invalid_cyclone_query" }));
        }
        match self.acquire().await {
            Ok(snapshot) => json_reply(200, &describe(Some(&snapshot), false)),
            Err(AcquireError::Busy) => json_reply(429, &json!({ "error": "cyclone_busy" })),
            Err(AcquireError::Failed) => {
                let now = (self.deps.now)();
                let cache = self.inner.lock().cache.clone();
                let usable = cache.filter(|cache| {
                    now - cache.fetched_at <= 12 * HOUR
                        && cache.storms.iter().all(|storm| {
                            storm["issuedAt"]
                                .as_str()
                                .and_then(parse_date)
                                .is_some_and(|issued| now - issued <= 12 * HOUR)
                        })
                });
                json_reply(200, &describe(usable.as_ref(), true))
            }
        }
    }

    async fn acquire(self: &Arc<Self>) -> Result<Snapshot, AcquireError> {
        let (id, shared) = {
            let mut inner = self.inner.lock();
            let now = (self.deps.now)();
            if let Some(cache) = &inner.cache
                && now - cache.fetched_at < 300_000
            {
                return Ok(cache.clone());
            }
            if inner.operation.is_none() {
                if inner.attempted_at.is_some_and(|at| now - at < 60_000) {
                    return Err(AcquireError::Failed); // cyclone_retry_later
                }
                inner.attempted_at = Some(now);
                inner.next_id += 1;
                let id = inner.next_id;
                let this = Arc::clone(self);
                let task = tokio::spawn(async move {
                    let result = tokio::time::timeout(this.deps.timeout, this.refresh())
                        .await
                        .unwrap_or(Err(()));
                    let mut inner = this.inner.lock();
                    if let Ok(snapshot) = &result {
                        inner.cache = Some(snapshot.clone());
                    }
                    if inner.operation.as_ref().is_some_and(|op| op.id == id) {
                        inner.operation = None;
                    }
                    result
                });
                let abort = task.abort_handle();
                let shared = async move { task.await.unwrap_or(Err(())) }
                    .boxed()
                    .shared();
                inner.operation = Some(Operation {
                    id,
                    shared,
                    abort,
                    waiters: 0,
                });
            }
            let operation = inner.operation.as_mut().expect("operation exists");
            if operation.waiters >= 32 {
                return Err(AcquireError::Busy);
            }
            operation.waiters += 1;
            (operation.id, operation.shared.clone())
        };
        let mut waiter = Waiter {
            proxy: self,
            id,
            completed: false,
        };
        let result = shared.await;
        waiter.completed = true;
        drop(waiter);
        result.map_err(|()| AcquireError::Failed)
    }

    async fn upstream(&self, url: String, cap: usize) -> Result<Value, FetchError> {
        let request = FetchRequest::get(url)
            .redirect(Redirect::Error)
            .header("Accept", "application/geo+json,application/json")
            .header("User-Agent", "Camera Eye (public NOAA weather context)");
        let response = (self.deps.fetch)(request).await?;
        if !response.ok() {
            return Err(FetchError::Network("cyclone_upstream_unavailable".into()));
        }
        let text = read_text_capped(response, cap).await?;
        serde_json::from_str(&text).map_err(|_| FetchError::Network("invalid JSON".into()))
    }

    async fn refresh(&self) -> Result<Snapshot, ()> {
        let status = self
            .upstream(STATUS_URL.into(), 128 * 1024)
            .await
            .map_err(|_| ())?;
        let mut storms = parse_cyclone_status(&status, (self.deps.now)()).map_err(|_| ())?;
        if !storms.is_empty() {
            let results = futures_util::future::join_all(LAYERS.iter().map(|spec| {
                let out_fields = if spec.kind == Kind::Points {
                    "idp_source,advisnum,tau,maxwind,gust"
                } else {
                    "idp_source,advisnum"
                };
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("where", "1=1")
                    .append_pair("outFields", out_fields)
                    .append_pair("outSR", "4326")
                    .append_pair("resultRecordCount", &spec.count.to_string())
                    .append_pair("geometryPrecision", "4")
                    .append_pair("f", "geojson")
                    .finish();
                self.upstream(format!("{GIS}/{}/query?{query}", spec.id), spec.cap)
            }))
            .await;
            let attached = results
                .into_iter()
                .collect::<Result<Vec<Value>, FetchError>>()
                .map_err(|_| Invalid)
                .and_then(|collections| attach_cyclone_geometry(&storms, &collections));
            storms = match attached {
                Ok(storms) => storms,
                Err(Invalid) => storms
                    .into_iter()
                    .map(|mut storm| {
                        storm["geometryStatus"] = Value::from("unavailable");
                        storm
                    })
                    .collect(),
            };
        }
        Ok(Snapshot {
            storms,
            fetched_at: (self.deps.now)(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::super::hazard_support::FetchResponse;
    use super::super::hazard_support::testing::{self, Calls};
    use super::*;

    const NOW: i64 = 1_789_529_700_000; // 2026-09-16T03:35:00Z
    const TIME: &str = "2026-09-16T03:00:00.000Z";

    fn status_with(overrides: Value) -> Value {
        let mut storm = json!({
            "id": "ep152026",
            "name": "Fifteen-E",
            "classification": "PTC",
            "intensity": "25",
            "pressure": "1006",
            "longitudeNumeric": -125.8,
            "latitudeNumeric": 15.5,
            "movementDir": 255,
            "movementSpeed": 12,
            "lastUpdate": TIME,
            "forecastAdvisory": {
                "advNum": "010",
                "issuance": TIME,
                "url": "https://www.nhc.noaa.gov/text/MIATCMEP5.shtml",
            },
        });
        for (k, v) in overrides.as_object().unwrap() {
            storm[k] = v.clone();
        }
        json!({ "activeStorms": [storm] })
    }

    fn status() -> Value {
        status_with(json!({}))
    }

    fn collections(adv: &str) -> Vec<Value> {
        let geometry = [
            json!({ "type": "Point", "coordinates": [-125.8, 15.5] }),
            json!({ "type": "LineString", "coordinates": [[-125.8, 15.5], [-128, 15]] }),
            json!({ "type": "Polygon", "coordinates": [[[-126, 15], [-128, 15], [-127, 17], [-126, 15]]] }),
        ];
        geometry
            .into_iter()
            .enumerate()
            .map(|(index, shape)| {
                json!({
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "properties": {
                            "idp_source": format!("ep152026-{adv:0>3}_5day_{}", ["pts", "lin", "pgn"][index]),
                            "advisnum": adv,
                            "tau": 0,
                            "maxwind": 25,
                            "gust": 9999,
                        },
                        "geometry": shape,
                    }],
                })
            })
            .collect()
    }

    fn answer(request: &FetchRequest, status: &Value, parts: &[Value]) -> FetchResponse {
        if request.url.contains("CurrentStorms") {
            return FetchResponse::json(status);
        }
        let layer = Regex::new(r"/(\d)/query")
            .unwrap()
            .captures(&request.url)
            .unwrap()[1]
            .parse::<usize>()
            .unwrap();
        FetchResponse::json(&parts[layer - 5])
    }

    fn good_fetcher(calls: Calls) -> Fetcher {
        let (status, parts) = (status(), collections("10"));
        testing::fetcher(calls, move |request| Ok(answer(request, &status, &parts)))
    }

    struct Harness {
        router: Router<AppState>,
        state: AppState,
    }

    impl Harness {
        fn new(fetch: Fetcher, now: Clock) -> Self {
            Self::with_timeout(fetch, now, Duration::from_millis(12_000))
        }

        fn with_timeout(fetch: Fetcher, now: Clock, timeout: Duration) -> Self {
            Self {
                router: router_with(Deps {
                    fetch,
                    now,
                    timeout,
                }),
                state: testing::state(),
            }
        }

        async fn request(&self, path: &str, method: &str) -> (u16, http::HeaderMap, Value) {
            let (status, headers, body) = testing::send(
                &self.router,
                &self.state,
                method,
                &format!("/api/cyclones{path}"),
                "127.0.0.1",
            )
            .await;
            (
                status,
                headers,
                serde_json::from_slice(&body).unwrap_or(Value::Null),
            )
        }

        async fn get(&self) -> Value {
            self.request("/", "GET").await.2
        }
    }

    fn fixed(now: i64) -> Clock {
        Arc::new(move || now)
    }

    #[test]
    fn status_retains_official_clocks_knots_links_and_position() {
        assert_eq!(parse_date(TIME), Some(NOW - 35 * 60_000));
        let storm = &parse_cyclone_status(&status(), NOW).unwrap()[0];
        assert_eq!(storm["advisoryNumber"], json!("10"));
        assert_eq!(storm["issuedAt"], json!(TIME));
        assert_eq!(storm["positionAt"], json!(TIME));
        assert_eq!(
            storm["position"],
            json!({ "longitude": -125.8, "latitude": 15.5 })
        );
        assert_eq!(
            storm["movement"],
            json!({ "directionDegrees": 255, "speedKt": 12 })
        );
        assert_eq!(storm["windKt"], json!(25));
        assert_eq!(storm["pressureHpa"], json!(1006));
        assert!(
            storm["advisoryUrl"]
                .as_str()
                .unwrap()
                .starts_with("https://www.nhc.noaa.gov/text/")
        );
        assert_eq!(storm["basin"], json!("EP"));
        assert_eq!(
            storm["outlookUrl"],
            json!("https://www.nhc.noaa.gov/gtwo.php?basin=epac&fdays=7")
        );
        let missing = &parse_cyclone_status(
            &status_with(json!({ "intensity": "9999", "pressure": null, "movementSpeed": "" })),
            NOW,
        )
        .unwrap()[0];
        assert_eq!(missing["windKt"], Value::Null);
        assert_eq!(missing["pressureHpa"], Value::Null);
        assert_eq!(missing["movement"]["speedKt"], Value::Null);
        let evil = status_with(json!({ "forecastAdvisory": {
            "advNum": "10", "issuance": TIME, "url": "https://www.nhc.noaa.gov.evil/secret"
        }}));
        assert_eq!(
            parse_cyclone_status(&evil, NOW).unwrap()[0]["advisoryUrl"],
            Value::Null
        );
    }

    #[test]
    fn malformed_duplicate_oversized_future_and_expired_status_is_rejected() {
        let storm = status()["activeStorms"][0].clone();
        let payloads = vec![
            json!({}),
            json!({ "activeStorms": null }),
            json!({ "activeStorms": vec![storm.clone(); 33] }),
            json!({ "activeStorms": [storm.clone(), storm.clone()] }),
            status_with(json!({ "id": "wp012026" })),
            status_with(json!({ "name": "a".repeat(81) })),
            status_with(json!({ "name": "<script>" })),
            status_with(json!({ "longitudeNumeric": "x" })),
            status_with(json!({ "latitudeNumeric": 91 })),
            status_with(json!({ "lastUpdate": "2026-09-17T00:00:00.000Z" })),
            status_with(json!({ "lastUpdate": "2026-02-30T00:00:00.000Z" })),
            status_with(
                json!({ "forecastAdvisory": { "advNum": "10", "issuance": "2026-09-15T03:00:00.000Z" } }),
            ),
        ];
        for payload in payloads {
            assert_eq!(
                parse_cyclone_status(&payload, NOW),
                Err(Invalid),
                "{payload}"
            );
        }
        assert_eq!(
            parse_cyclone_status(&json!({ "activeStorms": [] }), NOW),
            Ok(vec![])
        );
    }

    #[test]
    fn coherent_geometry_is_retained_while_older_advisory_geometry_is_omitted() {
        let storms = parse_cyclone_status(&status(), NOW).unwrap();
        let mut parts = collections("10");
        parts[1]["features"][0]["geometry"]["coordinates"] = json!([[179, 15], [-179, 16]]);
        parts[2]["features"][0]["geometry"]["coordinates"]
            .as_array_mut()
            .unwrap()
            .push(json!([
                [-126.5, 15.5],
                [-127, 15.5],
                [-126.8, 16],
                [-126.5, 15.5]
            ]));
        let good = &attach_cyclone_geometry(&storms, &parts).unwrap()[0];
        assert_eq!(good["geometryStatus"], json!("current"));
        assert_eq!(good["geometryAdvisoryNumber"], json!("10"));
        assert_eq!(good["track"], parts[1]["features"][0]["geometry"]);
        assert_eq!(good["cone"]["coordinates"].as_array().unwrap().len(), 2);
        assert_eq!(good["forecastPoints"][0]["gustKt"], Value::Null);
        assert_eq!(good["forecastPoints"][0]["tauHours"], json!(0));
        assert!(good["forecastPoints"][0].get("validAt").is_none());
        let old = &attach_cyclone_geometry(&storms, &collections("9")).unwrap()[0];
        assert_eq!(old["geometryStatus"], json!("pending"));
        assert_eq!(old["track"], Value::Null);
        assert_eq!(old["cone"], Value::Null);
        assert_eq!(old["forecastPoints"], json!([]));
        assert_eq!(old["position"], storms[0]["position"]);
        let mut mixed = collections("10");
        mixed[2] = collections("9")[2].clone();
        let partial = &attach_cyclone_geometry(&storms, &mixed).unwrap()[0];
        assert_eq!(partial["geometryStatus"], json!("pending"));
        assert_eq!(partial["track"], Value::Null);
    }

    #[test]
    fn geometry_rejects_truncation_duplicates_invalid_coordinates_and_budgets() {
        let storms = parse_cyclone_status(&status(), NOW).unwrap();
        #[allow(clippy::type_complexity)]
        let mutations: Vec<Box<dyn Fn(&mut Vec<Value>)>> = vec![
            Box::new(|p| p[0]["exceededTransferLimit"] = json!(true)),
            Box::new(|p| {
                let f = p[0]["features"][0].clone();
                p[0]["features"] = json!(vec![f; 501]);
            }),
            Box::new(|p| {
                let f = p[1]["features"][0].clone();
                p[1]["features"].as_array_mut().unwrap().push(f);
            }),
            Box::new(|p| {
                let f = p[0]["features"][0].clone();
                p[0]["features"].as_array_mut().unwrap().push(f);
            }),
            Box::new(|p| {
                p[0]["features"][0]["properties"]["idp_source"] = json!("ep152026-009_5day_pts")
            }),
            Box::new(|p| p[1]["features"][0]["geometry"]["coordinates"][1] = json!([181, 1])),
            Box::new(|p| {
                p[2]["features"][0]["geometry"]["coordinates"][0]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            }),
            Box::new(|p| {
                p[1]["features"][0]["geometry"]["coordinates"] = json!(vec![json!([0, 0]); 10_001])
            }),
            Box::new(|p| {
                p[1]["features"][0]["geometry"] = json!({
                    "type": "MultiLineString",
                    "coordinates": vec![vec![json!([0, 0]); 9000]; 3],
                })
            }),
            Box::new(|p| {
                p[2]["features"][0]["geometry"] =
                    json!({ "type": "GeometryCollection", "geometries": [] })
            }),
        ];
        for mutate in mutations {
            let mut parts = collections("10");
            mutate(&mut parts);
            assert_eq!(attach_cyclone_geometry(&storms, &parts), Err(Invalid));
        }
    }

    #[tokio::test]
    async fn serves_only_fixed_bounded_noaa_queries_caching_a_complete_snapshot() {
        let calls: Calls = Default::default();
        let harness = Harness::new(good_fetcher(Arc::clone(&calls)), fixed(NOW));
        let (status, headers, snapshot) = harness.request("/", "GET").await;
        assert_eq!(status, 200);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(snapshot["unavailable"], json!(false));
        assert_eq!(snapshot["stale"], json!(false));
        assert!(
            snapshot["coverage"]
                .as_str()
                .unwrap()
                .contains("not worldwide")
        );
        assert_eq!(snapshot["storms"][0]["geometryStatus"], json!("current"));
        assert_eq!(snapshot["fetchedAt"], json!(NOW));
        {
            let calls = calls.lock();
            assert_eq!(calls.len(), 4);
            assert_eq!(calls[0].url, "https://www.nhc.noaa.gov/CurrentStorms.json");
            assert!(calls.iter().all(|c| c.redirect == Redirect::Error));
            for call in &calls[1..] {
                let url = url::Url::parse(&call.url).unwrap();
                assert_eq!(
                    url.origin().ascii_serialization(),
                    "https://mapservices.weather.noaa.gov"
                );
                let get = |k: &str| {
                    url.query_pairs()
                        .find(|(key, _)| key == k)
                        .map(|(_, v)| v.into_owned())
                };
                assert_eq!(get("outSR").as_deref(), Some("4326"));
                assert_eq!(get("f").as_deref(), Some("geojson"));
                assert_ne!(get("outFields").as_deref(), Some("*"));
            }
            let track = url::Url::parse(&calls[2].url).unwrap();
            assert_eq!(
                track
                    .query_pairs()
                    .find(|(k, _)| k == "outFields")
                    .unwrap()
                    .1,
                "idp_source,advisnum"
            );
        }
        assert_eq!(harness.get().await, snapshot);
        assert_eq!(calls.lock().len(), 4);
    }

    #[tokio::test]
    async fn query_and_method_rejection_cannot_widen_provider_work() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Err(FetchError::Network("unexpected".into()))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        for url in [
            "/?url=https://evil",
            "/?storm=ep152026",
            "/other",
            "/?bbox=0,0,1,1",
        ] {
            assert_eq!(harness.request(url, "GET").await.0, 400, "{url}");
        }
        assert_eq!(harness.request("", "GET").await.0, 200);
        assert_eq!(harness.request("/", "POST").await.0, 405);
        assert_eq!(calls.lock().len(), 1);
    }

    #[tokio::test]
    async fn empty_status_skips_gis_and_broken_gis_keeps_honest_positions() {
        let calls: Calls = Default::default();
        let empty = json!({ "activeStorms": [] });
        let fetch = testing::fetcher(Arc::clone(&calls), move |request| {
            Ok(answer(request, &empty, &[]))
        });
        let harness = Harness::new(fetch, fixed(NOW));
        assert_eq!(harness.get().await["storms"], json!([]));
        assert_eq!(harness.get().await["unavailable"], json!(false));
        assert_eq!(calls.lock().len(), 1);
        let ok = status();
        let fetch = testing::fetcher(Default::default(), move |request| {
            Ok(if request.url.contains("CurrentStorms") {
                FetchResponse::json(&ok)
            } else {
                FetchResponse::from_bytes(503, &[], "secret detail")
            })
        });
        let snapshot = Harness::new(fetch, fixed(NOW)).get().await;
        assert_eq!(snapshot["unavailable"], json!(false));
        assert_eq!(
            snapshot["storms"][0]["geometryStatus"],
            json!("unavailable")
        );
        assert_eq!(snapshot["storms"][0]["track"], Value::Null);
        assert!(!snapshot.to_string().contains("secret"));
    }

    #[tokio::test]
    async fn ttl_failure_cooldown_and_age_limited_last_good() {
        let (clock, now) = testing::manual_clock(NOW);
        let fail = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let good = good_fetcher(Default::default());
        let fetch: Fetcher = {
            let fail = Arc::clone(&fail);
            let calls = Arc::clone(&calls);
            Arc::new(move |request| {
                calls.fetch_add(1, Ordering::SeqCst);
                if fail.load(Ordering::SeqCst) {
                    return Box::pin(async { Err(FetchError::Network("private".into())) });
                }
                good(request)
            })
        };
        let harness = Harness::new(fetch, clock);
        harness.get().await;
        now.fetch_add(299_000, Ordering::SeqCst);
        harness.get().await;
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        fail.store(true, Ordering::SeqCst);
        now.fetch_add(2000, Ordering::SeqCst);
        let stale = harness.get().await;
        assert_eq!(stale["stale"], json!(true));
        assert_eq!(stale["fetchedAt"], json!(NOW));
        assert_eq!(stale["storms"][0]["issuedAt"], json!(TIME));
        assert_eq!(
            stale["reason"],
            json!("Cached cyclone advisory; upstream unavailable")
        );
        harness.get().await;
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        now.fetch_add(12 * 3_600_000, Ordering::SeqCst);
        let expired = harness.get().await;
        assert_eq!(expired["unavailable"], json!(true));
        assert_eq!(expired["storms"], json!([]));
    }

    #[tokio::test]
    async fn shared_refresh_survives_one_disconnect_and_aborts_when_its_last_consumer_leaves() {
        // Two consumers: one leaves, the other still gets the snapshot.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let good = good_fetcher(Default::default());
        let fetch: Fetcher = {
            let gate = Arc::clone(&gate);
            Arc::new(move |request: FetchRequest| {
                let gate = Arc::clone(&gate);
                let good = good.clone();
                Box::pin(async move {
                    if request.url.contains("CurrentStorms") {
                        let _ = gate.acquire().await;
                    }
                    good(request).await
                })
            })
        };
        let harness = Arc::new(Harness::new(fetch, fixed(NOW)));
        let h = Arc::clone(&harness);
        let a = tokio::spawn(async move { h.get().await });
        let h = Arc::clone(&harness);
        let b = tokio::spawn(async move { h.get().await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        a.abort();
        assert!(a.await.is_err());
        gate.add_permits(100);
        assert_eq!(b.await.unwrap()["unavailable"], json!(false));

        // A lone consumer leaving cancels the refresh without a cooldown.
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let good = good_fetcher(Default::default());
        let fetch: Fetcher = {
            let calls = Arc::clone(&calls);
            let dropped = Arc::clone(&dropped);
            Arc::new(move |request: FetchRequest| {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let dropped = Arc::clone(&dropped);
                let good = good.clone();
                Box::pin(async move {
                    if n > 1 {
                        return good(request).await;
                    }
                    let _guard = DropFlag(dropped);
                    std::future::pending::<()>().await;
                    unreachable!()
                })
            })
        };
        let harness = Arc::new(Harness::new(fetch, fixed(NOW)));
        let h = Arc::clone(&harness);
        let c = tokio::spawn(async move { h.get().await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        c.abort();
        let _ = c.await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(dropped.load(Ordering::SeqCst), "the refresh is cancelled");
        assert_eq!(harness.get().await["unavailable"], json!(false));
    }

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn stream_caps_and_refresh_deadline_cancel_oversized_or_stalled_bodies() {
        for stalled in [false, true] {
            let dropped = Arc::new(AtomicBool::new(false));
            let flag = Arc::clone(&dropped);
            let fetch = testing::fetcher(Default::default(), move |_| {
                let body = if stalled {
                    testing::stalled(Arc::clone(&flag))
                } else {
                    testing::endless(
                        bytes::Bytes::from(vec![b' '; 128 * 1024 + 1]),
                        Arc::clone(&flag),
                    )
                };
                Ok(FetchResponse::from_stream(200, &[], body))
            });
            let harness = Harness::with_timeout(fetch, fixed(NOW), Duration::from_millis(15));
            assert_eq!(harness.get().await["unavailable"], json!(true));
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn shared_refresh_caps_waiting_consumers_and_rejects_redirects() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let good = good_fetcher(Default::default());
        let fetch: Fetcher = {
            let gate = Arc::clone(&gate);
            Arc::new(move |request: FetchRequest| {
                let gate = Arc::clone(&gate);
                let good = good.clone();
                Box::pin(async move {
                    if request.url.contains("CurrentStorms") {
                        let _ = gate.acquire().await;
                    }
                    good(request).await
                })
            })
        };
        let harness = Arc::new(Harness::new(fetch, fixed(NOW)));
        let waiting: Vec<_> = (0..32)
            .map(|_| {
                let h = Arc::clone(&harness);
                tokio::spawn(async move { h.request("/", "GET").await.0 })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let busy = harness.request("/", "GET").await;
        assert_eq!(busy.0, 429);
        assert_eq!(busy.1["retry-after"], "2");
        gate.add_permits(100);
        for task in waiting {
            assert_eq!(task.await.unwrap(), 200);
        }
        let fetch = testing::fetcher(Default::default(), |_| {
            Ok(FetchResponse::from_bytes(
                302,
                &[("location", "http://127.0.0.1/")],
                "private",
            ))
        });
        assert_eq!(
            Harness::new(fetch, fixed(NOW)).get().await["unavailable"],
            json!(true)
        );
    }
}
