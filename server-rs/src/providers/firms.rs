//! NASA FIRMS live active-fire proxy (port of `server/providers/firms.js`
//! and `src/data/firmsCsv.js`).
//!
//! Merges VIIRS NOAA-20, NOAA-21, Suomi-NPP and MODIS NRT detections,
//! fetched sequentially with `days=2` and clamped to the trailing 24 h.
//! TTL 30 min, single-flight refresh, serve-stale on failure, and a disk
//! cache (`.ce-cache/firms.json`) that survives restarts.
//!
//! - `/api/firms`        → `{fetchedAt, stale, ttlMs, sources, count, fires}`
//! - `/api/firms/status` → `{hasKey, lastFetch, count, stale, ttlMs, transactions}`
//!
//! Keyless (no `FIRMS_MAP_KEY`): `/api/firms` → 503 `{error:'no_key'}`;
//! status → `{hasKey:false}`. Upstream is never touched without a key.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::hazard_support::{
    Clock, FetchError, FetchRequest, Fetcher, encode_uri_component, jnum, mount, read_all,
    reqwest_fetcher, ser_num, sub_url, system_clock, with_deadline,
};
use crate::common::coalesce::Coalescer;
use crate::common::{disk, js, reply};
use crate::state::AppState;

const TTL_MS: i64 = 30 * 60_000;
const STATUS_TTL_MS: i64 = 5 * 60_000;
const SOURCES: [&str; 4] = [
    "VIIRS_NOAA20_NRT",
    "VIIRS_NOAA21_NRT",
    "VIIRS_SNPP_NRT",
    "MODIS_NRT",
];
const SOURCE_TIMEOUT: Duration = Duration::from_secs(60);
const STATUS_TIMEOUT: Duration = Duration::from_secs(10);

// ─── CSV parsing (src/data/firmsCsv.js) ─────────────────────

/// Header fields that must all be present for a payload to count as FIRMS CSV.
const REQUIRED_HEADER_FIELDS: [&str; 6] = [
    "latitude",
    "longitude",
    "acq_date",
    "acq_time",
    "confidence",
    "frp",
];
const HOUR_MS: i64 = 3_600_000;
const WINDOW_MS: i64 = 24 * HOUR_MS;
const FORWARD_SLACK_MS: i64 = 2 * HOUR_MS;

/// One detection, as the browser receives it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct FireRecord {
    #[serde(serialize_with = "ser_num")]
    pub lat: f64,
    #[serde(serialize_with = "ser_num")]
    pub lon: f64,
    #[serde(serialize_with = "ser_num")]
    pub frp: f64,
    /// Categorical (l/n/h) or numeric — passed through raw.
    pub confidence: String,
    #[serde(serialize_with = "ser_num")]
    pub brightness: f64,
    #[serde(serialize_with = "ser_num")]
    pub brightness_ti5: f64,
    pub daynight: String,
    pub acq_date: String,
    /// NOT zero-padded — kept verbatim.
    pub acq_time: String,
    pub satellite: String,
    pub instrument: String,
}

fn js_whitespace(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
}

/// `isLikelyCsv`: HTML error pages and text errors fail; any payload whose
/// first line carries the expected header fields passes.
pub fn is_likely_csv(text: &str) -> bool {
    let trimmed = text.trim_start_matches(js_whitespace);
    if trimmed.is_empty() || trimmed.starts_with('<') {
        return false;
    }
    let header_line = trimmed
        .split('\n')
        .next()
        .unwrap_or("")
        .trim_matches(js_whitespace)
        .to_lowercase();
    let fields: Vec<&str> = header_line
        .split(',')
        .map(|f| f.trim_matches(js_whitespace))
        .collect();
    REQUIRED_HEADER_FIELDS
        .iter()
        .all(|required| fields.contains(required))
}

/// `parseFirmsCsv`: detection records, or `None` for non-CSV input.
pub fn parse_firms_csv(text: &str) -> Option<Vec<FireRecord>> {
    if !is_likely_csv(text) {
        return None;
    }
    let lines: Vec<&str> = text.split('\n').collect();
    let mut header_index = 0;
    while header_index < lines.len() && lines[header_index].trim_matches(js_whitespace).is_empty() {
        header_index += 1;
    }
    let header: Vec<String> = lines[header_index]
        .trim_matches(js_whitespace)
        .to_lowercase()
        .split(',')
        .map(|f| f.trim_matches(js_whitespace).to_string())
        .collect();
    let mut col: HashMap<&str, usize> = HashMap::new();
    for (i, name) in header.iter().enumerate() {
        col.insert(name.as_str(), i);
    }
    let get = |name: &str| col.get(name).copied();
    let i_lat = get("latitude");
    let i_lon = get("longitude");
    let i_frp = get("frp");
    let i_confidence = get("confidence");
    let i_brightness = get("bright_ti4").or_else(|| get("brightness"));
    let i_brightness_ti5 = get("bright_ti5").or_else(|| get("bright_t31"));
    let i_daynight = get("daynight");
    let i_acq_date = get("acq_date");
    let i_acq_time = get("acq_time");
    let i_satellite = get("satellite");
    let i_instrument = get("instrument");

    let number_at = |parts: &[&str], index: Option<usize>| -> f64 {
        index
            .and_then(|i| parts.get(i))
            .map_or(f64::NAN, |part| js::number(part))
    };
    let finite_or_zero = |parts: &[&str], index: Option<usize>| -> f64 {
        let n = number_at(parts, index);
        if n.is_finite() { n } else { 0.0 }
    };
    let cell = |parts: &[&str], index: Option<usize>| -> String {
        index
            .and_then(|i| parts.get(i))
            .map_or(String::new(), |part| {
                part.trim_matches(js_whitespace).to_string()
            })
    };

    let mut records = Vec::new();
    for raw in lines.iter().skip(header_index + 1) {
        let line = raw.trim_matches(js_whitespace);
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < header.len() {
            continue; // malformed row — skip
        }
        let lat = number_at(&parts, i_lat);
        let lon = number_at(&parts, i_lon);
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        records.push(FireRecord {
            lat,
            lon,
            frp: finite_or_zero(&parts, i_frp),
            confidence: cell(&parts, i_confidence),
            brightness: finite_or_zero(&parts, i_brightness),
            brightness_ti5: finite_or_zero(&parts, i_brightness_ti5),
            daynight: cell(&parts, i_daynight),
            acq_date: cell(&parts, i_acq_date),
            acq_time: cell(&parts, i_acq_time),
            satellite: cell(&parts, i_satellite),
            instrument: cell(&parts, i_instrument),
        });
    }
    Some(records)
}

/// `Date.UTC(year, monthIndex, day, hours, minutes)` with JS day overflow.
fn date_utc(year: i64, month_index: i64, day: i64, hours: i64, minutes: i64) -> Option<i64> {
    let year = if (0..=99).contains(&year) {
        1900 + year
    } else {
        year
    };
    let first =
        chrono::NaiveDate::from_ymd_opt(i32::try_from(year).ok()?, (month_index + 1) as u32, 1)?;
    let days = first.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis() / 86_400_000 + (day - 1);
    Some(days * 86_400_000 + hours * HOUR_MS + minutes * 60_000)
}

/// `acquisitionMsUtc`: FIRMS `acq_date` + unpadded `acq_time` → epoch ms.
pub fn acquisition_ms_utc(acq_date: &str, acq_time: &str) -> Option<i64> {
    let date = acq_date.as_bytes();
    let date_ok = date.len() == 10
        && date[4] == b'-'
        && date[7] == b'-'
        && date
            .iter()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit());
    if !date_ok {
        return None;
    }
    let time_text = acq_time.trim_matches(js_whitespace);
    if time_text.is_empty() || time_text.len() > 4 || !time_text.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let hhmm = format!("{time_text:0>4}");
    let year: i64 = acq_date[0..4].parse().ok()?;
    let month: i64 = acq_date[5..7].parse().ok()?;
    let day: i64 = acq_date[8..10].parse().ok()?;
    let hours: i64 = hhmm[0..2].parse().ok()?;
    let minutes: i64 = hhmm[2..4].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hours > 23 || minutes > 59 {
        return None;
    }
    date_utc(year, month - 1, day, hours, minutes)
}

/// `filterTrailing24h`: records acquired within `[now − 24 h, now + 2 h]`.
pub fn filter_trailing_24h(records: &[FireRecord], now_ms: i64) -> Vec<FireRecord> {
    let oldest = now_ms - WINDOW_MS;
    let newest = now_ms + FORWARD_SLACK_MS;
    let mut memo: HashMap<(&str, &str), Option<i64>> = HashMap::new();
    records
        .iter()
        .filter(|record| {
            let ms = *memo
                .entry((record.acq_date.as_str(), record.acq_time.as_str()))
                .or_insert_with(|| acquisition_ms_utc(&record.acq_date, &record.acq_time));
            ms.is_some_and(|ms| ms >= oldest && ms <= newest)
        })
        .cloned()
        .collect()
}

// ─── Proxy ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    at: i64,
    sources: Vec<Value>,
    fires: Vec<FireRecord>,
}

impl CacheEntry {
    fn from_disk(value: &Value) -> Option<Self> {
        let at = value.get("at")?.as_f64().filter(|n| n.is_finite())? as i64;
        let sources = value.get("sources")?.as_array()?.clone();
        let fires = serde_json::from_value(value.get("fires")?.clone()).ok()?;
        Some(Self { at, sources, fires })
    }
}

type Transactions = Option<(f64, f64)>;

#[derive(Clone)]
pub struct Deps {
    pub fetch: Fetcher,
    pub now: Clock,
    pub cache_dir: PathBuf,
}

struct Firms {
    deps: Deps,
    mem: Mutex<Option<Arc<CacheEntry>>>,
    disk_checked: tokio::sync::OnceCell<()>,
    inflight: Coalescer<(), Option<Arc<CacheEntry>>>,
    status_cache: Mutex<Option<(i64, Transactions)>>,
    status_inflight: Coalescer<(), Transactions>,
}

pub fn router(state: &AppState) -> Router<AppState> {
    router_with(Deps {
        fetch: reqwest_fetcher(),
        now: system_clock(),
        cache_dir: state.cache_dir.clone(),
    })
}

fn router_with(deps: Deps) -> Router<AppState> {
    let firms = Arc::new(Firms {
        deps,
        mem: Mutex::new(None),
        disk_checked: tokio::sync::OnceCell::new(),
        inflight: Coalescer::new(),
        status_cache: Mutex::new(None),
        status_inflight: Coalescer::new(),
    });
    mount(
        "/api/firms",
        move |State(state): State<AppState>, request: Request| {
            let firms = Arc::clone(&firms);
            async move { firms.handle(&state, request).await }
        },
    )
}

fn send_json(status: u16, value: &Value) -> Response {
    reply::json(status, value, Some("no-store"))
}

/// `Number(value)` for a parsed JSON value.
fn js_number_of(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Null) => 0.0,
        Some(Value::Bool(b)) => f64::from(u8::from(*b)),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => js::number(s),
        Some(Value::Array(items)) if items.is_empty() => 0.0,
        Some(Value::Array(items)) if items.len() == 1 => js_number_of(items.first()),
        Some(_) => f64::NAN,
    }
}

/// Refresh every source sequentially (quota courtesy — never in parallel).
/// Partial success still produces an entry with failed sources marked
/// `ok:false`; total failure is an error so the caller can serve stale.
async fn refresh_sources<F, Fut>(now: i64, mut fetch_source: F) -> Result<CacheEntry, &'static str>
where
    F: FnMut(&'static str) -> Fut,
    Fut: Future<Output = Result<Vec<FireRecord>, String>>,
{
    let mut sources = Vec::new();
    let mut fires = Vec::new();
    for source in SOURCES {
        match fetch_source(source).await {
            Ok(records) => {
                let records = filter_trailing_24h(&records, now);
                sources.push(json!({ "source": source, "count": records.len(), "ok": true }));
                fires.extend(records);
            }
            Err(message) => {
                tracing::warn!("[firms-proxy] {source} fetch failed: {message}");
                sources.push(json!({ "source": source, "count": 0, "ok": false }));
            }
        }
    }
    if !sources.iter().any(|s| s["ok"] == Value::Bool(true)) {
        return Err("all FIRMS sources failed");
    }
    Ok(CacheEntry {
        at: now,
        sources,
        fires,
    })
}

impl Firms {
    fn cache_path(&self) -> PathBuf {
        self.deps.cache_dir.join("firms.json")
    }

    async fn read_disk_once(&self) {
        self.disk_checked
            .get_or_init(|| async {
                if let Some(value) = disk::read_json::<Value>(&self.cache_path()).await
                    && let Some(entry) = CacheEntry::from_disk(&value)
                {
                    *self.mem.lock() = Some(Arc::new(entry));
                }
            })
            .await;
    }

    /// Fetch + parse one source. Never log the URL — it embeds the MAP_KEY.
    async fn fetch_source(
        fetch: Fetcher,
        key: String,
        source: &'static str,
    ) -> Result<Vec<FireRecord>, String> {
        let url = format!(
            "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{}/{source}/world/2",
            encode_uri_component(&key)
        );
        let text = with_deadline(SOURCE_TIMEOUT, async {
            let response = fetch(FetchRequest::get(url)).await?;
            if !response.ok() {
                return Err(FetchError::Network(format!("HTTP {}", response.status)));
            }
            read_all(response).await
        })
        .await
        .map_err(|e| match e {
            FetchError::Network(m) if m.starts_with("HTTP ") => m,
            other => other.to_string(),
        })?;
        parse_firms_csv(&String::from_utf8_lossy(&text))
            .ok_or_else(|| "non-CSV upstream response".to_string())
    }

    /// Cache entry → response payload, re-filtered to the trailing 24 h at
    /// serve time so a stale cache never serves >24h-old detections.
    fn build_payload(&self, entry: &CacheEntry, stale: bool) -> Value {
        let fires = filter_trailing_24h(&entry.fires, (self.deps.now)());
        json!({
            "fetchedAt": entry.at,
            "stale": stale,
            "ttlMs": TTL_MS,
            "sources": entry.sources,
            "count": fires.len(),
            "fires": fires,
        })
    }

    /// `mapkey_status` transactions, cached 5 min, best-effort.
    async fn transactions(self: &Arc<Self>, key: &str) -> Transactions {
        let now = (self.deps.now)();
        if let Some((at, transactions)) = *self.status_cache.lock()
            && now - at < STATUS_TTL_MS
        {
            return transactions;
        }
        let this = Arc::clone(self);
        let key = key.to_string();
        let (transactions, _) = self
            .status_inflight
            .run((), move || async move {
                tokio::spawn(async move {
                    let url = format!(
                        "https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY={}",
                        encode_uri_component(&key)
                    );
                    let fetch = this.deps.fetch.clone();
                    let result = with_deadline(STATUS_TIMEOUT, async {
                        let response = fetch(FetchRequest::get(url)).await?;
                        if !response.ok() {
                            return Err(FetchError::Network(format!("HTTP {}", response.status)));
                        }
                        read_all(response).await
                    })
                    .await;
                    let transactions = match result {
                        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                            Ok(body) => {
                                let used = js_number_of(body.get("current_transactions"));
                                let limit = js_number_of(body.get("transaction_limit"));
                                (used.is_finite() && limit.is_finite()).then_some((used, limit))
                            }
                            Err(_) => {
                                tracing::warn!("[firms-proxy] mapkey status failed: invalid JSON");
                                None
                            }
                        },
                        Err(error) => {
                            tracing::warn!("[firms-proxy] mapkey status failed: {error}");
                            None
                        }
                    };
                    *this.status_cache.lock() = Some(((this.deps.now)(), transactions));
                    transactions
                })
                .await
                .ok()
                .flatten()
            })
            .await;
        transactions
    }

    async fn handle(self: Arc<Self>, state: &AppState, request: Request) -> Response {
        let url = sub_url(&request, "/api/firms");
        let sub_path = url.split('?').next().unwrap_or("");
        let key = state
            .env
            .raw("FIRMS_MAP_KEY")
            .unwrap_or_default()
            .trim()
            .to_string();
        self.read_disk_once().await;

        if sub_path == "/status" {
            if key.is_empty() {
                return send_json(
                    200,
                    &json!({
                        "hasKey": false,
                        "lastFetch": null,
                        "count": null,
                        "stale": false,
                        "ttlMs": TTL_MS,
                        "transactions": null,
                    }),
                );
            }
            let transactions = self.transactions(&key).await;
            let mem = self.mem.lock().clone();
            let now = (self.deps.now)();
            return send_json(
                200,
                &json!({
                    "hasKey": true,
                    "lastFetch": mem.as_ref().map(|m| m.at),
                    "count": mem.as_ref().map(|m| m.fires.len()),
                    "stale": mem.as_ref().is_some_and(|m| now - m.at >= TTL_MS),
                    "ttlMs": TTL_MS,
                    "transactions": transactions.map(|(used, limit)| json!({ "used": jnum(used), "limit": jnum(limit) })),
                }),
            );
        }

        if key.is_empty() {
            return send_json(503, &json!({ "error": "no_key" }));
        }

        let entry = self.mem.lock().clone();
        if let Some(entry) = &entry
            && (self.deps.now)() - entry.at < TTL_MS
        {
            return send_json(200, &self.build_payload(entry, false));
        }
        // Stale or missing → refresh, single-flight.
        let this = Arc::clone(&self);
        let (fresh, _) = self
            .inflight
            .run((), move || async move {
                tokio::spawn(async move { this.refresh(key).await })
                    .await
                    .ok()
                    .flatten()
            })
            .await;
        match (fresh, entry) {
            (Some(fresh), _) => send_json(200, &self.build_payload(&fresh, false)),
            // Upstream down — stale beats empty.
            (None, Some(entry)) => send_json(200, &self.build_payload(&entry, true)),
            (None, None) => send_json(
                502,
                &json!({ "error": "firms fetch failed and no cache available" }),
            ),
        }
    }

    async fn refresh(&self, key: String) -> Option<Arc<CacheEntry>> {
        let fetch = self.deps.fetch.clone();
        let result = refresh_sources((self.deps.now)(), |source| {
            Self::fetch_source(fetch.clone(), key.clone(), source)
        })
        .await;
        match result {
            Ok(fresh) => {
                let fresh = Arc::new(fresh);
                *self.mem.lock() = Some(Arc::clone(&fresh));
                if let Err(error) = disk::write_json(&self.cache_path(), &*fresh).await {
                    tracing::warn!("[firms-proxy] cache write failed: {error}");
                }
                Some(fresh)
            }
            Err(message) => {
                tracing::warn!("[firms-proxy] refresh failed ({message}) — serving cache if any");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::super::hazard_support::testing::{self, Calls};
    use super::super::hazard_support::{FetchResponse, parse_date};
    use super::*;

    const FIXTURE: &str = include_str!("../../../src/data/fixtures/firms-viirs-noaa20-sample.csv");
    const CASES: &str = include_str!("../../../src/data/fixtures/firms-csv-cases.json");
    const HEADER: &str = "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight";

    fn utc(y: i64, m: i64, d: i64, h: i64, min: i64) -> i64 {
        date_utc(y, m, d, h, min).unwrap()
    }

    #[test]
    fn fixture_parses_to_45_records_with_finite_lat_lon_frp() {
        let records = parse_firms_csv(FIXTURE).unwrap();
        assert_eq!(records.len(), 45);
        for r in &records {
            assert!(r.lat.is_finite() && r.lon.is_finite() && r.frp.is_finite());
            assert!((-90.0..=90.0).contains(&r.lat));
            assert!((-180.0..=180.0).contains(&r.lon));
        }
    }

    #[test]
    fn fixture_first_row_maps_every_field() {
        let first = &parse_firms_csv(FIXTURE).unwrap()[0];
        assert_eq!(first.lat, 38.99488);
        assert_eq!(first.lon, -121.67046);
        assert_eq!(first.brightness, 303.6);
        assert_eq!(first.brightness_ti5, 290.73);
        assert_eq!(first.frp, 0.53);
        assert_eq!(first.confidence, "n");
        assert_eq!(first.daynight, "N");
        assert_eq!(first.acq_date, "2026-07-16");
        assert_eq!(first.acq_time, "1006");
        assert_eq!(first.satellite, "N20");
        assert_eq!(first.instrument, "VIIRS");
    }

    #[test]
    fn modis_schema_maps_fields_and_preserves_numeric_confidence_raw() {
        let header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_t31,frp,daynight";
        let row =
            "34.123,-117.456,321.5,1.2,0.8,2026-07-16,1045,Aqua,MODIS,78,6.1NRT,290.25,12.4,D";
        let records = parse_firms_csv(&format!("{header}\n{row}\n")).unwrap();
        let r = &records[0];
        assert_eq!((r.lat, r.lon, r.frp), (34.123, -117.456, 12.4));
        assert_eq!((r.brightness, r.brightness_ti5), (321.5, 290.25));
        assert_eq!(r.daynight, "D");
        assert_eq!(r.acq_time, "1045");
        assert_eq!(r.satellite, "Aqua");
        assert_eq!(r.instrument, "MODIS");
        assert_eq!(r.confidence, "78");
    }

    #[test]
    fn acquisition_times_pad_and_reject_garbage() {
        assert_eq!(
            acquisition_ms_utc("2026-07-16", "1006"),
            Some(utc(2026, 6, 16, 10, 6))
        );
        assert_eq!(
            acquisition_ms_utc("2026-07-16", "45"),
            Some(utc(2026, 6, 16, 0, 45))
        );
        assert_eq!(
            acquisition_ms_utc("2026-07-16", "0"),
            Some(utc(2026, 6, 16, 0, 0))
        );
        assert_eq!(acquisition_ms_utc("not-a-date", "1006"), None);
        assert_eq!(acquisition_ms_utc("2026-07-16", "xx"), None);
        assert_eq!(
            utc(2026, 6, 16, 10, 6),
            parse_date("2026-07-16T10:06:00Z").unwrap()
        );
    }

    #[test]
    fn header_only_crlf_and_malformed_rows() {
        assert_eq!(parse_firms_csv(&format!("{HEADER}\n")), Some(vec![]));
        assert_eq!(parse_firms_csv(HEADER), Some(vec![]));
        let text = format!(
            "{HEADER}\r\n38.99488,-121.67046,303.6,0.39,0.36,2026-07-16,1006,N20,VIIRS,n,2.0NRT,290.73,0.53,N\r\n\r\n"
        );
        let records = parse_firms_csv(&text).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].daynight, "N");
        let text = [
            HEADER,
            "not,enough,fields",
            "garbage-line-with-no-commas",
            "NaN,-121.67046,303.6,0.39,0.36,2026-07-16,1006,N20,VIIRS,n,2.0NRT,290.73,0.53,N",
            "38.99488,-121.67046,303.6,0.39,0.36,2026-07-16,1006,N20,VIIRS,n,2.0NRT,290.73,0.53,N",
        ]
        .join("\n");
        let records = parse_firms_csv(&text).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lat, 38.99488);
    }

    #[test]
    fn upstream_errors_are_not_csv() {
        let html = "\n  <html><body><h1>Service unavailable</h1></body></html>";
        assert!(!is_likely_csv(html));
        assert_eq!(parse_firms_csv(html), None);
        assert!(!is_likely_csv("Invalid MAP_KEY."));
        assert_eq!(parse_firms_csv("Invalid MAP_KEY."), None);
        assert!(is_likely_csv(FIXTURE));
    }

    #[test]
    fn trailing_window_is_inclusive_with_forward_slack() {
        let now = utc(2026, 6, 16, 12, 0);
        let rec = |d: &str, t: &str| FireRecord {
            acq_date: d.into(),
            acq_time: t.into(),
            ..Default::default()
        };
        let records = vec![
            rec("2026-07-15", "1159"),
            rec("2026-07-15", "1200"),
            rec("2026-07-15", "1201"),
            rec("2026-07-16", "1130"),
            rec("2026-07-16", "1400"),
            rec("2026-07-16", "1401"),
            rec("bad-date", "1006"),
        ];
        let kept: Vec<String> = filter_trailing_24h(&records, now)
            .iter()
            .map(|r| format!("{} {}", r.acq_date, r.acq_time))
            .collect();
        assert_eq!(
            kept,
            vec![
                "2026-07-15 1200",
                "2026-07-15 1201",
                "2026-07-16 1130",
                "2026-07-16 1400"
            ]
        );
        let records = parse_firms_csv(FIXTURE).unwrap();
        assert_eq!(
            filter_trailing_24h(&records, utc(2026, 6, 17, 2, 0)).len(),
            records.len()
        );
    }

    #[test]
    fn portable_firms_csv_contract() {
        let cases: Vec<Value> = serde_json::from_str(CASES).unwrap();
        for case in cases {
            let records = parse_firms_csv(case["csv"].as_str().unwrap());
            let count = records.as_ref().map(Vec::len);
            assert_eq!(
                count.map(|c| c as u64),
                case["parsedCount"].as_u64(),
                "{}",
                case["name"]
            );
            let now = parse_date(case["now"].as_str().unwrap()).unwrap();
            let recent: Vec<Value> = filter_trailing_24h(&records.unwrap_or_default(), now)
                .into_iter()
                .map(|r| Value::from(r.acq_time))
                .collect();
            assert_eq!(Value::from(recent), case["recentTimes"], "{}", case["name"]);
        }
    }

    const NOW: i64 = 1_789_128_000_000; // 2026-09-11T12:00Z

    fn recent(marker: &str) -> FireRecord {
        FireRecord {
            acq_date: "2026-09-11".into(),
            acq_time: "1100".into(),
            satellite: marker.into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn refresh_retains_large_sources_in_order_and_filters_expired_rows() {
        assert_eq!(NOW, utc(2026, 8, 11, 12, 0));
        let header = "latitude,longitude,acq_date,acq_time,confidence,frp\n";
        let large = parse_firms_csv(&format!(
            "{header}{}",
            "1,2,2026-09-11,1100,n,4\n".repeat(200_000)
        ))
        .unwrap();
        let mut expired = recent("expired");
        expired.acq_date = "2026-09-09".into();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let result = refresh_sources(NOW, |source| {
            calls.lock().push(source);
            let out = if source == SOURCES[0] {
                let mut all = large.clone();
                all.push(expired.clone());
                all
            } else if source == SOURCES[1] {
                vec![recent("last")]
            } else {
                vec![]
            };
            async move { Ok(out) }
        })
        .await
        .unwrap();
        assert_eq!(*calls.lock(), SOURCES.to_vec());
        assert_eq!(result.fires.len(), 200_001);
        assert_eq!(result.fires.last().unwrap().satellite, "last");
        let counts: Vec<Value> = result.sources.iter().map(|s| s["count"].clone()).collect();
        assert_eq!(counts, vec![json!(200_000), json!(1), json!(0), json!(0)]);
        assert!(result.sources.iter().all(|s| s["ok"] == json!(true)));
    }

    #[tokio::test]
    async fn refresh_keeps_successful_sources_and_distinguishes_total_failure() {
        let result = refresh_sources(NOW, |source| async move {
            if source == SOURCES[1] {
                Err("upstream unavailable".to_string())
            } else {
                Ok(vec![recent("x")])
            }
        })
        .await
        .unwrap();
        assert_eq!(result.fires.len(), 3);
        let oks: Vec<Value> = result.sources.iter().map(|s| s["ok"].clone()).collect();
        assert_eq!(
            oks,
            vec![json!(true), json!(false), json!(true), json!(true)]
        );
        assert_eq!(result.sources[1]["count"], json!(0));
        let failed =
            refresh_sources(NOW, |_| async { Err("upstream unavailable".to_string()) }).await;
        assert_eq!(failed.unwrap_err(), "all FIRMS sources failed");
        let empty = refresh_sources(NOW, |_| async { Ok(vec![]) })
            .await
            .unwrap();
        assert!(empty.fires.is_empty());
        assert!(empty.sources.iter().all(|s| s["ok"] == json!(true)));
    }

    #[tokio::test]
    async fn middleware_retains_a_large_source_during_partial_failure_and_filters_at_serve_time() {
        let (clock, now) = testing::manual_clock(utc(2026, 8, 12, 12, 0));
        let header = "latitude,longitude,acq_date,acq_time,confidence,frp\n";
        let csv = format!(
            "{header}{}",
            "30,-97,2026-09-12,1200,h,10\n".repeat(130_001)
        );
        let offline = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let calls: Calls = Default::default();
        let flag = Arc::clone(&offline);
        let fetch = testing::fetcher(Arc::clone(&calls), move |request| {
            let url = url::Url::parse(&request.url).unwrap();
            assert_eq!(url.host_str(), Some("firms.modaps.eosdis.nasa.gov"));
            if flag.load(Ordering::SeqCst) {
                return Ok(FetchResponse::from_bytes(503, &[], "offline"));
            }
            if url.path().contains("mapkey_status") {
                return Ok(FetchResponse::json(
                    &json!({ "current_transactions": 3, "transaction_limit": 5000 }),
                ));
            }
            Ok(if url.path().contains("VIIRS_NOAA20") {
                FetchResponse::from_bytes(200, &[], csv.clone())
            } else {
                FetchResponse::from_bytes(503, &[], "offline")
            })
        });
        let router = router_with(Deps {
            fetch,
            now: clock,
            cache_dir: testing::temp_dir("firms"),
        });
        let state = testing::state();
        state.env.set("FIRMS_MAP_KEY", "");
        let get = |uri: &'static str| testing::send(&router, &state, "GET", uri, "127.0.0.1");
        let body = |res: (u16, http::HeaderMap, bytes::Bytes)| {
            serde_json::from_slice::<Value>(&res.2).unwrap()
        };
        let keyless = get("/api/firms").await;
        assert_eq!(keyless.0, 503);
        assert_eq!(keyless.1["cache-control"], "no-store");
        assert_eq!(body(get("/api/firms/status").await)["hasKey"], json!(false));
        assert_eq!(calls.lock().len(), 0);
        state.env.set("FIRMS_MAP_KEY", "fixture-key");
        let first = body(get("/api/firms").await);
        assert_eq!(first["count"], json!(130_001));
        assert_eq!(
            first["sources"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|s| s["ok"] == json!(true))
                .count(),
            1
        );
        assert_eq!(calls.lock().len(), 4);
        assert_eq!(body(get("/api/firms").await)["count"], json!(130_001));
        assert_eq!(calls.lock().len(), 4);
        assert_eq!(
            body(get("/api/firms/status").await)["transactions"],
            json!({ "used": 3, "limit": 5000 })
        );
        now.fetch_add(25 * 3_600_000, Ordering::SeqCst);
        offline.store(true, Ordering::SeqCst);
        let stale = body(get("/api/firms").await);
        assert_eq!(stale["stale"], json!(true));
        assert_eq!(stale["count"], json!(0));
    }
}
