//! The live aircraft and vessel sources: `src/sources/live/contract.js`,
//! `aircraft.js`, `vessels.js` and `standalone.js`.

use http::HeaderMap;
use serde_json::Value;

use super::fetch::{Api, Init};
use super::no_store;
use crate::mcp::catalog::Failure;
use crate::mcp::js;

/// `finite(value)`: a number from a number or numeric string, else None.
pub fn finite(value: Option<&Value>) -> Option<f64> {
    match value {
        None | Some(Value::Null) | Some(Value::Bool(_)) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        Some(value) => Some(js::to_number(value)).filter(|v| v.is_finite()),
    }
}

/// `epoch(value, scale)`: positive milliseconds within the Date range.
pub fn epoch(value: Option<&Value>, scale: f64) -> Option<f64> {
    epoch_number(finite(value), scale)
}

fn epoch_number(number: Option<f64>, scale: f64) -> Option<f64> {
    number
        .map(|n| n * scale)
        .filter(|ms| *ms > 0.0 && *ms <= 8_640_000_000_000_000.0)
}

/// `cleanText(value)`.
pub fn clean_text(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|s| js::trim(s).to_string())
        .unwrap_or_default()
}

fn coordinates(latitude: Option<f64>, longitude: Option<f64>) -> bool {
    matches!((latitude, longitude), (Some(lat), Some(lon))
        if lat.is_finite() && lat.abs() <= 90.0 && lon.is_finite() && lon.abs() <= 180.0)
}

/// A live-source HTTP failure (`httpError`).
pub fn http_error(status: u16, source: &str) -> Failure {
    let code = match status {
        429 => "limited",
        401 | 403 => "denied",
        _ => "unavailable",
    };
    Failure::Source {
        code: code.into(),
        message: if status == 429 {
            format!("{source} rate limited")
        } else {
            format!("{source} HTTP {status}")
        },
        status: Some(status),
        retry_after_ms: Some(if code == "unavailable" {
            20_000.0
        } else {
            45_000.0
        }),
    }
}

/// What `readResponse` returns: the response's status and headers and its
/// JSON body, or None when the body is not JSON.
pub struct Read {
    pub status: u16,
    pub headers: HeaderMap,
    pub payload: Option<Value>,
}

impl Read {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn header(&self, name: &str) -> Option<String> {
        let values: Vec<String> = self
            .headers
            .get_all(name)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect();
        (!values.is_empty()).then(|| values.join(", "))
    }
}

/// `readResponse`: a network failure becomes an `unavailable` source error.
pub async fn read_response(
    api: &Api,
    url: &str,
    init: Init,
    source: &str,
) -> Result<Read, Failure> {
    let response = api
        .fetch(url, init)
        .await
        .map_err(|_| Failure::source("unavailable", format!("{source} network error")))?;
    let status = response.status;
    let headers = response.headers.clone();
    let payload = response.json().await.ok();
    Ok(Read {
        status,
        headers,
        payload,
    })
}

/// `encodeURIComponent`.
pub fn encode_uri_component(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

// ------------------------------------------------------------ aircraft ----

/// An aircraft observation.
#[derive(Debug, Clone)]
pub struct Aircraft {
    pub id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub callsign: String,
    pub origin_country: Option<String>,
    pub position_time_ms: Option<f64>,
    pub contact_time_ms: Option<f64>,
    pub baro_altitude_m: Option<f64>,
    pub ellipsoid_altitude_m: Option<f64>,
    pub on_ground: bool,
    pub speed_mps: Option<f64>,
    pub course_deg: Option<f64>,
    pub vertical_rate_mps: Option<f64>,
    pub type_code: Option<String>,
    pub registration: Option<String>,
    pub operator: Option<String>,
}

/// A feed snapshot.
#[derive(Debug, Clone)]
pub struct Snapshot<R> {
    pub records: Vec<R>,
    pub source: Option<String>,
    pub coverage: Option<String>,
    pub observed_at_ms: Option<f64>,
    pub freshness: String,
}

fn normalize_open_sky(row: &Value) -> Option<Aircraft> {
    let row = row.as_array()?;
    let at = |i: usize| row.get(i);
    let id = clean_text(at(0)).to_lowercase();
    let latitude = at(6).and_then(Value::as_f64);
    let longitude = at(5).and_then(Value::as_f64);
    if id.is_empty() || !coordinates(latitude, longitude) {
        return None;
    }
    Some(Aircraft {
        id,
        latitude: latitude?,
        longitude: longitude?,
        callsign: clean_text(at(1)),
        origin_country: Some(clean_text(at(2))),
        position_time_ms: epoch(at(3), 1000.0),
        contact_time_ms: epoch(at(4), 1000.0),
        baro_altitude_m: finite(at(7)),
        ellipsoid_altitude_m: finite(at(13)),
        on_ground: at(8) == Some(&Value::Bool(true)),
        speed_mps: finite(at(9)),
        course_deg: finite(at(10)),
        vertical_rate_mps: finite(at(11)),
        type_code: None,
        registration: None,
        operator: None,
    })
}

fn normalize_readsb(row: &Value, snapshot_time_ms: Option<f64>) -> Option<Aircraft> {
    let id = clean_text(row.get("hex")).to_lowercase();
    let latitude = finite(row.get("lat"));
    let longitude = finite(row.get("lon"));
    if id.is_empty() || !coordinates(latitude, longitude) {
        return None;
    }
    let baro_ft = finite(row.get("alt_baro"));
    let geo_ft = finite(row.get("alt_geom"));
    let speed_kts = finite(row.get("gs"));
    let rate = finite(row.get("baro_rate"));
    let seen = finite(row.get("seen"));
    let seen_pos = finite(row.get("seen_pos"));
    Some(Aircraft {
        id,
        latitude: latitude?,
        longitude: longitude?,
        callsign: clean_text(row.get("flight")),
        origin_country: None,
        position_time_ms: snapshot_time_ms.map(|t| t - seen_pos.unwrap_or(0.0) * 1000.0),
        contact_time_ms: match (snapshot_time_ms, seen) {
            (Some(t), Some(seen)) => Some(t - seen * 1000.0),
            _ => None,
        },
        baro_altitude_m: baro_ft.map(|ft| ft * 0.3048),
        ellipsoid_altitude_m: geo_ft.map(|ft| ft * 0.3048),
        on_ground: clean_text(row.get("alt_baro")).to_lowercase() == "ground",
        speed_mps: speed_kts.map(|kts| kts * 0.514444),
        course_deg: finite(row.get("track")),
        vertical_rate_mps: rate.map(|r| r * 0.00508),
        type_code: Some(clean_text(row.get("t"))),
        registration: Some(clean_text(row.get("r"))),
        operator: Some(clean_text(row.get("ownOp"))),
    })
}

/// `admitRecords` for aircraft.
fn admit<R>(
    rows: Option<&Value>,
    normalize: impl Fn(&Value) -> Option<R>,
    id: impl Fn(&R) -> String,
    label: &str,
) -> Result<Vec<R>, Failure> {
    let Some(rows) = rows.and_then(Value::as_array) else {
        return Err(Failure::source(
            "malformed",
            format!("Malformed {label} response"),
        ));
    };
    let mut records = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for row in rows {
        if let Some(record) = normalize(row)
            && ids.insert(id(&record))
        {
            records.push(record);
        }
    }
    if !rows.is_empty() && records.is_empty() {
        return Err(Failure::source(
            "malformed",
            format!("Malformed {label} aircraft rows"),
        ));
    }
    Ok(records)
}

fn feed_source(read: &Read, fallback: &str) -> String {
    match read.header("x-feed-source") {
        Some(named) if !js::trim(&named).is_empty() => js::slice16(js::trim(&named), 80),
        _ => fallback.to_string(),
    }
}

fn open_sky_error(read: &Read) -> Failure {
    let mut error = http_error(read.status, "Flights");
    let mode = read
        .header("x-opensky-auth-mode-used")
        .filter(|v| !v.is_empty())
        .or_else(|| read.header("x-opensky-auth"))
        .unwrap_or_default()
        .to_lowercase();
    let reason = read
        .header("x-opensky-auth-reason")
        .unwrap_or_default()
        .to_lowercase();
    let set = |error: &mut Failure, text: &str| {
        if let Failure::Source { message, .. } = error {
            *message = text.to_string();
        }
    };
    if read.status == 429 && (mode.is_empty() || mode == "anon") {
        set(&mut error, "OpenSky rate limited (anonymous)");
    }
    if read.status == 401 || read.status == 403 {
        let known = match reason.as_str() {
            "oauth_invalid_or_missing" => Some("OpenSky OAuth client missing/invalid"),
            "oauth_invalid_credentials" => Some("OpenSky OAuth rejected credentials"),
            "basic_invalid_credentials" => Some("OpenSky username/password rejected"),
            "missing_basic_creds" | "missing_oauth_and_basic_creds" => Some("OpenSky auth missing"),
            "auth_required" | "forced_anonymous" => Some("OpenSky auth required"),
            _ => None,
        };
        let text = known.unwrap_or(
            if reason.starts_with("oauth_") || reason.starts_with("basic_") {
                "OpenSky auth invalid"
            } else if mode == "anon" {
                "OpenSky auth required"
            } else {
                "OpenSky auth failed"
            },
        );
        set(&mut error, text);
    }
    error
}

/// `aircraft.getSnapshot(query)` (`/api/flights`).
pub async fn flight_snapshot(
    api: &Api,
    center: Option<(f64, f64)>,
) -> Result<Snapshot<Aircraft>, Failure> {
    let mut url = "/api/flights".to_string();
    if let Some((lat, lon)) = center.filter(|(a, b)| a.is_finite() && b.is_finite()) {
        url.push_str(&format!(
            "?lat={}&lon={}",
            js::to_fixed(lat, 4),
            js::to_fixed(lon, 4)
        ));
    }
    let read = read_response(api, &url, Init::default(), "Flights").await?;
    if !read.ok() {
        return Err(open_sky_error(&read));
    }
    let payload = read.payload.clone().unwrap_or(Value::Null);
    let records = admit(
        payload.get("states"),
        normalize_open_sky,
        |r| r.id.clone(),
        "OpenSky",
    )?;
    let source = read
        .header("x-flight-source")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| feed_source(&read, "Flights"));
    let coverage = read
        .header("x-flight-coverage")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "worldwide upstream snapshot".into());
    let now = js::now();
    let observed = epoch(payload.get("time"), 1000.0);
    let age = observed.map(|t| js::math_max([0.0, now - t]));
    let freshness = match (observed, age) {
        (None, _) => "unknown",
        (_, Some(age)) if age > 120_000.0 => "stale",
        _ => "current",
    };
    Ok(Snapshot {
        records,
        source: Some(source),
        coverage: Some(coverage),
        observed_at_ms: observed,
        freshness: freshness.into(),
    })
}

/// `military.getSnapshot()` (`/api/military`).
pub async fn military_snapshot(api: &Api) -> Result<Snapshot<Aircraft>, Failure> {
    let read = read_response(api, "/api/military", Init::default(), "Military aircraft").await?;
    if !read.ok() {
        return Err(http_error(read.status, "Military aircraft"));
    }
    let age = finite(read.header("x-feed-age-ms").map(Value::String).as_ref());
    let observed = js::now() - age.filter(|a| *a > 0.0).unwrap_or(0.0);
    let stale = read.header("x-feed-cache").as_deref() == Some("STALE");
    let payload = read.payload.clone().unwrap_or(Value::Null);
    let records = admit(
        payload.get("ac"),
        |row| normalize_readsb(row, Some(observed)),
        |r| r.id.clone(),
        "adsb.lol",
    )?;
    Ok(Snapshot {
        records,
        source: Some(feed_source(&read, "Military aircraft")),
        coverage: Some("military upstream snapshot".into()),
        observed_at_ms: Some(observed),
        freshness: if stale { "stale" } else { "current" }.into(),
    })
}

/// A track point.
#[derive(Debug, Clone)]
pub struct TrackPoint {
    pub latitude: f64,
    pub longitude: f64,
    pub observed_at_ms: Option<f64>,
    pub baro_altitude_m: Option<f64>,
    pub on_ground: bool,
}

fn normalize_aircraft_track(
    rows: Option<&Value>,
    base_time_ms: f64,
    readsb: bool,
) -> Vec<TrackPoint> {
    let Some(rows) = rows.and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            let row = row.as_array()?;
            let latitude = finite(row.get(1));
            let longitude = finite(row.get(2));
            let offset = finite(row.first());
            let observed = offset.and_then(|o| epoch_number(Some(base_time_ms + o * 1000.0), 1.0));
            if !coordinates(latitude, longitude) || observed.is_none_or(|t| t <= 0.0) {
                return None;
            }
            let altitude = finite(row.get(3));
            Some(TrackPoint {
                latitude: latitude?,
                longitude: longitude?,
                observed_at_ms: observed,
                baro_altitude_m: altitude.map(|a| a * if readsb { 0.3048 } else { 1.0 }),
                on_ground: if readsb {
                    clean_text(row.get(3)).to_lowercase() == "ground"
                } else {
                    row.get(5) == Some(&Value::Bool(true))
                },
            })
        })
        .collect()
}

/// `getTrack(icao24)` on the flights or military feed.
pub async fn aircraft_track(
    api: &Api,
    military: bool,
    icao24: &str,
) -> Result<Vec<TrackPoint>, Failure> {
    if military {
        let url = format!("/api/military/track?hex={}", encode_uri_component(icao24));
        let read = read_response(api, &url, Init::default(), "Military aircraft").await?;
        if !read.ok() {
            return Err(http_error(read.status, "Military aircraft"));
        }
        let payload = read.payload.unwrap_or(Value::Null);
        Ok(match epoch(payload.get("timestamp"), 1000.0) {
            None => Vec::new(),
            Some(base) => normalize_aircraft_track(payload.get("trace"), base, true),
        })
    } else {
        let url = format!("/api/flights/track?icao24={}", encode_uri_component(icao24));
        let read = read_response(api, &url, Init::default(), "Flights").await?;
        if !read.ok() {
            return Err(http_error(read.status, "Flights"));
        }
        let payload = read.payload.unwrap_or(Value::Null);
        Ok(normalize_aircraft_track(payload.get("path"), 0.0, false))
    }
}

/// `aircraft.getEnrichment({ kind, id })` (`/api/adsbdb/...`): the payload.
pub async fn enrichment(api: &Api, kind: &str, id: &str) -> Result<Value, Failure> {
    let url = format!("/api/adsbdb/{kind}/{}", encode_uri_component(id));
    let read = read_response(api, &url, Init::default(), "adsbdb").await?;
    if !read.ok() {
        return Err(http_error(read.status, "adsbdb"));
    }
    Ok(read.payload.unwrap_or(Value::Null))
}

// ------------------------------------------------------------- vessels ----

/// A vessel observation.
#[derive(Debug, Clone)]
pub struct Vessel {
    pub id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub name: String,
    pub imo: String,
    pub kind: String,
    pub destination: String,
    pub speed_mps: Option<f64>,
    pub course_deg: Option<f64>,
    pub heading_deg: Option<f64>,
    pub observed_at_ms: Option<f64>,
}

/// `String(a || b || fallback)`.
fn string_or(values: &[Option<&Value>], fallback: &str) -> String {
    values
        .iter()
        .flatten()
        .find(|v| js::truthy(v))
        .map_or_else(|| fallback.to_string(), |v| js::to_js_string(v))
}

fn date_parse_value(value: Option<&Value>) -> Option<f64> {
    let text = value.map_or("undefined".to_string(), js::to_js_string);
    let parsed = js::date_parse(&text);
    Some(parsed).filter(|v| v.is_finite())
}

fn normalize_vessel(row: &Value) -> Option<Vessel> {
    let id = js::trim(&string_or(
        &[row.get("mmsi"), row.get("input_identifier")],
        "",
    ))
    .to_string();
    let latitude = finite(row.get("lat"));
    let longitude = finite(row.get("lon"));
    if id.is_empty() || !coordinates(latitude, longitude) {
        return None;
    }
    let speed = finite(row.get("speed"));
    Some(Vessel {
        latitude: latitude?,
        longitude: longitude?,
        name: string_or(&[row.get("name"), row.get("input_name")], &id),
        imo: string_or(&[row.get("imo")], ""),
        kind: string_or(&[row.get("type_specific"), row.get("type")], ""),
        destination: string_or(&[row.get("destination")], ""),
        speed_mps: speed.map(|kts| kts * 0.514444),
        course_deg: finite(row.get("course")),
        heading_deg: finite(row.get("heading")),
        observed_at_ms: epoch(row.get("last_position_epoch"), 1000.0)
            .or_else(|| epoch_number(date_parse_value(row.get("last_position_UTC")), 1.0)),
        id,
    })
}

/// `vessels.getSnapshot({ maxRows, area })`.
pub async fn vessel_snapshot(
    api: &Api,
    area: Option<(f64, f64, f64)>,
) -> Result<Snapshot<Vessel>, Failure> {
    let mut url = url::Url::parse(&api.origin())
        .and_then(|origin| origin.join("/api/vessels"))
        .map_err(|_| Failure::Type("Invalid URL".into()))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("maxRows", "1000000");
        if let Some((lat, lon, radius)) =
            area.filter(|(a, b, c)| a.is_finite() && b.is_finite() && c.is_finite())
        {
            query.append_pair("lat", &js::to_fixed(lat, 5));
            query.append_pair("lon", &js::to_fixed(lon, 5));
            query.append_pair("radius_km", &js::to_fixed(radius, 1));
        }
    }
    let read = read_response(api, url.as_str(), no_store(), "Vessels").await?;
    if !read.ok() {
        let mut error = http_error(read.status, "Vessels");
        let reason = match read
            .payload
            .as_ref()
            .and_then(|p| p.get("status"))
            .and_then(Value::as_str)
        {
            Some("missing-key") => Some("AISSTREAM_API_KEY not set"),
            Some("auth-failed") => Some("API key rejected — check AISSTREAM_API_KEY"),
            Some("unsupported") => Some("live feed unsupported"),
            Some("error") => Some("feed down"),
            Some("closed") => Some("feed disconnected"),
            _ => None,
        };
        if let (Some(reason), Failure::Source { message, .. }) = (reason, &mut error) {
            *message = reason.to_string();
        }
        return Err(error);
    }
    let payload = read.payload.clone().unwrap_or(Value::Null);
    let Some(rows) = payload.get("rows").and_then(Value::as_array) else {
        return Err(Failure::source("malformed", "Malformed vessel response"));
    };
    let mut records = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for row in rows {
        if let Some(record) = normalize_vessel(row)
            && ids.insert(record.id.clone())
        {
            records.push(record);
        }
    }
    let newest = payload.get("newestPositionAt");
    let observed = epoch(newest, 1.0)
        .or_else(|| epoch_number(date_parse_value(newest), 1.0))
        .or_else(|| {
            records.iter().fold(None, |newest: Option<f64>, row| {
                let value =
                    js::math_max([newest.unwrap_or(0.0), row.observed_at_ms.unwrap_or(0.0)]);
                if value == 0.0 || value.is_nan() {
                    None
                } else {
                    Some(value)
                }
            })
        });
    let refreshing = payload.get("refreshing").is_some_and(js::truthy);
    Ok(Snapshot {
        records,
        source: Some(feed_source(&read, "Vessels")),
        coverage: Some("received AIS positions".into()),
        observed_at_ms: observed,
        freshness: if refreshing {
            "stale"
        } else if observed.is_none() {
            "unknown"
        } else {
            "current"
        }
        .into(),
    })
}

/// `vessels.getTrack(mmsi)`: (lat, lon, observedAtMs) samples.
pub async fn vessel_track(api: &Api, mmsi: &str) -> Result<Vec<(f64, f64, Option<f64>)>, Failure> {
    let url = format!("/api/vessels/track?mmsi={}", encode_uri_component(mmsi));
    let read = read_response(api, &url, Init::default(), "Vessels").await?;
    if !read.ok() {
        return Err(http_error(read.status, "Vessels"));
    }
    let payload = read.payload.unwrap_or(Value::Null);
    Ok(payload
        .get("samples")
        .and_then(Value::as_array)
        .map(|samples| {
            samples
                .iter()
                .filter_map(|sample| {
                    let lat = finite(sample.get("lat"));
                    let lon = finite(sample.get("lon"));
                    coordinates(lat, lon).then(|| {
                        (
                            lat.unwrap_or_default(),
                            lon.unwrap_or_default(),
                            epoch(sample.get("t"), 1000.0),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default())
}
