//! Request services the environment tools read: weather and terrain
//! (`src/services/requests.js`), cyclones (`src/layers/cyclones/source.js`)
//! and wildfire perimeters (`src/layers/perimeters/source.js`).

use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;
use serde_json::Value;
use url::Url;

use super::fetch::{Api, Init, Redirect};
use super::live::encode_uri_component;
use super::no_store;
use super::vector_tiles::read_capped;
use crate::mcp::catalog::Failure;
use crate::mcp::js;

/// `request(endpoint)`: status and the JSON body, or null.
pub async fn request(api: &Api, endpoint: &str) -> Result<(bool, u16, Value), Failure> {
    let response = api.fetch(endpoint, Init::redirect(Redirect::Error)).await?;
    let ok = response.ok();
    let status = response.status;
    let data = response.json().await.unwrap_or(Value::Null);
    Ok((ok, status, data))
}

fn require_ok((ok, status, data): (bool, u16, Value), label: &str) -> Result<Value, Failure> {
    if !ok {
        return Err(Failure::other(format!("{label} unavailable ({status})")));
    }
    Ok(data)
}

/// `pointUrl(endpoint, latitude, longitude)`.
pub fn point_url(endpoint: &str, latitude: f64, longitude: f64) -> Result<String, Failure> {
    if !latitude.is_finite()
        || !(-90.0..=90.0).contains(&latitude)
        || !longitude.is_finite()
        || !(-180.0..=180.0).contains(&longitude)
    {
        return Err(Failure::Type("Valid coordinates are required".into()));
    }
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("latitude", &js::to_fixed(latitude, 5))
        .append_pair("longitude", &js::to_fixed(longitude, 5))
        .finish();
    Ok(format!("{endpoint}?{query}"))
}

/// `weather.getConditions(latitude, longitude)`.
pub async fn weather_conditions(
    api: &Api,
    latitude: f64,
    longitude: f64,
) -> Result<Value, Failure> {
    let url = point_url("/api/weather-effects", latitude, longitude)?;
    require_ok(request(api, &url).await?, "Weather")
}

/// `terrain.getHeights(points)`: the `results` value, if any.
pub async fn terrain_heights(api: &Api, points: &[(f64, f64)]) -> Result<Option<Value>, Failure> {
    let query: Vec<String> = points
        .iter()
        .map(|(lat, lon)| format!("{},{}", js::to_fixed(*lon, 5), js::to_fixed(*lat, 5)))
        .collect();
    let url = format!(
        "/api/terrain/heights?points={}",
        encode_uri_component(&query.join(";"))
    );
    let data = require_ok(request(api, &url).await?, "Terrain heights")?;
    Ok(if data.is_null() {
        None
    } else {
        data.get("results").cloned()
    })
}

/// `perimeters.getSnapshot()`: the rows.
pub async fn fire_perimeters(api: &Api) -> Result<Vec<Value>, Failure> {
    let response = api.get("/api/fire-perimeters").await?;
    if !response.ok() {
        return Err(Failure::other(format!("WFIGS HTTP {}", response.status)));
    }
    let bytes = read_capped(response, 80 * 1024 * 1024).await?;
    let payload =
        js::parse_json(&js::decode_text(&bytes)).ok_or_else(super::fetch::json_syntax_error)?;
    match payload.get("rows") {
        Some(Value::Array(rows)) => Ok(rows.clone()),
        _ => Err(Failure::other("Malformed perimeter snapshot")),
    }
}

// ---- Cyclones ----

/// A validated storm, with the fields the tool reads.
#[derive(Debug, Clone, PartialEq)]
pub struct Storm {
    pub id: String,
    pub name: String,
    pub classification: String,
    pub basin: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub position_at: String,
    pub advisory_number: String,
    pub issued_at: String,
    pub wind_kt: Option<f64>,
    pub pressure_hpa: Option<f64>,
    pub direction_degrees: Option<f64>,
    pub speed_kt: Option<f64>,
    pub advisory_url: Option<String>,
}

/// A validated cyclone snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct CycloneSnapshot {
    pub source: String,
    pub attribution: String,
    pub coverage: String,
    pub stale: bool,
    pub unavailable: bool,
    pub storms: Vec<Storm>,
}

#[derive(Debug)]
struct Malformed;

type V<T> = Result<T, Malformed>;

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("static regex"))
}

fn text(value: Option<&Value>, max: usize) -> V<String> {
    static CONTROL: OnceLock<Regex> = OnceLock::new();
    let s = value.and_then(Value::as_str).ok_or(Malformed)?;
    if js::trim(s).is_empty() || js::len16(s) > max || re(&CONTROL, r"[\x00-\x1f<>]").is_match(s) {
        return Err(Malformed);
    }
    Ok(s.to_string())
}

fn number(value: Option<&Value>, low: f64, high: f64) -> V<Option<f64>> {
    match value {
        Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => {
            let v = n.as_f64().ok_or(Malformed)?;
            if v.is_finite() && v >= low && v <= high {
                Ok(Some(v))
            } else {
                Err(Malformed)
            }
        }
        _ => Err(Malformed),
    }
}

fn time(value: Option<&Value>) -> V<String> {
    static ISO: OnceLock<Regex> = OnceLock::new();
    let s = value.and_then(Value::as_str).ok_or(Malformed)?;
    let parsed = js::date_parse(s);
    if !re(
        &ISO,
        r"^[0-9]{4}-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]{3}Z$",
    )
    .is_match(s)
        || !parsed.is_finite()
        || js::iso_string(parsed).as_deref() != Some(s)
    {
        return Err(Malformed);
    }
    Ok(s.to_string())
}

fn advisory(value: Option<&Value>) -> V<String> {
    static ADVISORY: OnceLock<Regex> = OnceLock::new();
    let s = value.and_then(Value::as_str).ok_or(Malformed)?;
    if !re(&ADVISORY, r"^(?:0|[1-9][0-9]{0,2})[A-Z]?$").is_match(s) {
        return Err(Malformed);
    }
    Ok(s.to_string())
}

fn position(value: Option<&Value>) -> V<(Option<f64>, Option<f64>)> {
    let value = value.filter(|v| js::truthy(v)).ok_or(Malformed)?;
    if value.get("longitude").is_some_and(Value::is_null)
        || value.get("latitude").is_some_and(Value::is_null)
    {
        return Err(Malformed);
    }
    Ok((
        number(value.get("longitude"), -180.0, 180.0)?,
        number(value.get("latitude"), -90.0, 90.0)?,
    ))
}

fn official_link(value: Option<&Value>, outlook: bool) -> V<Option<String>> {
    static OUTLOOK: OnceLock<Regex> = OnceLock::new();
    static TEXT: OnceLock<Regex> = OnceLock::new();
    if value.is_some_and(Value::is_null) {
        return Ok(None);
    }
    let raw = text(value, 256)?;
    let url = Url::parse(&raw).map_err(|_| Malformed)?;
    if url.origin().ascii_serialization() != "https://www.nhc.noaa.gov"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some_and(|f| !f.is_empty())
    {
        return Err(Malformed);
    }
    let search = url
        .query()
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let valid = if outlook {
        re(&OUTLOOK, r"^/gtwo\.php\?basin=(?:atlc|epac|cpac)&fdays=7$")
            .is_match(&format!("{}{search}", url.path()))
    } else {
        search.is_empty() && re(&TEXT, r"^/text/[A-Z0-9]+\.shtml$").is_match(url.path())
    };
    if !valid {
        return Err(Malformed);
    }
    Ok(Some(url.to_string()))
}

fn geometry(value: Option<&Value>, track: bool, budget: &mut usize) -> V<bool> {
    if value.is_some_and(Value::is_null) {
        return Ok(false);
    }
    let value = value.ok_or(Malformed)?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let allowed: [&str; 2] = if track {
        ["LineString", "MultiLineString"]
    } else {
        ["Polygon", "MultiPolygon"]
    };
    if !allowed.contains(&kind) {
        return Err(Malformed);
    }
    fn list(items: Option<&Value>, min: usize, max: usize) -> V<&Vec<Value>> {
        let items = items.and_then(Value::as_array).ok_or(Malformed)?;
        if items.len() < min || items.len() > max {
            return Err(Malformed);
        }
        Ok(items)
    }
    fn point(pair: &Value, budget: &mut usize) -> V<(f64, f64)> {
        let pair = pair.as_array().ok_or(Malformed)?;
        if pair.len() != 2 || !pair.iter().all(Value::is_number) {
            return Err(Malformed);
        }
        let (lon, lat) = (
            pair[0].as_f64().ok_or(Malformed)?,
            pair[1].as_f64().ok_or(Malformed)?,
        );
        if !(lon.is_finite()
            && (-180.0..=180.0).contains(&lon)
            && lat.is_finite()
            && (-90.0..=90.0).contains(&lat))
        {
            return Err(Malformed);
        }
        *budget += 1;
        if *budget > 25_000 {
            return Err(Malformed);
        }
        Ok((lon, lat))
    }
    fn line(pairs: &Value, budget: &mut usize) -> V<()> {
        for pair in list(Some(pairs), 2, 10_000)? {
            point(pair, budget)?;
        }
        Ok(())
    }
    fn ring(pairs: &Value, budget: &mut usize) -> V<()> {
        let items = list(Some(pairs), 4, 10_000)?;
        let mut points = Vec::with_capacity(items.len());
        for pair in items {
            points.push(point(pair, budget)?);
        }
        if points.first() != points.last() {
            return Err(Malformed);
        }
        Ok(())
    }
    fn polygon(rings: &Value, budget: &mut usize) -> V<()> {
        for r in list(Some(rings), 1, 128)? {
            ring(r, budget)?;
        }
        Ok(())
    }
    let coordinates = value.get("coordinates");
    match kind {
        "LineString" => line(coordinates.ok_or(Malformed)?, budget)?,
        "MultiLineString" => {
            for l in list(coordinates, 1, 128)? {
                line(l, budget)?;
            }
        }
        "Polygon" => polygon(coordinates.ok_or(Malformed)?, budget)?,
        _ => {
            for p in list(coordinates, 1, 128)? {
                polygon(p, budget)?;
            }
        }
    }
    Ok(true)
}

fn storm(raw: &Value, seen: &mut Vec<String>, budget: &mut usize, points: &mut usize) -> V<Storm> {
    static ID: OnceLock<Regex> = OnceLock::new();
    let id = raw.get("id").and_then(Value::as_str).ok_or(Malformed)?;
    let status = raw
        .get("geometryStatus")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !re(&ID, r"^(?:al|ep|cp)[0-9]{6}$").is_match(id)
        || seen.iter().any(|s| s == id)
        || !["current", "pending", "unavailable"].contains(&status)
    {
        return Err(Malformed);
    }
    seen.push(id.to_string());
    let advisory_number = advisory(raw.get("advisoryNumber"))?;
    let geometry_advisory = match raw.get("geometryAdvisoryNumber") {
        Some(Value::Null) => None,
        other => Some(advisory(other)?),
    };
    if status == "current" && geometry_advisory.as_deref() != Some(advisory_number.as_str()) {
        return Err(Malformed);
    }
    let forecast = raw
        .get("forecastPoints")
        .and_then(Value::as_array)
        .ok_or(Malformed)?;
    *points += forecast.len();
    if *points > 500 {
        return Err(Malformed);
    }
    let mut taus: Vec<f64> = Vec::new();
    for point in forecast {
        let tau = number(point.get("tauHours"), 0.0, 168.0)?.ok_or(Malformed)?;
        if taus.contains(&tau) {
            return Err(Malformed);
        }
        taus.push(tau);
        *budget += 1;
        if *budget > 25_000 {
            return Err(Malformed);
        }
        position(point.get("position"))?;
        number(point.get("windKt"), 0.0, 300.0)?;
        number(point.get("gustKt"), 0.0, 350.0)?;
    }
    let track = geometry(raw.get("track"), true, budget)?;
    let cone = geometry(raw.get("cone"), false, budget)?;
    if status != "current" && (track || cone || !forecast.is_empty()) {
        return Err(Malformed);
    }
    if status == "current" && (!track || !cone || forecast.is_empty()) {
        return Err(Malformed);
    }
    let basin = raw.get("basin").and_then(Value::as_str).unwrap_or_default();
    if basin != id[..2].to_uppercase() {
        return Err(Malformed);
    }
    let name = text(raw.get("name"), 80)?;
    let classification = text(raw.get("classification"), 16)?;
    let (longitude, latitude) = position(raw.get("position"))?;
    let position_at = time(raw.get("positionAt"))?;
    let issued_at = time(raw.get("issuedAt"))?;
    let wind_kt = number(raw.get("windKt"), 0.0, 300.0)?;
    let pressure_hpa = number(raw.get("pressureHpa"), 800.0, 1100.0)?;
    let movement = raw.get("movement").filter(|m| !m.is_null());
    let direction_degrees = number(movement.and_then(|m| m.get("directionDegrees")), 0.0, 360.0)?;
    let speed_kt = number(movement.and_then(|m| m.get("speedKt")), 0.0, 200.0)?;
    let advisory_url = official_link(raw.get("advisoryUrl"), false)?;
    official_link(raw.get("outlookUrl"), true)?;
    Ok(Storm {
        id: id.to_string(),
        name,
        classification,
        basin: basin.to_string(),
        latitude,
        longitude,
        position_at,
        advisory_number,
        issued_at,
        wind_kt,
        pressure_hpa,
        direction_degrees,
        speed_kt,
        advisory_url,
    })
}

/// `validateCycloneSnapshot(value)`.
pub fn validate_cyclones(value: &Value) -> Result<CycloneSnapshot, Failure> {
    let inner = || -> V<CycloneSnapshot> {
        let stale = value
            .get("stale")
            .and_then(Value::as_bool)
            .ok_or(Malformed)?;
        let unavailable = value
            .get("unavailable")
            .and_then(Value::as_bool)
            .ok_or(Malformed)?;
        let raw_storms = value
            .get("storms")
            .and_then(Value::as_array)
            .ok_or(Malformed)?;
        if value.get("schemaVersion").and_then(Value::as_f64) != Some(1.0) || raw_storms.len() > 32
        {
            return Err(Malformed);
        }
        let (mut seen, mut budget, mut points) = (Vec::new(), 0usize, 0usize);
        let storms = raw_storms
            .iter()
            .map(|raw| storm(raw, &mut seen, &mut budget, &mut points))
            .collect::<V<Vec<Storm>>>()?;
        if unavailable && !storms.is_empty() {
            return Err(Malformed);
        }
        let source = text(value.get("source"), 160)?;
        let attribution = text(value.get("attribution"), 240)?;
        let coverage = text(value.get("coverage"), 240)?;
        number(value.get("fetchedAt"), 0.0, 9_007_199_254_740_991.0)?;
        if !value.get("reason").is_some_and(Value::is_null) {
            text(value.get("reason"), 240)?;
        }
        Ok(CycloneSnapshot {
            source,
            attribution,
            coverage,
            stale,
            unavailable,
            storms,
        })
    };
    inner().map_err(|_| Failure::other("Malformed cyclone snapshot"))
}

/// `cyclones.getSnapshot()`.
pub async fn cyclone_snapshot(api: &Api) -> Result<CycloneSnapshot, Failure> {
    let work = async {
        let mut init = no_store();
        init.redirect = Redirect::Error;
        let response = api.fetch("/api/cyclones", init).await?;
        if !response.ok() {
            return Err(Failure::other(format!("Cyclone HTTP {}", response.status)));
        }
        let bytes = read_capped(response, 4 * 1024 * 1024).await?;
        let value =
            js::parse_json(&js::decode_text(&bytes)).ok_or_else(super::fetch::json_syntax_error)?;
        validate_cyclones(&value)
    };
    tokio::time::timeout(Duration::from_secs(15), work)
        .await
        .map_err(|_| Failure::other("Cyclone request timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot(storm: Value) -> Value {
        json!({
            "schemaVersion": 1, "stale": false, "unavailable": false,
            "source": "NHC", "attribution": "NOAA", "coverage": "Atlantic",
            "fetchedAt": 1, "reason": null, "storms": [storm],
        })
    }

    #[test]
    fn validates_pending_storms() {
        let storm = json!({
            "id": "al052026", "geometryStatus": "pending", "advisoryNumber": "12A",
            "geometryAdvisoryNumber": null, "forecastPoints": [], "track": null, "cone": null,
            "basin": "AL", "name": "Ana", "classification": "HU",
            "position": {"longitude": -60.5, "latitude": 20.0},
            "positionAt": "2026-09-01T12:00:00.000Z", "issuedAt": "2026-09-01T12:00:00.000Z",
            "windKt": 90, "pressureHpa": 970, "movement": {"directionDegrees": 280, "speedKt": 12},
            "advisoryUrl": "https://www.nhc.noaa.gov/text/MIATCPAT5.shtml", "outlookUrl": null,
        });
        let parsed = validate_cyclones(&snapshot(storm.clone())).unwrap();
        assert_eq!(parsed.storms[0].wind_kt, Some(90.0));
        let mut bad = storm;
        bad["basin"] = json!("EP");
        assert!(validate_cyclones(&snapshot(bad)).is_err());
    }

    #[test]
    fn point_urls_use_fixed_coordinates() {
        assert_eq!(
            point_url("/api/weather-effects", 30.2672, -97.7431).unwrap(),
            "/api/weather-effects?latitude=30.26720&longitude=-97.74310"
        );
    }
}
