//! Server-side record normalizers ported from `src/`:
//! `normalizeDump1090Aircraft` (`src/sources/adsbRecords.js`) and the
//! adsb.lol point-response adapter (`src/data/adsbLolFallback.js`).

use serde_json::{Value, json};

use super::jsval::{self, num, opt_num, to_number, truthy};

// ---------------------------------------------------------------------------
// src/sources/adsbRecords.js
// ---------------------------------------------------------------------------

/// `finiteOrNull`: strings are never numbers here.
fn finite_or_null(value: Option<&Value>) -> Option<f64> {
    match value {
        None | Some(Value::Null) | Some(Value::String(_)) => None,
        other => Some(to_number(other)).filter(|n| n.is_finite()),
    }
}

fn normalize_icao(value: Option<&Value>) -> Option<String> {
    let icao = jsval::string_or_empty(value).trim().to_lowercase();
    (icao.len() == 6
        && icao
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    .then_some(icao)
}

fn normalize_callsign(value: Option<&Value>) -> Option<String> {
    let mut replaced = String::new();
    for c in jsval::string_or_empty(value).chars() {
        if c.is_ascii_alphanumeric() {
            replaced.push(c);
        } else {
            // The JS regex replaces each UTF-16 code unit.
            for _ in 0..c.len_utf16() {
                replaced.push(' ');
            }
        }
    }
    let callsign = replaced.trim().to_uppercase();
    (!callsign.is_empty()).then_some(callsign)
}

fn normalize_track(value: Option<&Value>) -> Option<f64> {
    finite_or_null(value).map(|track| ((track % 360.0) + 360.0) % 360.0)
}

/// `normalizeAdsbCategory`: the dump1090 string (`'A7'`) or `None`.
pub fn normalize_adsb_category(value: Option<&Value>) -> Option<String> {
    let category = jsval::string_or_empty(value).trim().to_uppercase();
    let bytes = category.as_bytes();
    (bytes.len() == 2 && (b'A'..=b'D').contains(&bytes[0]) && (b'0'..=b'7').contains(&bytes[1]))
        .then_some(category)
}

fn normalize_position(lat: Option<&Value>, lon: Option<&Value>) -> (Option<f64>, Option<f64>) {
    match (finite_or_null(lat), finite_or_null(lon)) {
        (Some(lat), Some(lon)) if lat.abs() <= 90.0 && lon.abs() <= 180.0 => (Some(lat), Some(lon)),
        _ => (None, None),
    }
}

/// `normalizeDump1090Aircraft(json, nowMs, { band })`.
pub fn normalize_dump1090_aircraft(json: &Value, now_ms: f64, band: &str) -> Vec<Value> {
    let record_band = if band == "978" { "978" } else { "1090" };
    if !now_ms.is_finite() {
        return Vec::new();
    }
    let received_at = now_ms;
    let Some(aircraft) = json.get("aircraft").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut records = Vec::new();
    for entry in aircraft {
        let field = |name: &str| entry.get(name);
        let Some(icao) = normalize_icao(field("hex")) else {
            continue;
        };
        let seen_s = finite_or_null(field("seen")).unwrap_or(0.0).max(0.0);
        let seen_pos_s = finite_or_null(field("seen_pos"));
        let (lat, lon) = normalize_position(field("lat"), field("lon"));
        let has_position = lat.is_some() && seen_pos_s.is_some();
        let on_ground = field("alt_baro").and_then(Value::as_str) == Some("ground");
        let altitude = if on_ground {
            Some(0.0)
        } else {
            finite_or_null(field("alt_baro"))
        };
        let message_count = finite_or_null(field("messages"))
            .unwrap_or(0.0)
            .trunc()
            .max(0.0);
        records.push(json!({
            "icao": icao,
            "callsign": normalize_callsign(field("flight")),
            "category": normalize_adsb_category(field("category")),
            "onGround": on_ground,
            "lat": if has_position { opt_num(lat) } else { Value::Null },
            "lon": if has_position { opt_num(lon) } else { Value::Null },
            "altitudeFt": opt_num(altitude),
            "groundSpeedKt": opt_num(finite_or_null(field("gs"))),
            "trackDeg": opt_num(normalize_track(field("track"))),
            "verticalRateFpm": opt_num(finite_or_null(field("baro_rate"))),
            "lastPositionAt": match (has_position, seen_pos_s) {
                (true, Some(seen_pos)) => num(received_at - seen_pos.max(0.0) * 1000.0),
                _ => Value::Null,
            },
            "lastMessageAt": num(received_at - seen_s * 1000.0),
            "messageCount": num(message_count),
            "rssiDbfs": opt_num(finite_or_null(field("rssi"))),
            "band": record_band,
            "source": "feed",
        }));
    }
    records
}

// ---------------------------------------------------------------------------
// src/data/adsbLolFallback.js
// ---------------------------------------------------------------------------

const KNOT_TO_MPS: f64 = 0.514444;
const FOOT_TO_M: f64 = 0.3048;
const FPM_TO_MPS: f64 = 0.00508;

/// `finiteNumber`: `null`, `undefined` and `''` are absent; strings coerce.
fn finite_number(value: Option<&Value>) -> Option<f64> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        other => Some(to_number(other)).filter(|n| n.is_finite()),
    }
}

fn emitter_category(value: Option<&Value>) -> i64 {
    match jsval::string_or_empty_falsy(value)
        .trim()
        .to_uppercase()
        .as_str()
    {
        "A1" => 2,
        "A2" => 3,
        "A3" => 4,
        "A4" => 5,
        "A5" => 6,
        "A6" => 7,
        "A7" => 8,
        "B1" => 9,
        "B2" => 10,
        "B3" => 11,
        "B4" => 12,
        "B6" => 14,
        "B7" => 15,
        _ => 0,
    }
}

/// `normalizeAdsbLolAircraftState`: an OpenSky-shaped state vector.
pub fn normalize_adsb_lol_aircraft_state(aircraft: &Value, now_seconds: f64) -> Option<Value> {
    let field = |name: &str| aircraft.get(name);
    let hex = jsval::string_or_empty_falsy(field("hex"))
        .trim()
        .to_lowercase();
    let latitude = finite_number(field("lat"));
    let longitude = finite_number(field("lon"));
    let (Some(latitude), Some(longitude)) = (latitude, longitude) else {
        return None;
    };
    if hex.is_empty() {
        return None;
    }
    let seen_position = finite_number(field("seen_pos"))
        .or_else(|| finite_number(field("seen")))
        .unwrap_or(0.0)
        .max(0.0);
    let seen = finite_number(field("seen"))
        .unwrap_or(seen_position)
        .max(0.0);
    let on_ground = field("alt_baro").and_then(Value::as_str) == Some("ground");
    let barometric_feet = if on_ground {
        None
    } else {
        finite_number(field("alt_baro"))
    };
    let geometric_feet = finite_number(field("alt_geom"));
    let ground_speed_knots = finite_number(field("gs"));
    let vertical_rate_fpm =
        finite_number(field("baro_rate")).or_else(|| finite_number(field("geom_rate")));
    let track = finite_number(field("track"));
    let callsign = if truthy(field("flight")) {
        jsval::string_or_empty_falsy(field("flight"))
    } else {
        jsval::string_or_empty_falsy(field("r"))
    };
    let callsign = callsign.trim();
    let squawk = field("squawk")
        .filter(|v| truthy(Some(v)))
        .cloned()
        .unwrap_or(Value::Null);
    let spi = field("spi").and_then(Value::as_f64) == Some(1.0);
    Some(json!([
        hex,
        if callsign.is_empty() {
            Value::Null
        } else {
            Value::from(callsign)
        },
        Value::Null,
        num((now_seconds - seen_position).max(0.0)),
        num((now_seconds - seen).max(0.0)),
        num(longitude),
        num(latitude),
        opt_num(barometric_feet.map(|f| f * FOOT_TO_M)),
        on_ground,
        opt_num(ground_speed_knots.map(|k| k * KNOT_TO_MPS)),
        opt_num(track),
        opt_num(vertical_rate_fpm.map(|v| v * FPM_TO_MPS)),
        Value::Null,
        opt_num(geometric_feet.map(|f| f * FOOT_TO_M)),
        squawk,
        spi,
        0,
        emitter_category(field("category")),
    ]))
}

/// `normalizeAdsbLolPointResponse`: `{ time, states }` with `now_ms` as the
/// `Date.now()` fallback for a response without its own time.
pub fn normalize_adsb_lol_point_response(payload: &Value, now_ms: i64) -> Value {
    let response_now = finite_number(payload.get("now"));
    let now_seconds = match response_now {
        None => (now_ms as f64 / 1000.0).floor(),
        Some(now) if now > 10_000_000_000.0 => (now / 1000.0).floor(),
        Some(now) => now.floor(),
    };
    let states: Vec<Value> = payload
        .get("ac")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|aircraft| normalize_adsb_lol_aircraft_state(aircraft, now_seconds))
                .collect()
        })
        .unwrap_or_default();
    json!({ "time": num(now_seconds), "states": states })
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const FIXTURE: &str =
        include_str!("../../../../src/data/fixtures/adsb-austin-dump1090-aircraft.json");

    fn fixture() -> Value {
        serde_json::from_str(FIXTURE).unwrap()
    }

    fn by_icao<'a>(records: &'a [Value], icao: &str) -> &'a Value {
        records.iter().find(|r| r["icao"] == icao).unwrap()
    }

    #[test]
    fn dump1090_aircraft_json_maps_to_local_adsb_records() {
        let now = 1_790_127_100_000.0;
        let records = normalize_dump1090_aircraft(&fixture(), now, "1090");
        assert_eq!(records.len(), 4);
        assert_eq!(
            by_icao(&records, "ae5d8a"),
            &json!({
                "icao": "ae5d8a",
                "callsign": "SHINR42",
                "category": null,
                "onGround": false,
                "lat": 30.269662,
                "lon": -97.793262,
                "altitudeFt": 1600,
                "groundSpeedKt": 129.9,
                "trackDeg": 330.5,
                "verticalRateFpm": 64,
                "lastPositionAt": num(now - 34_600.0),
                "lastMessageAt": num(now - 30_500.0),
                "messageCount": 74,
                "rssiDbfs": -30,
                "band": "1090",
                "source": "feed",
            })
        );
        let cruise = by_icao(&records, "a3c775");
        assert_eq!(cruise["callsign"], Value::Null);
        assert_eq!(cruise["altitudeFt"], json!(34_000));
        assert_eq!(cruise["lat"], json!(30.360625));
        assert_eq!(cruise["lon"], json!(-97.919659));
        assert_eq!(cruise["lastPositionAt"], num(now - 43_400.0));
        let unpositioned = by_icao(&records, "a15c54");
        assert_eq!(unpositioned["callsign"], "SKW3301");
        assert_eq!(unpositioned["lat"], Value::Null);
        assert_eq!(unpositioned["lon"], Value::Null);
        assert_eq!(unpositioned["lastPositionAt"], Value::Null);
        assert_eq!(by_icao(&records, "a46e6a")["altitudeFt"], Value::Null);
    }

    #[test]
    fn dump1090_adapter_skips_invalid_addresses_and_tolerates_missing_fields() {
        let records = normalize_dump1090_aircraft(
            &json!({
                "now": 1,
                "aircraft": [
                    { "hex": "~12ab34", "seen": 1 },
                    { "hex": "zzzzzz" },
                    { "hex": "ABC123", "alt_baro": "ground", "lat": 95, "lon": 10, "seen_pos": 1, "category": "a7" },
                    { "hex": "def456", "category": "E9", "seen": 1 },
                ],
            }),
            10_000.0,
            "1090",
        );
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["icao"], "abc123");
        assert_eq!(records[0]["altitudeFt"], json!(0));
        assert_eq!(records[0]["onGround"], true);
        assert_eq!(records[0]["category"], "A7");
        assert_eq!(records[1]["category"], Value::Null);
        assert_eq!(records[1]["onGround"], false);
        assert_eq!(records[0]["lat"], Value::Null);
        assert_eq!(records[0]["lastMessageAt"], json!(10_000));
        assert!(normalize_dump1090_aircraft(&Value::Null, 1.0, "1090").is_empty());
        assert!(normalize_dump1090_aircraft(&json!({"aircraft": []}), f64::NAN, "1090").is_empty());
    }

    #[test]
    fn a_feed_document_names_its_band_on_every_record() {
        let uat = normalize_dump1090_aircraft(&fixture(), 1_790_127_100_000.0, "978");
        assert_eq!(uat.len(), 4);
        assert!(
            uat.iter()
                .all(|r| r["band"] == "978" && r["source"] == "feed")
        );
    }

    #[test]
    fn emitter_categories_normalize() {
        assert_eq!(
            normalize_adsb_category(Some(&json!(" a3 "))).as_deref(),
            Some("A3")
        );
        assert_eq!(normalize_adsb_category(Some(&json!("E1"))), None);
        assert_eq!(normalize_adsb_category(Some(&json!("A8"))), None);
        assert_eq!(normalize_adsb_category(None), None);
    }

    fn close(value: &Value, expected: f64) -> bool {
        (value.as_f64().unwrap() - expected).abs() < 0.001
    }

    #[test]
    fn normalizes_adsb_lol_units_into_an_opensky_state_vector() {
        let state = normalize_adsb_lol_aircraft_state(
            &json!({
                "hex": "A1B2C3", "flight": "UAL123 ", "lat": 30, "lon": -97,
                "alt_baro": 10000, "alt_geom": 10200, "gs": 200, "track": 90,
                "baro_rate": 600, "seen_pos": 2, "seen": 1, "category": "A3",
            }),
            1000.0,
        )
        .unwrap();
        assert_eq!(state[0], "a1b2c3");
        assert_eq!(state[1], "UAL123");
        assert_eq!(state[2], Value::Null);
        assert_eq!(state[3], json!(998));
        assert_eq!(state[5], json!(-97));
        assert_eq!(state[6], json!(30));
        assert_eq!(state[7], json!(3048));
        assert!(close(&state[9], 102.8888));
        assert_eq!(state[10], json!(90));
        assert!(close(&state[11], 3.048));
        assert_eq!(state[13], json!(3108.96));
        assert_eq!(state[17], json!(4));
    }

    #[test]
    fn keeps_grounded_fallback_contacts_and_rejects_rows_without_positions() {
        let normalized = normalize_adsb_lol_point_response(
            &json!({
                "now": 1_700_000_000_000i64,
                "ac": [
                    { "hex": "abc123", "lat": 0, "lon": 0, "alt_baro": "ground", "gs": 8 },
                    { "hex": "def456", "lat": null, "lon": 10, "alt_baro": 5000 },
                ],
            }),
            0,
        );
        assert_eq!(normalized["time"], json!(1_700_000_000));
        let states = normalized["states"].as_array().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0][7], Value::Null);
        assert_eq!(states[0][8], true);
    }
}
