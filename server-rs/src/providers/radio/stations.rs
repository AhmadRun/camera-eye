//! Port of `src/sources/radioBrowser.js` (re-exported by
//! `server/providers/radio/stations.js`): station admission and the public
//! field allowlist.

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Value, json};

use super::country::{normalize_country, normalize_radio_country_input};
use crate::providers::cctv::jsval;

/// `RADIO_UUID_RE` (case-insensitive).
pub static RADIO_UUID_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        .expect("regex")
});

/// `String(value ?? '')`.
fn text_of(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(v) => jsval::string(Some(v)),
    }
}

/// `cleanRadioText(value, maxLength)`: control characters to spaces,
/// whitespace collapsed, trimmed and bounded (UTF-16 units).
pub fn clean_radio_text(value: Option<&Value>, max_length: usize) -> String {
    clean_text(&text_of(value), max_length)
}

pub fn clean_text(text: &str, max_length: usize) -> String {
    let spaced: String = text
        .chars()
        .map(|c| {
            if (c as u32) <= 0x1f || c == '\u{7f}' {
                ' '
            } else {
                c
            }
        })
        .collect();
    let collapsed = jsval::collapse_ws(&spaced);
    let sliced = jsval::utf16_slice(jsval::trim(&collapsed), 0, max_length);
    jsval::trim(&sliced).to_string()
}

/// `isNonGlobalIpv4`: dotted-quad hosts outside the global unicast space.
pub fn is_non_global_ipv4(hostname: &str) -> bool {
    let pieces: Vec<&str> = hostname.split('.').collect();
    if pieces.len() != 4
        || pieces
            .iter()
            .any(|p| p.is_empty() || p.len() > 3 || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    let values: Vec<u32> = pieces.iter().map(|p| p.parse().unwrap_or(0)).collect();
    if values.iter().any(|v| *v > 255) {
        return true;
    }
    let (a, b, c) = (values[0], values[1], values[2]);
    a == 0
        || a == 10
        || a == 127
        || a >= 224
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
}

/// `publicRadioHttpsUrl`: a normalized public HTTPS URL, or `None` for
/// local, private or IPv6-literal targets.
pub fn public_radio_https_url(value: Option<&Value>) -> Option<String> {
    let mut url = url::Url::parse(&text_of(value)).ok()?;
    let host = url.host_str().unwrap_or("").to_lowercase();
    let host = host.strip_prefix('[').unwrap_or(&host);
    let host = host.strip_suffix(']').unwrap_or(host);
    let host = host.strip_suffix('.').unwrap_or(host);
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || host.is_empty()
    {
        return None;
    }
    if host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || is_non_global_ipv4(host)
        || host.contains(':')
    {
        return None;
    }
    url.set_fragment(None);
    Some(url.to_string())
}

/// A station as the directory keeps it (the public fields plus clicks).
#[derive(Debug, Clone, PartialEq)]
pub struct RadioStation {
    pub id: String,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub stream_url: String,
    pub homepage: Option<String>,
    pub tags: Vec<String>,
    pub languages: Vec<String>,
    pub state: String,
    pub country: String,
    pub country_code: String,
    pub metadata_trust: &'static str,
    pub codec: String,
    pub bitrate: Option<f64>,
    pub click_count: f64,
}

/// The tag normalization shared by stations and requested categories.
pub fn normalize_tag(text: &str) -> String {
    static SEPARATORS: Lazy<Regex> = Lazy::new(|| Regex::new("[_-]+").expect("regex"));
    let lowered = clean_text(text, 80).to_lowercase();
    let spaced = SEPARATORS.replace_all(&lowered, " ");
    jsval::trim(&jsval::collapse_ws(&spaced)).to_string()
}

/// `geo_lat === null || geo_lat === '' ? null : Number(geo_lat)`, as a
/// finite number or `None`.
fn coordinate(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Null) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        other => Some(jsval::number(other)).filter(|f| f.is_finite()),
    }
}

/// `normalizeRadioBrowserStation`: healthy, geolocated, public HTTPS
/// MP3/AAC streams only; favicons are never carried.
pub fn normalize_radio_browser_station(raw: &Value) -> Option<RadioStation> {
    static CODEC: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)^(?:MP3|AAC(?:\+|-LC|-HE)?|HE-AAC)$").expect("regex"));
    let r = if raw.is_object() || raw.is_array() {
        Some(raw)
    } else {
        None
    };
    let field = |k: &str| jsval::get(r, k);
    let id = clean_radio_text(field("stationuuid"), 40).to_lowercase();
    let lat = coordinate(field("geo_lat"));
    let lon = coordinate(field("geo_long"));
    let codec = clean_radio_text(field("codec"), 16).to_uppercase();
    let stream_url = public_radio_https_url(jsval::or(&[field("url_resolved"), field("url")]));
    let (Some(lat), Some(lon), Some(stream_url)) = (lat, lon, stream_url) else {
        return None;
    };
    if !RADIO_UUID_RE.is_match(&id)
        || jsval::number(field("lastcheckok")) != 1.0
        || jsval::number(field("hls")) == 1.0
        || !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lon)
        || !CODEC.is_match(&codec)
    {
        return None;
    }
    let name = clean_radio_text(field("name"), 140);
    if name.is_empty() {
        return None;
    }
    let mut tags: Vec<String> = Vec::new();
    for tag in text_of(field("tags")).split(',').map(normalize_tag) {
        if !tag.is_empty() && !tags.contains(&tag) {
            tags.push(tag);
        }
    }
    tags.truncate(24);
    let languages: Vec<String> = text_of(field("language"))
        .split(',')
        .map(|l| clean_text(l, 40))
        .filter(|l| !l.is_empty())
        .take(8)
        .collect();
    let raw_code = clean_radio_text(field("countrycode"), 2).to_uppercase();
    let by_code = normalize_country(&raw_code);
    let country = if by_code.valid && !by_code.empty {
        by_code
    } else {
        normalize_radio_country_input(Some(&Value::String(clean_radio_text(field("country"), 80))))
    };
    let bitrate = jsval::number(field("bitrate"));
    let clicks = jsval::number(field("clickcount"));
    let clicks = if clicks.is_nan() { 0.0 } else { clicks };
    Some(RadioStation {
        id,
        name,
        lat,
        lon,
        stream_url,
        homepage: public_radio_https_url(field("homepage")),
        tags,
        languages,
        state: clean_radio_text(field("state"), 80),
        country: if country.valid && !country.empty {
            country.name.clone()
        } else {
            clean_radio_text(field("country"), 80)
        },
        country_code: if country.valid {
            country.code.clone()
        } else {
            String::new()
        },
        metadata_trust: "untrusted-community",
        codec,
        bitrate: (bitrate.fract() == 0.0 && (8.0..=1024.0).contains(&bitrate)).then_some(bitrate),
        click_count: clicks.clamp(0.0, 10_000_000.0),
    })
}

/// `publicRadioStation`: the field allowlist that crosses the API boundary.
pub fn public_radio_station(station: &RadioStation) -> Value {
    json!({
        "id": station.id,
        "name": station.name,
        "lat": jsval::num(station.lat),
        "lon": jsval::num(station.lon),
        "streamUrl": station.stream_url,
        "homepage": station.homepage,
        "tags": station.tags,
        "languages": station.languages,
        "state": station.state,
        "country": station.country,
        "countryCode": station.country_code,
        "metadataTrust": station.metadata_trust,
        "codec": station.codec,
        "bitrate": station.bitrate.map(jsval::num),
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub const UUID: &str = "12345678-1234-4234-8234-123456789abc";

    pub fn station(overrides: Value) -> Value {
        let mut row = json!({
            "stationuuid": UUID,
            "name": "Test Radio",
            "url_resolved": "https://stream.example.org/live.mp3",
            "homepage": "https://station.example.org/",
            "favicon": "https://station.example.org/logo.png",
            "tags": "news,jazz",
            "language": "English",
            "country": "United States",
            "countrycode": "US",
            "state": "Texas",
            "codec": "MP3",
            "bitrate": 128,
            "hls": 0,
            "lastcheckok": 1,
            "geo_lat": 30.2672,
            "geo_long": -97.7431,
            "clickcount": 42,
        });
        row.as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        row
    }

    #[test]
    fn normalization_keeps_only_healthy_geolocated_public_https_streams() {
        let normalized = normalize_radio_browser_station(&station(json!({}))).unwrap();
        assert_eq!(normalized.id, UUID);
        assert_eq!(normalized.stream_url, "https://stream.example.org/live.mp3");
        assert_eq!(normalized.country, "United States");
        assert_eq!(normalized.country_code, "US");
        assert_eq!(normalized.metadata_trust, "untrusted-community");
        let france = normalize_radio_browser_station(&station(
            json!({"country": "France", "countrycode": ""}),
        ))
        .unwrap();
        assert_eq!(
            (france.country.as_str(), france.country_code.as_str()),
            ("France", "FR")
        );
        let atlantis = normalize_radio_browser_station(&station(
            json!({"country": "Atlantis", "countrycode": "ZZ"}),
        ))
        .unwrap();
        assert_eq!(
            (atlantis.country.as_str(), atlantis.country_code.as_str()),
            ("Atlantis", "")
        );
        assert!(public_radio_station(&normalized).get("favicon").is_none());
        for overrides in [
            json!({"url_resolved": "http://stream.example.org/live.mp3"}),
            json!({"url_resolved": "https://127.0.0.1/live.mp3"}),
            json!({"url_resolved": "https://[::ffff:127.0.0.1]/live.mp3"}),
            json!({"lastcheckok": 0}),
            json!({"hls": 1}),
            json!({"geo_lat": null}),
            json!({"geo_lat": ""}),
            json!({"codec": "OGG"}),
            json!({"stationuuid": "nope"}),
        ] {
            assert_eq!(
                normalize_radio_browser_station(&station(overrides.clone())),
                None,
                "{overrides}"
            );
        }
        assert_eq!(
            public_radio_https_url(Some(&json!("https://[::1]/stream"))),
            None
        );
        assert_eq!(
            public_radio_https_url(Some(&json!(
                "https://[2606:2800:220:1:248:1893:25c8:1946]/stream"
            ))),
            None
        );
        assert_eq!(
            public_radio_https_url(Some(&json!("https://Example.org/a#frag"))).as_deref(),
            Some("https://example.org/a")
        );
    }

    #[test]
    fn public_stations_follow_the_field_allowlist() {
        let public =
            public_radio_station(&normalize_radio_browser_station(&station(json!({}))).unwrap());
        let keys: Vec<&str> = public
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "id",
                "name",
                "lat",
                "lon",
                "streamUrl",
                "homepage",
                "tags",
                "languages",
                "state",
                "country",
                "countryCode",
                "metadataTrust",
                "codec",
                "bitrate"
            ]
        );
        assert_eq!(public["bitrate"], 128);
        assert_eq!(public["tags"], json!(["news", "jazz"]));
    }

    #[test]
    fn text_cleaning_and_tags() {
        assert_eq!(clean_text("  a\u{0}b\t\tc  ", 80), "a b c");
        assert_eq!(clean_text("abcdef", 3), "abc");
        assert_eq!(normalize_tag("Local_NEWS--Update"), "local news update");
    }
}
