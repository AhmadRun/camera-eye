//! Pure server-side mechanics for the `/api/transit` proxy (port of
//! `src/data/transitProxy.js`): path resolution, snapshot shaping, and the
//! cache/stale policy.

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Value, json};

use super::feeds::{TransitFeed, get_transit_feed};
use super::gtfs::{GTFS_INCREMENTALITY_FULL_DATASET, Vehicle, decode_vehicle_positions};
use super::history::valid_transit_identifier;
use crate::providers::mapdata_support::{decode_uri_component, js_json_number};

/// Fresh window.
pub const TRANSIT_PROXY_TTL_MS: f64 = 15_000.0;
/// Serve-stale window after an upstream failure.
pub const TRANSIT_PROXY_STALE_MAX_MS: f64 = 600_000.0;
/// Upstream fetch timeout, for the whole hop chain and body.
pub const TRANSIT_PROXY_TIMEOUT_MS: u64 = 15_000;
/// Hard cap on upstream bytes.
pub const TRANSIT_PROXY_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
/// Redirect hops followed before a feed is declared unreachable.
pub const TRANSIT_MAX_REDIRECTS: usize = 3;
pub const TRANSIT_REDIRECT_STATUSES: [u16; 5] = [301, 302, 303, 307, 308];
pub const TRANSIT_ADMISSION_WINDOW_MS: i64 = 60_000;
pub const TRANSIT_ADMISSION_MAX_PER_FEED: usize = 8;
pub const TRANSIT_ADMISSION_MAX_GLOBAL: usize = 40;
pub const TRANSIT_BACKOFF_LADDER_MS: [f64; 4] = [5_000.0, 15_000.0, 60_000.0, 300_000.0];

const USER_AGENT: &str = "camera-eye-transit-proxy/1.0 (+https://github.com/AhmadRun/camera-eye)";
const ACCEPT: &str = "application/x-protobuf, application/octet-stream;q=0.9, */*;q=0.1";

/// `isTransitRedirectStatus`.
pub fn is_transit_redirect_status(status: u16) -> bool {
    TRANSIT_REDIRECT_STATUSES.contains(&status)
}

/// A resolved request route.
#[derive(Debug)]
pub enum TransitRoute {
    Feeds,
    Vehicles(&'static TransitFeed),
    Trail(&'static TransitFeed, String),
}

static ROUTE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^/(vehicles|trail)/([^/]+)(?:/([^/]+))?/?$").expect("route regex"));

/// `resolveTransitRoute`: the path after the `/api/transit` mount.
pub fn resolve_transit_route(url: &str) -> Option<TransitRoute> {
    let pathname = url.split('?').next().unwrap_or("");
    if pathname == "/feeds" || pathname == "/feeds/" {
        return Some(TransitRoute::Feeds);
    }
    let caps = ROUTE_RE.captures(pathname)?;
    let id = decode_uri_component(&caps[2])?;
    let vehicle_id = match caps.get(3) {
        Some(m) => Some(decode_uri_component(m.as_str())?),
        None => None,
    };
    let feed = get_transit_feed(&id)?;
    if &caps[1] == "vehicles" {
        return vehicle_id.is_none().then_some(TransitRoute::Vehicles(feed));
    }
    let vehicle_id = vehicle_id?;
    (feed.history_retention && valid_transit_identifier(&vehicle_id))
        .then_some(TransitRoute::Trail(feed, vehicle_id))
}

/// `transitUpstreamHeaders`, in order.
pub fn transit_upstream_headers(
    feed: Option<&TransitFeed>,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> Vec<(String, String)> {
    let mut headers = vec![
        ("User-Agent".to_string(), USER_AGENT.to_string()),
        ("Accept".to_string(), ACCEPT.to_string()),
        ("Accept-Encoding".to_string(), "gzip".to_string()),
    ];
    if let Some(etag) = etag.filter(|e| !e.is_empty()) {
        headers.push(("If-None-Match".to_string(), etag.to_string()));
    }
    if let Some(modified) = last_modified.filter(|m| !m.is_empty()) {
        headers.push(("If-Modified-Since".to_string(), modified.to_string()));
    }
    for (name, value) in feed.map_or(&[][..], |f| f.headers) {
        headers.push((name.to_string(), value.to_string()));
    }
    headers
}

/// `isAcceptableTransitUpstreamUrl`.
pub fn is_acceptable_transit_upstream_url(url: &str) -> bool {
    url::Url::parse(url).is_ok_and(|u| u.scheme() == "https")
}

/// `transitRedirectDecision`: `Ok(next url)` or `Err(reason)`.
pub fn transit_redirect_decision(
    origin_url: &str,
    current_url: &str,
    location: Option<&str>,
) -> Result<String, String> {
    let Some(location) = location.filter(|l| !l.is_empty()) else {
        return Err("redirect without a Location header".into());
    };
    let parsed = url::Url::parse(origin_url).and_then(|origin| {
        let current = url::Url::parse(current_url)?;
        let next = url::Url::options()
            .base_url(Some(&current))
            .parse(location)?;
        Ok((origin.origin().ascii_serialization(), next))
    });
    let Ok((origin, next)) = parsed else {
        return Err("redirect target is not a valid URL".into());
    };
    if next.scheme() != "https" {
        return Err("redirect left https".into());
    }
    if next.origin().ascii_serialization() != origin {
        return Err(format!("redirect left {origin}"));
    }
    Ok(next.to_string())
}

/// `nextTransitBackoffMs`.
pub fn next_transit_backoff_ms(failures: f64) -> f64 {
    let count = if failures.is_finite() {
        failures.floor()
    } else {
        0.0
    };
    if count <= 0.0 {
        return 0.0;
    }
    let index = (count as usize).min(TRANSIT_BACKOFF_LADDER_MS.len()) - 1;
    TRANSIT_BACKOFF_LADDER_MS[index]
}

/// `repairVehicleTimestamps`.
pub fn repair_vehicle_timestamps(
    vehicles: Vec<Vehicle>,
    header: Option<f64>,
    fetched_at_s: f64,
) -> Vec<Vehicle> {
    let header = header.filter(|h| h.is_finite() && *h > 0.0);
    vehicles
        .into_iter()
        .map(|mut vehicle| {
            if vehicle.timestamp.is_some_and(|t| t.is_finite() && t > 0.0) {
                vehicle.timestamp_source = Some("vehicle");
            } else if let Some(header) = header {
                vehicle.timestamp = Some(header);
                vehicle.timestamp_source = Some("header");
            } else {
                vehicle.timestamp = Some(fetched_at_s);
                vehicle.timestamp_source = Some("fetch");
            }
            vehicle
        })
        .collect()
}

/// A snapshot the browser consumes.
pub struct Snapshot {
    pub fetched_at: f64,
    pub vehicles: Vec<Vehicle>,
    pub body: Value,
}

/// Why a feed's bytes could not become a snapshot.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotError {
    /// `TransitFeedShapeError` with `transitReason: 'differential'`.
    Differential(f64),
    /// The bytes are not a FeedMessage (`pbf` threw).
    Malformed,
}

/// One vehicle in the order `normalizeVehicleEntity` builds it.
pub fn vehicle_json(v: &Vehicle) -> Value {
    let num = |value: Option<f64>| value.map_or(Value::Null, js_json_number);
    let mut out = json!({
        "id": v.id,
        "lat": js_json_number(v.lat),
        "lon": js_json_number(v.lon),
        "bearing": num(v.bearing),
        "speedMps": num(v.speed_mps),
        "timestamp": num(v.timestamp),
        "routeId": v.route_id,
        "tripId": v.trip_id,
        "directionId": num(v.direction_id),
        "label": v.label,
        "stopId": v.stop_id,
        "status": v.status,
        "occupancy": v.occupancy,
    });
    if let Some(source) = v.timestamp_source {
        out["timestampSource"] = json!(source);
    }
    out
}

/// `buildTransitSnapshot`.
pub fn build_transit_snapshot(
    feed: &TransitFeed,
    bytes: &[u8],
    now: f64,
) -> Result<Snapshot, SnapshotError> {
    let decoded = decode_vehicle_positions(bytes).map_err(|_| SnapshotError::Malformed)?;
    if decoded.incrementality != GTFS_INCREMENTALITY_FULL_DATASET {
        return Err(SnapshotError::Differential(decoded.incrementality));
    }
    let vehicles =
        repair_vehicle_timestamps(decoded.vehicles, decoded.timestamp, (now / 1000.0).floor());
    let body = json!({
        "feedId": feed.id,
        "name": feed.name,
        "fetchedAt": js_json_number(now),
        "feedTimestamp": decoded.timestamp.map_or(Value::Null, js_json_number),
        "version": decoded.version,
        "entityCount": decoded.entity_count,
        "truncated": decoded.truncated,
        "count": vehicles.len(),
        "vehicles": vehicles.iter().map(vehicle_json).collect::<Vec<_>>(),
    });
    Ok(Snapshot {
        fetched_at: now,
        vehicles,
        body,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    None,
    Fresh,
    Stale,
    Expired,
}

/// `transitCacheState`.
pub fn transit_cache_state(at: Option<f64>, now: f64) -> CacheState {
    let Some(at) = at.filter(|a| a.is_finite()) else {
        return CacheState::None;
    };
    let age = now - at;
    if age < 0.0 || age < TRANSIT_PROXY_TTL_MS {
        CacheState::Fresh
    } else if age < TRANSIT_PROXY_STALE_MAX_MS {
        CacheState::Stale
    } else {
        CacheState::Expired
    }
}

/// `transitResponseHeaders`, in order.
pub fn transit_response_headers(
    cache_state: &str,
    upstream_host: &str,
    contacted_at: Option<f64>,
) -> Vec<(&'static str, String)> {
    let mut headers = vec![
        (
            "Content-Type",
            "application/json; charset=utf-8".to_string(),
        ),
        (
            "Cache-Control",
            if cache_state == "STALE-ERROR" {
                "no-store".to_string()
            } else {
                format!(
                    "public, max-age={}",
                    (TRANSIT_PROXY_TTL_MS / 1000.0).floor()
                )
            },
        ),
        ("X-CE-Cache", cache_state.to_string()),
    ];
    if !upstream_host.is_empty() {
        headers.push(("X-Transit-Upstream", upstream_host.to_string()));
    }
    if let Some(at) = contacted_at.filter(|a| a.is_finite()) {
        headers.push((
            "X-Transit-Contact",
            crate::providers::mapdata_support::js_number_string(at.floor()),
        ));
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::super::gtfs::tests::{Fixture, encode_feed};
    use super::*;

    #[test]
    fn route_resolution_admits_only_registered_ids() {
        assert!(matches!(
            resolve_transit_route("/feeds"),
            Some(TransitRoute::Feeds)
        ));
        assert!(matches!(
            resolve_transit_route("/feeds/?x=1"),
            Some(TransitRoute::Feeds)
        ));
        for ok in ["/vehicles/mbta", "/vehicles/mbta/", "/vehicles/mbta?trip=1"] {
            assert!(
                matches!(resolve_transit_route(ok), Some(TransitRoute::Vehicles(f)) if f.id == "mbta")
            );
        }
        for bad in [
            "/vehicles/nope",
            "/vehicles/",
            "/vehicles/mbta/extra",
            "/vehicles/..%2F..%2Fetc",
            "/vehicles/%E0%A4%A",
            "/",
            "",
        ] {
            assert!(resolve_transit_route(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn retained_routes_validate_the_entire_identifier() {
        assert!(
            matches!(resolve_transit_route("/trail/mbta/bus%201"), Some(TransitRoute::Trail(_, v)) if v == "bus 1")
        );
        let long = crate::providers::mapdata_support::encode_uri_component(&"é".repeat(129));
        for path in [
            "/trail/mbta/a%2Fb".to_string(),
            "/trail/mbta/%00".into(),
            "/trail/mbta/%5C".into(),
            "/trail/mbta/%2E%2E".into(),
            "/trail/mbta/%E0%A4%A".into(),
            format!("/trail/mbta/{long}"),
            "/trail/capmetro-austin/b".into(),
        ] {
            assert!(resolve_transit_route(&path).is_none(), "{path}");
        }
    }

    #[test]
    fn upstream_headers_identify_the_proxy() {
        let plain = transit_upstream_headers(get_transit_feed("mbta"), None, None);
        let get = |h: &[(String, String)], k: &str| {
            h.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone())
        };
        assert!(
            get(&plain, "User-Agent")
                .unwrap()
                .contains("camera-eye-transit-proxy")
        );
        assert!(get(&plain, "Accept").unwrap().contains("x-protobuf"));
        let entur = get_transit_feed("entur-norway");
        let headers = transit_upstream_headers(entur, None, None);
        assert_eq!(
            get(&headers, "ET-Client-Name").as_deref(),
            Some("camera-eye-transit")
        );
        assert_eq!(get(&headers, "Accept-Encoding").as_deref(), Some("gzip"));
        assert!(get(&headers, "If-None-Match").is_none());
        assert!(get(&transit_upstream_headers(None, None, None), "User-Agent").is_some());
        let conditional = transit_upstream_headers(
            entur,
            Some("W/\"abc\""),
            Some("Wed, 10 Sep 2026 12:00:00 GMT"),
        );
        assert_eq!(
            get(&conditional, "If-None-Match").as_deref(),
            Some("W/\"abc\"")
        );
        assert_eq!(
            get(&conditional, "If-Modified-Since").as_deref(),
            Some("Wed, 10 Sep 2026 12:00:00 GMT")
        );
    }

    #[test]
    fn only_https_upstreams() {
        assert!(is_acceptable_transit_upstream_url(
            "https://cdn.mbta.com/x.pb"
        ));
        assert!(!is_acceptable_transit_upstream_url(
            "http://gtfs.ovapi.nl/nl/vehiclePositions.pb"
        ));
        assert!(!is_acceptable_transit_upstream_url("ftp://x"));
        assert!(!is_acceptable_transit_upstream_url("not a url"));
    }

    #[test]
    fn snapshot_shape_carries_provenance() {
        let mut writer = super::super::gtfs::tests::Writer::default();
        writer.embed(1, |h| {
            h.string_field(1, "2.0");
            h.varint_field(3, 1_700_000_000);
        });
        let snapshot =
            build_transit_snapshot(get_transit_feed("mbta").unwrap(), &writer.0, 12345.0).unwrap();
        assert_eq!(
            serde_json::to_string(&snapshot.body).unwrap(),
            r#"{"feedId":"mbta","name":"MBTA","fetchedAt":12345,"feedTimestamp":1700000000,"version":"2.0","entityCount":0,"truncated":false,"count":0,"vehicles":[]}"#
        );
    }

    #[test]
    fn cache_policy_windows() {
        let now = 1_000_000.0;
        assert_eq!(transit_cache_state(None, now), CacheState::None);
        assert_eq!(transit_cache_state(Some(f64::NAN), now), CacheState::None);
        assert_eq!(transit_cache_state(Some(now), now), CacheState::Fresh);
        assert_eq!(
            transit_cache_state(Some(now - TRANSIT_PROXY_TTL_MS + 1.0), now),
            CacheState::Fresh
        );
        assert_eq!(
            transit_cache_state(Some(now - TRANSIT_PROXY_TTL_MS), now),
            CacheState::Stale
        );
        assert_eq!(
            transit_cache_state(Some(now - TRANSIT_PROXY_STALE_MAX_MS + 1.0), now),
            CacheState::Stale
        );
        assert_eq!(
            transit_cache_state(Some(now - TRANSIT_PROXY_STALE_MAX_MS), now),
            CacheState::Expired
        );
        assert_eq!(
            transit_cache_state(Some(now + 5000.0), now),
            CacheState::Fresh
        );
    }

    #[test]
    fn response_headers_mark_cache_state() {
        let find = |h: Vec<(&'static str, String)>, k: &str| {
            h.into_iter().find(|(n, _)| *n == k).map(|(_, v)| v)
        };
        assert_eq!(
            find(
                transit_response_headers("HIT", "cdn.mbta.com", None),
                "X-Transit-Upstream"
            )
            .as_deref(),
            Some("cdn.mbta.com")
        );
        assert_eq!(
            find(transit_response_headers("HIT", "", None), "Cache-Control").as_deref(),
            Some("public, max-age=15")
        );
        assert_eq!(
            find(
                transit_response_headers("STALE-ERROR", "", None),
                "Cache-Control"
            )
            .as_deref(),
            Some("no-store")
        );
        assert_eq!(
            find(transit_response_headers("MISS", "", None), "X-CE-Cache").as_deref(),
            Some("MISS")
        );
        assert_eq!(
            find(
                transit_response_headers("HIT", "", Some(12.7)),
                "X-Transit-Contact"
            )
            .as_deref(),
            Some("12")
        );
    }

    #[test]
    fn redirects_are_judged_before_they_are_followed() {
        let url = "https://data.example.gov/download/feed.pb";
        assert_eq!(
            transit_redirect_decision(url, url, Some("https://data.example.gov/files/a.pb")),
            Ok("https://data.example.gov/files/a.pb".into())
        );
        assert_eq!(
            transit_redirect_decision(url, url, Some("/files/b.pb")),
            Ok("https://data.example.gov/files/b.pb".into())
        );
        assert_eq!(
            transit_redirect_decision(url, url, Some("https://cdn.elsewhere.test/a.pb")),
            Err("redirect left https://data.example.gov".into())
        );
        for bad in [
            Some("http://data.example.gov/a.pb"),
            None,
            Some("javascript:alert(1)"),
            Some("data:text/plain,x"),
            Some("https://data.example.gov:8443/a.pb"),
        ] {
            assert!(transit_redirect_decision(url, url, bad).is_err(), "{bad:?}");
        }
        assert_eq!(
            transit_redirect_decision("not-a-url", "not-a-url", Some("/a.pb")),
            Err("redirect target is not a valid URL".into())
        );
    }

    #[test]
    fn failure_cooldown_climbs_and_stops() {
        assert_eq!(next_transit_backoff_ms(0.0), 0.0);
        assert_eq!(next_transit_backoff_ms(-3.0), 0.0);
        let ladder: Vec<f64> = [1.0, 2.0, 3.0, 4.0]
            .into_iter()
            .map(next_transit_backoff_ms)
            .collect();
        assert_eq!(ladder, TRANSIT_BACKOFF_LADDER_MS);
        assert_eq!(next_transit_backoff_ms(9.0), 300_000.0);
        assert_eq!(next_transit_backoff_ms(500.0), 300_000.0);
    }

    fn entity(id: &str, timestamp: Option<u64>) -> Fixture {
        Fixture {
            entity_id: Some(id.into()),
            vehicle_id: Some(id.into()),
            lat: Some(42.36),
            lon: -71.05,
            timestamp,
            ..Default::default()
        }
    }

    #[test]
    fn differential_feeds_are_refused() {
        let feed = get_transit_feed("mbta").unwrap();
        let differential = encode_feed(
            &[entity("v1", Some(1_700_000_000))],
            Some(1),
            Some(1_700_000_000),
        );
        assert_eq!(
            build_transit_snapshot(feed, &differential, 1.7e12).err(),
            Some(SnapshotError::Differential(1.0))
        );
        let full = encode_feed(
            &[entity("v1", Some(1_700_000_000))],
            Some(0),
            Some(1_700_000_000),
        );
        assert_eq!(
            build_transit_snapshot(feed, &full, 1.7e12)
                .unwrap()
                .vehicles
                .len(),
            1
        );
        assert!(
            build_transit_snapshot(feed, &encode_feed(&[], None, Some(1_700_000_000)), 1.0).is_ok()
        );
    }

    #[test]
    fn timestamps_borrow_from_header_and_say_so() {
        let vehicle = |id: &str, t: Option<f64>| Vehicle {
            id: id.into(),
            lat: 1.0,
            lon: 1.0,
            bearing: None,
            speed_mps: None,
            timestamp: t,
            route_id: None,
            trip_id: None,
            direction_id: None,
            label: None,
            stop_id: None,
            status: None,
            occupancy: None,
            timestamp_source: None,
        };
        let repaired = repair_vehicle_timestamps(
            vec![
                vehicle("own", Some(1_700_000_400.0)),
                vehicle("none", None),
                vehicle("zero", Some(0.0)),
            ],
            Some(1_700_000_450.0),
            1_700_000_500.0,
        );
        let got: Vec<(f64, &str)> = repaired
            .iter()
            .map(|v| (v.timestamp.unwrap(), v.timestamp_source.unwrap()))
            .collect();
        assert_eq!(
            got,
            [
                (1_700_000_400.0, "vehicle"),
                (1_700_000_450.0, "header"),
                (1_700_000_450.0, "header")
            ]
        );
        let none = repair_vehicle_timestamps(vec![vehicle("none", None)], None, 1_700_000_500.0);
        assert_eq!(
            (none[0].timestamp, none[0].timestamp_source),
            (Some(1_700_000_500.0), Some("fetch"))
        );

        let bytes = encode_feed(&[entity("unstamped", None)], None, Some(1_700_000_000));
        let snapshot = build_transit_snapshot(
            get_transit_feed("mbta").unwrap(),
            &bytes,
            1_700_000_900_000.0,
        )
        .unwrap();
        assert_eq!(snapshot.body["vehicles"][0]["timestamp"], 1_700_000_000_u64);
        assert_eq!(snapshot.body["vehicles"][0]["timestampSource"], "header");
        assert_eq!(snapshot.body["feedTimestamp"], 1_700_000_000_u64);
    }
}
