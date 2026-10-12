//! The registered GTFS-Realtime feeds (the server-side parts of
//! `src/data/transitFeeds.js`). Only URLs listed here are ever fetched.

use serde_json::{Value, json};

use crate::providers::mapdata_support::js_json_number;

/// One registry row.
#[derive(Debug)]
pub struct TransitFeed {
    pub id: &'static str,
    pub history_retention: bool,
    pub name: &'static str,
    pub operator: &'static str,
    pub region: &'static str,
    pub center: (f64, f64),
    pub load_radius_km: f64,
    pub url: &'static str,
    pub headers: &'static [(&'static str, &'static str)],
    pub license: &'static str,
    pub license_url: &'static str,
    pub attribution: &'static str,
    pub default_enabled: bool,
    pub default_mode: &'static str,
    pub route_mode: Option<fn(Option<&str>) -> &'static str>,
}

fn mbta_route_mode(route_id: Option<&str>) -> &'static str {
    let Some(route) = route_id.filter(|r| !r.is_empty()) else {
        return "unknown";
    };
    let word_prefix = |prefix: &str| {
        route.strip_prefix(prefix).is_some_and(|rest| {
            !rest
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    };
    if ["Red", "Orange", "Blue"].iter().any(|p| word_prefix(p)) {
        return "subway";
    }
    if route.starts_with("Green") || route.starts_with("Mattapan") {
        return "tram";
    }
    if route.starts_with("CR-") {
        return "rail";
    }
    if route.starts_with("Boat-") {
        return "ferry";
    }
    "bus"
}

fn entur_route_mode(route_id: Option<&str>) -> &'static str {
    let Some(route) = route_id.filter(|r| !r.is_empty()) else {
        return "unknown";
    };
    let codespace = route.split(':').next().unwrap_or("");
    if ["VYG", "GJB", "SJN", "FLT", "GOA", "NSB", "VYT", "FLB"].contains(&codespace) {
        return "rail";
    }
    "bus"
}

fn hsl_route_mode(route_id: Option<&str>) -> &'static str {
    let Some(route) = route_id.filter(|r| !r.is_empty()) else {
        return "unknown";
    };
    let b = route.as_bytes();
    if route.starts_with("31M") {
        return "subway";
    }
    if b.len() >= 4
        && &b[..2] == b"10"
        && ((b[2] == b'0' && (b'1'..=b'9').contains(&b[3]))
            || &b[2..4] == b"10"
            || &b[2..4] == b"15")
    {
        return "tram";
    }
    if route.starts_with("1019") {
        return "ferry";
    }
    if b.len() >= 4 && &b[..3] == b"300" && (b[3].is_ascii_digit() || b[3].is_ascii_uppercase()) {
        return "rail";
    }
    "bus"
}

fn metro_transit_route_mode(route_id: Option<&str>) -> &'static str {
    match route_id.filter(|r| !r.is_empty()) {
        None => "unknown",
        Some("901" | "902") => "tram",
        Some("888") => "rail",
        Some(_) => "bus",
    }
}

/// Registry of feeds, in presentation order.
pub static TRANSIT_FEED_REGISTRY: &[TransitFeed] = &[
    TransitFeed {
        id: "mbta",
        history_retention: true,
        name: "MBTA",
        operator: "Massachusetts Bay Transportation Authority",
        region: "Boston, MA",
        center: (42.3601, -71.0589),
        load_radius_km: 70.0,
        url: "https://cdn.mbta.com/realtime/VehiclePositions.pb",
        headers: &[],
        license: "MassDOT Developers License Agreement",
        license_url: "https://cdn.mbta.com/sites/default/files/2023-08/mbta-massdot-develop-license-agreement.pdf",
        attribution: "MBTA / MassDOT",
        default_enabled: true,
        default_mode: "bus",
        route_mode: Some(mbta_route_mode),
    },
    TransitFeed {
        id: "capmetro-austin",
        history_retention: false,
        name: "CapMetro",
        operator: "Capital Metropolitan Transportation Authority",
        region: "Austin, TX",
        center: (30.2672, -97.7431),
        load_radius_km: 60.0,
        url: "https://data.texas.gov/download/eiei-9rpf/application%2Foctet-stream",
        headers: &[],
        license: "CapMetro Developer Tools license",
        license_url: "https://www.capmetro.org/developertools",
        attribution: "Capital Metropolitan Transportation Authority — data.texas.gov",
        default_enabled: true,
        default_mode: "bus",
        route_mode: None,
    },
    TransitFeed {
        id: "metrotransit-msp",
        history_retention: false,
        name: "Metro Transit",
        operator: "Metro Transit (Metropolitan Council)",
        region: "Minneapolis–St Paul, MN",
        center: (44.9778, -93.265),
        load_radius_km: 70.0,
        url: "https://svc.metrotransit.org/mtgtfs/vehiclepositions.pb",
        headers: &[],
        license: "Public Metro Transit vehicle position data",
        license_url: "https://svc.metrotransit.org/",
        attribution: "Metro Transit — Metropolitan Council",
        default_enabled: true,
        default_mode: "bus",
        route_mode: Some(metro_transit_route_mode),
    },
    TransitFeed {
        id: "hsl-helsinki",
        history_retention: false,
        name: "HSL",
        operator: "Helsinki Region Transport (HSL)",
        region: "Helsinki, Finland",
        center: (60.1699, 24.9384),
        load_radius_km: 70.0,
        url: "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl",
        headers: &[],
        license: "CC BY 4.0",
        license_url: "https://www.hsl.fi/en/hsl/open-data",
        attribution: "HSL (Helsinki Region Transport)",
        default_enabled: true,
        default_mode: "bus",
        route_mode: Some(hsl_route_mode),
    },
    TransitFeed {
        id: "ovapi-nl",
        history_retention: false,
        name: "OVapi",
        operator: "Stichting OpenGeo (NDOV data)",
        region: "Netherlands",
        center: (52.2, 5.3),
        load_radius_km: 220.0,
        url: "https://gtfs.ovapi.nl/nl/vehiclePositions.pb",
        headers: &[],
        license: "Free to use per the OVapi README (best effort, no SLA)",
        license_url: "https://gtfs.ovapi.nl/README",
        attribution: "OVapi / Stichting OpenGeo — Dutch integrated real-time transit data",
        default_enabled: true,
        default_mode: "bus",
        route_mode: None,
    },
    TransitFeed {
        id: "entur-norway",
        history_retention: false,
        name: "Entur",
        operator: "Entur AS (Norwegian national transit data)",
        region: "Norway",
        center: (64.0, 11.5),
        load_radius_km: 720.0,
        url: "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions",
        headers: &[("ET-Client-Name", "camera-eye-transit")],
        license: "Norwegian Licence for Open Government Data (NLOD)",
        license_url: "https://developer.entur.org/pages-intro-authentication",
        attribution: "Entur — data under NLOD",
        default_enabled: true,
        default_mode: "bus",
        route_mode: Some(entur_route_mode),
    },
    TransitFeed {
        id: "translink-seq",
        history_retention: false,
        name: "TransLink",
        operator: "TransLink (Queensland Government)",
        region: "South East Queensland, Australia",
        center: (-27.4698, 153.0251),
        load_radius_km: 150.0,
        url: "https://gtfsrt.api.translink.com.au/api/realtime/seq/VehiclePositions",
        headers: &[],
        license: "CC BY 4.0",
        license_url: "https://translink.com.au/about-translink/open-data",
        attribution: "TransLink — Queensland Government (CC BY 4.0)",
        default_enabled: true,
        default_mode: "bus",
        route_mode: None,
    },
];

/// `TRANSIT_FEED_ID_PATTERN`: `^[a-z0-9][a-z0-9-]{1,63}$`.
fn valid_feed_id(id: &str) -> bool {
    let b = id.as_bytes();
    (2..=64).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// `getTransitFeed`: a pollable (enabled) feed by id.
pub fn get_transit_feed(id: &str) -> Option<&'static TransitFeed> {
    if !valid_feed_id(id) {
        return None;
    }
    TRANSIT_FEED_REGISTRY
        .iter()
        .find(|feed| feed.default_enabled && feed.id == id)
}

/// `publicTransitCatalog`: what `/api/transit/feeds` serves.
pub fn public_transit_catalog() -> Value {
    Value::Array(
        TRANSIT_FEED_REGISTRY
            .iter()
            .filter(|feed| feed.default_enabled)
            .map(|feed| {
                json!({
                    "id": feed.id,
                    "name": feed.name,
                    "operator": feed.operator,
                    "region": feed.region,
                    "center": { "lat": js_json_number(feed.center.0), "lon": js_json_number(feed.center.1) },
                    "loadRadiusKm": js_json_number(feed.load_radius_km),
                    "license": feed.license,
                    "licenseUrl": feed.license_url,
                    "attribution": feed.attribution,
                    "historyRetention": feed.history_retention,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_and_catalog() {
        assert_eq!(get_transit_feed("mbta").unwrap().name, "MBTA");
        assert!(get_transit_feed("nope").is_none());
        assert!(get_transit_feed("MBTA").is_none());
        assert!(get_transit_feed("../etc").is_none());
        let catalog = public_transit_catalog();
        let feeds = catalog.as_array().unwrap();
        assert_eq!(feeds.len(), 7);
        for feed in feeds {
            assert!(feed.get("url").is_none() && feed.get("headers").is_none());
        }
        assert_eq!(
            serde_json::to_string(&feeds[5]["center"]).unwrap(),
            "{\"lat\":64,\"lon\":11.5}"
        );
    }

    #[test]
    fn route_modes() {
        assert_eq!(mbta_route_mode(Some("Red")), "subway");
        assert_eq!(mbta_route_mode(Some("Redline")), "bus");
        assert_eq!(mbta_route_mode(Some("Green-B")), "tram");
        assert_eq!(mbta_route_mode(Some("CR-Fitchburg")), "rail");
        assert_eq!(mbta_route_mode(None), "unknown");
        assert_eq!(hsl_route_mode(Some("1015")), "tram");
        assert_eq!(hsl_route_mode(Some("1019")), "ferry");
        assert_eq!(hsl_route_mode(Some("31M1")), "subway");
        assert_eq!(hsl_route_mode(Some("300Z")), "rail");
        assert_eq!(entur_route_mode(Some("VYG:Line:1")), "rail");
        assert_eq!(metro_transit_route_mode(Some("901")), "tram");
    }
}
