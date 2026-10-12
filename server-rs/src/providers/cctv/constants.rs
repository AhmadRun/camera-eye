//! Port of `server/providers/cctv/constants.js`.

pub const DEFAULT_CCTV_SOURCE_FILE: &str = "config/cctv_sources.austin.json";
/// Austin Open Data portal endpoint for traffic camera records.
pub const DEFAULT_AUSTIN_ROWS_URL: &str =
    "https://data.austintexas.gov/api/views/b4k4-adkb/rows.json?accessType=DOWNLOAD";
pub const DEFAULT_AUSTIN_MAX_SOURCES: f64 = 250.0;
/// Catalog-wide safety ceiling on served cameras (shared round-robin, cap.rs).
pub const DEFAULT_CCTV_MAX_SOURCES: f64 = 4000.0;
/// Hard upper bound for CCTV_MAX_SOURCES; also sizes the health map.
pub const CCTV_MAX_SOURCES_CEILING: f64 = 5000.0;
pub const AUSTIN_DOWNTOWN: (f64, f64) = (30.2672, -97.7431);

pub const TXDOT_ORIGIN: &str = "https://its.txdot.gov";
pub fn txdot_cctv_status_url(district: &str) -> String {
    format!(
        "{TXDOT_ORIGIN}/its/DistrictIts/GetCctvStatusListByDistrict?districtCode={}",
        super::jsval::encode_uri_component(district)
    )
}
/// Per-camera frame: JSON `{snippet:<base64 jpeg>}`, decoded by media.rs.
pub const TXDOT_CCTV_SNAPSHOT_URL: &str =
    "https://its.txdot.gov/its/DistrictIts/GetCctvSnapshotByIcdId";
pub const TXDOT_DISTRICTS: [&str; 25] = [
    "ABL", "AMA", "ATL", "AUS", "BMT", "BWD", "BRY", "CHS", "CRP", "DAL", "ELP", "FTW", "HOU",
    "LRD", "LBB", "LFK", "ODA", "PAR", "PHR", "SJT", "SAT", "TYL", "WAC", "WFS", "YKM",
];
pub const DEFAULT_TXDOT_DISTRICTS: &str = "AUS,SAT";
pub const DEFAULT_TXDOT_MAX_SOURCES: f64 = 500.0;
pub const TXDOT_ANCHORS: [(f64, f64); 5] = [
    (30.2672, -97.7431),
    (29.4241, -98.4936),
    (29.7604, -95.3698),
    (32.7767, -96.797),
    (32.7555, -97.3308),
];
pub fn txdot_district_elevation_m(code: &str) -> Option<f64> {
    Some(match code {
        "ABL" => 520.0,
        "AMA" => 1099.0,
        "ATL" => 105.0,
        "AUS" => 149.0,
        "BMT" => 5.0,
        "BWD" => 425.0,
        "BRY" => 111.0,
        "CHS" => 250.0,
        "CRP" => 7.0,
        "DAL" => 131.0,
        "ELP" => 1140.0,
        "FTW" => 199.0,
        "HOU" => 15.0,
        "LRD" => 132.0,
        "LBB" => 992.0,
        "LFK" => 91.0,
        "ODA" => 890.0,
        "PAR" => 185.0,
        "PHR" => 30.0,
        "SJT" => 585.0,
        "SAT" => 198.0,
        "TYL" => 165.0,
        "WAC" => 143.0,
        "WFS" => 289.0,
        "YKM" => 70.0,
        _ => return None,
    })
}
pub const TXDOT_DEFAULT_ELEVATION_M: f64 = 150.0;

pub fn caltrans_cctv_url(district: i64) -> String {
    format!("https://cwwp2.dot.ca.gov/data/d{district}/cctv/cctvStatusD{district:02}.json")
}
pub const DEFAULT_CALTRANS_DISTRICTS: &str = "4,7,11,3";
pub const DEFAULT_CALTRANS_MAX_SOURCES: f64 = 300.0;
pub const CALTRANS_ANCHORS: [(f64, f64); 4] = [
    (37.7793, -122.4193),
    (34.0537, -118.2428),
    (32.7157, -117.1611),
    (38.5816, -121.4944),
];
pub const TFL_JAMCAM_URL: &str = "https://api.tfl.gov.uk/Place/Type/JamCam";
pub const TFL_IMAGE_ORIGIN: &str = "https://s3-eu-west-1.amazonaws.com/jamcams.tfl.gov.uk/";
pub const DEFAULT_TFL_MAX_SOURCES: f64 = 250.0;
pub const LONDON_CENTER: (f64, f64) = (51.5074, -0.1278);
pub const ONTARIO_511_CAMERAS_URL: &str = "https://511on.ca/api/v2/get/cameras?format=json&lang=en";
pub const ONTARIO_511_IMAGE_ORIGIN: &str = "https://511on.ca/map/Cctv/";
pub const DEFAULT_ONTARIO_MAX_SOURCES: f64 = 1000.0;
pub const ONTARIO_ANCHORS: [(f64, f64); 6] = [
    (43.4516, -80.4925),
    (43.6532, -79.3832),
    (45.4215, -75.6972),
    (43.2557, -79.8711),
    (42.9849, -81.2453),
    (42.3149, -83.0364),
];
pub const FINTRAFFIC_STATIONS_URL: &str = "https://tie.digitraffic.fi/api/weathercam/v1/stations";
pub const FINTRAFFIC_IMAGE_ORIGIN: &str = "https://weathercam.digitraffic.fi/";
pub const DIGITRAFFIC_USER: &str = "camera-eye";
pub const DEFAULT_FINTRAFFIC_MAX_SOURCES: f64 = 300.0;
pub const FINTRAFFIC_GROUND_ELEVATION_M: f64 = 90.0;
pub const FINLAND_ANCHORS: [(f64, f64); 7] = [
    (60.1699, 24.9384),
    (60.4518, 22.2666),
    (61.4978, 23.761),
    (62.2426, 25.7473),
    (62.8924, 27.677),
    (65.0121, 25.4651),
    (66.5039, 25.7294),
];
pub const DRIVEBC_WEBCAMS_URL: &str = "https://www.drivebc.ca/api/webcams/";
pub fn drivebc_image_url(id: &str) -> String {
    format!("https://www.drivebc.ca/images/{id}.jpg")
}
pub const DEFAULT_DRIVEBC_MAX_SOURCES: f64 = 250.0;
pub const DRIVEBC_ANCHORS: [(f64, f64); 2] = [(49.2827, -123.1207), (48.4284, -123.3656)];
pub const DEFAULT_TALLINN_SOURCE_FILE: &str = "config/cctv_sources.tallinn.json";
pub const DEFAULT_TALLINN_MAX_SOURCES: f64 = 255.0;
pub const TALLINN_IMAGE_ORIGIN: &str = "https://ristmikud.tallinn.ee/";
pub const TALLINN_CENTER: (f64, f64) = (59.437, 24.753);
pub const TARKTEE_LOCATIONS_URL: &str =
    "https://tarktee.transpordiamet.ee/api/v1/datex/roadCameraLocations";
pub const TARKTEE_IMAGES_URL: &str =
    "https://tarktee.transpordiamet.ee/api/v1/datex/roadCameraImages";
pub const TARKTEE_IMAGE_ORIGIN: &str = "https://tarktee.transpordiamet.ee/images/";
pub const DEFAULT_TARKTEE_MAX_SOURCES: f64 = 179.0;
pub const TARKTEE_ANCHORS: [(f64, f64); 4] = [
    (59.437, 24.753),
    (58.378, 26.729),
    (58.3859, 24.4971),
    (59.3797, 28.1791),
];
pub const DEFAULT_WARENDORF_SOURCE_FILE: &str = "config/cctv_sources.warendorf.json";
pub const WARENDORF_IMAGE_ORIGINS: [&str; 2] = [
    "http://webcam.warendorf.de/",
    "https://www.kreis-warendorf.de/",
];
pub const NSW_CAMERAS_URL: &str = "https://data.livetraffic.com/cameras/traffic-cam.json";
pub const NSW_IMAGE_ORIGIN: &str = "https://webcams.transport.nsw.gov.au/";
pub const NSW_IMAGE_HOST: &str = "webcams.transport.nsw.gov.au";
pub const DEFAULT_NSW_MAX_SOURCES: f64 = 250.0;
pub const SYDNEY_CENTER: (f64, f64) = (-33.8688, 151.2093);
/// The NSW webcam host answers non-browser clients with an HTML body, so the
/// proxy identifies as a browser for that one host (media.rs).
pub const NSW_IMAGE_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
pub const NSW_MAX_VIEW_LABEL: usize = 140;
pub const DEFAULT_CALGARY_ROWS_URL: &str =
    "https://data.calgary.ca/resource/k7p9-kppz.json?$limit=500";
pub const CALGARY_IMAGE_ORIGIN: &str = "https://trafficcam.calgary.ca/";
pub const DEFAULT_CALGARY_MAX_SOURCES: f64 = 220.0;
pub const CALGARY_DOWNTOWN: (f64, f64) = (51.0461, -114.0626);
pub const CALGARY_MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;
pub const DELDOT_CCTV_URL: &str = "https://tmc.deldot.gov/json/videocamera.json";
pub const DEFAULT_DELDOT_MAX_SOURCES: f64 = 300.0;
pub const DELDOT_MAX_CATALOG_BYTES: usize = 2 * 1024 * 1024;
pub const DELDOT_ANCHORS: [(f64, f64); 3] = [
    (39.7459, -75.5466),
    (39.1582, -75.5244),
    (38.6906, -75.3877),
];
/// Camera catalogs change rarely; frames are fetched per request.
pub const CCTV_SOURCE_CACHE_MS: i64 = 15 * 60 * 1000;
/// Per-provider catalog-fetch timeout.
pub const CCTV_SOURCE_FETCH_TIMEOUT_MS: u64 = 15 * 1000;
/// Individual frame fetches settle before the 10-second client refresh.
pub const CCTV_FRAME_FETCH_TIMEOUT_MS: u64 = 8 * 1000;
/// Maximum buffered snapshot size.
pub const CCTV_FRAME_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
/// Deadline for upstream response headers; live bodies keep streaming afterward.
pub const CCTV_MEDIA_FETCH_TIMEOUT_MS: u64 = 15 * 1000;
/// Silence a live body may carry before the relay gives up on it.
pub const CCTV_MEDIA_IDLE_TIMEOUT_MS: u64 = 30 * 1000;
/// Declared size ceiling for fixed media responses.
pub const CCTV_MEDIA_MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;

/// The proxy's own identifying User-Agent.
pub const CCTV_USER_AGENT: &str = "camera-eye-cctv-proxy/1.0";
