//! Port of `server/providers/radio/constants.js`.

pub const RADIO_DIRECTORY_CACHE_MS: i64 = 45 * 60 * 1000;
pub const RADIO_DIRECTORY_STALE_MS: i64 = 7 * 24 * 60 * 60 * 1000;
pub const RADIO_MIRROR_CACHE_MS: i64 = 6 * 60 * 60 * 1000;
pub const RADIO_FETCH_TIMEOUT_MS: u64 = 12_000;
pub const RADIO_RESPONSE_MAX_BYTES: usize = 4 * 1024 * 1024;
/// The mirror discovery list is small; its cap is tighter.
pub const RADIO_DISCOVERY_MAX_BYTES: usize = 256 * 1024;
pub const RADIO_DIRECTORY_LIMIT: usize = 750;
pub const RADIO_CATALOG_MIN_SUCCESSFUL_QUERIES: usize = 5;
pub const RADIO_CATALOG_HEALTHY_MIN_STATIONS: usize = RADIO_DIRECTORY_LIMIT.div_ceil(2);
pub const RADIO_USER_AGENT: &str = "CameraEye/1.0 (Radio Browser directory client)";
pub const RADIO_FALLBACK_MIRRORS: [&str; 3] = [
    "https://de1.api.radio-browser.info",
    "https://de2.api.radio-browser.info",
    "https://nl1.api.radio-browser.info",
];
