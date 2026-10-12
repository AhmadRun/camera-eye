//! Local decoder feeds for the Local ADS-B layer
//! (`server/providers/local-receivers.js`): `GET /api/local-receivers/aircraft`
//! reads the operator's `LOCAL_RECEIVER_FEEDS` dump1090/readsb/skyaware978
//! `aircraft.json` documents and returns them as Local ADS-B records.
//!
//! Hosts must be loopback, RFC1918, `localhost` or `*.local`. A name is
//! resolved before every read, every address must be local, and the
//! connection is pinned to exactly those addresses. Redirects are refused.

use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::response::Response;
use axum::routing::any;
use http::Method;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::jsval::{self, num};
use super::records::normalize_dump1090_aircraft;
use crate::common::coalesce::Coalescer;
use crate::common::http::{UpstreamError, read_text_capped};
use crate::common::js::now_ms;
use crate::common::reply;
use crate::state::AppState;

pub const LOCAL_RECEIVERS_ROUTE: &str = "/api/local-receivers/aircraft";
pub const LOCAL_RECEIVER_FEEDS_ENV: &str = "LOCAL_RECEIVER_FEEDS";
pub const LOCAL_RECEIVER_TIMEOUT_MS: u64 = 2_000;
pub const LOCAL_RECEIVER_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
pub const LOCAL_RECEIVER_CACHE_MS: f64 = 1_000.0;
/// A feed whose own `now` is older than this is reported stale.
pub const LOCAL_RECEIVER_STALE_MS: f64 = 10_000.0;
const MAX_FEEDS: usize = 8;
const LOG_PREFIX: &str = "[local-receivers]";

fn band_label(band: &str) -> &'static str {
    if band == "978" {
        "978 MHz UAT"
    } else {
        "1090 MHz"
    }
}

// ---------------------------------------------------------------------------
// src/data/tapAddress.js (the parts the feed parser uses)
// ---------------------------------------------------------------------------

/// `isLocalIpv4`: a dotted quad in loopback or RFC1918.
pub fn is_local_ipv4(host: &str) -> bool {
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() != 4
        || parts
            .iter()
            .any(|p| p.is_empty() || p.len() > 3 || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    let octets: Vec<u32> = parts.iter().map(|p| p.parse().unwrap_or(999)).collect();
    if octets.iter().any(|o| *o > 255) {
        return false;
    }
    let (a, b) = (octets[0], octets[1]);
    a == 127 || a == 10 || (a == 172 && (16..=31).contains(&b)) || (a == 192 && b == 168)
}

/// `MDNS_RE`: a dotted name ending in `.local`.
fn is_mdns_name(host: &str) -> bool {
    let Some(stem) = host.strip_suffix(".local") else {
        return false;
    };
    !stem.is_empty()
        && stem.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && !label.starts_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

/// `parseTapAddress`: `(host, port)` for a local `host:port`.
pub fn parse_tap_address(raw: &str) -> Option<(String, u16)> {
    let value = raw.trim().to_lowercase();
    if value.is_empty() || value.encode_utf16().count() > 263 {
        return None;
    }
    if value.contains(['/', '\\', '?', '#', '@']) || value.contains("://") {
        return None;
    }
    let colon = value.rfind(':')?;
    if colon == 0 || colon == value.len() - 1 {
        return None;
    }
    let host = &value[..colon];
    let port_text = &value[colon + 1..];
    if port_text.is_empty() || port_text.len() > 5 || !port_text.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let port: u32 = port_text.parse().ok()?;
    if !(1..=65535).contains(&port) {
        return None;
    }
    if !(host == "localhost" || is_mdns_name(host) || is_local_ipv4(host)) {
        return None;
    }
    Some((host.to_string(), port as u16))
}

// ---------------------------------------------------------------------------
// LOCAL_RECEIVER_FEEDS
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Feed {
    pub id: String,
    pub band: Option<String>,
    pub label: String,
    pub url: Option<String>,
    pub reason: Option<String>,
}

fn parse_entry(entry: &str) -> (Option<String>, Result<String, String>) {
    let Some(separator) = entry.find('=').filter(|s| *s > 0) else {
        return (None, Err("expected band=url".into()));
    };
    let band = entry[..separator].trim();
    let raw_url = entry[separator + 1..].trim();
    if band != "1090" && band != "978" {
        return (None, Err("band must be 1090 or 978".into()));
    }
    let band = Some(band.to_string());
    let Ok(url) = url::Url::parse(raw_url) else {
        return (band, Err("not a valid URL".into()));
    };
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return (band, Err("scheme must be http or https".into()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return (band, Err("credentials are not allowed in the URL".into()));
    }
    if url.query().is_some_and(|q| !q.is_empty())
        || url.fragment().is_some_and(|f| !f.is_empty())
        || raw_url.contains('?')
        || raw_url.contains('#')
    {
        return (band, Err("query and fragment are not allowed".into()));
    }
    let port = url
        .port()
        .unwrap_or(if scheme == "https" { 443 } else { 80 });
    let hostname = url.host_str().unwrap_or("");
    let Some((host, port)) = parse_tap_address(&format!("{hostname}:{port}")) else {
        return (
            band,
            Err("host must be loopback, RFC1918, localhost or *.local".into()),
        );
    };
    let path = url.path();
    if !(path == "aircraft.json" || path.ends_with("/aircraft.json")) {
        return (band, Err("path must end in aircraft.json".into()));
    }
    (band, Ok(format!("{scheme}://{host}:{port}{path}")))
}

/// `parseLocalReceiverFeeds`: `(configured, feeds)`. Labels name the band
/// only, so feed addresses never reach the browser.
pub fn parse_local_receiver_feeds(value: Option<&str>) -> (bool, Vec<Feed>) {
    let entries: Vec<&str> = value
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .collect();
    let mut feeds = Vec::new();
    let mut per_band: HashMap<Option<String>, usize> = HashMap::new();
    for (index, entry) in entries.iter().enumerate() {
        let (band, parsed) = if index < MAX_FEEDS {
            parse_entry(entry)
        } else {
            (None, Err(format!("at most {MAX_FEEDS} feeds are read")))
        };
        let ordinal = per_band.get(&band).copied().unwrap_or(0) + 1;
        per_band.insert(band.clone(), ordinal);
        let label = match &band {
            Some(band) if ordinal > 1 => format!("{} #{ordinal}", band_label(band)),
            Some(band) => band_label(band).to_string(),
            None => format!("entry {}", index + 1),
        };
        let (url, reason) = match parsed {
            Ok(url) => (Some(url), None),
            Err(reason) => (None, Some(reason)),
        };
        feeds.push(Feed {
            id: format!("feed-{}", index + 1),
            band,
            label,
            url,
            reason,
        });
    }
    // Ordinals are only needed when a band actually repeats.
    for feed in &mut feeds {
        if feed.band.is_some()
            && per_band.get(&feed.band).copied().unwrap_or(0) > 1
            && !feed.label.contains('#')
        {
            feed.label.push_str(" #1");
        }
    }
    (!feeds.is_empty(), feeds)
}

// ---------------------------------------------------------------------------
// Reading one feed
// ---------------------------------------------------------------------------

/// `isLocalReceiverAddress`: loopback/RFC1918 IPv4, `::1`, or IPv4-mapped.
pub fn is_local_receiver_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V6(v6) if v6.is_loopback() => true,
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .is_some_and(|v4| is_local_ipv4(&v4.to_string())),
        IpAddr::V4(v4) => is_local_ipv4(&v4.to_string()),
    }
}

pub type LookupFuture = Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, ()>> + Send>>;
/// `dns.promises.lookup(hostname, { all: true })`-compatible resolver.
pub type Lookup = Arc<dyn Fn(String) -> LookupFuture + Send + Sync>;
pub type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;

fn system_lookup() -> Lookup {
    Arc::new(|hostname: String| {
        Box::pin(async move {
            tokio::net::lookup_host((hostname.as_str(), 0))
                .await
                .map(|addrs| addrs.map(|a| a.ip()).collect())
                .map_err(|_| ())
        })
    })
}

/// `resolveLocalReceiverAddresses`.
async fn resolve_addresses(hostname: &str, lookup: &Lookup) -> Result<Vec<IpAddr>, &'static str> {
    let addresses = lookup(hostname.to_string())
        .await
        .map_err(|_| "UNRESOLVED")?;
    if addresses.is_empty() {
        return Err("UNRESOLVED");
    }
    if addresses.iter().any(|a| !is_local_receiver_address(*a)) {
        return Err("FORBIDDEN_ADDRESS");
    }
    Ok(addresses)
}

fn feed_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
}

/// `fetchLocalReceiverDocument`: the parsed document or an error code.
pub async fn fetch_local_receiver_document(
    url: &str,
    ip_client: &reqwest::Client,
    lookup: &Lookup,
    timeout: Duration,
    max_bytes: usize,
) -> Result<Value, &'static str> {
    let read = async {
        let parsed = url::Url::parse(url).map_err(|_| "UNREACHABLE")?;
        let hostname = parsed.host_str().unwrap_or("").to_string();
        // IP literals were validated with the feed list; a name is validated
        // by what it resolves to now, and the socket is pinned there.
        let client = if is_local_ipv4(&hostname) {
            ip_client.clone()
        } else {
            let addresses = resolve_addresses(&hostname, lookup).await?;
            let port = parsed.port_or_known_default().unwrap_or(80);
            let pinned: Vec<SocketAddr> = addresses
                .iter()
                .map(|ip| SocketAddr::new(*ip, port))
                .collect();
            // A fresh client per read: a pooled socket could outlive the pin.
            feed_client_builder()
                .resolve_to_addrs(&hostname, &pinned)
                .pool_max_idle_per_host(0)
                .build()
                .map_err(|_| "UNREACHABLE")?
        };
        let upstream = client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|_| "UNREACHABLE")?;
        let status = upstream.status();
        if status.is_redirection() {
            return Err("REDIRECT_REFUSED");
        }
        if !status.is_success() {
            return Err("HTTP_STATUS");
        }
        let text = read_text_capped(upstream, max_bytes)
            .await
            .map_err(|error| match error {
                UpstreamError::TooLarge => "RESPONSE_TOO_LARGE",
                _ => "UNREACHABLE",
            })?;
        let json: Value = serde_json::from_str(&text).map_err(|_| "BAD_JSON")?;
        if !json.get("aircraft").is_some_and(Value::is_array) {
            return Err("BAD_DOCUMENT");
        }
        Ok(json)
    };
    tokio::time::timeout(timeout, read)
        .await
        .unwrap_or(Err("TIMEOUT"))
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

pub struct Options {
    pub feeds_value: Option<String>,
    pub lookup: Lookup,
    pub now: Clock,
    pub timeout: Duration,
    pub max_bytes: usize,
    pub cache_ms: f64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            feeds_value: None,
            lookup: system_lookup(),
            now: Arc::new(|| now_ms() as f64),
            timeout: Duration::from_millis(LOCAL_RECEIVER_TIMEOUT_MS),
            max_bytes: LOCAL_RECEIVER_MAX_BODY_BYTES,
            cache_ms: LOCAL_RECEIVER_CACHE_MS,
        }
    }
}

struct Inner {
    configured: bool,
    feeds: Vec<Feed>,
    lookup: Lookup,
    now: Clock,
    timeout: Duration,
    max_bytes: usize,
    cache_ms: f64,
    ip_client: reqwest::Client,
    last_status: Mutex<HashMap<String, String>>,
    cached: Mutex<Option<(f64, Value)>>,
    in_flight: Coalescer<&'static str, Value>,
}

/// `createLocalReceiversHandler`.
#[derive(Clone)]
pub struct LocalReceivers(Arc<Inner>);

impl LocalReceivers {
    pub fn new(options: Options) -> Self {
        let (configured, feeds) = parse_local_receiver_feeds(options.feeds_value.as_deref());
        for feed in &feeds {
            if let Some(reason) = &feed.reason {
                tracing::warn!(
                    "{LOG_PREFIX} {LOCAL_RECEIVER_FEEDS_ENV} {} ({}) rejected: {reason}. It will not be fetched.",
                    feed.id,
                    feed.label
                );
            }
        }
        let ip_client = feed_client_builder()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self(Arc::new(Inner {
            configured,
            feeds,
            lookup: options.lookup,
            now: options.now,
            timeout: options.timeout,
            max_bytes: options.max_bytes,
            cache_ms: options.cache_ms,
            ip_client,
            last_status: Mutex::new(HashMap::new()),
            cached: Mutex::new(None),
            in_flight: Coalescer::new(),
        }))
    }

    /// Log a feed only when its status changes.
    fn log(&self, feed: &Feed, status: &str, code: Option<&str>) {
        let mut last = self.0.last_status.lock();
        if last.get(&feed.id).map(String::as_str) == Some(status) {
            return;
        }
        last.insert(feed.id.clone(), status.to_string());
        if status == "live" {
            tracing::info!("{LOG_PREFIX} {} live", feed.label);
        } else {
            let code = code.map(|c| format!(" ({c})")).unwrap_or_default();
            tracing::warn!("{LOG_PREFIX} {} {status}{code}", feed.label);
        }
    }

    async fn read_feed(&self, feed: &Feed) -> (Value, Vec<Value>) {
        let summary = |status: &str, aircraft: usize, age: Value| {
            json!({
                "band": feed.band,
                "label": feed.label,
                "status": status,
                "aircraft": aircraft,
                "ageMs": age,
            })
        };
        let Some(url) = &feed.url else {
            return (summary("invalid", 0, Value::Null), Vec::new());
        };
        let inner = &self.0;
        let json = match fetch_local_receiver_document(
            url,
            &inner.ip_client,
            &inner.lookup,
            inner.timeout,
            inner.max_bytes,
        )
        .await
        {
            Ok(json) => json,
            Err(code) => {
                self.log(feed, "unreachable", Some(code));
                return (summary("unreachable", 0, Value::Null), Vec::new());
            }
        };
        let received_at = (inner.now)();
        let feed_now_s = jsval::to_number(json.get("now"));
        // The feed's own clock decides staleness; no `now` reads as current.
        let age_ms = if feed_now_s.is_finite() {
            jsval::round(received_at - feed_now_s * 1000.0).max(0.0)
        } else {
            0.0
        };
        let status = if age_ms > LOCAL_RECEIVER_STALE_MS {
            "stale"
        } else {
            "live"
        };
        self.log(feed, status, None);
        let records = normalize_dump1090_aircraft(
            &json,
            received_at - age_ms,
            feed.band.as_deref().unwrap_or("1090"),
        );
        (summary(status, records.len(), num(age_ms)), records)
    }

    async fn snapshot(&self) -> Value {
        let results =
            futures_util::future::join_all(self.0.feeds.iter().map(|feed| self.read_feed(feed)))
                .await;
        let mut feeds = Vec::new();
        let mut records = Vec::new();
        for (status, feed_records) in results {
            feeds.push(status);
            records.extend(feed_records);
        }
        json!({
            "configured": true,
            "generatedAt": num((self.0.now)()),
            "feeds": feeds,
            "records": records,
        })
    }

    pub async fn payload(&self) -> Value {
        if !self.0.configured {
            return json!({ "configured": false, "feeds": [], "records": [] });
        }
        let now = (self.0.now)();
        if let Some((at, payload)) = &*self.0.cached.lock()
            && now - at < self.0.cache_ms
        {
            return payload.clone();
        }
        let this = self.clone();
        let (result, _) = self
            .0
            .in_flight
            .run("all", move || async move { this.snapshot().await })
            .await;
        *self.0.cached.lock() = Some(((self.0.now)(), result.clone()));
        result
    }

    pub async fn handle(&self, method: &Method) -> Response {
        if method != Method::GET && method != Method::HEAD {
            return reply::json(
                405,
                &json!({ "error": "Method Not Allowed" }),
                Some("no-store"),
            );
        }
        reply::json(200, &self.payload().await, Some("no-store"))
    }
}

/// Mount `GET /api/local-receivers/aircraft`. The feed list is read once,
/// when the server starts.
pub fn router(state: &AppState) -> Router<AppState> {
    let receivers = LocalReceivers::new(Options {
        feeds_value: state.env.raw(LOCAL_RECEIVER_FEEDS_ENV),
        ..Options::default()
    });
    let handler = move |method: Method| {
        let receivers = receivers.clone();
        async move { receivers.handle(&method).await }
    };
    Router::new()
        .route(LOCAL_RECEIVERS_ROUTE, any(handler.clone()))
        .route("/api/local-receivers/aircraft/", any(handler))
}

#[cfg(test)]
#[path = "local_receivers_tests.rs"]
mod tests;
