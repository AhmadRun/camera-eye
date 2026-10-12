//! Port of `server/providers/radio/transport.js`: the Radio Browser
//! destination allowlist, the resolved-address policy and the pinned fetch.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;

use super::stations::is_non_global_ipv4;
use crate::providers::cctv::fetch::{FetchRequest, FetchResponse};
use crate::providers::cctv::jsval;

/// `radioMirrorOrigin`: `https://<name>.api.radio-browser.info`, or `None`.
pub fn radio_mirror_origin(value: Option<&Value>) -> Option<String> {
    static MIRROR: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[a-z0-9-]+\.api\.radio-browser\.info$").expect("regex"));
    let text = match value {
        None | Some(Value::Null) => String::new(),
        Some(v) => jsval::string(Some(v)),
    }
    .to_lowercase();
    let host = text.strip_suffix('.').unwrap_or(&text);
    MIRROR.is_match(host).then(|| format!("https://{host}"))
}

fn origin_of_host(host: &str) -> Option<String> {
    radio_mirror_origin(Some(&Value::String(host.to_string())))
}

/// `isPublicRadioAddress`: whether a resolved address is safe to contact.
/// IPv4 must be global; IPv6 must be in 2000::/3 outside the Teredo /
/// benchmarking, documentation, 6to4 and 3fff::/20 documentation blocks.
pub fn is_public_radio_address(value: &str) -> bool {
    let lowered = jsval::trim(value).to_lowercase();
    let address = lowered.strip_prefix('[').unwrap_or(&lowered);
    let address = address.strip_suffix(']').unwrap_or(address);
    if address.is_empty() {
        return false;
    }
    if !address.contains(':') {
        let parts: Vec<&str> = address.split('.').collect();
        return parts.len() == 4
            && parts.iter().all(|p| {
                !p.is_empty()
                    && p.len() <= 3
                    && p.bytes().all(|b| b.is_ascii_digit())
                    && p.parse::<u32>().is_ok_and(|n| n <= 255)
            })
            && !is_non_global_ipv4(address);
    }
    let pieces: Vec<&str> = address.split("::").collect();
    if pieces.len() > 2 {
        return false;
    }
    let split = |piece: &str| -> Vec<String> {
        if piece.is_empty() {
            Vec::new()
        } else {
            piece.split(':').map(str::to_string).collect()
        }
    };
    let left = split(pieces[0]);
    let right = if pieces.len() == 2 {
        split(pieces[1])
    } else {
        Vec::new()
    };
    let missing = 8 - left.len() as i64 - right.len() as i64;
    if (pieces.len() == 1 && missing != 0) || (pieces.len() == 2 && missing < 1) {
        return false;
    }
    let mut groups = left;
    groups.extend(std::iter::repeat_n(
        "0".to_string(),
        missing.max(0) as usize,
    ));
    groups.extend(right);
    if groups.len() != 8
        || groups
            .iter()
            .any(|g| g.is_empty() || g.len() > 4 || !g.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return false;
    }
    let numeric = groups.iter().fold(0u128, |total, g| {
        (total << 16) | u128::from_str_radix(g, 16).unwrap_or(0)
    });
    let cidr = |base: u128, prefix: u32| {
        let shift = 128 - prefix;
        numeric >> shift == base >> shift
    };
    cidr(0x2000 << 112, 3)
        && !cidr(0x2001 << 112, 23)
        && !cidr((0x2001 << 112) | (0x0db8 << 96), 32)
        && !cidr(0x2002 << 112, 16)
        && !cidr(0x3fff << 112, 20)
}

/// `radioProxyDestination`: only the discovery, search and click endpoints
/// of a Radio Browser mirror, on the default HTTPS port, are permitted.
pub fn radio_proxy_destination(value: &str) -> Option<url::Url> {
    static CLICK: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)^/json/url/[0-9a-f-]+$").expect("regex"));
    let url = url::Url::parse(value).ok()?;
    let host = url.host_str()?;
    let origin = origin_of_host(host)?;
    let no_search = url.query().is_none_or(str::is_empty);
    if url.origin().ascii_serialization() != origin
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some_and(|f| !f.is_empty())
    {
        return None;
    }
    let discovery = host.eq_ignore_ascii_case("all.api.radio-browser.info")
        && url.path() == "/json/servers"
        && no_search;
    let directory = url.path() == "/json/stations/search";
    let click = CLICK.is_match(url.path()) && no_search;
    (discovery || directory || click).then_some(url)
}

/// DNS lookup (`dns.lookup(host, { all: true, verbatim: true })`).
pub type Lookup =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Vec<String>, String>> + Send + Sync>;

/// A request to a pinned set of resolved addresses.
pub type PinnedFetch = Arc<
    dyn Fn(FetchRequest, Vec<IpAddr>) -> BoxFuture<'static, Result<FetchResponse, String>>
        + Send
        + Sync,
>;

pub fn system_lookup() -> Lookup {
    Arc::new(|host: String| {
        async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 443))
                .await
                .map_err(|e| e.to_string())?;
            Ok(addresses.map(|a| a.ip().to_string()).collect())
        }
        .boxed()
    })
}

/// `resolveRadioProxyAddresses`: every resolved address must be public.
pub async fn resolve_radio_proxy_addresses(
    hostname: &str,
    lookup: &Lookup,
) -> Result<Vec<IpAddr>, String> {
    let rows: Vec<String> = lookup(hostname.to_string())
        .await?
        .into_iter()
        .filter(|a| !a.is_empty())
        .collect();
    if rows.is_empty() || rows.iter().any(|a| !is_public_radio_address(a)) {
        return Err("Radio Browser resolved to a forbidden address".into());
    }
    rows.iter()
        .map(|a| {
            a.parse::<IpAddr>()
                .map_err(|_| "Radio Browser resolved to a forbidden address".to_string())
        })
        .collect()
}

/// `fetchPinnedRadioResponse`: connect only to the checked addresses (TLS
/// still verifies the mirror's hostname), never follow redirects, never use
/// an environment proxy, and leave the body undecoded.
pub fn pinned_fetch() -> PinnedFetch {
    Arc::new(|request: FetchRequest, addresses: Vec<IpAddr>| {
        async move {
            let url = url::Url::parse(&request.url).map_err(|e| e.to_string())?;
            let host = url.host_str().ok_or("missing host")?.to_string();
            let pinned: Vec<SocketAddr> = addresses
                .iter()
                .map(|ip| SocketAddr::new(*ip, 443))
                .collect();
            let client = reqwest::Client::builder()
                .no_proxy()
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .redirect(reqwest::redirect::Policy::none())
                .resolve_to_addrs(&host, &pinned)
                .build()
                .map_err(|e| e.to_string())?;
            let mut builder = client.get(url);
            for (name, value) in &request.headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            let response = builder.send().await.map_err(|e| e.to_string())?;
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let body = response
                .bytes_stream()
                .map(|c| c.map_err(|e| e.to_string()))
                .boxed();
            Ok(FetchResponse {
                status,
                headers,
                body,
            })
        }
        .boxed()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_addresses_reject_local_private_and_documentation_forms() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "192.0.2.9",
            "192.88.99.9",
            "198.51.100.9",
            "203.0.113.9",
            "::1",
            "::",
            "::ffff:127.0.0.1",
            "fc00::1",
            "fd00::1",
            "fe80::1",
            "64:ff9b::c0a8:101",
            "100::1",
            "2001:2::1",
            "2001:db8::1",
            "2002:c0a8:101::",
            "3fff::1",
            "",
            "1.2.3",
            "1.2.3.256",
            "1:2:3:4:5:6:7",
            "1::2::3",
        ] {
            assert!(!is_public_radio_address(address), "{address}");
        }
        assert!(is_public_radio_address("93.184.216.34"));
        assert!(is_public_radio_address(
            "2606:2800:220:1:248:1893:25c8:1946"
        ));
        assert!(is_public_radio_address(
            "[2606:2800:220:1:248:1893:25c8:1946]"
        ));
    }

    #[test]
    fn destinations_are_limited_to_mirror_endpoints() {
        assert_eq!(
            radio_mirror_origin(Some(&Value::String("DE1.api.radio-browser.info.".into())))
                .as_deref(),
            Some("https://de1.api.radio-browser.info")
        );
        assert_eq!(
            radio_mirror_origin(Some(&Value::String("evil.example".into()))),
            None
        );
        assert_eq!(radio_mirror_origin(None), None);
        for ok in [
            "https://all.api.radio-browser.info/json/servers",
            "https://de1.api.radio-browser.info/json/stations/search?limit=1",
            "https://de1.api.radio-browser.info/json/url/12345678-1234-4234-8234-123456789abc",
        ] {
            assert!(radio_proxy_destination(ok).is_some(), "{ok}");
        }
        for bad in [
            "http://de1.api.radio-browser.info/json/stations/search",
            "https://de1.api.radio-browser.info:8443/json/stations/search",
            "https://user@de1.api.radio-browser.info/json/stations/search",
            "https://de1.api.radio-browser.info/json/stations/search#x",
            "https://de1.api.radio-browser.info/json/servers",
            "https://all.api.radio-browser.info/json/servers?x=1",
            "https://de1.api.radio-browser.info/json/url/abc?x=1",
            "https://de1.api.radio-browser.info/not-an-allowed-path",
            "https://public.example.org/json/stations/search",
            "https://169.254.169.254/json/stations/search",
            "not a url",
        ] {
            assert!(radio_proxy_destination(bad).is_none(), "{bad}");
        }
    }

    #[tokio::test]
    async fn any_forbidden_resolution_fails_the_lookup() {
        let lookup_of = |rows: Vec<&'static str>| -> Lookup {
            Arc::new(move |_| {
                let rows: Vec<String> = rows.iter().map(|s| s.to_string()).collect();
                async move { Ok(rows) }.boxed()
            })
        };
        assert!(
            resolve_radio_proxy_addresses("x", &lookup_of(vec!["93.184.216.34"]))
                .await
                .is_ok()
        );
        assert!(
            resolve_radio_proxy_addresses("x", &lookup_of(vec!["93.184.216.34", "fd00::1"]))
                .await
                .is_err()
        );
        assert!(
            resolve_radio_proxy_addresses("x", &lookup_of(vec![]))
                .await
                .is_err()
        );
    }
}
