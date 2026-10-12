//! Same-site gate for cost-bearing and log endpoints (port of
//! `src/localRequestGate.mjs` and `server/providers/common/same-site.js`).

use axum::response::Response;
use http::HeaderMap;

use super::reply;

/// Reverse-proxy / CDN forwarding headers.
pub const PROXY_SIGNALS: [&str; 9] = [
    "forwarded",
    "via",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-forwarded-proto",
    "x-real-ip",
    "cf-connecting-ip",
    "cf-ray",
];

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Whether a request carries reverse-proxy or CDN forwarding headers.
pub fn has_proxy_signals(headers: &HeaderMap) -> bool {
    PROXY_SIGNALS.iter().any(|name| {
        headers
            .get_all(*name)
            .iter()
            .any(|v| !String::from_utf8_lossy(v.as_bytes()).trim().is_empty())
    })
}

fn request_authority(host_header: Option<&str>, protocol: &str) -> Option<String> {
    let raw = host_header?.trim().to_lowercase();
    let scheme = protocol.to_lowercase();
    if raw.is_empty()
        || !(scheme == "http:" || scheme == "https:")
        || raw.chars().any(|c| c.is_whitespace() || "/?#@".contains(c))
    {
        return None;
    }
    let url = url::Url::parse(&format!("{scheme}//{raw}")).ok()?;
    Some(url.origin().ascii_serialization())
}

/// Pure decision: `Ok(())` to admit, `Err((status, message))` to refuse.
pub fn admit_same_site_request(
    host_header: Option<&str>,
    protocol: &str,
    origin: Option<&str>,
    sec_fetch_site: Option<&str>,
    headers: &HeaderMap,
) -> Result<(), (u16, &'static str)> {
    if has_proxy_signals(headers) {
        return Err((403, "Proxied requests are not accepted"));
    }
    let site = sec_fetch_site.unwrap_or("").trim().to_lowercase();
    if !site.is_empty() && site != "same-origin" && site != "none" {
        return Err((403, "Cross-site requests are not accepted"));
    }
    if let Some(origin) = origin.filter(|o| !o.is_empty()) {
        if origin == "null" {
            return Err((403, "Opaque origins are not accepted"));
        }
        let authority = request_authority(host_header, protocol);
        let Ok(parsed) = url::Url::parse(origin) else {
            return Err((403, "Unrecognized Origin refused"));
        };
        let exact = parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.path() == "/"
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && Some(parsed.origin().ascii_serialization()) == authority;
        if !exact {
            return Err((403, "Cross-origin requests are not accepted"));
        }
    }
    Ok(())
}

/// Gate a request: `Some(403 response)` when refused, `None` to continue.
pub fn admit_same_site(headers: &HeaderMap) -> Option<Response> {
    match admit_same_site_request(
        header(headers, "host"),
        "http:",
        header(headers, "origin"),
        header(headers, "sec-fetch-site"),
        headers,
    ) {
        Ok(()) => None,
        Err((status, message)) => Some(reply::error(status, message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(*k, v.parse().unwrap());
        }
        map
    }

    #[test]
    fn refuses_cross_site_shapes() {
        let h = headers(&[]);
        assert!(admit_same_site_request(Some("localhost:4173"), "http:", None, None, &h).is_ok());
        assert!(
            admit_same_site_request(
                Some("localhost:4173"),
                "http:",
                Some("http://localhost:4173"),
                None,
                &h
            )
            .is_ok()
        );
        assert_eq!(
            admit_same_site_request(
                Some("localhost:4173"),
                "http:",
                Some("http://evil.test"),
                None,
                &h
            ),
            Err((403, "Cross-origin requests are not accepted"))
        );
        assert_eq!(
            admit_same_site_request(Some("localhost:4173"), "http:", Some("null"), None, &h),
            Err((403, "Opaque origins are not accepted"))
        );
        assert_eq!(
            admit_same_site_request(
                Some("localhost:4173"),
                "http:",
                None,
                Some("cross-site"),
                &h
            ),
            Err((403, "Cross-site requests are not accepted"))
        );
        let proxied = headers(&[("x-forwarded-for", "1.2.3.4")]);
        assert_eq!(
            admit_same_site_request(Some("localhost"), "http:", None, None, &proxied),
            Err((403, "Proxied requests are not accepted"))
        );
    }
}
