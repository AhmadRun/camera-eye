//! GBFS bike-share proxy (port of `server/providers/gbfs.js` and
//! `src/data/gbfsSource.js`).
//!
//! `GET /api/gbfs/<encodeURIComponent(upstream URL)>` relays one
//! allowlisted https `station_information.json` / `station_status.json`
//! document: no redirects followed, a 5 MB streamed body cap and a 12 s
//! deadline covering headers and body.

use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use serde_json::json;

use super::hazard_support::{
    FetchError, FetchRequest, Fetcher, Redirect, decode_uri_component, mount, read_text_capped,
    reqwest_fetcher, sub_url,
};
use crate::common::reply;
use crate::state::AppState;

/// Upstream fetch timeout for GBFS requests.
const GBFS_PROXY_TIMEOUT: Duration = Duration::from_millis(12_000);
/// `GBFS_MAX_BODY_BYTES`.
pub const GBFS_MAX_BODY_BYTES: usize = 5 * 1024 * 1024;

/// Allowlisted GBFS hostnames; any `*.publicbikesystem.net` is accepted too.
const GBFS_ALLOWED_HOSTS: [&str; 8] = [
    "gbfs.lyft.com",
    "gbfs.bluebikes.com",
    "gbfs.bcycle.com",
    "gbfs.biketownpdx.com",
    "gbfs.cogobikeshare.com",
    "austin.publicbikesystem.net",
    "hon.publicbikesystem.net",
    "chat.publicbikesystem.net",
];

/// `isAllowedGbfsHost`.
pub fn is_allowed_gbfs_host(hostname: &str) -> bool {
    let host = hostname.trim().to_lowercase();
    if host.is_empty() {
        return false;
    }
    GBFS_ALLOWED_HOSTS.contains(&host.as_str()) || host.ends_with(".publicbikesystem.net")
}

fn ends_with_ignore_case(text: &str, suffix: &str) -> bool {
    text.len() >= suffix.len()
        && text.is_char_boundary(text.len() - suffix.len())
        && text[text.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
}

/// `isAllowedGbfsPath`: only station_information / station_status.
pub fn is_allowed_gbfs_path(pathname: &str) -> bool {
    ends_with_ignore_case(pathname, "/station_information.json")
        || ends_with_ignore_case(pathname, "/station_status.json")
}

/// `gbfsCacheControl`: station_information is semi-static (5 min);
/// station_status is real-time.
pub fn gbfs_cache_control(pathname: &str) -> &'static str {
    if ends_with_ignore_case(pathname, "/station_information.json") {
        "public, max-age=300"
    } else {
        "no-store"
    }
}

/// What a relayed upstream answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GbfsUpstream {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

/// Why the relay failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GbfsError {
    /// `code: 'GBFS_REDIRECT'` with the redirect's host (or empty).
    Redirect(String),
    /// `code: 'RESPONSE_TOO_LARGE'`.
    TooLarge,
    /// An `AbortError` from the deadline.
    Timeout,
    Other(String),
}

fn gbfs_redirect_host(location: Option<&str>, request_url: &str) -> String {
    let Some(location) = location.filter(|l| !l.is_empty()) else {
        return String::new();
    };
    url::Url::parse(request_url)
        .and_then(|base| base.join(location))
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default()
}

/// `fetchGbfsUpstream`: one validated endpoint, redirects refused, the body
/// cap enforced while streaming, one deadline over headers and body.
pub async fn fetch_gbfs_upstream(
    fetch: &Fetcher,
    url: &str,
    timeout: Duration,
    max_bytes: usize,
) -> Result<GbfsUpstream, GbfsError> {
    let request = FetchRequest::get(url)
        .header("Accept", "application/json")
        .header("User-Agent", "camera-eye-gbfs-proxy/1.0")
        .redirect(Redirect::Manual);
    let work = async {
        let upstream = fetch(request).await.map_err(|e| match e {
            FetchError::Timeout => GbfsError::Timeout,
            other => GbfsError::Other(other.to_string()),
        })?;
        if (300..400).contains(&upstream.status) {
            return Err(GbfsError::Redirect(gbfs_redirect_host(
                upstream.header("location"),
                url,
            )));
        }
        let status = upstream.status;
        let content_type = upstream
            .header("content-type")
            .filter(|c| !c.is_empty())
            .unwrap_or("application/json")
            .to_string();
        let body = read_text_capped(upstream, max_bytes)
            .await
            .map_err(|e| match e {
                FetchError::TooLarge => GbfsError::TooLarge,
                FetchError::Timeout => GbfsError::Timeout,
                FetchError::Network(m) => GbfsError::Other(m),
            })?;
        Ok(GbfsUpstream {
            status,
            content_type,
            body,
        })
    };
    tokio::time::timeout(timeout, work)
        .await
        .unwrap_or(Err(GbfsError::Timeout))
}

pub fn router(_state: &AppState) -> Router<AppState> {
    router_with(reqwest_fetcher())
}

fn router_with(fetch: Fetcher) -> Router<AppState> {
    mount("/api/gbfs", move |request: Request| {
        let fetch = fetch.clone();
        async move { handle(&fetch, request).await }
    })
}

fn error(status: u16, message: &str) -> Response {
    reply::json(status, &json!({ "error": message }), Some("no-store"))
}

async fn handle(fetch: &Fetcher, request: Request) -> Response {
    if request.method() != http::Method::GET {
        return error(405, "Method Not Allowed");
    }
    let raw = sub_url(&request, "/api/gbfs");
    let pathname = url::Url::parse("http://localhost/")
        .and_then(|base| base.join(&raw))
        .map(|u| u.path().to_string())
        .unwrap_or_default();
    let encoded_target = pathname.trim_start_matches('/');
    if encoded_target.is_empty() {
        return error(400, "Missing GBFS upstream target");
    }
    let Some(decoded_target) = decode_uri_component(encoded_target) else {
        return error(400, "Invalid GBFS target encoding");
    };
    let Ok(upstream_url) = url::Url::parse(&decoded_target) else {
        return error(400, "Invalid GBFS upstream URL");
    };
    if upstream_url.scheme() != "https" {
        return error(400, "Only https GBFS targets are allowed");
    }
    let hostname = upstream_url.host_str().unwrap_or("").to_string();
    if !is_allowed_gbfs_host(&hostname) {
        return error(403, "GBFS host not allowed");
    }
    if !is_allowed_gbfs_path(upstream_url.path()) {
        return error(
            400,
            "Only station_information/station_status endpoints are allowed",
        );
    }
    match fetch_gbfs_upstream(
        fetch,
        upstream_url.as_str(),
        GBFS_PROXY_TIMEOUT,
        GBFS_MAX_BODY_BYTES,
    )
    .await
    {
        Ok(upstream) => {
            let mut response = reply::raw(
                upstream.status,
                &upstream.content_type,
                upstream.body,
                Some(gbfs_cache_control(upstream_url.path())),
            );
            response = reply::with_header(response, "X-GBFS-Upstream", &hostname);
            reply::with_header(response, "X-GBFS-Cache", "MISS")
        }
        Err(GbfsError::Timeout) => error(504, "GBFS upstream timeout"),
        Err(GbfsError::Redirect(host)) => {
            tracing::warn!("[GBFS Proxy] Redirect refused {host}");
            error(502, "GBFS upstream redirect refused")
        }
        Err(GbfsError::TooLarge) => error(502, "GBFS upstream response too large"),
        Err(GbfsError::Other(message)) => {
            tracing::error!("[GBFS Proxy] {message}");
            error(502, "GBFS proxy error")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use bytes::Bytes;
    use futures_util::FutureExt;
    use serde_json::Value;

    use super::super::hazard_support::testing::{self, Calls};
    use super::super::hazard_support::{FetchResponse, encode_uri_component};
    use super::*;

    const STATION_URL: &str = "https://gbfs.lyft.com/gbfs/2.3/bay/en/station_status.json";
    const STATION_BODY: &str = r#"{"data":{"stations":[]}}"#;

    async fn fetch_with(fetch: Fetcher, max_bytes: usize) -> Result<GbfsUpstream, GbfsError> {
        fetch_gbfs_upstream(&fetch, STATION_URL, GBFS_PROXY_TIMEOUT, max_bytes).await
    }

    #[test]
    fn allowlists_match_the_client() {
        assert!(is_allowed_gbfs_host("GBFS.lyft.com "));
        assert!(is_allowed_gbfs_host("toronto.publicbikesystem.net"));
        assert!(!is_allowed_gbfs_host("publicbikesystem.net"));
        assert!(!is_allowed_gbfs_host("example.com"));
        assert!(!is_allowed_gbfs_host(""));
        assert!(is_allowed_gbfs_path("/x/Station_Status.JSON"));
        assert!(is_allowed_gbfs_path("/station_information.json"));
        assert!(!is_allowed_gbfs_path("/gbfs.json"));
        assert_eq!(
            gbfs_cache_control("/a/station_information.json"),
            "public, max-age=300"
        );
        assert_eq!(gbfs_cache_control("/a/station_status.json"), "no-store");
    }

    #[tokio::test]
    async fn a_direct_200_passes_through_unchanged() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |_| {
            Ok(FetchResponse::from_bytes(
                200,
                &[("content-type", "application/json; charset=utf-8")],
                STATION_BODY,
            ))
        });
        let result = fetch_with(fetch, GBFS_MAX_BODY_BYTES).await.unwrap();
        assert_eq!(calls.lock()[0].redirect, Redirect::Manual);
        assert_eq!(
            result,
            GbfsUpstream {
                status: 200,
                content_type: "application/json; charset=utf-8".into(),
                body: STATION_BODY.into()
            }
        );
    }

    #[tokio::test]
    async fn a_non_redirect_error_status_is_relayed() {
        let fetch = testing::fetcher(Default::default(), |_| {
            Ok(FetchResponse::from_bytes(404, &[], ""))
        });
        let result = fetch_with(fetch, GBFS_MAX_BODY_BYTES).await.unwrap();
        assert_eq!(result.status, 404);
        assert_eq!(result.content_type, "application/json");
        assert_eq!(result.body, "");
    }

    #[tokio::test]
    async fn every_redirect_status_is_refused_without_fetching_the_target() {
        for status in [301u16, 302, 303, 307, 308] {
            let calls: Calls = Default::default();
            let fetch = testing::fetcher(Arc::clone(&calls), move |_| {
                Ok(FetchResponse::from_bytes(
                    status,
                    &[("location", "https://attacker.example/station_status.json")],
                    "moved",
                ))
            });
            assert_eq!(
                fetch_with(fetch, GBFS_MAX_BODY_BYTES).await,
                Err(GbfsError::Redirect("attacker.example".into()))
            );
            assert_eq!(calls.lock().len(), 1);
        }
        let fetch = testing::fetcher(Default::default(), |_| {
            Ok(FetchResponse::from_bytes(
                302,
                &[("location", "/v2/station_status.json")],
                "",
            ))
        });
        assert_eq!(
            fetch_with(fetch, GBFS_MAX_BODY_BYTES).await,
            Err(GbfsError::Redirect("gbfs.lyft.com".into()))
        );
    }

    #[tokio::test]
    async fn oversized_declared_and_streamed_bodies_are_cancelled() {
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_stream(
                200,
                &[("content-length", &(GBFS_MAX_BODY_BYTES + 1).to_string())],
                testing::endless(Bytes::from(vec![0u8; 1024]), Arc::clone(&flag)),
            ))
        });
        assert_eq!(
            fetch_with(fetch, GBFS_MAX_BODY_BYTES).await,
            Err(GbfsError::TooLarge)
        );
        assert!(dropped.load(Ordering::SeqCst));
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_stream(
                200,
                &[],
                testing::endless(Bytes::from(vec![0u8; 1024]), Arc::clone(&flag)),
            ))
        });
        assert_eq!(fetch_with(fetch, 4 * 1024).await, Err(GbfsError::TooLarge));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn the_cap_is_measured_in_bytes_and_a_body_exactly_at_it_passes() {
        let body = "é".repeat(64);
        let answer = body.clone();
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_bytes(200, &[], answer.clone()))
        });
        assert_eq!(fetch_with(fetch, body.len()).await.unwrap().body, body);
        assert_eq!(GBFS_MAX_BODY_BYTES, 5 * 1024 * 1024);
    }

    #[tokio::test]
    async fn the_deadline_covers_stalled_connections_and_stalled_bodies() {
        let stalled: Fetcher = Arc::new(|_| futures_util::future::pending().boxed());
        let started = std::time::Instant::now();
        assert_eq!(
            fetch_gbfs_upstream(
                &stalled,
                STATION_URL,
                Duration::from_millis(20),
                GBFS_MAX_BODY_BYTES
            )
            .await,
            Err(GbfsError::Timeout)
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetch = testing::fetcher(Default::default(), move |_| {
            Ok(FetchResponse::from_stream(
                200,
                &[],
                testing::stalled(Arc::clone(&flag)),
            ))
        });
        assert_eq!(
            fetch_gbfs_upstream(
                &fetch,
                STATION_URL,
                Duration::from_millis(20),
                GBFS_MAX_BODY_BYTES
            )
            .await,
            Err(GbfsError::Timeout)
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    fn target(path: &str) -> String {
        format!(
            "/api/gbfs/{}",
            encode_uri_component(&format!("https://gbfs.lyft.com/{path}"))
        )
    }

    #[tokio::test]
    async fn the_route_keeps_host_path_method_guards_caps_and_cache_headers() {
        let calls: Calls = Default::default();
        let fetch = testing::fetcher(Arc::clone(&calls), |request| {
            assert_eq!(
                url::Url::parse(&request.url).unwrap().host_str(),
                Some("gbfs.lyft.com")
            );
            Ok(FetchResponse::json(&json!({ "data": { "stations": [] } })))
        });
        let router = router_with(fetch);
        let state = testing::state();
        let send = |method: &'static str, uri: String| {
            let (router, state) = (&router, &state);
            async move { testing::send(router, state, method, &uri, "127.0.0.1").await }
        };
        let error_of =
            |body: &Bytes| serde_json::from_slice::<Value>(body).unwrap()["error"].clone();
        let post = send("POST", target("station_status.json")).await;
        assert_eq!(post.0, 405);
        assert_eq!(error_of(&post.2), json!("Method Not Allowed"));
        let foreign = send(
            "GET",
            format!(
                "/api/gbfs/{}",
                encode_uri_component("https://example.com/station_status.json")
            ),
        )
        .await;
        assert_eq!(foreign.0, 403);
        assert_eq!(error_of(&foreign.2), json!("GBFS host not allowed"));
        assert_eq!(send("GET", target("gbfs.json")).await.0, 400);
        let bad = send("GET", "/api/gbfs/%zz".into()).await;
        assert_eq!(bad.0, 400);
        assert_eq!(error_of(&bad.2), json!("Invalid GBFS target encoding"));
        let query = send(
            "GET",
            format!("/api/gbfs?url={}", encode_uri_component(STATION_URL)),
        )
        .await;
        assert_eq!(query.0, 400);
        assert_eq!(error_of(&query.2), json!("Missing GBFS upstream target"));
        let http = send(
            "GET",
            format!(
                "/api/gbfs/{}",
                encode_uri_component("http://gbfs.lyft.com/station_status.json")
            ),
        )
        .await;
        assert_eq!(
            error_of(&http.2),
            json!("Only https GBFS targets are allowed")
        );
        assert_eq!(
            error_of(&send("GET", "/api/gbfs/not-a-url".into()).await.2),
            json!("Invalid GBFS upstream URL")
        );
        assert_eq!(calls.lock().len(), 0);
        let info = send("GET", target("station_information.json")).await;
        assert_eq!(info.0, 200);
        assert_eq!(info.1["cache-control"], "public, max-age=300");
        assert_eq!(info.1["x-gbfs-upstream"], "gbfs.lyft.com");
        assert_eq!(info.1["x-gbfs-cache"], "MISS");
        assert_eq!(info.1["content-type"], "application/json");
        let status = send("GET", target("station_status.json")).await;
        assert_eq!(status.1["cache-control"], "no-store");
        assert_eq!(status.2, Bytes::from_static(br#"{"data":{"stations":[]}}"#));
    }

    #[tokio::test]
    async fn the_route_refuses_redirects_and_oversized_bodies() {
        for (response, expected) in [
            (FetchResponse::from_bytes(302, &[], ""), 502u16),
            (
                FetchResponse::from_bytes(
                    200,
                    &[("content-length", &(GBFS_MAX_BODY_BYTES + 1).to_string())],
                    "x",
                ),
                502,
            ),
            (FetchResponse::from_bytes(200, &[], STATION_BODY), 200),
        ] {
            let slot = Arc::new(parking_lot::Mutex::new(Some(response)));
            let fetch: Fetcher = Arc::new(move |_| {
                let response = slot.lock().take().unwrap();
                async move { Ok(response) }.boxed()
            });
            let router = router_with(fetch);
            let (status, _, body) = testing::send(
                &router,
                &testing::state(),
                "GET",
                &format!("/api/gbfs/{}", encode_uri_component(STATION_URL)),
                "127.0.0.1",
            )
            .await;
            assert_eq!(status, expected);
            if status == 200 {
                assert_eq!(body, STATION_BODY);
            } else {
                assert!(serde_json::from_slice::<Value>(&body).unwrap()["error"].is_string());
            }
        }
    }
}
