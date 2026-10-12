//! Port of `server/providers/cctv/media.js`: the synthetic placeholder, the
//! live media relay, and the bounded frame fetches.

use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use base64::Engine;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderValue, StatusCode};

use super::constants::{
    CCTV_FRAME_FETCH_TIMEOUT_MS, CCTV_FRAME_MAX_BODY_BYTES, CCTV_MEDIA_FETCH_TIMEOUT_MS,
    CCTV_MEDIA_IDLE_TIMEOUT_MS, CCTV_MEDIA_MAX_BODY_BYTES, CCTV_USER_AGENT, NSW_IMAGE_HOST,
    NSW_IMAGE_USER_AGENT,
};
use super::fetch::{FetchRequest, FetchResponse, Fetcher, Redirect};
use super::normalize::{escape_xml, hash_seed};
use crate::common::reply;

/// `buildSyntheticCctvSvg`: a 960x540 placeholder billboard.
pub fn build_synthetic_cctv_svg(camera_id: &str, label: &str, city: &str, status: &str) -> String {
    let seed = hash_seed(&format!("{camera_id}:{label}:{city}"));
    let hue = seed % 360;
    let hue2 = (hue + 46) % 360;
    let iso = super::jsval::iso_string(crate::common::js::now_ms());
    let ts: String = iso.replace('T', " ").chars().take(20).collect();
    let safe_label = escape_xml(label);
    let safe_city = escape_xml(if city.is_empty() { "GLOBAL GRID" } else { city });
    let safe_id = escape_xml(camera_id);
    let safe_status = escape_xml(if status.is_empty() {
        "SYNTHETIC"
    } else {
        status
    });
    let safe_ts = escape_xml(&ts);
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="960" height="540" viewBox="0 0 960 540">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="hsl({hue}, 35%, 10%)" />
      <stop offset="60%" stop-color="hsl({hue2}, 42%, 6%)" />
      <stop offset="100%" stop-color="#020509" />
    </linearGradient>
    <radialGradient id="flare" cx="0.22" cy="0.24" r="0.78">
      <stop offset="0%" stop-color="hsla({hue2}, 100%, 65%, 0.35)" />
      <stop offset="100%" stop-color="hsla({hue2}, 100%, 40%, 0)" />
    </radialGradient>
    <pattern id="scan" width="8" height="8" patternUnits="userSpaceOnUse">
      <rect width="8" height="8" fill="transparent" />
      <rect y="0" width="8" height="1" fill="rgba(255,255,255,0.08)" />
      <rect y="4" width="8" height="1" fill="rgba(255,255,255,0.05)" />
    </pattern>
  </defs>
  <rect width="960" height="540" fill="url(#bg)" />
  <rect width="960" height="540" fill="url(#flare)" />
  <rect width="960" height="540" fill="url(#scan)" />
  <g stroke="rgba(123,233,255,0.25)" stroke-width="1" fill="none">
    <path d="M60 460 Q300 300 520 420 T900 320" />
    <path d="M100 160 Q340 40 620 130 T920 90" />
    <path d="M20 280 Q220 230 390 270 T760 250" />
  </g>
  <g fill="none" stroke="rgba(180,248,255,0.2)" stroke-width="1">
    <rect x="70" y="80" width="820" height="380" rx="8" />
    <line x1="70" y1="270" x2="890" y2="270" />
    <line x1="480" y1="80" x2="480" y2="460" />
  </g>
  <g fill="#9cefff" font-family="JetBrains Mono, monospace" text-transform="uppercase">
    <text x="74" y="54" font-size="16" letter-spacing="2">CCTV FEED PLACEHOLDER</text>
    <text x="74" y="512" font-size="14" letter-spacing="1.5">{safe_label} · {safe_city}</text>
    <text x="646" y="512" font-size="13" letter-spacing="1.2">{safe_id}</text>
    <text x="704" y="54" font-size="15" letter-spacing="2">{safe_ts}</text>
    <text x="74" y="486" font-size="13" letter-spacing="1.3">{safe_status}</text>
  </g>
</svg>"##
    )
}

/// `proxyMediaResponse`: relay an upstream media answer, refusing a declared
/// oversize body, and releasing the upstream when the viewer goes away or
/// the upstream goes silent for `idle` between chunks.
pub fn proxy_media_response(
    upstream: FetchResponse,
    source_header: &str,
    idle: Duration,
) -> Response {
    let content_type = upstream
        .header("content-type")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "application/octet-stream".into());
    let cache_control = upstream
        .header("cache-control")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "no-store".into());
    let content_length = upstream.header("content-length").filter(|v| !v.is_empty());
    let content_range = upstream.header("content-range").filter(|v| !v.is_empty());
    let accept_ranges = upstream.header("accept-ranges").filter(|v| !v.is_empty());

    let declared = upstream.declared_length();
    if declared.is_finite() && declared > CCTV_MEDIA_MAX_BODY_BYTES as f64 {
        return reply::json(
            502,
            &serde_json::json!({ "error": "Upstream media exceeds size cap" }),
            Some("no-store"),
        );
    }

    // Each wait for the next chunk is bounded; the wait only runs while the
    // client is pulling, so a slow viewer is never mistaken for a dead camera.
    // Dropping the response body (viewer gone) drops the upstream stream.
    let body = futures_util::stream::unfold(Some(upstream.body), move |state| async move {
        let mut body = state?;
        match tokio::time::timeout(idle, body.next()).await {
            Ok(Some(Ok(chunk))) => Some((Ok::<Bytes, std::io::Error>(chunk), Some(body))),
            _ => None,
        }
    });
    let mut response = Response::new(Body::from_stream(body));
    *response.status_mut() =
        StatusCode::from_u16(upstream.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let headers = response.headers_mut();
    let mut set = |name: &'static str, value: &str| {
        if let Ok(value) = HeaderValue::from_str(value) {
            headers.insert(name, value);
        }
    };
    set("content-type", &content_type);
    set("cache-control", &cache_control);
    set("x-cctv-source", source_header);
    if let Some(v) = &content_length {
        set("content-length", v);
    }
    if let Some(v) = &content_range {
        set("content-range", v);
    }
    if let Some(v) = &accept_ranges {
        set("accept-ranges", v);
    }
    response
}

/// Production idle deadline for [`proxy_media_response`].
pub fn media_idle_timeout() -> Duration {
    Duration::from_millis(CCTV_MEDIA_IDLE_TIMEOUT_MS)
}

/// Why a media fetch failed.
#[derive(Debug, PartialEq, Eq)]
pub enum MediaFetchError {
    Timeout,
    Failed,
}

/// `fetchCctvMediaUpstream`: open registered media within a header
/// deadline; a timely live body keeps running. Dropping the returned future
/// (the viewer left) cancels the request.
pub async fn fetch_cctv_media_upstream(
    fetch: &Fetcher,
    url: &str,
    headers: Vec<(String, String)>,
    timeout: Duration,
) -> Result<FetchResponse, MediaFetchError> {
    let mut request = FetchRequest::new(url, Redirect::Follow, false);
    request.headers = headers;
    match tokio::time::timeout(timeout, fetch(request)).await {
        Err(_) => Err(MediaFetchError::Timeout),
        Ok(Err(_)) => Err(MediaFetchError::Failed),
        Ok(Ok(response)) => Ok(response),
    }
}

/// Production header deadline.
pub fn media_fetch_timeout() -> Duration {
    Duration::from_millis(CCTV_MEDIA_FETCH_TIMEOUT_MS)
}

/// A fetched frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub body: Bytes,
    pub content_type: String,
}

/// Options for the frame fetches (injectable for tests, like the JS).
#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    pub timeout: Duration,
    pub max_bytes: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(CCTV_FRAME_FETCH_TIMEOUT_MS),
            max_bytes: CCTV_FRAME_MAX_BODY_BYTES,
        }
    }
}

fn is_canonical_base64(s: &str) -> bool {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(4) {
        return false;
    }
    let alphabet = |c: u8| c.is_ascii_alphanumeric() || c == b'+' || c == b'/';
    let groups = b.len() / 4;
    for (g, chunk) in b.chunks(4).enumerate() {
        let last = g + 1 == groups;
        let ok = if !last {
            chunk.iter().all(|c| alphabet(*c))
        } else {
            chunk.iter().all(|c| alphabet(*c))
                || (alphabet(chunk[0])
                    && alphabet(chunk[1])
                    && chunk[2] == b'='
                    && chunk[3] == b'=')
                || (alphabet(chunk[0])
                    && alphabet(chunk[1])
                    && alphabet(chunk[2])
                    && chunk[3] == b'=')
        };
        if !ok {
            return false;
        }
    }
    true
}

/// `fetchTxdotSnapshot`: decode TxDOT's JSON `{snippet: <base64 JPEG>}`,
/// only from the official origin and path, never following a redirect.
pub async fn fetch_txdot_snapshot(
    fetch: &Fetcher,
    url: Option<&str>,
    limits: FrameLimits,
) -> Option<Frame> {
    let url = url.filter(|u| !u.is_empty())?;
    let parsed = url::Url::parse(url).ok()?;
    if parsed.origin().ascii_serialization() != "https://its.txdot.gov"
        || parsed.path() != "/its/DistrictIts/GetCctvSnapshotByIcdId"
    {
        return None;
    }
    let max_envelope = (limits.max_bytes * 4).div_ceil(3) + 4096;
    let request = FetchRequest::new(parsed.as_str(), Redirect::Manual, false)
        .header("Accept", "application/json")
        .header("User-Agent", CCTV_USER_AGENT);
    let work = async {
        let upstream = fetch(request).await.ok()?;
        if !upstream.ok() {
            return None;
        }
        let envelope = upstream.bytes_capped(max_envelope).await.ok()?;
        let payload: serde_json::Value = serde_json::from_slice(&envelope).ok()?;
        let snippet = match payload.get("snippet") {
            Some(serde_json::Value::String(s)) => super::jsval::trim(s).to_string(),
            _ => String::new(),
        };
        if snippet.is_empty() {
            return None;
        }
        let snippet = match snippet.get(..23) {
            Some(prefix) if prefix.eq_ignore_ascii_case("data:image/jpeg;base64,") => {
                snippet[23..].to_string()
            }
            _ => snippet,
        };
        if snippet.len() > max_envelope || !is_canonical_base64(&snippet) {
            return None;
        }
        let body = base64::engine::general_purpose::STANDARD
            .decode(snippet.as_bytes())
            .ok()?;
        if body.len() < 4 || body.len() > limits.max_bytes {
            return None;
        }
        if body[0] != 0xff || body[1] != 0xd8 || body[2] != 0xff {
            return None;
        }
        Some(Frame {
            body: Bytes::from(body),
            content_type: "image/jpeg".into(),
        })
    };
    tokio::time::timeout(limits.timeout, work)
        .await
        .ok()
        .flatten()
}

/// Redirect hops the frame path follows, and only within the same origin.
const MAX_SAME_HOST_REDIRECTS: usize = 2;

/// `fetchWithinHost`: follow redirects only within the original origin.
pub async fn fetch_within_host(
    fetch: &Fetcher,
    url: &str,
    headers: Vec<(String, String)>,
) -> Option<FetchResponse> {
    let mut current = url::Url::parse(url).ok()?;
    let origin = current.origin();
    for hop in 0..=MAX_SAME_HOST_REDIRECTS {
        let mut request = FetchRequest::new(current.as_str(), Redirect::Manual, false);
        request.headers = headers.clone();
        let upstream = fetch(request).await.ok()?;
        if !(300..400).contains(&upstream.status) {
            return Some(upstream);
        }
        let location = upstream.header("location");
        drop(upstream);
        let location = location.filter(|l| !l.is_empty())?;
        if hop == MAX_SAME_HOST_REDIRECTS {
            return None;
        }
        let next = current.join(&location).ok()?;
        if next.origin() != origin {
            return None;
        }
        current = next;
    }
    None
}

/// `cctvUpstreamUserAgent`: a browser User-Agent for the NSW image host
/// only (by exact hostname), the proxy's own everywhere else.
pub fn cctv_upstream_user_agent(url: &str) -> &'static str {
    match url::Url::parse(url) {
        Ok(parsed) if parsed.host_str() == Some(NSW_IMAGE_HOST) => NSW_IMAGE_USER_AGENT,
        _ => CCTV_USER_AGENT,
    }
}

/// `fetchCctvImageFromUpstream`: one registered frame within the refresh
/// budget and byte cap; any miss is `None`.
pub async fn fetch_cctv_image_from_upstream(
    fetch: &Fetcher,
    url: Option<&str>,
    limits: FrameLimits,
) -> Option<Frame> {
    let url = url.filter(|u| {
        u.get(..7)
            .is_some_and(|p| p.eq_ignore_ascii_case("http://"))
            || u.get(..8)
                .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
    })?;
    let work = async {
        let headers = vec![(
            "User-Agent".to_string(),
            cctv_upstream_user_agent(url).to_string(),
        )];
        let upstream = fetch_within_host(fetch, url, headers).await?;
        let content_type = upstream.header("content-type").unwrap_or_default();
        if !upstream.ok() || !content_type.starts_with("image/") {
            return None;
        }
        let body = upstream.bytes_capped(limits.max_bytes).await.ok()?;
        Some(Frame { body, content_type })
    };
    tokio::time::timeout(limits.timeout, work)
        .await
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn fake<F>(respond: F) -> (Fetcher, Arc<Mutex<Vec<FetchRequest>>>)
    where
        F: Fn(&FetchRequest) -> FetchResponse + Send + Sync + 'static,
    {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let fetcher: Fetcher = Arc::new(move |request: FetchRequest| {
            let response = respond(&request);
            log.lock().push(request);
            async move { Ok(response) }.boxed()
        });
        (fetcher, seen)
    }

    fn limits(timeout_ms: u64, max_bytes: usize) -> FrameLimits {
        FrameLimits {
            timeout: Duration::from_millis(timeout_ms),
            max_bytes,
        }
    }

    /// A body of `count` chunks of `size` bytes, counting pulls and noting cancellation.
    fn chunked(
        size: usize,
        count: usize,
        pulled: Arc<AtomicUsize>,
        cancelled: Arc<AtomicBool>,
    ) -> futures_util::stream::BoxStream<'static, Result<Bytes, String>> {
        struct Guard(Arc<AtomicBool>, Arc<AtomicUsize>, usize);
        impl Drop for Guard {
            fn drop(&mut self) {
                if self.1.load(Ordering::SeqCst) < self.2 {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
        }
        let guard = Guard(cancelled, Arc::clone(&pulled), count);
        futures_util::stream::unfold((0usize, guard), move |(i, guard)| {
            let pulled = Arc::clone(&pulled);
            async move {
                if i >= count {
                    return None;
                }
                pulled.fetch_add(1, Ordering::SeqCst);
                Some((Ok(Bytes::from(vec![0u8; size])), (i + 1, guard)))
            }
        })
        .boxed()
    }

    #[tokio::test]
    async fn frame_fetch_is_bounded_by_its_deadline() {
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetcher: Fetcher = Arc::new(move |_r| {
            struct OnDrop(Arc<AtomicBool>);
            impl Drop for OnDrop {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let guard = OnDrop(Arc::clone(&flag));
            async move {
                let _guard = guard;
                futures_util::future::pending::<()>().await;
                Err("never".to_string())
            }
            .boxed()
        });
        let started = std::time::Instant::now();
        let result = fetch_cctv_image_from_upstream(
            &fetcher,
            Some("https://example.com/frame.jpg"),
            limits(20, 1024),
        )
        .await;
        assert_eq!(result, None);
        assert!(
            dropped.load(Ordering::SeqCst),
            "the pending request is cancelled"
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        const { assert!(CCTV_FRAME_FETCH_TIMEOUT_MS < 10_000) };
    }

    #[tokio::test]
    async fn frame_fetch_returns_a_valid_image() {
        let (fetcher, _) =
            fake(|_| FetchResponse::new(200, &[("content-type", "image/jpeg")], vec![1u8, 2, 3]));
        let frame = fetch_cctv_image_from_upstream(
            &fetcher,
            Some("https://example.com/frame.jpg"),
            limits(100, 1024),
        )
        .await
        .unwrap();
        assert_eq!(frame.content_type, "image/jpeg");
        assert_eq!(&frame.body[..], &[1, 2, 3]);
    }

    #[tokio::test]
    async fn frame_fetch_caps_declared_and_streamed_bodies() {
        let chunk = 64 * 1024;
        // Declared oversize: refused without reading.
        let pulled = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let (p, c) = (Arc::clone(&pulled), Arc::clone(&cancelled));
        let declared = (chunk * 64).to_string();
        let fetcher: Fetcher = Arc::new(move |_r| {
            let body = chunked(chunk, 64, Arc::clone(&p), Arc::clone(&c));
            let response = FetchResponse::streaming(
                200,
                &[
                    ("content-type", "image/jpeg"),
                    ("content-length", &declared),
                ],
                body,
            );
            async move { Ok(response) }.boxed()
        });
        let result = fetch_cctv_image_from_upstream(
            &fetcher,
            Some("https://example.com/f.jpg"),
            limits(1000, chunk * 4),
        )
        .await;
        assert_eq!(result, None);
        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(pulled.load(Ordering::SeqCst), 0);

        // Undeclared: stops at the cap.
        let pulled = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let (p, c) = (Arc::clone(&pulled), Arc::clone(&cancelled));
        let fetcher: Fetcher = Arc::new(move |_r| {
            let body = chunked(chunk, 64, Arc::clone(&p), Arc::clone(&c));
            let response = FetchResponse::streaming(200, &[("content-type", "image/jpeg")], body);
            async move { Ok(response) }.boxed()
        });
        let result = fetch_cctv_image_from_upstream(
            &fetcher,
            Some("https://example.com/f.jpg"),
            limits(1000, chunk * 4),
        )
        .await;
        assert_eq!(result, None);
        assert!(pulled.load(Ordering::SeqCst) <= 6);
        assert!(cancelled.load(Ordering::SeqCst));

        // Garbage Content-Length: still capped by counting.
        let fetcher: Fetcher = Arc::new(move |_r| {
            let body = chunked(
                1024,
                8,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicBool::new(false)),
            );
            let response = FetchResponse::streaming(
                200,
                &[
                    ("content-type", "image/jpeg"),
                    ("content-length", "not-a-number"),
                ],
                body,
            );
            async move { Ok(response) }.boxed()
        });
        assert_eq!(
            fetch_cctv_image_from_upstream(
                &fetcher,
                Some("https://example.com/f.jpg"),
                limits(1000, 2048)
            )
            .await,
            None
        );

        // Under and exactly at the cap: reassembled in order.
        for (size, count, cap) in [(1024, 3, 8 * 1024), (512, 4, 2048)] {
            let fetcher: Fetcher = Arc::new(move |_r| {
                let body = chunked(
                    size,
                    count,
                    Arc::new(AtomicUsize::new(0)),
                    Arc::new(AtomicBool::new(false)),
                );
                let response =
                    FetchResponse::streaming(200, &[("content-type", "image/jpeg")], body);
                async move { Ok(response) }.boxed()
            });
            let frame = fetch_cctv_image_from_upstream(
                &fetcher,
                Some("https://example.com/f.jpg"),
                limits(1000, cap),
            )
            .await
            .unwrap();
            assert_eq!(frame.body.len(), size * count);
        }
        assert!(CCTV_FRAME_MAX_BODY_BYTES as u64 <= CCTV_MEDIA_MAX_BODY_BYTES);
    }

    #[tokio::test]
    async fn the_snapshot_deadline_covers_a_stalled_body() {
        let fetcher: Fetcher = Arc::new(|_r| {
            let body = futures_util::stream::pending().boxed();
            async move {
                Ok(FetchResponse::streaming(
                    200,
                    &[("content-type", "image/jpeg")],
                    body,
                ))
            }
            .boxed()
        });
        assert_eq!(
            fetch_cctv_image_from_upstream(
                &fetcher,
                Some("https://example.com/f.jpg"),
                limits(20, 1024)
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn rejected_snapshot_responses_are_misses() {
        for (status, content_type) in [(503, "image/jpeg"), (200, "text/html")] {
            let (fetcher, _) = fake(move |_| {
                FetchResponse::new(status, &[("content-type", content_type)], "rejected")
            });
            assert_eq!(
                fetch_cctv_image_from_upstream(
                    &fetcher,
                    Some("https://example.com/f.jpg"),
                    FrameLimits::default()
                )
                .await,
                None
            );
        }
        let (fetcher, seen) =
            fake(|_| FetchResponse::new(200, &[("content-type", "image/jpeg")], "x"));
        assert_eq!(
            fetch_cctv_image_from_upstream(
                &fetcher,
                Some("ftp://example.com/f.jpg"),
                FrameLimits::default()
            )
            .await,
            None
        );
        assert_eq!(
            fetch_cctv_image_from_upstream(&fetcher, None, FrameLimits::default()).await,
            None
        );
        assert!(seen.lock().is_empty());
    }

    #[tokio::test]
    async fn media_fetch_header_deadline() {
        let fetcher: Fetcher = Arc::new(|_r| {
            async { futures_util::future::pending::<Result<FetchResponse, String>>().await }.boxed()
        });
        let started = std::time::Instant::now();
        let result = fetch_cctv_media_upstream(
            &fetcher,
            "https://example.com/s.m3u8",
            vec![],
            Duration::from_millis(25),
        )
        .await;
        assert_eq!(result.err(), Some(MediaFetchError::Timeout));
        assert!(started.elapsed() < Duration::from_secs(2));
        const { assert!(CCTV_MEDIA_FETCH_TIMEOUT_MS >= 10_000) };

        let (fetcher, _) = fake(|_| FetchResponse::new(200, &[("content-type", "video/mp2t")], ""));
        let upstream = fetch_cctv_media_upstream(
            &fetcher,
            "https://example.com/s.m3u8",
            vec![],
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        assert!(upstream.ok());
    }

    #[test]
    fn the_browser_user_agent_applies_to_the_nsw_host_only() {
        assert_eq!(
            cctv_upstream_user_agent(
                "https://webcams.transport.nsw.gov.au/livetraffic-webcams/cameras/x.jpeg"
            ),
            NSW_IMAGE_USER_AGENT
        );
        assert_eq!(
            cctv_upstream_user_agent("https://webcams.transport.nsw.gov.au.evil.test/x.jpeg"),
            CCTV_USER_AGENT
        );
        assert_eq!(
            cctv_upstream_user_agent("https://example.test/webcams.transport.nsw.gov.au/x.jpeg"),
            CCTV_USER_AGENT
        );
        assert_eq!(cctv_upstream_user_agent("not a url"), CCTV_USER_AGENT);
    }

    #[tokio::test]
    async fn the_frame_path_follows_redirects_within_the_host_only() {
        let start = "https://cctv.austinmobility.io/image/1.jpg";
        let jpeg = || {
            FetchResponse::new(
                200,
                &[("content-type", "image/jpeg")],
                vec![0xffu8, 0xd8, 0xff],
            )
        };
        let redirect = |to: &str| FetchResponse::new(302, &[("location", to)], "");

        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let (fetcher, seen) = fake(move |r| {
            assert_eq!(r.redirect, Redirect::Manual);
            assert_eq!(r.header_value("User-Agent"), Some(CCTV_USER_AGENT));
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                redirect("/image/moved/1.jpg")
            } else {
                jpeg()
            }
        });
        assert!(
            fetch_cctv_image_from_upstream(&fetcher, Some(start), limits(100, 1024))
                .await
                .is_some()
        );
        assert_eq!(
            seen.lock()
                .iter()
                .map(|r| r.url.clone())
                .collect::<Vec<_>>(),
            [start, "https://cctv.austinmobility.io/image/moved/1.jpg"]
        );

        for target in [
            "https://evil.example/1.jpg",
            "https://cctv.austinmobility.io:8443/image/1.jpg",
            "http://cctv.austinmobility.io/image/1.jpg",
            "//evil.example/1.jpg",
        ] {
            let target = target.to_string();
            let (fetcher, seen) = fake(move |_| redirect(&target));
            assert_eq!(
                fetch_cctv_image_from_upstream(&fetcher, Some(start), limits(100, 1024)).await,
                None
            );
            assert_eq!(seen.lock().len(), 1);
        }

        let (fetcher, seen) = fake(move |_| redirect("/image/again.jpg"));
        assert_eq!(
            fetch_cctv_image_from_upstream(&fetcher, Some(start), limits(100, 1024)).await,
            None
        );
        assert_eq!(
            seen.lock().len(),
            3,
            "at most two redirect hops are followed"
        );
    }

    #[tokio::test]
    async fn txdot_snapshot_decodes_only_official_jpeg() {
        let jpeg = [0xffu8, 0xd8, 0xff, 0xe0, 0x00];
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let json_response = |body: serde_json::Value| {
            FetchResponse::new(
                200,
                &[("content-type", "application/json; charset=utf-8")],
                body.to_string(),
            )
        };
        let official =
            "https://its.txdot.gov/its/DistrictIts/GetCctvSnapshotByIcdId?icdId=x&districtCode=AUS";
        let snippet = b64(&jpeg);
        let s = snippet.clone();
        let (fetcher, _) = fake(move |_| json_response(serde_json::json!({ "snippet": s })));
        let frame = fetch_txdot_snapshot(
            &fetcher,
            Some(official),
            limits(100, CCTV_FRAME_MAX_BODY_BYTES),
        )
        .await
        .unwrap();
        assert_eq!(frame.content_type, "image/jpeg");
        assert_eq!(&frame.body[..], &jpeg);

        for bad in [
            "https://evil.example/its/DistrictIts/GetCctvSnapshotByIcdId?icdId=x",
            "https://its.txdot.gov/its/DistrictIts/Other?icdId=x",
        ] {
            assert_eq!(
                fetch_txdot_snapshot(&fetcher, Some(bad), limits(100, 1024)).await,
                None,
                "{bad}"
            );
        }

        let (redirecting, seen) = fake(|r| {
            assert_eq!(r.redirect, Redirect::Manual);
            FetchResponse::new(302, &[("location", "https://evil.example/x.json")], "")
        });
        assert_eq!(
            fetch_txdot_snapshot(&redirecting, Some(official), limits(100, 1024)).await,
            None
        );
        assert_eq!(seen.lock().len(), 1);

        for junk in ["/9!@#j/4AA=", "/9j/4AA=="] {
            let (fetcher, _) = fake(move |_| json_response(serde_json::json!({ "snippet": junk })));
            assert_eq!(
                fetch_txdot_snapshot(&fetcher, Some(official), limits(100, 1024)).await,
                None,
                "{junk}"
            );
        }

        let mut big = jpeg.to_vec();
        big.extend(std::iter::repeat_n(0x11u8, 59));
        assert_eq!(big.len(), 64);
        let big_b64 = b64(&big);
        let s = big_b64.clone();
        let (fetcher, _) = fake(move |_| json_response(serde_json::json!({ "snippet": s })));
        assert_eq!(
            fetch_txdot_snapshot(&fetcher, Some(official), limits(100, 8)).await,
            None
        );
        assert_eq!(
            fetch_txdot_snapshot(&fetcher, Some(official), limits(100, 64))
                .await
                .unwrap()
                .body
                .len(),
            64
        );

        let mut huge = jpeg.to_vec();
        huge.extend(std::iter::repeat_n(0x11u8, 8192));
        let s = b64(&huge);
        let (fetcher, _) = fake(move |_| json_response(serde_json::json!({ "snippet": s })));
        assert_eq!(
            fetch_txdot_snapshot(&fetcher, Some(official), limits(100, 8)).await,
            None
        );

        let not_jpeg = b64(b"<html>nope</html>");
        for body in [
            serde_json::Value::Null,
            serde_json::json!({ "snippet": null }),
            serde_json::json!({ "snippet": "" }),
            serde_json::json!({ "snippet": not_jpeg }),
        ] {
            let (fetcher, _) = fake(move |_| json_response(body.clone()));
            assert_eq!(
                fetch_txdot_snapshot(&fetcher, Some(official), limits(100, 1024)).await,
                None
            );
        }
        let s = format!("data:image/JPEG;base64,{snippet}");
        let (fetcher, _) = fake(move |_| json_response(serde_json::json!({ "snippet": s })));
        assert!(
            fetch_txdot_snapshot(&fetcher, Some(official), limits(100, 1024))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn the_relay_refuses_a_declared_oversize_body() {
        let length = (CCTV_MEDIA_MAX_BODY_BYTES + 1).to_string();
        let upstream = FetchResponse::new(
            206,
            &[("content-type", "video/mp4"), ("content-length", &length)],
            "too big",
        );
        let response = proxy_media_response(upstream, "live-media", media_idle_timeout());
        assert_eq!(response.status(), 502);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("exceeds size cap"));
    }

    #[tokio::test]
    async fn the_relay_releases_a_silent_upstream_at_the_idle_deadline() {
        let released = Arc::new(AtomicBool::new(false));
        struct OnDrop(Arc<AtomicBool>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let guard = OnDrop(Arc::clone(&released));
        let body =
            futures_util::stream::once(async { Ok(Bytes::from_static(&[0xff, 0xd8, 0xff, 0xe0])) })
                .chain(futures_util::stream::pending())
                .map(move |chunk| {
                    let _ = &guard;
                    chunk
                })
                .boxed();
        let upstream = FetchResponse::streaming(200, &[("content-type", "video/mp4")], body);
        let response = proxy_media_response(upstream, "live-media", Duration::from_millis(40));
        let started = std::time::Instant::now();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(bytes.len(), 4, "the client gets what arrived, then the end");
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert!(
            released.load(Ordering::SeqCst),
            "the silent upstream was released"
        );
    }

    #[tokio::test]
    async fn a_live_feed_that_keeps_producing_is_not_cut_off() {
        let body = futures_util::stream::unfold(0u32, |n| async move {
            if n >= 15 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            Some((Ok(Bytes::from_static(b"frame")), n + 1))
        })
        .boxed();
        let upstream = FetchResponse::streaming(200, &[("content-type", "video/mp4")], body);
        let response = proxy_media_response(upstream, "live-media", Duration::from_millis(40));
        assert_eq!(response.headers()["x-cctv-source"], "live-media");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(bytes.len(), 15 * 5);
    }

    #[test]
    fn synthetic_svg_escapes_labels() {
        let svg = build_synthetic_cctv_svg("cam<1>", "A & B", "", "");
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("A &amp; B · GLOBAL GRID"));
        assert!(svg.contains("cam&lt;1&gt;"));
        assert!(svg.contains(">SYNTHETIC</text>"));
    }
}
