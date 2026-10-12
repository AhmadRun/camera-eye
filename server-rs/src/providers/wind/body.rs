//! Upstream fetching for the wind models: the injectable `fetchImpl` seam,
//! `readWindBody` (port of `src/sources/windBody.js`) and the bounded
//! `fetchText` / `fetchRange` / `loadOptionalScalar` helpers of
//! `server/providers/wind/gfs.js`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;

use super::WindError;
use super::grid::Decoded;

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// An upstream response: status, declared length and a body stream.
pub struct Upstream {
    pub status: u16,
    pub content_length: Option<u64>,
    pub body: BoxStream<'static, Result<Bytes, WindError>>,
}

impl Upstream {
    fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// `fetchImpl(url, { redirect: 'error', headers: { Range } })`. Dropping
/// the returned future (or the body stream) cancels the request.
pub trait Fetch: Send + Sync {
    fn fetch(&self, url: &str, range: Option<&str>) -> BoxFut<Result<Upstream, WindError>>;
}

/// `decodeImpl(bytes)`.
pub type DecodeFn = Arc<dyn Fn(Bytes) -> BoxFut<Result<Decoded, WindError>> + Send + Sync>;

/// The real upstream: a client that refuses redirects like `redirect: 'error'`.
pub struct HttpFetch(reqwest::Client);

impl HttpFetch {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!("camera-eye/", env!("CARGO_PKG_VERSION")))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        Self(client)
    }
}

impl Fetch for HttpFetch {
    fn fetch(&self, url: &str, range: Option<&str>) -> BoxFut<Result<Upstream, WindError>> {
        let mut request = self.0.get(url);
        if let Some(range) = range {
            request = request.header(http::header::RANGE, range);
        }
        Box::pin(async move {
            let response = request
                .send()
                .await
                .map_err(|_| WindError::new("Wind upstream unavailable"))?;
            let status = response.status().as_u16();
            // A redirect is a network error under `redirect: 'error'`.
            if (300..400).contains(&status) {
                return Err(WindError::new("Wind upstream unavailable"));
            }
            Ok(Upstream {
                status,
                content_length: response.content_length(),
                body: response
                    .bytes_stream()
                    .map(|chunk| chunk.map_err(|e| WindError::new(e.to_string())))
                    .boxed(),
            })
        })
    }
}

/// Read a fetch body under a hard byte budget, including chunked responses.
pub async fn read_wind_body(upstream: Upstream, max_bytes: usize) -> Result<Bytes, WindError> {
    let fail = || WindError::new("Wind response exceeds byte budget");
    if upstream
        .content_length
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(fail());
    }
    let mut body = upstream.body;
    let mut out = BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if out.len() + chunk.len() > max_bytes {
            return Err(fail());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

/// Fetch a bounded (2 MiB) text response.
pub async fn fetch_text(fetch: &dyn Fetch, url: &str) -> Result<String, WindError> {
    let upstream = fetch.fetch(url, None).await?;
    if !upstream.ok() {
        return Err(WindError::new("Wind upstream unavailable"));
    }
    let bytes = read_wind_body(upstream, 2 * 1024 * 1024).await?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Default `maxBytes` of `fetchRange`.
pub const MAX_RANGE_BYTES: f64 = 8.0 * 1024.0 * 1024.0;

fn is_safe_integer(value: f64) -> bool {
    value.is_finite() && value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991.0
}

/// Fetch one bounded byte range. Rejects oversized ranges, non-206 responses
/// (except a whole-object 200 starting at zero), and short bodies.
pub async fn fetch_range(
    fetch: &dyn Fetch,
    url: &str,
    start: f64,
    end: f64,
    max_bytes: f64,
) -> Result<Bytes, WindError> {
    if !is_safe_integer(start)
        || !is_safe_integer(end)
        || start < 0.0
        || end < start
        || end - start + 1.0 > max_bytes
    {
        return Err(WindError::new("range too large"));
    }
    let (start, end) = (start as u64, end as u64);
    let upstream = fetch
        .fetch(url, Some(&format!("bytes={start}-{end}")))
        .await?;
    let valid_status = upstream.status == 206 || (upstream.status == 200 && start == 0);
    if !valid_status || !upstream.ok() {
        return Err(WindError::new("Wind upstream unavailable"));
    }
    let length = (end - start + 1) as usize;
    let bytes = read_wind_body(upstream, length).await?;
    if bytes.len() != length {
        return Err(WindError::new("invalid range length"));
    }
    Ok(bytes)
}

/// A `{ start, end }` byte range (JS numbers).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ByteRange {
    pub start: f64,
    pub end: f64,
}

/// Fetch and decode one required message.
pub async fn load_message(
    fetch: &dyn Fetch,
    decode: &DecodeFn,
    url: &str,
    range: ByteRange,
) -> Result<Decoded, WindError> {
    let bytes = fetch_range(fetch, url, range.start, range.end, MAX_RANGE_BYTES).await?;
    decode(bytes).await
}

/// A bounded optional request may fail without cancelling valid wind
/// siblings. Cancelling the caller drops this future and its request.
pub async fn load_optional_scalar(
    range: Option<ByteRange>,
    url: &str,
    fetch: &dyn Fetch,
    decode: &DecodeFn,
    timeout: Duration,
) -> Option<Decoded> {
    let range = range?;
    tokio::time::timeout(timeout, load_message(fetch, decode, url, range))
        .await
        .ok()?
        .ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures_util::stream;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A recorded `fetchImpl` call.
    #[derive(Debug, Clone)]
    pub struct Call {
        pub url: String,
        pub range: Option<String>,
    }

    type Handler = dyn Fn(&str, Option<&str>) -> Result<Upstream, WindError> + Send + Sync;

    /// A synchronous fetch double that records its calls.
    pub struct FakeFetch {
        pub calls: Mutex<Vec<Call>>,
        handler: Box<Handler>,
    }

    impl FakeFetch {
        pub fn new(
            handler: impl Fn(&str, Option<&str>) -> Result<Upstream, WindError> + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                handler: Box::new(handler),
            })
        }
    }

    impl Fetch for FakeFetch {
        fn fetch(&self, url: &str, range: Option<&str>) -> BoxFut<Result<Upstream, WindError>> {
            self.calls.lock().push(Call {
                url: url.to_string(),
                range: range.map(str::to_string),
            });
            let result = (self.handler)(url, range);
            Box::pin(async move { result })
        }
    }

    /// `new Response(body, { status })`.
    pub fn response(status: u16, body: impl Into<Bytes>) -> Upstream {
        let body: Bytes = body.into();
        Upstream {
            status,
            content_length: Some(body.len() as u64),
            body: stream::once(async move { Ok(body) }).boxed(),
        }
    }

    /// The start offset of a `bytes=start-end` header.
    pub fn range_start(range: Option<&str>) -> u64 {
        range
            .and_then(|r| r.strip_prefix("bytes="))
            .and_then(|r| r.split('-').next())
            .and_then(|s| s.parse().ok())
            .unwrap()
    }

    #[tokio::test]
    async fn fetch_range_accepts_a_whole_object_from_zero() {
        let fetch = FakeFetch::new(|_, _| Ok(response(200, vec![1u8, 2, 3])));
        let bytes = fetch_range(&*fetch, "x", 0.0, 2.0, MAX_RANGE_BYTES)
            .await
            .unwrap();
        assert_eq!(&bytes[..], &[1, 2, 3]);
        assert_eq!(fetch.calls.lock()[0].range.as_deref(), Some("bytes=0-2"));
    }

    #[tokio::test]
    async fn fetch_range_rejects_invalid_offsets_and_caps_chunked_bodies() {
        let fetch = FakeFetch::new(|_, _| Ok(response(206, vec![0u8; 3])));
        let error = fetch_range(&*fetch, "x", -1.0, 2.0, MAX_RANGE_BYTES)
            .await
            .unwrap_err();
        assert!(error.0.contains("range too large"));
        assert!(
            fetch_range(&*fetch, "x", 0.5, 2.0, MAX_RANGE_BYTES)
                .await
                .is_err()
        );
        assert!(fetch.calls.lock().is_empty());

        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&cancelled);
        let fetch = FakeFetch::new(move |_, _| {
            let guard = DropFlag(Arc::clone(&flag));
            Ok(Upstream {
                status: 206,
                content_length: None,
                body: stream::repeat_with(move || {
                    let _ = &guard;
                    Ok(Bytes::from_static(&[0u8; 10]))
                })
                .boxed(),
            })
        });
        let error = fetch_range(&*fetch, "x", 0.0, 2.0, MAX_RANGE_BYTES)
            .await
            .unwrap_err();
        assert!(error.0.contains("budget"));
        assert!(cancelled.load(Ordering::SeqCst), "body stream was released");
    }

    #[tokio::test]
    async fn fetch_range_rejects_wrong_status_and_short_bodies() {
        let fetch = FakeFetch::new(|_, _| Ok(response(200, vec![0u8; 3])));
        assert!(
            fetch_range(&*fetch, "x", 1.0, 3.0, MAX_RANGE_BYTES)
                .await
                .is_err()
        );
        let fetch = FakeFetch::new(|_, _| Ok(response(206, vec![0u8; 2])));
        let error = fetch_range(&*fetch, "x", 0.0, 2.0, MAX_RANGE_BYTES)
            .await
            .unwrap_err();
        assert_eq!(error.0, "invalid range length");
        let fetch = FakeFetch::new(|_, _| Ok(response(206, vec![0u8; 3])));
        let error = fetch_range(&*fetch, "x", 0.0, 9.0 * 1024.0 * 1024.0, MAX_RANGE_BYTES)
            .await
            .unwrap_err();
        assert_eq!(error.0, "range too large");
    }

    #[tokio::test]
    async fn fetch_text_rejects_errors_and_oversized_bodies() {
        let fetch = FakeFetch::new(|_, _| Ok(response(503, "Unavailable")));
        assert!(fetch_text(&*fetch, "x").await.is_err());
        let fetch = FakeFetch::new(|_, _| {
            Ok(Upstream {
                status: 200,
                content_length: Some(3 * 1024 * 1024),
                body: stream::empty().boxed(),
            })
        });
        assert!(
            fetch_text(&*fetch, "x")
                .await
                .unwrap_err()
                .0
                .contains("budget")
        );
    }

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A fetch whose request never settles; records when it is dropped.
    struct HangingFetch(Arc<AtomicBool>);
    impl Fetch for HangingFetch {
        fn fetch(&self, _: &str, _: Option<&str>) -> BoxFut<Result<Upstream, WindError>> {
            let guard = DropFlag(Arc::clone(&self.0));
            Box::pin(async move {
                let _guard = guard;
                std::future::pending().await
            })
        }
    }

    #[tokio::test]
    async fn optional_scalar_timeout_cancels_only_its_own_fetch() {
        let aborted = Arc::new(AtomicBool::new(false));
        let fetch = HangingFetch(Arc::clone(&aborted));
        let decode: DecodeFn = Arc::new(|_| Box::pin(async { panic!("not reached") }));
        let value = load_optional_scalar(
            Some(ByteRange {
                start: 0.0,
                end: 1.0,
            }),
            "https://fixed.test/field",
            &fetch,
            &decode,
            Duration::from_millis(5),
        )
        .await;
        assert!(value.is_none());
        assert!(aborted.load(Ordering::SeqCst));
        let none = load_optional_scalar(None, "x", &fetch, &decode, Duration::from_secs(1)).await;
        assert!(none.is_none());
    }
}
