//! Shared plumbing for the space, hazards and tiles providers.
//!
//! The JS providers took injectable `fetchImpl`/`now` options so their unit
//! tests could stand in for an upstream. The Rust ports keep that seam: a
//! [`Fetcher`] closure (reqwest in production, canned responses in tests)
//! and a [`Clock`]. Also here: Connect-style sub-URLs for prefix-mounted
//! middleware, capped body reads over a [`FetchResponse`], and the JS
//! number/date formatting the JSON bodies relied on.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::handler::Handler;
use axum::routing::any;
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use http::HeaderMap;
use serde_json::Value;

use crate::common::js;
use crate::state::AppState;

/// How a request treats 3xx answers, like `fetch`'s `redirect` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Redirect {
    Follow,
    /// Return the 3xx response to the caller.
    Manual,
    /// A 3xx answer is a network error.
    Error,
}

/// One upstream request.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub method: http::Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub redirect: Redirect,
}

impl FetchRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: http::Method::GET,
            url: url.into(),
            headers: Vec::new(),
            body: None,
            redirect: Redirect::Follow,
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn redirect(mut self, redirect: Redirect) -> Self {
        self.redirect = redirect;
        self
    }

    #[cfg(test)]
    pub fn header_value(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Why an upstream exchange failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    /// `code: 'RESPONSE_TOO_LARGE'`.
    #[error("Upstream response too large")]
    TooLarge,
    /// The request's deadline passed (an `AbortError`).
    #[error("upstream timed out")]
    Timeout,
    #[error("upstream request failed: {0}")]
    Network(String),
}

pub type Body = BoxStream<'static, Result<Bytes, FetchError>>;

/// An upstream answer whose body has not been read yet.
pub struct FetchResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Body,
}

impl FetchResponse {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// A response with a fixed body (tests and synthetic answers).
    #[cfg(test)]
    pub fn from_bytes(status: u16, headers: &[(&str, &str)], body: impl Into<Bytes>) -> Self {
        let body: Bytes = body.into();
        Self::from_stream(
            status,
            headers,
            futures_util::stream::once(async move { Ok(body) }).boxed(),
        )
    }

    #[cfg(test)]
    pub fn from_stream(status: u16, headers: &[(&str, &str)], body: Body) -> Self {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_str(value),
            ) {
                map.append(name, value);
            }
        }
        Self {
            status,
            headers: map,
            body,
        }
    }

    #[cfg(test)]
    pub fn json(value: &Value) -> Self {
        Self::from_bytes(
            200,
            &[("content-type", "application/json")],
            serde_json::to_vec(value).unwrap_or_default(),
        )
    }
}

/// The injectable upstream transport.
pub type Fetcher = Arc<
    dyn Fn(FetchRequest) -> BoxFuture<'static, Result<FetchResponse, FetchError>> + Send + Sync,
>;

/// Milliseconds since the epoch, injectable like the JS `now` option.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(js::now_ms)
}

/// The production transport: reqwest, with redirects handled per request.
pub fn reqwest_fetcher() -> Fetcher {
    let build = |policy: reqwest::redirect::Policy| {
        reqwest::Client::builder()
            .user_agent(concat!("camera-eye/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .redirect(policy)
            .build()
            .unwrap_or_default()
    };
    let follow = build(reqwest::redirect::Policy::limited(20));
    let manual = build(reqwest::redirect::Policy::none());
    Arc::new(move |request: FetchRequest| {
        let client = if request.redirect == Redirect::Follow {
            follow.clone()
        } else {
            manual.clone()
        };
        async move {
            let mut builder = client.request(request.method.clone(), &request.url);
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            let response = builder.send().await.map_err(map_reqwest_error)?;
            let status = response.status().as_u16();
            if request.redirect == Redirect::Error && (300..400).contains(&status) {
                return Err(FetchError::Network("unexpected redirect".into()));
            }
            let headers = response.headers().clone();
            let body = response
                .bytes_stream()
                .map(|chunk| chunk.map_err(map_reqwest_error))
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

fn map_reqwest_error(error: reqwest::Error) -> FetchError {
    if error.is_timeout() {
        FetchError::Timeout
    } else {
        FetchError::Network(error.to_string())
    }
}

/// `Number(response.headers.get('content-length')) > maxBytes`.
fn declared_too_large(response: &FetchResponse, max_bytes: usize) -> bool {
    let declared = js::number(response.header("content-length").unwrap_or(""));
    declared.is_finite() && declared > max_bytes as f64
}

/// `readResponseBytesCapped`: refuse an oversized declared length before
/// reading, then stop (dropping, i.e. cancelling, the stream) at the cap.
pub async fn read_bytes_capped(
    response: FetchResponse,
    max_bytes: usize,
) -> Result<Bytes, FetchError> {
    if declared_too_large(&response, max_bytes) {
        return Err(FetchError::TooLarge);
    }
    let mut body = response.body;
    let mut out = BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if out.len() + chunk.len() > max_bytes {
            return Err(FetchError::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

/// `readResponseTextCapped`: the capped bytes decoded like `TextDecoder`.
pub async fn read_text_capped(
    response: FetchResponse,
    max_bytes: usize,
) -> Result<String, FetchError> {
    let bytes = read_bytes_capped(response, max_bytes).await?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// An unbounded read (`response.text()` / `.json()` in the JS).
pub async fn read_all(response: FetchResponse) -> Result<Bytes, FetchError> {
    read_bytes_capped(response, usize::MAX).await
}

/// Run `future` under a deadline that maps to [`FetchError::Timeout`].
pub async fn with_deadline<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = Result<T, FetchError>>,
) -> Result<T, FetchError> {
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or(Err(FetchError::Timeout))
}

/// `req.url` as Connect hands it to `middlewares.use(prefix, handler)`:
/// the path after the mount prefix (always starting with `/`) plus the
/// original query string.
pub fn sub_url(request: &Request, prefix: &str) -> String {
    let path = request.uri().path();
    let rest = path.strip_prefix(prefix).unwrap_or(path);
    let mut url = if rest.starts_with('/') {
        rest.to_string()
    } else {
        format!("/{rest}")
    };
    if let Some(query) = request.uri().query() {
        url.push('?');
        url.push_str(query);
    }
    url
}

/// Mount `handler` like Connect's `use(prefix, ...)`: the prefix itself
/// and everything below it, for every method.
pub fn mount<H, T>(prefix: &str, handler: H) -> Router<AppState>
where
    H: Handler<T, AppState> + Clone,
    T: 'static,
{
    Router::new()
        .route(prefix, any(handler.clone()))
        .route(&format!("{prefix}/"), any(handler.clone()))
        .route(&format!("{prefix}/{{*rest}}"), any(handler))
}

/// A number as `JSON.stringify` writes it: integral values without `.0`.
pub fn jnum(value: f64) -> Value {
    if !value.is_finite() {
        return Value::Null;
    }
    if value.fract() == 0.0 && value.abs() < 9.0e15 {
        return Value::from(value as i64);
    }
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// `serialize_with` for `f64` fields, matching [`jnum`].
pub fn ser_num<S: serde::Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(&jnum(*value), serializer)
}

/// `Number.isFinite(value)` on a parsed JSON value.
pub fn finite_number(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64).filter(|n| n.is_finite())
}

/// `new Date(ms).toISOString()`.
pub fn to_iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_default()
}

/// `Date.parse` for the formats these providers meet: ISO 8601 (with or
/// without an offset; a bare date-time reads as UTC, the server zone) and
/// RFC 2822 / HTTP dates. `None` stands for NaN.
pub fn parse_date(text: &str) -> Option<i64> {
    let s = text.trim();
    if let Ok(d) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(d.timestamp_millis());
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M%:z"] {
        if let Ok(d) = chrono::DateTime::parse_from_str(s, format) {
            return Some(d.timestamp_millis());
        }
    }
    let naive = s.strip_suffix('Z').unwrap_or(s);
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M"] {
        if let Ok(d) = chrono::NaiveDateTime::parse_from_str(naive, format) {
            return Some(d.and_utc().timestamp_millis());
        }
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d
            .and_hms_opt(0, 0, 0)
            .map(|d| d.and_utc().timestamp_millis());
    }
    if let Ok(d) = chrono::DateTime::parse_from_rfc2822(s) {
        return Some(d.timestamp_millis());
    }
    None
}

/// `Number.prototype.toFixed(digits)`: round half away from zero on the
/// exact binary value.
pub fn to_fixed(value: f64, digits: usize) -> String {
    if !value.is_finite() {
        return if value.is_nan() {
            "NaN".into()
        } else if value > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if value.abs() >= 1e21 {
        return js_number_string(value);
    }
    let negative = value < 0.0;
    // 1100 fractional digits hold any double's exact expansion.
    let exact = format!("{:.1100}", value.abs());
    let (int_part, frac_part) = exact.split_once('.').unwrap_or((&exact, ""));
    let mut digits_vec: Vec<u8> = int_part
        .bytes()
        .chain(frac_part.bytes().take(digits))
        .map(|b| b - b'0')
        .collect();
    let round_up = frac_part.as_bytes().get(digits).is_some_and(|d| *d >= b'5');
    if round_up {
        let mut i = digits_vec.len();
        loop {
            if i == 0 {
                digits_vec.insert(0, 1);
                break;
            }
            i -= 1;
            if digits_vec[i] == 9 {
                digits_vec[i] = 0;
            } else {
                digits_vec[i] += 1;
                break;
            }
        }
    }
    let all: String = digits_vec.iter().map(|d| (d + b'0') as char).collect();
    let split = all.len() - digits;
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&all[..split]);
    if digits > 0 {
        out.push('.');
        out.push_str(&all[split..]);
    }
    out
}

/// `String(number)` for the values these providers print.
pub fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if value == 0.0 {
        return "0".into();
    }
    if value.abs() >= 1e21 || value.abs() < 1e-6 {
        let formatted = format!("{value:e}");
        return match formatted.split_once('e') {
            Some((mantissa, exp)) if !exp.starts_with('-') => format!("{mantissa}e+{exp}"),
            _ => formatted,
        };
    }
    let text = format!("{value}");
    text.strip_suffix(".0").map(str::to_string).unwrap_or(text)
}

/// `String(value)` for a JSON value.
pub fn js_value_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => js_number_string(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_value_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// `encodeURIComponent`.
pub fn encode_uri_component(text: &str) -> String {
    const KEEP: &[u8] = b"-_.!~*'()";
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || KEEP.contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `decodeURIComponent`: `None` where JS throws a `URIError`.
pub fn decode_uri_component(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// A pseudo-random fraction in `[0, 1)` (`Math.random`).
pub fn random_fraction() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use parking_lot::Mutex;

    /// A recorded upstream request.
    pub type Calls = Arc<Mutex<Vec<FetchRequest>>>;

    /// A fetcher answering from `respond`, recording every request.
    pub fn fetcher<F>(calls: Calls, respond: F) -> Fetcher
    where
        F: Fn(&FetchRequest) -> Result<FetchResponse, FetchError> + Send + Sync + 'static,
    {
        let respond = Arc::new(respond);
        Arc::new(move |request: FetchRequest| {
            calls.lock().push(request.clone());
            let respond = Arc::clone(&respond);
            async move { respond(&request) }.boxed()
        })
    }

    /// A clock the test moves by hand.
    pub fn manual_clock(start: i64) -> (Clock, Arc<std::sync::atomic::AtomicI64>) {
        let cell = Arc::new(std::sync::atomic::AtomicI64::new(start));
        let read = Arc::clone(&cell);
        (
            Arc::new(move || read.load(std::sync::atomic::Ordering::SeqCst)),
            cell,
        )
    }

    /// A body that yields `chunk` forever, flagging when it is dropped.
    pub fn endless(chunk: Bytes, dropped: Arc<std::sync::atomic::AtomicBool>) -> Body {
        struct Flag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let flag = Flag(dropped);
        futures_util::stream::repeat_with(move || {
            let _ = &flag;
            Ok(chunk.clone())
        })
        .boxed()
    }

    /// A body that never yields, flagging when it is dropped.
    pub fn stalled(dropped: Arc<std::sync::atomic::AtomicBool>) -> Body {
        struct Flag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let flag = Flag(dropped);
        futures_util::stream::pending::<Result<Bytes, FetchError>>()
            .map(move |item| {
                let _ = &flag;
                item
            })
            .boxed()
    }

    /// Send `request` through `router` with a peer address attached.
    pub async fn send(
        router: &Router<AppState>,
        state: &AppState,
        method: &str,
        uri: &str,
        peer: &str,
    ) -> (u16, HeaderMap, Bytes) {
        use tower::ServiceExt;
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let addr: std::net::SocketAddr = format!("{peer}:1").parse().unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(addr));
        let response = router
            .clone()
            .with_state(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, headers, body)
    }

    /// A state for router tests, never touching the real cache directory.
    pub fn state() -> AppState {
        AppState::new(
            std::path::PathBuf::from("."),
            std::path::PathBuf::from("dist"),
        )
        .unwrap()
    }

    /// A fresh, empty directory under the system temp dir.
    pub fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ce-test-{tag}-{}-{}",
            std::process::id(),
            (random_fraction() * 1e12) as u64
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_fixed_matches_js() {
        assert_eq!(to_fixed(1.0, 5), "1.00000");
        assert_eq!(to_fixed(-97.7431, 5), "-97.74310");
        assert_eq!(to_fixed(0.015625, 5), "0.01563");
        assert_eq!(to_fixed(-0.015625, 5), "-0.01563");
        assert_eq!(to_fixed(2.56, 5), "2.56000");
        assert_eq!(to_fixed(-0.0, 5), "0.00000");
        assert_eq!(to_fixed(9.999999, 5), "10.00000");
    }

    #[test]
    fn numbers_and_dates_format_like_js() {
        assert_eq!(jnum(12.0), Value::from(12));
        assert_eq!(jnum(-0.0), Value::from(0));
        assert_eq!(jnum(1.5).to_string(), "1.5");
        assert_eq!(js_number_string(12.0), "12");
        assert_eq!(js_number_string(1.5), "1.5");
        assert_eq!(to_iso(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(parse_date("2026-07-15T20:20:00Z"), Some(1_784_146_800_000));
        assert_eq!(
            parse_date("2026-09-23T11:32:31-04:00"),
            parse_date("2026-09-23T15:32:31Z")
        );
        assert_eq!(parse_date("invalid"), None);
        assert_eq!(
            parse_date("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(1_445_412_480_000)
        );
    }

    #[test]
    fn uri_components_round_trip() {
        let url = "https://gbfs.lyft.com/a b/station_status.json?x=1&y=é";
        assert_eq!(
            decode_uri_component(&encode_uri_component(url)).as_deref(),
            Some(url)
        );
        assert_eq!(decode_uri_component("%zz"), None);
        assert_eq!(decode_uri_component("%C3"), None);
    }
}
