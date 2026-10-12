//! Response builders matching the JS `res.writeHead(...); res.end(...)` calls.

use axum::body::Body;
use axum::response::Response;
use http::{HeaderValue, StatusCode, header};
use serde::Serialize;

/// A JSON response with an optional `Cache-Control`.
pub fn json<T: Serialize + ?Sized>(
    status: u16,
    value: &T,
    cache_control: Option<&str>,
) -> Response {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"null".to_vec());
    raw(status, "application/json", body, cache_control)
}

/// A JSON body that is already serialized.
pub fn json_text(status: u16, body: impl Into<Body>, cache_control: Option<&str>) -> Response {
    raw(status, "application/json", body, cache_control)
}

/// `{ "error": message }` with `Cache-Control: no-store`.
pub fn error(status: u16, message: &str) -> Response {
    json(
        status,
        &serde_json::json!({ "error": message }),
        Some("no-store"),
    )
}

/// Any body with a content type and optional cache policy.
pub fn raw(
    status: u16,
    content_type: &str,
    body: impl Into<Body>,
    cache_control: Option<&str>,
) -> Response {
    let mut response = Response::new(body.into());
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(content_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Some(cache) = cache_control.and_then(|c| HeaderValue::from_str(c).ok()) {
        headers.insert(header::CACHE_CONTROL, cache);
    }
    response
}

/// Add or replace a header on a response.
pub fn with_header(mut response: Response, name: &str, value: &str) -> Response {
    if let (Ok(name), Ok(value)) = (
        http::HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(value),
    ) {
        response.headers_mut().insert(name, value);
    }
    response
}
