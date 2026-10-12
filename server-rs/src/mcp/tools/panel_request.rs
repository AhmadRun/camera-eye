//! `src/tools/queries/panelRequest.js`: the Camera Eye panel's requests to
//! the app's server, answered in base64 parts.
//!
//! The response body is gzip-compressed with flate2 rather than the
//! platform's `CompressionStream`, so compressed bytes may differ from the
//! JS server's (both are valid gzip of the same body).

use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use base64::Engine;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use indexmap::IndexMap;
use serde_json::{Map, Value, json};
use tokio::sync::Semaphore;
use url::Url;

use crate::mcp::catalog::{Ctx, Failure, ToolError, ToolResult};
use crate::mcp::js;
use crate::mcp::services::fetch::{Init, Redirect};

/// Bytes of response body per call, before base64.
pub const PANEL_PART_BYTES: usize = 512 * 1024;
const HELD_MS: f64 = 2.0 * 60.0 * 1000.0;
const HELD_LIMIT_BYTES: usize = 256 * 1024 * 1024;
/// The largest response the panel may load.
pub const PANEL_RESPONSE_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const METHODS: [&str; 3] = ["GET", "HEAD", "POST"];
const FORWARDED_REQUEST_HEADERS: [&str; 2] = ["accept", "content-type"];
const DROPPED_RESPONSE_HEADERS: [&str; 6] = [
    "connection",
    "content-encoding",
    "content-length",
    "keep-alive",
    "set-cookie",
    "transfer-encoding",
];
const COMPRESS_MIN_BYTES: usize = 1024;
/// Requests in flight at once for one server's panels.
pub const PANEL_CONCURRENT_REQUESTS: usize = 6;
/// Requests that may wait for a place.
pub const PANEL_QUEUED_REQUESTS: usize = 256;

struct Held {
    path: String,
    response: Map<String, Value>,
    bytes: Vec<u8>,
    expires: f64,
}

#[derive(Default)]
struct HeldState {
    held: IndexMap<String, Held>,
    held_bytes: usize,
}

impl HeldState {
    fn release(&mut self, id: &str) {
        if let Some(entry) = self.held.shift_remove(id) {
            self.held_bytes -= entry.bytes.len();
        }
    }

    /// `forgetExpired`.
    fn forget_expired(&mut self) {
        let now = js::now();
        let expired: Vec<String> = self
            .held
            .iter()
            .filter(|(_, entry)| entry.expires <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            self.release(&id);
        }
    }

    /// `hold`: keep a response for its later parts, making room first.
    fn hold(&mut self, id: String, entry: Held) {
        self.forget_expired();
        while let Some(oldest) = self.held.keys().next().cloned() {
            if self.held_bytes + entry.bytes.len() <= HELD_LIMIT_BYTES {
                break;
            }
            self.release(&oldest);
        }
        self.held_bytes += entry.bytes.len();
        self.held.insert(id, entry);
    }
}

/// One server's panel state: held responses and requests in flight.
pub struct PanelState {
    held: Mutex<HeldState>,
    slots: Semaphore,
    waiting: AtomicUsize,
}

impl Default for PanelState {
    fn default() -> Self {
        Self {
            held: Mutex::new(HeldState::default()),
            slots: Semaphore::new(PANEL_CONCURRENT_REQUESTS),
            waiting: AtomicUsize::new(0),
        }
    }
}

/// `isPanelKey(given, expected)`: compared in time independent of where
/// they differ.
fn is_panel_key(given: Option<&Value>, expected: Option<&str>) -> bool {
    let Some(expected) = expected.filter(|e| !e.is_empty()) else {
        return false;
    };
    let Some(given) = given.and_then(Value::as_str) else {
        return false;
    };
    let a: Vec<u16> = given.encode_utf16().collect();
    let b: Vec<u16> = expected.encode_utf16().collect();
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(&b).fold(0u16, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// `atob`: forgiving base64.
fn from_base64(text: &str) -> Option<Vec<u8>> {
    let mut cleaned: String = text
        .chars()
        .filter(|c| !matches!(c, ' ' | '\t' | '\n' | '\x0c' | '\r'))
        .collect();
    if cleaned.len().is_multiple_of(4) {
        for _ in 0..2 {
            if cleaned.ends_with('=') {
                cleaned.pop();
            }
        }
    }
    if cleaned.len() % 4 == 1
        || !cleaned
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/')
    {
        return None;
    }
    let engine = GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::RequireNone)
            .with_decode_allow_trailing_bits(true),
    );
    engine.decode(cleaned).ok()
}

/// `decodeURIComponent`, or None where it throws.
fn decode_uri_component(text: &str) -> Option<String> {
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

fn refused(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let mounted = |prefix: &str| {
        lower
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/') || rest.starts_with('.'))
    };
    ["/api/setup", "/api/realtime", "/api/openai", "/mcp"]
        .iter()
        .any(|p| mounted(p))
        || path.starts_with("/@")
        || path.starts_with("/__")
}

fn invalid(message: impl Into<String>) -> Failure {
    Failure::tool("invalid_arguments", message)
}

/// `checkedPath(path, baseUrl)`.
fn checked_path(path: Option<&Value>, base: &str) -> Result<String, Failure> {
    let Some(path) = path.and_then(Value::as_str).filter(|p| p.starts_with('/')) else {
        return Err(invalid("path must start with /"));
    };
    let base = Url::parse(base).map_err(|_| Failure::Type("Invalid URL".into()))?;
    let url = base
        .join(path)
        .map_err(|_| invalid("path is not a valid path"))?;
    if url.origin() != base.origin() {
        return Err(invalid("path must stay on the app's server"));
    }
    let pathname = url.path().to_string();
    let decoded =
        decode_uri_component(&pathname).ok_or_else(|| invalid("path is not a valid path"))?;
    if refused(&pathname) || refused(&decoded) {
        return Err(invalid(format!("{pathname} is not available")));
    }
    let search = url
        .query()
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    Ok(format!("{pathname}{search}"))
}

fn incompressible(kind: &str) -> bool {
    (kind.starts_with("image/") && !kind.starts_with("image/svg"))
        || kind.starts_with("video/")
        || kind.starts_with("audio/")
        || kind.starts_with("font/woff2")
        || kind.contains("zip")
        || kind.contains("compressed")
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("in-memory gzip");
    encoder.finish().expect("in-memory gzip")
}

/// `part(response, bytes, offset)`.
fn part(response: &Map<String, Value>, bytes: &[u8], offset: usize) -> Value {
    let end = bytes.len().min(offset.saturating_add(PANEL_PART_BYTES));
    let start = offset.min(end);
    let mut out = response.clone();
    out.insert("offset".into(), json!(offset));
    out.insert(
        "body".into(),
        json!(base64::engine::general_purpose::STANDARD.encode(&bytes[start..end])),
    );
    if end < bytes.len() {
        out.insert("nextOffset".into(), json!(end));
    }
    Value::Object(out)
}

fn random_uuid() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("system randomness");
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn status_text(status: u16) -> &'static str {
    http::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("")
}

pub async fn panel_request(args: Value, ctx: Ctx) -> Result<ToolResult, Failure> {
    let services = &ctx.services;
    if !is_panel_key(args.get("key"), services.panel_key.as_deref()) {
        return Err(invalid("Only the Camera Eye panel may make this request"));
    }
    let state = &services.panel;
    state.held.lock().expect("panel state").forget_expired();
    if let Some(id) = args.get("id") {
        let mut held = state.held.lock().expect("panel state");
        let key = js::to_js_string(id);
        let Some(mut entry) = held.held.shift_remove(&key) else {
            return Err(invalid(
                "That response is no longer held; request the path again",
            ));
        };
        entry.expires = js::now() + HELD_MS;
        let offset = args.get("offset").and_then(Value::as_f64).unwrap_or(0.0) as usize;
        let data = part(&entry.response, &entry.bytes, offset);
        let summary = format!("Part of {}", entry.path);
        held.held.insert(key, entry);
        return Ok(ToolResult::new(summary, data));
    }
    let base = services.app_base_url.clone().unwrap_or_default();
    let path = checked_path(args.get("path"), &base)?;
    let method = args
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET")
        .to_string();
    if !METHODS.contains(&method.as_str()) {
        return Err(invalid(format!("Unsupported method {method}")));
    }
    let mut init = Init::redirect(Redirect::Manual);
    init.method = method.clone();
    if let Some(headers) = args.get("headers").and_then(Value::as_object) {
        for (name, value) in headers {
            if FORWARDED_REQUEST_HEADERS.contains(&name.to_lowercase().as_str()) {
                init.headers.push((name.clone(), js::to_js_string(value)));
            }
        }
    }
    // A place among the requests in flight, waiting for one if need be.
    let permit = match state.slots.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            if state.waiting.load(Ordering::SeqCst) >= PANEL_QUEUED_REQUESTS {
                return Err(Failure::Tool(
                    ToolError::new("retry_later", "The panel has too many requests waiting")
                        .retry(Some(1.0)),
                ));
            }
            state.waiting.fetch_add(1, Ordering::SeqCst);
            let permit = state.slots.acquire().await;
            state.waiting.fetch_sub(1, Ordering::SeqCst);
            permit.map_err(|_| Failure::tool("unavailable", "The app's server did not answer"))?
        }
    };
    let api = &services.api;
    let body = args.get("body").filter(|_| method == "POST");
    let work = async {
        if let Some(body) = body {
            let text = js::to_js_string(body);
            init.body = Some(
                from_base64(&text)
                    .ok_or_else(|| Failure::other("Invalid character"))?
                    .into(),
            );
        }
        let answer = api.fetch(&path, init).await?;
        let status = answer.status;
        let headers = answer.headers.clone();
        let bytes = answer
            .bytes_capped(PANEL_RESPONSE_LIMIT_BYTES)
            .await?
            .ok_or_else(|| Failure::tool("unavailable", "The response is too large to load"))?;
        Ok::<_, Failure>((status, headers, bytes))
    };
    let outcome = tokio::time::timeout(REQUEST_TIMEOUT, work).await;
    drop(permit);
    let (status, headers, bytes) = match outcome {
        Ok(Ok(answer)) => answer,
        Ok(Err(error @ Failure::Tool(_))) => return Err(error),
        _ => {
            return Err(Failure::tool(
                "unavailable",
                "The app's server did not answer",
            ));
        }
    };
    let kind = headers
        .get_all("content-type")
        .iter()
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .collect::<Vec<_>>()
        .join(", ");
    let mut bytes = bytes.to_vec();
    let mut encoding = "identity";
    if bytes.len() >= COMPRESS_MIN_BYTES && !incompressible(&kind) {
        bytes = gzip(&bytes);
        encoding = "gzip";
    }
    let mut names: Vec<&str> = headers.keys().map(|name| name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let mut response_headers = Map::new();
    for name in names {
        if DROPPED_RESPONSE_HEADERS.contains(&name) {
            continue;
        }
        let value = headers
            .get_all(name)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect::<Vec<_>>()
            .join(", ");
        response_headers.insert(name.to_string(), json!(value));
    }
    let mut response = Map::new();
    response.insert("status".into(), json!(status));
    response.insert("statusText".into(), json!(status_text(status)));
    response.insert("headers".into(), Value::Object(response_headers));
    response.insert("encoding".into(), json!(encoding));
    response.insert("totalBytes".into(), json!(bytes.len()));
    if bytes.len() > PANEL_PART_BYTES {
        let id = random_uuid();
        response.insert("id".into(), json!(id));
        state.held.lock().expect("panel state").hold(
            id,
            Held {
                path: path.clone(),
                response: response.clone(),
                bytes: bytes.clone(),
                expires: js::now() + HELD_MS,
            },
        );
    }
    Ok(ToolResult::new(
        format!("{status} {method} {path}"),
        part(&response, &bytes, 0),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_stay_on_the_server() {
        let base = "http://localhost:4173";
        let check = |path: &str| {
            checked_path(Some(&json!(path)), base).map_err(|e| e.message().to_string())
        };
        assert_eq!(check("/api/x?y=1").unwrap(), "/api/x?y=1");
        assert_eq!(check("x").unwrap_err(), "path must start with /");
        assert_eq!(
            check("//evil.example/").unwrap_err(),
            "path must stay on the app's server"
        );
        assert_eq!(
            check("/MCP.json").unwrap_err(),
            "/MCP.json is not available"
        );
        assert_eq!(
            check("/api/%73etup").unwrap_err(),
            "/api/%73etup is not available"
        );
        assert_eq!(check("/__vite").unwrap_err(), "/__vite is not available");
        assert_eq!(check("/%E0%A4%A").unwrap_err(), "path is not a valid path");
        assert_eq!(check("/api/setupx").unwrap(), "/api/setupx");
    }

    #[test]
    fn keys_and_base64() {
        assert!(is_panel_key(Some(&json!("abc")), Some("abc")));
        assert!(!is_panel_key(Some(&json!("abd")), Some("abc")));
        assert!(!is_panel_key(Some(&json!("")), Some("")));
        assert_eq!(from_base64("aGk=").unwrap(), b"hi");
        assert_eq!(from_base64("aGk").unwrap(), b"hi");
        assert!(from_base64("a").is_none());
        assert!(incompressible("image/png"));
        assert!(!incompressible("image/svg+xml"));
        assert!(incompressible("application/zip"));
    }

    #[test]
    fn parts_carry_offsets() {
        let bytes = vec![7u8; PANEL_PART_BYTES + 3];
        let first = part(&Map::new(), &bytes, 0);
        assert_eq!(first["nextOffset"], json!(PANEL_PART_BYTES));
        let rest = part(&Map::new(), &bytes, PANEL_PART_BYTES);
        assert!(rest.get("nextOffset").is_none());
        assert_eq!(rest["body"], json!("BwcH"));
    }
}
