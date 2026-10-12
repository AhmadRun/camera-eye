//! The HTTP transport at `/mcp`: `server/mcp/plugin.js` (the local-request
//! gate, body limits and one server per Host) and
//! `src/tools/mcp/http.js` (one JSON-RPC message per POST).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode, header};
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::js;
use super::protocol::{MCP_PROTOCOL_VERSIONS, McpServer, parse_error_response};
use crate::common::same_site::has_proxy_signals;
use crate::state::AppState;

const MAX_BODY_BYTES: usize = 1024 * 1024;
const BODY_TIMEOUT: Duration = Duration::from_secs(30);
const LOOPBACK_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "[::1]"];
const LOOPBACK_ADDRESSES: [&str; 3] = ["127.0.0.1", "::1", "::ffff:127.0.0.1"];

static LOCAL_PORT: OnceLock<u16> = OnceLock::new();

/// Record the port the server listens on (`req.socket.localPort`).
pub fn set_local_port(port: u16) {
    let _ = LOCAL_PORT.set(port);
}

/// `isSharingEnabled(env)` from `src/keySetupCore.mjs`.
pub fn is_sharing_enabled(env: impl Fn(&str) -> Option<String>) -> bool {
    let share_var = env("PINOKIO_SHARE_VAR").unwrap_or_default();
    let share_var = js::trim(&share_var);
    ["PINOKIO_SHARE_CLOUDFLARE", "PINOKIO_SHARE_LOCAL"]
        .iter()
        .any(|name| {
            let value = env(name).unwrap_or_default();
            let value = js::trim(&value).to_ascii_lowercase();
            value == "1" || value == "true"
        })
        || (!share_var.is_empty() && share_var != "__ce_sharing_disabled__")
}

/// The request facts the gate reads.
pub struct LocalRequest<'a> {
    pub remote_address: Option<&'a str>,
    pub local_port: Option<u16>,
    pub host: &'a str,
    pub origin: Option<&'a str>,
    pub headers: &'a HeaderMap,
    pub sharing: bool,
}

/// `host` and `port` of `new URL(text)`.
fn url_host(url: &url::Url) -> Option<(String, Option<u16>)> {
    Some((url.host_str()?.to_string(), url.port()))
}

/// `isLocalMcpRequest`: loopback socket, no proxy, sharing off, a loopback
/// Host naming the port reached, and a same-host Origin when one is sent.
pub fn is_local_mcp_request(request: &LocalRequest<'_>) -> bool {
    if !request
        .remote_address
        .is_some_and(|address| LOOPBACK_ADDRESSES.contains(&address))
    {
        return false;
    }
    if has_proxy_signals(request.headers) || request.sharing {
        return false;
    }
    let Ok(host_url) = url::Url::parse(&format!("http://{}", request.host)) else {
        return false;
    };
    let Some((hostname, port)) = url_host(&host_url) else {
        return false;
    };
    if !LOOPBACK_HOSTS.contains(&hostname.as_str()) {
        return false;
    }
    if let Some(local) = request.local_port
        && port.unwrap_or(80) != local
    {
        return false;
    }
    let Some(origin) = request.origin else {
        return true;
    };
    let Ok(origin_url) = url::Url::parse(origin) else {
        return false;
    };
    if !matches!(origin_url.scheme(), "http" | "https") {
        return false;
    }
    let joined = |(name, port): (String, Option<u16>)| match port {
        Some(port) => format!("{name}:{port}"),
        None => name,
    };
    url_host(&origin_url).map(joined) == Some(joined((hostname, port)))
}

/// Whether a path is under the `/mcp` mount (Connect's prefix rule).
pub fn is_mcp_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 4
        && bytes[..4].eq_ignore_ascii_case(b"/mcp")
        && matches!(bytes.get(4), None | Some(b'/') | Some(b'.'))
}

fn json_response(status: u16, body: &Value) -> Response {
    let mut response = Response::new(Body::from(js::stringify(body)));
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn empty_response(status: u16) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    response
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    let values: Vec<String> = headers
        .get_all(name)
        .iter()
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(values.join(", "))
    }
}

/// `createMcpHttpHandler(server)` for one request.
pub async fn handle_mcp_request(
    server: &McpServer,
    method: &str,
    headers: &HeaderMap,
    body: Option<Bytes>,
) -> Response {
    if method != "POST" {
        let mut response = empty_response(405);
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
        return response;
    }
    if let Some(version) = header_text(headers, "mcp-protocol-version").filter(|v| !v.is_empty())
        && !MCP_PROTOCOL_VERSIONS.contains(&version.as_str())
    {
        return json_response(
            400,
            &json!({"error": format!("Unsupported MCP protocol version: {version}")}),
        );
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let json_type = content_type.len() >= 16
        && content_type.as_bytes()[..16].eq_ignore_ascii_case(b"application/json")
        && !content_type[16..]
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !json_type {
        return json_response(
            415,
            &json!({"error": "Content-Type must be application/json"}),
        );
    }
    let declared = header_text(headers, "content-length").map_or(0.0, |v| js::str_to_number(&v));
    if declared > MAX_BODY_BYTES as f64 {
        return json_response(413, &json!({"error": "Request too large"}));
    }
    let body = body.unwrap_or_default();
    if body.len() > MAX_BODY_BYTES {
        return json_response(413, &json!({"error": "Request too large"}));
    }
    let Some(message) = js::parse_json_bytes(&body) else {
        return json_response(400, &parse_error_response());
    };
    match server.handle(&message).await {
        Some(response) => json_response(200, &response),
        None => empty_response(202),
    }
}

/// Why reading a request body failed.
enum BodyError {
    TooLarge,
    TimedOut,
    Failed,
}

async fn read_body(body: Body) -> Result<Bytes, BodyError> {
    let read = async {
        let mut stream = body.into_data_stream();
        let mut out = Vec::new();
        while let Some(data) = stream.next().await {
            let data = data.map_err(|_| BodyError::Failed)?;
            if out.len() + data.len() > MAX_BODY_BYTES {
                return Err(BodyError::TooLarge);
            }
            out.extend_from_slice(&data);
        }
        Ok(Bytes::from(out))
    };
    tokio::time::timeout(BODY_TIMEOUT, read)
        .await
        .unwrap_or(Err(BodyError::TimedOut))
}

/// One MCP server per API base (`http://<Host>`), as the plugin keeps.
fn server_for(api_base: &str) -> Result<Arc<McpServer>, String> {
    static SERVERS: OnceLock<Mutex<HashMap<String, Arc<McpServer>>>> = OnceLock::new();
    let servers = SERVERS.get_or_init(Default::default);
    if let Some(server) = servers.lock().get(api_base) {
        return Ok(server.clone());
    }
    let server = Arc::new(super::create_local_mcp_server(api_base)?);
    Ok(servers
        .lock()
        .entry(api_base.to_string())
        .or_insert(server)
        .clone())
}

/// Middleware that answers `/mcp` and passes every other request on.
pub async fn mount(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if !is_mcp_path(request.uri().path()) {
        return next.run(request).await;
    }
    let headers = request.headers().clone();
    let host = header_text(&headers, "host").unwrap_or_default();
    let remote = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip().to_string());
    let origin = header_text(&headers, "origin");
    let local = is_local_mcp_request(&LocalRequest {
        remote_address: remote.as_deref(),
        local_port: LOCAL_PORT.get().copied(),
        host: &host,
        origin: origin.as_deref(),
        headers: &headers,
        sharing: is_sharing_enabled(|name| state.env.raw(name)),
    });
    if !local {
        return json_response(
            403,
            &json!({"error": "The MCP server only accepts local requests"}),
        );
    }
    let bad_request = |error: BodyError| {
        let (status, message) = match error {
            BodyError::TooLarge => (413, "Request too large"),
            BodyError::TimedOut => (408, "Request timed out"),
            BodyError::Failed => (400, "Bad request"),
        };
        json_response(status, &json!({"error": message}))
    };
    let method = request.method().as_str().to_string();
    let body = if method == "POST" {
        match read_body(request.into_body()).await {
            Ok(body) => Some(body),
            Err(error) => return bad_request(error),
        }
    } else {
        None
    };
    // `new Request` refuses these methods.
    if ["CONNECT", "TRACE", "TRACK"].contains(&method.to_ascii_uppercase().as_str()) {
        return bad_request(BodyError::Failed);
    }
    match server_for(&format!("http://{host}")) {
        Ok(server) => handle_mcp_request(&server, &method, &headers, body).await,
        Err(_) => bad_request(BodyError::Failed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request<'a>(
        headers: &'a HeaderMap,
        host: &'a str,
        origin: Option<&'a str>,
    ) -> LocalRequest<'a> {
        LocalRequest {
            remote_address: Some("127.0.0.1"),
            local_port: Some(4173),
            host,
            origin,
            headers,
            sharing: false,
        }
    }

    #[tokio::test]
    async fn a_streamed_body_stops_being_read_past_the_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let produced = Arc::new(AtomicUsize::new(0));
        let counter = produced.clone();
        let chunk = Bytes::from(vec![b' '; 64 * 1024]);
        let stream = futures_util::stream::iter(0..128).map(move |_| {
            counter.fetch_add(chunk.len(), Ordering::SeqCst);
            Ok::<_, std::io::Error>(chunk.clone())
        });
        let result = read_body(Body::from_stream(stream)).await;
        assert!(matches!(result, Err(BodyError::TooLarge)));
        let read = produced.load(Ordering::SeqCst);
        assert!(read <= MAX_BODY_BYTES + 4 * 64 * 1024, "read {read}");
    }

    #[test]
    fn only_local_requests_reach_the_server() {
        let none = HeaderMap::new();
        assert!(is_local_mcp_request(&request(
            &none,
            "localhost:4173",
            None
        )));
        assert!(is_local_mcp_request(&request(
            &none,
            "127.0.0.1:4173",
            Some("http://127.0.0.1:4173")
        )));
        assert!(is_local_mcp_request(&request(&none, "[::1]:4173", None)));
        assert!(!is_local_mcp_request(&request(
            &none,
            "evil.test:4173",
            None
        )));
        assert!(!is_local_mcp_request(&request(
            &none,
            "localhost:5173",
            None
        )));
        assert!(!is_local_mcp_request(&request(
            &none,
            "localhost:4173",
            Some("http://evil.test")
        )));
        assert!(!is_local_mcp_request(&request(
            &none,
            "localhost:4173",
            Some("null")
        )));
        assert!(!is_local_mcp_request(&request(&none, "", None)));
        let mut remote = request(&none, "localhost:4173", None);
        remote.remote_address = Some("10.0.0.2");
        assert!(!is_local_mcp_request(&remote));
        let mut shared = request(&none, "localhost:4173", None);
        shared.sharing = true;
        assert!(!is_local_mcp_request(&shared));
        let mut proxied = HeaderMap::new();
        proxied.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        assert!(!is_local_mcp_request(&request(
            &proxied,
            "localhost:4173",
            None
        )));
        let mut default_port = request(&none, "localhost", None);
        default_port.local_port = Some(80);
        assert!(is_local_mcp_request(&default_port));
    }

    #[test]
    fn sharing_follows_the_launcher_variables() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert!(!is_sharing_enabled(env(&[])));
        assert!(is_sharing_enabled(env(&[(
            "PINOKIO_SHARE_LOCAL",
            " TRUE "
        )])));
        assert!(is_sharing_enabled(env(&[("PINOKIO_SHARE_VAR", "x")])));
        assert!(!is_sharing_enabled(env(&[(
            "PINOKIO_SHARE_VAR",
            "__ce_sharing_disabled__"
        )])));
    }

    #[test]
    fn the_mount_matches_connect_prefixes() {
        for path in ["/mcp", "/MCP", "/mcp/", "/mcp/x", "/mcp.json"] {
            assert!(is_mcp_path(path), "{path}");
        }
        for path in ["/mcpx", "/api/mcp", "/", "/mc"] {
            assert!(!is_mcp_path(path), "{path}");
        }
    }
}
