//! What Vite's preview server did around the API routes: the Host
//! allowlist, browser security headers, embed framing, static assets with
//! the single-page fallback, and the JSON 404 for unknown API routes.

use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http::{HeaderValue, header};
use tower_http::services::{ServeDir, ServeFile};

use crate::common::reply;
use crate::state::AppState;

/// Content-Security-Policy for every document (`BROWSER_CSP` in build/vite.js).
pub const BROWSER_CSP: &str = concat!(
    "default-src 'self'; ",
    "script-src 'self' 'unsafe-eval' blob: https://www.youtube.com https://connect.facebook.net https://platform.twitter.com; ",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; ",
    "font-src 'self' data: https://fonts.gstatic.com; ",
    "img-src 'self' data: blob: https:; ",
    "media-src 'self' blob: https:; ",
    "connect-src 'self' blob: data: https: wss: ws:; ",
    "worker-src 'self' blob:; ",
    "child-src 'self' blob:; ",
    "frame-src 'self' blob: https://www.youtube-nocookie.com https://www.youtube.com https://www.facebook.com https://platform.twitter.com; ",
    "manifest-src 'self'; ",
    "object-src 'none'; ",
    "base-uri 'self'; ",
    "form-action 'self'; ",
    "frame-ancestors 'none'"
);

pub const DEFAULT_ALLOWED_HOSTS: [&str; 2] = ["localhost", "127.0.0.1"];

/// `resolveAllowedHosts`: defaults plus explicit names, never wildcards.
pub fn resolve_allowed_hosts(configured: Option<&str>) -> Vec<String> {
    let mut hosts: Vec<String> = DEFAULT_ALLOWED_HOSTS
        .iter()
        .map(|h| h.to_string())
        .collect();
    for host in configured.unwrap_or("").split(',').map(str::trim) {
        if host.is_empty() || host.starts_with('.') || host.contains('*') {
            continue;
        }
        if !hosts.iter().any(|h| h == host) {
            hosts.push(host.to_string());
        }
    }
    hosts
}

/// `isAllowedHost`, by Vite's rule: IPs, `localhost` and `*.localhost`
/// always; then the configured server host and the allowlist.
pub fn is_allowed_host(
    host_header: Option<&str>,
    allowed: &[String],
    additional: &[String],
) -> bool {
    let host = host_header.unwrap_or("").trim();
    if host.is_empty() {
        return false;
    }
    if let Some(rest) = host.strip_prefix('[') {
        return match rest.find(']') {
            Some(end) if end > 0 => rest[..end].parse::<std::net::Ipv6Addr>().is_ok(),
            _ => false,
        };
    }
    let hostname = host.split(':').next().unwrap_or("").to_lowercase();
    if hostname.parse::<std::net::Ipv4Addr>().is_ok() {
        return true;
    }
    if hostname == "localhost" || hostname.ends_with(".localhost") {
        return true;
    }
    if additional.iter().any(|h| h == &hostname) {
        return true;
    }
    allowed.iter().any(|a| {
        a == &hostname
            || (a.starts_with('.') && (a[1..] == hostname || hostname.ends_with(a.as_str())))
    })
}

/// Refuse any Host the server does not allow, before every route.
pub async fn host_check(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let allowed = resolve_allowed_hosts(state.env.raw("CE_ALLOWED_HOSTS").as_deref());
    let additional: Vec<String> = state.env.get("HOST").into_iter().collect();
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| request.uri().authority().map(|a| a.to_string()));
    if is_allowed_host(host.as_deref(), &allowed, &additional) {
        return next.run(request).await;
    }
    reply::raw(
        403,
        "text/plain",
        "Blocked request. This host is not allowed.",
        None,
    )
}

/// Whether a request is for an embed-mode document.
pub fn is_embed_document_request(path: &str, query: Option<&str>) -> bool {
    let params = crate::common::query::Params::parse(query.unwrap_or(""));
    params.get("embed") == Some("1") && (path == "/" || path.ends_with(".html"))
}

/// Apply `BROWSER_HEADERS` to every response, and the embed framing policy
/// (`CE_EMBED_FRAME_ANCESTORS`) to embed-mode documents.
pub async fn browser_headers(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let embed = is_embed_document_request(request.uri().path(), request.uri().query());
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    let ancestors = state
        .env
        .raw("CE_EMBED_FRAME_ANCESTORS")
        .unwrap_or_default();
    let ancestors = ancestors.trim();
    if embed && !ancestors.is_empty() {
        let mut directives: Vec<String> = BROWSER_CSP
            .split(';')
            .map(str::trim)
            .filter(|d| !d.is_empty() && !d.to_lowercase().starts_with("frame-ancestors"))
            .map(str::to_string)
            .collect();
        if ancestors != "*" {
            directives.push(format!("frame-ancestors {ancestors}"));
        }
        headers.remove(header::X_FRAME_OPTIONS);
        if let Ok(value) = HeaderValue::from_str(&directives.join("; ")) {
            headers.insert(header::CONTENT_SECURITY_POLICY, value);
        }
    } else {
        headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
        headers.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(BROWSER_CSP),
        );
    }
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// `apiNotFoundPlugin`: unknown `/api` routes get JSON, not the HTML shell.
pub async fn api_not_found() -> Response {
    reply::error(404, "Unknown API route")
}

/// Built assets from `dist/`, falling back to `index.html` like Vite's SPA preview.
pub fn static_files(state: &AppState) -> Router<AppState> {
    let index = state.dist.join("index.html");
    let serve = ServeDir::new(&state.dist)
        .append_index_html_on_directories(true)
        .fallback(ServeFile::new(index));
    Router::new().fallback_service(serve)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_rules_match_vite() {
        let allowed = resolve_allowed_hosts(Some("app.example.com, .bad, *.x"));
        assert_eq!(allowed, vec!["localhost", "127.0.0.1", "app.example.com"]);
        assert!(is_allowed_host(Some("localhost:4173"), &allowed, &[]));
        assert!(is_allowed_host(Some("10.0.0.5:4173"), &allowed, &[]));
        assert!(is_allowed_host(Some("[::1]:4173"), &allowed, &[]));
        assert!(is_allowed_host(Some("app.example.com"), &allowed, &[]));
        assert!(is_allowed_host(Some("foo.localhost"), &allowed, &[]));
        assert!(!is_allowed_host(Some("evil.example.com"), &allowed, &[]));
        assert!(!is_allowed_host(None, &allowed, &[]));
    }

    #[test]
    fn embed_documents() {
        assert!(is_embed_document_request("/", Some("embed=1")));
        assert!(is_embed_document_request(
            "/index.html",
            Some("a=b&embed=1")
        ));
        assert!(!is_embed_document_request("/api/x", Some("embed=1")));
        assert!(!is_embed_document_request("/", None));
    }
}
