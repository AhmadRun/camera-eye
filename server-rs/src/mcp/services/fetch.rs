//! The tools' `fetch`: `createApiFetch` from `server/mcp/services.js`.
//! Relative `/api/...` paths resolve against the running Camera Eye server,
//! absolute URLs pass through, and `file:` URLs read the bundled data
//! directory. Responses expose what the JS sources read from `Response`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use http::HeaderMap;
use serde_json::Value;
use url::Url;

use crate::mcp::catalog::Failure;
use crate::mcp::js;

/// How a request treats redirects (`RequestInit.redirect`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Redirect {
    Follow,
    /// A redirect fails the request, as `fetch` rejects it.
    Error,
    /// The redirect response itself is returned.
    Manual,
}

/// Options for one request.
#[derive(Debug, Clone)]
pub struct Init {
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Bytes>,
    pub redirect: Redirect,
    pub timeout: Option<Duration>,
}

impl Default for Init {
    fn default() -> Self {
        Self {
            method: "GET".into(),
            headers: Vec::new(),
            body: None,
            redirect: Redirect::Follow,
            timeout: None,
        }
    }
}

impl Init {
    pub fn redirect(redirect: Redirect) -> Self {
        Self {
            redirect,
            ..Self::default()
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// `TypeError: fetch failed`, the error `fetch` rejects with.
pub fn fetch_failed() -> Failure {
    Failure::Type("fetch failed".into())
}

/// The error a request cancelled by `AbortSignal.timeout` rejects with.
pub fn timeout_failure() -> Failure {
    Failure::Other {
        message: "The operation was aborted due to timeout".into(),
        status: None,
        code: Some("TimeoutError".into()),
    }
}

enum Body {
    Remote(reqwest::Response),
    Local(Bytes),
}

/// A response, read once.
pub struct Response {
    pub status: u16,
    pub headers: HeaderMap,
    /// The final URL.
    pub url: String,
    body: Body,
    timeout_at: Option<tokio::time::Instant>,
}

impl Response {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// `response.headers.get(name)`: values of a repeated header joined.
    pub fn header(&self, name: &str) -> Option<String> {
        let values: Vec<String> = self
            .headers
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

    /// The whole body (`arrayBuffer()`).
    pub async fn bytes(self) -> Result<Bytes, Failure> {
        self.bytes_capped(usize::MAX)
            .await?
            .ok_or_else(|| Failure::other("Response too large"))
    }

    /// The body, or None as soon as it passes `limit` bytes.
    pub async fn bytes_capped(self, limit: usize) -> Result<Option<Bytes>, Failure> {
        let deadline = self.timeout_at;
        let read = async move {
            match self.body {
                Body::Local(bytes) => Ok(if bytes.len() > limit {
                    None
                } else {
                    Some(bytes)
                }),
                Body::Remote(response) => {
                    let mut stream = response.bytes_stream();
                    let mut out = BytesMut::new();
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.map_err(|_| Failure::Type("terminated".into()))?;
                        if out.len() + chunk.len() > limit {
                            return Ok(None);
                        }
                        out.extend_from_slice(&chunk);
                    }
                    Ok(Some(out.freeze()))
                }
            }
        };
        match deadline {
            Some(at) => tokio::time::timeout_at(at, read)
                .await
                .map_err(|_| timeout_failure())?,
            None => read.await,
        }
    }

    /// `response.text()`.
    pub async fn text(self) -> Result<String, Failure> {
        Ok(js::decode_text(&self.bytes().await?))
    }

    /// `response.json()`.
    pub async fn json(self) -> Result<Value, Failure> {
        let bytes = self.bytes().await?;
        js::parse_json_bytes(&bytes).ok_or_else(json_syntax_error)
    }

    /// The local-file response `readBundled` makes.
    fn local(status: u16, content_type: Option<&str>, body: Bytes, url: String) -> Self {
        let mut headers = HeaderMap::new();
        if let Some(kind) = content_type {
            headers.insert(
                http::header::CONTENT_TYPE,
                kind.parse().expect("static header"),
            );
        }
        Self {
            status,
            headers,
            url,
            body: Body::Local(body),
            timeout_at: None,
        }
    }
}

/// The `SyntaxError` `JSON.parse` throws.
pub fn json_syntax_error() -> Failure {
    Failure::Other {
        message: "Unexpected token in JSON".into(),
        status: None,
        code: Some("SyntaxError".into()),
    }
}

/// The fetch every tool service uses.
#[derive(Clone)]
pub struct Api {
    base: Url,
    follow: reqwest::Client,
    manual: reqwest::Client,
    bundled: PathBuf,
}

impl Api {
    /// `createApiFetch({ apiBase })`; the base must be an http(s) URL.
    pub fn new(api_base: &str, root: &Path) -> Result<Self, String> {
        let base = Url::parse(api_base).map_err(|_| "Invalid URL".to_string())?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(format!("apiBase must be an http(s) URL: {api_base}"));
        }
        let builder = || {
            reqwest::Client::builder()
                .user_agent("node")
                .connect_timeout(Duration::from_secs(30))
        };
        let follow = builder()
            .redirect(reqwest::redirect::Policy::limited(20))
            .build()
            .map_err(|e| e.to_string())?;
        let manual = builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            base,
            follow,
            manual,
            bundled: root.join("src/data/local_data"),
        })
    }

    /// The API base's origin, as `new URL(apiBase).origin`.
    pub fn origin(&self) -> String {
        self.base.origin().ascii_serialization()
    }

    /// The absolute URL a request for `input` reaches.
    pub fn resolve(&self, input: &str) -> Result<Url, Failure> {
        if input.starts_with('/') {
            self.base
                .join(input)
                .map_err(|_| Failure::Type("Invalid URL".into()))
        } else {
            Url::parse(input).map_err(|_| Failure::Type("Invalid URL".into()))
        }
    }

    /// A `file:` URL for a path under the bundled data directory, as the
    /// sources' module-relative `new URL(...)` resolves.
    pub fn bundled_url(&self, relative: &str) -> String {
        format!("file://bundled/{relative}")
    }

    /// `fetch(input, init)`.
    pub async fn fetch(&self, input: &str, init: Init) -> Result<Response, Failure> {
        if let Some(relative) = input.strip_prefix("file://bundled/") {
            return Ok(self.read_bundled(relative, input).await);
        }
        let url = self.resolve(input)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(fetch_failed());
        }
        let client = match init.redirect {
            Redirect::Follow => &self.follow,
            Redirect::Error | Redirect::Manual => &self.manual,
        };
        let method = http::Method::from_bytes(init.method.as_bytes())
            .map_err(|_| Failure::Type(format!("'{}' is not a valid HTTP method.", init.method)))?;
        let mut request = client.request(method, url);
        for (name, value) in &init.headers {
            request = request.header(name.as_str(), value.as_str());
        }
        if let Some(body) = init.body.clone() {
            request = request.body(body);
        }
        let timeout_at = init.timeout.map(|t| tokio::time::Instant::now() + t);
        let send = request.send();
        let response = match timeout_at {
            Some(at) => tokio::time::timeout_at(at, send)
                .await
                .map_err(|_| timeout_failure())?,
            None => send.await,
        }
        .map_err(|_| fetch_failed())?;
        let status = response.status().as_u16();
        if init.redirect == Redirect::Error
            && matches!(status, 301 | 302 | 303 | 307 | 308)
            && response.headers().contains_key("location")
        {
            return Err(fetch_failed());
        }
        Ok(Response {
            status,
            headers: response.headers().clone(),
            url: response.url().to_string(),
            body: Body::Remote(response),
            timeout_at,
        })
    }

    /// `GET input` with default options.
    pub async fn get(&self, input: &str) -> Result<Response, Failure> {
        self.fetch(input, Init::default()).await
    }

    /// `readBundled`: a file inside the bundled data directory, or a 404.
    async fn read_bundled(&self, relative: &str, url: &str) -> Response {
        let not_found = || Response::local(404, None, Bytes::new(), url.to_string());
        if relative
            .split('/')
            .any(|part| part == ".." || part.is_empty())
        {
            return not_found();
        }
        match tokio::fs::read(self.bundled.join(relative)).await {
            Ok(body) => {
                let kind = if relative.ends_with(".json") {
                    "application/json"
                } else {
                    "application/octet-stream"
                };
                Response::local(200, Some(kind), Bytes::from(body), url.to_string())
            }
            Err(_) => not_found(),
        }
    }
}
