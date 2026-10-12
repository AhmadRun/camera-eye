//! OpenAI routes (port of `server/providers/openai.js` and
//! `server/providers/openai/*`):
//!
//! - `GET|POST /api/realtime/token`: mints a short-lived Realtime client
//!   secret so `OPENAI_API_KEY` stays server-side.
//! - `POST /api/openai/hud-summary`: a five-word HUD line from the Responses API.
//! - `POST /api/realtime/debug-log`: appends voice-session records to
//!   `<root>/.ce-logs/realtime-conversations.jsonl`.
//!
//! All three refuse cross-site requests first (`common::same_site`). The token
//! and HUD routes share one per-IP limiter (`CE_RATELIMIT_OPENAI_PER_MIN`,
//! default 30/min, `0` disables), built on first use like the JS.
//!
//! The Realtime session's instructions and tool definitions are generated
//! from the browser's own modules by `node scripts/export-voice-tools.mjs`,
//! which writes `server-rs/generated/realtime-session.json` under the
//! repository root (`state.root`). `CE_REALTIME_SESSION_FILE` points the
//! server at another file (relative paths resolve against `state.root`).

mod debug_log;
mod hud_summary;
mod realtime;
pub mod voice_cost;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::response::Response;
use axum::routing::{MethodRouter, any};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use once_cell::sync::OnceCell;
use serde_json::Value;

use crate::common::rate_limit::{RateLimiter, client_key, cost_rate_limiter};
use crate::common::reply;
use crate::state::AppState;

/// Requests/min/IP when `CE_RATELIMIT_OPENAI_PER_MIN` is unset.
pub const OPENAI_DEFAULT_PER_MIN: u64 = 30;

/// Upstream endpoints and overrides (the JS `realtime` options).
#[derive(Clone)]
pub struct Config {
    pub realtime_endpoint: String,
    pub responses_endpoint: String,
    /// `realtime.models.standard` / `.mini`: win over the env overrides.
    pub model_standard: Option<String>,
    pub model_mini: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            realtime_endpoint: "https://api.openai.com/v1/realtime/client_secrets".into(),
            responses_endpoint: "https://api.openai.com/v1/responses".into(),
            model_standard: None,
            model_mini: None,
        }
    }
}

pub struct OpenAi {
    config: Config,
    /// Redirects are refused, like `redirect: 'error'`.
    http: reqwest::Client,
    /// `undefined` until first use; `None` inside is the explicit `0` opt-out.
    limiter: OnceCell<Option<RateLimiter>>,
    debug_log: debug_log::DebugLog,
    session: realtime::SessionCache,
}

impl OpenAi {
    fn new(config: Config) -> Self {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            config,
            http,
            limiter: OnceCell::new(),
            debug_log: debug_log::DebugLog::new(),
            session: realtime::SessionCache::default(),
        }
    }

    /// `openAiRateLimiter()`: built lazily so a `.env` value is seen.
    fn limiter(&self, state: &AppState) -> Option<&RateLimiter> {
        self.limiter
            .get_or_init(|| {
                cost_rate_limiter(
                    state.env.raw("CE_RATELIMIT_OPENAI_PER_MIN").as_deref(),
                    OPENAI_DEFAULT_PER_MIN,
                )
            })
            .as_ref()
    }

    /// `enforceRateLimit`: `Some(429)` when over the cap.
    fn enforce_rate_limit(&self, state: &AppState, peer: Option<SocketAddr>) -> Option<Response> {
        let limiter = self.limiter(state)?;
        if limiter.allow(&client_key(peer)) {
            return None;
        }
        Some(reply::with_header(
            reply::json(
                429,
                &serde_json::json!({ "error": "Rate limit exceeded" }),
                None,
            ),
            "Retry-After",
            "5",
        ))
    }
}

pub fn router(state: &AppState) -> Router<AppState> {
    router_with(state, Config::default())
}

pub fn router_with(_state: &AppState, config: Config) -> Router<AppState> {
    let svc = Arc::new(OpenAi::new(config));
    let mut router = Router::new();
    router = mount(
        router,
        "/api/openai/hud-summary",
        handler(&svc, |svc, state, req| async move {
            hud_summary::handle(&svc, &state, req).await
        }),
    );
    router = mount(
        router,
        "/api/realtime/debug-log",
        handler(&svc, |svc, state, req| async move {
            svc.debug_log.handle(&state, req).await
        }),
    );
    mount(
        router,
        "/api/realtime/token",
        handler(&svc, |svc, state, req| async move {
            realtime::handle(&svc, &state, req).await
        }),
    )
}

fn handler<F, Fut>(svc: &Arc<OpenAi>, run: F) -> MethodRouter<AppState>
where
    F: Fn(Arc<OpenAi>, AppState, Request) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    let svc = svc.clone();
    any(move |State(state): State<AppState>, req: Request| {
        let svc = svc.clone();
        let run = run.clone();
        async move { run(svc, state, req).await }
    })
}

/// Connect's `use(path)` also answers every sub-path.
fn mount(
    router: Router<AppState>,
    path: &str,
    handler: MethodRouter<AppState>,
) -> Router<AppState> {
    router
        .route(path, handler.clone())
        .route(&format!("{path}/"), handler.clone())
        .route(&format!("{path}/{{*rest}}"), handler)
}

/// The socket peer, when the server was started with connect info.
fn peer(req: &Request) -> Option<SocketAddr> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0)
}

/// Why a request body could not be read.
#[derive(Debug)]
enum BodyError {
    /// `code: 'BODY_TOO_LARGE'`.
    TooLarge,
    Io,
}

/// `readRequestBody(req, maxBytes)`.
async fn read_body_capped(body: Body, max_bytes: usize) -> Result<Bytes, BodyError> {
    let mut stream = body.into_data_stream();
    let mut out = BytesMut::new();
    let mut too_large = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyError::Io)?;
        // Keep draining past the cap without retaining bytes, so the fixed
        // error response can still be delivered.
        if too_large {
            continue;
        }
        if out.len() + chunk.len() > max_bytes {
            too_large = true;
            out.clear();
            continue;
        }
        out.extend_from_slice(&chunk);
    }
    if too_large {
        return Err(BodyError::TooLarge);
    }
    Ok(out.freeze())
}

/// A JSON number the way JS writes it: integral values without a fraction.
fn js_number(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        Value::from(value as i64)
    } else {
        serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
    }
}

/// Re-serialize parsed JSON the way `JSON.stringify` would (`1.0` as `1`).
fn js_stringify(value: &Value) -> String {
    fn normalize(value: &Value) -> Value {
        match value {
            Value::Number(n) if n.is_f64() => js_number(n.as_f64().unwrap_or(f64::NAN)),
            Value::Array(items) => Value::Array(items.iter().map(normalize).collect()),
            Value::Object(map) => {
                Value::Object(map.iter().map(|(k, v)| (k.clone(), normalize(v))).collect())
            }
            other => other.clone(),
        }
    }
    serde_json::to_string(&normalize(value)).unwrap_or_else(|_| "null".into())
}

/// `JSON.parse(text || '{}')`.
fn parse_json_or_empty(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let text = String::from_utf8_lossy(bytes);
    if text.is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_str(&text)
}

#[cfg(test)]
pub(crate) mod testkit {
    //! Helpers shared by this module's tests.
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use http::Request;
    use tower::ServiceExt;

    use crate::env::Env;
    use crate::state::{AppState, Inner};

    pub fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ce-rs-{tag}-{}-{}-{}",
            std::process::id(),
            crate::common::js::now_ms(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub fn state(root: PathBuf) -> AppState {
        let http = reqwest::Client::builder().no_proxy().build().unwrap();
        AppState(Arc::new(Inner {
            http,
            env: Env::default(),
            dist: root.join("dist"),
            cache_dir: root.join(".ce-cache"),
            root,
        }))
    }

    pub struct Reply {
        pub status: u16,
        pub headers: http::HeaderMap,
        pub body: String,
    }

    impl Reply {
        pub fn json(&self) -> serde_json::Value {
            serde_json::from_str(&self.body).unwrap()
        }
        pub fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }
    }

    pub struct Call<'a> {
        pub method: &'a str,
        pub uri: &'a str,
        pub body: Vec<u8>,
        pub headers: Vec<(&'a str, &'a str)>,
        pub peer: &'a str,
    }

    impl Default for Call<'_> {
        fn default() -> Self {
            Self {
                method: "GET",
                uri: "/",
                body: Vec::new(),
                headers: vec![
                    ("host", "localhost:4173"),
                    ("content-type", "application/json"),
                ],
                peer: "127.0.0.1:50000",
            }
        }
    }

    impl<'a> Call<'a> {
        pub fn default_at(uri: &'a str) -> Self {
            Self {
                uri,
                ..Self::default()
            }
        }

        pub fn post(uri: &'a str, body: &str) -> Self {
            Self {
                method: "POST",
                uri,
                body: body.as_bytes().to_vec(),
                ..Self::default()
            }
        }
    }

    pub async fn call(router: &Router, call: Call<'_>) -> Reply {
        let mut request = Request::builder().method(call.method).uri(call.uri);
        for (name, value) in &call.headers {
            request = request.header(*name, *value);
        }
        let mut request = request.body(Body::from(call.body)).unwrap();
        let peer: SocketAddr = call.peer.parse().unwrap();
        request.extensions_mut().insert(ConnectInfo(peer));
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Reply {
            status,
            headers,
            body: String::from_utf8_lossy(&bytes).into_owned(),
        }
    }

    /// Serve `router` on an ephemeral loopback port; returns its base URL.
    pub async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_numbers_and_stringify() {
        assert_eq!(js_number(1.0), Value::from(1));
        assert_eq!(js_number(0.5), serde_json::json!(0.5));
        let parsed: Value = serde_json::from_str(r#"{"a":1.0,"b":[2.50,3],"c":"x"}"#).unwrap();
        assert_eq!(js_stringify(&parsed), r#"{"a":1,"b":[2.5,3],"c":"x"}"#);
    }

    #[tokio::test]
    async fn body_cap_reports_too_large() {
        let body = Body::from(vec![b'x'; 11]);
        assert!(matches!(
            read_body_capped(body, 10).await,
            Err(BodyError::TooLarge)
        ));
        let body = Body::from(vec![b'x'; 10]);
        assert_eq!(read_body_capped(body, 10).await.unwrap().len(), 10);
    }
}
