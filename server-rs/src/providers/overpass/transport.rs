//! The configured Overpass chain (port of
//! `server/providers/overpass/transport.js`).

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::geometry::{SimplifyOptions, simplify_overpass_payload_body};
use super::{OVERPASS_TIMEOUT_MS, OVERPASS_USER_AGENT};
use crate::common::http::{UpstreamError, read_text_capped};
use crate::common::js;
use crate::providers::mapdata_support::{
    decode_uri_component, js_date_parse, js_json_number, js_truthy,
};

/// One Overpass answer, cached or relayed (`{status, body, contentType, ...}`).
#[derive(Clone, Debug, PartialEq)]
pub struct OverpassPayload {
    pub status: f64,
    pub body: String,
    pub content_type: String,
    pub endpoint: Option<String>,
    pub rate_limited: bool,
    pub runtime_error: bool,
    pub retry_after_ms: Option<f64>,
    pub cached_at: Option<f64>,
}

impl OverpassPayload {
    /// `overpassPayloadIsData`.
    pub fn is_data(&self) -> bool {
        self.status.is_finite()
            && self.status >= 200.0
            && self.status < 300.0
            && !self.rate_limited
            && !self.runtime_error
    }

    /// Read a stored payload (a disk cache file written by either server).
    pub fn from_json(value: &Value) -> Option<Self> {
        let map = value.as_object()?;
        let body = map.get("body")?.as_str()?.to_string();
        let cached_at = map
            .get("cachedAt")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite());
        Some(Self {
            status: crate::providers::mapdata_support::js_to_number(map.get("status")),
            body,
            content_type: map
                .get("contentType")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            endpoint: map
                .get("endpoint")
                .and_then(Value::as_str)
                .map(str::to_string),
            rate_limited: js_truthy(map.get("rateLimited")),
            runtime_error: js_truthy(map.get("runtimeError")),
            retry_after_ms: js_truthy(map.get("retryAfterMs"))
                .then(|| crate::providers::mapdata_support::js_to_number(map.get("retryAfterMs"))),
            cached_at,
        })
    }

    /// `JSON.stringify(payload)` in the JS key order.
    pub fn to_json(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("status".into(), js_json_number(self.status));
        map.insert("body".into(), Value::String(self.body.clone()));
        map.insert(
            "contentType".into(),
            Value::String(self.content_type.clone()),
        );
        if let Some(endpoint) = &self.endpoint {
            map.insert("endpoint".into(), Value::String(endpoint.clone()));
        }
        map.insert("rateLimited".into(), Value::Bool(self.rate_limited));
        if self.runtime_error {
            map.insert("runtimeError".into(), Value::Bool(true));
        }
        if let Some(retry) = self.retry_after_ms {
            map.insert("retryAfterMs".into(), js_json_number(retry));
        }
        if let Some(cached_at) = self.cached_at {
            map.insert("cachedAt".into(), js_json_number(cached_at));
        }
        Value::Object(map)
    }
}

/// The non-retryable capability answer shared by every Overpass route.
pub fn overpass_not_configured() -> OverpassPayload {
    OverpassPayload {
        status: 503.0,
        body: json!({
            "error": "Detailed OpenStreetMap queries are not configured",
            "code": "OVERPASS_NOT_CONFIGURED",
            "retryable": false,
        })
        .to_string(),
        content_type: "application/json".into(),
        endpoint: None,
        rate_limited: false,
        runtime_error: false,
        retry_after_ms: None,
        cached_at: None,
    }
}

fn refusal(status: u16, retry_after_ms: f64) -> OverpassPayload {
    OverpassPayload {
        status: f64::from(status),
        body: json!({
            "error": "Configured Overpass upstream unavailable",
            "code": "OVERPASS_UNAVAILABLE",
            "retryable": true,
            "retryAfterMs": js_json_number(retry_after_ms),
        })
        .to_string(),
        content_type: "application/json".into(),
        endpoint: None,
        rate_limited: status == 429 || status == 406,
        runtime_error: false,
        retry_after_ms: Some(retry_after_ms),
        cached_at: None,
    }
}

fn looks_rate_limited(body: &str) -> bool {
    let text = body.to_lowercase();
    text.contains("rate_limited")
        || text.contains("quota of your ip address")
        || text.contains("dispatcher_client::request_read_and_idx::rate_limited")
        || text.contains("too many requests")
}

fn looks_runtime_error(body: &str) -> bool {
    let text = body.to_lowercase();
    text.contains("runtime error") || text.contains("timed out") || text.contains("out of memory")
}

/// Honor Retry-After seconds or dates; otherwise bounded exponential backoff.
fn retry_delay(value: Option<&str>, failures: u32, now: f64) -> f64 {
    let explicit = match value {
        Some(text) if !text.is_empty() && js::number(text).is_finite() => js::number(text) * 1000.0,
        Some(text) => js_date_parse(text).map_or(f64::NAN, |ms| ms as f64 - now),
        None => f64::NAN,
    };
    if explicit.is_finite() {
        explicit.max(1000.0)
    } else {
        (30_000.0 * 2f64.powi(failures.min(4) as i32)).min(300_000.0)
    }
}

/// What one POST to an endpoint returned (the body already read, capped).
pub struct RawReply {
    pub status: u16,
    pub retry_after: Option<String>,
    pub body: String,
}

/// The HTTP side of the chain; a seam so tests never touch the network.
pub trait OverpassHttp: Send + Sync {
    /// POST `body` to `url`. `Err` stands for any thrown fetch/read error.
    fn post(
        &self,
        url: String,
        authorization: Option<String>,
        body: String,
        max_bytes: usize,
    ) -> BoxFuture<'static, Result<RawReply, UpstreamError>>;
}

/// The production transport: no redirects (`redirect: 'error'`), 22 s for
/// the whole exchange, capped body.
pub struct ReqwestOverpass {
    pub client: reqwest::Client,
}

impl OverpassHttp for ReqwestOverpass {
    fn post(
        &self,
        url: String,
        authorization: Option<String>,
        body: String,
        max_bytes: usize,
    ) -> BoxFuture<'static, Result<RawReply, UpstreamError>> {
        let client = self.client.clone();
        Box::pin(async move {
            let timeout = Duration::from_millis(OVERPASS_TIMEOUT_MS);
            let exchange = async {
                let mut request = client
                    .post(&url)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("User-Agent", OVERPASS_USER_AGENT)
                    .body(body);
                if let Some(auth) = authorization {
                    request = request.header("Authorization", auth);
                }
                let response = request.send().await?;
                let status = response.status().as_u16();
                if matches!(status, 301 | 302 | 303 | 307 | 308) {
                    return Err(UpstreamError::Network("redirect refused".into()));
                }
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let body = read_text_capped(response, max_bytes).await?;
                Ok(RawReply {
                    status,
                    retry_after,
                    body,
                })
            };
            tokio::time::timeout(timeout, exchange)
                .await
                .map_err(|_| UpstreamError::Timeout)?
        })
    }
}

#[derive(Clone, Copy)]
struct Cooldown {
    until: f64,
    failures: u32,
    status: u16,
}

/// The configured chain with its per-endpoint cooldowns (module state in JS).
pub struct OverpassTransport {
    http: Arc<dyn OverpassHttp>,
    cooldowns: Mutex<Vec<(String, Cooldown)>>,
    now: Arc<dyn Fn() -> f64 + Send + Sync>,
    simplify: SimplifyOptions,
}

impl OverpassTransport {
    pub fn new(http: Arc<dyn OverpassHttp>) -> Self {
        Self {
            http,
            cooldowns: Mutex::new(Vec::new()),
            now: Arc::new(|| js::now_ms() as f64),
            simplify: SimplifyOptions::default(),
        }
    }

    #[cfg(test)]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> f64 + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    fn cooldown(&self, endpoint: &str) -> Option<Cooldown> {
        self.cooldowns
            .lock()
            .iter()
            .find(|(e, _)| e == endpoint)
            .map(|(_, c)| *c)
    }

    fn set_cooldown(&self, endpoint: &str, cooldown: Cooldown) {
        let mut list = self.cooldowns.lock();
        if let Some(slot) = list.iter_mut().find(|(e, _)| e == endpoint) {
            slot.1 = cooldown;
        } else {
            list.push((endpoint.to_string(), cooldown));
        }
        while list.len() > 64 {
            list.remove(0);
        }
    }

    fn clear_cooldown(&self, endpoint: &str) {
        self.cooldowns.lock().retain(|(e, _)| e != endpoint);
    }

    /// `fetchOverpassPayload(body, maxResponseBytes, {endpoints})`.
    pub async fn fetch(
        &self,
        body: &str,
        max_response_bytes: usize,
        endpoints: &[String],
    ) -> OverpassPayload {
        if endpoints.is_empty() {
            return overpass_not_configured();
        }
        let mut failure = refusal(502, 30_000.0);
        for endpoint in endpoints {
            let previous = self.cooldown(endpoint);
            if let Some(previous) = previous {
                let now = (self.now)();
                if previous.until > now {
                    failure = refusal(previous.status, previous.until - (self.now)());
                    continue;
                }
            }
            let failures_before = previous.map_or(0, |p| p.failures);
            match self.attempt(endpoint, body, max_response_bytes).await {
                Attempt::Data(body) => {
                    self.clear_cooldown(endpoint);
                    return OverpassPayload {
                        status: f64::from(body.0),
                        body: body.1,
                        content_type: "application/json".into(),
                        endpoint: Some("configured".into()),
                        rate_limited: false,
                        runtime_error: false,
                        retry_after_ms: None,
                        cached_at: None,
                    };
                }
                Attempt::Refused {
                    status,
                    retry_after,
                } => {
                    let failures = failures_before + 1;
                    let now = (self.now)();
                    let delay = retry_delay(retry_after.as_deref(), failures - 1, now);
                    self.set_cooldown(
                        endpoint,
                        Cooldown {
                            until: (self.now)() + delay,
                            failures,
                            status,
                        },
                    );
                    failure = refusal(status, delay);
                }
                Attempt::Threw => {
                    let failures = failures_before + 1;
                    let delay = retry_delay(None, failures - 1, (self.now)());
                    self.set_cooldown(
                        endpoint,
                        Cooldown {
                            until: (self.now)() + delay,
                            failures,
                            status: 502,
                        },
                    );
                    failure = refusal(502, delay);
                }
            }
        }
        failure
    }

    async fn attempt(&self, endpoint: &str, body: &str, max_bytes: usize) -> Attempt {
        let Ok(mut url) = url::Url::parse(endpoint) else {
            return Attempt::Threw;
        };
        let authorization = if !url.username().is_empty() || url.password().is_some() {
            let (Some(user), Some(pass)) = (
                decode_uri_component(url.username()),
                decode_uri_component(url.password().unwrap_or("")),
            ) else {
                return Attempt::Threw;
            };
            Some(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
            ))
        } else {
            None
        };
        let _ = url.set_username("");
        let _ = url.set_password(None);
        let reply = match self
            .http
            .post(url.to_string(), authorization, body.to_string(), max_bytes)
            .await
        {
            Ok(reply) => reply,
            Err(_) => return Attempt::Threw,
        };
        let rate_limited =
            reply.status == 429 || reply.status == 406 || looks_rate_limited(&reply.body);
        if rate_limited || !(200..300).contains(&reply.status) || looks_runtime_error(&reply.body) {
            let status = if rate_limited {
                if reply.status == 406 { 406 } else { 429 }
            } else {
                502
            };
            return Attempt::Refused {
                status,
                retry_after: reply.retry_after,
            };
        }
        let Ok(parsed) = serde_json::from_str::<Value>(&reply.body) else {
            return Attempt::Threw;
        };
        if !parsed.get("elements").is_some_and(Value::is_array) || js_truthy(parsed.get("remark")) {
            return Attempt::Threw;
        }
        match simplify_overpass_payload_body(&reply.body, &self.simplify) {
            Ok(simplified) => Attempt::Data((reply.status, simplified)),
            Err(()) => Attempt::Threw,
        }
    }
}

enum Attempt {
    Data((u16, String)),
    Refused {
        status: u16,
        retry_after: Option<String>,
    },
    Threw,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    type Responder = dyn Fn(&str, usize) -> Result<RawReply, UpstreamError> + Send + Sync;

    /// A scripted transport recording the URLs it was given.
    pub struct Scripted {
        pub seen: Mutex<Vec<(String, Option<String>)>>,
        pub respond: Box<Responder>,
    }

    impl OverpassHttp for Scripted {
        fn post(
            &self,
            url: String,
            authorization: Option<String>,
            _body: String,
            max_bytes: usize,
        ) -> BoxFuture<'static, Result<RawReply, UpstreamError>> {
            self.seen.lock().push((url.clone(), authorization));
            let count = self.seen.lock().len();
            let result = (self.respond)(&url, count).and_then(|reply| {
                if reply.body.len() > max_bytes {
                    Err(UpstreamError::TooLarge)
                } else {
                    Ok(reply)
                }
            });
            Box::pin(async move { result })
        }
    }

    pub fn reply(status: u16, body: &str, retry_after: Option<&str>) -> RawReply {
        RawReply {
            status,
            retry_after: retry_after.map(str::to_string),
            body: body.to_string(),
        }
    }

    fn scripted(
        respond: impl Fn(&str, usize) -> Result<RawReply, UpstreamError> + Send + Sync + 'static,
    ) -> Arc<Scripted> {
        Arc::new(Scripted {
            seen: Mutex::new(Vec::new()),
            respond: Box::new(respond),
        })
    }

    fn clock(start: f64) -> (Arc<AtomicU64>, Arc<dyn Fn() -> f64 + Send + Sync>) {
        let cell = Arc::new(AtomicU64::new(start.to_bits()));
        let reader = Arc::clone(&cell);
        (
            cell,
            Arc::new(move || f64::from_bits(reader.load(Ordering::SeqCst))),
        )
    }

    const DATA: &str = "{\"elements\":[]}";

    #[test]
    fn only_clean_2xx_is_data() {
        let mut p = overpass_not_configured();
        for (status, data) in [
            (200.0, true),
            (204.0, true),
            (400.0, false),
            (406.0, false),
            (429.0, false),
            (502.0, false),
        ] {
            p.status = status;
            assert_eq!(p.is_data(), data, "{status}");
        }
        p.status = 200.0;
        p.runtime_error = true;
        assert!(!p.is_data());
        p.runtime_error = false;
        p.rate_limited = true;
        assert!(!p.is_data());
        p.rate_limited = false;
        p.status = f64::NAN;
        assert!(!p.is_data());
    }

    #[tokio::test]
    async fn unset_chain_refuses_without_io() {
        let http = scripted(|_, _| panic!("default transport performed I/O"));
        let transport = OverpassTransport::new(http);
        let payload = transport.fetch("data=x", 1024, &[]).await;
        assert_eq!(payload.status, 503.0);
        let body: Value = serde_json::from_str(&payload.body).unwrap();
        assert_eq!(
            body,
            json!({"error": "Detailed OpenStreetMap queries are not configured", "code": "OVERPASS_NOT_CONFIGURED", "retryable": false})
        );
    }

    #[tokio::test]
    async fn configured_failover_accepts_empty_data() {
        let endpoints = vec![
            "https://first.example/query?secret=one".to_string(),
            "http://localhost:12345/api".to_string(),
        ];
        let http = scripted(|url, n| {
            Ok(reply(
                if n == 1 { 406 } else { 200 },
                if n == 1 { url } else { DATA },
                None,
            ))
        });
        let transport = OverpassTransport::new(http.clone());
        let payload = transport.fetch("data=x", 1024, &endpoints).await;
        let seen: Vec<String> = http.seen.lock().iter().map(|(u, _)| u.clone()).collect();
        assert_eq!(seen, endpoints);
        assert_eq!(payload.body, DATA);
        assert_eq!(payload.endpoint.as_deref(), Some("configured"));
    }

    #[tokio::test]
    async fn refusals_honor_retry_after_across_queries() {
        for status in [406u16, 429] {
            let endpoint = format!("https://cooldown-{status}.example/private?token=secret");
            let (time, now) = clock(1_000_000.0);
            let http =
                scripted(move |url, _| Ok(reply(status, &format!("Refused {url}"), Some("90"))));
            let transport = OverpassTransport::new(http.clone()).with_clock(now);
            let endpoints = vec![endpoint];
            let first = transport.fetch("first", 1024, &endpoints).await;
            assert_eq!(first.status, f64::from(status));
            assert_eq!(first.retry_after_ms, Some(90_000.0));
            assert!(!first.to_json().to_string().contains("secret"));
            assert!(!first.to_json().to_string().contains("https:"));
            time.store(1_030_000f64.to_bits(), Ordering::SeqCst);
            let next = transport.fetch("different", 1024, &endpoints).await;
            assert_eq!(http.seen.lock().len(), 1);
            assert_eq!(next.retry_after_ms, Some(60_000.0));
            time.store(1_090_001f64.to_bits(), Ordering::SeqCst);
            transport.fetch("third", 1024, &endpoints).await;
            assert_eq!(http.seen.lock().len(), 2);
        }
    }

    #[tokio::test]
    async fn dated_retry_after_and_bounded_backoff() {
        let start = js_date_parse("2026-09-23T12:00:00Z").unwrap() as f64;
        let (time, now) = clock(start);
        let http = scripted(|_, _| Ok(reply(429, "busy", Some("Wed, 23 Sep 2026 12:02:00 GMT"))));
        let transport = OverpassTransport::new(http).with_clock(now.clone());
        let payload = transport
            .fetch("x", 1024, &["https://dated.example/api".into()])
            .await;
        assert_eq!(payload.retry_after_ms, Some(120_000.0));

        let http = scripted(|_, _| {
            Err(UpstreamError::Network(
                "https://bounded.example/secret".into(),
            ))
        });
        let transport = OverpassTransport::new(http).with_clock(now.clone());
        for _ in 0..8 {
            let reply = transport
                .fetch("x", 1024, &["https://bounded.example".into()])
                .await;
            let retry = reply.retry_after_ms.unwrap();
            assert!(retry <= 300_000.0);
            assert!(!reply.body.contains("secret") && !reply.body.contains("https:"));
            let current = now();
            time.store((current + retry + 1.0).to_bits(), Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn oversized_malformed_and_runtime_failures_fail_over() {
        let bodies = [
            "x".repeat(200),
            "{\"remark\":\"runtime error: timed out\",\"elements\":[]}".to_string(),
            "rate_limited".to_string(),
            "{}".to_string(),
            "not json".to_string(),
        ];
        for (i, bad) in bodies.into_iter().enumerate() {
            let endpoints = vec![
                format!("https://bad-{i}.example/"),
                format!("https://good-{i}.example/"),
            ];
            let http = scripted(move |_, n| Ok(reply(200, if n == 1 { &bad } else { DATA }, None)));
            let transport = OverpassTransport::new(http.clone());
            let payload = transport.fetch("data=x", 100, &endpoints).await;
            assert_eq!(payload.body, DATA);
            let seen: Vec<String> = http.seen.lock().iter().map(|(u, _)| u.clone()).collect();
            assert_eq!(seen, endpoints);
        }
    }

    #[tokio::test]
    async fn url_credentials_become_basic_auth_and_never_leak() {
        let http = scripted(|_, _| Ok(reply(200, DATA, None)));
        let transport = OverpassTransport::new(http.clone());
        let payload = transport
            .fetch(
                "data=x",
                1024,
                &[
                    "https://operator:private%20key@paid-credentials.example/api?token=secret"
                        .into(),
                ],
            )
            .await;
        assert_eq!(payload.status, 200.0);
        let (url, auth) = http.seen.lock()[0].clone();
        assert_eq!(url, "https://paid-credentials.example/api?token=secret");
        assert_eq!(
            auth.as_deref(),
            Some(
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("operator:private key")
                )
                .as_str()
            )
        );
        let text = payload.to_json().to_string();
        for word in ["private", "secret", "operator", "https:"] {
            assert!(!text.contains(word), "{word}");
        }
    }
}
