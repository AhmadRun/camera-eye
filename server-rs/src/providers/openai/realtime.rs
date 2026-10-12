//! `/api/realtime/token` (port of `server/providers/openai/realtime.js`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::response::Response;
use http::Method;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::voice_cost::{MINI, STANDARD, is_known_voice_tier, resolve_voice_model};
use super::{OpenAi, js_number, peer};
use crate::common::js;
use crate::common::query::Params;
use crate::common::reply;
use crate::common::same_site::admit_same_site;
use crate::state::AppState;

pub const OPENAI_REALTIME_VOICE_DEFAULT: &str = "marin";
pub const OPENAI_REALTIME_REASONING_DEFAULT: &str = "low";
pub const OPENAI_REALTIME_CONTEXT_TOKENS_DEFAULT: f64 = 3000.0;
pub const OPENAI_REALTIME_CONTEXT_RETENTION_DEFAULT: f64 = 0.5;

/// Where `scripts/export-voice-tools.mjs` writes by default, under the root.
pub const SESSION_FILE_DEFAULT: &str = "server-rs/generated/realtime-session.json";

const TOKEN_FAILED: &str = "Failed to create Realtime token";

/// The generated session instructions and tools.
pub struct RealtimeSession {
    pub instructions: String,
    pub tools: Value,
}

/// Loaded on first use and kept once it loads, so running the export script
/// after the server started needs no restart.
#[derive(Default)]
pub struct SessionCache(Mutex<Option<Arc<RealtimeSession>>>);

impl SessionCache {
    fn get(&self, state: &AppState) -> Result<Arc<RealtimeSession>, String> {
        if let Some(session) = self.0.lock().as_ref() {
            return Ok(session.clone());
        }
        let path = session_path(state);
        let text = std::fs::read(&path).map_err(|e| {
            format!(
                "cannot read {} ({e}); run `node scripts/export-voice-tools.mjs`",
                path.display()
            )
        })?;
        let session = parse_session(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        let session = Arc::new(session);
        *self.0.lock() = Some(session.clone());
        Ok(session)
    }
}

fn session_path(state: &AppState) -> PathBuf {
    match state.env.get("CE_REALTIME_SESSION_FILE") {
        Some(custom) => state.root.join(custom),
        None => state.root.join(SESSION_FILE_DEFAULT),
    }
}

pub fn parse_session(bytes: &[u8]) -> Result<RealtimeSession, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let instructions = value
        .get("instructions")
        .and_then(Value::as_str)
        .ok_or("missing instructions")?
        .to_string();
    let tools = value.get("tools").cloned().unwrap_or(Value::Null);
    if !tools.is_array() {
        return Err("missing tools".into());
    }
    Ok(RealtimeSession {
        instructions,
        tools,
    })
}

/// `process.env[name] || fallback`.
fn env_or(state: &AppState, name: &str, fallback: &str) -> String {
    state
        .env
        .raw(name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

/// `Number(process.env[name]) || fallback`.
fn env_number_or(state: &AppState, name: &str, fallback: f64) -> f64 {
    let number = state.env.raw(name).map_or(f64::NAN, |v| js::number(&v));
    if number.is_nan() || number == 0.0 {
        fallback
    } else {
        number
    }
}

fn no_store(response: Response) -> Response {
    reply::with_header(response, "Cache-Control", "no-store")
}

pub async fn handle(svc: &OpenAi, state: &AppState, req: Request) -> Response {
    if let Some(refused) = admit_same_site(req.headers()) {
        return refused;
    }
    if req.method() != Method::GET && req.method() != Method::POST {
        return no_store(reply::json(
            405,
            &json!({ "error": "Method not allowed" }),
            None,
        ));
    }

    // Per-IP throttle (CE_RATELIMIT_OPENAI_PER_MIN). On by default; 0 disables.
    if let Some(limited) = svc.enforce_rate_limit(state, peer(&req)) {
        return no_store(limited);
    }

    let Some(api_key) = state.env.raw("OPENAI_API_KEY").filter(|k| !k.is_empty()) else {
        return no_store(reply::json(
            503,
            &json!({ "error": "OPENAI_API_KEY is not set" }),
            None,
        ));
    };

    // ?tier=standard|mini; anything else resolves to standard.
    let params = Params::parse(req.uri().query().unwrap_or(""));
    let requested_tier = params.get("tier");
    let tier = resolve_voice_model(requested_tier).tier;
    let model = if tier == MINI.tier {
        svc.config
            .model_mini
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| env_or(state, "OPENAI_REALTIME_MODEL_MINI", MINI.id))
    } else {
        svc.config
            .model_standard
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| env_or(state, "OPENAI_REALTIME_MODEL", STANDARD.id))
    };
    let voice = env_or(
        state,
        "OPENAI_REALTIME_VOICE",
        OPENAI_REALTIME_VOICE_DEFAULT,
    );
    let effort = env_or(
        state,
        "OPENAI_REALTIME_REASONING_EFFORT",
        OPENAI_REALTIME_REASONING_DEFAULT,
    );
    let context_token_limit = env_number_or(
        state,
        "OPENAI_REALTIME_CONTEXT_TOKENS",
        OPENAI_REALTIME_CONTEXT_TOKENS_DEFAULT,
    )
    .clamp(1000.0, 12000.0);
    let context_token_limit = (context_token_limit + 0.5).floor();
    let context_retention_ratio = env_number_or(
        state,
        "OPENAI_REALTIME_CONTEXT_RETENTION",
        OPENAI_REALTIME_CONTEXT_RETENTION_DEFAULT,
    )
    .clamp(0.1, 1.0);

    let fallback_header = requested_tier
        .filter(|t| !t.is_empty())
        .is_some_and(|t| !is_known_voice_tier(Some(t)));
    let echo = |response: Response| {
        let response = reply::with_header(response, "X-CE-Voice-Tier", tier);
        let response = reply::with_header(response, "X-CE-Voice-Model", &model);
        let response = if fallback_header {
            reply::with_header(response, "X-CE-Voice-Tier-Fallback", "1")
        } else {
            response
        };
        no_store(response)
    };
    let mint_failed = || {
        tracing::warn!("[realtime-token] mint failed");
        no_store(reply::json(502, &json!({ "error": TOKEN_FAILED }), None))
    };

    let session = match svc.session.get(state) {
        Ok(session) => session,
        Err(error) => {
            tracing::error!("[realtime-token] voice session definition unavailable: {error}");
            return mint_failed();
        }
    };
    let session_config = json!({
        "session": {
            "type": "realtime",
            "model": model,
            "reasoning": { "effort": effort },
            "truncation": {
                "type": "retention_ratio",
                "retention_ratio": js_number(context_retention_ratio),
                "token_limits": {
                    "post_instructions": js_number(context_token_limit),
                },
            },
            "audio": {
                "input": {
                    "noise_reduction": { "type": "near_field" },
                    "turn_detection": {
                        "type": "semantic_vad",
                        "eagerness": "low",
                        "create_response": true,
                        "interrupt_response": false,
                    },
                },
                "output": { "voice": voice },
            },
            "instructions": session.instructions,
            "tools": session.tools,
            "tool_choice": "auto",
        }
    });

    let result = async {
        let response = svc
            .http
            .post(&svc.config.realtime_endpoint)
            .timeout(Duration::from_secs(30))
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Content-Type", "application/json")
            .header("OpenAI-Safety-Identifier", "ce-local-dev")
            .body(serde_json::to_vec(&session_config).unwrap_or_default())
            .send()
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = response.bytes().await?;
        Ok::<_, reqwest::Error>((status, content_type, body))
    }
    .await;

    match result {
        // `redirect: 'error'`: a redirect is a failed fetch, never followed.
        Ok((status, _, _)) if status.is_redirection() => mint_failed(),
        Ok((status, content_type, body)) => {
            if !status.is_success() {
                tracing::warn!("[realtime-token] upstream HTTP {}", status.as_u16());
                return echo(reply::raw(
                    status.as_u16(),
                    "application/json; charset=utf-8",
                    serde_json::to_vec(&json!({ "error": TOKEN_FAILED })).unwrap_or_default(),
                    None,
                ));
            }
            echo(reply::raw(
                status.as_u16(),
                content_type.as_deref().unwrap_or("application/json"),
                body,
                None,
            ))
        }
        Err(_) => mint_failed(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::routing::post;
    use parking_lot::Mutex;
    use serde_json::{Value, json};

    use super::super::testkit::{Call, call, serve, state, temp_dir};
    use super::super::{Config, OPENAI_DEFAULT_PER_MIN, router_with};
    use super::SESSION_FILE_DEFAULT;

    type SentLog = Arc<Mutex<Vec<(Option<String>, Value)>>>;

    struct Upstream {
        url: String,
        sent: SentLog,
    }

    async fn upstream(status: u16, reply: Value) -> Upstream {
        let sent: SentLog = Arc::default();
        let log = sent.clone();
        let app = Router::new().route(
            "/client-secrets",
            post(move |headers: http::HeaderMap, body: String| {
                let log = log.clone();
                let reply = reply.clone();
                async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    log.lock()
                        .push((auth, serde_json::from_str(&body).unwrap_or(Value::Null)));
                    (
                        http::StatusCode::from_u16(status).unwrap(),
                        axum::Json(reply),
                    )
                }
            }),
        );
        let base = serve(app).await;
        Upstream {
            url: format!("{base}/client-secrets"),
            sent,
        }
    }

    fn fixture_root() -> std::path::PathBuf {
        let root = temp_dir("realtime");
        let file = root.join(SESSION_FILE_DEFAULT);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(
            &file,
            serde_json::to_vec(&json!({
                "instructions": "Fixture instructions.",
                "tools": [{ "type": "function", "name": "fly_to_location" }],
            }))
            .unwrap(),
        )
        .unwrap();
        root
    }

    fn app(root: std::path::PathBuf, config: Config, key: Option<&str>) -> Router {
        let state = state(root);
        state.env.set("OPENAI_API_KEY", key.unwrap_or(""));
        state.env.set("CE_RATELIMIT_OPENAI_PER_MIN", "");
        router_with(&state, config).with_state(state)
    }

    #[tokio::test]
    async fn mints_with_tools_and_instructions_and_keeps_the_key_server_side() {
        let upstream = upstream(200, json!({ "value": "fixture-ephemeral" })).await;
        let config = Config {
            realtime_endpoint: upstream.url.clone(),
            ..Config::default()
        };
        let app = app(fixture_root(), config, Some("fixture-upstream-secret"));
        let reply = call(
            &app,
            Call {
                uri: "/api/realtime/token?tier=unknown",
                ..Call::default()
            },
        )
        .await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.header("x-ce-voice-tier"), Some("standard"));
        assert_eq!(reply.header("x-ce-voice-model"), Some("gpt-realtime-2"));
        assert_eq!(reply.header("x-ce-voice-tier-fallback"), Some("1"));
        assert_eq!(reply.header("cache-control"), Some("no-store"));
        assert_eq!(reply.json(), json!({ "value": "fixture-ephemeral" }));
        assert!(!reply.body.contains("fixture-upstream-secret"));
        let sent = upstream.sent.lock();
        let (auth, body) = &sent[0];
        assert_eq!(auth.as_deref(), Some("Bearer fixture-upstream-secret"));
        let session = &body["session"];
        assert_eq!(session["instructions"], "Fixture instructions.");
        assert_eq!(session["tools"][0]["name"], "fly_to_location");
        assert_eq!(session["model"], "gpt-realtime-2");
        assert_eq!(session["audio"]["output"]["voice"], "marin");
        assert_eq!(session["reasoning"]["effort"], "low");
        assert_eq!(
            session["truncation"],
            json!({
                "type": "retention_ratio",
                "retention_ratio": 0.5,
                "token_limits": { "post_instructions": 3000 },
            })
        );
        assert_eq!(session["tool_choice"], "auto");
    }

    #[tokio::test]
    async fn mini_tier_and_configured_models_win() {
        let upstream = upstream(200, json!({ "value": "x" })).await;
        let config = Config {
            realtime_endpoint: upstream.url.clone(),
            model_standard: Some("configured-model".into()),
            ..Config::default()
        };
        let app = app(fixture_root(), config, Some("server-fixture"));
        let reply = call(
            &app,
            Call {
                uri: "/api/realtime/token?tier=arbitrary-model&model=other",
                ..Call::default()
            },
        )
        .await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.header("x-ce-voice-model"), Some("configured-model"));
        assert!(!reply.body.contains("server-fixture"));
        let mini = call(
            &app,
            Call {
                uri: "/api/realtime/token?tier=mini",
                method: "POST",
                ..Call::default()
            },
        )
        .await;
        assert_eq!(mini.header("x-ce-voice-tier"), Some("mini"));
        assert_eq!(
            mini.header("x-ce-voice-model"),
            Some("gpt-realtime-2.1-mini")
        );
        assert_eq!(mini.header("x-ce-voice-tier-fallback"), None);
        let sent = upstream.sent.lock();
        assert_eq!(sent[0].1["session"]["model"], "configured-model");
        assert_eq!(sent[1].1["session"]["model"], "gpt-realtime-2.1-mini");
    }

    #[tokio::test]
    async fn upstream_errors_answer_generically() {
        let leak = "fixture-upstream-secret req_fixture_1234 org-fixture quota exhausted";
        let upstream = upstream(429, json!({ "error": { "message": leak } })).await;
        let config = Config {
            realtime_endpoint: upstream.url.clone(),
            ..Config::default()
        };
        let app = app(fixture_root(), config, Some("fixture-upstream-secret"));
        let rejected = call(&app, Call::default_at("/api/realtime/token")).await;
        assert_eq!(rejected.status, 429);
        assert_eq!(
            rejected.header("content-type"),
            Some("application/json; charset=utf-8")
        );
        assert_eq!(
            rejected.json(),
            json!({ "error": "Failed to create Realtime token" })
        );
        assert!(!rejected.body.contains("req_fixture_1234"));

        // A network fault answers 502 without naming the upstream host.
        let config = Config {
            realtime_endpoint: "http://127.0.0.1:9/unreachable".into(),
            ..Config::default()
        };
        let app2 = self::app(fixture_root(), config, Some("fixture-upstream-secret"));
        let failed = call(&app2, Call::default_at("/api/realtime/token")).await;
        assert_eq!(failed.status, 502);
        assert_eq!(
            failed.json(),
            json!({ "error": "Failed to create Realtime token" })
        );
        assert!(!failed.body.contains("127.0.0.1"));
    }

    #[tokio::test]
    async fn keyless_method_and_cross_site_answers() {
        let app = app(fixture_root(), Config::default(), None);
        let keyless = call(&app, Call::default_at("/api/realtime/token")).await;
        assert_eq!(keyless.status, 503);
        assert_eq!(
            keyless.json(),
            json!({ "error": "OPENAI_API_KEY is not set" })
        );
        assert_eq!(keyless.header("cache-control"), Some("no-store"));

        let wrong = call(
            &app,
            Call {
                method: "DELETE",
                uri: "/api/realtime/token",
                ..Call::default()
            },
        )
        .await;
        assert_eq!(wrong.status, 405);
        assert_eq!(wrong.json(), json!({ "error": "Method not allowed" }));

        let mut cross = Call::default_at("/api/realtime/token");
        cross.headers.push(("origin", "https://evil.example"));
        let refused = call(&app, cross).await;
        assert_eq!(refused.status, 403);
        assert_eq!(
            refused.json(),
            json!({ "error": "Cross-origin requests are not accepted" })
        );
    }

    #[tokio::test]
    async fn the_token_route_throttles_by_default_and_says_so() {
        let upstream = upstream(200, json!({ "value": "fixture-ephemeral" })).await;
        let config = Config {
            realtime_endpoint: upstream.url.clone(),
            ..Config::default()
        };
        let app = app(fixture_root(), config, Some("fixture-upstream-secret"));
        for _ in 0..OPENAI_DEFAULT_PER_MIN {
            let reply = call(&app, Call::default_at("/api/realtime/token")).await;
            assert_eq!(reply.status, 200);
        }
        let over = call(&app, Call::default_at("/api/realtime/token")).await;
        assert_eq!(over.status, 429);
        assert_eq!(over.header("retry-after"), Some("5"));
        assert_eq!(over.json(), json!({ "error": "Rate limit exceeded" }));
        assert!(!over.body.contains("fixture-upstream-secret"));
        assert_eq!(upstream.sent.lock().len() as u64, OPENAI_DEFAULT_PER_MIN);
    }

    #[test]
    fn the_exported_session_parses_when_present() {
        // The real file exists only after `node scripts/export-voice-tools.mjs`.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("generated/realtime-session.json");
        if let Ok(bytes) = std::fs::read(path) {
            let session = super::parse_session(&bytes).unwrap();
            let tools = session.tools.as_array().unwrap();
            let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
            let unique: std::collections::HashSet<&&str> = names.iter().collect();
            assert_eq!(unique.len(), names.len());
            assert!(names.contains(&"get_weather"));
            assert!(names.contains(&"military_awareness"));
            assert!(!names.contains(&"show_in_camera_eye"));
            assert!(session.instructions.starts_with("You are CE Voice Control"));
        }
    }
}
