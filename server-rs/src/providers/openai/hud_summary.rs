//! `/api/openai/hud-summary` (port of `server/providers/openai/hud-summary.js`
//! and the server half of `src/hudSummaryResponse.js`).

use axum::extract::Request;
use axum::response::Response;
use http::Method;
use serde_json::{Value, json};

use super::{OpenAi, js_stringify, parse_json_or_empty, peer, read_body_capped};
use crate::common::reply;
use crate::common::same_site::admit_same_site;
use crate::state::AppState;

pub const OPENAI_HUD_SUMMARY_MODEL_DEFAULT: &str = "gpt-5-nano";

/// `HUD_SUMMARY_UNCONFIGURED_CODE`.
pub const HUD_SUMMARY_UNCONFIGURED_CODE: &str = "OPENAI_NOT_CONFIGURED";

/// Request body cap.
const HUD_SUMMARY_MAX_BODY_BYTES: usize = 64 * 1024;

const HUD_FAILED: &str = "OpenAI HUD summary request failed";

/// `HUD_SUMMARY_INSTRUCTIONS`.
pub const HUD_SUMMARY_INSTRUCTIONS: &str = concat!(
    "Write one concise intelligence-HUD summary for Camera Eye.",
    " Use only the supplied place, street, nearby-place, enabled-layer labels, and feedProvenance.",
    " Prefer the clearest named place and include a relevant enabled layer only when useful.",
    " If any enabled layer is not nominal, the five words MUST include that feedState token (STALE, DEGRADED, FALLBACK, LOADING, or UNAVAILABLE) and must not present the view as live.",
    " Do not infer from coordinates or invent a place or feed state.",
    " Output exactly five words with no title, punctuation, markdown, or introductory phrase."
);

/// `keylessHudSummaryResponse`: the graceful no-key answer, or `None`.
pub fn keyless_hud_summary_response(api_key: Option<&str>) -> Option<(u16, Value)> {
    if !api_key.unwrap_or("").trim().is_empty() {
        return None;
    }
    Some((
        200,
        json!({
            "configured": false,
            "code": HUD_SUMMARY_UNCONFIGURED_CODE,
            "error": null,
            "summary": null,
        }),
    ))
}

/// JS truthiness of a JSON value.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0 && !f.is_nan()),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// `String(value)` for the shapes a response text part can take.
fn js_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => super::js_stringify(&Value::Number(n.clone())),
        Value::Bool(b) => b.to_string(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
        Value::Null => "null".into(),
    }
}

/// `extractOpenAiResponseText`.
pub fn extract_open_ai_response_text(data: &Value) -> String {
    if let Some(text) = data.get("output_text").and_then(Value::as_str)
        && !text.trim().is_empty()
    {
        return text.trim().to_string();
    }
    let Some(output) = data.get("output").and_then(Value::as_array) else {
        return String::new();
    };
    output
        .iter()
        .flat_map(|item| {
            item.get("content")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        })
        .map(|part| {
            let text = part.get("text").unwrap_or(&Value::Null);
            if truthy(text) {
                return js_string(text);
            }
            let output_text = part.get("output_text").unwrap_or(&Value::Null);
            if truthy(output_text) {
                return js_string(output_text);
            }
            String::new()
        })
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

/// `toFiveWordHudSummary`: letters, digits, whitespace and hyphens only,
/// then at most five words.
pub fn to_five_word_hud_summary(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| {
            if c.is_alphabetic() || c.is_numeric() || c.is_whitespace() || c == '-' {
                c
            } else {
                ' '
            }
        })
        .collect();
    cleaned
        .split_whitespace()
        .take(5)
        .collect::<Vec<_>>()
        .join(" ")
}

fn json_utf8(status: u16, value: &Value, cache_control: Option<&str>) -> Response {
    reply::raw(
        status,
        "application/json; charset=utf-8",
        serde_json::to_vec(value).unwrap_or_default(),
        cache_control,
    )
}

pub async fn handle(svc: &OpenAi, state: &AppState, req: Request) -> Response {
    if let Some(refused) = admit_same_site(req.headers()) {
        return refused;
    }
    if req.method() != Method::POST {
        return reply::json(405, &json!({ "error": "Method not allowed" }), None);
    }

    let api_key = state.env.raw("OPENAI_API_KEY");
    if let Some((status, payload)) = keyless_hud_summary_response(api_key.as_deref()) {
        return json_utf8(status, &payload, Some("no-store"));
    }
    let api_key = api_key.unwrap_or_default();

    // The keyless answer above costs nothing, so it never spends quota.
    if let Some(limited) = svc.enforce_rate_limit(state, peer(&req)) {
        return limited;
    }

    let failed = || {
        tracing::warn!("[hud-summary] request failed");
        reply::json(502, &json!({ "error": HUD_FAILED }), None)
    };

    let Ok(body) = read_body_capped(req.into_body(), HUD_SUMMARY_MAX_BODY_BYTES).await else {
        return failed();
    };
    let Ok(context) = parse_json_or_empty(&body) else {
        return failed();
    };
    let model = state
        .env
        .raw("OPENAI_HUD_SUMMARY_MODEL")
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| OPENAI_HUD_SUMMARY_MODEL_DEFAULT.to_string());
    let request = json!({
        "model": model,
        "instructions": HUD_SUMMARY_INSTRUCTIONS,
        "input": js_stringify(&context),
        "reasoning": { "effort": "minimal" },
        "max_output_tokens": 100,
    });

    let result = async {
        let response = svc
            .http
            .post(&svc.config.responses_endpoint)
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&request).unwrap_or_default())
            .send()
            .await?;
        let status = response.status();
        // `response.json().catch(() => ({}))`
        let data = response
            .bytes()
            .await
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .unwrap_or_else(|| json!({}));
        Ok::<_, reqwest::Error>((status, data))
    }
    .await;

    let Ok((status, data)) = result else {
        return failed();
    };
    let ok = status.is_success();
    let summary = to_five_word_hud_summary(&extract_open_ai_response_text(&data));
    let code = if ok && !summary.is_empty() {
        200
    } else {
        status.as_u16()
    };
    if !ok {
        tracing::warn!("[hud-summary] upstream HTTP {}", status.as_u16());
    }
    json_utf8(
        code,
        &json!({
            "summary": if summary.is_empty() { Value::Null } else { Value::String(summary) },
            // Never relay OpenAI's own error wording.
            "error": if ok { Value::Null } else { Value::String(HUD_FAILED.into()) },
        }),
        Some("no-store"),
    )
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::routing::post;
    use serde_json::json;

    use super::super::testkit::{Call, call, serve, state, temp_dir};
    use super::super::{Config, OPENAI_DEFAULT_PER_MIN, router_with};
    use super::*;

    #[test]
    fn keyless_response_only_for_a_blank_key() {
        let (status, payload) = keyless_hud_summary_response(None).unwrap();
        assert_eq!(status, 200);
        assert_eq!(
            payload,
            json!({
                "configured": false,
                "code": "OPENAI_NOT_CONFIGURED",
                "error": null,
                "summary": null,
            })
        );
        assert!(keyless_hud_summary_response(Some("   ")).is_some());
        assert!(keyless_hud_summary_response(Some("sk-x")).is_none());
    }

    #[test]
    fn five_word_summaries() {
        assert_eq!(
            to_five_word_hud_summary("**Golden Gate Bridge, STALE flights — today!**"),
            "Golden Gate Bridge STALE flights"
        );
        assert_eq!(to_five_word_hud_summary("  "), "");
        assert_eq!(
            extract_open_ai_response_text(&json!({ "output_text": "  hi  " })),
            "hi"
        );
        assert_eq!(
            extract_open_ai_response_text(&json!({
                "output": [
                    { "content": [{ "text": "one" }, { "output_text": "two" }, {}] },
                    { "content": "nope" },
                ],
            })),
            "one two"
        );
        assert_eq!(extract_open_ai_response_text(&json!([1])), "");
        assert!(HUD_SUMMARY_INSTRUCTIONS.contains("feedState token"));
    }

    async fn upstream(status: u16, reply: Value) -> String {
        let app = Router::new().route(
            "/responses",
            post(move || {
                let reply = reply.clone();
                async move {
                    (
                        http::StatusCode::from_u16(status).unwrap(),
                        axum::Json(reply),
                    )
                }
            }),
        );
        format!("{}/responses", serve(app).await)
    }

    fn app(endpoint: String, key: &str, limit: &str) -> Router {
        let state = state(temp_dir("hud"));
        state.env.set("OPENAI_API_KEY", key);
        state.env.set("CE_RATELIMIT_OPENAI_PER_MIN", limit);
        let config = Config {
            responses_endpoint: endpoint,
            ..Config::default()
        };
        router_with(&state, config).with_state(state)
    }

    #[tokio::test]
    async fn the_paid_path_is_throttled_and_the_free_path_is_not() {
        let endpoint = upstream(200, json!({ "output_text": "five word hud summary here" })).await;
        let app = app(endpoint, "fixture-upstream-secret", "");
        let from = "10.9.9.9:1234";
        for _ in 0..OPENAI_DEFAULT_PER_MIN {
            let reply = call(
                &app,
                Call {
                    peer: from,
                    ..Call::post("/api/openai/hud-summary", "{}")
                },
            )
            .await;
            assert_eq!(reply.status, 200);
            assert_eq!(
                reply.json(),
                json!({ "summary": "five word hud summary here", "error": null })
            );
            assert_eq!(reply.header("cache-control"), Some("no-store"));
        }
        let over = call(
            &app,
            Call {
                peer: from,
                ..Call::post("/api/openai/hud-summary", "{}")
            },
        )
        .await;
        assert_eq!(over.status, 429);
        assert_eq!(over.header("retry-after"), Some("5"));
        assert_eq!(over.json(), json!({ "error": "Rate limit exceeded" }));
    }

    #[tokio::test]
    async fn keyless_answers_stay_successful_after_the_quota_is_spent() {
        let app = app("http://127.0.0.1:9/never".into(), "", "1");
        for _ in 0..2 {
            let reply = call(&app, Call::post("/api/openai/hud-summary", "")).await;
            assert_eq!(reply.status, 200);
            assert_eq!(
                reply.header("content-type"),
                Some("application/json; charset=utf-8")
            );
            assert_eq!(reply.header("cache-control"), Some("no-store"));
            assert_eq!(reply.json()["configured"], false);
        }
        // The token route still spends the shared quota.
        let first = call(&app, Call::default_at("/api/realtime/token")).await;
        assert_eq!(first.status, 503);
        let second = call(&app, Call::default_at("/api/realtime/token")).await;
        assert_eq!(second.status, 429);
    }

    #[tokio::test]
    async fn upstream_failures_are_generic() {
        let leak = "fixture-upstream-secret req_fixture_1234 org-fixture quota exhausted";
        let endpoint = upstream(429, json!({ "error": { "message": leak } })).await;
        let app = app(endpoint, "fixture-upstream-secret", "");
        let reply = call(
            &app,
            Call::post("/api/openai/hud-summary", r#"{"context":{}}"#),
        )
        .await;
        assert_eq!(reply.status, 429);
        assert_eq!(
            reply.json(),
            json!({ "summary": null, "error": "OpenAI HUD summary request failed" })
        );
        assert!(!reply.body.contains("req_fixture_1234"));

        let bad = call(&app, Call::post("/api/openai/hud-summary", "{nope")).await;
        assert_eq!(bad.status, 502);
        assert_eq!(
            bad.json(),
            json!({ "error": "OpenAI HUD summary request failed" })
        );
        let wrong = call(&app, Call::default_at("/api/openai/hud-summary")).await;
        assert_eq!(wrong.status, 405);
    }
}
