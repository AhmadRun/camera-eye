//! `/api/realtime/debug-log` (port of `server/providers/openai/debug-log.js`).

use std::path::Path;

use axum::extract::Request;
use axum::response::Response;
use http::Method;
use serde_json::{Map, Value, json};
use tokio::io::AsyncWriteExt;

use super::{BodyError, js_stringify, parse_json_or_empty, peer, read_body_capped};
use crate::common::rate_limit::{RateLimiter, client_key};
use crate::common::reply;
use crate::common::same_site::admit_same_site;
use crate::state::AppState;

/// Cap on one request body.
pub const REALTIME_DEBUG_LOG_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Ceiling for the live log before it rotates (one `.1` generation is kept).
pub const REALTIME_DEBUG_LOG_MAX_FILE_BYTES: u64 = 32 * 1024 * 1024;

/// Per-IP write ceiling. Always on.
pub const REALTIME_DEBUG_LOG_MAX_PER_MIN: usize = 120;

const LOG_FAILED: &str = "Failed to write Realtime debug log";

pub struct DebugLog {
    allow: RateLimiter,
    /// Appends are serialized: the rotation check is a read-then-write on
    /// the file size.
    queue: tokio::sync::Mutex<()>,
}

impl DebugLog {
    pub fn new() -> Self {
        Self {
            allow: RateLimiter::new(60_000, REALTIME_DEBUG_LOG_MAX_PER_MIN, Some(400)),
            queue: tokio::sync::Mutex::new(()),
        }
    }

    async fn append(&self, root: &Path, line: &str) -> std::io::Result<()> {
        let _turn = self.queue.lock().await;
        let log_dir = root.join(".ce-logs");
        let log_file = log_dir.join("realtime-conversations.jsonl");
        tokio::fs::create_dir_all(&log_dir).await?;
        let size = tokio::fs::metadata(&log_file)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if size + line.len() as u64 > REALTIME_DEBUG_LOG_MAX_FILE_BYTES {
            let retained = log_dir.join("realtime-conversations.jsonl.1");
            match tokio::fs::remove_file(&retained).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            tokio::fs::rename(&log_file, &retained).await?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.flush().await
    }

    pub async fn handle(&self, state: &AppState, req: Request) -> Response {
        if let Some(refused) = admit_same_site(req.headers()) {
            return refused;
        }
        if req.method() != Method::POST {
            return reply::json(405, &json!({ "error": "Method not allowed" }), None);
        }
        if !self.allow.allow(&client_key(peer(&req))) {
            return reply::with_header(
                reply::json(429, &json!({ "error": "Rate limit exceeded" }), None),
                "Retry-After",
                "60",
            );
        }

        // A malformed record or an oversized body is the caller's fault (400);
        // a failed write is ours (500). Neither answer carries the error text.
        let caller_fault = || reply::json(400, &json!({ "error": LOG_FAILED }), None);
        let body = match read_body_capped(req.into_body(), REALTIME_DEBUG_LOG_MAX_BYTES).await {
            Ok(body) => body,
            Err(BodyError::TooLarge) => return caller_fault(),
            Err(BodyError::Io) => {
                tracing::warn!("[realtime-debug-log] append failed");
                return reply::json(500, &json!({ "error": LOG_FAILED }), None);
            }
        };
        let Ok(record) = parse_json_or_empty(&body) else {
            return caller_fault();
        };
        let line = format!("{}\n", js_stringify(&stamp(record)));
        match self.append(&state.root, &line).await {
            Ok(()) => {
                let mut response = Response::new(axum::body::Body::empty());
                *response.status_mut() = http::StatusCode::NO_CONTENT;
                response
            }
            Err(_) => {
                tracing::warn!("[realtime-debug-log] append failed");
                reply::json(500, &json!({ "error": LOG_FAILED }), None)
            }
        }
    }
}

/// `{ ...record, loggedAt: new Date().toISOString() }`: the server's own
/// timestamp comes last, so a record cannot supply one.
pub fn stamp(record: Value) -> Value {
    let mut out = match record {
        Value::Object(map) => map,
        Value::Array(items) => items
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i.to_string(), v))
            .collect(),
        Value::String(text) => text
            .encode_utf16()
            .enumerate()
            .map(|(i, unit)| {
                (
                    i.to_string(),
                    Value::String(String::from_utf16_lossy(&[unit])),
                )
            })
            .collect(),
        _ => Map::new(),
    };
    out.insert(
        "loggedAt".into(),
        Value::String(
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string(),
        ),
    );
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use serde_json::json;

    use super::super::testkit::{Call, call, state, temp_dir};
    use super::super::{Config, router_with};
    use super::*;

    fn app(root: &Path) -> Router {
        let state = state(root.to_path_buf());
        router_with(&state, Config::default()).with_state(state)
    }

    fn lines(root: &Path) -> Vec<Value> {
        let text = std::fs::read_to_string(root.join(".ce-logs/realtime-conversations.jsonl"))
            .unwrap_or_default();
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn records_land_under_the_supplied_root_with_a_server_timestamp() {
        let root = temp_dir("debug-log");
        let app = app(&root);
        let before = chrono::Utc::now().timestamp_millis();
        let reply = call(
            &app,
            Call::post(
                "/api/realtime/debug-log",
                r#"{"loggedAt":"1999-01-01T00:00:00.000Z","note":"x"}"#,
            ),
        )
        .await;
        assert_eq!(reply.status, 204);
        assert_eq!(reply.body, "");
        let record = &lines(&root)[0];
        assert_eq!(record["note"], "x");
        let logged = chrono::DateTime::parse_from_rfc3339(record["loggedAt"].as_str().unwrap())
            .unwrap()
            .timestamp_millis();
        assert!(logged >= before - 1000);
    }

    #[tokio::test]
    async fn the_sink_is_bounded_rate_limited_and_quiet_about_failures() {
        let root = temp_dir("debug-log-limit");
        let app = app(&root);
        let malformed = call(&app, Call::post("/api/realtime/debug-log", "{nope")).await;
        assert_eq!(malformed.status, 400);
        assert_eq!(malformed.json(), json!({ "error": LOG_FAILED }));

        let mut limited = None;
        let mut accepted = 0;
        for n in 0..130 {
            let reply = call(
                &app,
                Call::post("/api/realtime/debug-log", &json!({ "n": n }).to_string()),
            )
            .await;
            match reply.status {
                429 => {
                    limited = Some(reply);
                    break;
                }
                204 => accepted += 1,
                other => panic!("unexpected {other}"),
            }
        }
        let limited = limited.expect("the sink refuses a caller past its ceiling");
        assert_eq!(limited.header("retry-after"), Some("60"));
        assert_eq!(limited.json(), json!({ "error": "Rate limit exceeded" }));
        // The malformed record above already spent one of the 120 slots.
        assert_eq!(accepted, 119);
        let records = lines(&root);
        assert_eq!(records.len(), accepted);
        assert!(records.iter().all(|r| r["loggedAt"].is_string()));
    }

    #[tokio::test]
    async fn an_oversized_request_gets_the_fixed_error() {
        let root = temp_dir("debug-log-big");
        let app = app(&root);
        let body = "x".repeat(REALTIME_DEBUG_LOG_MAX_BYTES + 1);
        let reply = call(&app, Call::post("/api/realtime/debug-log", &body)).await;
        assert_eq!(reply.status, 400);
        assert_eq!(reply.json(), json!({ "error": LOG_FAILED }));
    }

    #[tokio::test]
    async fn the_log_rotates_instead_of_growing_without_bound() {
        let root = temp_dir("debug-log-rotate");
        let log = DebugLog::new();
        let pad = "x".repeat(1024 * 1024);
        for n in 0..72 {
            let line = format!("{}\n", json!({ "n": n, "pad": pad }));
            log.append(&root, &line).await.unwrap();
        }
        let dir = root.join(".ce-logs");
        let live = std::fs::metadata(dir.join("realtime-conversations.jsonl"))
            .unwrap()
            .len();
        let kept = std::fs::metadata(dir.join("realtime-conversations.jsonl.1"))
            .unwrap()
            .len();
        assert!(live <= REALTIME_DEBUG_LOG_MAX_FILE_BYTES);
        assert!(kept <= REALTIME_DEBUG_LOG_MAX_FILE_BYTES);
        let last = lines(&root).last().cloned().unwrap();
        assert_eq!(last["n"], 71);
    }

    #[test]
    fn non_object_records_spread_like_js() {
        let stamped = stamp(json!(["a", "b"]));
        assert_eq!(stamped["0"], "a");
        assert_eq!(stamped["1"], "b");
        assert!(stamped["loggedAt"].is_string());
        let stamped = stamp(json!(5));
        assert_eq!(stamped.as_object().unwrap().len(), 1);
    }
}
