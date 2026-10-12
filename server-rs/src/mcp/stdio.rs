//! Port of `server/mcp/stdio.js`: newline-delimited JSON-RPC over stdin
//! and stdout. A client's `notifications/cancelled` aborts the named
//! request, which then gets no response.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures_util::future::{AbortHandle, Abortable};
use futures_util::task::noop_waker_ref;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::js;
use super::protocol::{McpServer, parse_error_response};

/// `describeRequest`: a request's method and the tool or resource it names.
pub fn describe_request(message: &Value) -> Option<String> {
    let method = message.get("method")?.as_str()?;
    let subject = match method {
        "tools/call" => message.get("params").and_then(|p| p.get("name")),
        "resources/read" => message.get("params").and_then(|p| p.get("uri")),
        _ => None,
    };
    Some(match subject.and_then(Value::as_str) {
        Some(subject) => format!("{method} {subject}"),
        None => method.to_string(),
    })
}

/// `parseArgs`: the `--api-base` value, if given.
pub fn parse_args(argv: &[String]) -> Result<Option<String>, String> {
    let mut api_base = None;
    let mut index = 0;
    while index < argv.len() {
        match argv.get(index + 1) {
            Some(value) if argv[index] == "--api-base" && !value.is_empty() => {
                api_base = Some(value.clone());
                index += 2;
            }
            _ => return Err(format!("Unknown argument: {}", argv[index])),
        }
    }
    Ok(api_base)
}

/// A request id as a `Map` key (SameValueZero).
#[derive(Clone, PartialEq, Eq, Hash)]
enum IdKey {
    Number(u64),
    Text(String),
    Bool(bool),
    /// Objects and arrays: never equal to an id parsed later.
    Unique(u64),
}

fn id_key(id: &Value, unique: &AtomicU64) -> Option<IdKey> {
    Some(match id {
        Value::Null => return None,
        Value::Number(n) => {
            let v = n.as_f64().unwrap_or(f64::NAN);
            IdKey::Number(if v == 0.0 {
                0f64.to_bits()
            } else {
                v.to_bits()
            })
        }
        Value::String(s) => IdKey::Text(s.clone()),
        Value::Bool(b) => IdKey::Bool(*b),
        _ => IdKey::Unique(unique.fetch_add(1, Ordering::SeqCst)),
    })
}

/// Splits input into lines at `\n`, `\r\n` or a lone `\r`, as readline does.
struct Lines {
    buffer: Vec<u8>,
    after_cr: bool,
}

impl Lines {
    fn push(&mut self, chunk: &[u8], out: &mut Vec<String>) {
        for &byte in chunk {
            match byte {
                b'\n' if self.after_cr => self.after_cr = false,
                b'\n' | b'\r' => {
                    self.after_cr = byte == b'\r';
                    out.push(String::from_utf8_lossy(&self.buffer).into_owned());
                    self.buffer.clear();
                }
                _ => {
                    self.after_cr = false;
                    self.buffer.push(byte);
                }
            }
        }
    }
}

type Output = Arc<tokio::sync::Mutex<Pin<Box<dyn AsyncWrite + Send>>>>;

async fn write_line(output: &Output, response: &Value) {
    let mut line = js::stringify(response);
    line.push('\n');
    let mut output = output.lock().await;
    let _ = output.write_all(line.as_bytes()).await;
    let _ = output.flush().await;
}

/// Serve newline-delimited JSON-RPC until `input` ends, then wait for the
/// requests still running.
pub async fn serve_stdio<R, W>(
    server: Arc<McpServer>,
    input: R,
    output: W,
    log: Arc<dyn Fn(&str) + Send + Sync>,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Send + 'static,
{
    let output: Output = Arc::new(tokio::sync::Mutex::new(Box::pin(output)));
    let running: Arc<Mutex<HashMap<IdKey, AbortHandle>>> = Arc::default();
    let unique = AtomicU64::new(0);
    let mut pending = tokio::task::JoinSet::new();
    let mut lines = Lines {
        buffer: Vec::new(),
        after_cr: false,
    };
    let mut input = input;
    let mut chunk = vec![0u8; 64 * 1024];
    let mut ready = Vec::new();
    let mut ended = false;
    while !ended {
        let read = input.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            ended = true;
            if !lines.buffer.is_empty() {
                ready.push(String::from_utf8_lossy(&lines.buffer).into_owned());
                lines.buffer.clear();
            }
        } else {
            lines.push(&chunk[..read], &mut ready);
        }
        for line in ready.drain(..) {
            if js::trim(&line).is_empty() {
                continue;
            }
            let Some(message) = js::parse_json(&line) else {
                let output = output.clone();
                pending.spawn(async move { write_line(&output, &parse_error_response()).await });
                continue;
            };
            if message.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
                let target = message.get("params").and_then(|p| p.get("requestId"));
                if let Some(key) = target.and_then(|id| id_key(id, &unique))
                    && let Some(handle) = running.lock().get(&key)
                {
                    handle.abort();
                }
                continue;
            }
            let described = describe_request(&message);
            if let Some(described) = &described {
                log(described);
            }
            let key = message
                .get("id")
                .and_then(|id| id_key(id, &unique))
                .filter(|key| !running.lock().contains_key(key));
            let (abort, registration) = AbortHandle::new_pair();
            if let Some(key) = &key {
                running.lock().insert(key.clone(), abort);
            }
            let task_server = server.clone();
            let work = async move { task_server.handle(&message).await };
            let mut work: Pin<Box<dyn Future<Output = _> + Send>> =
                Box::pin(Abortable::new(work, registration));
            // Answer what is ready at once (initialize, ping, lists), as the
            // JS server does before it reads the next line.
            let first = work
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()));
            let finish = {
                let output = output.clone();
                let running = running.clone();
                let log = log.clone();
                move |result: Result<Option<Value>, futures_util::future::Aborted>| async move {
                    if let Some(key) = &key {
                        running.lock().remove(key);
                    }
                    let Ok(Some(response)) = result else {
                        return;
                    };
                    if response
                        .get("result")
                        .and_then(|r| r.get("isError"))
                        .is_some_and(js::truthy)
                    {
                        let text = response["result"]
                            .get("content")
                            .and_then(|c| c.get(0))
                            .and_then(|c| c.get("text"))
                            .map_or("undefined".to_string(), js::to_js_string);
                        log(&format!(
                            "   {} failed: {text}",
                            described.as_deref().unwrap_or("null")
                        ));
                    }
                    write_line(&output, &response).await;
                }
            };
            match first {
                Poll::Ready(result) => finish(result).await,
                Poll::Pending => {
                    pending.spawn(async move {
                        let result = work.await;
                        finish(result).await;
                    });
                }
            }
        }
    }
    while pending.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn requests_are_described_without_arguments() {
        assert_eq!(
            describe_request(
                &json!({"method": "tools/call", "params": {"name": "get_earthquakes", "arguments": {"x": 1}}})
            ),
            Some("tools/call get_earthquakes".into())
        );
        assert_eq!(
            describe_request(
                &json!({"method": "resources/read", "params": {"uri": "ui://camera-eye/globe"}})
            ),
            Some("resources/read ui://camera-eye/globe".into())
        );
        assert_eq!(
            describe_request(&json!({"method": "ping"})),
            Some("ping".into())
        );
        assert_eq!(describe_request(&json!({"id": 1})), None);
    }

    #[test]
    fn arguments_name_the_api_base() {
        let args = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(parse_args(&args(&[])), Ok(None));
        assert_eq!(
            parse_args(&args(&["--api-base", "http://127.0.0.1:5173"])),
            Ok(Some("http://127.0.0.1:5173".into()))
        );
        assert_eq!(
            parse_args(&args(&["--api-base"])),
            Err("Unknown argument: --api-base".into())
        );
        assert_eq!(
            parse_args(&args(&["--port", "1"])),
            Err("Unknown argument: --port".into())
        );
    }

    #[test]
    fn lines_split_like_readline() {
        let mut lines = Lines {
            buffer: Vec::new(),
            after_cr: false,
        };
        let mut out = Vec::new();
        lines.push(b"a\r", &mut out);
        lines.push(b"\nb\rc\n\nd", &mut out);
        assert_eq!(out, vec!["a", "b", "c", ""]);
        assert_eq!(lines.buffer, b"d");
    }
}
