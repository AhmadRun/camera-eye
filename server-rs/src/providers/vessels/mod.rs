//! `/api/vessels`: the AISStream live vessel cache (port of
//! `server/providers/vessels/ais-live.js` and `ais-store.js`).
//!
//! AISStream has no browser CORS and needs a private key, so the server keeps
//! one backend websocket open and exposes a same-origin JSON snapshot. The
//! socket is driven by the watchdog on every request and on a 15 s
//! background tick, so recovery does not depend on browser traffic.

mod adapter;
mod store;
mod watchdog;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::{MethodRouter, any};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{Value, json};

use self::adapter::{Adapter, AdapterOptions, Transport, WsTransport};
use self::store::{AISSTREAM_CACHE_MAX, AISSTREAM_STALE_MS, AisStore};
use self::watchdog::{
    Clock, SilenceOverride, Snapshot, SystemClock, WatchdogEnv, WatchdogOptions,
    parse_silence_timeout_env,
};
use crate::common::query::Params;
use crate::common::reply;
use crate::providers::js_value::{number_value, round};
use crate::state::AppState;

const AISSTREAM_URL: &str = "wss://stream.aisstream.io/v0/stream";
const AISSTREAM_DEFAULT_MESSAGE_TYPES: [&str; 5] = [
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
    "StaticDataReport",
];
/// Silence is REPORTED quickly and ACTED ON slowly.
const AISSTREAM_SILENCE_REPORT_MS: f64 = 120_000.0;
const AISSTREAM_RECYCLE_RATIO: f64 = 2.5;
const AISSTREAM_BACKOFF_MS: [f64; 4] = [5_000.0, 15_000.0, 60_000.0, 300_000.0];
const AISSTREAM_DOWN_RETRY_MS: f64 = 900_000.0;
const AISSTREAM_AUTH_PROBE_MS: f64 = 3_600_000.0;
const AISSTREAM_TICK_MS: u64 = 15_000;

/// The environment-derived watchdog policy, resolved once.
#[derive(Clone)]
struct Policy {
    silence_watch: bool,
    report_ms: f64,
    recycle_ms: f64,
    url: String,
}

struct Vessels {
    state: AppState,
    store: Arc<AisStore>,
    transport: Arc<dyn Transport>,
    clock: Arc<dyn Clock>,
    policy: Mutex<Option<Policy>>,
    /// Built on first use and kept: it owns the socket-generation namespace.
    adapter: OnceCell<Arc<Adapter>>,
}

impl Vessels {
    fn new(state: AppState, transport: Arc<dyn Transport>, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            state,
            store: Arc::new(AisStore::default()),
            transport,
            clock,
            policy: Mutex::new(None),
            adapter: OnceCell::new(),
        })
    }

    fn key(&self) -> Option<String> {
        self.state
            .env
            .raw("AISSTREAM_API_KEY")
            .filter(|key| !key.is_empty())
    }

    /// `aisWatchdogPolicy`: read lazily, once.
    fn policy(&self) -> Policy {
        let mut cached = self.policy.lock();
        if let Some(policy) = cached.as_ref() {
            return policy.clone();
        }
        let env = &self.state.env;
        let set = |name: &str| env.raw(name).is_some_and(|v| !v.is_empty());
        let custom_subscription = set("AISSTREAM_BOUNDING_BOXES") || set("AISSTREAM_MESSAGE_TYPES");
        let raw = env.raw("AISSTREAM_SILENCE_TIMEOUT_MS");
        let silence_override = parse_silence_timeout_env(raw.as_deref(), &mut |message| {
            tracing::warn!("{message}");
        });
        let report_ms = match silence_override {
            SilenceOverride::Timeout(value) => value,
            _ => AISSTREAM_SILENCE_REPORT_MS,
        };
        let policy = Policy {
            silence_watch: match silence_override {
                SilenceOverride::Off => false,
                SilenceOverride::Timeout(_) => true,
                SilenceOverride::Default => !custom_subscription,
            },
            report_ms,
            recycle_ms: round(report_ms * AISSTREAM_RECYCLE_RATIO),
            url: env
                .raw("AISSTREAM_URL")
                .filter(|url| !url.is_empty())
                .unwrap_or_else(|| AISSTREAM_URL.to_string()),
        };
        *cached = Some(policy.clone());
        policy
    }

    fn budgets(&self) -> WatchdogOptions {
        let policy = self.policy();
        WatchdogOptions {
            stale_ms: Some(policy.report_ms),
            recycle_after_ms: Some(policy.recycle_ms),
            backoff_ms: Some(AISSTREAM_BACKOFF_MS.to_vec()),
            down_retry_ms: Some(AISSTREAM_DOWN_RETRY_MS),
            auth_probe_ms: Some(AISSTREAM_AUTH_PROBE_MS),
            start_generation: 0,
        }
    }

    fn adapter(self: &Arc<Self>) -> &Arc<Adapter> {
        self.adapter.get_or_init(|| {
            // Weak: the adapter must not keep its owner alive.
            let url_source = Arc::downgrade(self);
            let subscription_source = Arc::downgrade(self);
            let store = self.store.clone();
            let adapter = Adapter::new(AdapterOptions {
                transport: self.transport.clone(),
                resolve_url: Box::new(move || {
                    url_source
                        .upgrade()
                        .map_or_else(|| AISSTREAM_URL.to_string(), |v| v.policy().url)
                }),
                build_subscription: Box::new(move || {
                    subscription_source
                        .upgrade()
                        .map_or(Value::Null, |v| v.subscription())
                }),
                ingest: Box::new(move |envelope| store.ingest(envelope)),
                clock: self.clock.clone(),
            });
            adapter.set_watchdog_options(self.budgets());
            adapter
        })
    }

    /// Fingerprint the credential so a key change clears the terminal
    /// auth-failed state. Only a digest is kept, never the key.
    fn key_fingerprint(&self) -> Option<String> {
        let key = self.key()?;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        Some(format!("{:016x}", hasher.finish())[..12].to_string())
    }

    /// `ensureAisStreamConnection`: drive the watchdog once.
    fn ensure(self: &Arc<Self>) {
        let adapter = self.adapter();
        let policy = self.policy();
        adapter.ensure(&WatchdogEnv {
            has_key: self.key().is_some(),
            has_transport: true,
            silence_watch: policy.silence_watch,
            key_fingerprint: self.key_fingerprint(),
        });
    }

    /// `aisStreamStatusSnapshot`, safe before the first connect.
    fn status_snapshot(&self) -> Snapshot {
        if let Some(snapshot) = self.adapter.get().and_then(|a| a.snapshot()) {
            return snapshot;
        }
        let has_key = self.key().is_some();
        Snapshot {
            status: if has_key { "idle" } else { "missing-key" },
            error: (!has_key).then(|| "AISSTREAM_API_KEY is not set".to_string()),
            last_message_at: None,
            silent_for_ms: None,
            reconnect_attempt: 0,
            next_attempt_at: None,
            watchdog: "armed",
            stale_after_ms: AISSTREAM_SILENCE_REPORT_MS,
        }
    }

    /// `aisStreamSubscription`.
    fn subscription(&self) -> Value {
        let env = &self.state.env;
        let mut payload = serde_json::Map::new();
        if let Some(key) = env.raw("AISSTREAM_API_KEY") {
            payload.insert("APIKey".into(), Value::String(key));
        }
        let boxes = match env
            .raw("AISSTREAM_BOUNDING_BOXES")
            .filter(|v| !v.is_empty())
        {
            None => default_bounding_boxes(),
            Some(raw) => serde_json::from_str(&raw).unwrap_or_else(|_| {
                tracing::warn!("[AISStream] Invalid AISSTREAM_BOUNDING_BOXES; using default.");
                default_bounding_boxes()
            }),
        };
        payload.insert("BoundingBoxes".into(), boxes);
        let defaults = || json!(AISSTREAM_DEFAULT_MESSAGE_TYPES);
        let types = match env.raw("AISSTREAM_MESSAGE_TYPES").filter(|v| !v.is_empty()) {
            None => defaults(),
            Some(raw) => match serde_json::from_str::<Value>(&raw) {
                Ok(parsed) if parsed.is_array() => parsed,
                Ok(_) => defaults(),
                Err(_) => Value::Array(
                    raw.split(',')
                        .map(str::trim)
                        .filter(|entry| !entry.is_empty())
                        .map(|entry| Value::String(entry.to_string()))
                        .collect(),
                ),
            },
        };
        payload.insert("FilterMessageTypes".into(), types);
        Value::Object(payload)
    }
}

fn default_bounding_boxes() -> Value {
    json!([[[-90, -180], [90, 180]]])
}

pub fn router(state: &AppState) -> Router<AppState> {
    let vessels = Vessels::new(
        state.clone(),
        Arc::new(WsTransport),
        Arc::new(SystemClock::default()),
    );
    start_tick(&vessels);
    router_with(vessels)
}

/// The background watchdog tick (the first run lands one period in, like
/// `setInterval`). Without a runtime there is nothing to tick.
fn start_tick(vessels: &Arc<Vessels>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(vessels);
    runtime.spawn(async move {
        let period = Duration::from_millis(AISSTREAM_TICK_MS);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        loop {
            interval.tick().await;
            let Some(vessels) = weak.upgrade() else {
                return;
            };
            vessels.ensure();
        }
    });
}

fn router_with(vessels: Arc<Vessels>) -> Router<AppState> {
    let handler: MethodRouter<AppState> = any(move |State(_): State<AppState>, req: Request| {
        let vessels = vessels.clone();
        async move { handle(&vessels, &req) }
    });
    Router::new()
        .route("/api/vessels", handler.clone())
        .route("/api/vessels/", handler.clone())
        .route("/api/vessels/{*rest}", handler)
}

fn json_utf8(status: u16, value: &Value) -> Response {
    reply::raw(
        status,
        "application/json; charset=utf-8",
        serde_json::to_vec(value).unwrap_or_default(),
        Some("no-store"),
    )
}

fn handle(vessels: &Arc<Vessels>, req: &Request) -> Response {
    let response = respond(vessels, req);
    reply::with_header(response, "X-Feed-Source", "AISStream")
}

fn respond(vessels: &Arc<Vessels>, req: &Request) -> Response {
    vessels.ensure();
    let path = req.uri().path();
    let sub_path = path.strip_prefix("/api/vessels").unwrap_or(path);
    let params = Params::parse(req.uri().query().unwrap_or(""));

    // The mount prefix-matches every sub-path, so the track route must be
    // answered before the rows snapshot.
    if sub_path == "/track" || sub_path.starts_with("/track/") {
        let mmsi = params.get("mmsi").unwrap_or("").trim().to_string();
        let valid = (5..=10).contains(&mmsi.len()) && mmsi.bytes().all(|b| b.is_ascii_digit());
        if !valid {
            return json_utf8(
                400,
                &json!({ "error": "mmsi query param required", "samples": [] }),
            );
        }
        let samples = vessels.store.track(&mmsi);
        return json_utf8(
            200,
            &json!({
                "mmsi": mmsi,
                "samples": samples,
                "source": "AISStream (accumulated since server start)",
                "retainedSec": AISSTREAM_STALE_MS / 1000,
            }),
        );
    }

    let max_rows = params.clamp_int(
        "maxRows",
        1,
        AISSTREAM_CACHE_MAX as i64,
        AISSTREAM_CACHE_MAX as i64,
    );
    let rows = vessels
        .store
        .rows(usize::try_from(max_rows).unwrap_or(AISSTREAM_CACHE_MAX));
    let newest = rows
        .first()
        .and_then(|row| row.get("last_position_UTC"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map_or(Value::Null, |s| Value::String(s.to_string()));
    let feed = vessels.status_snapshot();
    let status = if vessels.key().is_some() { 200 } else { 503 };
    json_utf8(
        status,
        &json!({
            "rows": rows,
            "source": "AISStream",
            "status": feed.status,
            "error": feed.error,
            "refreshing": feed.status != "live",
            "newestPositionAt": newest,
            "lastMessageAt": feed.last_message_at_value(),
            "silentForMs": feed.silent_for_ms_value(),
            "reconnectAttempt": feed.reconnect_attempt,
            "nextAttemptAt": feed.next_attempt_at_value(),
            "staleAfterMs": number_value(feed.stale_after_ms),
            "watchdog": feed.watchdog,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::adapter::{EventSink, Frame, Socket, SocketEvent};
    use super::watchdog::tests::FakeClock;
    use super::*;
    use crate::providers::openai::testkit::{Call, call, state, temp_dir};

    /// Records connects; the test drives the sockets' events.
    #[derive(Default)]
    struct Recorder {
        sinks: Mutex<Vec<EventSink>>,
        sent: Arc<Mutex<Vec<String>>>,
    }

    struct Sent(Arc<Mutex<Vec<String>>>);

    impl Socket for Sent {
        fn send(&self, text: String) -> Result<(), String> {
            self.0.lock().push(text);
            Ok(())
        }
        fn terminate(&self) {}
    }

    impl Transport for Recorder {
        fn connect(&self, _url: &str, sink: EventSink) -> Result<Arc<dyn Socket>, String> {
            self.sinks.lock().push(sink);
            Ok(Arc::new(Sent(self.sent.clone())))
        }
    }

    fn setup(tag: &str) -> (AppState, Arc<Recorder>, Router) {
        let root: PathBuf = temp_dir(tag);
        let app_state = state(root);
        let recorder = Arc::new(Recorder::default());
        let vessels = Vessels::new(app_state.clone(), recorder.clone(), FakeClock::new());
        let router = router_with(vessels).with_state(app_state.clone());
        (app_state, recorder, router)
    }

    #[tokio::test]
    async fn keyless_rows_answer_503_with_the_feed_metadata() {
        let (app_state, recorder, router) = setup("vessels-keyless");
        app_state.env.set("AISSTREAM_API_KEY", "");
        let reply = call(&router, Call::default_at("/api/vessels")).await;
        assert_eq!(reply.status, 503);
        assert_eq!(reply.header("x-feed-source"), Some("AISStream"));
        assert_eq!(reply.header("cache-control"), Some("no-store"));
        assert_eq!(
            reply.header("content-type"),
            Some("application/json; charset=utf-8")
        );
        let body = reply.json();
        let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
        assert_eq!(
            keys,
            [
                "rows",
                "source",
                "status",
                "error",
                "refreshing",
                "newestPositionAt",
                "lastMessageAt",
                "silentForMs",
                "reconnectAttempt",
                "nextAttemptAt",
                "staleAfterMs",
                "watchdog"
            ]
        );
        assert_eq!(body["status"], "missing-key");
        assert_eq!(body["error"], "AISSTREAM_API_KEY is not set");
        assert_eq!(body["refreshing"], true);
        assert_eq!(body["staleAfterMs"], 120_000);
        assert_eq!(body["watchdog"], "armed");
        assert!(recorder.sinks.lock().is_empty());
    }

    #[tokio::test]
    async fn a_keyed_feed_connects_subscribes_and_serves_rows_and_tracks() {
        let (app_state, recorder, router) = setup("vessels-live");
        app_state.env.set("AISSTREAM_API_KEY", "test-key");
        let reply = call(&router, Call::default_at("/api/vessels")).await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.json()["status"], "connecting");
        let sink = recorder.sinks.lock()[0].clone();
        assert!(sink(SocketEvent::Open));
        let subscription: Value = serde_json::from_str(&recorder.sent.lock()[0]).unwrap();
        assert_eq!(
            subscription,
            json!({
                "APIKey": "test-key",
                "BoundingBoxes": [[[-90, -180], [90, 180]]],
                "FilterMessageTypes": AISSTREAM_DEFAULT_MESSAGE_TYPES,
            })
        );
        let frame = json!({
            "MessageType": "PositionReport",
            "MetaData": { "MMSI": 244060365, "latitude": 53.4, "longitude": 6.2, "time_utc": "2026-09-17 20:00:00 +0000 UTC" },
            "Message": { "PositionReport": { "Sog": 3.5 } },
        });
        assert!(sink(SocketEvent::Message(Frame::Text(frame.to_string()))));
        let reply = call(&router, Call::default_at("/api/vessels/?maxRows=5")).await;
        let body = reply.json();
        assert_eq!(body["status"], "live");
        assert_eq!(body["refreshing"], false);
        assert_eq!(body["error"], Value::Null);
        assert_eq!(body["rows"][0]["mmsi"], "244060365");
        assert_eq!(body["rows"][0]["speed"], 3.5);
        assert_eq!(body["newestPositionAt"], "2026-09-17T20:00:00.000Z");
        assert_eq!(body["silentForMs"], 0);
        assert!(body["lastMessageAt"].is_number());
        // Only one socket while live.
        call(&router, Call::default_at("/api/vessels")).await;
        assert_eq!(recorder.sinks.lock().len(), 1);

        let reply = call(
            &router,
            Call::default_at("/api/vessels/track?mmsi=244060365"),
        )
        .await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.header("x-feed-source"), Some("AISStream"));
        assert_eq!(
            reply.json(),
            json!({
                "mmsi": "244060365",
                "samples": [],
                "source": "AISStream (accumulated since server start)",
                "retainedSec": 1800,
            })
        );
        for uri in [
            "/api/vessels/track",
            "/api/vessels/track/x?mmsi=12",
            "/api/vessels/track?mmsi=abcdef",
        ] {
            let reply = call(&router, Call::default_at(uri)).await;
            assert_eq!(reply.status, 400, "{uri}");
            assert_eq!(
                reply.json(),
                json!({ "error": "mmsi query param required", "samples": [] })
            );
        }
    }

    #[tokio::test]
    async fn subscription_overrides_follow_the_js_parsers() {
        let (app_state, _recorder, _router) = setup("vessels-subscription");
        let vessels = Vessels::new(
            app_state.clone(),
            Arc::new(Recorder::default()),
            FakeClock::new(),
        );
        app_state.env.set("AISSTREAM_API_KEY", "k");
        app_state
            .env
            .set("AISSTREAM_BOUNDING_BOXES", "[[[50,0],[52,2]]]");
        app_state.env.set(
            "AISSTREAM_MESSAGE_TYPES",
            " PositionReport , ,ShipStaticData",
        );
        let subscription = vessels.subscription();
        assert_eq!(subscription["BoundingBoxes"], json!([[[50, 0], [52, 2]]]));
        assert_eq!(
            subscription["FilterMessageTypes"],
            json!(["PositionReport", "ShipStaticData"])
        );
        app_state.env.set("AISSTREAM_BOUNDING_BOXES", "not json");
        app_state.env.set("AISSTREAM_MESSAGE_TYPES", "{\"a\":1}");
        let subscription = vessels.subscription();
        assert_eq!(subscription["BoundingBoxes"], default_bounding_boxes());
        assert_eq!(
            subscription["FilterMessageTypes"],
            json!(AISSTREAM_DEFAULT_MESSAGE_TYPES)
        );
        // A custom subscription disarms the silence watch by default.
        let policy = vessels.policy();
        assert!(!policy.silence_watch);
        assert_eq!(policy.recycle_ms, 300_000.0);
        assert_eq!(policy.url, AISSTREAM_URL);
    }

    #[tokio::test]
    async fn a_silence_override_arms_the_watch_and_scales_recycle() {
        let (app_state, _recorder, _router) = setup("vessels-policy");
        let vessels = Vessels::new(
            app_state.clone(),
            Arc::new(Recorder::default()),
            FakeClock::new(),
        );
        app_state
            .env
            .set("AISSTREAM_MESSAGE_TYPES", "PositionReport");
        app_state.env.set("AISSTREAM_SILENCE_TIMEOUT_MS", "90000");
        let policy = vessels.policy();
        assert!(policy.silence_watch);
        assert_eq!(policy.report_ms, 90_000.0);
        assert_eq!(policy.recycle_ms, 225_000.0);
    }
}
