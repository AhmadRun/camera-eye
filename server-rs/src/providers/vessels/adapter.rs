//! Transport adapter for the AISStream watchdog (port of
//! `src/data/aisStreamAdapter.js`).
//!
//! The adapter owns sockets and translates watchdog actions into transport
//! calls; the watchdog owns policy and no I/O. Two ownership rules keep the
//! single-socket invariant when events arrive late or out of order:
//!
//!   1. Socket generations are monotonic for the adapter's lifetime and never
//!      reused, including across `dispose()`.
//!   2. Every mutation of the socket map is identity-checked: an entry is only
//!      deleted or acted on while the map still holds *that* socket.
//!
//! Liveness credit goes only to a frame that arrived on a socket we still own
//! and decoded into a real AIS record.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::store::{parse_date, string_value};
use super::watchdog::{
    Action, Clock, FailureDetail, FailureKind, Snapshot, Watchdog, WatchdogEnv, WatchdogOptions,
};
use crate::providers::js_value::{get, nullish, or};

/// Upper bound on a single frame before decoding.
pub const AIS_MAX_FRAME_BYTES: usize = 1_000_000;

/// The complete `AisMessageTypes` enum from AISStream's type definitions.
pub const AIS_RECOGNIZED_MESSAGE_TYPES: [&str; 25] = [
    "PositionReport",
    "UnknownMessage",
    "AddressedSafetyMessage",
    "AddressedBinaryMessage",
    "AidsToNavigationReport",
    "AssignedModeCommand",
    "BaseStationReport",
    "BinaryAcknowledge",
    "BinaryBroadcastMessage",
    "ChannelManagement",
    "CoordinatedUTCInquiry",
    "DataLinkManagementMessage",
    "DataLinkManagementMessageData",
    "ExtendedClassBPositionReport",
    "GroupAssignmentCommand",
    "GnssBroadcastBinaryMessage",
    "Interrogation",
    "LongRangeAisBroadcastMessage",
    "MultiSlotBinaryMessage",
    "SafetyBroadcastMessage",
    "ShipStaticData",
    "SingleSlotBinaryMessage",
    "StandardClassBPositionReport",
    "StandardSearchAndRescueAircraftReport",
    "StaticDataReport",
];

// --- Failure text patterns ------------------------------------------------
//
// A tiny backtracking matcher for the two case-insensitive JS regexes, so no
// regex crate is needed:
//   AUTH_TEXT = /(unauthoriz|unauthoris|forbidden|invalid\s*api[\s_-]*key|
//                invalid\s*key|bad\s*api[\s_-]*key|authentic|
//                api\s*key\s*(is\s*)?(invalid|required|missing|not\s*valid))/i
//   RATE_TEXT = /(rate[\s_-]*limit|too\s*many\s*(requests|connections)|
//                quota\s*exceeded|429)/i

enum P {
    Lit(&'static str),
    /// `\s*`
    Ws,
    /// `[\s_-]*`
    Sep,
    Opt(&'static [P]),
    Alt(&'static [&'static [P]]),
}

use P::{Alt, Lit, Opt, Sep, Ws};

const AUTH_TEXT: &[&[P]] = &[
    &[Lit("unauthoriz")],
    &[Lit("unauthoris")],
    &[Lit("forbidden")],
    &[Lit("invalid"), Ws, Lit("api"), Sep, Lit("key")],
    &[Lit("invalid"), Ws, Lit("key")],
    &[Lit("bad"), Ws, Lit("api"), Sep, Lit("key")],
    &[Lit("authentic")],
    &[
        Lit("api"),
        Ws,
        Lit("key"),
        Ws,
        Opt(&[Lit("is"), Ws]),
        Alt(&[
            &[Lit("invalid")],
            &[Lit("required")],
            &[Lit("missing")],
            &[Lit("not"), Ws, Lit("valid")],
        ]),
    ],
];

const RATE_TEXT: &[&[P]] = &[
    &[Lit("rate"), Sep, Lit("limit")],
    &[
        Lit("too"),
        Ws,
        Lit("many"),
        Ws,
        Alt(&[&[Lit("requests")], &[Lit("connections")]]),
    ],
    &[Lit("quota"), Ws, Lit("exceeded")],
    &[Lit("429")],
];

fn match_seq(pattern: &[P], s: &[char], i: usize, k: &dyn Fn(usize) -> bool) -> bool {
    let Some((head, rest)) = pattern.split_first() else {
        return k(i);
    };
    let next = |j: usize| match_seq(rest, s, j, k);
    match head {
        Lit(text) => {
            let mut j = i;
            for c in text.chars() {
                if s.get(j) != Some(&c) {
                    return false;
                }
                j += 1;
            }
            next(j)
        }
        Ws | Sep => {
            let allowed =
                |c: char| c.is_whitespace() || (matches!(head, Sep) && (c == '_' || c == '-'));
            let mut end = i;
            while end < s.len() && allowed(s[end]) {
                end += 1;
            }
            (i..=end).rev().any(next)
        }
        Opt(inner) => match_seq(inner, s, i, &next) || next(i),
        Alt(branches) => branches.iter().any(|branch| match_seq(branch, s, i, &next)),
    }
}

fn pattern_test(alternatives: &[&[P]], text: &str) -> bool {
    let chars: Vec<char> = text.to_lowercase().chars().collect();
    (0..=chars.len()).any(|start| {
        alternatives
            .iter()
            .any(|alt| match_seq(alt, &chars, start, &|_| true))
    })
}

/// `parseRetryAfterMs`: delta-seconds or an HTTP-date; 0 when absent.
pub fn parse_retry_after_ms(raw: Option<&str>, now_ms: f64) -> f64 {
    let Some(raw) = raw else { return 0.0 };
    let text = raw.trim();
    if text.is_empty() {
        return 0.0;
    }
    if text.bytes().all(|b| b.is_ascii_digit()) {
        return text.parse::<f64>().unwrap_or(0.0) * 1000.0;
    }
    match parse_date(text) {
        Some(date) => (date.timestamp_millis() as f64 - now_ms).max(0.0),
        None => 0.0,
    }
}

/// Input for [`classify_ais_failure`].
#[derive(Default)]
pub struct FailureInput<'a> {
    pub http_status: Option<u16>,
    pub message: Option<&'a str>,
    pub retry_after_header: Option<&'a str>,
    pub now_ms: f64,
}

/// `ws`-style "Unexpected server response: 401".
fn status_from_text(text: &str) -> Option<u16> {
    let lower = text.to_lowercase();
    let at = lower.find("unexpected server response:")?;
    let rest = lower[at + "unexpected server response:".len()..].trim_start();
    let digits: String = rest.chars().take(3).collect();
    (digits.len() == 3 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// `classifyAisFailure`: only transport faults may walk the fast ladder.
pub fn classify_ais_failure(input: &FailureInput<'_>) -> FailureDetail {
    let text = input.message.unwrap_or("").trim().to_string();
    let from_header = || Some(parse_retry_after_ms(input.retry_after_header, input.now_ms));
    let status = input.http_status.or_else(|| status_from_text(&text));
    let detail = |kind, message: String, retry_after_ms| FailureDetail {
        kind,
        message: Some(message),
        retry_after_ms,
    };
    match status {
        Some(code @ (401 | 403)) => {
            return detail(
                FailureKind::Auth,
                format!("AISStream rejected the API key (HTTP {code})"),
                None,
            );
        }
        Some(429) => {
            return detail(
                FailureKind::RateLimit,
                "AISStream rate-limited this key (HTTP 429)".into(),
                from_header(),
            );
        }
        Some(code) if code >= 400 => {
            return detail(
                FailureKind::Transport,
                format!("AISStream upgrade failed (HTTP {code})"),
                None,
            );
        }
        _ => {}
    }
    if pattern_test(AUTH_TEXT, &text) {
        return detail(FailureKind::Auth, text, None);
    }
    if pattern_test(RATE_TEXT, &text) {
        return detail(FailureKind::RateLimit, text, from_header());
    }
    let message = if text.is_empty() {
        "AISStream websocket error".to_string()
    } else {
        text
    };
    detail(FailureKind::Transport, message, None)
}

/// `aisEnvelopeMmsi`'s raw value: `MMSI ?? UserID ?? UserId ?? Mmsi`.
pub fn aisenvelope_mmsi_raw<'a>(metadata: &'a Value, body: &'a Value) -> Option<&'a Value> {
    nullish(&[
        get(Some(metadata), "MMSI"),
        get(Some(body), "UserID"),
        get(Some(body), "UserId"),
        get(Some(body), "Mmsi"),
    ])
}

/// `isRecognizedAisEnvelope`: a recognised MessageType, a body filed under
/// it, and an MMSI.
pub fn is_recognized_ais_envelope(envelope: &Value) -> bool {
    let Some(object) = envelope.as_object() else {
        return false;
    };
    let Some(message_type) = object.get("MessageType").and_then(Value::as_str) else {
        return false;
    };
    if !AIS_RECOGNIZED_MESSAGE_TYPES.contains(&message_type) {
        return false;
    }
    let Some(body) = get(object.get("Message"), message_type) else {
        return false;
    };
    if !(body.is_object() || body.is_array()) {
        return false;
    }
    let empty = Value::Object(serde_json::Map::new());
    let metadata = or(&[object.get("MetaData"), object.get("Metadata")]).unwrap_or(&empty);
    !string_value(aisenvelope_mmsi_raw(metadata, body)).is_empty()
}

/// `parseAisEnvelope`.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    Malformed,
    Error(String),
    Data(Value),
}

pub fn parse_ais_envelope(text: &str) -> Parsed {
    let Ok(envelope) = serde_json::from_str::<Value>(text) else {
        return Parsed::Malformed;
    };
    if !envelope.is_object() {
        return Parsed::Malformed;
    }
    let error = envelope.get("error");
    if crate::providers::js_value::truthy(error) {
        return Parsed::Error(crate::providers::js_value::to_js_string(
            error.unwrap_or(&Value::Null),
        ));
    }
    Parsed::Data(envelope)
}

// --- Transport ------------------------------------------------------------

/// One frame as the socket delivered it (tungstenite reassembles fragments).
pub enum Frame {
    Text(String),
    Binary(Vec<u8>),
}

impl Frame {
    fn byte_len(&self) -> usize {
        match self {
            Frame::Text(text) => text.len(),
            Frame::Binary(bytes) => bytes.len(),
        }
    }

    fn decode(self) -> String {
        match self {
            Frame::Text(text) => text,
            Frame::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        }
    }
}

/// The `ws` events the adapter listens to.
pub enum SocketEvent {
    Open,
    Message(Frame),
    UnexpectedResponse {
        status: u16,
        retry_after: Option<String>,
    },
    Error(String),
    Close,
}

/// Delivers a socket event; `false` tells the socket to hang itself up.
pub type EventSink = Arc<dyn Fn(SocketEvent) -> bool + Send + Sync>;

pub trait Socket: Send + Sync {
    fn send(&self, text: String) -> Result<(), String>;
    /// Hard-abort, never a graceful close.
    fn terminate(&self);
}

pub trait Transport: Send + Sync {
    fn connect(&self, url: &str, sink: EventSink) -> Result<Arc<dyn Socket>, String>;
}

pub struct AdapterOptions {
    pub transport: Arc<dyn Transport>,
    pub resolve_url: Box<dyn Fn() -> String + Send + Sync>,
    pub build_subscription: Box<dyn Fn() -> Value + Send + Sync>,
    /// True only when the envelope was a real AIS record.
    pub ingest: Box<dyn Fn(&Value) -> bool + Send + Sync>,
    pub clock: Arc<dyn Clock>,
}

type Owner = Arc<Mutex<Watchdog>>;

struct State {
    /// generation -> (socket identity, socket).
    sockets: HashMap<u64, (u64, Arc<dyn Socket>)>,
    /// Never reset, including across dispose().
    generation_high_water: u64,
    watchdog: Option<Owner>,
    next_socket_id: u64,
}

pub struct Adapter {
    options: AdapterOptions,
    state: Mutex<State>,
    me: Weak<Adapter>,
}

impl Adapter {
    pub fn new(options: AdapterOptions) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            options,
            state: Mutex::new(State {
                sockets: HashMap::new(),
                generation_high_water: 0,
                watchdog: None,
                next_socket_id: 0,
            }),
            me: me.clone(),
        })
    }

    /// (Re)build the state machine, preserving the generation namespace.
    pub fn set_watchdog_options(&self, options: WatchdogOptions) {
        let mut state = self.state.lock();
        Self::install_watchdog(&mut state, options, &self.options.clock);
    }

    fn install_watchdog(state: &mut State, options: WatchdogOptions, clock: &Arc<dyn Clock>) {
        if let Some(watchdog) = &state.watchdog {
            state.generation_high_water = state
                .generation_high_water
                .max(watchdog.lock().high_water_generation());
        }
        state.watchdog = Some(Arc::new(Mutex::new(Watchdog::new(
            WatchdogOptions {
                start_generation: state.generation_high_water,
                ..options
            },
            clock.clone(),
        ))));
    }

    /// Drive the machine once: re-declare the environment, then advance time.
    pub fn ensure(&self, env: &WatchdogEnv) {
        let mut state = self.state.lock();
        if state.watchdog.is_none() {
            Self::install_watchdog(&mut state, WatchdogOptions::default(), &self.options.clock);
        }
        let Some(owner) = state.watchdog.clone() else {
            return;
        };
        let actions = owner.lock().configure(env);
        self.run_actions(&mut state, &owner, actions);
        let actions = owner.lock().tick();
        self.run_actions(&mut state, &owner, actions);
    }

    /// Tear down sockets and machine state, keeping the generation namespace.
    #[allow(
        dead_code,
        reason = "the Rust server has no in-process restart; kept for parity and tests"
    )]
    pub fn dispose(&self) {
        let mut state = self.state.lock();
        if let Some(owner) = state.watchdog.clone() {
            let high = owner.lock().high_water_generation();
            state.generation_high_water = state.generation_high_water.max(high);
            let actions = owner.lock().dispose();
            self.run_actions(&mut state, &owner, actions);
        }
        for (_, (_, socket)) in state.sockets.drain() {
            socket.terminate();
        }
    }

    pub fn snapshot(&self) -> Option<Snapshot> {
        let state = self.state.lock();
        state.watchdog.as_ref().map(|w| w.lock().snapshot())
    }

    #[cfg(test)]
    fn live_sockets(&self) -> usize {
        self.state.lock().sockets.len()
    }

    fn run_actions(&self, state: &mut State, owner: &Owner, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Terminate { generation, .. } => {
                    if let Some((_, socket)) = state.sockets.remove(&generation) {
                        socket.terminate();
                    }
                }
                Action::Connect { generation } => self.open_socket(state, owner, generation),
            }
        }
    }

    fn fail_generation(
        &self,
        state: &mut State,
        owner: &Owner,
        generation: u64,
        detail: FailureDetail,
    ) {
        let actions = owner.lock().on_failure(generation, &detail);
        self.run_actions(state, owner, actions);
    }

    fn open_socket(&self, state: &mut State, owner: &Owner, generation: u64) {
        state.generation_high_water = state.generation_high_water.max(generation);
        state.next_socket_id += 1;
        let socket_id = state.next_socket_id;
        let me = self.me.clone();
        let handler_owner = owner.clone();
        let sink: EventSink = Arc::new(move |event| match me.upgrade() {
            Some(adapter) => adapter.handle(&handler_owner, generation, socket_id, event),
            None => false,
        });
        let url = (self.options.resolve_url)();
        match self.options.transport.connect(&url, sink) {
            Ok(socket) => {
                state.sockets.insert(generation, (socket_id, socket));
            }
            Err(message) => {
                let detail = classify_ais_failure(&FailureInput {
                    message: Some(&message),
                    ..FailureInput::default()
                });
                self.fail_generation(state, owner, generation, detail);
            }
        }
    }

    fn handle(&self, owner: &Owner, generation: u64, socket_id: u64, event: SocketEvent) -> bool {
        let mut state = self.state.lock();
        let owned = state
            .sockets
            .get(&generation)
            .filter(|(id, _)| *id == socket_id)
            .map(|(_, socket)| socket.clone());
        let now = self.options.clock.wall();
        match event {
            SocketEvent::Open => {
                let Some(socket) = owned else {
                    return false; // orphan-open
                };
                let actions = owner.lock().on_open(generation);
                if !actions.is_empty() {
                    self.run_actions(&mut state, owner, actions);
                    return true; // orphan — hung up, never subscribed
                }
                let subscription = (self.options.build_subscription)().to_string();
                if let Err(message) = socket.send(subscription) {
                    let detail = classify_ais_failure(&FailureInput {
                        message: Some(&message),
                        ..FailureInput::default()
                    });
                    self.fail_generation(&mut state, owner, generation, detail);
                }
                true
            }
            SocketEvent::Message(frame) => {
                let size = frame.byte_len();
                if size > AIS_MAX_FRAME_BYTES {
                    tracing::warn!("[AISStream] dropping oversized frame ({size} bytes)");
                    return true;
                }
                if owned.is_none() {
                    return false; // orphan-message
                }
                match parse_ais_envelope(&frame.decode()) {
                    Parsed::Malformed => true,
                    Parsed::Error(message) => {
                        let detail = classify_ais_failure(&FailureInput {
                            message: Some(&message),
                            now_ms: now,
                            ..FailureInput::default()
                        });
                        self.fail_generation(&mut state, owner, generation, detail);
                        true
                    }
                    Parsed::Data(envelope) => {
                        if (self.options.ingest)(&envelope) {
                            let actions = owner.lock().on_message(generation);
                            self.run_actions(&mut state, owner, actions);
                        }
                        true
                    }
                }
            }
            SocketEvent::UnexpectedResponse {
                status,
                retry_after,
            } => {
                let detail = classify_ais_failure(&FailureInput {
                    http_status: Some(status),
                    retry_after_header: retry_after.as_deref(),
                    now_ms: now,
                    ..FailureInput::default()
                });
                if owned.is_some() {
                    state.sockets.remove(&generation);
                }
                self.fail_generation(&mut state, owner, generation, detail);
                false
            }
            SocketEvent::Error(message) => {
                if owned.is_none() {
                    return false; // orphan-error: hang it up
                }
                let detail = classify_ais_failure(&FailureInput {
                    message: Some(&message),
                    now_ms: now,
                    ..FailureInput::default()
                });
                self.fail_generation(&mut state, owner, generation, detail);
                true
            }
            SocketEvent::Close => {
                if owned.is_some() {
                    state.sockets.remove(&generation);
                }
                let actions = owner.lock().on_close(generation);
                self.run_actions(&mut state, owner, actions);
                true
            }
        }
    }
}

// --- tokio-tungstenite transport -------------------------------------------

/// The real transport: one task per socket. `terminate()` aborts the task,
/// which drops the TCP connection at once (the `ws` `terminate()` contract).
pub struct WsTransport;

struct WsSocket {
    outgoing: mpsc::UnboundedSender<String>,
    abort: tokio::task::AbortHandle,
}

impl Socket for WsSocket {
    fn send(&self, text: String) -> Result<(), String> {
        self.outgoing
            .send(text)
            .map_err(|_| "send on a dead socket".to_string())
    }

    fn terminate(&self) {
        self.abort.abort();
    }
}

impl Transport for WsTransport {
    fn connect(&self, url: &str, sink: EventSink) -> Result<Arc<dyn Socket>, String> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "ws transport unavailable".to_string())?;
        let (tx, rx) = mpsc::unbounded_channel();
        let task = runtime.spawn(run_socket(url.to_string(), sink, rx));
        Ok(Arc::new(WsSocket {
            outgoing: tx,
            abort: task.abort_handle(),
        }))
    }
}

async fn run_socket(url: String, sink: EventSink, mut outgoing: mpsc::UnboundedReceiver<String>) {
    let stream = match tokio_tungstenite::connect_async(url.as_str()).await {
        Ok((stream, _)) => stream,
        Err(WsError::Http(response)) => {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            sink(SocketEvent::UnexpectedResponse {
                status: response.status().as_u16(),
                retry_after,
            });
            return;
        }
        Err(error) => {
            sink(SocketEvent::Error(error.to_string()));
            return;
        }
    };
    if !sink(SocketEvent::Open) {
        return;
    }
    let (mut write, mut read) = stream.split();
    loop {
        tokio::select! {
            out = outgoing.recv() => {
                // A dropped handle means the adapter abandoned this socket.
                let Some(text) = out else { return };
                if let Err(error) = write.send(Message::Text(text.into())).await {
                    sink(SocketEvent::Error(error.to_string()));
                    return;
                }
            }
            frame = read.next() => {
                let keep = match frame {
                    Some(Ok(Message::Text(text))) => sink(SocketEvent::Message(Frame::Text(text.as_str().to_owned()))),
                    Some(Ok(Message::Binary(bytes))) => sink(SocketEvent::Message(Frame::Binary(bytes.to_vec()))),
                    Some(Ok(Message::Close(_))) | None => {
                        sink(SocketEvent::Close);
                        false
                    }
                    Some(Ok(_)) => true,
                    Some(Err(error)) => {
                        sink(SocketEvent::Error(error.to_string()));
                        false
                    }
                };
                if !keep {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use serde_json::json;

    use super::super::watchdog::tests::FakeClock;
    use super::*;

    fn env() -> WatchdogEnv {
        WatchdogEnv {
            has_key: true,
            has_transport: true,
            silence_watch: true,
            key_fingerprint: Some("key-a".into()),
        }
    }

    fn budgets() -> WatchdogOptions {
        WatchdogOptions {
            stale_ms: Some(1_000.0),
            recycle_after_ms: Some(2_500.0),
            backoff_ms: Some(vec![5_000.0, 15_000.0]),
            down_retry_ms: Some(60_000.0),
            auth_probe_ms: Some(3_600_000.0),
            start_generation: 0,
        }
    }

    #[derive(Default)]
    struct MockSocket {
        terminated: AtomicBool,
        self_closed: AtomicBool,
        sent: Mutex<Vec<String>>,
    }

    impl MockSocket {
        fn open(&self) -> bool {
            !self.terminated.load(Ordering::SeqCst) && !self.self_closed.load(Ordering::SeqCst)
        }
    }

    struct Created {
        socket: Arc<MockSocket>,
        sink: EventSink,
    }

    impl Created {
        fn emit(&self, event: SocketEvent) -> bool {
            let keep = (self.sink)(event);
            if !keep {
                self.socket.terminated.store(true, Ordering::SeqCst);
            }
            keep
        }
        fn frame(&self, text: &str) -> bool {
            self.emit(SocketEvent::Message(Frame::Text(text.into())))
        }
        fn server_close(&self) {
            self.socket.self_closed.store(true, Ordering::SeqCst);
            self.emit(SocketEvent::Close);
        }
    }

    struct SocketRef(Arc<MockSocket>);

    impl Socket for SocketRef {
        fn send(&self, text: String) -> Result<(), String> {
            if !self.0.open() {
                return Err("send on a dead socket".into());
            }
            self.0.sent.lock().push(text);
            Ok(())
        }
        fn terminate(&self) {
            self.0.terminated.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct MockTransport {
        created: Mutex<Vec<Arc<Created>>>,
        fail_with: Mutex<Option<String>>,
    }

    impl MockTransport {
        fn get(&self, index: usize) -> Arc<Created> {
            self.created.lock()[index].clone()
        }
        fn open_connections(&self) -> usize {
            self.created
                .lock()
                .iter()
                .filter(|c| c.socket.open())
                .count()
        }
        fn count(&self) -> usize {
            self.created.lock().len()
        }
    }

    impl Transport for MockTransport {
        fn connect(&self, _url: &str, sink: EventSink) -> Result<Arc<dyn Socket>, String> {
            if let Some(message) = self.fail_with.lock().clone() {
                return Err(message);
            }
            let socket = Arc::new(MockSocket::default());
            self.created.lock().push(Arc::new(Created {
                socket: socket.clone(),
                sink,
            }));
            Ok(Arc::new(SocketRef(socket)))
        }
    }

    struct Context {
        adapter: Arc<Adapter>,
        transport: Arc<MockTransport>,
        time: Arc<FakeClock>,
        ingested: Arc<Mutex<Vec<Value>>>,
    }

    fn setup() -> Context {
        let time = FakeClock::new();
        let transport = Arc::new(MockTransport::default());
        let ingested = Arc::new(Mutex::new(Vec::new()));
        let sink = ingested.clone();
        let adapter = Adapter::new(AdapterOptions {
            transport: transport.clone(),
            resolve_url: Box::new(|| "ws://mock.invalid/stream".into()),
            build_subscription: Box::new(|| json!({ "APIKey": "redacted" })),
            ingest: Box::new(move |envelope| {
                sink.lock().push(envelope.clone());
                is_recognized_ais_envelope(envelope)
            }),
            clock: time.clone(),
        });
        adapter.set_watchdog_options(budgets());
        Context {
            adapter,
            transport,
            time,
            ingested,
        }
    }

    fn ais_frame() -> String {
        json!({
            "MessageType": "PositionReport",
            "MetaData": { "MMSI": "123456789", "latitude": 1, "longitude": 2 },
            "Message": { "PositionReport": { "Latitude": 1, "Longitude": 2 } },
        })
        .to_string()
    }

    fn status(context: &Context) -> &'static str {
        context.adapter.snapshot().unwrap().status
    }

    fn go_live(context: &Context) -> Arc<Created> {
        context.adapter.ensure(&env());
        let socket = context.transport.get(0);
        socket.emit(SocketEvent::Open);
        socket.frame(&ais_frame());
        assert_eq!(status(context), "live");
        socket
    }

    #[test]
    fn a_pre_disposal_close_cannot_erase_the_post_disposal_socket() {
        let context = setup();
        let first = go_live(&context);
        context.adapter.dispose();
        context.adapter.set_watchdog_options(budgets());
        context.adapter.ensure(&env());
        assert_eq!(context.transport.count(), 2);
        // The first socket's late close lands after the replacement opened.
        first.emit(SocketEvent::Close);
        assert_eq!(context.adapter.live_sockets(), 1);
        let second = context.transport.get(1);
        second.emit(SocketEvent::Open);
        second.frame(&ais_frame());
        assert_eq!(status(&context), "live");
        assert_eq!(context.transport.open_connections(), 1);
    }

    #[test]
    fn generations_never_repeat_across_disposals() {
        let context = setup();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..5 {
            context.adapter.ensure(&env());
            let high = context.adapter.state.lock().generation_high_water;
            assert!(seen.insert(high));
            context.adapter.dispose();
            context.adapter.set_watchdog_options(budgets());
        }
    }

    #[test]
    fn at_most_one_connection_through_a_failure_cycle() {
        let context = setup();
        go_live(&context);
        for _ in 0..10 {
            context.time.advance(70_000.0);
            context.adapter.ensure(&env());
            assert!(context.transport.open_connections() <= 1);
        }
        assert!(context.transport.count() > 2);
    }

    #[test]
    fn orphan_frames_and_non_records_are_not_liveness() {
        let context = setup();
        let first = go_live(&context);
        context.adapter.dispose();
        context.adapter.set_watchdog_options(budgets());
        let before = context.ingested.lock().len();
        assert!(!first.frame(&ais_frame()), "orphan frame hangs up");
        assert_eq!(context.ingested.lock().len(), before);

        let context = setup();
        context.adapter.ensure(&env());
        let socket = context.transport.get(0);
        socket.emit(SocketEvent::Open);
        socket.frame("not json");
        socket.frame("{\"hello\":1}");
        socket.frame(&json!({ "MetaData": { "MMSI": "1" } }).to_string());
        assert_eq!(status(&context), "connecting");
        assert_eq!(context.ingested.lock().len(), 2);
    }

    #[test]
    fn every_enum_type_counts_as_liveness() {
        for message_type in AIS_RECOGNIZED_MESSAGE_TYPES {
            let envelope = json!({
                "MessageType": message_type,
                "MetaData": { "MMSI": "1" },
                "Message": { message_type: {} },
            });
            assert!(is_recognized_ais_envelope(&envelope), "{message_type}");
        }
        assert!(!is_recognized_ais_envelope(&json!({
            "MessageType": "PositionReport",
            "MetaData": { "MMSI": "1" },
            "Message": {},
        })));
        assert!(!is_recognized_ais_envelope(&json!({
            "MessageType": "PositionReport",
            "Message": { "PositionReport": {} },
        })));
        assert!(is_recognized_ais_envelope(&json!({
            "MessageType": "PositionReport",
            "Message": { "PositionReport": { "UserID": 5 } },
        })));
        assert!(!is_recognized_ais_envelope(&json!([])));
    }

    #[test]
    fn opens_subscribe_once_and_orphan_opens_never_subscribe() {
        let context = setup();
        context.adapter.ensure(&env());
        let socket = context.transport.get(0);
        socket.emit(SocketEvent::Open);
        assert_eq!(*socket.socket.sent.lock(), [r#"{"APIKey":"redacted"}"#]);

        let context = setup();
        context.adapter.ensure(&env());
        let socket = context.transport.get(0);
        context.adapter.dispose();
        assert!(!socket.emit(SocketEvent::Open));
        assert!(socket.socket.sent.lock().is_empty());
    }

    #[test]
    fn auth_envelopes_go_terminal_even_with_a_same_tick_close() {
        let context = setup();
        let socket = go_live(&context);
        socket.frame(r#"{"error":"Api Key Is Not Valid"}"#);
        socket.emit(SocketEvent::Close);
        let snap = context.adapter.snapshot().unwrap();
        assert_eq!(snap.status, "auth-failed");
        assert_eq!(
            snap.next_attempt_at.unwrap() - *context.time.wall.lock(),
            3_600_000.0
        );
        // Repeated auth rejections never accelerate.
        for _ in 0..5 {
            context.time.advance(5_000.0);
            context.adapter.ensure(&env());
        }
        assert_eq!(context.transport.count(), 1);
    }

    #[test]
    fn upgrade_failures_are_classified_by_status() {
        for (status_code, expected, wait) in [
            (401, "auth-failed", 3_600_000.0),
            (403, "auth-failed", 3_600_000.0),
            (429, "reconnecting", 15_000.0),
            (500, "reconnecting", 5_000.0),
        ] {
            let context = setup();
            context.adapter.ensure(&env());
            let socket = context.transport.get(0);
            socket.emit(SocketEvent::UnexpectedResponse {
                status: status_code,
                retry_after: None,
            });
            let snap = context.adapter.snapshot().unwrap();
            assert_eq!(snap.status, expected, "{status_code}");
            assert_eq!(
                snap.next_attempt_at.unwrap() - *context.time.wall.lock(),
                wait,
                "{status_code}"
            );
            assert_eq!(context.transport.open_connections(), 0);
        }
        let context = setup();
        context.adapter.ensure(&env());
        context
            .transport
            .get(0)
            .emit(SocketEvent::UnexpectedResponse {
                status: 429,
                retry_after: Some("120".into()),
            });
        let snap = context.adapter.snapshot().unwrap();
        assert_eq!(
            snap.next_attempt_at.unwrap() - *context.time.wall.lock(),
            120_000.0
        );
    }

    #[test]
    fn errors_on_abandoned_sockets_hang_up_and_transport_errors_use_the_first_rung() {
        let context = setup();
        let first = go_live(&context);
        context.adapter.dispose();
        context.adapter.set_watchdog_options(budgets());
        assert!(!first.emit(SocketEvent::Error("late".into())));

        let context = setup();
        let socket = go_live(&context);
        socket.emit(SocketEvent::Error("ECONNRESET".into()));
        let snap = context.adapter.snapshot().unwrap();
        assert_eq!(snap.status, "reconnecting");
        assert_eq!(snap.error.as_deref(), Some("ECONNRESET"));
        assert_eq!(
            snap.next_attempt_at.unwrap() - *context.time.wall.lock(),
            5_000.0
        );
        assert_eq!(context.transport.open_connections(), 0);
    }

    #[test]
    fn server_close_reconnects_on_the_ladder_and_keyless_terminates() {
        let context = setup();
        let socket = go_live(&context);
        socket.server_close();
        assert_eq!(context.adapter.live_sockets(), 0);
        context.time.advance(5_000.0);
        context.adapter.ensure(&env());
        assert_eq!(context.transport.count(), 2);

        let context = setup();
        go_live(&context);
        context.adapter.ensure(&WatchdogEnv {
            has_key: false,
            ..env()
        });
        assert_eq!(context.transport.open_connections(), 0);
        assert_eq!(status(&context), "missing-key");
    }

    #[test]
    fn factory_failures_are_classified_and_oversized_frames_dropped() {
        let context = setup();
        *context.transport.fail_with.lock() = Some("Unexpected server response: 401".into());
        context.adapter.ensure(&env());
        assert_eq!(status(&context), "auth-failed");

        let context = setup();
        context.adapter.ensure(&env());
        let socket = context.transport.get(0);
        socket.emit(SocketEvent::Open);
        let huge = format!("{{\"pad\":\"{}\"}}", "x".repeat(AIS_MAX_FRAME_BYTES));
        assert!(socket.frame(&huge));
        assert!(context.ingested.lock().is_empty());
        assert!(socket.emit(SocketEvent::Message(Frame::Binary(
            ais_frame().into_bytes()
        ))));
        assert_eq!(status(&context), "live");
    }

    #[test]
    fn failure_classification() {
        let kind = |input: FailureInput<'_>| classify_ais_failure(&input).kind;
        let msg = |m: &'static str| FailureInput {
            message: Some(m),
            ..FailureInput::default()
        };
        let http = |s: u16| FailureInput {
            http_status: Some(s),
            ..FailureInput::default()
        };
        assert_eq!(kind(http(401)), FailureKind::Auth);
        assert_eq!(kind(http(403)), FailureKind::Auth);
        assert_eq!(kind(http(429)), FailureKind::RateLimit);
        assert_eq!(kind(http(500)), FailureKind::Transport);
        assert_eq!(
            kind(msg("Unexpected server response: 401")),
            FailureKind::Auth
        );
        assert_eq!(
            kind(msg("Unexpected server response: 429")),
            FailureKind::RateLimit
        );
        assert_eq!(kind(msg("Invalid API key")), FailureKind::Auth);
        assert_eq!(kind(msg("unauthorized")), FailureKind::Auth);
        assert_eq!(kind(msg("API key is required")), FailureKind::Auth);
        assert_eq!(kind(msg("api_key not valid")), FailureKind::Transport);
        assert_eq!(kind(msg("Api Key Not Valid")), FailureKind::Auth);
        assert_eq!(kind(msg("bad api-key")), FailureKind::Auth);
        assert_eq!(kind(msg("rate limit exceeded")), FailureKind::RateLimit);
        assert_eq!(kind(msg("rate_limit")), FailureKind::RateLimit);
        assert_eq!(kind(msg("too many connections")), FailureKind::RateLimit);
        assert_eq!(kind(msg("Quota  exceeded")), FailureKind::RateLimit);
        assert_eq!(kind(msg("ECONNRESET")), FailureKind::Transport);
        assert_eq!(kind(FailureInput::default()), FailureKind::Transport);
        assert_eq!(
            classify_ais_failure(&http(401)).message.as_deref(),
            Some("AISStream rejected the API key (HTTP 401)")
        );
        assert_eq!(
            classify_ais_failure(&http(502)).message.as_deref(),
            Some("AISStream upgrade failed (HTTP 502)")
        );
        assert_eq!(
            classify_ais_failure(&FailureInput::default())
                .message
                .as_deref(),
            Some("AISStream websocket error")
        );
    }

    #[test]
    fn retry_after_accepts_delta_seconds_and_http_dates() {
        let now = parse_date("2026-08-18T12:00:00Z")
            .unwrap()
            .timestamp_millis() as f64;
        assert_eq!(parse_retry_after_ms(Some("30"), now), 30_000.0);
        assert_eq!(parse_retry_after_ms(Some("  45  "), now), 45_000.0);
        assert_eq!(
            parse_retry_after_ms(Some("Tue, 18 Aug 2026 12:02:00 GMT"), now),
            120_000.0
        );
        assert_eq!(parse_retry_after_ms(None, now), 0.0);
        assert_eq!(parse_retry_after_ms(Some(""), now), 0.0);
        assert_eq!(parse_retry_after_ms(Some("soon"), now), 0.0);
    }

    #[test]
    fn envelope_parsing_separates_malformed_error_and_data() {
        assert_eq!(parse_ais_envelope("nope"), Parsed::Malformed);
        assert_eq!(parse_ais_envelope("[]"), Parsed::Malformed);
        assert_eq!(parse_ais_envelope("null"), Parsed::Malformed);
        assert_eq!(
            parse_ais_envelope(r#"{"error":"bad key"}"#),
            Parsed::Error("bad key".into())
        );
        assert!(matches!(parse_ais_envelope(&ais_frame()), Parsed::Data(_)));
    }
}
