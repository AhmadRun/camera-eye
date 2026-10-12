//! Pure state machine for the AISStream live-feed watchdog (port of
//! `src/data/aisWatchdog.js`).
//!
//! Liveness is judged by DATA, not socket state. Two budgets are decoupled:
//! `stale_ms` (when to report the feed stopped) and `recycle_after_ms` (when to
//! hard-abort and reconnect). Failures are classified: only transport faults
//! walk the backoff ladder; an auth rejection is terminal until the key
//! changes, and a rate limit honours the server's own Retry-After.
//!
//! The machine owns no sockets, no timers and no environment: it returns
//! [`Action`]s for the transport adapter. The only teardown action is
//! `Terminate`. Durations use a monotonic clock; wall time is for display.

use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
#[cfg(test)]
use serde_json::json;

use super::super::js_value::number_value;

/// `AIS_WATCHDOG_DEFAULTS`.
pub const DEFAULT_STALE_MS: f64 = 120_000.0;
pub const DEFAULT_RECYCLE_AFTER_MS: f64 = 300_000.0;
pub const DEFAULT_BACKOFF_MS: [f64; 4] = [5_000.0, 15_000.0, 60_000.0, 300_000.0];
pub const DEFAULT_DOWN_RETRY_MS: f64 = 900_000.0;
pub const DEFAULT_AUTH_PROBE_MS: f64 = 3_600_000.0;

/// Wall and monotonic time, both in milliseconds.
pub trait Clock: Send + Sync {
    fn wall(&self) -> f64;
    fn mono(&self) -> f64;
}

/// `Date.now()` and `performance.now()`.
pub struct SystemClock {
    start: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn wall(&self) -> f64 {
        crate::common::js::now_ms() as f64
    }
    fn mono(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }
}

/// What the adapter must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Connect { generation: u64 },
    Terminate { generation: u64, reason: String },
}

/// `AIS_FAILURE_KINDS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    Transport,
    Auth,
    RateLimit,
}

/// A classified failure.
#[derive(Debug, Clone, PartialEq)]
pub struct FailureDetail {
    pub kind: FailureKind,
    pub message: Option<String>,
    pub retry_after_ms: Option<f64>,
}

impl FailureDetail {
    #[cfg(test)]
    pub fn of(kind: FailureKind) -> Self {
        Self {
            kind,
            message: None,
            retry_after_ms: None,
        }
    }
}

/// `parseSilenceTimeoutEnv`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SilenceOverride {
    Default,
    Off,
    Timeout(f64),
}

/// `parseSilenceTimeoutEnv`: only a literal 0 is the kill switch; anything
/// unparseable falls back to the default and says so once.
pub fn parse_silence_timeout_env(
    raw: Option<&str>,
    warn: &mut dyn FnMut(String),
) -> SilenceOverride {
    let Some(raw) = raw else {
        return SilenceOverride::Default;
    };
    let text = raw.trim();
    if text.is_empty() {
        return SilenceOverride::Default;
    }
    let valid = {
        let (int, frac) = text
            .split_once('.')
            .map_or((text, None), |(i, f)| (i, Some(f)));
        !int.is_empty()
            && int.bytes().all(|b| b.is_ascii_digit())
            && frac.is_none_or(|f| !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit()))
    };
    if !valid {
        warn(format!(
            "[AISStream] Ignoring AISSTREAM_SILENCE_TIMEOUT_MS=\"{raw}\" (not a non-negative number); using the {}ms default.",
            DEFAULT_STALE_MS
        ));
        return SilenceOverride::Default;
    }
    let value: f64 = text.parse().unwrap_or(f64::NAN);
    if !value.is_finite() {
        return SilenceOverride::Default;
    }
    if value == 0.0 {
        return SilenceOverride::Off;
    }
    SilenceOverride::Timeout(value)
}

/// Options for [`Watchdog::new`]; `None` takes the default.
#[derive(Clone, Default)]
pub struct WatchdogOptions {
    pub stale_ms: Option<f64>,
    pub recycle_after_ms: Option<f64>,
    pub backoff_ms: Option<Vec<f64>>,
    pub down_retry_ms: Option<f64>,
    pub auth_probe_ms: Option<f64>,
    pub start_generation: u64,
}

/// `configure(env)`.
#[derive(Clone, Debug)]
pub struct WatchdogEnv {
    pub has_key: bool,
    pub has_transport: bool,
    pub silence_watch: bool,
    pub key_fingerprint: Option<String>,
}

fn positive_or(value: Option<f64>, fallback: f64) -> f64 {
    value
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(fallback)
}

pub struct Watchdog {
    stale_ms: f64,
    recycle_after_ms: f64,
    backoff_ms: Vec<f64>,
    down_retry_ms: f64,
    auth_probe_ms: f64,
    clock: Arc<dyn Clock>,

    status: &'static str,
    generation: u64,
    owned: Option<u64>,
    silence_since_mono: f64,
    last_message_wall: f64,
    reconnect_attempt: usize,
    next_attempt_mono: f64,
    error: Option<String>,
    silence_watch_armed: bool,
    key_fingerprint: Option<String>,
}

impl Watchdog {
    pub fn new(options: WatchdogOptions, clock: Arc<dyn Clock>) -> Self {
        let stale_ms = positive_or(options.stale_ms, DEFAULT_STALE_MS);
        let recycle_after_ms = stale_ms.max(positive_or(
            options.recycle_after_ms,
            DEFAULT_RECYCLE_AFTER_MS,
        ));
        let backoff_ms = match options.backoff_ms {
            Some(ladder) if !ladder.is_empty() => ladder
                .into_iter()
                .map(|ms| positive_or(Some(ms), 5_000.0))
                .collect(),
            _ => DEFAULT_BACKOFF_MS.to_vec(),
        };
        Self {
            stale_ms,
            recycle_after_ms,
            backoff_ms,
            down_retry_ms: positive_or(options.down_retry_ms, DEFAULT_DOWN_RETRY_MS),
            auth_probe_ms: positive_or(options.auth_probe_ms, DEFAULT_AUTH_PROBE_MS),
            clock,
            status: "idle",
            generation: options.start_generation,
            owned: None,
            silence_since_mono: 0.0,
            last_message_wall: 0.0,
            reconnect_attempt: 0,
            next_attempt_mono: 0.0,
            error: None,
            silence_watch_armed: true,
            key_fingerprint: None,
        }
    }

    #[cfg(test)]
    pub fn owned(&self) -> Option<u64> {
        self.owned
    }

    fn is_exhausted(&self) -> bool {
        self.reconnect_attempt > self.backoff_ms.len()
    }

    /// Count a failed session and pick the next attempt time, by class.
    fn schedule_retry(&mut self, kind: FailureKind, retry_after_ms: Option<f64>) {
        let mono_now = self.clock.mono();
        // While the key is refused, no probe outcome may return to the ladder.
        let effective = if self.status == "auth-failed" && kind != FailureKind::Auth {
            FailureKind::Auth
        } else {
            kind
        };
        self.reconnect_attempt += 1;
        match effective {
            FailureKind::Auth => {
                self.status = "auth-failed";
                self.next_attempt_mono = mono_now + self.auth_probe_ms;
            }
            FailureKind::RateLimit => {
                self.reconnect_attempt = self.reconnect_attempt.max(self.backoff_ms.len());
                let slowest = *self.backoff_ms.last().unwrap_or(&5_000.0);
                let wait = positive_or(retry_after_ms, slowest);
                let exhausted = self.is_exhausted();
                self.status = if exhausted { "down" } else { "reconnecting" };
                self.next_attempt_mono = mono_now
                    + if exhausted {
                        wait.max(self.down_retry_ms)
                    } else {
                        wait
                    };
            }
            FailureKind::Transport => {
                if self.is_exhausted() {
                    self.status = "down";
                    self.next_attempt_mono = mono_now + self.down_retry_ms;
                } else {
                    self.status = "reconnecting";
                    self.next_attempt_mono = mono_now + self.backoff_ms[self.reconnect_attempt - 1];
                }
            }
        }
    }

    fn terminate_owned(&mut self, reason: &str) -> Vec<Action> {
        match self.owned.take() {
            Some(generation) => vec![Action::Terminate {
                generation,
                reason: reason.into(),
            }],
            None => Vec::new(),
        }
    }

    /// Declare the environment. Safe to call repeatedly.
    pub fn configure(&mut self, env: &WatchdogEnv) -> Vec<Action> {
        self.silence_watch_armed = env.silence_watch;
        if !env.has_key {
            let actions = self.terminate_owned("key-absent");
            self.status = "missing-key";
            self.error = Some("AISSTREAM_API_KEY is not set".into());
            self.reconnect_attempt = 0;
            self.key_fingerprint = None;
            return actions;
        }
        if !env.has_transport {
            let actions = self.terminate_owned("no-transport");
            self.status = "unsupported";
            self.error = Some("Node WebSocket transport is unavailable".into());
            return actions;
        }
        // A changed credential clears the terminal auth state immediately,
        // and any socket in flight belongs to the OLD key.
        let next = env.key_fingerprint.clone();
        let changed = self.key_fingerprint.is_some() && next != self.key_fingerprint;
        self.key_fingerprint = next;
        if changed {
            let actions = self.terminate_owned("key-rotated");
            self.status = "idle";
            self.error = None;
            self.reconnect_attempt = 0;
            self.next_attempt_mono = self.clock.mono();
            return actions;
        }
        if self.status == "missing-key" || self.status == "unsupported" {
            self.status = "idle";
            self.error = None;
            self.reconnect_attempt = 0;
            self.next_attempt_mono = self.clock.mono();
        }
        Vec::new()
    }

    /// Advance time. Never returns a connect while a socket is owned.
    pub fn tick(&mut self) -> Vec<Action> {
        if self.status == "missing-key" || self.status == "unsupported" {
            return Vec::new();
        }
        let mono_now = self.clock.mono();
        if self.owned.is_some() {
            // AUTH-FAILED is sticky against staleness processing; the probe is
            // bounded by the auth cadence instead.
            if self.status == "auth-failed" {
                if mono_now - self.silence_since_mono >= self.auth_probe_ms {
                    let actions = self.terminate_owned("auth-probe-expired");
                    self.schedule_retry(FailureKind::Auth, None);
                    return actions;
                }
                return Vec::new();
            }
            if !self.silence_watch_armed {
                return Vec::new();
            }
            let silent_for = mono_now - self.silence_since_mono;
            if silent_for >= self.recycle_after_ms {
                self.error = Some(format!(
                    "AISStream delivered no data for {}s",
                    super::super::js_value::round(silent_for / 1000.0)
                ));
                let actions = self.terminate_owned("silent");
                self.schedule_retry(FailureKind::Transport, None);
                return actions;
            }
            if silent_for >= self.stale_ms {
                self.status = "stale";
            }
            return Vec::new();
        }
        if mono_now < self.next_attempt_mono {
            return Vec::new();
        }
        self.generation += 1;
        self.owned = Some(self.generation);
        // The silence budget starts when the socket is commissioned.
        self.silence_since_mono = mono_now;
        if self.status != "down" && self.status != "auth-failed" {
            self.status = "connecting";
        }
        vec![Action::Connect {
            generation: self.generation,
        }]
    }

    fn owns_generation(&self, generation: u64) -> bool {
        self.owned == Some(generation)
    }

    /// A socket finished its handshake: neither data nor a ladder reset.
    pub fn on_open(&mut self, generation: u64) -> Vec<Action> {
        if !self.owns_generation(generation) {
            return vec![Action::Terminate {
                generation,
                reason: "orphan".into(),
            }];
        }
        if !matches!(self.status, "down" | "auth-failed" | "stale") {
            self.status = "connecting";
        }
        Vec::new()
    }

    /// A VALID AIS message arrived: the only proof the feed works.
    pub fn on_message(&mut self, generation: u64) -> Vec<Action> {
        if !self.owns_generation(generation) {
            return vec![Action::Terminate {
                generation,
                reason: "orphan".into(),
            }];
        }
        self.silence_since_mono = self.clock.mono();
        self.last_message_wall = self.clock.wall();
        self.reconnect_attempt = 0;
        self.error = None;
        self.status = "live";
        Vec::new()
    }

    /// The socket closed on its own.
    pub fn on_close(&mut self, generation: u64) -> Vec<Action> {
        if !self.owns_generation(generation) {
            return Vec::new();
        }
        self.owned = None;
        if self.error.is_none() {
            self.error = Some("AISStream websocket closed".into());
        }
        self.schedule_retry(FailureKind::Transport, None);
        Vec::new()
    }

    /// A classified failure: terminate now and schedule by class.
    pub fn on_failure(&mut self, generation: u64, detail: &FailureDetail) -> Vec<Action> {
        if !self.owns_generation(generation) {
            return Vec::new();
        }
        let kind = detail.kind;
        if !(self.status == "auth-failed" && kind != FailureKind::Auth) {
            self.error = Some(
                detail
                    .message
                    .clone()
                    .filter(|m| !m.is_empty())
                    .unwrap_or_else(|| default_failure_message(kind).into()),
            );
        }
        let reason = match kind {
            FailureKind::Transport => "transport",
            FailureKind::Auth => "auth",
            FailureKind::RateLimit => "rate-limit",
        };
        let actions = self.terminate_owned(reason);
        self.schedule_retry(kind, detail.retry_after_ms);
        actions
    }

    /// Tear everything down; the generation namespace is kept.
    pub fn dispose(&mut self) -> Vec<Action> {
        let actions = self.terminate_owned("dispose");
        self.status = "idle";
        self.reconnect_attempt = 0;
        self.next_attempt_mono = 0.0;
        self.silence_since_mono = 0.0;
        self.last_message_wall = 0.0;
        self.error = None;
        self.key_fingerprint = None;
        actions
    }

    /// Highest generation ever issued.
    pub fn high_water_generation(&self) -> u64 {
        self.generation
    }

    /// Status metadata for `/api/vessels`.
    pub fn snapshot(&self) -> Snapshot {
        let mono_now = self.clock.mono();
        let wall_now = self.clock.wall();
        let waiting_ms = (self.next_attempt_mono - mono_now).max(0.0);
        Snapshot {
            status: self.status,
            error: self.error.clone(),
            last_message_at: (self.last_message_wall != 0.0).then_some(self.last_message_wall),
            silent_for_ms: self
                .owned
                .map(|_| (mono_now - self.silence_since_mono).max(0.0)),
            reconnect_attempt: self.reconnect_attempt,
            next_attempt_at: matches!(self.status, "reconnecting" | "down" | "auth-failed")
                .then_some(wall_now + waiting_ms),
            watchdog: if self.silence_watch_armed {
                "armed"
            } else {
                "custom-subscription-off"
            },
            stale_after_ms: self.stale_ms,
        }
    }
}

fn default_failure_message(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Auth => "AISStream rejected the API key",
        FailureKind::RateLimit => "AISStream rate-limited this key",
        FailureKind::Transport => "AISStream websocket error",
    }
}

/// `snapshot()`.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub status: &'static str,
    pub error: Option<String>,
    pub last_message_at: Option<f64>,
    pub silent_for_ms: Option<f64>,
    pub reconnect_attempt: usize,
    pub next_attempt_at: Option<f64>,
    pub watchdog: &'static str,
    pub stale_after_ms: f64,
}

impl Snapshot {
    pub fn last_message_at_value(&self) -> Value {
        self.last_message_at.map_or(Value::Null, number_value)
    }
    pub fn silent_for_ms_value(&self) -> Value {
        self.silent_for_ms.map_or(Value::Null, number_value)
    }
    pub fn next_attempt_at_value(&self) -> Value {
        self.next_attempt_at.map_or(Value::Null, number_value)
    }
    #[cfg(test)]
    pub fn to_value(&self) -> Value {
        json!({
            "status": self.status,
            "error": self.error,
            "lastMessageAt": self.last_message_at_value(),
            "silentForMs": self.silent_for_ms_value(),
            "reconnectAttempt": self.reconnect_attempt,
            "nextAttemptAt": self.next_attempt_at_value(),
            "watchdog": self.watchdog,
            "staleAfterMs": number_value(self.stale_after_ms),
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    use parking_lot::Mutex;

    use super::*;

    /// Separately controllable wall and monotonic clocks.
    pub struct FakeClock {
        pub wall: Mutex<f64>,
        pub mono: Mutex<f64>,
    }

    impl FakeClock {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                wall: Mutex::new(1_700_000_000_000.0),
                mono: Mutex::new(10_000.0),
            })
        }
        pub fn advance(&self, ms: f64) {
            *self.wall.lock() += ms;
            *self.mono.lock() += ms;
        }
    }

    impl Clock for FakeClock {
        fn wall(&self) -> f64 {
            *self.wall.lock()
        }
        fn mono(&self) -> f64 {
            *self.mono.lock()
        }
    }

    fn live_env() -> WatchdogEnv {
        WatchdogEnv {
            has_key: true,
            has_transport: true,
            silence_watch: true,
            key_fingerprint: Some("key-a".into()),
        }
    }

    fn keyless() -> WatchdogEnv {
        WatchdogEnv {
            has_key: false,
            ..live_env()
        }
    }

    fn key_b() -> WatchdogEnv {
        WatchdogEnv {
            key_fingerprint: Some("key-b".into()),
            ..live_env()
        }
    }

    /// Drives a watchdog, recording every action and asserting the
    /// single-socket invariant on each connect.
    struct Harness {
        watchdog: Watchdog,
        time: Arc<FakeClock>,
        actions: Vec<Action>,
        open: HashSet<u64>,
    }

    impl Harness {
        fn new() -> Self {
            Self::with(WatchdogOptions::default())
        }
        fn with(options: WatchdogOptions) -> Self {
            let time = FakeClock::new();
            Self {
                watchdog: Watchdog::new(options, time.clone()),
                time,
                actions: Vec::new(),
                open: HashSet::new(),
            }
        }
        fn record(&mut self, list: Vec<Action>) -> Vec<Action> {
            for action in &list {
                match action {
                    Action::Connect { generation } => {
                        assert!(
                            self.open.is_empty(),
                            "connect {generation} while {:?} live",
                            self.open
                        );
                        self.open.insert(*generation);
                    }
                    Action::Terminate { generation, .. } => {
                        self.open.remove(generation);
                    }
                }
                self.actions.push(action.clone());
            }
            list
        }
        fn advance(&mut self, ms: f64) {
            self.time.advance(ms);
        }
        fn wall(&self) -> f64 {
            *self.time.wall.lock()
        }
        fn configure(&mut self, env: WatchdogEnv) -> Vec<Action> {
            let list = self.watchdog.configure(&env);
            self.record(list)
        }
        fn tick(&mut self) -> Vec<Action> {
            let list = self.watchdog.tick();
            self.record(list)
        }
        fn open(&mut self, g: u64) -> Vec<Action> {
            let list = self.watchdog.on_open(g);
            self.record(list)
        }
        fn message(&mut self, g: u64) -> Vec<Action> {
            let list = self.watchdog.on_message(g);
            self.record(list)
        }
        fn close(&mut self, g: u64) -> Vec<Action> {
            self.open.remove(&g);
            let list = self.watchdog.on_close(g);
            self.record(list)
        }
        fn fail(&mut self, g: u64, detail: FailureDetail) -> Vec<Action> {
            let list = self.watchdog.on_failure(g, &detail);
            self.record(list)
        }
        fn snapshot(&self) -> Snapshot {
            self.watchdog.snapshot()
        }
        fn wait(&self) -> f64 {
            self.snapshot().next_attempt_at.unwrap() - self.wall()
        }
        fn types(&self) -> Vec<String> {
            self.actions
                .iter()
                .map(|a| match a {
                    Action::Connect { generation } => format!("connect:{generation}"),
                    Action::Terminate { generation, .. } => format!("terminate:{generation}"),
                })
                .collect()
        }
        fn go_live(mut self) -> Self {
            self.configure(live_env());
            self.tick();
            self.open(1);
            self.message(1);
            assert_eq!(self.snapshot().status, "live");
            self
        }
    }

    fn detail(kind: FailureKind, message: &str) -> FailureDetail {
        FailureDetail {
            kind,
            message: Some(message.into()),
            retry_after_ms: None,
        }
    }

    #[test]
    fn keyless_install_reports_off_and_never_opens() {
        let mut h = Harness::new();
        h.configure(keyless());
        h.tick();
        h.advance(600_000.0);
        h.tick();
        assert!(h.types().is_empty());
        let snap = h.snapshot();
        assert_eq!(snap.status, "missing-key");
        assert_eq!(snap.error.as_deref(), Some("AISSTREAM_API_KEY is not set"));
        assert_eq!(snap.reconnect_attempt, 0);
        assert_eq!(snap.next_attempt_at, None);
    }

    #[test]
    fn a_key_added_mid_session_connects_at_once_and_removal_terminates() {
        let mut h = Harness::new();
        h.configure(keyless());
        h.tick();
        h.configure(live_env());
        h.tick();
        assert_eq!(h.types(), ["connect:1"]);
        assert_eq!(h.snapshot().status, "connecting");

        let mut h = Harness::new().go_live();
        h.configure(keyless());
        assert_eq!(h.types(), ["connect:1", "terminate:1"]);
        assert_eq!(h.snapshot().status, "missing-key");
        assert!(h.open.is_empty());
    }

    #[test]
    fn liveness_is_data_not_socket_state() {
        let mut h = Harness::new();
        h.configure(live_env());
        h.tick();
        h.open(1);
        assert_eq!(h.snapshot().status, "connecting");
        assert_eq!(h.snapshot().last_message_at, None);
        h.message(1);
        assert_eq!(h.snapshot().status, "live");
        assert_eq!(h.snapshot().last_message_at, Some(h.wall()));

        let mut h = Harness::new().go_live();
        let started = h.wall();
        h.advance(DEFAULT_STALE_MS - 1_000.0);
        h.tick();
        assert_eq!(h.snapshot().status, "live");
        h.advance(2_000.0);
        h.tick();
        let snap = h.snapshot();
        assert_eq!(snap.status, "stale");
        assert_eq!(snap.last_message_at, Some(started));
        assert!(snap.silent_for_ms.unwrap() >= DEFAULT_STALE_MS);
        assert_eq!(h.types(), ["connect:1"]);
        for _ in 0..20 {
            h.advance(8_000.0);
            h.tick();
        }
        assert_eq!(h.snapshot().status, "stale");
        assert_eq!(
            h.types(),
            ["connect:1"],
            "no churn between stale and recycle"
        );
        h.open(1);
        assert_eq!(h.snapshot().status, "stale", "only data clears stale");
    }

    #[test]
    fn a_late_handshake_does_not_reset_the_silence_clock() {
        let mut h = Harness::new();
        h.configure(live_env());
        h.tick();
        h.advance(DEFAULT_RECYCLE_AFTER_MS - 1_000.0);
        h.open(1);
        assert!(h.snapshot().silent_for_ms.unwrap() >= DEFAULT_RECYCLE_AFTER_MS - 1_000.0);
        h.advance(1_001.0);
        h.tick();
        assert_eq!(h.types(), ["connect:1", "terminate:1"]);
    }

    #[test]
    fn a_wall_clock_rollback_cannot_suppress_stale_detection() {
        let mut h = Harness::new().go_live();
        h.advance(DEFAULT_STALE_MS + 1_000.0);
        *h.time.wall.lock() -= 3_600_000.0;
        h.tick();
        assert_eq!(h.snapshot().status, "stale");
        assert!(h.snapshot().silent_for_ms.unwrap() > 0.0);
        h.advance(DEFAULT_RECYCLE_AFTER_MS);
        h.tick();
        assert_eq!(h.types(), ["connect:1", "terminate:1"]);
    }

    #[test]
    fn a_silent_socket_is_terminated_once_then_reconnected_after_backoff() {
        let mut h = Harness::new().go_live();
        h.advance(DEFAULT_RECYCLE_AFTER_MS + 1.0);
        h.tick();
        assert_eq!(h.types(), ["connect:1", "terminate:1"]);
        let snap = h.snapshot();
        assert_eq!(snap.status, "reconnecting");
        assert_eq!(snap.reconnect_attempt, 1);
        assert_eq!(
            snap.error.as_deref(),
            Some("AISStream delivered no data for 300s")
        );
        h.advance(4_000.0);
        h.tick();
        assert_eq!(h.types().len(), 2);
        h.advance(1_001.0);
        h.tick();
        assert_eq!(h.types(), ["connect:1", "terminate:1", "connect:2"]);
    }

    #[test]
    fn single_socket_invariant_and_terminate_only() {
        let mut h = Harness::new().go_live();
        for _ in 0..6 {
            h.advance(DEFAULT_RECYCLE_AFTER_MS + 1.0);
            h.tick();
            h.advance(DEFAULT_DOWN_RETRY_MS + 1.0);
            h.tick();
        }
        let mut held: Option<u64> = None;
        for action in &h.actions {
            match action {
                Action::Connect { generation } => {
                    assert_eq!(held, None);
                    held = Some(*generation);
                }
                Action::Terminate { generation, .. } => {
                    assert_eq!(held, Some(*generation));
                    held = None;
                }
            }
        }
    }

    #[test]
    fn backoff_is_exponential_and_exhaustion_enters_down() {
        let mut h = Harness::new();
        h.configure(live_env());
        h.tick();
        let mut waits = Vec::new();
        for attempt in 0..DEFAULT_BACKOFF_MS.len() as u64 {
            let generation = attempt + 1;
            h.open(generation);
            h.close(generation);
            waits.push(h.wait());
            h.advance(*waits.last().unwrap());
            h.tick();
        }
        assert_eq!(waits, DEFAULT_BACKOFF_MS);
        let generation = h.watchdog.owned().unwrap();
        h.open(generation);
        h.close(generation);
        let down = h.snapshot();
        assert_eq!(down.status, "down");
        assert_eq!(down.reconnect_attempt, DEFAULT_BACKOFF_MS.len() + 1);
        assert_eq!(h.watchdog.owned(), None);
        let before = h.types().len();
        h.advance(DEFAULT_DOWN_RETRY_MS - 1_000.0);
        h.tick();
        assert_eq!(h.types().len(), before);
        h.advance(2_000.0);
        h.tick();
        assert_eq!(h.types().len(), before + 1);
        assert_eq!(h.snapshot().status, "down", "a retry is not recovery");
        let generation = h.watchdog.owned().unwrap();
        h.open(generation);
        assert_eq!(h.snapshot().status, "down");
        h.message(generation);
        let recovered = h.snapshot();
        assert_eq!(recovered.status, "live");
        assert_eq!(recovered.reconnect_attempt, 0);
        assert_eq!(recovered.error, None);
        h.close(generation);
        assert_eq!(h.wait(), DEFAULT_BACKOFF_MS[0]);
    }

    #[test]
    fn orphans_and_generations() {
        let mut h = Harness::new().go_live();
        h.advance(DEFAULT_RECYCLE_AFTER_MS + 1.0);
        h.tick();
        h.advance(6_000.0);
        h.tick();
        let orphan = vec![Action::Terminate {
            generation: 1,
            reason: "orphan".into(),
        }];
        assert_eq!(h.watchdog.on_open(1), orphan);
        assert_eq!(h.watchdog.on_message(1), orphan);
        assert_eq!(h.snapshot().status, "connecting");
        assert_eq!(h.watchdog.owned(), Some(2));
        h.open(2);
        h.message(2);
        h.close(1);
        assert_eq!(h.snapshot().status, "live");
        assert_eq!(h.watchdog.owned(), Some(2));

        let mut h = Harness::new().go_live();
        assert_eq!(h.watchdog.high_water_generation(), 1);
        let list = h.watchdog.dispose();
        h.record(list);
        h.configure(live_env());
        h.tick();
        assert_eq!(h.watchdog.owned(), Some(2));

        let time = FakeClock::new();
        let mut first = Watchdog::new(WatchdogOptions::default(), time.clone());
        first.configure(&live_env());
        first.tick();
        let high = first.high_water_generation();
        let mut second = Watchdog::new(
            WatchdogOptions {
                start_generation: high,
                ..WatchdogOptions::default()
            },
            time,
        );
        second.configure(&live_env());
        assert_eq!(
            second.tick(),
            vec![Action::Connect {
                generation: high + 1
            }]
        );
    }

    #[test]
    fn transport_and_auth_failures_are_classified() {
        let mut h = Harness::new().go_live();
        h.fail(1, detail(FailureKind::Transport, "ECONNRESET"));
        assert_eq!(h.types(), ["connect:1", "terminate:1"]);
        assert_eq!(h.snapshot().status, "reconnecting");
        assert_eq!(h.snapshot().error.as_deref(), Some("ECONNRESET"));
        assert_eq!(h.wait(), DEFAULT_BACKOFF_MS[0]);

        let mut h = Harness::new().go_live();
        h.fail(
            1,
            detail(
                FailureKind::Auth,
                "AISStream rejected the API key (HTTP 401)",
            ),
        );
        assert_eq!(h.snapshot().status, "auth-failed");
        assert_eq!(h.wait(), DEFAULT_AUTH_PROBE_MS);
        let before = h.types().len();
        for _ in 0..719 {
            h.advance(5_000.0);
            h.tick();
        }
        assert_eq!(h.types().len(), before);
        h.advance(10_000.0);
        h.tick();
        assert_eq!(h.types().len(), before + 1);
        let probe = h.watchdog.owned().unwrap();
        h.open(probe);
        assert_eq!(h.snapshot().status, "auth-failed");
    }

    #[test]
    fn no_probe_outcome_launders_auth_into_the_ladder() {
        for outcome in ["close", "transport", "rate-limit"] {
            let mut h = Harness::new().go_live();
            h.fail(
                1,
                detail(
                    FailureKind::Auth,
                    "AISStream rejected the API key (HTTP 401)",
                ),
            );
            h.advance(DEFAULT_AUTH_PROBE_MS);
            h.tick();
            let probe = h.watchdog.owned().unwrap();
            for _ in 0..200 {
                h.advance(1_000.0);
                h.tick();
            }
            assert_eq!(h.snapshot().status, "auth-failed");
            match outcome {
                "close" => {
                    h.close(probe);
                }
                "transport" => {
                    h.fail(probe, detail(FailureKind::Transport, "ECONNRESET"));
                }
                _ => {
                    h.fail(
                        probe,
                        FailureDetail {
                            retry_after_ms: Some(5_000.0),
                            ..FailureDetail::of(FailureKind::RateLimit)
                        },
                    );
                }
            }
            let snap = h.snapshot();
            assert_eq!(snap.status, "auth-failed", "{outcome}");
            assert_eq!(h.wait(), DEFAULT_AUTH_PROBE_MS, "{outcome}");
            assert!(
                snap.error.unwrap().contains("rejected the API key"),
                "{outcome}"
            );
        }

        // A silent probe is recycled on the auth cadence only.
        let mut h = Harness::new().go_live();
        h.fail(1, FailureDetail::of(FailureKind::Auth));
        h.advance(DEFAULT_AUTH_PROBE_MS);
        h.tick();
        let before = h.types().len();
        h.advance(DEFAULT_RECYCLE_AFTER_MS + 1_000.0);
        h.tick();
        assert_eq!(h.types().len(), before);
        h.advance(DEFAULT_AUTH_PROBE_MS);
        h.tick();
        assert_eq!(h.types().len(), before + 1);
        assert_eq!(h.watchdog.owned(), None);
        assert_eq!(h.snapshot().status, "auth-failed");
        assert_eq!(h.wait(), DEFAULT_AUTH_PROBE_MS);
    }

    #[test]
    fn key_rotation_clears_auth_and_replaces_old_key_sockets() {
        let mut h = Harness::new().go_live();
        h.fail(1, FailureDetail::of(FailureKind::Auth));
        let before = h.types().len();
        h.configure(live_env());
        h.tick();
        assert_eq!(h.snapshot().status, "auth-failed", "same key stays refused");
        assert_eq!(h.types().len(), before);

        h.advance(DEFAULT_AUTH_PROBE_MS);
        h.tick();
        assert_eq!(h.watchdog.owned(), Some(2));
        let rotation = h.configure(key_b());
        assert_eq!(
            rotation,
            vec![Action::Terminate {
                generation: 2,
                reason: "key-rotated".into()
            }]
        );
        h.tick();
        assert_eq!(h.watchdog.owned(), Some(3));
        assert_eq!(h.snapshot().status, "connecting");
        assert_eq!(h.snapshot().reconnect_attempt, 0);
        h.open(3);
        h.message(3);
        let late = h
            .watchdog
            .on_failure(2, &detail(FailureKind::Auth, "Invalid API key"));
        assert!(late.is_empty());
        assert_eq!(h.snapshot().status, "live");

        let mut h = Harness::new().go_live();
        let rotation = h.configure(key_b());
        assert_eq!(
            rotation,
            vec![Action::Terminate {
                generation: 1,
                reason: "key-rotated".into()
            }]
        );
    }

    #[test]
    fn rate_limits_honour_retry_after_or_enter_at_the_slowest_rung() {
        let mut h = Harness::new().go_live();
        h.fail(
            1,
            FailureDetail {
                kind: FailureKind::RateLimit,
                message: Some("429".into()),
                retry_after_ms: Some(120_000.0),
            },
        );
        assert_eq!(h.snapshot().status, "reconnecting");
        assert_eq!(h.wait(), 120_000.0);

        let mut h = Harness::new().go_live();
        h.fail(1, detail(FailureKind::RateLimit, "429"));
        assert_eq!(h.wait(), *DEFAULT_BACKOFF_MS.last().unwrap());
    }

    fn attempts_per_hour(kind: Option<FailureKind>) -> usize {
        let mut h = Harness::new();
        h.configure(live_env());
        let mut connects = 0;
        for _ in 0..=3600 {
            for action in h.tick() {
                if let Action::Connect { generation } = action {
                    connects += 1;
                    if let Some(kind) = kind {
                        h.fail(generation, FailureDetail::of(kind));
                    }
                }
            }
            h.advance(1_000.0);
        }
        connects
    }

    #[test]
    fn worst_case_attempt_rates_are_pinned() {
        assert_eq!(attempts_per_hour(Some(FailureKind::Transport)), 8);
        assert_eq!(attempts_per_hour(Some(FailureKind::Auth)), 2);
        assert_eq!(attempts_per_hour(Some(FailureKind::RateLimit)), 5);
        assert_eq!(attempts_per_hour(None), 6, "silent recycle loop");
    }

    #[test]
    fn custom_subscriptions_dispose_and_snapshot() {
        let mut h = Harness::new();
        h.configure(WatchdogEnv {
            silence_watch: false,
            ..live_env()
        });
        h.tick();
        h.open(1);
        h.message(1);
        h.advance(DEFAULT_RECYCLE_AFTER_MS * 4.0);
        h.tick();
        assert_eq!(h.types(), ["connect:1"]);
        assert_eq!(h.snapshot().watchdog, "custom-subscription-off");

        let mut h = Harness::new().go_live();
        h.advance(DEFAULT_RECYCLE_AFTER_MS + 1.0);
        h.tick();
        let snap = h.snapshot().to_value();
        let mut keys: Vec<&String> = snap.as_object().unwrap().keys().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "error",
                "lastMessageAt",
                "nextAttemptAt",
                "reconnectAttempt",
                "silentForMs",
                "staleAfterMs",
                "status",
                "watchdog"
            ]
        );
        assert_eq!(snap["staleAfterMs"], 120_000);
        assert!(h.snapshot().next_attempt_at.unwrap() > h.wall());
        h.advance(6_000.0);
        h.tick();
        assert_eq!(
            h.watchdog.dispose(),
            vec![Action::Terminate {
                generation: 2,
                reason: "dispose".into()
            }]
        );
        assert_eq!(h.snapshot().status, "idle");
        assert_eq!(h.snapshot().reconnect_attempt, 0);

        let mut idle = Harness::new();
        idle.configure(keyless());
        assert!(idle.watchdog.dispose().is_empty());
    }

    #[test]
    fn silence_override_parsing_is_strict() {
        let mut warnings = Vec::new();
        let mut warn = |m: String| warnings.push(m);
        for raw in [Some(""), Some("   "), None] {
            assert_eq!(
                parse_silence_timeout_env(raw, &mut warn),
                SilenceOverride::Default
            );
        }
        for raw in ["abc", "-5", "12s"] {
            assert_eq!(
                parse_silence_timeout_env(Some(raw), &mut warn),
                SilenceOverride::Default
            );
        }
        assert_eq!(
            parse_silence_timeout_env(Some("0"), &mut warn),
            SilenceOverride::Off
        );
        assert_eq!(
            parse_silence_timeout_env(Some(" 90000 "), &mut warn),
            SilenceOverride::Timeout(90_000.0)
        );
        assert_eq!(warnings.len(), 3);
        assert!(warnings[0].contains("Ignoring AISSTREAM_SILENCE_TIMEOUT_MS=\"abc\""));
    }

    #[test]
    fn recycle_can_never_be_tuned_below_stale() {
        let time = FakeClock::new();
        let mut watchdog = Watchdog::new(
            WatchdogOptions {
                stale_ms: Some(60_000.0),
                recycle_after_ms: Some(1_000.0),
                ..WatchdogOptions::default()
            },
            time.clone(),
        );
        watchdog.configure(&live_env());
        watchdog.tick();
        watchdog.on_message(1);
        time.advance(30_000.0);
        assert!(watchdog.tick().is_empty());
        time.advance(31_000.0);
        assert!(matches!(
            watchdog.tick().as_slice(),
            [Action::Terminate { .. }]
        ));
    }
}
