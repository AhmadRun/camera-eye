//! Per-key sliding-window admission (port of `makeRateLimiter` and
//! `server/providers/common/rate-limit.js`).

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

use parking_lot::Mutex;

use super::js;

const RATE_LIMITER_MAX_KEYS: usize = 2000;

pub struct RateLimiter {
    window_ms: i64,
    max: usize,
    global_max: Option<usize>,
    state: Mutex<LimiterState>,
}

#[derive(Default)]
struct LimiterState {
    // Insertion order is kept so the oldest key can be evicted first.
    order: VecDeque<String>,
    hits: HashMap<String, Vec<i64>>,
    global: Vec<i64>,
}

impl RateLimiter {
    pub fn new(window_ms: i64, max: usize, global_max: Option<usize>) -> Self {
        Self {
            window_ms,
            max,
            global_max: global_max.filter(|g| *g > 0),
            state: Mutex::new(LimiterState::default()),
        }
    }

    /// Whether `key` may make another request now.
    pub fn allow(&self, key: &str) -> bool {
        self.allow_at(key, js::now_ms())
    }

    fn allow_at(&self, key: &str, now: i64) -> bool {
        let window = self.window_ms;
        let mut state = self.state.lock();
        state.global.retain(|t| now - t < window);
        if let Some(global_max) = self.global_max {
            if state.global.len() >= global_max {
                return false;
            }
        }
        let mut recent: Vec<i64> = state
            .hits
            .get(key)
            .map(|times| times.iter().copied().filter(|t| now - t < window).collect())
            .unwrap_or_default();
        let is_new = !state.hits.contains_key(key);
        if recent.len() >= self.max {
            state.hits.insert(key.to_string(), recent);
            if is_new {
                state.order.push_back(key.to_string());
            }
            return false;
        }
        recent.push(now);
        state.hits.insert(key.to_string(), recent);
        if is_new {
            state.order.push_back(key.to_string());
        }
        state.global.push(now);
        if state.hits.len() > RATE_LIMITER_MAX_KEYS {
            if let Some(oldest) = state.order.pop_front() {
                state.hits.remove(&oldest);
            }
        }
        if state.hits.len() > 256 {
            let stale: Vec<String> = state
                .hits
                .iter()
                .filter(|(_, v)| v.last().is_none_or(|last| now - last > window))
                .map(|(k, _)| k.clone())
                .collect();
            for key in stale {
                state.hits.remove(&key);
                state.order.retain(|k| k != &key);
            }
        }
        true
    }
}

/// `resolvePerMinuteCap`: blank or unreadable takes the default, an
/// explicit `0` disables, a fraction rounds up to 1.
pub fn resolve_per_minute_cap(env_value: Option<&str>, default_per_min: u64) -> u64 {
    let Some(raw) = env_value.map(str::trim) else {
        return default_per_min;
    };
    if raw.is_empty() {
        return default_per_min;
    }
    let parsed = js::number(raw);
    if !parsed.is_finite() || parsed < 0.0 {
        return default_per_min;
    }
    if parsed > 0.0 && parsed < 1.0 {
        return 1;
    }
    parsed.floor() as u64
}

/// `makeCostRateLimiter`: `None` only for the explicit `0` opt-out.
pub fn cost_rate_limiter(env_value: Option<&str>, default_per_min: u64) -> Option<RateLimiter> {
    let max = resolve_per_minute_cap(env_value, default_per_min);
    if max == 0 {
        return None;
    }
    Some(RateLimiter::new(
        60_000,
        max as usize,
        Some(max as usize * 20),
    ))
}

/// The client key for rate limiting: the socket peer address only.
pub fn client_key(peer: Option<SocketAddr>) -> String {
    peer.map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "local".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_limits_per_key() {
        let limiter = RateLimiter::new(1000, 2, None);
        assert!(limiter.allow_at("a", 0));
        assert!(limiter.allow_at("a", 10));
        assert!(!limiter.allow_at("a", 20));
        assert!(limiter.allow_at("b", 20));
        assert!(limiter.allow_at("a", 1500));
    }

    #[test]
    fn caps_resolve_like_js() {
        assert_eq!(resolve_per_minute_cap(None, 10), 10);
        assert_eq!(resolve_per_minute_cap(Some(""), 10), 10);
        assert_eq!(resolve_per_minute_cap(Some("3O"), 10), 10);
        assert_eq!(resolve_per_minute_cap(Some("0"), 10), 0);
        assert_eq!(resolve_per_minute_cap(Some("0.5"), 10), 1);
        assert_eq!(resolve_per_minute_cap(Some("25"), 10), 25);
    }
}
