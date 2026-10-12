//! Helpers shared by the map-data and regional providers (overpass,
//! military installations, transit, regional): JavaScript string/number
//! semantics the JS modules relied on, insertion-ordered caches, spawned
//! request coalescing and capped request-body reads.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Body;
use futures_util::StreamExt;
use futures_util::future::{BoxFuture, FutureExt, Shared};
use parking_lot::Mutex;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

/// `String(number)` / `Number.prototype.toString()`.
pub fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if value == 0.0 {
        return "0".into();
    }
    let sign = if value < 0.0 { "-" } else { "" };
    let exp_form = format!("{:e}", value.abs());
    let (mantissa, exponent) = exp_form.split_once('e').unwrap_or((&exp_form, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = digits.len() as i64;
    let n = exponent.parse::<i64>().unwrap_or(0) + 1;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let e = n - 1;
        let sign_e = if e >= 0 { "+" } else { "-" };
        if k == 1 {
            format!("{digits}e{sign_e}{}", e.abs())
        } else {
            format!("{}.{}e{sign_e}{}", &digits[..1], &digits[1..], e.abs())
        }
    };
    format!("{sign}{body}")
}

/// A JSON number serialized the way `JSON.stringify` would: integral values
/// without a fraction, non-finite values as `null`.
pub fn js_json_number(value: f64) -> Value {
    if !value.is_finite() {
        return Value::Null;
    }
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        return Value::from(value as i64);
    }
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// `Number.prototype.toFixed(digits)`: exact decimal rounding, ties away
/// from zero.
pub fn to_fixed(value: f64, digits: usize) -> String {
    if !value.is_finite() || value.abs() >= 1e21 {
        return js_number_string(value);
    }
    let negative = value < 0.0;
    let abs = value.abs();
    let wide = format!("{:.*}", digits + 30, abs);
    let cut = wide.len() - 30;
    let (head, tail) = wide.split_at(cut);
    let mut out = if tail.starts_with('5') && tail[1..].bytes().all(|b| b == b'0') {
        round_up_decimal(head)
    } else {
        format!("{:.*}", digits, abs)
    };
    if digits == 0 && out.ends_with('.') {
        out.pop();
    }
    let is_zero = out.bytes().all(|b| b == b'0' || b == b'.');
    if negative && !is_zero {
        format!("-{out}")
    } else if negative && value != 0.0 {
        // (-0.0001).toFixed(2) is "-0.00" in JS.
        format!("-{out}")
    } else {
        out
    }
}

fn round_up_decimal(head: &str) -> String {
    let trimmed = head.strip_suffix('.').unwrap_or(head);
    let mut bytes: Vec<u8> = trimmed.bytes().collect();
    let mut i = bytes.len();
    loop {
        if i == 0 {
            bytes.insert(0, b'1');
            break;
        }
        i -= 1;
        match bytes[i] {
            b'.' => continue,
            b'9' => bytes[i] = b'0',
            d => {
                bytes[i] = d + 1;
                break;
            }
        }
    }
    String::from_utf8(bytes).unwrap_or_default()
}

/// `Math.round`: halves round toward +Infinity.
pub fn js_round(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let floor = value.floor();
    if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

/// `Math.max(a, b)` (NaN-propagating).
pub fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

/// `Math.min(a, b)` (NaN-propagating).
pub fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

/// `Number(value)` for a parsed JSON value.
pub fn js_to_number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Null) => 0.0,
        Some(Value::Bool(b)) => f64::from(u8::from(*b)),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => crate::common::js::number(s),
        Some(Value::Array(items)) => match items.as_slice() {
            [] => 0.0,
            [only] => js_to_number(Some(&Value::String(js_string(Some(only))))),
            _ => f64::NAN,
        },
        Some(Value::Object(_)) => f64::NAN,
    }
}

/// JavaScript truthiness of a JSON value (`undefined` is `None`).
pub fn js_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|v| v != 0.0 && !v.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_)) | Some(Value::Object(_)) => true,
    }
}

/// `String(value)` for a JSON value (`undefined` and `null` included).
pub fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => js_number_string(n.as_f64().unwrap_or(f64::NAN)),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_string(Some(other)),
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".into(),
    }
}

/// `String(value || '')`: falsy values become the empty string.
pub fn js_string_or_empty(value: Option<&Value>) -> String {
    if js_truthy(value) {
        js_string(value)
    } else {
        String::new()
    }
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// Whitespace as JavaScript's `\s` and `String.prototype.trim` see it.
pub fn is_js_space(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
}

/// `String.prototype.trim`.
pub fn js_trim(value: &str) -> &str {
    value.trim_matches(is_js_space)
}

/// `value.replace(/\s+/g, ' ')`.
pub fn collapse_js_space(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for c in value.chars() {
        if is_js_space(c) {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

/// Length in UTF-16 code units (`string.length`).
pub fn utf16_len(value: &str) -> usize {
    value.chars().map(char::len_utf16).sum()
}

/// `string.slice(0, max)` in UTF-16 units; a split surrogate pair is dropped.
pub fn utf16_prefix(value: &str, max: usize) -> String {
    let mut used = 0;
    let mut out = String::new();
    for c in value.chars() {
        used += c.len_utf16();
        if used > max {
            break;
        }
        out.push(c);
    }
    out
}

/// `encodeURIComponent`.
pub fn encode_uri_component(value: &str) -> String {
    const KEEP: &[u8] = b"-_.!~*'()";
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || KEEP.contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `decodeURIComponent`: `None` where JS throws a `URIError`.
pub fn decode_uri_component(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let text = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(text, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// SHA-1 hex digest (the JS disk caches' file names).
pub fn sha1_hex(value: &str) -> String {
    use sha1::{Digest, Sha1};
    let digest = Sha1::digest(value.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Dates
// ---------------------------------------------------------------------------

/// `new Date(ms).toISOString()`.
pub fn iso_from_ms(ms: f64) -> Option<String> {
    if !ms.is_finite() {
        return None;
    }
    let ms = ms.trunc() as i64;
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
}

/// `new Date().toISOString()`.
pub fn now_iso() -> String {
    iso_from_ms(crate::common::js::now_ms() as f64).unwrap_or_default()
}

/// `Date.parse` for the formats the providers meet: ECMAScript ISO date
/// time strings (zoneless date-times read as UTC, the server's zone in the
/// container), RFC 2822 / HTTP dates and a space-separated ISO variant.
pub fn js_date_parse(value: &str) -> Option<i64> {
    let s = js_trim(value);
    if s.is_empty() {
        return None;
    }
    if let Some(ms) = parse_iso(s) {
        return Some(ms);
    }
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(s) {
        return Some(date.timestamp_millis());
    }
    let normalized = s.replacen(' ', "T", 1);
    if normalized != s
        && let Some(ms) = parse_iso(&normalized)
    {
        return Some(ms);
    }
    for format in ["%a %b %d %Y %H:%M:%S GMT%z", "%a, %d %b %Y %H:%M:%S %z"] {
        if let Ok(date) = chrono::DateTime::parse_from_str(s, format) {
            return Some(date.timestamp_millis());
        }
    }
    None
}

fn parse_iso(s: &str) -> Option<i64> {
    let re = iso_regex();
    let caps = re.captures(s)?;
    let num = |i: usize| caps.get(i).map(|m| m.as_str().parse::<i64>().ok());
    let year: i64 = caps.get(1)?.as_str().parse().ok()?;
    let month = num(2).unwrap_or(Some(1))?;
    let day = num(3).unwrap_or(Some(1))?;
    let hour = num(4).unwrap_or(Some(0))?;
    let minute = num(5).unwrap_or(Some(0))?;
    let second = num(6).unwrap_or(Some(0))?;
    let millis = match caps.get(7) {
        Some(m) => {
            let digits: String = m.as_str().chars().chain("000".chars()).take(3).collect();
            digits.parse::<i64>().ok()?
        }
        None => 0,
    };
    if !(1..=12).contains(&month) || hour > 24 || minute > 59 || second > 59 {
        return None;
    }
    if hour == 24 && (minute != 0 || second != 0 || millis != 0) {
        return None;
    }
    let date = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)?;
    let base = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis();
    let mut ms = base + ((hour * 60 + minute) * 60 + second) * 1000 + millis;
    if let Some(zone) = caps.get(8) {
        let zone = zone.as_str();
        if zone != "Z" && zone != "z" {
            let sign = if zone.starts_with('-') { -1 } else { 1 };
            let digits: String = zone[1..].chars().filter(char::is_ascii_digit).collect();
            let hh: i64 = digits.get(..2)?.parse().ok()?;
            let mm: i64 = digits.get(2..4)?.parse().ok()?;
            if hh > 23 || mm > 59 {
                return None;
            }
            ms -= sign * (hh * 60 + mm) * 60_000;
        }
    }
    Some(ms)
}

fn iso_regex() -> &'static regex::Regex {
    static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
        regex::Regex::new(
            r"^([+-][0-9]{6}|[0-9]{4})(?:-([0-9]{2})(?:-([0-9]{2}))?)?(?:[Tt]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?)?([Zz]|[+-][0-9]{2}:?[0-9]{2})?$",
        )
        .expect("valid ISO regex")
    });
    &RE
}

// ---------------------------------------------------------------------------
// Insertion-ordered cache (a JS `Map` trimmed oldest-first)
// ---------------------------------------------------------------------------

/// A `Map` whose `set` keeps an existing key's position and whose trim
/// evicts in insertion order, like the JS caches it replaces.
pub struct InsertionMap<V> {
    items: HashMap<String, (u64, V)>,
    order: BTreeMap<u64, String>,
    next: u64,
}

impl<V> Default for InsertionMap<V> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: BTreeMap::new(),
            next: 0,
        }
    }
}

impl<V> InsertionMap<V> {
    pub fn get(&self, key: &str) -> Option<&V> {
        self.items.get(key).map(|(_, v)| v)
    }

    pub fn set(&mut self, key: &str, value: V) {
        if let Some(slot) = self.items.get_mut(key) {
            slot.1 = value;
            return;
        }
        self.next += 1;
        self.order.insert(self.next, key.to_string());
        self.items.insert(key.to_string(), (self.next, value));
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Evict the oldest entries until at most `max` remain.
    pub fn trim(&mut self, max: usize) {
        while self.items.len() > max {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            self.items.remove(&oldest);
        }
    }
}

// ---------------------------------------------------------------------------
// In-flight request sharing whose work runs to completion on its own task
// ---------------------------------------------------------------------------

type SharedTask<V> = Shared<BoxFuture<'static, V>>;
type TaskMap<K, V> = HashMap<K, (u64, SharedTask<V>)>;

/// The JS in-flight `Map<key, Promise>`: a started request settles and
/// leaves the map even when every caller has gone away.
pub struct InFlight<K, V: Clone> {
    map: Arc<Mutex<TaskMap<K, V>>>,
    next: AtomicU64,
}

impl<K, V: Clone> Default for InFlight<K, V> {
    fn default() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            next: AtomicU64::new(0),
        }
    }
}

impl<K, V> InFlight<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn contains(&self, key: &K) -> bool {
        self.map.lock().contains_key(key)
    }

    pub fn get(&self, key: &K) -> Option<SharedTask<V>> {
        self.map.lock().get(key).map(|(_, task)| task.clone())
    }

    /// Start `work` on its own task under `key`, replacing any entry.
    pub fn start<Fut>(&self, key: K, work: Fut) -> SharedTask<V>
    where
        Fut: Future<Output = V> + Send + 'static,
    {
        let mut map = self.map.lock();
        self.spawn_locked(&mut map, key, work)
    }

    /// `coalesceProxyRequest`: join the request in flight for `key`, or
    /// start one. Returns the shared result future and whether it was joined.
    pub fn coalesce<F, Fut>(&self, key: K, create: F) -> (SharedTask<V>, bool)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V> + Send + 'static,
    {
        let mut map = self.map.lock();
        if let Some((_, task)) = map.get(&key) {
            return (task.clone(), true);
        }
        let task = self.spawn_locked(&mut map, key, create());
        (task, false)
    }

    fn spawn_locked<Fut>(&self, map: &mut TaskMap<K, V>, key: K, work: Fut) -> SharedTask<V>
    where
        Fut: Future<Output = V> + Send + 'static,
    {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let map_ref = Arc::clone(&self.map);
        let cleanup_key = key.clone();
        let handle = tokio::spawn(async move {
            let value = work.await;
            let mut map = map_ref.lock();
            if map.get(&cleanup_key).is_some_and(|(entry, _)| *entry == id) {
                map.remove(&cleanup_key);
            }
            value
        });
        let task = handle
            .map(|joined| match joined {
                Ok(value) => value,
                Err(error) => std::panic::resume_unwind(error.into_panic()),
            })
            .boxed()
            .shared();
        map.insert(key, (id, task.clone()));
        task
    }
}

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

/// Why a capped request-body read failed.
#[derive(Debug)]
pub enum BodyReadError {
    /// `code: 'BODY_TOO_LARGE'`.
    TooLarge,
    Failed,
}

/// `readRequestBodyCapped`: buffer a request body, refusing more than `max`.
pub async fn read_body_capped(body: Body, max: usize) -> Result<Vec<u8>, BodyReadError> {
    let mut stream = body.into_data_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyReadError::Failed)?;
        if out.len() + chunk.len() > max {
            return Err(BodyReadError::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// The peer address recorded by `into_make_service_with_connect_info`.
pub fn peer_key(extensions: &http::Extensions) -> String {
    crate::common::rate_limit::client_key(
        extensions
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|info| info.0),
    )
}

/// The request path after a connect-style mount point (`req.url`): the
/// remainder of the path, `/` when empty, plus the query string.
pub fn mounted_url(uri: &http::Uri, mount: &str) -> String {
    let path = uri.path();
    let rest = path.strip_prefix(mount).unwrap_or(path);
    let rest = if rest.is_empty() { "/" } else { rest };
    match uri.query() {
        Some(query) => format!("{rest}?{query}"),
        None => rest.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_strings_match_js() {
        assert_eq!(js_number_string(30.0), "30");
        assert_eq!(js_number_string(-97.75), "-97.75");
        assert_eq!(js_number_string(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(js_number_string(1e21), "1e+21");
        assert_eq!(js_number_string(1e-7), "1e-7");
        assert_eq!(js_number_string(0.000001), "0.000001");
        assert_eq!(js_number_string(123456789012.0), "123456789012");
        assert_eq!(js_number_string(-0.0), "0");
    }

    #[test]
    fn to_fixed_matches_js() {
        assert_eq!(to_fixed(30.25, 3), "30.250");
        assert_eq!(to_fixed(0.125, 2), "0.13");
        assert_eq!(to_fixed(0.25, 1), "0.3");
        assert_eq!(to_fixed(-0.25, 1), "-0.3");
        assert_eq!(to_fixed(1.005, 2), "1.00");
        assert_eq!(to_fixed(-97.7431, 5), "-97.74310");
        assert_eq!(to_fixed(9.995, 0), "10");
        assert_eq!(to_fixed(0.5, 0), "1");
        assert_eq!(to_fixed(99.5, 0), "100");
        assert_eq!(to_fixed(-0.0, 1), "0.0");
        assert_eq!(to_fixed(42.36000061035156, 6), "42.360001");
    }

    #[test]
    fn js_round_and_dates() {
        assert_eq!(js_round(-2.5), -2.0);
        assert_eq!(js_round(2.5), 3.0);
        assert_eq!(
            js_date_parse("2026-08-17T00:15Z").and_then(|ms| iso_from_ms(ms as f64)),
            Some("2026-08-17T00:15:00.000Z".into())
        );
        assert_eq!(
            js_date_parse("2026-08-17T00:15:00+02:00").and_then(|ms| iso_from_ms(ms as f64)),
            Some("2026-08-16T22:15:00.000Z".into())
        );
        assert_eq!(
            js_date_parse("Wed, 23 Sep 2026 12:02:00 GMT"),
            Some(1_790_164_920_000)
        );
        assert_eq!(js_date_parse("not-a-time"), None);
        assert_eq!(js_date_parse("2026-07-22"), Some(1_784_678_400_000));
    }

    #[test]
    fn uri_components() {
        assert_eq!(encode_uri_component("a b/é(x)"), "a%20b%2F%C3%A9(x)");
        assert_eq!(decode_uri_component("bus%201").as_deref(), Some("bus 1"));
        assert_eq!(decode_uri_component("%E0%A4%A"), None);
        assert_eq!(decode_uri_component("%FF"), None);
    }

    #[test]
    fn insertion_map_keeps_first_position() {
        let mut map = InsertionMap::default();
        map.set("a", 1);
        map.set("b", 2);
        map.set("a", 3);
        map.set("c", 4);
        map.trim(2);
        assert_eq!(map.get("a"), None);
        assert_eq!(map.get("b"), Some(&2));
        assert_eq!(map.len(), 2);
    }

    #[tokio::test]
    async fn coalescing_shares_one_task_and_clears() {
        let flights: InFlight<String, u32> = InFlight::default();
        let (first, shared_a) = flights.coalesce("cell".into(), || async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            7
        });
        let (second, shared_b) = flights.coalesce("cell".into(), || async { 9 });
        assert!(!shared_a);
        assert!(shared_b);
        assert_eq!(second.await, 7);
        assert_eq!(first.await, 7);
        tokio::task::yield_now().await;
        assert!(!flights.contains(&"cell".to_string()));
    }
}
