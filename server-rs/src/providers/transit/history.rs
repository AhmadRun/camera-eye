//! Bounded, process-local operator position history (port of
//! `src/sources/transitHistoryStore.js`). No upstream or scene access.
//!
//! The JS keeps fixes and epoch contexts in fixed-size `DataView`s; this
//! port keeps them in plain structs but charges exactly the same byte
//! sizes against the same feed and process ceilings, so admission,
//! eviction and truncation decisions match.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::feeds::TransitFeed;
use super::gtfs::Vehicle;
use crate::providers::mapdata_support::js_json_number;

/// `TRANSIT_HISTORY_LIMITS`.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub retention_ms: f64,
    pub fixes: usize,
    pub feed_bytes: i64,
    pub process_bytes: i64,
    pub keys: i64,
    pub response_bytes: usize,
}

pub const TRANSIT_HISTORY_LIMITS: Limits = Limits {
    retention_ms: 900_000.0,
    fixes: 128,
    feed_bytes: 16 * 1024 * 1024,
    process_bytes: 64 * 1024 * 1024,
    keys: 15_000,
    response_bytes: 32_768,
};

/// Per-store overrides; each only ever lowers the default.
#[derive(Clone, Copy, Debug, Default)]
pub struct LimitOverrides {
    pub feed_bytes: Option<f64>,
    pub process_bytes: Option<f64>,
    pub keys: Option<f64>,
}

const FIX_BYTES: i64 = 32;
const CHUNK: usize = 8;
const EPOCH_SLOTS: u32 = 8;
const EPOCH_BYTES: i64 = 544;
const META_BYTES: i64 = 64;

/// `validTransitIdentifier`: the entire decoded identifier, including its
/// UTF-8 length. (A Rust `str` cannot hold a lone surrogate.)
pub fn valid_transit_identifier(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|c| (c as u32) <= 0x1f || c == '\u{7f}' || c == '/' || c == '\\')
        && value != "."
        && value != ".."
        && value.len() <= 256
}

#[derive(Clone, Copy, Debug)]
struct Fix {
    t: f64,
    lat: f64,
    lon: f64,
    flags: u32,
    epoch: u32,
}

#[derive(Clone, Default, Debug, PartialEq)]
struct Context {
    id: u32,
    trip: String,
    route: String,
    mode: String,
}

struct Entry {
    seq: u64,
    feed: String,
    contexts: [Option<Context>; EPOCH_SLOTS as usize],
    fixes: VecDeque<Fix>,
    capacity: usize,
    epoch: u32,
    observed_at: f64,
    truncated: bool,
    bytes: i64,
}

impl Entry {
    fn drop_oldest(&mut self) {
        self.fixes.pop_front();
        self.truncated = true;
    }
    fn first_time(&self) -> f64 {
        self.fixes.front().map_or(f64::INFINITY, |f| f.t)
    }
    fn context_at(&self, slot: u32) -> Context {
        self.contexts[slot as usize].clone().unwrap_or_default()
    }
}

/// A queue position: oldest `observedAt` first, ties by key in UTF-16
/// code-unit order (JS `<` on strings).
#[derive(Clone, Debug, PartialEq)]
struct QueueKey {
    observed_at: f64,
    key: String,
    /// No character at or above U+E000, where UTF-8 byte order and UTF-16
    /// code-unit order can disagree.
    plain: bool,
}

impl QueueKey {
    fn new(observed_at: f64, key: &str) -> Self {
        Self {
            observed_at,
            key: key.to_string(),
            plain: !key.bytes().any(|b| b >= 0xEE),
        }
    }
}

impl Eq for QueueKey {}

impl Ord for QueueKey {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.observed_at
            .total_cmp(&other.observed_at)
            .then_with(|| {
                if self.plain && other.plain {
                    self.key.as_bytes().cmp(other.key.as_bytes())
                } else {
                    self.key.encode_utf16().cmp(other.key.encode_utf16())
                }
            })
    }
}

impl PartialOrd for QueueKey {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// One `createTransitHistory` store's state.
struct Store {
    policy: Limits,
    entries: HashMap<String, Entry>,
    order: BTreeMap<u64, String>,
    next_seq: u64,
    eviction: BTreeSet<QueueKey>,
    feed_eviction: HashMap<String, BTreeSet<QueueKey>>,
    feed_bytes: HashMap<String, i64>,
    allocated_bytes: i64,
    pruned_at: f64,
    sweep_active: bool,
}

/// The allocation ceiling shared by every store in this process.
#[derive(Default)]
pub struct Storage {
    bytes: i64,
    keys: i64,
    feed_bytes: HashMap<String, i64>,
    loss_until: HashMap<String, f64>,
    /// In creation order (the JS `Set`).
    stores: BTreeMap<u64, Store>,
    next_store: u64,
}

type Except<'a> = Option<(u64, &'a str)>;

fn queue_oldest(
    queue: Option<&BTreeSet<QueueKey>>,
    sid: u64,
    except: Except<'_>,
) -> Option<QueueKey> {
    queue?
        .iter()
        .find(|item| except != Some((sid, item.key.as_str())))
        .cloned()
}

impl Storage {
    fn store(&mut self, sid: u64) -> &mut Store {
        self.stores
            .get_mut(&sid)
            .expect("live transit history store")
    }

    fn account(&mut self, sid: u64, feed: &str, delta: i64) {
        self.bytes += delta;
        let total = self.feed_bytes.entry(feed.to_string()).or_insert(0);
        *total += delta;
        if *total == 0 {
            self.feed_bytes.remove(feed);
        }
        let store = self.store(sid);
        store.allocated_bytes += delta;
        let own = store.feed_bytes.entry(feed.to_string()).or_insert(0);
        *own += delta;
        if *own == 0 {
            store.feed_bytes.remove(feed);
        }
    }

    fn record_loss(&mut self, feed: &str, until: f64) {
        let current = self.loss_until.get(feed).copied().unwrap_or(0.0);
        self.loss_until.insert(feed.to_string(), current.max(until));
    }

    fn queue(&mut self, sid: u64, key: &str) {
        let store = self.store(sid);
        let Some(entry) = store.entries.get(key) else {
            return;
        };
        let item = QueueKey::new(entry.observed_at, key);
        let feed = entry.feed.clone();
        store.eviction.insert(item.clone());
        store.feed_eviction.entry(feed).or_default().insert(item);
    }

    fn unqueue(&mut self, sid: u64, key: &str) {
        let store = self.store(sid);
        let Some(entry) = store.entries.get(key) else {
            return;
        };
        let item = QueueKey::new(entry.observed_at, key);
        let feed = entry.feed.clone();
        store.eviction.remove(&item);
        if let Some(queue) = store.feed_eviction.get_mut(&feed) {
            queue.remove(&item);
            if queue.is_empty() {
                store.feed_eviction.remove(&feed);
            }
        }
    }

    fn remove(&mut self, sid: u64, key: &str) {
        self.unqueue(sid, key);
        let store = self.store(sid);
        let Some(entry) = store.entries.remove(key) else {
            return;
        };
        store.order.remove(&entry.seq);
        self.keys -= 1;
        self.account(sid, &entry.feed, -entry.bytes);
    }

    fn can_allocate(&self, policy: &Limits, feed: &str, bytes: i64) -> bool {
        self.bytes + bytes <= policy.process_bytes
            && self.feed_bytes.get(feed).copied().unwrap_or(0) + bytes <= policy.feed_bytes
    }

    fn resize(&mut self, sid: u64, key: &str, capacity: usize) {
        let store = self.store(sid);
        let Some(entry) = store.entries.get_mut(key) else {
            return;
        };
        let delta = (capacity as i64 - entry.capacity as i64) * FIX_BYTES;
        entry.capacity = capacity;
        entry.bytes += delta;
        let feed = entry.feed.clone();
        self.account(sid, &feed, delta);
    }

    fn prune(&mut self, sid: u64, now: f64) {
        let store = self.store(sid);
        if now == store.pruned_at {
            return;
        }
        store.pruned_at = now;
        let policy = store.policy;
        self.loss_until.retain(|_, until| *until > now);
        let keys: Vec<String> = self.store(sid).order.values().cloned().collect();
        for key in keys {
            let store = self.store(sid);
            let Some(entry) = store.entries.get_mut(&key) else {
                continue;
            };
            while !entry.fixes.is_empty() && entry.first_time() < now - policy.retention_ms {
                entry.drop_oldest();
            }
            let count = entry.fixes.len();
            let capacity = entry.capacity;
            let feed = entry.feed.clone();
            if count == 0 || now - entry.observed_at >= policy.retention_ms {
                self.remove(sid, &key);
            } else {
                let target = CHUNK.max(count.div_ceil(CHUNK) * CHUNK);
                if capacity > CHUNK
                    && count * 2 <= capacity
                    && self.can_allocate(&policy, &feed, target as i64 * FIX_BYTES)
                {
                    self.resize(sid, &key, target);
                }
            }
        }
    }

    fn oldest(&self, sid: u64, feed: Option<&str>, except: Except<'_>) -> Option<QueueKey> {
        let store = self.stores.get(&sid)?;
        let queue = match feed {
            Some(feed) => store.feed_eviction.get(feed),
            None => Some(&store.eviction),
        };
        queue_oldest(queue, sid, except)
    }

    fn evict_one(&mut self, sid: u64, feed: Option<&str>, except: Except<'_>) -> bool {
        let Some(item) = self.oldest(sid, feed, except) else {
            return false;
        };
        let policy = self.store(sid).policy;
        let entry = &self.store(sid).entries[&item.key];
        let (entry_feed, observed_at, count, capacity) = (
            entry.feed.clone(),
            entry.observed_at,
            entry.fixes.len(),
            entry.capacity,
        );
        self.record_loss(&entry_feed, observed_at + policy.retention_ms);
        if count > CHUNK {
            let target = CHUNK.max(capacity.saturating_sub(CHUNK));
            if !self.can_allocate(&policy, &entry_feed, target as i64 * FIX_BYTES) {
                self.remove(sid, &item.key);
                return true;
            }
            let entry = self.store(sid).entries.get_mut(&item.key).expect("entry");
            while entry.fixes.len() > target {
                entry.drop_oldest();
            }
            self.resize(sid, &item.key, target);
        } else {
            self.remove(sid, &item.key);
        }
        true
    }

    /// The oldest evictable entry across every store, first store on ties.
    fn oldest_owner(&self, feed: Option<&str>, except: Except<'_>) -> Option<u64> {
        let mut best: Option<(u64, f64)> = None;
        for &sid in self.stores.keys() {
            if let Some(item) = self.oldest(sid, feed, except)
                && best.is_none_or(|(_, at)| item.observed_at < at)
            {
                best = Some((sid, item.observed_at));
            }
        }
        best.map(|(sid, _)| sid)
    }

    fn reserve(
        &mut self,
        sid: u64,
        feed: &str,
        bytes: i64,
        except: Except<'_>,
        new_key: bool,
    ) -> bool {
        let policy = self.store(sid).policy;
        while self.feed_bytes.get(feed).copied().unwrap_or(0) + bytes > policy.feed_bytes {
            match self.oldest_owner(Some(feed), except) {
                Some(owner) if self.evict_one(owner, Some(feed), except) => {}
                _ => return false,
            }
        }
        while self.bytes + bytes > policy.process_bytes || (new_key && self.keys >= policy.keys) {
            match self.oldest_owner(None, except) {
                Some(owner) if self.evict_one(owner, None, except) => {}
                _ => return false,
            }
        }
        true
    }

    fn ingest(&mut self, sid: u64, feed: &TransitFeed, records: &[Vehicle], now: f64) {
        if !self.stores.contains_key(&sid)
            || !feed.history_retention
            || !valid_transit_identifier(feed.id)
        {
            return;
        }
        self.prune(sid, now);
        let policy = self.store(sid).policy;
        // Pin all existing snapshot members before admitting any new key.
        let mut protected: Vec<String> = Vec::new();
        let mut protected_set: HashSet<String> = HashSet::new();
        for record in records {
            let key = format!("{}\u{0}{}", feed.id, record.id);
            if self.store(sid).entries.contains_key(&key) && protected_set.insert(key.clone()) {
                self.unqueue(sid, &key);
                protected.push(key);
            }
        }
        for record in records {
            if !valid_transit_identifier(&record.id)
                || !record.lat.is_finite()
                || record.lat.abs() > 90.0
                || !record.lon.is_finite()
                || record.lon.abs() > 180.0
            {
                continue;
            }
            let t = record.timestamp.unwrap_or(0.0) * 1000.0;
            if !t.is_finite() || t < now - policy.retention_ms || t > now + 10_000.0 {
                continue;
            }
            let mode = feed
                .route_mode
                .map(|f| f(record.route_id.as_deref()))
                .filter(|m| !m.is_empty())
                .unwrap_or(if feed.default_mode.is_empty() {
                    "unknown"
                } else {
                    feed.default_mode
                });
            let trip = record.trip_id.clone().unwrap_or_default();
            let route = record.route_id.clone().unwrap_or_default();
            if trip.len() > 256 || route.len() > 256 {
                continue;
            }
            let mode_bytes = &mode.as_bytes()[..mode.len().min(16)];
            let context = Context {
                id: 0,
                trip,
                route,
                mode: String::from_utf8_lossy(mode_bytes).into_owned(),
            };
            let key = format!("{}\u{0}{}", feed.id, record.id);
            if let Some(entry) = self.store(sid).entries.get(&key) {
                if entry.fixes.back().is_some_and(|last| t <= last.t) {
                    continue;
                }
            } else {
                let bytes = META_BYTES + EPOCH_BYTES + key.len() as i64 + CHUNK as i64 * FIX_BYTES;
                if !self.reserve(sid, feed.id, bytes, None, true) {
                    self.record_loss(feed.id, now + policy.retention_ms);
                    continue;
                }
                let truncated = self.loss_until.get(feed.id).copied().unwrap_or(0.0) > now;
                let store = self.store(sid);
                let seq = store.next_seq;
                store.next_seq += 1;
                store.order.insert(seq, key.clone());
                store.entries.insert(
                    key.clone(),
                    Entry {
                        seq,
                        feed: feed.id.to_string(),
                        contexts: [
                            Some(Context::default()),
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        ],
                        fixes: VecDeque::with_capacity(CHUNK),
                        capacity: CHUNK,
                        epoch: 0,
                        observed_at: now,
                        truncated,
                        bytes,
                    },
                );
                if protected_set.insert(key.clone()) {
                    protected.push(key.clone());
                }
                self.keys += 1;
                self.account(sid, feed.id, bytes);
            }
            let mut flags: u32 = match record.timestamp_source {
                Some("vehicle") => 1,
                Some("header") => 2,
                _ => 4,
            };
            let entry = &self.store(sid).entries[&key];
            let previous =
                (entry.epoch > 0).then(|| entry.context_at((entry.epoch - 1) % EPOCH_SLOTS));
            let changed = previous.is_none_or(|p| {
                p.trip != context.trip || p.route != context.route || p.mode != context.mode
            });
            if changed {
                let slot = entry.epoch % EPOCH_SLOTS;
                if entry.contexts[slot as usize].is_none() {
                    if !self.reserve(sid, feed.id, EPOCH_BYTES, Some((sid, key.as_str())), false) {
                        self.store(sid)
                            .entries
                            .get_mut(&key)
                            .expect("entry")
                            .truncated = true;
                        self.record_loss(feed.id, now + policy.retention_ms);
                        continue;
                    }
                    let entry = self.store(sid).entries.get_mut(&key).expect("entry");
                    entry.contexts[slot as usize] = Some(Context::default());
                    entry.bytes += EPOCH_BYTES;
                    self.account(sid, feed.id, EPOCH_BYTES);
                }
                let entry = self.store(sid).entries.get_mut(&key).expect("entry");
                entry.epoch += 1;
                // Expiring epoch metadata also expires its positions.
                let expired = i64::from(entry.epoch) - i64::from(EPOCH_SLOTS);
                while entry
                    .fixes
                    .front()
                    .is_some_and(|f| i64::from(f.epoch) <= expired)
                {
                    entry.drop_oldest();
                }
                let id = entry.epoch;
                entry.contexts[((id - 1) % EPOCH_SLOTS) as usize] = Some(Context { id, ..context });
                flags |= 16;
            }
            let entry = &self.store(sid).entries[&key];
            if entry.fixes.len() == entry.capacity {
                let capacity = entry.capacity;
                let next = policy.fixes.min(capacity + CHUNK);
                if next > capacity
                    && self.reserve(
                        sid,
                        feed.id,
                        next as i64 * FIX_BYTES,
                        Some((sid, key.as_str())),
                        false,
                    )
                {
                    self.resize(sid, &key, next);
                } else {
                    self.store(sid)
                        .entries
                        .get_mut(&key)
                        .expect("entry")
                        .drop_oldest();
                }
            }
            let entry = self.store(sid).entries.get_mut(&key).expect("entry");
            let epoch = entry.epoch;
            entry.fixes.push_back(Fix {
                t,
                lat: record.lat,
                lon: record.lon,
                flags,
                epoch,
            });
            entry.observed_at = now;
        }
        for key in protected {
            self.queue(sid, &key);
        }
    }

    fn get(&mut self, sid: u64, feed_id: &str, vehicle_id: &str, now: f64) -> Value {
        let mut fixes: Vec<Fix> = Vec::new();
        let mut epochs: Vec<Context> = Vec::new();
        let mut entry_truncated = None;
        if self.stores.contains_key(&sid) {
            self.prune(sid, now);
            if let Some(entry) = self
                .store(sid)
                .entries
                .get(&format!("{feed_id}\u{0}{vehicle_id}"))
            {
                let mut used: Vec<u32> = Vec::new();
                for fix in &entry.fixes {
                    fixes.push(*fix);
                    if !used.contains(&fix.epoch) {
                        used.push(fix.epoch);
                    }
                }
                epochs = used
                    .iter()
                    .map(|id| entry.context_at((id.wrapping_sub(1)) % EPOCH_SLOTS))
                    .collect();
                entry_truncated = Some(entry.truncated);
            }
        }
        let response_bytes = self
            .stores
            .get(&sid)
            .map_or(TRANSIT_HISTORY_LIMITS.response_bytes, |s| {
                s.policy.response_bytes
            });
        let mut truncated = entry_truncated
            .unwrap_or_else(|| self.loss_until.get(feed_id).copied().unwrap_or(0.0) > now);
        let render = |fixes: &[Fix], epochs: &[Context], truncated: bool| {
            json!({
                "version": 1,
                "feedId": feed_id,
                "vehicleId": vehicle_id,
                "oldestT": fixes.first().map_or(Value::Null, |f| js_json_number(f.t)),
                "newestT": fixes.last().map_or(Value::Null, |f| js_json_number(f.t)),
                "truncated": truncated,
                "epochs": epochs.iter().map(|e| json!({
                    "id": e.id,
                    "trip": e.trip,
                    "route": e.route,
                    "mode": e.mode,
                })).collect::<Vec<_>>(),
                "fixes": fixes.iter().map(|f| json!([
                    js_json_number(f.t),
                    js_json_number(f.lat),
                    js_json_number(f.lon),
                    f.flags,
                    f.epoch,
                ])).collect::<Vec<_>>(),
            })
        };
        let newest = fixes.last().map(|f| f.t);
        let mut payload = render(&fixes, &epochs, truncated);
        // A byte guard for worst-case Unicode.
        while serde_json::to_string(&payload).map_or(0, |s| s.len()) > response_bytes
            && !fixes.is_empty()
        {
            fixes.remove(0);
            truncated = true;
            epochs.retain(|e| fixes.iter().any(|f| f.epoch == e.id));
            payload = render(&fixes, &epochs, truncated);
            // `newestT` is not recomputed by the JS guard.
            payload["newestT"] = newest.map_or(Value::Null, js_json_number);
        }
        payload
    }
}

static PROCESS_STORAGE: Lazy<Arc<Mutex<Storage>>> =
    Lazy::new(|| Arc::new(Mutex::new(Storage::default())));

/// One `createTransitHistory` store.
pub struct TransitHistory {
    storage: Arc<Mutex<Storage>>,
    id: u64,
    sweep: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TransitHistory {
    /// A store on the process-wide allocation, with the 60 s prune sweep
    /// when a runtime is available.
    pub fn new(clock: Arc<dyn Fn() -> f64 + Send + Sync>) -> Arc<Self> {
        Self::with(
            LimitOverrides::default(),
            Arc::clone(&PROCESS_STORAGE),
            Some(clock),
        )
    }

    pub fn with(
        limits: LimitOverrides,
        storage: Arc<Mutex<Storage>>,
        sweep: Option<Arc<dyn Fn() -> f64 + Send + Sync>>,
    ) -> Arc<Self> {
        let mut policy = TRANSIT_HISTORY_LIMITS;
        let lower = |current: f64, limit: Option<f64>| match limit {
            Some(limit) if limit.is_finite() && limit > 0.0 => current.min(limit.floor()),
            _ => current,
        };
        policy.feed_bytes = lower(policy.feed_bytes as f64, limits.feed_bytes) as i64;
        policy.process_bytes = lower(policy.process_bytes as f64, limits.process_bytes) as i64;
        policy.keys = lower(policy.keys as f64, limits.keys) as i64;
        let id = {
            let mut guard = storage.lock();
            guard.next_store += 1;
            let id = guard.next_store;
            guard.stores.insert(
                id,
                Store {
                    policy,
                    entries: HashMap::new(),
                    order: BTreeMap::new(),
                    next_seq: 0,
                    eviction: BTreeSet::new(),
                    feed_eviction: HashMap::new(),
                    feed_bytes: HashMap::new(),
                    allocated_bytes: 0,
                    pruned_at: f64::NEG_INFINITY,
                    sweep_active: false,
                },
            );
            id
        };
        let history = Arc::new(Self {
            storage,
            id,
            sweep: Mutex::new(None),
        });
        if let (Some(clock), Ok(runtime)) = (sweep, tokio::runtime::Handle::try_current()) {
            let weak: Weak<Self> = Arc::downgrade(&history);
            let task = runtime.spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    let Some(history) = weak.upgrade() else { break };
                    history.prune(clock());
                }
            });
            *history.sweep.lock() = Some(task);
            if let Some(store) = history.storage.lock().stores.get_mut(&id) {
                store.sweep_active = true;
            }
        }
        history
    }

    pub fn ingest(&self, feed: &TransitFeed, records: &[Vehicle], now: f64) {
        self.storage.lock().ingest(self.id, feed, records, now);
    }

    pub fn get(&self, feed_id: &str, vehicle_id: &str, now: f64) -> Value {
        self.storage.lock().get(self.id, feed_id, vehicle_id, now)
    }

    pub fn prune(&self, now: f64) {
        let mut storage = self.storage.lock();
        if storage.stores.contains_key(&self.id) {
            storage.prune(self.id, now);
        }
    }

    /// Release every entry and leave the shared allocation.
    pub fn clear(&self) {
        if let Some(task) = self.sweep.lock().take() {
            task.abort();
        }
        let mut storage = self.storage.lock();
        if !storage.stores.contains_key(&self.id) {
            return;
        }
        let keys: Vec<String> = storage.store(self.id).order.values().cloned().collect();
        for key in keys {
            storage.remove(self.id, &key);
        }
        storage.stores.remove(&self.id);
    }

    #[cfg(test)]
    pub fn diagnostics(&self) -> (i64, i64, usize, bool) {
        let storage = self.storage.lock();
        match storage.stores.get(&self.id) {
            Some(store) => (
                store.allocated_bytes,
                storage.bytes,
                store.entries.len(),
                store.sweep_active,
            ),
            None => (0, storage.bytes, 0, false),
        }
    }
}

impl Drop for TransitHistory {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static FEED: TransitFeed = TransitFeed {
        id: "mbta",
        history_retention: true,
        name: "MBTA",
        operator: "",
        region: "",
        center: (0.0, 0.0),
        load_radius_km: 0.0,
        url: "https://example.test/",
        headers: &[],
        license: "",
        license_url: "",
        attribution: "",
        default_enabled: true,
        default_mode: "bus",
        route_mode: None,
    };

    fn feed_with(id: &'static str, retention: bool) -> TransitFeed {
        TransitFeed {
            id,
            history_retention: retention,
            ..clone_feed()
        }
    }

    fn clone_feed() -> TransitFeed {
        TransitFeed {
            id: FEED.id,
            history_retention: FEED.history_retention,
            name: FEED.name,
            operator: FEED.operator,
            region: FEED.region,
            center: FEED.center,
            load_radius_km: FEED.load_radius_km,
            url: FEED.url,
            headers: FEED.headers,
            license: FEED.license,
            license_url: FEED.license_url,
            attribution: FEED.attribution,
            default_enabled: FEED.default_enabled,
            default_mode: FEED.default_mode,
            route_mode: FEED.route_mode,
        }
    }

    fn row(t: f64, id: &str) -> Vehicle {
        Vehicle {
            id: id.to_string(),
            lat: 42.0,
            lon: -71.0,
            bearing: None,
            speed_mps: None,
            timestamp: Some(t / 1000.0),
            route_id: Some("1".into()),
            trip_id: Some("trip".into()),
            direction_id: None,
            label: None,
            stop_id: None,
            status: None,
            occupancy: None,
            timestamp_source: Some("vehicle"),
        }
    }

    fn storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::default()))
    }

    fn history(limits: LimitOverrides) -> Arc<TransitHistory> {
        TransitHistory::with(limits, storage(), None)
    }

    fn fixes(value: &Value) -> &Vec<Value> {
        value["fixes"].as_array().unwrap()
    }

    #[test]
    fn ttl_ages_reports_and_duplicate_times_never_append() {
        let h = history(LimitOverrides::default());
        h.ingest(&FEED, &[row(100_000.0, "bus")], 100_000.0);
        let mut moved = row(100_000.0, "bus");
        moved.lat = 43.0;
        h.ingest(&FEED, &[row(100_000.0, "bus"), moved], 500_000.0);
        assert_eq!(fixes(&h.get("mbta", "bus", 500_000.0)).len(), 1);
        assert_eq!(fixes(&h.get("mbta", "bus", 999_999.0)).len(), 1);
        assert_eq!(fixes(&h.get("mbta", "bus", 1_000_000.0)).len(), 0);
        assert_eq!(h.diagnostics().0, 0);
    }

    #[test]
    fn ring_of_128_fixes_and_bounded_epoch_contexts() {
        let h = history(LimitOverrides::default());
        for i in 0..200 {
            let mut r = row(f64::from(i) * 1000.0, "bus");
            r.trip_id = Some(if i < 100 { "first" } else { "second" }.into());
            h.ingest(&FEED, &[r], f64::from(i) * 1000.0);
        }
        let result = h.get("mbta", "bus", 199_000.0);
        assert_eq!(fixes(&result).len(), 128);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["epochs"].as_array().unwrap().len(), 2);
        let at = fixes(&result).iter().find(|f| f[0] == 100_000).unwrap();
        assert!(at[3].as_u64().unwrap() & 16 != 0);
        assert!(
            fixes(&result)
                .iter()
                .all(|f| f.as_array().unwrap().len() == 5)
        );
        assert!(serde_json::to_string(&result).unwrap().len() < 32_768);
        for i in 200..220 {
            let mut r = row(f64::from(i) * 1000.0, "bus");
            r.trip_id = Some(format!("trip-{i}"));
            h.ingest(&FEED, &[r], f64::from(i) * 1000.0);
        }
        let churn = h.get("mbta", "bus", 219_000.0);
        let epochs = churn["epochs"].as_array().unwrap();
        assert!(epochs.len() <= 8);
        assert!(
            fixes(&churn)
                .iter()
                .all(|f| epochs.iter().any(|e| e["id"] == f[4]))
        );
    }

    #[test]
    fn ceilings_hold_across_stores() {
        let shared = storage();
        let limits = LimitOverrides {
            feed_bytes: Some(16_000.0),
            process_bytes: Some(24_000.0),
            keys: Some(4.0),
        };
        let a = TransitHistory::with(limits, Arc::clone(&shared), None);
        let b = TransitHistory::with(limits, Arc::clone(&shared), None);
        let other = feed_with("other", true);
        for i in 0..60 {
            let now = f64::from(i) * 1000.0;
            a.ingest(&FEED, &[row(now, &format!("a{i}"))], now);
            b.ingest(
                if i % 2 == 1 { &FEED } else { &other },
                &[row(now, &format!("b{i}"))],
                now,
            );
            let s = shared.lock();
            assert!(s.bytes <= 24_000);
            assert!(s.keys <= 4);
            assert!(s.feed_bytes.values().all(|bytes| *bytes <= 16_000));
            drop(s);
            assert_eq!(a.diagnostics().0 + b.diagnostics().0, shared.lock().bytes);
        }
        a.clear();
        b.clear();
        assert_eq!(shared.lock().bytes, 0);
        assert_eq!(shared.lock().keys, 0);
    }

    #[test]
    fn two_hour_ingestion_plateaus_and_expires_idle_keys() {
        let h = history(LimitOverrides {
            feed_bytes: Some(128_000.0),
            process_bytes: Some(128_000.0),
            keys: None,
        });
        let mut samples = Vec::new();
        let mut now = 0.0;
        while now <= 7_200_000.0 {
            let rows: Vec<Vehicle> = (0..12).map(|i| row(now, &format!("v{i}"))).collect();
            h.ingest(&FEED, &rows, now);
            if now >= 900_000.0 {
                samples.push(h.diagnostics().0);
            }
            now += 15_000.0;
        }
        let max = *samples.iter().max().unwrap();
        assert!(max <= 128_000);
        assert_eq!(max, *samples.iter().min().unwrap());
        h.prune(8_100_001.0);
        assert_eq!(h.diagnostics().2, 0);
    }

    #[test]
    fn retention_is_opt_in_and_identifiers_are_bounded() {
        let h = history(LimitOverrides::default());
        h.ingest(&feed_with("mbta", false), &[row(1000.0, "bus")], 1000.0);
        assert_eq!(h.diagnostics().2, 0);
        for id in [
            "",
            ".",
            "..",
            "abc/def",
            "abc\\def",
            "abc\0def",
            &"é".repeat(129),
        ] {
            assert!(!valid_transit_identifier(id), "{id:?}");
            h.ingest(&FEED, &[row(1000.0, id)], 1000.0);
        }
        assert_eq!(h.diagnostics().2, 0);
        assert!(valid_transit_identifier("🚋 vehicle 1"));
    }

    #[tokio::test]
    async fn owned_sweep_and_clear_closes_the_store() {
        let h = TransitHistory::with(LimitOverrides::default(), storage(), Some(Arc::new(|| 0.0)));
        assert!(h.diagnostics().3);
        h.clear();
        h.ingest(&FEED, &[row(1000.0, "bus")], 1000.0);
        assert!(!h.diagnostics().3);
        assert_eq!(h.diagnostics().2, 0);
    }

    #[test]
    fn fifteen_thousand_vehicle_snapshots_retain_paths() {
        let h = history(LimitOverrides::default());
        let mut times = Vec::new();
        for poll in 0..3 {
            let now = 100_000.0 + f64::from(poll) * 15_000.0;
            let rows: Vec<Vehicle> = (0..15_000)
                .map(|i| row(now, &format!("vehicle-{i}")))
                .collect();
            let start = std::time::Instant::now();
            h.ingest(&FEED, &rows, now);
            times.push(start.elapsed());
        }
        assert_eq!(h.diagnostics().2, 15_000);
        for i in 0..15_000 {
            let result = h.get("mbta", &format!("vehicle-{i}"), 130_000.0);
            assert_eq!(fixes(&result).len(), 3);
            assert_eq!(result["truncated"], false);
        }
        assert!(h.diagnostics().0 < 16 * 1024 * 1024);
        assert!(times.iter().max().unwrap().as_millis() < 2000, "{times:?}");
    }

    #[test]
    fn capacity_pressure_preserves_unvisited_members_and_discloses_recreation() {
        let h = history(LimitOverrides {
            keys: Some(2.0),
            ..Default::default()
        });
        h.ingest(&FEED, &[row(1000.0, "a"), row(1000.0, "b")], 1000.0);
        for now in [2000.0, 3000.0] {
            h.ingest(&FEED, &[row(now, "c"), row(now, "a"), row(now, "b")], now);
        }
        assert_eq!(fixes(&h.get("mbta", "a", 3000.0)).len(), 3);
        assert_eq!(fixes(&h.get("mbta", "b", 3000.0)).len(), 3);
        assert_eq!(h.get("mbta", "c", 3000.0)["truncated"], true);
        h.ingest(&FEED, &[row(4000.0, "c")], 4000.0);
        h.ingest(&FEED, &[row(5000.0, "a")], 5000.0);
        let recreated = h.get("mbta", "a", 5000.0);
        assert_eq!(fixes(&recreated).len(), 1);
        assert_eq!(recreated["truncated"], true);
    }
}
