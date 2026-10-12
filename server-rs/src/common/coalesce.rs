//! Share one in-flight upstream request between concurrent callers
//! (port of `coalesceProxyRequest`).

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use futures_util::future::{BoxFuture, FutureExt, Shared};
use parking_lot::Mutex;

pub struct Coalescer<K, V: Clone> {
    in_flight: Arc<Mutex<HashMap<K, (u64, Shared<BoxFuture<'static, V>>)>>>,
    next_id: Mutex<u64>,
}

impl<K, V: Clone> Default for Coalescer<K, V> {
    fn default() -> Self {
        Self {
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            next_id: Mutex::new(0),
        }
    }
}

impl<K, V> Coalescer<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Await the in-flight request for `key`, or start one with `create`.
    /// Returns the value and whether it was shared with an earlier caller.
    pub async fn run<F, Fut>(&self, key: K, create: F) -> (V, bool)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V> + Send + 'static,
    {
        let (future, shared) = {
            let mut map = self.in_flight.lock();
            if let Some((_, existing)) = map.get(&key) {
                (existing.clone(), true)
            } else {
                let id = {
                    let mut next = self.next_id.lock();
                    *next += 1;
                    *next
                };
                let map_ref = Arc::clone(&self.in_flight);
                let cleanup_key = key.clone();
                let inner = create();
                let future = async move {
                    let value = inner.await;
                    let mut map = map_ref.lock();
                    if map.get(&cleanup_key).is_some_and(|(entry, _)| *entry == id) {
                        map.remove(&cleanup_key);
                    }
                    value
                }
                .boxed()
                .shared();
                map.insert(key, (id, future.clone()));
                (future, false)
            }
        };
        (future.await, shared)
    }
}
