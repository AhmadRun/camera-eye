//! Port of `createGeocodePlaceService` from `src/tools/places.js`: place
//! names to bounds through `/api/geocode`, with a small cache of recent
//! answers. A pending lookup is shared by its callers, has its own
//! deadline, is cancelled when every caller has given up, and only a
//! successful answer stays cached.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::{AbortHandle, BoxFuture, Shared, abortable};
use parking_lot::Mutex;
use serde_json::Value;

use super::fetch::{Api, timeout_failure};
use crate::mcp::area::{LatLon, Place};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

const POINT_RADIUS_DEGREES: f64 = 0.25;
const CACHE_SIZE: usize = 200;
const CACHE_MS: f64 = 10.0 * 60_000.0;
const TIMEOUT: Duration = Duration::from_secs(15);

type Lookup = Shared<BoxFuture<'static, Result<Option<Place>, Failure>>>;

struct Entry {
    id: u64,
    at: f64,
    place: Lookup,
    waiters: usize,
    settled: Arc<AtomicBool>,
    abort: AbortHandle,
}

/// `places.resolve(name)`.
pub struct GeocodePlaces {
    api: Api,
    cache: Arc<Mutex<Vec<(String, Entry)>>>,
    next_id: AtomicU64,
}

/// Decrements an entry's waiters when a caller gives up.
struct Waiter {
    cache: Arc<Mutex<Vec<(String, Entry)>>>,
    key: String,
    id: u64,
    done: bool,
}

impl Drop for Waiter {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut cache = self.cache.lock();
        if let Some(index) = cache
            .iter()
            .position(|(key, e)| *key == self.key && e.id == self.id)
        {
            let entry = &mut cache[index].1;
            entry.waiters = entry.waiters.saturating_sub(1);
            if entry.waiters == 0 && !entry.settled.load(Ordering::SeqCst) {
                entry.abort.abort();
                cache.remove(index);
            }
        }
    }
}

impl GeocodePlaces {
    pub fn new(api: Api) -> Self {
        Self {
            api,
            cache: Arc::new(Mutex::new(Vec::new())),
            next_id: AtomicU64::new(1),
        }
    }

    async fn lookup(api: Api, name: String) -> Result<Option<Place>, Failure> {
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("q", &name)
            .finish();
        let work = async {
            let response = api.get(&format!("/api/geocode?{query}")).await?;
            if !response.ok() {
                return Err(Failure::other(format!("Geocode HTTP {}", response.status)));
            }
            let payload = response.json().await?;
            Ok(place_from_geocode_result(
                payload.get("results").and_then(|r| r.get(0)),
            ))
        };
        tokio::time::timeout(TIMEOUT, work)
            .await
            .unwrap_or_else(|_| Err(timeout_failure()))
    }

    /// Resolve a place name to bounds, or None when nothing matched.
    pub async fn resolve(&self, name: &str) -> Result<Option<Place>, Failure> {
        let key = js::trim(name).to_lowercase();
        let (place, id) = {
            let mut cache = self.cache.lock();
            let now = js::now();
            let fresh = cache
                .iter()
                .position(|(k, e)| *k == key && now - e.at < CACHE_MS);
            let index = match fresh {
                Some(index) => {
                    let item = cache.remove(index);
                    cache.push(item);
                    cache.len() - 1
                }
                None => {
                    let (task, abort) = abortable(Self::lookup(self.api.clone(), name.to_string()));
                    let settled = Arc::new(AtomicBool::new(false));
                    let flag = settled.clone();
                    let place = async move {
                        let result = task
                            .await
                            .unwrap_or_else(|_| Err(Failure::other("This operation was aborted")));
                        flag.store(true, Ordering::SeqCst);
                        result
                    }
                    .boxed()
                    .shared();
                    let id = self.next_id.fetch_add(1, Ordering::SeqCst);
                    cache.retain(|(k, _)| *k != key);
                    cache.push((
                        key.clone(),
                        Entry {
                            id,
                            at: now,
                            place,
                            waiters: 0,
                            settled,
                            abort,
                        },
                    ));
                    while cache.len() > CACHE_SIZE {
                        cache.remove(0);
                    }
                    cache.len() - 1
                }
            };
            let entry = &mut cache[index].1;
            entry.waiters += 1;
            (entry.place.clone(), entry.id)
        };
        let mut waiter = Waiter {
            cache: self.cache.clone(),
            key: key.clone(),
            id,
            done: false,
        };
        let result = place.await;
        waiter.done = true;
        if result.is_err() {
            let mut cache = self.cache.lock();
            cache.retain(|(k, e)| !(*k == key && e.id == id));
        }
        result
    }
}

/// `placeFromGeocodeResult`: one geocode result as a bounded place.
pub fn place_from_geocode_result(result: Option<&Value>) -> Option<Place> {
    let result = result?;
    let location = result.get("geometry").and_then(|g| g.get("location"));
    let lat = location.and_then(|l| l.get("lat")).and_then(js::finite)?;
    let lng = location.and_then(|l| l.get("lng")).and_then(js::finite)?;
    let viewport = result["geometry"].get("viewport").filter(|v| js::truthy(v));
    let corner = |name: &str, key: &str| {
        viewport
            .and_then(|v| v.get(name))
            .and_then(|c| c.get(key))
            .and_then(js::finite)
    };
    let (west, south, east, north) = match (
        corner("southwest", "lat"),
        corner("southwest", "lng"),
        corner("northeast", "lat"),
        corner("northeast", "lng"),
    ) {
        (Some(s), Some(w), Some(n), Some(e)) => (w, s, e, n),
        _ => (
            js::math_max([-180.0, lng - POINT_RADIUS_DEGREES]),
            js::math_max([-90.0, lat - POINT_RADIUS_DEGREES]),
            js::math_min([180.0, lng + POINT_RADIUS_DEGREES]),
            js::math_min([90.0, lat + POINT_RADIUS_DEGREES]),
        ),
    };
    let name = result
        .get("formatted_address")
        .filter(|v| js::truthy(v))
        .map(js::to_js_string);
    Some(Place {
        name,
        west,
        south,
        east,
        north,
        point: Some(LatLon::new(lat, lng)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn geocode_results_become_bounded_places() {
        let oslo = place_from_geocode_result(Some(&json!({
            "formatted_address": "Oslo, Norway",
            "geometry": {
                "location": {"lat": 59.9, "lng": 10.7},
                "viewport": {"southwest": {"lat": 59.8, "lng": 10.5}, "northeast": {"lat": 60, "lng": 10.9}}
            }
        })))
        .unwrap();
        assert_eq!(oslo.name.as_deref(), Some("Oslo, Norway"));
        assert_eq!(
            (oslo.west, oslo.south, oslo.east, oslo.north),
            (10.5, 59.8, 10.9, 60.0)
        );
        assert_eq!(oslo.point, Some(LatLon::new(59.9, 10.7)));
        let point = place_from_geocode_result(Some(
            &json!({"geometry": {"location": {"lat": 0, "lng": 179.9}}}),
        ))
        .unwrap();
        assert_eq!(
            (point.west, point.south, point.east, point.north),
            (179.65, -0.25, 180.0, 0.25)
        );
        assert_eq!(
            place_from_geocode_result(Some(&json!({"geometry": {}}))),
            None
        );
    }
}
