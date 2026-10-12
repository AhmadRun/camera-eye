//! Port of `server/providers/radio/catalog.js`: the Radio Browser directory
//! (mirror discovery, the health-policed catalog and its generations).

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::constants::*;
use super::stations::{
    RadioStation, normalize_radio_browser_station, normalize_tag, public_radio_station,
};
use super::transport::{
    Lookup, PinnedFetch, radio_mirror_origin, radio_proxy_destination,
    resolve_radio_proxy_addresses,
};
use crate::common::coalesce::Coalescer;
use crate::providers::cctv::fetch::{FetchRequest, ReadError, Redirect};
use crate::providers::cctv::jsval;

/// The station-tag queries, broad popularity first.
const QUERIES: [Option<&str>; 9] = [
    None,
    Some("news"),
    Some("talk"),
    Some("weather"),
    Some("emergency"),
    Some("scanner"),
    Some("aviation"),
    Some("marine"),
    Some("traffic"),
];

/// One built catalog.
#[derive(Clone)]
pub struct Catalog {
    pub cached_at: i64,
    pub updated_at: String,
    pub stations: Arc<Vec<Value>>,
    pub station_ids: Arc<HashSet<String>>,
    pub degraded: bool,
    pub degraded_reason: Option<String>,
    pub coverage: Value,
    pub accepted_generation: Option<u64>,
}

/// A refresh that could not be served (`error.radioCatalogDegraded`).
#[derive(Clone, Debug)]
pub struct RefreshError {
    pub degraded_reason: Option<String>,
    pub coverage: Option<Value>,
}

/// A catalog as `/stations` serves it.
pub struct Served {
    pub catalog: Catalog,
    pub stale: bool,
}

struct Mirrors {
    origins: Vec<String>,
    cached_at: i64,
}

struct State {
    mirrors: Mirrors,
    cache: Option<Catalog>,
    generation: u64,
    served_ids: Arc<HashSet<String>>,
}

/// `createRadioProxyMiddleware`'s closure state.
pub struct Directory {
    fetch: PinnedFetch,
    lookup: Lookup,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub instance: String,
    state: Mutex<State>,
    mirror_flight: Coalescer<(), Vec<String>>,
    refresh_flight: Coalescer<(), Result<Catalog, RefreshError>>,
}

impl Directory {
    pub fn new(
        fetch: PinnedFetch,
        lookup: Lookup,
        now: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            fetch,
            lookup,
            now,
            instance: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(State {
                mirrors: Mirrors {
                    origins: RADIO_FALLBACK_MIRRORS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    cached_at: 0,
                },
                cache: None,
                generation: 0,
                served_ids: Arc::new(HashSet::new()),
            }),
            mirror_flight: Coalescer::new(),
            refresh_flight: Coalescer::new(),
        })
    }

    /// Whether `/click/<id>` may count this station.
    pub fn is_served(&self, id: &str) -> bool {
        self.state.lock().served_ids.contains(id)
    }

    /// `fetchJson`: an allowlisted destination, resolved to public
    /// addresses, fetched pinned without redirects, capped and parsed, all
    /// within the request deadline.
    async fn fetch_json(&self, url: &str, max_bytes: usize) -> Result<Value, String> {
        let destination =
            radio_proxy_destination(url).ok_or("Radio Browser destination is not permitted")?;
        let work = async {
            let host = destination.host_str().unwrap_or_default().to_string();
            let addresses = resolve_radio_proxy_addresses(&host, &self.lookup).await?;
            let request = FetchRequest::new(destination.as_str(), Redirect::Manual, false)
                .header("Accept", "application/json")
                .header("User-Agent", RADIO_USER_AGENT);
            let response = (self.fetch)(request, addresses).await?;
            if (300..400).contains(&response.status) {
                return Err("Radio Browser redirects are refused".to_string());
            }
            if !response.ok() {
                return Err(format!("Radio Browser returned {}", response.status));
            }
            let bytes = response
                .bytes_capped(max_bytes)
                .await
                .map_err(|e| match e {
                    ReadError::TooLarge => "Upstream response too large".to_string(),
                    ReadError::Failed => "Radio Browser body failed".to_string(),
                })?;
            jsval::parse_json(&bytes)
                .ok_or_else(|| "Radio Browser returned invalid JSON".to_string())
        };
        tokio::time::timeout(Duration::from_millis(RADIO_FETCH_TIMEOUT_MS), work)
            .await
            .map_err(|_| "This operation was aborted".to_string())?
    }

    /// `mirrors`: discovered mirrors first, then the fallbacks; cached.
    async fn mirrors(self: &Arc<Self>) -> Vec<String> {
        {
            let state = self.state.lock();
            if (self.now)() - state.mirrors.cached_at < RADIO_MIRROR_CACHE_MS {
                return state.mirrors.origins.clone();
            }
        }
        let this = Arc::clone(self);
        let (origins, _) = self
            .mirror_flight
            .run((), move || async move {
                let fetched = this
                    .fetch_json(
                        "https://all.api.radio-browser.info/json/servers",
                        RADIO_DISCOVERY_MAX_BYTES,
                    )
                    .await;
                let mut state = this.state.lock();
                match fetched {
                    Ok(rows) => {
                        let mut discovered: Vec<String> = Vec::new();
                        if let Value::Array(rows) = rows {
                            for row in &rows {
                                if let Some(origin) =
                                    radio_mirror_origin(jsval::get(Some(row), "name"))
                                    && !discovered.contains(&origin)
                                {
                                    discovered.push(origin);
                                }
                            }
                        }
                        if !discovered.is_empty() {
                            let mut origins = discovered.clone();
                            origins.extend(
                                RADIO_FALLBACK_MIRRORS
                                    .iter()
                                    .filter(|o| !discovered.iter().any(|d| d == *o))
                                    .map(|o| o.to_string()),
                            );
                            state.mirrors = Mirrors {
                                origins,
                                cached_at: (this.now)(),
                            };
                        }
                    }
                    Err(_) => state.mirrors.cached_at = (this.now)(),
                }
                state.mirrors.origins.clone()
            })
            .await;
        origins
    }

    /// `fetchPath`: the first mirror that answers.
    pub async fn fetch_path(self: &Arc<Self>, path: &str) -> Result<Value, String> {
        let mut last_error = None;
        for origin in self.mirrors().await {
            match self
                .fetch_json(&format!("{origin}{path}"), RADIO_RESPONSE_MAX_BYTES)
                .await
            {
                Ok(value) => return Ok(value),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| "No Radio Browser mirror is available".into()))
    }

    /// One tag query: `(succeeded, stations)`.
    async fn query(self: &Arc<Self>, tag: Option<&str>, index: usize) -> (bool, Vec<RadioStation>) {
        let query = {
            let mut params = url::form_urlencoded::Serializer::new(String::new());
            params
                .append_pair("has_geo_info", "true")
                .append_pair("is_https", "true")
                .append_pair("hidebroken", "true")
                .append_pair("order", "clickcount")
                .append_pair("reverse", "true")
                .append_pair("limit", if index == 0 { "1800" } else { "220" });
            if let Some(tag) = tag {
                params.append_pair("tag", tag);
            }
            params.finish()
        };
        let Ok(Value::Array(rows)) = self
            .fetch_path(&format!("/json/stations/search?{query}"))
            .await
        else {
            return (false, Vec::new());
        };
        let well_formed = rows.iter().all(|row| {
            let Value::Object(map) = row else {
                return false;
            };
            matches!(map.get("stationuuid"), Some(Value::String(_)))
                && matches!(map.get("name"), Some(Value::String(_)))
                && (matches!(map.get("url_resolved"), Some(Value::String(_)))
                    || matches!(map.get("url"), Some(Value::String(_))))
        });
        if !well_formed {
            return (false, Vec::new());
        }
        let stations: Vec<RadioStation> = rows
            .iter()
            .filter_map(normalize_radio_browser_station)
            .collect();
        let requested = normalize_tag(tag.unwrap_or(""));
        let covered = requested.is_empty()
            || stations.iter().any(|s| {
                s.tags
                    .iter()
                    .any(|t| *t == requested || t.contains(&requested))
            });
        (!stations.is_empty() && covered, stations)
    }

    async fn refresh(self: Arc<Self>) -> Result<Catalog, RefreshError> {
        // `mapRadioConcurrent(queries, 3, ...)`: three in flight, results in order.
        let jobs: Vec<futures_util::future::BoxFuture<'static, (bool, Vec<RadioStation>)>> =
            QUERIES
                .into_iter()
                .enumerate()
                .map(|(index, tag)| {
                    let this = Arc::clone(&self);
                    async move { this.query(tag, index).await }.boxed()
                })
                .collect();
        let outcomes: Vec<(bool, Vec<RadioStation>)> =
            futures_util::stream::iter(jobs).buffered(3).collect().await;

        let mut selected: Vec<RadioStation> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut take = |station: &RadioStation| {
            if seen.contains(&station.id) || selected.len() >= RADIO_DIRECTORY_LIMIT {
                return;
            }
            seen.insert(station.id.clone());
            selected.push(station.clone());
        };
        // Specialist categories are seeded before the popularity fill.
        for (_, rows) in &outcomes[1..] {
            rows.iter().take(45).for_each(&mut take);
        }
        let mut all: Vec<&RadioStation> =
            outcomes.iter().flat_map(|(_, rows)| rows.iter()).collect();
        all.sort_by(|a, b| {
            b.click_count
                .partial_cmp(&a.click_count)
                .unwrap_or(Ordering::Equal)
                .then_with(|| locale_compare(&a.name, &b.name))
        });
        all.into_iter().for_each(&mut take);

        let timestamp = (self.now)();
        let successful = outcomes.iter().filter(|(ok, _)| *ok).count();
        let broad_healthy = outcomes[0].0 && !outcomes[0].1.is_empty();
        let mut reasons = Vec::new();
        if !broad_healthy {
            reasons.push("broad-query-unhealthy");
        }
        if successful < RADIO_CATALOG_MIN_SUCCESSFUL_QUERIES {
            reasons.push("query-coverage-below-policy");
        }
        if selected.len() < RADIO_CATALOG_HEALTHY_MIN_STATIONS {
            reasons.push("station-coverage-below-policy");
        }
        let degraded = !reasons.is_empty();
        let coverage = json!({
            "successfulQueries": successful,
            "totalQueries": QUERIES.len(),
            "stationCount": selected.len(),
            "healthyStationMinimum": RADIO_CATALOG_HEALTHY_MIN_STATIONS,
        });
        let reason = degraded.then(|| reasons.join(","));
        let next = Catalog {
            cached_at: timestamp,
            updated_at: jsval::iso_string(timestamp),
            stations: Arc::new(selected.iter().map(public_radio_station).collect()),
            station_ids: Arc::new(selected.iter().map(|s| s.id.clone()).collect()),
            degraded,
            degraded_reason: reason.clone(),
            coverage: coverage.clone(),
            accepted_generation: None,
        };
        let mut state = self.state.lock();
        if degraded && (state.cache.is_some() || selected.is_empty()) {
            return Err(RefreshError {
                degraded_reason: reason,
                coverage: Some(coverage),
            });
        }
        if degraded {
            state.served_ids = Arc::clone(&next.station_ids);
            return Ok(next);
        }
        state.generation += 1;
        let accepted = Catalog {
            accepted_generation: Some(state.generation),
            ..next
        };
        state.served_ids = Arc::clone(&accepted.station_ids);
        state.cache = Some(accepted.clone());
        Ok(accepted)
    }

    /// `getCatalog`: fresh cache, else one shared refresh; a failed refresh
    /// serves the previous catalog as stale for up to a week.
    pub async fn catalog(self: &Arc<Self>) -> Result<Served, RefreshError> {
        {
            let state = self.state.lock();
            if let Some(cache) = &state.cache
                && (self.now)() - cache.cached_at < RADIO_DIRECTORY_CACHE_MS
            {
                return Ok(Served {
                    catalog: cache.clone(),
                    stale: false,
                });
            }
        }
        let this = Arc::clone(self);
        // Spawned so a viewer who leaves never cancels the shared refresh.
        let (result, _) = self
            .refresh_flight
            .run((), move || async move {
                tokio::spawn(this.refresh()).await.unwrap_or_else(|_| {
                    Err(RefreshError {
                        degraded_reason: None,
                        coverage: None,
                    })
                })
            })
            .await;
        match result {
            Ok(catalog) => Ok(Served {
                catalog,
                stale: false,
            }),
            Err(error) => {
                let state = self.state.lock();
                match &state.cache {
                    Some(cache) if (self.now)() - cache.cached_at <= RADIO_DIRECTORY_STALE_MS => {
                        Ok(Served {
                            catalog: Catalog {
                                degraded: true,
                                degraded_reason: Some(
                                    error
                                        .degraded_reason
                                        .unwrap_or_else(|| "refresh-failed".into()),
                                ),
                                coverage: error.coverage.unwrap_or_else(|| cache.coverage.clone()),
                                ..cache.clone()
                            },
                            stale: true,
                        })
                    }
                    _ => Err(error),
                }
            }
        }
    }
}

/// `a.localeCompare(b)`, approximated: case-insensitive first, then
/// lowercase before uppercase as ICU's tertiary strength orders them.
fn locale_compare(a: &str, b: &str) -> Ordering {
    a.to_lowercase()
        .cmp(&b.to_lowercase())
        .then_with(|| b.cmp(a))
}
