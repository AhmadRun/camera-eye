//! Port of `server/providers/cctv/catalog.js`: the merged, capped, cached
//! camera catalog with a single-flight refresh that serves stale on outage.

use std::path::PathBuf;
use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::cap::{SourcePack, allocate_source_cap, resolve_catalog_cap};
use super::constants::{CCTV_SOURCE_CACHE_MS, DEFAULT_CCTV_SOURCE_FILE};
use super::fetch::Fetcher;
use super::ground_heights::{GroundHeightsCache, join_ground_heights};
use super::jsval;
use super::normalize::{CctvSource, LoadedPack, Region, camera_region, normalize_source_item};
use super::sources::{self, EnvFn, LoaderDeps};
use crate::common::coalesce::Coalescer;

type Loader = for<'a> fn(&'a LoaderDeps) -> BoxFuture<'a, LoadedPack>;

/// Live open-data packs, in merge order: name, kill-switch variable, loader.
const LIVE_PACKS: [(&str, Option<&str>, Loader); 13] = [
    ("austin", None, |d| sources::load_austin(d).boxed()),
    ("caltrans", None, |d| sources::load_caltrans(d).boxed()),
    ("tfl", Some("CCTV_TFL_ENABLED"), |d| {
        sources::load_tfl(d).boxed()
    }),
    ("ontario", Some("CCTV_ONTARIO_ENABLED"), |d| {
        sources::load_ontario(d).boxed()
    }),
    ("fintraffic", Some("CCTV_FINTRAFFIC_ENABLED"), |d| {
        sources::load_fintraffic(d).boxed()
    }),
    ("drivebc", Some("CCTV_DRIVEBC_ENABLED"), |d| {
        sources::load_drivebc(d).boxed()
    }),
    ("txdot", Some("CCTV_TXDOT_ENABLED"), |d| {
        sources::load_txdot(d).boxed()
    }),
    ("tallinn", Some("CCTV_TALLINN_ENABLED"), |d| {
        async move { sources::load_tallinn(d) }.boxed()
    }),
    ("tarktee", Some("CCTV_TARKTEE_ENABLED"), |d| {
        sources::load_tarktee(d).boxed()
    }),
    ("warendorf", Some("CCTV_WARENDORF_ENABLED"), |d| {
        async move { sources::load_warendorf(d) }.boxed()
    }),
    ("nsw", Some("CCTV_NSW_ENABLED"), |d| {
        sources::load_nsw(d).boxed()
    }),
    ("calgary", Some("CCTV_CALGARY_ENABLED"), |d| {
        sources::load_calgary(d).boxed()
    }),
    ("deldot", Some("CCTV_DELDOT_ENABLED"), |d| {
        sources::load_deldot(d).boxed()
    }),
];

#[derive(Default)]
struct Cached {
    sources: Arc<Vec<CctvSource>>,
    at_ms: i64,
    trimmed_packs: Arc<Vec<Value>>,
}

/// `createCctvCatalog`.
pub struct Catalog {
    deps: LoaderDeps,
    heights: GroundHeightsCache,
    cache: Mutex<Cached>,
    inflight: Coalescer<(), Arc<Vec<CctvSource>>>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl Catalog {
    pub fn new(env: EnvFn, fetch: Fetcher, root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            deps: LoaderDeps { env, fetch, root },
            heights: GroundHeightsCache::default(),
            cache: Mutex::new(Cached::default()),
            inflight: Coalescer::new(),
            now: Arc::new(crate::common::js::now_ms),
        })
    }

    #[cfg(test)]
    pub fn with_clock(mut self: Arc<Self>, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        Arc::get_mut(&mut self).expect("unshared catalog").now = now;
        self
    }

    fn env_enabled(&self, name: &str) -> bool {
        let raw = self
            .deps
            .env(name)
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "1".into());
        jsval::trim(&raw) != "0"
    }

    /// Packs serving fewer cameras than they offer, as of the last refresh.
    pub fn trimmed_packs(&self) -> Arc<Vec<Value>> {
        Arc::clone(&self.cache.lock().trimmed_packs)
    }

    /// `getCctvSources`: cached for the TTL; one refresh at a time.
    pub async fn sources(self: &Arc<Self>) -> Arc<Vec<CctvSource>> {
        {
            let cache = self.cache.lock();
            if !cache.sources.is_empty() && (self.now)() - cache.at_ms <= CCTV_SOURCE_CACHE_MS {
                return Arc::clone(&cache.sources);
            }
        }
        let this = Arc::clone(self);
        // Spawned so a dropped request never cancels the shared refresh.
        let (value, _) = self
            .inflight
            .run((), move || async move {
                tokio::spawn(async move { this.refresh().await })
                    .await
                    .unwrap_or_default()
            })
            .await;
        value
    }

    fn load_file(&self) -> Vec<Value> {
        let file = self
            .deps
            .env_or("CCTV_SOURCES_FILE", DEFAULT_CCTV_SOURCE_FILE);
        let resolved = self.deps.root.join(&file);
        if !resolved.exists() {
            return Vec::new();
        }
        match std::fs::read(&resolved)
            .ok()
            .and_then(|bytes| jsval::parse_json(&bytes))
        {
            Some(Value::Array(rows)) => rows,
            Some(_) => Vec::new(),
            None => {
                tracing::warn!("[CCTV] failed to read source file: {}", resolved.display());
                Vec::new()
            }
        }
    }

    fn load_env(&self) -> Vec<Value> {
        let Some(raw) = self.deps.env("CCTV_SOURCES_JSON").filter(|v| !v.is_empty()) else {
            return Vec::new();
        };
        match serde_json::from_str::<Value>(&raw) {
            Ok(Value::Array(rows)) => rows,
            _ => Vec::new(),
        }
    }

    async fn refresh(&self) -> Arc<Vec<CctvSource>> {
        let from_file = self.load_file();
        let from_env = self.load_env();
        let force = jsval::trim(&self.deps.env("CCTV_FORCE_AUSTIN").unwrap_or_default()) == "1";
        let prefer = {
            let raw = self
                .deps
                .env("CCTV_PREFER_AUSTIN")
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "1".into());
            jsval::trim(&raw) != "0"
        };
        let needs_live = force || (from_file.len() + from_env.len() == 0 && prefer);
        let live: Vec<LoadedPack> = if needs_live {
            futures_util::future::join_all(LIVE_PACKS.iter().map(|(_, switch, load)| {
                let enabled = switch.is_none_or(|name| self.env_enabled(name));
                let deps = &self.deps;
                async move {
                    if enabled {
                        load(deps).await
                    } else {
                        LoadedPack::default()
                    }
                }
            }))
            .await
        } else {
            vec![LoadedPack::default(); LIVE_PACKS.len()]
        };

        struct Normalized {
            pack: SourcePack,
            available: usize,
            region: Option<Region>,
        }
        let normalize = |name: &str, loaded: LoadedPack| {
            let total = loaded.items.len();
            let sources: Vec<CctvSource> = loaded
                .items
                .iter()
                .filter(|item| item.is_object() || item.is_array())
                .map(|item| {
                    let mut source = normalize_source_item(item);
                    source.pack = name.to_string();
                    source
                })
                .filter(|source| !source.id.is_empty())
                .collect();
            Normalized {
                pack: SourcePack {
                    name: name.to_string(),
                    sources,
                },
                available: loaded.available.unwrap_or(total),
                region: loaded.region,
            }
        };
        let mut packs: Vec<Normalized> = LIVE_PACKS
            .iter()
            .zip(live)
            .map(|((name, _, _), loaded)| normalize(name, loaded))
            .collect();
        packs.push(normalize("file", LoadedPack::plain(from_file)));
        packs.push(normalize("env", LoadedPack::plain(from_env)));

        let max_count = resolve_catalog_cap(self.deps.env("CCTV_MAX_SOURCES").as_deref());
        let source_packs: Vec<SourcePack> = packs.iter().map(|p| p.pack.clone()).collect();
        let (mut capped, allocation) = allocate_source_cap(&source_packs, max_count);
        let heights = self.heights.load(
            &self.deps.root,
            self.deps.env("CCTV_GROUND_HEIGHTS_FILE").as_deref(),
        );
        join_ground_heights(&mut capped, &heights);

        let trimmed_packs: Vec<Value> = packs
            .iter()
            .zip(&allocation)
            .filter_map(|(pack, alloc)| {
                let available = pack.available.max(alloc.offered);
                if alloc.kept >= available {
                    return None;
                }
                let region = pack
                    .region
                    .or_else(|| camera_region(pack.pack.sources.iter().map(|s| (s.lat, s.lon))));
                Some(json!({
                    "pack": pack.pack.name,
                    "available": available,
                    "served": alloc.kept,
                    "region": region.map(Region::to_json),
                }))
            })
            .collect();
        let trimmed: Vec<String> = allocation
            .iter()
            .filter(|a| a.kept < a.offered)
            .map(|a| format!("{} {}/{}", a.name, a.kept, a.offered))
            .collect();
        if !trimmed.is_empty() {
            tracing::warn!(
                "[CCTV] source catalog exceeds cap {max_count}; shared round-robin across packs ({}). Raise CCTV_MAX_SOURCES or lower a per-pack cap to change the mix.",
                trimmed.join(", ")
            );
        }

        let mut cache = self.cache.lock();
        if !capped.is_empty() || cache.sources.is_empty() {
            cache.sources = Arc::new(capped);
            cache.trimmed_packs = Arc::new(trimmed_packs);
        } else {
            tracing::warn!(
                "[CCTV] source refresh returned empty; serving {} stale cameras",
                cache.sources.len()
            );
        }
        cache.at_ms = (self.now)();
        Arc::clone(&cache.sources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cctv::fetch::FetchResponse;
    use crate::providers::cctv::sources::tests::{env_map, json_fetch, json_response};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    fn live_only(extra: &[(&'static str, &'static str)]) -> EnvFn {
        let mut pairs = vec![
            ("CCTV_SOURCES_FILE", "config/does-not-exist.json"),
            ("CCTV_TALLINN_ENABLED", "0"),
            ("CCTV_WARENDORF_ENABLED", "0"),
        ];
        pairs.extend_from_slice(extra);
        env_map(&pairs)
    }

    fn root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    fn calgary_body() -> Value {
        json!([{
            "camera_url": { "url": "http://trafficcam.calgary.ca/loc142.jpg" },
            "camera_location": "Bow Trail / 37 Street SW",
            "point": { "type": "Point", "coordinates": [-114.1413616, 51.0453097] }
        }])
    }

    #[tokio::test]
    async fn calgary_lane_feeds_the_catalog_and_its_switch_turns_it_off() {
        let (fetch, _) = json_fetch(|request| {
            if request.url.contains("data.calgary.ca") {
                json_response(calgary_body())
            } else {
                FetchResponse::new(503, &[], "")
            }
        });
        let catalog = Catalog::new(live_only(&[]), fetch.clone(), root());
        let sources = catalog.sources().await;
        let ids: Vec<&str> = sources.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["calgary-142"]);
        assert_eq!(sources[0].pack, "calgary");

        let off = Catalog::new(live_only(&[("CCTV_CALGARY_ENABLED", "0")]), fetch, root());
        assert!(off.sources().await.is_empty());
    }

    #[tokio::test]
    async fn drivebc_switch_and_the_shared_cap_report_trimmed_packs() {
        let rows: Vec<Value> = (0..30)
            .map(|i| {
                json!({
                    "id": 100 + i, "name": format!("Cam {i}"), "is_on": true, "should_appear": true,
                    "orientation": "N", "elevation": 10,
                    "location": { "type": "Point", "coordinates": [-123.1 + i as f64 * 0.001, 49.25] }
                })
            })
            .collect();
        let body = Value::Array(rows);
        let (fetch, _) = json_fetch(move |request| {
            if request.url.contains("drivebc") {
                json_response(body.clone())
            } else {
                FetchResponse::new(503, &[], "")
            }
        });
        let catalog = Catalog::new(
            live_only(&[
                ("CCTV_MAX_SOURCES", "8"),
                ("CCTV_DRIVEBC_MAX_SOURCES", "20"),
            ]),
            fetch.clone(),
            root(),
        );
        assert_eq!(catalog.sources().await.len(), 8);
        let trimmed = catalog.trimmed_packs();
        assert_eq!(trimmed.len(), 1);
        assert_eq!(trimmed[0]["pack"], "drivebc");
        assert_eq!(trimmed[0]["available"], 30);
        assert_eq!(trimmed[0]["served"], 8);
        assert!(trimmed[0]["region"]["west"].is_number());

        let off = Catalog::new(live_only(&[("CCTV_DRIVEBC_ENABLED", "0")]), fetch, root());
        assert!(off.sources().await.is_empty());
    }

    #[tokio::test]
    async fn refresh_is_single_flight_and_serves_stale_on_outage() {
        let calls = Arc::new(AtomicUsize::new(0));
        let up = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (counter, healthy) = (Arc::clone(&calls), Arc::clone(&up));
        let (fetch, _) = json_fetch(move |request| {
            if request.url.contains("data.calgary.ca") {
                counter.fetch_add(1, Ordering::SeqCst);
                if healthy.load(Ordering::SeqCst) {
                    return json_response(calgary_body());
                }
            }
            FetchResponse::new(503, &[], "")
        });
        let clock = Arc::new(AtomicI64::new(1_000_000));
        let tick = Arc::clone(&clock);
        let catalog = Catalog::new(live_only(&[]), fetch, root())
            .with_clock(Arc::new(move || tick.load(Ordering::SeqCst)));
        let (a, b) = tokio::join!(catalog.sources(), catalog.sources());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        catalog.sources().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "cached within the TTL");

        up.store(false, Ordering::SeqCst);
        clock.fetch_add(CCTV_SOURCE_CACHE_MS + 1, Ordering::SeqCst);
        assert_eq!(catalog.sources().await.len(), 1, "stale catalog served");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_configured_file_pack_suppresses_live_packs_unless_forced() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let (fetch, _) = json_fetch(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            FetchResponse::new(503, &[], "")
        });
        let env = env_map(&[(
            "CCTV_SOURCES_JSON",
            r#"[{"id":"env-1","name":"Env cam","lat":30.1,"lon":-97.7,"url":"https://example.com/a.jpg"}, 5, null]"#,
        )]);
        let catalog = Catalog::new(env, fetch, root());
        let sources = catalog.sources().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // The shipped Austin file pack plus the env pack.
        assert!(sources.iter().any(|s| s.id == "env-1" && s.pack == "env"));
    }
}
