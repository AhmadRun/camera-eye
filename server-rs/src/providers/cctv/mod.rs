//! `/api/cctv/*`: port of `server/providers/cctv.js` (the `cctvProxy` plugin)
//! and its `server/providers/cctv/` modules.
//!
//! Endpoints (paths relative to the mount, like the Connect middleware saw):
//!   GET /sources        every registered camera source
//!   GET /health         per-camera health/status report
//!   GET /stream/:id     stream info (feedType, URLs) for a camera
//!   GET /media/:id      registered live media or image, relayed (HLS pulled)
//!   GET /frame/:id      one frame: upstream, then Street View, then synthetic

mod cap;
mod catalog;
mod constants;
pub(crate) mod fetch;
mod footprint;
mod ground_heights;
pub(crate) mod jsval;
mod media;
mod normalize;
mod range;
mod sources;
mod stream;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use regex::Regex;
use serde_json::{Value, json};

use self::catalog::Catalog;
use self::constants::{CCTV_FRAME_FETCH_TIMEOUT_MS, CCTV_MAX_SOURCES_CEILING, CCTV_USER_AGENT};
use self::fetch::{FetchRequest, Fetcher, Redirect};
use self::jsval::num;
use self::media::{FrameLimits, MediaFetchError};
use self::normalize::{CctvSource, is_video_feed_type, normalize_feed_type};
use self::sources::EnvFn;
use self::stream::{HLS_LIMITS, HlsPuller};
use crate::common::reply;
use crate::state::AppState;

/// One camera's health entry, as `/health` reports it.
#[derive(Clone)]
struct Health {
    status: String,
    source_kind: String,
    label: String,
    message: String,
    updated_at: i64,
}

/// A `setHealth` patch; empty fields keep the previous value.
struct Patch<'a> {
    status: &'a str,
    source_kind: &'a str,
    label: &'a str,
    message: &'a str,
}

/// Timeouts the route uses, shortened in tests.
#[derive(Clone, Copy)]
struct Timeouts {
    media_fetch: Duration,
    media_idle: Duration,
    frame: FrameLimits,
    street_view: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            media_fetch: media::media_fetch_timeout(),
            media_idle: media::media_idle_timeout(),
            frame: FrameLimits::default(),
            street_view: Duration::from_millis(CCTV_FRAME_FETCH_TIMEOUT_MS),
        }
    }
}

/// The plugin's state: catalog, health map and HLS puller.
struct Cctv {
    env: EnvFn,
    fetch: Fetcher,
    catalog: Arc<Catalog>,
    health: Mutex<IndexMap<String, Health>>,
    puller: Arc<HlsPuller>,
    timeouts: Timeouts,
}

/// Mounted routes for `/api/cctv` (state from the repository root).
pub fn router(state: &AppState) -> Router<AppState> {
    let env_state = state.clone();
    let env: EnvFn = Arc::new(move |name| env_state.env.raw(name));
    routes(Cctv::new(
        env,
        fetch::production(),
        state.root.clone(),
        Timeouts::default(),
    ))
}

fn routes<S: Clone + Send + Sync + 'static>(cctv: Arc<Cctv>) -> Router<S> {
    let handler = move |request: Request| {
        let cctv = Arc::clone(&cctv);
        async move { cctv.handle(request).await }
    };
    Router::new()
        .route("/api/cctv", any(handler.clone()))
        .route("/api/cctv/", any(handler.clone()))
        .route("/api/cctv/{*rest}", any(handler))
}

/// What the middleware saw as `req.url`: the mount stripped, query kept.
fn mounted_url(request: &http::request::Parts) -> Option<url::Url> {
    let path = request.uri.path();
    let rest = path.get("/api/cctv".len()..).unwrap_or("");
    let mut stripped = if rest.is_empty() {
        "/".to_string()
    } else {
        rest.to_string()
    };
    if let Some(query) = request.uri.query() {
        stripped.push('?');
        stripped.push_str(query);
    }
    url::Url::parse("http://localhost/")
        .ok()?
        .join(&stripped)
        .ok()
}

fn search_param(url: &url::Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

fn empty(status: u16) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() =
        http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    response
}

fn with_headers(mut response: Response, headers: &[(&str, &str)]) -> Response {
    for (name, value) in headers {
        response = reply::with_header(response, name, value);
    }
    response
}

/// `source?.provider || fallback`.
fn provider_or<'a>(source: Option<&'a CctvSource>, fallback: &'a str) -> &'a str {
    source
        .map(|s| s.provider.as_str())
        .filter(|p| !p.is_empty())
        .unwrap_or(fallback)
}

/// Releases a pending HLS lease when the viewer leaves mid-request.
struct LeaseGuard {
    puller: Arc<HlsPuller>,
    camera_id: String,
    lease_id: String,
    armed: bool,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.armed {
            self.puller.release(&self.camera_id, &self.lease_id);
        }
    }
}

impl Cctv {
    fn new(env: EnvFn, fetch: Fetcher, root: PathBuf, timeouts: Timeouts) -> Arc<Self> {
        Arc::new(Self {
            catalog: Catalog::new(Arc::clone(&env), Arc::clone(&fetch), root),
            puller: HlsPuller::new(Arc::clone(&fetch), HLS_LIMITS),
            env,
            fetch,
            health: Mutex::new(IndexMap::new()),
            timeouts,
        })
    }

    /// `setHealth`: evict the oldest entry at capacity, then merge.
    fn set_health(&self, camera_id: &str, patch: Patch<'_>) {
        let mut health = self.health.lock();
        if !health.contains_key(camera_id) && health.len() >= CCTV_MAX_SOURCES_CEILING as usize {
            health.shift_remove_index(0);
        }
        let prev = health.get(camera_id).cloned();
        let pick = |new: &str, old: Option<&String>, default: &str| {
            if !new.is_empty() {
                new.to_string()
            } else if let Some(old) = old.filter(|o| !o.is_empty()) {
                old.clone()
            } else {
                default.to_string()
            }
        };
        let entry = Health {
            status: pick(patch.status, prev.as_ref().map(|p| &p.status), "unknown"),
            source_kind: pick(
                patch.source_kind,
                prev.as_ref().map(|p| &p.source_kind),
                "unknown",
            ),
            label: pick(patch.label, prev.as_ref().map(|p| &p.label), ""),
            message: pick(patch.message, prev.as_ref().map(|p| &p.message), ""),
            updated_at: crate::common::js::now_ms(),
        };
        health.insert(camera_id.to_string(), entry);
    }

    fn list_health(&self) -> Value {
        let cameras: Vec<Value> = self
            .health
            .lock()
            .iter()
            .map(|(id, h)| {
                json!({
                    "id": id,
                    "status": h.status,
                    "sourceKind": h.source_kind,
                    "label": h.label,
                    "message": h.message,
                    "updatedAt": h.updated_at,
                })
            })
            .collect();
        json!({ "cameras": cameras })
    }

    async fn handle(self: Arc<Self>, request: Request) -> Response {
        match self.dispatch(request).await {
            Ok(response) => response,
            Err(message) => {
                tracing::error!("[CCTV Proxy] {message}");
                reply::json_text(500, r#"{"error":"CCTV proxy error"}"#, None)
            }
        }
    }

    async fn dispatch(&self, request: Request) -> Result<Response, String> {
        // Only the head is read; holding the body across awaits is not needed.
        let (request, _body) = request.into_parts();
        let sources = self.catalog.sources().await;
        let by_id: HashMap<&str, &CctvSource> =
            sources.iter().map(|s| (s.id.as_str(), s)).collect();
        let url = mounted_url(&request).ok_or("Invalid URL")?;
        let path = url.path();

        if path == "/sources" {
            let body = json!({
                "trimmedPacks": *self.catalog.trimmed_packs(),
                "sources": sources.iter().map(source_json).collect::<Vec<_>>(),
            });
            return Ok(reply::json(200, &body, Some("no-store")));
        }
        if path == "/health" {
            return Ok(reply::json(200, &self.list_health(), Some("no-store")));
        }
        if let Some(rest) = path.strip_prefix("/stream/") {
            let camera_id = decode_camera_id(rest)?;
            let payload = stream_payload(by_id.get(camera_id.as_str()).copied(), &camera_id);
            return Ok(reply::json(200, &payload, Some("no-store")));
        }
        if path.starts_with("/media/") {
            return self.media(&request, &url, &by_id).await;
        }
        let Some(rest) = path.strip_prefix("/frame/") else {
            return Ok(reply::json_text(404, r#"{"error":"not found"}"#, None));
        };
        let camera_id = decode_camera_id(rest)?;
        Ok(self
            .frame(&url, &camera_id, by_id.get(camera_id.as_str()).copied())
            .await)
    }

    async fn media(
        &self,
        request: &http::request::Parts,
        url: &url::Url,
        by_id: &HashMap<&str, &CctvSource>,
    ) -> Result<Response, String> {
        static MEDIA: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"^/media/([^/]+)(?:/(seg_([0-9]+)\.ts))?$").expect("regex"));
        static LEASE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)^[a-f0-9-]{36}$").expect("regex"));
        static HTTP: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^https?://").expect("regex"));
        static M3U8: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\.m3u8(?:\?|$)").expect("regex"));

        let Some(caps) = MEDIA.captures(url.path()) else {
            return Ok(empty(404));
        };
        let camera_id = jsval::decode_uri_component(&caps[1]).ok_or("URI malformed")?;
        let segment = caps.get(2).is_some();
        let seq = caps.get(3).and_then(|m| m.as_str().parse::<u64>().ok());
        let source = by_id.get(camera_id.as_str()).copied();
        let media_url = source.map(|s| s.url.clone()).unwrap_or_default();
        let feed_type = normalize_feed_type(
            source
                .map(|s| s.feed_type.as_str())
                .filter(|f| !f.is_empty())
                .unwrap_or("image"),
        );
        let lease_id = search_param(url, "lease");
        let method = &request.method;

        if feed_type == "hls" && !LEASE.is_match(lease_id.as_deref().unwrap_or("")) {
            return Ok(empty(400));
        }
        if method == http::Method::DELETE {
            if let Some(lease) = lease_id.as_deref().filter(|l| !l.is_empty()) {
                self.puller.release(&camera_id, lease);
            }
            return Ok(empty(204));
        }
        if method != http::Method::GET {
            return Ok(empty(405));
        }
        let label = provider_or(source, "Configured source");

        if feed_type == "hls" {
            let lease_id = lease_id.unwrap_or_default();
            if !HTTP.is_match(&media_url) || !M3U8.is_match(&media_url) {
                return Ok(reply::json_text(
                    503,
                    r#"{"error":"This stream requires an unsupported transport; use the frame fallback"}"#,
                    None,
                ));
            }
            if segment {
                let session = search_param(url, "session");
                let body = self
                    .puller
                    .get_segment(&camera_id, session.as_deref(), seq, &lease_id);
                let status = if body.is_some() { 200 } else { 404 };
                return Ok(reply::raw(
                    status,
                    "video/mp2t",
                    body.unwrap_or_default(),
                    Some("no-store"),
                ));
            }
            let mut guard = LeaseGuard {
                puller: Arc::clone(&self.puller),
                camera_id: camera_id.clone(),
                lease_id: lease_id.clone(),
                armed: false,
            };
            let outcome = async {
                let entry = self.puller.ensure(&camera_id, &media_url, &lease_id)?;
                guard.armed = true;
                if !self.puller.wait_ready(&entry).await {
                    return Err("Stream unavailable".to_string());
                }
                let playlist = self
                    .puller
                    .build_playlist(&entry, &camera_id, &lease_id)?
                    .ok_or("Stream unavailable")?;
                Ok((playlist, entry.token.clone()))
            }
            .await;
            guard.armed = false;
            return Ok(match outcome {
                Ok((playlist, token)) => {
                    self.set_health(
                        &camera_id,
                        Patch {
                            status: "ok",
                            source_kind: "live",
                            label,
                            message: "Live HLS connected",
                        },
                    );
                    with_headers(
                        reply::raw(
                            200,
                            "application/vnd.apple.mpegurl",
                            playlist,
                            Some("no-store"),
                        ),
                        &[("X-CCTV-Source", "hls-pull"), ("X-CCTV-Session", &token)],
                    )
                }
                Err(_) => {
                    self.set_health(
                        &camera_id,
                        Patch {
                            status: "degraded",
                            source_kind: "fallback",
                            label,
                            message: "Live HLS unavailable",
                        },
                    );
                    reply::with_header(
                        reply::error(503, "Live stream unavailable"),
                        "Retry-After",
                        "2",
                    )
                }
            });
        }
        if segment {
            return Ok(empty(404));
        }
        if media_url.is_empty() || !HTTP.is_match(&media_url) {
            self.set_health(
                &camera_id,
                Patch {
                    status: "degraded",
                    source_kind: "fallback",
                    label: provider_or(source, "No upstream URL"),
                    message: "No stream URL configured",
                },
            );
            return Ok(reply::error(404, "No media URL configured for this camera"));
        }

        let mut headers = vec![("User-Agent".to_string(), CCTV_USER_AGENT.to_string())];
        let ranges: Vec<String> = request
            .headers
            .get_all(http::header::RANGE)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect();
        let client_range = (!ranges.is_empty()).then(|| ranges.join(", "));
        let range = range::sanitize(client_range.as_deref());
        if !range.is_empty() {
            headers.push(("Range".to_string(), range));
        }
        let video = is_video_feed_type(&feed_type);
        match media::fetch_cctv_media_upstream(
            &self.fetch,
            &media_url,
            headers,
            self.timeouts.media_fetch,
        )
        .await
        {
            Ok(upstream) => {
                let content_type = upstream.header("content-type").unwrap_or_default();
                if !upstream.ok() {
                    let status = upstream.status;
                    drop(upstream);
                    self.set_health(
                        &camera_id,
                        Patch {
                            status: "degraded",
                            source_kind: "upstream",
                            label,
                            message: &format!("Upstream HTTP {status}"),
                        },
                    );
                    return Ok(reply::error(status, &format!("Upstream returned {status}")));
                }
                if video
                    && !(content_type.starts_with("video/") || content_type.contains("mpegurl"))
                {
                    let shown = if content_type.is_empty() {
                        "unknown"
                    } else {
                        content_type.as_str()
                    };
                    self.set_health(
                        &camera_id,
                        Patch {
                            status: "degraded",
                            source_kind: "upstream",
                            label,
                            message: &format!("Unexpected media type {shown}"),
                        },
                    );
                } else {
                    self.set_health(
                        &camera_id,
                        Patch {
                            status: "ok",
                            source_kind: if video { "live" } else { "snapshot" },
                            label,
                            message: if video {
                                "Live stream connected"
                            } else {
                                "Snapshot feed connected"
                            },
                        },
                    );
                }
                let source_header = if video {
                    "live-media"
                } else {
                    "upstream-image"
                };
                Ok(media::proxy_media_response(
                    upstream,
                    source_header,
                    self.timeouts.media_idle,
                ))
            }
            Err(error) => {
                // The text is dropped: it names the camera's upstream host.
                tracing::warn!("[CCTV Proxy] media fetch failed");
                self.set_health(
                    &camera_id,
                    Patch {
                        status: "degraded",
                        source_kind: "upstream",
                        label,
                        message: "Media fetch failed",
                    },
                );
                Ok(match error {
                    MediaFetchError::Timeout => reply::error(504, "Upstream media timeout"),
                    MediaFetchError::Failed => reply::error(502, "Media proxy failed"),
                })
            }
        }
    }

    async fn frame(
        &self,
        url: &url::Url,
        camera_id: &str,
        source: Option<&CctvSource>,
    ) -> Response {
        let param = |name: &str| search_param(url, name).filter(|v| !v.is_empty());
        let label = param("label")
            .or_else(|| source.map(|s| s.name.clone()).filter(|n| !n.is_empty()))
            .unwrap_or_else(|| camera_id.to_string());
        let city = param("city")
            .or_else(|| source.map(|s| s.city.clone()).filter(|c| !c.is_empty()))
            .unwrap_or_default();
        // `Number(param || source?.field)`.
        let number = |name: &str, field: fn(&CctvSource) -> f64| match param(name) {
            Some(text) => crate::common::js::number(&text),
            None => source.map(field).unwrap_or(f64::NAN),
        };
        let lat = number("lat", |s| s.lat);
        let lon = number("lon", |s| s.lon);
        let heading = number("heading", |s| s.heading_deg);
        let fov = number("fov", |s| s.fov_deg);
        let pitch = number("pitch", |s| s.pitch_deg);

        // Only server-registered upstream URLs, never client-supplied ones.
        let candidate = source.map(|s| {
            if !s.snapshot_url.is_empty() {
                s.snapshot_url.clone()
            } else if !is_video_feed_type(&normalize_feed_type(&s.feed_type)) {
                s.url.clone()
            } else {
                String::new()
            }
        });
        let limits = self.timeouts.frame;
        let upstream = if source.is_some_and(|s| s.source_kind == "txdot-its") {
            media::fetch_txdot_snapshot(&self.fetch, candidate.as_deref(), limits).await
        } else {
            media::fetch_cctv_image_from_upstream(&self.fetch, candidate.as_deref(), limits).await
        };
        if let Some(frame) = upstream {
            self.set_health(
                camera_id,
                Patch {
                    status: "ok",
                    source_kind: "snapshot",
                    label: provider_or(source, "Configured source"),
                    message: "Upstream snapshot active",
                },
            );
            return with_headers(
                reply::raw(200, &frame.content_type, frame.body, Some("no-store")),
                &[("X-CCTV-Source", "upstream-image")],
            );
        }

        if let Some((body, content_type)) = self.street_view(lat, lon, heading, fov, pitch).await {
            self.set_health(
                camera_id,
                Patch {
                    status: "degraded",
                    source_kind: "streetview",
                    label: "Google Street View",
                    message: "Fallback Street View frame",
                },
            );
            return with_headers(
                reply::raw(200, &content_type, body, Some("no-store")),
                &[("X-CCTV-Source", "streetview")],
            );
        }

        let has_url = source.is_some_and(|s| !s.url.is_empty());
        let svg = media::build_synthetic_cctv_svg(
            camera_id,
            &label,
            &city,
            if has_url {
                "UPSTREAM UNAVAILABLE"
            } else {
                "NO UPSTREAM CONFIGURED"
            },
        );
        self.set_health(
            camera_id,
            Patch {
                status: "degraded",
                source_kind: "synthetic",
                label: provider_or(source, "Synthetic fallback"),
                message: if has_url {
                    "Upstream unavailable"
                } else {
                    "No source configured"
                },
            },
        );
        with_headers(
            reply::raw(200, "image/svg+xml", svg, Some("no-store")),
            &[("X-CCTV-Source", "synthetic")],
        )
    }

    /// `googleServerApiKey()`: the server-scoped key, else the browser key.
    fn google_server_key(&self) -> String {
        let value = |name: &str| jsval::trim(&(self.env)(name).unwrap_or_default()).to_string();
        let server = value("GOOGLE_MAPS_SERVER_API_KEY");
        if server.is_empty() {
            value("GOOGLE_MAPS_API_KEY")
        } else {
            server
        }
    }

    /// `streetViewFallback`: a Street View still for the camera's pose.
    async fn street_view(
        &self,
        lat: f64,
        lon: f64,
        heading: f64,
        fov: f64,
        pitch: f64,
    ) -> Option<(bytes::Bytes, String)> {
        let key = self.google_server_key();
        if key.is_empty() || !lat.is_finite() || !lon.is_finite() {
            return None;
        }
        let mut sv = url::Url::parse("https://maps.googleapis.com/maps/api/streetview").ok()?;
        let n = jsval::number_string;
        sv.query_pairs_mut()
            .append_pair("size", "960x540")
            .append_pair("location", &format!("{},{}", n(lat), n(lon)))
            .append_pair(
                "heading",
                &n(if heading.is_finite() { heading } else { 0.0 }),
            )
            .append_pair(
                "fov",
                &n(if fov.is_finite() {
                    fov.clamp(20.0, 120.0)
                } else {
                    80.0
                }),
            )
            .append_pair(
                "pitch",
                &n(if pitch.is_finite() {
                    pitch.clamp(-40.0, 20.0)
                } else {
                    0.0
                }),
            )
            .append_pair("source", "outdoor")
            .append_pair("return_error_code", "true")
            .append_pair("key", &key);
        let request = FetchRequest::new(sv.as_str(), Redirect::Follow, true)
            .header("User-Agent", CCTV_USER_AGENT);
        let fetch = Arc::clone(&self.fetch);
        let work = async move {
            let response = fetch(request).await.ok()?;
            let content_type = response.header("content-type").unwrap_or_default();
            if !response.ok() || !content_type.starts_with("image/") {
                return None;
            }
            let body = response.bytes().await.ok()?;
            Some((body, content_type))
        };
        tokio::time::timeout(self.timeouts.street_view, work)
            .await
            .ok()
            .flatten()
    }
}

/// `server.httpServer.on('close', () => puller.shutdown())`: the routes
/// going away stops every HLS session.
impl Drop for Cctv {
    fn drop(&mut self) {
        self.puller.shutdown();
    }
}

/// `decodeURIComponent(rest.trim()) || 'camera'`.
fn decode_camera_id(rest: &str) -> Result<String, String> {
    let decoded = jsval::decode_uri_component(jsval::trim(rest)).ok_or("URI malformed")?;
    Ok(if decoded.is_empty() {
        "camera".into()
    } else {
        decoded
    })
}

/// One `/sources` entry, fields in the order the JS wrote them.
fn source_json(source: &CctvSource) -> Value {
    let mut entry = json!({
        "id": source.id,
        "pack": source.pack,
        "name": source.name,
        "city": source.city,
        "cityId": source.city_id,
        "provider": source.provider,
        "lat": num(source.lat),
        "lon": num(source.lon),
        "headingDeg": num(source.heading_deg),
        "headingConfidence": source.heading_confidence,
        "pitchDeg": num(source.pitch_deg),
        "fovDeg": num(source.fov_deg),
        "rangeM": num(source.range_m),
        "mountHeightM": num(source.mount_height_m),
        "groundElevationM": num(source.ground_elevation_m),
        "feedType": normalize_feed_type(&source.feed_type),
        "sourceKind": source_kind(Some(source)),
    });
    let map = entry.as_object_mut().expect("object");
    if let Some(pose) = &source.pose_source {
        map.insert("poseSource".into(), json!(pose));
    }
    map.insert("license".into(), json!(source.license));
    map.insert("credit".into(), json!(source.credit));
    map.insert("code".into(), json!(source.code));
    map.insert(
        "groundHeights".into(),
        source.ground_heights.clone().unwrap_or(Value::Null),
    );
    entry
}

/// `source?.sourceKind || (source?.url ? 'configured' : 'fallback')`.
fn source_kind(source: Option<&CctvSource>) -> String {
    match source {
        Some(s) if !s.source_kind.is_empty() => s.source_kind.clone(),
        Some(s) if !s.url.is_empty() => "configured".into(),
        _ => "fallback".into(),
    }
}

/// `buildStreamPayload`.
fn stream_payload(source: Option<&CctvSource>, camera_id: &str) -> Value {
    let feed_type = normalize_feed_type(
        source
            .map(|s| s.feed_type.as_str())
            .filter(|f| !f.is_empty())
            .unwrap_or("image"),
    );
    let encoded = jsval::encode_uri_component(camera_id);
    json!({
        "id": camera_id,
        "feedType": feed_type,
        "mediaUrl": if is_video_feed_type(&feed_type) { Value::String(format!("/api/cctv/media/{encoded}")) } else { Value::Null },
        "frameUrl": format!("/api/cctv/frame/{encoded}"),
        "provider": source.map(|s| s.provider.clone()).unwrap_or_default(),
        "sourceKind": source_kind(source),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cctv::fetch::FetchResponse;
    use crate::providers::cctv::sources::tests::{env_map, json_fetch};
    use futures_util::FutureExt;
    use tower::ServiceExt;

    const CAMERAS: &str = r#"[
        {"id":"cam-1","name":"Main St","provider":"City","lat":30.27,"lon":-97.74,"feedType":"mp4","url":"https://media.example.test/cam1.mp4"},
        {"id":"cam-img","name":"Still","provider":"City","lat":30.27,"lon":-97.74,"feedType":"image","url":"https://media.example.test/still.jpg"},
        {"id":"cam-none","name":"Nothing","lat":30.27,"lon":-97.74,"feedType":"mp4"},
        {"id":"cam-hls","name":"Live","lat":30.27,"lon":-97.74,"feedType":"hls","url":"rtsp://media.example.test/live"}
    ]"#;

    fn make_app(fetch: Fetcher, extra: &[(&'static str, &'static str)]) -> (Router, Arc<Cctv>) {
        let mut pairs = vec![
            ("CCTV_SOURCES_JSON", CAMERAS),
            ("CCTV_SOURCES_FILE", "config/does-not-exist.json"),
        ];
        pairs.extend_from_slice(extra);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
        let timeouts = Timeouts {
            media_fetch: Duration::from_millis(200),
            media_idle: Duration::from_millis(200),
            ..Timeouts::default()
        };
        let cctv = Cctv::new(env_map(&pairs), fetch, root, timeouts);
        (routes(Arc::clone(&cctv)), cctv)
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
    ) -> (u16, http::HeaderMap, Vec<u8>) {
        let mut builder = http::Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, headers, body)
    }

    fn json_of(body: &[u8]) -> Value {
        serde_json::from_slice(body).unwrap()
    }

    fn health_of(cctv: &Cctv, id: &str) -> Value {
        cctv.list_health()["cameras"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == id)
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn no_network() -> Fetcher {
        Arc::new(|_| async { Err("offline".to_string()) }.boxed())
    }

    #[tokio::test]
    async fn sources_health_stream_and_unknown_routes() {
        let (app, _) = make_app(no_network(), &[]);
        let (status, headers, body) = send(&app, "GET", "/api/cctv/sources", &[]).await;
        assert_eq!(status, 200);
        assert_eq!(headers["cache-control"], "no-store");
        let body = json_of(&body);
        assert_eq!(body["trimmedPacks"], json!([]));
        let first = &body["sources"][0];
        assert_eq!(first["id"], "cam-1");
        assert_eq!(first["pack"], "env");
        assert_eq!(first["feedType"], "mp4");
        assert_eq!(first["groundHeights"], Value::Null);
        assert!(first.get("poseSource").is_none());
        let keys: Vec<&str> = first
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys[..3], ["id", "pack", "name"]);
        assert_eq!(*keys.last().unwrap(), "groundHeights");

        let (_, _, body) = send(&app, "GET", "/api/cctv/stream/cam-1", &[]).await;
        assert_eq!(
            json_of(&body),
            json!({"id":"cam-1","feedType":"mp4","mediaUrl":"/api/cctv/media/cam-1","frameUrl":"/api/cctv/frame/cam-1","provider":"City","sourceKind":"configured"})
        );
        let (_, _, body) = send(&app, "GET", "/api/cctv/stream/", &[]).await;
        assert_eq!(json_of(&body)["id"], "camera");
        assert_eq!(json_of(&body)["sourceKind"], "fallback");
        assert_eq!(json_of(&body)["mediaUrl"], Value::Null);

        let (status, headers, body) = send(&app, "GET", "/api/cctv/stream/%E0%A4%A", &[]).await;
        assert_eq!(status, 500);
        assert!(headers.get("cache-control").is_none());
        assert_eq!(json_of(&body), json!({"error": "CCTV proxy error"}));

        for path in ["/api/cctv", "/api/cctv/", "/api/cctv/nope"] {
            let (status, headers, body) = send(&app, "GET", path, &[]).await;
            assert_eq!(status, 404, "{path}");
            assert_eq!(headers["content-type"], "application/json");
            assert_eq!(json_of(&body), json!({"error": "not found"}));
        }
        let (status, _, body) = send(&app, "GET", "/api/cctv/health", &[]).await;
        assert_eq!(status, 200);
        assert_eq!(json_of(&body), json!({"cameras": []}));
    }

    #[tokio::test]
    async fn media_forwards_a_sanitized_range_and_passes_206_through() {
        let (fetch, seen) = json_fetch(|_| {
            FetchResponse::new(
                206,
                &[
                    ("content-type", "video/mp4"),
                    ("content-range", "bytes 0-3/100"),
                    ("content-length", "4"),
                    ("accept-ranges", "bytes"),
                ],
                "abcd",
            )
        });
        let (app, cctv) = make_app(fetch, &[]);
        let (status, headers, body) = send(
            &app,
            "GET",
            "/api/cctv/media/cam-1",
            &[("range", "bytes=0-3")],
        )
        .await;
        assert_eq!(status, 206);
        assert_eq!(body, b"abcd");
        assert_eq!(headers["content-range"], "bytes 0-3/100");
        assert_eq!(headers["x-cctv-source"], "live-media");
        let request = seen.lock().last().cloned().unwrap();
        assert_eq!(request.url, "https://media.example.test/cam1.mp4");
        assert_eq!(request.header_value("Range"), Some("bytes=0-3"));
        assert_eq!(request.header_value("User-Agent"), Some(CCTV_USER_AGENT));
        assert_eq!(
            health_of(&cctv, "cam-1")["message"],
            "Live stream connected"
        );

        send(
            &app,
            "GET",
            "/api/cctv/media/cam-1",
            &[("range", "bytes=0-1,5-9")],
        )
        .await;
        assert_eq!(seen.lock().last().unwrap().header_value("Range"), None);
    }

    #[tokio::test]
    async fn media_status_passthrough_size_cap_and_failures() {
        let (fetch, _) =
            json_fetch(|_| FetchResponse::new(416, &[("content-type", "text/plain")], "nope"));
        let (app, cctv) = make_app(fetch, &[]);
        let (status, headers, body) = send(
            &app,
            "GET",
            "/api/cctv/media/cam-1",
            &[("range", "bytes=999-")],
        )
        .await;
        assert_eq!(status, 416);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(json_of(&body), json!({"error": "Upstream returned 416"}));
        assert_eq!(health_of(&cctv, "cam-1")["message"], "Upstream HTTP 416");

        let declared = (constants::CCTV_MEDIA_MAX_BODY_BYTES + 1).to_string();
        let (fetch, _) = json_fetch(move |_| {
            FetchResponse::new(
                200,
                &[("content-type", "video/mp4"), ("content-length", &declared)],
                "",
            )
        });
        let (app, _) = make_app(fetch, &[]);
        let (status, _, body) = send(&app, "GET", "/api/cctv/media/cam-1", &[]).await;
        assert_eq!(status, 502);
        assert_eq!(
            json_of(&body),
            json!({"error": "Upstream media exceeds size cap"})
        );

        let (app, cctv) = make_app(no_network(), &[]);
        let (status, _, body) = send(&app, "GET", "/api/cctv/media/cam-1", &[]).await;
        assert_eq!(status, 502);
        assert_eq!(json_of(&body), json!({"error": "Media proxy failed"}));
        assert_eq!(health_of(&cctv, "cam-1")["message"], "Media fetch failed");

        let hanging: Fetcher = Arc::new(|_| futures_util::future::pending().boxed());
        let (app, _) = make_app(hanging, &[]);
        let (status, _, body) = send(&app, "GET", "/api/cctv/media/cam-1", &[]).await;
        assert_eq!(status, 504);
        assert_eq!(json_of(&body), json!({"error": "Upstream media timeout"}));

        let (fetch, _) =
            json_fetch(|_| FetchResponse::new(200, &[("content-type", "text/html")], "<html>"));
        let (app, cctv) = make_app(fetch, &[]);
        send(&app, "GET", "/api/cctv/media/cam-1", &[]).await;
        assert_eq!(
            health_of(&cctv, "cam-1")["message"],
            "Unexpected media type text/html"
        );
        assert_eq!(health_of(&cctv, "cam-1")["status"], "degraded");
    }

    #[tokio::test]
    async fn media_route_guards() {
        let (app, cctv) = make_app(no_network(), &[]);
        assert_eq!(send(&app, "GET", "/api/cctv/media/a/b", &[]).await.0, 404);
        assert_eq!(
            send(&app, "POST", "/api/cctv/media/cam-1", &[]).await.0,
            405
        );
        assert_eq!(
            send(&app, "DELETE", "/api/cctv/media/cam-1", &[]).await.0,
            204
        );
        assert_eq!(
            send(&app, "GET", "/api/cctv/media/cam-1/seg_1.ts", &[])
                .await
                .0,
            404
        );

        let (status, _, body) = send(&app, "GET", "/api/cctv/media/cam-none", &[]).await;
        assert_eq!(status, 404);
        assert_eq!(
            json_of(&body),
            json!({"error": "No media URL configured for this camera"})
        );
        let health = health_of(&cctv, "cam-none");
        assert_eq!(health["label"], "Configured CCTV Source");
        assert_eq!(health["message"], "No stream URL configured");

        // HLS needs a lease, then an HTTP(S) .m3u8 URL.
        assert_eq!(
            send(&app, "GET", "/api/cctv/media/cam-hls", &[]).await.0,
            400
        );
        assert_eq!(
            send(&app, "GET", "/api/cctv/media/cam-hls?lease=short", &[])
                .await
                .0,
            400
        );
        let lease = "123e4567-e89b-12d3-a456-426614174000";
        let (status, headers, body) = send(
            &app,
            "GET",
            &format!("/api/cctv/media/cam-hls?lease={lease}"),
            &[],
        )
        .await;
        assert_eq!(status, 503);
        assert!(headers.get("cache-control").is_none());
        assert_eq!(
            json_of(&body),
            json!({"error": "This stream requires an unsupported transport; use the frame fallback"})
        );
    }

    #[tokio::test]
    async fn frames_fall_back_from_upstream_to_street_view_to_synthetic() {
        let (fetch, seen) = json_fetch(|request| {
            if request.url.starts_with("https://maps.googleapis.com/") {
                FetchResponse::new(200, &[("content-type", "image/jpeg")], "sv")
            } else if request.url.ends_with("still.jpg") {
                FetchResponse::new(200, &[("content-type", "image/jpeg")], "still")
            } else {
                FetchResponse::new(404, &[], "")
            }
        });
        let (app, cctv) = make_app(
            fetch,
            &[
                ("GOOGLE_MAPS_SERVER_API_KEY", " server-key "),
                ("GOOGLE_MAPS_API_KEY", "browser"),
            ],
        );
        let (status, headers, body) = send(&app, "GET", "/api/cctv/frame/cam-img", &[]).await;
        assert_eq!((status, body.as_slice()), (200, b"still".as_slice()));
        assert_eq!(headers["x-cctv-source"], "upstream-image");
        assert_eq!(
            health_of(&cctv, "cam-img")["message"],
            "Upstream snapshot active"
        );

        let (_, headers, body) = send(
            &app,
            "GET",
            "/api/cctv/frame/cam-1?fov=200&pitch=-90&heading=",
            &[],
        )
        .await;
        assert_eq!(headers["x-cctv-source"], "streetview");
        assert_eq!(body, b"sv");
        let sv = seen.lock().last().unwrap().url.clone();
        assert_eq!(
            sv,
            "https://maps.googleapis.com/maps/api/streetview?size=960x540&location=30.27%2C-97.74&heading=0&fov=120&pitch=-40&source=outdoor&return_error_code=true&key=server-key"
        );
        assert_eq!(health_of(&cctv, "cam-1")["label"], "Google Street View");

        let (app, cctv) = make_app(no_network(), &[]);
        let (status, headers, body) =
            send(&app, "GET", "/api/cctv/frame/unknown?label=Hi", &[]).await;
        assert_eq!(status, 200);
        assert_eq!(headers["content-type"], "image/svg+xml");
        assert_eq!(headers["x-cctv-source"], "synthetic");
        let svg = String::from_utf8(body).unwrap();
        assert!(svg.contains("NO UPSTREAM CONFIGURED") && svg.contains("Hi"));
        let health = health_of(&cctv, "unknown");
        assert_eq!(health["label"], "Synthetic fallback");
        assert_eq!(health["message"], "No source configured");
    }

    #[test]
    fn health_merges_patches_and_evicts_the_oldest() {
        let cctv = Cctv::new(
            env_map(&[]),
            no_network(),
            PathBuf::from("/nonexistent"),
            Timeouts::default(),
        );
        cctv.set_health(
            "a",
            Patch {
                status: "ok",
                source_kind: "live",
                label: "L",
                message: "m",
            },
        );
        cctv.set_health(
            "a",
            Patch {
                status: "",
                source_kind: "",
                label: "",
                message: "next",
            },
        );
        let a = health_of(&cctv, "a");
        assert_eq!(
            (
                a["status"].clone(),
                a["label"].clone(),
                a["message"].clone()
            ),
            (json!("ok"), json!("L"), json!("next"))
        );
        for i in 0..CCTV_MAX_SOURCES_CEILING as usize {
            cctv.set_health(
                &format!("c{i}"),
                Patch {
                    status: "",
                    source_kind: "",
                    label: "",
                    message: "",
                },
            );
        }
        assert_eq!(cctv.health.lock().len(), CCTV_MAX_SOURCES_CEILING as usize);
        assert_eq!(health_of(&cctv, "a"), Value::Null);
        assert_eq!(health_of(&cctv, "c0")["status"], "unknown");
    }
}
