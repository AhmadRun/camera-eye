//! Port of `server/providers/cctv/stream.js`: bounded, demand-owned HLS
//! sessions. No subprocess, disk cache or idle sweep.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use indexmap::IndexMap;
use parking_lot::Mutex;
use tokio::task::AbortHandle;

use super::constants::CCTV_USER_AGENT;
use super::fetch::{FetchRequest, Fetcher, Redirect};
use super::jsval;

/// `HLS_LIMITS`.
#[derive(Debug, Clone, Copy)]
pub struct HlsLimits {
    pub sessions: usize,
    pub leases_per_session: usize,
    pub playlist_bytes: usize,
    pub segment_bytes: usize,
    pub session_bytes: usize,
    pub segments: usize,
    pub poll_ms: u64,
    pub idle_ms: u64,
    pub timeout_ms: u64,
    pub ready_ms: u64,
}

pub const HLS_LIMITS: HlsLimits = HlsLimits {
    sessions: 2,
    leases_per_session: 8,
    playlist_bytes: 256 * 1024,
    segment_bytes: 4 * 1024 * 1024,
    session_bytes: 24 * 1024 * 1024,
    segments: 12,
    poll_ms: 2000,
    idle_ms: 15000,
    timeout_ms: 10000,
    ready_ms: 12000,
};

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// `fetchHlsBytes`: refuse redirects; count the streamed bytes.
pub async fn fetch_hls_bytes(
    fetch: &Fetcher,
    url: &str,
    max_bytes: usize,
    timeout: Duration,
) -> Result<Bytes, String> {
    let request =
        FetchRequest::new(url, Redirect::Manual, false).header("User-Agent", CCTV_USER_AGENT);
    let work = async {
        let response = fetch(request).await?;
        if !response.ok() {
            return Err("HLS upstream refused".to_string());
        }
        if response.declared_length() > max_bytes as f64 {
            return Err("HLS response too large".to_string());
        }
        let mut body = response.body;
        let mut out = BytesMut::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if out.len() + chunk.len() > max_bytes {
                return Err("HLS response too large".to_string());
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    };
    tokio::time::timeout(timeout, work)
        .await
        .map_err(|_| "HLS fetch timed out".to_string())?
}

/// `sameOriginHlsUrl`: resolve a reference against `base`, refusing any
/// other origin, scheme or credentials.
pub fn same_origin_hls_url(value: &str, base: &str) -> Result<String, String> {
    let escape = || "HLS reference escapes registered origin".to_string();
    let base_url = url::Url::parse(base).map_err(|_| escape())?;
    let url = base_url.join(value).map_err(|_| escape())?;
    if !matches!(url.scheme(), "https" | "http")
        || url.origin() != base_url.origin()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(escape());
    }
    Ok(url.to_string())
}

/// One upstream media segment reference.
#[derive(Debug, Clone, PartialEq)]
pub struct HlsSegment {
    pub seq: i64,
    pub duration: f64,
    pub uri: String,
    pub discontinuity: bool,
}

/// `parseHlsMedia`: only clear MPEG-TS media playlists; encrypted, fMP4 and
/// byte ranges fail closed.
pub fn parse_hls_media(text: &str, base: &str, limit: usize) -> Result<Vec<HlsSegment>, String> {
    if !text.starts_with("#EXTM3U")
        || [
            "#EXT-X-KEY",
            "#EXT-X-MAP",
            "#EXT-X-BYTERANGE",
            "#EXT-X-I-FRAMES-ONLY",
        ]
        .iter()
        .any(|tag| text.contains(tag))
    {
        return Err("Unsupported HLS playlist".into());
    }
    let safe = |n: f64| n.is_finite() && n.fract() == 0.0 && n.abs() <= MAX_SAFE_INTEGER;
    let mut seq: f64 = 0.0;
    let mut duration: Option<f64> = None;
    let mut discontinuity = false;
    let mut segments = Vec::new();
    for raw in text.split('\n') {
        let line = jsval::trim(raw);
        if let Some(rest) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            seq = crate::common::js::number(rest);
            if !safe(seq) || seq < 0.0 {
                return Err("Invalid media sequence".into());
            }
        } else if line == "#EXT-X-DISCONTINUITY" {
            discontinuity = true;
        } else if let Some(rest) = line.strip_prefix("#EXTINF:") {
            let d = crate::common::js::parse_float(rest);
            if !d.is_finite() || d <= 0.0 || d > 60.0 {
                return Err("Invalid segment duration".into());
            }
            duration = Some(d);
        } else if !line.is_empty() && !line.starts_with('#') {
            let Some(d) = duration else {
                return Err("Invalid HLS segment".into());
            };
            if !safe(seq) {
                return Err("Invalid HLS segment".into());
            }
            let uri = same_origin_hls_url(line, base)?;
            let path_ok = url::Url::parse(&uri).is_ok_and(|u| u.path().ends_with(".ts"));
            if !path_ok {
                return Err("Only MPEG-TS segments are supported".into());
            }
            segments.push(HlsSegment {
                seq: seq as i64,
                duration: d,
                uri,
                discontinuity,
            });
            seq += 1.0;
            discontinuity = false;
            duration = None;
        }
    }
    let skip = segments.len().saturating_sub(limit);
    Ok(segments.split_off(skip))
}

struct CachedSegment {
    duration: f64,
    discontinuity: bool,
    body: Bytes,
}

#[derive(Default)]
struct EntryState {
    segments: BTreeMap<u64, CachedSegment>,
    bytes: usize,
    stopping: bool,
    failures: u32,
    next_seq: u64,
    upstream: IndexMap<i64, String>,
    leases: HashMap<String, Option<AbortHandle>>,
    upstream_newest: i64,
    pending_discontinuity: bool,
    discontinuities_removed: u64,
    chunklist_url: Option<String>,
}

/// One camera's live session.
pub struct Entry {
    pub camera_id: String,
    pub url: String,
    pub token: String,
    state: Mutex<EntryState>,
    poll: Mutex<Option<AbortHandle>>,
}

impl Entry {
    pub fn stopping(&self) -> bool {
        self.state.lock().stopping
    }

    pub fn has_lease(&self, lease: &str) -> bool {
        self.state.lock().leases.contains_key(lease)
    }

    #[cfg(test)]
    pub fn lease_count(&self) -> usize {
        self.state.lock().leases.len()
    }
}

/// `createHlsPuller`.
pub struct HlsPuller {
    fetch: Fetcher,
    limits: HlsLimits,
    active: Mutex<HashMap<String, Arc<Entry>>>,
    closed: AtomicBool,
    me: Weak<HlsPuller>,
}

impl HlsPuller {
    pub fn new(fetch: Fetcher, limits: HlsLimits) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            fetch,
            limits,
            active: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            me: me.clone(),
        })
    }

    /// Stop a session (only the one holding `token`, when given).
    pub fn stop(&self, camera_id: &str, token: Option<&str>) {
        let entry = {
            let mut active = self.active.lock();
            let Some(entry) = active.get(camera_id) else {
                return;
            };
            if let Some(token) = token.filter(|t| !t.is_empty())
                && token != entry.token
            {
                return;
            }
            active.remove(camera_id)
        };
        let Some(entry) = entry else {
            return;
        };
        if let Some(handle) = entry.poll.lock().take() {
            handle.abort();
        }
        let mut state = entry.state.lock();
        state.stopping = true;
        for (_, timer) in state.leases.drain() {
            if let Some(timer) = timer {
                timer.abort();
            }
        }
        state.segments.clear();
        state.upstream.clear();
        state.bytes = 0;
    }

    /// Release one consumer; the last one out stops the session.
    pub fn release(&self, camera_id: &str, lease_id: &str) {
        let Some(entry) = self.active.lock().get(camera_id).cloned() else {
            return;
        };
        let now_empty = {
            let mut state = entry.state.lock();
            let Some(timer) = state.leases.remove(lease_id) else {
                return;
            };
            if let Some(timer) = timer {
                timer.abort();
            }
            state.leases.is_empty()
        };
        if now_empty {
            self.stop(camera_id, Some(&entry.token));
        }
    }

    fn touch(&self, entry: &Arc<Entry>, lease_id: &str, create: bool) -> Result<(), String> {
        let mut state = entry.state.lock();
        if !state.leases.contains_key(lease_id) {
            if !create || state.leases.len() >= self.limits.leases_per_session {
                return Err("HLS lease capacity reached".into());
            }
            state.leases.insert(lease_id.to_string(), None);
        }
        let me = self.me.clone();
        let camera_id = entry.camera_id.clone();
        let lease = lease_id.to_string();
        let idle = Duration::from_millis(self.limits.idle_ms);
        let timer = tokio::spawn(async move {
            tokio::time::sleep(idle).await;
            if let Some(puller) = me.upgrade() {
                puller.release(&camera_id, &lease);
            }
        })
        .abort_handle();
        if let Some(Some(old)) = state.leases.insert(lease_id.to_string(), Some(timer)) {
            old.abort();
        }
        Ok(())
    }

    async fn read(&self, url: &str, max_bytes: usize) -> Result<Bytes, String> {
        fetch_hls_bytes(
            &self.fetch,
            url,
            max_bytes,
            Duration::from_millis(self.limits.timeout_ms),
        )
        .await
    }

    async fn poll_once(&self, entry: &Arc<Entry>) {
        let outcome: Result<(), String> = async {
            let mut base = entry
                .state
                .lock()
                .chunklist_url
                .clone()
                .unwrap_or_else(|| entry.url.clone());
            let mut text =
                String::from_utf8_lossy(&self.read(&base, self.limits.playlist_bytes).await?)
                    .into_owned();
            if !text.contains("#EXTINF:") {
                let variant = text
                    .split('\n')
                    .map(jsval::trim)
                    .find(|line| !line.is_empty() && !line.starts_with('#'))
                    .map(str::to_string);
                let Some(variant) = variant.filter(|_| text.contains("#EXT-X-STREAM-INF:")) else {
                    return Err("Missing HLS variant".to_string());
                };
                base = same_origin_hls_url(&variant, &entry.url)?;
                text =
                    String::from_utf8_lossy(&self.read(&base, self.limits.playlist_bytes).await?)
                        .into_owned();
            }
            let segments = {
                let mut state = entry.state.lock();
                state.chunklist_url = Some(base.clone());
                let segments = parse_hls_media(&text, &base, self.limits.segments)?;
                let newest = segments.last().map_or(-1, |s| s.seq);
                let restarted = newest < state.upstream_newest
                    || segments.iter().any(|s| {
                        state
                            .upstream
                            .get(&s.seq)
                            .is_some_and(|prior| *prior != s.uri)
                    });
                if restarted {
                    state.upstream.clear();
                    state.pending_discontinuity = true;
                }
                state.upstream_newest = newest;
                segments
            };
            for segment in segments {
                {
                    let state = entry.state.lock();
                    if state.stopping {
                        return Ok(());
                    }
                    if state.upstream.contains_key(&segment.seq) {
                        continue;
                    }
                }
                let body = self.read(&segment.uri, self.limits.segment_bytes).await?;
                let mut state = entry.state.lock();
                if state.stopping {
                    return Ok(());
                }
                // Evict before retaining the incoming segment.
                while !state.segments.is_empty()
                    && (state.bytes + body.len() > self.limits.session_bytes
                        || state.segments.len() >= self.limits.segments)
                {
                    if let Some((_, removed)) = state.segments.pop_first() {
                        state.bytes -= removed.body.len();
                        if removed.discontinuity {
                            state.discontinuities_removed += 1;
                        }
                    }
                }
                if body.len() > self.limits.session_bytes {
                    return Err("Segment exceeds session budget".into());
                }
                let seq = state.next_seq;
                state.next_seq += 1;
                let discontinuity = segment.discontinuity || state.pending_discontinuity;
                state.bytes += body.len();
                state.segments.insert(
                    seq,
                    CachedSegment {
                        duration: segment.duration,
                        discontinuity,
                        body,
                    },
                );
                state.pending_discontinuity = false;
                state.upstream.insert(segment.seq, segment.uri.clone());
                while state.upstream.len() > self.limits.segments * 2 {
                    state.upstream.shift_remove_index(0);
                }
            }
            entry.state.lock().failures = 0;
            Ok(())
        }
        .await;
        if outcome.is_err() {
            let failures = {
                let mut state = entry.state.lock();
                state.chunklist_url = None;
                state.failures += 1;
                state.failures
            };
            if failures >= 3 {
                self.stop(&entry.camera_id, Some(&entry.token));
            }
        }
    }

    /// Open (or join) the session for `camera_id`.
    pub fn ensure(&self, camera_id: &str, url: &str, lease_id: &str) -> Result<Arc<Entry>, String> {
        if self.closed.load(Ordering::SeqCst) {
            return Err("HLS service closed".into());
        }
        same_origin_hls_url(url, url)?;
        let existing = self.active.lock().get(camera_id).cloned();
        if let Some(entry) = existing {
            if entry.url != url {
                self.stop(camera_id, None);
            } else {
                self.touch(&entry, lease_id, true)?;
                return Ok(entry);
            }
        }
        let entry = {
            let mut active = self.active.lock();
            if active.len() >= self.limits.sessions {
                return Err("HLS session capacity reached".into());
            }
            let entry = Arc::new(Entry {
                camera_id: camera_id.to_string(),
                url: url.to_string(),
                token: uuid::Uuid::new_v4().to_string(),
                state: Mutex::new(EntryState {
                    upstream_newest: -1,
                    ..EntryState::default()
                }),
                poll: Mutex::new(None),
            });
            active.insert(camera_id.to_string(), Arc::clone(&entry));
            entry
        };
        self.touch(&entry, lease_id, true)?;
        let me = self.me.clone();
        let polled = Arc::clone(&entry);
        let poll_ms = self.limits.poll_ms;
        let handle = tokio::spawn(async move {
            loop {
                let Some(puller) = me.upgrade() else {
                    return;
                };
                puller.poll_once(&polled).await;
                drop(puller);
                if polled.stopping() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
        })
        .abort_handle();
        *entry.poll.lock() = Some(handle);
        if entry.stopping()
            && let Some(handle) = entry.poll.lock().take()
        {
            handle.abort();
        }
        Ok(entry)
    }

    /// Wait until the session holds two segments (or stops, or times out).
    pub async fn wait_ready(&self, entry: &Arc<Entry>) -> bool {
        let until = Instant::now() + Duration::from_millis(self.limits.ready_ms);
        loop {
            {
                let state = entry.state.lock();
                if state.stopping || Instant::now() >= until {
                    return false;
                }
                if state.segments.len() >= 2 {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The local media playlist for one consumer.
    pub fn build_playlist(
        &self,
        entry: &Arc<Entry>,
        camera_id: &str,
        lease_id: &str,
    ) -> Result<Option<String>, String> {
        {
            let state = entry.state.lock();
            if state.stopping || state.segments.len() < 2 {
                return Ok(None);
            }
        }
        self.touch(entry, lease_id, false)?;
        let state = entry.state.lock();
        let target = state
            .segments
            .values()
            .map(|s| s.duration)
            .fold(f64::NEG_INFINITY, f64::max)
            .ceil();
        let first = state.segments.keys().next().copied().unwrap_or(0);
        let mut lines = vec![
            "#EXTM3U".to_string(),
            "#EXT-X-VERSION:3".to_string(),
            format!("#EXT-X-TARGETDURATION:{}", jsval::number_string(target)),
            format!("#EXT-X-MEDIA-SEQUENCE:{first}"),
            format!(
                "#EXT-X-DISCONTINUITY-SEQUENCE:{}",
                state.discontinuities_removed
            ),
        ];
        let mut previous: Option<u64> = None;
        for (seq, segment) in &state.segments {
            if segment.discontinuity || previous.is_some_and(|p| *seq != p + 1) {
                lines.push("#EXT-X-DISCONTINUITY".into());
            }
            lines.push(format!("#EXTINF:{},", jsval::to_fixed(segment.duration, 3)));
            lines.push(format!(
                "/api/cctv/media/{}/seg_{seq}.ts?session={}&lease={}",
                jsval::encode_uri_component(camera_id),
                entry.token,
                jsval::encode_uri_component(lease_id)
            ));
            previous = Some(*seq);
        }
        Ok(Some(lines.join("\n") + "\n"))
    }

    /// One cached segment, for the consumer holding `lease_id`.
    pub fn get_segment(
        &self,
        camera_id: &str,
        token: Option<&str>,
        seq: Option<u64>,
        lease_id: &str,
    ) -> Option<Bytes> {
        let entry = self.active.lock().get(camera_id).cloned()?;
        if token != Some(entry.token.as_str()) || !entry.has_lease(lease_id) {
            return None;
        }
        let body = seq.and_then(|seq| {
            entry
                .state
                .lock()
                .segments
                .get(&seq)
                .map(|s| s.body.clone())
        });
        if body.is_some() {
            let _ = self.touch(&entry, lease_id, false);
        }
        body
    }

    /// Stop every session and refuse new ones.
    pub fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let ids: Vec<String> = self.active.lock().keys().cloned().collect();
        for id in ids {
            self.stop(&id, None);
        }
    }

    /// `(sessions, bytes)`.
    #[cfg(test)]
    pub fn stats(&self) -> (usize, usize) {
        let active = self.active.lock();
        let bytes = active.values().map(|e| e.state.lock().bytes).sum();
        (active.len(), bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cctv::fetch::FetchResponse;
    use futures_util::FutureExt;
    use std::sync::atomic::AtomicUsize;

    const PLAYLIST: &str =
        "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:12\n#EXTINF:2,\na.ts\n#EXTINF:2,\nb.ts\n#EXTINF:2,\nc.ts\n";
    const BASE: &str = "https://camera.example/live/list.m3u8";

    fn serving(current: Arc<Mutex<String>>, downloads: Arc<AtomicUsize>) -> Fetcher {
        Arc::new(move |request: FetchRequest| {
            downloads.fetch_add(1, Ordering::SeqCst);
            let body = if request.url.ends_with(".m3u8") {
                current.lock().clone()
            } else {
                "abc".to_string()
            };
            async move { Ok(FetchResponse::new(200, &[], body)) }.boxed()
        })
    }

    fn simple() -> Fetcher {
        serving(
            Arc::new(Mutex::new(PLAYLIST.into())),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    #[test]
    fn playlist_parser_uses_sequence_and_refuses_escapes() {
        let seqs: Vec<i64> = parse_hls_media(PLAYLIST, BASE, 12)
            .unwrap()
            .iter()
            .map(|s| s.seq)
            .collect();
        assert_eq!(seqs, [12, 13, 14]);
        for bad in [
            PLAYLIST.replace("a.ts", "https://evil.example/a.ts"),
            PLAYLIST.replace("a.ts", "//evil.example/a.ts"),
            format!("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128\n{PLAYLIST}"),
            PLAYLIST.replace("12", "-1"),
            PLAYLIST.replacen("2,", "Infinity,", 1),
        ] {
            assert!(parse_hls_media(&bad, BASE, 12).is_err(), "{bad}");
        }
        let parsed = parse_hls_media(
            &PLAYLIST.replace("a.ts", "a.ts\n#EXT-X-DISCONTINUITY"),
            BASE,
            12,
        )
        .unwrap();
        assert!(!parsed[0].discontinuity);
        assert!(parsed[1].discontinuity);
    }

    #[tokio::test]
    async fn downloads_reject_redirects_and_oversized_bodies() {
        let redirect: Fetcher = Arc::new(|r: FetchRequest| {
            assert_eq!(r.redirect, Redirect::Manual);
            async {
                Ok(FetchResponse::new(
                    302,
                    &[("location", "https://evil.example")],
                    "",
                ))
            }
            .boxed()
        });
        assert!(
            fetch_hls_bytes(&redirect, BASE, 5, Duration::from_secs(1))
                .await
                .is_err()
        );
        let long: Fetcher =
            Arc::new(|_| async { Ok(FetchResponse::new(200, &[], "abcdef")) }.boxed());
        assert!(
            fetch_hls_bytes(&long, BASE, 5, Duration::from_secs(1))
                .await
                .is_err()
        );
        let declared: Fetcher = Arc::new(|_| {
            async { Ok(FetchResponse::new(200, &[("content-length", "100")], "a")) }.boxed()
        });
        assert!(
            fetch_hls_bytes(&declared, BASE, 5, Duration::from_secs(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn one_session_is_reserved_and_the_cache_stays_bounded() {
        let manager = HlsPuller::new(
            simple(),
            HlsLimits {
                sessions: 1,
                segment_bytes: 4,
                session_bytes: 6,
                segments: 2,
                poll_ms: 100_000,
                ..HLS_LIMITS
            },
        );
        let a = manager.ensure("a", BASE, "legacy").unwrap();
        let b = manager.ensure("a", BASE, "legacy").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(manager.ensure("b", BASE, "legacy").is_err());
        assert!(manager.wait_ready(&a).await);
        assert_eq!(manager.stats(), (1, 6));
        let text = manager.build_playlist(&a, "a", "legacy").unwrap().unwrap();
        assert!(text.contains("seg_0.ts?session="), "{text}");
        assert_eq!(
            manager.get_segment("a", Some("stale-token"), Some(0), "legacy"),
            None
        );
        assert_eq!(
            manager
                .get_segment("a", Some(&a.token), Some(0), "legacy")
                .unwrap()
                .len(),
            3
        );
        manager.stop("a", Some("stale-token"));
        assert_eq!(manager.stats().0, 1);
        manager.stop("a", Some(&a.token));
        assert_eq!(manager.stats(), (0, 0));
        manager.shutdown();
    }

    #[tokio::test]
    async fn shutdown_cancels_in_flight_downloads() {
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let fetcher: Fetcher = Arc::new(move |_| {
            struct OnDrop(Arc<AtomicBool>);
            impl Drop for OnDrop {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let guard = OnDrop(Arc::clone(&flag));
            async move {
                let _guard = guard;
                futures_util::future::pending::<Result<FetchResponse, String>>().await
            }
            .boxed()
        });
        let manager = HlsPuller::new(fetcher, HLS_LIMITS);
        let entry = manager.ensure("a", BASE, "legacy").unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        manager.shutdown();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(dropped.load(Ordering::SeqCst));
        assert!(entry.stopping());
        assert_eq!(manager.stats(), (0, 0));
        assert!(manager.ensure("a", BASE, "legacy").is_err());
    }

    #[tokio::test]
    async fn idle_cleanup_stops_polling() {
        let downloads = Arc::new(AtomicUsize::new(0));
        let manager = HlsPuller::new(
            serving(
                Arc::new(Mutex::new(PLAYLIST.into())),
                Arc::clone(&downloads),
            ),
            HlsLimits {
                idle_ms: 15,
                poll_ms: 100_000,
                ..HLS_LIMITS
            },
        );
        manager.ensure("a", BASE, "legacy").unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(manager.stats(), (0, 0));
        let stopped = downloads.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(downloads.load(Ordering::SeqCst), stopped);
    }

    #[tokio::test]
    async fn sequence_rollback_creates_a_local_discontinuity() {
        let current = Arc::new(Mutex::new(PLAYLIST.to_string()));
        let manager = HlsPuller::new(
            serving(Arc::clone(&current), Arc::new(AtomicUsize::new(0))),
            HlsLimits {
                poll_ms: 5,
                ..HLS_LIMITS
            },
        );
        let entry = manager.ensure("a", BASE, "legacy").unwrap();
        assert!(manager.wait_ready(&entry).await);
        let before = manager
            .build_playlist(&entry, "a", "legacy")
            .unwrap()
            .unwrap();
        assert!(before.contains("seg_0.ts"));
        *current.lock() = PLAYLIST.replace("SEQUENCE:12", "SEQUENCE:0");
        tokio::time::sleep(Duration::from_millis(60)).await;
        let after = manager
            .build_playlist(&entry, "a", "legacy")
            .unwrap()
            .unwrap();
        assert!(
            after.contains("#EXT-X-DISCONTINUITY\n#EXTINF:2.000,\n/api/cctv/media/a/seg_3.ts"),
            "{after}"
        );
        manager.shutdown();
    }

    #[tokio::test]
    async fn a_reused_sequence_with_a_changed_uri_cannot_remain_stale() {
        let current = Arc::new(Mutex::new(PLAYLIST.to_string()));
        let manager = HlsPuller::new(
            serving(Arc::clone(&current), Arc::new(AtomicUsize::new(0))),
            HlsLimits {
                poll_ms: 5,
                ..HLS_LIMITS
            },
        );
        let entry = manager.ensure("a", BASE, "legacy").unwrap();
        assert!(manager.wait_ready(&entry).await);
        *current.lock() = PLAYLIST.replace(".ts", ".ts?generation=2");
        tokio::time::sleep(Duration::from_millis(60)).await;
        let text = manager
            .build_playlist(&entry, "a", "legacy")
            .unwrap()
            .unwrap();
        assert!(
            text.contains("#EXT-X-DISCONTINUITY\n#EXTINF:2.000,\n/api/cctv/media/a/seg_3.ts"),
            "{text}"
        );
        manager.shutdown();
    }

    #[tokio::test]
    async fn two_consumers_share_downloads_but_release_independently() {
        let downloads = Arc::new(AtomicUsize::new(0));
        let manager = HlsPuller::new(
            serving(
                Arc::new(Mutex::new(PLAYLIST.into())),
                Arc::clone(&downloads),
            ),
            HlsLimits {
                poll_ms: 100_000,
                leases_per_session: 2,
                ..HLS_LIMITS
            },
        );
        let a = manager.ensure("camera", BASE, "viewer-a").unwrap();
        let b = manager.ensure("camera", BASE, "viewer-b").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(manager.wait_ready(&a).await);
        assert_eq!(downloads.load(Ordering::SeqCst), 4);
        assert_eq!(a.lease_count(), 2);
        assert!(manager.ensure("camera", BASE, "viewer-c").is_err());
        manager.release("camera", "viewer-a");
        assert_eq!(manager.stats().0, 1);
        assert_eq!(
            manager.get_segment("camera", Some(&a.token), Some(0), "viewer-a"),
            None
        );
        assert_eq!(
            manager
                .get_segment("camera", Some(&a.token), Some(0), "viewer-b")
                .unwrap()
                .len(),
            3
        );
        assert!(
            manager
                .build_playlist(&b, "camera", "viewer-b")
                .unwrap()
                .unwrap()
                .contains("lease=viewer-b")
        );
        manager.release("camera", "viewer-a");
        assert!(!b.stopping());
        manager.release("camera", "viewer-b");
        assert!(b.stopping());
        assert_eq!(manager.stats(), (0, 0));
    }

    #[tokio::test]
    async fn an_abandoned_consumer_expires_while_a_renewed_one_stays() {
        let manager = HlsPuller::new(
            simple(),
            HlsLimits {
                idle_ms: 80,
                poll_ms: 100_000,
                ..HLS_LIMITS
            },
        );
        let entry = manager.ensure("camera", BASE, "abandoned").unwrap();
        manager.ensure("camera", BASE, "active").unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        manager.ensure("camera", BASE, "active").unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!entry.has_lease("abandoned"));
        assert!(entry.has_lease("active"));
        assert!(!entry.stopping());
        manager.shutdown();
    }
}
