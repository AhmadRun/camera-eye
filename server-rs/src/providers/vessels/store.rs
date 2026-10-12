//! In-memory AISStream vessel cache and per-MMSI recent-path ring buffers
//! (port of `server/providers/vessels/ais-store.js`).
//!
//! The JS store is module state backed by `Map`s, whose iteration order is
//! insertion order (an update keeps the original slot). Rows here carry the
//! sequence number of their first insertion so every sort reproduces the
//! JS tie-breaking exactly.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use parking_lot::Mutex;
use serde_json::{Map, Value, json};

use super::adapter::{aisenvelope_mmsi_raw, is_recognized_ais_envelope};
use crate::common::js::now_ms;
use crate::providers::js_value::{get, nullish, number_value, or, to_js_string, to_number};

pub const AISSTREAM_CACHE_MAX: usize = 50_000;
pub const AISSTREAM_STALE_MS: i64 = 30 * 60 * 1000;
/// Float32 lat/lon + Uint32 epoch seconds per sample.
const AIS_TRACK_SAMPLES: usize = 64;
const AIS_TRACK_MIN_GAP_SEC: i64 = 30;
const AIS_TRACK_MIN_MOVE_M: f64 = 25.0;

#[derive(Clone, Default)]
struct StaticData {
    name: String,
    type_: String,
    destination: String,
    imo: String,
}

struct Row {
    seq: u64,
    lat: f64,
    lon: f64,
    name: String,
    mmsi: String,
    imo: String,
    type_: String,
    destination: String,
    speed: Option<f64>,
    course: Option<f64>,
    heading: Option<f64>,
    last_position_utc: String,
    last_position_epoch: i64,
    updated_at: i64,
}

impl Row {
    /// The row without `_updatedAt`, in the JS key order.
    fn to_value(&self) -> Value {
        let opt = |v: Option<f64>| v.map_or(Value::Null, number_value);
        json!({
            "lat": number_value(self.lat),
            "lon": number_value(self.lon),
            "name": self.name,
            "mmsi": self.mmsi,
            "imo": self.imo,
            "type": self.type_,
            "destination": self.destination,
            "speed": opt(self.speed),
            "course": opt(self.course),
            "heading": opt(self.heading),
            "last_position_UTC": self.last_position_utc,
            "last_position_epoch": self.last_position_epoch,
        })
    }
}

struct Track {
    lats: [f32; AIS_TRACK_SAMPLES],
    lons: [f32; AIS_TRACK_SAMPLES],
    times: [u32; AIS_TRACK_SAMPLES],
    head: usize,
    len: usize,
}

impl Track {
    fn new() -> Self {
        Self {
            lats: [0.0; AIS_TRACK_SAMPLES],
            lons: [0.0; AIS_TRACK_SAMPLES],
            times: [0; AIS_TRACK_SAMPLES],
            head: 0,
            len: 0,
        }
    }

    fn write(&mut self, lat: f64, lon: f64, epoch_sec: i64) {
        self.lats[self.head] = lat as f32;
        self.lons[self.head] = lon as f32;
        // Uint32Array stores modulo 2^32.
        self.times[self.head] = epoch_sec.rem_euclid(1 << 32) as u32;
        self.head = (self.head + 1) % AIS_TRACK_SAMPLES;
        self.len = (self.len + 1).min(AIS_TRACK_SAMPLES);
    }
}

#[derive(Clone, Copy)]
struct Pending {
    lat: f64,
    lon: f64,
    epoch_sec: i64,
}

#[derive(Default)]
struct Inner {
    next_seq: u64,
    vessels: HashMap<String, Row>,
    statics: HashMap<String, StaticData>,
    tracks: HashMap<String, Box<Track>>,
    pending: HashMap<String, Pending>,
}

/// The vessel cache. One per server, shared with the socket adapter.
#[derive(Default)]
pub struct AisStore {
    inner: Mutex<Inner>,
}

impl AisStore {
    /// `ingestAisStreamEnvelope`: true strictly when the envelope carried a
    /// real AIS record — the feed's only liveness proof.
    pub fn ingest(&self, envelope: &Value) -> bool {
        self.ingest_at(envelope, now_ms())
    }

    fn ingest_at(&self, envelope: &Value, now: i64) -> bool {
        if !is_recognized_ais_envelope(envelope) {
            return false;
        }
        let message_type = envelope
            .get("MessageType")
            .and_then(Value::as_str)
            .unwrap_or("");
        let empty = Value::Object(Map::new());
        let message = or(&[get(get(Some(envelope), "Message"), message_type)]).unwrap_or(&empty);
        let metadata = or(&[
            get(Some(envelope), "MetaData"),
            get(Some(envelope), "Metadata"),
        ])
        .unwrap_or(&empty);
        let mmsi = string_value(aisenvelope_mmsi_raw(metadata, message));
        if mmsi.is_empty() {
            return false;
        }
        let msg = |key: &str| get(Some(message), key);
        let meta = |key: &str| get(Some(metadata), key);

        let mut inner = self.inner.lock();
        if message_type == "ShipStaticData" || message_type == "StaticDataReport" {
            let previous = inner.statics.get(&mmsi).cloned();
            let static_data = StaticData {
                name: vessel_name(metadata, message, previous.as_ref()),
                type_: vessel_type(message, previous.as_ref()),
                destination: string_value(msg("Destination")),
                imo: string_value(nullish(&[msg("ImoNumber"), msg("IMO")])),
            };
            inner.statics.insert(mmsi.clone(), static_data.clone());
            if let Some(existing) = inner.vessels.get_mut(&mmsi) {
                merge_static(existing, &static_data);
            }
        }

        let lat = number_or_null(nullish(&[
            meta("latitude"),
            meta("Latitude"),
            msg("Latitude"),
        ]));
        let lon = number_or_null(nullish(&[
            meta("longitude"),
            meta("Longitude"),
            msg("Longitude"),
        ]));
        // A positionless but well-formed record (static data) is still the
        // feed delivering AIS traffic.
        let (Some(lat), Some(lon)) = (lat, lon) else {
            return true;
        };

        let static_data = inner.statics.get(&mmsi).cloned().unwrap_or_default();
        let time_utc = nullish(&[meta("time_utc"), meta("TimeUtc")]);
        let mut name = vessel_name(metadata, message, Some(&static_data));
        if name.is_empty() {
            name = format!("MMSI {mmsi}");
        }
        let static_str = |s: &str| Value::String(s.to_string());
        let imo_fallback = static_str(&static_data.imo);
        let destination_fallback = static_str(&static_data.destination);
        let epoch = ais_epoch_seconds(time_utc, now);
        let seq = match inner.vessels.get(&mmsi) {
            Some(existing) => existing.seq,
            None => {
                inner.next_seq += 1;
                inner.next_seq
            }
        };
        let row = Row {
            seq,
            lat,
            lon,
            name,
            mmsi: mmsi.clone(),
            imo: string_value(nullish(&[
                msg("ImoNumber"),
                msg("IMO"),
                Some(&imo_fallback),
            ])),
            type_: vessel_type(message, Some(&static_data)),
            destination: string_value(nullish(&[msg("Destination"), Some(&destination_fallback)])),
            speed: normalized_speed(nullish(&[msg("Sog"), msg("SOG")])),
            course: normalized_course(nullish(&[msg("Cog"), msg("COG")])),
            heading: normalized_heading(nullish(&[msg("TrueHeading"), msg("Heading")])),
            last_position_utc: normalize_ais_timestamp(time_utc, now),
            last_position_epoch: epoch,
            updated_at: now,
        };
        inner.vessels.insert(mmsi.clone(), row);
        inner.append_track_sample(&mmsi, lat, lon, epoch);
        inner.prune(now);
        true
    }

    /// `aisStreamRows(maxRows)`: fresh rows, newest first.
    pub fn rows(&self, max_rows: usize) -> Vec<Value> {
        self.rows_at(max_rows, now_ms())
    }

    fn rows_at(&self, max_rows: usize, now: i64) -> Vec<Value> {
        let inner = self.inner.lock();
        let cutoff = now - AISSTREAM_STALE_MS;
        let mut rows: Vec<&Row> = inner
            .vessels
            .values()
            .filter(|row| row.updated_at >= cutoff)
            .collect();
        rows.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.seq.cmp(&b.seq)));
        rows.into_iter().take(max_rows).map(Row::to_value).collect()
    }

    /// `readAisTrack(mmsi)`: the accumulated track, oldest first.
    pub fn track(&self, mmsi: &str) -> Vec<Value> {
        let inner = self.inner.lock();
        let Some(track) = inner.tracks.get(mmsi) else {
            return Vec::new();
        };
        let start = (track.head + AIS_TRACK_SAMPLES - track.len) % AIS_TRACK_SAMPLES;
        (0..track.len)
            .map(|i| {
                let idx = (start + i) % AIS_TRACK_SAMPLES;
                json!({
                    "lat": number_value(f64::from(track.lats[idx])),
                    "lon": number_value(f64::from(track.lons[idx])),
                    "t": track.times[idx],
                })
            })
            .collect()
    }
}

impl Inner {
    /// Thinned append; buffers allocate lazily on the second fix.
    fn append_track_sample(&mut self, mmsi: &str, lat: f64, lon: f64, epoch_sec: i64) {
        if let Some(track) = self.tracks.get_mut(mmsi) {
            let last = (track.head + AIS_TRACK_SAMPLES - 1) % AIS_TRACK_SAMPLES;
            if epoch_sec - i64::from(track.times[last]) < AIS_TRACK_MIN_GAP_SEC {
                return;
            }
            let moved = approx_meters_between(
                f64::from(track.lats[last]),
                f64::from(track.lons[last]),
                lat,
                lon,
            );
            if moved < AIS_TRACK_MIN_MOVE_M {
                return;
            }
            track.write(lat, lon, epoch_sec);
            return;
        }
        let Some(&pending) = self.pending.get(mmsi) else {
            self.pending.insert(
                mmsi.to_string(),
                Pending {
                    lat,
                    lon,
                    epoch_sec,
                },
            );
            return;
        };
        if epoch_sec - pending.epoch_sec < AIS_TRACK_MIN_GAP_SEC {
            return;
        }
        if approx_meters_between(pending.lat, pending.lon, lat, lon) < AIS_TRACK_MIN_MOVE_M {
            return;
        }
        let mut track = Box::new(Track::new());
        track.write(pending.lat, pending.lon, pending.epoch_sec);
        track.write(lat, lon, epoch_sec);
        self.tracks.insert(mmsi.to_string(), track);
        self.pending.remove(mmsi);
    }

    fn forget(&mut self, mmsi: &str) {
        self.vessels.remove(mmsi);
        self.tracks.remove(mmsi);
        self.pending.remove(mmsi);
    }

    /// `pruneAisStreamCache`.
    fn prune(&mut self, now: i64) {
        let cutoff = now - AISSTREAM_STALE_MS;
        let stale: Vec<String> = self
            .vessels
            .iter()
            .filter(|(_, row)| row.updated_at < cutoff)
            .map(|(mmsi, _)| mmsi.clone())
            .collect();
        for mmsi in stale {
            self.forget(&mmsi);
        }
        // Pending single-fix entries for vessels never seen again must not leak.
        let pending_cutoff_sec = cutoff.div_euclid(1000);
        self.pending
            .retain(|_, p| p.epoch_sec >= pending_cutoff_sec);
        if self.vessels.len() <= AISSTREAM_CACHE_MAX {
            return;
        }
        let mut ordered: Vec<(i64, u64, String)> = self
            .vessels
            .iter()
            .map(|(mmsi, row)| (row.updated_at, row.seq, mmsi.clone()))
            .collect();
        ordered.sort();
        let excess = self.vessels.len() - AISSTREAM_CACHE_MAX;
        for (_, _, mmsi) in ordered.into_iter().take(excess) {
            self.forget(&mmsi);
        }
    }
}

fn merge_static(existing: &mut Row, data: &StaticData) {
    if !data.name.is_empty()
        && (existing.name.is_empty() || existing.name == format!("MMSI {}", existing.mmsi))
    {
        existing.name = data.name.clone();
    }
    if !data.type_.is_empty() && existing.type_.is_empty() {
        existing.type_ = data.type_.clone();
    }
    if !data.destination.is_empty() && existing.destination.is_empty() {
        existing.destination = data.destination.clone();
    }
    if !data.imo.is_empty() && existing.imo.is_empty() {
        existing.imo = data.imo.clone();
    }
}

fn vessel_name(metadata: &Value, message: &Value, static_data: Option<&StaticData>) -> String {
    let fallback = static_data.map(|s| Value::String(s.name.clone()));
    string_value(nullish(&[
        get(Some(metadata), "ShipName"),
        get(Some(message), "Name"),
        get(Some(message), "ShipName"),
        get(get(Some(message), "ReportA"), "Name"),
        fallback.as_ref(),
    ]))
}

fn vessel_type(message: &Value, static_data: Option<&StaticData>) -> String {
    let fallback = static_data.map(|s| Value::String(s.type_.clone()));
    string_value(nullish(&[
        get(Some(message), "Type"),
        get(Some(message), "ShipType"),
        get(get(Some(message), "ReportB"), "ShipType"),
        fallback.as_ref(),
    ]))
}

/// `stringValue`: `String(value).trim()`, empty for null/undefined.
pub fn string_value(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(v) => to_js_string(v).trim().to_string(),
    }
}

/// `numberValue`: null for null/undefined/'' and non-finite numbers.
fn number_or_null(value: Option<&Value>) -> Option<f64> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        v => Some(to_number(v)).filter(|n| n.is_finite()),
    }
}

fn normalized_heading(value: Option<&Value>) -> Option<f64> {
    number_or_null(value).filter(|h| (0.0..=360.0).contains(h))
}

/// SOG 102.3 is AIS's "not available" code, not a reading.
fn normalized_speed(value: Option<&Value>) -> Option<f64> {
    number_or_null(value).filter(|s| *s >= 0.0 && *s < 102.3)
}

/// COG 360 is AIS's "not available" code.
fn normalized_course(value: Option<&Value>) -> Option<f64> {
    number_or_null(value).filter(|c| *c >= 0.0 && *c < 360.0)
}

fn iso(date: DateTime<Utc>) -> String {
    date.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn iso_now(now: i64) -> String {
    DateTime::from_timestamp_millis(now).map_or_else(String::new, iso)
}

/// `Date.parse` for the shapes AIS feeds and HTTP headers carry: ISO 8601
/// (with `T` or a space, any fraction length), a bare date, and RFC 2822 /
/// HTTP-date. Zone-less date-times are read as UTC.
pub fn parse_date(text: &str) -> Option<DateTime<Utc>> {
    let text = text.trim();
    if let Ok(date) = DateTime::parse_from_rfc3339(text) {
        return Some(date.with_timezone(&Utc));
    }
    if let Ok(date) = DateTime::parse_from_rfc2822(text) {
        return Some(date.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Some(naive.and_utc());
        }
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|naive| naive.and_utc())
}

fn normalized_ais_date(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let text = string_value(value);
    if text.is_empty() {
        return None;
    }
    let normalized = text.replacen(" +0000 UTC", "Z", 1).replacen(" UTC", "Z", 1);
    parse_date(&normalized)
}

/// `normalizeAisTimestamp`: an ISO string with milliseconds, else now.
fn normalize_ais_timestamp(value: Option<&Value>, now: i64) -> String {
    normalized_ais_date(value).map_or_else(|| iso_now(now), iso)
}

/// `aisEpochSeconds`: the report's own time in epoch seconds, else now.
fn ais_epoch_seconds(value: Option<&Value>, now: i64) -> i64 {
    let ms = normalized_ais_date(value).map_or(now, |d| d.timestamp_millis());
    ms.div_euclid(1000)
}

/// Equirectangular approximation — plenty for 25 m thinning.
fn approx_meters_between(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lat = (lat2 - lat1) * 111_320.0;
    let d_lon =
        (lon2 - lon1) * 111_320.0 * (((lat1 + lat2) / 2.0) * (std::f64::consts::PI / 180.0)).cos();
    d_lat.hypot(d_lon)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_789_000_000_000;

    fn position_report(mmsi: &str, body: Value) -> Value {
        let mut message = json!({ "UserID": mmsi.parse::<i64>().unwrap() });
        for (k, v) in body.as_object().unwrap() {
            message[k] = v.clone();
        }
        json!({
            "MessageType": "PositionReport",
            "Message": { "PositionReport": message },
            "MetaData": {
                "MMSI": mmsi,
                "latitude": 53.40879,
                "longitude": 6.19741,
                "time_utc": "2026-09-17 20:00:00.000000000 +0000 UTC",
            },
        })
    }

    fn row_for(store: &AisStore, mmsi: &str) -> Value {
        store
            .rows_at(50_000, NOW)
            .into_iter()
            .find(|row| row["mmsi"] == mmsi)
            .unwrap()
    }

    #[test]
    fn unavailable_sog_and_cog_sentinels_are_unknown() {
        let store = AisStore::default();
        assert!(store.ingest_at(&position_report("244060365", json!({ "Sog": 102.3 })), NOW));
        assert_eq!(row_for(&store, "244060365")["speed"], Value::Null);
        assert!(store.ingest_at(&position_report("245382000", json!({ "Cog": 360 })), NOW));
        assert_eq!(row_for(&store, "245382000")["course"], Value::Null);
    }

    #[test]
    fn genuine_and_boundary_readings_survive() {
        let store = AisStore::default();
        store.ingest_at(
            &position_report("341260000", json!({ "Sog": 12.9, "Cog": 74.6 })),
            NOW,
        );
        let row = row_for(&store, "341260000");
        assert_eq!(row["speed"], 12.9);
        assert_eq!(row["course"], 74.6);
        store.ingest_at(
            &position_report("244374235", json!({ "Sog": 0, "Cog": 0 })),
            NOW,
        );
        let row = row_for(&store, "244374235");
        assert_eq!(row["speed"], 0);
        assert_eq!(row["course"], 0);
        store.ingest_at(
            &position_report("244060366", json!({ "Sog": 102.2, "Cog": 359.9 })),
            NOW,
        );
        let row = row_for(&store, "244060366");
        assert_eq!(row["speed"], 102.2);
        assert_eq!(row["course"], 359.9);
    }

    #[test]
    fn rows_carry_the_js_shape_and_normalized_time() {
        let store = AisStore::default();
        store.ingest_at(
            &position_report("244060365", json!({ "TrueHeading": 511 })),
            NOW,
        );
        let row = row_for(&store, "244060365");
        let keys: Vec<&String> = row.as_object().unwrap().keys().collect();
        assert_eq!(
            keys,
            [
                "lat",
                "lon",
                "name",
                "mmsi",
                "imo",
                "type",
                "destination",
                "speed",
                "course",
                "heading",
                "last_position_UTC",
                "last_position_epoch"
            ]
        );
        assert_eq!(row["name"], "MMSI 244060365");
        assert_eq!(row["heading"], Value::Null);
        assert_eq!(row["last_position_UTC"], "2026-09-17T20:00:00.000Z");
        assert_eq!(row["last_position_epoch"], 1_789_675_200);
        assert_eq!(row["lat"], 53.40879);
    }

    #[test]
    fn static_data_merges_into_the_live_row_and_counts_as_liveness() {
        let store = AisStore::default();
        store.ingest_at(&position_report("211000001", json!({})), NOW);
        let static_report = json!({
            "MessageType": "ShipStaticData",
            "MetaData": { "MMSI": 211000001, "ShipName": "  NORDIC STAR " },
            "Message": { "ShipStaticData": { "UserID": 211000001, "Type": 70, "Destination": "HAMBURG", "ImoNumber": 9123456 } },
        });
        assert!(store.ingest_at(&static_report, NOW));
        let row = row_for(&store, "211000001");
        assert_eq!(row["name"], "NORDIC STAR");
        assert_eq!(row["type"], "70");
        assert_eq!(row["destination"], "HAMBURG");
        assert_eq!(row["imo"], "9123456");
        // A later position report keeps the static fields.
        store.ingest_at(&position_report("211000001", json!({})), NOW + 1);
        let row = row_for(&store, "211000001");
        assert_eq!(row["name"], "NORDIC STAR");
        assert_eq!(row["destination"], "HAMBURG");
    }

    #[test]
    fn unrecognized_envelopes_are_not_liveness() {
        let store = AisStore::default();
        assert!(!store.ingest_at(&json!({ "MetaData": { "MMSI": "1" } }), NOW));
        assert!(!store.ingest_at(&json!({ "MessageType": "Nope", "Message": { "Nope": {} }, "MetaData": { "MMSI": "1" } }), NOW));
        assert!(store.rows_at(10, NOW).is_empty());
    }

    #[test]
    fn rows_are_newest_first_with_insertion_order_ties_and_expire() {
        let store = AisStore::default();
        store.ingest_at(&position_report("100000001", json!({})), NOW);
        store.ingest_at(&position_report("100000002", json!({})), NOW);
        store.ingest_at(&position_report("100000003", json!({})), NOW + 5);
        let order: Vec<Value> = store
            .rows_at(10, NOW + 5)
            .into_iter()
            .map(|r| r["mmsi"].clone())
            .collect();
        assert_eq!(order, ["100000003", "100000001", "100000002"]);
        assert_eq!(store.rows_at(1, NOW + 5).len(), 1);
        assert_eq!(store.rows_at(10, NOW + AISSTREAM_STALE_MS + 1).len(), 1);
        // Pruning on the next ingest drops the stale rows outright.
        store.ingest_at(
            &position_report("100000004", json!({})),
            NOW + AISSTREAM_STALE_MS + 1,
        );
        assert_eq!(store.inner.lock().vessels.len(), 2);
    }

    fn fix(mmsi: &str, lat: f64, lon: f64, time: &str) -> Value {
        json!({
            "MessageType": "PositionReport",
            "Message": { "PositionReport": { "UserID": 1 } },
            "MetaData": { "MMSI": mmsi, "latitude": lat, "longitude": lon, "time_utc": time },
        })
    }

    #[test]
    fn tracks_allocate_lazily_and_thin_by_time_and_distance() {
        let store = AisStore::default();
        store.ingest_at(
            &fix("123456789", 10.0, 20.0, "2026-09-17 20:00:00 +0000 UTC"),
            NOW,
        );
        assert!(
            store.track("123456789").is_empty(),
            "single fix stays pending"
        );
        // Too soon.
        store.ingest_at(
            &fix("123456789", 10.01, 20.0, "2026-09-17 20:00:10 +0000 UTC"),
            NOW,
        );
        assert!(store.track("123456789").is_empty());
        // Too close.
        store.ingest_at(
            &fix("123456789", 10.0001, 20.0, "2026-09-17 20:01:00 +0000 UTC"),
            NOW,
        );
        assert!(store.track("123456789").is_empty());
        store.ingest_at(
            &fix("123456789", 10.01, 20.0, "2026-09-17 20:01:00 +0000 UTC"),
            NOW,
        );
        let track = store.track("123456789");
        assert_eq!(track.len(), 2);
        assert_eq!(track[0]["t"], 1_789_675_200);
        assert_eq!(track[1]["t"], 1_789_675_260);
        assert_eq!(track[0]["lat"], f64::from(10.0_f32));
        assert_eq!(track[1]["lat"], f64::from(10.01_f32));
        for minute in 2..80 {
            let time = format!(
                "2026-09-17 {:02}:{:02}:00 +0000 UTC",
                20 + minute / 60,
                minute % 60
            );
            store.ingest_at(
                &fix("123456789", 10.0 + 0.01 * minute as f64, 20.0, &time),
                NOW,
            );
        }
        let track = store.track("123456789");
        assert_eq!(track.len(), AIS_TRACK_SAMPLES);
        assert!(
            track
                .windows(2)
                .all(|w| w[0]["t"].as_u64() < w[1]["t"].as_u64())
        );
    }

    #[test]
    fn timestamps_normalize_like_the_js_store() {
        let now = NOW;
        let v = |s: &str| Value::String(s.into());
        assert_eq!(
            normalize_ais_timestamp(Some(&v("2022-12-29 18:22:32.318353 +0000 UTC")), now),
            "2022-12-29T18:22:32.318Z"
        );
        assert_eq!(
            normalize_ais_timestamp(Some(&v("2022-12-29T18:22:32Z")), now),
            "2022-12-29T18:22:32.000Z"
        );
        assert_eq!(
            normalize_ais_timestamp(Some(&v("garbage")), now),
            iso_now(now)
        );
        assert_eq!(normalize_ais_timestamp(None, now), iso_now(now));
        assert_eq!(ais_epoch_seconds(None, now), now / 1000);
    }
}
