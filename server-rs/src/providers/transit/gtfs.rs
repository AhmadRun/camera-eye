//! Minimal, bounded GTFS-Realtime VehiclePosition decoder (port of
//! `src/data/gtfsRealtime.js`). Field numbers are the contract; unknown
//! fields are skipped. Hand-rolled rather than generated so the entity cap
//! skips excess entities unread and over-long strings refuse the record,
//! exactly as the JS reader does.

use std::collections::HashMap;

/// Hard ceiling on decoded entities.
pub const GTFS_MAX_ENTITIES: usize = 50_000;
/// Longest accepted string field, in UTF-16 units like `string.length`.
pub const GTFS_MAX_STRING_CHARS: usize = 256;
/// `FeedHeader.Incrementality.FULL_DATASET`.
pub const GTFS_INCREMENTALITY_FULL_DATASET: f64 = 0.0;

const VEHICLE_STOP_STATUS: [&str; 3] = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"];
const OCCUPANCY_STATUS: [&str; 9] = [
    "EMPTY",
    "MANY_SEATS_AVAILABLE",
    "FEW_SEATS_AVAILABLE",
    "STANDING_ROOM_ONLY",
    "CRUSHED_STANDING_ROOM_ONLY",
    "FULL",
    "NOT_ACCEPTING_PASSENGERS",
    "NO_DATA_AVAILABLE",
    "NOT_BOARDABLE",
];

/// Malformed wire data (what `pbf` throws on).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError;

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut value: u64 = 0;
        for shift in (0..70).step_by(7) {
            let byte = *self.buf.get(self.pos).ok_or(DecodeError)?;
            self.pos += 1;
            if shift < 64 {
                value |= u64::from(byte & 0x7f) << shift;
            }
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError)
    }

    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = usize::try_from(self.varint()?).map_err(|_| DecodeError)?;
        let end = self.pos.checked_add(len).ok_or(DecodeError)?;
        let out = self.buf.get(self.pos..end).ok_or(DecodeError)?;
        self.pos = end;
        Ok(out)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let out: [u8; N] = self
            .buf
            .get(self.pos..self.pos + N)
            .ok_or(DecodeError)?
            .try_into()
            .map_err(|_| DecodeError)?;
        self.pos += N;
        Ok(out)
    }

    fn skip(&mut self, wire: u64) -> Result<(), DecodeError> {
        match wire {
            0 => {
                self.varint()?;
            }
            1 => {
                self.fixed::<8>()?;
            }
            2 => {
                self.bytes()?;
            }
            5 => {
                self.fixed::<4>()?;
            }
            _ => return Err(DecodeError),
        }
        Ok(())
    }

    /// `readFields`: call `field(tag, wire, reader)` for each field; a
    /// handler that reads nothing has its field skipped.
    fn fields(
        &mut self,
        mut field: impl FnMut(u64, u64, &mut Reader<'a>) -> Result<bool, DecodeError>,
    ) -> Result<(), DecodeError> {
        while self.pos < self.buf.len() {
            let key = self.varint()?;
            let (tag, wire) = (key >> 3, key & 7);
            if !field(tag, wire, self)? {
                self.skip(wire)?;
            }
        }
        Ok(())
    }

    fn message(&mut self) -> Result<Reader<'a>, DecodeError> {
        Ok(Reader {
            buf: self.bytes()?,
            pos: 0,
        })
    }

    /// A bounded string: `Ok(None)` (and `oversize`) past the cap.
    fn string(&mut self, oversize: &mut bool) -> Result<Option<String>, DecodeError> {
        let text = String::from_utf8_lossy(self.bytes()?).into_owned();
        if crate::providers::mapdata_support::utf16_len(&text) <= GTFS_MAX_STRING_CHARS {
            Ok(Some(text))
        } else {
            *oversize = true;
            Ok(None)
        }
    }

    fn float(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from(f32::from_le_bytes(self.fixed::<4>()?)))
    }
}

#[derive(Default, Debug)]
pub struct Header {
    pub version: Option<String>,
    pub incrementality: Option<f64>,
    pub timestamp: Option<f64>,
}

#[derive(Default, Debug)]
pub struct Trip {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<f64>,
    pub oversize: bool,
}

#[derive(Default, Debug)]
pub struct Position {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub bearing: Option<f64>,
    pub speed: Option<f64>,
}

#[derive(Default, Debug)]
pub struct Descriptor {
    pub id: Option<String>,
    pub label: Option<String>,
    pub oversize: bool,
}

#[derive(Default, Debug)]
pub struct VehiclePosition {
    pub trip: Option<Trip>,
    pub position: Option<Position>,
    pub current_status: Option<f64>,
    pub timestamp: Option<f64>,
    pub stop_id: Option<String>,
    pub vehicle: Option<Descriptor>,
    pub occupancy_status: Option<f64>,
    pub oversize: bool,
}

#[derive(Default, Debug)]
pub struct Entity {
    pub id: Option<String>,
    pub is_deleted: bool,
    pub vehicle: Option<VehiclePosition>,
    pub oversize: bool,
}

#[derive(Default, Debug)]
pub struct FeedMessage {
    pub header: Header,
    pub entities: Vec<Entity>,
    pub truncated: bool,
}

fn read_trip(mut r: Reader<'_>) -> Result<Trip, DecodeError> {
    let mut trip = Trip::default();
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 2) => {
                trip.trip_id = r.string(&mut trip.oversize)?;
                true
            }
            (2 | 3, 2) => {
                r.string(&mut trip.oversize)?;
                true
            }
            (5, 2) => {
                trip.route_id = r.string(&mut trip.oversize)?;
                true
            }
            (6, 0) => {
                trip.direction_id = Some(r.varint()? as f64);
                true
            }
            _ => false,
        })
    })?;
    Ok(trip)
}

fn read_position(mut r: Reader<'_>) -> Result<Position, DecodeError> {
    let mut position = Position::default();
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 5) => {
                position.latitude = Some(r.float()?);
                true
            }
            (2, 5) => {
                position.longitude = Some(r.float()?);
                true
            }
            (3, 5) => {
                position.bearing = Some(r.float()?);
                true
            }
            (5, 5) => {
                position.speed = Some(r.float()?);
                true
            }
            _ => false,
        })
    })?;
    Ok(position)
}

fn read_descriptor(mut r: Reader<'_>) -> Result<Descriptor, DecodeError> {
    let mut descriptor = Descriptor::default();
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 2) => {
                descriptor.id = r.string(&mut descriptor.oversize)?;
                true
            }
            (2, 2) => {
                descriptor.label = r.string(&mut descriptor.oversize)?;
                true
            }
            (3, 2) => {
                r.string(&mut descriptor.oversize)?;
                true
            }
            _ => false,
        })
    })?;
    Ok(descriptor)
}

fn read_vehicle(mut r: Reader<'_>) -> Result<VehiclePosition, DecodeError> {
    let mut v = VehiclePosition::default();
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 2) => {
                v.trip = Some(read_trip(r.message()?)?);
                true
            }
            (2, 2) => {
                v.position = Some(read_position(r.message()?)?);
                true
            }
            (4, 0) => {
                v.current_status = Some(r.varint()? as f64);
                true
            }
            (5, 0) => {
                v.timestamp = Some(r.varint()? as f64);
                true
            }
            (7, 2) => {
                v.stop_id = r.string(&mut v.oversize)?;
                true
            }
            (8, 2) => {
                v.vehicle = Some(read_descriptor(r.message()?)?);
                true
            }
            (9, 0) => {
                v.occupancy_status = Some(r.varint()? as f64);
                true
            }
            _ => false,
        })
    })?;
    Ok(v)
}

fn read_entity(mut r: Reader<'_>) -> Result<Entity, DecodeError> {
    let mut entity = Entity::default();
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 2) => {
                entity.id = r.string(&mut entity.oversize)?;
                true
            }
            (2, 0) => {
                entity.is_deleted = r.varint()? != 0;
                true
            }
            (4, 2) => {
                entity.vehicle = Some(read_vehicle(r.message()?)?);
                true
            }
            _ => false,
        })
    })?;
    Ok(entity)
}

fn read_header(mut r: Reader<'_>) -> Result<Header, DecodeError> {
    let mut header = Header::default();
    let mut oversize = false;
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 2) => {
                header.version = r.string(&mut oversize)?;
                true
            }
            (2, 0) => {
                header.incrementality = Some(r.varint()? as f64);
                true
            }
            (3, 0) => {
                header.timestamp = Some(r.varint()? as f64);
                true
            }
            _ => false,
        })
    })?;
    Ok(header)
}

/// `decodeFeedMessage`.
pub fn decode_feed_message(bytes: &[u8]) -> Result<FeedMessage, DecodeError> {
    let mut message = FeedMessage::default();
    let mut r = Reader { buf: bytes, pos: 0 };
    r.fields(|tag, wire, r| {
        Ok(match (tag, wire) {
            (1, 2) => {
                message.header = read_header(r.message()?)?;
                true
            }
            (2, 2) => {
                if message.entities.len() >= GTFS_MAX_ENTITIES {
                    message.truncated = true;
                    false
                } else {
                    message.entities.push(read_entity(r.message()?)?);
                    true
                }
            }
            _ => false,
        })
    })?;
    Ok(message)
}

/// One normalized vehicle record.
#[derive(Clone, Debug, PartialEq)]
pub struct Vehicle {
    pub id: String,
    pub lat: f64,
    pub lon: f64,
    pub bearing: Option<f64>,
    pub speed_mps: Option<f64>,
    pub timestamp: Option<f64>,
    pub route_id: Option<String>,
    pub trip_id: Option<String>,
    pub direction_id: Option<f64>,
    pub label: Option<String>,
    pub stop_id: Option<String>,
    pub status: Option<&'static str>,
    pub occupancy: Option<&'static str>,
    /// Set by `repairVehicleTimestamps`.
    pub timestamp_source: Option<&'static str>,
}

/// `isPlausibleVehiclePosition`.
pub fn is_plausible_vehicle_position(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && lat.abs() <= 90.0
        && lon.abs() <= 180.0
        && !(lat.abs() < 1e-6 && lon.abs() < 1e-6)
}

fn non_empty(value: Option<&String>) -> Option<String> {
    let text = crate::providers::mapdata_support::js_trim(value?);
    (!text.is_empty()).then(|| text.to_string())
}

fn label(table: &[&'static str], code: Option<f64>) -> Option<&'static str> {
    let code = code?;
    (code.fract() == 0.0 && code >= 0.0)
        .then(|| table.get(code as usize).copied())
        .flatten()
}

/// `normalizeVehicleEntity`.
pub fn normalize_vehicle_entity(entity: &Entity) -> Option<Vehicle> {
    if entity.is_deleted {
        return None;
    }
    let vehicle = entity.vehicle.as_ref()?;
    let position = vehicle.position.as_ref()?;
    if entity.oversize
        || vehicle.oversize
        || vehicle.trip.as_ref().is_some_and(|t| t.oversize)
        || vehicle.vehicle.as_ref().is_some_and(|d| d.oversize)
    {
        return None;
    }
    let lat = position.latitude.filter(|v| v.is_finite())?;
    let lon = position.longitude.filter(|v| v.is_finite())?;
    if !is_plausible_vehicle_position(lat, lon) {
        return None;
    }
    let id = non_empty(vehicle.vehicle.as_ref().and_then(|d| d.id.as_ref()))
        .or_else(|| non_empty(entity.id.as_ref()))?;
    let round6 =
        |v: f64| crate::common::js::number(&crate::providers::mapdata_support::to_fixed(v, 6));
    let trip = vehicle.trip.as_ref();
    Some(Vehicle {
        id,
        lat: round6(lat),
        lon: round6(lon),
        bearing: position
            .bearing
            .filter(|b| b.is_finite())
            .map(|b| ((b % 360.0) + 360.0) % 360.0),
        speed_mps: position.speed.filter(|s| s.is_finite() && *s >= 0.0),
        timestamp: vehicle.timestamp.filter(|t| *t > 0.0),
        route_id: non_empty(trip.and_then(|t| t.route_id.as_ref())),
        trip_id: non_empty(trip.and_then(|t| t.trip_id.as_ref())),
        direction_id: trip.and_then(|t| t.direction_id),
        label: non_empty(vehicle.vehicle.as_ref().and_then(|d| d.label.as_ref())),
        stop_id: non_empty(vehicle.stop_id.as_ref()),
        status: label(&VEHICLE_STOP_STATUS, vehicle.current_status),
        occupancy: label(&OCCUPANCY_STATUS, vehicle.occupancy_status),
        timestamp_source: None,
    })
}

/// `decodeVehiclePositions`'s result.
#[derive(Debug)]
pub struct DecodedPositions {
    pub version: Option<String>,
    pub timestamp: Option<f64>,
    pub incrementality: f64,
    pub entity_count: usize,
    pub truncated: bool,
    pub vehicles: Vec<Vehicle>,
}

/// `decodeVehiclePositions`: duplicate ids keep the newest timestamp.
pub fn decode_vehicle_positions(bytes: &[u8]) -> Result<DecodedPositions, DecodeError> {
    let message = decode_feed_message(bytes)?;
    let mut order: Vec<Vehicle> = Vec::new();
    let mut by_id: HashMap<String, usize> = HashMap::new();
    for entity in &message.entities {
        let Some(record) = normalize_vehicle_entity(entity) else {
            continue;
        };
        match by_id.get(&record.id) {
            Some(&index) => {
                if record.timestamp.unwrap_or(0.0) >= order[index].timestamp.unwrap_or(0.0) {
                    order[index] = record;
                }
            }
            None => {
                by_id.insert(record.id.clone(), order.len());
                order.push(record);
            }
        }
    }
    Ok(DecodedPositions {
        version: non_empty(message.header.version.as_ref()),
        timestamp: message.header.timestamp.filter(|t| *t > 0.0),
        incrementality: message
            .header
            .incrementality
            .unwrap_or(GTFS_INCREMENTALITY_FULL_DATASET),
        entity_count: message.entities.len(),
        truncated: message.truncated,
        vehicles: order,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A minimal protobuf writer for fixtures (the `PbfWriter` subset).
    #[derive(Default)]
    pub struct Writer(pub Vec<u8>);

    impl Writer {
        fn varint(&mut self, mut value: u64) {
            loop {
                let byte = (value & 0x7f) as u8;
                value >>= 7;
                if value == 0 {
                    self.0.push(byte);
                    break;
                }
                self.0.push(byte | 0x80);
            }
        }
        fn key(&mut self, tag: u64, wire: u64) {
            self.varint((tag << 3) | wire);
        }
        pub fn varint_field(&mut self, tag: u64, value: u64) {
            self.key(tag, 0);
            self.varint(value);
        }
        pub fn bytes_field(&mut self, tag: u64, bytes: &[u8]) {
            self.key(tag, 2);
            self.varint(bytes.len() as u64);
            self.0.extend_from_slice(bytes);
        }
        pub fn string_field(&mut self, tag: u64, text: &str) {
            self.bytes_field(tag, text.as_bytes());
        }
        pub fn float_field(&mut self, tag: u64, value: f32) {
            self.key(tag, 5);
            self.0.extend_from_slice(&value.to_le_bytes());
        }
        pub fn double_field(&mut self, tag: u64, value: f64) {
            self.key(tag, 1);
            self.0.extend_from_slice(&value.to_le_bytes());
        }
        pub fn embed(&mut self, tag: u64, build: impl FnOnce(&mut Writer)) {
            let mut inner = Writer::default();
            build(&mut inner);
            self.bytes_field(tag, &inner.0);
        }
    }

    #[derive(Default, Clone)]
    pub struct Fixture {
        pub entity_id: Option<String>,
        pub vehicle_id: Option<String>,
        pub label: Option<String>,
        pub lat: Option<f32>,
        pub lon: f32,
        pub bearing: Option<f32>,
        pub speed: Option<f32>,
        pub timestamp: Option<u64>,
        pub route_id: Option<String>,
        pub trip_id: Option<String>,
        pub direction_id: Option<u64>,
        pub stop_id: Option<String>,
        pub status: Option<u64>,
        pub occupancy: Option<u64>,
        pub is_deleted: bool,
        pub extra_unknown_fields: bool,
    }

    pub fn write_entity(writer: &mut Writer, f: &Fixture) {
        writer.embed(2, |entity| {
            if let Some(id) = &f.entity_id {
                entity.string_field(1, id);
            }
            if f.is_deleted {
                entity.varint_field(2, 1);
            }
            entity.embed(4, |vehicle| {
                if f.route_id.is_some() || f.trip_id.is_some() || f.direction_id.is_some() {
                    vehicle.embed(1, |trip| {
                        if let Some(t) = &f.trip_id {
                            trip.string_field(1, t);
                        }
                        if let Some(r) = &f.route_id {
                            trip.string_field(5, r);
                        }
                        if let Some(d) = f.direction_id {
                            trip.varint_field(6, d);
                        }
                    });
                }
                if let Some(lat) = f.lat {
                    vehicle.embed(2, |position| {
                        position.float_field(1, lat);
                        position.float_field(2, f.lon);
                        if let Some(b) = f.bearing {
                            position.float_field(3, b);
                        }
                        if let Some(s) = f.speed {
                            position.float_field(5, s);
                        }
                        if f.extra_unknown_fields {
                            position.double_field(4, 123_456.5);
                        }
                    });
                }
                if let Some(s) = f.status {
                    vehicle.varint_field(4, s);
                }
                if let Some(t) = f.timestamp {
                    vehicle.varint_field(5, t);
                }
                if let Some(s) = &f.stop_id {
                    vehicle.string_field(7, s);
                }
                if f.vehicle_id.is_some() || f.label.is_some() {
                    vehicle.embed(8, |d| {
                        if let Some(id) = &f.vehicle_id {
                            d.string_field(1, id);
                        }
                        if let Some(l) = &f.label {
                            d.string_field(2, l);
                        }
                    });
                }
                if let Some(o) = f.occupancy {
                    vehicle.varint_field(9, o);
                }
                if f.extra_unknown_fields {
                    vehicle.string_field(1001, "operator-extension");
                }
            });
        });
    }

    pub fn encode_feed(
        fixtures: &[Fixture],
        incrementality: Option<u64>,
        timestamp: Option<u64>,
    ) -> Vec<u8> {
        let mut writer = Writer::default();
        writer.embed(1, |header| {
            header.string_field(1, "2.0");
            if let Some(i) = incrementality {
                header.varint_field(2, i);
            }
            if let Some(t) = timestamp {
                header.varint_field(3, t);
            }
        });
        for f in fixtures {
            write_entity(&mut writer, f);
        }
        writer.0
    }

    fn fx(entity: &str, lat: f32, lon: f32) -> Fixture {
        Fixture {
            entity_id: Some(entity.into()),
            lat: Some(lat),
            lon,
            ..Default::default()
        }
    }

    const TS: u64 = 1_788_936_000;

    #[test]
    fn decodes_every_rendered_field() {
        let bytes = encode_feed(
            &[Fixture {
                entity_id: Some("e1".into()),
                vehicle_id: Some("bus-17".into()),
                label: Some("1557".into()),
                lat: Some(44.927_55),
                lon: -93.386_71,
                bearing: Some(248.0),
                speed: Some(11.2),
                timestamp: Some(1_788_936_945),
                route_id: Some("17".into()),
                trip_id: Some("1134773".into()),
                direction_id: Some(0),
                stop_id: Some("57458".into()),
                status: Some(1),
                occupancy: Some(2),
                ..Default::default()
            }],
            Some(0),
            Some(TS),
        );
        let snapshot = decode_vehicle_positions(&bytes).unwrap();
        assert_eq!(snapshot.version.as_deref(), Some("2.0"));
        assert_eq!(snapshot.timestamp, Some(TS as f64));
        assert_eq!(snapshot.entity_count, 1);
        let v = &snapshot.vehicles[0];
        assert_eq!(v.id, "bus-17");
        assert_eq!(v.label.as_deref(), Some("1557"));
        assert!((v.lat - 44.927551).abs() < 1e-5);
        assert!((v.lon + 93.386711).abs() < 1e-5);
        assert_eq!(v.bearing, Some(248.0));
        assert!((v.speed_mps.unwrap() - 11.2).abs() < 1e-5);
        assert_eq!(v.timestamp, Some(1_788_936_945.0));
        assert_eq!(v.route_id.as_deref(), Some("17"));
        assert_eq!(v.trip_id.as_deref(), Some("1134773"));
        assert_eq!(v.direction_id, Some(0.0));
        assert_eq!(v.stop_id.as_deref(), Some("57458"));
        assert_eq!(v.status, Some("STOPPED_AT"));
        assert_eq!(v.occupancy, Some("FEW_SEATS_AVAILABLE"));
    }

    #[test]
    fn unknown_fields_skipped_and_unusable_entities_dropped() {
        let bytes = encode_feed(
            &[Fixture {
                extra_unknown_fields: true,
                ..fx("x", 51.3, 5.33)
            }],
            Some(0),
            Some(TS),
        );
        let snapshot = decode_vehicle_positions(&bytes).unwrap();
        assert_eq!(snapshot.vehicles[0].id, "x");
        assert!((snapshot.vehicles[0].lat - 51.3).abs() < 1e-5);

        let bytes = encode_feed(
            &[
                Fixture {
                    entity_id: Some("no-position".into()),
                    ..Default::default()
                },
                Fixture {
                    is_deleted: true,
                    ..fx("deleted", 1.0, 1.0)
                },
                fx("null-island", 0.0, 0.0),
                fx("out-of-range", 91.0, 10.0),
                fx("nan", f32::NAN, 10.0),
                fx("keeper", 60.17, 24.94),
                Fixture {
                    entity_id: None,
                    ..fx("", 42.3, -71.1)
                },
            ],
            Some(0),
            Some(TS),
        );
        let snapshot = decode_vehicle_positions(&bytes).unwrap();
        assert_eq!(snapshot.entity_count, 7);
        let ids: Vec<&str> = snapshot.vehicles.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, ["keeper"]);
    }

    #[test]
    fn duplicates_keep_newest_and_bearing_normalizes() {
        let dup = |e: &str, lat: f32, ts: u64| Fixture {
            vehicle_id: Some("v1".into()),
            timestamp: Some(ts),
            ..fx(e, lat, -71.1)
        };
        let snapshot = decode_vehicle_positions(&encode_feed(
            &[
                dup("a", 42.3, 100),
                dup("b", 42.31, 200),
                dup("c", 42.32, 150),
            ],
            Some(0),
            Some(TS),
        ))
        .unwrap();
        assert_eq!(snapshot.vehicles.len(), 1);
        assert_eq!(snapshot.vehicles[0].timestamp, Some(200.0));
        assert!((snapshot.vehicles[0].lat - 42.31).abs() < 1e-5);

        let snapshot = decode_vehicle_positions(&encode_feed(
            &[
                Fixture {
                    bearing: Some(-90.0),
                    speed: Some(-1.0),
                    ..fx("neg", 30.2, -97.7)
                },
                Fixture {
                    bearing: Some(450.0),
                    ..fx("wrap", 30.2, -97.7)
                },
            ],
            Some(0),
            Some(TS),
        ))
        .unwrap();
        assert_eq!(snapshot.vehicles[0].bearing, Some(270.0));
        assert_eq!(snapshot.vehicles[0].speed_mps, None);
        assert_eq!(snapshot.vehicles[1].bearing, Some(90.0));
    }

    #[test]
    fn missing_header_and_empty_feeds() {
        let mut writer = Writer::default();
        write_entity(&mut writer, &fx("solo", 10.0, 10.0));
        let snapshot = decode_vehicle_positions(&writer.0).unwrap();
        assert_eq!(snapshot.version, None);
        assert_eq!(snapshot.timestamp, None);
        assert_eq!(snapshot.vehicles.len(), 1);
        let empty = decode_feed_message(&[]).unwrap();
        assert!(empty.entities.is_empty() && !empty.truncated);
        assert_eq!(
            decode_vehicle_positions(&encode_feed(&[], None, Some(TS)))
                .unwrap()
                .incrementality,
            0.0
        );
        assert_eq!(
            decode_vehicle_positions(&encode_feed(&[], Some(1), Some(TS)))
                .unwrap()
                .incrementality,
            1.0
        );
        assert!(!is_plausible_vehicle_position(0.0, 0.0));
        assert!(!is_plausible_vehicle_position(-90.0001, 0.0));
        assert!(!is_plausible_vehicle_position(1.0, f64::INFINITY));
        assert!(is_plausible_vehicle_position(45.0, 90.0));
        let unknown_status = Entity {
            id: Some("v".into()),
            vehicle: Some(VehiclePosition {
                position: Some(Position {
                    latitude: Some(1.0),
                    longitude: Some(2.0),
                    ..Default::default()
                }),
                current_status: Some(99.0),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            normalize_vehicle_entity(&unknown_status).unwrap().status,
            None
        );
    }

    #[test]
    fn decode_is_bounded_by_entities_and_strings() {
        let fixtures: Vec<Fixture> = (0..GTFS_MAX_ENTITIES + 25)
            .map(|i| Fixture {
                vehicle_id: Some(format!("v{i}")),
                timestamp: Some(TS),
                ..fx(&format!("e{i}"), 42.0 + i as f32 * 1e-6, -71.0)
            })
            .collect();
        let decoded = decode_vehicle_positions(&encode_feed(&fixtures, Some(0), Some(TS))).unwrap();
        assert_eq!(decoded.entity_count, GTFS_MAX_ENTITIES);
        assert!(decoded.truncated);
        assert!(
            !decoded
                .vehicles
                .iter()
                .any(|v| v.id == format!("v{GTFS_MAX_ENTITIES}"))
        );
        assert!(
            !decode_vehicle_positions(&encode_feed(&fixtures[..3], Some(0), Some(TS)))
                .unwrap()
                .truncated
        );

        let long = "x".repeat(GTFS_MAX_STRING_CHARS + 1);
        let decoded = decode_vehicle_positions(&encode_feed(
            &[
                Fixture {
                    vehicle_id: Some("good".into()),
                    timestamp: Some(TS),
                    ..fx("ok", 42.1, -71.1)
                },
                Fixture {
                    vehicle_id: Some(long.clone()),
                    timestamp: Some(TS),
                    ..fx("bad", 42.2, -71.2)
                },
                Fixture {
                    vehicle_id: Some("v3".into()),
                    route_id: Some(long.clone()),
                    timestamp: Some(TS),
                    ..fx("bad-route", 42.3, -71.3)
                },
            ],
            Some(0),
            Some(TS),
        ))
        .unwrap();
        assert_eq!(decoded.entity_count, 3);
        let ids: Vec<&str> = decoded.vehicles.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, ["good"]);
        let at_cap = "y".repeat(GTFS_MAX_STRING_CHARS);
        let kept = decode_vehicle_positions(&encode_feed(
            &[Fixture {
                vehicle_id: Some(at_cap.clone()),
                timestamp: Some(1),
                ..fx("e", 42.0, -71.0)
            }],
            Some(0),
            Some(TS),
        ))
        .unwrap();
        assert_eq!(kept.vehicles[0].id, at_cap);

        let mut writer = Writer::default();
        writer.embed(1, |h| {
            h.string_field(1, &"v".repeat(GTFS_MAX_STRING_CHARS + 1));
            h.varint_field(2, 0);
            h.varint_field(3, TS);
        });
        let decoded = decode_vehicle_positions(&writer.0).unwrap();
        assert_eq!(decoded.version, None);
        assert_eq!(decoded.timestamp, Some(TS as f64));
    }
}
