//! Mapbox Vector Tile decoding: the `pbf` 5 reader and
//! `@mapbox/vector-tile` 3 (`VectorTile`, `VectorTileLayer`,
//! `VectorTileFeature#toGeoJSON`, `classifyRings`).
//!
//! Reading past the end of the buffer is an error here; the JS reader
//! reads `undefined` and carries on with NaN.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::mcp::js;

/// A decoding failure (`Error` with this message).
#[derive(Debug, Clone, PartialEq)]
pub struct DecodeError(pub String);

type Result<T> = std::result::Result<T, DecodeError>;

fn truncated() -> DecodeError {
    DecodeError("Unexpected end of tile".into())
}

const PBF_VARINT: u32 = 0;
const PBF_FIXED64: u32 = 1;
const PBF_BYTES: u32 = 2;
const PBF_FIXED32: u32 = 5;
const TEXT_DECODER_MIN_LENGTH: usize = 12;

/// `PbfReader`.
pub struct Pbf<'a> {
    buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Pbf<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn byte(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(truncated)?;
        let bytes = self.buf.get(self.pos..end).ok_or_else(truncated)?;
        self.pos = end;
        Ok(bytes)
    }

    /// `readVarint(isSigned)`.
    pub fn read_varint(&mut self, signed: bool) -> Result<f64> {
        let b0 = self.byte()?;
        if b0 < 0x80 {
            return Ok(f64::from(b0));
        }
        let mut val: i32 = i32::from(b0 & 0x7f);
        for shift in [7, 14, 21] {
            let b = self.byte()?;
            val |= i32::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(f64::from(val));
            }
        }
        let b = *self.buf.get(self.pos).ok_or_else(truncated)?;
        val |= i32::from(b & 0x0f) << 28;
        self.read_varint_remainder(val, signed)
    }

    fn read_varint_remainder(&mut self, low: i32, signed: bool) -> Result<f64> {
        let to_num = |high: i32| {
            let low = f64::from(low as u32);
            if signed {
                f64::from(high) * 4_294_967_296.0 + low
            } else {
                f64::from(high as u32) * 4_294_967_296.0 + low
            }
        };
        let b = self.byte()?;
        let mut high: i32 = i32::from((b & 0x70) >> 4);
        if b < 0x80 {
            return Ok(to_num(high));
        }
        for shift in [3, 10, 17, 24] {
            let b = self.byte()?;
            high |= i32::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(to_num(high));
            }
        }
        let b = self.byte()?;
        high |= i32::from(b & 0x01) << 31;
        if b < 0x80 {
            return Ok(to_num(high));
        }
        Err(DecodeError("Expected varint not more than 10 bytes".into()))
    }

    /// `readSVarint()`: zigzag.
    pub fn read_svarint(&mut self) -> Result<f64> {
        let num = self.read_varint(false)?;
        Ok(if num % 2.0 == 1.0 {
            (num + 1.0) / -2.0
        } else {
            num / 2.0
        })
    }

    pub fn read_float(&mut self) -> Result<f64> {
        let bytes = self.take(4)?;
        Ok(f64::from(f32::from_le_bytes(
            bytes.try_into().expect("4 bytes"),
        )))
    }

    pub fn read_double(&mut self) -> Result<f64> {
        let bytes = self.take(8)?;
        Ok(f64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    fn read_length(&mut self) -> Result<usize> {
        let len = self.read_varint(false)?;
        Ok(len as usize)
    }

    /// `readString()`.
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_length()?;
        let bytes = self.take(len)?;
        Ok(if bytes.len() >= TEXT_DECODER_MIN_LENGTH {
            let bytes = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
            String::from_utf8_lossy(bytes).into_owned()
        } else {
            read_utf8(bytes)
        })
    }

    /// `skip(val)`.
    pub fn skip(&mut self, val: f64) -> Result<()> {
        let kind = (val as i64 as u32) & 0x7;
        match kind {
            PBF_VARINT => while self.byte()? > 0x7f {},
            PBF_BYTES => {
                let len = self.read_length()?;
                self.take(len)?;
            }
            PBF_FIXED32 => {
                self.take(4)?;
            }
            PBF_FIXED64 => {
                self.take(8)?;
            }
            other => return Err(DecodeError(format!("Unimplemented type: {other}"))),
        }
        Ok(())
    }

    /// The end position of a length-delimited message starting here.
    fn message_end(&mut self) -> Result<usize> {
        let len = self.read_length()?;
        Ok(self.pos.saturating_add(len))
    }
}

/// `readUtf8`: pbf's decoder for short strings.
fn read_utf8(buf: &[u8]) -> String {
    let mut out = String::new();
    let end = buf.len();
    let mut i = 0;
    while i < end {
        let b0 = u32::from(buf[i]);
        let mut per = if b0 > 0xef {
            4
        } else if b0 > 0xdf {
            3
        } else if b0 > 0xbf {
            2
        } else {
            1
        };
        if i + per > end {
            break;
        }
        let cont = |k: usize| u32::from(buf[i + k]);
        let mut c: Option<u32> = None;
        match per {
            1 => {
                if b0 < 0x80 {
                    c = Some(b0);
                }
            }
            2 => {
                let b1 = cont(1);
                if b1 & 0xc0 == 0x80 {
                    let v = (b0 & 0x1f) << 6 | (b1 & 0x3f);
                    if v > 0x7f {
                        c = Some(v);
                    }
                }
            }
            3 => {
                let (b1, b2) = (cont(1), cont(2));
                if b1 & 0xc0 == 0x80 && b2 & 0xc0 == 0x80 {
                    let v = (b0 & 0xf) << 12 | (b1 & 0x3f) << 6 | (b2 & 0x3f);
                    if v > 0x7ff && !(0xd800..=0xdfff).contains(&v) {
                        c = Some(v);
                    }
                }
            }
            _ => {
                let (b1, b2, b3) = (cont(1), cont(2), cont(3));
                if b1 & 0xc0 == 0x80 && b2 & 0xc0 == 0x80 && b3 & 0xc0 == 0x80 {
                    let v = (b0 & 0xf) << 18 | (b1 & 0x3f) << 12 | (b2 & 0x3f) << 6 | (b3 & 0x3f);
                    if v > 0xffff && v < 0x110000 {
                        c = Some(v);
                    }
                }
            }
        }
        let c = match c {
            Some(c) => c,
            None => {
                per = 1;
                0xfffd
            }
        };
        out.push(char::from_u32(c).unwrap_or('\u{fffd}'));
        i += per;
    }
    out
}

/// A `[lon, lat]` pair.
pub type Coord = [f64; 2];

/// A GeoJSON geometry as `toGeoJSON` builds it.
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point(Coord),
    MultiPoint(Vec<Coord>),
    LineString(Vec<Coord>),
    MultiLineString(Vec<Vec<Coord>>),
    Polygon(Vec<Vec<Coord>>),
    MultiPolygon(Vec<Vec<Vec<Coord>>>),
}

impl Geometry {
    pub fn type_name(&self) -> &'static str {
        match self {
            Geometry::Point(_) => "Point",
            Geometry::MultiPoint(_) => "MultiPoint",
            Geometry::LineString(_) => "LineString",
            Geometry::MultiLineString(_) => "MultiLineString",
            Geometry::Polygon(_) => "Polygon",
            Geometry::MultiPolygon(_) => "MultiPolygon",
        }
    }
}

/// A decoded feature (`toGeoJSON(x, y, z)`).
#[derive(Debug, Clone, PartialEq)]
pub struct Feature {
    pub id: Option<f64>,
    pub geometry: Geometry,
    pub properties: Map<String, Value>,
}

/// `VectorTileLayer`.
pub struct Layer<'a> {
    pub name: String,
    pub extent: f64,
    pub version: f64,
    buf: &'a [u8],
    keys: Vec<String>,
    values: Vec<Value>,
    features: Vec<usize>,
}

/// A feature before projection (`VectorTileFeature`).
pub struct RawFeature<'a> {
    pub id: Option<f64>,
    pub kind: f64,
    pub properties: Map<String, Value>,
    pub extent: f64,
    buf: &'a [u8],
    geometry: Option<usize>,
}

fn read_value(pbf: &mut Pbf<'_>) -> Result<Value> {
    let mut value = Value::Null;
    let end = pbf.message_end()?;
    while pbf.pos < end {
        let tag = pbf.read_varint(false)?;
        value = match tag as i64 {
            10 => Value::String(pbf.read_string()?),
            21 => js::num(pbf.read_float()?),
            25 => js::num(pbf.read_double()?),
            32 => js::num(pbf.read_varint(true)?),
            40 => js::num(pbf.read_varint(false)?),
            48 => js::num(pbf.read_svarint()?),
            56 => Value::Bool(pbf.read_varint(false)? != 0.0),
            _ => {
                pbf.skip(tag)?;
                Value::Null
            }
        };
    }
    if value.is_null() {
        return Err(DecodeError("unknown feature value".into()));
    }
    Ok(value)
}

impl<'a> Layer<'a> {
    fn read(pbf: &mut Pbf<'a>, end: usize) -> Result<Self> {
        let mut layer = Layer {
            name: String::new(),
            extent: 4096.0,
            version: 1.0,
            buf: pbf.buf,
            keys: Vec::new(),
            values: Vec::new(),
            features: Vec::new(),
        };
        while pbf.pos < end {
            let tag = pbf.read_varint(false)?;
            match tag as i64 {
                10 => layer.name = pbf.read_string()?,
                18 => {
                    layer.features.push(pbf.pos);
                    pbf.skip(tag)?;
                }
                26 => layer.keys.push(pbf.read_string()?),
                34 => layer.values.push(read_value(pbf)?),
                40 => layer.extent = pbf.read_varint(false)?,
                120 => layer.version = pbf.read_varint(false)?,
                _ => pbf.skip(tag)?,
            }
        }
        Ok(layer)
    }

    /// `layer.length`.
    pub fn len(&self) -> usize {
        self.features.len()
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }

    /// `layer.feature(i)`.
    pub fn feature(&self, i: usize) -> Result<RawFeature<'a>> {
        let start = *self
            .features
            .get(i)
            .ok_or_else(|| DecodeError("feature index out of bounds".into()))?;
        let mut pbf = Pbf::new(self.buf);
        pbf.pos = start;
        let end = pbf.message_end()?;
        let mut feature = RawFeature {
            id: None,
            kind: 0.0,
            properties: Map::new(),
            extent: self.extent,
            buf: self.buf,
            geometry: None,
        };
        while pbf.pos < end {
            let tag = pbf.read_varint(false)?;
            match tag as i64 {
                8 => feature.id = Some(pbf.read_varint(false)?),
                18 => {
                    let tags_end = pbf.message_end()?;
                    while pbf.pos < tags_end {
                        let key_index = pbf.read_varint(false)?;
                        let value_index = pbf.read_varint(false)?;
                        let key = self
                            .keys
                            .get(key_index as usize)
                            .cloned()
                            .unwrap_or_else(|| "undefined".into());
                        match self.values.get(value_index as usize) {
                            Some(value) => {
                                feature.properties.insert(key, value.clone());
                            }
                            None => {
                                feature.properties.remove(&key);
                            }
                        }
                    }
                }
                24 => feature.kind = pbf.read_varint(false)?,
                34 => {
                    feature.geometry = Some(pbf.pos);
                    pbf.skip(tag)?;
                }
                _ => pbf.skip(tag)?,
            }
        }
        Ok(feature)
    }
}

type TilePoint = (f64, f64);

impl RawFeature<'_> {
    /// `loadGeometry()`.
    pub fn load_geometry(&self) -> Result<Vec<Vec<TilePoint>>> {
        let start = self
            .geometry
            .ok_or_else(|| DecodeError("feature has no geometry".into()))?;
        let mut pbf = Pbf::new(self.buf);
        pbf.pos = start;
        let end = pbf.message_end()?;
        let mut lines = Vec::new();
        let mut line: Option<Vec<TilePoint>> = None;
        let (mut cmd, mut length) = (1i32, 0i32);
        let (mut x, mut y) = (0.0, 0.0);
        while pbf.pos < end {
            if length <= 0 {
                let cmd_len = pbf.read_varint(false)? as i64 as i32;
                cmd = cmd_len & 0x7;
                length = cmd_len >> 3;
                if length == 0 {
                    continue;
                }
            }
            length -= 1;
            match cmd {
                1 => {
                    x += pbf.read_svarint()?;
                    y += pbf.read_svarint()?;
                    if let Some(done) = line.take() {
                        lines.push(done);
                    }
                    line = Some(vec![(x, y)]);
                }
                2 => {
                    x += pbf.read_svarint()?;
                    y += pbf.read_svarint()?;
                    if let Some(line) = line.as_mut() {
                        line.push((x, y));
                    }
                }
                7 => {
                    if let Some(line) = line.as_mut() {
                        let first = line[0];
                        line.push(first);
                    }
                }
                other => return Err(DecodeError(format!("unknown command {other}"))),
            }
        }
        if let Some(line) = line {
            lines.push(line);
        }
        Ok(lines)
    }

    /// `toGeoJSON(x, y, z)`.
    pub fn to_geojson(&self, x: u32, y: u32, z: u32) -> Result<Feature> {
        let size = self.extent * 2f64.powi(z as i32);
        let x0 = self.extent * f64::from(x);
        let y0 = self.extent * f64::from(y);
        let rings = self.load_geometry()?;
        let project = |p: &TilePoint| -> Coord {
            [
                (p.0 + x0) * 360.0 / size - 180.0,
                360.0 / std::f64::consts::PI
                    * ((1.0 - (p.1 + y0) * 2.0 / size) * std::f64::consts::PI)
                        .exp()
                        .atan()
                    - 90.0,
            ]
        };
        let project_line =
            |line: &Vec<TilePoint>| -> Vec<Coord> { line.iter().map(project).collect() };
        let geometry = if self.kind == 1.0 {
            let mut points: Vec<Coord> = rings.iter().map(|line| project(&line[0])).collect();
            if points.len() == 1 {
                Geometry::Point(points.remove(0))
            } else {
                Geometry::MultiPoint(points)
            }
        } else if self.kind == 2.0 {
            let mut lines: Vec<Vec<Coord>> = rings.iter().map(project_line).collect();
            if lines.len() == 1 {
                Geometry::LineString(lines.remove(0))
            } else {
                Geometry::MultiLineString(lines)
            }
        } else if self.kind == 3.0 {
            let mut polygons: Vec<Vec<Vec<Coord>>> = classify_rings(rings)
                .iter()
                .map(|polygon| polygon.iter().map(project_line).collect())
                .collect();
            if polygons.len() == 1 {
                Geometry::Polygon(polygons.remove(0))
            } else {
                Geometry::MultiPolygon(polygons)
            }
        } else {
            return Err(DecodeError("unknown feature type".into()));
        };
        Ok(Feature {
            id: self.id,
            geometry,
            properties: self.properties.clone(),
        })
    }
}

/// `classifyRings`: outer rings with their holes.
pub fn classify_rings(rings: Vec<Vec<TilePoint>>) -> Vec<Vec<Vec<TilePoint>>> {
    if rings.len() <= 1 {
        return vec![rings];
    }
    let mut polygons = Vec::new();
    let mut polygon: Option<Vec<Vec<TilePoint>>> = None;
    let mut ccw: Option<bool> = None;
    for ring in rings {
        let area = signed_area(&ring);
        if area == 0.0 {
            continue;
        }
        let negative = area < 0.0;
        let ccw = *ccw.get_or_insert(negative);
        if ccw == negative {
            if let Some(done) = polygon.take() {
                polygons.push(done);
            }
            polygon = Some(vec![ring]);
        } else if let Some(polygon) = polygon.as_mut() {
            polygon.push(ring);
        }
    }
    if let Some(polygon) = polygon {
        polygons.push(polygon);
    }
    polygons
}

fn signed_area(ring: &[TilePoint]) -> f64 {
    let mut sum = 0.0;
    let len = ring.len();
    if len == 0 {
        return 0.0;
    }
    let mut j = len - 1;
    for i in 0..len {
        let (p1, p2) = (ring[i], ring[j]);
        sum += (p2.0 - p1.0) * (p1.1 + p2.1);
        j = i;
    }
    sum
}

/// `new VectorTile(new PbfReader(bytes)).layers`.
pub fn read_tile(bytes: &[u8]) -> Result<HashMap<String, Layer<'_>>> {
    let mut pbf = Pbf::new(bytes);
    let mut layers = HashMap::new();
    while pbf.pos < pbf.len() {
        let tag = pbf.read_varint(false)?;
        if tag == 26.0 {
            let end = pbf.message_end()?;
            let layer = Layer::read(&mut pbf, end)?;
            if !layer.is_empty() {
                layers.insert(layer.name.clone(), layer);
            }
        } else {
            pbf.skip(tag)?;
        }
    }
    Ok(layers)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// A minimal protobuf writer for test tiles.
    #[derive(Default)]
    pub struct Writer(pub Vec<u8>);

    impl Writer {
        pub fn varint(&mut self, mut v: u64) -> &mut Self {
            while v >= 0x80 {
                self.0.push((v as u8 & 0x7f) | 0x80);
                v >>= 7;
            }
            self.0.push(v as u8);
            self
        }
        pub fn tag(&mut self, field: u64, kind: u64) -> &mut Self {
            self.varint(field << 3 | kind)
        }
        pub fn bytes(&mut self, field: u64, bytes: &[u8]) -> &mut Self {
            self.tag(field, 2).varint(bytes.len() as u64);
            self.0.extend_from_slice(bytes);
            self
        }
        pub fn string(&mut self, field: u64, s: &str) -> &mut Self {
            self.bytes(field, s.as_bytes())
        }
    }

    pub fn zigzag(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }

    /// A tile with one layer of features: (type, geometry commands, tags).
    pub fn tile(
        layer: &str,
        keys: &[&str],
        values: &[Value],
        features: &[(u64, Vec<u64>, Vec<u64>)],
    ) -> Vec<u8> {
        let mut l = Writer::default();
        l.tag(15, 0).varint(2);
        l.string(1, layer);
        for (kind, geometry, tags) in features {
            let mut f = Writer::default();
            let mut t = Writer::default();
            for v in tags {
                t.varint(*v);
            }
            f.bytes(2, &t.0);
            f.tag(3, 0).varint(*kind);
            let mut g = Writer::default();
            for v in geometry {
                g.varint(*v);
            }
            f.bytes(4, &g.0);
            l.bytes(2, &f.0);
        }
        for key in keys {
            l.string(3, key);
        }
        for value in values {
            let mut v = Writer::default();
            match value {
                Value::String(s) => {
                    v.string(1, s);
                }
                Value::Bool(b) => {
                    v.tag(7, 0).varint(u64::from(*b));
                }
                Value::Number(n) if n.is_u64() => {
                    v.tag(5, 0).varint(n.as_u64().unwrap());
                }
                Value::Number(n) => {
                    v.tag(3, 1);
                    v.0.extend_from_slice(&n.as_f64().unwrap().to_le_bytes());
                }
                _ => unreachable!(),
            }
            l.bytes(4, &v.0);
        }
        l.tag(5, 0).varint(4096);
        let mut t = Writer::default();
        t.bytes(3, &l.0);
        t.0
    }

    #[test]
    fn varints_match_pbf() {
        let mut w = Writer::default();
        w.varint(300)
            .varint(u64::from(u32::MAX) + 5)
            .varint(zigzag(-3));
        let mut pbf = Pbf::new(&w.0);
        assert_eq!(pbf.read_varint(false).unwrap(), 300.0);
        assert_eq!(pbf.read_varint(false).unwrap(), 4_294_967_300.0);
        assert_eq!(pbf.read_svarint().unwrap(), -3.0);
        let mut neg = Writer::default();
        neg.varint((-2i64) as u64);
        assert_eq!(Pbf::new(&neg.0).read_varint(true).unwrap(), -2.0);
    }

    #[test]
    fn short_strings_replace_bad_bytes() {
        assert_eq!(read_utf8(&[0x61, 0x80, 0x62]), "a\u{fffd}b");
        assert_eq!(read_utf8(&[0x61, 0xff, 0x62]), "a");
        assert_eq!(read_utf8("é€😀".as_bytes()), "é€😀");
    }

    #[test]
    fn decodes_points_lines_and_polygons() {
        let point = vec![9, zigzag(2048), zigzag(2048)];
        let line = vec![9, zigzag(0), zigzag(0), 10, zigzag(4096), zigzag(4096)];
        let square = vec![
            9,
            0,
            0,
            26,
            zigzag(10),
            0,
            0,
            zigzag(10),
            zigzag(-10),
            0,
            15,
        ];
        let bytes = tile(
            "t",
            &["name"],
            &[json_str("a")],
            &[
                (1, point, vec![0, 0]),
                (2, line, vec![]),
                (3, square, vec![]),
            ],
        );
        let layers = read_tile(&bytes).unwrap();
        let layer = &layers["t"];
        assert_eq!(layer.len(), 3);
        let p = layer.feature(0).unwrap().to_geojson(0, 0, 0).unwrap();
        assert_eq!(p.geometry, Geometry::Point([0.0, 0.0]));
        assert_eq!(p.properties["name"], "a");
        let l = layer.feature(1).unwrap().to_geojson(0, 0, 0).unwrap();
        match l.geometry {
            Geometry::LineString(c) => {
                assert_eq!(c[0][0], -180.0);
                assert_eq!(c[1][0], 180.0);
                assert!((c[0][1] - 85.0511287798066).abs() < 1e-9);
            }
            other => panic!("{other:?}"),
        }
        let s = layer.feature(2).unwrap().to_geojson(0, 0, 0).unwrap();
        match s.geometry {
            Geometry::Polygon(rings) => assert_eq!(rings[0].len(), 5),
            other => panic!("{other:?}"),
        }
    }

    fn json_str(s: &str) -> Value {
        Value::String(s.into())
    }
}
