//! Mapped military installations (`createInstallationSource` in
//! `src/layers/installations/source.js`): the server's Overpass route,
//! and when the server has no Overpass, OpenFreeMap `landuse=military`
//! tiles (`src/sources/openFreeMap.js`, `militaryTileGeometry.js`) named
//! from the bundled OSM name pack (`src/data/militaryNames.js`).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use serde_json::Value;

use super::fetch::Api;
use super::mvt::{self, Geometry};
use super::vector_tiles::{
    BBox, TemplateSource, VectorTileSource, hypot2, tile_to_bbox, tiles_for_bounds,
};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

type P = [f64; 2];

const MILITARY_TILE_MIN_ZOOM: u32 = 9;
const MILITARY_TILE_MAX: usize = 16;
const NAMES_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_COOLDOWN_MS: f64 = 5_000.0;
const RETRY_COOLDOWN_MAX_MS: f64 = 300_000.0;

/// One mapped site, with the fields the tools read.
#[derive(Debug, Clone, PartialEq)]
pub struct Site {
    pub id: String,
    pub name: Option<Value>,
    pub kind: Option<String>,
    pub class: Option<Value>,
    pub latitude: f64,
    pub longitude: f64,
}

/// `getMappedSites` results.
#[derive(Debug, Clone, PartialEq)]
pub struct Sites {
    pub records: Vec<Site>,
    pub saturated: bool,
    pub source: Option<String>,
    /// The tile records were built before the name pack loaded, and the
    /// pack did not load (`enrichment` settled to null).
    pub names_pending: bool,
}

// ---- Name pack ----

/// One name pack record.
#[derive(Debug, Clone, PartialEq)]
pub struct NameRecord {
    pub id: String,
    pub osm_key: String,
    pub name: Value,
    pub longitude: f64,
    pub latitude: f64,
    pub bbox: [f64; 4],
    pub class: Option<Value>,
    pub area_m2: f64,
}

/// The loaded pack: records, largest first, and an index by OSM key.
pub struct Names {
    pub records: Vec<Arc<NameRecord>>,
    pub by_id: HashMap<String, Arc<NameRecord>>,
}

fn parse_names(pack: &Value) -> Result<Names, Failure> {
    let rows = pack
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Failure::Type("Cannot read properties of undefined (reading 'map')".into())
        })?;
    let classes = pack.get("classes");
    let mut records: Vec<Arc<NameRecord>> = rows
        .iter()
        .map(|row| {
            let at = |i: usize| row.get(i);
            let num = |i: usize| at(i).and_then(Value::as_f64).unwrap_or(f64::NAN);
            let osm_key = at(0)
                .map(js::to_js_string)
                .unwrap_or_else(|| "undefined".into());
            let class = at(8).and_then(|kind| {
                let index = js::to_js_string(kind);
                classes
                    .and_then(|c| c.get(index.parse::<usize>().ok()?))
                    .cloned()
            });
            Arc::new(NameRecord {
                id: format!("osm:military:{osm_key}"),
                name: at(1).cloned().unwrap_or(Value::Null),
                longitude: num(2),
                latitude: num(3),
                bbox: [num(4), num(5), num(6), num(7)],
                class,
                area_m2: num(9),
                osm_key,
            })
        })
        .collect();
    js::sort_by(&mut records, |a, b| {
        let by_area = b.area_m2 - a.area_m2;
        if by_area != 0.0 && !by_area.is_nan() {
            by_area
        } else {
            js::locale_compare(&a.osm_key, &b.osm_key)
        }
    });
    let by_id = records
        .iter()
        .map(|r| (r.osm_key.clone(), r.clone()))
        .collect();
    Ok(Names { records, by_id })
}

#[derive(Default)]
struct NamesState {
    loaded: Option<Arc<Names>>,
    failed_at: f64,
    failures: u32,
}

/// `militaryOsmKey(featureKey)`: Planetiler's typed id as `n123`/`w123`/`r123`.
pub fn military_osm_key(feature_key: &str) -> Option<String> {
    let id = js::str_to_number(feature_key);
    if !(id.is_finite() && id.fract() == 0.0 && id.abs() <= 9_007_199_254_740_991.0) || id <= 0.0 {
        return None;
    }
    let prefix = ["", "n", "w", "r"]
        .get((id % 10.0) as usize)
        .copied()
        .unwrap_or("");
    (!prefix.is_empty()).then(|| format!("{prefix}{}", js::fmt_num((id / 10.0).floor())))
}

/// `militaryNamesInView(names, box, Infinity, false)`: every pack record in
/// the box.
fn names_in_view(names: &Names, b: &BBox) -> Vec<Site> {
    let width = if b.east >= b.west {
        b.east - b.west
    } else {
        360.0 + b.east - b.west
    };
    names
        .records
        .iter()
        .filter(|r| {
            let x = (r.longitude - b.west + 360.0) % 360.0;
            !(x > width || r.latitude < b.south || r.latitude > b.north)
        })
        .map(|r| Site {
            id: r.id.clone(),
            name: Some(r.name.clone()),
            kind: Some("installation".into()),
            class: r.class.clone(),
            latitude: r.latitude,
            longitude: r.longitude,
        })
        .collect()
}

// ---- Tile geometry ----

/// `clipTileRing(ring, box)`: Sutherland–Hodgman to the tile core.
pub fn clip_tile_ring(ring: &[P], b: &BBox) -> Vec<P> {
    let mut points: Vec<P> = ring[..ring.len().saturating_sub(1)].to_vec();
    for (axis, edge, sign) in [
        (0usize, b.west, 1.0),
        (0, b.east, -1.0),
        (1, b.south, 1.0),
        (1, b.north, -1.0),
    ] {
        let mut output = Vec::new();
        let n = points.len();
        for i in 0..n {
            let a = points[(i + n - 1) % n];
            let bp = points[i];
            let ai = (a[axis] - edge) * sign >= 0.0;
            let bi = (bp[axis] - edge) * sign >= 0.0;
            if ai != bi {
                let t = (edge - a[axis]) / (bp[axis] - a[axis]);
                let mut p = [a[0] + t * (bp[0] - a[0]), a[1] + t * (bp[1] - a[1])];
                p[axis] = edge;
                output.push(p);
            }
            if bi {
                output.push(bp);
            }
        }
        points = output;
    }
    if points.len() >= 3 {
        let first = points[0];
        points.push(first);
        points
    } else {
        Vec::new()
    }
}

/// `polygonCentroid(ring)`.
fn polygon_centroid(ring: &[P]) -> Option<P> {
    let [ox, oy] = *ring.first()?;
    let (mut area, mut x, mut y) = (0.0, 0.0, 0.0);
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let (ax, ay) = (ring[j][0] - ox, ring[j][1] - oy);
        let (bx, by) = (ring[i][0] - ox, ring[i][1] - oy);
        let cross = ax * by - bx * ay;
        area += cross;
        x += (ax + bx) * cross;
        y += (ay + by) * cross;
        j = i;
    }
    Some(if area.abs() > 1e-15 {
        [ox + x / (3.0 * area), oy + y / (3.0 * area)]
    } else {
        ring[0]
    })
}

/// A decoded tile fragment of a military area.
#[derive(Debug, Clone)]
pub struct Fragment {
    pub id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub footprint: Vec<P>,
    pub rings: Vec<Vec<P>>,
    pub feature_key: String,
    pub tile_epsilon: f64,
    pub tile_zoom: u32,
    pub name: Value,
    pub class: Value,
    pub named_area: bool,
    pub name_record: Option<Arc<NameRecord>>,
}

/// `decodeOpenFreeMapMilitaryTile(bytes, z, x, y)`: the military polygons.
pub fn decode_military_tile(
    bytes: &[u8],
    z: u32,
    x: u32,
    y: u32,
) -> Result<Vec<Fragment>, Failure> {
    let fail = |e: mvt::DecodeError| Failure::other(e.0);
    let layers = mvt::read_tile(bytes).map_err(fail)?;
    let b = tile_to_bbox(z, x, y);
    let mut military = Vec::new();
    let Some(layer) = layers.get("landuse") else {
        return Ok(military);
    };
    if layer.len() > 40_000 {
        return Err(Failure::other("Vector tile feature limit exceeded"));
    }
    for i in 0..layer.len() {
        let raw = layer.feature(i).map_err(fail)?;
        if raw.properties.get("class").and_then(Value::as_str) != Some("military") {
            continue;
        }
        let feature = raw.to_geojson(x, y, z).map_err(fail)?;
        let polygons = match feature.geometry {
            Geometry::Polygon(rings) => vec![rings],
            Geometry::MultiPolygon(polygons) => polygons,
            _ => Vec::new(),
        };
        for (p, polygon) in polygons.iter().enumerate() {
            let rings: Vec<Vec<P>> = polygon
                .iter()
                .map(|r| clip_tile_ring(r, &b))
                .filter(|r| r.len() >= 4)
                .collect();
            let Some(ring) = rings.first() else { continue };
            let Some([longitude, latitude]) = polygon_centroid(ring) else {
                continue;
            };
            let id_part = feature.id.map_or_else(|| i.to_string(), js::fmt_num);
            military.push(Fragment {
                id: format!("ofm:{z}/{x}/{y}:{id_part}:{p}"),
                latitude,
                longitude,
                footprint: ring.clone(),
                feature_key: match feature.id {
                    Some(id) => js::fmt_num(id),
                    None => format!("tile:{z}/{x}/{y}:{i}"),
                },
                tile_epsilon: (b.east - b.west) / raw.extent,
                tile_zoom: z,
                name: Value::String("Military area".into()),
                class: Value::String("military_land".into()),
                named_area: false,
                name_record: None,
                rings,
            });
        }
    }
    Ok(military)
}

/// `nameMilitaryFragment(fragment, names)`.
fn name_fragment(fragment: &Fragment, names: Option<&Names>) -> Fragment {
    let osm_key = military_osm_key(&fragment.feature_key);
    let named = names.and_then(|n| n.by_id.get(osm_key.as_deref()?));
    let Some(named) = named.filter(|_| !fragment.footprint.is_empty()) else {
        return fragment.clone();
    };
    let [west, south, east, north] = named.bbox;
    let epsilon = fragment.tile_epsilon * 2.0 + 0.0001;
    let xs = fragment.footprint.iter().map(|p| p[0]);
    let ys = fragment.footprint.iter().map(|p| p[1]);
    if js::math_max(xs.clone()) < west - epsilon
        || js::math_min(xs) > east + epsilon
        || js::math_max(ys.clone()) < south - epsilon
        || js::math_min(ys) > north + epsilon
    {
        return fragment.clone();
    }
    Fragment {
        name: named.name.clone(),
        class: named.class.clone().unwrap_or(Value::Null),
        named_area: true,
        name_record: Some(named.clone()),
        ..fragment.clone()
    }
}

fn inside(point: P, ring: &[P]) -> bool {
    let mut hit = false;
    if ring.is_empty() {
        return false;
    }
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let (a, b) = (ring[i], ring[j]);
        if (a[1] > point[1]) != (b[1] > point[1])
            && point[0] < (b[0] - a[0]) * (point[1] - a[1]) / (b[1] - a[1]) + a[0]
        {
            hit = !hit;
        }
        j = i;
    }
    hit
}

fn in_polygon(point: P, rings: &[Vec<P>]) -> bool {
    inside(point, &rings[0]) && !rings[1..].iter().any(|ring| inside(point, ring))
}

#[derive(Clone, Copy)]
struct Bounds {
    west: f64,
    east: f64,
    south: f64,
    north: f64,
}

fn bounds(ring: &[P]) -> Bounds {
    Bounds {
        west: js::math_min(ring.iter().map(|p| p[0])),
        east: js::math_max(ring.iter().map(|p| p[0])),
        south: js::math_min(ring.iter().map(|p| p[1])),
        north: js::math_max(ring.iter().map(|p| p[1])),
    }
}

fn overlaps(a: &Bounds, b: &Bounds, eps: f64) -> bool {
    a.west <= b.east + eps
        && b.west <= a.east + eps
        && a.south <= b.north + eps
        && b.south <= a.north + eps
}

fn distance2(p: P, a: P, b: P) -> f64 {
    let (dx, dy) = (b[0] - a[0], b[1] - a[1]);
    let raw = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / (dx * dx + dy * dy);
    let raw = if raw == 0.0 || raw.is_nan() { 0.0 } else { raw };
    let t = js::math_max([0.0, js::math_min([1.0, raw])]);
    (p[0] - a[0] - t * dx).powi(2) + (p[1] - a[1] - t * dy).powi(2)
}

struct Piece {
    fragment: Fragment,
    bbox: Bounds,
}

fn touches(a: &Piece, b: &Piece, epsilon: f64) -> bool {
    if !overlaps(&a.bbox, &b.bbox, epsilon) {
        return false;
    }
    let (ar, br) = (&a.fragment.rings, &b.fragment.rings);
    if ar[0].iter().any(|p| in_polygon(*p, br)) || br[0].iter().any(|p| in_polygon(*p, ar)) {
        return true;
    }
    let cross = |p: P, q: P, r: P| (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0]);
    for ring_a in ar {
        for ring_b in br {
            for i in 1..ring_a.len() {
                for j in 1..ring_b.len() {
                    let (p, q, r, s) = (ring_a[i - 1], ring_a[i], ring_b[j - 1], ring_b[j]);
                    if cross(p, q, r) * cross(p, q, s) < 0.0
                        && cross(r, s, p) * cross(r, s, q) < 0.0
                    {
                        return true;
                    }
                    let nearest = js::math_min([
                        distance2(p, r, s),
                        distance2(q, r, s),
                        distance2(r, p, q),
                        distance2(s, p, q),
                    ]);
                    if nearest <= epsilon.powi(2) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn ring_moment(ring: &[P]) -> (f64, P) {
    let [ox, oy] = ring[0];
    let (mut sum, mut x, mut y) = (0.0, 0.0, 0.0);
    for i in 1..ring.len() {
        let a = [ring[i - 1][0] - ox, ring[i - 1][1] - oy];
        let b = [ring[i][0] - ox, ring[i][1] - oy];
        let c = a[0] * b[1] - b[0] * a[1];
        sum += c;
        x += (a[0] + b[0]) * c;
        y += (a[1] + b[1]) * c;
    }
    let point = if sum != 0.0 && !sum.is_nan() {
        [ox + x / (3.0 * sum), oy + y / (3.0 * sum)]
    } else {
        ring[0]
    };
    ((sum / 2.0).abs(), point)
}

/// Session-stable installation ids (`installationIds`) and the named
/// members remembered per id (`groupNames`).
#[derive(Default)]
struct Aliases {
    ids: IndexMap<String, String>,
    titles: IndexMap<String, Vec<Arc<NameRecord>>>,
}

/// A merged installation before `militaryNameInView`.
struct Merged {
    site: Site,
    named_area: bool,
    footprints: Vec<Vec<Vec<P>>>,
}

fn sort_named(members: &mut [Arc<NameRecord>]) {
    js::sort_by(members, |a, b| {
        let by_area = b.area_m2 - a.area_m2;
        if by_area != 0.0 && !by_area.is_nan() {
            by_area
        } else {
            js::locale_compare(&a.osm_key, &b.osm_key)
        }
    });
}

/// `mergeMilitaryFragments(records, aliases)`.
fn merge_fragments(records: Vec<Fragment>, aliases: &mut Aliases) -> Vec<Merged> {
    let pieces: Vec<Piece> = records
        .into_iter()
        .filter(|r| r.footprint.len() >= 4)
        .map(|fragment| Piece {
            bbox: bounds(&fragment.footprint),
            fragment,
        })
        .collect();
    let n = pieces.len();
    let mut parents: Vec<usize> = (0..n).collect();
    fn root(parents: &mut [usize], mut i: usize) -> usize {
        while parents[i] != i {
            parents[i] = parents[parents[i]];
            i = parents[i];
        }
        i
    }
    for i in 0..n {
        for j in 0..i {
            if root(&mut parents, i) == root(&mut parents, j) {
                continue;
            }
            let (a, b) = (&pieces[i], &pieces[j]);
            let scope = a.fragment.tile_zoom;
            let known = aliases
                .ids
                .get(&format!("{scope}:{}", a.fragment.feature_key));
            let same_alias = scope == b.fragment.tile_zoom
                && known.is_some_and(|k| !k.is_empty())
                && known
                    == aliases
                        .ids
                        .get(&format!("{scope}:{}", b.fragment.feature_key));
            if same_alias
                || a.fragment.feature_key == b.fragment.feature_key
                || touches(a, b, 1e-10)
            {
                let (ri, rj) = (root(&mut parents, i), root(&mut parents, j));
                parents[ri] = rj;
            }
        }
    }
    let mut groups: IndexMap<usize, Vec<&Piece>> = IndexMap::new();
    for (i, piece) in pieces.iter().enumerate() {
        let key = root(&mut parents, i);
        groups.entry(key).or_default().push(piece);
    }
    let mut used_ids: HashSet<String> = HashSet::new();
    let mut merged = Vec::new();
    for group in groups.values() {
        let first = &group[0].fragment;
        let mut keys: Vec<String> = Vec::new();
        for piece in group {
            if !keys.contains(&piece.fragment.feature_key) {
                keys.push(piece.fragment.feature_key.clone());
            }
        }
        keys.sort_by(|a, b| js::cmp16(a, b));
        let zoom = first.tile_zoom;
        let scoped: Vec<String> = keys.iter().map(|key| format!("{zoom}:{key}")).collect();
        let mut prior: Vec<String> = scoped
            .iter()
            .filter_map(|key| aliases.ids.get(key).filter(|v| !v.is_empty()).cloned())
            .collect();
        prior.sort_by(|a, b| js::cmp16(a, b));
        let mut by_key: IndexMap<String, Arc<NameRecord>> = IndexMap::new();
        let titled = prior.iter().flat_map(|id| {
            aliases
                .titles
                .get(&format!("{zoom}:{id}"))
                .cloned()
                .unwrap_or_default()
        });
        let own = group.iter().filter_map(|p| p.fragment.name_record.clone());
        for record in titled.chain(own) {
            by_key.insert(record.osm_key.clone(), record);
        }
        let mut named_members: Vec<Arc<NameRecord>> = by_key.into_values().collect();
        sort_named(&mut named_members);
        let primary = named_members.first().cloned();
        let typed_key = military_osm_key(&keys[0]);
        let mut id = match (&primary, prior.first()) {
            (Some(primary), _) if !primary.id.is_empty() => primary.id.clone(),
            (_, Some(prior)) => prior.clone(),
            _ => match &typed_key {
                Some(typed) => format!("osm:military:{typed}"),
                None => format!("ofm:installation:{}", keys[0]),
            },
        };
        if used_ids.contains(&id) {
            id = format!("ofm:installation:{}", keys[0]);
        }
        used_ids.insert(id.clone());
        let prior_ids: HashSet<&String> = prior.iter().collect();
        let zoom_prefix = format!("{zoom}:");
        for (key, previous) in aliases.ids.iter_mut() {
            if key.starts_with(&zoom_prefix) && prior_ids.contains(previous) {
                *previous = id.clone();
            }
        }
        for key in &scoped {
            aliases.ids.insert(key.clone(), id.clone());
        }
        for previous in &prior {
            aliases.titles.shift_remove(&format!("{zoom}:{previous}"));
        }
        if !named_members.is_empty() {
            aliases.titles.insert(
                format!("{zoom}:{id}"),
                named_members.iter().take(2048).cloned().collect(),
            );
        }
        let (mut area, mut x, mut y) = (0.0, 0.0, 0.0);
        for piece in group {
            for (i, ring) in piece.fragment.rings.iter().enumerate() {
                let (ring_area, point) = ring_moment(ring);
                let weight = ring_area * if i > 0 { -1.0 } else { 1.0 };
                area += weight;
                x += point[0] * weight;
                y += point[1] * weight;
            }
        }
        let in_group = |point: P| group.iter().any(|p| in_polygon(point, &p.fragment.rings));
        let label_point = primary.as_ref().map(|p| [p.longitude, p.latitude]);
        let mut point = match label_point {
            Some(label) if in_group(label) => label,
            _ if area > 0.0 => [x / area, y / area],
            _ => first.footprint[0],
        };
        if !in_group(point) {
            let mut candidates: Vec<P> = Vec::new();
            for piece in group {
                let lat = (piece.bbox.south + piece.bbox.north) / 2.0;
                let mut hits: Vec<f64> = Vec::new();
                for ring in &piece.fragment.rings {
                    for i in 1..ring.len() {
                        let (a, b) = (ring[i - 1], ring[i]);
                        if (a[1] > lat) != (b[1] > lat) {
                            hits.push(a[0] + (lat - a[1]) * (b[0] - a[0]) / (b[1] - a[1]));
                        }
                    }
                }
                js::sort_by(&mut hits, |a, b| a - b);
                for i in 1..hits.len() {
                    let p = [(hits[i - 1] + hits[i]) / 2.0, lat];
                    if in_polygon(p, &piece.fragment.rings) {
                        candidates.push(p);
                    }
                }
            }
            js::sort_by(&mut candidates, |a, b| {
                hypot2(a[0] - point[0], a[1] - point[1]) - hypot2(b[0] - point[0], b[1] - point[1])
            });
            point = candidates.first().copied().unwrap_or(first.footprint[0]);
        }
        let (name, class, named_area) = match &primary {
            Some(primary) => (primary.name.clone(), primary.class.clone(), true),
            None => (
                first.name.clone(),
                Some(first.class.clone()),
                first.named_area,
            ),
        };
        merged.push(Merged {
            site: Site {
                id,
                name: Some(name),
                kind: Some("installation".into()),
                class,
                latitude: point[1],
                longitude: point[0],
            },
            named_area,
            footprints: group.iter().map(|p| p.fragment.rings.clone()).collect(),
        });
    }
    while aliases.ids.len() > 4096 {
        aliases.ids.shift_remove_index(0);
    }
    while aliases.titles.len() > 4096 {
        aliases.titles.shift_remove_index(0);
    }
    merged
}

/// `militaryNameInView(record, box)`.
fn name_in_view(record: Merged, b: &BBox) -> Site {
    let site = record.site;
    if !record.named_area
        || record.footprints.is_empty()
        || (site.longitude >= b.west
            && site.longitude <= b.east
            && site.latitude >= b.south
            && site.latitude <= b.north)
    {
        return site;
    }
    for x in [0.5, 0.25, 0.75] {
        for y in [0.5, 0.25, 0.75] {
            let point = [
                b.west + (b.east - b.west) * x,
                b.south + (b.north - b.south) * y,
            ];
            if record
                .footprints
                .iter()
                .any(|rings| in_polygon(point, rings))
            {
                return Site {
                    longitude: point[0],
                    latitude: point[1],
                    ..site
                };
            }
        }
    }
    site
}

/// `installationTileZoom(box)`.
pub fn installation_tile_zoom(b: &BBox) -> Option<u32> {
    let mut zoom = if js::math_max([b.north - b.south, b.east - b.west]) > 0.2 {
        10
    } else {
        12
    };
    let fits = |z: u32| tiles_for_bounds(b, z, MILITARY_TILE_MAX + 1).len() <= MILITARY_TILE_MAX;
    while zoom > MILITARY_TILE_MIN_ZOOM && !fits(zoom) {
        zoom -= 1;
    }
    fits(zoom).then_some(zoom)
}

// ---- Overpass records ----

fn class_of(tags: &Value) -> Option<&'static str> {
    let military = match tags.get("military").filter(|v| js::truthy(v)) {
        Some(value) => js::to_js_string(value).to_lowercase(),
        None => String::new(),
    };
    match military.as_str() {
        "airfield" => Some("airfield"),
        "naval_base" => Some("naval_base"),
        "range" => Some("range"),
        "barracks" | "base" => Some("military_land"),
        _ => (tags.get("landuse").and_then(Value::as_str) == Some("military"))
            .then_some("military_land"),
    }
}

fn humanize(class: &str) -> &'static str {
    match class {
        "airfield" => "Military airfield",
        "naval_base" => "Naval base",
        "range" => "Firing range",
        _ => "Military land",
    }
}

fn number_of(value: Option<&Value>) -> f64 {
    value.map_or(f64::NAN, js::to_number)
}

fn point_from(element: &Value) -> Option<(f64, f64)> {
    let pick = |key: &str| {
        element.get(key).filter(|v| !v.is_null()).or_else(|| {
            element
                .get("center")
                .filter(|c| !c.is_null())
                .and_then(|c| c.get(key))
        })
    };
    let lat = number_of(pick("lat"));
    let lon = number_of(pick("lon"));
    let lat_ok = |v: f64| v.is_finite() && (-90.0..=90.0).contains(&v);
    let lon_ok = |v: f64| v.is_finite() && (-180.0..=180.0).contains(&v);
    if lat_ok(lat) && lon_ok(lon) {
        return Some((lat, lon));
    }
    let b = element.get("bounds")?;
    let edge = |key: &str| b.get(key).and_then(Value::as_f64).filter(|v| v.is_finite());
    let (south, west, north, east) = (
        edge("minlat")?,
        edge("minlon")?,
        edge("maxlat")?,
        edge("maxlon")?,
    );
    (lat_ok(south)
        && lat_ok(north)
        && lon_ok(west)
        && lon_ok(east)
        && south <= north
        && west <= east
        && north - south <= 10.0
        && east - west <= 10.0)
        .then(|| ((south + north) / 2.0, (west + east) / 2.0))
}

/// `normalizeMilitaryInstallations(payload)`: the records.
pub fn normalize_installations(payload: &Value) -> Vec<Site> {
    let mut records = Vec::new();
    let mut ids = HashSet::new();
    for element in payload
        .get("elements")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let kind = match element.get("type").filter(|v| js::truthy(v)) {
            Some(value) => js::to_js_string(value),
            None => String::new(),
        };
        let osm_id = number_of(element.get("id"));
        let safe =
            osm_id.is_finite() && osm_id.fract() == 0.0 && osm_id.abs() <= 9_007_199_254_740_991.0;
        if !["node", "way", "relation"].contains(&kind.as_str()) || !safe {
            continue;
        }
        let id = format!("osm:{kind}:{}", js::fmt_num(osm_id));
        let empty = Value::Object(Default::default());
        let tags = element
            .get("tags")
            .filter(|t| js::truthy(t))
            .unwrap_or(&empty);
        let class = class_of(tags);
        let point = point_from(element);
        let (Some(class), Some((latitude, longitude))) = (class, point) else {
            continue;
        };
        if !ids.insert(id.clone()) {
            continue;
        }
        let raw_name = tags
            .get("name")
            .filter(|v| js::truthy(v))
            .or_else(|| tags.get("name:en").filter(|v| js::truthy(v)))
            .map(js::to_js_string)
            .unwrap_or_default();
        let name = match js::trim(&raw_name) {
            "" => humanize(class).to_string(),
            trimmed => trimmed.to_string(),
        };
        records.push(Site {
            id,
            name: Some(Value::String(name)),
            kind: Some("installation".into()),
            class: Some(Value::String(class.into())),
            latitude,
            longitude,
        });
    }
    records
}

/// `installationResponseSaturated(payload)`.
fn response_saturated(payload: &Value) -> bool {
    if let Some(saturated) = payload.get("saturated").and_then(Value::as_bool) {
        return saturated;
    }
    let cap = number_of(payload.get("elementCap"));
    if !cap.is_finite() || cap <= 0.0 {
        return false;
    }
    payload
        .get("elements")
        .and_then(Value::as_array)
        .is_some_and(|e| e.len() as f64 >= cap)
}

/// `isUnavailableCapability(value)`.
pub fn is_unavailable_capability(value: &Value) -> bool {
    value.get("code").and_then(Value::as_str) == Some("OVERPASS_NOT_CONFIGURED")
        || value.get("unavailable") == Some(&Value::Bool(true))
        || value.get("retryable") == Some(&Value::Bool(false))
}

/// `createInstallationSource`.
pub struct InstallationSource {
    api: Api,
    tiles: VectorTileSource<Fragment>,
    overpass_unavailable: AtomicBool,
    aliases: Mutex<Aliases>,
    names: tokio::sync::Mutex<NamesState>,
}

impl InstallationSource {
    pub fn new(api: &Api) -> Self {
        Self {
            api: api.clone(),
            tiles: VectorTileSource::new(
                api.clone(),
                TemplateSource::TileJson("https://tiles.openfreemap.org/planet".into()),
                "https://tiles.openfreemap.org",
                decode_military_tile,
                16,
                f64::INFINITY,
            )
            .with_cache(192, 64 * 1024 * 1024),
            overpass_unavailable: AtomicBool::new(false),
            aliases: Mutex::new(Aliases::default()),
            names: tokio::sync::Mutex::new(NamesState::default()),
        }
    }

    /// `loadMilitaryNames()`: loaded once; a failure is replayed during a
    /// doubling cooldown.
    async fn load_names(&self) -> Result<Arc<Names>, Failure> {
        let mut state = self.names.lock().await;
        if let Some(names) = &state.loaded {
            return Ok(names.clone());
        }
        let unavailable = || Failure::other("Mapped names temporarily unavailable");
        if state.failures > 0 {
            let wait = js::math_min([
                RETRY_COOLDOWN_MS * 2f64.powi(state.failures as i32 - 1),
                RETRY_COOLDOWN_MAX_MS,
            ]);
            if js::now() - state.failed_at < wait {
                return Err(unavailable());
            }
        }
        let load = async {
            let response = self
                .api
                .get(&self.api.bundled_url("osm_military_names/names.json"))
                .await?;
            if !response.ok() {
                return Err(Failure::other(format!("HTTP {}", response.status)));
            }
            parse_names(&response.json().await?)
        };
        match tokio::time::timeout(NAMES_TIMEOUT, load).await {
            Ok(Ok(names)) => {
                let names = Arc::new(names);
                state.loaded = Some(names.clone());
                state.failures = 0;
                Ok(names)
            }
            _ => {
                state.failures += 1;
                state.failed_at = js::now();
                Err(unavailable())
            }
        }
    }

    async fn named_sites(&self, b: &BBox) -> Result<Sites, Failure> {
        let names = self.load_names().await?;
        Ok(Sites {
            records: names_in_view(&names, b),
            saturated: false,
            source: Some("OpenStreetMap".into()),
            names_pending: false,
        })
    }

    fn snapshot(
        &self,
        tiles: &[Arc<Vec<Fragment>>],
        partial: bool,
        names: Option<&Names>,
        b: &BBox,
    ) -> Sites {
        let fragments: Vec<Fragment> = tiles
            .iter()
            .flat_map(|tile| tile.iter())
            .map(|fragment| name_fragment(fragment, names))
            .collect();
        let count = fragments.len();
        let mut aliases = self.aliases.lock().expect("installation aliases");
        let merged = merge_fragments(fragments.into_iter().take(2048).collect(), &mut aliases);
        Sites {
            records: merged
                .into_iter()
                .map(|record| name_in_view(record, b))
                .collect(),
            saturated: partial || count > 2048,
            source: Some("OpenStreetMap tiles".into()),
            names_pending: false,
        }
    }

    async fn tile_sites(&self, b: &BBox) -> Result<Sites, Failure> {
        let Some(zoom) = installation_tile_zoom(b) else {
            return self.named_sites(b).await;
        };
        let (tiles, names) =
            tokio::join!(self.tiles.fetch_bounds(b, zoom, None), self.load_names());
        let result = tiles?;
        match names {
            Ok(names) => Ok(self.snapshot(&result.tiles, result.partial, Some(&names), b)),
            Err(_) => {
                let mut sites = self.snapshot(&result.tiles, result.partial, None, b);
                sites.names_pending = true;
                Ok(sites)
            }
        }
    }

    /// `getMappedSites(box, { exact: true, thinned: false })`.
    pub async fn mapped_sites(&self, b: &BBox) -> Result<Sites, Failure> {
        let BBox {
            south,
            west,
            north,
            east,
        } = *b;
        if ![south, west, north, east].iter().all(|v| v.is_finite())
            || south < -90.0
            || north > 90.0
            || !(-180.0..=180.0).contains(&west)
            || !(-180.0..=180.0).contains(&east)
            || north <= south
            || east == west
        {
            return Err(Failure::Type(
                "A bounded installation viewport is required".into(),
            ));
        }
        if east < west || north - south > 10.0 || east - west > 10.0 {
            return self.named_sites(b).await;
        }
        if self.overpass_unavailable.load(Ordering::Relaxed) {
            return self.tile_sites(b).await;
        }
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("south", &js::to_fixed(south, 5))
            .append_pair("west", &js::to_fixed(west, 5))
            .append_pair("north", &js::to_fixed(north, 5))
            .append_pair("east", &js::to_fixed(east, 5))
            .append_pair("exact", "1")
            .finish();
        let response = self
            .api
            .get(&format!("/api/military-installations?{query}"))
            .await?;
        let ok = response.ok();
        let body = response.json().await?;
        if is_unavailable_capability(&body) {
            self.overpass_unavailable.store(true, Ordering::Relaxed);
            return self.tile_sites(b).await;
        }
        if !ok {
            return Err(Failure::other("Installation context unavailable"));
        }
        if !body.get("elements").is_some_and(Value::is_array) {
            return Err(Failure::other("Malformed installation snapshot"));
        }
        Ok(Sites {
            records: normalize_installations(&body),
            saturated: response_saturated(&body),
            source: None,
            names_pending: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_osm_keys() {
        assert_eq!(military_osm_key("1222"), Some("w122".into()));
        assert_eq!(military_osm_key("1223"), Some("r122".into()));
        assert_eq!(military_osm_key("1220"), None);
        assert_eq!(military_osm_key("tile:9/1/2:3"), None);
    }

    #[test]
    fn normalizes_overpass_elements() {
        let payload = json!({"elements": [
            {"type": "way", "id": 5, "tags": {"military": "Airfield"}, "bounds": {"minlat": 1, "minlon": 2, "maxlat": 3, "maxlon": 4}},
            {"type": "node", "id": 6, "tags": {"landuse": "military", "name": " Camp X "}, "lat": 10, "lon": 20},
            {"type": "node", "id": 7, "tags": {"military": "bunker"}, "lat": 10, "lon": 20},
        ]});
        let records = normalize_installations(&payload);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].name, Some(json!("Military airfield")));
        assert_eq!((records[0].latitude, records[0].longitude), (2.0, 3.0));
        assert_eq!(records[1].name, Some(json!("Camp X")));
    }

    #[test]
    fn merges_touching_fragments() {
        let square = |x0: f64| {
            vec![
                [x0, 0.0],
                [x0 + 1.0, 0.0],
                [x0 + 1.0, 1.0],
                [x0, 1.0],
                [x0, 0.0],
            ]
        };
        let fragment = |key: &str, ring: Vec<P>| Fragment {
            id: key.into(),
            latitude: 0.5,
            longitude: 0.5,
            footprint: ring.clone(),
            rings: vec![ring],
            feature_key: key.into(),
            tile_epsilon: 0.0,
            tile_zoom: 12,
            name: json!("Military area"),
            class: json!("military_land"),
            named_area: false,
            name_record: None,
        };
        let mut aliases = Aliases::default();
        let merged = merge_fragments(
            vec![
                fragment("1232", square(0.0)),
                fragment("4562", square(1.0)),
                fragment("7892", square(5.0)),
            ],
            &mut aliases,
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].site.id, "osm:military:w123");
        assert_eq!(
            (merged[0].site.longitude, merged[0].site.latitude),
            (1.0, 0.5)
        );
        assert_eq!(
            aliases.ids.get("12:4562").map(String::as_str),
            Some("osm:military:w123")
        );
    }

    #[test]
    fn clips_rings_to_the_tile() {
        let b = BBox {
            west: 0.0,
            south: 0.0,
            east: 1.0,
            north: 1.0,
        };
        let ring = vec![
            [-1.0, 0.5],
            [0.5, 0.5],
            [0.5, 2.0],
            [-1.0, 2.0],
            [-1.0, 0.5],
        ];
        let clipped = clip_tile_ring(&ring, &b);
        assert_eq!(clipped.first(), clipped.last());
        assert!(clipped.iter().all(|p| p[0] >= 0.0 && p[1] <= 1.0));
    }
}
