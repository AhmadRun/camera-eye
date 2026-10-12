//! Port of `src/tools/area.js`: the `area` and point arguments shared by
//! location-scoped tools, and the geometry tests on resolved areas.

use std::f64::consts::PI;

use serde_json::{Map, Value, json};

use super::catalog::{Failure, ToolError};
use super::js;
use super::services::Services;

pub const EARTH_RADIUS_KM: f64 = 6371.0088;

/// A point in degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLon {
    pub lat: f64,
    pub lon: f64,
}

impl LatLon {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }
}

/// A radius area's center.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Circle {
    pub lat: f64,
    pub lon: f64,
    pub radius_km: f64,
}

/// A resolved area: `{ label, west, south, east, north, center?, point? }`.
#[derive(Debug, Clone, PartialEq)]
pub struct Area {
    pub label: String,
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
    pub center: Option<Circle>,
    pub point: Option<LatLon>,
}

impl Area {
    pub fn bbox(west: f64, south: f64, east: f64, north: f64) -> Self {
        Self {
            label: "the requested box".into(),
            west,
            south,
            east,
            north,
            center: None,
            point: None,
        }
    }
}

/// A place a name resolves to.
#[derive(Debug, Clone, PartialEq)]
pub struct Place {
    pub name: Option<String>,
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
    pub point: Option<LatLon>,
}

fn invalid(message: impl Into<String>) -> Failure {
    Failure::Tool(ToolError::new("invalid_arguments", message))
}

/// `resolveArea`.
pub async fn resolve_area(area: &Value, services: Option<&Services>) -> Result<Area, Failure> {
    let field = |key: &str| area.get(key).filter(|v| !v.is_null());
    let given = ["place", "bbox", "lat"]
        .iter()
        .filter(|key| field(key).is_some())
        .count();
    if given != 1 {
        return Err(invalid(
            "area needs exactly one of place, bbox, or lat/lon with radius_km",
        ));
    }
    if let Some(bbox) = field("bbox").filter(|v| js::truthy(v)) {
        let at = |index: usize| {
            bbox.get(index)
                .map_or(f64::NAN, |v| v.as_f64().unwrap_or(f64::NAN))
        };
        let (west, south, east, north) = (at(0), at(1), at(2), at(3));
        let bad = |v: f64, limit: f64| v.abs() > limit;
        if bad(west, 180.0)
            || bad(east, 180.0)
            || bad(south, 90.0)
            || bad(north, 90.0)
            || south >= north
        {
            return Err(invalid(
                "area.bbox must be [west, south, east, north] with south below north",
            ));
        }
        return Ok(Area::bbox(west, south, east, north));
    }
    if field("lat").is_some() {
        if field("lon").is_none() || field("radius_km").is_none() {
            return Err(invalid("area with lat also needs lon and radius_km"));
        }
        let number = |key: &str| field(key).map_or(f64::NAN, js::to_number);
        return Ok(circle_area(
            number("lat"),
            number("lon"),
            number("radius_km"),
        ));
    }
    let Some(services) = services else {
        return Err(Failure::tool(
            "unsupported",
            "Place names cannot be resolved here; give a bbox or lat/lon instead",
        ));
    };
    let name = field("place").map(js::to_js_string).unwrap_or_default();
    let Some(place) = services.places.resolve(&name).await? else {
        return Err(invalid(format!(
            "No place matched \"{name}\"; try a more specific name or a bbox"
        )));
    };
    Ok(Area {
        label: place.name.clone().filter(|n| !n.is_empty()).unwrap_or(name),
        west: place.west,
        south: place.south,
        east: place.east,
        north: place.north,
        center: None,
        point: place.point,
    })
}

/// The bounding box of a spherical cap.
pub fn circle_area(lat: f64, lon: f64, radius_km: f64) -> Area {
    let angular = radius_km / EARTH_RADIUS_KM;
    let lat_span = angular * (180.0 / PI);
    let reaches_pole = lat + lat_span >= 90.0 || lat - lat_span <= -90.0;
    let ratio = angular.sin() / (lat * PI / 180.0).cos();
    let lon_span = if reaches_pole || ratio >= 1.0 {
        180.0
    } else {
        ratio.asin() * (180.0 / PI)
    };
    let wrap = |value: f64| (((value + 180.0) % 360.0) + 360.0) % 360.0 - 180.0;
    Area {
        label: format!(
            "{} km around {}, {}",
            js::fmt_num(radius_km),
            js::to_fixed(lat, 3),
            js::to_fixed(lon, 3)
        ),
        west: if lon_span >= 180.0 {
            -180.0
        } else {
            wrap(lon - lon_span)
        },
        east: if lon_span >= 180.0 {
            180.0
        } else {
            wrap(lon + lon_span)
        },
        south: js::math_max([-90.0, lat - lat_span]),
        north: js::math_min([90.0, lat + lat_span]),
        center: Some(Circle {
            lat,
            lon,
            radius_km,
        }),
        point: None,
    }
}

/// A resolved point: `{ label, lat, lon }`.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedPoint {
    pub label: String,
    pub lat: f64,
    pub lon: f64,
}

/// `resolvePoint`.
pub async fn resolve_point(
    point: &Value,
    services: Option<&Services>,
) -> Result<NamedPoint, Failure> {
    let field = |key: &str| point.get(key).filter(|v| !v.is_null());
    if let Some(place) = field("place")
        && field("lat").is_none()
        && field("lon").is_none()
    {
        let area = resolve_area(&json!({"place": place}), services).await?;
        let center = area_center(&area);
        return Ok(NamedPoint {
            label: area.label,
            lat: center.lat,
            lon: center.lon,
        });
    }
    if field("place").is_none()
        && let (Some(lat), Some(lon)) = (field("lat"), field("lon"))
    {
        let (lat, lon) = (js::to_number(lat), js::to_number(lon));
        return Ok(NamedPoint {
            label: format!("{}, {}", js::to_fixed(lat, 4), js::to_fixed(lon, 4)),
            lat,
            lon,
        });
    }
    Err(invalid("A location needs either place, or lat and lon"))
}

/// Distance in km from an area's center to its farthest edge point.
pub fn area_radius_km(area: &Area) -> f64 {
    if let Some(center) = area.center {
        return center.radius_km;
    }
    let center = area_center(area);
    let antipode = LatLon::new(
        -center.lat,
        if center.lon > 0.0 {
            center.lon - 180.0
        } else {
            center.lon + 180.0
        },
    );
    if area_contains(area, antipode) {
        return PI * EARTH_RADIUS_KM;
    }
    let width = if area.west <= area.east {
        area.east - area.west
    } else {
        360.0 - (area.west - area.east)
    };
    let lon_at = |t: f64| area.west + width * t;
    let steps = 64;
    let mut points = Vec::with_capacity(4 * (steps + 1) + 2);
    for step in 0..=steps {
        let t = step as f64 / steps as f64;
        let lat = area.south + (area.north - area.south) * t;
        points.push(LatLon::new(lat, area.west));
        points.push(LatLon::new(lat, area.east));
        points.push(LatLon::new(area.north, lon_at(t)));
        points.push(LatLon::new(area.south, lon_at(t)));
    }
    let offset = (((antipode.lon - area.west) % 360.0) + 360.0) % 360.0;
    if offset <= width {
        points.push(LatLon::new(area.north, antipode.lon));
        points.push(LatLon::new(area.south, antipode.lon));
    }
    js::math_max(points.iter().map(|p| distance_km(center, *p)))
}

/// Great-circle distance in kilometers.
pub fn distance_km(a: LatLon, b: LatLon) -> f64 {
    let to_rad = PI / 180.0;
    let d_lat = (b.lat - a.lat) * to_rad;
    let d_lon = (b.lon - a.lon) * to_rad;
    let s1 = (d_lat / 2.0).sin();
    let s2 = (d_lon / 2.0).sin();
    let h = s1 * s1 + (a.lat * to_rad).cos() * (b.lat * to_rad).cos() * (s2 * s2);
    2.0 * EARTH_RADIUS_KM * js::math_min([1.0, h.sqrt()]).asin()
}

/// The center of a resolved area.
pub fn area_center(area: &Area) -> LatLon {
    if let Some(center) = area.center {
        return LatLon::new(center.lat, center.lon);
    }
    if let Some(point) = area.point {
        return point;
    }
    let width = if area.west <= area.east {
        area.east - area.west
    } else {
        area.east + 360.0 - area.west
    };
    let lon = area.west + width / 2.0;
    LatLon::new(
        (area.south + area.north) / 2.0,
        if lon > 180.0 { lon - 360.0 } else { lon },
    )
}

/// Whether a point lies inside a resolved area.
pub fn area_contains(area: &Area, point: LatLon) -> bool {
    if !point.lat.is_finite() || !point.lon.is_finite() {
        return false;
    }
    if point.lat < area.south || point.lat > area.north {
        return false;
    }
    let in_longitude = if area.west <= area.east {
        point.lon >= area.west && point.lon <= area.east
    } else {
        point.lon >= area.west || point.lon <= area.east
    };
    if !in_longitude {
        return false;
    }
    match area.center {
        Some(center) => distance_km(LatLon::new(center.lat, center.lon), point) <= center.radius_km,
        None => true,
    }
}

/// `areaContains(area, record)` for a record with numeric `lat`/`lon`.
pub fn area_contains_value(area: &Area, record: &Value) -> bool {
    match (
        record.get("lat").and_then(Value::as_f64),
        record.get("lon").and_then(Value::as_f64),
    ) {
        (Some(lat), Some(lon)) => area_contains(area, LatLon::new(lat, lon)),
        _ => false,
    }
}

fn segment_meets_box(a: LatLon, b: LatLon, west: f64, south: f64, east: f64, north: f64) -> bool {
    let dx = b.lon - a.lon;
    let dy = b.lat - a.lat;
    let mut low: f64 = 0.0;
    let mut high: f64 = 1.0;
    for (p, q) in [
        (-dx, a.lon - west),
        (dx, east - a.lon),
        (-dy, a.lat - south),
        (dy, north - a.lat),
    ] {
        if p == 0.0 {
            if q < 0.0 {
                return false;
            }
        } else {
            let t = q / p;
            if p < 0.0 {
                low = js::math_max([low, t]);
            } else {
                high = js::math_min([high, t]);
            }
            if low > high {
                return false;
            }
        }
    }
    true
}

fn segment_meets_bounds(a: LatLon, b: LatLon, area: &Area) -> bool {
    let end = if (b.lon - a.lon).abs() > 180.0 {
        LatLon::new(b.lat, b.lon + if b.lon < a.lon { 360.0 } else { -360.0 })
    } else {
        b
    };
    let boxes: Vec<(f64, f64)> = if area.west <= area.east {
        vec![(area.west, area.east)]
    } else {
        vec![(area.west, 180.0), (-180.0, area.east)]
    };
    boxes.iter().any(|(west, east)| {
        [-360.0, 0.0, 360.0].iter().any(|shift| {
            segment_meets_box(a, end, west + shift, area.south, east + shift, area.north)
        })
    })
}

fn segment_distance_km(point: LatLon, a: LatLon, b: LatLon) -> f64 {
    let to_rad = PI / 180.0;
    let bearing = |from: LatLon, to: LatLon| {
        let lat1 = from.lat * to_rad;
        let lat2 = to.lat * to_rad;
        let d_lon = (to.lon - from.lon) * to_rad;
        (d_lon.sin() * lat2.cos())
            .atan2(lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * d_lon.cos())
    };
    let to_point = distance_km(a, point) / EARTH_RADIUS_KM;
    let length = distance_km(a, b) / EARTH_RADIUS_KM;
    let angle = bearing(a, point) - bearing(a, b);
    if length == 0.0 || angle.cos() <= 0.0 {
        return distance_km(a, point);
    }
    let cross = (to_point.sin() * angle.sin()).asin();
    let along = js::math_min([1.0, to_point.cos() / js::math_max([1e-12, cross.cos()])]).acos();
    if along >= length {
        return distance_km(b, point);
    }
    cross.abs() * EARTH_RADIUS_KM
}

/// Whether a line of `[lon, lat]` vertices passes through an area.
pub fn line_touches_area(coords: &[(f64, f64)], area: &Area) -> bool {
    let points: Vec<LatLon> = coords
        .iter()
        .map(|(lon, lat)| LatLon::new(*lat, *lon))
        .filter(|p| p.lat.is_finite() && p.lon.is_finite())
        .collect();
    if points.iter().any(|p| area_contains(area, *p)) {
        return true;
    }
    for pair in points.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if !segment_meets_bounds(a, b, area) {
            continue;
        }
        match area.center {
            None => return true,
            Some(center) => {
                if segment_distance_km(LatLon::new(center.lat, center.lon), a, b)
                    <= center.radius_km
                {
                    return true;
                }
            }
        }
    }
    false
}

/// `[lon, lat]` pairs from a JSON coordinate list (missing values as NaN).
pub fn coord_pairs(line: &Value) -> Vec<(f64, f64)> {
    line.as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let at = |i: usize| {
                        item.get(i)
                            .map_or(f64::NAN, |v| v.as_f64().unwrap_or(f64::NAN))
                    };
                    (at(0), at(1))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn ring_contains(ring: &[(f64, f64)], point: LatLon) -> bool {
    let mut inside = false;
    let n = ring.len();
    if n == 0 {
        return false;
    }
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        if (yi > point.lat) != (yj > point.lat)
            && point.lon < ((xj - xi) * (point.lat - yi)) / (yj - yi) + xi
        {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// Whether polygons (lists of `[lon, lat]` rings, outer first) overlap an area.
pub fn polygons_touch_area(polygons: &[Vec<Vec<(f64, f64)>>], area: &Area) -> bool {
    let center = area_center(area);
    polygons.iter().any(|rings| {
        if rings.is_empty() {
            return false;
        }
        // A ring whose first and last vertices are different arrays is
        // closed by repeating the first; JSON rings never share arrays.
        let closed: Vec<Vec<(f64, f64)>> = rings
            .iter()
            .map(|ring| {
                let mut out = ring.clone();
                if ring.len() > 1 {
                    out.push(ring[0]);
                }
                out
            })
            .collect();
        if closed.iter().any(|ring| line_touches_area(ring, area)) {
            return true;
        }
        ring_contains(&rings[0], center)
            && !rings[1..].iter().any(|hole| ring_contains(hole, center))
    })
}

/// The area as the JSON object JS tools spread or return.
pub fn area_json(area: &Area) -> Value {
    let mut out = Map::new();
    out.insert("label".into(), json!(area.label));
    out.insert("west".into(), js::num(area.west));
    out.insert("south".into(), js::num(area.south));
    out.insert("east".into(), js::num(area.east));
    out.insert("north".into(), js::num(area.north));
    if let Some(center) = area.center {
        out.insert(
            "center".into(),
            json!({"lat": js::num(center.lat), "lon": js::num(center.lon), "radiusKm": js::num(center.radius_km)}),
        );
    }
    if let Some(point) = area.point {
        out.insert(
            "point".into(),
            json!({"lat": js::num(point.lat), "lon": js::num(point.lon)}),
        );
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn resolve(area: Value) -> Result<Area, Failure> {
        block(resolve_area(&area, None))
    }

    #[test]
    fn an_area_needs_exactly_one_form() {
        for area in [
            json!({}),
            json!({"place": "Oslo", "bbox": [0, 0, 1, 1]}),
            json!({"lat": 1, "lon": 2}),
        ] {
            assert_eq!(resolve(area).unwrap_err().code(), Some("invalid_arguments"));
        }
        assert!(
            resolve(json!({"bbox": [0, 10, 1, 5]}))
                .unwrap_err()
                .message()
                .contains("south below north")
        );
        assert_eq!(
            resolve(json!({"place": "Taiwan"})).unwrap_err().code(),
            Some("unsupported")
        );
    }

    #[test]
    fn boxes_contain_the_expected_points() {
        let b = resolve(json!({"bbox": [-10, 40, 10, 50]})).unwrap();
        assert!(area_contains(&b, LatLon::new(45.0, 0.0)));
        assert!(!area_contains(&b, LatLon::new(45.0, 20.0)));
        let pacific = resolve(json!({"bbox": [170, -20, -170, 0]})).unwrap();
        assert!(area_contains(&pacific, LatLon::new(-10.0, 179.0)));
        assert!(area_contains(&pacific, LatLon::new(-10.0, -175.0)));
        assert!(!area_contains(&pacific, LatLon::new(-10.0, 0.0)));
        assert!(!area_contains(&b, LatLon::new(f64::NAN, 0.0)));
    }

    #[test]
    fn radius_areas_use_great_circle_distance() {
        let circle = resolve(json!({"lat": 37.77, "lon": -122.42, "radius_km": 50})).unwrap();
        assert_eq!(circle.center.unwrap().radius_km, 50.0);
        assert_eq!(circle.label, "50 km around 37.770, -122.420");
        assert!(area_contains(&circle, LatLon::new(37.8, -122.27)));
        assert!(!area_contains(&circle, LatLon::new(38.58, -121.49)));
        assert!((distance_km(LatLon::new(0.0, 0.0), LatLon::new(0.0, 1.0)) - 111.2).abs() < 0.1);
        let dateline = resolve(json!({"lat": 0, "lon": 179.9, "radius_km": 100})).unwrap();
        assert!(dateline.west > dateline.east);
        assert!(area_contains(&dateline, LatLon::new(0.0, -179.9)));
        let polar = resolve(json!({"lat": 80, "lon": 0, "radius_km": 1500})).unwrap();
        assert!(area_contains(&polar, LatLon::new(89.0, 180.0)));
        assert_eq!((polar.west, polar.east, polar.north), (-180.0, 180.0, 90.0));
    }

    #[test]
    fn lines_and_polygons_touch_areas() {
        let b = resolve(json!({"bbox": [-1, -1, 1, 1]})).unwrap();
        assert!(line_touches_area(&[(-2.0, 0.0), (2.0, 0.0)], &b));
        assert!(!line_touches_area(&[(-2.0, 2.0), (2.0, 2.0)], &b));
        let circle = resolve(json!({"lat": 0, "lon": 0, "radius_km": 50})).unwrap();
        assert!(line_touches_area(&[(-2.0, 0.3), (2.0, 0.3)], &circle));
        assert!(!line_touches_area(&[(0.3, 0.42), (0.6, 0.42)], &circle));
        let near = resolve(json!({"bbox": [179.5, -1, 179.9, 1]})).unwrap();
        assert!(line_touches_area(&[(179.0, 0.0), (-179.0, 0.0)], &near));
        assert!(!line_touches_area(&[(170.0, 0.0), (175.0, 0.0)], &near));
        let square = |half: f64, x: f64| {
            vec![vec![
                (x - half, -half),
                (x + half, -half),
                (x + half, half),
                (x - half, half),
            ]]
        };
        assert!(polygons_touch_area(&[square(5.0, 0.0)], &b));
        assert!(polygons_touch_area(&[square(0.5, 0.0)], &b));
        assert!(polygons_touch_area(&[square(1.0, 1.5)], &b));
        assert!(!polygons_touch_area(&[square(0.5, 5.0)], &b));
        let mut ring = square(5.0, 0.0);
        ring.extend(square(3.0, 0.0));
        assert!(!polygons_touch_area(&[ring], &b));
    }

    #[test]
    fn a_box_radius_reaches_every_point() {
        let world = resolve(json!({"bbox": [-180, -90, 180, 90]})).unwrap();
        assert!((area_radius_km(&world) - PI * EARTH_RADIUS_KM).abs() < 1.0);
        let small = resolve(json!({"bbox": [-97.94, 30.1, -97.56, 30.52]})).unwrap();
        assert!(area_radius_km(&small) < 30.0);
        assert_eq!(
            area_center(&resolve(json!({"bbox": [0, 0, 10, 10]})).unwrap()),
            LatLon::new(5.0, 5.0)
        );
    }
}
