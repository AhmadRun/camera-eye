//! Server-side simplification of large `out geom` payloads (port of
//! `server/providers/overpass/geometry.js`).

use serde_json::Value;

use super::{
    OVERPASS_SIMPLIFY_MIN_BYTES, OVERPASS_SIMPLIFY_MIN_POINTS, OVERPASS_SIMPLIFY_TOLERANCE_DEG,
};
use crate::providers::mapdata_support::{js_max, js_min, js_to_number, utf16_len};

/// Thresholds; the defaults are the production constants.
#[derive(Clone, Copy, Debug)]
pub struct SimplifyOptions {
    pub min_bytes: usize,
    pub min_points: usize,
    pub tolerance_deg: f64,
}

impl Default for SimplifyOptions {
    fn default() -> Self {
        Self {
            min_bytes: OVERPASS_SIMPLIFY_MIN_BYTES,
            min_points: OVERPASS_SIMPLIFY_MIN_POINTS,
            tolerance_deg: OVERPASS_SIMPLIFY_TOLERANCE_DEG,
        }
    }
}

/// A geometry point as JS arithmetic sees it; `None` where reading `.lat`
/// or `.lon` would throw (a `null` point).
fn coords(point: &Value) -> Option<(f64, f64)> {
    match point {
        Value::Null => None,
        Value::Object(map) => Some((js_to_number(map.get("lon")), js_to_number(map.get("lat")))),
        _ => Some((f64::NAN, f64::NAN)),
    }
}

/// Iterative Douglas-Peucker on `[{lat,lon},...]`; endpoints always kept.
/// `Err` where the JS would throw on a null point.
fn douglas_peucker(points: &[Value], tolerance: f64) -> Result<Vec<Value>, ()> {
    let n = points.len();
    if n <= 2 {
        return Ok(points.to_vec());
    }
    let xy = |i: usize| coords(&points[i]).ok_or(());
    let mut keep = vec![false; n];
    keep[0] = true;
    keep[n - 1] = true;
    let mut stack = vec![(0usize, n - 1)];
    while let Some((a, b)) = stack.pop() {
        if b - a < 2 {
            continue;
        }
        let (ax, ay) = xy(a)?;
        let (bx, by) = xy(b)?;
        let vx = bx - ax;
        let vy = by - ay;
        let c2 = vx * vx + vy * vy;
        let mut worst: Option<usize> = None;
        let mut worst_dist = tolerance;
        for i in a + 1..b {
            let (px, py) = xy(i)?;
            let wx = px - ax;
            let wy = py - ay;
            let d = if c2 == 0.0 {
                wx.hypot(wy)
            } else {
                let t = js_max(0.0, js_min(1.0, (vx * wx + vy * wy) / c2));
                (wx - t * vx).hypot(wy - t * vy)
            };
            if d > worst_dist {
                worst_dist = d;
                worst = Some(i);
            }
        }
        if let Some(worst) = worst {
            keep[worst] = true;
            stack.push((a, worst));
            stack.push((worst, b));
        }
    }
    Ok(points
        .iter()
        .zip(keep)
        .filter(|(_, kept)| *kept)
        .map(|(point, _)| point.clone())
        .collect())
}

fn simplify_geometry(holder: &mut Value, options: &SimplifyOptions) -> Result<(), ()> {
    if let Some(Value::Array(geometry)) = holder.get_mut("geometry")
        && geometry.len() >= options.min_points
    {
        *geometry = douglas_peucker(geometry, options.tolerance_deg)?;
    }
    Ok(())
}

/// `simplifyOverpassPayloadBody`: `Err` where the JS would throw.
#[allow(clippy::result_unit_err)]
pub fn simplify_overpass_payload_body(body: &str, options: &SimplifyOptions) -> Result<String, ()> {
    if utf16_len(body) < options.min_bytes {
        return Ok(body.to_string());
    }
    let Ok(mut data) = serde_json::from_str::<Value>(body) else {
        return Ok(body.to_string());
    };
    let Some(Value::Array(elements)) = data.get_mut("elements") else {
        return Ok(body.to_string());
    };
    for element in elements.iter_mut() {
        if element.is_null() {
            // `el?.geometry` on a null element is a no-op.
            continue;
        }
        simplify_geometry(element, options)?;
        if let Some(Value::Array(members)) = element.get_mut("members") {
            for member in members.iter_mut() {
                simplify_geometry(member, options)?;
            }
        }
    }
    Ok(serde_json::to_string(&data).unwrap_or_else(|_| body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dense_ring(n: usize) -> Vec<Value> {
        let (lat_c, lon_c, radius) = (37.5, 14.2, 0.5);
        let mut points: Vec<Value> = (0..n)
            .map(|i| {
                let a = (i as f64 / n as f64) * 2.0 * std::f64::consts::PI;
                let jitter = (i % 7) as f64 * 0.000004;
                json!({
                    "lat": lat_c + a.sin() * (radius + jitter),
                    "lon": lon_c + a.cos() * (radius + jitter),
                })
            })
            .collect();
        points.push(points[0].clone());
        points
    }

    const TEST_OPTS: SimplifyOptions = SimplifyOptions {
        min_bytes: 0,
        min_points: 200,
        tolerance_deg: 0.0004,
    };

    fn point_seg_dist(p: &Value, a: &Value, b: &Value) -> f64 {
        let g = |v: &Value, k: &str| v[k].as_f64().unwrap();
        let vx = g(b, "lon") - g(a, "lon");
        let vy = g(b, "lat") - g(a, "lat");
        let wx = g(p, "lon") - g(a, "lon");
        let wy = g(p, "lat") - g(a, "lat");
        let c1 = vx * wx + vy * wy;
        if c1 <= 0.0 {
            return wx.hypot(wy);
        }
        let c2 = vx * vx + vy * vy;
        if c2 <= c1 {
            return (g(p, "lon") - g(b, "lon")).hypot(g(p, "lat") - g(b, "lat"));
        }
        let t = c1 / c2;
        (wx - t * vx).hypot(wy - t * vy)
    }

    #[test]
    fn giant_way_geometry_is_decimated_with_endpoints() {
        let ring = dense_ring(4000);
        let body = json!({"elements": [{"type": "way", "id": 1, "geometry": ring}]}).to_string();
        let out: Value =
            serde_json::from_str(&simplify_overpass_payload_body(&body, &TEST_OPTS).unwrap())
                .unwrap();
        let g = out["elements"][0]["geometry"].as_array().unwrap();
        assert!(g.len() < ring.len() / 2);
        assert!(g.len() >= 16);
        assert_eq!(g[0], ring[0]);
        assert_eq!(g[g.len() - 1], ring[ring.len() - 1]);
        for i in (0..ring.len()).step_by(97) {
            let best = (1..g.len())
                .map(|j| point_seg_dist(&ring[i], &g[j - 1], &g[j]))
                .fold(f64::INFINITY, f64::min);
            assert!(
                best <= TEST_OPTS.tolerance_deg * 1.01,
                "vertex {i} deviates {best}"
            );
        }
    }

    #[test]
    fn relation_members_are_decimated_and_small_shapes_pass() {
        let ring = dense_ring(3000);
        let body = json!({"elements": [{"type": "relation", "id": 2, "members": [
            {"type": "way", "role": "outer", "geometry": ring},
            {"type": "node", "role": "admin_centre"}
        ]}]})
        .to_string();
        let out: Value =
            serde_json::from_str(&simplify_overpass_payload_body(&body, &TEST_OPTS).unwrap())
                .unwrap();
        assert!(
            out["elements"][0]["members"][0]["geometry"]
                .as_array()
                .unwrap()
                .len()
                < 1500
        );
        assert!(out["elements"][0]["members"][1].get("geometry").is_none());

        let square = json!([
            {"lat": 30.27, "lon": -97.74}, {"lat": 30.271, "lon": -97.74},
            {"lat": 30.271, "lon": -97.741}, {"lat": 30.27, "lon": -97.741},
            {"lat": 30.27, "lon": -97.74}
        ]);
        let body = json!({"elements": [{"type": "way", "id": 3, "geometry": square}]}).to_string();
        let out: Value =
            serde_json::from_str(&simplify_overpass_payload_body(&body, &TEST_OPTS).unwrap())
                .unwrap();
        assert_eq!(out["elements"][0]["geometry"], square);
    }

    #[test]
    fn sub_threshold_and_non_json_pass_through() {
        let tiny = json!({"elements": [{"type": "way", "geometry": dense_ring(3000)}]}).to_string();
        let options = SimplifyOptions {
            min_bytes: tiny.len() + 1,
            ..TEST_OPTS
        };
        assert_eq!(
            simplify_overpass_payload_body(&tiny, &options).unwrap(),
            tiny
        );
        let junk = "this is not json {";
        assert_eq!(
            simplify_overpass_payload_body(junk, &TEST_OPTS).unwrap(),
            junk
        );
    }
}
