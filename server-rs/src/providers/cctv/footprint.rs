//! Port of `src/data/cctvFootprint.js`: the pure monitor-plane footprint
//! math. The server only needs [`pose_hash`] (the ground-height sidecar
//! join); the geometry is kept with it so the two stay one definition.

#![allow(dead_code)]

use std::collections::BTreeMap;

use super::jsval;

const EARTH_RADIUS_M: f64 = 6_371_000.0;
pub const PLANE_VERT_ASPECT: f64 = 16.0 / 9.0;
pub const SUPPORT_KEYS: [&str; 9] = ["bl", "bm", "br", "ml", "mc", "mr", "tl", "tm", "tr"];

/// The pose fields the footprint depends on.
#[derive(Debug, Clone, Copy)]
pub struct Pose {
    pub lat: f64,
    pub lon: f64,
    pub heading_deg: f64,
    pub pitch_deg: f64,
    pub fov_deg: f64,
    pub range_m: f64,
    pub mount_height_m: f64,
}

/// `projectPoint`: offset a lat/lon along a compass heading (spherical earth).
pub fn project_point(lat: f64, lon: f64, heading_deg: f64, dist_m: f64) -> (f64, f64) {
    let b = heading_deg.to_radians();
    let la = lat.to_radians();
    let lo = lon.to_radians();
    let ad = dist_m / EARTH_RADIUS_M;
    let la2 = (la.sin() * ad.cos() + la.cos() * ad.sin() * b.cos()).asin();
    let lo2 = lo + (b.sin() * ad.sin() * la.cos()).atan2(ad.cos() - la.sin() * la2.sin());
    (la2.to_degrees(), lo2.to_degrees())
}

/// `planeDimensions`.
#[derive(Debug, Clone, Copy)]
pub struct PlaneDims {
    pub r: f64,
    pub pitch: f64,
    pub half_w: f64,
    pub half_h: f64,
    pub horiz: f64,
    pub vert: f64,
    pub up_vert: f64,
    pub up_horiz: f64,
    pub v_fov_deg: f64,
}

/// `Number(x) || fallback` for an f64 that may be NaN or 0.
fn or_number(x: f64, fallback: f64) -> f64 {
    if x.is_nan() || x == 0.0 { fallback } else { x }
}

pub fn plane_dimensions(pose: &Pose) -> PlaneDims {
    let r = or_number(pose.range_m, 1.0).max(1.0);
    let pitch = or_number(pose.pitch_deg, 0.0)
        .clamp(-89.0, 89.0)
        .to_radians();
    let h_fov = or_number(pose.fov_deg, 74.0).clamp(8.0, 160.0).to_radians();
    let half_w = r * (h_fov / 2.0).tan();
    let v_fov = 2.0 * ((h_fov / 2.0).tan() / PLANE_VERT_ASPECT).atan();
    let half_h = r * (v_fov / 2.0).tan();
    PlaneDims {
        r,
        pitch,
        half_w,
        half_h,
        horiz: r * pitch.cos(),
        vert: r * pitch.sin(),
        up_vert: pitch.cos() * half_h,
        up_horiz: -pitch.sin() * half_h,
        v_fov_deg: v_fov.to_degrees(),
    }
}

fn row_of(key: &str) -> f64 {
    match key.as_bytes()[0] {
        b'b' => -1.0,
        b't' => 1.0,
        _ => 0.0,
    }
}

fn col_of(key: &str) -> f64 {
    match key.as_bytes()[1] {
        b'l' => -1.0,
        b'r' => 1.0,
        _ => 0.0,
    }
}

/// One support point's horizontal position and grid cell.
#[derive(Debug, Clone, Copy)]
pub struct Support {
    pub lat: f64,
    pub lon: f64,
    pub row: f64,
    pub col: f64,
}

/// Mount, cap centre and the named support points.
pub type SupportPoints = ((f64, f64), (f64, f64), BTreeMap<&'static str, Support>);

/// `planeSupportPoints`: mount, cap centre and the nine support points.
pub fn plane_support_points(pose: &Pose) -> SupportPoints {
    let dims = plane_dimensions(pose);
    let heading = or_number(pose.heading_deg, 0.0);
    let cap = project_point(pose.lat, pose.lon, heading, dims.horiz);
    let mut supports = BTreeMap::new();
    for key in SUPPORT_KEYS {
        let row = row_of(key);
        let col = col_of(key);
        let across = project_point(cap.0, cap.1, heading + 90.0, col * dims.half_w);
        let along = project_point(across.0, across.1, heading, row * dims.up_horiz);
        supports.insert(
            key,
            Support {
                lat: along.0,
                lon: along.1,
                row,
                col,
            },
        );
    }
    ((pose.lat, pose.lon), cap, supports)
}

/// `supportAltitude`.
pub fn support_altitude(cap_alt_m: f64, row: f64, dims: &PlaneDims) -> f64 {
    cap_alt_m + row * dims.up_vert
}

/// `requiredPlaneLift`: the rigid lift that clears every support point.
pub fn required_plane_lift(
    cap_alt_m: f64,
    dims: &PlaneDims,
    ground_under: Option<&serde_json::Map<String, serde_json::Value>>,
    fallback_ground_m: f64,
    clearance_m: f64,
) -> (f64, Option<&'static str>) {
    let mut lift = 0.0;
    let mut limiting = None;
    for key in SUPPORT_KEYS {
        let raw = ground_under
            .and_then(|g| g.get(key))
            .and_then(|v| v.as_f64())
            .filter(|v| v.is_finite());
        let ground = raw.unwrap_or(fallback_ground_m);
        if !ground.is_finite() {
            continue;
        }
        let alt = support_altitude(cap_alt_m, row_of(key), dims);
        let deficit = ground + clearance_m - alt;
        if deficit > lift {
            lift = deficit;
            limiting = Some(key);
        }
    }
    (lift, limiting)
}

/// `poseHash`: stable hash of the nominal pose a footprint was sampled for.
pub fn pose_hash(pose: &Pose) -> String {
    let text = [
        jsval::to_fixed(pose.lat, 6),
        jsval::to_fixed(pose.lon, 6),
        jsval::to_fixed(pose.heading_deg, 1),
        jsval::to_fixed(pose.pitch_deg, 1),
        jsval::to_fixed(pose.fov_deg, 1),
        jsval::to_fixed(pose.range_m, 1),
        jsval::to_fixed(pose.mount_height_m, 1),
    ]
    .join("|");
    let mut h: u32 = 0x811c_9dc5;
    for unit in text.encode_utf16() {
        h ^= u32::from(unit);
        h = h.wrapping_mul(0x0100_0193);
    }
    format!("p1-{}", base36(h))
}

fn base36(mut n: u32) -> String {
    if n == 0 {
        return "0".into();
    }
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    while n > 0 {
        out.push(digits[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const POSE: Pose = Pose {
        lat: 30.2672,
        lon: -97.7431,
        heading_deg: 90.0,
        pitch_deg: -24.0,
        fov_deg: 56.0,
        range_m: 210.0,
        mount_height_m: 12.0,
    };

    #[test]
    fn support_points_form_a_grid_across_the_far_cap() {
        let (mount, cap, supports) = plane_support_points(&POSE);
        assert_eq!(mount, (POSE.lat, POSE.lon));
        assert_eq!(supports.len(), 9);
        let dims = plane_dimensions(&POSE);
        let east = project_point(POSE.lat, POSE.lon, 90.0, dims.horiz);
        assert!((cap.0 - east.0).abs() < 1e-9 && (cap.1 - east.1).abs() < 1e-9);
        assert!((supports["mc"].lat - cap.0).abs() < 1e-9);
        assert!(supports["ml"].lat > cap.0 && supports["mr"].lat < cap.0);
        assert!(supports["tm"].lon > supports["mc"].lon && supports["bm"].lon < supports["mc"].lon);
    }

    #[test]
    fn rigid_lift_is_the_largest_deficit() {
        let dims = plane_dimensions(&POSE);
        let ground = 149.0;
        let cap_alt = ground + POSE.mount_height_m + dims.vert;
        let flat = required_plane_lift(cap_alt, &dims, None, ground, 2.0);
        assert!((flat.0 - (ground + 2.0 - (cap_alt - dims.up_vert))).abs() < 1e-9);
        assert!(flat.1.unwrap().starts_with('b'));
        let bump = json!({ "tr": ground + 200.0 });
        let bumped = required_plane_lift(cap_alt, &dims, bump.as_object(), ground, 2.0);
        assert_eq!(bumped.1, Some("tr"));
        assert!(bumped.0 > flat.0);
        assert_eq!(
            required_plane_lift(ground + 500.0, &dims, None, ground, 2.0),
            (0.0, None)
        );
        let junk = json!({ "bl": null, "br": "x" });
        assert_eq!(
            required_plane_lift(cap_alt, &dims, junk.as_object(), ground, 2.0).0,
            flat.0
        );
    }

    #[test]
    fn pose_hash_is_stable_and_changes_with_any_field() {
        let h = pose_hash(&POSE);
        assert_eq!(pose_hash(&POSE), h);
        assert!(h.starts_with("p1-"));
        // Computed with the JS implementation.
        assert_eq!(h, JS_POSE_HASH);
        let mut changed = POSE;
        changed.lat += 0.00001;
        assert_ne!(pose_hash(&changed), h);
        let mut changed = POSE;
        changed.heading_deg += 0.1;
        assert_ne!(pose_hash(&changed), h);
        let mut changed = POSE;
        changed.range_m += 1.0;
        assert_ne!(pose_hash(&changed), h);
        let mut changed = POSE;
        changed.mount_height_m += 0.1;
        assert_ne!(pose_hash(&changed), h);
    }

    const JS_POSE_HASH: &str = "p1-13xgwl8";

    #[test]
    fn pose_hash_rounds_ties_like_to_fixed() {
        let tricky = Pose {
            lat: 0.25,
            lon: -0.000001,
            heading_deg: 0.25,
            pitch_deg: f64::NAN,
            fov_deg: 1.05,
            range_m: 2.5,
            mount_height_m: -0.04,
        };
        // Computed with the JS implementation.
        assert_eq!(pose_hash(&tricky), "p1-e2oji6");
    }
}
