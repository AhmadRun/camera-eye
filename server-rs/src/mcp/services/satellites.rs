//! The satellite catalog source (`src/layers/satellites/source.js`), TLE
//! parsing (`src/sources/tle.js`), satellite.js's `twoline2satrec`,
//! `propagate`, `gstime` and look angles over the `sgp4` crate, and pass
//! prediction (`src/data/satellitePass.js`).

use std::f64::consts::PI;

use super::fetch::Api;
use crate::mcp::catalog::Failure;
use crate::mcp::js;

pub const GROUPS: [&str; 7] = [
    "stations", "visual", "gps-ops", "glo-ops", "galileo", "geo", "starlink",
];

/// `readGroup`'s answer.
pub struct GroupText {
    pub ok: bool,
    pub status: u16,
    pub text: String,
    pub stale: bool,
}

/// `satellites.readGroup(group)`.
pub async fn read_group(api: &Api, group: &str) -> Result<GroupText, Failure> {
    if !GROUPS.contains(&group) {
        return Err(Failure::Type("Unknown satellite group".into()));
    }
    let response = api.get(&format!("/api/celestrak/{group}")).await?;
    let ok = response.ok();
    let status = response.status;
    let stale = response.header("x-tle-cache").as_deref() == Some("STALE-ERROR");
    let text = if ok {
        response.text().await?
    } else {
        String::new()
    };
    Ok(GroupText {
        ok,
        status,
        text,
        stale,
    })
}

/// One catalog entry.
pub struct TleEntry {
    pub name: String,
    pub line1: String,
    pub line2: String,
}

/// `parseTleText`: three-line blocks whose lines 2 and 3 are TLE lines.
pub fn parse_tle_text(text: &str) -> Vec<TleEntry> {
    let lines: Vec<&str> = js::trim(text)
        .split('\n')
        .map(js::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let mut result = Vec::new();
    let mut i = 0;
    while i + 2 < lines.len() {
        let (name, line1, line2) = (lines[i], lines[i + 1], lines[i + 2]);
        if line1.starts_with("1 ") && line2.starts_with("2 ") {
            result.push(TleEntry {
                name: name.into(),
                line1: line1.into(),
                line2: line2.into(),
            });
        }
        i += 3;
    }
    result
}

/// `String#substring(start, end)` in UTF-16 units (TLE text is ASCII).
fn substring(text: &str, start: usize, end: usize) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    let end = end.min(units.len());
    let start = start.min(end);
    String::from_utf16_lossy(&units[start..end])
}

/// `tleCatalogNumber`: the NORAD number from TLE line 1, or None.
pub fn tle_catalog_number(line1: &str) -> Option<i64> {
    crate::common::js::parse_int(&substring(line1, 2, 7))
}

const XPDOTP: f64 = 1440.0 / (2.0 * PI);
const DEG2RAD: f64 = PI / 180.0;
const EARTH_RADIUS_KM: f64 = 6378.135;

/// An initialized SGP4 record.
pub struct Satrec {
    constants: Option<sgp4::Constants>,
    jdsatepoch: f64,
}

fn days2mdhms(year: f64, days: f64) -> (f64, f64, f64, f64, f64) {
    let leap = if year % 4.0 == 0.0 { 29.0 } else { 28.0 };
    let lmonth = [
        31.0, leap, 31.0, 30.0, 31.0, 30.0, 31.0, 31.0, 30.0, 31.0, 30.0, 31.0,
    ];
    let dayofyr = days.floor();
    let mut i = 1;
    let mut inttemp = 0.0;
    while dayofyr > inttemp + lmonth[i - 1] && i < 12 {
        inttemp += lmonth[i - 1];
        i += 1;
    }
    let mon = i as f64;
    let day = dayofyr - inttemp;
    let mut temp = (days - dayofyr) * 24.0;
    let hr = temp.floor();
    temp = (temp - hr) * 60.0;
    let minute = temp.floor();
    let sec = (temp - minute) * 60.0;
    (mon, day, hr, minute, sec)
}

fn jday_internal(year: f64, mon: f64, day: f64, hr: f64, minute: f64, sec: f64, msec: f64) -> f64 {
    367.0 * year - (7.0 * (year + ((mon + 9.0) / 12.0).floor()) * 0.25).floor()
        + (275.0 * mon / 9.0).floor()
        + day
        + 1_721_013.5
        + ((msec / 60000.0 + sec / 60.0 + minute) / 60.0 + hr) / 24.0
}

/// `jday(new Date(ms))`.
pub fn jday_ms(ms: f64) -> f64 {
    let ms = ms.trunc() as i64;
    let days = ms.div_euclid(86_400_000);
    let in_day = ms.rem_euclid(86_400_000);
    let (year, month, day) = js::civil_from_days(days);
    jday_internal(
        year as f64,
        f64::from(month),
        f64::from(day),
        (in_day / 3_600_000) as f64,
        (in_day / 60_000 % 60) as f64,
        (in_day / 1000 % 60) as f64,
        (in_day % 1000) as f64,
    )
}

/// `gstime(new Date(ms))`.
pub fn gstime_ms(ms: f64) -> f64 {
    let tut1 = (jday_ms(ms) - 2_451_545.0) / 36525.0;
    let temp = -6.2e-6 * tut1 * tut1 * tut1
        + 0.093104 * tut1 * tut1
        + (876_600.0 * 3600.0 + 8_640_184.812866) * tut1
        + 67310.54841;
    let mut temp = (temp * DEG2RAD / 240.0) % (2.0 * PI);
    if temp < 0.0 {
        temp += 2.0 * PI;
    }
    temp
}

/// `twoline2satrec(line1, line2)` (improved mode, WGS72).
pub fn twoline2satrec(line1: &str, line2: &str) -> Satrec {
    let pf = |text: String| crate::common::js::parse_float(&text);
    let epochyr =
        crate::common::js::parse_int(&substring(line1, 18, 20)).map_or(f64::NAN, |v| v as f64);
    let epochdays = pf(substring(line1, 20, 32));
    let bstar = pf(format!(
        "{}.{}E{}",
        substring(line1, 53, 54),
        substring(line1, 54, 59),
        substring(line1, 59, 61)
    ));
    let inclo = pf(substring(line2, 8, 16)) * DEG2RAD;
    let nodeo = pf(substring(line2, 17, 25)) * DEG2RAD;
    let ecco = pf(format!(
        ".{}",
        substring(line2, 26, 33)
            .chars()
            .map(|c| if js::is_js_space(c) { '0' } else { c })
            .collect::<String>()
    ));
    let argpo = pf(substring(line2, 34, 42)) * DEG2RAD;
    let mo = pf(substring(line2, 43, 51)) * DEG2RAD;
    let no = pf(substring(line2, 52, 63)) / XPDOTP;
    let year = if epochyr < 57.0 {
        epochyr + 2000.0
    } else {
        epochyr + 1900.0
    };
    let (mon, day, hr, minute, sec) = days2mdhms(year, epochdays);
    let jdsatepoch = jday_internal(year, mon, day, hr, minute, sec, 0.0);
    let constants =
        sgp4::Orbit::from_kozai_elements(&sgp4::WGS72, inclo, nodeo, ecco, argpo, mo, no)
            .ok()
            .and_then(|orbit| {
                sgp4::Constants::new(
                    sgp4::WGS72,
                    sgp4::iau_epoch_to_sidereal_time,
                    (jdsatepoch - 2_451_545.0) / 365.25,
                    bstar,
                    orbit,
                )
                .ok()
            })
            .filter(|_| {
                [bstar, inclo, nodeo, ecco, argpo, mo, no, jdsatepoch]
                    .iter()
                    .all(|v| v.is_finite())
            });
    Satrec {
        constants,
        jdsatepoch,
    }
}

impl Satrec {
    /// `propagate(satrec, new Date(ms)).position` in km (TEME), or None.
    pub fn position(&self, ms: f64) -> Option<[f64; 3]> {
        let constants = self.constants.as_ref()?;
        let minutes = (jday_ms(ms) - self.jdsatepoch) * 1440.0;
        let prediction = constants.propagate(sgp4::MinutesSinceEpoch(minutes)).ok()?;
        let [x, y, z] = prediction.position;
        if ![x, y, z].iter().all(|v| v.is_finite()) {
            return None;
        }
        // satellite.js reports a decayed satellite (inside the Earth) as no
        // position.
        if hypot3(x, y, z) / EARTH_RADIUS_KM < 1.0 {
            return None;
        }
        Some([x, y, z])
    }
}

/// `Math.hypot(a, b, c)` as V8 computes it.
pub fn hypot3(a: f64, b: f64, c: f64) -> f64 {
    let values = [a.abs(), b.abs(), c.abs()];
    let mut max = 0.0f64;
    let mut nan = false;
    for v in values {
        if v.is_nan() {
            nan = true;
        }
        if v > max {
            max = v;
        }
    }
    if max == f64::INFINITY {
        return f64::INFINITY;
    }
    if nan {
        return f64::NAN;
    }
    if max == 0.0 {
        return 0.0;
    }
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for v in values {
        let n = v / max;
        let summand = n * n - compensation;
        let preliminary = sum + summand;
        compensation = (preliminary - sum) - summand;
        sum = preliminary;
    }
    sum.sqrt() * max
}

/// Observer look angles.
pub struct Look {
    pub elev_deg: f64,
    pub az_deg: f64,
    pub sat_eci: [f64; 3],
}

const R2D: f64 = 180.0 / PI;
const D2R: f64 = PI / 180.0;

/// `lookAnglesAt`.
pub fn look_angles_at(satrec: &Satrec, ms: f64, lat_deg: f64, lon_deg: f64) -> Option<Look> {
    let pos = satrec.position(ms)?;
    let gmst = gstime_ms(ms);
    let ecf = [
        pos[0] * gmst.cos() + pos[1] * gmst.sin(),
        pos[0] * -gmst.sin() + pos[1] * gmst.cos(),
        pos[2],
    ];
    let latitude = lat_deg * D2R;
    let longitude = lon_deg * D2R;
    let a = 6378.137;
    let b = 6356.7523142;
    let f = (a - b) / a;
    let e2 = 2.0 * f - f * f;
    let normal = a / (1.0 - e2 * (latitude.sin() * latitude.sin())).sqrt();
    let observer = [
        normal * latitude.cos() * longitude.cos(),
        normal * latitude.cos() * longitude.sin(),
        (normal * (1.0 - e2)) * latitude.sin(),
    ];
    let rx = ecf[0] - observer[0];
    let ry = ecf[1] - observer[1];
    let rz = ecf[2] - observer[2];
    let top_s = latitude.sin() * longitude.cos() * rx + latitude.sin() * longitude.sin() * ry
        - latitude.cos() * rz;
    let top_e = -longitude.sin() * rx + longitude.cos() * ry;
    let top_z = latitude.cos() * longitude.cos() * rx
        + latitude.cos() * longitude.sin() * ry
        + latitude.sin() * rz;
    let range = (top_s * top_s + top_e * top_e + top_z * top_z).sqrt();
    let elevation = (top_z / range).asin();
    let azimuth = (-top_e).atan2(top_s) + PI;
    Some(Look {
        elev_deg: elevation * R2D,
        az_deg: (((azimuth * R2D) % 360.0) + 360.0) % 360.0,
        sat_eci: pos,
    })
}

const SHADOW_RADIUS_KM: f64 = 6371.0;
const AU_KM: f64 = 149_597_870.7;

struct Sun {
    x: f64,
    y: f64,
    z: f64,
    ra_deg: f64,
    dec_deg: f64,
}

fn solar_position_eci(ms: f64) -> Sun {
    let jd = ms / 86_400_000.0 + 2_440_587.5;
    let d = jd - 2_451_545.0;
    let g = ((((357.529 + 0.985_600_28 * d) % 360.0) + 360.0) % 360.0) * D2R;
    let q = (((280.459 + 0.985_647_36 * d) % 360.0) + 360.0) % 360.0;
    let l = ((((q + 1.915 * g.sin() + 0.02 * (2.0 * g).sin()) % 360.0) + 360.0) % 360.0) * D2R;
    let e = (23.439 - 0.000_000_36 * d) * D2R;
    let ra = (e.cos() * l.sin()).atan2(l.cos());
    let dec = (e.sin() * l.sin()).asin();
    Sun {
        x: AU_KM * dec.cos() * ra.cos(),
        y: AU_KM * dec.cos() * ra.sin(),
        z: AU_KM * dec.sin(),
        ra_deg: (((ra * R2D) % 360.0) + 360.0) % 360.0,
        dec_deg: dec * R2D,
    }
}

fn is_satellite_sunlit(sat: [f64; 3], ms: f64) -> bool {
    let sun = solar_position_eci(ms);
    let len = hypot3(sun.x, sun.y, sun.z);
    let (sx, sy, sz) = (sun.x / len, sun.y / len, sun.z / len);
    let proj = sat[0] * sx + sat[1] * sy + sat[2] * sz;
    if proj > 0.0 {
        return true;
    }
    let dx = sat[0] - proj * sx;
    let dy = sat[1] - proj * sy;
    let dz = sat[2] - proj * sz;
    hypot3(dx, dy, dz) > SHADOW_RADIUS_KM
}

fn observer_solar_elevation(lat_deg: f64, lon_deg: f64, ms: f64) -> f64 {
    let sun = solar_position_eci(ms);
    let gmst = gstime_ms(ms);
    let gmst_deg = (((gmst * R2D) % 360.0) + 360.0) % 360.0;
    let hour_angle = (((gmst_deg + lon_deg - sun.ra_deg) % 360.0) + 360.0) % 360.0;
    let h = hour_angle * D2R;
    let phi = lat_deg * D2R;
    let dec = sun.dec_deg * D2R;
    let sin_elev = phi.sin() * dec.sin() + phi.cos() * dec.cos() * h.cos();
    js::math_max([-1.0, js::math_min([1.0, sin_elev])]).asin() * R2D
}

fn is_observer_dark(lat_deg: f64, lon_deg: f64, ms: f64, max_sun_elev_deg: f64) -> bool {
    observer_solar_elevation(lat_deg, lon_deg, ms) <= max_sun_elev_deg
}

#[allow(clippy::too_many_arguments)]
fn bisect_boundary(
    satrec: &Satrec,
    lat: f64,
    lon: f64,
    t_low: f64,
    t_high: f64,
    min_elev: f64,
    rising: bool,
) -> f64 {
    let (mut low, mut high) = (t_low, t_high);
    for _ in 0..10 {
        let mid = (low + high) / 2.0;
        let elev = look_angles_at(satrec, mid, lat, lon).map_or(-90.0, |l| l.elev_deg);
        if rising == (elev >= min_elev) {
            high = mid;
        } else {
            low = mid;
        }
    }
    if rising { high.ceil() } else { low.floor() }
}

/// A predicted pass.
pub struct Pass {
    pub rise_ms: f64,
    pub set_ms: f64,
    pub max_elev_deg: f64,
    pub max_elev_ms: f64,
    pub rise_az_deg: f64,
    pub visible: bool,
}

/// `findNextSatellitePass` with the default scan steps and darkness.
pub fn find_next_satellite_pass(
    satrec: &Satrec,
    lat: f64,
    lon: f64,
    from_ms: f64,
    min_elev: f64,
    horizon_hours: f64,
    require_visible: bool,
) -> Option<Pass> {
    let coarse_step_sec = 20.0;
    let fine_step_sec: f64 = 5.0;
    let max_sun = -6.0;
    if ![lat, lon, from_ms, min_elev, horizon_hours]
        .iter()
        .all(|v| v.is_finite())
        || lat.abs() > 90.0
        || lon.abs() > 180.0
        || from_ms.abs() > 8.63e15
        || !(0.0..=90.0).contains(&min_elev)
        || horizon_hours <= 0.0
        || horizon_hours > 72.0
    {
        return None;
    }
    let horizon_ms = from_ms + horizon_hours * 3_600_000.0;
    let coarse_ms = coarse_step_sec * 1000.0;
    let fine_ms = js::math_max([1000.0, fine_step_sec * 1000.0]);
    let elev_at = |t: f64| look_angles_at(satrec, t, lat, lon).map_or(-90.0, |l| l.elev_deg);
    let mut cursor = from_ms;
    while cursor <= horizon_ms {
        let mut t_prev = cursor;
        let mut t_hit = None;
        let mut elev_prev = elev_at(t_prev);
        if elev_prev >= min_elev {
            t_hit = Some(cursor);
        } else {
            let mut t = cursor + coarse_ms;
            while t <= horizon_ms {
                let elev = elev_at(t);
                if elev >= min_elev {
                    t_hit = Some(t);
                    break;
                }
                t_prev = t;
                elev_prev = elev;
                t += coarse_ms;
            }
        }
        let t_hit = t_hit?;
        let rise_ms = if t_hit == cursor && elev_prev >= min_elev {
            cursor
        } else {
            bisect_boundary(satrec, lat, lon, t_prev, t_hit, min_elev, true)
        };
        let mut t_curr = rise_ms;
        let mut max_elev = -90.0;
        let mut max_elev_ms = rise_ms;
        let mut t_step_prev = rise_ms;
        let mut set_ms = None;
        let mut visible = false;
        let mut elev_prev_sample: Option<f64> = None;
        let mut peak_left: Option<f64> = None;
        let mut peak_right: Option<f64> = None;
        let mut peak_step_ms = fine_ms;
        let check_visibility = |t: f64, visible: &mut bool| {
            if *visible {
                return;
            }
            let Some(look) = look_angles_at(satrec, t, lat, lon) else {
                return;
            };
            if is_satellite_sunlit(look.sat_eci, t) && is_observer_dark(lat, lon, t, max_sun) {
                *visible = true;
            }
        };
        check_visibility(rise_ms, &mut visible);
        while t_curr <= horizon_ms {
            let look = look_angles_at(satrec, t_curr, lat, lon);
            let elev = look.as_ref().map_or(-90.0, |l| l.elev_deg);
            if let Some(look) = &look {
                let sunlit = is_satellite_sunlit(look.sat_eci, t_curr);
                let dark = is_observer_dark(lat, lon, t_curr, max_sun);
                if sunlit && dark && elev >= min_elev {
                    visible = true;
                }
            }
            if elev < min_elev && t_curr > rise_ms {
                if peak_right.is_none() && t_curr > max_elev_ms {
                    peak_right = Some(elev);
                    peak_step_ms = t_curr - max_elev_ms;
                }
                set_ms = Some(bisect_boundary(
                    satrec,
                    lat,
                    lon,
                    t_step_prev,
                    t_curr,
                    min_elev,
                    false,
                ));
                break;
            }
            if elev > max_elev {
                max_elev = elev;
                max_elev_ms = t_curr;
                peak_left = elev_prev_sample;
                peak_right = None;
            } else if peak_right.is_none() && t_curr > max_elev_ms {
                peak_right = Some(elev);
                peak_step_ms = t_curr - max_elev_ms;
            }
            elev_prev_sample = Some(elev);
            t_step_prev = t_curr;
            t_curr += fine_ms;
        }
        let set_ms = set_ms.unwrap_or_else(|| js::math_min([horizon_ms, t_curr]));
        let mut refined_ms = max_elev_ms;
        let mut refined_elev = max_elev;
        if let (Some(left), Some(right)) = (
            peak_left.filter(|v| v.is_finite()),
            peak_right.filter(|v| v.is_finite()),
        ) && left - 2.0 * max_elev + right < 0.0
        {
            let denom = 2.0 * (left - 2.0 * max_elev + right);
            let shift = ((left - right) / denom) * peak_step_ms;
            if shift.abs() < peak_step_ms {
                refined_ms = js::math_max([
                    rise_ms,
                    js::math_min([set_ms, js::math_round(max_elev_ms + shift)]),
                ]);
                refined_elev = js::math_min([
                    90.0,
                    max_elev - (left - right).powf(2.0) / (8.0 * (left - 2.0 * max_elev + right)),
                ]);
            }
        }
        check_visibility(refined_ms, &mut visible);
        check_visibility(set_ms, &mut visible);
        if !visible {
            visible = has_visible_interval(rise_ms, set_ms, |time| {
                let look = look_angles_at(satrec, time, lat, lon);
                [
                    look.is_some_and(|l| is_satellite_sunlit(l.sat_eci, time)),
                    is_observer_dark(lat, lon, time, max_sun),
                ]
            });
        }
        if require_visible && !visible {
            cursor = js::math_max([set_ms + 1000.0, cursor + coarse_ms]);
            continue;
        }
        let rise_az = look_angles_at(satrec, rise_ms, lat, lon).map_or(0.0, |l| l.az_deg);
        return Some(Pass {
            rise_ms,
            set_ms,
            max_elev_deg: refined_elev,
            max_elev_ms: refined_ms,
            rise_az_deg: rise_az,
            visible,
        });
    }
    None
}

/// `hasVisibleInterval`.
fn has_visible_interval(start: f64, end: f64, conditions_at: impl Fn(f64) -> [bool; 2]) -> bool {
    let visible = |s: [bool; 2]| s[0] && s[1];
    let mut left = start;
    while left < end {
        let right = js::math_min([end, left + 5000.0]);
        let a = conditions_at(left);
        let b = conditions_at(right);
        if visible(a) || visible(b) {
            return true;
        }
        let mut cuts = vec![left, right];
        for condition in 0..2 {
            if a[condition] == b[condition] {
                continue;
            }
            let (mut low, mut high) = (left, right);
            while high - low > 20.0 {
                let mid = (low + high) / 2.0;
                if conditions_at(mid)[condition] == a[condition] {
                    low = mid;
                } else {
                    high = mid;
                }
            }
            cuts.push((low + high) / 2.0);
        }
        js::sort_by(&mut cuts, |a, b| a - b);
        for pair in cuts.windows(2) {
            if visible(conditions_at((pair[0] + pair[1]) / 2.0)) {
                return true;
            }
        }
        left += 5000.0;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    const ISS: [&str; 3] = [
        "ISS (ZARYA)",
        "1 25544U 98067A   20194.88612269 -.00002218  00000-0 -31515-4 0  9992",
        "2 25544  51.6461 221.2784 0001413  89.1723 280.4612 15.49507896236008",
    ];

    #[test]
    fn parses_three_line_catalogs() {
        let text = format!("{}\n{}\n{}\nJUNK\n1 x\n3 y\n", ISS[0], ISS[1], ISS[2]);
        let entries = parse_tle_text(&text);
        assert_eq!(entries.len(), 1);
        assert_eq!(tle_catalog_number(&entries[0].line1), Some(25544));
    }

    #[test]
    fn propagates_like_satellite_js() {
        let satrec = twoline2satrec(ISS[1], ISS[2]);
        // satellite.js 6.0.2 at the epoch: position of the ISS in TEME.
        let epoch_ms = (satrec.jdsatepoch - 2_440_587.5) * 86_400_000.0;
        let position = satrec.position(epoch_ms).unwrap();
        let radius = hypot3(position[0], position[1], position[2]);
        assert!((6700.0..6900.0).contains(&radius), "{radius}");
        assert!(look_angles_at(&satrec, epoch_ms, 0.0, 0.0).is_some());
    }

    #[test]
    fn hypot_matches_v8() {
        assert_eq!(hypot3(3.0, 4.0, 0.0), 5.0);
        assert_eq!(hypot3(0.0, 0.0, 0.0), 0.0);
    }
}
