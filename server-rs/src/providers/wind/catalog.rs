//! Port of `server/providers/wind/catalog.js`: GFS object keys and cycle /
//! forecast-step selection.

use chrono::{DateTime, NaiveDate, Timelike, Utc};

pub const GFS_BUCKET: &str = "noaa-gfs-bdp-pds";

/// `{ date: 'YYYYMMDD', hour }` of a model run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleRef {
    pub date: String,
    pub hour: u32,
}

/// Build the public GFS object key.
pub fn gfs_object_key(date: &str, hour: u32, forecast_hour: u32) -> String {
    format!("gfs.{date}/{hour:02}/atmos/gfs.t{hour:02}z.pgrb2.0p25.f{forecast_hour:03}")
}

fn utc(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_default()
}

/// The UTC day and six-hourly cycle containing `now - lag`
/// (`selectLatestGfsCycle` / `selectLatestIfsCycle`).
pub fn select_cycle(now_ms: i64, availability_lag_ms: i64) -> CycleRef {
    let shifted = utc(now_ms - availability_lag_ms);
    CycleRef {
        date: shifted.format("%Y%m%d").to_string(),
        hour: shifted.hour() / 6 * 6,
    }
}

/// Select the latest GFS cycle that should be available.
pub fn select_latest_gfs_cycle(now_ms: i64) -> CycleRef {
    select_cycle(now_ms, 5 * 3_600_000)
}

/// `Date.UTC(year, month - 1, day, hour)` for a cycle.
pub fn cycle_run_ms(cycle: &CycleRef) -> i64 {
    let parse = |range: std::ops::Range<usize>| -> u32 {
        cycle
            .date
            .get(range)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    };
    NaiveDate::from_ymd_opt(parse(0..4) as i32, parse(4..6), parse(6..8))
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc().timestamp_millis() + i64::from(cycle.hour) * 3_600_000)
        .unwrap_or(0)
}

/// `new Date(ms).toISOString()`.
pub fn iso(ms: i64) -> String {
    utc(ms).format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// `Math.round` for the non-negative values used here.
fn round(value: f64) -> f64 {
    value.round()
}

/// Round a time offset to the nearest published GFS forecast step so the
/// field shown is the one valid closest to now, not always the analysis
/// (f000). GFS 0.25° is hourly through f120, then every 3 h through f384.
pub fn nearest_gfs_step(hours: f64) -> u32 {
    let value = if hours.is_finite() { hours } else { 0.0 }.max(0.0);
    let step = if value <= 120.0 {
        round(value)
    } else {
        round(value / 3.0) * 3.0
    };
    step.clamp(0.0, 384.0) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc_ms(y: i32, m: u32, d: u32, h: u32) -> i64 {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis()
    }

    #[test]
    fn catalog_helpers() {
        assert_eq!(
            gfs_object_key("20260914", 6, 3),
            "gfs.20260914/06/atmos/gfs.t06z.pgrb2.0p25.f003"
        );
        assert_eq!(
            select_latest_gfs_cycle(utc_ms(2026, 9, 15, 4)),
            CycleRef {
                date: "20260914".into(),
                hour: 18
            }
        );
        assert_eq!(nearest_gfs_step(8.2), 8);
        assert_eq!(nearest_gfs_step(f64::NAN), 0);
        assert_eq!(nearest_gfs_step(130.0), 129);
        assert_eq!(nearest_gfs_step(1000.0), 384);
        let cycle = CycleRef {
            date: "20260915".into(),
            hour: 6,
        };
        assert_eq!(cycle_run_ms(&cycle), utc_ms(2026, 9, 15, 6));
        assert_eq!(iso(cycle_run_ms(&cycle)), "2026-09-15T06:00:00.000Z");
    }
}
