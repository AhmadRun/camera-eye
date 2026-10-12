//! Port of `server/providers/wind/gfs.js`: the NOAA GFS 0.25° inventory and
//! the 10 m wind (plus optional 2 m temperature / MSL pressure) snapshot.

use super::WindError;
use super::body::{ByteRange, fetch_text};
use super::catalog::{
    GFS_BUCKET, cycle_run_ms, gfs_object_key, nearest_gfs_step, select_latest_gfs_cycle,
};
use super::grid::Overlay;
use super::{Deps, ModelValue, Ranges, load_snapshot};

/// One `.idx` record.
#[derive(Debug, Clone, PartialEq)]
pub struct IdxMessage {
    pub index: f64,
    pub offset: f64,
    pub variable: String,
    pub level: String,
}

fn all_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

/// Parse a GFS `.idx` inventory into ordered message records.
///
/// Each line is `msgNumber:byteOffset:d=YYYYMMDDHH:VAR:LEVEL:...:`. Malformed
/// lines are skipped; a payload with no valid lines is rejected so callers
/// never treat an HTML error page as an empty inventory.
pub fn parse_gfs_idx(text: &str) -> Result<Vec<IdxMessage>, WindError> {
    let mut messages = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() < 6
            || !all_digits(parts[0])
            || !all_digits(parts[1])
            || parts[3].is_empty()
            || parts[4].is_empty()
        {
            continue;
        }
        messages.push(IdxMessage {
            index: parts[0].parse().unwrap_or(f64::NAN),
            offset: parts[1].parse().unwrap_or(f64::NAN),
            variable: parts[3].to_string(),
            level: parts[4].to_string(),
        });
    }
    if messages.is_empty() {
        return Err(WindError::new("malformed GFS index"));
    }
    Ok(messages)
}

/// Byte ranges for the two 10 m wind components and the overlay's scalar.
/// A message runs from its own offset to one byte before the next message
/// in file order.
pub fn wind_message_ranges(
    messages: &[IdxMessage],
    level: &str,
    overlay: Overlay,
) -> Result<Ranges, WindError> {
    let range = |variable: &str, field_level: &str| -> Result<ByteRange, WindError> {
        let index = messages
            .iter()
            .position(|m| m.variable == variable && m.level == field_level);
        match index.and_then(|i| Some((messages.get(i)?, messages.get(i + 1)?))) {
            Some((message, next)) => Ok(ByteRange {
                start: message.offset,
                end: next.offset - 1.0,
            }),
            None => Err(WindError::new(format!(
                "missing {variable} at {field_level}"
            ))),
        }
    };
    Ok(Ranges {
        u: range("UGRD", level)?,
        v: range("VGRD", level)?,
        scalar: match overlay {
            Overlay::None => None,
            Overlay::Temperature => Some(range("TMP", "2 m above ground")?),
            Overlay::Pressure => Some(range("PRMSL", "mean sea level")?),
        },
    })
}

pub const WIND_LEVEL: &str = "10 m above ground";

/// Fetch and decode the GFS 10 m wind field valid closest to now.
pub async fn fetch_gfs_wind(deps: Deps, overlay: Overlay) -> Result<ModelValue, WindError> {
    let now_ms = (deps.now)();
    let cycle = select_latest_gfs_cycle(now_ms);
    let run_ms = cycle_run_ms(&cycle);
    // Pick the forecast step whose valid time is closest to now, so the layer
    // shows the freshest field the cycle offers instead of the analysis.
    let forecast_hour = nearest_gfs_step((now_ms - run_ms) as f64 / 3_600_000.0);
    let base = format!(
        "https://{GFS_BUCKET}.s3.amazonaws.com/{}",
        gfs_object_key(&cycle.date, cycle.hour, forecast_hour)
    );
    let index = fetch_text(&*deps.fetch, &format!("{base}.idx")).await?;
    let inventory = parse_gfs_idx(&index)?;
    let mut ranges = wind_message_ranges(&inventory, WIND_LEVEL, Overlay::None)?;
    if overlay != Overlay::None {
        // A missing optional field does not discard valid wind.
        ranges.scalar = wind_message_ranges(&inventory, WIND_LEVEL, overlay)
            .ok()
            .and_then(|r| r.scalar);
    }
    // All selected messages come from this one issue/valid-time object.
    load_snapshot(&deps, overlay, cycle, run_ms, forecast_hour, &base, ranges).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::wind::body::tests::{FakeFetch, range_start, response};
    use crate::providers::wind::grid::Decoded;
    use crate::providers::wind::tests::{deps_with, utc_ms};
    use std::sync::Arc;

    #[test]
    fn index_helpers() {
        let p = parse_gfs_idx(
            "1:10:d=x:UGRD:10 m above ground:anl:\n2:20:d=x:VGRD:10 m above ground:anl:\n3:30:d=x:X:y:z",
        )
        .unwrap();
        let ranges = wind_message_ranges(&p, WIND_LEVEL, Overlay::None).unwrap();
        assert_eq!(
            (ranges.u, ranges.v, ranges.scalar),
            (
                ByteRange {
                    start: 10.0,
                    end: 19.0
                },
                ByteRange {
                    start: 20.0,
                    end: 29.0
                },
                None
            )
        );
        assert!(wind_message_ranges(&p[..1], WIND_LEVEL, Overlay::None).is_err());
        assert!(parse_gfs_idx("<html>error</html>").is_err());
        assert!(parse_gfs_idx("1:x:d:U:L:z\r\n").is_err());
    }

    const WEATHER_IDX: &str = "1:0:d=2026091506:UGRD:10 m above ground:6 hour fcst:\n\
2:2:d=2026091506:VGRD:10 m above ground:6 hour fcst:\n\
3:4:d=2026091506:TMP:850 mb:6 hour fcst:\n\
4:6:d=2026091506:TMP:2 m above ground:6 hour fcst:\n\
5:8:d=2026091506:PRMSL:mean sea level:6 hour fcst:\n\
6:10:d=2026091506:HGT:surface:6 hour fcst:";

    fn fake_decoded(values: f64, units: &str, lo1: f64) -> Decoded {
        Decoded {
            ni: 4,
            nj: 3,
            lo1,
            la1: 90.0,
            di: 90.0,
            dj: 90.0,
            values: vec![values; 12],
            units: Some(units.to_string()),
        }
    }

    #[tokio::test]
    async fn overlays_select_exact_levels_from_the_same_object() {
        let parsed = parse_gfs_idx(WEATHER_IDX).unwrap();
        assert_eq!(
            wind_message_ranges(&parsed, WIND_LEVEL, Overlay::Temperature)
                .unwrap()
                .scalar,
            Some(ByteRange {
                start: 6.0,
                end: 7.0
            })
        );
        assert_eq!(
            wind_message_ranges(&parsed, WIND_LEVEL, Overlay::Pressure)
                .unwrap()
                .scalar,
            Some(ByteRange {
                start: 8.0,
                end: 9.0
            })
        );
        for overlay in [Overlay::None, Overlay::Temperature, Overlay::Pressure] {
            let fetch = FakeFetch::new(|url, range| {
                if url.ends_with(".idx") {
                    return Ok(response(200, WEATHER_IDX));
                }
                Ok(response(206, vec![range_start(range) as u8, 0]))
            });
            let deps = deps_with(
                fetch.clone(),
                utc_ms(2026, 9, 15, 12),
                Arc::new(|bytes| {
                    let value = match bytes[0] {
                        6 => fake_decoded(293.15, "K", 0.0),
                        8 => fake_decoded(101325.0, "Pa", 0.0),
                        n => fake_decoded(f64::from(n), "m/s", 0.0),
                    };
                    Box::pin(async move { Ok(value) })
                }),
                90.0,
            );
            let result = fetch_gfs_wind(deps, overlay).await.unwrap();
            let calls = fetch.calls.lock().clone();
            assert_eq!(calls.len(), if overlay == Overlay::None { 3 } else { 4 });
            assert!(calls[1..].iter().all(|c| c.url == calls[1].url));
            assert!(calls[0].url.ends_with(".f006.idx"));
            assert_eq!(
                result.cycle.valid_iso.as_deref(),
                Some("2026-09-15T12:00:00.000Z")
            );
            match overlay {
                Overlay::None => assert!(result.scalar.is_none()),
                _ => {
                    let expected = if overlay == Overlay::Temperature {
                        20.0
                    } else {
                        1013.25
                    };
                    let scalar = result.grid.scalar.unwrap();
                    assert!((f64::from(scalar[0]) - expected).abs() < 0.0001);
                }
            }
        }
        let missing =
            parse_gfs_idx(&WEATHER_IDX.replace("TMP:2 m above ground", "TMP:surface")).unwrap();
        let error = wind_message_ranges(&missing, WIND_LEVEL, Overlay::Temperature).unwrap_err();
        assert!(error.0.contains("missing TMP"));
    }

    #[tokio::test]
    async fn preserves_wind_when_optional_temperature_is_missing_or_malformed() {
        for failure in ["missing", "fetch", "decode", "geometry", "units"] {
            let fetch = FakeFetch::new(move |url, range| {
                if url.ends_with(".idx") {
                    return Ok(response(
                        200,
                        if failure == "missing" {
                            WEATHER_IDX.replace("TMP:2 m above ground", "TMP:surface")
                        } else {
                            WEATHER_IDX.to_string()
                        },
                    ));
                }
                let start = range_start(range);
                if start == 6 && failure == "fetch" {
                    return Err(WindError::new("private provider failure"));
                }
                Ok(response(206, vec![start as u8, 0]))
            });
            let deps = deps_with(
                fetch,
                utc_ms(2026, 9, 15, 12),
                Arc::new(move |bytes| {
                    let first = bytes[0];
                    Box::pin(async move {
                        if first == 6 && failure == "decode" {
                            return Err(WindError::new("bad message"));
                        }
                        let lo1 = if first == 6 && failure == "geometry" {
                            90.0
                        } else {
                            0.0
                        };
                        let units = if first == 6 && failure != "units" {
                            "K"
                        } else {
                            "m/s"
                        };
                        let value = if first == 6 { 293.15 } else { f64::from(first) };
                        Ok(fake_decoded(value, units, lo1))
                    })
                }),
                90.0,
            );
            let result = fetch_gfs_wind(deps, Overlay::Temperature).await.unwrap();
            assert!(result.scalar.is_none(), "{failure}");
            assert!(result.grid.scalar.is_none());
            assert_eq!(
                result.scalar_error.as_deref(),
                Some("Temperature field unavailable")
            );
            assert_eq!(result.grid.v, vec![2.0; 12]);
            assert_eq!(
                result.cycle.valid_iso.as_deref(),
                Some("2026-09-15T12:00:00.000Z")
            );
        }
    }
}
