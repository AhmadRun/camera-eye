//! Port of `server/providers/wind/ifs.js`: the keyless ECMWF Open Data IFS
//! 0.25° inventory and the 10 m wind (plus optional 2t / msl) snapshot.

use serde_json::Value;

use super::WindError;
use super::body::{ByteRange, fetch_text};
use super::catalog::{CycleRef, cycle_run_ms, select_cycle};
use super::grid::Overlay;
use super::{Deps, ModelValue, Ranges, load_snapshot};

/// Keyless ECMWF Open Data root for real-time IFS forecasts.
pub const IFS_BASE: &str = "https://data.ecmwf.int/forecasts";

/// Select the latest IFS cycle that should be available (6 h lag).
pub fn select_latest_ifs_cycle(now_ms: i64) -> CycleRef {
    select_cycle(now_ms, 6 * 3_600_000)
}

/// The IFS GRIB2 and `.index` URLs for one cycle and step.
pub struct IfsUrls {
    pub grib: String,
    pub index: String,
}

/// Build the IFS GRIB2 and `.index` URLs for one cycle and step.
pub fn ifs_object_urls(date: &str, hour: u32, step: u32) -> IfsUrls {
    let stem =
        format!("{IFS_BASE}/{date}/{hour:02}z/ifs/0p25/oper/{date}{hour:02}0000-{step}h-oper-fc");
    // The inventory sits beside the GRIB2 as `<stem>.index`, NOT `<grib2>.index`.
    IfsUrls {
        grib: format!("{stem}.grib2"),
        index: format!("{stem}.index"),
    }
}

/// One inventory record. `param` keeps the raw JSON value: any truthy value
/// counts as a record, but only a string can match a wanted parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct IfsEntry {
    pub param: Value,
    pub offset: f64,
    pub length: f64,
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Parse the IFS JSON Lines inventory. Malformed lines are skipped; a
/// payload with no valid lines is rejected.
pub fn parse_ifs_index(text: &str) -> Result<Vec<IfsEntry>, WindError> {
    let mut entries = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue; // non-JSON line in the index
        };
        let param = value.get("param").cloned().unwrap_or(Value::Null);
        let number = |key: &str| value.get(key).and_then(Value::as_f64);
        if let (true, Some(offset), Some(length)) =
            (truthy(&param), number("_offset"), number("_length"))
        {
            entries.push(IfsEntry {
                param,
                offset,
                length,
            });
        }
    }
    if entries.is_empty() {
        return Err(WindError::new("malformed IFS index"));
    }
    Ok(entries)
}

/// Byte ranges for the IFS 10 m wind components and the overlay's scalar.
pub fn ifs_wind_ranges(entries: &[IfsEntry], overlay: Overlay) -> Result<Ranges, WindError> {
    let range = |param: &str| -> Result<ByteRange, WindError> {
        let item = entries
            .iter()
            .find(|entry| entry.param.as_str() == Some(param))
            .ok_or_else(|| WindError::new(format!("missing {param}")))?;
        Ok(ByteRange {
            start: item.offset,
            end: item.offset + item.length - 1.0,
        })
    };
    Ok(Ranges {
        u: range("10u")?,
        v: range("10v")?,
        scalar: match overlay {
            Overlay::None => None,
            Overlay::Temperature => Some(range("2t")?),
            Overlay::Pressure => Some(range("msl")?),
        },
    })
}

/// Round a time offset to the nearest published IFS forecast step. IFS 0.25°
/// oper is 3-hourly through f144, then 6-hourly to f360 for 00/12z.
pub fn nearest_ifs_step(hours: f64, cycle_hour: u32) -> u32 {
    let max = if cycle_hour == 6 || cycle_hour == 18 {
        144.0
    } else {
        360.0
    };
    let value = if hours.is_finite() { hours } else { 0.0 }.max(0.0);
    let step = if value <= 144.0 {
        (value / 3.0).round() * 3.0
    } else {
        (value / 6.0).round() * 6.0
    };
    step.clamp(0.0, max) as u32
}

/// Fetch and decode the IFS 10 m wind field valid closest to now.
pub async fn fetch_ifs_wind(deps: Deps, overlay: Overlay) -> Result<ModelValue, WindError> {
    let now_ms = (deps.now)();
    let cycle = select_latest_ifs_cycle(now_ms);
    let run_ms = cycle_run_ms(&cycle);
    // Show the field valid closest to now rather than always the analysis.
    let forecast_hour = nearest_ifs_step((now_ms - run_ms) as f64 / 3_600_000.0, cycle.hour);
    let urls = ifs_object_urls(&cycle.date, cycle.hour, forecast_hour);
    let index = fetch_text(&*deps.fetch, &urls.index).await?;
    let inventory = parse_ifs_index(&index)?;
    let mut ranges = ifs_wind_ranges(&inventory, Overlay::None)?;
    if overlay != Overlay::None {
        // A missing optional field does not discard valid wind.
        ranges.scalar = ifs_wind_ranges(&inventory, overlay)
            .ok()
            .and_then(|r| r.scalar);
    }
    // All selected messages come from this one issue/valid-time object.
    load_snapshot(
        &deps,
        overlay,
        cycle,
        run_ms,
        forecast_hour,
        &urls.grib,
        ranges,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::wind::body::tests::{FakeFetch, range_start, response};
    use crate::providers::wind::catalog::nearest_gfs_step;
    use crate::providers::wind::grid::Decoded;
    use crate::providers::wind::tests::{deps_with, utc_ms};
    use std::sync::Arc;

    #[test]
    fn inventory_selects_only_10m_components_and_distinct_index_suffix() {
        let urls = ifs_object_urls("20260915", 6, 9);
        assert!(urls.index.ends_with("9h-oper-fc.index"));
        assert!(!urls.index.contains(".grib2.index"));
        let ranges = ifs_wind_ranges(
            &parse_ifs_index(
                "{\"param\":\"10u\",\"_offset\":20,\"_length\":10}\n{\"param\":\"10v\",\"_offset\":40,\"_length\":15}",
            )
            .unwrap(),
            Overlay::None,
        )
        .unwrap();
        assert_eq!(
            (ranges.u, ranges.v),
            (
                ByteRange {
                    start: 20.0,
                    end: 29.0
                },
                ByteRange {
                    start: 40.0,
                    end: 54.0
                }
            )
        );
        assert_eq!(
            select_latest_ifs_cycle(utc_ms(2026, 9, 15, 4)),
            CycleRef {
                date: "20260914".into(),
                hour: 18
            }
        );
        assert_eq!(nearest_ifs_step(8.0, 6), 9);
        assert_eq!(nearest_ifs_step(200.0, 6), 144);
        assert_eq!(nearest_ifs_step(151.0, 0), 150);
        assert_eq!(nearest_gfs_step(8.2), 8);
        assert!(parse_ifs_index("not json\n{\"param\":\"\",\"_offset\":1,\"_length\":1}").is_err());
        assert!(parse_ifs_index("{\"param\":\"10u\",\"_offset\":\"1\",\"_length\":1}").is_err());
    }

    fn entries_text(params: &[&str]) -> String {
        params
            .iter()
            .enumerate()
            .map(|(i, param)| {
                serde_json::json!({ "param": param, "_offset": i * 2, "_length": 2 }).to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn fake(value: f64, units: Option<&str>) -> Decoded {
        Decoded {
            ni: 4,
            nj: 3,
            lo1: 0.0,
            la1: 90.0,
            di: 90.0,
            dj: 90.0,
            values: vec![value; 12],
            units: units.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn companions_use_the_same_issue_and_valid_object_as_the_wind() {
        let index = entries_text(&["10u", "10v", "2t", "msl"]);
        for overlay in [Overlay::None, Overlay::Temperature, Overlay::Pressure] {
            let text = index.clone();
            let fetch = FakeFetch::new(move |url, range| {
                if url.ends_with(".index") {
                    return Ok(response(200, text.clone()));
                }
                Ok(response(206, vec![range_start(range) as u8, 0]))
            });
            let deps = deps_with(
                fetch.clone(),
                utc_ms(2026, 9, 15, 14),
                Arc::new(|bytes| {
                    let value = match bytes[0] {
                        4 => fake(283.15, Some("K")),
                        6 => fake(100000.0, Some("Pa")),
                        n => fake(f64::from(n), Some("m/s")),
                    };
                    Box::pin(async move { Ok(value) })
                }),
                90.0,
            );
            let result = fetch_ifs_wind(deps, overlay).await.unwrap();
            let calls = fetch.calls.lock().clone();
            assert_eq!(calls.len(), if overlay == Overlay::None { 3 } else { 4 });
            assert!(calls[1..].iter().all(|c| c.url == calls[1].url));
            assert!(calls[0].url.ends_with("-9h-oper-fc.index"));
            assert_eq!(
                result.cycle.run_iso.as_deref(),
                Some("2026-09-15T06:00:00.000Z")
            );
            assert_eq!(
                result.cycle.valid_iso.as_deref(),
                Some("2026-09-15T15:00:00.000Z")
            );
            match overlay {
                Overlay::None => assert!(result.scalar.is_none()),
                _ => {
                    let expected = if overlay == Overlay::Temperature {
                        10.0
                    } else {
                        1000.0
                    };
                    let scalar = result.grid.scalar.unwrap();
                    assert!((f64::from(scalar[0]) - expected).abs() < 0.0001);
                }
            }
        }
        let error = ifs_wind_ranges(
            &parse_ifs_index(&entries_text(&["10u", "10v"])).unwrap(),
            Overlay::Pressure,
        )
        .unwrap_err();
        assert!(error.0.contains("missing msl"));
    }

    #[tokio::test]
    async fn optional_pressure_refusal_preserves_same_cycle_wind() {
        let index = entries_text(&["10u", "10v", "msl"]);
        let fetch = FakeFetch::new(move |url, range| {
            if url.ends_with(".index") {
                return Ok(response(200, index.clone()));
            }
            let start = range_start(range);
            if start == 4 {
                return Ok(response(503, "Unavailable"));
            }
            Ok(response(206, vec![start as u8, 0]))
        });
        let deps = deps_with(
            fetch,
            utc_ms(2026, 9, 15, 14),
            Arc::new(|bytes| {
                let value = fake(f64::from(bytes[0]), None);
                Box::pin(async move { Ok(value) })
            }),
            90.0,
        );
        let result = fetch_ifs_wind(deps, Overlay::Pressure).await.unwrap();
        assert_eq!(
            result.scalar_error.as_deref(),
            Some("Mean sea level pressure field unavailable")
        );
        assert!(result.grid.scalar.is_none());
        assert_eq!(result.grid.v, vec![2.0; 12]);
        assert_eq!(
            result.cycle.valid_iso.as_deref(),
            Some("2026-09-15T15:00:00.000Z")
        );
    }
}
