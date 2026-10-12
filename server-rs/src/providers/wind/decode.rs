//! Port of `server/providers/wind/decode.js`. The JS decodes with ecCodes
//! compiled to WASM; this uses the pure-Rust `grib` crate, which covers the
//! data representation templates the two models publish: complex packing
//! with spatial differencing (5.3) for GFS and CCSDS (5.42) for ECMWF IFS,
//! plus simple (5.0), complex (5.2), JPEG 2000 (5.40) and PNG (5.41).

use grib::{Grib2SubmessageDecoder, GridDefinitionTemplateValues};

use super::WindError;
use super::grid::Decoded;

/// Largest message accepted (`8 * 1024 * 1024` in the JS).
pub const MAX_GRIB_BYTES: usize = 8 * 1024 * 1024;

/// ecCodes' default `missingValue` for bitmapped (absent) points.
const ECCODES_MISSING_VALUE: f64 = 9999.0;

/// ecCodes' `GRIB_MISSING_DOUBLE`, returned for a missing increment.
const ECCODES_MISSING_DOUBLE: f64 = -1e100;

/// ecCodes `units` for the parameters the wind layer reads, keyed by
/// (discipline, parameter category, parameter number).
fn parameter_units(discipline: u8, category: Option<u8>, number: Option<u8>) -> &'static str {
    match (discipline, category, number) {
        // 2t / TMP
        (0, Some(0), Some(0)) => "K",
        // sp / PRES and msl / PRMSL
        (0, Some(3), Some(0 | 1)) => "Pa",
        // 10u / UGRD and 10v / VGRD
        (0, Some(2), Some(2 | 3)) => "m s**-1",
        _ => "unknown",
    }
}

/// Decode one bounded GRIB2 message.
pub fn decode_wind_grib_message(buffer: &[u8]) -> Result<Decoded, WindError> {
    if buffer.len() > MAX_GRIB_BYTES {
        return Err(WindError::new("Wind GRIB exceeds byte budget"));
    }
    let invalid = |_| WindError::new("Invalid wind GRIB message");
    let grib2 = grib::from_bytes(buffer).map_err(invalid)?;
    let (_, message) = grib2
        .iter()
        .next()
        .ok_or_else(|| WindError::new("Invalid wind GRIB message"))?;
    let geometry = || WindError::new("Invalid wind GRIB geometry");
    let template =
        GridDefinitionTemplateValues::try_from(message.grid_def()).map_err(|_| geometry())?;
    // Only a regular latitude/longitude grid matches the resampler's model.
    let GridDefinitionTemplateValues::Template0(template) = template else {
        return Err(geometry());
    };
    let lat_lon = &template.lat_lon;
    let grid = &lat_lon.grid;
    let (ni, nj) = (i64::from(grid.ni), i64::from(grid.nj));
    if ni < 1 || nj < 1 || ni * nj > 2_000_000 {
        return Err(geometry());
    }
    // ecCodes: degrees = value * basicAngle / subdivisions, or value / 1e6
    // when the basic angle is 0 or missing.
    let (multiplier, divisor) = match (
        grid.initial_production_domain_basic_angle,
        grid.basic_angle_subdivisions,
    ) {
        (0 | u32::MAX, _) | (_, 0 | u32::MAX) => (1.0, 1_000_000.0),
        (basic, sub) => (f64::from(basic), f64::from(sub)),
    };
    let degrees = |value: f64| value * multiplier / divisor;
    let increment = |value: u32| {
        if value == u32::MAX {
            ECCODES_MISSING_DOUBLE
        } else {
            degrees(f64::from(value))
        }
    };
    let lo1 = degrees(f64::from(grid.first_point_lon));
    let la1 = degrees(f64::from(grid.first_point_lat));
    let di = increment(lat_lon.i_direction_inc);
    let dj = increment(lat_lon.j_direction_inc);
    let discipline = message.indicator().discipline;
    let prod = message.prod_def();
    let units = parameter_units(
        discipline,
        prod.parameter_category(),
        prod.parameter_number(),
    );
    let decoder = Grib2SubmessageDecoder::from(message).map_err(invalid)?;
    let values: Vec<f64> = decoder
        .dispatch()
        .map_err(invalid)?
        .map(|value| {
            if value.is_nan() {
                ECCODES_MISSING_VALUE
            } else {
                f64::from(value)
            }
        })
        .collect();
    Ok(Decoded {
        ni,
        nj,
        lo1,
        la1,
        di,
        dj,
        values,
        units: Some(units.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_and_malformed_messages() {
        let big = vec![0u8; MAX_GRIB_BYTES + 1];
        assert!(
            decode_wind_grib_message(&big)
                .unwrap_err()
                .0
                .contains("budget")
        );
        assert!(decode_wind_grib_message(&[0u8; 20]).is_err());
        assert!(decode_wind_grib_message(b"GRIB\0\0\0\x02").is_err());
    }

    /// One reference row from ecCodes (`eccodes-wasm` 2.48.2 under Node 24).
    #[derive(serde::Deserialize)]
    struct Reference {
        file: String,
        ni: i64,
        nj: i64,
        lo1: f64,
        la1: f64,
        di: f64,
        dj: f64,
        units: String,
        n: usize,
        min: f64,
        max: f64,
        samples: Vec<f64>,
    }

    /// Compare against real GFS / IFS messages and ecCodes output. Run with
    /// `WIND_GRIB_DIR=<dir with *.grib2 and eccodes.jsonl> cargo test -- --ignored`.
    #[test]
    #[ignore = "needs downloaded GRIB2 messages"]
    fn matches_eccodes_on_real_messages() {
        let dir = std::path::PathBuf::from(std::env::var("WIND_GRIB_DIR").unwrap());
        let reference = std::fs::read_to_string(dir.join("eccodes.jsonl")).unwrap();
        let indices = [0usize, 1, 2, 1440, 500_000, 1_038_239, 777_777, 123_456];
        for line in reference.lines().filter(|l| !l.is_empty()) {
            let expected: Reference = serde_json::from_str(line).unwrap();
            let bytes = std::fs::read(dir.join(&expected.file)).unwrap();
            let decoded = decode_wind_grib_message(&bytes).unwrap();
            assert_eq!((decoded.ni, decoded.nj), (expected.ni, expected.nj));
            assert_eq!(
                (decoded.lo1, decoded.la1, decoded.di, decoded.dj),
                (expected.lo1, expected.la1, expected.di, expected.dj),
                "{}",
                expected.file
            );
            assert_eq!(decoded.units.as_deref(), Some(expected.units.as_str()));
            assert_eq!(decoded.values.len(), expected.n);
            let min = decoded.values.iter().copied().fold(f64::INFINITY, f64::min);
            let max = decoded
                .values
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            let mut worst = 0f64;
            for (i, want) in indices.iter().zip(&expected.samples) {
                let got = decoded.values[*i];
                // f32 decoding: within a couple of f32 ulps of ecCodes' double.
                let tolerance = (want.abs() * 2.0 * f64::from(f32::EPSILON)).max(1e-6);
                assert!(
                    (got - want).abs() <= tolerance,
                    "{} [{i}] {got} vs {want}",
                    expected.file
                );
                worst = worst.max((got - want).abs());
            }
            assert!((min - expected.min).abs() <= expected.min.abs() * 1e-6);
            assert!((max - expected.max).abs() <= expected.max.abs() * 1e-6);
            println!("{}: max sample error {worst:e}", expected.file);
        }
    }

    /// Compare the browser payload (decode + resample at 1°) with the JS
    /// pipeline's `<model>_<overlay>.js.bin` in `WIND_GRIB_DIR`.
    #[test]
    #[ignore = "needs downloaded GRIB2 messages"]
    fn resampled_payload_matches_the_js_pipeline() {
        use crate::providers::wind::grid::{Overlay, WeatherInput, resample_weather_snapshot};
        let dir = std::path::PathBuf::from(std::env::var("WIND_GRIB_DIR").unwrap());
        let load =
            |name: &str| decode_wind_grib_message(&std::fs::read(dir.join(name)).unwrap()).unwrap();
        for model in ["gfs", "ifs"] {
            let u = load(&format!("{model}_u.grib2"));
            let v = load(&format!("{model}_v.grib2"));
            for (overlay, file) in [(Overlay::Temperature, "t"), (Overlay::Pressure, "p")] {
                let scalar = load(&format!("{model}_{file}.grib2"));
                let fields = resample_weather_snapshot(WeatherInput {
                    u: Some(&u),
                    v: Some(&v),
                    scalar: Some(&scalar),
                    overlay,
                    target_dx: 1.0,
                })
                .unwrap();
                assert!(fields.scalar_error.is_none());
                let ours: Vec<f32> =
                    [fields.grid.u, fields.grid.v, fields.grid.scalar.unwrap()].concat();
                let js: Vec<f32> =
                    std::fs::read(dir.join(format!("{model}_{}.js.bin", overlay.as_str())))
                        .unwrap()
                        .chunks_exact(4)
                        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                        .collect();
                assert_eq!(ours.len(), js.len());
                let (mut exact, mut worst_ulps, mut worst_abs) = (0usize, 0u32, 0f32);
                for (a, b) in ours.iter().zip(&js) {
                    if a.to_bits() == b.to_bits() {
                        exact += 1;
                    }
                    worst_ulps = worst_ulps.max(a.to_bits().abs_diff(b.to_bits()));
                    worst_abs = worst_abs.max((a - b).abs());
                }
                println!(
                    "{model} {}: {exact}/{} bit-identical, worst {worst_ulps} ulp ({worst_abs:e})",
                    overlay.as_str(),
                    js.len()
                );
                assert!(worst_abs < 0.01, "{model} {}", overlay.as_str());
            }
        }
    }
}
