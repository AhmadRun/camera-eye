//! Port of `server/providers/wind/grid.js`: resample a decoded GRIB field onto
//! the compact equirectangular grid the browser renders, plus the optional
//! same-cycle temperature / pressure companion.

use super::WindError;

/// The `overlay` query parameter: wind alone or wind plus one scalar field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Overlay {
    None,
    Temperature,
    Pressure,
}

impl Overlay {
    /// `weatherScalarMetadata` validation: only the three known names.
    pub fn parse(value: &str) -> Result<Self, WindError> {
        match value {
            "none" => Ok(Overlay::None),
            "temperature" => Ok(Overlay::Temperature),
            "pressure" => Ok(Overlay::Pressure),
            _ => Err(WindError::new("Unknown weather overlay")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Overlay::None => "none",
            Overlay::Temperature => "temperature",
            Overlay::Pressure => "pressure",
        }
    }
}

/// `{ kind, units, level }` describing the scalar companion.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ScalarMeta {
    pub kind: String,
    pub units: String,
    pub level: String,
}

/// `weatherScalarMetadata(overlay)`.
pub fn weather_scalar_metadata(overlay: Overlay) -> Option<ScalarMeta> {
    let (units, level) = match overlay {
        Overlay::None => return None,
        Overlay::Temperature => ("°C", "2 m above ground"),
        Overlay::Pressure => ("hPa", "mean sea level"),
    };
    Some(ScalarMeta {
        kind: overlay.as_str().to_string(),
        units: units.to_string(),
        level: level.to_string(),
    })
}

/// `weatherScalarError(overlay)` (`None` has no companion to lose).
pub fn weather_scalar_error(overlay: Overlay) -> Option<&'static str> {
    match overlay {
        Overlay::None => None,
        Overlay::Temperature => Some("Temperature field unavailable"),
        Overlay::Pressure => Some("Mean sea level pressure field unavailable"),
    }
}

/// One decoded GRIB message (the `decodeWindGribMessage` result).
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub ni: i64,
    pub nj: i64,
    pub lo1: f64,
    pub la1: f64,
    pub di: f64,
    pub dj: f64,
    pub values: Vec<f64>,
    pub units: Option<String>,
}

/// A resampled grid; `u`, `v` and `scalar` are row-major from (0°E, 90°N).
#[derive(Debug, Clone, PartialEq)]
pub struct Grid {
    pub nx: usize,
    pub ny: usize,
    pub lo1: f64,
    pub la1: f64,
    pub dx: f64,
    pub dy: f64,
    pub u: Vec<f32>,
    pub v: Vec<f32>,
    pub scalar: Option<Vec<f32>>,
}

/// Inputs of `resampleWindGrid`.
pub struct ResampleInput<'a> {
    pub u: &'a [f64],
    pub v: &'a [f64],
    pub scalar: Option<&'a [f64]>,
    pub scalar_scale: f64,
    pub scalar_offset: f64,
    pub ni: i64,
    pub nj: i64,
    pub lo1: f64,
    pub la1: f64,
    pub di: f64,
    pub dj: f64,
    pub dx: f64,
    pub dy: f64,
}

/// Resample a GRIB wind grid into a compact equirectangular grid.
///
/// Source grids are north-to-south (`la1` at the top, `dj` increasing toward
/// the south) and start at `lo1`. Longitude wraps across the seam; latitude
/// clamps.
pub fn resample_wind_grid(input: &ResampleInput) -> Result<Grid, WindError> {
    let ResampleInput {
        u,
        v,
        scalar,
        scalar_scale,
        scalar_offset,
        ni,
        nj,
        lo1,
        la1,
        di,
        dj,
        dx,
        dy,
    } = *input;
    let points = ni.checked_mul(nj);
    let geometry_ok = ni >= 1
        && nj >= 1
        && points.is_some_and(|p| p <= 2_000_000)
        && points.is_some_and(|p| {
            u.len() as i64 == p && v.len() as i64 == p && scalar.is_none_or(|s| s.len() as i64 == p)
        })
        && [lo1, la1, di, dj, dx, dy, scalar_scale, scalar_offset]
            .iter()
            .all(|n| n.is_finite())
        && di > 0.0
        && dj > 0.0
        && (0.25..=180.0).contains(&dx)
        && (0.25..=180.0).contains(&dy);
    if !geometry_ok {
        return Err(WindError::new("Invalid wind grid geometry"));
    }
    for values in [Some(u), Some(v), scalar].into_iter().flatten() {
        if !values.iter().all(|n| n.is_finite()) {
            return Err(WindError::new("Invalid wind grid values"));
        }
    }
    let nx = (360.0 / dx).round() as usize;
    let ny = (180.0 / dy).round() as usize + 1;
    if nx * ny > 1_000_000 {
        return Err(WindError::new("Resampled wind grid exceeds point budget"));
    }
    let (ni_u, nj_u) = (ni as usize, nj as usize);
    let (nif, njf) = (ni as f64, nj as f64);
    let sample = |source: &[f64], lon: f64, lat: f64| -> f64 {
        let x = ((lon - lo1) / di + nif) % nif;
        let y = ((la1 - lat) / dj).min(njf - 1.0).max(0.0);
        let x0 = x.floor();
        let fx = x - x0;
        let x0 = x0 as usize;
        let x1 = (x0 + 1) % ni_u;
        let y0 = y.floor();
        let fy = y - y0;
        let y0 = y0 as usize;
        let y1 = (nj_u - 1).min(y0 + 1);
        let top = source[y0 * ni_u + x0] * (1.0 - fx) + source[y0 * ni_u + x1] * fx;
        let bottom = source[y1 * ni_u + x0] * (1.0 - fx) + source[y1 * ni_u + x1] * fx;
        top * (1.0 - fy) + bottom * fy
    };
    let mut out_u = vec![0f32; nx * ny];
    let mut out_v = vec![0f32; nx * ny];
    let mut out_scalar = scalar.map(|_| vec![0f32; nx * ny]);
    for y in 0..ny {
        for x in 0..nx {
            let index = y * nx + x;
            let lon = x as f64 * dx;
            let lat = 90.0 - y as f64 * dy;
            out_u[index] = sample(u, lon, lat) as f32;
            out_v[index] = sample(v, lon, lat) as f32;
            if let (Some(out), Some(source)) = (out_scalar.as_mut(), scalar) {
                out[index] = (sample(source, lon, lat) * scalar_scale + scalar_offset) as f32;
            }
        }
    }
    for values in [Some(&out_u), Some(&out_v), out_scalar.as_ref()]
        .into_iter()
        .flatten()
    {
        if !values.iter().all(|n| n.is_finite()) {
            return Err(WindError::new("Invalid resampled wind values"));
        }
    }
    Ok(Grid {
        nx,
        ny,
        lo1: 0.0,
        la1: 90.0,
        dx,
        dy,
        u: out_u,
        v: out_v,
        scalar: out_scalar,
    })
}

/// `resampleWeatherFields` / `resampleWeatherSnapshot` result.
#[derive(Debug, Clone, PartialEq)]
pub struct Fields {
    pub grid: Grid,
    pub scalar: Option<ScalarMeta>,
    pub scalar_error: Option<&'static str>,
}

/// Inputs shared by the weather resamplers.
#[derive(Clone, Copy)]
pub struct WeatherInput<'a> {
    pub u: Option<&'a Decoded>,
    pub v: Option<&'a Decoded>,
    pub scalar: Option<&'a Decoded>,
    pub overlay: Overlay,
    pub target_dx: f64,
}

/// Reject shifted/mismatched components before sharing U's grid geometry.
pub fn resample_weather_fields(input: WeatherInput) -> Result<Fields, WindError> {
    let metadata = weather_scalar_metadata(input.overlay);
    let mismatched = || WindError::new("Mismatched weather grid geometry");
    let u = input.u.ok_or_else(mismatched)?;
    let mut fields = vec![input.u, input.v];
    if metadata.is_some() {
        fields.push(input.scalar);
    }
    for field in fields {
        let field = field.ok_or_else(mismatched)?;
        // `!==` semantics: NaN never equals itself.
        #[allow(clippy::float_cmp)]
        let same = field.ni == u.ni
            && field.nj == u.nj
            && field.lo1 == u.lo1
            && field.la1 == u.la1
            && field.di == u.di
            && field.dj == u.dj;
        if !same {
            return Err(mismatched());
        }
    }
    let v = input.v.ok_or_else(mismatched)?;
    let scalar = if metadata.is_some() {
        let scalar = input.scalar.ok_or_else(mismatched)?;
        let expected = if input.overlay == Overlay::Temperature {
            "K"
        } else {
            "Pa"
        };
        if scalar.units.as_deref() != Some(expected) {
            return Err(WindError::new("Unexpected weather scalar units"));
        }
        Some(scalar)
    } else {
        None
    };
    let grid = resample_wind_grid(&ResampleInput {
        u: &u.values,
        v: &v.values,
        scalar: scalar.map(|s| s.values.as_slice()),
        scalar_scale: if input.overlay == Overlay::Pressure {
            0.01
        } else {
            1.0
        },
        scalar_offset: if input.overlay == Overlay::Temperature {
            -273.15
        } else {
            0.0
        },
        ni: u.ni,
        nj: u.nj,
        lo1: u.lo1,
        la1: u.la1,
        di: u.di,
        dj: u.dj,
        dx: input.target_dx,
        dy: input.target_dx,
    })?;
    Ok(Fields {
        grid,
        scalar: metadata,
        scalar_error: None,
    })
}

/// Preserve usable same-cycle wind when only its optional companion fails.
pub fn resample_weather_snapshot(input: WeatherInput) -> Result<Fields, WindError> {
    match resample_weather_fields(input) {
        Ok(fields) => Ok(fields),
        Err(error) if input.overlay == Overlay::None => Err(error),
        Err(_) => {
            // Revalidate wind alone: a wind defect must never become a partial success.
            let wind = resample_weather_fields(WeatherInput {
                overlay: Overlay::None,
                ..input
            })?;
            Ok(Fields {
                scalar_error: weather_scalar_error(input.overlay),
                ..wind
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resamples_and_wraps_a_wind_grid() {
        let u: Vec<f64> = (0..12).map(f64::from).collect();
        let v = vec![1.0; 12];
        let r = resample_wind_grid(&ResampleInput {
            u: &u,
            v: &v,
            scalar: None,
            scalar_scale: 1.0,
            scalar_offset: 0.0,
            ni: 4,
            nj: 3,
            lo1: 0.0,
            la1: 90.0,
            di: 90.0,
            dj: 90.0,
            dx: 90.0,
            dy: 90.0,
        })
        .unwrap();
        assert_eq!((r.nx, r.ny), (4, 3));
        assert_eq!(r.u[0], 0.0);
        assert_eq!(r.u[3], 3.0);
        assert_eq!(r.u[4], 4.0);
    }

    pub(crate) fn component(value: f64, units: Option<&str>) -> Decoded {
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

    fn input<'a>(
        u: &'a Decoded,
        v: &'a Decoded,
        scalar: Option<&'a Decoded>,
        overlay: Overlay,
        target_dx: f64,
    ) -> WeatherInput<'a> {
        WeatherInput {
            u: Some(u),
            v: Some(v),
            scalar,
            overlay,
            target_dx,
        }
    }

    fn err_matches(result: Result<Fields, WindError>, needle: &str) {
        match result {
            Err(error) => assert!(error.0.contains(needle), "{} !~ {needle}", error.0),
            Ok(_) => panic!("expected error matching {needle}"),
        }
    }

    #[test]
    fn converts_temperature_and_msl_pressure_on_the_wind_geometry() {
        let u = component(3.0, None);
        let v = component(-4.0, None);
        for (overlay, raw, units, expected, out_units) in [
            (Overlay::Temperature, 293.15, "K", 20.0, "°C"),
            (Overlay::Pressure, 101325.0, "Pa", 1013.25, "hPa"),
        ] {
            let scalar = component(raw, Some(units));
            let result =
                resample_weather_fields(input(&u, &v, Some(&scalar), overlay, 90.0)).unwrap();
            let meta = result.scalar.unwrap();
            assert_eq!(meta.kind, overlay.as_str());
            assert_eq!(meta.units, out_units);
            assert_eq!(result.grid.u, vec![3.0; 12]);
            assert_eq!(result.grid.v, vec![-4.0; 12]);
            assert!(
                result
                    .grid
                    .scalar
                    .unwrap()
                    .iter()
                    .all(|n| (f64::from(*n) - expected).abs() < 0.0001)
            );
        }
        let none = resample_weather_fields(input(&u, &v, None, Overlay::None, 90.0)).unwrap();
        assert!(none.grid.scalar.is_none());
    }

    #[test]
    fn components_cannot_share_a_grid_when_shifted_truncated_or_non_finite() {
        let u = component(1.0, None);
        let v = component(2.0, None);
        let scalar = component(273.15, Some("K"));
        for bad in [
            Decoded {
                lo1: 90.0,
                ..v.clone()
            },
            Decoded { ni: 3, ..v.clone() },
            Decoded {
                dj: 45.0,
                ..v.clone()
            },
        ] {
            err_matches(
                resample_weather_fields(input(&u, &bad, None, Overlay::None, 1.0)),
                "Mismatched",
            );
        }
        let shifted = Decoded {
            la1: 89.0,
            ..scalar.clone()
        };
        err_matches(
            resample_weather_fields(input(&u, &v, Some(&shifted), Overlay::Temperature, 1.0)),
            "Mismatched",
        );
        let celsius = Decoded {
            units: Some("°C".into()),
            ..scalar.clone()
        };
        err_matches(
            resample_weather_fields(input(&u, &v, Some(&celsius), Overlay::Temperature, 1.0)),
            "units",
        );
        let short = Decoded {
            values: vec![1.0],
            ..scalar.clone()
        };
        err_matches(
            resample_weather_fields(input(&u, &v, Some(&short), Overlay::Temperature, 1.0)),
            "geometry",
        );
        let mut invalid = component(1.0, None);
        invalid.values[5] = f64::NAN;
        err_matches(
            resample_weather_fields(input(&u, &invalid, None, Overlay::None, 180.0)),
            "values",
        );
        let invalid_scalar = Decoded {
            values: invalid.values.clone(),
            ..scalar.clone()
        };
        err_matches(
            resample_weather_fields(input(
                &u,
                &v,
                Some(&invalid_scalar),
                Overlay::Temperature,
                1.0,
            )),
            "values",
        );
        assert!(
            Overlay::parse("dewpoint")
                .unwrap_err()
                .0
                .contains("Unknown")
        );
    }

    #[test]
    fn optional_degradation_cannot_hide_invalid_wind_geometry() {
        let u = component(1.0, None);
        let v = component(2.0, None);
        let result =
            resample_weather_snapshot(input(&u, &v, None, Overlay::Pressure, 90.0)).unwrap();
        assert_eq!(
            result.scalar_error,
            Some("Mean sea level pressure field unavailable")
        );
        assert!(result.grid.scalar.is_none());
        assert!(result.scalar.is_none());
        let shifted = Decoded {
            la1: 89.0,
            ..v.clone()
        };
        err_matches(
            resample_weather_snapshot(input(&u, &shifted, None, Overlay::Pressure, 1.0)),
            "Mismatched",
        );
        let budget = resample_wind_grid(&ResampleInput {
            u: &u.values,
            v: &v.values,
            scalar: None,
            scalar_scale: 1.0,
            scalar_offset: 0.0,
            ni: 4,
            nj: 3,
            lo1: 0.0,
            la1: 90.0,
            di: 90.0,
            dj: 90.0,
            dx: 0.25,
            dy: 0.25,
        });
        assert!(budget.unwrap_err().0.contains("budget"));
    }
}
