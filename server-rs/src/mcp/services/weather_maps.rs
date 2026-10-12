//! NOAA weather map imagery (`createWeatherSource` in
//! `src/layers/weather/source.js`).

use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;

use super::fetch::{Api, Init, Redirect};
use super::live::encode_uri_component;
use super::no_store;
use super::vector_tiles::{BBox, read_capped};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

pub const WEATHER_PRODUCTS: [&str; 4] = ["radar", "clouds", "clouds-regional", "lightning"];

fn malformed() -> Failure {
    Failure::other("Malformed weather manifest")
}

fn iso_like(time: &str) -> bool {
    let b = time.as_bytes();
    let digits = |range: std::ops::Range<usize>| range.into_iter().all(|i| b[i].is_ascii_digit());
    b.len() == 24
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && b[10] == b'T'
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19)
        && b[19] == b'.'
        && digits(20..23)
        && b[23] == b'Z'
}

/// `validateWeatherSnapshot(value, product)`.
pub fn validate_snapshot(value: Value, product: &str) -> Result<Value, Failure> {
    if !js::truthy(&value)
        || value.get("schemaVersion").and_then(Value::as_f64) != Some(1.0)
        || value.get("product").and_then(Value::as_str) != Some(product)
    {
        return Err(malformed());
    }
    if value.get("unavailable").is_some_and(js::truthy) {
        return Ok(value);
    }
    let bounds = value
        .get("bounds")
        .filter(|b| js::truthy(b))
        .ok_or_else(malformed)?;
    let edge = |key: &str| bounds.get(key).and_then(js::finite);
    let (Some(west), Some(south), Some(east), Some(north)) =
        (edge("west"), edge("south"), edge("east"), edge("north"))
    else {
        return Err(malformed());
    };
    if west < -180.0
        || east > 180.0
        || south < -90.0
        || north > 90.0
        || west >= east
        || south >= north
    {
        return Err(malformed());
    }
    let times = value
        .get("times")
        .and_then(Value::as_array)
        .ok_or_else(malformed)?;
    if times.is_empty() || times.len() > 13 {
        return Err(malformed());
    }
    let mut previous: Option<&str> = None;
    for time in times {
        let Some(time) = time.as_str() else {
            return Err(malformed());
        };
        let parsed = js::date_parse(time);
        if !iso_like(time)
            || !parsed.is_finite()
            || js::iso_string(parsed).as_deref() != Some(time)
            || previous.is_some_and(|p| js::cmp16(time, p).is_le())
        {
            return Err(malformed());
        }
        previous = Some(time);
    }
    if value.get("latest") != times.last()
        || value.get("tileSize").and_then(Value::as_f64) != Some(256.0)
        || value.get("maxLevel").and_then(Value::as_f64) != Some(6.0)
        || value.get("tilingScheme").and_then(Value::as_str) != Some("geographic")
    {
        return Err(malformed());
    }
    Ok(value)
}

fn largest(product: &str, detail: bool) -> f64 {
    if detail || product != "clouds" {
        4096.0
    } else {
        2048.0
    }
}

/// `weatherImageUrl(product, time, size, bbox)`.
pub fn image_url(
    product: &str,
    time: &str,
    size: Option<(f64, f64)>,
    bbox: Option<&BBox>,
) -> Result<String, Failure> {
    if !WEATHER_PRODUCTS.contains(&product) || !js::date_parse(time).is_finite() {
        return Err(Failure::other("Invalid weather frame"));
    }
    let mut box_param = String::new();
    if let Some(b) = bbox {
        let edges = [b.west, b.south, b.east, b.north];
        if !edges.iter().all(|v| v.is_finite()) || edges[0] >= edges[2] || edges[1] >= edges[3] {
            return Err(Failure::other("Invalid weather window"));
        }
        let joined: Vec<String> = edges.iter().map(|v| js::fmt_num(*v)).collect();
        box_param = format!("&bbox={}", joined.join(","));
    }
    let largest = largest(product, bbox.is_some());
    let mut size_param = String::new();
    if let Some((width, height)) = size {
        if ![1024.0, 2048.0, 4096.0].contains(&width) || height != width / 2.0 || width > largest {
            return Err(Failure::other("Invalid weather image size"));
        }
        if width != largest {
            size_param = format!("&size={}x{}", js::fmt_num(width), js::fmt_num(height));
        }
    }
    Ok(format!(
        "/api/weather/image?product={product}&time={}{box_param}{size_param}",
        encode_uri_component(time)
    ))
}

/// `getSnapshot({ product })`.
pub async fn snapshot(api: &Api, product: &str) -> Result<Value, Failure> {
    if !WEATHER_PRODUCTS.contains(&product) {
        return Err(Failure::other("Unknown weather product"));
    }
    let work = async {
        let mut init = no_store();
        init.redirect = Redirect::Error;
        let response = api
            .fetch(&format!("/api/weather/manifest?product={product}"), init)
            .await?;
        if !response.ok() {
            return Err(Failure::other(format!("Weather HTTP {}", response.status)));
        }
        let bytes = read_capped(response, 16_384).await?;
        let value =
            js::parse_json(&js::decode_text(&bytes)).ok_or_else(super::fetch::json_syntax_error)?;
        validate_snapshot(value, product)
    };
    tokio::time::timeout(Duration::from_secs(15), work)
        .await
        .map_err(|_| Failure::other("Weather request timed out"))?
}

/// `getImage({ product, time, size, bbox })`: media type and bytes.
pub async fn image(
    api: &Api,
    product: &str,
    time: &str,
    size: (f64, f64),
    bbox: Option<&BBox>,
) -> Result<(String, Bytes), Failure> {
    let url = image_url(product, time, Some(size), bbox)?;
    let response = api.fetch(&url, Init::redirect(Redirect::Error)).await?;
    if !response.ok() {
        return Err(Failure::other(format!(
            "Weather image HTTP {}",
            response.status
        )));
    }
    let kind = response.header("content-type").unwrap_or_default();
    let kind = js::trim(kind.split(';').next().unwrap_or_default()).to_lowercase();
    Ok((kind, response.bytes().await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_urls_match_js() {
        let b = BBox {
            west: -100.0,
            south: 30.25,
            east: -98.0,
            north: 31.25,
        };
        assert_eq!(
            image_url(
                "radar",
                "2026-01-01T00:00:00.000Z",
                Some((1024.0, 512.0)),
                Some(&b)
            )
            .unwrap(),
            "/api/weather/image?product=radar&time=2026-01-01T00%3A00%3A00.000Z&bbox=-100,30.25,-98,31.25&size=1024x512"
        );
        assert_eq!(
            image_url(
                "clouds",
                "2026-01-01T00:00:00.000Z",
                Some((2048.0, 1024.0)),
                None
            )
            .unwrap(),
            "/api/weather/image?product=clouds&time=2026-01-01T00%3A00%3A00.000Z"
        );
    }
}
