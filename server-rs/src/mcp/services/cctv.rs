//! The CCTV source (`src/layers/cctv/source.js`): the camera catalog and
//! one current frame.

use serde_json::Value;

use super::fetch::Api;
use super::live::encode_uri_component;
use super::{no_store, template};
use crate::mcp::catalog::Failure;
use crate::mcp::js;

/// `cctv.getCatalog()`.
pub async fn catalog(api: &Api) -> Result<Value, Failure> {
    let response = api.fetch("/api/cctv/sources", no_store()).await?;
    if !response.ok() {
        return Err(Failure::other(format!(
            "Camera source HTTP {}",
            response.status
        )));
    }
    let payload = response.json().await?;
    if !payload.get("sources").is_some_and(Value::is_array) {
        return Err(Failure::other("Malformed camera sources snapshot"));
    }
    Ok(payload)
}

/// `cctv.getFrame(camera)`: the frame's media type and bytes.
pub async fn frame(api: &Api, camera: &Value) -> Result<(String, bytes::Bytes), Failure> {
    let number = |key: &str| camera.get(key).map_or(f64::NAN, js::to_number);
    let tick = (js::now() / 10_000.0).floor();
    let pitch = match camera.get("pitchDeg").filter(|v| js::truthy(v)) {
        Some(value) => js::to_number(value),
        None => -10.0,
    };
    let params = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("label", &template(camera.get("name")))
        .append_pair("city", &template(camera.get("city")))
        .append_pair("lat", &js::to_fixed(number("lat"), 6))
        .append_pair("lon", &js::to_fixed(number("lon"), 6))
        .append_pair(
            "heading",
            &js::fmt_num(js::math_round(number("headingDeg"))),
        )
        .append_pair("fov", &js::fmt_num(js::math_round(number("fovDeg"))))
        .append_pair("pitch", &js::fmt_num(js::math_round(pitch)))
        .append_pair("ts", &js::fmt_num(tick))
        .finish();
    let url = format!(
        "/api/cctv/frame/{}?{params}",
        encode_uri_component(&template(camera.get("id")))
    );
    let response = api.fetch(&url, no_store()).await?;
    if !response.ok() {
        return Err(Failure::other(format!(
            "Camera frame HTTP {}",
            response.status
        )));
    }
    let kind = response.header("content-type").unwrap_or_default();
    let kind = js::trim(kind.split(';').next().unwrap_or_default()).to_lowercase();
    Ok((kind, response.bytes().await?))
}
