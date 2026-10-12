//! Port of `src/view/index.js` (createView, viewToParams, viewUrl) and
//! `src/tools/views.js` (suggested views and cameras).

use std::f64::consts::PI;

use serde_json::{Map, Value, json};
use url::Url;

use super::area::{Area, LatLon, area_center, area_radius_km};
use super::data;
use super::js;
use super::services::Services;

pub const STYLE_URL_NAMES: [(&str, &str); 7] = [
    ("normal", "normal"),
    ("retro", "crt"),
    ("surveillance", "nvg"),
    ("thermal", "flir"),
    ("anime", "anime"),
    ("noir", "noir"),
    ("snow", "snow"),
];
pub const VIEW_MAPS: [&str; 5] = [
    "photoreal",
    "bing-aerial",
    "bing-labels",
    "esri-imagery",
    "osm",
];
pub const VIEW_ANNOTATION_TYPES: [&str; 6] =
    ["pin", "highlight", "area", "arrow", "route", "label"];
const ANNOTATION_COLORS: [&str; 5] = ["primary", "amber", "cyan", "green", "red"];
const MAX_ANNOTATIONS: usize = 24;
const MAX_ANNOTATION_PARAM_CHARS: usize = 6000;
const MIN_ALTITUDE_M: f64 = 50.0;
const MAX_ALTITUDE_M: f64 = 20_000_000.0;

/// What can be followed: (kind, layer, owner token, option token, cockpit).
struct Followable {
    kind: &'static str,
    layer: &'static str,
    cockpit: bool,
}

const FOLLOWABLE: [Followable; 3] = [
    Followable {
        kind: "aircraft",
        layer: "flights",
        cockpit: true,
    },
    Followable {
        kind: "military_aircraft",
        layer: "military",
        cockpit: true,
    },
    Followable {
        kind: "satellite",
        layer: "satellites",
        cockpit: false,
    },
];

fn followable(kind: &str) -> Option<&'static Followable> {
    FOLLOWABLE.iter().find(|f| f.kind == kind)
}

/// A camera: lat and lon required, the rest optional.
#[derive(Debug, Clone, Copy, Default)]
pub struct Camera {
    pub lat: f64,
    pub lon: f64,
    pub altitude_m: Option<f64>,
    pub heading_deg: Option<f64>,
    pub pitch_deg: Option<f64>,
}

/// Something the camera follows.
#[derive(Debug, Clone)]
pub struct Follow {
    pub kind: String,
    pub id: String,
    pub cockpit: bool,
}

/// `createView`'s input.
#[derive(Debug, Clone, Default)]
pub struct ViewSpec {
    pub camera: Camera,
    pub layers: Vec<String>,
    pub style: Option<String>,
    pub map: Option<String>,
    pub follow: Option<Follow>,
    pub annotations: Vec<Value>,
}

fn latitude(value: Option<&Value>) -> Option<f64> {
    js::finite(value?).filter(|v| v.abs() <= 90.0)
}

fn longitude(value: Option<&Value>) -> Option<f64> {
    js::finite(value?).filter(|v| v.abs() <= 180.0)
}

fn text(value: Option<&Value>, max: usize) -> Option<String> {
    let value = value?.as_str()?;
    let trimmed = js::trim(value);
    if trimmed.is_empty() {
        None
    } else {
        Some(js::slice16(trimmed, max))
    }
}

/// `annotationOf`: one annotation with only the fields the app draws.
pub fn annotation_of(input: &Value) -> Option<Value> {
    let kind = input.get("type")?.as_str()?;
    if !VIEW_ANNOTATION_TYPES.contains(&kind) {
        return None;
    }
    let points: Vec<Value> = match input.get("points").and_then(Value::as_array) {
        Some(list) => list
            .iter()
            .take(12)
            .filter_map(|point| {
                let lat = latitude(point.get("latitude"))?;
                let lon = longitude(point.get("longitude"))?;
                Some(json!({"latitude": js::num(lat), "longitude": js::num(lon)}))
            })
            .collect(),
        None => Vec::new(),
    };
    let pair = |lat: Option<&Value>, lon: Option<&Value>| match (latitude(lat), longitude(lon)) {
        (Some(a), Some(b)) => (Some(a), Some(b)),
        _ => (None, None),
    };
    let (lat, lon) = pair(input.get("latitude"), input.get("longitude"));
    let (to_lat, to_lon) = pair(input.get("toLatitude"), input.get("toLongitude"));
    let mut entry = Map::new();
    entry.insert("type".into(), json!(kind));
    let target = text(input.get("target"), 200);
    if let Some(target) = &target {
        entry.insert("target".into(), json!(target));
    }
    if let Some(v) = lat {
        entry.insert("latitude".into(), js::num(v));
    }
    if let Some(v) = lon {
        entry.insert("longitude".into(), js::num(v));
    }
    if let Some(v) = to_lat {
        entry.insert("toLatitude".into(), js::num(v));
    }
    if let Some(v) = to_lon {
        entry.insert("toLongitude".into(), js::num(v));
    }
    let has_points = points.len() >= 2;
    if has_points {
        entry.insert("points".into(), Value::Array(points));
    }
    if let Some(label) = text(input.get("label"), 120) {
        entry.insert("label".into(), json!(label));
    }
    if let Some(color) = input
        .get("color")
        .and_then(Value::as_str)
        .filter(|c| ANNOTATION_COLORS.contains(c))
    {
        entry.insert("color".into(), json!(color));
    }
    let placed = target.is_some() || has_points || (lat.is_some() && lon.is_some());
    placed.then_some(Value::Object(entry))
}

/// `createView`: a complete view, or the TypeError message.
pub fn create_view(spec: &ViewSpec) -> Result<Value, String> {
    let camera = spec.camera;
    if !camera.lat.is_finite() || !camera.lon.is_finite() {
        return Err("A view needs a camera lat and lon".into());
    }
    if let Some(style) = &spec.style
        && !STYLE_URL_NAMES.iter().any(|(name, _)| name == style)
    {
        return Err(format!("Unknown view style: {style}"));
    }
    if let Some(map) = &spec.map
        && !VIEW_MAPS.contains(&map.as_str())
    {
        return Err(format!("Unknown view map: {map}"));
    }
    let followed = match &spec.follow {
        Some(follow) => match followable(&follow.kind) {
            Some(f) => Some(f),
            None => return Err(format!("Cannot follow a {}", follow.kind)),
        },
        None => None,
    };
    if let (Some(follow), Some(f)) = (&spec.follow, followed)
        && follow.cockpit
        && !f.cockpit
    {
        return Err(format!("A {} has no cockpit view", follow.kind));
    }
    let registry = data::layer_ids();
    let mut enabled: Vec<&str> = spec
        .layers
        .iter()
        .map(String::as_str)
        .filter(|layer| registry.contains(layer))
        .collect();
    if let Some(f) = followed {
        enabled.push(f.layer);
    }
    let layers: Vec<Value> = registry
        .iter()
        .filter(|id| enabled.contains(id))
        .map(|id| json!(id))
        .collect();
    let camera_json = json!({
        "lat": js::num(js::round_to(js::clamp(camera.lat, -90.0, 90.0), 4)),
        "lon": js::num(js::round_to(js::clamp(camera.lon, -180.0, 180.0), 4)),
        "altitude_m": js::num(js::math_round(js::clamp(camera.altitude_m.unwrap_or(800_000.0), MIN_ALTITUDE_M, MAX_ALTITUDE_M))),
        "heading_deg": js::num(js::math_round(camera.heading_deg.unwrap_or(0.0)) % 360.0),
        "pitch_deg": js::num(js::math_round(js::clamp(camera.pitch_deg.unwrap_or(-90.0), -90.0, 0.0))),
    });
    let follow = match &spec.follow {
        Some(follow) => {
            let mut out = Map::new();
            out.insert("kind".into(), json!(follow.kind));
            out.insert("id".into(), json!(follow.id.to_lowercase()));
            if follow.cockpit {
                out.insert("cockpit".into(), json!(true));
            }
            Value::Object(out)
        }
        None => Value::Null,
    };
    let annotations: Vec<Value> = spec
        .annotations
        .iter()
        .take(MAX_ANNOTATIONS)
        .filter_map(annotation_of)
        .collect();
    Ok(json!({
        "camera": camera_json,
        "layers": layers,
        "style": spec.style,
        "map": spec.map,
        "follow": follow,
        "annotations": annotations,
    }))
}

/// `createView` over arbitrary JSON parts, as a view passed through a tool
/// argument reaches it: the TypeError message on bad input.
pub fn create_view_from(
    camera: &Map<String, Value>,
    layers: Option<&Value>,
    style: &Value,
    map: &Value,
    follow: &Value,
    annotations: Option<&Value>,
) -> Result<Value, String> {
    let (Some(lat), Some(lon)) = (
        camera.get("lat").and_then(js::finite),
        camera.get("lon").and_then(js::finite),
    ) else {
        return Err("A view needs a camera lat and lon".into());
    };
    if !style.is_null()
        && !style
            .as_str()
            .is_some_and(|s| STYLE_URL_NAMES.iter().any(|(name, _)| *name == s))
    {
        return Err(format!("Unknown view style: {}", js::to_js_string(style)));
    }
    if !map.is_null() && !map.as_str().is_some_and(|m| VIEW_MAPS.contains(&m)) {
        return Err(format!("Unknown view map: {}", js::to_js_string(map)));
    }
    let follow = Some(follow).filter(|f| js::truthy(f));
    let kind = |f: &Value| {
        f.get("kind")
            .map_or_else(|| "undefined".to_string(), js::to_js_string)
    };
    let followed = match follow {
        Some(f) => match f.get("kind").and_then(Value::as_str).and_then(followable) {
            Some(followed) => Some(followed),
            None => return Err(format!("Cannot follow a {}", kind(f))),
        },
        None => None,
    };
    let cockpit = follow.is_some_and(|f| f.get("cockpit") == Some(&Value::Bool(true)));
    if cockpit && followed.is_some_and(|f| !f.cockpit) {
        return Err(format!(
            "A {} has no cockpit view",
            kind(follow.expect("follow"))
        ));
    }
    let layers: Vec<&Value> = match layers {
        None => Vec::new(),
        Some(Value::Array(list)) => list.iter().collect(),
        Some(Value::Null) => return Err("Cannot read properties of null (reading 'filter')".into()),
        Some(_) => return Err("layers.filter is not a function".into()),
    };
    let registry = data::layer_ids();
    let mut enabled: Vec<&str> = layers
        .iter()
        .filter_map(|layer| layer.as_str())
        .filter(|layer| registry.contains(layer))
        .collect();
    if let Some(f) = followed {
        enabled.push(f.layer);
    }
    let number = |key: &str, default: f64| match camera.get(key) {
        None | Some(Value::Null) => default,
        Some(value) => js::to_number(value),
    };
    let camera_json = json!({
        "lat": js::num(js::round_to(js::clamp(lat, -90.0, 90.0), 4)),
        "lon": js::num(js::round_to(js::clamp(lon, -180.0, 180.0), 4)),
        "altitude_m": js::num(js::math_round(js::clamp(number("altitude_m", 800_000.0), MIN_ALTITUDE_M, MAX_ALTITUDE_M))),
        "heading_deg": js::num(js::math_round(number("heading_deg", 0.0)) % 360.0),
        "pitch_deg": js::num(js::math_round(js::clamp(number("pitch_deg", -90.0), -90.0, 0.0))),
    });
    let follow_json = match (follow, followed) {
        (Some(f), Some(followed)) => {
            let mut out = Map::new();
            out.insert("kind".into(), json!(followed.kind));
            let id = f
                .get("id")
                .map_or_else(|| "undefined".to_string(), js::to_js_string);
            out.insert("id".into(), json!(id.to_lowercase()));
            if cockpit {
                out.insert("cockpit".into(), json!(true));
            }
            Value::Object(out)
        }
        _ => Value::Null,
    };
    let annotations: Vec<Value> = annotations
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .take(MAX_ANNOTATIONS)
                .filter_map(annotation_of)
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({
        "camera": camera_json,
        "layers": registry.iter().filter(|id| enabled.contains(id)).map(|id| json!(id)).collect::<Vec<_>>(),
        "style": style,
        "map": map,
        "follow": follow_json,
        "annotations": annotations,
    }))
}

/// A camera as the JSON object JS tools spread.
pub fn camera_json(camera: &Camera) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("lat".into(), js::num(camera.lat));
    out.insert("lon".into(), js::num(camera.lon));
    if let Some(v) = camera.altitude_m {
        out.insert("altitude_m".into(), js::num(v));
    }
    if let Some(v) = camera.heading_deg {
        out.insert("heading_deg".into(), js::num(v));
    }
    if let Some(v) = camera.pitch_deg {
        out.insert("pitch_deg".into(), js::num(v));
    }
    out
}

/// `viewToParams(view).toString()`.
pub fn view_to_params(view: &Value) -> String {
    let camera = &view["camera"];
    let number = |key: &str| camera.get(key).and_then(Value::as_f64).unwrap_or(f64::NAN);
    let mut params: Vec<(String, String)> = vec![
        ("v".into(), "2".into()),
        ("lat".into(), js::to_fixed(number("lat"), 4)),
        ("lon".into(), js::to_fixed(number("lon"), 4)),
        ("alt".into(), js::fmt_num(number("altitude_m"))),
        ("heading".into(), js::fmt_num(number("heading_deg"))),
        ("pitch".into(), js::fmt_num(number("pitch_deg"))),
        ("roll".into(), "0".into()),
    ];
    if let Some(style) = view.get("style").and_then(Value::as_str)
        && let Some((_, url)) = STYLE_URL_NAMES.iter().find(|(name, _)| *name == style)
    {
        params.push(("style".into(), (*url).into()));
    }
    if let Some(map) = view
        .get("map")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
    {
        params.push(("map".into(), map.into()));
    }
    let layers: Vec<&str> = view
        .get("layers")
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if !layers.is_empty() {
        let tokens: Vec<&str> = data::layer_registry()
            .iter()
            .filter(|(id, _)| layers.contains(&id.as_str()))
            .map(|(_, token)| token.as_str())
            .collect();
        params.push(("l".into(), tokens.join(".")));
        // Options that differ from what an absent token means: flights'
        // models3d (now on by default) and wind's overlay, plus the followed
        // entity's tracking id.
        let mut options = vec!["f.e.1".to_string()];
        let follow = view.get("follow").filter(|f| f.is_object());
        let follow_kind = follow.and_then(|f| f.get("kind")).and_then(Value::as_str);
        let follow_id = follow.and_then(|f| f.get("id")).map(js::to_js_string);
        if let (Some(kind), Some(id)) = (follow_kind, follow_id)
            && let Some(f) = followable(kind)
            && layers.contains(&f.layer)
        {
            match kind {
                "aircraft" => {
                    if let Some(id) = tracking_id(&id) {
                        options.push(format!("f.t.{id}"));
                    }
                }
                "military_aircraft" => {
                    if let Some(id) = tracking_id(&id) {
                        options.push(format!("f.u.{id}"));
                    }
                }
                _ => {
                    if let Some(id) = satellite_id(&id) {
                        options.push(format!("s.t.{}", js::fmt_num(id.trunc())));
                    }
                }
            }
        }
        options.push("k.o.n".into());
        params.push(("lo".into(), options.join("_")));
    }
    if let Some(annotations) = view.get("annotations").and_then(Value::as_array)
        && !annotations.is_empty()
    {
        let encoded = js::stringify(&Value::Array(annotations.clone()));
        if js::len16(&encoded) <= MAX_ANNOTATION_PARAM_CHARS {
            params.push(("an".into(), encoded));
        }
    }
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish()
}

/// The layer codec's tracking-id grammar for aircraft.
fn tracking_id(raw: &str) -> Option<String> {
    let normalized = js::trim(raw).to_lowercase();
    let ok = (1..=16).contains(&normalized.chars().count())
        && normalized
            .chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase() || matches!(c, '~' | '_' | '-'));
    ok.then_some(normalized)
}

/// The layer codec's positive-integer satellite id.
fn satellite_id(raw: &str) -> Option<f64> {
    let candidate = js::trim(raw);
    if candidate.is_empty() {
        return None;
    }
    let parsed = js::str_to_number(candidate);
    (parsed.is_finite() && parsed.fract() == 0.0 && parsed > 0.0).then_some(parsed)
}

/// `viewUrl(baseUrl, view)`.
pub fn view_url(base: &Url, view: &Value) -> String {
    let mut url = base.clone();
    url.set_fragment(Some(&view_to_params(view)));
    url.to_string()
}

const MIN_AREA_ALTITUDE_M: f64 = 500.0;
const MAX_AREA_ALTITUDE_M: f64 = 15_000_000.0;
const GROUND_PER_ALTITUDE: f64 = 0.55;
pub const POINT_ALTITUDE_M: f64 = 50_000.0;
const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// `cameraForArea`.
pub fn camera_for_area(area: &Area) -> Camera {
    let center = area_center(area);
    Camera {
        lat: center.lat,
        lon: center.lon,
        altitude_m: Some(js::math_min([
            MAX_AREA_ALTITUDE_M,
            js::math_max([
                MIN_AREA_ALTITUDE_M,
                (area_radius_km(area) * 1000.0) / GROUND_PER_ALTITUDE,
            ]),
        ])),
        heading_deg: None,
        pitch_deg: None,
    }
}

/// `cameraLookingAt`.
pub fn camera_looking_at(
    target: LatLon,
    altitude_m: f64,
    heading_deg: f64,
    pitch_deg: f64,
) -> Camera {
    let camera = Camera {
        lat: target.lat,
        lon: target.lon,
        altitude_m: Some(altitude_m),
        heading_deg: Some(heading_deg),
        pitch_deg: Some(pitch_deg),
    };
    if pitch_deg <= -89.5 {
        return camera;
    }
    let back = altitude_m / ((-js::math_min([pitch_deg, -5.0]) * PI) / 180.0).tan();
    let heading = (heading_deg * PI) / 180.0;
    let lat = target.lat - ((back * heading.cos()) / EARTH_RADIUS_M) * (180.0 / PI);
    let lon = target.lon
        - ((back * heading.sin()) / (EARTH_RADIUS_M * ((target.lat * PI) / 180.0).cos()))
            * (180.0 / PI);
    Camera {
        lat: js::math_max([-90.0, js::math_min([90.0, lat])]),
        lon: (((lon + 180.0) % 360.0) + 360.0) % 360.0 - 180.0,
        ..camera
    }
}

/// The app's address, or None when it is not configured.
pub fn app_url(services: &Services) -> Option<Url> {
    Url::parse(services.app_base_url.as_deref()?).ok()
}

/// What `suggestView` frames.
pub enum Frame<'a> {
    Area(&'a Area),
    Point { lat: f64, lon: f64, altitude_m: f64 },
}

impl Frame<'_> {
    pub fn point(lat: f64, lon: f64) -> Self {
        Frame::Point {
            lat,
            lon,
            altitude_m: POINT_ALTITUDE_M,
        }
    }
}

/// `suggestView`: a view with layers on, plus `url` (null without the
/// app's address). Panics never: invalid cameras come from tool code only.
pub fn suggest_view(
    services: &Services,
    frame: Frame<'_>,
    layers: &[&str],
    follow: Option<Follow>,
) -> Result<Value, String> {
    let camera = match frame {
        Frame::Area(area) => camera_for_area(area),
        Frame::Point {
            lat,
            lon,
            altitude_m,
        } => Camera {
            lat,
            lon,
            altitude_m: Some(altitude_m),
            ..Camera::default()
        },
    };
    let view = create_view(&ViewSpec {
        camera,
        layers: layers.iter().map(|l| l.to_string()).collect(),
        follow,
        ..ViewSpec::default()
    })?;
    Ok(with_url(services, view))
}

/// `{ ...view, url }`.
pub fn with_url(services: &Services, view: Value) -> Value {
    let url = app_url(services).map(|base| view_url(&base, &view));
    let mut out = match view {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    out.insert("url".into(), url.map_or(Value::Null, Value::String));
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(spec: ViewSpec) -> Value {
        create_view(&spec).unwrap()
    }

    #[test]
    fn params_match_the_share_link_codec() {
        let base = ViewSpec {
            camera: Camera {
                lat: 1.0,
                lon: 2.0,
                ..Camera::default()
            },
            ..ViewSpec::default()
        };
        assert_eq!(
            view_to_params(&view(base.clone())),
            "v=2&lat=1.0000&lon=2.0000&alt=800000&heading=0&pitch=-90&roll=0"
        );
        let layers = vec![
            "wind".to_string(),
            "flights".to_string(),
            "cctv".to_string(),
        ];
        let cases = [
            (None, "l=c.f.k&lo=f.e.1_k.o.n"),
            (Some(("aircraft", "ABC")), "l=c.f.k&lo=f.e.1_f.t.abc_k.o.n"),
            (
                Some(("military_aircraft", "x_y")),
                "l=c.f.m.k&lo=f.e.1_f.u.x_y_k.o.n",
            ),
            (
                Some(("satellite", "25544")),
                "l=c.f.s.k&lo=f.e.1_s.t.25544_k.o.n",
            ),
        ];
        for (follow, expected) in cases {
            let spec = ViewSpec {
                layers: layers.clone(),
                follow: follow.map(|(kind, id)| Follow {
                    kind: kind.into(),
                    id: id.into(),
                    cockpit: false,
                }),
                ..base.clone()
            };
            assert_eq!(
                view_to_params(&view(spec)),
                format!(
                    "v=2&lat=1.0000&lon=2.0000&alt=800000&heading=0&pitch=-90&roll=0&{expected}"
                )
            );
        }
        let annotated = ViewSpec {
            layers: vec!["satellites".into()],
            follow: Some(Follow {
                kind: "aircraft".into(),
                id: "a".into(),
                cockpit: true,
            }),
            annotations: vec![
                json!({"type": "pin", "latitude": 1, "longitude": 2, "label": "é x"}),
            ],
            ..base
        };
        assert_eq!(
            view_to_params(&view(annotated)),
            "v=2&lat=1.0000&lon=2.0000&alt=800000&heading=0&pitch=-90&roll=0&l=f.s&lo=f.e.1_f.t.a_k.o.n&an=%5B%7B%22type%22%3A%22pin%22%2C%22latitude%22%3A1%2C%22longitude%22%3A2%2C%22label%22%3A%22%C3%A9+x%22%7D%5D"
        );
    }

    #[test]
    fn views_validate_their_parts() {
        let spec = |follow: Follow| ViewSpec {
            camera: Camera {
                lat: 0.0,
                lon: 0.0,
                ..Camera::default()
            },
            follow: Some(follow),
            ..ViewSpec::default()
        };
        assert_eq!(
            create_view(&spec(Follow {
                kind: "satellite".into(),
                id: "1".into(),
                cockpit: true
            })),
            Err("A satellite has no cockpit view".into())
        );
        assert_eq!(
            create_view(&spec(Follow {
                kind: "ship".into(),
                id: "1".into(),
                cockpit: false
            })),
            Err("Cannot follow a ship".into())
        );
    }
}
