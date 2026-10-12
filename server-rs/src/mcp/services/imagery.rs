//! Recent keyless satellite imagery: `createImageryService` in
//! `src/tools/services.js`, with the parts of
//! `src/layers/recentImagery/catalog.js` and `model.js` it uses.

use serde_json::Value;

use super::fetch::{Api, Init};
use super::vector_tiles::BBox;
use crate::mcp::catalog::Failure;
use crate::mcp::js;

const CATALOG_DAYS: i64 = 30;
const MAX_BOX_SIDE_KM: f64 = 1000.0;
const MERCATOR_LAT_LIMIT: f64 = 85.0511;
const MAX_CLOUD_FOR_CLEAR: f64 = 20.0;
const KM_PER_DEG_LAT: f64 = 111.32;
const DAY_MS: f64 = 86_400_000.0;
const CMR_ENDPOINT: &str = "https://cmr.earthdata.nasa.gov/search";
const CMR_PAGE_SIZE: usize = 200;
const CMR_MAX_RECORDS: usize = 2000;
const WVS_ENDPOINT: &str = "https://wvs.earthdata.nasa.gov/api/v1/snapshot";
const PRODUCT_ORDER: [&str; 3] = ["S30", "L30", "VIIRS"];

/// The GIBS layer and CMR collection of each product.
fn product_spec(product: &str) -> Option<(&'static str, Option<&'static str>)> {
    Some(match product {
        "S30" => (
            "HLS_S30_Nadir_BRDF_Adjusted_Reflectance",
            Some("C2021957295-LPCLOUD"),
        ),
        "L30" => (
            "HLS_L30_Nadir_BRDF_Adjusted_Reflectance",
            Some("C2021957657-LPCLOUD"),
        ),
        "VIIRS" => ("VIIRS_NOAA21_CorrectedReflectance_TrueColor", None),
        _ => return None,
    })
}

/// A CMR granule.
#[derive(Debug, Clone, PartialEq)]
pub struct Granule {
    pub product: String,
    pub time_start: String,
    pub time_end: String,
    pub cloud: Option<f64>,
    pub footprint: Option<Vec<(f64, f64)>>,
}

/// One product-day.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub key: String,
    pub product: String,
    pub day: String,
    pub granules: Vec<Granule>,
    pub cloud: Option<(f64, f64)>,
    pub availability: &'static str,
    pub coverage: &'static str,
}

/// `{ ...rankLatest(...), errors }`.
pub struct Latest {
    pub candidate: Option<Candidate>,
    pub reason: Option<&'static str>,
    pub errors: Vec<String>,
}

fn format_km(km: f64) -> String {
    let rounded = js::math_round(if km.is_nan() { 0.0 } else { km });
    let digits = js::fmt_num(rounded.abs());
    if digits.contains('e') || !rounded.is_finite() {
        return if rounded.is_infinite() {
            if rounded > 0.0 {
                "∞".into()
            } else {
                "-∞".into()
            }
        } else {
            digits
        };
    }
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if rounded < 0.0 {
        format!("-{out}")
    } else {
        out
    }
}

/// `validateBox(box)`: the normalized box, or the refusal message.
pub fn validate_box(b: &BBox) -> Result<BBox, String> {
    if ![b.west, b.south, b.east, b.north]
        .iter()
        .all(|v| v.is_finite())
    {
        return Err("Box edges must be finite".into());
    }
    let (west, east) = (b.west, b.east);
    let south = js::math_min([b.south, b.north]);
    let north = js::math_max([b.south, b.north]);
    if west.abs() > 180.0 || east.abs() > 180.0 {
        return Err("Longitudes must be within ±180°".into());
    }
    if west > east {
        return Err("Select one side of the dateline".into());
    }
    if south.abs() > MERCATOR_LAT_LIMIT || north.abs() > MERCATOR_LAT_LIMIT {
        return Err("Imagery stops at ±85.05° latitude".into());
    }
    if east == west || north == south {
        return Err("Box must enclose an area".into());
    }
    let mid_lat = (south + north) / 2.0;
    let width = (east - west) * KM_PER_DEG_LAT * (mid_lat * std::f64::consts::PI / 180.0).cos();
    let height = (north - south) * KM_PER_DEG_LAT;
    if width > MAX_BOX_SIDE_KM || height > MAX_BOX_SIDE_KM {
        return Err(format!(
            "Box is {} km wide · limit {} km",
            format_km(js::math_max([width, height])),
            format_km(MAX_BOX_SIDE_KM)
        ));
    }
    Ok(BBox {
        west,
        south,
        east,
        north,
    })
}

/// `finiteOrNull(value)`.
fn finite_or_null(value: Option<&Value>) -> Option<f64> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        Some(value) => Some(js::to_number(value)).filter(|n| n.is_finite()),
    }
}

fn iso_or_null(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|s| js::date_parse(s).is_finite())
        .map(str::to_string)
}

fn ring_of(points: Vec<(Option<&Value>, Option<&Value>)>) -> Option<Vec<(f64, f64)>> {
    let ring: Vec<(f64, f64)> = points
        .into_iter()
        .filter_map(|(lon, lat)| Some((finite_or_null(lon)?, finite_or_null(lat)?)))
        .collect();
    (ring.len() >= 3).then_some(ring)
}

fn footprint_of(geometry: Option<&Value>) -> Option<Vec<(f64, f64)>> {
    let geometry = geometry?;
    if let Some(points) = geometry
        .get("GPolygons")
        .and_then(|p| p.get(0))
        .and_then(|p| p.get("Boundary"))
        .and_then(|b| b.get("Points"))
        .and_then(Value::as_array)
        && let Some(ring) = ring_of(
            points
                .iter()
                .map(|p| (p.get("Longitude"), p.get("Latitude")))
                .collect(),
        )
    {
        return Some(ring);
    }
    let r = geometry
        .get("BoundingRectangles")
        .and_then(|r| r.get(0))
        .filter(|r| js::truthy(r))?;
    let w = r.get("WestBoundingCoordinate");
    let s = r.get("SouthBoundingCoordinate");
    let e = r.get("EastBoundingCoordinate");
    let n = r.get("NorthBoundingCoordinate");
    ring_of(vec![(w, s), (e, s), (e, n), (w, n)]).filter(|ring| ring.len() == 4)
}

/// `parseCmrUmm(json, product)`: granules and the hit count.
pub fn parse_cmr_umm(json: &Value, product: &str) -> (Vec<Granule>, f64) {
    let mut granules = Vec::new();
    for item in json
        .get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let umm = item.get("umm").filter(|u| js::truthy(u));
        let range = umm
            .and_then(|u| u.get("TemporalExtent"))
            .and_then(|t| t.get("RangeDateTime"))
            .filter(|r| js::truthy(r));
        let Some(time_start) = iso_or_null(range.and_then(|r| r.get("BeginningDateTime"))) else {
            continue;
        };
        let cloud = umm
            .and_then(|u| u.get("AdditionalAttributes"))
            .and_then(Value::as_array)
            .and_then(|attrs| {
                attrs
                    .iter()
                    .find(|a| a.get("Name").and_then(Value::as_str) == Some("CLOUD_COVERAGE"))
            })
            .and_then(|a| a.get("Values"))
            .and_then(|v| v.get(0));
        granules.push(Granule {
            product: product.to_string(),
            time_end: iso_or_null(range.and_then(|r| r.get("EndingDateTime")))
                .unwrap_or_else(|| time_start.clone()),
            time_start,
            cloud: finite_or_null(cloud),
            footprint: footprint_of(
                umm.and_then(|u| u.get("SpatialExtent"))
                    .and_then(|s| s.get("HorizontalSpatialDomain"))
                    .and_then(|h| h.get("Geometry")),
            ),
        });
    }
    let hits = finite_or_null(json.get("hits")).unwrap_or(granules.len() as f64);
    (granules, hits)
}

fn is_valid_day(day: &str) -> bool {
    let b = day.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let num = |r: std::ops::Range<usize>| {
        if b[r.clone()].iter().all(u8::is_ascii_digit) {
            day[r].parse::<f64>().ok()
        } else {
            None
        }
    };
    let (Some(y), Some(m), Some(d)) = (num(0..4), num(5..7), num(8..10)) else {
        return false;
    };
    let time = js::date_utc(y, m - 1.0, d, 0.0, 0.0, 0.0, 0.0);
    time.is_finite() && js::iso_string(time).is_some_and(|iso| &iso[..10] == day)
}

/// `utcDay(value)` for a string.
pub fn utc_day(value: &str) -> Option<String> {
    let b = value.as_bytes();
    let shaped = b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && [0..4, 5..7, 8..10]
            .into_iter()
            .all(|r| b[r].iter().all(u8::is_ascii_digit));
    if shaped {
        return is_valid_day(value).then(|| value.to_string());
    }
    day_of_ms(js::date_parse(value))
}

fn day_of_ms(ms: f64) -> Option<String> {
    if !ms.is_finite() {
        return None;
    }
    js::iso_string(ms).map(|iso| iso[..10].to_string())
}

fn build_candidate(product: &str, day: &str, granules: Vec<Granule>) -> Candidate {
    let clouds: Vec<f64> = granules.iter().filter_map(|g| g.cloud).collect();
    Candidate {
        key: format!("{product}:{day}"),
        product: product.to_string(),
        day: day.to_string(),
        cloud: (!clouds.is_empty()).then(|| (js::math_min(clouds.clone()), js::math_max(clouds))),
        availability: if granules.is_empty() {
            "empty"
        } else {
            "present"
        },
        granules,
        coverage: "unknown",
    }
}

/// `groupGranulesByDay(granules)`.
fn group_by_day(granules: Vec<Granule>) -> Vec<Candidate> {
    let mut buckets: Vec<(String, String, Vec<Granule>)> = Vec::new();
    for granule in granules {
        let Some(day) = utc_day(&granule.time_start) else {
            continue;
        };
        if product_spec(&granule.product).is_none() {
            continue;
        }
        match buckets
            .iter_mut()
            .find(|(p, d, _)| *p == granule.product && *d == day)
        {
            Some(bucket) => bucket.2.push(granule),
            None => buckets.push((granule.product.clone(), day, vec![granule])),
        }
    }
    merge_candidates(vec![
        buckets
            .into_iter()
            .map(|(product, day, list)| build_candidate(&product, &day, list))
            .collect(),
    ])
}

/// `viirsCandidates(todayIso, days)`.
fn viirs_candidates(today_iso: &str, days: i64) -> Vec<Candidate> {
    let Some(today) = utc_day(today_iso) else {
        return Vec::new();
    };
    let start = js::date_parse(&format!("{today}T00:00:00Z"));
    (0..days.max(0))
        .filter_map(|i| day_of_ms(start - i as f64 * DAY_MS))
        .map(|day| Candidate {
            key: format!("VIIRS:{day}"),
            product: "VIIRS".into(),
            day,
            granules: Vec::new(),
            cloud: None,
            availability: "unknown",
            coverage: "full",
        })
        .collect()
}

/// `mergeCandidates(lists)`: first by key, newest day first.
fn merge_candidates(lists: Vec<Vec<Candidate>>) -> Vec<Candidate> {
    let mut out: Vec<Candidate> = Vec::new();
    for candidate in lists.into_iter().flatten() {
        if !candidate.key.is_empty() && !out.iter().any(|c| c.key == candidate.key) {
            out.push(candidate);
        }
    }
    let order = |p: &str| {
        PRODUCT_ORDER
            .iter()
            .position(|o| *o == p)
            .map_or(-1.0, |i| i as f64)
    };
    js::sort_by(&mut out, |a, b| {
        if a.day != b.day {
            if js::cmp16(&a.day, &b.day).is_lt() {
                1.0
            } else {
                -1.0
            }
        } else {
            order(&a.product) - order(&b.product)
        }
    });
    out
}

fn point_in_polygon(lon: f64, lat: f64, ring: &[(f64, f64)]) -> bool {
    let mut inside = false;
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        let dy = yj - yi;
        let dy = if dy == 0.0 || dy.is_nan() {
            f64::EPSILON
        } else {
            dy
        };
        if (yi > lat) != (yj > lat) && lon < (xj - xi) * (lat - yi) / dy + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// `coverageFor(candidate, box)`.
fn coverage_for(candidate: &Candidate, b: &BBox) -> &'static str {
    let footprints: Vec<&Vec<(f64, f64)>> = candidate
        .granules
        .iter()
        .filter_map(|g| g.footprint.as_ref())
        .filter(|ring| ring.len() >= 3)
        .collect();
    if footprints.is_empty() {
        return "unknown";
    }
    let samples = [
        (b.west, b.south),
        (b.east, b.south),
        (b.east, b.north),
        (b.west, b.north),
        ((b.west + b.east) / 2.0, (b.south + b.north) / 2.0),
    ];
    if samples.iter().all(|(lon, lat)| {
        footprints
            .iter()
            .any(|ring| point_in_polygon(*lon, *lat, ring))
    }) {
        "full"
    } else {
        "partial"
    }
}

/// `rankLatest(candidates)`.
fn rank_latest(candidates: Vec<Candidate>) -> (Option<Candidate>, Option<&'static str>) {
    let sorted = merge_candidates(vec![candidates]);
    let present = |c: &Candidate| c.availability != "empty";
    let hls_day = |c: &Candidate| {
        matches!(c.product.as_str(), "S30" | "L30") && present(c) && !c.granules.is_empty()
    };
    let covers = |c: &Candidate| c.coverage != "partial";
    let clear = |c: &Candidate| {
        c.granules
            .iter()
            .all(|g| g.cloud.is_some_and(|cloud| cloud <= MAX_CLOUD_FOR_CLEAR))
    };
    type Tier<'a> = (&'a dyn Fn(&Candidate) -> bool, &'static str);
    let tiers: [Tier; 5] = [
        (&|c| hls_day(c) && covers(c) && clear(c), "clear"),
        (&|c| hls_day(c) && covers(c), "cloudy"),
        (&|c| hls_day(c) && clear(c), "partial"),
        (&hls_day, "partial"),
        (&|c| c.product == "VIIRS" && present(c), "overview"),
    ];
    for (matches, reason) in tiers {
        if let Some(candidate) = sorted.iter().find(|c| matches(c)) {
            return (Some(candidate.clone()), Some(reason));
        }
    }
    (None, None)
}

/// `fetchCmrPages`: one product's granules across CMR pages.
async fn fetch_cmr_pages(api: &Api, product: &str, url: &str) -> Result<Vec<Granule>, Failure> {
    let mut granules = Vec::new();
    let mut hits = 0.0f64;
    let mut cursor: Option<String> = None;
    loop {
        let mut init = Init::default();
        if let Some(cursor) = &cursor {
            init = init.header("CMR-Search-After", cursor);
        }
        let response = api.fetch(url, init).await?;
        if !response.ok() {
            return Err(Failure::other(format!("HTTP {} from CMR", response.status)));
        }
        let next = response.header("CMR-Search-After");
        let (page, page_hits) = parse_cmr_umm(&response.json().await?, product);
        let page_len = page.len();
        granules.extend(page);
        hits = js::math_max([hits, page_hits]);
        cursor = next;
        if cursor.is_none()
            || page_len < CMR_PAGE_SIZE
            || granules.len() >= CMR_MAX_RECORDS
            || granules.len() as f64 >= hits
        {
            break;
        }
    }
    Ok(granules)
}

/// `imagery.latest({ box })`; an invalid box is the `TypeError` message.
pub async fn latest(api: &Api, b: &BBox) -> Result<Latest, Failure> {
    let validated = validate_box(b).map_err(Failure::Type)?;
    let now = js::now();
    let end_iso = js::iso_string(now).unwrap_or_default();
    let start_day = day_of_ms(now - (CATALOG_DAYS - 1) as f64 * DAY_MS).unwrap_or_default();
    let start_iso = format!("{start_day}T00:00:00Z");
    let products = ["S30", "L30"];
    let urls: Vec<String> = products
        .iter()
        .map(|product| {
            let collection = product_spec(product).and_then(|s| s.1).unwrap_or_default();
            format!(
                "{CMR_ENDPOINT}/granules.umm_json?collection_concept_id={collection}&bounding_box={},{},{},{}&temporal={start_iso},{end_iso}&sort_key=-start_date&page_size={CMR_PAGE_SIZE}",
                js::fmt_num(validated.west),
                js::fmt_num(validated.south),
                js::fmt_num(validated.east),
                js::fmt_num(validated.north),
            )
        })
        .collect();
    let settled = futures_util::future::join_all(
        products
            .iter()
            .zip(&urls)
            .map(|(product, url)| fetch_cmr_pages(api, product, url)),
    )
    .await;
    let mut granules = Vec::new();
    let mut errors = Vec::new();
    for (result, product) in settled.into_iter().zip(products) {
        match result {
            Ok(list) => granules.extend(list),
            Err(_) => errors.push(product.to_string()),
        }
    }
    let hls: Vec<Candidate> = group_by_day(granules)
        .into_iter()
        .map(|mut candidate| {
            candidate.coverage = coverage_for(&candidate, &validated);
            candidate
        })
        .collect();
    let candidates = merge_candidates(vec![hls, viirs_candidates(&end_iso, CATALOG_DAYS)]);
    let (candidate, reason) = rank_latest(candidates);
    Ok(Latest {
        candidate,
        reason,
        errors,
    })
}

/// `wvsSnapshotUrl({ product, day, box, width, height })`.
pub fn wvs_snapshot_url(
    product: &str,
    day: &str,
    b: &BBox,
    width: f64,
    height: f64,
) -> Result<String, Failure> {
    let (layer, _) = product_spec(product)
        .ok_or_else(|| Failure::Type(format!("Unknown imagery product: {product}")))?;
    let south = js::math_min([b.south, b.north]);
    let north = js::math_max([b.south, b.north]);
    let bbox: Vec<String> = [south, b.west, north, b.east]
        .iter()
        .map(|v| js::fmt_num(js::round_to(*v, 6)))
        .collect();
    Ok(format!(
        "{WVS_ENDPOINT}?REQUEST=GetSnapshot&LAYERS={layer}&CRS=EPSG:4326&TIME={day}&BBOX={}&WIDTH={}&HEIGHT={}&FORMAT=image/png",
        bbox.join(","),
        js::fmt_num(js::math_round(width)),
        js::fmt_num(js::math_round(height)),
    ))
}

/// `imagery.getSnapshot(...)`: media type and bytes.
pub async fn snapshot(
    api: &Api,
    product: &str,
    day: &str,
    b: &BBox,
    width: f64,
    height: f64,
) -> Result<(String, bytes::Bytes), Failure> {
    let url = wvs_snapshot_url(product, day, b, width, height)?;
    let response = api.get(&url).await?;
    if !response.ok() {
        return Err(Failure::other(format!("Imagery HTTP {}", response.status)));
    }
    let kind = response.header("content-type").unwrap_or_default();
    let kind = js::trim(kind.split(';').next().unwrap_or_default()).to_lowercase();
    Ok((kind, response.bytes().await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_boxes_like_the_model() {
        let wide = BBox {
            west: -10.0,
            south: 0.0,
            east: 10.0,
            north: 1.0,
        };
        assert_eq!(
            validate_box(&wide).unwrap_err(),
            "Box is 2,226 km wide · limit 1,000 km"
        );
        assert_eq!(format_km(1234567.0), "1,234,567");
    }

    #[test]
    fn ranks_clear_hls_days_first() {
        let item = |start: &str, cloud: f64| {
            json!({"umm": {
                "TemporalExtent": {"RangeDateTime": {"BeginningDateTime": start}},
                "AdditionalAttributes": [{"Name": "CLOUD_COVERAGE", "Values": [cloud.to_string()]}],
                "SpatialExtent": {"HorizontalSpatialDomain": {"Geometry": {"BoundingRectangles": [{
                    "WestBoundingCoordinate": -1, "SouthBoundingCoordinate": -1,
                    "EastBoundingCoordinate": 1, "NorthBoundingCoordinate": 1}]}}}
            }})
        };
        let json = json!({"hits": 2, "items": [item("2026-01-05T10:00:00.000Z", 50.0), item("2026-01-03T10:00:00.000Z", 5.0)]});
        let (granules, hits) = parse_cmr_umm(&json, "S30");
        assert_eq!(hits, 2.0);
        let b = BBox {
            west: -0.5,
            south: -0.5,
            east: 0.5,
            north: 0.5,
        };
        let candidates: Vec<Candidate> = group_by_day(granules)
            .into_iter()
            .map(|mut c| {
                c.coverage = coverage_for(&c, &b);
                c
            })
            .collect();
        assert_eq!(candidates[0].day, "2026-01-05");
        let (best, reason) = rank_latest(candidates);
        assert_eq!(best.unwrap().day, "2026-01-03");
        assert_eq!(reason, Some("clear"));
    }

    #[test]
    fn snapshot_urls_match_js() {
        let b = BBox {
            west: -0.1234567,
            south: 51.0,
            east: 0.5,
            north: 51.5,
        };
        assert_eq!(
            wvs_snapshot_url("S30", "2026-01-03", &b, 1024.0, 811.0).unwrap(),
            "https://wvs.earthdata.nasa.gov/api/v1/snapshot?REQUEST=GetSnapshot&LAYERS=HLS_S30_Nadir_BRDF_Adjusted_Reflectance&CRS=EPSG:4326&TIME=2026-01-03&BBOX=51,-0.123457,51.5,0.5&WIDTH=1024&HEIGHT=811&FORMAT=image/png"
        );
    }
}
