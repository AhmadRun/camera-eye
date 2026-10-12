//! Regional normalizers (the server-side parts of `src/data/regionalModel.js`;
//! `normalizeRegionalPlace`, `weatherCodeLabel` and `regionalDistanceM` are
//! browser-only).

use serde_json::{Value, json};

use crate::providers::mapdata_support::{
    collapse_js_space, iso_from_ms, js_date_parse, js_string_or_empty, js_to_number, js_trim,
    utf16_prefix,
};

const MAX_ARTICLES: usize = 5;

/// `cleanText(value, maxLength)`.
pub fn clean_text(value: Option<&Value>, max_length: usize) -> String {
    let text = js_string_or_empty(value);
    utf16_prefix(js_trim(&collapse_js_space(&text)), max_length)
}

/// `safeHttpUrl`: the parsed href of an http(s) URL.
pub fn safe_http_url(value: &str) -> Option<String> {
    let parsed = url::Url::parse(value).ok()?;
    matches!(parsed.scheme(), "http" | "https").then(|| parsed.to_string())
}

fn hostname(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default()
}

/// `value || other` over JSON values.
fn or<'a>(value: Option<&'a Value>, other: Option<&'a Value>) -> Option<&'a Value> {
    if crate::providers::mapdata_support::js_truthy(value) {
        value
    } else {
        other
    }
}

/// `normalizeRegionalArticles` (GDELT ArticleList output).
pub fn normalize_regional_articles(payload: &Value, limit: usize) -> Vec<Value> {
    let Some(rows) = payload.get("articles").and_then(Value::as_array) else {
        return Vec::new();
    };
    let max = limit.clamp(1, MAX_ARTICLES);
    let mut seen = std::collections::HashSet::new();
    let mut articles = Vec::new();
    for row in rows {
        let raw_url = js_string_or_empty(or(row.get("url"), row.get("url_mobile")));
        let Some(url) = safe_http_url(&raw_url) else {
            continue;
        };
        let title = clean_text(row.get("title"), 180);
        if title.is_empty() {
            continue;
        }
        let host = hostname(&url);
        let signature = format!("{}|{}", title.to_lowercase(), host);
        if !seen.insert(signature) {
            continue;
        }
        let raw_date = clean_text(row.get("seendate"), 32);
        let published_at = compact_date(&raw_date)
            .or_else(|| js_date_parse(&raw_date).and_then(|ms| iso_from_ms(ms as f64)));
        let domain_source = if crate::providers::mapdata_support::js_truthy(row.get("domain")) {
            row.get("domain").cloned()
        } else {
            Some(Value::String(
                host.strip_prefix("www.").unwrap_or(&host).to_string(),
            ))
        };
        let country = clean_text(row.get("sourcecountry"), 60);
        articles.push(json!({
            "title": title,
            "url": url,
            "domain": clean_text(domain_source.as_ref(), 80),
            "publishedAt": published_at,
            "sourceCountry": if country.is_empty() { Value::Null } else { Value::String(country) },
        }));
        if articles.len() >= max {
            break;
        }
    }
    articles
}

/// `/^(\d{8})T(\d{6})Z$/` -> ISO without milliseconds.
fn compact_date(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let (date, time) = (&raw[..8], &raw[9..15]);
    if !date.bytes().chain(time.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(format!(
        "{}-{}-{}T{}:{}:{}Z",
        &date[..4],
        &date[4..6],
        &date[6..8],
        &time[..2],
        &time[2..4],
        &time[4..6]
    ))
}

/// `/(?:[zZ]|[+-]\d\d:?\d\d)$/`.
fn has_zone(value: &str) -> bool {
    if value.ends_with(['z', 'Z']) {
        return true;
    }
    let b = value.as_bytes();
    let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
    let n = b.len();
    let sign = |i: usize| b.get(i).is_some_and(|c| *c == b'+' || *c == b'-');
    (n >= 5 && sign(n - 5) && digit(n - 4) && digit(n - 3) && digit(n - 2) && digit(n - 1))
        || (n >= 6
            && sign(n - 6)
            && digit(n - 5)
            && digit(n - 4)
            && b[n - 3] == b':'
            && digit(n - 2)
            && digit(n - 1))
}

/// `normalizeRegionalWeather` (Open-Meteo current conditions).
pub fn normalize_regional_weather(payload: &Value) -> Option<Value> {
    let current = payload
        .get("current")
        .filter(|c| crate::providers::mapdata_support::js_truthy(Some(c)))?;
    if !js_to_number(current.get("temperature_2m")).is_finite() {
        return None;
    }
    let number_or_null = |name: &str| -> Value {
        match current.get(name) {
            None | Some(Value::Null) => Value::Null,
            Some(Value::String(s)) if s.is_empty() => Value::Null,
            value => {
                let n = js_to_number(value);
                if n.is_finite() {
                    crate::providers::mapdata_support::js_json_number(n)
                } else {
                    Value::Null
                }
            }
        }
    };
    let observed_at = match current.get("time") {
        Some(Value::String(time)) => {
            let raw = if has_zone(time) {
                time.clone()
            } else {
                format!("{time}Z")
            };
            js_date_parse(&raw).and_then(|ms| iso_from_ms(ms as f64))
        }
        _ => None,
    };
    Some(json!({
        "observedAt": observed_at,
        "temperatureC": number_or_null("temperature_2m"),
        "apparentTemperatureC": number_or_null("apparent_temperature"),
        "precipitationMm": number_or_null("precipitation"),
        "cloudCoverPct": number_or_null("cloud_cover"),
        "windKph": number_or_null("wind_speed_10m"),
        "windDirectionDeg": number_or_null("wind_direction_10m"),
        "visibilityM": number_or_null("visibility"),
        "weatherCode": number_or_null("weather_code"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn articles_dedupe_and_reject_unsafe_rows() {
        let articles = normalize_regional_articles(
            &json!({ "articles": [
                { "title": "Regional update", "url": "https://news.example/one", "domain": "news.example", "seendate": "20260722T081500Z" },
                { "title": "Regional   update", "url": "https://news.example/two" },
                { "title": "Unsafe", "url": "javascript:alert(1)" },
                { "title": "Second story", "url": "https://other.example/story", "seendate": "2026-07-22T07:00:00Z" }
            ]}),
            5,
        );
        assert_eq!(articles.len(), 2);
        assert_eq!(articles[0]["publishedAt"], "2026-07-22T08:15:00Z");
        assert_eq!(articles[1]["domain"], "other.example");
        assert_eq!(articles[1]["publishedAt"], "2026-07-22T07:00:00.000Z");
    }

    #[test]
    fn weather_values_and_timestamps() {
        let weather = normalize_regional_weather(&json!({ "current": {
            "time": "2026-07-22T08:15:00Z", "temperature_2m": 21.4, "apparent_temperature": 20.8,
            "precipitation": 0, "cloud_cover": 42, "wind_speed_10m": 18.2, "wind_direction_10m": 270,
            "visibility": 18000, "weather_code": 2
        }}))
        .unwrap();
        assert_eq!(weather["temperatureC"], 21.4);
        assert_eq!(weather["visibilityM"], 18000);
        assert_eq!(weather["weatherCode"], 2);
        assert_eq!(normalize_regional_weather(&json!({ "current": {} })), None);
        let naive = normalize_regional_weather(
            &json!({ "current": { "time": "2026-08-17T00:15", "temperature_2m": 20 }}),
        )
        .unwrap();
        assert_eq!(naive["observedAt"], "2026-08-17T00:15:00.000Z");
        let zoned = normalize_regional_weather(
            &json!({ "current": { "time": "2026-08-17T00:15:00+02:00", "temperature_2m": 20 }}),
        )
        .unwrap();
        assert_eq!(zoned["observedAt"], "2026-08-16T22:15:00.000Z");
        let invalid = normalize_regional_weather(
            &json!({ "current": { "time": "not-a-time", "temperature_2m": 20 }}),
        )
        .unwrap();
        assert_eq!(invalid["observedAt"], Value::Null);
    }
}
