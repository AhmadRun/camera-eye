//! Regional headlines: Google News RSS, then the GDELT index (port of
//! `server/providers/regional/news.js`).

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Value, json};

use super::RegionalHttp;
use super::model::normalize_regional_articles;
use crate::providers::mapdata_support::{
    collapse_js_space, iso_from_ms, js_date_parse, js_string_or_empty, js_trim, utf16_prefix,
};

static CDATA_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)<!\[CDATA\[(.*?)\]\]>").expect("cdata regex"));
static TAG_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]+>").expect("tag regex"));
static ITEM_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<item>(.*?)</item>").expect("item regex"));

fn decode_rss_text(value: &str) -> String {
    let text = CDATA_RE.replace_all(value, "$1");
    let text = text
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">");
    let text = TAG_RE.replace_all(&text, " ");
    js_trim(&collapse_js_space(&text)).to_string()
}

fn rss_tag(block: &str, tag: &str) -> String {
    let pattern = format!(r"(?is)<{tag}(?:\s[^>]*)?>(.*?)</{tag}>");
    let Ok(re) = Regex::new(&pattern) else {
        return String::new();
    };
    re.captures(block)
        .and_then(|caps| caps.get(1))
        .map(|m| decode_rss_text(m.as_str()))
        .unwrap_or_default()
}

/// `normalizeRssArticles`.
pub fn normalize_rss_articles(xml: &str, limit: usize) -> Vec<Value> {
    let mut seen = std::collections::HashSet::new();
    let mut articles = Vec::new();
    for caps in ITEM_RE.captures_iter(xml) {
        let item = &caps[1];
        let title = utf16_prefix(&rss_tag(item, "title"), 180);
        let link = rss_tag(item, "link");
        let Ok(parsed) = url::Url::parse(&link) else {
            continue;
        };
        if title.is_empty() || !matches!(parsed.scheme(), "http" | "https") {
            continue;
        }
        let host = parsed.host_str().unwrap_or("").to_string();
        let source = rss_tag(item, "source");
        let source_key = source.to_lowercase();
        let signature = format!(
            "{}|{}",
            title.to_lowercase(),
            if source_key.is_empty() {
                &host
            } else {
                &source_key
            }
        );
        if !seen.insert(signature) {
            continue;
        }
        let raw_date = rss_tag(item, "pubDate");
        articles.push(json!({
            "title": title,
            "url": parsed.to_string(),
            "domain": if source.is_empty() { host.strip_prefix("www.").unwrap_or(&host).to_string() } else { source },
            "publishedAt": js_date_parse(&raw_date).and_then(|ms| iso_from_ms(ms as f64)),
            "sourceCountry": null,
        }));
        if articles.len() >= limit {
            break;
        }
    }
    articles
}

/// `{status, query, articles, source}`.
pub struct News {
    pub status: &'static str,
    pub query: Value,
    pub articles: Vec<Value>,
    pub source: Value,
}

/// `fetchRegionalNews(place)`.
pub async fn fetch_regional_news(http: &RegionalHttp, place: Option<&Value>) -> News {
    let query = place.and_then(|place| {
        ["locality", "region", "country"]
            .iter()
            .map(|key| place.get(*key))
            .find(|value| crate::providers::mapdata_support::js_truthy(*value))
            .flatten()
            .cloned()
    });
    let Some(query) = query else {
        return News {
            status: "unavailable",
            query: Value::Null,
            articles: Vec::new(),
            source: Value::Null,
        };
    };
    let text = js_string_or_empty(Some(&query));
    let cleaned: String = text
        .chars()
        .map(|c| if c == '"' || c == '\\' { ' ' } else { c })
        .collect();
    let cleaned = js_trim(&cleaned).to_string();
    let headers = [("User-Agent", "CameraEye/0.1")];

    let rss = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", &cleaned)
        .append_pair("hl", "en-US")
        .append_pair("gl", "US")
        .append_pair("ceid", "US:en")
        .finish();
    if let Ok(xml) = http
        .text(
            &format!("https://news.google.com/rss/search?{rss}"),
            &headers,
            12_000,
            true,
        )
        .await
    {
        let articles = normalize_rss_articles(&xml, 5);
        if !articles.is_empty() {
            return News {
                status: "ready",
                query,
                articles,
                source: json!("Google News RSS"),
            };
        }
    }

    let params = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", &format!("\"{cleaned}\""))
        .append_pair("mode", "artlist")
        .append_pair("format", "json")
        .append_pair("maxrecords", "5")
        .append_pair("sort", "datedesc")
        .append_pair("timespan", "48h")
        .finish();
    match http
        .json(
            &format!("https://api.gdeltproject.org/api/v2/doc/doc?{params}"),
            &headers,
            12_000,
            super::REGIONAL_MAX_RESPONSE_BYTES,
            true,
        )
        .await
    {
        Ok(payload) => {
            let articles = normalize_regional_articles(&payload, 5);
            News {
                status: if articles.is_empty() {
                    "empty"
                } else {
                    "ready"
                },
                query,
                articles,
                source: json!("GDELT fallback"),
            }
        }
        Err(_) => News {
            status: "unavailable",
            query,
            articles: Vec::new(),
            source: Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_items_are_decoded_and_deduped() {
        let xml = r#"<rss><channel>
            <item><title><![CDATA[Storm &amp; flood <b>warning</b>]]></title><link>https://www.example.com/a</link><pubDate>Wed, 23 Sep 2026 12:02:00 GMT</pubDate></item>
            <item><title>Storm &amp; flood warning</title><link>https://www.example.com/b</link></item>
            <item><title>Other</title><link>javascript:alert(1)</link></item>
            <item><title>Sourced</title><link>http://news.test/x</link><source url="http://s">Wire</source></item>
        </channel></rss>"#;
        let articles = normalize_rss_articles(xml, 5);
        assert_eq!(articles.len(), 2);
        assert_eq!(articles[0]["title"], "Storm & flood warning");
        assert_eq!(articles[0]["domain"], "example.com");
        assert_eq!(articles[0]["publishedAt"], "2026-09-23T12:02:00.000Z");
        assert_eq!(articles[1]["domain"], "Wire");
        assert_eq!(articles[1]["publishedAt"], Value::Null);
    }
}
