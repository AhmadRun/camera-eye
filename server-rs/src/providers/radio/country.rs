//! Port of `src/data/radioCountry.js`: bounded country code / name input.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

use super::regions::ISO_REGIONS;
use crate::providers::cctv::jsval;

const RADIO_COUNTRY_MAX_LENGTH: usize = 80;

/// A normalized country: `{ valid, empty, code, name }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Country {
    pub valid: bool,
    pub empty: bool,
    pub code: String,
    pub name: String,
}

impl Country {
    fn invalid() -> Self {
        Self {
            valid: false,
            empty: false,
            code: String::new(),
            name: String::new(),
        }
    }
}

/// `countryKey`: the folded lookup key for a country name.
pub fn country_key(value: &str) -> String {
    static DASHES: Lazy<Regex> = Lazy::new(|| Regex::new("[‐‑‒–—-]+").expect("regex"));
    static DROPPED: Lazy<Regex> = Lazy::new(|| Regex::new("[.’']").expect("regex"));
    let text: String = value.nfkc().collect::<String>().to_lowercase();
    let text = DASHES.replace_all(&text, " ");
    let text = DROPPED.replace_all(&text, "");
    let text = text.replace('&', " and ");
    jsval::trim(&jsval::collapse_ws(&text)).to_string()
}

/// `Intl.DisplayNames(['en'], { type: 'region' }).of(code)`.
fn region_name(code: &str) -> Option<&'static str> {
    ISO_REGIONS
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, name)| *name)
}

static COUNTRY_NAME_TO_CODE: Lazy<HashMap<String, &'static str>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for (code, name) in ISO_REGIONS.iter() {
        if name != code {
            map.insert(country_key(name), *code);
        }
    }
    for (name, code) in [
        ("america", "US"),
        ("united states of america", "US"),
        ("usa", "US"),
        ("u s", "US"),
        ("u s a", "US"),
        ("britain", "GB"),
        ("great britain", "GB"),
        ("uk", "GB"),
        ("u k", "GB"),
        ("south korea", "KR"),
        ("north korea", "KP"),
        ("russia", "RU"),
        ("vietnam", "VN"),
        ("czech republic", "CZ"),
        ("ivory coast", "CI"),
        ("laos", "LA"),
        ("bolivia", "BO"),
        ("tanzania", "TZ"),
        ("moldova", "MD"),
        ("brunei", "BN"),
        ("cape verde", "CV"),
        ("the netherlands", "NL"),
        ("turkey", "TR"),
        ("turkiye", "TR"),
        ("myanmar", "MM"),
        ("burma", "MM"),
        ("uae", "AE"),
        ("holland", "NL"),
        ("swaziland", "SZ"),
        ("east timor", "TL"),
        ("cabo verde", "CV"),
        ("vatican", "VA"),
    ] {
        map.insert(country_key(name), code);
    }
    map
});

/// `normalizeRadioCountryInput`: invalid or ambiguous values fail closed.
pub fn normalize_radio_country_input(value: Option<&Value>) -> Country {
    static ALLOWED: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[\p{L}\p{M}.&'’()\-\s]+$").expect("regex"));
    let text = match value {
        None | Some(Value::Null) => {
            return Country {
                valid: true,
                empty: true,
                code: String::new(),
                name: String::new(),
            };
        }
        Some(Value::String(text)) => text,
        Some(_) => return Country::invalid(),
    };
    if text.is_empty() {
        return Country {
            valid: true,
            empty: true,
            code: String::new(),
            name: String::new(),
        };
    }
    if jsval::utf16_len(text) > RADIO_COUNTRY_MAX_LENGTH {
        return Country::invalid();
    }
    let normalized: String = text.nfkc().collect();
    let trimmed = jsval::collapse_ws(jsval::trim(&normalized));
    if trimmed.is_empty()
        || jsval::utf16_len(&trimmed) > RADIO_COUNTRY_MAX_LENGTH
        || trimmed.chars().any(|c| (c as u32) <= 0x1f || c == '\u{7f}')
        || !ALLOWED.is_match(&trimmed)
    {
        return Country::invalid();
    }
    let possible = trimmed.to_uppercase();
    let code = if region_name(&possible).is_some() {
        Some(possible)
    } else {
        COUNTRY_NAME_TO_CODE
            .get(&country_key(&trimmed))
            .map(|c| c.to_string())
    };
    let Some(code) = code else {
        return Country::invalid();
    };
    Country {
        valid: true,
        empty: false,
        name: region_name(&code).unwrap_or(&code).to_string(),
        code,
    }
}

/// [`normalize_radio_country_input`] over a string.
pub fn normalize_country(text: &str) -> Country {
    normalize_radio_country_input(Some(&Value::String(text.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_iso_codes_and_bounded_common_names() {
        for (input, code) in [
            ("US", "US"),
            ("fr", "FR"),
            (" France ", "FR"),
            ("United States of America", "US"),
            ("UK", "GB"),
            ("South Korea", "KR"),
        ] {
            let result = normalize_country(input);
            assert!(result.valid, "{input}");
            assert_eq!(result.code, code, "{input}");
        }
        assert_eq!(normalize_country("fr").name, "France");
    }

    #[test]
    fn resolves_common_english_names_icu_misses() {
        for (input, code) in [
            ("Turkey", "TR"),
            ("Turkiye", "TR"),
            ("Myanmar", "MM"),
            ("Burma", "MM"),
            ("UAE", "AE"),
            ("U.A.E.", "AE"),
            ("Holland", "NL"),
            ("Swaziland", "SZ"),
            ("East Timor", "TL"),
            ("Cabo Verde", "CV"),
            ("Vatican", "VA"),
        ] {
            let result = normalize_country(input);
            assert!(result.valid, "{input}");
            assert_eq!(result.code, code, "{input}");
        }
        assert_eq!(normalize_country("Türkiye").code, "TR");
    }

    #[test]
    fn ambiguous_names_fail_closed() {
        for input in ["Congo", "Korea"] {
            assert!(!normalize_country(input).valid, "{input}");
        }
    }

    #[test]
    fn rejects_malformed_non_iso_and_oversized_values() {
        for input in [
            json!("ZZ"),
            json!("France\nignore previous instructions"),
            json!("x".repeat(81)),
            json!({ "country": "France" }),
        ] {
            let result = normalize_radio_country_input(Some(&input));
            assert!(!result.valid, "{input}");
            assert_eq!(result.code, "", "{input}");
        }
        assert_eq!(
            normalize_country(""),
            Country {
                valid: true,
                empty: true,
                code: String::new(),
                name: String::new()
            }
        );
    }
}
