//! `URLSearchParams`-style access to a request's query string.

use axum::extract::FromRequestParts;
use http::request::Parts;

use super::js;

/// Parsed query parameters, preserving order and repeats.
#[derive(Debug, Clone, Default)]
pub struct Params(pub Vec<(String, String)>);

impl Params {
    pub fn parse(query: &str) -> Self {
        Self(
            url::form_urlencoded::parse(query.trim_start_matches('?').as_bytes())
                .into_owned()
                .collect(),
        )
    }

    /// The first value for `key`, like `URLSearchParams.get`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn get_all(&self, key: &str) -> Vec<&str> {
        self.0
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    pub fn has(&self, key: &str) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }

    /// `requiredFiniteQueryNumber`: a finite number, or `None` when the
    /// value is missing, blank or not a number.
    pub fn finite(&self, key: &str) -> Option<f64> {
        let value = self.get(key)?;
        if value.trim().is_empty() {
            return None;
        }
        let number = js::number(value);
        number.is_finite().then_some(number)
    }

    /// `clampInt(params.get(key), min, max, fallback)`.
    pub fn clamp_int(&self, key: &str, min: i64, max: i64, fallback: i64) -> i64 {
        clamp_int(self.get(key), min, max, fallback)
    }

    /// Serialize back to a query string.
    pub fn to_query(&self) -> String {
        url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(self.0.iter())
            .finish()
    }
}

/// `clampInt(value, min, max, fallback)` with `parseInt` semantics.
pub fn clamp_int(value: Option<&str>, min: i64, max: i64, fallback: i64) -> i64 {
    match value.and_then(js::parse_int) {
        Some(number) => number.clamp(min, max),
        None => fallback,
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Params {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Params::parse(parts.uri.query().unwrap_or("")))
    }
}
