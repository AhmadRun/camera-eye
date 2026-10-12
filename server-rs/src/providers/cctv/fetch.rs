//! The upstream HTTP seam for the CCTV provider: what the JS took from
//! `fetch` (and its tests injected as `fetchImpl`).

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use http::HeaderMap;

use super::jsval;

/// How a request treats a 3xx answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Redirect {
    /// Follow redirects, like `fetch`'s default (up to 20 hops).
    Follow,
    /// Hand the 3xx back (`redirect: 'manual'`; `'error'` callers refuse it).
    Manual,
}

/// One outbound request.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub redirect: Redirect,
    /// Whether a compressed body is decoded (catalog JSON); media and frame
    /// relays ask for identity bodies so declared lengths stay true.
    pub decompress: bool,
}

impl FetchRequest {
    pub fn new(url: impl Into<String>, redirect: Redirect, decompress: bool) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
            redirect,
            decompress,
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// The value of a request header, by case-insensitive name.
    #[cfg(test)]
    pub fn header_value(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// An upstream answer: status, headers, and a body that is only read on demand.
pub struct FetchResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: BoxStream<'static, Result<Bytes, String>>,
}

/// Why a capped body read failed.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadError {
    TooLarge,
    Failed,
}

impl FetchResponse {
    /// A response from parts (tests and fakes).
    #[cfg(test)]
    pub fn new(status: u16, headers: &[(&str, &str)], body: impl Into<Bytes>) -> Self {
        let body: Bytes = body.into();
        Self::streaming(
            status,
            headers,
            futures_util::stream::iter([Ok(body)]).boxed(),
        )
    }

    #[cfg(test)]
    pub fn streaming(
        status: u16,
        headers: &[(&str, &str)],
        body: BoxStream<'static, Result<Bytes, String>>,
    ) -> Self {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(k.as_bytes()),
                http::HeaderValue::from_str(v),
            ) {
                map.append(name, value);
            }
        }
        Self {
            status,
            headers: map,
            body,
        }
    }

    /// `response.ok`.
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// `headers.get(name)`: repeated values joined with `", "`.
    pub fn header(&self, name: &str) -> Option<String> {
        let values: Vec<String> = self
            .headers
            .get_all(name)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect();
        (!values.is_empty()).then(|| values.join(", "))
    }

    /// `Number(headers.get('content-length'))`; missing is 0 like `Number(null)`.
    pub fn declared_length(&self) -> f64 {
        match self.header("content-length") {
            Some(text) => crate::common::js::number(&text),
            None => 0.0,
        }
    }

    /// Read the body, refusing more than `max_bytes`: early on a declared
    /// oversize length, then while streaming.
    pub async fn bytes_capped(self, max_bytes: usize) -> Result<Bytes, ReadError> {
        let declared = self.declared_length();
        if declared.is_finite() && declared > max_bytes as f64 {
            return Err(ReadError::TooLarge);
        }
        let mut body = self.body;
        let mut out = BytesMut::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|_| ReadError::Failed)?;
            if out.len() + chunk.len() > max_bytes {
                return Err(ReadError::TooLarge);
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// Read the whole body without a cap (what `response.json()` did).
    pub async fn bytes(self) -> Result<Bytes, ReadError> {
        self.bytes_capped(usize::MAX).await
    }

    /// `JSON.parse(await response.text())`.
    pub async fn json(self) -> Option<serde_json::Value> {
        let bytes = self.bytes().await.ok()?;
        jsval::parse_json(&bytes)
    }

    /// `readResponseJsonCapped`.
    pub async fn json_capped(self, max_bytes: usize) -> Option<serde_json::Value> {
        let bytes = self.bytes_capped(max_bytes).await.ok()?;
        jsval::parse_json(&bytes)
    }

    /// `response.text()`.
    pub async fn text(self) -> Option<String> {
        let bytes = self.bytes().await.ok()?;
        Some(String::from_utf8_lossy(jsval::strip_bom(&bytes)).into_owned())
    }
}

/// The outbound fetch function.
pub type Fetcher =
    Arc<dyn Fn(FetchRequest) -> BoxFuture<'static, Result<FetchResponse, String>> + Send + Sync>;

/// `fetch` over reqwest: one client per redirect/decompression policy. Node's
/// fetch sends `User-Agent: node` when a request names none, and ignores
/// proxy environment variables; both are kept.
pub fn production() -> Fetcher {
    let build = |redirect: Redirect, decompress: bool| {
        let builder = reqwest::Client::builder()
            .user_agent("node")
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .redirect(match redirect {
                Redirect::Follow => reqwest::redirect::Policy::limited(20),
                Redirect::Manual => reqwest::redirect::Policy::none(),
            });
        let builder = if decompress {
            builder
        } else {
            builder.no_gzip().no_brotli().no_deflate()
        };
        builder.build().expect("reqwest client")
    };
    let clients = Arc::new([
        build(Redirect::Follow, true),
        build(Redirect::Manual, true),
        build(Redirect::Follow, false),
        build(Redirect::Manual, false),
    ]);
    Arc::new(move |request: FetchRequest| {
        let clients = Arc::clone(&clients);
        async move {
            let index = match (request.redirect, request.decompress) {
                (Redirect::Follow, true) => 0,
                (Redirect::Manual, true) => 1,
                (Redirect::Follow, false) => 2,
                (Redirect::Manual, false) => 3,
            };
            let mut builder = clients[index].get(&request.url);
            for (name, value) in &request.headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            let response = builder.send().await.map_err(|e| e.to_string())?;
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let body = response
                .bytes_stream()
                .map(|chunk| chunk.map_err(|e| e.to_string()))
                .boxed();
            Ok(FetchResponse {
                status,
                headers,
                body,
            })
        }
        .boxed()
    })
}
