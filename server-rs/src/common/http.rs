//! Upstream fetching with hard byte caps (port of `readResponse*Capped`).

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::Response;

/// Why an upstream read failed.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// `code: 'RESPONSE_TOO_LARGE'` in the JS.
    #[error("Upstream response too large")]
    TooLarge,
    #[error("upstream timed out")]
    Timeout,
    #[error("upstream HTTP {0}")]
    Status(u16),
    #[error("upstream request failed: {0}")]
    Network(String),
    #[error("upstream returned invalid JSON: {0}")]
    Json(String),
}

impl UpstreamError {
    pub fn is_too_large(&self) -> bool {
        matches!(self, UpstreamError::TooLarge)
    }
}

impl From<reqwest::Error> for UpstreamError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            UpstreamError::Timeout
        } else {
            UpstreamError::Network(error.to_string())
        }
    }
}

/// Read a response body as bytes, refusing more than `max_bytes`: early on
/// an oversized `Content-Length`, then while streaming.
pub async fn read_bytes_capped(
    response: Response,
    max_bytes: usize,
) -> Result<Bytes, UpstreamError> {
    if let Some(declared) = response.content_length()
        && declared as usize > max_bytes
    {
        return Err(UpstreamError::TooLarge);
    }
    let mut stream = response.bytes_stream();
    let mut out = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if out.len() + chunk.len() > max_bytes {
            return Err(UpstreamError::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

/// [`read_bytes_capped`] decoded as UTF-8 (lossy, like `TextDecoder`).
pub async fn read_text_capped(
    response: Response,
    max_bytes: usize,
) -> Result<String, UpstreamError> {
    let bytes = read_bytes_capped(response, max_bytes).await?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// [`read_bytes_capped`] parsed as JSON.
pub async fn read_json_capped(
    response: Response,
    max_bytes: usize,
) -> Result<serde_json::Value, UpstreamError> {
    let bytes = read_bytes_capped(response, max_bytes).await?;
    serde_json::from_slice(&bytes).map_err(|e| UpstreamError::Json(e.to_string()))
}

/// GET `url` with a total timeout, returning the response whatever its status.
pub async fn get(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
    headers: &[(&str, &str)],
) -> Result<Response, UpstreamError> {
    let mut request = client.get(url).timeout(timeout);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    Ok(request.send().await?)
}

/// GET `url` and read a capped JSON body; non-2xx is [`UpstreamError::Status`].
pub async fn get_json(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
    max_bytes: usize,
    headers: &[(&str, &str)],
) -> Result<serde_json::Value, UpstreamError> {
    let response = get(client, url, timeout, headers).await?;
    if !response.status().is_success() {
        return Err(UpstreamError::Status(response.status().as_u16()));
    }
    tokio::time::timeout(timeout, read_json_capped(response, max_bytes))
        .await
        .map_err(|_| UpstreamError::Timeout)?
}

/// GET `url` and read a capped text body; non-2xx is [`UpstreamError::Status`].
pub async fn get_text(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
    max_bytes: usize,
    headers: &[(&str, &str)],
) -> Result<String, UpstreamError> {
    let response = get(client, url, timeout, headers).await?;
    if !response.status().is_success() {
        return Err(UpstreamError::Status(response.status().as_u16()));
    }
    tokio::time::timeout(timeout, read_text_capped(response, max_bytes))
        .await
        .map_err(|_| UpstreamError::Timeout)?
}
