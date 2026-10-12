//! Small JSON disk caches (the JS providers' `.ce-cache/*.json` files).

use std::path::Path;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Read and parse a JSON file; `None` when missing or unreadable.
pub async fn read_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let text = tokio::fs::read(path).await.ok()?;
    serde_json::from_slice(&text).ok()
}

/// Write a JSON file atomically (temp file, then rename), creating parents.
pub async fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    write_bytes(path, &bytes).await
}

/// Write bytes atomically, creating parent directories.
pub async fn write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension().and_then(|e| e.to_str()).unwrap_or(""),
        std::process::id()
    ));
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::rename(&tmp, path).await
}
