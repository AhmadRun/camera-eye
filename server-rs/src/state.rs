use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::env::Env;

/// Shared server state handed to every route.
#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    /// HTTP client for upstream providers.
    pub http: reqwest::Client,
    /// Runtime configuration (API keys and settings).
    pub env: Env,
    /// Repository root: `.env`, bundled data under `public/` and `src/`.
    pub root: PathBuf,
    /// Built browser assets served at `/`.
    pub dist: PathBuf,
    /// Disk cache directory (`.ce-cache` in the working directory).
    pub cache_dir: PathBuf,
}

impl std::ops::Deref for AppState {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

impl AppState {
    pub fn new(root: PathBuf, dist: PathBuf) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("camera-eye/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        let cache_dir = std::env::current_dir()?.join(".ce-cache");
        Ok(Self(Arc::new(Inner {
            http,
            env: Env::default(),
            root,
            dist,
            cache_dir,
        })))
    }
}
