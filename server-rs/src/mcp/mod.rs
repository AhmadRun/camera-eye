//! The local MCP server (port of `server/mcp/*` and the protocol and tool
//! logic in `src/tools/`): Core's tools over services that read a running
//! Camera Eye server's `/api` routes. The HTTP transport is mounted at
//! `/mcp`; the `camera-eye-mcp` binary serves stdio.

pub mod area;
pub mod catalog;
pub mod data;
pub mod http;
pub mod js;
pub mod panel;
pub mod protocol;
pub mod schema;
pub mod services;
pub mod stdio;
pub mod tools;
pub mod view;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::Engine;

use self::catalog::{Composed, SurfaceCatalog, tool_from_export};
use self::protocol::McpServer;
use self::services::Services;

pub use self::http::{mount, set_local_port};

/// The default API base (`DEFAULT_API_BASE`).
pub const DEFAULT_API_BASE: &str = "http://localhost:4173";

pub const INSTRUCTIONS: &str = concat!(
    "Tools answer questions from Camera Eye's live public data. Location ",
    "tools take an area: a place name, a bbox, or lat/lon with radius_km. ",
    "Results are capped; check truncated and total before concluding there is nothing more. ",
    "Answers that can be shown in Camera Eye include data.view; to show one, call ",
    "show_in_camera_eye with that view, adding layers, style, a camera or marks as ",
    "needed. It shows live Camera Eye where the client displays apps and ",
    "returns a link everywhere."
);

/// The repository root: bundled data and `package.json` live there.
pub fn repository_root() -> PathBuf {
    if let Ok(root) = std::env::var("CE_ROOT") {
        return PathBuf::from(root);
    }
    if let Ok(dir) = std::env::current_dir()
        && (dir.join("src/data/local_data").is_dir() || dir.join("package.json").is_file())
    {
        return dir;
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// The app's version from `package.json`, or the exported one.
fn package_version(root: &Path) -> String {
    std::fs::read(root.join("package.json"))
        .ok()
        .and_then(|bytes| js::parse_json_bytes(&bytes))
        .and_then(|package| package.get("version").map(js::to_js_string))
        .unwrap_or_else(|| data::version().to_string())
}

/// A new panel key: 32 random bytes in base64url.
fn new_panel_key() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("the OS provides random bytes");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Compose Core's tools with services for `api_base`.
pub fn compose(services: Arc<Services>) -> Result<Composed, String> {
    let mut tools = Vec::new();
    for definition in data::tools() {
        let name = definition["name"].as_str().unwrap_or_default();
        let run = tools::run_for(name).ok_or_else(|| format!("{name} has no run function"))?;
        tools.push(tool_from_export(definition, run)?);
    }
    let check = services.clone();
    Composed::new(tools, services, move |key| check.has(key))
}

/// `createLocalMcpServer({ apiBase })`.
pub fn create_local_mcp_server(api_base: &str) -> Result<McpServer, String> {
    let root = repository_root();
    let version = package_version(&root);
    let panel_key = new_panel_key();
    let services = Arc::new(Services::new(api_base, &root, Some(panel_key.clone()))?);
    let catalog = compose(services)?;
    Ok(McpServer::new(
        Box::new(SurfaceCatalog::new(catalog, data::surfaces(), "mcp")),
        "camera-eye",
        version,
        Some(INSTRUCTIONS.to_string()),
        vec![panel::globe_panel_resource(
            data::panel_runtime(),
            &panel_key,
        )],
    ))
}
