//! Static data exported from the JS sources by
//! `scripts/export-mcp-tools.mjs`: Core's tool definitions, the surface
//! table, the layer registry, the panel's runtime script and the data the
//! tools embed.

use std::sync::OnceLock;

use serde_json::Value;

const EXPORT: &str = include_str!("data/tools.json");

/// The parsed export.
pub fn export() -> &'static Value {
    static PARSED: OnceLock<Value> = OnceLock::new();
    PARSED.get_or_init(|| serde_json::from_str(EXPORT).expect("tools.json is valid JSON"))
}

/// Tool definitions in composition order.
pub fn tools() -> &'static [Value] {
    export()["tools"].as_array().map_or(&[], Vec::as_slice)
}

/// The definition of one tool.
pub fn tool(name: &str) -> Option<&'static Value> {
    tools().iter().find(|tool| tool["name"] == name)
}

/// `TOOL_SURFACES`.
pub fn surfaces() -> &'static Value {
    &export()["surfaces"]
}

/// The package version when the export was made.
pub fn version() -> &'static str {
    export()["version"].as_str().unwrap_or("0.0.0")
}

/// `String(panelRuntime)`: the panel page's script.
pub fn panel_runtime() -> &'static str {
    export()["panelRuntime"].as_str().unwrap_or_default()
}

/// `BHOTE_KOSHI_FLOOD_PATH`.
pub fn bhote_koshi_flood_path() -> &'static Value {
    &export()["bhoteKoshiFloodPath"]
}

/// `LAYER_STATE_REGISTRY` as (id, token) in order.
pub fn layer_registry() -> &'static [(String, String)] {
    static REGISTRY: OnceLock<Vec<(String, String)>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        export()["layerRegistry"]
            .as_array()
            .map(|list| {
                list.iter()
                    .map(|entry| {
                        (
                            entry["id"].as_str().unwrap_or_default().to_string(),
                            entry["token"].as_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    })
}

/// `REGISTERED_LAYER_IDS`.
pub fn layer_ids() -> &'static [&'static str] {
    static IDS: OnceLock<Vec<&'static str>> = OnceLock::new();
    IDS.get_or_init(|| layer_registry().iter().map(|(id, _)| id.as_str()).collect())
}
