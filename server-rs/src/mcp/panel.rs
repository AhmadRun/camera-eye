//! Port of `src/tools/globePanel.js`: the MCP Apps globe panel resource.

use serde_json::{Map, Value, json};

use super::js;
use super::protocol::Resource;

pub const GLOBE_PANEL_URI: &str = "ui://camera-eye/globe";
pub const PANEL_BASE: &str = "/panel/";
pub const PANEL_WORKER_PRELUDE_PATH: &str = "cesium/worker-prelude.js";
pub const MCP_APP_MIME_TYPE: &str = "text/html;profile=mcp-app";
const MCP_APPS_PROTOCOL_VERSION: &str = "2026-01-26";
const PANEL_HEIGHT_PX: u32 = 520;
const LOAD_TIMEOUT_MS: u32 = 90_000;
const PANEL_APP_BASE_URL: &str = "https://app.camera-eye.invalid/";
pub const PANEL_REQUEST_TOOL: &str = "panel_request";

const PROVIDER_ORIGINS: [&str; 17] = [
    "https://tile.googleapis.com",
    "https://maps.googleapis.com",
    "https://api.cesium.com",
    "https://assets.ion.cesium.com",
    "https://assets.cesium.com",
    "https://dev.virtualearth.net",
    "https://ecn.t0.tiles.virtualearth.net",
    "https://ecn.t1.tiles.virtualearth.net",
    "https://ecn.t2.tiles.virtualearth.net",
    "https://ecn.t3.tiles.virtualearth.net",
    "https://services.arcgisonline.com",
    "https://server.arcgisonline.com",
    "https://tile.openstreetmap.org",
    "https://tiles.openfreemap.org",
    "https://gibs.earthdata.nasa.gov",
    "https://fonts.googleapis.com",
    "https://fonts.gstatic.com",
];

/// Replace every `</script`, in any case, with `<\/script`.
fn escape_script_end(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    let mut search = 0;
    while let Some(found) = lower[search..].find("</script") {
        let at = search + found;
        out.push_str(&text[last..at]);
        out.push_str("<\\/script");
        last = at + "</script".len();
        search = last;
    }
    out.push_str(&text[last..]);
    out
}

fn panel_html(runtime: &str, panel_key: &str) -> String {
    let config = json!({
        "appBaseUrl": PANEL_APP_BASE_URL,
        "loadTimeoutMs": LOAD_TIMEOUT_MS,
        "panelBase": PANEL_BASE,
        "panelHeight": PANEL_HEIGHT_PX,
        "panelKey": panel_key,
        "protocolVersion": MCP_APPS_PROTOCOL_VERSION,
        "toolName": PANEL_REQUEST_TOOL,
        "workerPreludePath": PANEL_WORKER_PRELUDE_PATH,
    });
    let script = escape_script_end(&format!("({runtime})({});", js::stringify(&config)));
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Camera Eye</title>
<style>
  html, body {{ margin: 0; height: 100%; min-height: {PANEL_HEIGHT_PX}px; background: #05070a;
    color: #b8c4cc; font: 13px/1.4 system-ui, sans-serif; overflow: hidden; }}
  #status {{ position: absolute; inset: 0; z-index: 10000; display: flex; align-items: center;
    justify-content: center; padding: 0 24px; text-align: center; }}
  #actions {{ position: absolute; right: 10px; top: 10px; z-index: 10001; display: flex; gap: 6px; }}
  #actions button {{ padding: 6px 10px; border: 1px solid #3a4a55; border-radius: 6px;
    background: rgba(5, 7, 10, 0.75); color: #dfe8ee; font: inherit; cursor: pointer; }}
  #actions button[hidden] {{ display: none; }}
</style>
</head>
<body>
<div id="status">Waiting for a view…</div>
<div id="actions">
<button id="expand" type="button" hidden>Expand</button>
<button id="open" type="button" hidden>Open in Camera Eye</button>
</div>
<script>{script}</script>
</body>
</html>
"#
    )
}

/// `createGlobePanelResource({ runtime, panelKey })`.
pub fn globe_panel_resource(runtime: &str, panel_key: &str) -> Resource {
    let origins: Vec<Value> = PROVIDER_ORIGINS.iter().map(|o| json!(o)).collect();
    let mut fields = Map::new();
    fields.insert("uri".into(), json!(GLOBE_PANEL_URI));
    fields.insert("name".into(), json!("globe"));
    fields.insert("title".into(), json!("Camera Eye globe"));
    fields.insert(
        "description".into(),
        json!("Live Camera Eye, showing the view a tool returns."),
    );
    fields.insert("mimeType".into(), json!(MCP_APP_MIME_TYPE));
    fields.insert("text".into(), json!(panel_html(runtime, panel_key)));
    fields.insert(
        "_meta".into(),
        json!({
            "ui": {
                "csp": {"resourceDomains": origins.clone(), "connectDomains": origins},
                "prefersBorder": true,
            }
        }),
    );
    Resource::from_json(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_ends_are_escaped_in_any_case() {
        assert_eq!(
            escape_script_end("a</script>b</SCRIPT"),
            "a<\\/script>b<\\/script"
        );
    }

    #[test]
    fn the_panel_carries_its_key_and_policy() {
        let resource = globe_panel_resource("function run(config) {}", "KEY");
        let text = resource.text.unwrap();
        assert!(text.contains(r#"(function run(config) {})({"appBaseUrl":"https://app.camera-eye.invalid/","loadTimeoutMs":90000,"panelBase":"/panel/","panelHeight":520,"panelKey":"KEY","#));
        assert_eq!(resource.entry["mimeType"], MCP_APP_MIME_TYPE);
        assert!(!resource.entry.contains_key("text"));
    }
}
