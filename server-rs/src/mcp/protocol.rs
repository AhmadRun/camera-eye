//! Port of `src/tools/mcp/protocol.js`: JSON-RPC 2.0 handling of
//! initialize, ping, tools/list, tools/call, resources/list and
//! resources/read. Transports pass parsed messages in and send the returned
//! responses out.

use std::sync::Arc;

use serde_json::{Map, Value, json};

use super::catalog::{Catalog, Failure, Tool};
use super::js;

/// Protocol revisions this server implements, newest first.
pub const MCP_PROTOCOL_VERSIONS: [&str; 3] = ["2025-11-25", "2025-06-18", "2025-03-26"];

pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;
pub const RESOURCE_NOT_FOUND: i64 = -32002;

const METHODS: [&str; 6] = [
    "initialize",
    "ping",
    "tools/list",
    "resources/list",
    "resources/read",
    "tools/call",
];

/// An MCP resource, such as the MCP Apps panel.
#[derive(Debug, Clone)]
pub struct Resource {
    /// Every field but `text`, in order (`uri`, `name`, ...).
    pub entry: Map<String, Value>,
    pub uri: String,
    pub mime_type: Option<Value>,
    pub text: Option<String>,
    pub meta: Option<Value>,
}

impl Resource {
    /// A resource from its JSON fields.
    pub fn from_json(fields: Map<String, Value>) -> Self {
        let mut entry = fields.clone();
        let text = entry
            .remove("text")
            .and_then(|v| v.as_str().map(String::from));
        Self {
            uri: fields
                .get("uri")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            mime_type: fields.get("mimeType").cloned(),
            meta: fields.get("_meta").filter(|v| js::truthy(v)).cloned(),
            text,
            entry,
        }
    }
}

/// `toMcpTools`: catalog tools as MCP tool definitions.
pub fn to_mcp_tools(tools: &[Arc<Tool>]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            let mut definition = Map::new();
            definition.insert("name".into(), json!(tool.name));
            definition.insert("title".into(), json!(tool.title));
            definition.insert("description".into(), json!(tool.description));
            definition.insert("inputSchema".into(), tool.input_schema.clone());
            let mut annotations = Map::new();
            annotations.insert("title".into(), json!(tool.title));
            for (key, value) in &tool.annotations {
                annotations.insert(key.clone(), value.clone());
            }
            definition.insert("annotations".into(), Value::Object(annotations));
            if let Some(ui) = &tool.ui {
                let mut meta = Map::new();
                meta.insert("ui".into(), Value::Object(ui.clone()));
                if let Some(uri) = ui.get("resourceUri").filter(|v| js::truthy(v)) {
                    meta.insert("ui/resourceUri".into(), uri.clone());
                }
                definition.insert("_meta".into(), Value::Object(meta));
            }
            Value::Object(definition)
        })
        .collect()
}

/// The protocol handler for a catalog.
pub struct McpServer {
    catalog: Box<dyn Catalog>,
    name: String,
    version: String,
    instructions: Option<String>,
    resources: Vec<Resource>,
}

impl McpServer {
    pub fn new(
        catalog: Box<dyn Catalog>,
        name: impl Into<String>,
        version: impl Into<String>,
        instructions: Option<String>,
        resources: Vec<Resource>,
    ) -> Self {
        Self {
            catalog,
            name: name.into(),
            version: version.into(),
            instructions,
            resources,
        }
    }

    pub fn catalog(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    /// The JSON-RPC response to a message, or None for notifications and
    /// client responses. Dropping the future cancels the request.
    pub async fn handle(&self, message: &Value) -> Option<Value> {
        let Some(object) = message.as_object() else {
            return Some(failure(
                Value::Null,
                INVALID_REQUEST,
                "Invalid JSON-RPC message",
            ));
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            let id = object.get("id").cloned().unwrap_or(Value::Null);
            return Some(failure(id, INVALID_REQUEST, "Invalid JSON-RPC message"));
        }
        let id = object.get("id");
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            // Responses from the client are accepted and ignored.
            return match id {
                Some(id) if !object.contains_key("result") && !object.contains_key("error") => {
                    Some(failure(id.clone(), INVALID_REQUEST, "Missing method"))
                }
                _ => None,
            };
        };
        let id = id?.clone();
        if !METHODS.contains(&method) {
            return Some(failure(
                id,
                METHOD_NOT_FOUND,
                &format!("Unknown method: {method}"),
            ));
        }
        let params = object.get("params");
        if params.is_some_and(|p| !p.is_object()) {
            return Some(failure(id, INVALID_PARAMS, "params must be an object"));
        }
        let params = params.and_then(Value::as_object);
        match self.dispatch(method, params).await {
            Ok(result) => Some(json!({"jsonrpc": "2.0", "id": id, "result": result})),
            Err((code, message)) => Some(failure(id, code, &message)),
        }
    }

    async fn dispatch(
        &self,
        method: &str,
        params: Option<&Map<String, Value>>,
    ) -> Result<Value, (i64, String)> {
        let param = |key: &str| params.and_then(|p| p.get(key));
        match method {
            "initialize" => {
                let requested = param("protocolVersion").and_then(Value::as_str);
                let version = requested
                    .filter(|v| MCP_PROTOCOL_VERSIONS.contains(v))
                    .unwrap_or(MCP_PROTOCOL_VERSIONS[0]);
                let mut capabilities = Map::new();
                capabilities.insert("tools".into(), json!({"listChanged": false}));
                if !self.resources.is_empty() {
                    capabilities.insert("resources".into(), json!({"listChanged": false}));
                }
                let mut result = Map::new();
                result.insert("protocolVersion".into(), json!(version));
                result.insert("capabilities".into(), Value::Object(capabilities));
                result.insert(
                    "serverInfo".into(),
                    json!({"name": self.name, "version": self.version}),
                );
                if let Some(instructions) = self.instructions.as_ref().filter(|i| !i.is_empty()) {
                    result.insert("instructions".into(), json!(instructions));
                }
                Ok(Value::Object(result))
            }
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({"tools": to_mcp_tools(&self.catalog.list())})),
            "resources/list" => Ok(json!({
                "resources": self.resources.iter().map(|r| Value::Object(r.entry.clone())).collect::<Vec<_>>()
            })),
            "resources/read" => {
                let uri = param("uri");
                let resource = uri
                    .and_then(Value::as_str)
                    .and_then(|uri| self.resources.iter().find(|r| r.uri == uri));
                let Some(resource) = resource else {
                    let shown = uri.map_or("undefined".into(), js::to_js_string);
                    return Err((RESOURCE_NOT_FOUND, format!("Unknown resource: {shown}")));
                };
                let mut content = Map::new();
                content.insert("uri".into(), json!(resource.uri));
                if let Some(mime) = &resource.mime_type {
                    content.insert("mimeType".into(), mime.clone());
                }
                if let Some(text) = &resource.text {
                    content.insert("text".into(), json!(text));
                }
                if let Some(meta) = &resource.meta {
                    content.insert("_meta".into(), meta.clone());
                }
                Ok(json!({"contents": [Value::Object(content)]}))
            }
            "tools/call" => {
                let Some(name) = param("name").and_then(Value::as_str) else {
                    return Err((INVALID_PARAMS, "tools/call needs a tool name".into()));
                };
                let Some(tool) = self.catalog.get(name) else {
                    return Err((INVALID_PARAMS, format!("Unknown tool: {name}")));
                };
                let args = match param("arguments") {
                    None | Some(Value::Null) => json!({}),
                    Some(args) => args.clone(),
                };
                Ok(match self.catalog.call(name, args).await {
                    Ok(result) => {
                        let app_only = tool
                            .ui
                            .as_ref()
                            .and_then(|ui| ui.get("visibility"))
                            .and_then(Value::as_array)
                            .is_some_and(|who| !who.iter().any(|w| w == "model"));
                        let mut content = vec![json!({"type": "text", "text": result.summary})];
                        if !app_only {
                            content
                                .push(json!({"type": "text", "text": js::stringify(&result.data)}));
                        }
                        for image in result.images.iter().flatten() {
                            content.push(json!({
                                "type": "image",
                                "data": image.data,
                                "mimeType": image.mime_type,
                            }));
                        }
                        json!({
                            "content": content,
                            "structuredContent": result.data,
                            "isError": false,
                        })
                    }
                    Err(error) => tool_failure(&error),
                })
            }
            _ => Err((INTERNAL_ERROR, "Internal error".into())),
        }
    }
}

/// The `isError` result for a failed tool call: a tool error is reported to
/// the model; anything else is not described.
fn tool_failure(error: &Failure) -> Value {
    let (text, structured) = match error {
        Failure::Tool(known) => {
            let mut structured = Map::new();
            structured.insert("error".into(), json!(known.code));
            if let Some(seconds) = known.retry_after_seconds {
                structured.insert("retry_after_seconds".into(), js::num(seconds));
            }
            (known.message.clone(), structured)
        }
        _ => {
            let mut structured = Map::new();
            structured.insert("error".into(), json!("unavailable"));
            ("The tool failed; try again later".to_string(), structured)
        }
    };
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": Value::Object(structured),
        "isError": true,
    })
}

/// A JSON-RPC parse-error response.
pub fn parse_error_response() -> Value {
    failure(Value::Null, PARSE_ERROR, "Parse error")
}

fn failure(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}
