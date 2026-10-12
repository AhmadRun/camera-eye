//! Port of `src/tools/catalog.js` and `src/tools/surfaces.js`: tool
//! definitions, their composition into a callable catalog, and the view of
//! it a surface (MCP) offers.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use serde_json::{Map, Value, json};

use super::js;
use super::schema;
use super::services::Services;

/// Error codes a tool may report.
pub const TOOL_ERROR_CODES: [&str; 5] = [
    "invalid_arguments",
    "unavailable",
    "unsupported",
    "malformed",
    "retry_later",
];

/// An expected failure with a stable code and a message for the caller.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolError {
    pub code: &'static str,
    pub message: String,
    pub retry_after_seconds: Option<f64>,
}

impl ToolError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        debug_assert!(TOOL_ERROR_CODES.contains(&code));
        Self {
            code,
            message: message.into(),
            retry_after_seconds: None,
        }
    }

    pub fn retry(mut self, seconds: Option<f64>) -> Self {
        self.retry_after_seconds = seconds;
        self
    }
}

/// What a tool's work can fail with, mirroring the JS error kinds:
/// `ToolError`, a live source's `LiveSourceError`, a `TypeError` and any
/// other `Error` (with the `status`/`code` properties some carry).
#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    Tool(ToolError),
    Source {
        code: String,
        message: String,
        status: Option<u16>,
        retry_after_ms: Option<f64>,
    },
    Type(String),
    Other {
        message: String,
        status: Option<u16>,
        code: Option<String>,
    },
}

impl Failure {
    pub fn tool(code: &'static str, message: impl Into<String>) -> Self {
        Failure::Tool(ToolError::new(code, message))
    }

    pub fn other(message: impl Into<String>) -> Self {
        Failure::Other {
            message: message.into(),
            status: None,
            code: None,
        }
    }

    /// `new LiveSourceError(code, message)` (retry after 20 s by default).
    pub fn source(code: &str, message: impl Into<String>) -> Self {
        Failure::Source {
            code: code.to_string(),
            message: message.into(),
            status: None,
            retry_after_ms: Some(20_000.0),
        }
    }

    /// `error.message`.
    pub fn message(&self) -> &str {
        match self {
            Failure::Tool(error) => &error.message,
            Failure::Source { message, .. }
            | Failure::Type(message)
            | Failure::Other { message, .. } => message,
        }
    }

    /// `error.code`, for the kinds that carry one.
    pub fn code(&self) -> Option<&str> {
        match self {
            Failure::Tool(error) => Some(error.code),
            Failure::Source { code, .. } => Some(code),
            Failure::Other { code, .. } => code.as_deref(),
            Failure::Type(_) => None,
        }
    }

    /// `error.status`, for errors that carry an HTTP status.
    pub fn status(&self) -> Option<u16> {
        match self {
            Failure::Other { status, .. } | Failure::Source { status, .. } => *status,
            _ => None,
        }
    }

    /// `fromSourceError`: a live source's failure as a tool error.
    pub fn from_source(self) -> Self {
        match self {
            Failure::Source {
                code,
                message,
                status,
                retry_after_ms,
            } => {
                let mapped = match code.as_str() {
                    "limited" => "retry_later",
                    "denied" | "unavailable" => "unavailable",
                    "malformed" => "malformed",
                    "unsupported" => "unsupported",
                    _ => {
                        return Failure::Source {
                            code,
                            message,
                            status,
                            retry_after_ms,
                        };
                    }
                };
                let retry = if mapped == "retry_later" {
                    retry_after_ms
                        .filter(|ms| ms.is_finite())
                        .map(|ms| (ms / 1000.0).ceil())
                } else {
                    None
                };
                Failure::Tool(ToolError::new(mapped, message).retry(retry))
            }
            other => other,
        }
    }
}

impl From<ToolError> for Failure {
    fn from(error: ToolError) -> Self {
        Failure::Tool(error)
    }
}

/// A base64 image a tool returns alongside its data.
#[derive(Debug, Clone, PartialEq)]
pub struct Image {
    pub mime_type: String,
    pub data: String,
}

/// `{ summary, data, images? }`.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResult {
    pub summary: String,
    pub data: Value,
    pub images: Option<Vec<Image>>,
}

impl ToolResult {
    pub fn new(summary: impl Into<String>, data: Value) -> Self {
        Self {
            summary: summary.into(),
            data,
            images: None,
        }
    }
}

pub type ToolFuture = BoxFuture<'static, Result<ToolResult, Failure>>;
pub type Run = Arc<dyn Fn(Value, Ctx) -> ToolFuture + Send + Sync>;

/// A validated tool definition (`defineTool`).
pub struct Tool {
    pub name: String,
    pub kind: String,
    pub title: String,
    pub description: String,
    pub input_schema: Value,
    /// `{ readOnlyHint, ...annotations }`.
    pub annotations: Map<String, Value>,
    pub requires: Vec<String>,
    /// `{ resourceUri?, visibility? }` or None.
    pub ui: Option<Map<String, Value>>,
    pub run: Run,
}

/// The fields of a tool definition, as `defineTool` takes them.
pub struct ToolSpec {
    pub name: String,
    pub kind: Option<String>,
    pub title: String,
    pub description: String,
    pub input_schema: Value,
    pub annotations: Map<String, Value>,
    pub requires: Vec<String>,
    pub ui: Option<Value>,
}

/// `defineTool`: validate a definition, or say what is wrong with it.
pub fn define_tool(spec: ToolSpec, run: Run) -> Result<Tool, String> {
    let name = spec.name;
    let valid_name = name.len() <= 64
        && name.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if !valid_name {
        return Err(format!("Invalid tool name: {name}"));
    }
    let kind = spec.kind.unwrap_or_else(|| "query".into());
    if kind != "query" && kind != "action" {
        return Err(format!("Invalid tool kind: {kind}"));
    }
    if spec.title.is_empty() {
        return Err(format!("{name} needs a title"));
    }
    if spec.description.is_empty() {
        return Err(format!("{name} needs a description"));
    }
    schema::assert_supported(&spec.input_schema, &format!("{name}.inputSchema"))?;
    if spec.input_schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(format!("{name}.inputSchema must describe an object"));
    }
    let ui = match spec.ui {
        None | Some(Value::Null) => None,
        Some(ui) => {
            let resource = ui.get("resourceUri");
            let visibility = ui.get("visibility");
            if let Some(uri) = resource
                && !uri.as_str().is_some_and(|u| u.starts_with("ui://"))
            {
                return Err(format!("{name}.ui.resourceUri must be a ui:// URI"));
            }
            if let Some(who) = visibility {
                let ok = who.as_array().is_some_and(|list| {
                    !list.is_empty()
                        && list
                            .iter()
                            .all(|w| matches!(w.as_str(), Some("model" | "app")))
                });
                if !ok {
                    return Err(format!("{name}.ui.visibility must list model and/or app"));
                }
            }
            if resource.is_none() && visibility.is_none() {
                return Err(format!("{name}.ui needs a resourceUri or a visibility"));
            }
            let mut out = Map::new();
            if let Some(uri) = resource.filter(|v| js::truthy(v)) {
                out.insert("resourceUri".into(), uri.clone());
            }
            if let Some(who) = visibility {
                out.insert("visibility".into(), who.clone());
            }
            Some(out)
        }
    };
    let mut annotations = Map::new();
    annotations.insert("readOnlyHint".into(), Value::Bool(kind == "query"));
    for (key, value) in spec.annotations {
        annotations.insert(key, value);
    }
    Ok(Tool {
        name,
        kind,
        title: spec.title,
        description: spec.description,
        input_schema: spec.input_schema,
        annotations,
        requires: spec.requires,
        ui,
        run,
    })
}

/// What a running tool can reach: the services and the other tools.
#[derive(Clone)]
pub struct Ctx {
    pub services: Arc<Services>,
    pub tools: Tools,
}

/// `tools.has(name)` and `tools.call(name, args)` through the same catalog.
#[derive(Clone)]
pub struct Tools {
    catalog: Composed,
}

impl Tools {
    pub fn has(&self, name: &str) -> bool {
        self.catalog.inner.index.contains_key(name)
    }

    pub fn call(&self, name: &str, args: Value) -> ToolFuture {
        self.catalog.invoke(name, args)
    }
}

/// The protocol's view of a catalog.
pub trait Catalog: Send + Sync + 'static {
    /// Available tool definitions, in composition order.
    fn list(&self) -> Vec<Arc<Tool>>;
    /// The available tool with this name.
    fn get(&self, name: &str) -> Option<Arc<Tool>>;
    /// Validate arguments and run a tool.
    fn call(&self, name: &str, args: Value) -> ToolFuture;
}

struct ComposedInner {
    available: Vec<Arc<Tool>>,
    index: HashMap<String, Arc<Tool>>,
    services: Arc<Services>,
}

/// `composeCatalog`: tools with the services an application supplies.
#[derive(Clone)]
pub struct Composed {
    inner: Arc<ComposedInner>,
}

impl Composed {
    /// Compose tools; a duplicate name fails, and tools whose services are
    /// missing are left out (`has_service` says which exist).
    pub fn new(
        tools: Vec<Tool>,
        services: Arc<Services>,
        has_service: impl Fn(&str) -> bool,
    ) -> Result<Self, String> {
        let mut seen = HashSet::new();
        for tool in &tools {
            if !seen.insert(tool.name.clone()) {
                return Err(format!("Duplicate tool name: {}", tool.name));
            }
        }
        let available: Vec<Arc<Tool>> = tools
            .into_iter()
            .filter(|tool| tool.requires.iter().all(|key| has_service(key)))
            .map(Arc::new)
            .collect();
        let index = available
            .iter()
            .map(|tool| (tool.name.clone(), tool.clone()))
            .collect();
        Ok(Self {
            inner: Arc::new(ComposedInner {
                available,
                index,
                services,
            }),
        })
    }

    fn invoke(&self, name: &str, args: Value) -> ToolFuture {
        let Some(tool) = self.inner.index.get(name).cloned() else {
            let error = Failure::tool("unsupported", format!("No tool named {name} is available"));
            return Box::pin(async move { Err(error) });
        };
        let ctx = Ctx {
            services: self.inner.services.clone(),
            tools: Tools {
                catalog: self.clone(),
            },
        };
        Box::pin(async move {
            let problems = schema::validate(&tool.input_schema, &args, "arguments");
            if !problems.is_empty() {
                return Err(Failure::tool("invalid_arguments", problems.join("; ")));
            }
            let run = (tool.run)(args, ctx);
            let result = match AssertUnwindSafe(run).catch_unwind().await {
                Ok(result) => result.map_err(Failure::from_source)?,
                Err(_) => return Err(Failure::other(format!("{} panicked", tool.name))),
            };
            if !js::truthy(&result.data) {
                return Err(Failure::Type(format!(
                    "{} returned no summary or data",
                    tool.name
                )));
            }
            Ok(result)
        })
    }
}

impl Catalog for Composed {
    fn list(&self) -> Vec<Arc<Tool>> {
        self.inner.available.clone()
    }

    fn get(&self, name: &str) -> Option<Arc<Tool>> {
        self.inner.index.get(name).cloned()
    }

    fn call(&self, name: &str, args: Value) -> ToolFuture {
        self.invoke(name, args)
    }
}

/// `catalogForSurface`: the tools a surface offers; tools that combine
/// others still reach the whole catalog.
pub struct SurfaceCatalog<C: Catalog> {
    catalog: C,
    offered: HashSet<String>,
}

/// Whether the surface table turns a tool off for `surface`.
pub fn offered_on(surfaces: &Value, tool: &str, surface: &str) -> bool {
    surfaces.get(tool).and_then(|entry| entry.get(surface)) != Some(&Value::Bool(false))
}

impl<C: Catalog> SurfaceCatalog<C> {
    pub fn new(catalog: C, surfaces: &Value, surface: &str) -> Self {
        let offered = catalog
            .list()
            .iter()
            .filter(|tool| offered_on(surfaces, &tool.name, surface))
            .map(|tool| tool.name.clone())
            .collect();
        Self { catalog, offered }
    }
}

impl<C: Catalog> Catalog for SurfaceCatalog<C> {
    fn list(&self) -> Vec<Arc<Tool>> {
        self.catalog
            .list()
            .into_iter()
            .filter(|tool| self.offered.contains(&tool.name))
            .collect()
    }

    fn get(&self, name: &str) -> Option<Arc<Tool>> {
        if self.offered.contains(name) {
            self.catalog.get(name)
        } else {
            None
        }
    }

    fn call(&self, name: &str, args: Value) -> ToolFuture {
        if !self.offered.contains(name) {
            let error = Failure::tool("unsupported", format!("No tool named {name} is available"));
            return Box::pin(async move { Err(error) });
        }
        self.catalog.call(name, args)
    }
}

/// A tool's definition from the exported data (`tools.json`) with `run`.
pub fn tool_from_export(definition: &Value, run: Run) -> Result<Tool, String> {
    let text = |key: &str| {
        definition
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    define_tool(
        ToolSpec {
            name: text("name"),
            kind: Some(text("kind")),
            title: text("title"),
            description: text("description"),
            input_schema: definition
                .get("inputSchema")
                .cloned()
                .unwrap_or(json!(null)),
            annotations: definition
                .get("annotations")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            requires: definition
                .get("requires")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            ui: definition.get("ui").cloned(),
        },
        run,
    )
}
