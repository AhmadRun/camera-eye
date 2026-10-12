//! Port of `src/tools/mcp/mcp.test.mjs`, plus checks on the composed local
//! server.

use std::sync::Arc;

use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, header};
use serde_json::{Map, Value, json};

use super::catalog::{Composed, Failure, Run, Tool, ToolError, ToolResult, ToolSpec, define_tool};
use super::http::handle_mcp_request;
use super::protocol::{MCP_PROTOCOL_VERSIONS, McpServer, Resource, to_mcp_tools};
use super::services::Services;

fn services() -> Arc<Services> {
    Arc::new(Services::new("http://127.0.0.1:9", &super::repository_root(), None).unwrap())
}

fn run(f: fn(Value) -> Result<ToolResult, Failure>) -> Run {
    Arc::new(move |args, _ctx| Box::pin(async move { f(args) }))
}

fn spec(
    name: &str,
    title: &str,
    description: &str,
    input_schema: Value,
    ui: Option<Value>,
) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        kind: None,
        title: title.into(),
        description: description.into(),
        input_schema,
        annotations: Map::new(),
        requires: Vec::new(),
        ui,
    }
}

fn count_tool(name: &str, f: fn(Value) -> Result<ToolResult, Failure>) -> Tool {
    let schema = json!({
        "type": "object",
        "properties": { "n": { "type": "integer" } },
        "additionalProperties": false,
    });
    define_tool(spec(name, "Count", "Counts things.", schema, None), run(f)).unwrap()
}

fn test_server() -> McpServer {
    let tools = vec![
        count_tool("count", |args| {
            let n = args.get("n").cloned().unwrap_or(json!(1));
            Ok(ToolResult::new(format!("{n} things."), json!({ "n": n })))
        }),
        count_tool("busy", |_| {
            Err(Failure::Tool(
                ToolError::new("retry_later", "Upstream is busy").retry(Some(10.0)),
            ))
        }),
        count_tool("crash", |_| {
            Err(Failure::other("database password is hunter2"))
        }),
    ];
    let catalog = Composed::new(tools, services(), |_| true).unwrap();
    McpServer::new(
        Box::new(catalog),
        "test",
        "1.0.0",
        Some("Be brief.".into()),
        Vec::new(),
    )
}

async fn request(server: &McpServer, method: &str, params: Option<Value>) -> Value {
    let mut message = json!({ "jsonrpc": "2.0", "id": 1, "method": method });
    if let Some(params) = params {
        message["params"] = params;
    }
    server.handle(&message).await.unwrap()
}

#[tokio::test]
async fn initialize_negotiates_a_supported_protocol_version() {
    let server = test_server();
    let known = request(
        &server,
        "initialize",
        Some(json!({ "protocolVersion": "2025-06-18" })),
    )
    .await;
    assert_eq!(
        known["result"],
        json!({
            "protocolVersion": "2025-06-18",
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": "test", "version": "1.0.0" },
            "instructions": "Be brief.",
        })
    );
    let unknown = request(
        &server,
        "initialize",
        Some(json!({ "protocolVersion": "1999-01-01" })),
    )
    .await;
    assert_eq!(
        unknown["result"]["protocolVersion"],
        MCP_PROTOCOL_VERSIONS[0]
    );
    assert_eq!(request(&server, "ping", None).await["result"], json!({}));
}

#[tokio::test]
async fn notifications_and_client_responses_get_no_reply() {
    let server = test_server();
    let note = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
    assert_eq!(server.handle(&note).await, None);
    assert_eq!(
        server
            .handle(&json!({ "jsonrpc": "2.0", "id": 9, "result": {} }))
            .await,
        None
    );
}

#[tokio::test]
async fn invalid_messages_and_unknown_methods_are_json_rpc_errors() {
    let server = test_server();
    let code = |response: Option<Value>| response.unwrap()["error"]["code"].clone();
    assert_eq!(
        code(server.handle(&json!({ "id": 1, "method": "ping" })).await),
        -32600
    );
    let batch = json!([{ "jsonrpc": "2.0", "id": 1, "method": "ping" }]);
    assert_eq!(code(server.handle(&batch).await), -32600);
    assert_eq!(
        code(server.handle(&json!({ "jsonrpc": "2.0", "id": 1 })).await),
        -32600
    );
    assert_eq!(
        request(&server, "tools/remove", None).await["error"]["code"],
        -32601
    );
    assert_eq!(
        request(&server, "toString", None).await["error"]["code"],
        -32601
    );
    assert_eq!(
        request(&server, "tools/list", Some(json!([]))).await["error"]["code"],
        -32602
    );
    let missing = request(&server, "tools/call", Some(json!({ "name": "missing" }))).await;
    assert_eq!(missing["error"]["code"], -32602);
}

#[tokio::test]
async fn tools_are_listed_with_titles_schemas_and_annotations() {
    let server = test_server();
    let list = request(&server, "tools/list", None).await;
    assert_eq!(
        list["result"]["tools"][0],
        json!({
            "name": "count",
            "title": "Count",
            "description": "Counts things.",
            "inputSchema": {
                "type": "object",
                "properties": { "n": { "type": "integer" } },
                "additionalProperties": false,
            },
            "annotations": { "title": "Count", "readOnlyHint": true },
        })
    );
    assert_eq!(to_mcp_tools(&server.catalog().list()).len(), 3);
}

#[tokio::test]
async fn tool_results_carry_text_and_structured_content_failures_stay_generic() {
    let server = test_server();
    let call = |params: Value| {
        let server = &server;
        async move { request(server, "tools/call", Some(params)).await["result"].clone() }
    };
    assert_eq!(
        call(json!({ "name": "count", "arguments": { "n": 3 } })).await,
        json!({
            "content": [
                { "type": "text", "text": "3 things." },
                { "type": "text", "text": "{\"n\":3}" },
            ],
            "structuredContent": { "n": 3 },
            "isError": false,
        })
    );
    assert_eq!(
        call(json!({ "name": "busy" })).await,
        json!({
            "content": [{ "type": "text", "text": "Upstream is busy" }],
            "structuredContent": { "error": "retry_later", "retry_after_seconds": 10 },
            "isError": true,
        })
    );
    let invalid = call(json!({ "name": "count", "arguments": { "n": "x" } })).await;
    assert_eq!(invalid["isError"], true);
    assert_eq!(invalid["structuredContent"]["error"], "invalid_arguments");
    let crash = call(json!({ "name": "crash" })).await;
    assert_eq!(crash["isError"], true);
    assert!(!crash.to_string().contains("hunter2"));
}

#[tokio::test]
async fn the_http_transport_accepts_one_json_message_per_post() {
    let server = test_server();
    let post = |body: &str, extra: &[(&'static str, &'static str)]| {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        for (name, value) in extra {
            headers.insert(*name, HeaderValue::from_static(value));
        }
        let body = Bytes::from(body.to_string());
        let server = &server;
        async move { handle_mcp_request(server, "POST", &headers, Some(body)).await }
    };
    let json_of = |response: axum::response::Response| async move {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    let ok = post(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#, &[]).await;
    assert_eq!(ok.status(), 200);
    assert_eq!(ok.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(
        json_of(ok).await,
        json!({ "jsonrpc": "2.0", "id": 1, "result": {} })
    );
    let note = post(
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        &[],
    )
    .await;
    assert_eq!(note.status(), 202);
    let parse = post("{", &[]).await;
    assert_eq!(parse.status(), 400);
    assert_eq!(json_of(parse).await["error"]["code"], -32700);
    assert_eq!(
        post("{}", &[("mcp-protocol-version", "1999-01-01")])
            .await
            .status(),
        400
    );
    assert_eq!(
        post("{}", &[("content-type", "text/plain")]).await.status(),
        415
    );
    assert_eq!(post(&"x".repeat(1024 * 1024 + 1), &[]).await.status(), 413);
    let get = handle_mcp_request(&server, "GET", &HeaderMap::new(), None).await;
    assert_eq!(get.status(), 405);
    assert_eq!(get.headers()[header::ALLOW], "POST");
}

#[tokio::test]
async fn resources_are_listed_and_read_and_tools_name_their_ui_resource() {
    let shown = define_tool(
        spec(
            "show",
            "Show",
            "Shows something in a view.",
            json!({ "type": "object", "properties": {} }),
            Some(json!({ "resourceUri": "ui://fixture/view" })),
        ),
        run(|_| Ok(ToolResult::new("shown", json!({})))),
    )
    .unwrap();
    let resource = json!({
        "uri": "ui://fixture/view",
        "name": "view",
        "mimeType": "text/html;profile=mcp-app",
        "text": "<!doctype html><p>view</p>",
        "_meta": { "ui": { "csp": { "frameDomains": ["https://a.example"] } } },
    });
    let with_ui = McpServer::new(
        Box::new(Composed::new(vec![shown], services(), |_| true).unwrap()),
        "fixture",
        "1",
        None,
        vec![Resource::from_json(resource.as_object().unwrap().clone())],
    );
    let call = |method: &'static str, params: Option<Value>| {
        let server = &with_ui;
        async move {
            let response = request(server, method, params).await;
            response
                .get("result")
                .or_else(|| response.get("error"))
                .unwrap()
                .clone()
        }
    };
    assert_eq!(
        call("initialize", Some(json!({}))).await["capabilities"],
        json!({ "tools": { "listChanged": false }, "resources": { "listChanged": false } })
    );
    assert_eq!(
        call("tools/list", None).await["tools"][0]["_meta"],
        json!({ "ui": { "resourceUri": "ui://fixture/view" }, "ui/resourceUri": "ui://fixture/view" })
    );
    let meta = json!({ "ui": { "csp": { "frameDomains": ["https://a.example"] } } });
    assert_eq!(
        call("resources/list", None).await["resources"],
        json!([{
            "uri": "ui://fixture/view",
            "name": "view",
            "mimeType": "text/html;profile=mcp-app",
            "_meta": meta,
        }])
    );
    assert_eq!(
        call(
            "resources/read",
            Some(json!({ "uri": "ui://fixture/view" }))
        )
        .await["contents"],
        json!([{
            "uri": "ui://fixture/view",
            "mimeType": "text/html;profile=mcp-app",
            "text": "<!doctype html><p>view</p>",
            "_meta": meta,
        }])
    );
    let other = call(
        "resources/read",
        Some(json!({ "uri": "ui://fixture/other" })),
    )
    .await;
    assert_eq!(other["code"], -32002);
    // A server without resources does not advertise them.
    let plain = request(&test_server(), "initialize", None).await;
    assert!(plain["result"]["capabilities"].get("resources").is_none());

    let bad = |ui: Value| {
        define_tool(
            spec("bad", "Bad", "Bad.", json!({ "type": "object" }), Some(ui)),
            run(|_| Ok(ToolResult::new("", json!({})))),
        )
        .err()
        .unwrap()
    };
    assert!(bad(json!({ "resourceUri": "https://a.example" })).contains("ui:// URI"));
    assert!(bad(json!({ "visibility": ["user"] })).contains("visibility"));
}

#[tokio::test]
async fn a_tool_only_an_app_may_call_answers_without_json_text() {
    let loader = define_tool(
        spec(
            "load",
            "Load",
            "Loads a file for the view.",
            json!({ "type": "object", "properties": {} }),
            Some(json!({ "visibility": ["app"] })),
        ),
        run(|_| Ok(ToolResult::new("loaded", json!({ "body": "abc" })))),
    )
    .unwrap();
    let server = McpServer::new(
        Box::new(Composed::new(vec![loader], services(), |_| true).unwrap()),
        "fixture",
        "1",
        None,
        Vec::new(),
    );
    let list = request(&server, "tools/list", None).await;
    assert_eq!(
        list["result"]["tools"][0]["_meta"],
        json!({ "ui": { "visibility": ["app"] } })
    );
    let result = request(
        &server,
        "tools/call",
        Some(json!({ "name": "load", "arguments": {} })),
    )
    .await;
    assert_eq!(
        result["result"]["content"],
        json!([{ "type": "text", "text": "loaded" }])
    );
    assert_eq!(
        result["result"]["structuredContent"],
        json!({ "body": "abc" })
    );
}

#[tokio::test]
async fn the_local_server_offers_every_exported_mcp_tool() {
    let server = super::create_local_mcp_server("http://127.0.0.1:9").unwrap();
    let init = request(
        &server,
        "initialize",
        Some(json!({ "protocolVersion": "2025-06-18" })),
    )
    .await;
    assert_eq!(init["result"]["serverInfo"]["name"], "camera-eye");
    assert!(init["result"]["capabilities"].get("resources").is_some());
    let list = request(&server, "tools/list", None).await;
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    for name in [
        "show_in_camera_eye",
        "situation_brief",
        "get_cyclones",
        "panel_request",
    ] {
        assert!(names.contains(&name), "{name} missing from {names:?}");
    }
    for name in &names {
        assert!(
            !super::tools::NOT_PORTED.contains(name),
            "{name} is offered on MCP but not ported"
        );
    }
    let resources = request(&server, "resources/list", None).await;
    assert!(
        !resources["result"]["resources"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn local_tools_answer_without_the_network() {
    let server = super::create_local_mcp_server("http://127.0.0.1:9").unwrap();
    let call = |params: Value| {
        let server = &server;
        async move { request(server, "tools/call", Some(params)).await["result"].clone() }
    };
    let empty = call(json!({ "name": "get_terrain_height", "arguments": { "points": [] } })).await;
    assert_eq!(empty["isError"], true);
    let shown = call(json!({
        "name": "show_in_camera_eye",
        "arguments": { "camera": { "lat": 10, "lon": 20 }, "layers": ["flights"] },
    }))
    .await;
    assert!(
        shown["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Open 10.000, 20.000 in Camera Eye")
    );
    let unreachable =
        call(json!({ "name": "get_weather_map", "arguments": { "map": "radar" } })).await;
    assert_eq!(unreachable["isError"], true);
    assert_eq!(unreachable["structuredContent"]["error"], "unavailable");
}
