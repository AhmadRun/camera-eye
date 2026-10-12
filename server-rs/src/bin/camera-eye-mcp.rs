//! Local MCP server over stdio for agents such as Claude Code and Codex
//! (port of `server/mcp/stdio.js`).
//!
//!   camera-eye-mcp [--api-base http://localhost:4173]
//!
//! Reads newline-delimited JSON-RPC from stdin and writes responses to
//! stdout. Tools read data from a running Camera Eye server at the API
//! base. Diagnostics go to stderr.

use std::process::ExitCode;
use std::sync::Arc;

use camera_eye_server::mcp::{self, stdio};

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let api_base = match stdio::parse_args(&argv) {
        Ok(api_base) => api_base,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(1);
        }
    };
    let api_base = api_base.unwrap_or_else(|| mcp::DEFAULT_API_BASE.to_string());
    eprintln!("Camera Eye MCP server reading {api_base}");
    let server = match mcp::create_local_mcp_server(&api_base) {
        Ok(server) => Arc::new(server),
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(1);
        }
    };
    stdio::serve_stdio(
        server,
        tokio::io::stdin(),
        tokio::io::stdout(),
        Arc::new(|line: &str| eprintln!("<- {line}")),
    )
    .await;
    ExitCode::SUCCESS
}
