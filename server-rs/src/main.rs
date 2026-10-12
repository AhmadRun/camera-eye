//! Camera Eye server: serves the built browser app and its `/api` provider
//! routes. Replaces the Vite preview server and its Node middleware.

mod common;
mod env;
mod providers;
mod state;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::Router;
use axum::middleware;
use axum::routing::any;

use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "camera_eye_server=info".into()),
        )
        .init();

    let root = std::env::var("CE_ROOT")
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    // Like Vite's loadEnv: .env values fill only what the environment lacks.
    let _ = dotenvy::from_path(root.join(".env"));
    let dist = std::env::var("CE_DIST")
        .map(PathBuf::from)
        .unwrap_or_else(|_| root.join("dist"));

    let state = AppState::new(root, dist)?;
    let host = state.env.get("HOST").unwrap_or_else(|| "localhost".into());
    let port: u16 = state
        .env
        .get("PORT")
        .and_then(|p| common::js::parse_int(&p))
        .and_then(|p| u16::try_from(p).ok())
        .filter(|p| *p != 0)
        .unwrap_or(4173);

    let app = app(state);
    let bind_host = if host == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let listener = tokio::net::TcpListener::bind((bind_host.as_str(), port)).await?;
    tracing::info!("Camera Eye listening on http://{}", listener.local_addr()?);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// The whole server: provider routes, the API 404, then static assets.
pub fn app(state: AppState) -> Router {
    Router::new()
        .merge(providers::router(&state))
        .route("/api", any(web::api_not_found))
        .route("/api/{*rest}", any(web::api_not_found))
        .merge(web::static_files(&state))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            web::browser_headers,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            web::host_check,
        ))
        .with_state(state)
}
