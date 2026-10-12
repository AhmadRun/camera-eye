//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

pub mod wind;

use axum::Router;

use crate::state::AppState;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new().merge(wind::router(state))
}
