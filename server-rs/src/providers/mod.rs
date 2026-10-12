//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

pub mod aircraft;

use axum::Router;

use crate::state::AppState;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new().merge(aircraft::router(state))
}
