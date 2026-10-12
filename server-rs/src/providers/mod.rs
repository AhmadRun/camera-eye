//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

use axum::Router;

use crate::state::AppState;

pub mod cctv;
pub mod radio;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new()
        .merge(cctv::router(state))
        .merge(radio::router(state))
}
