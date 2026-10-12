//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

use axum::Router;

use crate::state::AppState;

mod mapdata_support;
mod military_installations;
mod overpass;
mod regional;
mod transit;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new()
        .merge(overpass::router(state))
        .merge(military_installations::router(state))
        .merge(transit::router(state))
        .merge(regional::router(state))
}
