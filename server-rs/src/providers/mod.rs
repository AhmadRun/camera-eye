//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

use axum::Router;

use crate::state::AppState;

pub mod cyclones;
pub mod fire_perimeters;
pub mod firms;
pub mod gbfs;
pub mod hazard_support;
pub mod space;
pub mod terrain;
pub mod traffic;
pub mod weather;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new()
        .merge(space::router(state))
        .merge(traffic::router(state))
        .merge(firms::router(state))
        .merge(terrain::router(state))
        .merge(gbfs::router(state))
        .merge(weather::router(state))
        .merge(cyclones::router(state))
        .merge(fire_perimeters::router(state))
}
