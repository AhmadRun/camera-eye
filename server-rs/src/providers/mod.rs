//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

pub mod aircraft;
pub mod cyclones;
pub mod fire_perimeters;
pub mod firms;
pub mod gbfs;
pub mod hazard_support;
pub mod mapdata_support;
pub mod military_installations;
pub mod overpass;
pub mod regional;
pub mod space;
pub mod terrain;
pub mod traffic;
pub mod transit;
pub mod weather;
pub mod wind;

use axum::Router;

use crate::state::AppState;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new()
        .merge(aircraft::router(state))
        .merge(space::router(state))
        .merge(traffic::router(state))
        .merge(firms::router(state))
        .merge(terrain::router(state))
        .merge(overpass::router(state))
        .merge(military_installations::router(state))
        .merge(regional::router(state))
        .merge(gbfs::router(state))
        .merge(transit::router(state))
        .merge(wind::router(state))
        .merge(weather::router(state))
        .merge(cyclones::router(state))
        .merge(fire_perimeters::router(state))
}
