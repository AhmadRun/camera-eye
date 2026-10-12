//! The `/api/*` routes, one module per provider (port of
//! `server/providers/local.js`).

use axum::Router;

use crate::state::AppState;

mod js_value;
mod key_setup;
mod openai;
mod places;
mod vessels;

/// Every provider route, in the order `localProviderPlugins` installed them.
pub fn router(state: &AppState) -> Router<AppState> {
    Router::new()
        .merge(openai::router(state))
        .merge(places::router(state))
        .merge(key_setup::router(state))
        .merge(vessels::router(state))
}
