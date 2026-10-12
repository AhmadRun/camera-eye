//! Aircraft providers: OpenSky flights (`/api/flights`), adsb.lol military
//! (`/api/military`), track backfill (`/api/flights/track`,
//! `/api/military/track`), adsbdb enrichment (`/api/adsbdb/...`) and local
//! decoder feeds (`/api/local-receivers/aircraft`).
//!
//! Ports of `server/providers/aircraft/*.js` and
//! `server/providers/local-receivers.js`, with the server-side record
//! normalizers from `src/sources/adsbRecords.js` and
//! `src/data/adsbLolFallback.js`.

mod adsb_lol;
mod enrichment;
mod jsval;
mod local_receivers;
mod opensky;
mod records;
mod tracks;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;

use crate::state::AppState;

/// Upstream endpoints, overridable in tests.
pub struct Upstreams {
    pub opensky_states: String,
    pub opensky_token: String,
    pub opensky_api: String,
    pub adsblol_api: String,
    pub adsblol_mil: String,
    pub adsblol_site: String,
    pub adsbdb: String,
}

impl Default for Upstreams {
    fn default() -> Self {
        Self {
            opensky_states: opensky::OPENSKY_STATES_URL.into(),
            opensky_token: opensky::OPENSKY_TOKEN_URL.into(),
            opensky_api: tracks::OPENSKY_API_BASE.into(),
            adsblol_api: opensky::ADSBLOL_API_BASE.into(),
            adsblol_mil: adsb_lol::ADSBLOL_MIL_URL.into(),
            adsblol_site: tracks::ADSBLOL_SITE_BASE.into(),
            adsbdb: enrichment::ADSBDB_BASE.into(),
        }
    }
}

/// Every aircraft route, against the real upstreams.
pub fn router(state: &AppState) -> Router<AppState> {
    router_with(state, Upstreams::default())
}

pub fn router_with(state: &AppState, upstreams: Upstreams) -> Router<AppState> {
    let tokens = Arc::new(opensky::TokenManager::new(upstreams.opensky_token));
    let opensky = Arc::new(opensky::OpenSky::new(
        Arc::clone(&tokens),
        upstreams.opensky_states,
        upstreams.adsblol_api,
    ));
    let adsbdb = Arc::new(enrichment::Adsbdb::new(
        upstreams.adsbdb,
        state.cache_dir.join("adsbdb.json"),
    ));
    let military = Arc::new(adsb_lol::Military::new(upstreams.adsblol_mil));
    let tracks = Arc::new(tracks::Tracks::new(
        Arc::clone(&opensky.tokens),
        upstreams.opensky_api,
        upstreams.adsblol_site,
    ));
    Router::new()
        .merge(opensky::router(opensky))
        .merge(enrichment::router(adsbdb))
        .merge(local_receivers::router(state))
        .merge(adsb_lol::router(military))
        .merge(tracks::router(tracks))
}
