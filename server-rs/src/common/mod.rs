//! Helpers shared by every provider route (port of `server/providers/common`
//! and the server-side parts of `src/sources/httpBody.js` / `rateLimit.js`).

pub mod coalesce;
pub mod disk;
pub mod geo;
pub mod http;
pub mod js;
pub mod query;
pub mod rate_limit;
pub mod reply;
pub mod same_site;
