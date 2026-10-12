# Porting a JS provider to Rust

The Rust server replaces the Vite preview server and the Node middleware in
`server/`. The browser app (`src/`) is unchanged and must keep working, so
every route must answer with the **same status codes, headers, JSON shapes and
error strings** as the JS version.

## Layout
- `src/providers/<name>.rs` or `src/providers/<name>/mod.rs`, one per JS provider
  group. Each exposes `pub fn router(state: &AppState) -> Router<AppState>`.
- Register it in `src/providers/mod.rs` with `.merge(<name>::router(state))`.
- Shared helpers live in `src/common/` (see below). Add a helper there only if
  two providers need it; otherwise keep it in the provider module.

## Conventions
- Read keys and settings through `state.env.get("NAME")` (trimmed, `None` when
  blank) or `state.env.raw("NAME")`, at request time: the key setup panel can
  change them while the server runs. Never call `std::env::set_var`.
- Upstream HTTP: `state.http` (reqwest) with the helpers in `common::http`
  (`get_json`, `get_text`, `read_*_capped`). Keep the JS byte caps and timeouts.
- Query strings: the `common::query::Params` extractor (`get`, `finite`,
  `clamp_int`), which follows `URLSearchParams` and JS `Number`/`parseInt`.
  JS number conversions are in `common::js`.
- Responses: `common::reply::{json, json_text, error, raw, with_header}`.
- In-flight request sharing: `common::coalesce::Coalescer`.
- Rate limits: `common::rate_limit` (`RateLimiter`, `cost_rate_limiter`,
  `client_key` from `ConnectInfo<SocketAddr>`).
- Same-site gate for cost-bearing routes: `common::same_site::admit_same_site`.
- Disk caches (`.ce-cache/...`): `common::disk` under `state.cache_dir`.
- In-memory caches with TTL: `moka::future::Cache` or a `parking_lot::Mutex`
  map with timestamps, matching the JS TTL and size limits.
- Long-lived state (caches, limiters) goes in a struct held in an `Arc` and
  captured by the module's route closures, built once in `router()`.
- Port the JS unit tests that cover the server logic as `#[cfg(test)]` tests.
- `cargo fmt`, `cargo clippy` and `cargo test` must be clean.
