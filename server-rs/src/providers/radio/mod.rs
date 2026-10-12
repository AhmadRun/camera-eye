//! `/api/radio/*`: port of `server/providers/radio.js` and
//! `server/providers/radio/*` (the Radio Browser directory proxy).
//!
//!   GET  /stations      the health-policed station catalog
//!   POST /click/:uuid   count a listen for a station this catalog served

mod catalog;
mod constants;
mod country;
mod regions;
mod stations;
mod transport;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Value, json};

use self::catalog::Directory;
use self::stations::RADIO_UUID_RE;
use crate::common::reply;
use crate::state::AppState;

/// Mounted routes for `/api/radio`.
pub fn router(_state: &AppState) -> Router<AppState> {
    let directory = Directory::new(
        transport::pinned_fetch(),
        transport::system_lookup(),
        Arc::new(crate::common::js::now_ms),
    );
    routes(directory)
}

fn routes<S: Clone + Send + Sync + 'static>(directory: Arc<Directory>) -> Router<S> {
    let handler = move |request: Request| {
        let directory = Arc::clone(&directory);
        async move { handle(directory, request).await }
    };
    Router::new()
        .route("/api/radio", any(handler.clone()))
        .route("/api/radio/", any(handler.clone()))
        .route("/api/radio/{*rest}", any(handler))
}

/// `sendJson`.
fn send_json(status: u16, body: &Value) -> Response {
    reply::raw(
        status,
        "application/json; charset=utf-8",
        body.to_string(),
        Some("no-store"),
    )
}

/// An empty answer with only the given headers.
fn bare(status: u16, headers: &[(&str, &str)]) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() =
        http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in headers {
        response = reply::with_header(response, name, value);
    }
    response
}

/// What the middleware saw as `req.url`: the mount stripped, query kept.
fn mounted_path(request: &Request) -> Option<String> {
    let path = request.uri().path();
    let rest = path.get("/api/radio".len()..).unwrap_or("");
    let mut stripped = if rest.is_empty() {
        "/".to_string()
    } else {
        rest.to_string()
    };
    if let Some(query) = request.uri().query() {
        stripped.push('?');
        stripped.push_str(query);
    }
    let url = url::Url::parse("http://localhost/")
        .ok()?
        .join(&stripped)
        .ok()?;
    Some(url.path().to_string())
}

async fn handle(directory: Arc<Directory>, request: Request) -> Response {
    static CLICK: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)^/click/([0-9a-f-]+)$").expect("regex"));
    let Some(path) = mounted_path(&request) else {
        return send_json(404, &json!({ "error": "Unknown radio route" }));
    };
    let method = request.method().clone();
    drop(request);

    if path == "/stations" {
        if method != http::Method::GET {
            return bare(405, &[("Allow", "GET"), ("Cache-Control", "no-store")]);
        }
        return match directory.catalog().await {
            Ok(served) => {
                let catalog = served.catalog;
                send_json(
                    200,
                    &json!({
                        "stations": *catalog.stations,
                        "updatedAt": catalog.updated_at,
                        "stale": served.stale,
                        "degraded": catalog.degraded,
                        "degradedReason": catalog.degraded_reason.filter(|r| !r.is_empty()),
                        "coverage": catalog.coverage,
                        "acceptedGeneration": catalog.accepted_generation,
                        "catalogInstance": directory.instance,
                    }),
                )
            }
            Err(error) => send_json(
                503,
                &json!({
                    "error": "Radio directory is temporarily unavailable",
                    "degraded": true,
                    "degradedReason": error.degraded_reason.filter(|r| !r.is_empty()),
                }),
            ),
        };
    }

    if let Some(caps) = CLICK.captures(&path) {
        if method != http::Method::POST {
            return bare(405, &[("Allow", "POST"), ("Cache-Control", "no-store")]);
        }
        let id = caps[1].to_lowercase();
        if !RADIO_UUID_RE.is_match(&id) || !directory.is_served(&id) {
            return send_json(404, &json!({ "error": "Unknown radio station" }));
        }
        // Fire and forget, after the answer is decided.
        let counter = Arc::clone(&directory);
        tokio::spawn(async move {
            let _ = counter.fetch_path(&format!("/json/url/{id}")).await;
        });
        return bare(204, &[("Cache-Control", "no-store")]);
    }

    send_json(404, &json!({ "error": "Unknown radio route" }))
}

#[cfg(test)]
mod tests {
    use super::stations::tests::{UUID, station};
    use super::transport::{Lookup, PinnedFetch};
    use super::*;
    use crate::providers::cctv::fetch::{FetchRequest, FetchResponse};
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use tower::ServiceExt;

    fn public_lookup() -> Lookup {
        Arc::new(|_| async { Ok(vec!["93.184.216.34".to_string()]) }.boxed())
    }

    fn respond(value: Value) -> FetchResponse {
        FetchResponse::new(
            200,
            &[("content-type", "application/json")],
            value.to_string(),
        )
    }

    fn fake<F>(handler: F) -> PinnedFetch
    where
        F: Fn(&FetchRequest) -> Result<FetchResponse, String> + Send + Sync + 'static,
    {
        Arc::new(move |request, _| {
            let result = handler(&request);
            async move { result }.boxed()
        })
    }

    fn clock(start: i64) -> (Arc<AtomicI64>, Arc<dyn Fn() -> i64 + Send + Sync>) {
        let cell = Arc::new(AtomicI64::new(start));
        let read = Arc::clone(&cell);
        (cell, Arc::new(move || read.load(Ordering::SeqCst)))
    }

    fn proxy(fetch: PinnedFetch, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Router {
        routes(Directory::new(fetch, public_lookup(), now))
    }

    fn query_tag(url: &str) -> Option<String> {
        url::Url::parse(url)
            .ok()?
            .query_pairs()
            .find(|(k, _)| k == "tag")
            .map(|(_, v)| v.into_owned())
    }

    fn catalog_rows(prefix: &str, tags: &str, count: usize) -> Value {
        Value::Array(
            (0..count)
                .map(|index| {
                    station(json!({
                        "stationuuid": format!("{prefix}-0000-4000-8000-{index:012x}"),
                        "name": format!("{prefix} Station {index}"),
                        "tags": tags,
                        "clickcount": 1000 - index as i64,
                    }))
                })
                .collect(),
        )
    }

    fn healthy_rows(url: &str, prefix: &str) -> Value {
        catalog_rows(
            prefix,
            &query_tag(url).unwrap_or_else(|| "news".into()),
            400,
        )
    }

    fn servers() -> Result<FetchResponse, String> {
        Ok(respond(json!([{ "name": "de1.api.radio-browser.info" }])))
    }

    async fn invoke(app: &Router, path: &str, method: &str) -> (u16, http::HeaderMap, Value) {
        let request = http::Request::builder()
            .method(method)
            .uri(format!("/api/radio{path}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, headers, body)
    }

    #[tokio::test]
    async fn proxy_coalesces_refreshes_omits_favicons_and_counts_known_stations_only() {
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        let fetch = fake(move |request| {
            counter.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.redirect, Redirect::Manual);
            assert_eq!(
                request.header_value("User-Agent"),
                Some(constants::RADIO_USER_AGENT)
            );
            if request.url.contains("/json/servers") {
                return servers();
            }
            if request.url.contains("/json/stations/search") {
                return Ok(respond(json!([station(json!({}))])));
            }
            if request.url.contains(&format!("/json/url/{UUID}")) {
                return Ok(respond(json!({ "ok": "true" })));
            }
            Ok(FetchResponse::new(404, &[], "{}"))
        });
        let app = proxy(fetch, Arc::new(crate::common::js::now_ms));
        let (first, second) = tokio::join!(
            invoke(&app, "/stations", "GET"),
            invoke(&app, "/stations", "GET")
        );
        assert_eq!((first.0, second.0), (200, 200));
        assert_eq!(first.1["content-type"], "application/json; charset=utf-8");
        assert_eq!(first.1["cache-control"], "no-store");
        let body = first.2;
        assert_eq!(body["stations"].as_array().unwrap().len(), 1);
        assert_eq!(body["acceptedGeneration"], Value::Null);
        assert!(body["stations"][0].get("favicon").is_none());
        let keys: Vec<&str> = body
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "stations",
                "updatedAt",
                "stale",
                "degraded",
                "degradedReason",
                "coverage",
                "acceptedGeneration",
                "catalogInstance"
            ]
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            10,
            "one discovery plus nine shared catalog queries"
        );

        let unknown = invoke(&app, "/click/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "POST").await;
        assert_eq!(unknown.0, 404);
        assert_eq!(unknown.2, json!({ "error": "Unknown radio station" }));
        let known = invoke(&app, &format!("/click/{}", UUID.to_uppercase()), "POST").await;
        assert_eq!(known.0, 204);
        assert_eq!(known.1["cache-control"], "no-store");
        for _ in 0..50 {
            if count.load(Ordering::SeqCst) == 11 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(count.load(Ordering::SeqCst), 11);
    }

    #[tokio::test]
    async fn method_and_route_validation_happen_before_any_upstream_refresh() {
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        let app = proxy(
            fake(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(respond(json!([])))
            }),
            Arc::new(crate::common::js::now_ms),
        );
        let (status, headers, _) = invoke(&app, "/stations", "POST").await;
        assert_eq!(status, 405);
        assert_eq!(headers["allow"], "GET");
        let (status, headers, _) = invoke(&app, "/click/1234", "GET").await;
        assert_eq!(status, 405);
        assert_eq!(headers["allow"], "POST");
        assert_eq!(invoke(&app, "/click/not-a-uuid", "POST").await.0, 404);
        let (status, _, body) = invoke(&app, "/anything", "GET").await;
        assert_eq!(status, 404);
        assert_eq!(body, json!({ "error": "Unknown radio route" }));
        assert_eq!(invoke(&app, "", "GET").await.0, 404);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_failed_refresh_serves_the_previous_catalog_as_stale() {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failing = Arc::clone(&fail);
        let (time, now) = clock(1_000_000);
        let app = proxy(
            fake(move |request| {
                if failing.load(Ordering::SeqCst) {
                    return Err("offline".into());
                }
                if request.url.contains("/json/servers") {
                    return servers();
                }
                Ok(respond(healthy_rows(&request.url, "30000000")))
            }),
            now,
        );
        assert_eq!(invoke(&app, "/stations", "GET").await.0, 200);
        time.fetch_add(46 * 60 * 1000, Ordering::SeqCst);
        fail.store(true, Ordering::SeqCst);
        let (status, _, body) = invoke(&app, "/stations", "GET").await;
        assert_eq!(status, 200);
        assert_eq!(body["stale"], true);
        assert_eq!(body["degraded"], true);
    }

    /// A warm healthy catalog, then a refresh whose broad query returns
    /// `replacement` and whose specialist queries answer with `specialist`.
    async fn warm_then_refresh(replacement: Value, specialist: Value) -> (Value, Value) {
        let degraded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&degraded);
        let (time, now) = clock(3_000_000);
        let app = proxy(
            fake(move |request| {
                if request.url.contains("/json/servers") {
                    return servers();
                }
                if !flag.load(Ordering::SeqCst) {
                    return Ok(respond(healthy_rows(&request.url, "30000000")));
                }
                if query_tag(&request.url).is_none() {
                    return Ok(respond(replacement.clone()));
                }
                Ok(respond(specialist.clone()))
            }),
            now,
        );
        let healthy = invoke(&app, "/stations", "GET").await.2;
        time.fetch_add(46 * 60 * 1000, Ordering::SeqCst);
        degraded.store(true, Ordering::SeqCst);
        let retained = invoke(&app, "/stations", "GET").await.2;
        (healthy, retained)
    }

    fn assert_retained(healthy: &Value, retained: &Value) {
        assert_eq!(retained["coverage"]["successfulQueries"], 1);
        assert_eq!(retained["degraded"], true);
        assert_eq!(retained["stale"], true);
        assert_eq!(retained["updatedAt"], healthy["updatedAt"]);
        assert_eq!(healthy["acceptedGeneration"], 1);
        assert_eq!(
            retained["acceptedGeneration"],
            healthy["acceptedGeneration"]
        );
        assert_eq!(retained["stations"][0]["id"], healthy["stations"][0]["id"]);
    }

    #[tokio::test]
    async fn malformed_specialist_payloads_cannot_replace_a_warm_catalog() {
        let (healthy, retained) = warm_then_refresh(
            catalog_rows("40000000", "news,jazz", 400),
            json!({ "error": "partial outage" }),
        )
        .await;
        assert_retained(&healthy, &retained);
    }

    #[tokio::test]
    async fn rejected_specialist_rows_do_not_count_toward_replacement() {
        let (healthy, retained) = warm_then_refresh(
            catalog_rows("40000000", "news,jazz", 400),
            json!([station(json!({ "lastcheckok": 0 }))]),
        )
        .await;
        assert_retained(&healthy, &retained);
    }

    #[tokio::test]
    async fn music_only_specialist_responses_cannot_replace_a_warm_catalog() {
        let rows = catalog_rows("40000000", "music,pop", 400);
        let (healthy, retained) = warm_then_refresh(rows.clone(), rows).await;
        assert_eq!(healthy["degraded"], false);
        assert_eq!(healthy["coverage"]["successfulQueries"], 9);
        assert_retained(&healthy, &retained);
    }

    #[tokio::test]
    async fn each_instance_scopes_its_generation_with_a_unique_token() {
        let fetch = || {
            fake(|request| {
                if request.url.contains("/json/servers") {
                    servers()
                } else {
                    Ok(respond(healthy_rows(&request.url, "30000000")))
                }
            })
        };
        let first = proxy(fetch(), clock(4_000_000).1);
        let second = proxy(fetch(), clock(4_000_000).1);
        let a1 = invoke(&first, "/stations", "GET").await.2;
        let a2 = invoke(&first, "/stations", "GET").await.2;
        let b1 = invoke(&second, "/stations", "GET").await.2;
        assert!(!a1["catalogInstance"].as_str().unwrap().is_empty());
        assert_eq!(a1["catalogInstance"], a2["catalogInstance"]);
        assert_ne!(a1["catalogInstance"], b1["catalogInstance"]);
        assert_eq!(a1["acceptedGeneration"], 1);
        assert_eq!(b1["acceptedGeneration"], 1);
    }

    #[tokio::test]
    async fn specialist_credit_requires_matching_tags_not_names() {
        let app = proxy(
            fake(|request| {
                if request.url.contains("/json/servers") {
                    return servers();
                }
                let tag = query_tag(&request.url);
                let mut rows = catalog_rows("50000000", "music,pop", 400);
                for row in rows.as_array_mut().unwrap() {
                    if let Some(tag) = &tag {
                        row["name"] =
                            json!(format!("{tag} Radio {}", row["name"].as_str().unwrap()));
                    }
                }
                Ok(respond(rows))
            }),
            Arc::new(crate::common::js::now_ms),
        );
        let (status, _, body) = invoke(&app, "/stations", "GET").await;
        assert_eq!(status, 200);
        assert_eq!(body["coverage"]["successfulQueries"], 1);
        assert_eq!(body["degraded"], true);
        assert!(
            body["degradedReason"]
                .as_str()
                .unwrap()
                .contains("query-coverage-below-policy")
        );
    }

    #[tokio::test]
    async fn specialist_credit_follows_normalized_embedded_tags() {
        let app = proxy(
            fake(|request| {
                if request.url.contains("/json/servers") {
                    return servers();
                }
                let tags = match query_tag(&request.url).as_deref() {
                    Some("news") => "Local_NEWS_Update",
                    Some("talk") => "Community-TALK-Show",
                    Some("weather") => "Severe WEATHER Alerts",
                    Some("emergency") => "Emergency_Dispatch",
                    Some("scanner") => "Police-Scanner-Radio",
                    Some("aviation") => "Civil AVIATION Radio",
                    Some("marine") => "Coastal-Marine-Radio",
                    Some("traffic") => "Daily_TRAFFIC_Reports",
                    _ => "music",
                };
                Ok(respond(catalog_rows("60000000", tags, 400)))
            }),
            Arc::new(crate::common::js::now_ms),
        );
        let (status, _, body) = invoke(&app, "/stations", "GET").await;
        assert_eq!(status, 200);
        assert_eq!(body["coverage"]["successfulQueries"], 9);
        assert_eq!(body["coverage"]["stationCount"], 400);
        assert_eq!(body["degraded"], false);
    }

    #[tokio::test]
    async fn a_cold_partial_catalog_is_degraded_while_zero_rows_are_unavailable() {
        let partial = proxy(
            fake(|request| {
                if request.url.contains("/json/servers") {
                    return servers();
                }
                if query_tag(&request.url).is_none() {
                    return Ok(respond(json!([station(json!({}))])));
                }
                Err("specialist query unavailable".into())
            }),
            Arc::new(crate::common::js::now_ms),
        );
        let (status, _, body) = invoke(&partial, "/stations", "GET").await;
        assert_eq!(status, 200);
        assert_eq!(body["stations"].as_array().unwrap().len(), 1);
        assert_eq!(body["stale"], false);
        assert_eq!(body["degraded"], true);
        assert_eq!(body["acceptedGeneration"], Value::Null);
        assert!(
            body["degradedReason"]
                .as_str()
                .unwrap()
                .contains("query-coverage-below-policy")
        );

        let empty = proxy(
            fake(|request| {
                if request.url.contains("/json/servers") {
                    servers()
                } else {
                    Ok(respond(json!([])))
                }
            }),
            Arc::new(crate::common::js::now_ms),
        );
        let (status, _, body) = invoke(&empty, "/stations", "GET").await;
        assert_eq!(status, 503);
        assert_eq!(body["error"], "Radio directory is temporarily unavailable");
        assert_eq!(body["degraded"], true);
    }

    #[tokio::test]
    async fn generations_advance_only_after_healthy_admission() {
        let mode = Arc::new(parking_lot::Mutex::new("healthy-a"));
        let current = Arc::clone(&mode);
        let (time, now) = clock(5_000_000);
        let app = proxy(
            fake(move |request| {
                if request.url.contains("/json/servers") {
                    return servers();
                }
                let mode = *current.lock();
                if mode == "degraded" {
                    return Ok(respond(if query_tag(&request.url).is_some() {
                        json!([])
                    } else {
                        catalog_rows("70000000", "news,jazz", 400)
                    }));
                }
                let prefix = if mode == "healthy-a" {
                    "71000000"
                } else {
                    "72000000"
                };
                Ok(respond(healthy_rows(&request.url, prefix)))
            }),
            now,
        );
        let first = invoke(&app, "/stations", "GET").await.2;
        assert_eq!(first["acceptedGeneration"], 1);
        assert_eq!(first["degraded"], false);

        time.fetch_add(46 * 60 * 1000, Ordering::SeqCst);
        *mode.lock() = "degraded";
        let retained = invoke(&app, "/stations", "GET").await.2;
        assert_eq!(retained["acceptedGeneration"], 1);
        assert_eq!(retained["stale"], true);
        assert_eq!(retained["stations"][0]["id"], first["stations"][0]["id"]);

        time.fetch_add(46 * 60 * 1000, Ordering::SeqCst);
        *mode.lock() = "healthy-b";
        let recovered = invoke(&app, "/stations", "GET").await.2;
        assert_eq!(recovered["acceptedGeneration"], 2);
        assert_ne!(recovered["stations"][0]["id"], first["stations"][0]["id"]);
    }

    #[tokio::test]
    async fn the_catalog_is_hard_capped_at_750_stations() {
        let rows: Vec<Value> = (0..810)
            .map(|index| {
                station(json!({
                    "stationuuid": format!("00000000-0000-4000-8000-{index:012x}"),
                    "name": format!("Station {index}"),
                    "geo_lat": -70 + (index % 140),
                    "geo_long": -175 + (index % 350),
                    "clickcount": 1000 - index,
                }))
            })
            .collect();
        let rows = Value::Array(rows);
        let app = proxy(
            fake(move |request| {
                if request.url.contains("/json/servers") {
                    servers()
                } else {
                    Ok(respond(rows.clone()))
                }
            }),
            Arc::new(crate::common::js::now_ms),
        );
        let (status, _, body) = invoke(&app, "/stations", "GET").await;
        assert_eq!(status, 200);
        assert_eq!(body["stations"].as_array().unwrap().len(), 750);
    }

    #[tokio::test]
    async fn redirects_are_refused_for_every_target_class() {
        for location in [
            "http://169.254.169.254/latest/meta-data/",
            "https://127.0.0.1/private",
            "https://10.0.0.1/private",
            "https://192.168.1.1/private",
            "https://[::1]/private",
            "https://public.example.org/off-policy",
            "https://de1.api.radio-browser.info/not-an-allowed-path",
        ] {
            let count = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&count);
            let app = proxy(
                fake(move |request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.redirect, Redirect::Manual);
                    Ok(FetchResponse::new(302, &[("location", location)], ""))
                }),
                Arc::new(crate::common::js::now_ms),
            );
            assert_eq!(invoke(&app, "/stations", "GET").await.0, 503, "{location}");
            assert!(count.load(Ordering::SeqCst) > 0, "{location}");
        }
    }

    #[tokio::test]
    async fn any_forbidden_resolution_is_rejected_before_fetch() {
        let count = Arc::new(AtomicUsize::new(0));
        for addresses in [
            vec!["127.0.0.1"],
            vec!["169.254.169.254"],
            vec!["198.51.100.9"],
            vec!["203.0.113.9"],
            vec!["2001:db8::1"],
            vec!["93.184.216.34", "fd00::1"],
        ] {
            let counter = Arc::clone(&count);
            let lookup: Lookup = Arc::new(move |_| {
                let rows: Vec<String> = addresses.iter().map(|a| a.to_string()).collect();
                async move { Ok(rows) }.boxed()
            });
            let fetch = fake(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(respond(json!([])))
            });
            let app = routes(Directory::new(
                fetch,
                lookup,
                Arc::new(crate::common::js::now_ms),
            ));
            assert_eq!(invoke(&app, "/stations", "GET").await.0, 503);
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn oversized_directory_bodies_are_refused() {
        let app = proxy(
            fake(|request| {
                if request.url.contains("/json/servers") {
                    return servers();
                }
                let declared = (constants::RADIO_RESPONSE_MAX_BYTES + 1).to_string();
                Ok(FetchResponse::new(
                    200,
                    &[("content-length", &declared)],
                    json!([station(json!({}))]).to_string(),
                ))
            }),
            Arc::new(crate::common::js::now_ms),
        );
        assert_eq!(invoke(&app, "/stations", "GET").await.0, 503);
    }

    use crate::providers::cctv::fetch::Redirect;
}
