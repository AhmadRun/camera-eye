//! Port of `src/data/localReceiversProxy.test.mjs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::{Path, State};
use axum::routing::get;

use super::*;

const FIXTURE: &str =
    include_str!("../../../../src/data/fixtures/adsb-austin-dump1090-aircraft.json");

fn fixture_now_ms() -> f64 {
    let json: Value = serde_json::from_str(FIXTURE).unwrap();
    json["now"].as_f64().unwrap() * 1000.0
}

#[derive(Clone, Default)]
struct Seen {
    hosts: Arc<Mutex<Vec<String>>>,
    fetches: Arc<AtomicUsize>,
}

async fn route(
    State(seen): State<Seen>,
    Path(kind): Path<String>,
    headers: http::HeaderMap,
) -> Response {
    seen.fetches.fetch_add(1, Ordering::SeqCst);
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    seen.hosts.lock().push(host);
    match kind.as_str() {
        "data" => reply::json_text(200, FIXTURE, None),
        "redirect" => reply::with_header(
            reply::raw(302, "text/plain", "", None),
            "location",
            "http://169.254.169.254/",
        ),
        "big" => reply::json_text(
            200,
            format!("{{\"aircraft\":[{}]}}", " ".repeat(5000)),
            None,
        ),
        "error" => reply::raw(500, "text/plain", "upstream internal detail", None),
        "garbage" => reply::raw(200, "text/html", "<html>", None),
        "slow" => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            reply::json_text(200, FIXTURE, None)
        }
        status => reply::raw(status.parse().unwrap_or(500), "application/json", "", None),
    }
}

/// A loopback feed server: `/data`, `/redirect`, `/big`, `/error`,
/// `/garbage`, `/slow` and `/<status>` before `/aircraft.json`.
async fn feed_server() -> (u16, Seen) {
    let seen = Seen::default();
    let app = Router::new()
        .route("/{kind}/aircraft.json", get(route))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (port, seen)
}

fn lookup_from(
    answers: HashMap<&'static str, Vec<&'static str>>,
    log: Arc<Mutex<Vec<String>>>,
) -> Lookup {
    let answers = Arc::new(answers);
    Arc::new(move |hostname: String| {
        let answers = Arc::clone(&answers);
        let log = Arc::clone(&log);
        Box::pin(async move {
            log.lock().push(hostname.clone());
            match answers.get(hostname.as_str()) {
                Some(list) => Ok(list.iter().map(|a| a.parse().unwrap()).collect()),
                None => Err(()),
            }
        })
    })
}

fn localhost_lookup() -> Lookup {
    lookup_from(
        HashMap::from([("localhost", vec!["127.0.0.1"])]),
        Arc::default(),
    )
}

fn fixed_clock(at: f64) -> Clock {
    Arc::new(move || at)
}

fn shared_clock(clock: &Arc<Mutex<f64>>) -> Clock {
    let clock = Arc::clone(clock);
    Arc::new(move || *clock.lock())
}

fn statuses(payload: &Value) -> Vec<String> {
    payload["feeds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            format!(
                "{} {}",
                f["band"].as_str().unwrap_or("null"),
                f["status"].as_str().unwrap()
            )
        })
        .collect()
}

#[test]
fn feeds_accept_local_http_aircraft_json_per_band() {
    let feed_1090 = "http://localhost:8080/data/aircraft.json";
    let (configured, feeds) = parse_local_receiver_feeds(Some(&format!(
        " 1090={feed_1090} , 978=http://skyaware.local/skyaware978/data/aircraft.json,1090=https://192.168.1.20:8443/tar1090/data/aircraft.json"
    )));
    assert!(configured);
    let summary: Vec<(Option<&str>, &str, Option<&str>)> = feeds
        .iter()
        .map(|f| (f.band.as_deref(), f.label.as_str(), f.url.as_deref()))
        .collect();
    assert_eq!(
        summary,
        vec![
            (Some("1090"), "1090 MHz #1", Some(feed_1090)),
            (
                Some("978"),
                "978 MHz UAT",
                Some("http://skyaware.local:80/skyaware978/data/aircraft.json")
            ),
            (
                Some("1090"),
                "1090 MHz #2",
                Some("https://192.168.1.20:8443/tar1090/data/aircraft.json")
            ),
        ]
    );
    assert_eq!(parse_local_receiver_feeds(None), (false, Vec::new()));
    assert!(!parse_local_receiver_feeds(Some(" , ")).0);
}

#[test]
fn feeds_reject_public_link_local_and_named_hosts_bad_paths_and_bands() {
    let cases = [
        ("1090=http://8.8.8.8/data/aircraft.json", "host must be"),
        (
            "1090=http://169.254.169.254/latest/aircraft.json",
            "host must be",
        ),
        ("1090=http://example.com/data/aircraft.json", "host must be"),
        ("1090=http://[::1]:8080/data/aircraft.json", "host must be"),
        (
            "1090=http://localhost:8080/data/receiver.json",
            "path must end",
        ),
        (
            "1090=http://localhost:8080/data/aircraft.json.bak",
            "path must end",
        ),
        ("1090=http://localhost:8080/data/aircraft.json?x=1", "query"),
        (
            "1090=http://user:pw@localhost:8080/data/aircraft.json",
            "credentials",
        ),
        ("1090=ftp://localhost/data/aircraft.json", "scheme"),
        ("1090=not a url", "not a valid URL"),
        (
            "868=http://localhost:8080/data/aircraft.json",
            "band must be",
        ),
        (
            "http://localhost:8080/data/aircraft.json",
            "expected band=url",
        ),
    ];
    for (entry, reason) in cases {
        let (_, feeds) = parse_local_receiver_feeds(Some(entry));
        assert_eq!(feeds[0].url, None, "{entry} must not be fetchable");
        assert!(
            feeds[0].reason.as_deref().unwrap().contains(reason),
            "{entry}: {:?}",
            feeds[0].reason
        );
    }
    let (_, feeds) = parse_local_receiver_feeds(Some("868=http://localhost/aircraft.json"));
    assert_eq!(feeds[0].band, None);
    assert_eq!(feeds[0].label, "entry 1");
    let many = ["1090=http://localhost/aircraft.json"; 9].join(",");
    let (_, feeds) = parse_local_receiver_feeds(Some(&many));
    assert_eq!(feeds[8].reason.as_deref(), Some("at most 8 feeds are read"));
    assert_eq!(feeds[0].label, "1090 MHz #1");
}

#[test]
fn receiver_addresses_must_be_local() {
    for ok in [
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "::1",
        "::ffff:127.0.0.1",
    ] {
        assert!(is_local_receiver_address(ok.parse().unwrap()), "{ok}");
    }
    for bad in [
        "8.8.8.8",
        "169.254.169.254",
        "172.32.0.1",
        "::ffff:8.8.8.8",
        "fe80::1",
    ] {
        assert!(!is_local_receiver_address(bad.parse().unwrap()), "{bad}");
    }
    assert!(!is_local_ipv4("999.1.1.1"));
    assert!(!is_local_ipv4("127.0.0"));
    assert_eq!(
        parse_tap_address("Rayhunter.local:8080"),
        Some(("rayhunter.local".into(), 8080))
    );
    assert_eq!(parse_tap_address("-bad.local:80"), None);
    assert_eq!(parse_tap_address("localhost:0"), None);
}

#[tokio::test]
async fn an_unconfigured_route_answers_without_fetching() {
    let receivers = LocalReceivers::new(Options::default());
    let response = receivers.handle(&Method::GET).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        receivers.payload().await,
        json!({ "configured": false, "feeds": [], "records": [] })
    );
}

#[tokio::test]
async fn live_feeds_are_read_and_labelled_with_their_band() {
    let (port, seen) = feed_server().await;
    let now = fixture_now_ms();
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(format!(
            "1090=http://localhost:{port}/data/aircraft.json,978=http://127.0.0.1:{port}/data/aircraft.json"
        )),
        lookup: localhost_lookup(),
        now: fixed_clock(now + 2000.0),
        ..Options::default()
    });
    let payload = receivers.payload().await;
    assert_eq!(seen.fetches.load(Ordering::SeqCst), 2);
    assert_eq!(payload["configured"], true);
    assert_eq!(
        payload["feeds"],
        json!([
            { "band": "1090", "label": "1090 MHz", "status": "live", "aircraft": 4, "ageMs": 2000 },
            { "band": "978", "label": "978 MHz UAT", "status": "live", "aircraft": 4, "ageMs": 2000 },
        ])
    );
    let records = payload["records"].as_array().unwrap();
    assert_eq!(records.len(), 8);
    let shinr: Vec<&Value> = records.iter().filter(|r| r["icao"] == "ae5d8a").collect();
    assert_eq!(shinr[0]["band"], "1090");
    assert_eq!(shinr[1]["band"], "978");
    assert!(shinr.iter().all(|r| r["source"] == "feed"));
    // Ages are anchored at the feed's own `now`.
    assert_eq!(shinr[0]["lastPositionAt"], num(now - 34_600.0));
    assert_eq!(shinr[0]["lat"], json!(30.269662));
}

#[tokio::test]
async fn stale_feeds_and_failures_are_reported_without_detail() {
    let (port, _) = feed_server().await;
    // A closed port: connection refused.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let feeds = [
        format!("1090=http://localhost:{port}/data/aircraft.json"),
        format!("978=http://127.0.0.1:{closed}/data/aircraft.json"),
        format!("1090=http://127.0.0.1:{port}/redirect/aircraft.json"),
        format!("1090=http://127.0.0.1:{port}/big/aircraft.json"),
        format!("1090=http://127.0.0.1:{port}/error/aircraft.json"),
        format!("978=http://127.0.0.1:{port}/garbage/aircraft.json"),
        "1090=http://8.8.8.8/aircraft.json".to_string(),
    ];
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(feeds.join(",")),
        lookup: localhost_lookup(),
        now: fixed_clock(fixture_now_ms() + 10_001.0),
        max_bytes: 4096,
        ..Options::default()
    });
    let payload = receivers.payload().await;
    assert_eq!(
        statuses(&payload),
        vec![
            "1090 stale",
            "978 unreachable",
            "1090 unreachable",
            "1090 unreachable",
            "1090 unreachable",
            "978 unreachable",
            "1090 invalid",
        ]
    );
    assert_eq!(payload["feeds"][0]["ageMs"], json!(10_001));
    assert_eq!(
        payload["records"].as_array().unwrap().len(),
        4,
        "stale feed records still flow"
    );
    let body = payload.to_string();
    for secret in [
        "refused",
        "internal detail",
        "169.254",
        "127.0.0.1",
        "8.8.8.8",
    ] {
        assert!(!body.contains(secret), "{secret} leaked");
    }

    let client = feed_client_builder().build().unwrap();
    let lookup = localhost_lookup();
    for (path, code) in [
        ("redirect", "REDIRECT_REFUSED"),
        ("big", "RESPONSE_TOO_LARGE"),
        ("error", "HTTP_STATUS"),
        ("garbage", "BAD_JSON"),
    ] {
        let url = format!("http://127.0.0.1:{port}/{path}/aircraft.json");
        let result =
            fetch_local_receiver_document(&url, &client, &lookup, Duration::from_secs(2), 4096)
                .await;
        assert_eq!(result, Err(code), "{path}");
    }
}

#[tokio::test]
async fn a_feed_that_exceeds_the_timeout_is_unreachable() {
    let (port, _) = feed_server().await;
    let url = format!("http://127.0.0.1:{port}/slow/aircraft.json");
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(format!("1090={url}")),
        timeout: Duration::from_millis(50),
        ..Options::default()
    });
    let payload = receivers.payload().await;
    assert_eq!(payload["feeds"][0]["status"], "unreachable");
    let client = feed_client_builder().build().unwrap();
    let result = fetch_local_receiver_document(
        &url,
        &client,
        &localhost_lookup(),
        Duration::from_millis(50),
        1024,
    )
    .await;
    assert_eq!(result, Err("TIMEOUT"));
}

#[tokio::test]
async fn requests_within_a_second_share_one_read() {
    let (port, seen) = feed_server().await;
    let clock = Arc::new(Mutex::new(fixture_now_ms()));
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(format!("1090=http://127.0.0.1:{port}/data/aircraft.json")),
        now: shared_clock(&clock),
        ..Options::default()
    });
    futures_util::future::join_all((0..3).map(|_| receivers.payload())).await;
    assert_eq!(seen.fetches.load(Ordering::SeqCst), 1, "single-flight");
    *clock.lock() += 500.0;
    receivers.payload().await;
    assert_eq!(seen.fetches.load(Ordering::SeqCst), 1, "cached for ~1 s");
    *clock.lock() += 1000.0;
    receivers.payload().await;
    assert_eq!(seen.fetches.load(Ordering::SeqCst), 2);
    let response = receivers.handle(&Method::POST).await;
    assert_eq!(response.status(), 405);
    assert_eq!(response.headers()["cache-control"], "no-store");
}

#[tokio::test]
async fn a_feed_name_is_resolved_checked_and_pinned() {
    let (port, seen) = feed_server().await;
    let lookups = Arc::new(Mutex::new(Vec::new()));
    // `good.local` has no real DNS entry: only the pinned address reaches
    // the loopback server.
    let lookup = lookup_from(
        HashMap::from([
            ("good.local", vec!["127.0.0.1"]),
            ("public.local", vec!["93.184.216.34"]),
            ("linklocal.local", vec!["169.254.169.254"]),
            ("mixed.local", vec!["192.168.1.4", "8.8.8.8"]),
        ]),
        Arc::clone(&lookups),
    );
    let feeds = [
        format!("1090=http://good.local:{port}/data/aircraft.json"),
        format!("1090=http://public.local:{port}/data/aircraft.json"),
        format!("978=http://linklocal.local:{port}/data/aircraft.json"),
        format!("978=http://mixed.local:{port}/data/aircraft.json"),
        format!("1090=http://127.0.0.1:{port}/data/aircraft.json"),
    ];
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(feeds.join(",")),
        lookup,
        now: fixed_clock(fixture_now_ms()),
        ..Options::default()
    });
    let payload = receivers.payload().await;
    let states: Vec<&str> = payload["feeds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["status"].as_str().unwrap())
        .collect();
    assert_eq!(
        states,
        vec!["live", "unreachable", "unreachable", "unreachable", "live"]
    );
    let mut resolved = lookups.lock().clone();
    resolved.sort();
    assert_eq!(
        resolved,
        vec![
            "good.local",
            "linklocal.local",
            "mixed.local",
            "public.local"
        ],
        "IP literals are not resolved"
    );
    let mut hosts = seen.hosts.lock().clone();
    hosts.sort();
    assert_eq!(
        hosts,
        vec![format!("127.0.0.1:{port}"), format!("good.local:{port}")],
        "a name resolving outside loopback/RFC1918 is never contacted"
    );
}

#[tokio::test]
async fn null_body_statuses_are_a_clean_feed_error() {
    let (port, _) = feed_server().await;
    let feeds: Vec<String> = [204, 205, 304]
        .iter()
        .map(|s| format!("1090=http://receiver-null-body.local:{port}/{s}/aircraft.json"))
        .collect();
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(feeds.join(",")),
        lookup: lookup_from(
            HashMap::from([("receiver-null-body.local", vec!["127.0.0.1"])]),
            Arc::default(),
        ),
        now: fixed_clock(fixture_now_ms()),
        timeout: Duration::from_millis(300),
        ..Options::default()
    });
    let payload = receivers.payload().await;
    assert_eq!(
        statuses(&payload),
        vec!["1090 unreachable", "1090 unreachable", "1090 unreachable"]
    );
}

#[tokio::test]
async fn a_stalled_dns_lookup_fails_its_feed_at_the_deadline() {
    let (port, seen) = feed_server().await;
    let stalled: Lookup =
        Arc::new(|_hostname: String| Box::pin(std::future::pending::<Result<Vec<IpAddr>, ()>>()));
    let clock = Arc::new(Mutex::new(fixture_now_ms()));
    let receivers = LocalReceivers::new(Options {
        feeds_value: Some(format!(
            "1090=http://stalled.local/data/aircraft.json,978=http://127.0.0.1:{port}/data/aircraft.json"
        )),
        lookup: stalled,
        now: shared_clock(&clock),
        timeout: Duration::from_millis(20),
        ..Options::default()
    });
    for _ in 0..2 {
        let payload = tokio::time::timeout(Duration::from_millis(500), receivers.payload())
            .await
            .expect("the snapshot is not held by DNS");
        assert_eq!(statuses(&payload), vec!["1090 unreachable", "978 live"]);
        *clock.lock() += 2000.0;
    }
    assert!(
        seen.hosts.lock().iter().all(|h| !h.contains("stalled")),
        "a stalled name is never fetched"
    );
}
