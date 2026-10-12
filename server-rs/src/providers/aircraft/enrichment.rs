//! adsbdb.com enrichment proxy (`server/providers/aircraft/enrichment.js`):
//! `/api/adsbdb/route/<callsign>` and `/api/adsbdb/type/<hex>`, one upstream
//! request per key per 24 h (404s negative-cached), persisted to
//! `.ce-cache/adsbdb.json`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::Uri;
use axum::response::Response;
use axum::routing::any;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use tokio::sync::OnceCell;

use super::jsval::{self, encode_uri_component, truthy};
use crate::common::coalesce::Coalescer;
use crate::common::disk;
use crate::common::js::now_ms;
use crate::common::reply;
use crate::state::AppState;

const TTL_MS: f64 = 24.0 * 3_600_000.0;
const FETCH_TIMEOUT: Duration = Duration::from_millis(8000);
const FLUSH_EVERY: Duration = Duration::from_millis(15_000);
pub const ADSBDB_BASE: &str = "https://api.adsbdb.com";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Route,
    Aircraft,
}

impl Kind {
    fn store(self) -> &'static str {
        match self {
            Kind::Route => "routes",
            Kind::Aircraft => "aircraft",
        }
    }
}

struct Store {
    routes: Map<String, Value>,
    aircraft: Map<String, Value>,
    dirty: bool,
}

impl Store {
    fn map(&mut self, kind: Kind) -> &mut Map<String, Value> {
        match kind {
            Kind::Route => &mut self.routes,
            Kind::Aircraft => &mut self.aircraft,
        }
    }

    /// `fresh(store[key]) ? store[key].data : None`.
    fn fresh_data(&mut self, kind: Kind, key: &str) -> Option<Value> {
        let entry = self.map(kind).get(key)?;
        let at = jsval::to_number(entry.get("at"));
        (now_ms() as f64 - at < TTL_MS).then(|| entry.get("data").cloned().unwrap_or(Value::Null))
    }

    fn put(&mut self, kind: Kind, key: &str, data: Value) {
        self.map(kind)
            .insert(key.to_string(), json!({ "at": now_ms(), "data": data }));
        self.dirty = true;
    }

    fn snapshot(&self) -> Value {
        json!({ "routes": self.routes, "aircraft": self.aircraft })
    }
}

pub struct Adsbdb {
    base: String,
    path: PathBuf,
    store: Arc<Mutex<Store>>,
    loaded: OnceCell<()>,
    in_flight: Coalescer<String, Value>,
}

/// `parseRoute`.
pub fn parse_route(json: &Value) -> Value {
    let Some(fr) = json.get("response").and_then(|r| r.get("flightroute")) else {
        return Value::Null;
    };
    if !truthy(fr.get("origin")) || !truthy(fr.get("destination")) {
        return Value::Null;
    }
    let first_truthy = |a: &Value, names: &[&str]| -> Value {
        names
            .iter()
            .find_map(|n| a.get(*n).filter(|v| truthy(Some(v))).cloned())
            .unwrap_or_else(|| Value::from(""))
    };
    let finite = |v: Option<&Value>| match v {
        Some(Value::Number(n)) if n.as_f64().is_some_and(f64::is_finite) => {
            Value::Number(n.clone())
        }
        _ => Value::Null,
    };
    let airport = |a: &Value| {
        json!({
            "code": first_truthy(a, &["iata_code", "icao_code"]),
            "name": first_truthy(a, &["municipality", "name"]),
            "lat": finite(a.get("latitude")),
            "lon": finite(a.get("longitude")),
        })
    };
    let airline = fr
        .get("airline")
        .and_then(|a| a.get("name"))
        .filter(|v| truthy(Some(v)))
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "airline": airline,
        "origin": airport(&fr["origin"]),
        "destination": airport(&fr["destination"]),
    })
}

/// `parseAircraft`.
pub fn parse_aircraft(json: &Value) -> Value {
    let Some(a) = json
        .get("response")
        .and_then(|r| r.get("aircraft"))
        .filter(|a| truthy(Some(a)))
    else {
        return Value::Null;
    };
    let or_null = |name: &str| {
        a.get(name)
            .filter(|v| truthy(Some(v)))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let type_name = if truthy(a.get("manufacturer")) && truthy(a.get("type")) {
        Value::from(format!(
            "{} {}",
            jsval::to_js_string(&a["manufacturer"]),
            jsval::to_js_string(&a["type"])
        ))
    } else {
        or_null("type")
    };
    json!({
        "typeCode": or_null("icao_type"),
        "typeName": type_name,
        "registration": or_null("registration"),
    })
}

impl Adsbdb {
    pub fn new(base: impl Into<String>, path: PathBuf) -> Self {
        Self {
            base: base.into(),
            path,
            store: Arc::new(Mutex::new(Store {
                routes: Map::new(),
                aircraft: Map::new(),
                dirty: false,
            })),
            loaded: OnceCell::new(),
            in_flight: Coalescer::new(),
        }
    }

    /// `loadOnce`: read the disk cache, then flush it every 15 s when dirty.
    async fn load_once(&self) {
        self.loaded
            .get_or_init(|| async {
                if let Some(parsed) = disk::read_json::<Value>(&self.path).await {
                    let mut store = self.store.lock();
                    let pick = |name: &str| match parsed.get(name) {
                        Some(Value::Object(map)) => map.clone(),
                        _ => Map::new(),
                    };
                    store.routes = pick("routes");
                    store.aircraft = pick("aircraft");
                }
                let store = Arc::clone(&self.store);
                let path = self.path.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(FLUSH_EVERY);
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        let snapshot = {
                            let mut store = store.lock();
                            if !store.dirty {
                                continue;
                            }
                            store.dirty = false;
                            store.snapshot()
                        };
                        if disk::write_json(&path, &snapshot).await.is_err() {
                            // Retry next tick.
                            store.lock().dirty = true;
                        }
                    }
                });
            })
            .await;
    }

    async fn lookup(&self, http: &reqwest::Client, kind: Kind, key: &str) -> Value {
        if let Some(data) = self.store.lock().fresh_data(kind, key) {
            return data;
        }
        let url = format!(
            "{}/v0/{}/{}",
            self.base,
            if kind == Kind::Route {
                "callsign"
            } else {
                "aircraft"
            },
            encode_uri_component(key)
        );
        let http = http.clone();
        let store = Arc::clone(&self.store);
        let key = key.to_string();
        let in_flight_key = format!("{}:{key}", kind.store());
        let (value, _) = self
            .in_flight
            .run(in_flight_key, move || async move {
                let result = async {
                    let response = http.get(&url).timeout(FETCH_TIMEOUT).send().await?;
                    let status = response.status();
                    let json = if status.is_success() {
                        Some(response.json::<Value>().await?)
                    } else {
                        None
                    };
                    Ok::<_, reqwest::Error>((status.as_u16(), json))
                }
                .await;
                let mut store = store.lock();
                match result {
                    Ok((_, Some(json))) => {
                        let data = match kind {
                            Kind::Route => parse_route(&json),
                            Kind::Aircraft => parse_aircraft(&json),
                        };
                        // `data` may be null: a negative cache entry.
                        store.put(kind, &key, data.clone());
                        data
                    }
                    Ok((404, None)) => {
                        store.put(kind, &key, Value::Null);
                        store.fresh_data(kind, &key).unwrap_or(Value::Null)
                    }
                    // Other statuses stay uncached; network errors serve stale.
                    _ => store.fresh_data(kind, &key).unwrap_or(Value::Null),
                }
            })
            .await;
        value
    }

    /// The `/api/adsbdb` handler; `rest` is the path after the mount.
    async fn handle(&self, http: &reqwest::Client, rest: &str) -> Response {
        self.load_once().await;
        let send = |status: u16, value: Value| reply::json(status, &value, None);
        let mut parts = rest.split('/');
        let _ = parts.next();
        let kind = parts.next().unwrap_or("");
        let raw_key = parts.next().unwrap_or("");
        let found = |data: Value| match data {
            Value::Null => json!({ "found": false }),
            Value::Object(map) => {
                let mut out = Map::new();
                out.insert("found".into(), Value::Bool(true));
                out.extend(map);
                Value::Object(out)
            }
            _ if !truthy(Some(&data)) => json!({ "found": false }),
            _ => json!({ "found": true }),
        };
        match kind {
            "route" => {
                let callsign = raw_key.to_uppercase();
                if !(2..=8).contains(&callsign.len())
                    || !callsign
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                {
                    return send(400, json!({ "error": "invalid callsign" }));
                }
                let data = self.lookup(http, Kind::Route, &callsign).await;
                send(200, found(data))
            }
            "type" => {
                let hex = raw_key.to_lowercase();
                if hex.len() != 6
                    || !hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return send(400, json!({ "error": "invalid hex" }));
                }
                let data = self.lookup(http, Kind::Aircraft, &hex).await;
                send(200, found(data))
            }
            _ => send(404, json!({ "error": "unknown endpoint" })),
        }
    }
}

pub fn router(adsbdb: Arc<Adsbdb>) -> Router<AppState> {
    let handler = move |State(app): State<AppState>, uri: Uri| {
        let adsbdb = Arc::clone(&adsbdb);
        async move {
            // Connect handed the middleware the URL below its mount.
            let rest = uri.path().strip_prefix("/api/adsbdb").unwrap_or("");
            let rest = if rest.is_empty() { "/" } else { rest };
            adsbdb.handle(&app.http, rest).await
        }
    };
    Router::new()
        .route("/api/adsbdb", any(handler.clone()))
        .route("/api/adsbdb/", any(handler.clone()))
        .route("/api/adsbdb/{*rest}", any(handler))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_routes_and_aircraft() {
        let route = parse_route(&json!({
            "response": { "flightroute": {
                "airline": { "name": "United" },
                "origin": { "iata_code": "AUS", "icao_code": "KAUS", "municipality": "Austin", "latitude": 30.19, "longitude": -97.67 },
                "destination": { "iata_code": "", "icao_code": "KDEN", "name": "Denver Intl", "latitude": "x" },
            }}
        }));
        assert_eq!(
            route,
            json!({
                "airline": "United",
                "origin": { "code": "AUS", "name": "Austin", "lat": 30.19, "lon": -97.67 },
                "destination": { "code": "KDEN", "name": "Denver Intl", "lat": null, "lon": null },
            })
        );
        assert_eq!(
            parse_route(&json!({ "response": "unknown callsign" })),
            Value::Null
        );
        assert_eq!(
            parse_aircraft(&json!({ "response": { "aircraft": {
                "icao_type": "B738", "manufacturer": "Boeing", "type": "737-800", "registration": "N123"
            }}})),
            json!({ "typeCode": "B738", "typeName": "Boeing 737-800", "registration": "N123" })
        );
        assert_eq!(
            parse_aircraft(&json!({ "response": { "aircraft": { "type": "A320" }}})),
            json!({ "typeCode": null, "typeName": "A320", "registration": null })
        );
        assert_eq!(parse_aircraft(&json!({})), Value::Null);
    }
}
