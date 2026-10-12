//! In-app key setup, the "POWER UP" panel (port of
//! `server/standalone/key-setup.js`, `server/standalone/key-setup-hardening.mjs`
//! and `src/keySetupCore.mjs`):
//!
//! - `GET /api/setup/status`: which keys are configured, as presence plus a
//!   source classification. Never a value or suffix.
//! - `POST /api/setup/keys`: validate `{ENV_VAR: value|null}` pairs, upsert
//!   them into the store this launch owns (`<root>/.env`, or
//!   `<root>/pinokio/ENVIRONMENT` under the Pinokio launcher), and make them
//!   live at once through `Env::set`.
//!
//! Both answer only this machine: no proxy headers, no sharing signal, a
//! loopback socket, a local Host, and on POST an exact local Origin and a JSON
//! content type.
//!
//! "Boot" provenance (which keys came from the environment that launched the
//! server rather than from the store) is read from `/proc/self/environ`, the
//! environment as it was before `main` loaded `.env`. Where that file is
//! unavailable, a key whose live value differs from (or is absent in) `.env`
//! at startup counts as external.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::response::Response;
use axum::routing::{MethodRouter, any};
use bytes::BytesMut;
use futures_util::StreamExt;
use http::{HeaderMap, Method};
use serde_json::{Map, Value, json};

use crate::common::reply;
use crate::common::same_site::has_proxy_signals;
use crate::env::Env;
use crate::state::AppState;

/// Longest accepted key/token value.
pub const KEY_SETUP_VALUE_LIMIT: usize = 512;
/// Most env vars accepted in one save.
pub const KEY_SETUP_UPDATE_LIMIT: usize = 16;
/// Header line written above keys the panel appends to a .env file.
pub const KEY_SETUP_APPEND_HEADER: &str = "# Keys added by the in-app POWER UP panel";
/// Longest accepted request body, in UTF-16 code units like the JS string.
const KEY_SETUP_MAX_BODY: usize = 8192;

/// One provider credential in the registry.
pub struct KeyEntry {
    pub id: &'static str,
    pub title: &'static str,
    pub unlocks: &'static str,
    pub get_url: &'static str,
    pub env_vars: &'static [&'static str],
    pub tier: &'static str,
    pub client_exposed: bool,
    pub hidden: bool,
}

/// `KEY_SETUP_KEYS`, in display order.
pub const KEY_SETUP_KEYS: &[KeyEntry] = &[
    KeyEntry {
        id: "google-maps",
        title: "GOOGLE MAPS",
        unlocks: "The photorealistic 3D planet + place search",
        get_url: "https://developers.google.com/maps/documentation/tile/get-api-key",
        env_vars: &["GOOGLE_MAPS_API_KEY"],
        tier: "metered",
        client_exposed: true,
        hidden: false,
    },
    KeyEntry {
        id: "google-maps-server",
        title: "GOOGLE MAPS — SERVER",
        unlocks: "Places context + Street View fallback; optional separate key",
        get_url: "https://developers.google.com/maps/documentation/places/web-service/get-api-key",
        env_vars: &["GOOGLE_MAPS_SERVER_API_KEY"],
        tier: "metered",
        client_exposed: false,
        hidden: true,
    },
    KeyEntry {
        id: "openai",
        title: "OPENAI",
        unlocks: "Voice control — talk to the planet",
        get_url: "https://platform.openai.com/api-keys",
        env_vars: &["OPENAI_API_KEY"],
        tier: "metered",
        client_exposed: false,
        hidden: false,
    },
    KeyEntry {
        id: "aisstream",
        title: "AISSTREAM",
        unlocks: "Live ships, worldwide",
        get_url: "https://aisstream.io",
        env_vars: &["AISSTREAM_API_KEY"],
        tier: "free",
        client_exposed: false,
        hidden: false,
    },
    KeyEntry {
        id: "firms",
        title: "NASA FIRMS",
        unlocks: "Live active-fire detections",
        get_url: "https://firms.modaps.eosdis.nasa.gov/api/map_key/",
        env_vars: &["FIRMS_MAP_KEY"],
        tier: "free",
        client_exposed: false,
        hidden: false,
    },
    KeyEntry {
        id: "tomtom",
        title: "TOMTOM",
        unlocks: "Real live traffic (keyless runs a simulation)",
        get_url: "https://developer.tomtom.com",
        env_vars: &["TOMTOM_API_KEY"],
        tier: "free",
        client_exposed: false,
        hidden: false,
    },
    KeyEntry {
        id: "cesium-ion",
        title: "CESIUM ION",
        unlocks: "Bing imagery map stacks + world terrain",
        get_url: "https://ion.cesium.com/tokens",
        env_vars: &["CESIUM_ION_TOKEN"],
        tier: "free",
        client_exposed: true,
        hidden: false,
    },
    KeyEntry {
        id: "opensky",
        title: "OPENSKY",
        unlocks: "More flight-polling credits (anonymous works without)",
        get_url: "https://opensky-network.org",
        env_vars: &["OPENSKY_CLIENT_ID", "OPENSKY_CLIENT_SECRET"],
        tier: "free",
        client_exposed: false,
        hidden: false,
    },
    KeyEntry {
        id: "launch-library",
        title: "LAUNCH LIBRARY",
        unlocks: "Higher space-missions request allowance",
        get_url: "https://thespacedevs.com",
        env_vars: &["LL2_API_TOKEN"],
        tier: "free",
        client_exposed: false,
        hidden: false,
    },
];

const LOCAL_HOSTNAMES: [&str; 4] = ["localhost", "127.0.0.1", "::1", "[::1]"];
const LOOPBACK_ADDRESSES: [&str; 3] = ["127.0.0.1", "::1", "::ffff:127.0.0.1"];

/// `knownKeySetupEnvVars`.
pub fn known_key_setup_env_vars() -> Vec<&'static str> {
    let mut names = Vec::new();
    for entry in KEY_SETUP_KEYS {
        for name in entry.env_vars {
            if !names.contains(name) {
                names.push(*name);
            }
        }
    }
    names
}

/// JS `\s`.
fn is_js_space(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
}

/// `localAuthority`: the exact local origin a Host header names, or `None`.
fn local_authority(host_header: Option<&str>, protocol: &str) -> Option<String> {
    let raw = host_header.unwrap_or("").trim().to_lowercase();
    let scheme = protocol.to_lowercase();
    if raw.is_empty()
        || !(scheme == "http:" || scheme == "https:")
        || raw.chars().any(|c| is_js_space(c) || "/?#@".contains(c))
    {
        return None;
    }
    let parsed = url::Url::parse(&format!("{scheme}//{raw}")).ok()?;
    let hostname = parsed.host_str()?.to_lowercase();
    LOCAL_HOSTNAMES
        .contains(&hostname.as_str())
        .then(|| parsed.origin().ascii_serialization())
}

/// `isSharingEnabled`: any launcher sharing signal disables the surface.
pub fn is_sharing_enabled(env: &dyn Fn(&str) -> Option<String>) -> bool {
    let truthy = |name: &str| {
        let value = env(name).unwrap_or_default();
        let value = value.trim().to_lowercase();
        value == "1" || value == "true"
    };
    let share_var = env("PINOKIO_SHARE_VAR").unwrap_or_default();
    let share_var = share_var.trim();
    truthy("PINOKIO_SHARE_CLOUDFLARE")
        || truthy("PINOKIO_SHARE_LOCAL")
        || (!share_var.is_empty() && share_var != "__ce_sharing_disabled__")
}

/// The inputs of `admitKeySetupRequest`.
pub struct Admission<'a> {
    pub method: &'a str,
    pub remote_address: Option<&'a str>,
    pub host_header: Option<&'a str>,
    pub protocol: &'a str,
    pub origin: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub proxy_headers: &'a HeaderMap,
    pub env: &'a dyn Fn(&str) -> Option<String>,
}

/// `admitKeySetupRequest`: `Ok(())` or `Err((status, error))`.
pub fn admit_key_setup_request(input: &Admission) -> Result<(), (u16, &'static str)> {
    if has_proxy_signals(input.proxy_headers) {
        return Err((403, "Provider Settings does not answer proxied requests"));
    }
    if is_sharing_enabled(input.env) {
        return Err((
            403,
            "Provider Settings is disabled while sharing is enabled",
        ));
    }
    if !LOOPBACK_ADDRESSES.contains(&input.remote_address.unwrap_or("")) {
        return Err((
            403,
            "Provider Settings answers only the machine running the server",
        ));
    }
    let Some(authority) = local_authority(input.host_header, input.protocol) else {
        return Err((403, "Provider Settings answers only local hostnames"));
    };
    let origin = input.origin.filter(|o| !o.is_empty());
    if input.method == "POST" && origin.is_none() {
        return Err((403, "Provider Settings requires an exact local Origin"));
    }
    if let Some(origin) = origin {
        let Ok(parsed) = url::Url::parse(origin) else {
            return Err((403, "Unrecognized Origin refused"));
        };
        let exact = parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.path() == "/"
            && parsed.query().is_none_or(str::is_empty)
            && parsed.fragment().is_none_or(str::is_empty)
            && parsed.origin().ascii_serialization() == authority;
        if !exact {
            return Err((403, "Cross-origin requests are refused"));
        }
    }
    if input.method == "POST"
        && !input
            .content_type
            .unwrap_or("")
            .to_lowercase()
            .starts_with("application/json")
    {
        return Err((415, "Content-Type must be application/json"));
    }
    Ok(())
}

/// `isKeySetupExternallyManaged`.
pub fn is_key_setup_externally_managed(
    effective_value: Option<&str>,
    stored_value: Option<&str>,
    was_external_at_boot: bool,
) -> bool {
    let effective = effective_value.unwrap_or("").trim();
    let stored = stored_value.unwrap_or("").trim();
    !effective.is_empty() && (was_external_at_boot || effective != stored)
}

/// `keySetupStatus`: presence per visible entry, never a value.
pub fn key_setup_status(env: &dyn Fn(&str) -> Option<String>) -> (Vec<Map<String, Value>>, usize) {
    let mut keys = Vec::new();
    let mut set_count = 0;
    for entry in KEY_SETUP_KEYS.iter().filter(|e| !e.hidden) {
        let set = entry
            .env_vars
            .iter()
            .all(|name| !env(name).unwrap_or_default().trim().is_empty());
        if set {
            set_count += 1;
        }
        let mut key = Map::new();
        key.insert("id".into(), json!(entry.id));
        key.insert("title".into(), json!(entry.title));
        key.insert("unlocks".into(), json!(entry.unlocks));
        key.insert("getUrl".into(), json!(entry.get_url));
        key.insert("envVars".into(), json!(entry.env_vars));
        key.insert("tier".into(), json!(entry.tier));
        key.insert("clientExposed".into(), json!(entry.client_exposed));
        key.insert("set".into(), json!(set));
        keys.push(key);
    }
    (keys, set_count)
}

/// Validated updates, in request order; `None` means remove.
pub type Updates = Vec<(String, Option<String>)>;

/// `Object.entries` order: integer-like keys first, ascending.
fn js_entries(map: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let is_index = |k: &str| {
        !k.is_empty()
            && k.len() <= 10
            && k.bytes().all(|b| b.is_ascii_digit())
            && (k == "0" || !k.starts_with('0'))
            && k.parse::<u64>().is_ok_and(|n| n < u64::from(u32::MAX))
    };
    let mut indexes: Vec<(&String, &Value)> = map.iter().filter(|(k, _)| is_index(k)).collect();
    indexes.sort_by_key(|(k, _)| k.parse::<u64>().unwrap_or(0));
    indexes.extend(map.iter().filter(|(k, _)| !is_index(k)));
    indexes
}

/// `validateKeySetupUpdates`.
pub fn validate_key_setup_updates(body: &Value) -> Result<Updates, String> {
    let Some(map) = body.as_object() else {
        return Err("Body must be a JSON object of {ENV_VAR: value}".into());
    };
    let entries = js_entries(map);
    if entries.is_empty() {
        return Err("No keys provided".into());
    }
    if entries.len() > KEY_SETUP_UPDATE_LIMIT {
        return Err(format!("At most {KEY_SETUP_UPDATE_LIMIT} keys per save"));
    }
    let known = known_key_setup_env_vars();
    let mut updates = Vec::new();
    for (name, raw) in entries {
        if !known.contains(&name.as_str()) {
            return Err(format!("Unknown key: {name}"));
        }
        let value = match raw {
            Value::Null => {
                updates.push((name.clone(), None));
                continue;
            }
            Value::String(text) => text.trim_matches(is_js_space),
            _ => return Err(format!("{name} must be a string")),
        };
        if value.is_empty() {
            return Err(format!("{name} is empty"));
        }
        if value.encode_utf16().count() > KEY_SETUP_VALUE_LIMIT {
            return Err(format!(
                "{name} is longer than any real key ({KEY_SETUP_VALUE_LIMIT} max)"
            ));
        }
        if !value.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return Err(format!(
                "{name} may only contain printable characters with no spaces"
            ));
        }
        if value.contains(['#', '"', '\'', '$', '\\', '`']) {
            return Err(format!(
                "{name} contains a character that is not valid in a key (#, quotes, $, \\, or backtick)"
            ));
        }
        updates.push((name.clone(), Some(value.to_string())));
    }
    Ok(updates)
}

/// `^\s*(?:export\s+)?NAME\s*=` after an optional `#\s*` when `commented`.
fn assigns(line: &str, name: &str, commented: bool) -> bool {
    let mut rest = line.trim_start_matches(is_js_space);
    if commented {
        let Some(after) = rest.strip_prefix('#') else {
            return false;
        };
        rest = after.trim_start_matches(is_js_space);
    }
    let matches_name = |text: &str| {
        text.strip_prefix(name)
            .is_some_and(|after| after.trim_start_matches(is_js_space).starts_with('='))
    };
    if matches_name(rest) {
        return true;
    }
    if let Some(after) = rest.strip_prefix("export") {
        let trimmed = after.trim_start_matches(is_js_space);
        if trimmed.len() < after.len() && matches_name(trimmed) {
            return true;
        }
    }
    false
}

/// `upsertDotenvValues`: upsert `KEY=value` lines, disturbing nothing else.
pub fn upsert_dotenv_values(text: &str, updates: &Updates) -> String {
    let mut lines: Vec<String> = if text.is_empty() {
        Vec::new()
    } else {
        text.split('\n')
            .map(|l| l.strip_suffix('\r').unwrap_or(l).to_string())
            .collect()
    };
    let mut additions = Vec::new();
    for (name, value) in updates {
        let Some(value) = value else {
            for line in lines.iter_mut() {
                if assigns(line, name, false) {
                    *line = format!("# {name}=");
                }
            }
            continue;
        };
        let assignment = format!("{name}={value}");
        let mut last_active = None;
        let mut last_commented = None;
        for (index, line) in lines.iter().enumerate() {
            if assigns(line, name, false) {
                last_active = Some(index);
            } else if assigns(line, name, true) {
                last_commented = Some(index);
            }
        }
        if let Some(index) = last_active.or(last_commented) {
            lines[index] = assignment;
        } else {
            additions.push(assignment);
        }
    }
    if !additions.is_empty() {
        while lines.last().is_some_and(|l| l.trim().is_empty()) {
            lines.pop();
        }
        if !lines.iter().any(|l| l.trim() == KEY_SETUP_APPEND_HEADER) {
            if !lines.is_empty() {
                lines.push(String::new());
            }
            lines.push(KEY_SETUP_APPEND_HEADER.to_string());
        }
        lines.extend(additions);
    }
    let joined = lines.join("\n");
    if joined.is_empty() {
        return String::new();
    }
    if joined.ends_with('\n') {
        joined
    } else {
        format!("{joined}\n")
    }
}

/// `util.parseEnv`: the dotenv assignments in `text`, last one winning.
pub fn parse_dotenv(text: &str) -> HashMap<String, String> {
    let content = text.replace("\r\n", "\n");
    let mut out = HashMap::new();
    let mut rest = content.as_str();
    while !rest.is_empty() {
        let (line, tail) = rest.split_once('\n').unwrap_or((rest, ""));
        let trimmed = line.trim();
        let Some(eq) = line.find('=').filter(|_| !trimmed.starts_with('#')) else {
            rest = tail;
            continue;
        };
        let mut key = line[..eq].trim();
        if let Some(stripped) = key.strip_prefix("export ") {
            key = stripped.trim();
        }
        if key.is_empty() {
            rest = tail;
            continue;
        }
        // A quoted value may span lines up to its closing quote.
        let after = rest[eq + 1..].trim_start_matches([' ', '\t']);
        let quote = after
            .chars()
            .next()
            .filter(|c| matches!(c, '"' | '\'' | '`'));
        if let Some(q) = quote
            && let Some(end) = after[1..].find(q)
        {
            let mut inner = after[1..1 + end].to_string();
            if q == '"' {
                inner = inner.replace("\\n", "\n");
            }
            out.insert(key.to_string(), inner);
            let start = after.as_ptr() as usize - rest.as_ptr() as usize;
            let past_quote = &rest[start + 1 + end + 1..];
            rest = past_quote.split_once('\n').map_or("", |(_, t)| t);
            continue;
        }
        let value = line[eq + 1..].split('#').next().unwrap_or("").trim();
        out.insert(key.to_string(), value.to_string());
        rest = tail;
    }
    out
}

/// `detectEnvironmentEncoding` + a fatal `TextDecoder` (BOM stripped).
fn decode_pinokio_environment(bytes: &[u8]) -> Result<String, ()> {
    let utf16 = |little: bool, data: &[u8]| -> Result<String, ()> {
        if !data.len().is_multiple_of(2) {
            return Err(());
        }
        let units: Vec<u16> = data
            .chunks_exact(2)
            .map(|p| {
                if little {
                    u16::from_le_bytes([p[0], p[1]])
                } else {
                    u16::from_be_bytes([p[0], p[1]])
                }
            })
            .collect();
        let text = String::from_utf16(&units).map_err(|_| ())?;
        Ok(text
            .strip_prefix('\u{feff}')
            .map(str::to_string)
            .unwrap_or(text))
    };
    if bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] == 0xfe {
        return utf16(true, bytes);
    }
    if bytes.len() >= 2 && bytes[0] == 0xfe && bytes[1] == 0xff {
        return utf16(false, bytes);
    }
    let sample = &bytes[..bytes.len().min(512)];
    let (mut even, mut odd) = (0usize, 0usize);
    for (index, byte) in sample.iter().enumerate() {
        if *byte == 0 {
            if index % 2 == 0 {
                even += 1;
            } else {
                odd += 1;
            }
        }
    }
    let minimum = 2usize.max(sample.len() / 16);
    if odd >= minimum && odd > even * 2 {
        return utf16(true, bytes);
    }
    if even >= minimum && even > odd * 2 {
        return utf16(false, bytes);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| ())?;
    Ok(text.strip_prefix('\u{feff}').unwrap_or(text).to_string())
}

/// The environment the process was launched with, before `.env` loaded.
fn boot_environment(root: &Path) -> HashMap<String, String> {
    if let Ok(raw) = std::fs::read("/proc/self/environ") {
        return raw
            .split(|b| *b == 0)
            .filter_map(|pair| {
                let text = String::from_utf8_lossy(pair);
                let (k, v) = text.split_once('=')?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
    }
    // No /proc: count a value as launched-with unless `.env` supplied it.
    let dotenv = std::fs::read_to_string(root.join(".env"))
        .map(|t| parse_dotenv(&t))
        .unwrap_or_default();
    std::env::vars()
        .filter(|(k, v)| dotenv.get(k) != Some(v))
        .collect()
}

/// Why a save failed with a message of its own.
#[derive(Debug)]
enum StoreError {
    /// `CE_STORE_UNREADABLE`.
    Unreadable,
    /// `CE_HARDEN_FAILED`.
    HardenFailed,
    Other,
}

impl StoreError {
    fn message(&self) -> &'static str {
        match self {
            StoreError::Unreadable => {
                "the existing configuration could not be read, so nothing was changed"
            }
            StoreError::HardenFailed => {
                "could not restrict the credential file to your account; nothing was saved"
            }
            StoreError::Other => "",
        }
    }
}

pub struct KeySetup {
    root: PathBuf,
    launcher_at_boot: Option<String>,
    /// `PROVIDER_ENV_AT_BOOT`: trimmed boot value per known key.
    provider_env_at_boot: HashMap<String, String>,
    /// `DEV_FRESH_EXTERNAL_KEYS_AT_BOOT`.
    dev_fresh_external_keys: HashSet<String>,
    /// Saves are serialized: each is a read-modify-replace of the store.
    save: tokio::sync::Mutex<()>,
}

impl KeySetup {
    pub fn new(root: PathBuf, boot: &HashMap<String, String>) -> Self {
        let known = known_key_setup_env_vars();
        let provider_env_at_boot = known
            .iter()
            .map(|name| {
                (
                    name.to_string(),
                    boot.get(*name)
                        .map(|v| v.trim().to_string())
                        .unwrap_or_default(),
                )
            })
            .collect();
        let dev_fresh_external_keys = boot
            .get("CE_KEY_SETUP_EXTERNAL_KEYS")
            .map(|list| {
                list.split(',')
                    .map(str::trim)
                    .filter(|name| known.contains(name))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            root,
            launcher_at_boot: boot.get("CE_LAUNCHER").cloned(),
            provider_env_at_boot,
            dev_fresh_external_keys,
            save: tokio::sync::Mutex::new(()),
        }
    }

    fn pinokio_managed(&self) -> bool {
        self.launcher_at_boot.as_deref() == Some("pinokio")
    }

    fn store_name(&self) -> &'static str {
        if self.pinokio_managed() {
            "pinokio-environment"
        } else {
            "env-file"
        }
    }

    fn store_path(&self) -> PathBuf {
        if self.pinokio_managed() {
            self.root.join("pinokio").join("ENVIRONMENT")
        } else {
            self.root.join(".env")
        }
    }

    /// `readStore`: only a missing store reads as empty.
    fn read_store(&self) -> Result<String, StoreError> {
        let path = self.store_path();
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
            Err(_) => return Err(StoreError::Unreadable),
        };
        if self.pinokio_managed() {
            return decode_pinokio_environment(&bytes).map_err(|_| StoreError::Unreadable);
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn store_values(&self) -> HashMap<String, String> {
        self.read_store()
            .map(|text| parse_dotenv(&text))
            .unwrap_or_default()
    }

    fn is_externally_managed(
        &self,
        env: &Env,
        name: &str,
        in_store: &HashMap<String, String>,
    ) -> bool {
        let was_external_at_boot = if self.pinokio_managed() {
            false
        } else if self.launcher_at_boot.as_deref() == Some("dev-fresh") {
            self.dev_fresh_external_keys.contains(name)
        } else {
            self.provider_env_at_boot
                .get(name)
                .is_some_and(|v| !v.is_empty())
        };
        is_key_setup_externally_managed(
            env.raw(name).as_deref(),
            in_store.get(name).map(String::as_str),
            was_external_at_boot,
        )
    }

    fn provider_status(&self, env: &Env) -> Value {
        let in_store = self.store_values();
        let lookup = |name: &str| env.raw(name);
        let (keys, set_count) = key_setup_status(&lookup);
        let total = keys.len();
        let keys: Vec<Value> = keys
            .into_iter()
            .map(|mut key| {
                let set = key.get("set") == Some(&Value::Bool(true));
                let managed = if !set {
                    Value::Null
                } else {
                    let names = key
                        .get("envVars")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let external = names
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|name| self.is_externally_managed(env, name, &in_store));
                    json!(if external { "external" } else { "file" })
                };
                key.insert("managed".into(), managed);
                Value::Object(key)
            })
            .collect();
        json!({
            "keys": keys,
            "setCount": set_count,
            "total": total,
            "store": self.store_name(),
        })
    }

    /// `persistStore`: same-dir 0600 temp, hardened before the secret lands,
    /// fsync, rename over the target.
    fn persist_store(&self, text: &str) -> Result<(), StoreError> {
        use std::io::Write;
        let filepath = self.store_path();
        match std::fs::symlink_metadata(&filepath) {
            Ok(meta) if meta.file_type().is_symlink() => return Err(StoreError::Other),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(StoreError::Other),
        }
        let dir = filepath.parent().ok_or(StoreError::Other)?;
        let base = filepath
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(StoreError::Other)?;
        let tmp = dir.join(format!(".{base}.{}.tmp", random_suffix()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp).map_err(|_| StoreError::Other)?;
        let staged = (|| {
            if !harden_credential_file(&tmp) {
                return Err(StoreError::HardenFailed);
            }
            file.write_all(text.as_bytes())
                .map_err(|_| StoreError::Other)?;
            file.sync_all().map_err(|_| StoreError::Other)
        })();
        drop(file);
        if let Err(error) = staged {
            let _ = std::fs::remove_file(&tmp);
            return Err(error);
        }
        if std::fs::rename(&tmp, &filepath).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return Err(StoreError::Other);
        }
        Ok(())
    }

    async fn status(&self, state: &AppState, req: Request) -> Response {
        if req.method() != Method::GET {
            return respond(405, &json!({ "error": "Method not allowed" }));
        }
        if let Err((status, error)) = admit(state, &req) {
            return respond(status, &json!({ "error": error }));
        }
        respond(200, &self.provider_status(&state.env))
    }

    async fn keys(&self, state: &AppState, req: Request) -> Response {
        if req.method() != Method::POST {
            return respond(405, &json!({ "error": "Method not allowed" }));
        }
        if let Err((status, error)) = admit(state, &req) {
            return respond(status, &json!({ "error": error }));
        }
        let Some(body) = read_body_limited(req.into_body()).await else {
            return respond(413, &json!({ "error": "Request too large" }));
        };
        let parsed: Value = if body.is_empty() {
            json!({})
        } else {
            match serde_json::from_str(&body) {
                Ok(value) => value,
                Err(_) => return respond(400, &json!({ "error": "Invalid JSON" })),
            }
        };
        let updates = match validate_key_setup_updates(&parsed) {
            Ok(updates) => updates,
            Err(error) => return respond(400, &json!({ "error": error })),
        };

        let _turn = self.save.lock().await;
        // Neither a replace nor a removal may touch an externally-supplied key.
        let in_store = self.store_values();
        for (name, _) in &updates {
            if self.is_externally_managed(&state.env, name, &in_store) {
                return respond(
                    409,
                    &json!({
                        "error": format!(
                            "{name} is configured outside Provider Settings and can only be changed where it was set"
                        ),
                    }),
                );
            }
        }
        let saved = self
            .read_store()
            .and_then(|text| self.persist_store(&upsert_dotenv_values(&text, &updates)));
        if let Err(error) = saved {
            return match error {
                StoreError::HardenFailed | StoreError::Unreadable => respond(
                    500,
                    &json!({ "error": format!("The key was not saved: {}", error.message()) }),
                ),
                StoreError::Other => respond(
                    500,
                    &json!({ "error": format!("Could not write the {} store", self.store_name()) }),
                ),
            };
        }
        // Live for every provider at once. Removal sets '' rather than
        // deleting, matching the launcher's blank-field semantics.
        for (name, value) in &updates {
            state.env.set(name, value.as_deref().unwrap_or(""));
        }
        respond(
            200,
            &json!({
                "ok": true,
                "saved": updates.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>(),
                "status": self.provider_status(&state.env),
                // The JS dev server restarted itself here; this server applies
                // the keys live, so the flag only tells the panel to reload.
                "restarting": true,
            }),
        )
    }
}

/// A random-enough temp suffix (never the pid alone: a stale temp must not
/// block every later save).
fn random_suffix() -> String {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::SeqCst));
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    format!("{:016x}", hasher.finish())[..8].to_string()
}

/// `hardenCredentialFile`: owner-only before any secret is written. On
/// Windows the JS verified an exact ACL with system tools; this server does
/// not, and refuses the save instead (fail closed).
fn harden_credential_file(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if cfg!(target_os = "macos") {
            let stripped = std::process::Command::new("/bin/chmod")
                .arg("-N")
                .arg(path)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            if !stripped.is_ok_and(|s| s.success()) {
                return false;
            }
        }
        if std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).is_err() {
            return false;
        }
        std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o777 == 0o600)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

/// The JS read the body as a string and gave up past 8192 code units.
async fn read_body_limited(body: Body) -> Option<String> {
    let mut stream = body.into_data_stream();
    let mut bytes = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        bytes.extend_from_slice(&chunk);
        // UTF-8 never takes fewer bytes than UTF-16 code units.
        if bytes.len() > KEY_SETUP_MAX_BODY * 3 {
            return None;
        }
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    (text.encode_utf16().count() <= KEY_SETUP_MAX_BODY).then_some(text)
}

/// Every Provider Settings answer: JSON, never cached, never framed.
fn respond(status: u16, payload: &Value) -> Response {
    let response = reply::json(status, payload, Some("no-store"));
    let response = reply::with_header(response, "X-Frame-Options", "DENY");
    reply::with_header(
        response,
        "Content-Security-Policy",
        "frame-ancestors 'none'",
    )
}

fn admit(state: &AppState, req: &Request) -> Result<(), (u16, &'static str)> {
    let headers = req.headers();
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let remote = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip().to_string());
    let lookup = |name: &str| state.env.raw(name);
    admit_key_setup_request(&Admission {
        method: req.method().as_str(),
        remote_address: remote.as_deref(),
        host_header: header("host"),
        protocol: "http:",
        origin: header("origin"),
        content_type: header("content-type"),
        proxy_headers: headers,
        env: &lookup,
    })
}

pub fn router(state: &AppState) -> Router<AppState> {
    let boot = boot_environment(&state.root);
    router_with(Arc::new(KeySetup::new(state.root.clone(), &boot)))
}

fn router_with(svc: Arc<KeySetup>) -> Router<AppState> {
    let status = {
        let svc = svc.clone();
        any(move |State(state): State<AppState>, req: Request| {
            let svc = svc.clone();
            async move { svc.status(&state, req).await }
        })
    };
    let keys = any(move |State(state): State<AppState>, req: Request| {
        let svc = svc.clone();
        async move { svc.keys(&state, req).await }
    });
    let router = mount(Router::new(), "/api/setup/status", status);
    mount(router, "/api/setup/keys", keys)
}

/// Connect's `use(path)` also answers every sub-path.
fn mount(
    router: Router<AppState>,
    path: &str,
    handler: MethodRouter<AppState>,
) -> Router<AppState> {
    router
        .route(path, handler.clone())
        .route(&format!("{path}/"), handler.clone())
        .route(&format!("{path}/{{*rest}}"), handler)
}

#[cfg(test)]
mod tests {
    use super::super::openai::testkit::{Call, call, state, temp_dir};
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    struct Req<'a> {
        method: &'a str,
        remote: Option<&'a str>,
        host: Option<&'a str>,
        origin: Option<&'a str>,
        content_type: Option<&'a str>,
        headers: HeaderMap,
        env: Vec<(&'a str, &'a str)>,
    }

    fn local<'a>() -> Req<'a> {
        Req {
            method: "POST",
            remote: Some("127.0.0.1"),
            host: Some("localhost:4173"),
            origin: Some("http://localhost:4173"),
            content_type: Some("application/json"),
            headers: HeaderMap::new(),
            env: vec![],
        }
    }

    fn admitted(req: Req) -> Result<(), (u16, &'static str)> {
        let env = req.env.clone();
        let lookup = move |name: &str| {
            env.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        };
        admit_key_setup_request(&Admission {
            method: req.method,
            remote_address: req.remote,
            host_header: req.host,
            protocol: "http:",
            origin: req.origin,
            content_type: req.content_type,
            proxy_headers: &req.headers,
            env: &lookup,
        })
    }

    #[test]
    fn the_gate_refuses_every_non_local_shape() {
        assert!(admitted(local()).is_ok());
        assert!(
            admitted(Req {
                method: "GET",
                content_type: None,
                ..local()
            })
            .is_ok()
        );
        assert!(
            admitted(Req {
                origin: None,
                ..local()
            })
            .is_err()
        );
        assert!(
            admitted(Req {
                method: "GET",
                origin: None,
                content_type: None,
                ..local()
            })
            .is_ok()
        );
        assert!(
            admitted(Req {
                remote: Some("::ffff:127.0.0.1"),
                host: Some("[::1]:4173"),
                origin: Some("http://[::1]:4173"),
                ..local()
            })
            .is_ok()
        );
        assert!(
            admitted(Req {
                env: vec![("PINOKIO_SHARE_CLOUDFLARE", "true")],
                ..local()
            })
            .is_err()
        );
        assert!(
            admitted(Req {
                env: vec![("PINOKIO_SHARE_LOCAL", "1")],
                ..local()
            })
            .is_err()
        );
        assert_eq!(
            admitted(Req {
                remote: Some("192.168.1.20"),
                ..local()
            }),
            Err((
                403,
                "Provider Settings answers only the machine running the server"
            ))
        );
        assert_eq!(
            admitted(Req {
                remote: None,
                ..local()
            })
            .unwrap_err()
            .0,
            403
        );
        for host in [
            "abc.trycloudflare.com",
            "workstation.local:4173",
            "",
            "[::1].evil:4173",
        ] {
            assert_eq!(
                admitted(Req {
                    host: Some(host),
                    ..local()
                }),
                Err((403, "Provider Settings answers only local hostnames")),
                "{host}"
            );
        }
        assert_eq!(
            admitted(Req {
                origin: Some("https://evil.example"),
                ..local()
            }),
            Err((403, "Cross-origin requests are refused"))
        );
        assert_eq!(
            admitted(Req {
                origin: Some("not a url"),
                ..local()
            }),
            Err((403, "Unrecognized Origin refused"))
        );
        for origin in [
            "http://localhost:4174",
            "https://localhost:4173",
            "http://127.0.0.1:4173",
            "http://localhost:4173/path",
        ] {
            assert!(
                admitted(Req {
                    origin: Some(origin),
                    ..local()
                })
                .is_err(),
                "{origin}"
            );
        }
        assert_eq!(
            admitted(Req {
                content_type: Some("text/plain"),
                ..local()
            }),
            Err((415, "Content-Type must be application/json"))
        );
        assert_eq!(
            admitted(Req {
                origin: None,
                ..local()
            }),
            Err((403, "Provider Settings requires an exact local Origin"))
        );
    }

    #[test]
    fn sharing_and_proxy_signals() {
        for (value, ok) in [
            ("", true),
            ("__ce_sharing_disabled__", true),
            ("MY_TUNNEL_TOKEN", false),
        ] {
            assert_eq!(
                admitted(Req {
                    env: vec![("PINOKIO_SHARE_VAR", value)],
                    ..local()
                })
                .is_ok(),
                ok,
                "{value}"
            );
        }
        for header in [
            "x-forwarded-for",
            "forwarded",
            "via",
            "cf-connecting-ip",
            "cf-ray",
            "x-real-ip",
            "x-forwarded-host",
            "x-forwarded-port",
            "x-forwarded-proto",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header, "anything".parse().unwrap());
            assert_eq!(
                admitted(Req { headers, ..local() }),
                Err((403, "Provider Settings does not answer proxied requests")),
                "{header}"
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "".parse().unwrap());
        assert!(admitted(Req { headers, ..local() }).is_ok());
    }

    #[test]
    fn external_ownership_uses_boot_provenance() {
        assert!(is_key_setup_externally_managed(
            Some("same"),
            Some("same"),
            true
        ));
        assert!(!is_key_setup_externally_managed(
            Some("file"),
            Some("file"),
            false
        ));
        assert!(is_key_setup_externally_managed(
            Some("shell"),
            Some("stale"),
            false
        ));
        assert!(!is_key_setup_externally_managed(
            Some(""),
            Some("stale"),
            true
        ));
    }

    #[test]
    fn status_reports_presence_only() {
        let env = |name: &str| match name {
            "GOOGLE_MAPS_API_KEY" => Some("AIzaSyFakeFakeFakeFake1234".to_string()),
            "OPENSKY_CLIENT_ID" => Some("client-id-abcdef".to_string()),
            "GOOGLE_MAPS_SERVER_API_KEY" => Some("server-key-fixture".to_string()),
            "OPENAI_API_KEY" => Some("   ".to_string()),
            _ => None,
        };
        let (keys, set_count) = key_setup_status(&env);
        assert_eq!(
            keys.len(),
            KEY_SETUP_KEYS.iter().filter(|k| !k.hidden).count()
        );
        assert_eq!(set_count, 1);
        let find = |id: &str| keys.iter().find(|k| k["id"] == id).unwrap();
        assert_eq!(find("google-maps")["set"], true);
        assert_eq!(find("opensky")["set"], false);
        assert_eq!(find("openai")["set"], false);
        let serialized = serde_json::to_string(&keys).unwrap();
        for leak in [
            "AIzaSy",
            "1234",
            "abcdef",
            "server-key-fixture",
            "GOOGLE_MAPS_SERVER_API_KEY",
        ] {
            assert!(!serialized.contains(leak), "{leak}");
        }
        assert_eq!(
            serde_json::to_value(&keys[0]).unwrap(),
            json!({
                "id": "google-maps",
                "title": "GOOGLE MAPS",
                "unlocks": "The photorealistic 3D planet + place search",
                "getUrl": "https://developers.google.com/maps/documentation/tile/get-api-key",
                "envVars": ["GOOGLE_MAPS_API_KEY"],
                "tier": "metered",
                "clientExposed": true,
                "set": true,
            })
        );
        let (_, none) = key_setup_status(&no_env);
        assert_eq!(none, 0);
    }

    #[test]
    fn validation_rules() {
        for name in known_key_setup_env_vars() {
            let updates = validate_key_setup_updates(&json!({ name: "valid-value-123" })).unwrap();
            assert_eq!(
                updates,
                vec![(name.to_string(), Some("valid-value-123".to_string()))]
            );
        }
        assert!(validate_key_setup_updates(&json!({ "PATH": "/usr/bin" })).is_err());
        assert_eq!(
            validate_key_setup_updates(&json!({ "OPENAI_API_KEY": "  sk-abc123  " })).unwrap(),
            vec![("OPENAI_API_KEY".to_string(), Some("sk-abc123".to_string()))]
        );
        for bad in ["", "   ", "a\nb", "a b", "kéy"] {
            assert!(
                validate_key_setup_updates(&json!({ "OPENAI_API_KEY": bad })).is_err(),
                "{bad:?}"
            );
        }
        assert!(
            validate_key_setup_updates(
                &json!({ "OPENAI_API_KEY": "x".repeat(KEY_SETUP_VALUE_LIMIT + 1) })
            )
            .is_err()
        );
        assert_eq!(
            validate_key_setup_updates(&Value::Null).unwrap_err(),
            "Body must be a JSON object of {ENV_VAR: value}"
        );
        assert!(validate_key_setup_updates(&json!([])).is_err());
        assert_eq!(
            validate_key_setup_updates(&json!({})).unwrap_err(),
            "No keys provided"
        );
        assert_eq!(
            validate_key_setup_updates(&json!({ "OPENAI_API_KEY": 42 })).unwrap_err(),
            "OPENAI_API_KEY must be a string"
        );
        assert_eq!(
            validate_key_setup_updates(&json!({ "OPENAI_API_KEY": null })).unwrap(),
            vec![("OPENAI_API_KEY".to_string(), None)]
        );
        assert!(validate_key_setup_updates(&json!({ "PATH": null })).is_err());
        for bad in ["abc#def", "ab\"cd", "ab'cd", "ab$cd", "ab\\cd", "ab`cd"] {
            assert!(
                validate_key_setup_updates(&json!({ "OPENAI_API_KEY": bad })).is_err(),
                "{bad}"
            );
        }
        for good in [
            "sk-AbC0-9_x",
            "eyJhbGc.eyJzdWI.QWxpY2U",
            "a1b2c3d4e5f6",
            "AB+cd/ef=",
        ] {
            assert!(
                validate_key_setup_updates(&json!({ "OPENAI_API_KEY": good })).is_ok(),
                "{good}"
            );
        }
        let many: Map<String, Value> = (0..17).map(|i| (format!("K{i}"), json!("v"))).collect();
        assert_eq!(
            validate_key_setup_updates(&Value::Object(many)).unwrap_err(),
            "At most 16 keys per save"
        );
    }

    fn upd(pairs: &[(&str, Option<&str>)]) -> Updates {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect()
    }

    #[test]
    fn upsert_shapes() {
        let text = "# comment stays\nOPENAI_API_KEY=old-one\nPORT=4173\nOPENAI_API_KEY=old-two\n";
        assert_eq!(
            upsert_dotenv_values(text, &upd(&[("OPENAI_API_KEY", Some("new-key"))])),
            "# comment stays\nOPENAI_API_KEY=old-one\nPORT=4173\nOPENAI_API_KEY=new-key\n"
        );
        let text = "# Optional: NASA FIRMS live active fires.\n# FIRMS_MAP_KEY=\n\nPORT=4173";
        assert_eq!(
            upsert_dotenv_values(text, &upd(&[("FIRMS_MAP_KEY", Some("firms-123"))])),
            "# Optional: NASA FIRMS live active fires.\nFIRMS_MAP_KEY=firms-123\n\nPORT=4173\n"
        );
        let first = upsert_dotenv_values("PORT=4173\n", &upd(&[("OPENAI_API_KEY", Some("sk-1"))]));
        assert_eq!(
            first,
            format!("PORT=4173\n\n{KEY_SETUP_APPEND_HEADER}\nOPENAI_API_KEY=sk-1\n")
        );
        let second = upsert_dotenv_values(&first, &upd(&[("FIRMS_MAP_KEY", Some("f-2"))]));
        assert_eq!(
            second,
            format!(
                "PORT=4173\n\n{KEY_SETUP_APPEND_HEADER}\nOPENAI_API_KEY=sk-1\nFIRMS_MAP_KEY=f-2\n"
            )
        );
        assert_eq!(
            upsert_dotenv_values("", &upd(&[("GOOGLE_MAPS_API_KEY", Some("AIza-x"))])),
            format!("{KEY_SETUP_APPEND_HEADER}\nGOOGLE_MAPS_API_KEY=AIza-x\n")
        );
        let text = "export OPENAI_API_KEY=old\nNOT_OPENAI_API_KEY=keep-me\nOPENAI_API_KEY_MINI=keep-me-too";
        let next = upsert_dotenv_values(text, &upd(&[("OPENAI_API_KEY", Some("new"))]));
        let lines: Vec<&str> = next.split('\n').collect();
        assert_eq!(
            lines[..3],
            [
                "OPENAI_API_KEY=new",
                "NOT_OPENAI_API_KEY=keep-me",
                "OPENAI_API_KEY_MINI=keep-me-too"
            ]
        );
        let both = upd(&[
            ("OPENAI_API_KEY", Some("sk-1")),
            ("FIRMS_MAP_KEY", Some("f-1")),
        ]);
        let once = upsert_dotenv_values("", &both);
        assert_eq!(upsert_dotenv_values(&once, &both), once);

        let example = "# Camera Eye — environment variables\nGOOGLE_MAPS_API_KEY=your_google_maps_api_key_here\n\n# Optional: OpenAI Realtime voice control. Do not prefix with VITE_.\nOPENAI_API_KEY=\nOPENAI_REALTIME_MODEL=gpt-realtime-2\n\n# TOMTOM_API_KEY=";
        let next = upsert_dotenv_values(
            example,
            &upd(&[
                ("GOOGLE_MAPS_API_KEY", Some("AIza-real")),
                ("OPENAI_API_KEY", Some("sk-real")),
                ("TOMTOM_API_KEY", Some("tt-real")),
            ]),
        );
        let lines: Vec<&str> = next.split('\n').collect();
        assert_eq!(lines[1], "GOOGLE_MAPS_API_KEY=AIza-real");
        assert_eq!(lines[4], "OPENAI_API_KEY=sk-real");
        assert_eq!(lines[5], "OPENAI_REALTIME_MODEL=gpt-realtime-2");
        assert_eq!(lines[7], "TOMTOM_API_KEY=tt-real");

        let text = "# Optional: OpenAI Realtime voice control.\nOPENAI_API_KEY=sk-live\nPORT=4173";
        let removed = upsert_dotenv_values(text, &upd(&[("OPENAI_API_KEY", None)]));
        let lines: Vec<&str> = removed.split('\n').collect();
        assert_eq!(lines[1], "# OPENAI_API_KEY=");
        assert_eq!(lines[2], "PORT=4173");
        assert_eq!(
            upsert_dotenv_values(&removed, &upd(&[("FIRMS_MAP_KEY", None)])),
            removed
        );
        let again = upsert_dotenv_values(&removed, &upd(&[("OPENAI_API_KEY", Some("sk-new"))]));
        assert_eq!(again.split('\n').nth(1), Some("OPENAI_API_KEY=sk-new"));
        assert_eq!(
            upsert_dotenv_values("A=1\r\nB=2\r\n", &upd(&[("FIRMS_MAP_KEY", None)])),
            "A=1\nB=2\n"
        );
    }

    #[test]
    fn dotenv_parsing() {
        let parsed = parse_dotenv(
            "# c\nA=1\nexport B = two # note\nC=\"multi\nline\"\nD='x # y'\nE\nA=last\n",
        );
        assert_eq!(parsed["A"], "last");
        assert_eq!(parsed["B"], "two");
        assert_eq!(parsed["C"], "multi\nline");
        assert_eq!(parsed["D"], "x # y");
        assert!(!parsed.contains_key("E"));
    }

    #[test]
    fn pinokio_environment_decoding() {
        let text = "OPENAI_API_KEY=x\n";
        let mut le = vec![0xff, 0xfe];
        le.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
        assert_eq!(decode_pinokio_environment(&le).unwrap(), text);
        let be: Vec<u8> = text.encode_utf16().flat_map(u16::to_be_bytes).collect();
        assert_eq!(decode_pinokio_environment(&be).unwrap(), text);
        assert_eq!(decode_pinokio_environment(text.as_bytes()).unwrap(), text);
        assert!(decode_pinokio_environment(&[0xc3, 0x28]).is_err());
    }

    fn app(root: &Path, boot: &[(&str, &str)]) -> (axum::Router, AppState) {
        let state = state(root.to_path_buf());
        for name in known_key_setup_env_vars() {
            state.env.set(name, "");
        }
        for name in [
            "PINOKIO_SHARE_CLOUDFLARE",
            "PINOKIO_SHARE_LOCAL",
            "PINOKIO_SHARE_VAR",
        ] {
            state.env.set(name, "");
        }
        let boot: HashMap<String, String> = boot
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let svc = Arc::new(KeySetup::new(root.to_path_buf(), &boot));
        (router_with(svc).with_state(state.clone()), state)
    }

    fn post_keys(body: &str) -> Call<'static> {
        let body = body.to_string();
        Call {
            method: "POST",
            uri: "/api/setup/keys",
            body: body.into_bytes(),
            headers: vec![
                ("host", "localhost:4173"),
                ("origin", "http://localhost:4173"),
                ("content-type", "application/json"),
            ],
            ..Call::default()
        }
    }

    #[tokio::test]
    async fn saves_write_only_the_root_store_owner_only_and_go_live() {
        let root = temp_dir("key-setup");
        let (app, state) = app(&root, &[]);
        let body = r#"{"OPENAI_API_KEY":"sk-fixture-only-not-a-real-key"}"#;
        let mut cross = post_keys(body);
        cross.headers[1] = ("origin", "https://example.com");
        assert_eq!(call(&app, cross).await.status, 403);
        assert!(!root.join(".env").exists());

        let saved = call(&app, post_keys(body)).await;
        assert_eq!(saved.status, 200);
        assert_eq!(saved.header("cache-control"), Some("no-store"));
        assert_eq!(saved.header("x-frame-options"), Some("DENY"));
        assert_eq!(
            saved.header("content-security-policy"),
            Some("frame-ancestors 'none'")
        );
        let reply = saved.json();
        assert_eq!(reply["ok"], true);
        assert_eq!(reply["saved"], json!(["OPENAI_API_KEY"]));
        assert_eq!(reply["restarting"], true);
        assert_eq!(reply["status"]["store"], "env-file");
        let openai = reply["status"]["keys"]
            .as_array()
            .unwrap()
            .iter()
            .find(|k| k["id"] == "openai")
            .unwrap()
            .clone();
        assert_eq!(openai["set"], true);
        assert_eq!(openai["managed"], "file");
        assert!(!saved.body.contains("sk-fixture-only-not-a-real-key"));
        let written = std::fs::read_to_string(root.join(".env")).unwrap();
        assert!(written.contains("OPENAI_API_KEY=sk-fixture-only-not-a-real-key"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(root.join(".env"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert_eq!(
            state.env.get("OPENAI_API_KEY").as_deref(),
            Some("sk-fixture-only-not-a-real-key"),
            "the new key is live without a restart"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty());

        // Removal comments the line out and blanks the live value.
        let removed = call(&app, post_keys(r#"{"OPENAI_API_KEY":null}"#)).await;
        assert_eq!(removed.status, 200);
        assert!(
            std::fs::read_to_string(root.join(".env"))
                .unwrap()
                .contains("# OPENAI_API_KEY=")
        );
        assert_eq!(state.env.get("OPENAI_API_KEY"), None);
    }

    #[tokio::test]
    async fn external_keys_are_read_only_and_errors_are_fixed() {
        let root = temp_dir("key-setup-external");
        let (app, state) = app(&root, &[("FIRMS_MAP_KEY", "from-shell")]);
        state.env.set("FIRMS_MAP_KEY", "from-shell");
        let status = call(
            &app,
            Call {
                headers: vec![("host", "localhost:4173")],
                ..Call::default_at("/api/setup/status")
            },
        )
        .await;
        assert_eq!(status.status, 200);
        let firms = status.json()["keys"]
            .as_array()
            .unwrap()
            .iter()
            .find(|k| k["id"] == "firms")
            .unwrap()
            .clone();
        assert_eq!(firms["managed"], "external");
        assert!(!status.body.contains("from-shell"));

        let conflict = call(&app, post_keys(r#"{"FIRMS_MAP_KEY":"new-value"}"#)).await;
        assert_eq!(conflict.status, 409);
        assert_eq!(
            conflict.json()["error"],
            "FIRMS_MAP_KEY is configured outside Provider Settings and can only be changed where it was set"
        );
        assert_eq!(
            call(&app, post_keys("{nope")).await.json()["error"],
            "Invalid JSON"
        );
        assert_eq!(
            call(&app, post_keys(r#"{"PATH":"x"}"#)).await.json()["error"],
            "Unknown key: PATH"
        );
        let big = format!(r#"{{"OPENAI_API_KEY":"{}"}}"#, "x".repeat(9000));
        assert_eq!(call(&app, post_keys(&big)).await.status, 413);
        let wrong = call(&app, Call::default_at("/api/setup/keys")).await;
        assert_eq!(wrong.status, 405);
        assert_eq!(wrong.json()["error"], "Method not allowed");
        let lan = call(
            &app,
            Call {
                peer: "192.168.1.20:5000",
                ..post_keys(r#"{"OPENAI_API_KEY":"sk"}"#)
            },
        )
        .await;
        assert_eq!(lan.status, 403);
        state.env.set("PINOKIO_SHARE_LOCAL", "true");
        let shared = call(&app, post_keys(r#"{"OPENAI_API_KEY":"sk"}"#)).await;
        assert_eq!(
            shared.json()["error"],
            "Provider Settings is disabled while sharing is enabled"
        );
        assert!(!root.join(".env").exists());
    }

    #[tokio::test]
    async fn an_unreadable_store_aborts_the_save() {
        let root = temp_dir("key-setup-unreadable");
        std::fs::create_dir_all(root.join(".env")).unwrap();
        let (app, _) = app(&root, &[]);
        let reply = call(&app, post_keys(r#"{"OPENAI_API_KEY":"sk-1"}"#)).await;
        assert_eq!(reply.status, 500);
        assert_eq!(
            reply.json()["error"],
            "The key was not saved: the existing configuration could not be read, so nothing was changed"
        );
    }

    #[tokio::test]
    async fn the_pinokio_launcher_owns_its_environment_store() {
        let root = temp_dir("key-setup-pinokio");
        std::fs::create_dir_all(root.join("pinokio")).unwrap();
        let (app, _) = app(&root, &[("CE_LAUNCHER", "pinokio")]);
        let reply = call(&app, post_keys(r#"{"TOMTOM_API_KEY":"tt-1"}"#)).await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.json()["status"]["store"], "pinokio-environment");
        assert!(
            std::fs::read_to_string(root.join("pinokio/ENVIRONMENT"))
                .unwrap()
                .contains("TOMTOM_API_KEY=tt-1")
        );
        assert!(!root.join(".env").exists());
    }
}
