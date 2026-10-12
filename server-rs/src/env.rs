//! Process configuration that can change at runtime.
//!
//! The JS server read `process.env` on every request, and the key setup
//! panel wrote new keys into it. Rust cannot safely mutate the process
//! environment from a multi-threaded server, so keys live here instead:
//! values set through [`Env::set`] shadow the process environment.

use std::collections::HashMap;

use parking_lot::RwLock;

#[derive(Default)]
pub struct Env {
    overrides: RwLock<HashMap<String, String>>,
}

impl Env {
    /// The raw value of `name`, as `process.env[name]` would return it.
    pub fn raw(&self, name: &str) -> Option<String> {
        if let Some(value) = self.overrides.read().get(name) {
            return Some(value.clone());
        }
        std::env::var(name).ok()
    }

    /// The trimmed value of `name`, or `None` when unset or blank.
    pub fn get(&self, name: &str) -> Option<String> {
        self.raw(name)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    /// Set `name` for the rest of this process.
    pub fn set(&self, name: &str, value: &str) {
        self.overrides
            .write()
            .insert(name.to_string(), value.to_string());
    }

    /// Every variable visible to the server, overrides winning.
    pub fn snapshot(&self) -> HashMap<String, String> {
        let mut all: HashMap<String, String> = std::env::vars().collect();
        for (key, value) in self.overrides.read().iter() {
            all.insert(key.clone(), value.clone());
        }
        all
    }
}
