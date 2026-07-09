use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Process-global failure-injection registry.
///
/// Gate all hooks behind `cfg!(feature = "failpoints")` so that
/// production builds compiled *without* the feature have zero overhead.
#[derive(Clone, Default)]
pub struct FailRegistry {
    active: Arc<Mutex<HashSet<String>>>,
}

impl FailRegistry {
    /// Create a new registry with no active fail-points.
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm a named fail-point.
    pub fn arm(&self, name: &str) {
        if cfg!(feature = "failpoints") {
            if let Ok(mut g) = self.active.lock() {
                g.insert(name.to_owned());
            }
        }
    }

    /// Disarm a named fail-point.
    pub fn disarm(&self, name: &str) {
        if cfg!(feature = "failpoints") {
            if let Ok(mut g) = self.active.lock() {
                g.remove(name);
            }
        }
    }

    /// Return `true` when the named fail-point is armed.
    pub fn is_armed(&self, name: &str) -> bool {
        if cfg!(feature = "failpoints") {
            self.active
                .lock()
                .map(|g| g.contains(name))
                .unwrap_or(false)
        } else {
            false
        }
    }

    /// Disarm all fail-points.
    pub fn reset(&self) {
        if cfg!(feature = "failpoints") {
            if let Ok(mut g) = self.active.lock() {
                g.clear();
            }
        }
    }
}
