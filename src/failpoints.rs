//! Deterministic failure-injection harness for OmniKV crash-consistency tests.
//!
//! # Usage
//!
//! ```rust,ignore
//! use omnikv::failpoints::FailRegistry;
//!
//! let reg = FailRegistry::new();
//! reg.arm("wal::sync");
//! assert!(reg.should_fail("wal::sync"));
//! reg.disarm("wal::sync");
//! assert!(!reg.should_fail("wal::sync"));
//! ```
//!
//! The `failpoints` feature gate controls whether this module compiles in.
//! Without that feature, `should_fail` always returns `false`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Process-global registry of armed failure-injection points.
///
/// Clone is cheap — the inner state is reference-counted.
#[derive(Clone, Default)]
pub struct FailRegistry {
    armed: Arc<Mutex<HashSet<String>>>,
}

impl FailRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm a failure-injection point by name.
    pub fn arm(&self, point: &str) {
        self.armed
            .lock()
            .expect("lock poisoned")
            .insert(point.to_owned());
    }

    /// Disarm a failure-injection point by name.
    pub fn disarm(&self, point: &str) {
        self.armed
            .lock()
            .expect("lock poisoned")
            .remove(point);
    }

    /// Returns `true` if the named failure-injection point is currently armed.
    pub fn should_fail(&self, point: &str) -> bool {
        self.armed
            .lock()
            .expect("lock poisoned")
            .contains(point)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arm_disarm_round_trip() {
        let reg = FailRegistry::new();
        assert!(!reg.should_fail("x"));
        reg.arm("x");
        assert!(reg.should_fail("x"));
        reg.disarm("x");
        assert!(!reg.should_fail("x"));
    }

    #[test]
    fn clone_shares_state() {
        let reg = FailRegistry::new();
        let reg2 = reg.clone();
        reg.arm("y");
        assert!(reg2.should_fail("y"));
    }
}
