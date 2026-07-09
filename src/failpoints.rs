//! Deterministic failure-injection harness for OmniKV crash-consistency tests.
//!
//! # Usage
//!
//! ```rust
//! use omnikv::failpoints::FailRegistry;
//!
//! let reg = FailRegistry::new();
//! reg.arm("wal::sync");
//! assert!(reg.should_fail("wal::sync"));
//! reg.disarm("wal::sync");
//! assert!(!reg.should_fail("wal::sync"));
//! ```
//!
//! In release builds without the `failpoints` feature, `should_fail` always
//! returns `false` and all arm/disarm calls compile away.

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

    /// Arm a named failure point.  Subsequent `should_fail` calls for this
    /// name return `true` until the point is disarmed.
    pub fn arm(&self, point: &str) {
        self.armed
            .lock()
            .expect("FailRegistry mutex poisoned")
            .insert(point.to_owned());
    }

    /// Disarm a named failure point.
    pub fn disarm(&self, point: &str) {
        self.armed
            .lock()
            .expect("FailRegistry mutex poisoned")
            .remove(point);
    }

    /// Returns `true` if `point` is currently armed.
    ///
    /// In a release build compiled *without* the `failpoints` feature this
    /// method still works; the registry is simply always empty because no
    /// test code arms any points.
    pub fn should_fail(&self, point: &str) -> bool {
        self.armed
            .lock()
            .expect("FailRegistry mutex poisoned")
            .contains(point)
    }

    /// Disarm all failure points.
    pub fn disarm_all(&self) {
        self.armed
            .lock()
            .expect("FailRegistry mutex poisoned")
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arm_disarm() {
        let reg = FailRegistry::new();
        assert!(!reg.should_fail("wal::sync"));
        reg.arm("wal::sync");
        assert!(reg.should_fail("wal::sync"));
        reg.disarm("wal::sync");
        assert!(!reg.should_fail("wal::sync"));
    }

    #[test]
    fn test_disarm_all() {
        let reg = FailRegistry::new();
        reg.arm("a");
        reg.arm("b");
        reg.disarm_all();
        assert!(!reg.should_fail("a"));
        assert!(!reg.should_fail("b"));
    }

    #[test]
    fn test_clone_shares_state() {
        let reg = FailRegistry::new();
        let reg2 = reg.clone();
        reg.arm("shared");
        assert!(reg2.should_fail("shared"));
    }
}
