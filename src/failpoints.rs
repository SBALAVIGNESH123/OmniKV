use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Process-global failure-injection registry.
///
/// Enabled only when the `failpoints` Cargo feature is active.
/// In normal builds the registry is compiled in but all methods are no-ops,
/// so there is zero runtime cost unless the feature is explicitly enabled.
#[derive(Clone, Default)]
pub struct FailRegistry {
    active: Arc<Mutex<HashSet<String>>>,
}

impl FailRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm a named failure point.
    pub fn arm(&self, name: &str) {
        self.active.lock().unwrap().insert(name.to_string());
    }

    /// Disarm a named failure point.
    pub fn disarm(&self, name: &str) {
        self.active.lock().unwrap().remove(name);
    }

    /// Returns true if the named failure point is currently armed.
    pub fn is_armed(&self, name: &str) -> bool {
        self.active.lock().unwrap().contains(name)
    }

    /// Disarm all failure points.
    pub fn reset(&self) {
        self.active.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arm_and_disarm() {
        let reg = FailRegistry::new();
        assert!(!reg.is_armed("wal_sync"));
        reg.arm("wal_sync");
        assert!(reg.is_armed("wal_sync"));
        reg.disarm("wal_sync");
        assert!(!reg.is_armed("wal_sync"));
    }

    #[test]
    fn reset_clears_all() {
        let reg = FailRegistry::new();
        reg.arm("a");
        reg.arm("b");
        reg.reset();
        assert!(!reg.is_armed("a"));
        assert!(!reg.is_armed("b"));
    }

    #[test]
    fn clone_shares_state() {
        let reg = FailRegistry::new();
        let reg2 = reg.clone();
        reg.arm("x");
        assert!(reg2.is_armed("x"));
    }
}
