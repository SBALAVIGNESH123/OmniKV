use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

fn global_active() -> Arc<Mutex<HashSet<String>>> {
    static ACTIVE: OnceLock<Arc<Mutex<HashSet<String>>>> = OnceLock::new();
    ACTIVE
        .get_or_init(|| Arc::new(Mutex::new(HashSet::new())))
        .clone()
}

/// Process-global failure-injection registry.
///
/// The registry is always available so tests and tooling can share the same
/// control surface. Production call sites use [`maybe_fail`], whose body is
/// compiled to a no-op unless the `failpoints` Cargo feature is enabled.
#[derive(Clone)]
pub struct FailRegistry {
    active: Arc<Mutex<HashSet<String>>>,
}

impl Default for FailRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FailRegistry {
    pub fn new() -> Self {
        Self {
            active: global_active(),
        }
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

/// Trigger a named failure point when the `failpoints` feature is enabled.
///
/// In a normal build this is an inlined no-op. With the feature enabled, an
/// armed point returns an I/O error so callers can exercise the same error
/// path as a real filesystem failure without panicking the process.
#[inline]
pub fn maybe_fail(name: &str) -> std::io::Result<()> {
    #[cfg(feature = "failpoints")]
    if FailRegistry::new().is_armed(name) {
        return Err(std::io::Error::other(format!(
            "failure point armed: {name}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arm_and_disarm() {
        let reg = FailRegistry::new();
        reg.reset();
        assert!(!reg.is_armed("wal_sync"));
        reg.arm("wal_sync");
        assert!(reg.is_armed("wal_sync"));
        reg.disarm("wal_sync");
        assert!(!reg.is_armed("wal_sync"));
    }

    #[test]
    fn reset_clears_all() {
        let reg = FailRegistry::new();
        reg.reset();
        reg.arm("a");
        reg.arm("b");
        reg.reset();
        assert!(!reg.is_armed("a"));
        assert!(!reg.is_armed("b"));
    }

    #[test]
    fn clone_shares_state() {
        let reg = FailRegistry::new();
        reg.reset();
        let reg2 = reg.clone();
        reg.arm("x");
        assert!(reg2.is_armed("x"));
        reg.reset();
    }

    #[test]
    fn disarmed_failure_point_is_a_noop() {
        let reg = FailRegistry::new();
        reg.reset();
        assert!(maybe_fail("not-armed").is_ok());
    }
}
