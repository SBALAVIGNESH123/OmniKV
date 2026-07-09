//! Deterministic failure-injection harness for OmniKV crash-consistency tests.
//!
//! # Design
//! Each `FailurePoint` is a named hook that can be armed with a `FailureMode`.
//! Production code calls `maybe_fail("point_name")` which is a no-op unless the
//! point is armed. Tests arm points, drive the engine, then verify recovery.
//!
//! # Thread Safety
//! Points are stored in a process-global `DashMap`-free registry using
//! `std::sync::OnceLock` + `Mutex` so the harness has zero non-std dependencies.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// The mode a failure point is armed with.
#[derive(Clone, Debug)]
pub enum FailureMode {
    /// Panic immediately — simulates process death.
    Panic,
    /// Return an `io::Error` with the given message.
    IoError(String),
    /// Fire only on the N-th call (1-based).
    OnNthCall { n: usize, mode: Box<FailureMode> },
}

struct PointState {
    mode: FailureMode,
    call_count: usize,
}

static REGISTRY: OnceLock<Mutex<HashMap<String, PointState>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, PointState>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arm a failure point.
pub fn arm(name: &str, mode: FailureMode) {
    let mut reg = registry().lock().unwrap();
    reg.insert(name.to_string(), PointState { mode, call_count: 0 });
}

/// Disarm a failure point (no-op if not armed).
pub fn disarm(name: &str) {
    let mut reg = registry().lock().unwrap();
    reg.remove(name);
}

/// Disarm all failure points.
pub fn disarm_all() {
    let mut reg = registry().lock().unwrap();
    reg.clear();
}

/// Check a failure point. Call from production code; is a no-op in release
/// builds unless the `failpoints` feature is enabled.
///
/// # Panics
/// Panics if the point is armed with `FailureMode::Panic`.
///
/// # Errors
/// Returns `Err(io::Error)` if armed with `FailureMode::IoError`.
#[inline]
pub fn maybe_fail(name: &str) -> std::io::Result<()> {
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        return Ok(());
    }
    #[cfg(feature = "failpoints")]
    {
        let mut reg = registry().lock().unwrap();
        if let Some(state) = reg.get_mut(name) {
            state.call_count += 1;
            let count = state.call_count;
            let mode = state.mode.clone();
            drop(reg);
            return fire(name, &mode, count);
        }
        Ok(())
    }
}

#[cfg(feature = "failpoints")]
fn fire(name: &str, mode: &FailureMode, call_count: usize) -> std::io::Result<()> {
    match mode {
        FailureMode::Panic => panic!("failpoint '{name}' triggered (panic)"),
        FailureMode::IoError(msg) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("failpoint '{name}': {msg}"),
        )),
        FailureMode::OnNthCall { n, mode } => {
            if call_count == *n {
                fire(name, mode, call_count)
            } else {
                Ok(())
            }
        }
    }
}
