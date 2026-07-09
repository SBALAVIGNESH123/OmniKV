//! Deterministic failure-injection harness for OmniKV crash testing.
//!
//! When the `failpoints` Cargo feature is disabled every public function
//! is a no-op and the entire registry is compiled away.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Action to take when a named fail-point is triggered.
#[derive(Clone, Debug)]
pub enum FailAction {
    /// Do nothing (default / disarmed state).
    NoOp,
    /// Panic with the given message.
    Panic(String),
    /// Return an error string to the call site.
    Error(String),
}

static REGISTRY: OnceLock<Mutex<HashMap<String, FailAction>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, FailAction>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arm a named fail-point.
pub fn set(name: &str, action: FailAction) {
    registry()
        .lock()
        .expect("failpoints registry poisoned")
        .insert(name.to_owned(), action);
}

/// Disarm a named fail-point.
pub fn clear(name: &str) {
    registry()
        .lock()
        .expect("failpoints registry poisoned")
        .remove(name);
}

/// Evaluate a fail-point.
///
/// Returns `Err` when the action is `Error`.
///
/// # Panics
/// Panics when the action is `Panic`.
pub fn eval(name: &str) -> Result<(), String> {
    let action = registry()
        .lock()
        .expect("failpoints registry poisoned")
        .get(name)
        .cloned()
        .unwrap_or(FailAction::NoOp);
    match action {
        FailAction::NoOp => Ok(()),
        FailAction::Panic(msg) => {
            panic!("failpoint '{name}' triggered: {msg}")
        }
        FailAction::Error(msg) => Err(msg),
    }
}

/// Disarm all registered fail-points.
pub fn clear_all() {
    registry()
        .lock()
        .expect("failpoints registry poisoned")
        .clear();
}
