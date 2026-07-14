#![no_main]

use libfuzzer_sys::fuzz_target;
use omnikv_server::api_contracts::{BatchRequest, ScanQuery, SetRequest, TokenRequest};

fuzz_target!(|data: &[u8]| {
    if data.len() > 32 * 1024 {
        return;
    }

    let _ = serde_json::from_slice::<SetRequest>(data);
    let _ = serde_json::from_slice::<BatchRequest>(data);
    let _ = serde_json::from_slice::<ScanQuery>(data);
    let _ = serde_json::from_slice::<TokenRequest>(data);
    let _ = serde_json::from_slice::<serde_json::Value>(data);
});
