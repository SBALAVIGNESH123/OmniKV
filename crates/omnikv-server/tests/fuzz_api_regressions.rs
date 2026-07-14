use omnikv_server::api_contracts::{BatchRequest, ScanQuery, SetRequest, TokenRequest};
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("server crate should live below repo root")
        .to_path_buf()
}

fn api_json_corpus() -> Vec<Vec<u8>> {
    let mut bytes = Vec::new();
    for kind in ["corpus", "regressions"] {
        let dir = repo_root().join("fuzz").join(kind).join("api_json");
        if !dir.exists() {
            continue;
        }
        for entry in std::fs::read_dir(&dir).expect("read API fuzz corpus dir") {
            let path = entry.expect("read API fuzz corpus entry").path();
            if path.is_file() {
                bytes.push(std::fs::read(&path).unwrap_or_else(|err| {
                    panic!("read {}: {err}", path.display());
                }));
            }
        }
    }
    assert!(!bytes.is_empty(), "expected checked-in API JSON corpus");
    bytes
}

#[test]
fn api_request_json_corpus_does_not_panic() {
    for bytes in api_json_corpus() {
        let _ = serde_json::from_slice::<SetRequest>(&bytes);
        let _ = serde_json::from_slice::<BatchRequest>(&bytes);
        let _ = serde_json::from_slice::<ScanQuery>(&bytes);
        let _ = serde_json::from_slice::<TokenRequest>(&bytes);
        let _ = serde_json::from_slice::<serde_json::Value>(&bytes);
    }
}
