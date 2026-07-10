//! Manifest — tracks the database topology on disk (heap path, SSTable list, max sequence).

use crate::record::OmniError;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Manifest {
    pub heap_path: String,
    pub base_path: String,
    pub sstables: Vec<String>,
    #[serde(default)]
    pub l1_sstables: Vec<String>,
    #[serde(default)]
    pub max_seq: u64,
}

impl Manifest {
    pub fn load(path: &str) -> Result<Self, OmniError> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(|e| OmniError::IoError(e.to_string()))
    }
    pub fn save(&self, path: &str) -> Result<(), OmniError> {
        let content = serde_json::to_string(self)
            .map_err(|e| OmniError::IoError(format!("Manifest serialize: {}", e)))?;
        let tmp_path = format!("{}.tmp", path);
        std::fs::write(&tmp_path, content)?;
        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&tmp_path) {
            let _ = file.sync_all();
        }
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}
