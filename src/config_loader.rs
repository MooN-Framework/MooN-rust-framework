use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct SystemConfig {
    pub system_id: u8,
    pub system_size: u8,
    pub min_sys_size: u8,
    pub timeout_ms: u16,
    pub port: String,
}

pub fn load_json_config<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> Result<T, String> {
    let path_ref = path.as_ref();

    let content = fs::read_to_string(path_ref)
        .map_err(|e| format!("Failed to read config file {:?}: {}", path_ref, e))?;

    serde_json::from_str::<T>(&content)
        .map_err(|e| format!("Failed to parse JSON config {:?}: {}", path_ref, e))
}
