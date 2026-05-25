use std::path::Path;
use std::sync::Arc;

use tsoracle_consensus::ConsensusDriver;
use tsoracle_driver_file::FileDriver;

use crate::config::FileConfig;
use crate::error::StandaloneError;
use crate::{Standalone, TransportHandle};

/// One-shot seeded initialization for the file driver (migration setup).
pub fn init_file_seeded(state_dir: &Path, seed_physical_ms: u64) -> Result<(), StandaloneError> {
    FileDriver::init_seeded(state_dir, seed_physical_ms).map_err(|source| {
        StandaloneError::Storage {
            path: state_dir.to_path_buf(),
            source: Box::new(source),
        }
    })
}

pub(crate) fn build_file(cfg: FileConfig) -> Result<Standalone, StandaloneError> {
    let driver =
        FileDriver::open_or_init(&cfg.state_dir).map_err(|source| StandaloneError::Storage {
            path: cfg.state_dir.clone(),
            source: Box::new(source),
        })?;
    Ok(Standalone {
        driver: driver as Arc<dyn ConsensusDriver>,
        transport: TransportHandle::noop(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_file_opens_a_driver_in_a_fresh_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = FileConfig {
            state_dir: dir.path().join("state"),
        };
        let std_node = build_file(cfg).expect("build file driver");
        // load_high_water is the cheapest ConsensusDriver call to prove it's live.
        let hw = std_node.driver.load_high_water().await.unwrap();
        assert_eq!(hw, 0);
    }
}
