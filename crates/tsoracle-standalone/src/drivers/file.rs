//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::path::Path;
use std::sync::Arc;

use tsoracle_consensus::ConsensusDriver;
use tsoracle_driver_file::FileDriver;

use crate::config::FileConfig;
use crate::error::StandaloneError;
use crate::{FatalSignal, Standalone, TransportHandle};

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
        drain: None,
        admin: std::sync::Arc::new(crate::admin::UnsupportedAdmin::new(
            crate::admin::MembershipView {
                members: Vec::new(),
                leader: None,
            },
        )),
        admin_transport: crate::TransportHandle::noop(),
        admin_listen_addr: None,
        // No transport servers, so nothing can ever trip it.
        fatal: FatalSignal::new(),
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

    /// Seeding initializes durable state at the requested physical floor; a
    /// driver opened on the seeded dir reads back that high-water. The seed is
    /// a `physical_ms` and is reported in the same units (not a packed
    /// `Timestamp`), matching `FileDriver::init_seeded`'s own contract.
    #[tokio::test]
    async fn init_file_seeded_writes_a_physical_floor() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("seeded");
        let seed_ms = 1_700_000_000_000;
        init_file_seeded(&state_dir, seed_ms).expect("seed file state");

        let std_node = build_file(FileConfig {
            state_dir: state_dir.clone(),
        })
        .expect("open seeded driver");
        let hw = std_node.driver.load_high_water().await.unwrap();
        assert_eq!(
            hw, seed_ms,
            "seeded high-water should be the physical_ms floor"
        );
    }

    /// Seeding over already-initialized state is rejected, and the underlying
    /// driver error is wrapped as `StandaloneError::Storage`.
    #[tokio::test]
    async fn init_file_seeded_on_existing_state_is_a_storage_error() {
        let dir = tempfile::tempdir().unwrap();
        init_file_seeded(dir.path(), 1_700_000_000_000).expect("first seed");
        match init_file_seeded(dir.path(), 1_700_000_000_000) {
            Err(StandaloneError::Storage { .. }) => {}
            other => panic!("expected Storage error on re-seed, got {other:?}"),
        }
    }
}
