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

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::{Epoch, SeqKey};

pub async fn populate(driver: &impl ConsensusDriver) {
    driver.persist_high_water(500, Epoch::ZERO).await.unwrap();
    driver
        .advance_dense(&SeqKey::try_new("orders").unwrap(), 7, Epoch::ZERO)
        .await
        .unwrap();
    driver.persist_leases(&[], Epoch::ZERO).await.unwrap();
}

#[allow(dead_code)]
pub fn snapshot(dir: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
    fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name();
            // Windows excludes reads from another handle while the sentinel is locked.
            let bytes = if name == "LOCK" && entry.metadata().unwrap().len() == 0 {
                Vec::new()
            } else {
                fs::read(entry.path()).unwrap()
            };
            (name, bytes)
        })
        .collect()
}
