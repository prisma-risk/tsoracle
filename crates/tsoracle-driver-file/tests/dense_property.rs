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
use std::sync::Arc;

use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::{Epoch, SeqKey};
use tsoracle_driver_file::FileDriver;

// Interleaved concurrent advances across keys must tile each key's space
// contiguously: the union of all returned blocks for a key equals [0, total)
// with no gap and no overlap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_advances_tile_contiguously() {
    let dir = tempfile::tempdir().unwrap();
    let d = FileDriver::open_or_init(dir.path()).unwrap();

    let keys = ["orders", "users", "invoices"];
    let mut handles = Vec::new();
    for k in keys {
        for _ in 0..20 {
            let d = Arc::clone(&d);
            handles.push(tokio::spawn(async move {
                let key = SeqKey::try_new(k).unwrap();
                let start = d.advance_dense(&key, 3, Epoch(1)).await.unwrap();
                (k, start, 3u32)
            }));
        }
    }

    let mut blocks: BTreeMap<&str, Vec<(u64, u32)>> = BTreeMap::new();
    for h in handles {
        let (k, start, count) = h.await.unwrap();
        blocks.entry(k).or_default().push((start, count));
    }

    for k in keys {
        let mut v = blocks.remove(k).unwrap();
        v.sort_by_key(|(s, _)| *s);
        // Tiles [0, 60): each block starts exactly where the previous ended.
        let mut expected = 0u64;
        for (start, count) in v {
            assert_eq!(start, expected, "gap or overlap in key {k}");
            expected += u64::from(count);
        }
        assert_eq!(expected, 60, "wrong total for key {k}");
        assert_eq!(d_load(&d, k).await, 60);
    }
}

async fn d_load(d: &FileDriver, k: &str) -> u64 {
    d.load_dense_seq(&SeqKey::try_new(k).unwrap())
        .await
        .unwrap()
}
