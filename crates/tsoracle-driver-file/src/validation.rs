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

use tsoracle_core::{LeaseRecord, MAX_LEASE_HOLDER_LEN, PHYSICAL_MS_MAX, SeqKey};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StoredStateError {
    #[error("reservation high-water {0} exceeds the physical milliseconds range")]
    HighWater(u64),
    #[error("dense key count {count} exceeds cardinality cap {cap}")]
    DenseCardinality { count: u64, cap: u64 },
    #[error("invalid stored dense key {0:?}")]
    DenseKey(String),
    #[error("lease {lease_id} must satisfy lease_id <= upper_bound <= high_water")]
    LeaseBounds { lease_id: u64 },
    #[error("lease {lease_id} has an invalid holder or zero TTL")]
    LeaseFields { lease_id: u64 },
}

pub(crate) fn validate(
    high_water: u64,
    dense: &BTreeMap<String, u64>,
    cap: u64,
    leases: &[LeaseRecord],
) -> Result<(), StoredStateError> {
    if high_water > PHYSICAL_MS_MAX {
        return Err(StoredStateError::HighWater(high_water));
    }
    if dense.len() as u64 > cap {
        return Err(StoredStateError::DenseCardinality {
            count: dense.len() as u64,
            cap,
        });
    }
    for key in dense.keys() {
        if SeqKey::try_new(key.clone()).is_err() {
            return Err(StoredStateError::DenseKey(key.clone()));
        }
    }
    for lease in leases {
        if lease.lease_id > lease.ts_upper_bound || lease.ts_upper_bound > high_water {
            return Err(StoredStateError::LeaseBounds {
                lease_id: lease.lease_id,
            });
        }
        if lease.holder.is_empty() || lease.holder.len() > MAX_LEASE_HOLDER_LEN || lease.ttl_ms == 0
        {
            return Err(StoredStateError::LeaseFields {
                lease_id: lease.lease_id,
            });
        }
    }
    Ok(())
}
