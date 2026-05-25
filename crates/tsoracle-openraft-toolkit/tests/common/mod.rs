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

//! Shared test fixtures used by integration tests across the crate.

use std::fmt;

use serde::{Deserialize, Serialize};
use tsoracle_openraft_toolkit::declare_raft_types_ext;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestPeer {
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestAppData {
    pub bytes: Vec<u8>,
}

impl fmt::Display for TestAppData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestAppData({} bytes)", self.bytes.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestAppliedState;

declare_raft_types_ext! {
    pub TestTypeConfig:
        Node            = TestPeer,
        AppData         = TestAppData,
        AppDataResponse = TestAppliedState,
        SnapshotData    = std::io::Cursor<Vec<u8>>,
}

/// Concrete `LeaderId` / `CommittedLeaderId` type used by `TestTypeConfig`.
///
/// `declare_raft_types_ext!` pins `LeaderId = leader_id_adv::LeaderId<Term, NodeId>`;
/// matching the macro's choice keeps test type-hints aligned with the macro's defaults.
#[allow(dead_code)]
pub type TestLeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
