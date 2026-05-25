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

//! Smoke test for `declare_raft_types_ext!`: the macro must produce a type
//! that satisfies `openraft::RaftTypeConfig` with the minimum-required four
//! slots filled in.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;
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

// Same config with every optional override exercised, to confirm the override
// arms parse and the inner helper macros emit valid type expressions.
declare_raft_types_ext! {
    pub TestTypeConfigFull:
        NodeId          = u64,
        Node            = TestPeer,
        Entry           = openraft::Entry<
            <Self::LeaderId as openraft::vote::RaftLeaderId>::Committed,
            Self::D,
            Self::NodeId,
            Self::Node,
        >,
        AppData         = TestAppData,
        AppDataResponse = TestAppliedState,
        SnapshotData    = std::io::Cursor<Vec<u8>>,
        AsyncRuntime    = openraft::impls::TokioRuntime,
        Responder       = openraft::impls::OneshotResponder<Self, T>,
}

#[test]
fn type_config_compiles() {
    fn assert_impls<C: openraft::RaftTypeConfig>() {}
    assert_impls::<TestTypeConfig>();
    assert_impls::<TestTypeConfigFull>();
}
