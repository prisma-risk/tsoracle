//! Shared test fixtures used by integration tests across the crate.

use std::fmt;

use openraft_toolkit::declare_raft_types_ext;
use serde::{Deserialize, Serialize};

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
