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
