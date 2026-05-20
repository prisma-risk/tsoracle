//! Type configuration for our openraft instance.

use std::fmt;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Node = BasicNode;

/// Request carried through the raft log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TsoExtend {
    pub at_least: u64,
    pub epoch: u64,
}

impl fmt::Display for TsoExtend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TsoExtend {{ at_least: {}, epoch: {} }}",
            self.at_least, self.epoch
        )
    }
}

/// Response returned from state-machine apply.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TsoExtendResp {
    pub persisted: u64,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D      = TsoExtend,
        R      = TsoExtendResp,
        NodeId = NodeId,
        Node   = Node,
);
