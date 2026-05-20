//! Tests for the lifecycle helpers.
//!
//! Real cluster behavior is covered by the PD migration's integration tests
//! once the toolkit is wired up. The tests here are compile-time signature
//! checks plus pure-function assertions where possible.

use std::collections::BTreeMap;

use openraft_toolkit::BootstrapMode;

mod common;
use common::{TestPeer, TestTypeConfig};

// Verifies the public types compile in the shapes downstream consumers expect.
#[test]
fn bootstrap_mode_constructs_in_each_shape() {
    let mut members: BTreeMap<u64, TestPeer> = BTreeMap::new();
    members.insert(
        1,
        TestPeer {
            addr: "host-1:9000".into(),
        },
    );

    let _fresh: BootstrapMode<TestTypeConfig> = BootstrapMode::Fresh {
        initial_members: members,
    };
    let _reopen: BootstrapMode<TestTypeConfig> = BootstrapMode::Reopen;
    let _join: BootstrapMode<TestTypeConfig> = BootstrapMode::Join;
}

// Verifies the `bootstrap` function has the expected signature.
// Doesn't execute it — we have no Raft<...> instance in a unit test.
#[allow(dead_code)]
fn _bootstrap_signature_compiles<C, SM>(raft: &openraft::Raft<C, SM>, mode: BootstrapMode<C>)
where
    C: openraft::RaftTypeConfig,
    SM: openraft::storage::RaftStateMachine<C>,
{
    let fut = async move { openraft_toolkit::bootstrap(raft, mode).await };
    drop(fut);
}

// Same idea for `change_membership` / `add_learner`: keep a compile-time
// signature check so an openraft bump that shifts the argument shape breaks
// here rather than at the first downstream call site.
#[allow(dead_code)]
fn _change_membership_signature_compiles<C, SM>(
    raft: &openraft::Raft<C, SM>,
    voters: std::collections::BTreeSet<C::NodeId>,
) where
    C: openraft::RaftTypeConfig,
    SM: openraft::storage::RaftStateMachine<C>,
{
    let fut = async move { openraft_toolkit::change_membership(raft, voters, false).await };
    drop(fut);
}

#[allow(dead_code)]
fn _add_learner_signature_compiles<C, SM>(
    raft: &openraft::Raft<C, SM>,
    id: C::NodeId,
    node: C::Node,
) where
    C: openraft::RaftTypeConfig,
    SM: openraft::storage::RaftStateMachine<C>,
{
    let fut = async move { openraft_toolkit::add_learner(raft, id, node, false).await };
    drop(fut);
}
