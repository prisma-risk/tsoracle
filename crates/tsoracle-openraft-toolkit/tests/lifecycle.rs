//! Tests for the lifecycle helpers.
//!
//! Real cluster behavior is exercised by downstream consumers' integration
//! tests. The tests here are compile-time signature checks plus pure-function
//! assertions where possible.

use std::collections::BTreeMap;

use tsoracle_openraft_toolkit::BootstrapMode;

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
    let fut = async move { tsoracle_openraft_toolkit::bootstrap(raft, mode).await };
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
    let fut =
        async move { tsoracle_openraft_toolkit::change_membership(raft, voters, false).await };
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
    let fut = async move { tsoracle_openraft_toolkit::add_learner(raft, id, node, false).await };
    drop(fut);
}

// Compile-time signature check for `leadership_events`. Like the other shims,
// this never executes (we have no real `Raft<C, SM>` in a unit test) — its job
// is to break the build if alpha.20 ever shifts the metrics accessor's return
// type or the `Raft<C, SM>` shape.
#[allow(dead_code)]
fn _leadership_events_signature_compiles<C, SM>(
    raft: &openraft::Raft<C, SM>,
) -> impl futures::Stream<Item = tsoracle_openraft_toolkit::LeadershipState<C>>
where
    C: openraft::RaftTypeConfig,
    SM: openraft::storage::RaftStateMachine<C>,
{
    tsoracle_openraft_toolkit::leadership_events(raft)
}

#[tokio::test]
async fn leadership_events_emits_initial_state_and_terminates_on_drop() {
    use futures::StreamExt;
    use openraft::RaftMetrics;
    use openraft::type_config::TypeConfigExt;
    use tsoracle_openraft_toolkit::LeadershipState;

    // Construct a `RaftMetrics<TestTypeConfig>` via the public `new_initial`
    // constructor — `Default` isn't implemented on alpha.20's `RaftMetrics`.
    // `new_initial` produces a Follower-state snapshot with `current_term = 0`
    // and no current leader.
    let metrics: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);

    // Use the type config's own watch channel so the receiver matches the
    // runtime-abstracted alias `WatchReceiverOf<C, RaftMetrics<C>>` exactly.
    let (tx, rx) = <TestTypeConfig as TypeConfigExt>::watch_channel(metrics);

    let mut stream = std::pin::pin!(
        tsoracle_openraft_toolkit::lifecycle::leader::stream_from_receiver::<TestTypeConfig>(rx)
    );

    // Initial state emits unconditionally.
    let first = stream.next().await.expect("initial state emitted");
    assert!(
        matches!(
            first,
            LeadershipState::Follower {
                term: 0,
                leader: None
            }
        ),
        "expected initial Follower {{ term: 0, leader: None }}; got {first:?}",
    );

    // Dropping the sender terminates the stream after any in-flight wait.
    drop(tx);
    assert!(
        stream.next().await.is_none(),
        "stream should terminate when sender drops"
    );
}

#[tokio::test]
async fn leadership_events_dedups_repeated_class_until_transition() {
    use futures::StreamExt;
    use openraft::RaftMetrics;
    use openraft::ServerState;
    use openraft::WatchSender;
    use openraft::type_config::TypeConfigExt;
    use tsoracle_openraft_toolkit::LeadershipState;

    // Start in Follower.
    let initial: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);
    let (tx, rx) = <TestTypeConfig as TypeConfigExt>::watch_channel(initial);

    let mut stream = std::pin::pin!(
        tsoracle_openraft_toolkit::lifecycle::leader::stream_from_receiver::<TestTypeConfig>(rx)
    );

    // First poll yields the initial Follower.
    let first = stream.next().await.expect("initial");
    assert!(
        matches!(first, LeadershipState::Follower { .. }),
        "got {first:?}"
    );

    // Send another Follower-class metrics value — must be swallowed by dedup.
    // We schedule a Leader transition right after so the next stream poll has
    // something to surface, and confirm the in-between Follower update was not
    // emitted as its own item.
    let mut next_follower: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);
    next_follower.current_term = 1;
    tx.send(next_follower).unwrap();

    let mut leader_metrics: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);
    leader_metrics.state = ServerState::Leader;
    leader_metrics.current_term = 1;
    tx.send(leader_metrics).unwrap();

    let next = stream.next().await.expect("transition");
    assert!(
        matches!(next, LeadershipState::Leader { term: 1 }),
        "expected Leader {{ term: 1 }}; got {next:?}",
    );

    drop(tx);
    assert!(stream.next().await.is_none());
}

// Walks the role-class projections that the Follower→Leader test doesn't touch:
// Candidate, Learner, Shutdown. Each is sent as the *initial* watch value so the
// dedup logic emits it unconditionally on first poll; this keeps the test
// straight-line rather than threading a multi-step transition through one
// sender.
#[tokio::test]
async fn leadership_events_projects_candidate_learner_and_shutdown() {
    use futures::StreamExt;
    use openraft::RaftMetrics;
    use openraft::ServerState;
    use openraft::type_config::TypeConfigExt;
    use tsoracle_openraft_toolkit::LeadershipState;

    async fn first_emission(state: ServerState, term: u64) -> LeadershipState<TestTypeConfig> {
        let mut metrics: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);
        metrics.state = state;
        metrics.current_term = term;
        let (_tx, rx) = <TestTypeConfig as TypeConfigExt>::watch_channel(metrics);
        let mut stream = std::pin::pin!(
            tsoracle_openraft_toolkit::lifecycle::leader::stream_from_receiver::<TestTypeConfig>(
                rx
            )
        );
        stream.next().await.expect("initial state emitted")
    }

    assert!(matches!(
        first_emission(ServerState::Candidate, 5).await,
        LeadershipState::Candidate { term: 5 }
    ));
    assert!(matches!(
        first_emission(ServerState::Learner, 9).await,
        LeadershipState::Learner
    ));
    assert!(matches!(
        first_emission(ServerState::Shutdown, 2).await,
        LeadershipState::Shutdown
    ));
}

// Drives `resolve_leader`'s populated-membership branch: when the metrics report
// a `current_leader` that exists in `membership_config.nodes()`, the Follower
// projection carries the resolved `(id, node)` pair. Without this, only the
// `current_leader = None` path (covered by the initial-state test above) gets
// exercised.
#[tokio::test]
async fn leadership_events_resolves_follower_leader_when_in_membership() {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use futures::StreamExt;
    use openraft::Membership;
    use openraft::RaftMetrics;
    use openraft::StoredMembership;
    use openraft::type_config::TypeConfigExt;
    use openraft::type_config::alias::StoredMembershipOf;
    use tsoracle_openraft_toolkit::LeadershipState;

    let peer_2 = TestPeer {
        addr: "host-2:9000".into(),
    };
    let nodes: BTreeMap<u64, TestPeer> = BTreeMap::from([
        (
            1,
            TestPeer {
                addr: "host-1:9000".into(),
            },
        ),
        (2, peer_2.clone()),
    ]);
    let membership: Membership<u64, TestPeer> =
        Membership::new(vec![BTreeSet::from([1u64, 2])], nodes).unwrap();
    let stored: StoredMembershipOf<TestTypeConfig> = StoredMembership::new(None, membership);

    let mut metrics: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);
    metrics.current_leader = Some(2);
    metrics.membership_config = Arc::new(stored);

    let (_tx, rx) = <TestTypeConfig as TypeConfigExt>::watch_channel(metrics);
    let mut stream = std::pin::pin!(
        tsoracle_openraft_toolkit::lifecycle::leader::stream_from_receiver::<TestTypeConfig>(rx)
    );

    let first = stream.next().await.expect("initial state emitted");
    match first {
        LeadershipState::Follower {
            term: 0,
            leader: Some((id, node)),
        } => {
            assert_eq!(id, 2);
            assert_eq!(node, peer_2);
        }
        other => panic!("expected Follower with resolved leader; got {other:?}"),
    }
}

// Counterpart to the test above: when `current_leader` is set but the node id
// isn't present in `membership_config.nodes()` (e.g. the leader was just
// removed from the config), `resolve_leader` returns `None` and the Follower
// projection carries `leader: None`. This exercises `find(...).map(...)`'s
// no-match arm.
#[tokio::test]
async fn leadership_events_drops_follower_leader_when_not_in_membership() {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use futures::StreamExt;
    use openraft::Membership;
    use openraft::RaftMetrics;
    use openraft::StoredMembership;
    use openraft::type_config::TypeConfigExt;
    use openraft::type_config::alias::StoredMembershipOf;
    use tsoracle_openraft_toolkit::LeadershipState;

    let nodes: BTreeMap<u64, TestPeer> = BTreeMap::from([(
        1,
        TestPeer {
            addr: "host-1:9000".into(),
        },
    )]);
    let membership: Membership<u64, TestPeer> =
        Membership::new(vec![BTreeSet::from([1u64])], nodes).unwrap();
    let stored: StoredMembershipOf<TestTypeConfig> = StoredMembership::new(None, membership);

    let mut metrics: RaftMetrics<TestTypeConfig> = RaftMetrics::new_initial(1u64);
    // 99 is intentionally absent from the membership above.
    metrics.current_leader = Some(99);
    metrics.membership_config = Arc::new(stored);

    let (_tx, rx) = <TestTypeConfig as TypeConfigExt>::watch_channel(metrics);
    let mut stream = std::pin::pin!(
        tsoracle_openraft_toolkit::lifecycle::leader::stream_from_receiver::<TestTypeConfig>(rx)
    );

    let first = stream.next().await.expect("initial state emitted");
    assert!(
        matches!(
            first,
            LeadershipState::Follower {
                term: 0,
                leader: None
            }
        ),
        "expected Follower with no resolved leader; got {first:?}",
    );
}
