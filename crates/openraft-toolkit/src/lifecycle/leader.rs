//! Leader-state stream derived from `Raft::metrics()`.
//!
//! [`leadership_events`] converts openraft's metrics watch into a stream of
//! [`LeadershipState`] transitions, emitting only when the role class changes
//! (Leader → Follower, Follower → Candidate, etc.). Term updates and
//! current-leader changes that don't change the role class are swallowed so
//! consumers don't see one event per internal raft tick.
//!
//! The metrics watch in alpha.20 is `WatchReceiverOf<C, RaftMetrics<C>>` — a
//! runtime-abstracted receiver, not a plain `tokio::sync::watch::Receiver`. The
//! stream is therefore built by hand on top of `WatchReceiver::changed` /
//! `borrow_watched` rather than via `tokio_stream::wrappers::WatchStream`.
//!
//! The very first state observed on the channel is emitted unconditionally; the
//! dedup logic only kicks in for subsequent values.

use futures::Stream;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::{Raft, RaftMetrics, RaftTypeConfig, ServerState};

/// A coarse projection of openraft's `RaftMetrics::state` (plus the few fields
/// downstream consumers need to act on each role transition).
///
/// `Leader::term` carries the term in which this node became leader.
/// `Follower::leader` is the resolved `(NodeId, Node)` pair for the leader this
/// node is currently following, or `None` if no leader is yet known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipState<C: RaftTypeConfig> {
    Leader {
        term: u64,
    },
    Follower {
        term: u64,
        leader: Option<(C::NodeId, C::Node)>,
    },
    Candidate {
        term: u64,
    },
    Learner,
    Shutdown,
}

/// Role-class discriminant used for dedup. Two consecutive projections compare
/// equal when their classes match; the inner fields (term, leader hint) are
/// allowed to drift without re-emitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleClass {
    Leader,
    Follower,
    Candidate,
    Learner,
    Shutdown,
}

impl<C: RaftTypeConfig> LeadershipState<C> {
    fn class(&self) -> RoleClass {
        match self {
            LeadershipState::Leader { .. } => RoleClass::Leader,
            LeadershipState::Follower { .. } => RoleClass::Follower,
            LeadershipState::Candidate { .. } => RoleClass::Candidate,
            LeadershipState::Learner => RoleClass::Learner,
            LeadershipState::Shutdown => RoleClass::Shutdown,
        }
    }
}

/// Build a stream of `LeadershipState` transitions from a raft instance.
///
/// The returned stream:
///
/// 1. Emits one initial `LeadershipState` derived from the current metrics
///    value (always — even on first poll).
/// 2. Emits subsequent values only when the role class changes.
/// 3. Terminates when openraft drops its sender (i.e., the raft has shut
///    down).
pub fn leadership_events<C, SM>(raft: &Raft<C, SM>) -> impl Stream<Item = LeadershipState<C>>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    stream_from_receiver::<C>(raft.metrics())
}

/// Internal: build the dedup stream from a constructed receiver without going
/// through `Raft<C, SM>`. Exposed (with `#[doc(hidden)]`) so the crate's
/// integration tests can drive the dedup logic with a synthetic watch channel;
/// not part of the public API and may change without notice.
#[doc(hidden)]
pub fn stream_from_receiver<C: RaftTypeConfig>(
    rx: WatchReceiverOf<C, RaftMetrics<C>>,
) -> impl Stream<Item = LeadershipState<C>> {
    // (receiver, last-emitted class) carried across unfold iterations.
    // `last` = None means "haven't emitted yet"; the next state always emits.
    let init: (WatchReceiverOf<C, RaftMetrics<C>>, Option<RoleClass>) = (rx, None);

    futures::stream::unfold(init, |(mut rx, mut last)| async move {
        loop {
            // Read the current value first. On first iteration `last` is None
            // so the freshly-borrowed projection is emitted unconditionally.
            // On later iterations we compare classes and skip duplicates.
            let projected: LeadershipState<C> = {
                let snap = rx.borrow_watched();
                project_state::<C>(&snap)
                // `snap` (the lock guard) is dropped here, before any `.await`.
            };

            let class = projected.class();
            if last.map(|l| l != class).unwrap_or(true) {
                last = Some(class);
                return Some((projected, (rx, last)));
            }

            // Same class as last emit — wait for the next change.
            if rx.changed().await.is_err() {
                // Sender dropped (raft shut down). Terminate the stream.
                return None;
            }
        }
    })
}

fn project_state<C: RaftTypeConfig>(m: &RaftMetrics<C>) -> LeadershipState<C> {
    // `RaftTerm::as_u64` returns `None` only for term types that can't
    // represent themselves as a u64. None of our configurations use such a
    // term; default to 0 if a custom term ever does.
    use openraft::vote::RaftTerm;
    let term = m.current_term.as_u64().unwrap_or(0);

    match m.state {
        ServerState::Leader => LeadershipState::Leader { term },
        ServerState::Candidate => LeadershipState::Candidate { term },
        ServerState::Follower => LeadershipState::Follower {
            term,
            leader: resolve_leader::<C>(m),
        },
        ServerState::Learner => LeadershipState::Learner,
        ServerState::Shutdown => LeadershipState::Shutdown,
    }
}

fn resolve_leader<C: RaftTypeConfig>(m: &RaftMetrics<C>) -> Option<(C::NodeId, C::Node)> {
    let leader_id = m.current_leader.clone()?;
    // `membership_config` is `Arc<StoredMembership<...>>`; `nodes()` yields
    // `(&NodeId, &Node)`. Match openraft's lookup style verbatim (see
    // `placement-driver::pd_handle::forward_to_leader_from_metrics`).
    m.membership_config
        .nodes()
        .find(|(id, _)| *id == &leader_id)
        .map(|(id, node)| (id.clone(), node.clone()))
}
