//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Leader-state stream derived from `Raft::metrics()`.
//!
//! [`leadership_events`] converts openraft's metrics watch into a stream of
//! [`LeadershipState`] transitions. Every change in role, term, or
//! follower-leader identity is emitted; consecutive *identical* projections
//! are suppressed so steady-state metric ticks don't produce a stream of
//! duplicate events.
//!
//! Dedup is full-value (`LeadershipState<C>: PartialEq`), not role-class only.
//! Class-only dedup is unsafe here: the underlying watch coalesces between
//! polls, so a `Leader(N) → Follower(M) → Leader(K)` sequence that lands
//! during a slow poll would be silently swallowed (both endpoints are
//! "Leader"), and the downstream failover fence — which only runs on
//! `Leader` events — would miss the new term. See issue #77.
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

/// Build a stream of `LeadershipState` transitions from a raft instance.
///
/// The returned stream:
///
/// 1. Emits one initial `LeadershipState` derived from the current metrics
///    value (always — even on first poll).
/// 2. Emits subsequent values whenever the projection differs from the last
///    emitted one (any change in role, term, or follower-leader identity).
///    Identical projections are suppressed.
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
    // (receiver, last-emitted projection) carried across unfold iterations.
    // `last` = None means "haven't emitted yet"; the next state always emits.
    let init: (
        WatchReceiverOf<C, RaftMetrics<C>>,
        Option<LeadershipState<C>>,
    ) = (rx, None);

    futures::stream::unfold(init, |(mut rx, mut last)| async move {
        loop {
            // Read the current value first. On first iteration `last` is None
            // so the freshly-borrowed projection is emitted unconditionally.
            // On later iterations we compare the full projection and skip
            // duplicates — class-only dedup would silently swallow term
            // changes coalesced through an intermediate Follower (see #77).
            let projected: LeadershipState<C> = {
                let snap = rx.borrow_watched();
                project_state::<C>(&snap)
                // `snap` (the lock guard) is dropped here, before any `.await`.
            };

            if last.as_ref().map(|l| l != &projected).unwrap_or(true) {
                last = Some(projected.clone());
                return Some((projected, (rx, last)));
            }

            // Same projection as last emit — wait for the next change.
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
    // `(&NodeId, &Node)`.
    m.membership_config
        .nodes()
        .find(|(id, _)| *id == &leader_id)
        .map(|(id, node)| (id.clone(), node.clone()))
}
