//! Translate openraft metrics into tsoracle [`LeaderState`].
//!
//! [`spawn`] starts a background task that reads the openraft metrics watch channel,
//! maps each update to a [`LeaderState`], and writes the result to a new watch channel
//! whose receiver is returned to the caller (typically [`super::driver::OpenraftDriver`]).
//!
//! Debounce policy: emit only when the [`LeaderState`] value changes by *equality*,
//! not just by variant. A variant-only check would silently drop:
//!   - term advances within a leadership streak (`Leader { epoch: 5 } → Leader { epoch: 7 }`),
//!     which the server uses to re-fence at the new epoch;
//!   - follower-side leader-endpoint changes (`Follower { None } → Follower { Some(addr) }`,
//!     or one leader replaced by another while we keep following), which clients rely on
//!     to receive an up-to-date `LeaderHint` redirect.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::async_runtime::WatchReceiver as _;
use openraft::{Raft, ServerState};
use tokio::sync::watch;
use tsoracle_consensus::LeaderState;
use tsoracle_core::Epoch;

use crate::types::{NodeId, TypeConfig};

/// Spawn the leader-watch task.
///
/// The returned [`watch::Receiver<LeaderState>`] is initialized to [`LeaderState::Unknown`]
/// and updated whenever the raft node's server state changes.
///
/// # Parameters
///
/// * `raft`      – handle to the local openraft instance; used only to obtain its metrics receiver.
/// * `tso_peers` – map of `NodeId → tsoracle service address`, used to populate
///   [`LeaderState::Follower::leader_endpoint`] when the local node is following a
///   known leader.
pub fn spawn<SM: Send + 'static>(
    raft: Raft<TypeConfig, SM>,
    tso_peers: Arc<HashMap<NodeId, String>>,
) -> watch::Receiver<LeaderState> {
    let (tx, rx) = watch::channel(LeaderState::Unknown);

    tokio::spawn(async move {
        // `raft.metrics()` returns an openraft `WatchReceiverOf<TypeConfig, RaftMetrics<TypeConfig>>`
        // which is a `TokioWatchReceiver<RaftMetrics<TypeConfig>>` wrapping a tokio watch channel.
        let mut metrics_rx = raft.metrics();

        // Last value successfully sent. `None` means we have only the initial
        // `LeaderState::Unknown` published by the watch channel and have not
        // explicitly sent anything yet.
        let mut last_sent: Option<LeaderState> = None;

        loop {
            // --- Read the current metrics snapshot ---
            // `borrow_watched()` returns a reference that derefs to `RaftMetrics<TypeConfig>`.
            // We clone immediately so the borrow guard is released before the `.await`.
            let metrics = metrics_rx.borrow_watched().clone();

            // --- Map to LeaderState ---
            let next = match metrics.state {
                ServerState::Leader => LeaderState::Leader {
                    // `current_term` is `u64` for our TypeConfig (default set by
                    // `declare_raft_types!`).
                    epoch: Epoch(metrics.current_term),
                },

                ServerState::Follower | ServerState::Learner => {
                    let endpoint = metrics
                        .current_leader
                        .and_then(|id| tso_peers.get(&id).cloned());
                    LeaderState::Follower {
                        leader_endpoint: endpoint,
                    }
                }

                // Candidate or Shutdown: we have no usable leader information.
                _ => LeaderState::Unknown,
            };

            // --- Debounce by value, not by variant ---
            // Suppresses no-op metric ticks (vanilla heartbeat updates that
            // don't change the LeaderState payload) while still propagating
            // term advances and leader-endpoint changes.
            if last_sent.as_ref() != Some(&next) {
                if tx.send(next.clone()).is_err() {
                    // All receivers have been dropped; shut down the task.
                    break;
                }
                last_sent = Some(next);
            }

            // --- Wait for the next metrics update ---
            // `changed()` blocks until the sender emits a new value, then marks it as seen.
            // Returns `Err` when the sender half of the openraft metrics channel is dropped
            // (i.e., the raft instance has shut down).
            if metrics_rx.changed().await.is_err() {
                break;
            }
        }
    });

    rx
}
