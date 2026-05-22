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

//! OmniPaxos lifecycle: tick task, outbound drain, leader-event emission.

pub mod events;
pub mod state;

pub use events::{LeaderEventSender, LeaderEventStream, SendError, leader_event_channel};
pub use state::{LeadershipState, PeerEntry};

use std::sync::Arc;
use std::time::Duration;

use omnipaxos::OmniPaxos;
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Storage};
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, warn};
use tsoracle_core::Epoch;

#[async_trait::async_trait]
pub trait MessageSink<T: Entry>: Send + Sync + 'static {
    async fn send(&self, message: Message<T>);
}

pub struct PaxosRunner<T, S>
where
    T: Entry + Send + 'static,
    S: Storage<T> + Send + 'static,
{
    omnipaxos: Arc<Mutex<OmniPaxos<T, S>>>,
    my_node_id: u64,
    peers: Vec<PeerEntry>,
    tick_interval: Duration,
    leader_sender: LeaderEventSender,
    leader_stream: Option<LeaderEventStream>,
    apply_notify: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl<T, S> PaxosRunner<T, S>
where
    T: Entry + Send + 'static,
    S: Storage<T> + Send + 'static,
{
    pub fn new(
        omnipaxos: Arc<Mutex<OmniPaxos<T, S>>>,
        my_node_id: u64,
        peers: Vec<PeerEntry>,
        tick_interval: Duration,
    ) -> Self {
        let (leader_sender, leader_stream) = leader_event_channel();
        Self {
            omnipaxos,
            my_node_id,
            peers,
            tick_interval,
            leader_sender,
            leader_stream: Some(leader_stream),
            apply_notify: Arc::new(Notify::new()),
            handle: None,
            shutdown_tx: None,
        }
    }

    /// Take ownership of the leader-event stream. Returns `None` if already taken.
    #[must_use]
    pub fn take_leader_stream(&mut self) -> Option<LeaderEventStream> {
        self.leader_stream.take()
    }

    /// Notification that fires once per tick after outbound messages have
    /// been drained. External apply tasks (driver crate) await this so they
    /// can pull decided entries opportunistically rather than polling.
    #[must_use]
    pub fn apply_notify(&self) -> Arc<Notify> {
        self.apply_notify.clone()
    }

    #[must_use]
    pub fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<T, S>>> {
        self.omnipaxos.clone()
    }

    pub fn start<Sink: MessageSink<T>>(&mut self, sink: Arc<Sink>)
    where
        <T as Entry>::Snapshot: Send,
    {
        let omnipaxos = self.omnipaxos.clone();
        let my_node_id = self.my_node_id;
        let peers = self.peers.clone();
        let tick_interval = self.tick_interval;
        let leader_sender = self.leader_sender.clone();
        let apply_notify = self.apply_notify.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let handle = tokio::spawn(async move {
            let mut ticker = interval(tick_interval);
            // Locally-tracked leader observation + monotonic counter for the
            // epoch placeholder (see the runner module's doc).
            let mut last_observed_leader: Option<u64> = None;
            let mut leader_change_counter: u64 = 0;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // 1. Tick + drain in a short critical section, then
                        //    drop the guard before any await.
                        let outgoing = {
                            let mut op = omnipaxos.lock();
                            op.tick();
                            op.outgoing_messages()
                        };

                        // 2. Send outbound messages with the lock released.
                        for message in outgoing {
                            sink.send(message).await;
                        }

                        // 3. Observe leadership.
                        //
                        //    KNOWN LIMITATION (resolved in Plan 2): the
                        //    counter-derived epoch does NOT match the
                        //    spec's fencing strategy in
                        //    persist_high_water(at_least, epoch), which
                        //    compares epoch == encode_epoch(promise). A
                        //    leader who passes its own epoch back to
                        //    persist would fail the fence check. Plan 2's
                        //    driver crate replaces this leader event
                        //    stream with one that derives epoch from
                        //    omnipaxos.get_promise().
                        let leader_pid: Option<u64> = {
                            let op = omnipaxos.lock();
                            op.get_current_leader()
                        };
                        if leader_pid != last_observed_leader {
                            last_observed_leader = leader_pid;
                            if leader_pid.is_some() {
                                leader_change_counter = leader_change_counter.wrapping_add(1);
                            }
                        }
                        let epoch = leader_pid.map(|_| Epoch(leader_change_counter));
                        let state = LeadershipState::from_omnipaxos(
                            my_node_id, leader_pid, epoch, &peers,
                        );
                        if let Err(err) = leader_sender.send(state.to_consensus()) {
                            warn!(error = %err, "leader event channel closed");
                            break;
                        }

                        // 4. Wake the apply task in case decided_idx advanced.
                        apply_notify.notify_waiters();
                    }
                    _ = &mut shutdown_rx => {
                        debug!("paxos runner received shutdown");
                        break;
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl<T, S> Drop for PaxosRunner<T, S>
where
    T: Entry + Send + 'static,
    S: Storage<T> + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real runner-level integration test belongs in tests/lifecycle.rs
    // (lands in #162) where the in-memory test fakes (MemNetwork,
    // MemStorage) are wired up. Here we only confirm the public API
    // compiles correctly.
    #[allow(dead_code)]
    fn assert_runner_api_compiles<T, S>(_runner: PaxosRunner<T, S>)
    where
        T: omnipaxos::storage::Entry + Send + 'static,
        S: omnipaxos::storage::Storage<T> + Send + 'static,
    {
    }
}
