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

//! Process-local in-memory transport for OmniPaxos cluster tests.
//!
//! Each registered node holds a tokio `mpsc::Receiver<Message<T>>`.
//! `deliver` routes to the destination's inbox, consulting a shared
//! [`PartitionController`] to drop messages for isolated endpoints.
//! Messages to unregistered nodes are silently dropped; a full channel
//! also drops (matches what a best-effort UDP-class transport would do).

use std::collections::HashMap;
use std::sync::Arc;

use omnipaxos::messages::Message;
use omnipaxos::storage::Entry;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::test_fakes::partition::PartitionController;

const CHANNEL_CAPACITY: usize = 1024;

pub struct MemNetwork<T: Entry> {
    senders: Mutex<HashMap<u64, mpsc::Sender<Message<T>>>>,
    partition: Arc<PartitionController>,
}

impl<T: Entry + Send + 'static> MemNetwork<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            partition: Arc::new(PartitionController::new()),
        }
    }

    /// Borrow the partition controller. Cluster harnesses isolate / heal
    /// via the returned `Arc` to inject network chaos.
    #[must_use]
    pub fn partition(&self) -> Arc<PartitionController> {
        self.partition.clone()
    }

    /// Register a receiving inbox for `node_id`. The returned receiver
    /// yields every message routed to this node id by [`Self::deliver`].
    /// Re-registering the same node id silently replaces the previous
    /// sender; the old receiver will yield no further messages.
    pub fn register(&self, node_id: u64) -> mpsc::Receiver<Message<T>> {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        self.senders.lock().insert(node_id, sender);
        receiver
    }

    /// Route a message to its destination's inbox.
    ///
    /// Drops the message silently if:
    /// - the partition controller has the source OR destination isolated,
    /// - the destination node has not been registered,
    /// - the destination's channel is full (best-effort delivery).
    pub async fn deliver(&self, message: Message<T>) {
        self.deliver_now(message);
    }

    /// Synchronous (non-`async`) sibling of [`Self::deliver`], for deterministic
    /// test stepping that routes messages without an executor. Delivery is
    /// already non-blocking (`try_send`), so this shares the same routing and
    /// drop semantics.
    pub fn deliver_now(&self, message: Message<T>) {
        let (from, to) = endpoints(&message);
        if self.partition.is_blocked(from, to) {
            return;
        }
        let sender = {
            let guard = self.senders.lock();
            guard.get(&to).cloned()
        };
        if let Some(sender) = sender {
            let _ = sender.try_send(message);
        }
    }
}

impl<T: Entry + Send + 'static> Default for MemNetwork<T> {
    fn default() -> Self {
        Self::new()
    }
}

fn endpoints<T: Entry>(message: &Message<T>) -> (u64, u64) {
    match message {
        Message::SequencePaxos(paxos) => (paxos.from, paxos.to),
        Message::BLE(ble) => (ble.from, ble.to),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnipaxos::messages::Message;
    use omnipaxos::messages::sequence_paxos::{PaxosMessage, PaxosMsg};

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Cmd(u64);

    impl omnipaxos::storage::Entry for Cmd {
        type Snapshot = Snap;
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Snap;

    impl omnipaxos::storage::Snapshot<Cmd> for Snap {
        fn create(_: &[Cmd]) -> Self {
            Self
        }
        fn merge(&mut self, _: Self) {}
        fn use_snapshots() -> bool {
            false
        }
    }

    fn proposal_forward(from: u64, to: u64) -> Message<Cmd> {
        Message::SequencePaxos(PaxosMessage {
            from,
            to,
            msg: PaxosMsg::ProposalForward(vec![Cmd(7)]),
        })
    }

    #[tokio::test]
    async fn message_routes_to_registered_destination() {
        let network: MemNetwork<Cmd> = MemNetwork::new();
        let mut inbox = network.register(2);
        network.deliver(proposal_forward(1, 2)).await;
        let received = inbox.recv().await.expect("recv");
        match received {
            Message::SequencePaxos(p) => {
                assert_eq!(p.from, 1);
                assert_eq!(p.to, 2);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[tokio::test]
    async fn message_to_unregistered_node_is_silently_dropped() {
        let network: MemNetwork<Cmd> = MemNetwork::new();
        // No registration for node 99 — deliver should not panic.
        network.deliver(proposal_forward(1, 99)).await;
    }

    #[tokio::test]
    async fn partitioned_endpoint_drops_message() {
        let network: MemNetwork<Cmd> = MemNetwork::new();
        let mut inbox = network.register(2);
        network.partition().isolate(2);
        network.deliver(proposal_forward(1, 2)).await;
        // The isolated node sees no message even though it is registered.
        let result = tokio::time::timeout(std::time::Duration::from_millis(10), inbox.recv()).await;
        assert!(result.is_err(), "isolated node must not receive messages");
    }

    #[tokio::test]
    async fn healed_partition_resumes_routing() {
        let network: MemNetwork<Cmd> = MemNetwork::new();
        let mut inbox = network.register(2);
        network.partition().isolate(2);
        network.deliver(proposal_forward(1, 2)).await;
        // Heal and try again.
        network.partition().heal();
        network.deliver(proposal_forward(1, 2)).await;
        let received = inbox.recv().await.expect("recv");
        assert!(matches!(received, Message::SequencePaxos(_)));
    }
}
