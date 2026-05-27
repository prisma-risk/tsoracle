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

//! `PaxosRunner` integration: boot one runner per node in a 3-node cluster,
//! assert at least one emits a definite (Leader or Follower) event.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestCommand, build_mem_cluster};
use futures::StreamExt;
use omnipaxos::messages::Message;
use tsoracle_consensus::LeaderState;
use tsoracle_paxos_toolkit::lifecycle::{MessageSink, PaxosRunner, PeerEndpoint, TsoPeer};
use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;

struct NetworkSink {
    network: Arc<MemNetwork<TestCommand>>,
}

#[async_trait::async_trait]
impl MessageSink<TestCommand> for NetworkSink {
    async fn send(&self, message: Message<TestCommand>) {
        self.network.deliver(message).await;
    }
}

// Runs under tokio virtual time (`start_paused`): the runners' `interval`
// ticks, the leader-stream `next()` awaits, and the `timeout(3s)` guard all
// advance in simulated time once the runtime goes idle, so the election still
// runs through the real async runner tasks but without wall-clock variance.
// `start_paused` implies the current-thread runtime.
#[tokio::test(start_paused = true)]
async fn runners_emit_leader_or_follower_event_after_election() {
    let cluster = build_mem_cluster(3);
    let sink = Arc::new(NetworkSink {
        network: cluster.network.clone(),
    });

    let mut runners = Vec::with_capacity(3);
    let mut streams = Vec::with_capacity(3);
    for node in &cluster.nodes {
        let peers = cluster
            .nodes
            .iter()
            .filter(|peer_node| peer_node.node_id != node.node_id)
            .map(|peer_node| TsoPeer {
                node_id: peer_node.node_id,
                endpoint: PeerEndpoint::try_from(format!("mem-peer-{}:1", peer_node.node_id))
                    .expect("synthetic peer endpoint is contract-conforming"),
            })
            .collect();
        let mut runner = PaxosRunner::new(
            node.omnipaxos.clone(),
            node.node_id,
            peers,
            Duration::from_millis(10),
        );
        let stream = runner
            .take_leader_subscriber()
            .expect("subscriber")
            .subscribe()
            .into_pin();
        runner.start(sink.clone()).expect("runner starts");
        runners.push(runner);
        streams.push(stream);
    }

    // Wait until at least one stream yields a definite (non-Unknown) state.
    let saw_definite = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            for stream in &mut streams {
                if let Some(state) = stream.next().await
                    && !matches!(state, LeaderState::Unknown)
                {
                    return true;
                }
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(saw_definite, "expected a definite leader/follower event");
    for runner in &mut runners {
        runner.stop().await;
    }
}
