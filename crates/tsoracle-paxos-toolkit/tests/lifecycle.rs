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
use tsoracle_paxos_toolkit::lifecycle::{MessageSink, PaxosRunner, TsoPeer};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
                endpoint: format!("mem://{}", peer_node.node_id),
            })
            .collect();
        let mut runner = PaxosRunner::new(
            node.omnipaxos.clone(),
            node.node_id,
            peers,
            Duration::from_millis(10),
        );
        let stream = runner.take_leader_stream().expect("stream").into_pin();
        runner.start(sink.clone());
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
