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

//! tonic peer transport for OmniPaxos.
//!
//! Wire format: a single `Send(PaxosMessage) → Ack` unary RPC whose `payload`
//! is a postcard-encoded `omnipaxos::messages::Message<HighWaterCommand>`. The
//! server decodes and feeds the result to `OmniPaxos::handle_incoming`.
//!
//! OmniPaxos's outbound dispatch is itself fire-and-forget; we do not need a
//! response payload, only a transport-level Ack so the client can observe
//! connection-level failures and recycle its connection.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use omnipaxos::messages::Message;
use omnipaxos::storage::Storage;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::Channel;
use tracing::warn;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_paxos_toolkit::lifecycle::MessageSink;

pub mod proto {
    tonic::include_proto!("tsoracle.example.paxos.v1");
}

use proto::paxos_peer_service_client::PaxosPeerServiceClient;
use proto::paxos_peer_service_server::{PaxosPeerService, PaxosPeerServiceServer};
use proto::{Ack, PaxosMessage};

/// Outbound message sink. Holds the peer-address map and a lazy tonic client
/// cache so each peer's `Channel` is only set up once per process lifetime.
pub struct PeerSink {
    addrs: Arc<HashMap<u64, String>>,
    pool: AsyncMutex<HashMap<u64, PaxosPeerServiceClient<Channel>>>,
}

impl PeerSink {
    pub fn new(addrs: HashMap<u64, String>) -> Self {
        Self {
            addrs: Arc::new(addrs),
            pool: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn client(&self, target: u64) -> Option<PaxosPeerServiceClient<Channel>> {
        let mut pool = self.pool.lock().await;
        if let Some(client) = pool.get(&target) {
            return Some(client.clone());
        }
        let endpoint = self.addrs.get(&target)?;
        let url = format!("http://{endpoint}");
        match PaxosPeerServiceClient::connect(url).await {
            Ok(client) => {
                pool.insert(target, client.clone());
                Some(client)
            }
            Err(err) => {
                // Peer reachability is a normal transient condition during
                // bring-up (the others may not be listening yet). OmniPaxos
                // retries on its own next tick, so we only log at warn.
                warn!(target, error = %err, "paxos peer connect failed");
                None
            }
        }
    }
}

#[async_trait]
impl MessageSink<HighWaterCommand> for PeerSink {
    async fn send(&self, message: Message<HighWaterCommand>) {
        let target = message.get_receiver();
        let payload = match postcard::to_stdvec(&message) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(target, error = %err, "encode outbound paxos message");
                return;
            }
        };
        let Some(mut client) = self.client(target).await else {
            return;
        };
        if let Err(err) = client.send(PaxosMessage { payload }).await {
            // Drop the cached client on RPC failure so the next attempt
            // reconnects rather than re-using a half-broken channel.
            self.pool.lock().await.remove(&target);
            warn!(target, error = %err, "send paxos peer rpc");
        }
    }
}

/// Server-side handler: decodes inbound `PaxosMessage` payloads and feeds them
/// to the shared `OmniPaxos` handle.
pub struct PaxosPeerServiceImpl<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
}

#[tonic::async_trait]
impl<S> PaxosPeerService for PaxosPeerServiceImpl<S>
where
    S: Storage<HighWaterCommand> + Send + Sync + 'static,
{
    async fn send(
        &self,
        request: tonic::Request<PaxosMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        let payload = request.into_inner().payload;
        let message: Message<HighWaterCommand> = postcard::from_bytes(&payload)
            .map_err(|err| tonic::Status::invalid_argument(format!("decode message: {err}")))?;
        self.omnipaxos.lock().handle_incoming(message);
        Ok(tonic::Response::new(Ack {}))
    }
}

/// Construct the tonic server-side handler for the peer-transport service.
pub fn server<S>(
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
) -> PaxosPeerServiceServer<PaxosPeerServiceImpl<S>>
where
    S: Storage<HighWaterCommand> + Send + Sync + 'static,
{
    PaxosPeerServiceServer::new(PaxosPeerServiceImpl { omnipaxos })
}
