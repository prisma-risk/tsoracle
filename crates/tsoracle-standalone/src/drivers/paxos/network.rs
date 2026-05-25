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
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::warn;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_paxos_toolkit::lifecycle::MessageSink;

pub mod proto {
    tonic::include_proto!("tsoracle.paxos.peer.v1");
}

use proto::paxos_peer_service_client::PaxosPeerServiceClient;
use proto::paxos_peer_service_server::{PaxosPeerService, PaxosPeerServiceServer};
use proto::{Ack, PaxosMessage};

/// Outbound message sink. Holds the peer-address map and a lazy tonic client
/// cache so each peer's `Channel` is only set up once per process lifetime.
pub struct PeerSink {
    addrs: Arc<HashMap<u64, String>>,
    pool: AsyncMutex<HashMap<u64, PaxosPeerServiceClient<Channel>>>,
    tls: Option<ClientTlsConfig>,
}

impl PeerSink {
    pub fn new(addrs: HashMap<u64, String>, tls: Option<ClientTlsConfig>) -> Self {
        Self {
            addrs: Arc::new(addrs),
            pool: AsyncMutex::new(HashMap::new()),
            tls,
        }
    }

    async fn client(&self, target: u64) -> Option<PaxosPeerServiceClient<Channel>> {
        let mut pool = self.pool.lock().await;
        if let Some(client) = pool.get(&target) {
            return Some(client.clone());
        }
        let endpoint = self.addrs.get(&target)?;
        let channel = match &self.tls {
            Some(tls) => {
                tonic::transport::Channel::from_shared(format!("https://{endpoint}"))
                    .ok()?
                    .tls_config(tls.clone())
                    .ok()?
                    .connect()
                    .await
            }
            None => {
                tonic::transport::Channel::from_shared(format!("http://{endpoint}"))
                    .ok()?
                    .connect()
                    .await
            }
        };
        match channel {
            Ok(channel) => {
                let client = PaxosPeerServiceClient::new(channel);
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

#[cfg(test)]
mod tls_tests {
    use super::*;
    use crate::config::PeerTlsConfig;
    use crate::peer_tls::build_peer_tls;
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    // --- cert helpers (rcgen 0.13) ---
    struct Certs {
        ca_pem: String,
        node_cert: String,
        node_key: String,
        other_leaf_cert: String,
        other_leaf_key: String,
    }

    fn mint() -> Certs {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let mk_ca = |name: &str| {
            let key = KeyPair::generate().unwrap();
            let mut p = CertificateParams::new(vec![name.to_string()]).unwrap();
            p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let cert = p.self_signed(&key).unwrap();
            (cert, key)
        };
        let (ca, ca_key) = mk_ca("tso-ca");
        let leaf_key = KeyPair::generate().unwrap();
        let leaf_params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
        let (other_ca, other_ca_key) = mk_ca("other-ca");
        let other_key = KeyPair::generate().unwrap();
        let other_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let other_leaf = other_params
            .signed_by(&other_key, &other_ca, &other_ca_key)
            .unwrap();
        Certs {
            ca_pem: ca.pem(),
            node_cert: leaf.pem(),
            node_key: leaf_key.serialize_pem(),
            other_leaf_cert: other_leaf.pem(),
            other_leaf_key: other_key.serialize_pem(),
        }
    }

    fn node_material(c: &Certs, dir: &std::path::Path) -> crate::peer_tls::PeerTlsMaterial {
        let cert = dir.join("n.crt");
        let key = dir.join("n.key");
        let ca = dir.join("ca.crt");
        std::fs::write(&cert, &c.node_cert).unwrap();
        std::fs::write(&key, &c.node_key).unwrap();
        std::fs::write(&ca, &c.ca_pem).unwrap();
        build_peer_tls(&PeerTlsConfig { cert, key, ca }).unwrap()
    }

    // Minimal stub server (handlers never called — only the TLS handshake is).
    #[derive(Clone)]
    struct Stub;

    #[tonic::async_trait]
    impl proto::paxos_peer_service_server::PaxosPeerService for Stub {
        async fn send(
            &self,
            _: tonic::Request<proto::PaxosMessage>,
        ) -> Result<tonic::Response<proto::Ack>, tonic::Status> {
            Ok(tonic::Response::new(proto::Ack {}))
        }
    }

    async fn spawn_stub(server_tls: tonic::transport::ServerTlsConfig) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(server_tls)
                .unwrap()
                .add_service(proto::paxos_peer_service_server::PaxosPeerServiceServer::new(Stub))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        addr
    }

    fn make_sink(addr: std::net::SocketAddr, tls: Option<ClientTlsConfig>) -> PeerSink {
        PeerSink::new(
            std::collections::HashMap::from([(2u64, addr.to_string())]),
            tls,
        )
    }

    // `Channel::connect()` is lazy — the TLS handshake happens on the first RPC.
    // We issue a real `send` RPC to force the handshake and check the status code.
    // A successful mTLS handshake produces Ok (the stub returns Ack {}); a rejected
    // handshake means `client()` returns None after the failed dial.
    async fn probe(sink: PeerSink) -> tonic::Code {
        match sink.client(2).await {
            None => tonic::Code::Unavailable,
            Some(mut c) => {
                match c
                    .send(tonic::Request::new(proto::PaxosMessage {
                        payload: Vec::new(),
                    }))
                    .await
                {
                    Ok(_) => tonic::Code::Ok,
                    Err(s) => s.code(),
                }
            }
        }
    }

    #[tokio::test]
    async fn valid_node_cert_connects() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        // Stub returns Ok(Ack {}) — handshake succeeded.
        assert_eq!(
            probe(make_sink(addr, Some(m.client.clone()))).await,
            tonic::Code::Ok
        );
    }

    #[tokio::test]
    async fn no_client_cert_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        let no_id = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&c.ca_pem))
            .domain_name("localhost");
        let code = probe(make_sink(addr, Some(no_id))).await;
        assert_ne!(
            code,
            tonic::Code::Ok,
            "server must reject a client with no cert"
        );
    }

    #[tokio::test]
    async fn wrong_ca_client_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        let wrong = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&c.ca_pem))
            .identity(Identity::from_pem(&c.other_leaf_cert, &c.other_leaf_key))
            .domain_name("localhost");
        let code = probe(make_sink(addr, Some(wrong))).await;
        assert_ne!(
            code,
            tonic::Code::Ok,
            "server must reject a cert from a foreign CA"
        );
    }

    #[tokio::test]
    async fn plaintext_against_tls_fails() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        let code = probe(make_sink(addr, None)).await;
        assert_ne!(
            code,
            tonic::Code::Ok,
            "plaintext must not reach a TLS-only server"
        );
    }
}
