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

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no reachable endpoints")]
    NoReachableEndpoints,
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A [`Self::Transport`] failure observed on one endpoint, fanned out to
    /// the sibling waiters of a coalesced request chunk. `tonic::transport::Error`
    /// is not `Clone`, so the originating value cannot be duplicated across
    /// waiters; this variant carries its `Display` text instead. Distinct
    /// from [`Self::NoReachableEndpoints`]: a single endpoint's transport
    /// failed, *not* the whole cluster — collapsing the two would mislead
    /// tracing/alerting on the receiving side.
    #[error("transport (fanned out to coalesced waiter): {0}")]
    TransportFanout(String),
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("invalid count: {0}")]
    InvalidCount(u32),
    #[error("custom channel connector failed: {0}")]
    Connector(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Local driver task has terminated (typically a panic in the user-supplied
    /// RPC closure) and can no longer serve requests. Distinct from
    /// [`Self::NoReachableEndpoints`]: the network is fine, the in-process
    /// driver is dead. Operators should look for a panic in their `tracing`
    /// logs, not a network issue.
    #[error("client driver task is gone; subsequent requests cannot be served by this Client")]
    DriverGone,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_variant_renders_source_in_display() {
        // Display must embed the boxed source's message so operators reading
        // logs see the underlying cause, not just the wrapper.
        let inner: Box<dyn std::error::Error + Send + Sync + 'static> = "boom".into();
        let err = ClientError::Connector(inner);
        assert_eq!(err.to_string(), "custom channel connector failed: boom");
    }

    #[test]
    fn connector_variant_exposes_source() {
        use std::error::Error;
        let inner: Box<dyn std::error::Error + Send + Sync + 'static> =
            std::io::Error::other("io-err").into();
        let err = ClientError::Connector(inner);
        assert!(err.source().is_some(), "Connector must propagate source()");
    }
}
