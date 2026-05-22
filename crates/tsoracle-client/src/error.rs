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

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no reachable endpoints")]
    NoReachableEndpoints,
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("invalid count: {0}")]
    InvalidCount(u32),
    #[error("custom channel connector failed: {0}")]
    Connector(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
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
