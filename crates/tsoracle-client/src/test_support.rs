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

//! Shared test-only helpers for the client crate's retry/attempt/leader-hint
//! unit tests. Compiled only under `cfg(test)`.

use std::time::Duration;

use crate::RetryPolicy;

/// Install a process-global TRACE subscriber so the retry/attempt code's
/// `tracing::{debug,warn}!` sites evaluate and format their fields under test.
/// Idempotent: `try_init` installs once and returns `Err` (ignored) thereafter.
pub(crate) fn enable_tracing() {
    use tracing_subscriber::filter::LevelFilter;
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_test_writer()
        .try_init();
}

/// Aggressive policy used by the unit tests to keep them fast.
pub(crate) fn short_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 2,
        per_attempt_deadline: Duration::from_millis(100),
        overall_deadline: Duration::from_millis(300),
        base_backoff: Duration::from_millis(1),
        leader_ttl: Duration::from_secs(30),
    }
}

/// Build a `FAILED_PRECONDITION` status with a `LeaderHint` trailer encoded
/// under the same key the server uses, mirroring the production encoding in
/// `crates/tsoracle-server/src/leader_hint.rs`.
pub(crate) fn make_status_with_hint(hint: tsoracle_proto::v1::LeaderHint) -> tonic::Status {
    use prost::Message;
    use tonic::metadata::{BinaryMetadataValue, MetadataKey};
    let mut buf = Vec::new();
    hint.encode(&mut buf)
        .expect("LeaderHint encode is infallible");
    let mut status = tonic::Status::failed_precondition("not leader");
    let key = MetadataKey::from_bytes(tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY.as_bytes())
        .expect("static ASCII key parses");
    status
        .metadata_mut()
        .insert_bin(key, BinaryMetadataValue::from_bytes(&buf));
    status
}
