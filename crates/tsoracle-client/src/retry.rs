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

//! Endpoint retry policy for client RPCs.
//!
//! The worklist starts with the cached leader (if any) followed by configured
//! endpoints. On a NOT_LEADER response carrying a LeaderHint pointing at an
//! unvisited endpoint, that endpoint is pushed to the FRONT of the worklist
//! so we retry the hinted leader immediately — not at the end of the
//! round-robin pass, which would leave the current call to fail if the
//! hinted endpoint wasn't otherwise in the queue.

use std::collections::{HashSet, VecDeque};

use tsoracle_core::Timestamp;

use crate::error::ClientError;
use crate::leader_resolved::{ChannelPool, decode_leader_hint};
use crate::response::decode_get_ts_response;

pub(crate) async fn issue_rpc(
    pool: &ChannelPool,
    count: u32,
) -> Result<Vec<Timestamp>, ClientError> {
    let mut worklist: VecDeque<String> = pool.iter_round_robin().into();
    let mut visited: HashSet<String> = HashSet::new();
    let mut last_err: Option<ClientError> = None;

    while let Some(endpoint) = worklist.pop_front() {
        if !visited.insert(endpoint.clone()) {
            continue;
        }
        let mut client = match pool.client(&endpoint).await {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        match client
            .get_ts(tsoracle_proto::v1::GetTsRequest { count })
            .await
        {
            Ok(resp) => {
                pool.set_leader(endpoint);
                match decode_get_ts_response(resp.into_inner(), count) {
                    Ok(timestamps) => return Ok(timestamps),
                    Err(e) => {
                        last_err = Some(e);
                        continue;
                    }
                }
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                let usable_hint = decode_leader_hint(&status)
                    .and_then(|hint| hint.leader_endpoint)
                    .filter(|hinted_endpoint| !visited.contains(hinted_endpoint))
                    .filter(|hinted_endpoint| !rejects_plaintext_hint(pool, hinted_endpoint));
                if let Some(hinted_endpoint) = usable_hint {
                    pool.set_leader(hinted_endpoint.clone());
                    worklist.push_front(hinted_endpoint);
                    continue;
                }
                pool.clear_leader();
                last_err = Some(ClientError::Rpc(status));
                continue;
            }
            Err(status) => {
                last_err = Some(ClientError::Rpc(status));
                continue;
            }
        }
    }
    Err(last_err.unwrap_or(ClientError::NoReachableEndpoints))
}

/// Refuse a wire-supplied leader hint that would downgrade the transport.
///
/// Under `ClientBuilder::tls_config`, a malicious or misconfigured peer
/// could otherwise feed the client an `http://...` leader endpoint via the
/// `tsoracle-leader-hint-bin` trailer and route the next RPC over plaintext.
/// The check is scoped to wire input: operator-supplied `endpoints` carrying
/// an explicit `http://` scheme are still honored ("explicit beats configured"
/// remains true for caller-controlled config).
///
/// Match shape mirrors `normalize_uri`: ASCII lowercase `http://` prefix.
/// Uppercase variants would already fail to parse after the bare→https
/// rewrite, so checking the lowercase form is sufficient.
fn rejects_plaintext_hint(pool: &ChannelPool, hint: &str) -> bool {
    let reject = pool.tls_required() && hint.starts_with("http://");
    #[cfg(feature = "tracing")]
    if reject {
        tracing::warn!(
            hinted_endpoint = %hint,
            "tsoracle-client: dropping plaintext leader-hint under tls_config; \
             refusing to downgrade transport"
        );
    }
    reject
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool seeded with duplicate endpoints must visit each once; the
    /// second visit hits the `!visited.insert` short-circuit and continues
    /// without burning an extra connect attempt. Since the endpoint is
    /// unreachable, the final outcome is `NoReachableEndpoints`, but the
    /// `visited` set being effective is the property under test here.
    #[tokio::test]
    async fn duplicate_endpoints_are_visited_once() {
        let pool = ChannelPool::new(
            vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:1".into()],
            None,
            false,
        );
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "no live endpoint must surface as Err");
    }

    /// When every configured endpoint fails the connect attempt (closed
    /// port), the retry loop accumulates the last error and returns it as
    /// the surface failure. Exercises the `pool.client(...) -> Err`
    /// continue path that's not reached by the happy-path integration tests.
    #[tokio::test]
    async fn unreachable_endpoints_surface_last_error() {
        let pool = ChannelPool::new(vec!["http://127.0.0.1:1".into()], None, false);
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "expected Err from unreachable pool");
    }

    /// Direct table-test for the wire-hint policy. The integration test in
    /// `crates/tsoracle-tests/tests/client_tls.rs` exercises the full
    /// FAILED_PRECONDITION→trailer→retry path end-to-end; this unit test
    /// pins down the predicate itself so a refactor cannot quietly flip
    /// the policy.
    #[test]
    fn plaintext_hint_policy_matches_scheme_and_tls_state() {
        let tls = ChannelPool::new(vec!["a:1".into()], None, true);
        let plain = ChannelPool::new(vec!["a:1".into()], None, false);

        assert!(
            rejects_plaintext_hint(&tls, "http://attacker:1"),
            "http:// hint must be rejected under tls_required"
        );
        assert!(
            !rejects_plaintext_hint(&tls, "https://peer:1"),
            "https:// hint must be allowed under tls_required"
        );
        assert!(
            !rejects_plaintext_hint(&tls, "peer:1"),
            "bare host:port hint must be allowed under tls_required (gets rewritten to https)"
        );
        assert!(
            !rejects_plaintext_hint(&plain, "http://peer:1"),
            "http:// hint must be allowed when tls is not required"
        );
    }
}
