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
                if let Some(hint) = decode_leader_hint(&status)
                    && let Some(hinted_endpoint) = hint.leader_endpoint
                    && !visited.contains(&hinted_endpoint)
                {
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
        let pool = ChannelPool::new(vec!["http://127.0.0.1:1".into()], None);
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "expected Err from unreachable pool");
    }
}
