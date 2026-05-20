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
