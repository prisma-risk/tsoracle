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

//! Classifying a `FAILED_PRECONDITION` (NOT_LEADER) reply: leader-hint trailer
//! decoding, the plaintext-downgrade guard, and the epoch-monotone gate — with
//! the cache write (`ChannelPool::compare_and_set_leader`) co-located with the
//! decision it gates. The retry loop (`crate::retry`) and one RPC attempt
//! (`crate::attempt`) both route NOT_LEADER replies through here.

use tsoracle_core::Epoch;

use crate::attempt::{AttemptOutcome, HintUnusableReason};
use crate::channel_pool::{ChannelPool, LeaderHintLookup, decode_leader_hint};

/// Decide what the retry loop should do with a `FAILED_PRECONDITION` reply.
///
/// Lives in its own module so the decision tree — hint decoding,
/// plaintext-downgrade rejection, and the epoch-monotone gate — is
/// unit-testable without standing up a real gRPC peer. The production path
/// (`crate::attempt::attempt`) routes NOT_LEADER replies through here too, so
/// the integration and unit tests exercise the same code.
pub(crate) fn classify_not_leader_hint(
    pool: &ChannelPool,
    endpoint: &str,
    status: tonic::Status,
) -> AttemptOutcome {
    // Silence the unused-variable warning when `tracing` is off; the
    // parameter only flows into log fields below.
    let _ = endpoint;
    let (hinted_endpoint, hint_epoch) = match decode_leader_hint(&status) {
        LeaderHintLookup::Decoded(hint) => {
            // `leader_epoch` is present in full or absent — the nested
            // `EpochWire` makes a half-populated epoch unrepresentable — so
            // reassembly is a single map with no partial-pair case.
            let epoch = hint
                .leader_epoch
                .map(|epoch| Epoch::from_wire(epoch.hi, epoch.lo).0);
            (hint.leader_endpoint, epoch)
        }
        LeaderHintLookup::Absent => {
            // No leader-hint trailer: the peer is up but no leader is known
            // yet (the election signature). Return early as its own outcome so
            // the retry loop can ride out the election — distinct from the
            // malformed / guard-dropped cases below, which stay `HintUnusable { reason: Rejected }`
            // and keep failing fast.
            #[cfg(feature = "tracing")]
            tracing::warn!(
                endpoint = %endpoint,
                "tsoracle-client: FAILED_PRECONDITION without leader-hint trailer; \
                 contacted peer cannot redirect us (no leader yet)",
            );
            return AttemptOutcome::NoLeaderYet(status);
        }
        LeaderHintLookup::Malformed => {
            #[cfg(feature = "metrics")]
            metrics::counter!("tsoracle.client.leader_hint.decode_failures.total").increment(1);
            #[cfg(feature = "tracing")]
            tracing::warn!(
                endpoint = %endpoint,
                "tsoracle-client: FAILED_PRECONDITION carried a malformed \
                 leader-hint trailer; treating as no hint",
            );
            (None, None)
        }
    };
    let usable_endpoint = hinted_endpoint.filter(|hinted| !rejects_plaintext_hint(pool, hinted));
    match usable_endpoint {
        Some(hinted) => {
            // Seat the hint under the same lock that checks the
            // monotone-forward rule. Doing the check and the write as one
            // atomic step (rather than gating here and writing later in the
            // dispatch loop) is what prevents a concurrent higher-epoch
            // promotion from being clobbered by this lower-or-equal hint.
            if pool.compare_and_set_leader(hinted.clone(), hint_epoch) {
                AttemptOutcome::LeaderHint {
                    endpoint: hinted,
                    epoch: hint_epoch,
                }
            } else {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    from = %endpoint,
                    to = %hinted,
                    hint_epoch = ?hint_epoch,
                    "tsoracle-client: dropping leader hint that cannot outrank \
                     the cached leader — either a stale epoch behind the cached \
                     one, or an epoch-less hint to an off-list endpoint",
                );
                AttemptOutcome::HintUnusable {
                    status,
                    reason: HintUnusableReason::StaleEpoch,
                }
            }
        }
        None => AttemptOutcome::HintUnusable {
            status,
            reason: HintUnusableReason::Rejected,
        },
    }
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
    use crate::RetryPolicy;
    use crate::test_support::{enable_tracing, make_status_with_hint};

    /// Direct table-test for the wire-hint policy. The integration test in
    /// `crates/tsoracle-tests/tests/client_tls.rs` exercises the full
    /// FAILED_PRECONDITION→trailer→retry path end-to-end; this unit test
    /// pins down the predicate itself so a refactor cannot quietly flip
    /// the policy.
    #[test]
    fn plaintext_hint_policy_matches_scheme_and_tls_state() {
        enable_tracing();
        let tls = ChannelPool::new(vec!["a:1".into()], None, true, RetryPolicy::default());
        let plain = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());

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

    /// A FAILED_PRECONDITION with NO leader-hint trailer (a follower that does
    /// not yet know a leader — the election signature) classifies as
    /// `NoLeaderYet`, distinct from a malformed/guard-dropped hint
    /// (`HintUnusable { reason: Rejected }`). Pins the split that lets the retry loop ride out an
    /// election without also riding out deterministic bad-hint rejections.
    #[test]
    fn absent_hint_classifies_as_no_leader_yet() {
        let pool = ChannelPool::new(vec!["peer:1".into()], None, false, RetryPolicy::default());
        let status = tonic::Status::failed_precondition("electing, no leader yet");
        match classify_not_leader_hint(&pool, "peer:1", status) {
            AttemptOutcome::NoLeaderYet(s) => {
                assert_eq!(s.code(), tonic::Code::FailedPrecondition);
            }
            other => panic!("absent hint must be NoLeaderYet, got {other:?}"),
        }
    }

    /// A `FAILED_PRECONDITION` carrying no trailer at all surfaces as
    /// `NoLeaderYet` (the election signature: the peer is up but no leader is
    /// known yet). Covers the `LeaderHintLookup::Absent` arm.
    #[test]
    fn classify_absent_hint_returns_no_leader_yet() {
        enable_tracing();
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        let status = tonic::Status::failed_precondition("not leader");
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::NoLeaderYet(_) => {}
            other => panic!("expected NoLeaderYet, got {other:?}"),
        }
    }

    /// A trailer containing bytes that don't decode as a `LeaderHint`
    /// (here: 0xff repeated — never a valid protobuf prefix) must
    /// route to `HintUnusable { reason: Rejected }`, not panic, and must bump the
    /// decode-failures metric. Covers `LeaderHintLookup::Malformed`.
    #[test]
    fn classify_malformed_hint_returns_rejected() {
        enable_tracing();
        use tonic::metadata::{BinaryMetadataValue, MetadataKey};
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        let mut status = tonic::Status::failed_precondition("not leader");
        let key = MetadataKey::from_bytes(tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY.as_bytes())
            .unwrap();
        status.metadata_mut().insert_bin(
            key,
            BinaryMetadataValue::from_bytes(&[0xff, 0xff, 0xff, 0xff]),
        );
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintUnusable {
                reason: HintUnusableReason::Rejected,
                ..
            } => {}
            other => panic!("expected HintUnusable {{ reason: Rejected }}, got {other:?}"),
        }
    }

    /// A well-formed hint with a higher leader epoch than the
    /// cached leader's must be followed. This is the bread-and-butter
    /// case: a freshly-elected leader supersedes our cached one.
    #[test]
    fn classify_higher_epoch_hint_returns_leader_hint() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 5);
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("b:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 7 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::LeaderHint { endpoint, epoch } => {
                assert_eq!(endpoint, "b:1");
                assert_eq!(epoch, Some(7));
            }
            other => panic!("expected LeaderHint, got {other:?}"),
        }
    }

    /// Two peers redirect us at different epochs. Whatever order the hints
    /// arrive in, the client must end up following the higher-epoch leader and
    /// never flap its cache back to the lower-epoch one. This is the end-to-end
    /// safety property the populated server epoch unlocks.
    #[test]
    fn higher_epoch_hint_wins_regardless_of_arrival_order() {
        for stale_first in [true, false] {
            let pool = ChannelPool::new(
                vec!["a:1".into(), "b:1".into(), "c:1".into()],
                None,
                false,
                RetryPolicy::default(),
            );
            // Bootstrap the cache at a low epoch so the first hint is accepted.
            pool.record_success("a:1", 1);

            let fresh = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                leader_endpoint: Some("c:1".into()),
                leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 9 }),
            });
            let stale = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                leader_endpoint: Some("b:1".into()),
                leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 4 }),
            });
            let ordered = if stale_first {
                vec![stale, fresh]
            } else {
                vec![fresh, stale]
            };

            // `classify_not_leader_hint` seats the cache atomically as part
            // of the monotone-forward check, so the loop need only drive
            // each redirect through it — no separate write step.
            for status in ordered {
                let _ = classify_not_leader_hint(&pool, "a:1", status);
            }

            assert_eq!(
                pool.cached_leader().as_deref(),
                Some("c:1"),
                "must follow the epoch-9 leader (stale_first={stale_first})",
            );
        }
    }

    /// A well-formed hint whose `leader_epoch` is strictly less than
    /// the cached leader's epoch must be dropped — that is the whole
    /// point of the epoch-monotone gate. The outcome carries the
    /// originating `FAILED_PRECONDITION` so the retry loop's
    /// `HintUnusable { reason: StaleEpoch }` arm can record it as `last_err`; the arm
    /// continues without mutating the cache.
    #[test]
    fn classify_stale_epoch_hint_returns_stale_epoch() {
        enable_tracing();
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 10);
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("b:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 5 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintUnusable {
                status,
                reason: HintUnusableReason::StaleEpoch,
            } => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            }
            other => panic!("expected HintUnusable {{ reason: StaleEpoch }}, got {other:?}"),
        }
        // Cache must be untouched.
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
    }

    /// A hint that carries no `leader_epoch` (a paxos backend, or an older
    /// openraft server) is accepted unconditionally so the client remains
    /// useful during a mixed-version deployment.
    #[test]
    fn classify_no_epoch_hint_returns_leader_hint() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 10);
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("b:1".into()),
            leader_epoch: None,
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::LeaderHint { endpoint, epoch } => {
                assert_eq!(endpoint, "b:1");
                assert_eq!(epoch, None);
            }
            other => panic!("expected LeaderHint, got {other:?}"),
        }
    }

    /// Under `tls_required = true`, a hint with an explicit `http://`
    /// scheme must be refused so a malicious or misconfigured peer
    /// cannot downgrade the transport. The outcome is `HintUnusable { reason: Rejected }`
    /// (not `HintUnusable { reason: StaleEpoch }`) because the cache is still valid; the
    /// hint just wasn't usable.
    #[test]
    fn classify_plaintext_hint_under_tls_returns_rejected() {
        enable_tracing();
        let pool = ChannelPool::new(vec!["a:1".into()], None, true, RetryPolicy::default());
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("http://attacker:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 7 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintUnusable {
                reason: HintUnusableReason::Rejected,
                ..
            } => {}
            other => panic!("expected HintUnusable {{ reason: Rejected }}, got {other:?}"),
        }
    }
}
