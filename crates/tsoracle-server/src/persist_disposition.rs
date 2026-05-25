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

//! Shared classification of a consensus persist/load failure into the
//! policy-neutral [`PersistDisposition`] categories the server acts on.
//!
//! Two sites react to a `ConsensusError` from the consensus driver: the
//! request-path window extension ([`service::extend_window`]) and the
//! leadership fence ([`fence::run_leader_watch`]). Their *policies* diverge —
//! the extend path surfaces a transient fault to the caller immediately and a
//! permanent one as `INTERNAL`, while the fence retries a transient fault with
//! backoff and treats a permanent one as fatal — but the *classification* that
//! feeds those policies is identical. [`classify`] owns that classification in
//! one place; each site then maps a `PersistDisposition` to its own action, so
//! the divergence is explicit at the call sites rather than smeared across two
//! near-identical `match` blocks. A fifth `ConsensusError` variant becomes one
//! edit here plus a compiler-forced arm at each site.
//!
//! [`service::extend_window`]: crate::service
//! [`fence::run_leader_watch`]: crate::fence

use tsoracle_consensus::ConsensusError;
use tsoracle_core::Epoch;

/// The policy-neutral category of a consensus persist/load failure.
///
/// This deliberately collapses `ConsensusError::Fenced` and
/// `ConsensusError::NotLeader` into the single [`SteppedDown`] category: both
/// mean "leadership moved under us, abandon this epoch", and both call sites
/// react identically (step down to `NotServing`). The only thing they differ on
/// — the epoch to advertise in a leader hint — is preserved in `fenced_by`, so
/// nothing is lost by the collapse.
///
/// [`SteppedDown`]: PersistDisposition::SteppedDown
#[derive(Debug)]
pub(crate) enum PersistDisposition {
    /// Leadership moved under us (`Fenced` or `NotLeader`). `fenced_by` is the
    /// epoch that fenced us when the driver named it: `Fenced` reports it as
    /// `current`; `NotLeader` exposes none. The extend path threads it into the
    /// `NOT_LEADER` hint so the client can validate its next leader; the fence
    /// path ignores it (it republishes `NotServing` without a hint and awaits
    /// the next leadership event).
    SteppedDown { fenced_by: Option<Epoch> },
    /// A recoverable driver fault: storage I/O hiccup, peer transport flap,
    /// momentary quorum loss. The caller MAY retry. Carries the boxed source so
    /// the call site can format the original `persist: {source}` message.
    Transient(Box<dyn std::error::Error + Send + Sync>),
    /// A permanent driver fault: read-only filesystem, corruption, gone storage
    /// device, invariant violation. The caller MUST NOT silently retry. Carries
    /// the boxed source for the same reason as [`Transient`].
    ///
    /// [`Transient`]: PersistDisposition::Transient
    Permanent(Box<dyn std::error::Error + Send + Sync>),
}

/// Classify a consensus persist/load failure into its policy-neutral
/// [`PersistDisposition`].
///
/// Takes the error by value so the `Transient` / `Permanent` categories can
/// move the boxed source out of the `ConsensusError` rather than cloning it
/// (the source is a `Box<dyn Error>`, which is not `Clone`), letting each call
/// site format the original source text.
pub(crate) fn classify(error: ConsensusError) -> PersistDisposition {
    match error {
        ConsensusError::Fenced { current, .. } => PersistDisposition::SteppedDown {
            fenced_by: Some(current),
        },
        ConsensusError::NotLeader { .. } => PersistDisposition::SteppedDown { fenced_by: None },
        ConsensusError::TransientDriver(source) => PersistDisposition::Transient(source),
        ConsensusError::PermanentDriver(source) => PersistDisposition::Permanent(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fenced_carries_the_current_epoch_into_the_hint() {
        // Fenced names the epoch that fenced us as `current`; classify must
        // surface it so the extend path can advertise it in the NOT_LEADER
        // hint. The `expected` field is irrelevant to the disposition.
        let disposition = classify(ConsensusError::Fenced {
            expected: Epoch(1),
            current: Epoch(2),
        });
        assert!(matches!(
            disposition,
            PersistDisposition::SteppedDown {
                fenced_by: Some(Epoch(2))
            }
        ));
    }

    #[test]
    fn not_leader_steps_down_without_an_epoch() {
        // NotLeader carries `observed`, but the persist path does NOT propagate
        // it into the hint (see #244 / the churn test) — so the disposition
        // deliberately drops it, yielding fenced_by: None even when observed is
        // Some.
        let disposition = classify(ConsensusError::NotLeader {
            observed: Some(Epoch(7)),
        });
        assert!(matches!(
            disposition,
            PersistDisposition::SteppedDown { fenced_by: None }
        ));
    }

    #[test]
    fn transient_preserves_the_source_text() {
        let disposition = classify(ConsensusError::TransientDriver(Box::new(
            std::io::Error::other("flap"),
        )));
        match disposition {
            PersistDisposition::Transient(source) => assert_eq!(source.to_string(), "flap"),
            other => panic!("expected Transient, got a different disposition: {other:?}"),
        }
    }

    #[test]
    fn permanent_preserves_the_source_text() {
        let disposition = classify(ConsensusError::PermanentDriver(Box::new(
            std::io::Error::other("corrupted"),
        )));
        match disposition {
            PersistDisposition::Permanent(source) => assert_eq!(source.to_string(), "corrupted"),
            other => panic!("expected Permanent, got a different disposition: {other:?}"),
        }
    }
}
