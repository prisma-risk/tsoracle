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

//! Binary wire codec for openraft RPC payloads and storage records.
//!
//! Every payload is encoded as `[version_byte | postcard(value)]`. The leading
//! byte lets us evolve the wire format without an explicit migration when both
//! sides of an upgrade run mixed versions briefly.
//!
//! The framing itself lives in the shared [`tsoracle_codec`] crate, re-used
//! verbatim by the paxos toolkit. The version constants ([`MIN_READABLE_VERSION`],
//! [`MAX_READABLE_VERSION`], [`BASELINE_WRITE_VERSION`]) and the runtime
//! [`ActiveWriteVersion`] cell are owned here, so this toolkit's wire/on-disk
//! format versions independently of the others.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

pub use tsoracle_codec::{CodecError, codec_io_error, decode, encode};

/// Oldest on-disk/wire format version this binary still ships a parser for.
/// The decode side (disk now, peer RPC in P3) accepts any version in the
/// inclusive range `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]`. Under the
/// never-remove decoder policy this only ever stays put or moves down; it never
/// rises. Seeded at the feature baseline (4): the historical 1->2->3->4 bumps
/// were stop-the-world, so no pre-v4 on-disk data can rolling-coexist in any
/// cluster this feature touches. A future release may lower it (re-adding
/// older parsers) if a real pre-v4 rolling-read need ever surfaces.
pub const MIN_READABLE_VERSION: u8 = 4;

/// Newest on-disk/wire format version this binary has a parser for. Only ever
/// grows. Decode accepts `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]`.
///
/// Today this is 5 (`DENSE_WRITE_VERSION`): a v5-capable binary can read both
/// the v4 baseline layout and the v5 dense layout. A node writes
/// `BASELINE_WRITE_VERSION` (4) until a committed `SetFormatVersion` activation
/// advances the active write version through the all-members gate.
pub const MAX_READABLE_VERSION: u8 = 5;

// Compile-time guard: the readable range must be non-empty (`MIN <= MAX`) or
// every decode rejects every record. Catches a future inverted-constants edit
// at build time rather than at test runtime.
const _: () = assert!(MIN_READABLE_VERSION <= MAX_READABLE_VERSION);

/// Default active write version — the single version this node emits before any
/// activation barrier advances it. It is independent of [`MIN_READABLE_VERSION`]
/// and is the assumed version of any unframed legacy peer payload (P3). Held at
/// runtime in an [`ActiveWriteVersion`] cell that defaults to this value; the
/// cell only ever advances via a successful, committed activation apply (P5),
/// so in this release every framed record still leads with this byte.
pub const BASELINE_WRITE_VERSION: u8 = 4;

/// The write version that introduces the `AdvanceDense` log command and the
/// dense fields in the state-machine snapshot. A leader must not append an
/// `AdvanceDense` entry (and `GetSeq` is refused) until the active write
/// version has been activated to at least this value through the all-members
/// gate — otherwise an older member could receive an entry it cannot decode.
pub const DENSE_WRITE_VERSION: u8 = 5;

/// Process-shared, runtime-mutable active write version.
///
/// A thin newtype over `Arc<AtomicU8>` so the three writers — the log store
/// (stamps appended records), the state machine (stamps snapshots), and the peer
/// RPC sender (P3) — share **one** cell, the single source of truth for "the
/// version this node currently emits". Constructed once at bootstrap, seeded by
/// [`recover_active_write_version`], and cloned (cheap refcount bump) into each
/// writer. It is mutated only by a successful, committed activation apply (P5);
/// in this release it never leaves [`BASELINE_WRITE_VERSION`].
///
/// There is deliberately no persisted copy of this value. The state machine
/// reaches storage only through its opaque snapshot store and cannot write the
/// log store's meta column family, and a separate non-synced counter would
/// reintroduce the barrier-seq durability hazard. Durability comes from the raft
/// log (a committed `SetFormatVersion` entry is fsynced and re-applied
/// deterministically on restart) and the snapshot's leading frame byte; recovery
/// re-seeds the cell from that durable evidence via [`recover_active_write_version`].
#[derive(Clone, Debug)]
pub struct ActiveWriteVersion(Arc<AtomicU8>);

impl ActiveWriteVersion {
    /// Construct a cell holding `version`.
    pub fn new(version: u8) -> Self {
        Self(Arc::new(AtomicU8::new(version)))
    }

    /// The version this node currently emits.
    pub fn get(&self) -> u8 {
        self.0.load(Ordering::Acquire)
    }

    /// Set the active write version. Called only by a successful activation
    /// apply (P5); `Release` pairs with the `Acquire` in [`get`](Self::get) so a
    /// writer that observes the new version also observes everything sequenced
    /// before the activation. Not exposed as a CAS in P2 because no concurrent
    /// mutator exists yet; P5 may revisit if it needs a compare-and-set.
    pub fn set(&self, version: u8) {
        self.0.store(version, Ordering::Release);
    }
}

impl Default for ActiveWriteVersion {
    /// A fresh cell at [`BASELINE_WRITE_VERSION`]. Used by the default log-store
    /// and state-machine constructors (and tests) that do not run the bootstrap
    /// recovery seed; behavior is then identical to a pre-feature node.
    fn default() -> Self {
        Self::new(BASELINE_WRITE_VERSION)
    }
}

/// Compute the bootstrap seed for the [`ActiveWriteVersion`] cell from
/// fsync-durable lower bounds.
///
/// Returns `max(BASELINE_WRITE_VERSION, snapshot_leading_byte?,
/// highest_log_record_byte?)` when every present input is within the readable
/// range `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]`, and
/// [`CodecError::VersionUnsupported`] when any present input exceeds
/// `MAX_READABLE_VERSION`. Both inputs are `Option` because a fresh store has
/// neither a persisted snapshot nor any log record. Taking the max of
/// durably-*written* evidence is monotone and safe: a rejected or no-op
/// activation never caused any record to be written at the higher version, so it
/// cannot be resurrected here, and a node never recovers a version below what it
/// actually emitted.
///
/// The upper-bound check matters when this binary is older than the on-disk
/// data — for example a rolling-restart that downgrades a node to a release
/// whose [`MAX_READABLE_VERSION`] is lower than a leading byte already stamped
/// in the log or snapshot by a newer release. Without the check the cell would
/// be seeded to a value the decode side cannot handle, and the writers would
/// stamp every new record at that unreadable version (a persistent availability
/// failure). Failing the recovery fast surfaces the version mismatch at the
/// boundary between durable bytes and the in-memory cell, instead of silently
/// poisoning later reads.
///
/// This deliberately does NOT read any meta-CF counter and does NOT inspect the
/// mere presence of a committed `SetFormatVersion` log entry. Durability of an
/// activation is the raft log itself (the entry is fsynced and re-applied
/// deterministically on restart, re-establishing the cell through P5's apply) and
/// the snapshot's leading frame byte (snapshots are encoded at the active write
/// version, carrying it across log purge). This mirrors the project's barrier-seq
/// rule: a safety-critical recovery value derives from fsynced written state, not
/// a non-synced side counter or unapplied log presence.
pub fn recover_active_write_version(
    snapshot_leading_byte: Option<u8>,
    highest_log_record_byte: Option<u8>,
) -> Result<u8, CodecError> {
    for byte in [snapshot_leading_byte, highest_log_record_byte]
        .into_iter()
        .flatten()
    {
        if byte > MAX_READABLE_VERSION {
            return Err(CodecError::VersionUnsupported {
                min: MIN_READABLE_VERSION,
                max: MAX_READABLE_VERSION,
                actual: byte,
            });
        }
    }
    Ok(BASELINE_WRITE_VERSION
        .max(snapshot_leading_byte.unwrap_or(BASELINE_WRITE_VERSION))
        .max(highest_log_record_byte.unwrap_or(BASELINE_WRITE_VERSION)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_constants_are_at_expected_values() {
        assert_eq!(MIN_READABLE_VERSION, 4);
        // MAX is 5 to cover the dense write version (DENSE_WRITE_VERSION).
        assert_eq!(MAX_READABLE_VERSION, 5);
        assert_eq!(BASELINE_WRITE_VERSION, 4);
        assert_eq!(DENSE_WRITE_VERSION, 5);
    }

    #[test]
    fn active_write_version_defaults_to_baseline() {
        let cell = ActiveWriteVersion::default();
        assert_eq!(cell.get(), BASELINE_WRITE_VERSION);
    }

    #[test]
    fn active_write_version_new_seeds_the_given_version() {
        let cell = ActiveWriteVersion::new(5);
        assert_eq!(cell.get(), 5);
    }

    #[test]
    fn active_write_version_set_is_visible_through_a_clone() {
        let writer = ActiveWriteVersion::default();
        let reader = writer.clone();
        writer.set(7);
        assert_eq!(reader.get(), 7);
        assert_eq!(writer.get(), 7);
    }

    #[test]
    fn recover_returns_baseline_with_no_durable_evidence() {
        assert_eq!(
            recover_active_write_version(None, None).unwrap(),
            BASELINE_WRITE_VERSION
        );
    }

    #[test]
    fn recover_takes_the_max_of_present_lower_bounds() {
        // MAX_READABLE_VERSION (5) > BASELINE_WRITE_VERSION (4), so these
        // assertions exercise the genuine multi-version max (4 vs 5).
        assert_eq!(
            recover_active_write_version(Some(MAX_READABLE_VERSION), None).unwrap(),
            MAX_READABLE_VERSION
        );
        assert_eq!(
            recover_active_write_version(None, Some(MAX_READABLE_VERSION)).unwrap(),
            MAX_READABLE_VERSION
        );
        assert_eq!(
            recover_active_write_version(Some(BASELINE_WRITE_VERSION), Some(MAX_READABLE_VERSION))
                .unwrap(),
            MAX_READABLE_VERSION
        );
        assert_eq!(
            recover_active_write_version(Some(MAX_READABLE_VERSION), Some(BASELINE_WRITE_VERSION))
                .unwrap(),
            MAX_READABLE_VERSION
        );
    }

    #[test]
    fn recover_never_drops_below_baseline() {
        assert_eq!(
            recover_active_write_version(Some(1), Some(2)).unwrap(),
            BASELINE_WRITE_VERSION
        );
    }

    #[test]
    fn recover_rejects_snapshot_byte_above_max_readable() {
        let above = MAX_READABLE_VERSION + 1;
        let err = recover_active_write_version(Some(above), None).unwrap_err();
        assert!(
            matches!(
                err,
                CodecError::VersionUnsupported {
                    min: MIN_READABLE_VERSION,
                    max: MAX_READABLE_VERSION,
                    actual,
                } if actual == above,
            ),
            "expected VersionUnsupported for snapshot byte above MAX_READABLE_VERSION, got {err:?}",
        );
    }

    #[test]
    fn recover_rejects_log_byte_above_max_readable() {
        let above = MAX_READABLE_VERSION + 1;
        let err = recover_active_write_version(None, Some(above)).unwrap_err();
        assert!(
            matches!(
                err,
                CodecError::VersionUnsupported {
                    min: MIN_READABLE_VERSION,
                    max: MAX_READABLE_VERSION,
                    actual,
                } if actual == above,
            ),
            "expected VersionUnsupported for log byte above MAX_READABLE_VERSION, got {err:?}",
        );
    }

    #[test]
    fn recover_accepts_byte_at_max_readable() {
        assert_eq!(
            recover_active_write_version(Some(MAX_READABLE_VERSION), Some(MAX_READABLE_VERSION))
                .unwrap(),
            MAX_READABLE_VERSION,
        );
    }
}
