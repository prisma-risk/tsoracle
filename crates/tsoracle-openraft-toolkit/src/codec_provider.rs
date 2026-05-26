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

//! Orphan-rule seam between the toolkit's generic `RocksdbLogStore` and the
//! concrete openraft types it persists.
//!
//! The toolkit cannot `impl tsoracle_codec::VersionedCodec` for openraft's
//! `Entry`/`Vote`/`LogId` (both the trait and the types are foreign here), and
//! neither can a driver. So the toolkit defines this *toolkit-local* provider
//! trait; a driver supplies a local marker type that implements it (satisfying
//! the orphan rule), and `RocksdbLogStore<C, K, Codec>` dispatches the *body*
//! codec through it. The toolkit keeps ownership of the version-byte framing
//! (see `super::encode_entry_record` / `super::decode_entry_record` and the
//! vote/log-id analogues), so the provider only ever sees and returns the bare
//! postcard body — never the leading version byte.
//!
//! [`DefaultLogStoreCodec`] is the behavior-preserving default: it postcards
//! each value whole, exactly as the pre-seam codec did, and is the default
//! `Codec` type parameter so existing call sites compile unchanged.

use openraft::RaftTypeConfig;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tsoracle_codec::{CodecError, decode_postcard_exact, encode_postcard};

/// Provider of the *body* codec for the three openraft types `RocksdbLogStore`
/// persists: the log `Entry`, the `Vote`, and the `LogId` meta singletons.
///
/// Every method operates on the bare body (the bytes after the leading version
/// byte); the toolkit adds and validates that version byte itself. `version` is
/// passed through so a provider can dispatch a per-version body layout in a
/// later phase; in P1b the only version in play is the current `SCHEMA_VERSION`.
///
/// The methods take no `self`: a `Codec` is a pure type-level marker selected
/// at the `RocksdbLogStore<C, K, Codec>` type parameter, never an instance.
pub trait LogStoreCodec<C: RaftTypeConfig> {
    /// Encode a log `Entry` body at `version`.
    fn encode_entry(version: u8, entry: &C::Entry) -> Result<Vec<u8>, CodecError>;
    /// Decode a log `Entry` body known to be `version`.
    fn decode_entry(version: u8, body: &[u8]) -> Result<C::Entry, CodecError>;
    /// Encode a `Vote` body at `version`.
    fn encode_vote(version: u8, vote: &VoteOf<C>) -> Result<Vec<u8>, CodecError>;
    /// Decode a `Vote` body known to be `version`.
    fn decode_vote(version: u8, body: &[u8]) -> Result<VoteOf<C>, CodecError>;
    /// Encode a `LogId` body at `version`.
    fn encode_log_id(version: u8, log_id: &LogIdOf<C>) -> Result<Vec<u8>, CodecError>;
    /// Decode a `LogId` body known to be `version`.
    fn decode_log_id(version: u8, body: &[u8]) -> Result<LogIdOf<C>, CodecError>;
}

/// The default, behavior-preserving [`LogStoreCodec`]: whole-value postcard for
/// every type, identical bytes to the pre-seam `encode_record`/`decode_record`.
///
/// Zero-sized; it exists only as the default `Codec` type parameter of
/// [`super::RocksdbLogStore`] so the toolkit's own consumers and tests need no
/// change. `version` is accepted for trait-shape symmetry but ignored — there
/// is a single body layout in P1b.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultLogStoreCodec;

impl<C> LogStoreCodec<C> for DefaultLogStoreCodec
where
    C: RaftTypeConfig,
    C::Entry: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
{
    fn encode_entry(_version: u8, entry: &C::Entry) -> Result<Vec<u8>, CodecError> {
        encode_postcard(entry)
    }
    fn decode_entry(_version: u8, body: &[u8]) -> Result<C::Entry, CodecError> {
        decode_postcard_exact(body)
    }
    fn encode_vote(_version: u8, vote: &VoteOf<C>) -> Result<Vec<u8>, CodecError> {
        encode_postcard(vote)
    }
    fn decode_vote(_version: u8, body: &[u8]) -> Result<VoteOf<C>, CodecError> {
        decode_postcard_exact(body)
    }
    fn encode_log_id(_version: u8, log_id: &LogIdOf<C>) -> Result<Vec<u8>, CodecError> {
        encode_postcard(log_id)
    }
    fn decode_log_id(_version: u8, body: &[u8]) -> Result<LogIdOf<C>, CodecError> {
        decode_postcard_exact(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::declare_raft_types_ext;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbePeer {
        addr: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeData {
        n: u64,
    }

    impl std::fmt::Display for ProbeData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ProbeData({})", self.n)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeApplied;

    declare_raft_types_ext! {
        pub ProbeConfig:
            Node            = ProbePeer,
            AppData         = ProbeData,
            AppDataResponse = ProbeApplied,
            SnapshotData    = std::io::Cursor<Vec<u8>>,
    }

    #[test]
    fn default_codec_vote_body_matches_raw_postcard() {
        // The provider must emit the SAME bytes the pre-seam codec did: a bare
        // whole-value postcard, no version byte. This is the behavior-preserving
        // contract of `DefaultLogStoreCodec`.
        let vote: VoteOf<ProbeConfig> = openraft::Vote::new_committed(7, 3);
        let via_provider =
            <DefaultLogStoreCodec as LogStoreCodec<ProbeConfig>>::encode_vote(3, &vote)
                .expect("encode vote");
        let raw = postcard::to_stdvec(&vote).expect("raw postcard");
        assert_eq!(via_provider, raw);
        let back: VoteOf<ProbeConfig> =
            <DefaultLogStoreCodec as LogStoreCodec<ProbeConfig>>::decode_vote(3, &via_provider)
                .expect("decode vote");
        assert_eq!(back, vote);
    }

    #[test]
    fn default_codec_rejects_trailing_bytes() {
        // Inherits `decode_postcard_exact`'s trailing-byte rejection, matching
        // the old `decode_record` corruption contract.
        let vote: VoteOf<ProbeConfig> = openraft::Vote::new_committed(1, 1);
        let mut body =
            <DefaultLogStoreCodec as LogStoreCodec<ProbeConfig>>::encode_vote(3, &vote).unwrap();
        body.extend_from_slice(&[0xAB, 0xCD]);
        assert!(matches!(
            <DefaultLogStoreCodec as LogStoreCodec<ProbeConfig>>::decode_vote(3, &body),
            Err(CodecError::TrailingBytes { extra: 2 })
        ));
    }
}
