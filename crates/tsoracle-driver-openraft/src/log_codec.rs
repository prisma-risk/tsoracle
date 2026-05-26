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

//! Concrete [`LogStoreCodec`] provider for this driver's [`TypeConfig`].
//!
//! The orphan rule forbids implementing `tsoracle_codec::VersionedCodec` for
//! openraft's `Entry`/`Vote`/`LogId` directly (foreign trait, foreign types),
//! so the toolkit exposes the local [`LogStoreCodec`] provider trait and this
//! driver supplies the marker that implements it. The v4 arms postcard each
//! openraft value whole via `encode_postcard`/`decode_postcard_exact`,
//! byte-identical to the toolkit's [`tsoracle_openraft_toolkit::DefaultLogStoreCodec`] —
//! `RocksdbLogStore` frames the version byte, this provider only handles the
//! body.

use tsoracle_codec::{CodecError, decode_postcard_exact, encode_postcard};
use tsoracle_openraft_toolkit::LogStoreCodec;

use crate::type_config::{OpenraftLogId, OpenraftVote, TypeConfig};

/// The [`LogStoreCodec`] provider this driver wires into `RocksdbLogStore`.
///
/// Zero-sized type-level marker. Behavior-preserving: every body is the same
/// whole-value postcard the store wrote before the seam existed.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenraftLogCodec;

impl LogStoreCodec<TypeConfig> for OpenraftLogCodec {
    fn encode_entry(
        _version: u8,
        entry: &<TypeConfig as openraft::RaftTypeConfig>::Entry,
    ) -> Result<Vec<u8>, CodecError> {
        encode_postcard(entry)
    }

    fn decode_entry(
        _version: u8,
        body: &[u8],
    ) -> Result<<TypeConfig as openraft::RaftTypeConfig>::Entry, CodecError> {
        decode_postcard_exact(body)
    }

    fn encode_vote(_version: u8, vote: &OpenraftVote) -> Result<Vec<u8>, CodecError> {
        encode_postcard(vote)
    }

    fn decode_vote(_version: u8, body: &[u8]) -> Result<OpenraftVote, CodecError> {
        decode_postcard_exact(body)
    }

    fn encode_log_id(_version: u8, log_id: &OpenraftLogId) -> Result<Vec<u8>, CodecError> {
        encode_postcard(log_id)
    }

    fn decode_log_id(_version: u8, body: &[u8]) -> Result<OpenraftLogId, CodecError> {
        decode_postcard_exact(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_entry::HighWaterCommand;
    use tsoracle_consensus::AdvancePayload;

    #[test]
    fn entry_body_matches_raw_postcard() {
        // The driver provider must agree byte-for-byte with raw postcard (and
        // therefore with the toolkit DefaultLogStoreCodec), proving the seam is
        // behavior-preserving.
        let lid = openraft::testing::log_id::<TypeConfig>(1, 1, 1);
        let entry: <TypeConfig as openraft::RaftTypeConfig>::Entry =
            openraft::entry::RaftEntry::new_normal(
                lid,
                HighWaterCommand::Advance(AdvancePayload { at_least: 5 }),
            );
        let via_provider =
            <OpenraftLogCodec as LogStoreCodec<TypeConfig>>::encode_entry(4, &entry).unwrap();
        let raw = postcard::to_stdvec(&entry).unwrap();
        assert_eq!(via_provider, raw);
    }

    #[test]
    fn vote_body_matches_raw_postcard() {
        let vote: OpenraftVote = openraft::Vote::new_committed(7, 3);
        let via_provider =
            <OpenraftLogCodec as LogStoreCodec<TypeConfig>>::encode_vote(4, &vote).unwrap();
        assert_eq!(via_provider, postcard::to_stdvec(&vote).unwrap());
        let back: OpenraftVote =
            <OpenraftLogCodec as LogStoreCodec<TypeConfig>>::decode_vote(4, &via_provider).unwrap();
        assert_eq!(back, vote);
    }

    #[test]
    fn log_id_body_matches_raw_postcard() {
        let log_id: OpenraftLogId = openraft::testing::log_id::<TypeConfig>(2, 1, 9);
        let via_provider =
            <OpenraftLogCodec as LogStoreCodec<TypeConfig>>::encode_log_id(4, &log_id).unwrap();
        assert_eq!(via_provider, postcard::to_stdvec(&log_id).unwrap());
    }
}
