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

//! Internal helpers for the meta column family.
//!
//! The meta CF holds four small openraft values: current vote, last-committed
//! log id, last-purged log id, last-applied membership. Each is keyed by a
//! [`MetaLabel`](super::MetaLabel) via the active [`KeySpace`](super::KeySpace).
//! The vote and log-id helpers frame their values through the toolkit's
//! provider-backed record helpers ([`super::encode_vote_record`] /
//! [`super::decode_vote_record`] and the log-id analogues), so a
//! recovery-critical meta record loud-rejects on a version mismatch instead of
//! misdecoding silently. rocksdb errors are mapped to [`io::Error`] to match
//! the storage trait surface.

use openraft::RaftTypeConfig;
use openraft::type_config::alias::{LogIdOf, VoteOf};
use rocksdb::{BoundColumnFamily, DB, WriteBatch};
use std::io;
use std::sync::Arc;

use crate::codec_provider::LogStoreCodec;

use super::key_space::{KeySpace, MetaLabel};
use super::{decode_log_id_record, decode_vote_record, encode_log_id_record, encode_vote_record};

pub(super) fn read_vote<C, K, Codec>(
    db: &DB,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
) -> io::Result<Option<VoteOf<C>>>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C>,
{
    let key = keys.meta_key(label);
    match db.get_pinned_cf(cf, &key).map_err(io::Error::other)? {
        Some(bytes) => Ok(Some(decode_vote_record::<C, Codec>(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) fn read_log_id<C, K, Codec>(
    db: &DB,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
) -> io::Result<Option<LogIdOf<C>>>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C>,
{
    let key = keys.meta_key(label);
    match db.get_pinned_cf(cf, &key).map_err(io::Error::other)? {
        Some(bytes) => Ok(Some(decode_log_id_record::<C, Codec>(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) fn put_vote<C, K, Codec>(
    batch: &mut WriteBatch,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
    value: &VoteOf<C>,
) -> io::Result<()>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C>,
{
    let key = keys.meta_key(label);
    let bytes = encode_vote_record::<C, Codec>(value)?;
    batch.put_cf(cf, &key, &bytes);
    Ok(())
}

pub(super) fn put_log_id<C, K, Codec>(
    batch: &mut WriteBatch,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
    value: &LogIdOf<C>,
) -> io::Result<()>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C>,
{
    let key = keys.meta_key(label);
    let bytes = encode_log_id_record::<C, Codec>(value)?;
    batch.put_cf(cf, &key, &bytes);
    Ok(())
}

pub(super) fn delete<K: KeySpace>(
    batch: &mut WriteBatch,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
) {
    let key = keys.meta_key(label);
    batch.delete_cf(cf, &key);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::declare_raft_types_ext;
    use crate::log_store::{DefaultLogStoreCodec, Flat, KeySpace, MetaLabel};
    use openraft::type_config::alias::VoteOf;
    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MetaPeer {
        addr: String,
    }
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MetaData {
        v: u64,
    }
    impl std::fmt::Display for MetaData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MetaData({})", self.v)
        }
    }
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MetaApplied;

    declare_raft_types_ext! {
        pub MetaConfig:
            Node            = MetaPeer,
            AppData         = MetaData,
            AppDataResponse = MetaApplied,
            SnapshotData    = std::io::Cursor<Vec<u8>>,
    }

    #[test]
    fn meta_vote_roundtrips_through_provider_with_version_byte() {
        let dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![ColumnFamilyDescriptor::new("meta", Options::default())];
        let db = DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap();
        let keys = Flat;
        let cf = db.cf_handle("meta").unwrap();

        let vote: VoteOf<MetaConfig> = openraft::Vote::new_committed(7, 3);
        let mut batch = rocksdb::WriteBatch::default();
        put_vote::<MetaConfig, Flat, DefaultLogStoreCodec>(
            &mut batch,
            &cf,
            &keys,
            MetaLabel::Vote,
            &vote,
        )
        .unwrap();
        db.write(batch).unwrap();

        // On-disk leading byte is SCHEMA_VERSION.
        let raw = db
            .get_cf(&cf, keys.meta_key(MetaLabel::Vote))
            .unwrap()
            .unwrap();
        assert_eq!(raw[0], crate::codec::SCHEMA_VERSION);

        let back: Option<VoteOf<MetaConfig>> =
            read_vote::<MetaConfig, Flat, DefaultLogStoreCodec>(&db, &cf, &keys, MetaLabel::Vote)
                .unwrap();
        assert_eq!(back, Some(vote));
    }
}
