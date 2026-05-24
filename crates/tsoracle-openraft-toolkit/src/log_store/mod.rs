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

//! RocksDB-backed `RaftLogStorage` implementation.

pub mod key_space;
mod meta;

pub use key_space::{Flat, GroupPrefixed, KeySpace, MetaLabel};

use thiserror::Error;

/// Errors produced by the rocksdb-backed log store.
#[derive(Debug, Error)]
pub enum RocksdbLogStoreError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("decode error: {0}")]
    Decode(#[from] postcard::Error),
    #[error("column family `{0}` not found")]
    MissingColumnFamily(String),
}

use std::fmt;
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogIdOptionExt;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use rocksdb::{BoundColumnFamily, DB, IteratorMode, WriteBatch, WriteOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// RocksDB-backed `RaftLogStorage` implementation.
///
/// Parameterized by:
/// - `C`: the consumer's `RaftTypeConfig`.
/// - `K`: the active [`KeySpace`] — [`Flat`] for single-group deployments,
///   [`GroupPrefixed`] for multi-group deployments that multiplex N raft
///   instances onto shared column families.
///
/// `Arc<DB>` is `Clone`, so the store can be cloned cheaply to satisfy
/// `RaftLogStorage::LogReader = Self`. All rocksdb operations take `&DB`, so
/// the `&mut self` on storage methods is purely a trait shape — no internal
/// locking required.
///
/// Construct via [`RocksdbLogStore::open`], which validates that the two
/// column-family names you pass already exist on the database.
pub struct RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    db: Arc<DB>,
    log_cf: String,
    meta_cf: String,
    keys: K,
    _phantom: PhantomData<C>,
}

impl<C, K> RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    /// Open a log store on top of an already-opened `DB`. Both column families
    /// must already exist; `open_cf_descriptors` should have created them when
    /// the database was opened.
    pub fn open(
        db: Arc<DB>,
        log_cf: impl Into<String>,
        meta_cf: impl Into<String>,
        keys: K,
    ) -> Result<Self, RocksdbLogStoreError> {
        let log_cf = log_cf.into();
        let meta_cf = meta_cf.into();
        db.cf_handle(&log_cf)
            .ok_or_else(|| RocksdbLogStoreError::MissingColumnFamily(log_cf.clone()))?;
        db.cf_handle(&meta_cf)
            .ok_or_else(|| RocksdbLogStoreError::MissingColumnFamily(meta_cf.clone()))?;
        Ok(Self {
            db,
            log_cf,
            meta_cf,
            keys,
            _phantom: PhantomData,
        })
    }

    #[expect(
        clippy::expect_used,
        reason = "`self.log_cf` is created and validated by `open` before this struct is constructed; `cf_handle` cannot return `None` here unless the DB is corrupted underneath us, in which case panicking is the right outcome."
    )]
    pub(super) fn log_cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db
            .cf_handle(&self.log_cf)
            .expect("log CF was validated at open")
    }

    #[expect(
        clippy::expect_used,
        reason = "`self.meta_cf` is created and validated by `open` before this struct is constructed; `cf_handle` cannot return `None` here unless the DB is corrupted underneath us, in which case panicking is the right outcome."
    )]
    pub(super) fn meta_cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db
            .cf_handle(&self.meta_cf)
            .expect("meta CF was validated at open")
    }

    /// `WriteOptions` with `set_sync(true)`, fsyncing the WAL before the write
    /// returns.
    ///
    /// Every log-store mutation that openraft may treat as durably acknowledged
    /// is written through this: `save_vote`, `append`, `truncate_after`, and
    /// `purge`. Each is a single atomic `WriteBatch`, so a crash always loses a
    /// whole op cleanly; the reason to fsync is durability *skew* across ops.
    /// openraft's recovery contract assumes these survive a crash together — if
    /// a synced `save_vote` persisted while an acknowledged `truncate_after` or
    /// `purge` were lost, recovery would present a durable new-term vote beside
    /// a non-durable, un-truncated (or un-purged) log, a combination openraft
    /// expects to be durably consistent. `save_committed` is the lone exception
    /// (its record is optional and flushed lazily by the next synced write); it
    /// deliberately uses the non-synced `db.write` and says so at its call site.
    fn write_sync_opts() -> WriteOptions {
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        wo
    }
}

impl<C, K> Clone for RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            log_cf: self.log_cf.clone(),
            meta_cf: self.meta_cf.clone(),
            keys: self.keys.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<C, K> fmt::Debug for RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksdbLogStore")
            .field("log_cf", &self.log_cf)
            .field("meta_cf", &self.meta_cf)
            .field("keys", &self.keys)
            .finish()
    }
}

fn range_boundary<RB: RangeBounds<u64>>(range: RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&n) => n.saturating_add(1),
        Bound::Excluded(&n) => n,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

/// Encode a log record as `[SCHEMA_VERSION | postcard(value)]`.
fn encode_record<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    crate::codec::encode(crate::codec::SCHEMA_VERSION, value).map_err(codec_io_error)
}

/// Decode a record framed by [`encode_record`], rejecting a foreign version.
fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    crate::codec::decode(crate::codec::SCHEMA_VERSION, bytes).map_err(codec_io_error)
}

/// Map a codec failure to `io::Error`, surfacing a version mismatch as
/// `InvalidData` so a foreign-format record is a distinguishable, structured
/// error rather than a generic decode failure.
fn codec_io_error(err: crate::codec::CodecError) -> io::Error {
    match err {
        e @ crate::codec::CodecError::Version { .. } => {
            io::Error::new(io::ErrorKind::InvalidData, e)
        }
        other => io::Error::other(other),
    }
}

impl<C, K> RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
    C::Entry: Serialize + DeserializeOwned,
{
    /// Scan the log CF in reverse and return the highest-index `LogId`, or
    /// `None` if the log range is empty. The full log id (not just the index)
    /// is read out of the encoded `Entry`'s `log_id` field.
    ///
    /// For `GroupPrefixed` the log CF can host multiple raft groups. The
    /// reverse iterator from `hi` will happily walk back into a neighbouring
    /// group's bytes if the current group is empty, so the first item must
    /// also be bounded by `lo`. Once we observe a key below `lo` the iterator
    /// is monotonically decreasing and can never re-enter our range, so
    /// returning `None` is safe.
    fn last_log_id_in_cf(&self) -> io::Result<Option<LogIdOf<C>>> {
        let cf = self.log_cf_handle();
        let (lo, hi) = self.keys.log_range();
        let mut it = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&hi, rocksdb::Direction::Reverse));
        let Some(item) = it.next() else {
            return Ok(None);
        };
        let (k, v) = item.map_err(io::Error::other)?;
        if &*k < lo.as_slice() {
            return Ok(None);
        }
        let entry: C::Entry = decode_record(&v)?;
        Ok(Some(entry.log_id()))
    }
}

impl<C, K> RaftLogReader<C> for RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
    C::Entry: Serialize + DeserializeOwned,
{
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<C::Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let (start, end) = range_boundary(range);
        if start >= end {
            return Ok(Vec::new());
        }

        let cf = self.log_cf_handle();
        let start_key = self.keys.log_key(start);
        let end_key = self.keys.log_key(end);
        let it = self.db.iterator_cf(
            &cf,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut out = Vec::new();
        for item in it {
            let (k, v) = item.map_err(io::Error::other)?;
            // Stop as soon as we cross `end` (exclusive) or leave the keyspace
            // range — `GroupPrefixed` shares its CF with other groups so the
            // iterator can walk into the next group's bytes if we don't break.
            if &*k >= end_key.as_slice() {
                break;
            }
            let entry: C::Entry = decode_record(&v)?;
            out.push(entry);
        }
        Ok(out)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        let cf = self.meta_cf_handle();
        meta::read::<VoteOf<C>, K>(&self.db, &cf, &self.keys, MetaLabel::Vote)
            .map_err(io::Error::other)
    }
}

impl<C, K> RaftLogStorage<C> for RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
    C::Entry: Serialize + DeserializeOwned,
{
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let cf_meta = self.meta_cf_handle();
        let last_purged_log_id: Option<LogIdOf<C>> =
            meta::read::<LogIdOf<C>, K>(&self.db, &cf_meta, &self.keys, MetaLabel::LastPurged)
                .map_err(io::Error::other)?;

        let last_in_log = self.last_log_id_in_cf()?;
        let last_log_id = last_in_log.or_else(|| last_purged_log_id.clone());

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        let cf_meta = self.meta_cf_handle();
        let mut batch = WriteBatch::default();
        meta::put::<VoteOf<C>, K>(&mut batch, &cf_meta, &self.keys, MetaLabel::Vote, vote)
            .map_err(io::Error::other)?;
        // fsync: the vote must be durable before it is acknowledged, or a crash
        // could let this node vote twice in one term and split the cluster.
        let wo = Self::write_sync_opts();
        self.db.write_opt(batch, &wo).map_err(io::Error::other)?;
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let cf_meta = self.meta_cf_handle();
        let mut batch = WriteBatch::default();
        match committed {
            Some(committed) => meta::put::<LogIdOf<C>, K>(
                &mut batch,
                &cf_meta,
                &self.keys,
                MetaLabel::Committed,
                &committed,
            )
            .map_err(io::Error::other)?,
            None => meta::delete::<K>(&mut batch, &cf_meta, &self.keys, MetaLabel::Committed),
        }
        // No fsync (the deliberate exception to `write_sync_opts`): persisting
        // the committed id is optional per the openraft contract. The next
        // append's sync flushes this record along with the batch.
        self.db.write(batch).map_err(io::Error::other)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        let cf_meta = self.meta_cf_handle();
        meta::read::<LogIdOf<C>, K>(&self.db, &cf_meta, &self.keys, MetaLabel::Committed)
            .map_err(io::Error::other)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let cf_log = self.log_cf_handle();
        let mut batch = WriteBatch::default();
        for entry in entries {
            let (_leader, idx) = entry.log_id_parts();
            let key = self.keys.log_key(idx);
            let value = encode_record(&entry)?;
            batch.put_cf(&cf_log, &key, &value);
        }

        // fsync: openraft treats the `IOFlushed` callback below as a durability
        // signal, so the entries must reach disk before completion is reported.
        let wo = Self::write_sync_opts();
        crate::failpoint!("tsoracle_openraft_toolkit::log_store::before_write_batch");
        let result = self.db.write_opt(batch, &wo).map_err(io::Error::other);

        crate::failpoint!(
            "tsoracle_openraft_toolkit::log_store::after_write_before_sync",
            |_arg: Option<String>| -> Result<(), io::Error> {
                Err(io::Error::other(
                    "failpoint: tsoracle_openraft_toolkit::log_store::after_write_before_sync",
                ))
            }
        );

        match &result {
            Ok(()) => callback.io_completed(Ok(())),
            Err(e) => callback.io_completed(Err(io::Error::other(e.to_string()))),
        }
        result
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        // truncate_after(None)        => delete everything (start at 0)
        // truncate_after(Some(log_id)) => keep up to and including log_id
        let truncate_at = last_log_id.next_index();
        let cf_log = self.log_cf_handle();
        let start_key = self.keys.log_key(truncate_at);
        let (_lo, hi) = self.keys.log_range();
        let it = self.db.iterator_cf(
            &cf_log,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut batch = WriteBatch::default();
        for item in it {
            let (k, _v) = item.map_err(io::Error::other)?;
            if &*k > hi.as_slice() {
                break;
            }
            batch.delete_cf(&cf_log, &k);
        }
        // fsync: a lost truncate would resurrect conflicting tail entries on
        // recovery, contradicting the durable vote/append that drove it (see
        // `write_sync_opts`).
        let wo = Self::write_sync_opts();
        crate::failpoint!("tsoracle_openraft_toolkit::log_store::truncate::before_write_batch");
        self.db.write_opt(batch, &wo).map_err(io::Error::other)?;
        crate::failpoint!(
            "tsoracle_openraft_toolkit::log_store::truncate::after_write_before_sync",
            |_arg: Option<String>| -> Result<(), io::Error> {
                Err(io::Error::other(
                    "failpoint: tsoracle_openraft_toolkit::log_store::truncate::after_write_before_sync",
                ))
            }
        );
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        let cf_log = self.log_cf_handle();
        let cf_meta = self.meta_cf_handle();

        let mut batch = WriteBatch::default();
        let (lo, _hi) = self.keys.log_range();
        let stop_at = self.keys.log_key(log_id.index);
        let it = self.db.iterator_cf(
            &cf_log,
            IteratorMode::From(&lo, rocksdb::Direction::Forward),
        );
        for item in it {
            let (k, _v) = item.map_err(io::Error::other)?;
            if &*k > stop_at.as_slice() {
                break;
            }
            batch.delete_cf(&cf_log, &k);
        }

        meta::put::<LogIdOf<C>, K>(
            &mut batch,
            &cf_meta,
            &self.keys,
            MetaLabel::LastPurged,
            &log_id,
        )
        .map_err(io::Error::other)?;

        // fsync: the prefix deletes and the `LastPurged` marker share one
        // atomic batch; losing them would let recovery rebuild log state from a
        // prefix openraft believes is already purged (see `write_sync_opts`).
        let wo = Self::write_sync_opts();
        crate::failpoint!("tsoracle_openraft_toolkit::log_store::purge::before_write_batch");
        self.db.write_opt(batch, &wo).map_err(io::Error::other)?;
        crate::failpoint!(
            "tsoracle_openraft_toolkit::log_store::purge::after_write_before_sync",
            |_arg: Option<String>| -> Result<(), io::Error> {
                Err(io::Error::other(
                    "failpoint: tsoracle_openraft_toolkit::log_store::purge::after_write_before_sync",
                ))
            }
        );
        Ok(())
    }
}

#[cfg(test)]
mod range_boundary_tests {
    use super::range_boundary;
    use proptest::prelude::*;
    use std::ops::Bound;

    proptest! {
        // The half-open range form `a..b` is the canonical case used by
        // `try_get_log_entries`. The output must equal `(a, b)` exactly so
        // iteration starts at `a` and stops before `b`.
        #[test]
        fn half_open_range_passes_through(a in any::<u64>(), b in any::<u64>()) {
            prop_assert_eq!(range_boundary(a..b), (a, b));
        }

        // Inclusive end form `a..=b` becomes `(a, b.saturating_add(1))`. At
        // u64::MAX the saturation collapses the range to `(a, u64::MAX)` —
        // the highest index then becomes unreachable. That is the documented
        // (and intentional) limit; pinning it here means any future refactor
        // that "fixes" the saturation by widening the output to u128 has to
        // confront this test first.
        #[test]
        fn inclusive_end_saturates_at_max(a in any::<u64>(), b in any::<u64>()) {
            prop_assert_eq!(range_boundary(a..=b), (a, b.saturating_add(1)));
        }

        // Excluded start form (Bound::Excluded) bumps the start by one with
        // saturation. Range types in std don't expose this directly, so the
        // test constructs it through the trait directly.
        #[test]
        fn excluded_start_saturates_at_max(a in any::<u64>(), b in any::<u64>()) {
            let r = (Bound::Excluded(a), Bound::Excluded(b));
            prop_assert_eq!(range_boundary(r), (a.saturating_add(1), b));
        }

        // Unbounded sides default to (0, u64::MAX). Combined with explicit
        // bounds on the other side, the open side keeps its default.
        #[test]
        fn open_start_defaults_to_zero(b in any::<u64>()) {
            prop_assert_eq!(range_boundary(..b), (0, b));
        }

        #[test]
        fn open_end_defaults_to_u64_max(a in any::<u64>()) {
            prop_assert_eq!(range_boundary(a..), (a, u64::MAX));
        }
    }

    #[test]
    fn fully_unbounded_range_is_full_u64_space() {
        assert_eq!(range_boundary::<std::ops::RangeFull>(..), (0, u64::MAX));
    }
}

#[cfg(test)]
mod record_codec_tests {
    use super::{decode_record, encode_record};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Rec {
        v: u64,
    }

    #[test]
    fn encode_record_stamps_schema_version_and_roundtrips() {
        let bytes = encode_record(&Rec { v: 5 }).expect("encode");
        assert_eq!(bytes[0], crate::codec::SCHEMA_VERSION);
        let back: Rec = decode_record(&bytes).expect("decode");
        assert_eq!(back, Rec { v: 5 });
    }

    #[test]
    fn decode_record_rejects_foreign_version() {
        // A 0xFF-framed record must surface as InvalidData rather than a
        // successful-but-wrong decode.
        let err = decode_record::<Rec>(&[0xFF, 5]).expect_err("must reject");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
