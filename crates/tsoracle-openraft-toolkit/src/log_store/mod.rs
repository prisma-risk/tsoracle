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

//! RocksDB-backed `RaftLogStorage` implementation.

pub mod key_space;
mod meta;

pub use crate::codec_provider::{DefaultLogStoreCodec, LogStoreCodec};
pub use key_space::{Flat, GroupPrefixed, KeySpace, MetaLabel};

use thiserror::Error;

/// Errors produced opening the rocksdb-backed log store.
///
/// Construction (`open`) only fails when a named column family is absent;
/// every later storage operation reports through `io::Error` to match the
/// `RaftLogStorage` trait surface (decode/version failures route through the
/// `[active write version | postcard]` codec, surfacing an out-of-range version
/// as `InvalidData`).
#[derive(Debug, Error)]
pub enum RocksdbLogStoreError {
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

/// RocksDB-backed `RaftLogStorage` implementation.
///
/// Parameterized by:
/// - `C`: the consumer's `RaftTypeConfig`.
/// - `K`: the active [`KeySpace`] — [`Flat`] for single-group deployments,
///   [`GroupPrefixed`] for multi-group deployments that multiplex N raft
///   instances onto shared column families.
/// - `Codec`: the [`LogStoreCodec`] body provider. The toolkit owns the
///   leading version-byte framing; this codec produces and consumes only the
///   bare body. Defaults to [`DefaultLogStoreCodec`], a behavior-preserving
///   whole-value postcard provider, so toolkit-internal call sites compile
///   unchanged. Drivers supply their own marker to satisfy the orphan rule.
///
/// `Arc<DB>` is `Clone`, so the store can be cloned cheaply to satisfy
/// `RaftLogStorage::LogReader = Self`. All rocksdb operations take `&DB`, so
/// the `&mut self` on storage methods is purely a trait shape — no internal
/// locking required.
///
/// Construct via [`RocksdbLogStore::open`], which validates that the two
/// column-family names you pass already exist on the database.
pub struct RocksdbLogStore<C, K, Codec = DefaultLogStoreCodec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: 'static,
{
    db: Arc<DB>,
    log_cf: String,
    meta_cf: String,
    keys: K,
    /// Shared active write version: supplies the `version` argument every record
    /// encode stamps. Constructed once at bootstrap and shared with the state
    /// machine (and, in P3, the wire sender) so all writers emit the same
    /// version. Defaults to a fresh `BASELINE_WRITE_VERSION` cell for
    /// constructors that do not run the bootstrap recovery seed.
    active_write_version: crate::codec::ActiveWriteVersion,
    // `fn() -> Codec` is a type-level marker that's always `Send + Sync` no
    // matter what `Codec` is — the struct holds no actual `Codec` value.
    _phantom: PhantomData<(C, fn() -> Codec)>,
}

impl<C, K, Codec> RocksdbLogStore<C, K, Codec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: 'static,
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
            active_write_version: crate::codec::ActiveWriteVersion::default(),
            _phantom: PhantomData,
        })
    }

    /// Replace the store's active-write-version cell with a shared one.
    ///
    /// Bootstrap constructs a single
    /// [`ActiveWriteVersion`](crate::codec::ActiveWriteVersion), seeds it via
    /// [`recover_active_write_version`](crate::codec::recover_active_write_version),
    /// and threads the same cell here and into the state machine so both stamp
    /// writes from one source of truth.
    pub fn with_active_write_version(
        mut self,
        active_write_version: crate::codec::ActiveWriteVersion,
    ) -> Self {
        self.active_write_version = active_write_version;
        self
    }

    /// The version this store currently stamps onto appended records.
    pub fn active_write_version(&self) -> u8 {
        self.active_write_version.get()
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

impl<C, K, Codec> Clone for RocksdbLogStore<C, K, Codec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: 'static,
{
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            log_cf: self.log_cf.clone(),
            meta_cf: self.meta_cf.clone(),
            keys: self.keys.clone(),
            active_write_version: self.active_write_version.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<C, K, Codec> fmt::Debug for RocksdbLogStore<C, K, Codec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksdbLogStore")
            .field("log_cf", &self.log_cf)
            .field("meta_cf", &self.meta_cf)
            .field("keys", &self.keys)
            .field("active_write_version", &self.active_write_version.get())
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

/// Prepend the toolkit's version byte to a provider-produced body.
///
/// The toolkit owns framing; `Codec` owns the body. The `version` argument is
/// the active write version supplied by the caller (the log store reads it from
/// its [`ActiveWriteVersion`](crate::codec::ActiveWriteVersion) cell).
fn frame_with_version(version: u8, body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(version);
    out.extend_from_slice(&body);
    out
}

/// Split a framed record into `(version, body)`, rejecting an out-of-range
/// version before any body parse. Accepts any version in the inclusive readable
/// range `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]`; the returned `version`
/// is the leading byte, dispatched to a per-version body parser by the codec.
fn unframe_with_version(bytes: &[u8]) -> io::Result<(u8, &[u8])> {
    let (first, body) = bytes.split_first().ok_or_else(|| {
        crate::codec::codec_io_error("log-store record decode", crate::codec::CodecError::Empty)
    })?;
    let version = *first;
    if !(crate::codec::MIN_READABLE_VERSION..=crate::codec::MAX_READABLE_VERSION).contains(&version)
    {
        return Err(crate::codec::codec_io_error(
            "log-store record decode",
            crate::codec::CodecError::Version {
                expected: crate::codec::MAX_READABLE_VERSION,
                actual: version,
            },
        ));
    }
    Ok((version, body))
}

/// Encode a log `Entry` as `[version | Codec::encode_entry(version, ..)]`.
fn encode_entry_record<C, Codec>(version: u8, entry: &C::Entry) -> io::Result<Vec<u8>>
where
    C: RaftTypeConfig,
    Codec: LogStoreCodec<C>,
{
    let body = Codec::encode_entry(version, entry)
        .map_err(|err| crate::codec::codec_io_error("log-store record encode", err))?;
    Ok(frame_with_version(version, body))
}

/// Decode a log `Entry` framed by [`encode_entry_record`], rejecting an
/// out-of-range version.
fn decode_entry_record<C, Codec>(bytes: &[u8]) -> io::Result<C::Entry>
where
    C: RaftTypeConfig,
    Codec: LogStoreCodec<C>,
{
    let (version, body) = unframe_with_version(bytes)?;
    Codec::decode_entry(version, body)
        .map_err(|err| crate::codec::codec_io_error("log-store record decode", err))
}

/// Encode a `Vote` as `[version | Codec::encode_vote(version, ..)]`.
fn encode_vote_record<C, Codec>(version: u8, vote: &VoteOf<C>) -> io::Result<Vec<u8>>
where
    C: RaftTypeConfig,
    Codec: LogStoreCodec<C>,
{
    let body = Codec::encode_vote(version, vote)
        .map_err(|err| crate::codec::codec_io_error("log-store record encode", err))?;
    Ok(frame_with_version(version, body))
}

/// Decode a `Vote` framed by [`encode_vote_record`], rejecting an out-of-range
/// version.
fn decode_vote_record<C, Codec>(bytes: &[u8]) -> io::Result<VoteOf<C>>
where
    C: RaftTypeConfig,
    Codec: LogStoreCodec<C>,
{
    let (version, body) = unframe_with_version(bytes)?;
    Codec::decode_vote(version, body)
        .map_err(|err| crate::codec::codec_io_error("log-store record decode", err))
}

/// Encode a `LogId` as `[version | Codec::encode_log_id(version, ..)]`.
fn encode_log_id_record<C, Codec>(version: u8, log_id: &LogIdOf<C>) -> io::Result<Vec<u8>>
where
    C: RaftTypeConfig,
    Codec: LogStoreCodec<C>,
{
    let body = Codec::encode_log_id(version, log_id)
        .map_err(|err| crate::codec::codec_io_error("log-store record encode", err))?;
    Ok(frame_with_version(version, body))
}

/// Decode a `LogId` framed by [`encode_log_id_record`], rejecting an
/// out-of-range version.
fn decode_log_id_record<C, Codec>(bytes: &[u8]) -> io::Result<LogIdOf<C>>
where
    C: RaftTypeConfig,
    Codec: LogStoreCodec<C>,
{
    let (version, body) = unframe_with_version(bytes)?;
    Codec::decode_log_id(version, body)
        .map_err(|err| crate::codec::codec_io_error("log-store record decode", err))
}

impl<C, K, Codec> RocksdbLogStore<C, K, Codec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C> + 'static,
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
        let entry: C::Entry = decode_entry_record::<C, Codec>(&v)?;
        Ok(Some(entry.log_id()))
    }

    /// The highest format-version leading byte among durably-stored log records,
    /// or `None` if the log is empty. One of the fsync-durable lower bounds
    /// [`recover_active_write_version`](crate::codec::recover_active_write_version)
    /// takes the max of when bootstrap seeds the shared cell.
    ///
    /// Reads only each record's leading byte (no postcard body parse), so a
    /// record from any version in the readable range contributes its byte
    /// without needing its parser. Bounded by `lo` like [`last_log_id_in_cf`]:
    /// for `GroupPrefixed` the reverse iterator can walk into a neighbouring
    /// group's bytes, so any key below `lo` ends the scan.
    pub fn highest_log_record_version(&self) -> io::Result<Option<u8>> {
        let cf = self.log_cf_handle();
        let (lo, hi) = self.keys.log_range();
        let it = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&hi, rocksdb::Direction::Reverse));
        let mut highest: Option<u8> = None;
        for item in it {
            let (key, value) = item.map_err(io::Error::other)?;
            if &*key < lo.as_slice() {
                break;
            }
            if let Some(&leading) = value.first() {
                highest = Some(highest.map_or(leading, |current| current.max(leading)));
            }
        }
        Ok(highest)
    }
}

impl<C, K, Codec> RaftLogReader<C> for RocksdbLogStore<C, K, Codec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C> + 'static,
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
            let entry: C::Entry = decode_entry_record::<C, Codec>(&v)?;
            out.push(entry);
        }
        Ok(out)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        let cf = self.meta_cf_handle();
        meta::read_vote::<C, K, Codec>(&self.db, &cf, &self.keys, MetaLabel::Vote)
    }
}

impl<C, K, Codec> RaftLogStorage<C> for RocksdbLogStore<C, K, Codec>
where
    C: RaftTypeConfig,
    K: KeySpace,
    Codec: LogStoreCodec<C> + 'static,
{
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let cf_meta = self.meta_cf_handle();
        let last_purged_log_id: Option<LogIdOf<C>> = meta::read_log_id::<C, K, Codec>(
            &self.db,
            &cf_meta,
            &self.keys,
            MetaLabel::LastPurged,
        )?;

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
        meta::put_vote::<C, K, Codec>(
            &mut batch,
            &cf_meta,
            &self.keys,
            MetaLabel::Vote,
            self.active_write_version.get(),
            vote,
        )?;
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
            Some(committed) => meta::put_log_id::<C, K, Codec>(
                &mut batch,
                &cf_meta,
                &self.keys,
                MetaLabel::Committed,
                self.active_write_version.get(),
                &committed,
            )?,
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
        meta::read_log_id::<C, K, Codec>(&self.db, &cf_meta, &self.keys, MetaLabel::Committed)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let cf_log = self.log_cf_handle();
        let mut batch = WriteBatch::default();
        let write_version = self.active_write_version.get();
        for entry in entries {
            let (_leader, idx) = entry.log_id_parts();
            let key = self.keys.log_key(idx);
            let value = encode_entry_record::<C, Codec>(write_version, &entry)?;
            batch.put_cf(&cf_log, &key, &value);
        }

        // fsync: openraft treats the `IOFlushed` callback below as a durability
        // signal, so the entries must reach disk before completion is reported.
        let wo = Self::write_sync_opts();
        tsoracle_failpoint::failpoint!("tsoracle_openraft_toolkit::log_store::before_write_batch");
        let result = self.db.write_opt(batch, &wo).map_err(io::Error::other);

        tsoracle_failpoint::failpoint!(
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
        // Exclusive end past `log_key(u64::MAX)`; a single range tombstone
        // replaces the former per-key delete loop (matches the paxos `trim`
        // strategy). For `GroupPrefixed` the bound stays below the next group.
        let end_key = self.keys.log_end_bound();

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&cf_log, &start_key, &end_key);
        // fsync: a lost truncate would resurrect conflicting tail entries on
        // recovery, contradicting the durable vote/append that drove it (see
        // `write_sync_opts`).
        let wo = Self::write_sync_opts();
        tsoracle_failpoint::failpoint!(
            "tsoracle_openraft_toolkit::log_store::truncate::before_write_batch"
        );
        self.db.write_opt(batch, &wo).map_err(io::Error::other)?;
        tsoracle_failpoint::failpoint!(
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
        // purge removes entries up to AND INCLUDING `log_id.index`, so the
        // exclusive end is `log_key(index + 1)`. At u64::MAX there is no next
        // index; `log_end_bound()` covers everything. A single range tombstone
        // replaces the former per-key delete loop.
        let end_key = match log_id.index.checked_add(1) {
            Some(next) => self.keys.log_key(next),
            None => self.keys.log_end_bound(),
        };
        batch.delete_range_cf(&cf_log, &lo, &end_key);

        meta::put_log_id::<C, K, Codec>(
            &mut batch,
            &cf_meta,
            &self.keys,
            MetaLabel::LastPurged,
            self.active_write_version.get(),
            &log_id,
        )?;

        // fsync: the prefix deletes and the `LastPurged` marker share one
        // atomic batch; losing them would let recovery rebuild log state from a
        // prefix openraft believes is already purged (see `write_sync_opts`).
        let wo = Self::write_sync_opts();
        tsoracle_failpoint::failpoint!(
            "tsoracle_openraft_toolkit::log_store::purge::before_write_batch"
        );
        self.db.write_opt(batch, &wo).map_err(io::Error::other)?;
        tsoracle_failpoint::failpoint!(
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
    use super::{decode_entry_record, encode_entry_record};
    use crate::codec::BASELINE_WRITE_VERSION;
    #[cfg(not(feature = "e2e-max-readable-next"))]
    use crate::codec::{MAX_READABLE_VERSION, MIN_READABLE_VERSION};
    use crate::declare_raft_types_ext;
    use crate::log_store::DefaultLogStoreCodec;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RecPeer {
        addr: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RecData {
        v: u64,
    }

    impl std::fmt::Display for RecData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecData({})", self.v)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RecApplied;

    declare_raft_types_ext! {
        pub RecConfig:
            Node            = RecPeer,
            AppData         = RecData,
            AppDataResponse = RecApplied,
            SnapshotData    = std::io::Cursor<Vec<u8>>,
    }

    type RecEntry = openraft::type_config::alias::EntryOf<RecConfig>;

    fn sample_entry() -> RecEntry {
        let lid = openraft::testing::log_id::<RecConfig>(1, 1, 1);
        openraft::entry::RaftEntry::new_normal(lid, RecData { v: 5 })
    }

    #[test]
    fn encode_entry_record_stamps_baseline_version_and_roundtrips() {
        let entry = sample_entry();
        let bytes =
            encode_entry_record::<RecConfig, DefaultLogStoreCodec>(BASELINE_WRITE_VERSION, &entry)
                .expect("encode");
        assert_eq!(bytes[0], BASELINE_WRITE_VERSION);
        let back: RecEntry =
            decode_entry_record::<RecConfig, DefaultLogStoreCodec>(&bytes).expect("decode");
        // Compare via re-encode: EntryOf is not PartialEq across all type params.
        let reencoded =
            encode_entry_record::<RecConfig, DefaultLogStoreCodec>(BASELINE_WRITE_VERSION, &back)
                .expect("re-encode");
        assert_eq!(bytes, reencoded);
    }

    #[test]
    fn decode_entry_record_rejects_out_of_range_version() {
        // A 0xFF-framed record must surface as InvalidData rather than a
        // successful-but-wrong decode.
        let err = decode_entry_record::<RecConfig, DefaultLogStoreCodec>(&[0xFF, 5])
            .expect_err("must reject");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    // Pinned to BASELINE in the default build; the test-only
    // `e2e-max-readable-next` feature lifts MAX to `BASELINE + 1`, so
    // skip the MAX assertion under that feature (covered by
    // `version_constants_under_e2e_max_readable_next_feature` in
    // `codec.rs`).
    #[cfg(not(feature = "e2e-max-readable-next"))]
    #[test]
    fn version_constants_are_at_expected_values() {
        assert_eq!(BASELINE_WRITE_VERSION, 4);
        assert_eq!(MIN_READABLE_VERSION, 4);
        // MAX raised to 5 to cover the dense write version (DENSE_WRITE_VERSION).
        assert_eq!(MAX_READABLE_VERSION, 5);
    }
}

#[cfg(test)]
mod active_write_version_wiring_tests {
    use super::*;
    use crate::codec::ActiveWriteVersion;
    use crate::declare_raft_types_ext;
    use crate::log_store::{DefaultLogStoreCodec, Flat};
    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WirePeer {
        addr: String,
    }
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WireData {
        v: u64,
    }
    impl std::fmt::Display for WireData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "WireData({})", self.v)
        }
    }
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WireApplied;

    declare_raft_types_ext! {
        pub WireConfig:
            Node            = WirePeer,
            AppData         = WireData,
            AppDataResponse = WireApplied,
            SnapshotData    = std::io::Cursor<Vec<u8>>,
    }

    fn open_empty_store() -> (
        RocksdbLogStore<WireConfig, Flat, DefaultLogStoreCodec>,
        TempDir,
    ) {
        let dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new("log", Options::default()),
            ColumnFamilyDescriptor::new("meta", Options::default()),
        ];
        let db = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap());
        let store: RocksdbLogStore<WireConfig, Flat, DefaultLogStoreCodec> =
            RocksdbLogStore::open(db, "log", "meta", Flat).unwrap();
        (store, dir)
    }

    #[test]
    fn highest_log_record_version_is_none_for_empty_log() {
        let (store, _guard) = open_empty_store();
        assert_eq!(store.highest_log_record_version().expect("scan"), None);
    }

    #[tokio::test]
    async fn log_store_stamps_appended_records_at_the_shared_cell_version() {
        // The store reads the SHARED cell for its write version. With a default
        // (baseline) cell, appended records lead with BASELINE_WRITE_VERSION,
        // and the scan that bootstrap uses reports that same byte back.
        use openraft::storage::{IOFlushed, RaftLogStorage};

        let cell = ActiveWriteVersion::default();
        let (mut store, _guard) = open_empty_store();
        store = store.with_active_write_version(cell.clone());

        let lid = openraft::testing::log_id::<WireConfig>(1, 1, 1);
        let entry: openraft::type_config::alias::EntryOf<WireConfig> =
            openraft::entry::RaftEntry::new_normal(lid, WireData { v: 5 });
        store
            .append(std::iter::once(entry), IOFlushed::noop())
            .await
            .expect("append");
        assert_eq!(
            store.highest_log_record_version().expect("scan"),
            Some(crate::codec::BASELINE_WRITE_VERSION)
        );

        // And the cell is the single source of truth: log store reports the
        // same version the cell holds.
        assert_eq!(store.active_write_version(), cell.get());
        assert_eq!(cell.get(), crate::codec::BASELINE_WRITE_VERSION);
    }
}
