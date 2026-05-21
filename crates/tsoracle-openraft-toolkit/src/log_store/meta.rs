//! Internal helpers for the meta column family.
//!
//! The meta CF holds four small openraft values: current vote, last-committed
//! log id, last-purged log id, last-applied membership. Each is keyed by a
//! [`MetaLabel`](super::MetaLabel) via the active [`KeySpace`](super::KeySpace).
//! These helpers serialize values with `postcard` and translate rocksdb errors
//! into [`RocksdbLogStoreError`](super::RocksdbLogStoreError).

use rocksdb::{BoundColumnFamily, DB, WriteBatch};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

use super::RocksdbLogStoreError;
use super::key_space::{KeySpace, MetaLabel};

pub(super) fn read<T: DeserializeOwned, K: KeySpace>(
    db: &DB,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
) -> Result<Option<T>, RocksdbLogStoreError> {
    let key = keys.meta_key(label);
    match db.get_pinned_cf(cf, &key)? {
        Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) fn put<T: Serialize, K: KeySpace>(
    batch: &mut WriteBatch,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
    value: &T,
) -> Result<(), RocksdbLogStoreError> {
    let key = keys.meta_key(label);
    let bytes = postcard::to_stdvec(value)?;
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
