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

//! Internal helpers for the meta column family.
//!
//! The meta CF holds four small openraft values: current vote, last-committed
//! log id, last-purged log id, last-applied membership. Each is keyed by a
//! [`MetaLabel`](super::MetaLabel) via the active [`KeySpace`](super::KeySpace).
//! These helpers frame values with the same `[SCHEMA_VERSION | postcard]` codec
//! the log column uses ([`super::encode_record`] / [`super::decode_record`]), so
//! a recovery-critical meta record (Vote/Committed/LastPurged) loud-rejects on a
//! version mismatch instead of misdecoding silently. rocksdb errors are mapped
//! to [`io::Error`] to match the storage trait surface.

use rocksdb::{BoundColumnFamily, DB, WriteBatch};
use serde::{Serialize, de::DeserializeOwned};
use std::io;
use std::sync::Arc;

use super::key_space::{KeySpace, MetaLabel};
use super::{decode_record, encode_record};

pub(super) fn read<T: DeserializeOwned, K: KeySpace>(
    db: &DB,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
) -> io::Result<Option<T>> {
    let key = keys.meta_key(label);
    match db.get_pinned_cf(cf, &key).map_err(io::Error::other)? {
        Some(bytes) => Ok(Some(decode_record(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) fn put<T: Serialize, K: KeySpace>(
    batch: &mut WriteBatch,
    cf: &Arc<BoundColumnFamily<'_>>,
    keys: &K,
    label: MetaLabel,
    value: &T,
) -> io::Result<()> {
    let key = keys.meta_key(label);
    let bytes = encode_record(value)?;
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
