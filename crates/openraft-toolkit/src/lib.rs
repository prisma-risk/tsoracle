#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod codec;
pub mod macros;

#[cfg(feature = "rocksdb-log-store")]
pub mod log_store;

pub mod lifecycle;

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

pub use codec::{CodecError, decode, encode};

#[cfg(feature = "rocksdb-log-store")]
pub use log_store::{Flat, GroupPrefixed, KeySpace, MetaLabel};
