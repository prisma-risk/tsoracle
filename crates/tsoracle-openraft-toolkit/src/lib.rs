#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

#[macro_use]
mod failpoint;

pub mod codec;
pub mod macros;

#[cfg(feature = "rocksdb-log-store")]
pub mod log_store;

pub mod lifecycle;

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

pub use codec::{CodecError, decode, encode};
pub use lifecycle::{
    BootstrapError, BootstrapMode, LeadershipState, MembershipError, add_learner, bootstrap,
    change_membership, leadership_events,
};

#[cfg(feature = "rocksdb-log-store")]
pub use log_store::{
    Flat, GroupPrefixed, KeySpace, MetaLabel, RocksdbLogStore, RocksdbLogStoreError,
};
