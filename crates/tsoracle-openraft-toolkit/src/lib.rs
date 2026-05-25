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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod codec;
pub mod macros;

#[cfg(feature = "rocksdb-log-store")]
pub mod log_store;

pub mod lifecycle;

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

pub use codec::{CodecError, SCHEMA_VERSION, codec_io_error, decode, encode};
pub use lifecycle::{
    BootstrapError, BootstrapMode, LeadershipState, MembershipError, add_learner, bootstrap,
    change_membership, join, leadership_events, leadership_events_from_metrics,
};

#[cfg(feature = "rocksdb-log-store")]
pub use log_store::{
    Flat, GroupPrefixed, KeySpace, MetaLabel, RocksdbLogStore, RocksdbLogStoreError,
};
