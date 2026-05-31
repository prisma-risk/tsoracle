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
pub mod codec_provider;
pub mod macros;

#[cfg(feature = "rocksdb-log-store")]
pub mod log_store;

pub mod lifecycle;

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

pub use codec::{
    ActiveWriteVersion, BASELINE_WRITE_VERSION, BATCH_WRITE_VERSION, CodecError,
    DENSE_WRITE_VERSION, MAX_READABLE_VERSION, MIN_READABLE_VERSION, codec_io_error, decode,
    encode, recover_active_write_version,
};
pub use codec_provider::{DefaultLogStoreCodec, LogStoreCodec};
pub use lifecycle::{
    BootstrapError, BootstrapMode, GatedAdmissionError, JoinerGateError, LeadershipState,
    MembershipError, add_learner, add_learner_gated, bootstrap, change_membership, join,
    leadership_events, leadership_events_from_metrics, learner_meets_active_write_version,
};

#[cfg(feature = "rocksdb-log-store")]
pub use log_store::{
    Flat, GroupPrefixed, KeySpace, MetaLabel, RocksdbLogStore, RocksdbLogStoreError,
};

#[cfg(all(test, feature = "rocksdb-log-store"))]
mod reexport_tests {
    #[test]
    fn seam_items_are_reachable_from_crate_root() {
        // Compile-time reachability: name the re-exported items. (A type-level
        // assertion; the driver imports both from the crate root.)
        fn _assert_reachable<C, Codec>()
        where
            C: openraft::RaftTypeConfig,
            Codec: crate::LogStoreCodec<C>,
        {
        }
        let _ = std::any::type_name::<crate::DefaultLogStoreCodec>();
    }
}
