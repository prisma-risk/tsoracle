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

//! In-memory fakes for the toolkit's own tests and downstream conformance suites.
//! Only compiled with the `test-fakes` feature or under `cfg(test)`.

// Test scaffolding: `clippy::unwrap_used` / `expect_used` do not fire inside
// `#[test]` / `#[cfg(test)]` (via `clippy.toml`), but these fakes live behind
// a feature flag rather than under `cfg(test)` so consumer crates can pull
// them in. Same intent — exempt the whole sub-tree.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod mem_network;
pub mod partition;

pub use mem_network::{MemNetwork, MemNetworkFactory, MemNetworkPeer};
pub use partition::PartitionController;
