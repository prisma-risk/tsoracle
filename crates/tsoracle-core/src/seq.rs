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

//! Keyed dense sequence types: validated keys, contiguous grants, and the
//! leadership/epoch gate. Pure and synchronous, no I/O — the same discipline as
//! `allocator.rs`. Unlike `Allocator`, this holds NO per-key counter state: every
//! counter lives in the durable layer and every block `start` is assigned there.

use crate::{CoreError, Epoch};

/// Maximum length, in bytes, of a sequence key's UTF-8 encoding.
pub const MAX_SEQ_KEY_LEN: usize = 128;

/// Maximum `count` a single `GetSeq` may reserve in one block.
pub const MAX_SEQ_COUNT: u32 = 65_536;
