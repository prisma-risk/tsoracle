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

//! Compaction trigger policy. Filled in by a follow-up commit.

/// Placeholder snapshot policy. The next commit replaces this with the
/// real every-N-decided trigger.
#[derive(Clone, Copy, Debug, Default)]
pub struct SnapshotPolicy;

impl SnapshotPolicy {
    /// Always returns `false` until the real policy lands.
    #[must_use]
    pub fn should_snapshot(&self, _decided_idx: u64) -> bool {
        false
    }
}
