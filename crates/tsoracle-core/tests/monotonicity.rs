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

//! Property test: under arbitrary (now_ms, count, leadership-event, extension)
//! sequences, the Allocator never issues two equal timestamps and never
//! regresses across leadership transitions.

use proptest::prelude::*;
use tsoracle_core::{Allocator, Epoch, Timestamp};

#[derive(Debug, Clone)]
enum Op {
    Grant { now_ms: u64, count: u32 },
    Extend { now_ms: u64, ahead_ms: u64 },
    Gain { high_water: u64, epoch_bump: u8 },
    Lose,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u64..2_000_000_000_000, 1u32..1024)
            .prop_map(|(now_ms, count)| Op::Grant { now_ms, count }),
        (0u64..2_000_000_000_000, 100u64..10_000)
            .prop_map(|(now_ms, ahead_ms)| Op::Extend { now_ms, ahead_ms }),
        (0u64..2_000_000_000_000, 1u8..16).prop_map(|(high_water, epoch_bump)| Op::Gain {
            high_water,
            epoch_bump
        }),
        Just(Op::Lose),
    ]
}

proptest! {
    #[test]
    fn allocator_never_regresses(ops in prop::collection::vec(op_strategy(), 0..256)) {
        let mut allocator = Allocator::new();
        let mut last_issued: Option<Timestamp> = None;
        let mut epoch_counter: u64 = 0;

        for op in ops {
            match op {
                Op::Grant { now_ms, count } => {
                    if let Ok(grant) = allocator.try_grant(now_ms, count) {
                        let first = grant.first();
                        let last  = grant.last();
                        prop_assert!(first <= last, "grant internal order");
                        if let Some(prev) = last_issued {
                            prop_assert!(first > prev, "grant first {:?} > last_issued {:?}", first, prev);
                        }
                        last_issued = Some(last);
                    }
                }
                Op::Extend { now_ms, ahead_ms } => {
                    // prepare now errors with NotLeader off-leader (matching every
                    // other mutating method), so only attempt it on a leader. The
                    // driver's monotonic-advance is simulated by committing target
                    // at the allocator's current epoch.
                    if let Some(epoch) = allocator.epoch() {
                        let target = allocator
                            .try_prepare_window_extension(now_ms, ahead_ms)
                            .unwrap();
                        allocator.try_commit_window_extension(target, epoch).unwrap();
                    }
                }
                Op::Gain { high_water, epoch_bump } => {
                    epoch_counter = epoch_counter.saturating_add(epoch_bump as u64);
                    // Across a leader transition the fence_floor must be strictly
                    // above the last physical_ms any prior leader could have issued.
                    // The server's fence protocol writes this atomically; here we
                    // model it as max(high_water, last_issued.physical_ms + 1).
                    let fence_floor = match last_issued {
                        Some(prev) => high_water.max(prev.physical_ms() + 1),
                        None       => high_water,
                    };
                    // committed_ceiling must be >= fence_floor for immediate serving.
                    let committed_ceiling = fence_floor.saturating_add(30_000);
                    allocator.try_on_leadership_gained(fence_floor, committed_ceiling, Epoch(u128::from(epoch_counter))).unwrap();
                }
                Op::Lose => {
                    allocator.on_leadership_lost();
                }
            }
        }
    }
}
