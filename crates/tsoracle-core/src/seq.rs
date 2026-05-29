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

/// A validated sequence key: non-empty, valid UTF-8 (guaranteed by `String`),
/// and at most [`MAX_SEQ_KEY_LEN`] bytes. `try_new` is the single validation
/// site — a value of this type is proof the key is in range.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeqKey(String);

impl SeqKey {
    pub fn try_new(s: impl Into<String>) -> Result<Self, CoreError> {
        let s = s.into();
        if s.is_empty() {
            return Err(CoreError::SeqKeyEmpty);
        }
        if s.len() > MAX_SEQ_KEY_LEN {
            return Err(CoreError::SeqKeyTooLong {
                len: s.len(),
                max: MAX_SEQ_KEY_LEN,
            });
        }
        Ok(SeqKey(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A contiguous block of `count` dense ordinals for one key, starting at
/// `start`, issued under one leadership `epoch`. Covers `[start, start + count)`.
/// `count >= 1` is a caller invariant established by `SeqAllocator::validate_request`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqGrant {
    key: SeqKey,
    start: u64,
    count: u32,
    epoch: Epoch,
}

impl SeqGrant {
    pub fn new(key: SeqKey, start: u64, count: u32, epoch: Epoch) -> Self {
        debug_assert!(count != 0, "SeqGrant::new: count must be >= 1");
        SeqGrant {
            key,
            start,
            count,
            epoch,
        }
    }
    pub fn key(&self) -> &SeqKey {
        &self.key
    }
    pub fn start(&self) -> u64 {
        self.start
    }
    pub fn count(&self) -> u32 {
        self.count
    }
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    /// The last ordinal in the block: `start + count - 1`. `count >= 1`, so this
    /// does not underflow. The addition cannot overflow in any reachable counter
    /// range (u64::MAX / MAX_SEQ_COUNT exceeds 10^14 blocks).
    pub fn last(&self) -> u64 {
        self.start + u64::from(self.count) - 1
    }
}

#[derive(Debug)]
enum SeqState {
    NotLeader,
    Leader { epoch: Epoch },
}

/// Leadership/epoch gate plus request validation for the dense path. Holds NO
/// per-key counter state — counters live in the durable layer and `start` is
/// assigned there. This type only decides "may this request proceed, and is it
/// well-formed?".
pub struct SeqAllocator {
    state: SeqState,
}

impl SeqAllocator {
    pub fn new() -> Self {
        SeqAllocator {
            state: SeqState::NotLeader,
        }
    }

    /// Transition to leader state for `epoch`. The caller must already hold
    /// consensus leadership for `epoch` before calling this.
    pub fn become_leader(&mut self, epoch: Epoch) {
        self.state = SeqState::Leader { epoch };
    }

    pub fn step_down(&mut self) {
        self.state = SeqState::NotLeader;
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, SeqState::Leader { .. })
    }

    pub fn epoch(&self) -> Option<Epoch> {
        match self.state {
            SeqState::Leader { epoch } => Some(epoch),
            SeqState::NotLeader => None,
        }
    }

    /// Validate a request without touching durable state. Leadership is checked
    /// first; then count bounds (zero, then oversized), then key validity.
    /// Returns the validated [`SeqKey`].
    pub fn validate_request(&self, key: &str, count: u32) -> Result<SeqKey, CoreError> {
        if !self.is_leader() {
            return Err(CoreError::NotLeader);
        }
        if count == 0 {
            return Err(CoreError::SeqCountZero);
        }
        if count > MAX_SEQ_COUNT {
            return Err(CoreError::SeqCountTooLarge {
                count,
                max: MAX_SEQ_COUNT,
            });
        }
        SeqKey::try_new(key)
    }
}

impl Default for SeqAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod seqallocator_tests {
    use super::*;

    #[test]
    fn new_is_not_leader() {
        let a = SeqAllocator::new();
        assert!(!a.is_leader());
        assert_eq!(a.epoch(), None);
    }

    #[test]
    fn become_leader_then_step_down() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(3));
        assert!(a.is_leader());
        assert_eq!(a.epoch(), Some(Epoch(3)));
        a.step_down();
        assert!(!a.is_leader());
        assert_eq!(a.epoch(), None);
    }

    #[test]
    fn validate_request_off_leader_is_not_leader() {
        let a = SeqAllocator::new();
        assert_eq!(a.validate_request("orders", 1), Err(CoreError::NotLeader));
    }

    #[test]
    fn validate_request_rejects_zero_count() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(
            a.validate_request("orders", 0),
            Err(CoreError::SeqCountZero)
        );
    }

    #[test]
    fn validate_request_rejects_oversized_count() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(
            a.validate_request("orders", MAX_SEQ_COUNT + 1),
            Err(CoreError::SeqCountTooLarge {
                count: MAX_SEQ_COUNT + 1,
                max: MAX_SEQ_COUNT
            })
        );
    }

    #[test]
    fn validate_request_rejects_bad_key() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(a.validate_request("", 1), Err(CoreError::SeqKeyEmpty));
    }

    #[test]
    fn validate_request_ok_returns_key() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        let k = a.validate_request("orders", 10).unwrap();
        assert_eq!(k.as_str(), "orders");
    }

    #[test]
    fn validate_request_accepts_max_count_exactly() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert!(a.validate_request("orders", MAX_SEQ_COUNT).is_ok());
    }
}

#[cfg(test)]
mod seqgrant_tests {
    use super::*;

    #[test]
    fn exposes_fields_and_last() {
        let key = SeqKey::try_new("users").unwrap();
        let g = SeqGrant::new(key.clone(), 100, 5, Epoch(7));
        assert_eq!(g.key().as_str(), "users");
        assert_eq!(g.start(), 100);
        assert_eq!(g.count(), 5);
        assert_eq!(g.epoch(), Epoch(7));
        // [100, 105): last issued ordinal is 104.
        assert_eq!(g.last(), 104);
    }

    #[test]
    fn last_equals_start_when_count_is_one() {
        let g1 = SeqGrant::new(SeqKey::try_new("x").unwrap(), 42, 1, Epoch(1));
        assert_eq!(g1.last(), 42);
    }
}

#[cfg(test)]
mod seqkey_tests {
    use super::*;

    #[test]
    fn accepts_normal_key() {
        let k = SeqKey::try_new("orders").unwrap();
        assert_eq!(k.as_str(), "orders");
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(SeqKey::try_new(""), Err(CoreError::SeqKeyEmpty));
    }

    #[test]
    fn accepts_max_length() {
        let s = "a".repeat(MAX_SEQ_KEY_LEN);
        assert!(SeqKey::try_new(&s).is_ok());
    }

    #[test]
    fn rejects_one_past_max_length() {
        let s = "a".repeat(MAX_SEQ_KEY_LEN + 1);
        assert_eq!(
            SeqKey::try_new(&s),
            Err(CoreError::SeqKeyTooLong {
                len: MAX_SEQ_KEY_LEN + 1,
                max: MAX_SEQ_KEY_LEN
            })
        );
    }

    #[test]
    fn length_is_measured_in_utf8_bytes_not_chars() {
        // 'é' is 2 bytes; 64 of them = 128 bytes = exactly the cap.
        let ok = "é".repeat(MAX_SEQ_KEY_LEN / 2);
        assert!(SeqKey::try_new(&ok).is_ok());
        let too_long = "é".repeat(MAX_SEQ_KEY_LEN / 2 + 1);
        assert!(matches!(
            SeqKey::try_new(&too_long),
            Err(CoreError::SeqKeyTooLong { .. })
        ));
    }
}
