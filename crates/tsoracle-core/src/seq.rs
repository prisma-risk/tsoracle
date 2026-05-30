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

/// Default per-call ceiling on `GetSeq`'s `count` — the largest block a single
/// request may reserve when the server has not overridden it.
///
/// Unlike the timestamp path's `MAX_TIMESTAMPS_PER_RPC` (forced by the 18-bit
/// logical field in the packed wire format), nothing in the dense format
/// requires a particular ceiling: `start` is `u64`, the wire `count` is `u32`,
/// and the durable counter is `u64`. This cap is a soft anti-abuse guardrail —
/// a dense block is permanently consumed (the gapless counter only moves
/// forward), so an unbounded `count` would let one call irrevocably burn a huge
/// span of a key's namespace. The value (`2^16`) is a deliberate round default;
/// operators tune it via [`ServerBuilder::max_seq_count`].
///
/// [`ServerBuilder::max_seq_count`]: https://docs.rs/tsoracle-server
pub const DEFAULT_MAX_SEQ_COUNT: u32 = 65_536;

/// A validated sequence key: non-empty, valid UTF-8 (guaranteed by `String`),
/// and at most [`MAX_SEQ_KEY_LEN`] bytes. `try_new` is the single validation
/// site — a value of this type is proof the key is in range.
///
/// The `serde` impls are hand-written (not derived) so deserialization routes
/// through `try_new`: a derived `Deserialize` for a newtype builds the inner
/// `String` directly and would be a second construction site that bypasses the
/// invariant. Mirrors [`crate::peer::PeerEndpoint`]'s validate-on-deserialize
/// discipline. `Serialize` emits the bare string (newtype-transparent), so the
/// on-disk and wire byte layout is unchanged.
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

#[cfg(feature = "serde")]
impl serde::Serialize for SeqKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for SeqKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Re-validate on the way in: `try_new` is the single validation site, so
        // a crafted or corrupted key (empty, or longer than `MAX_SEQ_KEY_LEN`
        // bytes) arriving in a replicated log entry or a persisted snapshot is
        // rejected here rather than silently observed as an in-range `SeqKey`.
        let s = String::deserialize(deserializer)?;
        SeqKey::try_new(s).map_err(serde::de::Error::custom)
    }
}

/// A contiguous block of `count` dense ordinals for one key, starting at
/// `start`, issued under one leadership `epoch`. Covers `[start, start + count)`.
///
/// The only constructor, [`try_new`](Self::try_new), validates that the block is
/// non-empty (`count >= 1`) and that its last ordinal `start + count - 1` does
/// not overflow `u64`. A constructed value is therefore proof that
/// [`last`](Self::last) neither underflows nor wraps — the invariant lives in the
/// type, not in whatever code happened to build it. Mirrors [`WindowGrant`].
///
/// [`WindowGrant`]: crate::WindowGrant
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqGrant {
    key: SeqKey,
    start: u64,
    count: u32,
    epoch: Epoch,
}

impl SeqGrant {
    /// Construct a grant, validating that [`last`](Self::last) is infallible.
    ///
    /// Rejects `count == 0` ([`CoreError::SeqCountZero`]) — a block covers at
    /// least one ordinal, and `last`'s `start + count - 1` would underflow — and
    /// a block whose last ordinal `start + count - 1` exceeds `u64::MAX`
    /// ([`CoreError::SeqBlockOverflow`]). In the server's serving path neither
    /// can occur (`count` is validated `1..=max_seq_count` and the durable
    /// fetch-add already rejected an overflowing advance), but `SeqGrant` is
    /// public, so the constructor enforces the invariant for every caller.
    pub fn try_new(key: SeqKey, start: u64, count: u32, epoch: Epoch) -> Result<Self, CoreError> {
        if count == 0 {
            return Err(CoreError::SeqCountZero);
        }
        // last() = start + count - 1; reject inputs where that would wrap.
        if start.checked_add(u64::from(count) - 1).is_none() {
            return Err(CoreError::SeqBlockOverflow { start, count });
        }
        Ok(SeqGrant {
            key,
            start,
            count,
            epoch,
        })
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
    /// The last ordinal in the block: `start + count - 1`. Infallible:
    /// [`try_new`](Self::try_new) validated `count >= 1` (so `count - 1` does not
    /// underflow) and that `start + (count - 1)` fits in `u64`, so a constructed
    /// `SeqGrant` witnesses this cannot panic. The `start + (count - 1)`
    /// association matches that check exactly — computing `(start + count) - 1`
    /// would overflow the intermediate at the `last() == u64::MAX` boundary even
    /// though the result fits.
    pub fn last(&self) -> u64 {
        self.start + (u64::from(self.count) - 1)
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
    ///
    /// `max_count` is the caller's per-call ceiling — the server's configured
    /// [`ServerBuilder::max_seq_count`], which defaults to
    /// [`DEFAULT_MAX_SEQ_COUNT`]. Injecting it (rather than reading the constant
    /// here) keeps the cap a server-side policy the operator can tune, while the
    /// `count >= 1` floor stays a hard invariant enforced unconditionally.
    ///
    /// [`ServerBuilder::max_seq_count`]: https://docs.rs/tsoracle-server
    pub fn validate_request(
        &self,
        key: &str,
        count: u32,
        max_count: u32,
    ) -> Result<SeqKey, CoreError> {
        if !self.is_leader() {
            return Err(CoreError::NotLeader);
        }
        if count == 0 {
            return Err(CoreError::SeqCountZero);
        }
        if count > max_count {
            return Err(CoreError::SeqCountTooLarge {
                count,
                max: max_count,
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
        assert_eq!(
            a.validate_request("orders", 1, DEFAULT_MAX_SEQ_COUNT),
            Err(CoreError::NotLeader)
        );
    }

    #[test]
    fn validate_request_rejects_zero_count() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(
            a.validate_request("orders", 0, DEFAULT_MAX_SEQ_COUNT),
            Err(CoreError::SeqCountZero)
        );
    }

    #[test]
    fn validate_request_rejects_oversized_count() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(
            a.validate_request("orders", DEFAULT_MAX_SEQ_COUNT + 1, DEFAULT_MAX_SEQ_COUNT),
            Err(CoreError::SeqCountTooLarge {
                count: DEFAULT_MAX_SEQ_COUNT + 1,
                max: DEFAULT_MAX_SEQ_COUNT
            })
        );
    }

    #[test]
    fn validate_request_uses_caller_provided_max() {
        // The ceiling is the `max_count` argument, not the default constant: a
        // smaller cap rejects below the default, and a larger cap accepts above
        // it — proving the cap is injected policy, not a hard-coded limit.
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(
            a.validate_request("orders", 11, 10),
            Err(CoreError::SeqCountTooLarge { count: 11, max: 10 }),
            "count above a small configured cap must be rejected",
        );
        assert!(
            a.validate_request("orders", 10, 10).is_ok(),
            "count at the configured cap must be accepted",
        );
        assert!(
            a.validate_request(
                "orders",
                DEFAULT_MAX_SEQ_COUNT + 1,
                DEFAULT_MAX_SEQ_COUNT * 2
            )
            .is_ok(),
            "a larger configured cap must accept counts above the default",
        );
    }

    #[test]
    fn validate_request_rejects_bad_key() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert_eq!(
            a.validate_request("", 1, DEFAULT_MAX_SEQ_COUNT),
            Err(CoreError::SeqKeyEmpty)
        );
    }

    #[test]
    fn validate_request_ok_returns_key() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        let k = a
            .validate_request("orders", 10, DEFAULT_MAX_SEQ_COUNT)
            .unwrap();
        assert_eq!(k.as_str(), "orders");
    }

    #[test]
    fn validate_request_accepts_max_count_exactly() {
        let mut a = SeqAllocator::new();
        a.become_leader(Epoch(1));
        assert!(
            a.validate_request("orders", DEFAULT_MAX_SEQ_COUNT, DEFAULT_MAX_SEQ_COUNT)
                .is_ok()
        );
    }
}

#[cfg(test)]
mod seqgrant_tests {
    use super::*;

    #[test]
    fn exposes_fields_and_last() {
        let key = SeqKey::try_new("users").unwrap();
        let g = SeqGrant::try_new(key.clone(), 100, 5, Epoch(7)).unwrap();
        assert_eq!(g.key().as_str(), "users");
        assert_eq!(g.start(), 100);
        assert_eq!(g.count(), 5);
        assert_eq!(g.epoch(), Epoch(7));
        // [100, 105): last issued ordinal is 104.
        assert_eq!(g.last(), 104);
    }

    #[test]
    fn last_equals_start_when_count_is_one() {
        let g1 = SeqGrant::try_new(SeqKey::try_new("x").unwrap(), 42, 1, Epoch(1)).unwrap();
        assert_eq!(g1.last(), 42);
    }

    #[test]
    fn try_new_rejects_zero_count() {
        let key = SeqKey::try_new("x").unwrap();
        assert_eq!(
            SeqGrant::try_new(key, 5, 0, Epoch(1)),
            Err(CoreError::SeqCountZero)
        );
    }

    #[test]
    fn try_new_rejects_block_overflow() {
        // start + count - 1 overflows u64, which would make `last()` wrap.
        let key = SeqKey::try_new("x").unwrap();
        assert_eq!(
            SeqGrant::try_new(key, u64::MAX, 2, Epoch(1)),
            Err(CoreError::SeqBlockOverflow {
                start: u64::MAX,
                count: 2
            })
        );
    }

    #[test]
    fn try_new_accepts_max_boundary() {
        // start + count - 1 == u64::MAX exactly is the largest valid block.
        let key = SeqKey::try_new("x").unwrap();
        let g = SeqGrant::try_new(key, u64::MAX - 4, 5, Epoch(1)).unwrap();
        assert_eq!(g.last(), u64::MAX);
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

    // ---- SeqKey: serde re-validates on deserialize (try_new is the single
    // validation site, including the postcard decode path the openraft log and
    // dense snapshot travel through). A derived `Deserialize` would build the
    // inner `String` directly and bypass `try_new`, so these pin that decode
    // routes through the invariant.

    #[cfg(feature = "serde")]
    #[test]
    fn serde_deserialize_rejects_empty_key() {
        // `postcard::to_stdvec(&String::new())` is a structurally valid postcard
        // String (a single zero length-prefix byte). Decoding it as a `SeqKey`
        // must re-run `try_new` and reject the empty key, not return `SeqKey("")`.
        let bytes = postcard::to_stdvec(&String::new()).expect("encode empty string");
        let decoded = postcard::from_bytes::<SeqKey>(&bytes);
        assert!(
            decoded.is_err(),
            "empty-key postcard payload must fail to decode, got {decoded:?}",
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_deserialize_rejects_oversized_key() {
        let oversized = "a".repeat(MAX_SEQ_KEY_LEN + 1);
        let bytes = postcard::to_stdvec(&oversized).expect("encode oversized string");
        let decoded = postcard::from_bytes::<SeqKey>(&bytes);
        assert!(
            decoded.is_err(),
            "oversized-key postcard payload must fail to decode, got {decoded:?}",
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trip_preserves_valid_key() {
        let key = SeqKey::try_new("orders").unwrap();
        let bytes = postcard::to_stdvec(&key).expect("encode key");
        // Serialize is newtype-transparent: the bytes are exactly a postcard
        // `String`, so the on-disk/wire format is unchanged by routing decode
        // through `try_new`.
        assert_eq!(bytes, postcard::to_stdvec(&"orders".to_string()).unwrap());
        let back: SeqKey = postcard::from_bytes(&bytes).expect("decode key");
        assert_eq!(back, key);
        assert_eq!(back.as_str(), "orders");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_deserialize_accepts_max_length_key() {
        let max = "a".repeat(MAX_SEQ_KEY_LEN);
        let bytes = postcard::to_stdvec(&max).expect("encode max-length string");
        let back: SeqKey = postcard::from_bytes(&bytes).expect("max-length key must decode");
        assert_eq!(back.as_str(), max);
    }
}
