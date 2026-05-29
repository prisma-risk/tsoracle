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

use crate::CoreError;

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
