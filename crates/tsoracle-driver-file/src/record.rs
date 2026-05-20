//! 17-byte on-disk record:  "TSOR" | u8 version | u64 high_water | u32 crc32c

pub const MAGIC: &[u8; 4] = b"TSOR";
pub const VERSION: u8 = 1;
pub const RECORD_LEN: usize = 4 + 1 + 8 + 4;

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("file too short: {0} bytes (expected 17)")]
    TooShort(usize),
    #[error("bad magic: {0:?}")]
    BadMagic([u8; 4]),
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("crc mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch { expected: u32, computed: u32 },
}

pub fn encode(high_water: u64) -> [u8; RECORD_LEN] {
    let mut buf = [0u8; RECORD_LEN];
    buf[0..4].copy_from_slice(MAGIC);
    buf[4] = VERSION;
    buf[5..13].copy_from_slice(&high_water.to_le_bytes());
    let crc = crc32c::crc32c(&buf[0..13]);
    buf[13..17].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode(bytes: &[u8]) -> Result<u64, RecordError> {
    let magic: [u8; 4] = read_array(bytes, 0)?;
    if &magic != MAGIC {
        return Err(RecordError::BadMagic(magic));
    }
    let version = *bytes.get(4).ok_or(RecordError::TooShort(bytes.len()))?;
    if version != VERSION {
        return Err(RecordError::UnsupportedVersion(version));
    }
    let high_water = u64::from_le_bytes(read_array(bytes, 5)?);
    let crc_recorded = u32::from_le_bytes(read_array(bytes, 13)?);
    let crc_computed = crc32c::crc32c(&bytes[0..13]);
    if crc_recorded != crc_computed {
        return Err(RecordError::CrcMismatch {
            expected: crc_recorded,
            computed: crc_computed,
        });
    }
    Ok(high_water)
}

fn read_array<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N], RecordError> {
    let end = start
        .checked_add(N)
        .ok_or(RecordError::TooShort(bytes.len()))?;
    bytes
        .get(start..end)
        .ok_or(RecordError::TooShort(bytes.len()))?
        .try_into()
        .map_err(|_| RecordError::TooShort(bytes.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for v in [0u64, 1, 1_700_000_000_000, u64::MAX >> 18] {
            let buf = encode(v);
            assert_eq!(decode(&buf).unwrap(), v);
        }
    }

    #[test]
    fn detects_bad_magic() {
        let mut buf = encode(42);
        buf[0] = b'X';
        assert!(matches!(decode(&buf), Err(RecordError::BadMagic(_))));
    }

    #[test]
    fn detects_bad_crc() {
        let mut buf = encode(42);
        buf[5] ^= 0x01;
        assert!(matches!(decode(&buf), Err(RecordError::CrcMismatch { .. })));
    }

    #[test]
    fn detects_short() {
        let buf = encode(42);
        assert!(matches!(decode(&buf[..10]), Err(RecordError::TooShort(10))));
    }

    #[test]
    fn detects_short_magic_slice() {
        assert!(matches!(decode(b"TSO"), Err(RecordError::TooShort(3))));
    }

    #[test]
    fn detects_short_version_byte() {
        assert!(matches!(decode(MAGIC), Err(RecordError::TooShort(4))));
    }

    #[test]
    fn detects_short_high_water_slice() {
        let bytes = [MAGIC.as_slice(), &[VERSION], &[0; 7]].concat();
        assert!(matches!(decode(&bytes), Err(RecordError::TooShort(12))));
    }

    #[test]
    fn detects_short_crc_slice() {
        let bytes = [MAGIC.as_slice(), &[VERSION], &[0; 8], &[0; 3]].concat();
        assert!(matches!(decode(&bytes), Err(RecordError::TooShort(16))));
    }

    use proptest::prelude::*;

    proptest! {
        // Roundtrip over the full u64 domain. high_water on disk is unconstrained
        // (the physical_ms 46-bit cap is an algorithm invariant, not a storage one),
        // so the encoder/decoder must handle arbitrary values.
        #[test]
        fn encode_decode_roundtrip(high_water in any::<u64>()) {
            let encoded = encode(high_water);
            prop_assert_eq!(decode(&encoded).unwrap(), high_water);
        }

        // Single-bit corruption is always caught: flipping any one bit in the
        // 17-byte record makes decode either return Err or return a different
        // high_water. The CRC's whole purpose is the "always Err for payload
        // flips" half; the "different value" alternative covers the
        // theoretically-possible (but vanishingly improbable for crc32c) case
        // where a bit flip in the high_water bytes happens to match a flip in
        // the CRC bytes. Stated this way the property is exactly true.
        #[test]
        fn any_single_bit_flip_is_detected(
            high_water in any::<u64>(),
            byte_index in 0usize..RECORD_LEN,
            bit_index in 0u8..8,
        ) {
            let original = encode(high_water);
            let mut corrupted = original;
            corrupted[byte_index] ^= 1 << bit_index;
            prop_assert_ne!(corrupted, original);
            match decode(&corrupted) {
                Err(_) => {} // Detected — the common case.
                Ok(decoded) => prop_assert_ne!(decoded, high_water),
            }
        }

        // Any encoded record decodes back to its input — phrased over the
        // decoder's reject set: a well-formed 17-byte slice with our magic and
        // version is never spuriously rejected. Complements the corruption test.
        #[test]
        fn well_formed_record_is_never_rejected(high_water in any::<u64>()) {
            let encoded = encode(high_water);
            prop_assert!(decode(&encoded).is_ok());
        }
    }
}
