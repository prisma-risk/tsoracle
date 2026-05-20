#![no_main]

use libfuzzer_sys::fuzz_target;
use tsoracle_driver_file::record::{decode, encode};

fuzz_target!(|data: &[u8]| {
    // Forward direction: any u64 round-trips through encode/decode unchanged.
    if data.len() >= 8 {
        let value = u64::from_le_bytes(data[..8].try_into().unwrap());
        let encoded = encode(value);
        let decoded = decode(&encoded).expect("encoded record must decode");
        assert_eq!(decoded, value);
    }
    // Reverse direction: any bytes that decode successfully must re-encode
    // to the exact input. Under the strict-trailing-bytes contract from
    // the preceding RecordError::TrailingBytes change, decode only accepts
    // inputs of length RECORD_LEN, so full equality (not just prefix
    // equality) is the correct property.
    if let Ok(decoded_value) = decode(data) {
        let re_encoded = encode(decoded_value);
        assert_eq!(&re_encoded[..], data);
    }
});
