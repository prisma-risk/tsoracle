//! Regenerates the fuzz seed corpus for `toolkit_codec_decode`, deriving
//! valid `[version | postcard(body)]` payloads from the canonical
//! [`openraft_toolkit::encode`] entry point.
//!
//! Run with:
//!
//!     cargo test -p openraft-toolkit --test generate_fuzz_seeds -- --ignored
//!
//! Idempotent. Re-run after any change to the codec framing.

use std::fs;
use std::path::PathBuf;

use openraft_toolkit::encode;
use serde::{Deserialize, Serialize};

/// Body type that mirrors `tsoracle_driver_openraft::HighWaterApplied` byte-
/// for-byte under postcard (single `u64` field). Re-declared locally so this
/// seed generator does not pull `tsoracle-driver-openraft` into
/// `openraft-toolkit`'s dev-dependency graph; postcard's wire format is
/// structural, so the encoded bytes are identical.
#[derive(Debug, Serialize, Deserialize)]
struct HighWaterAppliedShape {
    value: u64,
}

fn fuzz_corpus_dir(target: &str) -> PathBuf {
    PathBuf::from("../..").join("fuzz/corpus").join(target)
}

fn write_seed(target: &str, name: &str, bytes: &[u8]) {
    let dir = fuzz_corpus_dir(target);
    fs::create_dir_all(&dir).expect("create seed corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed file");
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_toolkit_codec_decode_seeds() {
    let target = "toolkit_codec_decode";
    // Empty input — exercises the `CodecError::Empty` branch.
    write_seed(target, "seed_empty", &[]);
    // Version-only payload — exercises the trailing-postcard EOF branch.
    write_seed(target, "seed_version_only", &[0u8]);
    // Valid `[0 | postcard(HighWaterAppliedShape)]` payloads at boundary
    // values. The fuzz harness drives `decode::<HighWaterApplied>(0, data)`,
    // so version 0 hits the happy path.
    for (name, value) in [
        ("seed_value_zero", 0u64),
        ("seed_value_one", 1u64),
        ("seed_value_max", u64::MAX),
    ] {
        let bytes = encode(0u8, &HighWaterAppliedShape { value }).expect("encode");
        write_seed(target, name, &bytes);
    }
    // Version-mismatch payload — exercises the `CodecError::Version` branch
    // when the harness invokes `decode::<HighWaterApplied>(0, data)`.
    let mismatched = encode(7u8, &HighWaterAppliedShape { value: 42 }).expect("encode");
    write_seed(target, "seed_version_mismatch", &mismatched);
}
