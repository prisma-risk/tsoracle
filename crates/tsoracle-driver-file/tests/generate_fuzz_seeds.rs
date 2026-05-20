//! Regenerates the fuzz seed corpus for the record_decode and
//! record_roundtrip targets, deriving valid-record bytes from the
//! canonical encoder in tsoracle_driver_file::record.
//!
//! Run with:
//!
//!     cargo test -p tsoracle-driver-file --test generate_fuzz_seeds -- --ignored
//!
//! Idempotent: writes the same bytes every time. Re-run after any change
//! to record::encode to refresh the on-disk seeds. The seeds are then
//! committed to git so every fuzz run on every machine starts from the
//! same deterministic floor.

use std::fs;
use std::path::PathBuf;
use tsoracle_driver_file::record::{RECORD_LEN, encode};

fn fuzz_corpus_dir(target: &str) -> PathBuf {
    // tests/ runs with CWD = crate root, so step up two levels to the
    // repo root, then into the fuzz crate's corpus directory.
    PathBuf::from("../..").join("fuzz/corpus").join(target)
}

fn write_seed(target: &str, name: &str, bytes: &[u8]) {
    let dir = fuzz_corpus_dir(target);
    fs::create_dir_all(&dir).expect("create seed corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed file");
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_record_decode_seeds() {
    let target = "record_decode";
    write_seed(target, "seed_empty", &[]);
    write_seed(target, "seed_short_16_zeros", &[0u8; 16]);
    write_seed(target, "seed_17_zeros", &[0u8; RECORD_LEN]);
    write_seed(target, "seed_17_all_ff", &[0xFFu8; RECORD_LEN]);
    write_seed(target, "seed_encode_0", &encode(0));
    write_seed(target, "seed_encode_max", &encode(u64::MAX));
    write_seed(target, "seed_encode_typical", &encode(1_700_000_000_000));
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_record_roundtrip_seeds() {
    let target = "record_roundtrip";
    // Forward-direction seeds: 8-byte little-endian u64 values that the
    // harness reads via from_le_bytes to drive encode/decode roundtrips.
    write_seed(target, "seed_u64_zero", &0u64.to_le_bytes());
    write_seed(target, "seed_u64_one", &1u64.to_le_bytes());
    write_seed(target, "seed_u64_typical", &1_700_000_000_000u64.to_le_bytes());
    write_seed(target, "seed_u64_max", &u64::MAX.to_le_bytes());
}
