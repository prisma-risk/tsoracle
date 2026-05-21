#![no_main]

use libfuzzer_sys::fuzz_target;
use tsoracle_openraft_toolkit::decode;
use tsoracle_driver_openraft::HighWaterApplied;

// Adversarial-bytes safety for the `[version_byte | postcard(body)]` envelope
// used for openraft RPC payloads and storage records (see
// `tsoracle-openraft-toolkit/src/codec.rs`). Splits coverage between the framing
// (empty input, version-mismatch branch) and the postcard body decode by
// driving both `decode::<HighWaterApplied>(0, data)` (expected-version path)
// and `decode::<HighWaterApplied>(data[0], data)` (escape from the version
// gate so the postcard decoder sees the whole body for any first byte).
//
// `HighWaterApplied` is intentionally tiny — it makes the framing logic the
// dominant source of new coverage relative to the existing
// `snapshot_payload_decode` target, which already saturates postcard on a
// large serde shape.
fuzz_target!(|data: &[u8]| {
    let _ = decode::<HighWaterApplied>(0, data);
    if let Some(&first) = data.first() {
        let _ = decode::<HighWaterApplied>(first, data);
    }
});
