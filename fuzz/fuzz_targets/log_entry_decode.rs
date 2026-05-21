#![no_main]

use libfuzzer_sys::fuzz_target;
use tsoracle_driver_openraft::HighWaterCommand;

// Adversarial-bytes safety for the raft log entry decoder. `HighWaterCommand`
// rides every replicated entry through the openraft log store: bytes arrive
// over the wire from peers and from disk on recovery, so a panic here
// crashes a follower.
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterCommand>(data);
});
