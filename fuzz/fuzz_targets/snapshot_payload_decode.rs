#![no_main]

use libfuzzer_sys::fuzz_target;
use tsoracle_driver_openraft::HighWaterStateMachineSnapshot;

// Adversarial-bytes safety for `install_snapshot` and snapshot-store loads.
// Snapshot bytes arrive from a leader over the openraft snapshot-install RPC
// (decoded at `state_machine.rs::install_snapshot`) and from disk via
// `SnapshotStore::load` (envelope unwrapped at `with_store`). The inner blob
// is a postcard-serialized `HighWaterStateMachineSnapshot` whose
// `last_membership` field is a length-prefixed openraft `StoredMembership` —
// exactly the kind of nested length-prefixed structure that rewards fuzzing.
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterStateMachineSnapshot>(data);
});
