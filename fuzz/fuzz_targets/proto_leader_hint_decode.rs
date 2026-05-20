#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use tsoracle_proto::v1::LeaderHint;

fuzz_target!(|data: &[u8]| {
    let _ = LeaderHint::decode(data);
});
