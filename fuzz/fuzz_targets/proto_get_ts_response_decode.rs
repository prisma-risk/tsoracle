#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use tsoracle_proto::v1::GetTsResponse;

fuzz_target!(|data: &[u8]| {
    let _ = GetTsResponse::decode(data);
});
