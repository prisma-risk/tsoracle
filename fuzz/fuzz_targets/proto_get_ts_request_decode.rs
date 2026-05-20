#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use tsoracle_proto::v1::GetTsRequest;

fuzz_target!(|data: &[u8]| {
    let _ = GetTsRequest::decode(data);
});
