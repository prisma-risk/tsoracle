#![no_main]

use libfuzzer_sys::fuzz_target;
use tsoracle_driver_file::record;

fuzz_target!(|data: &[u8]| {
    let _ = record::decode(data);
});
