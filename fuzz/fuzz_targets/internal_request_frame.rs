#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() <= 16 * 1024 {
        let _ = postcard::from_bytes::<multidb::internal_transport::InternalRequest>(data);
    }
});
