#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = multidb::txn::decode_commit_log_record(data);
});
