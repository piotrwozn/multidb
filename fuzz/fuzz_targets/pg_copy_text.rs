#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(line) = std::str::from_utf8(data) {
        let _ = multidb::verification::parse_pg_copy_text_for_verification(line);
    }
});
