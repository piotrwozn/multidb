#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let end = multidb::keyenc::range_end(data);
    if !end.is_empty() {
        assert!(data < end.as_slice());
        assert!(data.iter().all(|byte| *byte == 0xff) || end.starts_with(common_prefix(data, &end)));
    }
    if let Some(successor) = multidb::keyenc::successor(data) {
        assert_eq!(successor, end);
    } else {
        assert!(end.is_empty());
    }
});

fn common_prefix<'a>(left: &'a [u8], right: &[u8]) -> &'a [u8] {
    let len = left
        .iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count();
    &left[..len]
}
