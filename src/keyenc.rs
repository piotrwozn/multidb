use crate::storage::Bytes;

/// Returns the smallest byte string strictly greater than every key with `prefix`.
#[must_use]
pub fn successor(prefix: &[u8]) -> Option<Bytes> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
}

/// Returns an exclusive range end for `prefix`; an empty end means open-ended.
#[must_use]
pub fn range_end(prefix: &[u8]) -> Bytes {
    successor(prefix).unwrap_or_default()
}

#[must_use]
pub fn u32_prefix_range(value: u32) -> (Bytes, Bytes) {
    let start = value.to_be_bytes().to_vec();
    let end = range_end(&start);
    (start, end)
}

pub fn push_len_bytes(out: &mut Bytes, bytes: &[u8]) {
    out.extend_from_slice(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    out.extend_from_slice(bytes);
}

#[must_use]
pub fn read_len_bytes<'a>(key: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let len = read_len(key, cursor)?;
    let end = cursor.checked_add(len)?;
    if end > key.len() {
        return None;
    }
    let bytes = &key[*cursor..end];
    *cursor = end;
    Some(bytes)
}

#[must_use]
pub fn encode_i64_ordered(value: i64) -> [u8; 8] {
    let bits = u64::from_be_bytes(value.to_be_bytes());
    (bits ^ (1_u64 << 63)).to_be_bytes()
}

#[must_use]
pub fn encode_f64_ordered(value: f64) -> [u8; 8] {
    let bits = normalize_f64(value).to_bits();
    let ordered = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1_u64 << 63)
    };
    ordered.to_be_bytes()
}

pub fn encode_terminated_bytes(value: &[u8], out: &mut Bytes) {
    for byte in value {
        match byte {
            0x00 => out.extend_from_slice(&[0x00, 0xFF]),
            other => out.push(*other),
        }
    }

    out.extend_from_slice(&[0x00, 0x00]);
}

#[must_use]
pub fn normalize_f64(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn read_len(key: &[u8], cursor: &mut usize) -> Option<usize> {
    let end = cursor.checked_add(8)?;
    if end > key.len() {
        return None;
    }
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&key[*cursor..end]);
    *cursor = end;
    usize::try_from(u64::from_be_bytes(raw)).ok()
}

#[cfg(test)]
mod tests {
    use super::{
        encode_f64_ordered, push_len_bytes, range_end, read_len_bytes, successor, u32_prefix_range,
    };

    #[test]
    fn successor_carries_and_opens_max_prefix() {
        assert_eq!(successor(&[0x01, 0xFE]), Some(vec![0x01, 0xFF]));
        assert_eq!(successor(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(successor(&[0xFF, 0xFF]), None);
        assert_eq!(range_end(&[0xFF]), Vec::<u8>::new());
    }

    #[test]
    fn prefix_range_contains_keys_with_ff_bytes() {
        let prefix = [0x10, 0xFF];
        let end = range_end(&prefix);

        assert_eq!(end, vec![0x11]);
        assert!(prefix.as_slice() < end.as_slice());
        assert!(vec![0x10, 0xFF, 0x00] < end);
        assert!(vec![0x10, 0xFF, 0xFF] < range_end(&prefix));
    }

    #[test]
    fn max_u32_prefix_has_open_end() {
        let (start, end) = u32_prefix_range(u32::MAX);

        assert_eq!(start, vec![0xFF; 4]);
        assert!(end.is_empty());
    }

    #[test]
    fn len_bytes_round_trip_and_zero_normalizes() {
        let mut key = Vec::new();
        push_len_bytes(&mut key, &[0xAA, 0xFF, 0x00]);
        let mut cursor = 0;

        assert_eq!(
            read_len_bytes(&key, &mut cursor),
            Some([0xAA, 0xFF, 0x00].as_slice())
        );
        assert_eq!(cursor, key.len());
        assert_eq!(encode_f64_ordered(-0.0), encode_f64_ordered(0.0));
    }
}
