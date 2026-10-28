//! Validate stored JSON siblings without constructing vectors/maps first.

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use crate::{repl::VersionedBytes, storage::StorageError};

/// Workspace of the slice decoder, distinct from the owned sibling allocations.
/// Unescaped strings are borrowed; numeric byte-array length does not determine
/// scratch capacity. Also cover `IgnoredAny`'s container stack and optional
/// `serde_json` number-parser features without depending on feature unification.
pub(super) fn working_bytes(bytes: &[u8]) -> usize {
    let mut start = None;
    let mut escaped = false;
    let mut has_escape = false;
    let mut longest_string = 0_usize;
    let mut longest_escaped = 0_usize;
    let mut depth = 0_usize;
    let mut max_depth = 0_usize;
    let mut number = 0_usize;
    let mut longest_number = 0_usize;
    for (index, &byte) in bytes.iter().enumerate() {
        if let Some(begin) = start {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
                has_escape = true;
            } else if byte == b'"' {
                longest_string = longest_string.max(index - begin);
                if has_escape {
                    longest_escaped = longest_escaped.max(index - begin);
                }
                start = None;
            }
            continue;
        }
        if byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E') {
            number = number.saturating_add(1);
            longest_number = longest_number.max(number);
        } else {
            number = 0;
        }
        match byte {
            b'"' => {
                start = Some(index + 1);
                has_escape = false;
            }
            b'[' | b'{' => {
                depth = depth.saturating_add(1);
                max_depth = max_depth.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    if let Some(begin) = start {
        longest_string = longest_string.max(bytes.len() - begin);
        if has_escape {
            longest_escaped = longest_escaped.max(bytes.len() - begin);
        }
    }
    // Vec capacity can grow to twice its contents. Malformed typed strings can
    // also be quoted into serde's error: at most six output bytes per input
    // byte, with capacity slack and overlapping formatting buffers. Reserve
    // that workspace before parsing too; it is constant for normal field names.
    longest_escaped
        .saturating_mul(2)
        .saturating_add(longest_string.saturating_mul(24))
        .saturating_add(max_depth.saturating_mul(2))
        .saturating_add(longest_number.saturating_mul(2))
        .saturating_add(512)
}

pub(super) fn versions_bound(bytes: &[u8], limit: usize) -> Result<usize, StorageError> {
    let mut bound = Bound { used: 0, limit };
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let result = Versions(&mut bound).deserialize(&mut decoder);
    if bound.used > limit {
        return Err(StorageError::ResourceLimit(
            "AP siblings exceed read budget".to_owned(),
        ));
    }
    result.map_err(|_| StorageError::Corruption("invalid stored AP siblings JSON".to_owned()))?;
    decoder
        .end()
        .map_err(|_| StorageError::Corruption("invalid stored AP siblings JSON".to_owned()))?;
    Ok(bound.used)
}

struct Bound {
    used: usize,
    limit: usize,
}
impl Bound {
    fn add<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        self.used = self.used.saturating_add(bytes);
        if self.used > self.limit {
            Err(E::custom("AP decoded allocation limit"))
        } else {
            Ok(())
        }
    }
}

struct Versions<'a>(&'a mut Bound);
impl<'de> DeserializeSeed<'de> for Versions<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        // JSON has no sequence size hint. Vec's first growth reserves at
        // least four non-byte elements, even for one sibling.
        self.0
            .add(std::mem::size_of::<VersionedBytes>().saturating_mul(4))?;
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for Versions<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AP versions")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while sequence.next_element_seed(Version(self.0))?.is_some() {}
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum Field {
    Value,
    Clock,
    Origin,
    #[serde(other)]
    Other,
}

struct Version<'a>(&'a mut Bound);
impl<'de> DeserializeSeed<'de> for Version<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0
            .add(std::mem::size_of::<VersionedBytes>().saturating_mul(2))?;
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Version<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AP version")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(field) = map.next_key::<Field>()? {
            match field {
                Field::Value => map.next_value_seed(Value(self.0))?,
                Field::Clock => map.next_value_seed(Clock(self.0))?,
                Field::Origin => {
                    map.next_value::<u64>()?;
                }
                Field::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}

struct Value<'a>(&'a mut Bound);
impl<'de> DeserializeSeed<'de> for Value<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_option(self)
    }
}
impl<'de> Visitor<'de> for Value<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("optional AP bytes")
    }
    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_some<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0.add(8)?; // minimum first-growth capacity of Vec<u8>
        decoder.deserialize_seq(self)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while sequence.next_element::<u8>()?.is_some() {
            self.0.add(2)?;
        }
        Ok(())
    }
}

struct Clock<'a>(&'a mut Bound);
impl<'de> DeserializeSeed<'de> for Clock<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Clock<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AP clock")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map.next_key::<u64>()?.is_some() {
            self.0.add(1_024)?;
            map.next_value::<u64>()?;
        }
        Ok(())
    }
}
