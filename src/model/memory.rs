//! Allocation preflight for the canonical and legacy Value codecs.

use serde::de::{DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};

use super::{
    StorageError, TAG_ARRAY, TAG_BOOL, TAG_BYTES, TAG_FLOAT, TAG_GEO_POINT, TAG_INT, TAG_NULL,
    TAG_OBJECT, TAG_STR, TAG_VECTOR, VALUE_FORMAT_V1, VALUE_MAGIC, Value, ValueCodecLimits,
    ensure_encoded_size, read_array, read_len, read_len_bytes, read_u8, str, value_too_deep_error,
};

/// Bounds decoded heap/inline storage without constructing a `Value`.
///
/// Strings and binary/vector payloads are charged by length; arrays include
/// their element storage and objects include B-tree node allocations. Legacy
/// serde vectors include growth capacity. The input buffer and JSON unescape
/// scratch (at most the input length) must be accounted by the caller as well.
/// A bound above `stop_after` may return early; ordinary decoding remains
/// responsible for full validation. This does not change `decode_value`.
/// # Errors
/// Fails for malformed encodings encountered before the allocation limit.
pub fn value_decode_memory_bound(bytes: &[u8], stop_after: usize) -> Result<usize, StorageError> {
    let limits = ValueCodecLimits::default();
    ensure_encoded_size(bytes, limits)?;
    let mut memory = ValueMemoryCounter {
        used: 0,
        stop_after,
    };
    if bytes.starts_with(VALUE_MAGIC) {
        if bytes.get(VALUE_MAGIC.len()) != Some(&VALUE_FORMAT_V1) {
            return Err(StorageError::Corruption(
                "unsupported or truncated value header".to_owned(),
            ));
        }
        let mut cursor = VALUE_MAGIC.len() + 1;
        binary(bytes, &mut cursor, 0, limits, &mut memory)?;
        if !memory.exceeded() && cursor != bytes.len() {
            return Err(StorageError::Corruption(
                "trailing bytes after value".to_owned(),
            ));
        }
    } else {
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        let result = ValueSeed {
            memory: &mut memory,
            depth: 0,
        }
        .deserialize(&mut decoder);
        if !memory.exceeded() {
            result.map_err(|error| StorageError::Corruption(error.to_string()))?;
            decoder
                .end()
                .map_err(|error| StorageError::Corruption(error.to_string()))?;
        }
    }
    Ok(memory.used)
}

/// Allocation-only preflight for typed legacy JSON containers holding Values.
/// Seeds validate shapes without constructing Values; callers separately reserve
/// input/unescape scratch and the bound before the ordinary typed deserialization.
pub(crate) struct ValueMemoryCounter {
    used: usize,
    stop_after: usize,
}
impl ValueMemoryCounter {
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            used: 0,
            stop_after: limit,
        }
    }
    pub(crate) const fn used(&self) -> usize {
        self.used
    }
    pub(crate) fn value_seed(&mut self) -> impl for<'de> DeserializeSeed<'de, Value = ()> + '_ {
        ValueSeed {
            memory: self,
            depth: 0,
        }
    }
    pub(crate) fn row_seed(&mut self) -> impl for<'de> DeserializeSeed<'de, Value = ()> + '_ {
        RowSeed(self)
    }
    fn add(&mut self, bytes: usize) {
        self.used = self.used.saturating_add(bytes);
    }
    fn exceeded(&self) -> bool {
        self.used > self.stop_after
    }
    pub(crate) fn charge<E: serde::de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        self.add(bytes);
        if self.exceeded() {
            Err(E::custom("decoded allocation limit reached"))
        } else {
            Ok(())
        }
    }
}

struct RowSeed<'a>(&'a mut ValueMemoryCounter);
impl<'de> DeserializeSeed<'de> for RowSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0.charge(std::mem::size_of::<Vec<Value>>())?;
        ArraySeed {
            memory: self.0,
            depth: 0,
        }
        .deserialize(decoder)
    }
}

fn binary(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ValueCodecLimits,
    memory: &mut ValueMemoryCounter,
) -> Result<(), StorageError> {
    if depth > limits.max_nesting_depth {
        return Err(value_too_deep_error(limits.max_nesting_depth));
    }
    memory.add(std::mem::size_of::<Value>());
    if memory.exceeded() {
        return Ok(());
    }
    match read_u8(bytes, cursor)? {
        TAG_NULL => {}
        TAG_BOOL => {
            read_u8(bytes, cursor)?;
        }
        TAG_INT | TAG_FLOAT => {
            read_array::<8>(bytes, cursor)?;
        }
        TAG_GEO_POINT => {
            read_array::<16>(bytes, cursor)?;
        }
        TAG_STR | TAG_BYTES => memory.add(read_len_bytes(bytes, cursor)?.len()),
        TAG_VECTOR => {
            let count = read_len(bytes, cursor)?;
            let len = count
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| StorageError::Corruption("vector length overflows".to_owned()))?;
            if len > bytes.len().saturating_sub(*cursor) {
                return Err(StorageError::Corruption("truncated vector".to_owned()));
            }
            memory.add(len);
            *cursor += len;
        }
        TAG_ARRAY => {
            let count = read_len(bytes, cursor)?;
            if count > bytes.len().saturating_sub(*cursor) {
                return Err(StorageError::Corruption(
                    "array item count exceeds remaining bytes".to_owned(),
                ));
            }
            // The canonical decoder reserves exactly count elements. Each
            // recursive charge includes the inline Value in that allocation.
            for _ in 0..count {
                binary(bytes, cursor, depth + 1, limits, memory)?;
                if memory.exceeded() {
                    break;
                }
            }
        }
        TAG_OBJECT => {
            let count = read_len(bytes, cursor)?;
            if count
                .checked_mul(9)
                .is_none_or(|minimum| minimum > bytes.len().saturating_sub(*cursor))
            {
                return Err(StorageError::Corruption(
                    "object item count exceeds remaining bytes".to_owned(),
                ));
            }
            // A root node plus conservative per-entry leaf/internal capacity.
            if count != 0 {
                memory.add(1_024);
            }
            for _ in 0..count {
                memory.add(read_len_bytes(bytes, cursor)?.len().saturating_add(256));
                if memory.exceeded() {
                    break;
                }
                binary(bytes, cursor, depth + 1, limits, memory)?;
                if memory.exceeded() {
                    break;
                }
            }
        }
        tag => return Err(StorageError::Corruption(format!("bad value tag {tag}"))),
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(field_identifier)]
enum Kind {
    Null,
    Bool,
    Int,
    Float,
    Str,
    Bytes,
    Array,
    Object,
    Vector,
    GeoPoint,
}

struct ValueSeed<'a> {
    memory: &'a mut ValueMemoryCounter,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        if self.depth > ValueCodecLimits::default().max_nesting_depth {
            return Err(serde::de::Error::custom("value nesting depth exceeded"));
        }
        self.memory.charge(std::mem::size_of::<Value>())?;
        decoder.deserialize_enum(
            "Value",
            &[
                "Null", "Bool", "Int", "Float", "Str", "Bytes", "Array", "Object", "Vector",
                "GeoPoint",
            ],
            self,
        )
    }
}
impl<'de> Visitor<'de> for ValueSeed<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an encoded Value")
    }
    fn visit_enum<A: EnumAccess<'de>>(self, value: A) -> Result<(), A::Error> {
        let (kind, variant) = value.variant::<Kind>()?;
        match kind {
            Kind::Null => variant.unit_variant(),
            Kind::Bool => variant.newtype_variant::<bool>().map(|_| ()),
            Kind::Int => variant.newtype_variant::<i64>().map(|_| ()),
            Kind::Float => variant.newtype_variant::<f64>().map(|_| ()),
            Kind::Str => variant.newtype_variant_seed(TextSeed(self.memory)),
            Kind::Bytes => variant.newtype_variant_seed(ScalarSequence {
                memory: self.memory,
                vector: false,
            }),
            Kind::Vector => variant.newtype_variant_seed(ScalarSequence {
                memory: self.memory,
                vector: true,
            }),
            Kind::Array => variant.newtype_variant_seed(ArraySeed {
                memory: self.memory,
                depth: self.depth + 1,
            }),
            Kind::Object => variant.newtype_variant_seed(ObjectSeed {
                memory: self.memory,
                depth: self.depth + 1,
            }),
            Kind::GeoPoint => variant
                .newtype_variant::<serde::de::IgnoredAny>()
                .map(|_| ()),
        }
    }
}

struct TextSeed<'a>(&'a mut ValueMemoryCounter);
impl<'de> DeserializeSeed<'de> for TextSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_str(self)
    }
}
impl Visitor<'_> for TextSeed<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a string")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        self.0.charge(value.len())
    }
}

struct ScalarSequence<'a> {
    memory: &'a mut ValueMemoryCounter,
    vector: bool,
}
impl<'de> DeserializeSeed<'de> for ScalarSequence<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for ScalarSequence<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a scalar sequence")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        let mut first = true;
        if self.vector {
            while values.next_element::<f32>()?.is_some() {
                self.memory.charge(if first {
                    4 * std::mem::size_of::<f32>()
                } else {
                    2 * std::mem::size_of::<f32>()
                })?;
                first = false;
            }
        } else {
            while values.next_element::<u8>()?.is_some() {
                self.memory.charge(if first { 8 } else { 2 })?;
                first = false;
            }
        }
        Ok(())
    }
}

struct ArraySeed<'a> {
    memory: &'a mut ValueMemoryCounter,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for ArraySeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for ArraySeed<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Value array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        let mut first = true;
        while values
            .next_element_seed(ValueSeed {
                memory: self.memory,
                depth: self.depth,
            })?
            .is_some()
        {
            self.memory.charge(if first {
                3 * std::mem::size_of::<Value>()
            } else {
                std::mem::size_of::<Value>()
            })?;
            first = false;
        }
        Ok(())
    }
}

struct ObjectSeed<'a> {
    memory: &'a mut ValueMemoryCounter,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for ObjectSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for ObjectSeed<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Value object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        let mut first = true;
        while values.next_key_seed(TextSeed(self.memory))?.is_some() {
            self.memory.charge(if first { 1_280 } else { 256 })?;
            first = false;
            values.next_value_seed(ValueSeed {
                memory: self.memory,
                depth: self.depth,
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{decode_value, encode_value};
    use std::collections::BTreeMap;

    #[test]
    fn nested_value_and_bare_row_seeds_share_allocation_accounting()
    -> Result<(), Box<dyn std::error::Error>> {
        let row = vec![
            Value::Null,
            Value::Str("nested".to_owned()),
            Value::Array(vec![Value::Int(1)]),
        ];
        let array = Value::Array(row.clone());
        let encoded = serde_json::to_vec(&array)?;
        let expected = value_decode_memory_bound(&encoded, usize::MAX)?;
        let mut counter = ValueMemoryCounter::new(usize::MAX);
        counter
            .value_seed()
            .deserialize(&mut serde_json::Deserializer::from_slice(&encoded))?;
        assert_eq!(counter.used(), expected);
        let mut counter = ValueMemoryCounter::new(usize::MAX);
        counter
            .row_seed()
            .deserialize(&mut serde_json::Deserializer::from_slice(
                &serde_json::to_vec(&row)?,
            ))?;
        assert_eq!(
            counter.used(),
            expected - std::mem::size_of::<Value>() + std::mem::size_of::<Vec<Value>>()
        );
        let mut counter = ValueMemoryCounter::new(64);
        assert!(
            counter
                .row_seed()
                .deserialize(&mut serde_json::Deserializer::from_slice(
                    &serde_json::to_vec(&row)?
                ))
                .is_err()
        );
        assert!(counter.used() > 64);
        Ok(())
    }

    #[test]
    fn large_strings_are_charged_by_length_in_both_codecs() -> Result<(), Box<dyn std::error::Error>>
    {
        let value = Value::Str("x".repeat(2 * 1024 * 1024));
        for bytes in [encode_value(&value)?, serde_json::to_vec(&value)?] {
            let bound = value_decode_memory_bound(&bytes, 3 * 1024 * 1024)?;
            assert!(bound >= 2 * 1024 * 1024);
            assert!(bound < 3 * 1024 * 1024);
            assert!(value_decode_memory_bound(&bytes, 1024)? > 1024);
            assert_eq!(decode_value(&bytes)?, value);
        }
        Ok(())
    }

    #[test]
    fn tiny_array_encodings_cannot_hide_decoded_element_allocations()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = Value::Array(vec![Value::Null; 10_000]);
        for bytes in [encode_value(&value)?, serde_json::to_vec(&value)?] {
            let bound = value_decode_memory_bound(&bytes, usize::MAX)?;
            assert!(bound >= std::mem::size_of::<Value>() * 10_001);
            let stopped = value_decode_memory_bound(&bytes, 4096)?;
            assert!(stopped > 4096 && stopped <= 4096 + 4 * std::mem::size_of::<Value>());
        }
        Ok(())
    }

    #[test]
    fn legacy_nested_types_and_growth_capacity_are_preflighted()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = Value::Object(BTreeMap::from([
            ("array".to_owned(), Value::Array(vec![Value::Int(7)])),
            ("bytes".to_owned(), Value::Bytes(vec![1])),
            ("vector".to_owned(), Value::Vector(vec![1.5])),
            ("point".to_owned(), Value::GeoPoint { lon: 1., lat: 2. }),
            ("escaped".to_owned(), Value::Str("a\n\"ą".to_owned())),
        ]));
        for bytes in [encode_value(&value)?, serde_json::to_vec(&value)?] {
            assert!(value_decode_memory_bound(&bytes, usize::MAX)? > 1024);
            assert_eq!(decode_value(&bytes)?, value);
        }
        let one = Value::Array(vec![Value::Null]);
        assert!(
            value_decode_memory_bound(&serde_json::to_vec(&one)?, usize::MAX)?
                >= 5 * std::mem::size_of::<Value>()
        );
        Ok(())
    }

    #[test]
    fn malformed_lengths_and_trailing_bytes_fail_before_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut bytes = VALUE_MAGIC.to_vec();
        bytes.extend_from_slice(&[VALUE_FORMAT_V1, TAG_ARRAY]);
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        assert!(value_decode_memory_bound(&bytes, usize::MAX).is_err());
        let mut bytes = encode_value(&Value::Str("hello".to_owned()))?;
        bytes.pop();
        assert!(value_decode_memory_bound(&bytes, usize::MAX).is_err());
        let mut bytes = encode_value(&Value::Null)?;
        bytes.push(0);
        assert!(value_decode_memory_bound(&bytes, usize::MAX).is_err());
        assert!(value_decode_memory_bound(b"{\"Array\":[\"Null\"]} trailing", usize::MAX).is_err());
        Ok(())
    }
}
