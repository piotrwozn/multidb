//! Allocation-free validation before Parquet's allocating Thrift/page decoders.
//!
//! Always validate the same immutable bytes subsequently given to Arrow. A
//! second read from a mutable source would reintroduce a check/use race.
use super::{Guard, check, checked_size, decode_error, reserve};
use crate::{federation::FederationError, query::QueryError};
use parquet::{
    basic::{Compression, Type},
    compression::{CodecOptions, create_codec},
    data_type::Int32Type,
    decoding::{Decoder, DeltaBitPackDecoder},
    file::metadata::ColumnChunkMetaData,
};
use std::io::Read;

const MAX_DEPTH: usize = 64;
const ITEM_ALLOCATION: usize = 1024;

#[derive(Clone, Copy)]
enum Shape {
    File,
    Schema,
    RowGroup,
    ColumnChunk,
    Column,
    KeyValue,
    Sorting,
    ColumnOrder,
    Logical,
    Decimal,
    Time,
    TimeUnit,
    Integer,
    Variant,
    Geometry,
    Geography,
    GeoStats,
    Box,
    Empty,
    Page,
    DataV1,
    DataV2,
    Dictionary,
}

#[derive(Clone, Copy)]
enum Field {
    Scalar(u8),
    Struct(Shape),
    List(u8, Option<Shape>),
}

// Parquet's reader dispatches known field IDs without checking their wire type.
// Match its known grammar, not just the input's self-declared wire grammar.
#[allow(clippy::match_same_arms)] // Keep the table grouped by the protocol struct being validated.
fn field_shape(shape: Shape, id: i16) -> Option<Field> {
    use Field::{List, Scalar, Struct};
    use Shape::{
        Box as BoundingBox, Column, ColumnChunk, ColumnOrder, DataV1, DataV2, Decimal, Dictionary,
        Empty, File, GeoStats, Geography, Geometry, Integer, KeyValue, Logical, Page, RowGroup,
        Schema, Sorting, Time, TimeUnit, Variant,
    };
    Some(match (shape, id) {
        (File, 1) => Scalar(5),
        (File, 2) => List(12, Some(Schema)),
        (File, 3) => Scalar(6),
        (File, 4) => List(12, Some(RowGroup)),
        (File, 5) => List(12, Some(KeyValue)),
        (File, 6) => Scalar(8),
        (File, 7) => List(12, Some(ColumnOrder)),
        (Schema, 1..=3 | 5..=9) => Scalar(5),
        (Schema, 4) => Scalar(8),
        (Schema, 10) => Struct(Logical),
        (RowGroup, 1) => List(12, Some(ColumnChunk)),
        (RowGroup, 2 | 3 | 5) => Scalar(6),
        (RowGroup, 4) => List(12, Some(Sorting)),
        (RowGroup, 7) => Scalar(4),
        (ColumnChunk, 1) => Scalar(8),
        (ColumnChunk, 2 | 4 | 6) => Scalar(6),
        (ColumnChunk, 3) => Struct(Column),
        (ColumnChunk, 5 | 7) => Scalar(5),
        (Column, 1 | 4 | 15) => Scalar(5),
        (Column, 2) => List(5, None),
        (Column, 5..=7 | 9..=11 | 14) => Scalar(6),
        // Statistics and encoding/size statistics are deliberately skipped by
        // ArrowReaderOptions, exactly as unknown fields are skipped by Thrift.
        (Column, 17) => Struct(GeoStats),
        (KeyValue, 1 | 2) => Scalar(8),
        (Sorting, 1) => Scalar(5),
        (Sorting, 2 | 3) => Scalar(1),
        (ColumnOrder, 1) => Struct(Empty),
        (Logical, 1..=4 | 6 | 11..=15) => Struct(Empty),
        (Logical, 5) => Struct(Decimal),
        (Logical, 7 | 8) => Struct(Time),
        (Logical, 10) => Struct(Integer),
        (Logical, 16) => Struct(Variant),
        (Logical, 17) => Struct(Geometry),
        (Logical, 18) => Struct(Geography),
        (Decimal, 1 | 2) => Scalar(5),
        (Time, 1) => Scalar(1),
        (Time, 2) => Struct(TimeUnit),
        (TimeUnit, 1..=3) => Struct(Empty),
        (Integer | Variant, 1) => Scalar(3),
        (Integer, 2) => Scalar(1),
        (Geometry | Geography, 1) => Scalar(8),
        (Geography, 2) => Scalar(5),
        (GeoStats, 1) => Struct(BoundingBox),
        (GeoStats, 2) => List(5, None),
        (BoundingBox, 1..=8) => Scalar(7),
        (Page, 1..=4) => Scalar(5),
        (Page, 5) => Struct(DataV1),
        (Page, 6) => Struct(Empty),
        (Page, 7) => Struct(Dictionary),
        (Page, 8) => Struct(DataV2),
        (DataV1, 1..=4) | (DataV2, 1..=6) | (Dictionary, 1 | 2) => Scalar(5),
        (DataV2, 7) | (Dictionary, 3) => Scalar(1),
        _ => return None,
    })
}

fn invalid(message: &'static str) -> FederationError {
    decode_error(message)
}

fn overflow() -> FederationError {
    QueryError::ResourceLimit("Parquet decoder size overflow".to_owned()).into()
}

/// A Compact Thrift reader that skips binary fields and visits containers
/// without constructing vectors or strings from their declared lengths.
struct Compact<'a> {
    bytes: &'a [u8],
    position: usize,
    allocations: usize,
    guard: &'a Guard,
}

impl<'a> Compact<'a> {
    fn new(bytes: &'a [u8], guard: &'a Guard) -> Self {
        Self {
            bytes,
            position: 0,
            allocations: 0,
            guard,
        }
    }

    fn account(&mut self, bytes: usize) -> Result<(), FederationError> {
        check(self.guard)?;
        self.allocations = self.allocations.checked_add(bytes).ok_or_else(overflow)?;
        if self
            .guard
            .as_ref()
            .is_some_and(|guard| self.allocations > guard.remaining_bytes())
        {
            return Err(QueryError::ResourceLimit(
                "Parquet metadata/page allocations exceed the statement budget".to_owned(),
            )
            .into());
        }
        Ok(())
    }

    fn byte(&mut self) -> Result<u8, FederationError> {
        let value = *self
            .bytes
            .get(self.position)
            .ok_or_else(|| invalid("truncated Parquet Thrift value"))?;
        self.position += 1;
        Ok(value)
    }

    fn advance(&mut self, length: usize) -> Result<(), FederationError> {
        self.position = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid("Parquet Thrift length exceeds its input"))?;
        Ok(())
    }

    fn unsigned(&mut self, bits: u32) -> Result<u64, FederationError> {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = self.byte()?;
            let payload = u64::from(byte & 127);
            if shift >= bits || (payload > (u64::MAX >> shift)) {
                return Err(invalid("Parquet Thrift integer overflow"));
            }
            value |= payload << shift;
            if bits < 64 && value >= (1u64 << bits) {
                return Err(invalid("Parquet Thrift integer overflow"));
            }
            if byte & 128 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    fn integer(&mut self, bits: u32) -> Result<i64, FederationError> {
        let value = self.unsigned(bits)?;
        let magnitude =
            i64::try_from(value >> 1).map_err(|_| invalid("Parquet integer overflow"))?;
        Ok(if value & 1 == 0 {
            magnitude
        } else {
            -magnitude - 1
        })
    }

    fn field(&mut self, last: &mut i16) -> Result<Option<(i16, u8)>, FederationError> {
        check(self.guard)?;
        let header = self.byte()?;
        if header == 0 {
            return Ok(None);
        }
        let kind = header & 15;
        if !(1..=12).contains(&kind) {
            return Err(invalid("invalid Parquet Thrift field type"));
        }
        let delta = i16::from(header >> 4);
        *last = if delta == 0 {
            i16::try_from(self.integer(16)?).map_err(|_| invalid("invalid Parquet field id"))?
        } else {
            last.checked_add(delta)
                .ok_or_else(|| invalid("Parquet field id overflow"))?
        };
        if *last <= 0 {
            return Err(invalid("invalid Parquet field id"));
        }
        Ok(Some((*last, kind)))
    }

    fn list(&mut self) -> Result<(usize, u8), FederationError> {
        let header = self.byte()?;
        let kind = header & 15;
        let count = if header >> 4 == 15 {
            usize::try_from(self.unsigned(32)?).map_err(|_| overflow())?
        } else {
            usize::from(header >> 4)
        };
        if !(1..=12).contains(&kind) || kind == 2 {
            return Err(invalid("invalid Parquet Thrift list type"));
        }
        // Every element, including a zero-field struct, consumes at least a
        // byte. Reject forged counts before Parquet's Vec::with_capacity.
        if count > self.bytes.len() - self.position {
            return Err(invalid("Parquet Thrift list count exceeds its input"));
        }
        self.account(count.checked_mul(ITEM_ALLOCATION).ok_or_else(overflow)?)?;
        Ok((count, kind))
    }

    fn structure(&mut self, depth: usize) -> Result<(), FederationError> {
        if depth > MAX_DEPTH {
            return Err(invalid("Parquet Thrift nesting exceeds 64 levels"));
        }
        let mut last = 0;
        while let Some((_, kind)) = self.field(&mut last)? {
            self.skip(kind, depth, true)?;
        }
        Ok(())
    }

    fn typed_structure(&mut self, shape: Shape, depth: usize) -> Result<(), FederationError> {
        if depth > MAX_DEPTH {
            return Err(invalid("Parquet Thrift nesting exceeds 64 levels"));
        }
        let mut last = 0;
        while let Some((id, kind)) = self.field(&mut last)? {
            if matches!((shape, id), (Shape::File | Shape::ColumnChunk, 8 | 9)) {
                return Err(invalid("encrypted foreign Parquet metadata is unsupported"));
            }
            match field_shape(shape, id) {
                Some(Field::Scalar(expected)) => {
                    if kind != expected && !(expected == 1 && kind == 2) {
                        return Err(invalid("Parquet known field has an invalid wire type"));
                    }
                    self.skip(kind, depth, true)?;
                }
                Some(Field::Struct(child)) => {
                    if kind != 12 {
                        return Err(invalid("Parquet known struct has an invalid wire type"));
                    }
                    self.typed_structure(child, depth + 1)?;
                }
                Some(Field::List(expected, child)) => {
                    if kind != 9 {
                        return Err(invalid("Parquet known list has an invalid wire type"));
                    }
                    let (count, element) = self.list()?;
                    if element != expected {
                        return Err(invalid("Parquet known list has an invalid element type"));
                    }
                    for _ in 0..count {
                        if let Some(child) = child {
                            self.typed_structure(child, depth + 1)?;
                        } else {
                            self.skip(element, depth + 1, false)?;
                        }
                    }
                }
                None => self.skip(kind, depth, true)?,
            }
        }
        Ok(())
    }

    fn skip(&mut self, kind: u8, depth: usize, field: bool) -> Result<(), FederationError> {
        if depth > MAX_DEPTH {
            return Err(invalid("Parquet Thrift nesting exceeds 64 levels"));
        }
        match kind {
            1 | 2 if field => {} // Boolean struct fields are in the field tag.
            1 => {
                if !matches!(self.byte()?, 1 | 2) {
                    return Err(invalid("invalid Parquet Thrift boolean"));
                }
            }
            3 => self.advance(1)?,
            4 => {
                self.integer(16)?;
            }
            5 => {
                self.integer(32)?;
            }
            6 => {
                self.integer(64)?;
            }
            7 => self.advance(8)?,
            8 => {
                let length = usize::try_from(self.unsigned(32)?).map_err(|_| overflow())?;
                self.advance(length)?;
                self.account(checked_size(length, 4, 0)?)?;
            }
            9 | 10 => {
                let (count, element) = self.list()?;
                for _ in 0..count {
                    self.skip(element, depth + 1, false)?;
                }
            }
            11 => {
                let count = usize::try_from(self.unsigned(32)?).map_err(|_| overflow())?;
                if count != 0 {
                    let kinds = self.byte()?;
                    if count > (self.bytes.len() - self.position) / 2 {
                        return Err(invalid("Parquet Thrift map count exceeds its input"));
                    }
                    self.account(
                        count
                            .checked_mul(2 * ITEM_ALLOCATION)
                            .ok_or_else(overflow)?,
                    )?;
                    for _ in 0..count {
                        self.skip(kinds >> 4, depth + 1, false)?;
                        self.skip(kinds & 15, depth + 1, false)?;
                    }
                }
            }
            12 => self.structure(depth + 1)?,
            _ => return Err(invalid("invalid Parquet Thrift value type")),
        }
        Ok(())
    }

    /// Captures numeric fields without allocating a map. Duplicate known fields
    /// are rejected so preflight and decoder cannot choose different values.
    fn numbers(&mut self, v2_compressed: bool) -> Result<[Option<i64>; 9], FederationError> {
        let mut out = [None; 9];
        let mut last = 0;
        while let Some((id, kind)) = self.field(&mut last)? {
            let index = usize::try_from(id).map_err(|_| invalid("invalid Parquet field id"))?;
            if v2_compressed && id == 7 && matches!(kind, 1 | 2) {
                if out[0].is_some() {
                    return Err(invalid("duplicate Parquet compression flag"));
                }
                out[0] = Some(i64::from(kind == 1));
            } else if index < out.len() && kind == 5 {
                if out[index].is_some() {
                    return Err(invalid("duplicate Parquet numeric field"));
                }
                out[index] = Some(self.integer(32)?);
            } else {
                self.skip(kind, 1, true)?;
            }
        }
        Ok(out)
    }
}

pub(super) fn metadata(
    bytes: &[u8],
    expected_columns: usize,
    guard: &Guard,
) -> Result<usize, FederationError> {
    let mut typed = Compact::new(bytes, guard);
    typed.typed_structure(Shape::File, 0)?;
    if typed.position != bytes.len() {
        return Err(invalid("trailing Parquet metadata bytes"));
    }
    let mut input = Compact::new(bytes, guard);
    let mut schema_seen = false;
    let mut last = 0;
    while let Some((id, kind)) = input.field(&mut last)? {
        if id != 2 {
            input.skip(kind, 0, true)?;
            continue;
        }
        if schema_seen || kind != 9 {
            return Err(invalid("invalid or duplicate Parquet schema"));
        }
        schema_seen = true;
        let (count, element) = input.list()?;
        if element != 12 || count != expected_columns.checked_add(1).ok_or_else(overflow)? {
            return Err(invalid(
                "Parquet columns do not match the scalar foreign schema",
            ));
        }
        for index in 0..count {
            let fields = input.numbers(false)?;
            if index == 0 {
                if fields[1].is_some()
                    || fields[5] != Some(i64::try_from(expected_columns).map_err(|_| overflow())?)
                {
                    return Err(invalid("invalid Parquet root schema"));
                }
            } else if !fields[1].is_some_and(|kind| (0..=7).contains(&kind))
                || fields[5].is_some_and(|children| children != 0)
                || !matches!(fields[3], Some(0 | 1))
            {
                return Err(invalid(
                    "foreign Parquet columns must be flat, non-repeated scalars",
                ));
            }
        }
    }
    if !schema_seen || input.position != bytes.len() {
        return Err(invalid("invalid Parquet metadata structure"));
    }
    Ok(input.allocations.max(typed.allocations))
}

#[derive(Default, Debug)]
struct Page {
    kind: i64,
    decoded: usize,
    encoded: usize,
    values: usize,
    dictionary_values: usize,
    levels: usize,
    compressed: bool,
    encoding: i64,
    definition_encoding: i64,
}

fn required(value: Option<i64>) -> Result<usize, FederationError> {
    usize::try_from(value.ok_or_else(|| invalid("missing Parquet page field"))?)
        .map_err(|_| invalid("negative Parquet page size or value count"))
}

fn page(input: &mut Compact<'_>) -> Result<Page, FederationError> {
    let before = input.position;
    let old_allocations = input.allocations;
    input.typed_structure(Shape::Page, 0)?;
    let typed_allocations = input.allocations;
    input.position = before;
    input.allocations = old_allocations;
    let mut sizes = [None; 4];
    let mut headers: [Option<[Option<i64>; 9]>; 3] = [None; 3];
    let mut last = 0;
    while let Some((id, kind)) = input.field(&mut last)? {
        if (1..=3).contains(&id) {
            let index = usize::try_from(id).map_err(|_| overflow())?;
            if kind != 5 || sizes[index].is_some() {
                return Err(invalid("invalid or duplicate Parquet page size"));
            }
            sizes[index] = Some(input.integer(32)?);
        } else if matches!(id, 5 | 7 | 8) {
            let index = match id {
                5 => 0,
                7 => 1,
                _ => 2,
            };
            if kind != 12 || headers[index].is_some() {
                return Err(invalid("invalid or duplicate Parquet page subheader"));
            }
            headers[index] = Some(input.numbers(index == 2)?);
        } else {
            input.skip(kind, 0, true)?;
        }
    }
    let kind = sizes[1].ok_or_else(|| invalid("missing Parquet page type"))?;
    let mut page = Page {
        kind,
        decoded: required(sizes[2])?,
        encoded: required(sizes[3])?,
        compressed: true,
        ..Page::default()
    };
    match kind {
        0 => {
            let fields = headers[0]
                .as_ref()
                .ok_or_else(|| invalid("missing Parquet v1 header"))?;
            page.values = required(fields[1])?;
            page.encoding = fields[2].ok_or_else(|| invalid("missing Parquet data encoding"))?;
            page.definition_encoding =
                fields[3].ok_or_else(|| invalid("missing Parquet definition encoding"))?;
        }
        1 => {} // Index pages contain no decoded row values.
        2 => {
            page.dictionary_values = required(headers[1].as_ref().and_then(|h| h[1]))?;
        }
        3 => {
            let fields = headers[2]
                .as_ref()
                .ok_or_else(|| invalid("missing Parquet v2 page header"))?;
            page.values = required(fields[1])?;
            page.encoding = fields[4].ok_or_else(|| invalid("missing Parquet data encoding"))?;
            let nulls = required(fields[2])?;
            let rows = required(fields[3])?;
            let levels = required(fields[5])?
                .checked_add(required(fields[6])?)
                .ok_or_else(overflow)?;
            if nulls > page.values
                || rows != page.values
                || levels > page.decoded
                || levels > page.encoded
            {
                return Err(invalid("invalid Parquet scalar v2 page counts or levels"));
            }
            page.levels = levels;
            page.compressed = fields[0] != Some(0);
            if !page.compressed && page.encoded != page.decoded {
                return Err(invalid("uncompressed Parquet v2 page sizes differ"));
            }
        }
        _ => return Err(invalid("unsupported Parquet page type")),
    }
    input.allocations = input.allocations.max(typed_allocations);
    Ok(page)
}

#[derive(Debug)]
pub(super) struct ColumnBudget {
    pub(super) decoded: usize,
    pub(super) allocations: usize,
}

pub(super) fn column(
    bytes: &[u8],
    column: &ColumnChunkMetaData,
    group_rows: i64,
    batch_rows: usize,
    guard: &Guard,
) -> Result<ColumnBudget, FederationError> {
    let values = usize::try_from(column.num_values())
        .map_err(|_| invalid("negative Parquet column value count"))?;
    if column.num_values() != group_rows || group_rows < 0 {
        return Err(invalid(
            "Parquet scalar column value count differs from row group",
        ));
    }
    let declared = usize::try_from(column.uncompressed_size())
        .map_err(|_| invalid("negative Parquet decoded column size"))?;
    let mut input = Compact::new(bytes, guard);
    let mut decoded = 0usize;
    let mut data_values = 0usize;
    let mut dictionary_seen = false;
    let mut codec_working = 0usize;
    let mut dictionary_width = 0usize;
    while input.position < bytes.len() {
        check(guard)?;
        let start = input.position;
        let page = page(&mut input)?;
        let header = input.position - start;
        decoded = decoded
            .checked_add(header)
            .and_then(|n| n.checked_add(page.decoded))
            .ok_or_else(overflow)?;
        if decoded > declared {
            return Err(invalid(
                "Parquet page size exceeds the declared decoded column size",
            ));
        }
        let payload_start = input.position;
        input.advance(page.encoded)?;
        if page.compressed {
            let payload = &bytes[payload_start + page.levels..input.position];
            codec_working = codec_working.max(validate_compression(
                payload,
                page.decoded - page.levels,
                column.compression(),
                guard,
            )?);
        }
        data_values = data_values.checked_add(page.values).ok_or_else(overflow)?;
        if data_values > values {
            return Err(invalid(
                "Parquet page values exceed the scalar column count",
            ));
        }
        // Dictionary decoders construct the dictionary in full, independently
        // of the requested Arrow batch size or SQL LIMIT.
        if page.kind == 2 {
            if dictionary_seen || data_values != 0 {
                return Err(invalid("misplaced or duplicate Parquet dictionary page"));
            }
            dictionary_seen = true;
            let count = page.dictionary_values;
            let minimum = match column.column_descr().physical_type() {
                Type::BOOLEAN => count.checked_add(7).map(|n| n / 8),
                Type::INT32 | Type::FLOAT | Type::BYTE_ARRAY => count.checked_mul(4),
                Type::INT64 | Type::DOUBLE => count.checked_mul(8),
                Type::INT96 => count.checked_mul(12),
                Type::FIXED_LEN_BYTE_ARRAY => usize::try_from(column.column_descr().type_length())
                    .ok()
                    .filter(|width| *width != 0)
                    .and_then(|width| count.checked_mul(width)),
            }
            .ok_or_else(overflow)?;
            if minimum > page.decoded {
                return Err(invalid(
                    "Parquet dictionary count exceeds its decoded byte capacity",
                ));
            }
            input.account(checked_size(count, 64, 0)?)?;
        }
        input.account(checked_size(page.values, 32, 0)?)?;
        let expansion = validate_encoding(
            &bytes[payload_start..input.position],
            &page,
            column,
            batch_rows,
            &mut dictionary_width,
            codec_working,
            guard,
        )?;
        input.account(expansion)?;
    }
    if decoded != declared || data_values != values {
        return Err(invalid(
            "Parquet page totals do not match the column metadata",
        ));
    }
    Ok(ColumnBudget {
        decoded,
        allocations: input
            .allocations
            .checked_add(codec_working)
            .ok_or_else(overflow)?,
    })
}

#[allow(clippy::too_many_lines)] // Keep the guarded decode lifetime and length validations together.
fn validate_encoding(
    payload: &[u8],
    page: &Page,
    column: &ColumnChunkMetaData,
    batch_rows: usize,
    dictionary_width: &mut usize,
    codec_working: usize,
    guard: &Guard,
) -> Result<usize, FederationError> {
    let dictionary = page.kind == 2 && column.column_descr().physical_type() == Type::BYTE_ARRAY;
    let delta = matches!(page.encoding, 5..=7) && matches!(page.kind, 0 | 3);
    if !dictionary && !delta {
        return checked_size(
            *dictionary_width,
            batch_rows.min(page.values).saturating_mul(8),
            0,
        );
    }
    let scratch = checked_size(
        page.decoded,
        8,
        checked_size(page.values, 16, codec_working)?,
    )?;
    let _scratch = reserve(guard, scratch)?;
    let body = if page.kind == 3 {
        &payload[page.levels..]
    } else {
        payload
    };
    let expected = page.decoded - page.levels;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(expected)
        .map_err(|error| QueryError::ResourceLimit(error.to_string()))?;
    if page.compressed {
        if let Some(mut codec) =
            create_codec(column.compression(), &CodecOptions::default()).map_err(decode_error)?
        {
            codec
                .decompress(body, &mut decoded, Some(expected))
                .map_err(decode_error)?;
        } else {
            decoded.extend_from_slice(body);
        }
    } else {
        decoded.extend_from_slice(body);
    }
    if decoded.len() != expected {
        return Err(invalid(
            "Parquet decoded payload size differs from its page",
        ));
    }
    let data = if dictionary {
        decoded.as_slice()
    } else {
        value_data(&decoded, page, column)?
    };
    if dictionary {
        let mut position = 0usize;
        for _ in 0..page.dictionary_values {
            check(guard)?;
            let prefix = data
                .get(position..position.saturating_add(4))
                .ok_or_else(|| invalid("truncated Parquet dictionary value length"))?;
            let length = u32::from_le_bytes(
                prefix
                    .try_into()
                    .map_err(|_| invalid("invalid dictionary length"))?,
            ) as usize;
            position = position
                .checked_add(4)
                .and_then(|n| n.checked_add(length))
                .filter(|end| *end <= data.len())
                .ok_or_else(|| invalid("Parquet dictionary value exceeds its payload"))?;
            *dictionary_width = (*dictionary_width).max(length);
        }
        return checked_size(*dictionary_width, batch_rows.saturating_mul(8), 0);
    }
    if page.encoding == 5 {
        validate_delta_header(data, page.values, guard)?;
        return Ok(0);
    }
    let (first, offset) = delta_lengths(data, page.values, guard)?;
    if page.encoding == 6 {
        first
            .iter()
            .try_fold(0usize, |n, length| {
                n.checked_add(usize::try_from(*length).ok()?)
                    .filter(|n| *n <= data.len().saturating_sub(offset))
            })
            .ok_or_else(|| invalid("Parquet delta lengths exceed their payload"))?;
        return Ok(0);
    }
    let (suffix, suffix_offset) = delta_lengths(&data[offset..], page.values, guard)?;
    if first.len() != suffix.len() {
        return Err(invalid("Parquet delta prefix and suffix counts differ"));
    }
    let suffix_bytes = data
        .len()
        .checked_sub(offset)
        .and_then(|n| n.checked_sub(suffix_offset))
        .ok_or_else(overflow)?;
    let mut consumed = 0usize;
    let mut previous = 0usize;
    let mut widest = 0usize;
    for (prefix, suffix) in first.into_iter().zip(suffix) {
        check(guard)?;
        let prefix =
            usize::try_from(prefix).map_err(|_| invalid("negative Parquet delta prefix"))?;
        let suffix =
            usize::try_from(suffix).map_err(|_| invalid("negative Parquet delta suffix"))?;
        consumed = consumed
            .checked_add(suffix)
            .filter(|n| *n <= suffix_bytes)
            .ok_or_else(|| invalid("Parquet delta suffix exceeds its payload"))?;
        if prefix > previous {
            return Err(invalid("Parquet delta prefix exceeds the previous value"));
        }
        previous = prefix.checked_add(suffix).ok_or_else(overflow)?;
        widest = widest.max(previous);
    }
    checked_size(
        widest,
        batch_rows
            .min(page.values)
            .saturating_add(2)
            .saturating_mul(8),
        0,
    )
}

fn value_data<'a>(
    data: &'a [u8],
    page: &Page,
    column: &ColumnChunkMetaData,
) -> Result<&'a [u8], FederationError> {
    if page.kind != 0 || column.column_descr().max_def_level() == 0 {
        return Ok(data);
    }
    let offset = match page.definition_encoding {
        3 => {
            let prefix = data
                .get(..4)
                .ok_or_else(|| invalid("missing Parquet definition level size"))?;
            let size = u32::from_le_bytes(
                prefix
                    .try_into()
                    .map_err(|_| invalid("invalid definition size"))?,
            ) as usize;
            size.checked_add(4).ok_or_else(overflow)?
        }
        4 => page.values.checked_add(7).ok_or_else(overflow)? / 8,
        _ => return Err(invalid("unsupported Parquet definition encoding")),
    };
    data.get(offset..)
        .ok_or_else(|| invalid("Parquet definition levels exceed the page"))
}

fn validate_delta_header(
    data: &[u8],
    maximum: usize,
    guard: &Guard,
) -> Result<(usize, usize), FederationError> {
    let mut reader = Compact::new(data, guard);
    let block = usize::try_from(reader.unsigned(32)?).map_err(|_| overflow())?;
    let mini = usize::try_from(reader.unsigned(32)?).map_err(|_| overflow())?;
    let count = usize::try_from(reader.unsigned(32)?).map_err(|_| overflow())?;
    reader.integer(64)?;
    if count > maximum {
        return Err(invalid(
            "Parquet inner delta value count exceeds its data page",
        ));
    }
    if block == 0
        || mini == 0
        || block % 128 != 0
        || block % mini != 0
        || (block / mini) % 32 != 0
        || (count > 1 && mini > data.len() - reader.position)
        || block.checked_mul(64).is_none()
    {
        return Err(invalid("invalid Parquet delta block geometry"));
    }
    let per_mini = block / mini;
    let mut remaining = count.saturating_sub(1);
    while remaining != 0 {
        check(guard)?;
        // Validate every block's VLQ before calling BitReader, whose malformed
        // overlong-VLQ path asserts instead of returning a decoding error.
        reader.integer(64)?;
        let widths_start = reader.position;
        reader.advance(mini)?;
        for index in 0..mini {
            if remaining == 0 {
                break;
            }
            let width = usize::from(data[widths_start + index]);
            if width > 64 {
                return Err(invalid("invalid Parquet delta bit width"));
            }
            let packed = per_mini.checked_mul(width).ok_or_else(overflow)? / 8;
            reader.advance(packed)?;
            remaining = remaining.saturating_sub(per_mini);
        }
    }
    Ok((count, reader.position))
}

fn delta_lengths(
    data: &[u8],
    maximum: usize,
    guard: &Guard,
) -> Result<(Vec<i32>, usize), FederationError> {
    let (count, validated_end) = validate_delta_header(data, maximum, guard)?;
    let mut decoder = DeltaBitPackDecoder::<Int32Type>::new();
    decoder
        .set_data(bytes::Bytes::copy_from_slice(data), count)
        .map_err(decode_error)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|error| QueryError::ResourceLimit(error.to_string()))?;
    values.resize(count, 0);
    for chunk in values.chunks_mut(256) {
        check(guard)?;
        if decoder.get(chunk).map_err(decode_error)? != chunk.len() {
            return Err(invalid("truncated Parquet delta length values"));
        }
    }
    let offset = decoder.get_offset();
    if offset > data.len() || offset != validated_end {
        return Err(invalid("Parquet delta block extends beyond its payload"));
    }
    Ok((values, offset))
}

// Some Parquet codecs grow their output Vec with read_to_end even when a page
// supplies an expected size. Verify those immutable payloads with fixed output
// scratch before the upstream decoder can allocate an oversized output.
fn validate_compression(
    input: &[u8],
    expected: usize,
    codec: Compression,
    guard: &Guard,
) -> Result<usize, FederationError> {
    match codec {
        Compression::GZIP(_) => {
            let working = checked_size(input.len(), 4, 512 * 1024)?;
            let _working = reserve(guard, working)?;
            count_decoded(flate2::read::MultiGzDecoder::new(input), expected, guard)?;
            Ok(working)
        }
        Compression::BROTLI(_) => {
            let window = brotli_window(input)?;
            // Ring-buffer replacement can briefly hold old+new windows; other
            // decoder tables have bounded alphabets independent of output size.
            let working = checked_size(window, 2, 8 * 1024 * 1024)?;
            let _working = reserve(guard, working)?;
            count_decoded(brotli::Decompressor::new(input, 8192), expected, guard)?;
            Ok(working)
        }
        Compression::LZ4
            if input.starts_with(&[4, 34, 77, 24]) || input.starts_with(&[2, 33, 76, 24]) =>
        {
            // The Hadoop codec's compatibility branch uses an LZ4 frame
            // decoder that also ignores the page's expected output size.
            let working = 32 * 1024 * 1024;
            let _working = reserve(guard, working)?;
            count_decoded(lz4_flex::frame::FrameDecoder::new(input), expected, guard)?;
            Ok(working)
        }
        _ => Ok(0),
    }
}

fn count_decoded(
    mut reader: impl Read,
    expected: usize,
    guard: &Guard,
) -> Result<(), FederationError> {
    let mut buffer = [0u8; 8192];
    let mut decoded = 0usize;
    loop {
        check(guard)?;
        let count = reader.read(&mut buffer).map_err(decode_error)?;
        if count == 0 {
            break;
        }
        decoded = decoded.checked_add(count).ok_or_else(overflow)?;
        if decoded > expected {
            return Err(invalid(
                "Parquet payload expands beyond its declared page size",
            ));
        }
    }
    if decoded != expected {
        return Err(invalid(
            "Parquet payload does not match its declared page size",
        ));
    }
    Ok(())
}

fn brotli_window(input: &[u8]) -> Result<usize, FederationError> {
    let first = *input
        .first()
        .ok_or_else(|| invalid("truncated Brotli stream"))?;
    let bits = if first & 1 == 0 {
        16
    } else if first & 0x0e != 0 {
        17 + ((first >> 1) & 7)
    } else {
        match (first >> 4) & 7 {
            0 => 17,
            1 => {
                // Brotli's large-window extension, accepted by the upstream
                // decoder, can request up to 1 GiB. Account it before decoding.
                if first & 0x80 != 0 {
                    return Err(invalid("invalid Brotli window"));
                }
                let bits = input
                    .get(1)
                    .copied()
                    .ok_or_else(|| invalid("truncated Brotli window"))?
                    & 63;
                if !(10..=30).contains(&bits) {
                    return Err(invalid("invalid Brotli window"));
                }
                bits
            }
            n => 8 + n,
        }
    };
    Ok(1usize << bits)
}

#[cfg(test)]
#[path = "parquet_preflight_tests.rs"]
mod tests;
