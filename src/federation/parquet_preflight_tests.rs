use super::*;
use crate::{
    cloud::ObjectStoreUri,
    federation::{ForeignScanGuard, ForeignScanRequest, ForeignScanReservation, ForeignSource},
    model::Value,
    query::{ColumnDef, ColumnType, TableSchema},
};
use datafusion::arrow::{
    array::{BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel},
    file::{metadata::ColumnChunkMetaData, properties::WriterProperties, reader::ChunkReader},
    schema::{
        parser::parse_message_type,
        types::{ColumnPath, SchemaDescriptor},
    },
};
use std::{
    fs::File,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Budget {
    used: Arc<AtomicUsize>,
    limit: usize,
}
struct Token(Arc<AtomicUsize>, usize);
impl Drop for Token {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::AcqRel);
    }
}
impl ForeignScanGuard for Budget {
    fn check(&self) -> Result<(), QueryError> {
        Ok(())
    }
    fn charge(&self, bytes: usize) -> Result<(), QueryError> {
        let old = self.used.fetch_add(bytes, Ordering::AcqRel);
        if old.checked_add(bytes).is_none_or(|n| n > self.limit) {
            self.used.fetch_sub(bytes, Ordering::AcqRel);
            return Err(QueryError::ResourceLimit(
                "test Parquet memory budget".to_owned(),
            ));
        }
        Ok(())
    }
    fn remaining_bytes(&self) -> usize {
        self.limit.saturating_sub(self.used.load(Ordering::Acquire))
    }
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ForeignScanReservation>, QueryError> {
        self.charge(bytes)?;
        Ok(Box::new(Token(Arc::clone(&self.used), bytes)))
    }
}
#[allow(clippy::unnecessary_wraps)] // Fixtures intentionally match the optional production Guard type.
fn budget(limit: usize) -> Guard {
    Some(Arc::new(Budget {
        used: Arc::new(AtomicUsize::new(0)),
        limit,
    }))
}

fn varint(mut n: u64, out: &mut Vec<u8>) -> Result<(), std::num::TryFromIntError> {
    while n >= 128 {
        out.push(u8::try_from(n & 127)? | 128);
        n >>= 7;
    }
    out.push(u8::try_from(n)?);
    Ok(())
}
fn integer(n: u32, out: &mut Vec<u8>) -> Result<(), std::num::TryFromIntError> {
    varint(u64::from(n) * 2, out)
}
fn page_header(
    kind: u32,
    decoded: u32,
    encoded: u32,
    values: u32,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    page_with_encoding(kind, decoded, encoded, values, 0)
}

fn page_with_encoding(
    kind: u32,
    decoded: u32,
    encoded: u32,
    values: u32,
    encoding: u32,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut bytes = vec![0x15];
    integer(kind, &mut bytes)?;
    bytes.push(0x15);
    integer(decoded, &mut bytes)?;
    bytes.push(0x15);
    integer(encoded, &mut bytes)?;
    bytes.push(if kind == 2 { 0x4c } else { 0x2c });
    bytes.push(0x15);
    integer(values, &mut bytes)?;
    bytes.push(0x15);
    integer(encoding, &mut bytes)?;
    if kind == 0 {
        bytes.push(0x15);
        integer(3, &mut bytes)?; // RLE levels
        bytes.push(0x15);
        integer(3, &mut bytes)?;
    }
    bytes.extend_from_slice(&[0, 0]);
    Ok(bytes)
}
fn column_metadata(
    decoded: usize,
    encoded: usize,
    values: i64,
    codec: Compression,
) -> Result<ColumnChunkMetaData, Box<dyn std::error::Error>> {
    let schema = SchemaDescriptor::new(Arc::new(parse_message_type(
        "message root { REQUIRED INT64 id; }",
    )?));
    Ok(ColumnChunkMetaData::builder(schema.column(0))
        .set_num_values(values)
        .set_compression(codec)
        .set_total_compressed_size(i64::try_from(encoded)?)
        .set_total_uncompressed_size(i64::try_from(decoded)?)
        .build()?)
}

#[test]
fn forged_footer_collection_count_fails_before_allocating() -> TestResult {
    // Version, then a schema list claiming 2^31-1 structs in a tiny footer.
    let mut bytes = vec![0x15, 2, 0x19, 0xfc];
    varint(i32::MAX.unsigned_abs().into(), &mut bytes)?;
    bytes.push(0);
    let error = metadata(&bytes, 1, &budget(16 * 1024))
        .err()
        .ok_or("accepted forged list")?;
    assert!(error.to_string().contains("list count exceeds"), "{error}");
    Ok(())
}

#[test]
fn forged_page_size_and_dictionary_count_fail_before_decoder() -> TestResult {
    let mut forged = page_header(0, i32::MAX.unsigned_abs(), 1, 1)?;
    forged.push(0);
    let meta = column_metadata(32, forged.len(), 1, Compression::SNAPPY)?;
    let error = column(&forged, &meta, 1, 1, &budget(64 * 1024))
        .err()
        .ok_or("accepted page bomb")?;
    assert!(error.to_string().contains("page size exceeds"), "{error}");

    let mut dictionary = page_header(2, 8, 8, i32::MAX.unsigned_abs())?;
    let header_len = dictionary.len();
    dictionary.extend_from_slice(&[0; 8]);
    let meta = column_metadata(
        header_len + 8,
        dictionary.len(),
        1,
        Compression::UNCOMPRESSED,
    )?;
    let error = column(&dictionary, &meta, 1, 1, &budget(64 * 1024))
        .err()
        .ok_or("accepted dictionary bomb")?;
    assert!(
        error.to_string().contains("dictionary count exceeds"),
        "{error}"
    );
    Ok(())
}

#[test]
fn forged_data_value_count_cannot_expand_an_rle_page() -> TestResult {
    let mut bytes = page_header(0, 1, 1, i32::MAX.unsigned_abs())?;
    let header = bytes.len();
    bytes.push(0);
    let meta = column_metadata(header + 1, bytes.len(), 1, Compression::UNCOMPRESSED)?;
    let error = column(&bytes, &meta, 1, 1, &budget(64 * 1024))
        .err()
        .ok_or("accepted value-count bomb")?;
    assert!(error.to_string().contains("page values exceed"), "{error}");
    Ok(())
}

#[test]
fn gzip_payload_bomb_is_stopped_even_with_consistent_page_and_footer_sizes() -> TestResult {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&vec![0; 1024 * 1024])?;
    let compressed = encoder.finish()?;
    let mut bytes = page_header(0, 8, u32::try_from(compressed.len())?, 1)?;
    let header = bytes.len();
    bytes.extend_from_slice(&compressed);
    let meta = column_metadata(
        header + 8,
        bytes.len(),
        1,
        Compression::GZIP(GzipLevel::default()),
    )?;
    let error = column(&bytes, &meta, 1, 1, &budget(2 * 1024 * 1024))
        .err()
        .ok_or("accepted payload bomb")?;
    assert!(error.to_string().contains("expands beyond"), "{error}");
    Ok(())
}

#[test]
fn malformed_thrift_depth_and_integer_do_not_reach_the_upstream_parser() {
    let mut nested = vec![0x1c; 66];
    nested.extend_from_slice(&[0; 67]);
    assert!(metadata(&nested, 1, &None).is_err());
    let mut overflow = vec![0x16];
    overflow.extend_from_slice(&[0xff; 10]);
    assert!(metadata(&overflow, 1, &None).is_err());
}

#[test]
fn known_thrift_field_types_cannot_desynchronize_preflight_and_decoder() -> TestResult {
    // Known FileMetaData.version is an i32. A lying binary field tag must not
    // let preflight skip bytes that the upstream parser reads as another field.
    let bytes = [0x18, 2, 0x19, 0xff, 0];
    let error = metadata(&bytes, 1, &None)
        .err()
        .ok_or("accepted mismatched field type")?;
    assert!(error.to_string().contains("invalid wire type"), "{error}");
    Ok(())
}

#[test]
fn inner_delta_count_and_overlong_block_varint_fail_before_allocations() -> TestResult {
    let mut payload = vec![128, 1, 4]; // Block size 128; four mini-blocks.
    varint(i32::MAX.unsigned_abs().into(), &mut payload)?;
    payload.push(0);
    let mut bytes = page_with_encoding(
        0,
        u32::try_from(payload.len())?,
        u32::try_from(payload.len())?,
        1,
        6,
    )?;
    let header = bytes.len();
    bytes.extend_from_slice(&payload);
    let meta = column_metadata(
        header + payload.len(),
        bytes.len(),
        1,
        Compression::UNCOMPRESSED,
    )?;
    let error = column(&bytes, &meta, 1, 1, &budget(64 * 1024))
        .err()
        .ok_or("accepted inner delta count")?;
    assert!(
        error
            .to_string()
            .contains("inner delta value count exceeds"),
        "{error}"
    );
    let mut overlong_block = vec![128, 1, 4, 2, 0];
    overlong_block.extend_from_slice(&[255; 12]);
    assert!(validate_delta_header(&overlong_block, 2, &None).is_err());
    Ok(())
}

#[test]
fn legal_delta_encodings_and_nullable_values_work_across_block_and_batch_boundaries() -> TestResult
{
    let temporary = tempfile::tempdir()?;
    let arrow = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("bytes", DataType::Binary, true),
    ]));
    let text: Vec<_> = (0..300)
        .map(|i| (i % 7 != 0).then(|| format!("prefix-{i:04}-{}", "x".repeat(i % 13))))
        .collect();
    let binary: Vec<_> = text
        .iter()
        .map(|v| v.as_ref().map(String::as_bytes))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&arrow),
        vec![
            Arc::new(Int64Array::from_iter_values(0..300)),
            Arc::new(StringArray::from_iter(text.iter().map(|v| v.as_deref()))),
            Arc::new(BinaryArray::from(binary)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_column_encoding(
            ColumnPath::new(vec!["id".to_owned()]),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_encoding(
            ColumnPath::new(vec!["text".to_owned()]),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )
        .set_column_encoding(
            ColumnPath::new(vec!["bytes".to_owned()]),
            Encoding::DELTA_BYTE_ARRAY,
        )
        .build();
    let file = File::create(temporary.path().join("delta.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, arrow, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("text", ColumnType::Str, true),
            ColumnDef::new("bytes", ColumnType::Bytes, true),
        ],
        0,
    );
    let source = ForeignSource::Parquet {
        uri: ObjectStoreUri::from_local_dir(temporary.path()),
        path: "delta.parquet".to_owned(),
    };
    let result = super::super::scan_local(
        &schema,
        &source,
        &ForeignScanRequest::new(Some(vec![2, 0, 1, 2]), vec![], None),
        17,
        &budget(8 * 1024 * 1024),
    )?;
    assert_eq!(result.rows.len(), 300);
    for (index, row) in result.rows.iter().enumerate() {
        let string = text[index]
            .as_ref()
            .map_or(Value::Null, |v| Value::Str(v.clone()));
        let bytes = text[index]
            .as_ref()
            .map_or(Value::Null, |v| Value::Bytes(v.as_bytes().to_vec()));
        assert_eq!(
            row,
            &vec![
                bytes.clone(),
                Value::Int(i64::try_from(index)?),
                string,
                bytes
            ]
        );
    }
    Ok(())
}

#[test]
fn valid_nullable_scalars_bytes_and_large_strings_keep_projection_semantics() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let arrow = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("bytes", DataType::Binary, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("number", DataType::Float64, true),
    ]));
    let large = "x".repeat(128 * 1024);
    let batch = RecordBatch::try_new(
        Arc::clone(&arrow),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some(large.as_str()), None])),
            Arc::new(BinaryArray::from(vec![Some(&b"\x00\xff"[..]), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
        ],
    )?;
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, true),
            ColumnDef::new("text", ColumnType::Str, true),
            ColumnDef::new("bytes", ColumnType::Bytes, true),
            ColumnDef::new("flag", ColumnType::Bool, true),
            ColumnDef::new("number", ColumnType::Float, true),
        ],
        0,
    );
    for codec in [
        Compression::UNCOMPRESSED,
        Compression::SNAPPY,
        Compression::GZIP(GzipLevel::default()),
        Compression::BROTLI(BrotliLevel::default()),
        Compression::LZ4,
        Compression::LZ4_RAW,
        Compression::ZSTD(ZstdLevel::default()),
    ] {
        let file = File::create(temporary.path().join("rows.parquet"))?;
        let properties = WriterProperties::builder().set_compression(codec).build();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&arrow), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        let source = ForeignSource::Parquet {
            uri: ObjectStoreUri::from_local_dir(temporary.path()),
            path: "rows.parquet".to_owned(),
        };
        let request = ForeignScanRequest::new(Some(vec![2, 0, 1, 4, 3, 2]), vec![], None);
        let result =
            super::super::scan_local(&schema, &source, &request, 1, &budget(128 * 1024 * 1024))?;
        assert_eq!(
            result.rows[0],
            vec![
                Value::Bytes(vec![0, 255]),
                Value::Int(1),
                Value::Str(large.clone()),
                Value::Float(1.5),
                Value::Bool(true),
                Value::Bytes(vec![0, 255])
            ],
            "{codec:?}"
        );
        assert_eq!(result.rows[1], vec![Value::Null; 6], "{codec:?}");
        let native = crate::federation::guarded_parquet_bytes_to_batches(
            std::fs::read(temporary.path().join("rows.parquet"))?.into(),
            &schema,
            Some(&[2, 0, 1, 4, 3, 2]),
            Some(1),
            budget(128 * 1024 * 1024).ok_or("missing test budget")?,
        )?;
        assert_eq!(
            crate::query::batches_to_rows(&native)?,
            vec![result.rows[0].clone()]
        );
    }
    Ok(())
}

#[test]
fn native_guarded_decoder_preserves_limit_zero_and_typed_memory_failure() -> TestResult {
    let schema = TableSchema::new(vec![ColumnDef::new("id", ColumnType::Int, false)], 0);
    let empty = crate::federation::guarded_parquet_bytes_to_batches(
        bytes::Bytes::new(),
        &schema,
        Some(&[0, 0]),
        Some(0),
        budget(0).ok_or("test budget")?,
    )?;
    assert_eq!(empty.len(), 0);
    assert!(
        crate::federation::guarded_parquet_bytes_to_batches(
            bytes::Bytes::new(),
            &schema,
            Some(&[1]),
            Some(0),
            budget(0).ok_or("test budget")?,
        )
        .is_err()
    );
    let mut payload = b"PAR1".to_vec();
    payload.extend_from_slice(&[0; 16]);
    payload.extend_from_slice(&16u32.to_le_bytes());
    payload.extend_from_slice(b"PAR1");
    let error = crate::federation::guarded_parquet_bytes_to_batches(
        payload.into(),
        &schema,
        None,
        None,
        budget(100).ok_or("test budget")?,
    )
    .err()
    .ok_or("accepted impossible metadata budget")?;
    assert!(matches!(error, QueryError::ResourceLimit(_)), "{error}");
    Ok(())
}

#[test]
fn immutable_ranges_prevent_substituting_a_page_after_preflight() -> TestResult {
    let input = super::super::GuardedFile {
        length: 8,
        regions: Arc::new(vec![(4, bytes::Bytes::from_static(b"data"))]),
        guard: None,
        read_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };
    assert_eq!(input.get_bytes(4, 4)?.as_ref(), b"data");
    assert!(input.get_bytes(3, 5).is_err());
    assert!(input.get_bytes(4, usize::MAX).is_err());
    assert!(input.get_read(0).is_err());
    Ok(())
}
