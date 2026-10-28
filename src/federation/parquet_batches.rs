//! Native-columnar entry point sharing foreign Parquet's preallocation checks.
use super::{
    Arc, ArrowReaderMetadata, ArrowReaderOptions, AtomicU64, FederationError, ForeignScanGuard,
    Guard, GuardedFile, ParquetRecordBatchReaderBuilder, ParquetStatisticsPolicy, ProjectionMask,
    QueryError, TableSchema, Value, check, checked_size, decode_error, parquet_decode_error,
    parquet_preflight, reserve, validate_parquet_scalar_types,
};
use datafusion::arrow::record_batch::RecordBatch;

/// Decodes already-budgeted immutable segment bytes. Projection preserves its
/// requested order and duplicate columns. Retained Arrow output is charged to
/// the statement before temporary decoder reservations are released.
#[allow(clippy::needless_pass_by_value)] // Owned bytes retain the storage segment lease throughout decoding.
pub(crate) fn guarded_parquet_bytes_to_batches(
    payload: bytes::Bytes,
    schema: &TableSchema,
    projection: Option<&[usize]>,
    limit: Option<usize>,
    guard: Arc<dyn ForeignScanGuard>,
) -> Result<Vec<RecordBatch>, QueryError> {
    decode(&payload, schema, projection, limit, &Some(guard)).map_err(|error| match error {
        FederationError::Query(error) => error,
        FederationError::Unsupported(message) => QueryError::Unsupported(message),
        FederationError::InvalidSource(message) => QueryError::InvalidValue(message),
        error => QueryError::Storage(crate::storage::StorageError::Backend(error.to_string())),
    })
}

#[allow(clippy::too_many_lines)] // Decoder reservations must surround construction and retained output.
fn decode(
    payload: &bytes::Bytes,
    schema: &TableSchema,
    projection: Option<&[usize]>,
    limit: Option<usize>,
    guard: &Guard,
) -> Result<Vec<RecordBatch>, FederationError> {
    check(guard)?;
    if projection.is_some_and(|projection| {
        projection
            .iter()
            .any(|index| *index >= schema.columns.len())
    }) {
        return Err(decode_error("Parquet projection column is out of range"));
    }
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    let plan_width = schema
        .columns
        .len()
        .checked_add(projection.map_or(0, <[usize]>::len))
        .ok_or_else(|| QueryError::ResourceLimit("Parquet projection size overflow".to_owned()))?;
    let _plan = reserve(
        guard,
        checked_size(plan_width, 4 * std::mem::size_of::<usize>(), 256)?,
    )?;
    let mut needed =
        projection.map_or_else(|| (0..schema.columns.len()).collect(), <[usize]>::to_vec);
    if needed.iter().any(|index| *index >= schema.columns.len()) {
        return Err(decode_error("Parquet projection column is out of range"));
    }
    needed.sort_unstable();
    needed.dedup();
    let remap = projection
        .map(|projection| {
            projection
                .iter()
                .map(|index| {
                    needed
                        .binary_search(index)
                        .map_err(|_| decode_error("Parquet projection is missing a column"))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    if payload.len() < 12 || &payload[..4] != b"PAR1" || &payload[payload.len() - 4..] != b"PAR1" {
        return Err(decode_error("invalid Parquet segment framing"));
    }
    let footer_word: [u8; 4] = payload[payload.len() - 8..payload.len() - 4]
        .try_into()
        .map_err(decode_error)?;
    let footer_size = u32::from_le_bytes(footer_word) as usize;
    let footer_start = payload
        .len()
        .checked_sub(8)
        .and_then(|end| end.checked_sub(footer_size))
        .filter(|start| *start >= 4)
        .ok_or_else(|| decode_error("invalid Parquet segment footer length"))?;
    let _metadata = reserve(guard, checked_size(footer_size, 32, 16384)?)?;
    let metadata_size = parquet_preflight::metadata(
        &payload[footer_start..payload.len() - 8],
        schema.columns.len(),
        guard,
    )?;
    let _metadata_allocations = reserve(guard, metadata_size)?;
    let input = GuardedFile {
        length: payload.len() as u64,
        regions: Arc::new(vec![(footer_start as u64, payload.slice(footer_start..))]),
        guard: guard.clone(),
        read_bytes: Arc::new(AtomicU64::new(0)),
    };
    let metadata = ArrowReaderMetadata::load(
        &input,
        ArrowReaderOptions::new()
            .with_skip_arrow_metadata(true)
            .with_column_stats_policy(ParquetStatisticsPolicy::SkipAll)
            .with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll)
            .with_size_stats_policy(ParquetStatisticsPolicy::SkipAll),
    )
    .map_err(|error| parquet_decode_error(guard, error))?;
    if metadata.schema().fields().len() != schema.columns.len() {
        return Err(decode_error(
            "Parquet segment column count differs from its schema",
        ));
    }
    validate_parquet_scalar_types(&metadata, schema, &needed)?;
    let mut output = Vec::new();
    let mut rows = 0usize;
    for (index, group) in metadata.metadata().row_groups().iter().enumerate() {
        check(guard)?;
        if group.num_rows() < 0 {
            return Err(decode_error("negative Parquet row group count"));
        }
        let mut regions = input.regions.as_ref().clone();
        let mut decoded = 0usize;
        let mut allocations = 0usize;
        let batch_rows = limit
            .map_or(256, |limit| limit.saturating_sub(rows).min(256))
            .max(1);
        for &column in &needed {
            let column = group
                .columns()
                .get(column)
                .ok_or_else(|| decode_error("missing Parquet segment column"))?;
            let start = usize::try_from(
                column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset()),
            )
            .map_err(decode_error)?;
            let size = usize::try_from(column.compressed_size()).map_err(decode_error)?;
            let end = start
                .checked_add(size)
                .filter(|end| start >= 4 && *end <= footer_start)
                .ok_or_else(|| decode_error("Parquet segment column exceeds its data region"))?;
            let bytes = payload.slice(start..end);
            let budget =
                parquet_preflight::column(&bytes, column, group.num_rows(), batch_rows, guard)?;
            decoded = decoded
                .checked_add(budget.decoded)
                .ok_or_else(|| decode_error("Parquet segment decoded size overflow"))?;
            allocations = allocations
                .checked_add(budget.allocations)
                .ok_or_else(|| decode_error("Parquet segment allocation overflow"))?;
            regions.push((start as u64, bytes));
        }
        let overhead = checked_size(
            schema.columns.len(),
            batch_rows * 4 * std::mem::size_of::<Value>(),
            32768,
        )?;
        let _working = reserve(
            guard,
            checked_size(
                decoded,
                8,
                allocations
                    .checked_add(overhead)
                    .ok_or_else(|| decode_error("Parquet segment working size overflow"))?,
            )?,
        )?;
        let group_input = GuardedFile {
            regions: Arc::new(regions),
            ..input.clone()
        };
        let mask = ProjectionMask::roots(
            metadata.metadata().file_metadata().schema_descr(),
            needed.iter().copied(),
        );
        let mut builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(group_input, metadata.clone())
                .with_row_groups(vec![index])
                .with_projection(mask)
                .with_batch_size(batch_rows);
        if let Some(limit) = limit {
            builder = builder.with_limit(limit.saturating_sub(rows));
        }
        let reader = builder
            .build()
            .map_err(|error| parquet_decode_error(guard, error))?;
        for batch in reader {
            check(guard)?;
            let batch = batch.map_err(|error| parquet_decode_error(guard, error))?;
            let batch = match &remap {
                Some(remap) => batch.project(remap).map_err(decode_error)?,
                None => batch,
            };
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or_else(|| decode_error("Parquet segment row count overflow"))?;
            if let Some(guard) = guard {
                guard.charge(checked_size(
                    batch.get_array_memory_size(),
                    4,
                    4 * std::mem::size_of::<RecordBatch>(),
                )?)?;
            }
            output.push(batch);
            if limit.is_some_and(|limit| rows >= limit) {
                return Ok(output);
            }
        }
    }
    Ok(output)
}
