//! Bounded foreign scans. Source bytes are streamed; only projected output is
//! retained. SQL callers supply the same pool/deadline used by `DataFusion`.
use super::{
    FederationError, ForeignScanGuard, ForeignScanRequest, ForeignScanReservation,
    ForeignScanResult, ForeignSource, ForeignTableOptions, ForeignTableSpec, ForeignTableStats,
    csv_value_to_value, foreign_filter_matches, postgres_row_to_values, remote_plan_with_cap,
    resolve_secret, validate_remote_identifier_path, validate_scan_request,
};
use crate::{
    cloud::LocalObjectStore,
    migration::{json_object_to_row, parse_csv_line},
    model::Value,
    query::{ColumnType, QueryError, Row, TableSchema, batches_to_rows, validate_row},
};
use datafusion::arrow::datatypes::DataType;
use futures::TryStreamExt;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    errors::ParquetError,
    file::{
        metadata::ParquetStatisticsPolicy,
        reader::{ChunkReader, Length},
    },
};
use std::{
    fs::File,
    future::Future,
    io::{self, BufRead, BufReader, Cursor, Read, Seek, SeekFrom},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

type Guard = Option<Arc<dyn ForeignScanGuard>>;
type Reservation = Option<Box<dyn ForeignScanReservation>>;
type LineBuffer = (Vec<u8>, Vec<Box<dyn ForeignScanReservation>>);

#[path = "parquet_batches.rs"]
mod parquet_batches;
#[path = "parquet_preflight.rs"]
mod parquet_preflight;
pub(crate) use parquet_batches::guarded_parquet_bytes_to_batches;
#[path = "remote_transport.rs"]
mod remote_transport;

/// Scans a source under a caller-provided shared statement budget. Blocking file
/// decoding runs off the async executor and observes cancellation between reads.
/// # Errors
/// Fails on invalid input, I/O/decoding errors, cancellation or budget exhaustion.
pub async fn scan_foreign_table_guarded(
    spec: &ForeignTableSpec,
    request: ForeignScanRequest,
    guard: Guard,
) -> Result<ForeignScanResult, FederationError> {
    validate_scan_request(&spec.schema, &request)?;
    check(&guard)?;
    match &spec.source {
        ForeignSource::Postgres { .. } | ForeignSource::MultidbPgWire { .. } => {
            scan_remote(&spec.schema, &spec.source, &spec.options, &request, guard).await
        }
        _ => {
            let spec = spec.clone();
            tokio::task::spawn_blocking(move || {
                scan_local(
                    &spec.schema,
                    &spec.source,
                    &request,
                    spec.options.batch_size,
                    &guard,
                )
            })
            .await
            .map_err(|error| {
                FederationError::InvalidSource(format!("foreign scan worker failed: {error}"))
            })?
        }
    }
}

fn check(guard: &Guard) -> Result<(), FederationError> {
    if let Some(guard) = guard {
        guard.check()?;
    }
    Ok(())
}
fn reserve(guard: &Guard, bytes: usize) -> Result<Reservation, FederationError> {
    check(guard)?;
    guard
        .as_ref()
        .map(|guard| guard.reserve(bytes))
        .transpose()
        .map_err(Into::into)
}
fn empty(request: &ForeignScanRequest) -> ForeignScanResult {
    ForeignScanResult {
        rows: Vec::new(),
        stats: ForeignTableStats {
            row_count: Some(0),
            pushed_filters: request.filters.len(),
            ..ForeignTableStats::default()
        },
    }
}
fn io_error(error: io::Error) -> FederationError {
    FederationError::Cloud(error.into())
}
fn decode_error(error: impl std::fmt::Display) -> FederationError {
    FederationError::InvalidSource(error.to_string())
}
fn checked_size(
    bytes: usize,
    multiplier: usize,
    overhead: usize,
) -> Result<usize, FederationError> {
    bytes
        .checked_mul(multiplier)
        .and_then(|n| n.checked_add(overhead))
        .ok_or_else(|| QueryError::ResourceLimit("foreign decoder size overflow".to_owned()).into())
}

pub(super) fn scan_local(
    schema: &TableSchema,
    source: &ForeignSource,
    request: &ForeignScanRequest,
    batch_size: usize,
    guard: &Guard,
) -> Result<ForeignScanResult, FederationError> {
    validate_scan_request(schema, request)?;
    check(guard)?;
    let (ForeignSource::Csv { uri, path, .. }
    | ForeignSource::JsonLines { uri, path }
    | ForeignSource::Parquet { uri, path }) = source
    else {
        return Err(FederationError::Unsupported(
            "remote sources must be scanned asynchronously".to_owned(),
        ));
    };
    if request.limit == Some(0) {
        return Ok(empty(request));
    }
    // This factory currently supports local files only. Do not substitute a
    // legacy CloudObjectStore::get fallback that allocates the entire object.
    let file = LocalObjectStore::new(uri.clone()).open_file(path)?;
    let mut out = empty(request);
    if matches!(source, ForeignSource::Parquet { .. }) {
        scan_parquet(file, schema, request, batch_size, guard, &mut out)?;
    } else {
        scan_lines(file, schema, source, request, guard, &mut out)?;
    }
    check(guard)?;
    out.stats.row_count = Some(out.rows.len() as u64);
    crate::observability::record_cloud_object("get", "ok", out.stats.scanned_bytes);
    Ok(out)
}

fn scan_lines(
    file: File,
    schema: &TableSchema,
    source: &ForeignSource,
    request: &ForeignScanRequest,
    guard: &Guard,
    out: &mut ForeignScanResult,
) -> Result<(), FederationError> {
    let _buffer = reserve(guard, 8192)?;
    let mut reader = BufReader::with_capacity(8192, file);
    let mut first = true;
    loop {
        check(guard)?;
        // Account line storage and parser expansion before growing any buffer.
        // Reservations are released at the end of this iteration, rather than
        // charging cumulative bytes of a file that is not retained.
        let (line, _line_storage) = read_line(&mut reader, guard)?;
        if line.is_empty() {
            break;
        }
        out.stats.scanned_bytes = out.stats.scanned_bytes.saturating_add(line.len() as u64);
        let skip_header = first
            && matches!(
                source,
                ForeignSource::Csv {
                    has_header: true,
                    ..
                }
            );
        first = false;
        let text = std::str::from_utf8(&line)
            .map_err(decode_error)?
            .trim_end_matches(['\r', '\n']);
        if skip_header || text.trim().is_empty() {
            continue;
        }
        let _parser = reserve(
            guard,
            parser_working_bytes(text, source, schema.columns.len())?,
        )?;
        let row = match source {
            ForeignSource::Csv { .. } => {
                let values = parse_csv_line(text);
                if values.len() != schema.columns.len() {
                    return Err(decode_error(format!(
                        "CSV row has {} fields, expected {}",
                        values.len(),
                        schema.columns.len()
                    )));
                }
                values
                    .iter()
                    .zip(&schema.columns)
                    .map(|(value, column)| csv_value_to_value(value, &column.ty))
                    .collect::<Result<Row, _>>()?
            }
            ForeignSource::JsonLines { .. } => {
                let value =
                    serde_json::from_str::<serde_json::Value>(text).map_err(decode_error)?;
                json_object_to_row(Some(schema), &value).map_err(decode_error)?
            }
            _ => unreachable!(),
        };
        out.stats.scanned_rows = out.stats.scanned_rows.saturating_add(1);
        if retain_row(row, request, guard, &mut out.rows)? {
            break;
        }
    }
    Ok(())
}

fn read_line(reader: &mut impl BufRead, guard: &Guard) -> Result<LineBuffer, FederationError> {
    let mut line = Vec::new();
    let mut tokens = Vec::new();
    loop {
        check(guard)?;
        let bytes = reader.fill_buf().map_err(io_error)?;
        if bytes.is_empty() {
            return Ok((line, tokens));
        }
        let size = bytes
            .iter()
            .position(|b| *b == b'\n')
            .map_or(bytes.len(), |n| n + 1);
        let needed = line
            .len()
            .checked_add(size)
            .ok_or_else(|| decode_error("foreign line size overflow"))?;
        if needed > line.capacity() {
            let target = needed.max(line.capacity().saturating_mul(2)).max(256);
            // The line buffer is reserved before growth. Parser allocations
            // are reserved separately from the actual shape below, allowing a
            // large string without treating each byte as an object/array node.
            let expansion = checked_size(target - line.capacity(), 2, 256)?;
            if let Some(token) = reserve(guard, expansion)? {
                tokens.push(token);
            }
            line.try_reserve_exact(target - line.len())
                .map_err(decode_error)?;
        }
        line.extend_from_slice(&bytes[..size]);
        let done = bytes[size - 1] == b'\n';
        reader.consume(size);
        if done {
            return Ok((line, tokens));
        }
    }
}

fn parser_working_bytes(
    text: &str,
    source: &ForeignSource,
    columns: usize,
) -> Result<usize, FederationError> {
    let mut quoted = false;
    let mut escaped = false;
    let mut structural = 0usize;
    let json = matches!(source, ForeignSource::JsonLines { .. });
    for byte in text.bytes() {
        if json && escaped {
            escaped = false;
            continue;
        }
        if quoted && json && byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' {
            quoted = !quoted;
            continue;
        }
        if !quoted && (byte == b',' || (json && matches!(byte, b'{' | b'[' | b':'))) {
            structural = structural
                .checked_add(1)
                .ok_or_else(|| decode_error("foreign row structure overflow"))?;
        }
    }
    // serde_json's tree and CSV's cell vector need space per structural node,
    // whereas long string payloads need space proportional to their byte size.
    let structure = checked_size(
        structural,
        256,
        columns
            .saturating_mul(4 * std::mem::size_of::<Value>())
            .saturating_add(1024),
    )?;
    checked_size(text.len(), 4, structure)
}

fn retain_row(
    row: Row,
    request: &ForeignScanRequest,
    guard: &Guard,
    out: &mut Vec<Row>,
) -> Result<bool, FederationError> {
    check(guard)?;
    if !request
        .filters
        .iter()
        .all(|filter| foreign_filter_matches(&row, filter))
    {
        return Ok(false);
    }
    let mut bytes = 4 * std::mem::size_of::<Row>();
    let width = request.projection.as_ref().map_or(row.len(), Vec::len);
    for index in 0..width {
        let index = request
            .projection
            .as_ref()
            .map_or(index, |projection| projection[index]);
        let value = row
            .get(index)
            .ok_or_else(|| decode_error("foreign row has a missing column"))?;
        let heap = match value {
            Value::Str(v) => v.capacity(),
            Value::Bytes(v) => v.capacity(),
            Value::Null | Value::Int(_) | Value::Float(_) | Value::Bool(_) => 0,
            _ => return Err(decode_error("foreign schema produced a non-scalar field")),
        };
        bytes = bytes
            .checked_add(checked_size(heap, 4, 4 * std::mem::size_of::<Value>())?)
            .ok_or_else(|| decode_error("foreign retained row size overflow"))?;
    }
    if let Some(guard) = guard {
        guard.charge(bytes)?;
    }
    let row = if let Some(projection) = &request.projection {
        projection.iter().map(|index| row[*index].clone()).collect()
    } else {
        row
    };
    out.push(row);
    Ok(request.limit.is_some_and(|limit| out.len() >= limit))
}

#[derive(Clone)]
struct GuardedFile {
    length: u64,
    regions: Arc<Vec<(u64, bytes::Bytes)>>,
    guard: Guard,
    read_bytes: Arc<AtomicU64>,
}
struct GuardedRead {
    reader: Cursor<bytes::Bytes>,
    guard: Guard,
    read_bytes: Arc<AtomicU64>,
}
impl Read for GuardedRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        check(&self.guard).map_err(|error| io::Error::other(error.to_string()))?;
        let bytes = self.reader.read(buf)?;
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}
impl Length for GuardedFile {
    fn len(&self) -> u64 {
        self.length
    }
}
impl GuardedFile {
    fn region(&self, start: u64, length: Option<usize>) -> Result<bytes::Bytes, ParquetError> {
        for (offset, bytes) in self.regions.iter() {
            let Some(relative) = start
                .checked_sub(*offset)
                .and_then(|v| usize::try_from(v).ok())
            else {
                continue;
            };
            let end = match length {
                Some(length) => relative.checked_add(length),
                None => Some(bytes.len()),
            };
            if let Some(end) = end.filter(|end| relative <= *end && *end <= bytes.len()) {
                return Ok(bytes.slice(relative..end));
            }
        }
        Err(ParquetError::General(
            "Parquet read is outside preflighted immutable ranges".to_owned(),
        ))
    }
}
impl ChunkReader for GuardedFile {
    type T = GuardedRead;
    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        check(&self.guard).map_err(|error| ParquetError::General(error.to_string()))?;
        Ok(GuardedRead {
            reader: Cursor::new(self.region(start, None)?),
            guard: self.guard.clone(),
            read_bytes: Arc::clone(&self.read_bytes),
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> Result<bytes::Bytes, ParquetError> {
        check(&self.guard).map_err(|error| ParquetError::General(error.to_string()))?;
        let bytes = self.region(start, Some(length))?;
        self.read_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

fn read_parquet_region(
    file: &mut File,
    start: u64,
    length: usize,
    guard: &Guard,
) -> Result<bytes::Bytes, FederationError> {
    check(guard)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|error| QueryError::ResourceLimit(error.to_string()))?;
    bytes.resize(length, 0);
    file.seek(SeekFrom::Start(start)).map_err(io_error)?;
    file.read_exact(&mut bytes).map_err(io_error)?;
    check(guard)?;
    Ok(bytes.into())
}

fn parquet_decode_error(guard: &Guard, error: impl std::fmt::Display) -> FederationError {
    // A guard can expire inside ChunkReader; do not replace its typed failure
    // with the decoder's stringified I/O/Parquet error.
    check(guard).err().unwrap_or_else(|| decode_error(error))
}

fn validate_parquet_scalar_types(
    metadata: &ArrowReaderMetadata,
    schema: &TableSchema,
    needed: &[usize],
) -> Result<(), FederationError> {
    for &index in needed {
        let supported = matches!(
            (
                metadata.schema().field(index).data_type(),
                &schema.columns[index].ty
            ),
            (DataType::Int64 | DataType::UInt64, ColumnType::Int)
                | (DataType::Float64, ColumnType::Float)
                | (DataType::Utf8, ColumnType::Str)
                | (DataType::Boolean, ColumnType::Bool)
                | (DataType::Binary, ColumnType::Bytes)
                | (DataType::Null, _)
        );
        if !supported {
            return Err(decode_error(
                "Parquet scalar type does not match the foreign schema",
            ));
        }
    }
    Ok(())
}

// Keep the immutable input and its preflight reservations in one lexical scope.
#[allow(clippy::too_many_lines)]
fn scan_parquet(
    mut file: File,
    schema: &TableSchema,
    request: &ForeignScanRequest,
    batch_size: usize,
    guard: &Guard,
    out: &mut ForeignScanResult,
) -> Result<(), FederationError> {
    let length = file.metadata().map_err(io_error)?.len();
    if length == 0 {
        return Ok(());
    }
    if length < 8 {
        return Err(decode_error("truncated Parquet footer"));
    }
    check(guard)?;
    file.seek(SeekFrom::End(-8)).map_err(io_error)?;
    let mut footer = [0; 8];
    file.read_exact(&mut footer).map_err(io_error)?;
    if &footer[4..] != b"PAR1" {
        return Err(decode_error("invalid Parquet footer"));
    }
    let footer_size = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]) as usize;
    if footer_size as u64 > length - 8 {
        return Err(decode_error("invalid Parquet metadata length"));
    }
    let _metadata = reserve(guard, checked_size(footer_size, 32, 16384)?)?;
    let footer_start = length - 8 - footer_size as u64;
    let footer_bytes = read_parquet_region(&mut file, footer_start, footer_size + 8, guard)?;
    // Pin the footer used for both preflight and Arrow metadata parsing. A
    // concurrently replaced source cannot substitute unvalidated lengths.
    if footer_bytes[footer_size..] != footer {
        return Err(decode_error("Parquet footer changed during the scan"));
    }
    let allocation =
        parquet_preflight::metadata(&footer_bytes[..footer_size], schema.columns.len(), guard)?;
    let _metadata_allocations = reserve(guard, allocation)?;
    let input = GuardedFile {
        length,
        regions: Arc::new(vec![(footer_start, footer_bytes)]),
        guard: guard.clone(),
        read_bytes: Arc::new(AtomicU64::new(8)),
    };
    // The API supports scalar TableSchema types; do not run a second, embedded
    // Arrow IPC schema decoder controlled by ARROW:schema key/value metadata.
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
            "Parquet columns do not match the foreign schema",
        ));
    }
    let mut needed = request
        .projection
        .clone()
        .unwrap_or_else(|| (0..schema.columns.len()).collect());
    needed.extend(request.filters.iter().map(|filter| filter.column));
    needed.sort_unstable();
    needed.dedup();
    validate_parquet_scalar_types(&metadata, schema, &needed)?;
    let projected = TableSchema::new(
        needed.iter().map(|i| schema.columns[*i].clone()).collect(),
        0,
    );
    let batch_size = batch_size.clamp(1, 256);
    for (group_index, group) in metadata.metadata().row_groups().iter().enumerate() {
        check(guard)?;
        if group.num_rows() < 0 {
            return Err(decode_error("negative Parquet row group count"));
        }
        let mut encoded = 0usize;
        let mut decoded = 0usize;
        let mut allocations = 0usize;
        let mut ranges = Vec::new();
        // Foreign schemas are scalar; account only selected leaf columns.
        for &index in &needed {
            let column = group
                .columns()
                .get(index)
                .ok_or_else(|| decode_error("Parquet column is missing"))?;
            let start = u64::try_from(
                column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset()),
            )
            .map_err(decode_error)?;
            let size = usize::try_from(column.compressed_size()).map_err(decode_error)?;
            if start < 4
                || start
                    .checked_add(size as u64)
                    .is_none_or(|end| end > footer_start)
            {
                return Err(decode_error("Parquet column is outside the data section"));
            }
            encoded = encoded
                .checked_add(size)
                .ok_or_else(|| decode_error("Parquet compressed size overflow"))?;
            ranges.push((index, start, size));
        }
        let _encoded = reserve(guard, encoded)?;
        let mut regions = input.regions.as_ref().clone();
        for (index, start, size) in ranges {
            let bytes = read_parquet_region(&mut file, start, size, guard)?;
            let budget = parquet_preflight::column(
                &bytes,
                &group.columns()[index],
                group.num_rows(),
                batch_size,
                guard,
            )?;
            decoded = decoded
                .checked_add(budget.decoded)
                .ok_or_else(|| decode_error("Parquet decoded size overflow"))?;
            allocations = allocations
                .checked_add(budget.allocations)
                .ok_or_else(|| decode_error("Parquet decoder allocation overflow"))?;
            regions.push((start, bytes));
        }
        let overhead = checked_size(
            schema.columns.len(),
            batch_size.saturating_mul(4 * std::mem::size_of::<Value>()),
            32768,
        )?;
        let working = checked_size(
            decoded,
            8,
            allocations
                .checked_add(overhead)
                .ok_or_else(|| decode_error("Parquet decoder allocation overflow"))?,
        )?;
        let _working = reserve(guard, working)?;
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
                .with_row_groups(vec![group_index])
                .with_projection(mask)
                .with_batch_size(batch_size);
        if request.filters.is_empty()
            && let Some(limit) = request.limit
        {
            builder = builder.with_limit(limit.saturating_sub(out.rows.len()));
        }
        let reader = builder
            .build()
            .map_err(|error| parquet_decode_error(guard, error))?;
        for batch in reader {
            check(guard)?;
            let batch = batch.map_err(|error| parquet_decode_error(guard, error))?;
            for row in batches_to_rows(&[batch])? {
                validate_row(Some(&projected), &row)?;
                let mut original = vec![Value::Null; schema.columns.len()];
                for (index, value) in needed.iter().zip(row) {
                    original[*index] = value;
                }
                out.stats.scanned_rows = out.stats.scanned_rows.saturating_add(1);
                if retain_row(original, request, guard, &mut out.rows)? {
                    out.stats.scanned_bytes = input.read_bytes.load(Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
    }
    out.stats.scanned_bytes = input.read_bytes.load(Ordering::Relaxed);
    Ok(())
}

async fn await_guarded<F: Future>(future: F, guard: &Guard) -> Result<F::Output, FederationError> {
    if guard.is_none() {
        return Ok(future.await);
    }
    tokio::pin!(future);
    let mut ticks = tokio::time::interval(Duration::from_millis(10));
    loop {
        check(guard)?;
        tokio::select! {
            value = &mut future => { check(guard)?; return Ok(value); },
            _ = ticks.tick() => {},
        }
    }
}

// The connection, row stream and temporary reservations share this scan's lifetime.
#[allow(clippy::too_many_lines)]
pub(super) async fn scan_remote(
    schema: &TableSchema,
    source: &ForeignSource,
    options: &ForeignTableOptions,
    request: &ForeignScanRequest,
    guard: Guard,
) -> Result<ForeignScanResult, FederationError> {
    validate_scan_request(schema, request)?;
    check(&guard)?;
    let (ForeignSource::Postgres { connection, table }
    | ForeignSource::MultidbPgWire { connection, table }) = source
    else {
        return Err(FederationError::Unsupported(
            "local object sources are not remote postgres".to_owned(),
        ));
    };
    validate_remote_identifier_path(table)?;
    if request.limit == Some(0) {
        return Ok(empty(request));
    }
    let plan_width = schema
        .columns
        .len()
        .checked_add(request.projection.as_ref().map_or(0, Vec::len))
        .and_then(|n| n.checked_add(request.filters.len()))
        .ok_or_else(|| QueryError::ResourceLimit("foreign plan width overflow".to_owned()))?;
    let plan_text = schema
        .columns
        .iter()
        .try_fold(table.len(), |bytes, column| {
            bytes.checked_add(column.name.len())
        })
        .and_then(|bytes| {
            request.filters.iter().try_fold(bytes, |bytes, filter| {
                bytes.checked_add(match &filter.value {
                    Value::Str(value) => value.len(),
                    Value::Bytes(value) => value.len(),
                    _ => 0,
                })
            })
        })
        .ok_or_else(|| QueryError::ResourceLimit("foreign plan text size overflow".to_owned()))?;
    let _plan_memory = reserve(
        &guard,
        checked_size(plan_width, 256, checked_size(plan_text, 8, 4096)?)?,
    )?;
    let plan = remote_plan_with_cap(schema, table, options, request, guard.is_some())?;
    let projected = TableSchema::new(
        plan.needed
            .iter()
            .map(|index| schema.columns[*index].clone())
            .collect(),
        0,
    );
    let mut effective_request = request.clone();
    effective_request.limit = plan.limit;
    let connection = resolve_secret(connection)?;
    let remote = remote_transport::connect(&connection, guard.clone()).await?;
    let client = remote.client();
    let statement = await_guarded(client.prepare_typed(&plan.sql, &plan.types), &guard)
        .await?
        .map_err(|error| remote.map_error(error))?;
    let stream = await_guarded(
        client.query_raw(&statement, plan.params.iter().copied()),
        &guard,
    )
    .await?
    .map_err(|error| remote.map_error(error))?;
    tokio::pin!(stream);
    let mut out = empty(request);
    while let Some(row) = await_guarded(stream.try_next(), &guard)
        .await?
        .map_err(|error| remote.map_error(error))?
    {
        let mut encoded = 0usize;
        for index in 0..row.len() {
            let raw: Option<RawLength> = row
                .try_get(index)
                .map_err(|error| remote.map_error(error))?;
            encoded = encoded
                .checked_add(raw.map_or(0, |v| v.0))
                .ok_or_else(|| decode_error("remote row size overflow"))?;
        }
        let _working = reserve(
            &guard,
            checked_size(
                encoded,
                8,
                schema
                    .columns
                    .len()
                    .saturating_mul(4 * std::mem::size_of::<Value>())
                    .saturating_add(4096),
            )?,
        )?;
        out.stats.scanned_bytes = out.stats.scanned_bytes.saturating_add(encoded as u64);
        out.stats.scanned_rows = out.stats.scanned_rows.saturating_add(1);
        let values = postgres_row_to_values(&projected, &row)?;
        let mut original = vec![Value::Null; schema.columns.len()];
        for (index, value) in plan.needed.iter().zip(values) {
            original[*index] = value;
        }
        let limit_reached = retain_row(original, &effective_request, &guard, &mut out.rows)?;
        if plan.overflow_limit.is_some_and(|cap| out.rows.len() > cap) {
            return Err(QueryError::ResourceLimit("remote foreign scan exceeds remote_scan_limit; increase the source cap or use a supported pushed LIMIT".to_owned()).into());
        }
        if limit_reached {
            break;
        }
    }
    out.stats.row_count = Some(out.rows.len() as u64);
    check(&guard)?;
    Ok(out)
}

struct RawLength(usize);
impl<'a> tokio_postgres::types::FromSql<'a> for RawLength {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw.len()))
    }
    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
