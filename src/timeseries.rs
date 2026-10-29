use std::{collections::BTreeMap, time::UNIX_EPOCH};

use crate::{
    keyenc,
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition,
        propose_system,
    },
    storage::{Bytes, StorageError},
    txn::{Clock, SystemClock},
};

pub const TIME_SERIES_CHUNKS_TABLE: &str = "time_series_chunks";
pub const TIME_SERIES_LATEST_TABLE: &str = "time_series_latest";
pub const TIME_SERIES_META_TABLE: &str = "__time_series_meta";
/// Default bound on how far ahead of the server clock an inserted point may be.
pub const DEFAULT_MAX_FUTURE_SKEW_MILLIS: i64 = 300_000;
const GORILLA_CHUNK_MAGIC: &[u8] = b"MDTSG1";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TimeSeriesConfig {
    pub name: String,
    pub chunk_millis: i64,
    pub retention_millis: Option<i64>,
    /// Largest distance a point's timestamp may lie ahead of the server clock. New collections
    /// default to [`DEFAULT_MAX_FUTURE_SKEW_MILLIS`]; configs persisted before this field existed
    /// keep accepting any future timestamp, so upgrades do not start rejecting their writes.
    #[serde(default = "legacy_max_future_skew_millis")]
    pub max_future_skew_millis: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TimePoint {
    pub timestamp_millis: i64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TimeChunk {
    pub series: String,
    pub bucket_start: i64,
    pub points: Vec<TimePoint>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimeSeriesStats {
    pub raw_bytes: usize,
    pub encoded_bytes: usize,
    pub compression_ratio: f64,
}

#[derive(thiserror::Error, Debug)]
pub enum TimeSeriesError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("invalid time-series config: {0}")]
    InvalidConfig(String),

    #[error("invalid point: {0}")]
    InvalidPoint(String),

    #[error("codec corruption: {0}")]
    Corruption(String),
}

/// Opaque position in a bounded sweep of chunks followed by latest-point entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionCursor {
    latest: bool,
    after_key: Bytes,
}

/// Progress of one maintenance batch. Repeat with `next_cursor` until it is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetentionReport {
    pub scanned_rows: usize,
    pub deleted_chunks: usize,
    pub deleted_latest: usize,
    pub conflicted_rows: usize,
    pub next_cursor: Option<RetentionCursor>,
}

/// Prunes expired chunks and latest-point entries without requiring a new series write.
///
/// Each call scans at most `batch_rows` entries (maximum 4096), and deletes only values
/// that still equal those scanned. A concurrent writer wins a conflict; the next complete
/// sweep retries that entry. Expiration uses server time and whole-chunk retention.
/// Maintenance callers should use the underlying replication backend, without user hooks.
/// # Errors
/// Fails for invalid batch sizes or retention configs, malformed keys, or storage errors.
pub fn sweep_retention<R: Replication + ?Sized>(
    repl: &R,
    configs: &BTreeMap<String, TimeSeriesConfig>,
    cursor: Option<RetentionCursor>,
    batch_rows: usize,
) -> Result<RetentionReport, TimeSeriesError> {
    sweep_retention_at(repl, configs, cursor, batch_rows, unix_millis(&SystemClock))
}

fn sweep_retention_at<R: Replication + ?Sized>(
    repl: &R,
    configs: &BTreeMap<String, TimeSeriesConfig>,
    cursor: Option<RetentionCursor>,
    batch_rows: usize,
    now: i64,
) -> Result<RetentionReport, TimeSeriesError> {
    if !(1..=4096).contains(&batch_rows) {
        return Err(TimeSeriesError::InvalidConfig(
            "retention batch_rows must be between 1 and 4096".to_owned(),
        ));
    }
    let cursor = cursor.unwrap_or(RetentionCursor {
        latest: false,
        after_key: Vec::new(),
    });
    let table = if cursor.latest {
        TIME_SERIES_LATEST_TABLE
    } else {
        TIME_SERIES_CHUNKS_TABLE
    };
    let mut start = cursor.after_key;
    if !start.is_empty() {
        start.push(0);
    }
    let mut rows = Vec::new();
    repl.scan_range_batches(
        table,
        &start,
        &[],
        ReadConsistency::Strong,
        batch_rows,
        &|| false,
        &mut |batch| {
            rows.extend_from_slice(batch);
            Ok(false)
        },
    )?;
    let mut report = RetentionReport {
        scanned_rows: rows.len(),
        ..RetentionReport::default()
    };
    report.next_cursor = rows
        .last()
        .map(|(key, _)| RetentionCursor {
            latest: cursor.latest,
            after_key: key.clone(),
        })
        .or_else(|| {
            (!cursor.latest).then(|| RetentionCursor {
                latest: true,
                after_key: Vec::new(),
            })
        });
    for (key, value) in rows {
        if !retention_entry_expired(configs, &key, &value, cursor.latest, now)? {
            continue;
        }
        let batch = ConditionalBatch::new(
            vec![WriteCondition::ValueEquals {
                table: table.to_owned(),
                key: key.clone(),
                expected: Some(value),
            }],
            vec![Op::Delete {
                table: table.to_owned(),
                key,
            }],
        );
        match repl.propose_conditional_batch(batch) {
            Ok(()) if cursor.latest => report.deleted_latest += 1,
            Ok(()) => report.deleted_chunks += 1,
            Err(ReplError::Conflict | ReplError::Storage(StorageError::Conflict)) => {
                report.conflicted_rows += 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(report)
}

fn retention_entry_expired(
    configs: &BTreeMap<String, TimeSeriesConfig>,
    key: &[u8],
    value: &[u8],
    latest: bool,
    now: i64,
) -> Result<bool, TimeSeriesError> {
    let mut offset = 0;
    let name = keyenc::read_len_bytes(key, &mut offset).ok_or_else(|| {
        TimeSeriesError::Corruption("invalid retention collection key".to_owned())
    })?;
    let name = std::str::from_utf8(name)
        .map_err(|error| TimeSeriesError::Corruption(error.to_string()))?;
    let Some(config) = configs.get(name) else {
        return Ok(false);
    };
    let Some(retention) = config.retention_millis else {
        return Ok(false);
    };
    validate_config(config)?;
    validate_retention(config)?;
    let cutoff = now.saturating_sub(retention);
    if latest {
        let point = decode_point(value)?;
        // Keep the latest entry until its entire chunk expires, like stored chunks.
        Ok(bucket_start(point.timestamp_millis, config.chunk_millis)
            .zip(bucket_start(cutoff, config.chunk_millis))
            .is_some_and(|(bucket, cutoff_bucket)| bucket < cutoff_bucket))
    } else {
        keyenc::read_len_bytes(key, &mut offset).ok_or_else(|| {
            TimeSeriesError::Corruption("invalid retention series key".to_owned())
        })?;
        let encoded: [u8; 8] = key[offset..]
            .try_into()
            .map_err(|_| TimeSeriesError::Corruption("invalid retention bucket key".to_owned()))?;
        let bucket = i64::from_be_bytes(encoded) ^ i64::MIN;
        Ok(bucket_start(cutoff, config.chunk_millis).is_some_and(|end| bucket < end))
    }
}

pub struct TimeSeriesCollection<'repl, R: Replication + ?Sized> {
    repl: &'repl R,
    config: TimeSeriesConfig,
}

impl TimeSeriesConfig {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            chunk_millis: 60_000,
            retention_millis: None,
            max_future_skew_millis: DEFAULT_MAX_FUTURE_SKEW_MILLIS,
        }
    }

    #[must_use]
    pub const fn with_chunk_millis(mut self, chunk_millis: i64) -> Self {
        self.chunk_millis = chunk_millis;
        self
    }

    #[must_use]
    pub const fn with_retention_millis(mut self, retention_millis: i64) -> Self {
        self.retention_millis = Some(retention_millis);
        self
    }

    #[must_use]
    pub const fn with_max_future_skew_millis(mut self, max_future_skew_millis: i64) -> Self {
        self.max_future_skew_millis = max_future_skew_millis;
        self
    }
}

const fn legacy_max_future_skew_millis() -> i64 {
    i64::MAX
}

impl<'repl, R: Replication + ?Sized> TimeSeriesCollection<'repl, R> {
    /// Opens a time-series collection handle.
    /// # Errors
    /// Fails when the config is invalid.
    pub fn new(repl: &'repl R, config: TimeSeriesConfig) -> Result<Self, TimeSeriesError> {
        validate_config(&config)?;
        Ok(Self { repl, config })
    }

    #[must_use]
    pub const fn config(&self) -> &TimeSeriesConfig {
        &self.config
    }

    /// Persists collection metadata.
    /// # Errors
    /// Fails when storage rejects metadata.
    pub fn create_metadata(&self) -> Result<(), TimeSeriesError> {
        propose_system(self.repl, Self::metadata_op(&self.config)?)?;
        Ok(())
    }

    /// Builds the system metadata write for atomic catalog DDL.
    /// # Errors
    /// Fails when the config is invalid (including a non-positive `retention_millis` or one
    /// shorter than `chunk_millis`) or when metadata cannot be serialized.
    pub fn metadata_op(config: &TimeSeriesConfig) -> Result<Op, TimeSeriesError> {
        validate_config(config)?;
        validate_retention(config)?;
        Ok(Op::Put {
            table: TIME_SERIES_META_TABLE.to_owned(),
            key: config.name.as_bytes().to_vec(),
            value: serde_json::to_vec(config)
                .map_err(|error| TimeSeriesError::Corruption(error.to_string()))?,
        })
    }

    /// Inserts or replaces one point.
    ///
    /// Retention is evaluated against the server clock, never against the point's timestamp.
    /// # Errors
    /// Fails when the point is invalid, lies more than `max_future_skew_millis` ahead of the
    /// server clock, is older than the retention window or cannot be bucketed, when a persisted
    /// `retention_millis` is not positive, or when the chunk cannot be written.
    pub fn insert_point(&self, series: &str, point: TimePoint) -> Result<(), TimeSeriesError> {
        self.insert_point_with_clock(series, point, &SystemClock)
    }

    fn insert_point_with_clock(
        &self,
        series: &str,
        point: TimePoint,
        clock: &impl Clock,
    ) -> Result<(), TimeSeriesError> {
        validate_point(point)?;
        let cutoff = check_point_time(&self.config, point.timestamp_millis, unix_millis(clock))?;
        let bucket =
            bucket_start(point.timestamp_millis, self.config.chunk_millis).ok_or_else(|| {
                TimeSeriesError::InvalidPoint(format!(
                    "timestamp {} cannot be bucketed by chunk_millis {} without overflow",
                    point.timestamp_millis, self.config.chunk_millis
                ))
            })?;
        let key = chunk_key(&self.config.name, series, bucket);
        let old_chunk_bytes =
            self.repl
                .read(TIME_SERIES_CHUNKS_TABLE, &key, ReadConsistency::Strong)?;
        let mut chunk = old_chunk_bytes
            .as_deref()
            .map(decode_chunk)
            .transpose()?
            .unwrap_or_else(|| TimeChunk {
                series: series.to_owned(),
                bucket_start: bucket,
                points: Vec::new(),
            });
        if let Some(existing) = chunk
            .points
            .iter_mut()
            .find(|existing| existing.timestamp_millis == point.timestamp_millis)
        {
            *existing = point;
        } else {
            chunk.points.push(point);
            chunk.points.sort_by_key(|point| point.timestamp_millis);
        }

        let latest_key = latest_key(&self.config.name, series);
        let old_latest_bytes = self.repl.read(
            TIME_SERIES_LATEST_TABLE,
            &latest_key,
            ReadConsistency::Strong,
        )?;
        let update_latest = match old_latest_bytes.as_deref().map(decode_point).transpose()? {
            Some(latest) => point.timestamp_millis >= latest.timestamp_millis,
            None => true,
        };

        let mut ops = vec![Op::Put {
            table: TIME_SERIES_CHUNKS_TABLE.to_owned(),
            key: key.clone(),
            value: encode_chunk(&chunk)?,
        }];
        if update_latest {
            ops.push(Op::Put {
                table: TIME_SERIES_LATEST_TABLE.to_owned(),
                key: latest_key.clone(),
                value: encode_point(point),
            });
        }
        if let Some(cutoff) = cutoff {
            append_retention_ops(&self.config, self.repl, series, cutoff, &mut ops)?;
        }
        let mut conditions = vec![WriteCondition::ValueEquals {
            table: TIME_SERIES_CHUNKS_TABLE.to_owned(),
            key,
            expected: old_chunk_bytes,
        }];
        if update_latest {
            conditions.push(WriteCondition::ValueEquals {
                table: TIME_SERIES_LATEST_TABLE.to_owned(),
                key: latest_key,
                expected: old_latest_bytes,
            });
        }
        match self
            .repl
            .propose_conditional_batch(ConditionalBatch::new(conditions, ops))
        {
            Ok(()) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Reads points in the half-open range `[start, end)`.
    /// # Errors
    /// Fails when chunks cannot be read.
    pub fn range(
        &self,
        series: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<TimePoint>, TimeSeriesError> {
        if end <= start {
            return Ok(Vec::new());
        }
        // `chunk_key(i64::MIN)` is the smallest chunk key of the series, so an unrepresentable
        // start bucket is clamped there; an unrepresentable end bucket lies below every chunk.
        let start_bucket = bucket_start(start, self.config.chunk_millis).unwrap_or(i64::MIN);
        let Some(end_bucket) = bucket_start(end.saturating_sub(1), self.config.chunk_millis) else {
            return Ok(Vec::new());
        };
        let start_key = chunk_key(&self.config.name, series, start_bucket);
        let end_key = keyenc::range_end(&chunk_key(&self.config.name, series, end_bucket));
        let rows = self.repl.range(
            TIME_SERIES_CHUNKS_TABLE,
            &start_key,
            &end_key,
            ReadConsistency::Strong,
        )?;
        let mut points = Vec::new();
        for (_, bytes) in rows {
            let chunk = decode_chunk(&bytes)?;
            points.extend(
                chunk.points.into_iter().filter(|point| {
                    point.timestamp_millis >= start && point.timestamp_millis < end
                }),
            );
        }
        points.sort_by_key(|point| point.timestamp_millis);
        Ok(points)
    }

    /// Reads the latest point for a series.
    /// # Errors
    /// Fails when storage cannot be read.
    pub fn latest(&self, series: &str) -> Result<Option<TimePoint>, TimeSeriesError> {
        let Some(bytes) = self.repl.read(
            TIME_SERIES_LATEST_TABLE,
            &latest_key(&self.config.name, series),
            ReadConsistency::Strong,
        )?
        else {
            return Ok(None);
        };
        decode_point(&bytes).map(Some)
    }

    /// Returns codec size stats for a range.
    /// # Errors
    /// Fails when chunks cannot be read.
    pub fn codec_stats(
        &self,
        series: &str,
        start: i64,
        end: i64,
    ) -> Result<TimeSeriesStats, TimeSeriesError> {
        let points = self.range(series, start, end)?;
        let raw_bytes = points.len().saturating_mul(16);
        let anchor = points.first().map_or(start, |point| point.timestamp_millis);
        let chunk = TimeChunk {
            series: series.to_owned(),
            bucket_start: bucket_start(anchor, self.config.chunk_millis).unwrap_or(i64::MIN),
            points,
        };
        let encoded_bytes = encode_chunk(&chunk)?.len();
        let raw = raw_bytes.to_string().parse::<f64>().unwrap_or(0.0);
        let encoded = encoded_bytes.to_string().parse::<f64>().unwrap_or(1.0);
        Ok(TimeSeriesStats {
            raw_bytes,
            encoded_bytes,
            compression_ratio: if encoded <= 0.0 { 0.0 } else { raw / encoded },
        })
    }
}

/// Returns the UTC bucket start for a timestamp.
/// # Errors
/// Fails when the interval is not positive or the bucket start does not fit in an `i64`.
pub fn time_bucket(interval_millis: i64, timestamp_millis: i64) -> Result<i64, TimeSeriesError> {
    if interval_millis <= 0 {
        return Err(TimeSeriesError::InvalidConfig(
            "bucket interval must be positive".to_owned(),
        ));
    }
    bucket_start(timestamp_millis, interval_millis).ok_or_else(|| {
        TimeSeriesError::InvalidPoint(format!(
            "timestamp {timestamp_millis} cannot be bucketed by interval {interval_millis} \
             without overflow"
        ))
    })
}

/// Encodes a chunk with Gorilla-style delta-of-delta timestamps and XOR float deltas.
/// # Errors
/// Fails when a timestamp delta or delta-of-delta does not fit in an `i64`.
pub fn encode_chunk(chunk: &TimeChunk) -> Result<Bytes, TimeSeriesError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(GORILLA_CHUNK_MAGIC);
    keyenc::push_len_bytes(&mut bytes, chunk.series.as_bytes());
    bytes.extend_from_slice(&chunk.bucket_start.to_be_bytes());
    write_var_u64(
        &mut bytes,
        u64::try_from(chunk.points.len()).unwrap_or(u64::MAX),
    );
    let mut previous_ts = chunk.bucket_start;
    let mut previous_delta = 0_i64;
    let mut previous_bits = 0_u64;
    for point in &chunk.points {
        let delta = point
            .timestamp_millis
            .checked_sub(previous_ts)
            .ok_or_else(|| {
                TimeSeriesError::InvalidPoint("timestamp delta overflows i64".to_owned())
            })?;
        let bits = point.value.to_bits();
        let delta_of_delta = delta.checked_sub(previous_delta).ok_or_else(|| {
            TimeSeriesError::InvalidPoint("timestamp delta-of-delta overflows i64".to_owned())
        })?;
        let xor = bits ^ previous_bits;
        if delta_of_delta == 0 && xor == 0 {
            bytes.push(0);
        } else {
            bytes.push(1);
            write_var_u64(&mut bytes, zigzag_encode(delta_of_delta));
            write_xor_delta(&mut bytes, xor);
        }
        previous_ts = point.timestamp_millis;
        previous_delta = delta;
        previous_bits = bits;
    }
    Ok(bytes)
}

/// Decodes a time-series chunk.
/// # Errors
/// Fails when the byte stream is truncated or malformed.
pub fn decode_chunk(bytes: &[u8]) -> Result<TimeChunk, TimeSeriesError> {
    if bytes.starts_with(GORILLA_CHUNK_MAGIC) {
        return decode_gorilla_chunk(bytes);
    }
    decode_legacy_chunk(bytes)
}

fn decode_gorilla_chunk(bytes: &[u8]) -> Result<TimeChunk, TimeSeriesError> {
    let mut cursor = GORILLA_CHUNK_MAGIC.len();
    let series = read_len_string(bytes, &mut cursor)?;
    let bucket_start = read_i64(bytes, &mut cursor)?;
    let count = read_var_u64(bytes, &mut cursor)?;
    let mut points = Vec::new();
    let mut previous_ts = bucket_start;
    let mut previous_delta = 0_i64;
    let mut previous_bits = 0_u64;
    for _ in 0..count {
        let tag = read_u8(bytes, &mut cursor)?;
        let (delta, bits) = match tag {
            0 => (previous_delta, previous_bits),
            1 => {
                let delta_of_delta = zigzag_decode(read_var_u64(bytes, &mut cursor)?);
                let delta = previous_delta.checked_add(delta_of_delta).ok_or_else(|| {
                    TimeSeriesError::Corruption("timestamp delta overflows i64".to_owned())
                })?;
                let bits = previous_bits ^ read_xor_delta(bytes, &mut cursor)?;
                (delta, bits)
            }
            _ => {
                return Err(TimeSeriesError::Corruption(
                    "invalid Gorilla point tag".to_owned(),
                ));
            }
        };
        let timestamp_millis = previous_ts
            .checked_add(delta)
            .ok_or_else(|| TimeSeriesError::Corruption("timestamp overflows i64".to_owned()))?;
        points.push(decoded_point(timestamp_millis, bits)?);
        previous_ts = timestamp_millis;
        previous_delta = delta;
        previous_bits = bits;
    }
    if cursor != bytes.len() {
        return Err(TimeSeriesError::Corruption(
            "trailing bytes in Gorilla chunk".to_owned(),
        ));
    }
    Ok(TimeChunk {
        series,
        bucket_start,
        points,
    })
}

fn decode_legacy_chunk(bytes: &[u8]) -> Result<TimeChunk, TimeSeriesError> {
    let mut cursor = 0_usize;
    let series = read_len_string(bytes, &mut cursor)?;
    let bucket_start = read_i64(bytes, &mut cursor)?;
    let count = read_u64(bytes, &mut cursor)?;
    let mut points = Vec::new();
    let mut previous_ts = bucket_start;
    let mut previous_bits = 0_u64;
    for _ in 0..count {
        let delta = read_i64(bytes, &mut cursor)?;
        let xor = read_u64(bytes, &mut cursor)?;
        let timestamp_millis = previous_ts
            .checked_add(delta)
            .ok_or_else(|| TimeSeriesError::Corruption("timestamp overflows i64".to_owned()))?;
        let bits = previous_bits ^ xor;
        points.push(decoded_point(timestamp_millis, bits)?);
        previous_ts = timestamp_millis;
        previous_bits = bits;
    }
    if cursor != bytes.len() {
        return Err(TimeSeriesError::Corruption(
            "trailing bytes in legacy chunk".to_owned(),
        ));
    }
    Ok(TimeChunk {
        series,
        bucket_start,
        points,
    })
}

fn validate_config(config: &TimeSeriesConfig) -> Result<(), TimeSeriesError> {
    if config.name.is_empty() {
        return Err(TimeSeriesError::InvalidConfig(
            "collection name cannot be empty".to_owned(),
        ));
    }
    if config.chunk_millis <= 0 {
        return Err(TimeSeriesError::InvalidConfig(
            "chunk_millis must be positive".to_owned(),
        ));
    }
    if config.max_future_skew_millis < 0 {
        return Err(TimeSeriesError::InvalidConfig(
            "max_future_skew_millis cannot be negative".to_owned(),
        ));
    }
    Ok(())
}

fn validate_retention(config: &TimeSeriesConfig) -> Result<(), TimeSeriesError> {
    let Some(retention) = config.retention_millis else {
        return Ok(());
    };
    if retention <= 0 {
        return Err(TimeSeriesError::InvalidConfig(
            "retention_millis must be positive".to_owned(),
        ));
    }
    if retention < config.chunk_millis {
        return Err(TimeSeriesError::InvalidConfig(
            "retention_millis must be at least chunk_millis".to_owned(),
        ));
    }
    Ok(())
}

/// Checks a point's timestamp against server time and returns the retention cutoff, if any.
fn check_point_time(
    config: &TimeSeriesConfig,
    timestamp_millis: i64,
    now: i64,
) -> Result<Option<i64>, TimeSeriesError> {
    if let Some(retention) = config.retention_millis
        && retention <= 0
    {
        return Err(TimeSeriesError::InvalidConfig(format!(
            "time series {} has non-positive retention_millis {retention}; writes are refused \
             because they would delete existing chunks, create a new collection with a positive \
             retention_millis",
            config.name
        )));
    }
    if timestamp_millis > now.saturating_add(config.max_future_skew_millis) {
        return Err(TimeSeriesError::InvalidPoint(format!(
            "timestamp {timestamp_millis} is more than {} ms ahead of server time {now}",
            config.max_future_skew_millis
        )));
    }
    let Some(retention) = config.retention_millis else {
        return Ok(None);
    };
    let cutoff = now.saturating_sub(retention);
    if timestamp_millis < cutoff {
        return Err(TimeSeriesError::InvalidPoint(format!(
            "timestamp {timestamp_millis} is older than the retention cutoff {cutoff}"
        )));
    }
    Ok(Some(cutoff))
}

fn unix_millis(clock: &impl Clock) -> i64 {
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn validate_point(point: TimePoint) -> Result<(), TimeSeriesError> {
    if !point.value.is_finite() {
        return Err(TimeSeriesError::InvalidPoint(
            "time-series values must be finite".to_owned(),
        ));
    }
    Ok(())
}

fn append_retention_ops<R: Replication + ?Sized>(
    config: &TimeSeriesConfig,
    repl: &R,
    series: &str,
    cutoff: i64,
    ops: &mut Vec<Op>,
) -> Result<(), TimeSeriesError> {
    // Nothing storable lies below the lowest representable bucket.
    let Some(end_bucket) = bucket_start(cutoff, config.chunk_millis) else {
        return Ok(());
    };
    let start = chunk_prefix(&config.name, series);
    let end = chunk_key(&config.name, series, end_bucket);
    for (key, _) in repl.range(
        TIME_SERIES_CHUNKS_TABLE,
        &start,
        &end,
        ReadConsistency::Strong,
    )? {
        ops.push(Op::Delete {
            table: TIME_SERIES_CHUNKS_TABLE.to_owned(),
            key,
        });
    }
    Ok(())
}

fn bucket_start(timestamp: i64, interval: i64) -> Option<i64> {
    timestamp
        .checked_div_euclid(interval)?
        .checked_mul(interval)
}

fn chunk_prefix(name: &str, series: &str) -> Bytes {
    let mut key = Vec::new();
    keyenc::push_len_bytes(&mut key, name.as_bytes());
    keyenc::push_len_bytes(&mut key, series.as_bytes());
    key
}

fn chunk_key(name: &str, series: &str, bucket: i64) -> Bytes {
    let mut key = chunk_prefix(name, series);
    key.extend_from_slice(&(bucket ^ i64::MIN).to_be_bytes());
    key
}

fn latest_key(name: &str, series: &str) -> Bytes {
    chunk_prefix(name, series)
}

fn encode_point(point: TimePoint) -> Bytes {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&point.timestamp_millis.to_be_bytes());
    bytes.extend_from_slice(&point.value.to_bits().to_be_bytes());
    bytes
}

fn decode_point(bytes: &[u8]) -> Result<TimePoint, TimeSeriesError> {
    if bytes.len() != 16 {
        return Err(TimeSeriesError::Corruption(
            "invalid point length".to_owned(),
        ));
    }
    let mut ts = [0_u8; 8];
    ts.copy_from_slice(&bytes[..8]);
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[8..]);
    decoded_point(i64::from_be_bytes(ts), u64::from_be_bytes(value))
}

fn decoded_point(timestamp_millis: i64, bits: u64) -> Result<TimePoint, TimeSeriesError> {
    let value = f64::from_bits(bits);
    if !value.is_finite() {
        return Err(TimeSeriesError::Corruption(
            "non-finite stored point value".to_owned(),
        ));
    }
    Ok(TimePoint {
        timestamp_millis,
        value,
    })
}

fn write_xor_delta(bytes: &mut Bytes, xor: u64) {
    if xor == 0 {
        bytes.push(0);
    } else {
        bytes.push(1);
        write_var_u64(bytes, xor);
    }
}

fn read_xor_delta(bytes: &[u8], cursor: &mut usize) -> Result<u64, TimeSeriesError> {
    let tag = read_u8(bytes, cursor)?;
    match tag {
        0 => Ok(0),
        1 => read_var_u64(bytes, cursor),
        _ => Err(TimeSeriesError::Corruption(
            "invalid Gorilla XOR tag".to_owned(),
        )),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn write_var_u64(bytes: &mut Bytes, mut value: u64) {
    while value >= 0x80 {
        bytes.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn read_var_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, TimeSeriesError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = read_u8(bytes, cursor)?;
        if shift == 63 && byte > 1 {
            return Err(TimeSeriesError::Corruption(
                "varint is too large".to_owned(),
            ));
        }
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift = shift.saturating_add(7);
        if shift >= 64 {
            return Err(TimeSeriesError::Corruption(
                "varint is too large".to_owned(),
            ));
        }
    }
}

#[allow(clippy::cast_sign_loss)]
fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[allow(clippy::cast_possible_wrap)]
fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, TimeSeriesError> {
    let Some(value) = bytes.get(*cursor) else {
        return Err(TimeSeriesError::Corruption("truncated u8".to_owned()));
    };
    *cursor = (*cursor).saturating_add(1);
    Ok(*value)
}

fn read_len_string(bytes: &[u8], cursor: &mut usize) -> Result<String, TimeSeriesError> {
    let Some(raw) = keyenc::read_len_bytes(bytes, cursor) else {
        return Err(TimeSeriesError::Corruption("truncated string".to_owned()));
    };
    String::from_utf8(raw.to_vec()).map_err(|error| TimeSeriesError::Corruption(error.to_string()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, TimeSeriesError> {
    let mut raw = [0_u8; 8];
    let end = cursor.saturating_add(8);
    if end > bytes.len() {
        return Err(TimeSeriesError::Corruption("truncated i64".to_owned()));
    }
    raw.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(i64::from_be_bytes(raw))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, TimeSeriesError> {
    let mut raw = [0_u8; 8];
    let end = cursor.saturating_add(8);
    if end > bytes.len() {
        return Err(TimeSeriesError::Corruption("truncated u64".to_owned()));
    }
    raw.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(u64::from_be_bytes(raw))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use crate::{
        db::{CatalogEntry, DbConfig, DbError, Profile, create_database},
        repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication},
        storage::Bytes,
        txn,
    };

    use super::{
        DEFAULT_MAX_FUTURE_SKEW_MILLIS, TIME_SERIES_META_TABLE, TimeChunk, TimePoint,
        TimeSeriesCollection, TimeSeriesConfig, TimeSeriesError, decode_chunk, encode_chunk,
        time_bucket,
    };

    const NOW: i64 = 1_700_000_000_000;

    #[test]
    fn time_series_chunks_round_trip_and_query_ranges() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let ts = TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("metrics").with_chunk_millis(1_000),
        )?;
        ts.insert_point(
            "cpu",
            TimePoint {
                timestamp_millis: 1_000,
                value: 1.5,
            },
        )?;
        ts.insert_point(
            "cpu",
            TimePoint {
                timestamp_millis: 1_250,
                value: 2.5,
            },
        )?;
        ts.insert_point(
            "cpu",
            TimePoint {
                timestamp_millis: 2_000,
                value: 3.5,
            },
        )?;
        assert_eq!(ts.range("cpu", 1_000, 2_000)?.len(), 2);
        assert_eq!(
            ts.latest("cpu")?.map(|point| point.timestamp_millis),
            Some(2_000)
        );
        Ok(())
    }

    #[test]
    fn time_series_latest_does_not_move_backwards() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let ts = TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("metrics").with_chunk_millis(1_000),
        )?;

        ts.insert_point(
            "cpu",
            TimePoint {
                timestamp_millis: 2_000,
                value: 3.5,
            },
        )?;
        ts.insert_point(
            "cpu",
            TimePoint {
                timestamp_millis: 1_500,
                value: 2.5,
            },
        )?;

        assert_eq!(
            ts.latest("cpu")?.map(|point| point.timestamp_millis),
            Some(2_000)
        );
        assert_eq!(ts.range("cpu", 1_000, 2_500)?.len(), 2);
        Ok(())
    }

    #[test]
    fn insert_point_requires_conditional_batch_support() -> Result<(), Box<dyn std::error::Error>> {
        let ts = TimeSeriesCollection::new(
            &NoConditionalRepl,
            TimeSeriesConfig::new("metrics").with_chunk_millis(1_000),
        )?;

        assert!(matches!(
            ts.insert_point(
                "cpu",
                TimePoint {
                    timestamp_millis: 1_000,
                    value: 1.0,
                },
            ),
            Err(super::TimeSeriesError::Repl(ReplError::Unsupported(_)))
        ));

        Ok(())
    }

    #[test]
    fn time_series_codec_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let chunk = TimeChunk {
            series: "s".to_owned(),
            bucket_start: 0,
            points: vec![
                TimePoint {
                    timestamp_millis: 10,
                    value: 1.0,
                },
                TimePoint {
                    timestamp_millis: 20,
                    value: 1.5,
                },
            ],
        };
        assert_eq!(decode_chunk(&encode_chunk(&chunk)?)?, chunk);
        Ok(())
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn gorilla_codec_compresses_regular_series() -> Result<(), Box<dyn std::error::Error>> {
        let points = (0..2_048)
            .map(|index| TimePoint {
                timestamp_millis: i64::from(index) * 1_000,
                value: 42.0,
            })
            .collect::<Vec<_>>();
        let chunk = TimeChunk {
            series: "cpu".to_owned(),
            bucket_start: 0,
            points,
        };
        let encoded = encode_chunk(&chunk)?;
        let raw_bytes = chunk.points.len() * 16;
        let ratio = raw_bytes as f64 / encoded.len() as f64;

        assert_eq!(decode_chunk(&encoded)?, chunk);
        assert!(ratio >= 8.0, "ratio was {ratio}");
        Ok(())
    }

    #[test]
    fn retention_cutoff_uses_server_clock_not_point_timestamp()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let ts = TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("metrics")
                .with_chunk_millis(1_000)
                .with_retention_millis(3_600_000),
        )?;
        let clock = FixedClock(NOW);
        for timestamp in [NOW - 4_000, NOW - 3_000, NOW - 2_000, NOW - 1_000, NOW] {
            ts.insert_point_with_clock("cpu", point(timestamp, 1.0), &clock)?;
        }

        // Microsecond- and nanosecond-scaled timestamps must not move the retention cutoff.
        for scaled in [NOW * 1_000, NOW * 1_000_000] {
            assert!(matches!(
                ts.insert_point_with_clock("cpu", point(scaled, 2.0), &clock),
                Err(TimeSeriesError::InvalidPoint(_))
            ));
        }

        assert_eq!(ts.range("cpu", NOW - 10_000, NOW + 10_000)?.len(), 5);
        assert_eq!(
            ts.latest("cpu")?.map(|latest| latest.timestamp_millis),
            Some(NOW)
        );
        Ok(())
    }

    #[test]
    fn retention_prunes_chunks_older_than_server_cutoff() -> Result<(), Box<dyn std::error::Error>>
    {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let ts = TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("metrics")
                .with_chunk_millis(1_000)
                .with_retention_millis(10_000),
        )?;

        ts.insert_point_with_clock("cpu", point(NOW - 20_000, 1.0), &FixedClock(NOW - 20_000))?;
        assert_eq!(ts.range("cpu", NOW - 30_000, NOW + 1)?.len(), 1);

        // Cutoff is NOW - 19_000, so the chunk at NOW - 20_000 is pruned in the same batch.
        ts.insert_point_with_clock("cpu", point(NOW - 9_500, 2.0), &FixedClock(NOW - 9_000))?;
        assert_eq!(
            timestamps(&ts.range("cpu", NOW - 30_000, NOW + 1)?),
            vec![NOW - 9_500]
        );

        // Cutoff is NOW - 10_000; the chunk starting exactly there is kept.
        ts.insert_point_with_clock("cpu", point(NOW, 3.0), &FixedClock(NOW))?;
        assert_eq!(
            timestamps(&ts.range("cpu", NOW - 30_000, NOW + 1)?),
            vec![NOW - 9_500, NOW]
        );
        Ok(())
    }

    #[test]
    fn points_older_than_retention_window_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let ts = TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("metrics")
                .with_chunk_millis(1_000)
                .with_retention_millis(10_000),
        )?;
        let clock = FixedClock(NOW);

        assert!(matches!(
            ts.insert_point_with_clock("cpu", point(NOW - 10_001, 1.0), &clock),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        assert!(
            ts.range("cpu", NOW - 20_000, NOW + 1)?.is_empty(),
            "expected an empty value, got {:?}",
            ts.range("cpu", NOW - 20_000, NOW + 1)?
        );
        assert!(ts.latest("cpu")?.is_none());

        ts.insert_point_with_clock("cpu", point(NOW - 10_000, 2.0), &clock)?;
        assert_eq!(
            timestamps(&ts.range("cpu", NOW - 20_000, NOW + 1)?),
            vec![NOW - 10_000]
        );
        Ok(())
    }

    #[test]
    fn future_skew_is_bounded_and_configurable() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let clock = FixedClock(NOW);

        let ts = TimeSeriesCollection::new(repl.as_ref(), TimeSeriesConfig::new("metrics"))?;
        ts.insert_point_with_clock(
            "cpu",
            point(NOW + DEFAULT_MAX_FUTURE_SKEW_MILLIS, 1.0),
            &clock,
        )?;
        assert!(matches!(
            ts.insert_point_with_clock(
                "cpu",
                point(NOW + DEFAULT_MAX_FUTURE_SKEW_MILLIS + 1, 2.0),
                &clock,
            ),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        assert_eq!(
            ts.latest("cpu")?.map(|latest| latest.timestamp_millis),
            Some(NOW + DEFAULT_MAX_FUTURE_SKEW_MILLIS)
        );

        let strict = TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("strict").with_max_future_skew_millis(0),
        )?;
        assert!(matches!(
            strict.insert_point_with_clock("cpu", point(NOW + 1, 1.0), &clock),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        strict.insert_point_with_clock("cpu", point(NOW, 1.0), &clock)?;

        assert!(matches!(
            TimeSeriesCollection::new(
                repl.as_ref(),
                TimeSeriesConfig::new("bad").with_max_future_skew_millis(-1),
            ),
            Err(TimeSeriesError::InvalidConfig(_))
        ));
        Ok(())
    }

    #[test]
    fn create_rejects_non_positive_and_sub_chunk_retention()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        for config in [
            TimeSeriesConfig::new("zero")
                .with_chunk_millis(1_000)
                .with_retention_millis(0),
            TimeSeriesConfig::new("negative")
                .with_chunk_millis(1_000)
                .with_retention_millis(-1),
            TimeSeriesConfig::new("short")
                .with_chunk_millis(1_000)
                .with_retention_millis(500),
        ] {
            let name = config.name.clone();
            assert!(TimeSeriesCollection::<dyn Replication>::metadata_op(&config).is_err());
            assert!(matches!(
                database.create_time_series(config),
                Err(DbError::TimeSeries(TimeSeriesError::InvalidConfig(_)))
            ));
            assert!(!database.catalog().contains_key(&name));
            assert!(
                Replication::read(
                    &database,
                    TIME_SERIES_META_TABLE,
                    name.as_bytes(),
                    ReadConsistency::Strong,
                )?
                .is_none()
            );
        }

        database.create_time_series(
            TimeSeriesConfig::new("ok")
                .with_chunk_millis(1_000)
                .with_retention_millis(1_000),
        )?;
        assert!(database.catalog().contains_key("ok"));
        Ok(())
    }

    #[test]
    fn legacy_non_positive_retention_fails_writes_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let clock = FixedClock(NOW);
        TimeSeriesCollection::new(
            repl.as_ref(),
            TimeSeriesConfig::new("metrics").with_chunk_millis(1_000),
        )?
        .insert_point_with_clock("cpu", point(NOW, 1.0), &clock)?;

        for retention in [-1, 0] {
            // Legacy configs stay readable, but every write is refused.
            let legacy = TimeSeriesCollection::new(
                repl.as_ref(),
                TimeSeriesConfig::new("metrics")
                    .with_chunk_millis(1_000)
                    .with_retention_millis(retention),
            )?;
            assert!(matches!(
                legacy.insert_point_with_clock("cpu", point(NOW + 1, 2.0), &clock),
                Err(TimeSeriesError::InvalidConfig(_))
            ));
            assert_eq!(legacy.range("cpu", NOW - 1_000, NOW + 1_000)?.len(), 1);
        }
        Ok(())
    }

    #[test]
    fn legacy_config_json_defaults_future_skew() -> Result<(), Box<dyn std::error::Error>> {
        let config: TimeSeriesConfig = serde_json::from_slice(
            br#"{"name":"metrics","chunk_millis":60000,"retention_millis":null}"#,
        )?;
        assert_eq!(
            config,
            TimeSeriesConfig::new("metrics").with_max_future_skew_millis(i64::MAX)
        );

        let entry: CatalogEntry = serde_json::from_slice(
            br#"{"TimeSeries":{"config":{"name":"metrics","chunk_millis":60000,"retention_millis":3600000}}}"#,
        )?;
        assert!(matches!(
            entry,
            CatalogEntry::TimeSeries { config }
                if config.retention_millis == Some(3_600_000)
                    && config.max_future_skew_millis == i64::MAX
        ));
        Ok(())
    }

    #[test]
    fn bucket_math_is_overflow_safe_at_i64_bounds() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            time_bucket(60_000, i64::MIN),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        assert!(matches!(
            time_bucket(1_000, i64::MIN),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        assert_eq!(
            time_bucket(60_000, i64::MAX)?,
            i64::MAX - i64::MAX.rem_euclid(60_000)
        );
        assert_eq!(time_bucket(1_000, -1)?, -1_000);

        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let ts = TimeSeriesCollection::new(repl.as_ref(), TimeSeriesConfig::new("metrics"))?;
        let clock = FixedClock(NOW);
        assert!(matches!(
            ts.insert_point_with_clock("cpu", point(i64::MIN, 1.0), &clock),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        ts.insert_point_with_clock("cpu", point(1_000, 1.0), &clock)?;
        ts.insert_point_with_clock("cpu", point(NOW, 2.0), &clock)?;

        assert_eq!(ts.range("cpu", i64::MIN, i64::MAX)?.len(), 2);
        assert!(
            ts.range("cpu", i64::MIN, i64::MIN + 1)?.is_empty(),
            "expected an empty value, got {:?}",
            ts.range("cpu", i64::MIN, i64::MIN + 1)?
        );
        assert_eq!(ts.codec_stats("cpu", i64::MIN, i64::MAX)?.raw_bytes, 32);
        Ok(())
    }

    fn point(timestamp_millis: i64, value: f64) -> TimePoint {
        TimePoint {
            timestamp_millis,
            value,
        }
    }

    fn timestamps(points: &[TimePoint]) -> Vec<i64> {
        points.iter().map(|point| point.timestamp_millis).collect()
    }

    #[test]
    fn retention_sweep_removes_idle_series_in_bounded_resumable_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let config = TimeSeriesConfig::new("metrics")
            .with_chunk_millis(1_000)
            .with_retention_millis(10_000);
        let ts = TimeSeriesCollection::new(&database, config.clone())?;
        for series in ["cpu", "memory", "disk"] {
            ts.insert_point_with_clock(
                series,
                point(NOW - 20_000, 1.0),
                &FixedClock(NOW - 20_000),
            )?;
        }
        ts.insert_point_with_clock("active", point(NOW, 2.0), &FixedClock(NOW))?;
        let configs = std::collections::BTreeMap::from([(config.name.clone(), config)]);
        let mut cursor = None;
        let mut chunks = 0;
        let mut latest = 0;
        for _ in 0..20 {
            let report = super::sweep_retention_at(&database, &configs, cursor, 1, NOW)?;
            assert!(report.scanned_rows <= 1);
            chunks += report.deleted_chunks;
            latest += report.deleted_latest;
            cursor = report.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert!(cursor.is_none());
        assert_eq!(chunks, 3);
        assert_eq!(latest, 3);
        for series in ["cpu", "memory", "disk"] {
            assert_eq!(ts.range(series, NOW - 30_000, NOW + 1)?, Vec::new());
            assert!(ts.latest(series)?.is_none());
        }
        assert_eq!(ts.latest("active")?, Some(point(NOW, 2.0)));
        assert!(super::sweep_retention_at(&database, &configs, None, 0, NOW).is_err());
        Ok(())
    }

    #[test]
    fn retention_sweep_preserves_the_boundary_chunk_and_unlimited_collections()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let bounded = TimeSeriesConfig::new("bounded")
            .with_chunk_millis(1_000)
            .with_retention_millis(10_000);
        let unlimited = TimeSeriesConfig::new("unlimited").with_chunk_millis(1_000);
        for config in [&bounded, &unlimited] {
            let ts = TimeSeriesCollection::new(&database, config.clone())?;
            ts.insert_point_with_clock("cpu", point(NOW - 10_999, 1.0), &FixedClock(NOW - 10_999))?;
        }
        let configs = std::collections::BTreeMap::from([
            (bounded.name.clone(), bounded.clone()),
            (unlimited.name.clone(), unlimited.clone()),
        ]);
        // Cutoff is NOW - 10_500: that chunk is kept until the entire chunk expires.
        let report = super::sweep_retention_at(&database, &configs, None, 128, NOW - 500)?;
        assert_eq!(report.deleted_chunks, 0);
        let _ = super::sweep_retention_at(&database, &configs, report.next_cursor, 128, NOW - 500)?;
        assert!(
            TimeSeriesCollection::new(&database, bounded)?
                .latest("cpu")?
                .is_some()
        );
        assert!(
            TimeSeriesCollection::new(&database, unlimited)?
                .latest("cpu")?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn codec_rejects_timestamp_overflow_instead_of_panicking()
    -> Result<(), Box<dyn std::error::Error>> {
        let span = TimeChunk {
            series: "cpu".to_owned(),
            bucket_start: i64::MIN,
            points: vec![point(i64::MIN, 1.0), point(0, 2.0)],
        };
        assert!(matches!(
            encode_chunk(&span),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        let delta_of_delta = TimeChunk {
            series: "cpu".to_owned(),
            bucket_start: 0,
            points: vec![point(i64::MAX, 1.0), point(i64::MAX - 2, 2.0)],
        };
        assert!(matches!(
            encode_chunk(&delta_of_delta),
            Err(TimeSeriesError::InvalidPoint(_))
        ));
        let mut bytes = encode_chunk(&TimeChunk {
            series: "cpu".to_owned(),
            bucket_start: 0,
            points: vec![point(1, 1.0)],
        })?;
        // Change the stored anchor so decoding the otherwise-valid delta would overflow.
        let offset = super::GORILLA_CHUNK_MAGIC.len() + 8 + 3;
        bytes[offset..offset + 8].copy_from_slice(&i64::MAX.to_be_bytes());
        assert!(matches!(
            decode_chunk(&bytes),
            Err(TimeSeriesError::Corruption(_))
        ));
        let mut legacy = Vec::new();
        crate::keyenc::push_len_bytes(&mut legacy, b"cpu");
        legacy.extend_from_slice(&i64::MAX.to_be_bytes());
        legacy.extend_from_slice(&1_u64.to_be_bytes());
        legacy.extend_from_slice(&1_i64.to_be_bytes());
        legacy.extend_from_slice(&0_u64.to_be_bytes());
        assert!(matches!(
            decode_chunk(&legacy),
            Err(TimeSeriesError::Corruption(_))
        ));
        Ok(())
    }

    fn legacy_codec_bytes(chunk: &TimeChunk) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut bytes = Vec::new();
        crate::keyenc::push_len_bytes(&mut bytes, chunk.series.as_bytes());
        bytes.extend_from_slice(&chunk.bucket_start.to_be_bytes());
        bytes.extend_from_slice(&u64::try_from(chunk.points.len())?.to_be_bytes());
        let mut timestamp = chunk.bucket_start;
        let mut bits = 0;
        for point in &chunk.points {
            let delta = point
                .timestamp_millis
                .checked_sub(timestamp)
                .ok_or("legacy delta overflow")?;
            bytes.extend_from_slice(&delta.to_be_bytes());
            bytes.extend_from_slice(&(bits ^ point.value.to_bits()).to_be_bytes());
            timestamp = point.timestamp_millis;
            bits = point.value.to_bits();
        }
        Ok(bytes)
    }

    #[test]
    fn both_chunk_formats_preserve_finite_values_and_reject_trailing_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let chunk = TimeChunk {
            series: "cpu".to_owned(),
            bucket_start: 0,
            points: vec![
                point(-100, -0.0),
                point(0, f64::MAX),
                point(2_000, -f64::MIN_POSITIVE),
                point(3_000, f64::from_bits(1)),
            ],
        };
        for mut bytes in [encode_chunk(&chunk)?, legacy_codec_bytes(&chunk)?] {
            let decoded = decode_chunk(&bytes)?;
            assert_eq!(decoded, chunk);
            for (actual, expected) in decoded.points.iter().zip(&chunk.points) {
                assert_eq!(actual.value.to_bits(), expected.value.to_bits());
            }
            bytes.push(0);
            assert!(matches!(
                decode_chunk(&bytes),
                Err(TimeSeriesError::Corruption(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn overflowing_gorilla_varints_cannot_alias_valid_counts_deltas_or_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut prefix = super::GORILLA_CHUNK_MAGIC.to_vec();
        crate::keyenc::push_len_bytes(&mut prefix, b"cpu");
        prefix.extend_from_slice(&0_i64.to_be_bytes());
        let mut overflow = vec![0x80; 9];
        overflow.push(2); // 2^64 must not truncate to zero at shift 63.
        let count = [prefix.clone(), overflow.clone()].concat();
        let delta = [prefix.clone(), vec![1, 1], overflow.clone(), vec![0]].concat();
        let value = [prefix, vec![1, 1, 0, 1], overflow].concat();
        for bytes in [count, delta, value] {
            assert!(matches!(
                decode_chunk(&bytes),
                Err(TimeSeriesError::Corruption(_))
            ));
        }
        let mut largest = Vec::new();
        super::write_var_u64(&mut largest, u64::MAX);
        let mut cursor = 0;
        assert_eq!(super::read_var_u64(&largest, &mut cursor)?, u64::MAX);
        assert_eq!(cursor, largest.len());
        Ok(())
    }

    #[test]
    fn all_stored_point_formats_reject_nonfinite_values() -> Result<(), Box<dyn std::error::Error>>
    {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let stored = point(0, value);
            let chunk = TimeChunk {
                series: "cpu".to_owned(),
                bucket_start: 0,
                points: vec![stored],
            };
            for bytes in [encode_chunk(&chunk)?, legacy_codec_bytes(&chunk)?] {
                assert!(matches!(
                    decode_chunk(&bytes),
                    Err(TimeSeriesError::Corruption(_))
                ));
            }
            assert!(matches!(
                super::decode_point(&super::encode_point(stored)),
                Err(TimeSeriesError::Corruption(_))
            ));
        }
        Ok(())
    }

    struct FixedClock(i64);

    impl txn::Clock for FixedClock {
        fn now(&self) -> SystemTime {
            UNIX_EPOCH + Duration::from_millis(u64::try_from(self.0).unwrap_or(0))
        }
    }

    struct NoConditionalRepl;

    impl Replication for NoConditionalRepl {
        fn propose(&self, _op: Op) -> Result<(), ReplError> {
            Ok(())
        }

        fn propose_batch(&self, _ops: Vec<Op>) -> Result<(), ReplError> {
            Err(ReplError::Transport(
                "plain batch fallback was called".to_owned(),
            ))
        }

        fn propose_authorized_batch(
            &self,
            _ops: Vec<Op>,
            _authorization: txn::WriteAuthorization,
        ) -> Result<(), ReplError> {
            Ok(())
        }

        fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
            if batch.conditions.is_empty() {
                self.propose_batch(batch.ops)
            } else {
                Err(ReplError::Unsupported(
                    "conditional batches are not supported".to_owned(),
                ))
            }
        }

        fn read(
            &self,
            _table: &str,
            _key: &[u8],
            _consistency: ReadConsistency,
        ) -> Result<Option<Bytes>, ReplError> {
            Ok(None)
        }

        fn range(
            &self,
            _table: &str,
            _start: &[u8],
            _end: &[u8],
            _consistency: ReadConsistency,
        ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
            Ok(Vec::new())
        }
    }
}
