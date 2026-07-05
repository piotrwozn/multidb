use crate::{
    keyenc,
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition,
        propose_system,
    },
    storage::{Bytes, StorageError},
};

pub const TIME_SERIES_CHUNKS_TABLE: &str = "time_series_chunks";
pub const TIME_SERIES_LATEST_TABLE: &str = "time_series_latest";
pub const TIME_SERIES_META_TABLE: &str = "__time_series_meta";
const GORILLA_CHUNK_MAGIC: &[u8] = b"MDTSG1";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TimeSeriesConfig {
    pub name: String,
    pub chunk_millis: i64,
    pub retention_millis: Option<i64>,
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
    /// Fails when metadata cannot be serialized.
    pub fn metadata_op(config: &TimeSeriesConfig) -> Result<Op, TimeSeriesError> {
        Ok(Op::Put {
            table: TIME_SERIES_META_TABLE.to_owned(),
            key: config.name.as_bytes().to_vec(),
            value: serde_json::to_vec(config)
                .map_err(|error| TimeSeriesError::Corruption(error.to_string()))?,
        })
    }

    /// Inserts or replaces one point.
    /// # Errors
    /// Fails when the point is invalid or the chunk cannot be written.
    pub fn insert_point(&self, series: &str, point: TimePoint) -> Result<(), TimeSeriesError> {
        validate_point(point)?;
        let bucket = bucket_start(point.timestamp_millis, self.config.chunk_millis);
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
        append_retention_ops(
            &self.config,
            self.repl,
            series,
            point.timestamp_millis,
            &mut ops,
        )?;
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
        let start_bucket = bucket_start(start, self.config.chunk_millis);
        let end_bucket = bucket_start(end.saturating_sub(1), self.config.chunk_millis);
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
        let chunk = TimeChunk {
            series: series.to_owned(),
            bucket_start: bucket_start(start, self.config.chunk_millis),
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
/// Fails when the interval is not positive.
pub fn time_bucket(interval_millis: i64, timestamp_millis: i64) -> Result<i64, TimeSeriesError> {
    if interval_millis <= 0 {
        return Err(TimeSeriesError::InvalidConfig(
            "bucket interval must be positive".to_owned(),
        ));
    }
    Ok(bucket_start(timestamp_millis, interval_millis))
}

/// Encodes a chunk with Gorilla-style delta-of-delta timestamps and XOR float deltas.
/// # Errors
/// Currently only fails if an internal size conversion cannot be represented.
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
        let delta = point.timestamp_millis - previous_ts;
        let bits = point.value.to_bits();
        let delta_of_delta = delta - previous_delta;
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
                let delta = previous_delta + delta_of_delta;
                let bits = previous_bits ^ read_xor_delta(bytes, &mut cursor)?;
                (delta, bits)
            }
            _ => {
                return Err(TimeSeriesError::Corruption(
                    "invalid Gorilla point tag".to_owned(),
                ));
            }
        };
        let timestamp_millis = previous_ts + delta;
        points.push(TimePoint {
            timestamp_millis,
            value: f64::from_bits(bits),
        });
        previous_ts = timestamp_millis;
        previous_delta = delta;
        previous_bits = bits;
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
        let timestamp_millis = previous_ts + delta;
        let bits = previous_bits ^ xor;
        points.push(TimePoint {
            timestamp_millis,
            value: f64::from_bits(bits),
        });
        previous_ts = timestamp_millis;
        previous_bits = bits;
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
    Ok(())
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
    now: i64,
    ops: &mut Vec<Op>,
) -> Result<(), TimeSeriesError> {
    let Some(retention) = config.retention_millis else {
        return Ok(());
    };
    let cutoff = now.saturating_sub(retention);
    let end_bucket = bucket_start(cutoff, config.chunk_millis);
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

fn bucket_start(timestamp: i64, interval: i64) -> i64 {
    timestamp.div_euclid(interval) * interval
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
    Ok(TimePoint {
        timestamp_millis: i64::from_be_bytes(ts),
        value: f64::from_bits(u64::from_be_bytes(value)),
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
    use std::sync::Arc;

    use crate::{
        db::{DbConfig, Profile, create_database},
        repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication},
        storage::Bytes,
        txn,
    };

    use super::{
        TimeChunk, TimePoint, TimeSeriesCollection, TimeSeriesConfig, decode_chunk, encode_chunk,
    };

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
