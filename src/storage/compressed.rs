use crate::{
    observability,
    performance::{CompressionAlgorithm, CompressionConfig},
    storage::{Bytes, RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
};

const MAGIC: &[u8; 5] = b"MDBC1";
const HEADER_LEN: usize = MAGIC.len() + 1 + 8 + 8;
const ALGO_RAW: u8 = 0;
const ALGO_LZ4: u8 = 1;
const ALGO_ZSTD: u8 = 2;
const MAX_DECODED_VALUE_BYTES: u64 = 64 * 1024 * 1024;

pub struct CompressedEngine<S> {
    inner: S,
    config: CompressionConfig,
}

pub struct CompressedReadTxn<T> {
    inner: T,
}

pub struct CompressedWriteTxn<T> {
    inner: T,
    config: CompressionConfig,
}

impl<S> CompressedEngine<S> {
    #[must_use]
    pub const fn new(inner: S, config: CompressionConfig) -> Self {
        Self { inner, config }
    }

    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> StorageEngine for CompressedEngine<S>
where
    S: StorageEngine,
{
    type ReadTxn<'a>
        = CompressedReadTxn<S::ReadTxn<'a>>
    where
        Self: 'a;

    type WriteTxn<'a>
        = CompressedWriteTxn<S::WriteTxn<'a>>
    where
        Self: 'a;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        Ok(CompressedReadTxn {
            inner: self.inner.begin_read()?,
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        Ok(CompressedWriteTxn {
            inner: self.inner.begin_write()?,
            config: self.config.clone(),
        })
    }
}

impl<T> ReadTransaction for CompressedReadTxn<T>
where
    T: ReadTransaction,
{
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        self.inner
            .get(table, key)?
            .map(|bytes| decode_stored_value(&bytes))
            .transpose()
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let rows = self
            .inner
            .range(table, start, end)?
            .map(|item| {
                let (key, value) = item?;
                Ok((key, decode_stored_value(&value)?))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl<T> ReadTransaction for CompressedWriteTxn<T>
where
    T: WriteTransaction,
{
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        self.inner
            .get(table, key)?
            .map(|bytes| decode_stored_value(&bytes))
            .transpose()
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let rows = self
            .inner
            .range(table, start, end)?
            .map(|item| {
                let (key, value) = item?;
                Ok((key, decode_stored_value(&value)?))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl<T> WriteTransaction for CompressedWriteTxn<T>
where
    T: WriteTransaction,
{
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let encoded = encode_stored_value(value, &self.config)?;
        self.inner.put(table, key, &encoded)
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        self.inner.delete(table, key)
    }

    fn commit(self) -> Result<(), StorageError> {
        self.inner.commit()
    }

    fn rollback(self) {
        self.inner.rollback();
    }
}

/// Encodes one value with a versioned compression header.
/// # Errors
/// Fails when compression backend rejects the value.
pub fn encode_stored_value(
    value: &[u8],
    config: &CompressionConfig,
) -> Result<Bytes, StorageError> {
    let (algorithm, payload) = if value.len() < config.min_bytes {
        (ALGO_RAW, value.to_vec())
    } else {
        match config.algorithm {
            CompressionAlgorithm::None => (ALGO_RAW, value.to_vec()),
            CompressionAlgorithm::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(value);
                if compressed.len() < value.len() {
                    (ALGO_LZ4, compressed)
                } else {
                    (ALGO_RAW, value.to_vec())
                }
            }
            CompressionAlgorithm::Zstd => {
                let compressed = zstd::bulk::compress(value, config.zstd_level)
                    .map_err(|error| StorageError::Backend(error.to_string()))?;
                if compressed.len() < value.len() {
                    (ALGO_ZSTD, compressed)
                } else {
                    (ALGO_RAW, value.to_vec())
                }
            }
        }
    };

    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len());
    encoded.extend_from_slice(MAGIC);
    encoded.push(algorithm);
    encoded.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    encoded.extend_from_slice(&checksum(value).to_be_bytes());
    encoded.extend_from_slice(&payload);
    observability::record_compression(algorithm_label(algorithm), value.len(), encoded.len());
    Ok(encoded)
}

/// Decodes a value. Unframed legacy bytes are returned unchanged.
/// # Errors
/// Fails when the frame is corrupt or decompression fails.
pub fn decode_stored_value(value: &[u8]) -> Result<Bytes, StorageError> {
    if !value.starts_with(MAGIC) {
        return Ok(value.to_vec());
    }
    if value.len() < HEADER_LEN {
        return Err(StorageError::Corruption(
            "compressed value has truncated header".to_owned(),
        ));
    }

    let algorithm = value[MAGIC.len()];
    let original_len_start = MAGIC.len() + 1;
    let checksum_start = original_len_start + 8;
    let payload_start = checksum_start + 8;
    let original_len =
        u64::from_be_bytes(read_array::<8>(&value[original_len_start..checksum_start])?);
    let expected_checksum =
        u64::from_be_bytes(read_array::<8>(&value[checksum_start..payload_start])?);
    let payload = &value[payload_start..];
    if original_len > MAX_DECODED_VALUE_BYTES {
        return Err(StorageError::Corruption(format!(
            "compressed value original length {original_len} exceeds decode limit {MAX_DECODED_VALUE_BYTES}"
        )));
    }

    let decoded = match algorithm {
        ALGO_RAW => payload.to_vec(),
        ALGO_LZ4 => {
            validate_lz4_prepended_size(payload, original_len)?;
            lz4_flex::decompress_size_prepended(payload)
                .map_err(|error| StorageError::Corruption(error.to_string()))?
        }
        ALGO_ZSTD => zstd::bulk::decompress(
            payload,
            usize::try_from(original_len).map_err(|error| {
                StorageError::Corruption(format!("compressed value length: {error}"))
            })?,
        )
        .map_err(|error| StorageError::Corruption(error.to_string()))?,
        _ => {
            return Err(StorageError::Corruption(
                "compressed value has unknown algorithm".to_owned(),
            ));
        }
    };

    if u64::try_from(decoded.len()).unwrap_or(u64::MAX) != original_len {
        return Err(StorageError::Corruption(
            "compressed value length mismatch".to_owned(),
        ));
    }
    if checksum(&decoded) != expected_checksum {
        return Err(StorageError::Corruption(
            "compressed value checksum mismatch".to_owned(),
        ));
    }

    Ok(decoded)
}

fn validate_lz4_prepended_size(payload: &[u8], original_len: u64) -> Result<(), StorageError> {
    if payload.len() < 4 {
        return Err(StorageError::Corruption(
            "lz4 payload is missing prepended size".to_owned(),
        ));
    }
    let announced = u32::from_le_bytes(read_array::<4>(&payload[..4])?);
    let announced = u64::from(announced);
    if announced > MAX_DECODED_VALUE_BYTES {
        return Err(StorageError::Corruption(format!(
            "lz4 payload length {announced} exceeds decode limit {MAX_DECODED_VALUE_BYTES}"
        )));
    }
    if announced != original_len {
        return Err(StorageError::Corruption(
            "lz4 prepended length does not match frame length".to_owned(),
        ));
    }
    Ok(())
}

fn read_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], StorageError> {
    bytes
        .try_into()
        .map_err(|_| StorageError::Corruption("compressed value has malformed header".to_owned()))
}

fn checksum(value: &[u8]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(value);
    let digest = hasher.finalize();
    u64::from_be_bytes(digest.as_bytes()[..8].try_into().unwrap_or([0; 8]))
}

const fn algorithm_label(algorithm: u8) -> &'static str {
    match algorithm {
        ALGO_RAW => "raw",
        ALGO_LZ4 => "lz4",
        ALGO_ZSTD => "zstd",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ALGO_LZ4, HEADER_LEN, MAGIC, MAX_DECODED_VALUE_BYTES, decode_stored_value,
        encode_stored_value,
    };
    use crate::{
        performance::{CompressionAlgorithm, CompressionConfig},
        storage::{CompressedEngine, MemEngine, ReadTransaction, StorageEngine, WriteTransaction},
    };

    #[test]
    fn lz4_round_trips_and_reads_legacy_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_bytes: 1,
            zstd_level: 0,
        };
        let value = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let encoded = encode_stored_value(value, &config)?;

        assert_eq!(decode_stored_value(&encoded)?, value);
        assert_eq!(decode_stored_value(b"legacy")?, b"legacy");
        Ok(())
    }

    #[test]
    fn checksum_mismatch_is_corruption() -> Result<(), Box<dyn std::error::Error>> {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_bytes: 1,
            zstd_level: 0,
        };
        let mut encoded = encode_stored_value(b"aaaaaaaaaaaaaaaaaaaaaaaa", &config)?;
        let last = encoded.len().saturating_sub(1);
        encoded[last] ^= 0x01;

        assert!(matches!(
            decode_stored_value(&encoded),
            Err(crate::storage::StorageError::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn oversized_lz4_frame_is_rejected_before_allocation() {
        let announced = MAX_DECODED_VALUE_BYTES + 1;
        let mut encoded = Vec::with_capacity(HEADER_LEN + 4);
        encoded.extend_from_slice(MAGIC);
        encoded.push(ALGO_LZ4);
        encoded.extend_from_slice(&announced.to_be_bytes());
        encoded.extend_from_slice(&0_u64.to_be_bytes());
        encoded.extend_from_slice(&(u32::MAX).to_le_bytes());

        assert!(matches!(
            decode_stored_value(&encoded),
            Err(crate::storage::StorageError::Corruption(_))
        ));
    }

    #[test]
    fn compressed_engine_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_bytes: 1,
            zstd_level: 0,
        };
        let engine = CompressedEngine::new(MemEngine::new(), config);
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"aaaaaaaaaaaaaaaaaaaaaaaa")?;
        write.commit()?;

        let read = engine.begin_read()?;
        assert_eq!(
            read.get("t", b"k")?,
            Some(b"aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
        Ok(())
    }
}
