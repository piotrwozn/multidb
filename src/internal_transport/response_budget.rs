//! Bound outbound serialization before allocating its wire buffer, and stop
//! native AP scans before retaining an oversized response.

use postcard::ser_flavors::Flavor;
use serde::Serialize;

use super::{InternalResponse, InternalTransportError, frame_budget::MAX_DECODED_BYTES};
use crate::{
    repl::{ReplError, VersionedBytes, VersionedRecord, ap::ApReplicaEndpoint},
    storage::Bytes,
};

struct Count {
    bytes: usize,
    limit: usize,
}

pub(super) fn encoded_size<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<usize, InternalTransportError> {
    postcard::serialize_with_flavor(value, Count { bytes: 0, limit }).map_err(|error| {
        InternalTransportError::Codec(format!("encoded frame exceeds limit {limit}: {error}"))
    })
}

impl Flavor for Count {
    type Output = usize;

    fn try_push(&mut self, _byte: u8) -> postcard::Result<()> {
        self.try_extend(&[0])
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(self.bytes)
    }
}

struct Buffer {
    bytes: Bytes,
    limit: usize,
}

impl Flavor for Buffer {
    type Output = Bytes;

    fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
        self.try_extend(&[byte])
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        if next > self.bytes.capacity() {
            // Vec's geometric growth must not reserve beyond the configured
            // wire cap. A doubling policy still avoids per-byte reservations.
            let capacity = self
                .bytes
                .capacity()
                .max(64)
                .saturating_mul(2)
                .max(next)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| postcard::Error::SerializeBufferFull)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(self.bytes)
    }
}

pub(super) fn encode<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<Bytes, InternalTransportError> {
    let size = encoded_size(value, limit)?;
    postcard::serialize_with_flavor(
        value,
        Buffer {
            bytes: Vec::with_capacity(size),
            limit: size,
        },
    )
    .map_err(|error| {
        InternalTransportError::Codec(format!(
            "encoded frame exceeds limit {limit} or serialization failed: {error}"
        ))
    })
}

fn rejected() -> ReplError {
    ReplError::Rejected(
        "internal AP response exceeds its wire or decoded byte budget; use bounded range pages"
            .to_owned(),
    )
}

fn varint_length(mut value: usize) -> usize {
    let mut length = 1;
    while value >= 128 {
        value >>= 7;
        length += 1;
    }
    length
}

struct ResponseBudget {
    limit: usize,
    count: usize,
    payload: usize,
    allocated: usize,
}

impl ResponseBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            count: 0,
            payload: 0,
            allocated: std::mem::size_of::<InternalResponse>() * 2,
        }
    }

    fn add<T: Serialize>(&mut self, item: &T, allocation: usize) -> Result<(), ReplError> {
        let size = postcard::serialize_with_flavor(
            item,
            Count {
                bytes: 0,
                limit: self.limit,
            },
        )
        .map_err(|_| rejected())?;
        self.count = self.count.checked_add(1).ok_or_else(rejected)?;
        self.payload = self.payload.checked_add(size).ok_or_else(rejected)?;
        // Both enum discriminants occupy one byte; the vector count can grow.
        let wire_bytes = self
            .payload
            .checked_add(2 + varint_length(self.count))
            .ok_or_else(rejected)?;
        self.allocated = self
            .allocated
            .checked_add(allocation)
            .ok_or_else(rejected)?;
        if wire_bytes > self.limit || self.allocated > MAX_DECODED_BYTES {
            return Err(rejected());
        }
        Ok(())
    }
}

fn record_allocation(record: &VersionedRecord) -> usize {
    let mut bytes = std::mem::size_of::<VersionedRecord>()
        .saturating_mul(2)
        .saturating_add(record.table.len().saturating_mul(2))
        .saturating_add(record.key.len().saturating_mul(2))
        .saturating_add(
            record
                .versions
                .len()
                .saturating_mul(std::mem::size_of::<VersionedBytes>())
                .saturating_mul(2),
        );
    for version in &record.versions {
        bytes = bytes
            .saturating_add(version.value.as_ref().map_or(0, Vec::len).saturating_mul(2))
            .saturating_add(
                version
                    .clock
                    .entries()
                    .len()
                    .saturating_mul(512)
                    .saturating_mul(2),
            );
    }
    bytes
}

pub(super) fn records(
    endpoint: &dyn ApReplicaEndpoint,
    limit: usize,
) -> Result<Vec<VersionedRecord>, ReplError> {
    let mut budget = ResponseBudget::new(limit);
    let mut records = Vec::new();
    endpoint.visit_all_versions(&mut |record| {
        budget.add(&record, record_allocation(&record))?;
        records.push(record);
        Ok(true)
    })?;
    Ok(records)
}

pub(super) fn merkle(
    endpoint: &dyn ApReplicaEndpoint,
    prefix: &[u8],
    count: usize,
    limit: usize,
) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    let mut budget = ResponseBudget::new(limit);
    let mut records = Vec::new();
    endpoint.visit_merkle_range(prefix, count, &mut |key, value| {
        let allocation = std::mem::size_of::<(Bytes, Bytes)>()
            .saturating_mul(2)
            .saturating_add(key.len().saturating_mul(2))
            .saturating_add(value.len().saturating_mul(2));
        budget.add(&(&key, &value), allocation)?;
        records.push((key, value));
        Ok(true)
    })?;
    Ok(records)
}

#[cfg(test)]
mod tests;
