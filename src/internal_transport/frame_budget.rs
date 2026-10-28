//! Allocation-free validation of the internal postcard wire grammar. Container
//! counts are charged before serde can reserve their advertised capacities.
//! Keep this grammar synchronized with the transport's request/response types.

use super::{InternalRequest, InternalResponse, InternalTransportError};
use crate::repl::{CpClusterNodeStatus, RaftNode, VersionedBytes, VersionedRecord, VersionedWrite};

pub(super) const MAX_WIRE_BYTES: usize = 64 * 1024 * 1024;
const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

pub(super) trait FrameWire {
    fn preflight(bytes: &[u8]) -> Result<(), InternalTransportError>;
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
    allocated: usize,
}

impl Cursor<'_> {
    fn invalid(message: &'static str) -> InternalTransportError {
        InternalTransportError::Codec(message.to_owned())
    }

    fn take(&mut self, length: usize) -> Result<&[u8], InternalTransportError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| Self::invalid("internal frame length overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| Self::invalid("truncated internal frame"))?;
        self.position = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, InternalTransportError> {
        Ok(self.take(1)?[0])
    }

    fn integer(&mut self) -> Result<u64, InternalTransportError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(Self::invalid("internal frame integer overflow"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Self::invalid("invalid internal frame integer"))
    }

    fn boolean(&mut self) -> Result<bool, InternalTransportError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Self::invalid("invalid internal frame boolean/option")),
        }
    }

    fn variant(&mut self, count: u64) -> Result<u64, InternalTransportError> {
        let variant = self.integer()?;
        if variant >= count {
            return Err(Self::invalid("unknown internal frame variant"));
        }
        Ok(variant)
    }

    fn charge(&mut self, bytes: usize) -> Result<(), InternalTransportError> {
        self.allocated = self
            .allocated
            .checked_add(bytes)
            .filter(|total| *total <= MAX_DECODED_BYTES)
            .ok_or_else(|| Self::invalid("internal frame decoded allocation budget exceeded"))?;
        Ok(())
    }

    fn count(&mut self, element_bytes: usize) -> Result<usize, InternalTransportError> {
        let count = usize::try_from(self.integer()?)
            .map_err(|_| Self::invalid("internal frame container count overflow"))?;
        // Serde's cautious initial reservation can grow by doubling. Charge
        // twice the declared Rust element storage, including empty strings.
        let bytes = count
            .checked_mul(element_bytes)
            .and_then(|size| size.checked_mul(2))
            .ok_or_else(|| Self::invalid("internal frame container allocation overflow"))?;
        self.charge(bytes)?;
        Ok(count)
    }

    fn bytes(&mut self) -> Result<(), InternalTransportError> {
        let length = self.count(1)?;
        self.take(length)?;
        Ok(())
    }

    fn string(&mut self) -> Result<(), InternalTransportError> {
        let length = self.count(1)?;
        std::str::from_utf8(self.take(length)?)
            .map_err(|_| Self::invalid("invalid UTF-8 in internal frame"))?;
        Ok(())
    }

    fn integers(&mut self) -> Result<(), InternalTransportError> {
        for _ in 0..self.count(std::mem::size_of::<u64>())? {
            self.integer()?;
        }
        Ok(())
    }

    fn raft_nodes(&mut self) -> Result<(), InternalTransportError> {
        for _ in 0..self.count(std::mem::size_of::<RaftNode>())? {
            self.integer()?;
            self.string()?;
        }
        Ok(())
    }

    fn version(&mut self) -> Result<(), InternalTransportError> {
        if self.boolean()? {
            self.bytes()?;
        }
        // VectorClock is a BTreeMap<u64,u64>. A deliberately conservative node
        // allowance covers even a sparsely populated root/internal allocation.
        for _ in 0..self.count(512)? {
            self.integer()?;
            self.integer()?;
        }
        self.integer()?; // origin
        Ok(())
    }

    fn versions(&mut self) -> Result<(), InternalTransportError> {
        for _ in 0..self.count(std::mem::size_of::<VersionedBytes>())? {
            self.version()?;
        }
        Ok(())
    }

    fn raft_request(&mut self) -> Result<(), InternalTransportError> {
        let variant = self.variant(4)?;
        self.integer()?; // from
        self.integer()?; // term
        if variant == 3 {
            self.string()?;
            self.integer()?;
            self.boolean()?;
        }
        self.bytes()
    }

    fn admin_request(&mut self) -> Result<(), InternalTransportError> {
        match self.variant(3)? {
            0 => {
                self.raft_nodes()?;
                self.raft_nodes()?;
            }
            1 => {
                self.integer()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn ap_request(&mut self) -> Result<(), InternalTransportError> {
        match self.variant(5)? {
            0 => {
                for _ in 0..self.count(std::mem::size_of::<VersionedWrite>())? {
                    self.string()?;
                    self.bytes()?;
                    self.version()?;
                }
            }
            1 => {
                self.string()?;
                self.bytes()?;
            }
            2 => {
                for _ in 0..self.count(std::mem::size_of::<Vec<u8>>())? {
                    self.bytes()?;
                }
            }
            3 => {}
            _ => {
                self.bytes()?;
                self.integer()?;
            }
        }
        Ok(())
    }

    fn request(&mut self) -> Result<(), InternalTransportError> {
        match self.variant(5)? {
            0 => {
                self.integer()?;
            }
            1 => {
                self.integer()?;
                self.integer()?;
                self.integer()?;
            }
            2 => self.raft_request()?,
            3 => self.admin_request()?,
            _ => self.ap_request()?,
        }
        Ok(())
    }

    fn status(&mut self) -> Result<(), InternalTransportError> {
        self.integer()?; // node_id
        if self.boolean()? {
            self.integer()?;
        }
        self.integers()?; // voters
        self.integers()?; // learners
        for _ in 0..4 {
            self.integer()?;
        } // quorum, reachable, committed, applied
        for _ in 0..self.count(std::mem::size_of::<CpClusterNodeStatus>())? {
            self.integer()?;
            self.string()?;
            self.variant(3)?; // role
            self.boolean()?;
            self.boolean()?;
        }
        Ok(())
    }

    fn ap_response(&mut self) -> Result<(), InternalTransportError> {
        match self.variant(4)? {
            0 => {}
            1 => self.versions()?,
            2 => {
                for _ in 0..self.count(std::mem::size_of::<VersionedRecord>())? {
                    self.string()?;
                    self.bytes()?;
                    self.versions()?;
                }
            }
            _ => {
                for _ in 0..self.count(std::mem::size_of::<(Vec<u8>, Vec<u8>)>())? {
                    self.bytes()?;
                    self.bytes()?;
                }
            }
        }
        Ok(())
    }

    fn response(&mut self) -> Result<(), InternalTransportError> {
        match self.variant(6)? {
            0 => {
                self.integer()?;
            }
            1 => {}
            2 => {
                let variant = self.variant(2)?;
                self.integer()?;
                if variant == 0 {
                    self.bytes()?;
                } else {
                    self.string()?;
                }
            }
            3 => {
                if self.variant(2)? == 1 {
                    self.status()?;
                }
            }
            4 => self.ap_response()?,
            _ => self.string()?,
        }
        Ok(())
    }

    fn finish(self) -> Result<(), InternalTransportError> {
        if self.position != self.bytes.len() {
            return Err(Self::invalid("trailing bytes in internal frame"));
        }
        Ok(())
    }
}

impl FrameWire for InternalRequest {
    fn preflight(bytes: &[u8]) -> Result<(), InternalTransportError> {
        let mut cursor = Cursor {
            bytes,
            position: 0,
            allocated: std::mem::size_of::<Self>() * 2,
        };
        cursor.request()?;
        cursor.finish()
    }
}

impl FrameWire for InternalResponse {
    fn preflight(bytes: &[u8]) -> Result<(), InternalTransportError> {
        let mut cursor = Cursor {
            bytes,
            position: 0,
            allocated: std::mem::size_of::<Self>() * 2,
        };
        cursor.response()?;
        cursor.finish()
    }
}

#[cfg(test)]
mod tests;
