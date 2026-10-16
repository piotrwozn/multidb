pub type Bytes = Vec<u8>;

mod any;
mod compressed;
#[cfg(test)]
mod conformance;
mod encrypted;
mod error;
mod memory;
mod redb;
mod sim;
mod traits;

pub use any::{AnyEngine, AnyReadTxn, AnyWriteTxn, EngineKind};
pub(crate) use compressed::is_framed;
pub use compressed::{
    CompressedEngine, CompressedReadTxn, CompressedWriteTxn, decode_stored_value,
    encode_stored_value,
};
pub use encrypted::{
    ConfiguredKeyProvider, CryptoShredReport, EncryptedEngine, EncryptedReadTxn, EncryptedWriteTxn,
    EnvelopeKeyProvider, FileKeyProvider, KekProvider, KeyProvider, KeyRotationPlan, LocalFileKms,
    ProtectedKey, StaticKeyProvider, VaultKekProvider,
};
pub use error::StorageError;
pub use memory::MemEngine;
pub(crate) use redb::ExistingRedbOpen;
pub use redb::RedbEngine;
pub use sim::{Fault, FaultPlan, FaultPoint, ScheduledFault, SimReadTxn, SimStorage, SimWriteTxn};
pub use traits::{RangeIter, ReadTransaction, StorageEngine, WriteTransaction};
