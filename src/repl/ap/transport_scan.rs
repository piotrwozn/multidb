//! Native transport visitors stop before collecting the whole replica.

use super::{AP_MERKLE_TABLE, AP_VERSIONS_TABLE, ApDynamo, read_record_from_encoded_key};
use crate::{
    repl::{ReplError, VersionedRecord},
    storage::{Bytes, ReadTransaction, StorageEngine},
};

impl<S: StorageEngine> ApDynamo<S> {
    pub(super) fn visit_local_versions(
        &self,
        visitor: &mut dyn FnMut(VersionedRecord) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let transaction = self.storage.begin_read()?;
        for entry in transaction.range(AP_VERSIONS_TABLE, &[], &[])? {
            let (key, value) = entry?;
            if !visitor(read_record_from_encoded_key(&key, &value)?)? {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn visit_local_merkle(
        &self,
        prefix: &[u8],
        limit: usize,
        visitor: &mut dyn FnMut(Bytes, Bytes) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let transaction = self.storage.begin_read()?;
        for entry in transaction.range(AP_MERKLE_TABLE, prefix, &[])?.take(limit) {
            let (key, value) = entry?;
            if !key.starts_with(prefix) || !visitor(key, value)? {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        repl::{
            ReplError, VectorClock, VersionedBytes, VersionedWrite,
            ap::{AP_VERSIONS_TABLE, ApDynamo, ApReplicaEndpoint, version_key},
        },
        storage::{MemEngine, StorageEngine, WriteTransaction},
    };

    #[test]
    fn native_version_visitor_stops_before_decoding_a_later_corrupt_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = MemEngine::new();
        let node = ApDynamo::new_for_test(storage, 1);
        node.receive_batch(&[VersionedWrite {
            table: "t".to_owned(),
            key: b"a".to_vec(),
            version: VersionedBytes {
                value: Some(b"v".to_vec()),
                clock: VectorClock::new([(1, 1)]),
                origin: 1,
            },
        }])?;
        let mut write = node.storage().begin_write()?;
        write.put(AP_VERSIONS_TABLE, &version_key("t", b"b")?, b"not-json")?;
        write.commit()?;
        let mut seen = Vec::new();
        node.visit_all_versions(&mut |record| {
            seen.push(record.key);
            Ok(false)
        })?;
        assert_eq!(seen, vec![b"a".to_vec()]);
        assert!(matches!(
            node.visit_all_versions(&mut |_| Ok(true)),
            Err(ReplError::Storage(_))
        ));
        Ok(())
    }

    #[test]
    fn native_merkle_visitor_respects_prefix_and_zero_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = MemEngine::new();
        let node = ApDynamo::new_for_test(storage, 1);
        let mut write = node.storage().begin_write()?;
        for key in [b"aa", b"ab", b"ba"] {
            write.put(super::AP_MERKLE_TABLE, key, b"hash")?;
        }
        write.commit()?;
        let mut seen = Vec::new();
        node.visit_merkle_range(b"a", usize::MAX, &mut |key, _| {
            seen.push(key);
            Ok(true)
        })?;
        assert_eq!(seen, vec![b"aa".to_vec(), b"ab".to_vec()]);
        let mut visited = false;
        node.visit_merkle_range(&[], 0, &mut |_, _| {
            visited = true;
            Ok(true)
        })?;
        assert!(!visited);
        Ok(())
    }
}
