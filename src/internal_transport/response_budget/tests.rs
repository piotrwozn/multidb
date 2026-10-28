use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

use tokio::io::AsyncWrite;

use super::{ResponseBudget, merkle, records};
use crate::{
    internal_transport::{ApResponse, InternalResponse, encode_frame, write_response_and_shutdown},
    repl::{
        ReplError, VectorClock, VersionedBytes, VersionedRecord, VersionedWrite,
        ap::ApReplicaEndpoint,
    },
    storage::Bytes,
};

struct StreamingEndpoint {
    count: usize,
    visited: AtomicUsize,
}

impl StreamingEndpoint {
    fn new(count: usize) -> Self {
        Self {
            count,
            visited: AtomicUsize::new(0),
        }
    }
}

impl ApReplicaEndpoint for StreamingEndpoint {
    fn receive_batch(&self, _: &[VersionedWrite]) -> Result<(), ReplError> {
        Ok(())
    }
    fn read_versions(&self, _: &str, _: &[u8]) -> Result<Vec<VersionedBytes>, ReplError> {
        Ok(Vec::new())
    }
    fn read_records_by_version_keys(&self, _: &[Bytes]) -> Result<Vec<VersionedRecord>, ReplError> {
        Ok(Vec::new())
    }
    fn read_all_versions(&self) -> Result<Vec<VersionedRecord>, ReplError> {
        panic!("transport must use the streaming visitor");
    }
    fn read_merkle_range(&self, _: &[u8], _: usize) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        panic!("transport must use the streaming visitor");
    }
    fn visit_all_versions(
        &self,
        visitor: &mut dyn FnMut(VersionedRecord) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        for index in 0..self.count {
            self.visited.fetch_add(1, Ordering::Relaxed);
            if !visitor(VersionedRecord {
                table: "t".to_owned(),
                key: index.to_be_bytes().to_vec(),
                versions: vec![VersionedBytes {
                    value: Some(vec![7; 256]),
                    clock: VectorClock::new([(1, 1)]),
                    origin: 1,
                }],
            })? {
                break;
            }
        }
        Ok(())
    }
    fn visit_merkle_range(
        &self,
        _: &[u8],
        limit: usize,
        visitor: &mut dyn FnMut(Bytes, Bytes) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        for index in 0..self.count.min(limit) {
            self.visited.fetch_add(1, Ordering::Relaxed);
            if !visitor(index.to_be_bytes().to_vec(), vec![7; 32])? {
                break;
            }
        }
        Ok(())
    }
}

#[test]
fn oversized_ap_responses_stop_native_visitors_before_retaining_the_full_scan() {
    let endpoint = StreamingEndpoint::new(1_000_000);
    assert!(matches!(
        records(&endpoint, 1024),
        Err(ReplError::Rejected(_))
    ));
    assert!(endpoint.visited.load(Ordering::Relaxed) < 10);
    endpoint.visited.store(0, Ordering::Relaxed);
    assert!(matches!(
        merkle(&endpoint, &[], usize::MAX, 1024),
        Err(ReplError::Rejected(_))
    ));
    assert!(endpoint.visited.load(Ordering::Relaxed) < 30);
}

#[test]
fn response_budget_preserves_exact_full_results_up_to_the_wire_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoint = StreamingEndpoint::new(128); // includes the Vec-count varint boundary
    let all = records(&endpoint, 1024 * 1024)?;
    let response = InternalResponse::Ap(ApResponse::Records(all.clone()));
    let length = postcard::to_allocvec(&response)?.len();
    assert_eq!(records(&endpoint, length)?, all);
    assert!(matches!(
        records(&endpoint, length - 1),
        Err(ReplError::Rejected(_))
    ));
    let all = merkle(&endpoint, &[], usize::MAX, 1024 * 1024)?;
    let length =
        postcard::to_allocvec(&InternalResponse::Ap(ApResponse::Merkle(all.clone())))?.len();
    assert_eq!(merkle(&endpoint, &[], usize::MAX, length)?, all);
    assert!(matches!(
        merkle(&endpoint, &[], usize::MAX, length - 1),
        Err(ReplError::Rejected(_))
    ));
    Ok(())
}

#[test]
fn decoded_response_budget_rejects_allocation_overflow() {
    let mut budget = ResponseBudget::new(1024);
    assert!(matches!(
        budget.add(&0_u8, usize::MAX),
        Err(ReplError::Rejected(_))
    ));
}

#[test]
fn encoder_stops_before_visiting_a_huge_advertised_output() {
    struct HugeOutput(AtomicUsize);
    impl serde::Serialize for HugeOutput {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            let mut sequence = serializer.serialize_seq(Some(1_000_000_000))?;
            for _ in 0..1_000_000_000 {
                self.0.fetch_add(1, Ordering::Relaxed);
                sequence.serialize_element(&0_u8)?;
            }
            sequence.end()
        }
    }
    let value = HugeOutput(AtomicUsize::new(0));
    assert!(encode_frame(&value, 1024).is_err());
    assert!(value.0.load(Ordering::Relaxed) <= 1024);
}

struct StalledWriter {
    stall_shutdown: bool,
}

impl AsyncWrite for StalledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.stall_shutdown {
            Poll::Ready(Ok(bytes.len()))
        } else {
            Poll::Pending
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn stalled_response_writes_and_shutdowns_release_the_connection_deadline() {
    for stall_shutdown in [false, true] {
        let mut stream = StalledWriter { stall_shutdown };
        let result =
            write_response_and_shutdown(&mut stream, 1024, 1, &InternalResponse::FlowAck).await;
        assert!(
            matches!(result, Err(crate::internal_transport::InternalTransportError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
        );
    }
}
