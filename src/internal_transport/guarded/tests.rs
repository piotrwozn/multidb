use std::{sync::Arc, time::Duration};

use tokio::{io::AsyncWriteExt, sync::oneshot};

use super::{PageBudget, read_response};
use crate::{
    internal_transport::{
        ApResponse, InternalResponse, InternalTransportError, InternalTransportServer,
        TcpApTransport, encode_frame, mtls_tests::TestPki,
    },
    repl::{ApTransport, Op, Replication, ap::ApDynamo},
    storage::{MemEngine, ReadBudget, StorageError},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn guarded_wire_length_is_rejected_before_allocating_the_advertised_buffer() -> TestResult {
    let budget = PageBudget::new(1024, Duration::from_secs(1))?;
    let (mut sender, mut receiver) = tokio::io::duplex(16);
    sender.write_u32(64 * 1024 * 1024).await?;
    let result = read_response(
        &mut receiver,
        64 * 1024 * 1024,
        Duration::from_secs(1),
        &budget,
    )
    .await;
    assert!(matches!(
        result,
        Err(InternalTransportError::Budget(StorageError::ResourceLimit(
            _
        )))
    ));
    assert_eq!(budget.remaining_bytes(), 1024);
    Ok(())
}

#[tokio::test]
async fn guarded_decoded_count_is_rejected_before_serde_allocation() -> TestResult {
    let budget = PageBudget::new(1024, Duration::from_secs(1))?;
    let (mut sender, mut receiver) = tokio::io::duplex(32);
    let mut bytes = vec![4, 2]; // Ap(Records) with a forged Vec length
    bytes.extend(postcard::to_allocvec(&1_000_000_000_usize)?);
    sender.write_u32(u32::try_from(bytes.len())?).await?;
    sender.write_all(&bytes).await?;
    let result = read_response(&mut receiver, 1024, Duration::from_secs(1), &budget).await;
    assert!(matches!(
        result,
        Err(InternalTransportError::Budget(StorageError::ResourceLimit(
            _
        )))
    ));
    assert_eq!(budget.remaining_bytes(), 1024);
    Ok(())
}

#[tokio::test]
async fn guarded_response_keeps_the_decode_reservation_until_its_value_drops() -> TestResult {
    let budget = PageBudget::new(8192, Duration::from_secs(1))?;
    let (mut sender, mut receiver) = tokio::io::duplex(1024);
    let response = InternalResponse::Ap(ApResponse::Merkle(vec![(vec![1; 32], vec![2; 32])]));
    let bytes = encode_frame(&response, 1024)?;
    sender.write_u32(u32::try_from(bytes.len())?).await?;
    sender.write_all(&bytes).await?;
    let owned = read_response(&mut receiver, 1024, Duration::from_secs(1), &budget).await?;
    assert_eq!(owned.response, response);
    assert!(budget.remaining_bytes() < 8192);
    drop(owned);
    assert_eq!(budget.remaining_bytes(), 8192);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_ap_page_uses_mtls_and_rejects_one_entry_over_the_peer_budget() -> TestResult {
    let pki = TestPki::new()?;
    let endpoint = Arc::new(ApDynamo::new_for_test(MemEngine::new(), 2));
    endpoint.propose(Op::Put {
        table: "t".to_owned(),
        key: b"k".to_vec(),
        value: vec![7; 1024],
    })?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let (shutdown, receiver) = oneshot::channel();
    let task = tokio::spawn(InternalTransportServer::serve_ap_until_shutdown(
        listener,
        TestPki::transport(pki.tls(2, &[1])),
        2,
        endpoint,
        async {
            let _ = receiver.await;
        },
    ));
    let transport = TcpApTransport::new(
        TestPki::transport(pki.tls(1, &[2])),
        std::collections::BTreeMap::from([(2, address)]),
    )?;
    let budget = PageBudget::new(64 * 1024, Duration::from_secs(3))?;
    let page = transport.read_range_page_guarded(2, "t", b"k", b"k\0", 1, &budget)?;
    assert_eq!(page.records.len(), 1);
    assert_eq!(
        page.records[0].versions[0].value.as_deref(),
        Some(vec![7; 1024].as_slice())
    );
    assert!(budget.remaining_bytes() < 64 * 1024);
    drop(page);
    assert_eq!(budget.remaining_bytes(), 64 * 1024);
    let small = PageBudget::new(2048, Duration::from_secs(3))?;
    assert!(matches!(
        transport.read_range_page_guarded(2, "t", b"k", b"k\0", 1, &small),
        Err(crate::repl::ReplError::Storage(
            StorageError::ResourceLimit(_)
        ))
    ));
    assert_eq!(small.remaining_bytes(), 2048);
    let _ = shutdown.send(());
    task.await??;
    Ok(())
}
