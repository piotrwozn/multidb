use super::*;
use crate::{
    internal_transport::{
        ApRequest, ApResponse, ClusterAdminRequest, ClusterAdminResponse, RaftRequest, RaftResponse,
    },
    repl::{CpClusterRole, CpClusterStatus, VectorClock},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn version() -> VersionedBytes {
    VersionedBytes {
        value: Some(vec![0, 1, 255]),
        clock: VectorClock::new([(1, 7), (2, 9)]),
        origin: 1,
    }
}

fn status() -> CpClusterStatus {
    CpClusterStatus {
        node_id: 1,
        leader_id: Some(1),
        voters: vec![1, 2],
        learners: vec![3],
        quorum_size: 2,
        reachable_voters: 2,
        last_committed: 127,
        last_applied: 126,
        nodes: [
            CpClusterRole::Leader,
            CpClusterRole::Follower,
            CpClusterRole::Learner,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, role)| CpClusterNodeStatus {
            id: u64::try_from(index).unwrap_or(0) + 1,
            addr: "127.0.0.1:5432".to_owned(),
            role,
            voter: index != 2,
            reachable: true,
        })
        .collect(),
    }
}

fn check<
    T: serde::Serialize + serde::de::DeserializeOwned + FrameWire + PartialEq + std::fmt::Debug,
>(
    value: &T,
) -> TestResult {
    let bytes = postcard::to_allocvec(value)?;
    T::preflight(&bytes)?;
    assert_eq!(postcard::from_bytes::<T>(&bytes)?, *value);
    for end in 0..bytes.len() {
        assert!(
            T::preflight(&bytes[..end]).is_err(),
            "truncated prefix {end} must fail"
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(T::preflight(&trailing).is_err());
    Ok(())
}

#[test]
fn preflight_matches_every_request_variant_and_rejects_truncation() -> TestResult {
    let requests = [
        InternalRequest::HealthPing { from: 1 },
        InternalRequest::FlowAck {
            from: 2,
            frames: 128,
            bytes: 65_536,
        },
        InternalRequest::Raft(RaftRequest::AppendEntries {
            from: 1,
            term: 2,
            payload: vec![1, 2],
        }),
        InternalRequest::Raft(RaftRequest::Vote {
            from: 1,
            term: 2,
            payload: vec![3, 4],
        }),
        InternalRequest::Raft(RaftRequest::PreVote {
            from: 1,
            term: 2,
            payload: Vec::new(),
        }),
        InternalRequest::Raft(RaftRequest::InstallSnapshot {
            from: 1,
            term: 2,
            snapshot_id: "snapshot-ą".to_owned(),
            offset: 128,
            done: true,
            chunk: vec![7; 256],
        }),
        InternalRequest::ClusterAdmin(ClusterAdminRequest::ChangeMembership {
            voters: vec![RaftNode::new(1, "127.0.0.1:5001")],
            learners: vec![RaftNode::new(2, "127.0.0.1:5002")],
        }),
        InternalRequest::ClusterAdmin(ClusterAdminRequest::TransferLeader { target: 2 }),
        InternalRequest::ClusterAdmin(ClusterAdminRequest::Status),
        InternalRequest::Ap(ApRequest::SendBatch {
            writes: vec![VersionedWrite {
                table: "t".to_owned(),
                key: vec![8],
                version: version(),
            }],
        }),
        InternalRequest::Ap(ApRequest::ReadVersions {
            table: "t".to_owned(),
            key: vec![8],
        }),
        InternalRequest::Ap(ApRequest::ReadRecords {
            version_keys: vec![vec![0, 1], Vec::new()],
        }),
        InternalRequest::Ap(ApRequest::ReadAllVersions),
        InternalRequest::Ap(ApRequest::ReadMerkle {
            prefix: vec![9],
            limit: 1024,
        }),
    ];
    for value in requests {
        check(&value)?;
    }
    Ok(())
}

#[test]
fn preflight_matches_every_response_variant_and_rejects_truncation() -> TestResult {
    let responses = [
        InternalResponse::HealthPong { node: 1 },
        InternalResponse::FlowAck,
        InternalResponse::Raft(RaftResponse::Accepted {
            term: 2,
            payload: vec![1, 2],
        }),
        InternalResponse::Raft(RaftResponse::Rejected {
            term: 2,
            reason: "żółw".to_owned(),
        }),
        InternalResponse::ClusterAdmin(ClusterAdminResponse::Accepted),
        InternalResponse::ClusterAdmin(ClusterAdminResponse::Status(status())),
        InternalResponse::Ap(ApResponse::Ack),
        InternalResponse::Ap(ApResponse::Versions(vec![
            version(),
            VersionedBytes {
                value: None,
                clock: VectorClock::default(),
                origin: 2,
            },
        ])),
        InternalResponse::Ap(ApResponse::Records(vec![VersionedRecord {
            table: "t".to_owned(),
            key: vec![3],
            versions: vec![version()],
        }])),
        InternalResponse::Ap(ApResponse::Merkle(vec![(vec![4], vec![5])])),
        InternalResponse::Error("failure".to_owned()),
    ];
    for value in responses {
        check(&value)?;
    }
    let mut without_leader = status();
    without_leader.leader_id = None;
    check(&InternalResponse::ClusterAdmin(
        ClusterAdminResponse::Status(without_leader),
    ))?;
    Ok(())
}

#[test]
fn tiny_frames_cannot_advertise_massive_container_or_string_allocations() -> TestResult {
    let huge = postcard::to_allocvec(&u64::MAX)?;
    // InternalRequest::Ap -> SendBatch -> Vec<VersionedWrite>.
    let mut writes = vec![4, 0];
    writes.extend_from_slice(&huge);
    assert!(InternalRequest::preflight(&writes).is_err());
    // One empty write: table, key, absent value; then huge VectorClock map.
    let mut clock = vec![4, 0, 1, 0, 0, 0];
    clock.extend_from_slice(&huge);
    assert!(InternalRequest::preflight(&clock).is_err());
    // InternalResponse::Ap -> Records -> one record with an enormous versions Vec.
    let mut versions = vec![4, 2, 1, 0, 0];
    versions.extend_from_slice(&huge);
    assert!(InternalResponse::preflight(&versions).is_err());
    let mut string = vec![5];
    string.extend_from_slice(&huge);
    assert!(InternalResponse::preflight(&string).is_err());
    let mut status = status();
    status.nodes.clear();
    let mut nodes = postcard::to_allocvec(&InternalResponse::ClusterAdmin(
        ClusterAdminResponse::Status(status),
    ))?;
    assert_eq!(nodes.pop(), Some(0));
    nodes.extend_from_slice(&huge);
    assert!(InternalResponse::preflight(&nodes).is_err());
    Ok(())
}

#[test]
fn compact_small_elements_are_charged_for_their_decoded_rust_storage() -> TestResult {
    // This count alone is rejected before attempting to walk or deserialize its
    // million empty records, despite a frame of fewer than ten bytes.
    let mut writes = vec![4, 0];
    writes.extend(postcard::to_allocvec(&1_000_000_u64)?);
    let error = InternalRequest::preflight(&writes)
        .err()
        .ok_or("must reject allocation")?;
    assert!(error.to_string().contains("allocation"));
    // Similarly a list of byte buffers must account for every Vec header.
    let mut keys = vec![4, 2];
    keys.extend(postcard::to_allocvec(&10_000_000_u64)?);
    assert!(InternalRequest::preflight(&keys).is_err());
    Ok(())
}

#[tokio::test]
async fn network_decoder_runs_preflight_before_serde_container_allocation() -> TestResult {
    use tokio::io::AsyncWriteExt;
    let (mut sender, mut receiver) = tokio::io::duplex(128);
    let mut bytes = vec![4, 0];
    bytes.extend(postcard::to_allocvec(&1_000_000_000_u64)?);
    sender.write_u32(u32::try_from(bytes.len())?).await?;
    sender.write_all(&bytes).await?;
    let result = super::super::read_frame::<_, InternalRequest>(&mut receiver, 128, 1000).await;
    assert!(
        matches!(result, Err(InternalTransportError::Codec(message)) if message.contains("allocation"))
    );
    Ok(())
}
