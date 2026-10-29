//! A real three-process, durable-storage CP smoke gate using admitted mTLS
//! identities. It uses loopback, not a claim of separate-host qualification.

use std::{
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
    phase30::RaftRuntimeConfig,
    repl::{
        CpClusterConfig, CpClusterStatus, NodeId, Op, RaftNode, ReadConsistency, Replication,
        cluster_status, shutdown_cp_cluster, start_cp_cluster,
    },
    storage::RedbEngine,
};

use super::mtls_tests::TestPki;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const CHILD_FILTER: &str = "internal_transport::process_smoke::mtls_node_process";

#[derive(Deserialize, Serialize)]
enum NodeCommand {
    Status,
    Put { key: Vec<u8>, value: Vec<u8> },
    Read { key: Vec<u8>, strong: bool },
}

#[derive(Default, Deserialize, Serialize)]
struct NodeResponse {
    status: Option<CpClusterStatus>,
    value: Option<Vec<u8>>,
    error: Option<String>,
}

#[test]
#[ignore = "subprocess entrypoint; run mtls_three_process_failover_and_quorum_smoke instead"]
fn mtls_node_process() -> TestResult {
    let Some(directory) = std::env::var_os("MULTIDB_INTERNAL_SMOKE_NODE") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    let config: CpClusterConfig =
        serde_json::from_slice(&std::fs::read(directory.join("config.json"))?)?;
    let handle = start_cp_cluster(
        RedbEngine::open_high_durability(directory.join("data.redb"))?,
        config,
    )?;
    publish(&directory.join("ready"), b"ready")?;
    let deadline = Instant::now() + Duration::from_secs(150);
    while Instant::now() < deadline {
        let request_path = directory.join("request.json");
        let request = match std::fs::read(&request_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        std::fs::remove_file(request_path)?;
        let request: NodeCommand = serde_json::from_slice(&request)?;
        let mut response = NodeResponse::default();
        let result = match request {
            NodeCommand::Status => {
                cluster_status(&handle).map(|status| response.status = Some(status))
            }
            NodeCommand::Put { key, value } => handle.replication().propose(Op::Put {
                table: "smoke".to_owned(),
                key,
                value,
            }),
            NodeCommand::Read { key, strong } => handle
                .replication()
                .read(
                    "smoke",
                    &key,
                    if strong {
                        ReadConsistency::Strong
                    } else {
                        ReadConsistency::Eventual
                    },
                )
                .map(|value| response.value = value),
        };
        if let Err(error) = result {
            response.error = Some(error.to_string());
        }
        publish(
            &directory.join("response.json"),
            &serde_json::to_vec(&response)?,
        )?;
    }
    shutdown_cp_cluster(handle)?;
    Err("smoke worker watchdog expired".into())
}

fn publish(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_extension("pending");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)
}

struct NodeProcess {
    node: NodeId,
    directory: PathBuf,
    child: Option<Child>,
}

impl NodeProcess {
    fn spawn(node: NodeId, directory: PathBuf, config: &CpClusterConfig) -> TestResult<Self> {
        std::fs::create_dir_all(&directory)?;
        std::fs::write(directory.join("config.json"), serde_json::to_vec(config)?)?;
        let mut process = Self {
            node,
            directory,
            child: None,
        };
        process.restart()?;
        Ok(process)
    }

    fn restart(&mut self) -> TestResult {
        for name in ["ready", "request.json", "response.json"] {
            match std::fs::remove_file(self.directory.join(name)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let log = File::create(self.directory.join("process.log"))?;
        self.child = Some(
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    CHILD_FILTER,
                    "--ignored",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("MULTIDB_INTERNAL_SMOKE_NODE", &self.directory)
                .stdin(Stdio::null())
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                .spawn()?,
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.directory.join("ready").exists() {
            self.check_alive()?;
            if Instant::now() >= deadline {
                return Err(format!("node {} failed to start: {}", self.node, self.log()).into());
            }
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn check_alive(&mut self) -> TestResult {
        if let Some(status) = self.child.as_mut().ok_or("node is stopped")?.try_wait()? {
            return Err(format!("node {} exited with {status}: {}", self.node, self.log()).into());
        }
        Ok(())
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.directory.join("process.log")).unwrap_or_default()
    }

    fn stop(&mut self) -> TestResult {
        if let Some(mut child) = self.child.take() {
            // The child may exit between the kill and wait calls. Always reap
            // it, including when kill reports that the process already exited.
            let _ = child.kill();
            child.wait()?;
        }
        Ok(())
    }

    fn request(&mut self, request: &NodeCommand) -> TestResult<NodeResponse> {
        self.check_alive()?;
        publish(
            &self.directory.join("request.json"),
            &serde_json::to_vec(request)?,
        )?;
        let response = self.directory.join("response.json");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match std::fs::read(&response) {
                Ok(bytes) => {
                    std::fs::remove_file(&response)?;
                    return Ok(serde_json::from_slice(&bytes)?);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            self.check_alive()?;
            if Instant::now() >= deadline {
                return Err(
                    format!("node {} response timed out: {}", self.node, self.log()).into(),
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn commit_on_leader(nodes: &mut [NodeProcess], key: &[u8], value: &[u8]) -> TestResult<usize> {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        for (index, node) in nodes
            .iter_mut()
            .enumerate()
            .filter(|(_, node)| node.child.is_some())
        {
            let response = node.request(&NodeCommand::Status)?;
            if response
                .status
                .is_some_and(|status| status.leader_id == Some(node.node))
            {
                let response = node.request(&NodeCommand::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                })?;
                if response.error.is_none() {
                    return Ok(index);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err("secure subprocess cluster failed to commit with a live majority".into());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_all_values(nodes: &mut [NodeProcess], key: &[u8], value: &[u8]) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut all_equal = true;
        for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
            let response = node.request(&NodeCommand::Read {
                key: key.to_vec(),
                strong: false,
            })?;
            all_equal &= response.error.is_none() && response.value.as_deref() == Some(value);
        }
        if all_equal {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("secure subprocess cluster lost an acknowledged value".into());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
#[ignore = "three real processes; run through scripts/cluster-smoke.ps1"]
fn mtls_three_process_failover_and_quorum_smoke() -> TestResult {
    let pki = TestPki::new()?;
    let directory = tempfile::tempdir()?;
    // Reserve all addresses together to avoid accidentally choosing the same
    // ephemeral port for two members, then release immediately before startup.
    let listeners = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<Result<Vec<_>, _>>()?;
    let voters = listeners
        .iter()
        .enumerate()
        .map(|(index, listener)| {
            Ok(RaftNode::new(
                u64::try_from(index)? + 1,
                listener.local_addr()?.to_string(),
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    drop(listeners);
    let mut nodes = Vec::new();
    for voter in &voters {
        let peers = voters
            .iter()
            .filter(|peer| peer.id != voter.id)
            .map(|peer| peer.id)
            .collect::<Vec<_>>();
        let mut transport = TestPki::transport(pki.tls(voter.id, &peers));
        transport.bind_addr.clone_from(&voter.addr);
        transport.connect_timeout_ms = 300;
        transport.request_timeout_ms = 2_000;
        let config = CpClusterConfig::new(voter.id, voter.addr.clone(), voters.clone())
            .with_transport(transport)
            .with_runtime(RaftRuntimeConfig {
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                heartbeat_interval_ms: 50,
                snapshot_threshold: 256,
                max_payload_entries: 64,
                install_snapshot_timeout_ms: 5_000,
            });
        nodes.push(NodeProcess::spawn(
            voter.id,
            directory.path().join(voter.id.to_string()),
            &config,
        )?);
    }

    let first_leader = commit_on_leader(&mut nodes, b"before-crash", b"durable")?;
    wait_all_values(&mut nodes, b"before-crash", b"durable")?;
    nodes[first_leader].stop()?;
    let next_leader = commit_on_leader(&mut nodes, b"after-crash", b"majority")?;
    assert_ne!(first_leader, next_leader);
    wait_all_values(&mut nodes, b"after-crash", b"majority")?;

    let second_stopped = (0..nodes.len())
        .find(|index| *index != first_leader && *index != next_leader)
        .ok_or("missing third node")?;
    nodes[second_stopped].stop()?;
    let response = nodes[next_leader].request(&NodeCommand::Put {
        key: b"isolated".to_vec(),
        value: b"must-not-ack".to_vec(),
    })?;
    assert!(
        response.error.is_some(),
        "minority node acknowledged a write"
    );
    let response = nodes[next_leader].request(&NodeCommand::Read {
        key: b"before-crash".to_vec(),
        strong: true,
    })?;
    assert!(
        response.error.is_some(),
        "minority node served a strong read"
    );

    nodes[first_leader].restart()?;
    nodes[second_stopped].restart()?;
    commit_on_leader(&mut nodes, b"after-reopen", b"restored")?;
    wait_all_values(&mut nodes, b"before-crash", b"durable")?;
    wait_all_values(&mut nodes, b"after-crash", b"majority")?;
    wait_all_values(&mut nodes, b"after-reopen", b"restored")?;
    // An unacknowledged timed-out write has an unknown outcome after quorum
    // returns; do not mistake timeout for a guaranteed rollback.
    Ok(())
}
