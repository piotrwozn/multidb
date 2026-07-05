use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    observability,
    storage::{StorageEngine, WriteTransaction},
};

use super::{ApDynamo, NodeId, RaftNode, ReplError, cp::CpRaft as CpRaftBackend};

pub const HEALING_STATE_TABLE: &str = "__healing_state";
pub const HEALING_EVENTS_TABLE: &str = "__healing_events";

const LAST_EVENT_KEY: &[u8] = b"last_event";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HealthState {
    Healthy,
    Suspect,
    Dead,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthConfig {
    pub sample_interval: Duration,
    pub suspect_after: u32,
    pub failure_timeout: Duration,
    pub recovery_samples: u32,
    pub cooldown: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealingPolicy {
    pub enabled: bool,
    pub max_parallel_resyncs: usize,
    pub promote_only_when_caught_up: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeHealth {
    pub node_id: NodeId,
    pub state: HealthState,
    pub failed_samples: u32,
    pub healthy_samples: u32,
    pub last_seen: Option<SystemTime>,
    pub state_since: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HealingAction {
    MarkSuspect(NodeId),
    MarkDead(NodeId),
    EvictDeadNode(NodeId),
    AddReplacementLearner(RaftNode),
    ResyncNode(NodeId),
    PromoteLearner(NodeId),
    DeliverApHints(NodeId),
    RunApAntiEntropy(NodeId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealingMode {
    Cp,
    Ap,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HealthView {
    states: BTreeMap<NodeId, HealthState>,
}

pub trait HealingBackend: Send + Sync {
    fn mode(&self) -> HealingMode;
    fn local_node_id(&self) -> NodeId;
    fn known_nodes(&self) -> Vec<NodeId>;
    fn has_write_quorum(&self, health: &HealthView) -> bool;

    /// Evicts a dead CP voter.
    /// # Errors
    /// Fails when quorum is unavailable or the backend rejects membership changes.
    fn evict_dead_node(&self, node: NodeId) -> Result<(), ReplError>;

    /// Adds a replacement CP node as learner.
    /// # Errors
    /// Fails when quorum is unavailable or the backend rejects membership changes.
    fn add_replacement_learner(&self, node: RaftNode) -> Result<(), ReplError>;

    /// Resynchronizes a replacement node.
    /// # Errors
    /// Fails when the backend cannot resync the node.
    fn resync_node(&self, node: NodeId) -> Result<(), ReplError>;

    /// Promotes a caught-up learner to voter.
    /// # Errors
    /// Fails when quorum is unavailable or the backend rejects membership changes.
    fn promote_learner(&self, node: NodeId) -> Result<(), ReplError>;

    /// Delivers AP hinted handoff records.
    /// # Errors
    /// Fails when the AP backend rejects hint delivery.
    fn deliver_ap_hints(&self, node: NodeId) -> Result<(), ReplError>;

    /// Runs one AP anti-entropy round.
    /// # Errors
    /// Fails when the AP backend cannot synchronize with the peer.
    fn run_ap_anti_entropy(&self, node: NodeId) -> Result<(), ReplError>;

    /// Persists a local healing audit event.
    /// # Errors
    /// Fails when local metadata storage rejects the audit write.
    fn record_healing_action(
        &self,
        action: &HealingAction,
        at: SystemTime,
    ) -> Result<(), ReplError>;
}

pub trait HealthProbe: Send + Sync {
    /// Checks whether a node responds.
    /// # Errors
    /// Fails when the probe cannot reach the health source.
    fn check(&self, node: NodeId) -> Result<bool, ReplError>;
}

#[derive(Clone, Debug, Default)]
pub struct ManualHealthProbe {
    states: Arc<Mutex<BTreeMap<NodeId, bool>>>,
}

pub struct SelfHealingController<B, P> {
    pub backend: B,
    pub probe: P,
    pub config: HealthConfig,
    pub policy: HealingPolicy,
    health: BTreeMap<NodeId, NodeHealth>,
    replacement_pool: VecDeque<RaftNode>,
    planned_dead: BTreeSet<NodeId>,
}

pub struct SelfHealingHandle {
    shutdown: Option<mpsc::Sender<()>>,
    join: Option<thread::JoinHandle<Result<(), ReplError>>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct HealingEvent {
    at_unix_millis: u64,
    action: HealingAction,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_secs(1),
            suspect_after: 3,
            failure_timeout: Duration::from_secs(30),
            recovery_samples: 1,
            cooldown: Duration::from_secs(60),
        }
    }
}

impl HealthConfig {
    /// Validates health-check timing.
    /// # Errors
    /// Fails when counters are zero or failure detection is too aggressive.
    pub fn validate(&self) -> Result<(), ReplError> {
        if self.sample_interval.is_zero() {
            return Err(ReplError::Transport(
                "health sample interval must be greater than zero".to_owned(),
            ));
        }

        if self.suspect_after == 0 {
            return Err(ReplError::Transport(
                "health suspect_after must be greater than zero".to_owned(),
            ));
        }

        if self.recovery_samples == 0 {
            return Err(ReplError::Transport(
                "health recovery_samples must be greater than zero".to_owned(),
            ));
        }

        if self.failure_timeout <= self.sample_interval {
            return Err(ReplError::Transport(
                "health failure_timeout must be greater than sample_interval".to_owned(),
            ));
        }

        Ok(())
    }

    /// Validates health-check timing against a Raft election timeout.
    /// # Errors
    /// Fails when failure detection can race leader election.
    pub fn validate_against_election_timeout(
        &self,
        election_timeout: Duration,
    ) -> Result<(), ReplError> {
        self.validate()?;
        if self.failure_timeout <= election_timeout {
            return Err(ReplError::Transport(
                "health failure_timeout must be greater than election_timeout".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for HealingPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_parallel_resyncs: 1,
            promote_only_when_caught_up: true,
        }
    }
}

impl NodeHealth {
    #[must_use]
    pub fn new(node_id: NodeId, now: SystemTime) -> Self {
        Self {
            node_id,
            state: HealthState::Healthy,
            failed_samples: 0,
            healthy_samples: 0,
            last_seen: None,
            state_since: now,
        }
    }
}

impl HealthView {
    #[must_use]
    pub fn new(states: impl IntoIterator<Item = (NodeId, HealthState)>) -> Self {
        Self {
            states: states.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn state(&self, node: NodeId) -> HealthState {
        self.states
            .get(&node)
            .copied()
            .unwrap_or(HealthState::Healthy)
    }

    #[must_use]
    pub fn is_live(&self, node: NodeId) -> bool {
        self.state(node) != HealthState::Dead
    }

    #[must_use]
    pub fn states(&self) -> &BTreeMap<NodeId, HealthState> {
        &self.states
    }
}

impl SelfHealingHandle {
    /// Requests shutdown and waits for the worker thread.
    /// # Errors
    /// Fails if the worker thread panicked or returned a replication error.
    pub fn shutdown(mut self) -> Result<(), ReplError> {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }
        if let Some(join) = self.join.take() {
            return join
                .join()
                .map_err(|_| ReplError::Transport("self-healing worker panicked".to_owned()))?;
        }
        Ok(())
    }
}

impl Drop for SelfHealingHandle {
    fn drop(&mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl<F> HealthProbe for F
where
    F: Fn(NodeId) -> Result<bool, ReplError> + Send + Sync,
{
    fn check(&self, node: NodeId) -> Result<bool, ReplError> {
        self(node)
    }
}

impl ManualHealthProbe {
    #[must_use]
    pub fn new(states: impl IntoIterator<Item = (NodeId, bool)>) -> Self {
        Self {
            states: Arc::new(Mutex::new(states.into_iter().collect())),
        }
    }

    /// Sets the next health result for a node.
    /// # Errors
    /// Fails when the probe state lock is poisoned.
    pub fn set(&self, node: NodeId, healthy: bool) -> Result<(), ReplError> {
        self.states
            .lock()
            .map_err(|_| ReplError::Transport("health probe lock poisoned".to_owned()))?
            .insert(node, healthy);
        Ok(())
    }
}

impl HealthProbe for ManualHealthProbe {
    fn check(&self, node: NodeId) -> Result<bool, ReplError> {
        Ok(self
            .states
            .lock()
            .map_err(|_| ReplError::Transport("health probe lock poisoned".to_owned()))?
            .get(&node)
            .copied()
            .unwrap_or(true))
    }
}

impl<B, P> SelfHealingController<B, P>
where
    B: HealingBackend,
    P: HealthProbe,
{
    /// Creates a self-healing controller.
    /// # Errors
    /// Fails when the health configuration is invalid.
    pub fn new(
        backend: B,
        probe: P,
        config: HealthConfig,
        policy: HealingPolicy,
    ) -> Result<Self, ReplError> {
        config.validate()?;
        Ok(Self {
            backend,
            probe,
            config,
            policy,
            health: BTreeMap::new(),
            replacement_pool: VecDeque::new(),
            planned_dead: BTreeSet::new(),
        })
    }

    #[must_use]
    pub fn with_replacements(mut self, replacements: impl IntoIterator<Item = RaftNode>) -> Self {
        self.replacement_pool = replacements.into_iter().collect();
        self
    }

    #[must_use]
    pub fn health(&self, node: NodeId) -> Option<&NodeHealth> {
        self.health.get(&node)
    }

    #[must_use]
    pub fn health_view(&self) -> HealthView {
        HealthView::new(
            self.health
                .iter()
                .map(|(node_id, health)| (*node_id, health.state)),
        )
    }

    /// Runs one health-check cycle using the system clock.
    /// # Errors
    /// Fails when probes or healing actions fail.
    pub fn tick(&mut self) -> Result<Vec<HealingAction>, ReplError> {
        self.tick_at(SystemTime::now())
    }

    /// Starts the controller as a managed background worker.
    ///
    #[must_use]
    pub fn start_background(mut self) -> SelfHealingHandle
    where
        B: 'static,
        P: 'static,
    {
        let interval = self.config.sample_interval;
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let join = thread::spawn(move || {
            loop {
                match shutdown_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = self.tick() {
                            observability::record_replication_error(error.metric_kind());
                            tracing::warn!(error = %error, "self-healing tick failed");
                        }
                    }
                }
            }
        });

        SelfHealingHandle {
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    /// Runs one deterministic health-check cycle.
    /// # Errors
    /// Fails when probes or healing actions fail.
    pub fn tick_at(&mut self, now: SystemTime) -> Result<Vec<HealingAction>, ReplError> {
        let mut actions = Vec::new();
        let mut newly_dead = Vec::new();
        let mut recovered = Vec::new();

        for node in self.backend.known_nodes() {
            let healthy = self.probe.check(node)?;
            let transition = self.update_node_health(node, healthy, now);
            match transition {
                Some(HealingAction::MarkDead(node)) => {
                    if self.backend.mode() == HealingMode::Ap {
                        self.planned_dead.insert(node);
                    }
                    newly_dead.push(node);
                    actions.push(HealingAction::MarkDead(node));
                }
                Some(action) => actions.push(action),
                None => {
                    if healthy && self.planned_dead.remove(&node) {
                        recovered.push(node);
                    }
                }
            }
        }

        let view = self.health_view();
        if self.policy.enabled {
            self.plan_dead_nodes(&newly_dead, &view, &mut actions)?;
            self.plan_recovered_ap_nodes(&recovered, &mut actions)?;
        }

        for action in &actions {
            self.backend.record_healing_action(action, now)?;
        }

        Ok(actions)
    }

    fn update_node_health(
        &mut self,
        node: NodeId,
        healthy: bool,
        now: SystemTime,
    ) -> Option<HealingAction> {
        let entry = self
            .health
            .entry(node)
            .or_insert_with(|| NodeHealth::new(node, now));

        if healthy {
            return recover_node(entry, &self.config, now);
        }

        fail_node(entry, &self.config, now)
    }

    fn plan_dead_nodes(
        &mut self,
        nodes: &[NodeId],
        view: &HealthView,
        actions: &mut Vec<HealingAction>,
    ) -> Result<(), ReplError> {
        for node in nodes {
            if self.backend.mode() != HealingMode::Cp || self.planned_dead.contains(node) {
                continue;
            }

            if !self.backend.has_write_quorum(view) {
                return Err(ReplError::NoQuorum);
            }

            self.backend.evict_dead_node(*node)?;
            self.planned_dead.insert(*node);
            actions.push(HealingAction::EvictDeadNode(*node));

            if self.policy.max_parallel_resyncs > 0 {
                self.plan_one_replacement(actions)?;
            }
        }
        Ok(())
    }

    fn plan_one_replacement(&mut self, actions: &mut Vec<HealingAction>) -> Result<(), ReplError> {
        let Some(replacement) = self.replacement_pool.pop_front() else {
            return Ok(());
        };

        let node_id = replacement.id;
        self.backend.add_replacement_learner(replacement.clone())?;
        actions.push(HealingAction::AddReplacementLearner(replacement));

        self.backend.resync_node(node_id)?;
        actions.push(HealingAction::ResyncNode(node_id));

        if self.policy.promote_only_when_caught_up {
            self.backend.promote_learner(node_id)?;
            actions.push(HealingAction::PromoteLearner(node_id));
        }

        Ok(())
    }

    fn plan_recovered_ap_nodes(
        &self,
        nodes: &[NodeId],
        actions: &mut Vec<HealingAction>,
    ) -> Result<(), ReplError> {
        if self.backend.mode() != HealingMode::Ap {
            return Ok(());
        }

        for node in nodes {
            self.backend.deliver_ap_hints(*node)?;
            actions.push(HealingAction::DeliverApHints(*node));
            self.backend.run_ap_anti_entropy(*node)?;
            actions.push(HealingAction::RunApAntiEntropy(*node));
        }

        Ok(())
    }
}

impl<T> HealingBackend for Arc<T>
where
    T: HealingBackend,
{
    fn mode(&self) -> HealingMode {
        self.as_ref().mode()
    }

    fn local_node_id(&self) -> NodeId {
        self.as_ref().local_node_id()
    }

    fn known_nodes(&self) -> Vec<NodeId> {
        self.as_ref().known_nodes()
    }

    fn has_write_quorum(&self, health: &HealthView) -> bool {
        self.as_ref().has_write_quorum(health)
    }

    fn evict_dead_node(&self, node: NodeId) -> Result<(), ReplError> {
        self.as_ref().evict_dead_node(node)
    }

    fn add_replacement_learner(&self, node: RaftNode) -> Result<(), ReplError> {
        self.as_ref().add_replacement_learner(node)
    }

    fn resync_node(&self, node: NodeId) -> Result<(), ReplError> {
        self.as_ref().resync_node(node)
    }

    fn promote_learner(&self, node: NodeId) -> Result<(), ReplError> {
        self.as_ref().promote_learner(node)
    }

    fn deliver_ap_hints(&self, node: NodeId) -> Result<(), ReplError> {
        self.as_ref().deliver_ap_hints(node)
    }

    fn run_ap_anti_entropy(&self, node: NodeId) -> Result<(), ReplError> {
        self.as_ref().run_ap_anti_entropy(node)
    }

    fn record_healing_action(
        &self,
        action: &HealingAction,
        at: SystemTime,
    ) -> Result<(), ReplError> {
        self.as_ref().record_healing_action(action, at)
    }
}

impl<S> HealingBackend for CpRaftBackend<S>
where
    S: StorageEngine,
{
    fn mode(&self) -> HealingMode {
        HealingMode::Cp
    }

    fn local_node_id(&self) -> NodeId {
        self.cluster_config().node_id
    }

    fn known_nodes(&self) -> Vec<NodeId> {
        self.known_node_ids()
    }

    fn has_write_quorum(&self, health: &HealthView) -> bool {
        self.effective_voter_ids().is_ok_and(|voters| {
            voters.iter().filter(|node| health.is_live(**node)).count() > voters.len() / 2
        })
    }

    fn evict_dead_node(&self, node: NodeId) -> Result<(), ReplError> {
        self.evict_healing_voter(node)
    }

    fn add_replacement_learner(&self, node: RaftNode) -> Result<(), ReplError> {
        self.add_healing_learner(node)
    }

    fn resync_node(&self, node: NodeId) -> Result<(), ReplError> {
        self.resync_healing_node(node)
    }

    fn promote_learner(&self, node: NodeId) -> Result<(), ReplError> {
        self.promote_healing_learner(node)
    }

    fn deliver_ap_hints(&self, _node: NodeId) -> Result<(), ReplError> {
        Err(ReplError::Transport(
            "AP hinted handoff is not available on CP replication".to_owned(),
        ))
    }

    fn run_ap_anti_entropy(&self, _node: NodeId) -> Result<(), ReplError> {
        Err(ReplError::Transport(
            "AP anti-entropy is not available on CP replication".to_owned(),
        ))
    }

    fn record_healing_action(
        &self,
        action: &HealingAction,
        at: SystemTime,
    ) -> Result<(), ReplError> {
        write_healing_event(self.storage(), action, at)
    }
}

impl<S> HealingBackend for ApDynamo<S>
where
    S: StorageEngine,
{
    fn mode(&self) -> HealingMode {
        HealingMode::Ap
    }

    fn local_node_id(&self) -> NodeId {
        self.cluster_config().node_id
    }

    fn known_nodes(&self) -> Vec<NodeId> {
        self.cluster_config()
            .nodes
            .iter()
            .map(|node| node.id)
            .collect()
    }

    fn has_write_quorum(&self, health: &HealthView) -> bool {
        self.known_nodes()
            .into_iter()
            .any(|node| health.is_live(node))
    }

    fn evict_dead_node(&self, _node: NodeId) -> Result<(), ReplError> {
        Err(ReplError::Transport(
            "AP replication does not use CP voter eviction".to_owned(),
        ))
    }

    fn add_replacement_learner(&self, _node: RaftNode) -> Result<(), ReplError> {
        Err(ReplError::Transport(
            "AP replication does not use Raft learners".to_owned(),
        ))
    }

    fn resync_node(&self, _node: NodeId) -> Result<(), ReplError> {
        Err(ReplError::Transport(
            "AP replication resync uses anti-entropy".to_owned(),
        ))
    }

    fn promote_learner(&self, _node: NodeId) -> Result<(), ReplError> {
        Err(ReplError::Transport(
            "AP replication does not promote learners".to_owned(),
        ))
    }

    fn deliver_ap_hints(&self, _node: NodeId) -> Result<(), ReplError> {
        self.deliver_hints()
    }

    fn run_ap_anti_entropy(&self, node: NodeId) -> Result<(), ReplError> {
        if node == self.cluster_config().node_id {
            return self.deliver_hints();
        }

        self.anti_entropy_with(node)
    }

    fn record_healing_action(
        &self,
        action: &HealingAction,
        at: SystemTime,
    ) -> Result<(), ReplError> {
        write_healing_event(self.storage(), action, at)
    }
}

fn recover_node(
    health: &mut NodeHealth,
    config: &HealthConfig,
    now: SystemTime,
) -> Option<HealingAction> {
    health.failed_samples = 0;
    health.healthy_samples = health.healthy_samples.saturating_add(1);
    health.last_seen = Some(now);

    if health.state == HealthState::Healthy || health.healthy_samples < config.recovery_samples {
        return None;
    }

    if health.state == HealthState::Dead && elapsed(health.state_since, now) < config.cooldown {
        return None;
    }

    health.state = HealthState::Healthy;
    health.state_since = now;
    None
}

fn fail_node(
    health: &mut NodeHealth,
    config: &HealthConfig,
    now: SystemTime,
) -> Option<HealingAction> {
    health.healthy_samples = 0;
    health.failed_samples = health.failed_samples.saturating_add(1);

    if health.state == HealthState::Healthy && health.failed_samples >= config.suspect_after {
        health.state = HealthState::Suspect;
        health.state_since = now;
        return Some(HealingAction::MarkSuspect(health.node_id));
    }

    if health.state == HealthState::Suspect {
        let since = health.last_seen.unwrap_or(health.state_since);
        if elapsed(since, now) >= config.failure_timeout {
            health.state = HealthState::Dead;
            health.state_since = now;
            return Some(HealingAction::MarkDead(health.node_id));
        }
    }

    None
}

fn write_healing_event<S: StorageEngine>(
    storage: &S,
    action: &HealingAction,
    at: SystemTime,
) -> Result<(), ReplError> {
    let event = HealingEvent {
        at_unix_millis: unix_millis(at)?,
        action: action.clone(),
    };
    let bytes =
        serde_json::to_vec(&event).map_err(|error| ReplError::Transport(error.to_string()))?;
    let mut key = event.at_unix_millis.to_be_bytes().to_vec();
    key.extend_from_slice(blake3::hash(&bytes).as_bytes());

    let mut txn = storage.begin_write()?;
    txn.put(HEALING_EVENTS_TABLE, &key, &bytes)?;
    txn.put(HEALING_STATE_TABLE, LAST_EVENT_KEY, &bytes)?;
    txn.commit()?;
    Ok(())
}

fn unix_millis(at: SystemTime) -> Result<u64, ReplError> {
    let millis = at
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ReplError::Transport(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|error| ReplError::Transport(error.to_string()))
}

fn elapsed(from: SystemTime, to: SystemTime) -> Duration {
    to.duration_since(from).unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{
        HealingAction, HealingPolicy, HealthConfig, HealthState, ManualHealthProbe,
        SelfHealingController,
    };
    use crate::{
        phase30::{InternalTransportConfig, InternalTransportSecurity},
        repl::{
            ApDynamo, CpClusterConfig, CpRaft, HEALING_EVENTS_TABLE, HealingBackend, NodeId, Op,
            RaftNode, ReadConsistency, ReplError, Replication,
        },
        storage::{MemEngine, ReadTransaction, StorageEngine},
    };

    fn at(seconds: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn fast_config() -> HealthConfig {
        HealthConfig {
            sample_interval: Duration::from_millis(100),
            suspect_after: 1,
            failure_timeout: Duration::from_secs(1),
            recovery_samples: 1,
            cooldown: Duration::ZERO,
        }
    }

    fn cp_config() -> CpClusterConfig {
        CpClusterConfig::new(
            1,
            "127.0.0.1:7301",
            vec![
                RaftNode::new(1, "127.0.0.1:7301"),
                RaftNode::new(2, "127.0.0.1:7302"),
                RaftNode::new(3, "127.0.0.1:7303"),
            ],
        )
        .with_transport(InternalTransportConfig::new(
            "127.0.0.1:7301",
            InternalTransportSecurity::PlaintextForTests,
        ))
    }

    #[test]
    fn health_state_moves_through_suspect_dead_and_recovers()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = Arc::new(ApDynamo::new_for_test(MemEngine::new(), 1));
        let probe = ManualHealthProbe::new([(1, false)]);
        let mut controller = SelfHealingController::new(
            backend,
            probe.clone(),
            fast_config(),
            HealingPolicy::default(),
        )?;

        let actions = controller.tick_at(at(10))?;
        assert_eq!(actions, vec![HealingAction::MarkSuspect(1)]);
        assert_eq!(
            controller.health(1).map(|health| health.state),
            Some(HealthState::Suspect)
        );

        let actions = controller.tick_at(at(12))?;
        assert_eq!(actions, vec![HealingAction::MarkDead(1)]);
        assert_eq!(
            controller.health(1).map(|health| health.state),
            Some(HealthState::Dead)
        );

        probe.set(1, true)?;
        let actions = controller.tick_at(at(13))?;
        assert_eq!(
            actions,
            vec![
                HealingAction::DeliverApHints(1),
                HealingAction::RunApAntiEntropy(1),
            ]
        );
        assert_eq!(
            controller.health(1).map(|health| health.state),
            Some(HealthState::Healthy)
        );

        Ok(())
    }

    #[test]
    fn cp_healing_evicts_dead_voter_and_promotes_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let raft = Arc::new(CpRaft::new(MemEngine::new(), cp_config())?);
        raft.set_available_voters_for_tests([1, 2])?;
        let probe = ManualHealthProbe::new([(1, true), (2, true), (3, false)]);
        let mut controller = SelfHealingController::new(
            raft.clone(),
            probe,
            fast_config(),
            HealingPolicy::default(),
        )?
        .with_replacements([RaftNode::new(4, "127.0.0.1:7304")]);

        assert_eq!(
            controller.tick_at(at(10))?,
            vec![HealingAction::MarkSuspect(3)]
        );
        let actions = controller.tick_at(at(12))?;
        assert_eq!(
            actions,
            vec![
                HealingAction::MarkDead(3),
                HealingAction::EvictDeadNode(3),
                HealingAction::AddReplacementLearner(RaftNode::new(4, "127.0.0.1:7304")),
                HealingAction::ResyncNode(4),
                HealingAction::PromoteLearner(4),
            ]
        );

        let voters = raft.effective_voter_ids()?;
        assert!(voters.contains(&1));
        assert!(voters.contains(&2));
        assert!(voters.contains(&4));
        assert!(!voters.contains(&3));

        raft.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
        assert_eq!(
            raft.read("t", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );

        let read = raft.storage().begin_read()?;
        assert!(
            !read
                .range(HEALING_EVENTS_TABLE, &[], &[0xFF])?
                .collect::<Result<Vec<_>, _>>()?
                .is_empty()
        );

        Ok(())
    }

    #[test]
    fn cp_healing_refuses_to_act_without_quorum() -> Result<(), Box<dyn std::error::Error>> {
        let raft = Arc::new(CpRaft::new(MemEngine::new(), cp_config())?);
        raft.set_available_voters_for_tests([1])?;
        let probe = ManualHealthProbe::new([(1, true), (2, false), (3, false)]);
        let mut controller =
            SelfHealingController::new(raft, probe, fast_config(), HealingPolicy::default())?;

        assert_eq!(
            controller.tick_at(at(10))?,
            vec![HealingAction::MarkSuspect(2), HealingAction::MarkSuspect(3)]
        );
        assert!(matches!(
            controller.tick_at(at(12)),
            Err(ReplError::NoQuorum)
        ));

        Ok(())
    }

    #[test]
    fn ap_recovery_runs_hint_delivery_and_anti_entropy() -> Result<(), Box<dyn std::error::Error>> {
        let backend = Arc::new(ApDynamo::new_for_test(MemEngine::new(), 1));
        let probe = ManualHealthProbe::new([(1, false)]);
        let mut controller = SelfHealingController::new(
            backend,
            probe.clone(),
            fast_config(),
            HealingPolicy::default(),
        )?;

        controller.tick_at(at(10))?;
        controller.tick_at(at(12))?;
        probe.set(1, true)?;

        assert_eq!(
            controller.tick_at(at(13))?,
            vec![
                HealingAction::DeliverApHints(1),
                HealingAction::RunApAntiEntropy(1),
            ]
        );

        Ok(())
    }

    #[test]
    fn health_config_requires_failure_timeout_beyond_election() {
        assert!(
            HealthConfig::default()
                .validate_against_election_timeout(Duration::from_secs(5))
                .is_ok()
        );
        assert!(
            HealthConfig::default()
                .validate_against_election_timeout(Duration::from_secs(30))
                .is_err()
        );
    }

    #[test]
    fn cp_backend_quorum_uses_health_view() -> Result<(), Box<dyn std::error::Error>> {
        let raft = CpRaft::new(MemEngine::new(), cp_config())?;
        assert!(raft.has_write_quorum(&super::HealthView::new([
            (1, HealthState::Healthy),
            (2, HealthState::Healthy),
            (3, HealthState::Dead),
        ])));
        assert!(!raft.has_write_quorum(&super::HealthView::new([
            (1, HealthState::Healthy),
            (2, HealthState::Dead),
            (3, HealthState::Dead),
        ])));
        Ok(())
    }

    fn _node_id(value: NodeId) -> NodeId {
        value
    }
}
