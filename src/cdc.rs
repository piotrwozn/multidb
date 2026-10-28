use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

#[cfg(test)]
#[path = "cdc_metadata_race_tests.rs"]
mod metadata_race_tests;

#[cfg(test)]
#[path = "cdc_view_integrity_tests.rs"]
mod view_integrity_tests;

use crate::{
    backup::{Lsn, TimelineId},
    db::{CatalogEntry, ConfigError, Database, DbError, read_catalog_entry_from_repl},
    geo::{GEO_INDEX_TABLE, GEO_POINTS_TABLE},
    graph::{GRAPH_IN_EDGES_TABLE, GRAPH_OUT_EDGES_TABLE},
    model::{DOCUMENT_TABLE, INDEX_TABLE, Value, decode_value},
    observability,
    phase30::{CdcWorkerConfig, HlcTimestamp},
    query::{REL_COLUMNAR_SEGMENTS_TABLE, REL_INDEX_TABLE, REL_ROWS_TABLE, Row, decode_row_bytes},
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition,
        propose_system, propose_system_batch, propose_system_conditional_batch,
    },
    security::{AuthzError, AuthzPolicy, Permission, Principal, Resource},
    storage::{Bytes, StorageError},
    text::{FULL_TEXT_DOCS_TABLE, FULL_TEXT_META_TABLE, FULL_TEXT_POSTINGS_TABLE},
    timeseries::{TIME_SERIES_CHUNKS_TABLE, TIME_SERIES_LATEST_TABLE, TIME_SERIES_META_TABLE},
    txn::{self, CommitLogRecord, TxnId},
    vector::VECTOR_TABLE,
};

pub const CDC_SUBSCRIPTIONS_TABLE: &str = "__cdc_subscriptions";
pub const CDC_META_TABLE: &str = "__cdc_meta";
pub const CDC_TIMELINES_TABLE: &str = "__cdc_timelines";
pub const MATERIALIZED_VIEWS_TABLE: &str = "__materialized_views";
pub const MATERIALIZED_VIEW_STATE_TABLE: &str = "__materialized_view_state";
pub const HOOKS_TABLE: &str = "__hooks";
pub const HOOK_DELIVERIES_TABLE: &str = "__hook_deliveries";
pub const POST_COMMIT_FAILURES_TABLE: &str = "__post_commit_failures";

#[cfg(test)]
pub(crate) const FAIL_AFTER_HOOK_FOR_TESTS: &str = "test-fail-after-hook";

const BACKUP_META_TABLE: &str = "__backup_meta";
const KEY_TIMELINE_ID: &[u8] = b"timeline_id";
const DEFAULT_TIMELINE_ID: &str = "default";
const MATERIALIZED_VIEW_CDC_WINDOW: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ResumeToken {
    pub timeline_id: TimelineId,
    pub lsn: Lsn,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LogicalTarget {
    Database,
    Table(String),
    Collection(String),
    VectorCollection(String),
    FullTextIndex(String),
    TimeSeries(String),
    Graph(String),
    GeoIndex(String),
    System(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ChangeOp {
    TxBegin,
    TxCommit,
    Upsert { key: Bytes, value_after: Bytes },
    Delete { key: Bytes },
    Ddl { name: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TransactionBoundary {
    Begin,
    Data,
    Commit,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChangeEvent {
    pub lsn: Lsn,
    pub tx_id: TxnId,
    pub at: SystemTime,
    #[serde(default)]
    pub hlc: Option<HlcTimestamp>,
    pub target: LogicalTarget,
    pub op: ChangeOp,
    pub transaction_boundary: TransactionBoundary,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChangefeedOptions {
    /// Also returns writes to system keyspaces (`LogicalTarget::System`): admin password hashes
    /// (`__admin_auth`), the RBAC policy and principal registry, the audit log, policy
    /// definitions and raw index keys, which carry values that masking and row policies hide.
    /// [`crate::db::Database::poll_changefeed_as`] requires `System:Admin` for it.
    pub include_system: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChangefeedPage {
    pub events: Vec<ChangeEvent>,
    pub next: ResumeToken,
    pub has_more: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChangefeedFilter {
    pub target: ChangefeedTarget,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ChangefeedTarget {
    #[default]
    All,
    Table(String),
    Collection(String),
    VectorCollection(String),
    FullTextIndex(String),
    TimeSeries(String),
    Graph(String),
    GeoIndex(String),
    System(String),
}

#[derive(thiserror::Error, Debug)]
pub enum FeedError {
    #[error("token expired: requested lsn {requested}, earliest available lsn is {earliest}")]
    TokenExpired { requested: Lsn, earliest: Lsn },

    #[error("timeline mismatch: expected {expected}, found {found}")]
    TimelineMismatch {
        expected: TimelineId,
        found: TimelineId,
    },

    #[error("timeline forked: requested {requested}, current {current}, fork lsn {fork_lsn}")]
    TimelineForked {
        requested: TimelineId,
        current: TimelineId,
        fork_lsn: Lsn,
    },

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SubscriptionConfig {
    pub name: String,
    pub filter: ChangefeedFilter,
    pub start: ResumeToken,
    pub buffer_limit: usize,
    pub ack_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SubscriptionState {
    /// The principal whose live permissions and policies must be checked on delivery.
    /// Subscriptions explicitly created by trusted APIs have no owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<Principal>,
    /// Explicit operator trust. Legacy records default to false because their original
    /// creator cannot be inferred safely; they must be adopted before delivery.
    #[serde(default)]
    pub trusted: bool,
    pub config: SubscriptionConfig,
    pub last_ack: ResumeToken,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CdcTimelineRecord {
    pub timeline_id: TimelineId,
    pub parent: Option<TimelineId>,
    pub fork_lsn: Lsn,
    pub reason: String,
    pub created_at: SystemTime,
}

pub struct SubscriptionWorker;

pub struct SubscriptionHandle {
    shutdown: Option<oneshot::Sender<()>>,
    join: JoinHandle<Result<ResumeToken, SubscriptionError>>,
}

#[derive(thiserror::Error, Debug)]
pub enum SubscriptionError {
    #[error(
        "principal-bound subscription requires push_subscription_once_as or SubscriptionWorker::start_as"
    )]
    PrincipalRequired,

    #[error("subscription belongs to a different principal")]
    PrincipalMismatch,

    #[error("subscription policy: {0}")]
    Policy(String),

    #[error("missing subscription: {0}")]
    MissingSubscription(String),

    #[error("authorization denied: {0}")]
    Authz(#[from] AuthzError),

    #[error("feed: {0}")]
    Feed(#[from] FeedError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("metadata serialization: {0}")]
    Serde(String),

    #[error("subscription buffer is closed")]
    BufferClosed,

    #[error("subscription buffer timed out")]
    BackpressureTimeout,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MaterializedViewSpec {
    pub name: String,
    pub source_table: String,
    pub filter: Option<SimplePredicate>,
    pub group_by: usize,
    pub aggregates: Vec<AggregateSpec>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SimplePredicate {
    pub column: usize,
    pub equals: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AggregateSpec {
    pub output_name: String,
    pub kind: AggregateKind,
    pub column: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum AggregateKind {
    Count,
    Sum,
    Avg,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MaterializedViewRows {
    pub fresh_to_lsn: Lsn,
    pub rows: Vec<Row>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MaterializedViewState {
    pub fresh_to_lsn: Lsn,
    pub source_rows: BTreeMap<String, Row>,
    pub groups: BTreeMap<String, GroupState>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GroupState {
    pub group_value: Value,
    pub aggregates: Vec<AggregateValue>,
    /// Counts matching source rows independently of aggregate kinds. Older stored
    /// views omit this field and are migrated from their retained source rows.
    #[serde(default)]
    pub row_count: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum AggregateValue {
    Count(i64),
    Sum(f64),
    Avg { sum: f64, count: i64 },
}

#[derive(thiserror::Error, Debug)]
pub enum MaterializedViewError {
    #[error("missing materialized view: {0}")]
    MissingView(String),

    #[error("unsupported materialized view: {0}")]
    Unsupported(String),

    #[error("feed: {0}")]
    Feed(#[from] FeedError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("query: {0}")]
    Query(#[from] crate::query::QueryError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HookSpec {
    pub name: String,
    pub kind: HookKind,
    pub target: HookTarget,
    pub action: HookAction,
    pub timeout_ms: u64,
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HookKind {
    BeforeCommit,
    AfterCommit,
}

/// The writes a hook applies to.
///
/// Named targets are scoped to that object: `Table`, `FullTextIndex`,
/// `TimeSeries` and `GeoIndex` match by name, and `Collection`,
/// `VectorCollection` and `Graph` resolve the name to the object's catalog id.
/// A missing object, or an object of another kind, matches nothing.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HookTarget {
    AnyUser,
    Keyspace(String),
    Table(String),
    Collection(String),
    VectorCollection(String),
    FullTextIndex(String),
    TimeSeries(String),
    Graph(String),
    GeoIndex(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HookAction {
    RejectEmptyValue,
    RequireJsonObject,
    MaxValueBytes(usize),
    RejectKeyPrefix(Bytes),
    RecordAfterCommit,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HookDelivery {
    pub hook_name: String,
    pub lsn: Lsn,
    pub delivered_at: SystemTime,
    pub event_count: usize,
}

/// The post-commit stage that failed after a durable commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum PostCommitStage {
    AfterHook,
    AfterTrigger,
}

/// An after-commit hook or after-trigger failure recorded in
/// [`POST_COMMIT_FAILURES_TABLE`]. The write itself is committed and was reported
/// as successful; `start_lsn..=end_lsn` is the commit window the stage covered.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PostCommitFailure {
    pub stage: PostCommitStage,
    /// The after trigger name, or `after_commit_hooks` for the after-hook pass.
    pub name: String,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub error: String,
    pub recorded_at: SystemTime,
}

#[derive(thiserror::Error, Debug)]
pub enum HookError {
    #[error("hook rejected write: {0}")]
    Rejected(String),

    #[error("hook timed out: {0}")]
    Timeout(String),

    #[error("invalid hook target: {0}")]
    InvalidTarget(String),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

impl ResumeToken {
    #[must_use]
    pub fn new(timeline_id: impl Into<TimelineId>, lsn: Lsn) -> Self {
        Self {
            timeline_id: timeline_id.into(),
            lsn,
        }
    }
}

impl Default for ResumeToken {
    fn default() -> Self {
        Self::new(DEFAULT_TIMELINE_ID, 0)
    }
}

impl SubscriptionConfig {
    #[must_use]
    pub fn new(name: impl Into<String>, start: ResumeToken) -> Self {
        Self {
            name: name.into(),
            filter: ChangefeedFilter::default(),
            start,
            buffer_limit: 1_024,
            ack_timeout_ms: 30_000,
        }
    }
}

impl CdcTimelineRecord {
    #[must_use]
    pub fn new(
        timeline_id: impl Into<TimelineId>,
        parent: Option<TimelineId>,
        fork_lsn: Lsn,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            timeline_id: timeline_id.into(),
            parent,
            fork_lsn,
            reason: reason.into(),
            created_at: SystemTime::now(),
        }
    }
}

impl SubscriptionWorker {
    /// Starts a bounded CDC push worker for one named subscription.
    /// # Errors
    /// Fails when the subscription cannot be read before the worker starts.
    pub fn start(
        repl: Arc<dyn Replication>,
        catalog: Arc<BTreeMap<String, CatalogEntry>>,
        name: impl Into<String>,
        config: CdcWorkerConfig,
    ) -> Result<(SubscriptionHandle, mpsc::Receiver<ChangeEvent>), SubscriptionError> {
        let name = name.into();
        require_trusted_subscription(&subscription_state(repl.as_ref(), &name)?)?;
        let (sender, receiver) = mpsc::channel(config.channel_capacity.max(1));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let worker_name = name.clone();
        let join = tokio::spawn(async move {
            run_subscription_worker(repl, catalog, worker_name, sender, shutdown_rx, config).await
        });
        Ok((
            SubscriptionHandle {
                shutdown: Some(shutdown),
                join,
            },
            receiver,
        ))
    }
}

impl SubscriptionHandle {
    pub fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    /// Waits for the worker to finish.
    /// # Errors
    /// Fails when the worker panics or returns a subscription error.
    pub async fn join(self) -> Result<ResumeToken, SubscriptionError> {
        self.join
            .await
            .map_err(|error| SubscriptionError::Serde(error.to_string()))?
    }
}

impl HookSpec {
    #[must_use]
    pub fn before(
        name: impl Into<String>,
        target: HookTarget,
        action: HookAction,
        timeout_ms: u64,
    ) -> Self {
        Self {
            name: name.into(),
            kind: HookKind::BeforeCommit,
            target,
            action,
            timeout_ms,
            enabled: true,
        }
    }

    /// Builds an after-commit hook. After-commit hooks never fail the write that
    /// triggered them: a failed delivery is recorded as a [`PostCommitFailure`].
    #[must_use]
    pub fn after(
        name: impl Into<String>,
        target: HookTarget,
        action: HookAction,
        timeout_ms: u64,
    ) -> Self {
        Self {
            name: name.into(),
            kind: HookKind::AfterCommit,
            target,
            action,
            timeout_ms,
            enabled: true,
        }
    }
}

impl PostCommitStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterHook => "after_hook",
            Self::AfterTrigger => "after_trigger",
        }
    }
}

impl PostCommitFailure {
    #[must_use]
    pub fn new(
        stage: PostCommitStage,
        name: impl Into<String>,
        start_lsn: Lsn,
        end_lsn: Lsn,
        error: impl Into<String>,
    ) -> Self {
        Self {
            stage,
            name: name.into(),
            start_lsn,
            end_lsn,
            error: error.into(),
            recorded_at: SystemTime::now(),
        }
    }
}

/// Polls committed change events after `from`.
///
/// # Errors
/// Fails when the token is invalid, storage is corrupt, or replication rejects the read.
pub fn poll_changefeed<R: Replication + ?Sized>(
    repl: &R,
    catalog: &BTreeMap<String, CatalogEntry>,
    from: &ResumeToken,
    filter: &ChangefeedFilter,
    options: &ChangefeedOptions,
    max: usize,
) -> Result<(Vec<ChangeEvent>, ResumeToken), FeedError> {
    let page = poll_changefeed_page(repl, catalog, from, filter, options, max)?;
    Ok((page.events, page.next))
}

/// Polls one page of committed change events after `from`.
///
/// # Errors
/// Fails when the token is invalid, storage is corrupt, or replication rejects the read.
pub fn poll_changefeed_page<R: Replication + ?Sized>(
    repl: &R,
    catalog: &BTreeMap<String, CatalogEntry>,
    from: &ResumeToken,
    filter: &ChangefeedFilter,
    options: &ChangefeedOptions,
    max: usize,
) -> Result<ChangefeedPage, FeedError> {
    poll_changefeed_page_through(repl, catalog, from, filter, options, max, u64::MAX)
}

#[allow(clippy::too_many_arguments)]
fn poll_changefeed_page_through<R: Replication + ?Sized>(
    repl: &R,
    catalog: &BTreeMap<String, CatalogEntry>,
    from: &ResumeToken,
    filter: &ChangefeedFilter,
    options: &ChangefeedOptions,
    max: usize,
    through: Lsn,
) -> Result<ChangefeedPage, FeedError> {
    ensure_commit_log(repl)?;
    let timeline_id = timeline_id(repl)?;
    let from = map_resume_token_to_current_timeline(repl, from, &timeline_id)?;

    let earliest = earliest_lsn(repl)?;
    if earliest > 0 {
        let next_requested = from
            .lsn
            .checked_add(1)
            .ok_or_else(|| FeedError::Serde("lsn overflow".to_owned()))?;
        if next_requested < earliest {
            return Err(FeedError::TokenExpired {
                requested: from.lsn,
                earliest,
            });
        }
    }

    let current = current_lsn(repl)?.min(through);
    let page = txn::history::commit_records_after(repl, from.lsn, current, max.max(1))?;
    ensure_no_truncated_prefix(repl, &from, current, &page)?;
    let has_more = page.has_more;
    let records = page.records;

    let mut events = Vec::new();
    let mut last_lsn = from.lsn;
    for record in records {
        let record_events = events_for_record(&record, catalog, filter, options);
        if !record_events.is_empty() {
            events.push(ChangeEvent {
                lsn: record.txn_id,
                tx_id: record.txn_id,
                at: record.committed_at,
                hlc: record.hlc,
                target: LogicalTarget::Database,
                op: ChangeOp::TxBegin,
                transaction_boundary: TransactionBoundary::Begin,
            });
            events.extend(record_events);
            events.push(ChangeEvent {
                lsn: record.txn_id,
                tx_id: record.txn_id,
                at: record.committed_at,
                hlc: record.hlc,
                target: LogicalTarget::Database,
                op: ChangeOp::TxCommit,
                transaction_boundary: TransactionBoundary::Commit,
            });
        }
        last_lsn = record.txn_id;
    }
    observability::record_cdc_events(events.len());
    Ok(ChangefeedPage {
        events,
        next: ResumeToken::new(timeline_id, last_lsn),
        has_more,
    })
}

/// Returns a token at the current durable LSN.
///
/// # Errors
/// Fails when CDC metadata or transaction metadata cannot be read.
pub(crate) fn current_resume_token<R: Replication + ?Sized>(
    repl: &R,
) -> Result<ResumeToken, FeedError> {
    Ok(ResumeToken::new(timeline_id(repl)?, current_lsn(repl)?))
}

/// Records a CDC timeline fork.
/// # Errors
/// Fails when metadata cannot be serialized or written.
pub fn record_timeline_fork(
    repl: &dyn Replication,
    record: &CdcTimelineRecord,
) -> Result<(), FeedError> {
    let bytes = serde_json::to_vec(record).map_err(|error| FeedError::Serde(error.to_string()))?;
    propose_system(
        repl,
        Op::Put {
            table: CDC_TIMELINES_TABLE.to_owned(),
            key: record.timeline_id.as_bytes().to_vec(),
            value: bytes,
        },
    )?;
    Ok(())
}

/// Creates or replaces a named subscription.
///
/// # Errors
/// Fails when authorization or metadata writes fail.
pub fn create_subscription(
    repl: &dyn Replication,
    authz: &AuthzPolicy,
    principal: &Principal,
    config: SubscriptionConfig,
) -> Result<SubscriptionState, SubscriptionError> {
    ensure_commit_log(repl)?;
    authorize_filter_access(authz, principal, &config.filter)?;
    let previous = subscription_snapshot(repl, &config.name)?;
    if let Some((existing, _)) = &previous {
        require_subscription_owner(existing, principal)?;
    }
    let now = SystemTime::now();
    let state = SubscriptionState {
        principal: Some(principal.clone()),
        trusted: false,
        last_ack: config.start.clone(),
        config,
        created_at: now,
        updated_at: now,
    };
    write_subscription_if_unchanged(repl, &state, previous.map(|(_, bytes)| bytes))?;
    Ok(state)
}

/// Creates or replaces a named subscription for trusted embedded/admin callers.
///
/// # Errors
/// Fails when metadata writes fail.
pub fn create_subscription_trusted(
    repl: &dyn Replication,
    config: SubscriptionConfig,
) -> Result<SubscriptionState, SubscriptionError> {
    let now = SystemTime::now();
    ensure_commit_log(repl)?;
    let previous = subscription_snapshot(repl, &config.name)?;
    let state = SubscriptionState {
        principal: None,
        trusted: true,
        last_ack: config.start.clone(),
        config,
        created_at: now,
        updated_at: now,
    };
    write_subscription_if_unchanged(repl, &state, previous.map(|(_, bytes)| bytes))?;
    Ok(state)
}

/// Acknowledges a subscription position.
///
/// Re-acknowledging the token that is already stored returns the stored state
/// without writing a commit.
///
/// # Errors
/// Fails when the subscription is missing or metadata writes fail.
pub fn ack_subscription(
    repl: &dyn Replication,
    name: &str,
    token: ResumeToken,
) -> Result<SubscriptionState, SubscriptionError> {
    let (state, expected) = subscription_snapshot(repl, name)?
        .ok_or_else(|| SubscriptionError::MissingSubscription(name.to_owned()))?;
    ack_subscription_snapshot(repl, state, expected, token)
}

/// Acknowledges only the owner/filter state that was authorized, without a second
/// unchecked read or an unconditional overwrite of a concurrent metadata change.
pub(crate) fn ack_subscription_as(
    repl: &dyn Replication,
    authz: &AuthzPolicy,
    principal: &Principal,
    name: &str,
    token: ResumeToken,
) -> Result<SubscriptionState, SubscriptionError> {
    let (state, expected) = subscription_snapshot(repl, name)?
        .ok_or_else(|| SubscriptionError::MissingSubscription(name.to_owned()))?;
    require_subscription_owner(&state, principal)?;
    authorize_filter_access(authz, principal, &state.config.filter)?;
    ack_subscription_snapshot(repl, state, expected, token)
}

fn ack_subscription_snapshot(
    repl: &dyn Replication,
    mut state: SubscriptionState,
    expected: Bytes,
    token: ResumeToken,
) -> Result<SubscriptionState, SubscriptionError> {
    validate_ack_advance(&state.last_ack, &token)?;
    if state.last_ack == token {
        return Ok(state);
    }
    state.last_ack = token;
    state.updated_at = SystemTime::now();
    write_subscription_if_unchanged(repl, &state, Some(expected))?;
    Ok(state)
}

pub(crate) fn validate_ack_advance(
    previous: &ResumeToken,
    token: &ResumeToken,
) -> Result<(), SubscriptionError> {
    if previous.timeline_id == token.timeline_id && token.lsn < previous.lsn {
        return Err(SubscriptionError::Policy(
            "acknowledgement cannot move a durable cursor backwards".to_owned(),
        ));
    }
    Ok(())
}

/// Reads subscription state.
///
/// # Errors
/// Fails when metadata is corrupt or unavailable.
pub fn subscription_state(
    repl: &dyn Replication,
    name: &str,
) -> Result<SubscriptionState, SubscriptionError> {
    read_subscription(repl, name)?
        .ok_or_else(|| SubscriptionError::MissingSubscription(name.to_owned()))
}

/// Pushes one bounded batch for a named subscription.
///
/// # Errors
/// Fails when feed polling fails or the receiver cannot keep up.
pub async fn push_subscription_once(
    repl: &dyn Replication,
    catalog: &BTreeMap<String, CatalogEntry>,
    name: &str,
    sender: &mpsc::Sender<ChangeEvent>,
) -> Result<ResumeToken, SubscriptionError> {
    let state = subscription_state(repl, name)?;
    require_trusted_subscription(&state)?;
    let (events, token) = poll_changefeed(
        repl,
        catalog,
        &state.last_ack,
        &state.config.filter,
        &ChangefeedOptions::default(),
        state.config.buffer_limit,
    )?;
    let timeout = Duration::from_millis(state.config.ack_timeout_ms);
    for event in events {
        tokio::time::timeout(timeout, sender.send(event))
            .await
            .map_err(|_| SubscriptionError::BackpressureTimeout)?
            .map_err(|_| SubscriptionError::BufferClosed)?;
    }
    ack_subscription(repl, name, token.clone())?;
    observability::record_subscription_push(name, token.lsn);
    Ok(token)
}

/// Pushes one batch with the principal's current database permissions and policies.
///
/// Unlike the trusted raw-replication API, this is safe for subscriptions created by
/// `Database::create_subscription_as`. Permissions are checked again on every delivery.
/// # Errors
/// Fails when the caller is not the subscription owner, authorization or policies reject
/// the read, or delivery/acknowledgement fails.
pub async fn push_subscription_once_as(
    database: &Database,
    principal: &Principal,
    name: &str,
    sender: &mpsc::Sender<ChangeEvent>,
) -> Result<ResumeToken, SubscriptionError> {
    let state = subscription_state(database, name)?;
    require_subscription_owner(&state, principal)?;
    let (events, token) = database
        .poll_changefeed_as(
            principal,
            &state.last_ack,
            &state.config.filter,
            &ChangefeedOptions::default(),
            state.config.buffer_limit,
        )
        .map_err(subscription_policy_error)?;
    let timeout = Duration::from_millis(state.config.ack_timeout_ms);
    for event in events {
        tokio::time::timeout(timeout, sender.send(event))
            .await
            .map_err(|_| SubscriptionError::BackpressureTimeout)?
            .map_err(|_| SubscriptionError::BufferClosed)?;
    }
    database
        .ack_subscription_as(principal, name, token.clone())
        .map_err(subscription_policy_error)?;
    observability::record_subscription_push(name, token.lsn);
    Ok(token)
}

/// Resolves a registered user and pushes one batch with their current roles and policies.
/// # Errors
/// Fails when the user is unmapped, ownership/authorization rejects delivery, or delivery fails.
pub async fn push_subscription_once_for_user(
    database: &Database,
    pg_user: &str,
    name: &str,
    sender: &mpsc::Sender<ChangeEvent>,
) -> Result<ResumeToken, SubscriptionError> {
    let principal = database.principal_for_user(pg_user);
    push_subscription_once_as(database, &principal, name, sender).await
}

fn require_trusted_subscription(state: &SubscriptionState) -> Result<(), SubscriptionError> {
    if state.principal.is_some() || !state.trusted {
        return Err(SubscriptionError::PrincipalRequired);
    }
    Ok(())
}

/// Assigns a principal to an existing subscription without resetting its durable cursor.
/// This is the migration path for legacy records with unknown ownership.
/// # Errors
/// Fails unless the operator has System:Admin and the owner can read the filter target,
/// or when reading/writing the subscription metadata fails.
pub fn adopt_subscription_as(
    repl: &dyn Replication,
    authz: &AuthzPolicy,
    operator: &Principal,
    owner: &Principal,
    name: &str,
) -> Result<SubscriptionState, SubscriptionError> {
    ensure_commit_log(repl)?;
    authz.authorize(operator, &Resource::System, Permission::Admin)?;
    let (mut state, expected) = subscription_snapshot(repl, name)?
        .ok_or_else(|| SubscriptionError::MissingSubscription(name.to_owned()))?;
    authorize_filter_access(authz, owner, &state.config.filter)?;
    state.principal = Some(owner.clone());
    state.trusted = false;
    state.updated_at = SystemTime::now();
    write_subscription_if_unchanged(repl, &state, Some(expected))?;
    Ok(state)
}

pub(crate) fn require_subscription_owner(
    state: &SubscriptionState,
    principal: &Principal,
) -> Result<(), SubscriptionError> {
    if state
        .principal
        .as_ref()
        .is_none_or(|owner| owner.name() != principal.name())
    {
        return Err(SubscriptionError::PrincipalMismatch);
    }
    Ok(())
}

fn subscription_policy_error(error: DbError) -> SubscriptionError {
    match error {
        DbError::AuthzDenied(error) => SubscriptionError::Authz(error),
        error => SubscriptionError::Policy(error.to_string()),
    }
}

fn ensure_commit_log<R: Replication + ?Sized>(repl: &R) -> Result<(), ReplError> {
    if !repl.supports_commit_log() {
        return Err(ReplError::Unsupported(
            "changefeeds and CDC subscriptions require a commit log".to_owned(),
        ));
    }
    Ok(())
}

enum SubscriptionIdentity {
    Principal(Principal),
    RegisteredUser(String),
}

impl SubscriptionIdentity {
    fn resolve(&self, database: &Database) -> Principal {
        match self {
            Self::Principal(principal) => principal.clone(),
            Self::RegisteredUser(user) => database.principal_for_user(user),
        }
    }
}

impl SubscriptionWorker {
    /// Starts a worker with explicit principal roles, checking live grants and policies.
    /// Use `start_for_user` when role membership comes from the database registry.
    /// # Errors
    /// Fails when the database lock is poisoned, the subscription has another owner,
    /// or the first page is not readable by the principal.
    pub fn start_as(
        database: Arc<RwLock<Database>>,
        principal: Principal,
        name: impl Into<String>,
        config: CdcWorkerConfig,
    ) -> Result<(SubscriptionHandle, mpsc::Receiver<ChangeEvent>), SubscriptionError> {
        Self::start_with_identity(
            database,
            SubscriptionIdentity::Principal(principal),
            name.into(),
            config,
        )
    }

    /// Starts a worker that resolves the registered user's principal again for every page.
    /// Removing/remapping the user or changing their roles takes effect on the next poll.
    /// # Errors
    /// Fails when the database lock, subscription ownership, or live authorization rejects it.
    pub fn start_for_user(
        database: Arc<RwLock<Database>>,
        pg_user: impl Into<String>,
        name: impl Into<String>,
        config: CdcWorkerConfig,
    ) -> Result<(SubscriptionHandle, mpsc::Receiver<ChangeEvent>), SubscriptionError> {
        Self::start_with_identity(
            database,
            SubscriptionIdentity::RegisteredUser(pg_user.into()),
            name.into(),
            config,
        )
    }

    fn start_with_identity(
        database: Arc<RwLock<Database>>,
        identity: SubscriptionIdentity,
        name: String,
        config: CdcWorkerConfig,
    ) -> Result<(SubscriptionHandle, mpsc::Receiver<ChangeEvent>), SubscriptionError> {
        {
            let database = database
                .read()
                .map_err(|_| SubscriptionError::Policy("database lock poisoned".to_owned()))?;
            let principal = identity.resolve(&database);
            let state = subscription_state(&*database, &name)?;
            require_subscription_owner(&state, &principal)?;
            database
                .poll_changefeed_page_as(
                    &principal,
                    &state.last_ack,
                    &state.config.filter,
                    &ChangefeedOptions::default(),
                    1,
                )
                .map_err(subscription_policy_error)?;
        }
        let (sender, receiver) = mpsc::channel(config.channel_capacity.max(1));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let join = tokio::spawn(run_principal_subscription_worker(
            database,
            identity,
            name,
            sender,
            shutdown_rx,
            config,
        ));
        Ok((
            SubscriptionHandle {
                shutdown: Some(shutdown),
                join,
            },
            receiver,
        ))
    }
}

async fn run_principal_subscription_worker(
    database: Arc<RwLock<Database>>,
    identity: SubscriptionIdentity,
    name: String,
    sender: mpsc::Sender<ChangeEvent>,
    mut shutdown: oneshot::Receiver<()>,
    config: CdcWorkerConfig,
) -> Result<ResumeToken, SubscriptionError> {
    let mut last = database
        .read()
        .map_err(|_| SubscriptionError::Policy("database lock poisoned".to_owned()))?
        .subscription_state(&name)
        .map_err(subscription_policy_error)?
        .last_ack;
    loop {
        // Release the synchronous database lock before awaiting the receiver or shutdown.
        let page = {
            let database = database
                .read()
                .map_err(|_| SubscriptionError::Policy("database lock poisoned".to_owned()))?;
            let principal = identity.resolve(&database);
            let state = subscription_state(&*database, &name)?;
            require_subscription_owner(&state, &principal)?;
            database
                .poll_changefeed_page_as(
                    &principal,
                    &state.last_ack,
                    &state.config.filter,
                    &ChangefeedOptions::default(),
                    config.page_size.max(1),
                )
                .map_err(subscription_policy_error)?
        };
        for event in page.events {
            tokio::select! {
                result = tokio::time::timeout(
                    Duration::from_millis(config.deliver_timeout_ms.max(1)),
                    sender.send(event),
                ) => {
                    result
                        .map_err(|_| SubscriptionError::BackpressureTimeout)?
                        .map_err(|_| SubscriptionError::BufferClosed)?;
                }
                _ = &mut shutdown => return Ok(last),
            }
        }
        last = page.next;
        {
            let database = database
                .read()
                .map_err(|_| SubscriptionError::Policy("database lock poisoned".to_owned()))?;
            let principal = identity.resolve(&database);
            database
                .ack_subscription_as(&principal, &name, last.clone())
                .map_err(subscription_policy_error)?;
        }
        observability::record_subscription_push(&name, last.lsn);
        if !page.has_more {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(config.poll_interval_ms.max(1))) => {}
                _ = &mut shutdown => return Ok(last),
            }
        }
    }
}

async fn run_subscription_worker(
    repl: Arc<dyn Replication>,
    catalog: Arc<BTreeMap<String, CatalogEntry>>,
    name: String,
    sender: mpsc::Sender<ChangeEvent>,
    mut shutdown: oneshot::Receiver<()>,
    config: CdcWorkerConfig,
) -> Result<ResumeToken, SubscriptionError> {
    let mut last = subscription_state(repl.as_ref(), &name)?.last_ack;
    let poll_interval = Duration::from_millis(config.poll_interval_ms.max(1));
    let deliver_timeout = Duration::from_millis(config.deliver_timeout_ms.max(1));

    loop {
        let state = subscription_state(repl.as_ref(), &name)?;
        require_trusted_subscription(&state)?;
        let page = poll_changefeed_page(
            repl.as_ref(),
            &catalog,
            &state.last_ack,
            &state.config.filter,
            &ChangefeedOptions::default(),
            config.page_size.max(1),
        )?;

        for event in page.events {
            tokio::select! {
                result = tokio::time::timeout(deliver_timeout, sender.send(event)) => {
                    result
                        .map_err(|_| SubscriptionError::BackpressureTimeout)?
                        .map_err(|_| SubscriptionError::BufferClosed)?;
                }
                _ = &mut shutdown => {
                    return Ok(last);
                }
            }
        }

        let next = page.next.clone();
        let has_more = page.has_more;
        last = next.clone();
        ack_subscription(repl.as_ref(), &name, next)?;
        observability::record_subscription_push(&name, last.lsn);
        if !has_more {
            tokio::select! {
                () = tokio::time::sleep(poll_interval) => {}
                _ = &mut shutdown => return Ok(last),
            }
        }
    }
}

/// Creates a materialized view and initializes it from the current source table.
///
/// The view replays the changefeed from LSN 0. When history GC truncated the
/// commit log, it instead starts from the current source rows and replays the
/// changes after the LSN read before that scan.
///
/// # Errors
/// Fails when the database uses AP replication, the spec is unsupported, source table is
/// missing, or metadata writes fail.
pub fn create_materialized_view(
    database: &crate::db::Database,
    spec: &MaterializedViewSpec,
) -> Result<MaterializedViewRows, MaterializedViewError> {
    ensure_view_commit_log(database)?;
    validate_view_spec(spec)?;
    let table = database
        .table(&spec.source_table)
        .map_err(|error| MaterializedViewError::Unsupported(error.to_string()))?;
    ensure_view_source_layout(&table)?;
    let previous_spec = database.read(
        MATERIALIZED_VIEWS_TABLE,
        spec.name.as_bytes(),
        ReadConsistency::Strong,
    )?;
    let previous_state = database.read(
        MATERIALIZED_VIEW_STATE_TABLE,
        spec.name.as_bytes(),
        ReadConsistency::Strong,
    )?;
    let mut state = MaterializedViewState {
        fresh_to_lsn: 0,
        source_rows: BTreeMap::new(),
        groups: BTreeMap::new(),
    };
    let mut token = ResumeToken::new(timeline_id(database)?, 0);
    if earliest_lsn(database)? > 1 {
        // LSN 0 can no longer be replayed. Take the token before the scan: replaying
        // a change the scan already saw is idempotent, because the state keeps the
        // latest row per source key.
        token = current_resume_token(database)?;
        seed_view_from_source_rows(database, spec, &mut state)?;
    }
    let filter = ChangefeedFilter {
        target: ChangefeedTarget::Table(spec.source_table.clone()),
    };
    let next = apply_view_changefeed(database, spec, &mut state, token, &filter)?;
    state.fresh_to_lsn = next.lsn;
    write_view(database, spec, &state, previous_spec, previous_state)?;
    Ok(state_to_rows(spec, &state))
}

/// Refreshes a materialized view to the database current LSN.
///
/// # Errors
/// Fails when the database uses AP replication, view metadata is corrupt, changefeed read fails,
/// or state writes fail.
pub fn refresh_materialized_view(
    database: &crate::db::Database,
    name: &str,
) -> Result<MaterializedViewRows, MaterializedViewError> {
    ensure_view_commit_log(database)?;
    let previous_spec = database
        .read(
            MATERIALIZED_VIEWS_TABLE,
            name.as_bytes(),
            ReadConsistency::Strong,
        )?
        .ok_or_else(|| MaterializedViewError::MissingView(name.to_owned()))?;
    let spec: MaterializedViewSpec = serde_json::from_slice(&previous_spec)
        .map_err(|error| MaterializedViewError::Serde(error.to_string()))?;
    let table = database
        .table(&spec.source_table)
        .map_err(|error| MaterializedViewError::Unsupported(error.to_string()))?;
    ensure_view_source_layout(&table)?;
    let previous_state = database
        .read(
            MATERIALIZED_VIEW_STATE_TABLE,
            name.as_bytes(),
            ReadConsistency::Strong,
        )?
        .ok_or_else(|| MaterializedViewError::MissingView(name.to_owned()))?;
    let mut state = decode_view_state(&previous_state, &spec)?;
    let token = ResumeToken::new(timeline_id(database)?, state.fresh_to_lsn);
    let filter = ChangefeedFilter {
        target: ChangefeedTarget::Table(spec.source_table.clone()),
    };
    let next = apply_view_changefeed(database, &spec, &mut state, token, &filter)?;
    state.fresh_to_lsn = next.lsn;
    write_view(
        database,
        &spec,
        &state,
        Some(previous_spec),
        Some(previous_state),
    )?;
    observability::record_materialized_view_refresh(name, state.fresh_to_lsn);
    Ok(state_to_rows(&spec, &state))
}

fn ensure_view_commit_log(database: &crate::db::Database) -> Result<(), MaterializedViewError> {
    if !database.supports_commit_log() {
        return Err(MaterializedViewError::Unsupported(
            "AP replication does not support incremental materialized views: AP writes are not \
             recorded in the commit log"
                .to_owned(),
        ));
    }
    Ok(())
}

fn ensure_view_source_layout(table: &crate::query::RelTable) -> Result<(), MaterializedViewError> {
    if table.layout() == crate::query::TableLayout::Columnar {
        return Err(MaterializedViewError::Unsupported(
            "incremental materialized views currently require a row-layout source table".to_owned(),
        ));
    }
    Ok(())
}

/// Reads materialized view rows.
///
/// # Errors
/// Fails when view metadata is missing or corrupt.
pub fn read_materialized_view(
    repl: &dyn Replication,
    name: &str,
) -> Result<MaterializedViewRows, MaterializedViewError> {
    let spec = read_view_spec_from_repl(repl, name)?;
    let state = read_view_state_from_repl(repl, name, &spec)?;
    Ok(state_to_rows(&spec, &state))
}

/// Persists a hook.
///
/// # Errors
/// Fails when hook metadata cannot be written.
pub fn register_hook(repl: &dyn Replication, hook: &HookSpec) -> Result<(), HookError> {
    if hook.kind == HookKind::AfterCommit {
        ensure_commit_log(repl)?;
    }
    // A hook scoped to a missing object would match nothing, so a typo would
    // silently turn a validation hook off. Refuse it instead.
    if matches!(
        hook.target,
        HookTarget::Collection(_) | HookTarget::VectorCollection(_) | HookTarget::Graph(_)
    ) && resolve_hook_scope(repl, hook)?.is_none()
    {
        return Err(HookError::InvalidTarget(format!(
            "hook {} targets {:?}, which does not exist or has another kind",
            hook.name, hook.target
        )));
    }
    let value = serde_json::to_vec(hook).map_err(|error| HookError::Serde(error.to_string()))?;
    propose_system(
        repl,
        Op::Put {
            table: HOOKS_TABLE.to_owned(),
            key: hook.name.as_bytes().to_vec(),
            value,
        },
    )?;
    Ok(())
}

/// Deletes a hook.
///
/// # Errors
/// Fails when hook metadata cannot be deleted.
pub fn unregister_hook(repl: &dyn Replication, name: &str) -> Result<(), HookError> {
    propose_system(
        repl,
        Op::Delete {
            table: HOOKS_TABLE.to_owned(),
            key: name.as_bytes().to_vec(),
        },
    )?;
    Ok(())
}

/// Reads persisted hooks.
///
/// # Errors
/// Fails when hook metadata is corrupt or unavailable.
pub fn read_hooks(repl: &dyn Replication) -> Result<Vec<HookSpec>, HookError> {
    repl.range(HOOKS_TABLE, &[], &[], ReadConsistency::Strong)?
        .into_iter()
        .map(|(_, value)| {
            serde_json::from_slice(&value).map_err(|error| HookError::Serde(error.to_string()))
        })
        .collect()
}

pub(crate) fn authorize_filter_access(
    authz: &AuthzPolicy,
    principal: &Principal,
    filter: &ChangefeedFilter,
) -> Result<(), AuthzError> {
    authorize_filter(authz, principal, filter)
}

/// Reports whether a changefeed read can return system rows, which need `System:Admin` on top
/// of the filter's own permission.
pub(crate) fn reads_system_rows(filter: &ChangefeedFilter, options: &ChangefeedOptions) -> bool {
    options.include_system || matches!(filter.target, ChangefeedTarget::System(_))
}

/// Drops the system rows from `events` unless `principal` holds `System:Admin`, so that a read
/// whose options went unchecked still cannot return them.
pub(crate) fn retain_events_visible_to(
    authz: &AuthzPolicy,
    principal: &Principal,
    mut events: Vec<ChangeEvent>,
) -> Vec<ChangeEvent> {
    if authorize_system_rows(authz, principal).is_err() {
        events.retain(|event| !matches!(event.target, LogicalTarget::System(_)));
    }
    events
}

fn authorize_system_rows(authz: &AuthzPolicy, principal: &Principal) -> Result<(), AuthzError> {
    authz.authorize(principal, &Resource::System, Permission::Admin)
}

pub(crate) fn validate_before_hooks(repl: &dyn Replication, ops: &[Op]) -> Result<(), HookError> {
    let hooks = read_hooks(repl)?;
    for hook in hooks
        .iter()
        .filter(|hook| hook.enabled && hook.kind == HookKind::BeforeCommit)
    {
        let scope = resolve_hook_scope(repl, hook)?;
        let started = Instant::now();
        for op in ops {
            if hook_matches_op(hook, scope, op) {
                apply_before_hook(hook, op)?;
            }
            if started.elapsed() > Duration::from_millis(hook.timeout_ms) {
                return Err(HookError::Timeout(hook.name.clone()));
            }
        }
    }
    Ok(())
}

/// Delivers after-commit hooks for the commits in `start..=end`. The caller has
/// already committed the write, so an error here must be recorded, not returned
/// to the writer.
/// Reports whether any enabled after-commit hook is registered.
pub(crate) fn has_enabled_after_commit_hooks(repl: &dyn Replication) -> Result<bool, HookError> {
    Ok(read_hooks(repl)?
        .iter()
        .any(|hook| hook.enabled && hook.kind == HookKind::AfterCommit))
}

pub(crate) fn deliver_after_commit_hooks(
    repl: &dyn Replication,
    start: Lsn,
    end: Lsn,
) -> Result<(), HookError> {
    deliver_after_hooks(repl, start, end)
}

fn deliver_after_hooks(repl: &dyn Replication, start: Lsn, end: Lsn) -> Result<(), HookError> {
    if end <= start {
        return Ok(());
    }
    let hooks = read_hooks(repl)?;
    let after_hooks = hooks
        .into_iter()
        .filter(|hook| hook.enabled && hook.kind == HookKind::AfterCommit)
        .collect::<Vec<_>>();
    if after_hooks.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    if let Some(hook) = after_hooks
        .iter()
        .find(|hook| hook.name == FAIL_AFTER_HOOK_FOR_TESTS)
    {
        return Err(HookError::Rejected(format!(
            "{}: injected after-hook failure",
            hook.name
        )));
    }

    let records = commit_records_between(repl, start, end).map_err(|error| match error {
        FeedError::Repl(error) => HookError::Repl(error),
        FeedError::Storage(error) => HookError::Repl(ReplError::Storage(error)),
        FeedError::Serde(error)
        | FeedError::TimelineMismatch {
            expected: error, ..
        } => HookError::Serde(error),
        FeedError::TokenExpired {
            requested,
            earliest,
        } => HookError::Serde(format!(
            "after-hook token expired while reading {requested}, earliest {earliest}"
        )),
        FeedError::TimelineForked {
            requested,
            current,
            fork_lsn,
        } => HookError::Serde(format!(
            "after-hook timeline forked while reading {requested} on {current} at {fork_lsn}"
        )),
    })?;
    let mut ops = Vec::new();
    for hook in after_hooks {
        let scope = resolve_hook_scope(repl, &hook)?;
        for record in &records {
            let event_count = record
                .writes
                .iter()
                .filter(|write| hook_matches_parts(&hook, scope, &write.table, &write.key))
                .count();
            if event_count == 0 {
                continue;
            }
            let delivery = HookDelivery {
                hook_name: hook.name.clone(),
                lsn: record.txn_id,
                delivered_at: SystemTime::now(),
                event_count,
            };
            ops.push(Op::Put {
                table: HOOK_DELIVERIES_TABLE.to_owned(),
                key: hook_delivery_key(&hook.name, record.txn_id),
                value: serde_json::to_vec(&delivery)
                    .map_err(|error| HookError::Serde(error.to_string()))?,
            });
        }
    }
    if !ops.is_empty() {
        propose_system_batch(repl, ops)?;
    }
    Ok(())
}

/// Records post-commit failures: a warning and a metric for each one, then one
/// best-effort system write to [`POST_COMMIT_FAILURES_TABLE`]. Never fails, because
/// the write that caused the failures is already committed.
pub(crate) fn record_post_commit_failures(repl: &dyn Replication, failures: &[PostCommitFailure]) {
    let mut ops = Vec::with_capacity(failures.len());
    for failure in failures {
        tracing::warn!(
            stage = failure.stage.as_str(),
            name = %failure.name,
            start_lsn = failure.start_lsn,
            end_lsn = failure.end_lsn,
            error = %failure.error,
            "post-commit stage failed after a durable commit"
        );
        observability::record_post_commit_failure(failure.stage.as_str());
        match serde_json::to_vec(failure) {
            Ok(value) => ops.push(Op::Put {
                table: POST_COMMIT_FAILURES_TABLE.to_owned(),
                key: Uuid::now_v7().as_bytes().to_vec(),
                value,
            }),
            Err(error) => {
                tracing::error!(
                    error = %error,
                    name = %failure.name,
                    "post-commit failure was not recorded"
                );
                observability::record_post_commit_failure("unrecorded");
            }
        }
    }
    if ops.is_empty() {
        return;
    }

    let count = ops.len();
    if let Err(error) = propose_system_batch(repl, ops) {
        tracing::error!(error = %error, count, "post-commit failures were not recorded");
        for _ in 0..count {
            observability::record_post_commit_failure("unrecorded");
        }
    }
}

/// Reads recorded post-commit failures, oldest first.
///
/// # Errors
/// Fails when the records cannot be read or decoded.
pub fn read_post_commit_failures(
    repl: &dyn Replication,
) -> Result<Vec<PostCommitFailure>, HookError> {
    repl.range(
        POST_COMMIT_FAILURES_TABLE,
        &[],
        &[],
        ReadConsistency::Strong,
    )?
    .into_iter()
    .map(|(_, value)| {
        serde_json::from_slice(&value).map_err(|error| HookError::Serde(error.to_string()))
    })
    .collect()
}

/// Resolves the catalog id a `Collection`, `VectorCollection` or `Graph` hook is
/// scoped to. `None` means the object is missing or has another kind, so the hook
/// matches nothing. Other targets match by name and need no lookup.
fn resolve_hook_scope(repl: &dyn Replication, hook: &HookSpec) -> Result<Option<u32>, HookError> {
    let (HookTarget::Collection(name)
    | HookTarget::VectorCollection(name)
    | HookTarget::Graph(name)) = &hook.target
    else {
        return Ok(None);
    };
    let entry = read_catalog_entry_from_repl(repl, name).map_err(|error| match error {
        ConfigError::Repl(error) => HookError::Repl(error),
        other => HookError::Serde(format!(
            "hook {} cannot resolve its target {name}: {other}",
            hook.name
        )),
    })?;
    Ok(match (&hook.target, entry) {
        (HookTarget::Collection(_), Some(CatalogEntry::Collection { collection_id, .. }))
        | (HookTarget::VectorCollection(_), Some(CatalogEntry::Vector { collection_id, .. })) => {
            Some(collection_id.as_u32())
        }
        (HookTarget::Graph(_), Some(CatalogEntry::Graph { graph_id })) => Some(graph_id.as_u32()),
        _ => None,
    })
}

fn apply_before_hook(hook: &HookSpec, op: &Op) -> Result<(), HookError> {
    match (&hook.action, op) {
        (HookAction::RejectEmptyValue, Op::Put { value, .. }) if value.is_empty() => {
            Err(HookError::Rejected(hook.name.clone()))
        }
        (HookAction::RequireJsonObject, Op::Put { value, .. }) => match decode_value(value) {
            Ok(Value::Object(_)) => Ok(()),
            Ok(_) => Err(HookError::Rejected(format!(
                "{} expected JSON object",
                hook.name
            ))),
            Err(error) => Err(HookError::Rejected(format!("{}: {error}", hook.name))),
        },
        (HookAction::MaxValueBytes(limit), Op::Put { value, .. }) if value.len() > *limit => Err(
            HookError::Rejected(format!("{} value exceeds {limit} bytes", hook.name)),
        ),
        (HookAction::RejectKeyPrefix(prefix), Op::Put { key, .. } | Op::Delete { key, .. })
            if key.starts_with(prefix) =>
        {
            Err(HookError::Rejected(format!(
                "{} rejected key prefix",
                hook.name
            )))
        }
        _ => Ok(()),
    }
}

fn events_for_record(
    record: &CommitLogRecord,
    catalog: &BTreeMap<String, CatalogEntry>,
    filter: &ChangefeedFilter,
    options: &ChangefeedOptions,
) -> Vec<ChangeEvent> {
    record
        .writes
        .iter()
        .filter_map(|write| {
            let target = logical_target_for(&write.table, &write.key, catalog);
            if !options.include_system && matches!(target, LogicalTarget::System(_)) {
                return None;
            }
            if !filter_matches(filter, &target) {
                return None;
            }
            let op = change_op_for(&write.table, &write.key, write.value.as_ref());
            Some(ChangeEvent {
                lsn: record.txn_id,
                tx_id: record.txn_id,
                at: record.committed_at,
                hlc: record.hlc,
                target,
                op,
                transaction_boundary: TransactionBoundary::Data,
            })
        })
        .collect()
}

fn change_op_for(table: &str, key: &[u8], value: Option<&Bytes>) -> ChangeOp {
    if table == "__catalog__" || table == crate::query::REL_SCHEMA_TABLE {
        return ChangeOp::Ddl {
            name: String::from_utf8_lossy(key).into_owned(),
        };
    }

    match value {
        Some(value) => ChangeOp::Upsert {
            key: key.to_vec(),
            value_after: value.clone(),
        },
        None => ChangeOp::Delete { key: key.to_vec() },
    }
}

fn logical_target_for(
    table: &str,
    key: &[u8],
    catalog: &BTreeMap<String, CatalogEntry>,
) -> LogicalTarget {
    match table {
        REL_ROWS_TABLE | REL_COLUMNAR_SEGMENTS_TABLE => parse_table_name_from_key(key).map_or_else(
            || LogicalTarget::System(table.to_owned()),
            LogicalTarget::Table,
        ),
        DOCUMENT_TABLE => parse_collection_id(key)
            .and_then(|id| collection_name_for(catalog, id))
            .map_or_else(
                || LogicalTarget::System(table.to_owned()),
                LogicalTarget::Collection,
            ),
        VECTOR_TABLE => parse_collection_id(key)
            .and_then(|id| vector_name_for(catalog, id))
            .map_or_else(
                || LogicalTarget::System(table.to_owned()),
                LogicalTarget::VectorCollection,
            ),
        FULL_TEXT_DOCS_TABLE | FULL_TEXT_POSTINGS_TABLE => parse_len_prefixed_name(key)
            .map_or_else(
                || LogicalTarget::System(table.to_owned()),
                LogicalTarget::FullTextIndex,
            ),
        FULL_TEXT_META_TABLE => parse_full_text_meta_name(key).map_or_else(
            || LogicalTarget::System(table.to_owned()),
            LogicalTarget::FullTextIndex,
        ),
        TIME_SERIES_CHUNKS_TABLE | TIME_SERIES_LATEST_TABLE => parse_len_prefixed_name(key)
            .map_or_else(
                || LogicalTarget::System(table.to_owned()),
                LogicalTarget::TimeSeries,
            ),
        TIME_SERIES_META_TABLE => String::from_utf8(key.to_vec()).map_or_else(
            |_| LogicalTarget::System(table.to_owned()),
            LogicalTarget::TimeSeries,
        ),
        GRAPH_OUT_EDGES_TABLE | GRAPH_IN_EDGES_TABLE => parse_graph_id(key)
            .and_then(|id| graph_name_for(catalog, id))
            .map_or_else(
                || LogicalTarget::System(table.to_owned()),
                LogicalTarget::Graph,
            ),
        GEO_POINTS_TABLE | GEO_INDEX_TABLE => parse_len_prefixed_name(key).map_or_else(
            || LogicalTarget::System(table.to_owned()),
            LogicalTarget::GeoIndex,
        ),
        "__catalog__" | crate::query::REL_SCHEMA_TABLE => LogicalTarget::Database,
        REL_INDEX_TABLE | INDEX_TABLE => LogicalTarget::System(table.to_owned()),
        other if other.starts_with("__") => LogicalTarget::System(other.to_owned()),
        other => LogicalTarget::System(other.to_owned()),
    }
}

fn filter_matches(filter: &ChangefeedFilter, target: &LogicalTarget) -> bool {
    match (&filter.target, target) {
        (ChangefeedTarget::All, _) => true,
        (ChangefeedTarget::Table(expected), LogicalTarget::Table(actual))
        | (ChangefeedTarget::Collection(expected), LogicalTarget::Collection(actual))
        | (ChangefeedTarget::VectorCollection(expected), LogicalTarget::VectorCollection(actual))
        | (ChangefeedTarget::FullTextIndex(expected), LogicalTarget::FullTextIndex(actual))
        | (ChangefeedTarget::TimeSeries(expected), LogicalTarget::TimeSeries(actual))
        | (ChangefeedTarget::Graph(expected), LogicalTarget::Graph(actual))
        | (ChangefeedTarget::GeoIndex(expected), LogicalTarget::GeoIndex(actual))
        | (ChangefeedTarget::System(expected), LogicalTarget::System(actual)) => expected == actual,
        _ => false,
    }
}

fn parse_collection_id(key: &[u8]) -> Option<u32> {
    let prefix = key.get(..4)?;
    Some(u32::from_be_bytes(prefix.try_into().ok()?))
}

fn parse_graph_id(key: &[u8]) -> Option<u32> {
    parse_collection_id(key)
}

fn parse_len_prefixed_name(key: &[u8]) -> Option<String> {
    let mut cursor = 0;
    let name = crate::keyenc::read_len_bytes(key, &mut cursor)?;
    String::from_utf8(name.to_vec()).ok()
}

fn parse_full_text_meta_name(key: &[u8]) -> Option<String> {
    let raw = key
        .strip_prefix(b"config:")
        .or_else(|| key.strip_prefix(b"state:"))?;
    String::from_utf8(raw.to_vec()).ok()
}

fn parse_table_name_from_key(key: &[u8]) -> Option<String> {
    let len = u64::from_be_bytes(key.get(..8)?.try_into().ok()?);
    let len = usize::try_from(len).ok()?;
    let name = key.get(8..8 + len)?;
    String::from_utf8(name.to_vec()).ok()
}

fn collection_name_for(catalog: &BTreeMap<String, CatalogEntry>, id: u32) -> Option<String> {
    catalog.iter().find_map(|(name, entry)| match entry {
        CatalogEntry::Collection { collection_id, .. } if collection_id.as_u32() == id => {
            Some(name.clone())
        }
        _ => None,
    })
}

fn vector_name_for(catalog: &BTreeMap<String, CatalogEntry>, id: u32) -> Option<String> {
    catalog.iter().find_map(|(name, entry)| match entry {
        CatalogEntry::Vector { collection_id, .. } if collection_id.as_u32() == id => {
            Some(name.clone())
        }
        _ => None,
    })
}

fn graph_name_for(catalog: &BTreeMap<String, CatalogEntry>, id: u32) -> Option<String> {
    catalog.iter().find_map(|(name, entry)| match entry {
        CatalogEntry::Graph { graph_id } if graph_id.as_u32() == id => Some(name.clone()),
        _ => None,
    })
}

fn authorize_filter(
    authz: &AuthzPolicy,
    principal: &Principal,
    filter: &ChangefeedFilter,
) -> Result<(), AuthzError> {
    match &filter.target {
        ChangefeedTarget::All => authz.authorize(principal, &Resource::Database, Permission::Read),
        ChangefeedTarget::Table(name) => {
            authz.authorize(principal, &Resource::Table(name.clone()), Permission::Read)
        }
        ChangefeedTarget::Collection(name) => authz.authorize(
            principal,
            &Resource::Collection(name.clone()),
            Permission::Read,
        ),
        ChangefeedTarget::VectorCollection(name) => authz.authorize(
            principal,
            &Resource::VectorCollection(name.clone()),
            Permission::Read,
        ),
        ChangefeedTarget::FullTextIndex(name) => authz.authorize(
            principal,
            &Resource::FullTextIndex(name.clone()),
            Permission::Read,
        ),
        ChangefeedTarget::TimeSeries(name) => authz.authorize(
            principal,
            &Resource::TimeSeries(name.clone()),
            Permission::Read,
        ),
        ChangefeedTarget::Graph(name) => {
            authz.authorize(principal, &Resource::Graph(name.clone()), Permission::Read)
        }
        ChangefeedTarget::GeoIndex(name) => authz.authorize(
            principal,
            &Resource::GeoIndex(name.clone()),
            Permission::Read,
        ),
        ChangefeedTarget::System(_) => authorize_system_rows(authz, principal),
    }
}

#[cfg(test)]
fn write_subscription(
    repl: &dyn Replication,
    state: &SubscriptionState,
) -> Result<(), SubscriptionError> {
    let value =
        serde_json::to_vec(state).map_err(|error| SubscriptionError::Serde(error.to_string()))?;
    propose_system(
        repl,
        Op::Put {
            table: CDC_SUBSCRIPTIONS_TABLE.to_owned(),
            key: state.config.name.as_bytes().to_vec(),
            value,
        },
    )?;
    Ok(())
}

fn read_subscription(
    repl: &dyn Replication,
    name: &str,
) -> Result<Option<SubscriptionState>, SubscriptionError> {
    Ok(subscription_snapshot(repl, name)?.map(|(state, _)| state))
}

pub(crate) fn subscription_snapshot(
    repl: &dyn Replication,
    name: &str,
) -> Result<Option<(SubscriptionState, Bytes)>, SubscriptionError> {
    let Some(bytes) = repl.read(
        CDC_SUBSCRIPTIONS_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Ok(None);
    };
    let state = serde_json::from_slice(&bytes)
        .map_err(|error| SubscriptionError::Serde(error.to_string()))?;
    Ok(Some((state, bytes)))
}

pub(crate) fn write_subscription_if_unchanged(
    repl: &dyn Replication,
    state: &SubscriptionState,
    expected: Option<Bytes>,
) -> Result<(), SubscriptionError> {
    propose_system_conditional_batch(
        repl,
        ConditionalBatch::new(
            vec![WriteCondition::ValueEquals {
                table: CDC_SUBSCRIPTIONS_TABLE.to_owned(),
                key: state.config.name.as_bytes().to_vec(),
                expected,
            }],
            vec![Op::Put {
                table: CDC_SUBSCRIPTIONS_TABLE.to_owned(),
                key: state.config.name.as_bytes().to_vec(),
                value: serde_json::to_vec(state)
                    .map_err(|error| SubscriptionError::Serde(error.to_string()))?,
            }],
        ),
    )?;
    Ok(())
}

/// Reads the id of the last committed transaction; every commit advances it.
pub(crate) fn current_lsn<R: Replication + ?Sized>(repl: &R) -> Result<Lsn, ReplError> {
    let Some(bytes) = repl.read(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        ReadConsistency::Strong,
    )?
    else {
        return Ok(0);
    };
    Ok(txn::decode_txn_id(&bytes)?)
}

fn timeline_id<R: Replication + ?Sized>(repl: &R) -> Result<TimelineId, FeedError> {
    if let Some(bytes) = repl.read(CDC_META_TABLE, KEY_TIMELINE_ID, ReadConsistency::Strong)? {
        return String::from_utf8(bytes).map_err(|error| FeedError::Serde(error.to_string()));
    }
    if let Some(bytes) = repl.read(BACKUP_META_TABLE, KEY_TIMELINE_ID, ReadConsistency::Strong)? {
        return String::from_utf8(bytes).map_err(|error| FeedError::Serde(error.to_string()));
    }
    Ok(DEFAULT_TIMELINE_ID.to_owned())
}

fn map_resume_token_to_current_timeline<R: Replication + ?Sized>(
    repl: &R,
    token: &ResumeToken,
    current: &TimelineId,
) -> Result<ResumeToken, FeedError> {
    if &token.timeline_id == current {
        return Ok(token.clone());
    }

    let Some(record) = read_timeline_record(repl, current)? else {
        return Err(FeedError::TimelineMismatch {
            expected: current.clone(),
            found: token.timeline_id.clone(),
        });
    };

    if record.parent.as_ref() == Some(&token.timeline_id) && token.lsn <= record.fork_lsn {
        return Ok(ResumeToken::new(current.clone(), token.lsn));
    }

    Err(FeedError::TimelineForked {
        requested: token.timeline_id.clone(),
        current: current.clone(),
        fork_lsn: record.fork_lsn,
    })
}

fn read_timeline_record<R: Replication + ?Sized>(
    repl: &R,
    timeline_id: &str,
) -> Result<Option<CdcTimelineRecord>, FeedError> {
    repl.read(
        CDC_TIMELINES_TABLE,
        timeline_id.as_bytes(),
        ReadConsistency::Strong,
    )?
    .map(|bytes| {
        serde_json::from_slice(&bytes).map_err(|error| FeedError::Serde(error.to_string()))
    })
    .transpose()
}

fn earliest_lsn<R: Replication + ?Sized>(repl: &R) -> Result<Lsn, FeedError> {
    Ok(txn::history::earliest_commit_lsn(repl)?)
}

/// Re-checks the retained log floor when the page does not start right after
/// `from`, so a truncation racing with this poll can never silently skip records.
fn ensure_no_truncated_prefix<R: Replication + ?Sized>(
    repl: &R,
    from: &ResumeToken,
    current: Lsn,
    page: &txn::history::CommitRecordsPage,
) -> Result<(), FeedError> {
    if from.lsn >= current {
        return Ok(());
    }
    let expected = from
        .lsn
        .checked_add(1)
        .ok_or_else(|| FeedError::Serde("lsn overflow".to_owned()))?;
    if page
        .records
        .first()
        .is_some_and(|record| record.txn_id == expected)
    {
        return Ok(());
    }
    let earliest = earliest_lsn(repl)?;
    if expected < earliest {
        return Err(FeedError::TokenExpired {
            requested: from.lsn,
            earliest,
        });
    }
    Ok(())
}

/// Reads every commit record in `(start, end]`, failing closed instead of
/// skipping LSNs that history GC truncated or that are missing from the log.
fn commit_records_between<R: Replication + ?Sized>(
    repl: &R,
    start: Lsn,
    end: Lsn,
) -> Result<Vec<CommitLogRecord>, FeedError> {
    if end <= start {
        return Ok(Vec::new());
    }
    let first = start
        .checked_add(1)
        .ok_or_else(|| FeedError::Serde("lsn overflow".to_owned()))?;
    ensure_commit_log_retained(repl, start)?;
    let start_key = txn::commit_log_key(first);
    let end_key = end.checked_add(1).map_or([0xFF; 8], txn::commit_log_key);
    let records = repl
        .range(
            txn::COMMIT_LOG_TABLE,
            &start_key,
            &end_key,
            ReadConsistency::Strong,
        )?
        .into_iter()
        .map(|(_, value)| txn::decode_commit_log_record(&value).map_err(FeedError::from))
        .collect::<Result<Vec<_>, _>>()?;
    if records.iter().map(|record| record.txn_id).eq(first..=end) {
        return Ok(records);
    }
    // A truncation that committed after the first check also leaves a gap.
    ensure_commit_log_retained(repl, start)?;
    Err(FeedError::Storage(StorageError::Corruption(format!(
        "commit log is missing records in {first}..={end}"
    ))))
}

fn ensure_commit_log_retained<R: Replication + ?Sized>(
    repl: &R,
    start: Lsn,
) -> Result<(), FeedError> {
    let earliest = earliest_lsn(repl)?;
    if start.saturating_add(1) < earliest {
        return Err(FeedError::TokenExpired {
            requested: start,
            earliest,
        });
    }
    Ok(())
}

fn validate_view_spec(spec: &MaterializedViewSpec) -> Result<(), MaterializedViewError> {
    if spec.name.trim().is_empty() || spec.source_table.trim().is_empty() {
        return Err(MaterializedViewError::Unsupported(
            "view name and source table are required".to_owned(),
        ));
    }
    if spec.aggregates.is_empty() {
        return Err(MaterializedViewError::Unsupported(
            "materialized view needs at least one aggregate".to_owned(),
        ));
    }
    for aggregate in &spec.aggregates {
        match aggregate.kind {
            AggregateKind::Count => {}
            AggregateKind::Sum | AggregateKind::Avg if aggregate.column.is_some() => {}
            AggregateKind::Sum | AggregateKind::Avg => {
                return Err(MaterializedViewError::Unsupported(format!(
                    "{:?} requires a source column",
                    aggregate.kind
                )));
            }
        }
    }
    Ok(())
}

fn write_view(
    repl: &dyn Replication,
    spec: &MaterializedViewSpec,
    state: &MaterializedViewState,
    previous_spec: Option<Bytes>,
    previous_state: Option<Bytes>,
) -> Result<(), MaterializedViewError> {
    propose_system_conditional_batch(
        repl,
        ConditionalBatch::new(
            vec![
                WriteCondition::ValueEquals {
                    table: MATERIALIZED_VIEWS_TABLE.to_owned(),
                    key: spec.name.as_bytes().to_vec(),
                    expected: previous_spec,
                },
                WriteCondition::ValueEquals {
                    table: MATERIALIZED_VIEW_STATE_TABLE.to_owned(),
                    key: spec.name.as_bytes().to_vec(),
                    expected: previous_state,
                },
            ],
            vec![
                Op::Put {
                    table: MATERIALIZED_VIEWS_TABLE.to_owned(),
                    key: spec.name.as_bytes().to_vec(),
                    value: serde_json::to_vec(spec)
                        .map_err(|error| MaterializedViewError::Serde(error.to_string()))?,
                },
                Op::Put {
                    table: MATERIALIZED_VIEW_STATE_TABLE.to_owned(),
                    key: spec.name.as_bytes().to_vec(),
                    value: serde_json::to_vec(state)
                        .map_err(|error| MaterializedViewError::Serde(error.to_string()))?,
                },
            ],
        ),
    )?;
    Ok(())
}

fn read_view_spec_from_repl(
    repl: &dyn Replication,
    name: &str,
) -> Result<MaterializedViewSpec, MaterializedViewError> {
    let Some(bytes) = repl.read(
        MATERIALIZED_VIEWS_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Err(MaterializedViewError::MissingView(name.to_owned()));
    };
    serde_json::from_slice(&bytes).map_err(|error| MaterializedViewError::Serde(error.to_string()))
}

fn read_view_state_from_repl(
    repl: &dyn Replication,
    name: &str,
    spec: &MaterializedViewSpec,
) -> Result<MaterializedViewState, MaterializedViewError> {
    let Some(bytes) = repl.read(
        MATERIALIZED_VIEW_STATE_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Err(MaterializedViewError::MissingView(name.to_owned()));
    };
    decode_view_state(&bytes, spec)
}

fn decode_view_state(
    bytes: &[u8],
    spec: &MaterializedViewSpec,
) -> Result<MaterializedViewState, MaterializedViewError> {
    let mut state: MaterializedViewState = serde_json::from_slice(bytes)
        .map_err(|error| MaterializedViewError::Serde(error.to_string()))?;
    if state.groups.values().any(|group| group.row_count.is_none()) {
        for group in state.groups.values_mut() {
            group.row_count = Some(0);
        }
        for row in state
            .source_rows
            .values()
            .filter(|row| row_matches(spec, row))
        {
            let value = row.get(spec.group_by).ok_or_else(|| {
                MaterializedViewError::Unsupported("group_by column is out of range".to_owned())
            })?;
            if let Some(group) = state.groups.get_mut(&group_key(value)?) {
                let count = group.row_count.get_or_insert(0);
                *count = count.checked_add(1).ok_or_else(|| {
                    MaterializedViewError::Unsupported(
                        "materialized group row count overflow".to_owned(),
                    )
                })?;
            }
        }
        state.groups.retain(|_, group| !group_is_empty(group));
    }
    Ok(state)
}

fn apply_view_changefeed(
    database: &crate::db::Database,
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
    mut token: ResumeToken,
    filter: &ChangefeedFilter,
) -> Result<ResumeToken, MaterializedViewError> {
    // New writes must not keep extending an in-flight refresh indefinitely.
    let through = current_lsn(database)?;
    loop {
        let page = poll_changefeed_page_through(
            database,
            database.catalog(),
            &token,
            filter,
            &ChangefeedOptions::default(),
            MATERIALIZED_VIEW_CDC_WINDOW,
            through,
        )?;
        for event in page.events {
            apply_view_event(spec, state, &event)?;
        }
        token = page.next;
        if !page.has_more {
            return Ok(token);
        }
    }
}

/// Applies every stored row of the source table as an upsert, matching what a
/// changefeed replay from LSN 0 leaves behind.
fn seed_view_from_source_rows(
    database: &crate::db::Database,
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
) -> Result<(), MaterializedViewError> {
    let prefix = crate::temporal::table_prefix(&spec.source_table);
    let end = crate::keyenc::range_end(&prefix);
    for table in [REL_ROWS_TABLE, REL_COLUMNAR_SEGMENTS_TABLE] {
        for (key, value) in database.range(table, &prefix, &end, ReadConsistency::Strong)? {
            apply_view_upsert(spec, state, &key, &value)?;
        }
    }
    Ok(())
}

fn apply_view_event(
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
    event: &ChangeEvent,
) -> Result<(), MaterializedViewError> {
    let ChangeOp::Upsert { key, value_after } = &event.op else {
        if let ChangeOp::Delete { key } = &event.op
            && let Some(old) = state.source_rows.remove(&state_key(key))
        {
            remove_row_from_groups(spec, state, &old)?;
        }
        return Ok(());
    };
    apply_view_upsert(spec, state, key, value_after)
}

fn apply_view_upsert(
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
    key: &[u8],
    value_after: &[u8],
) -> Result<(), MaterializedViewError> {
    let Some(row) = decode_row_bytes(value_after)? else {
        return Ok(());
    };
    if let Some(old) = state.source_rows.insert(state_key(key), row.clone()) {
        remove_row_from_groups(spec, state, &old)?;
    }
    add_row_to_groups(spec, state, &row)?;
    Ok(())
}

fn add_row_to_groups(
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
    row: &Row,
) -> Result<(), MaterializedViewError> {
    if !row_matches(spec, row) {
        return Ok(());
    }
    let group_value = row.get(spec.group_by).cloned().ok_or_else(|| {
        MaterializedViewError::Unsupported("group_by column is out of range".to_owned())
    })?;
    let key = group_key(&group_value)?;
    let group = state.groups.entry(key).or_insert_with(|| GroupState {
        group_value,
        aggregates: initial_aggregates(&spec.aggregates),
        row_count: Some(0),
    });
    let count = group.row_count.get_or_insert(0);
    *count = count.checked_add(1).ok_or_else(|| {
        MaterializedViewError::Unsupported("materialized group row count overflow".to_owned())
    })?;
    apply_aggregate_delta(&spec.aggregates, &mut group.aggregates, row, 1.0, 1)
}

fn remove_row_from_groups(
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
    row: &Row,
) -> Result<(), MaterializedViewError> {
    if !row_matches(spec, row) {
        return Ok(());
    }
    let Some(group_value) = row.get(spec.group_by) else {
        return Ok(());
    };
    let key = group_key(group_value)?;
    if let Some(group) = state.groups.get_mut(&key) {
        apply_aggregate_delta(&spec.aggregates, &mut group.aggregates, row, -1.0, -1)?;
        let count = group.row_count.get_or_insert(0);
        *count = count.checked_sub(1).ok_or_else(|| {
            MaterializedViewError::Unsupported("materialized group row count underflow".to_owned())
        })?;
    }
    if state.groups.get(&key).is_some_and(group_is_empty) {
        state.groups.remove(&key);
    }
    Ok(())
}

fn row_matches(spec: &MaterializedViewSpec, row: &Row) -> bool {
    spec.filter.as_ref().is_none_or(|predicate| {
        row.get(predicate.column)
            .is_some_and(|value| value == &predicate.equals)
    })
}

fn initial_aggregates(specs: &[AggregateSpec]) -> Vec<AggregateValue> {
    specs
        .iter()
        .map(|spec| match spec.kind {
            AggregateKind::Count => AggregateValue::Count(0),
            AggregateKind::Sum => AggregateValue::Sum(0.0),
            AggregateKind::Avg => AggregateValue::Avg { sum: 0.0, count: 0 },
        })
        .collect()
}

fn apply_aggregate_delta(
    specs: &[AggregateSpec],
    values: &mut [AggregateValue],
    row: &Row,
    numeric_sign: f64,
    count_sign: i64,
) -> Result<(), MaterializedViewError> {
    for (spec, value) in specs.iter().zip(values.iter_mut()) {
        match (spec.kind, value) {
            (AggregateKind::Count, AggregateValue::Count(count)) => {
                *count = count.saturating_add(count_sign);
            }
            (AggregateKind::Sum, AggregateValue::Sum(sum)) => {
                let updated = *sum + numeric_value(row, spec.column)? * numeric_sign;
                if !updated.is_finite() {
                    return Err(MaterializedViewError::Unsupported(
                        "aggregate sum exceeds the finite f64 range".to_owned(),
                    ));
                }
                *sum = updated;
            }
            (AggregateKind::Avg, AggregateValue::Avg { sum, count }) => {
                let updated = *sum + numeric_value(row, spec.column)? * numeric_sign;
                if !updated.is_finite() {
                    return Err(MaterializedViewError::Unsupported(
                        "aggregate sum exceeds the finite f64 range".to_owned(),
                    ));
                }
                *sum = updated;
                *count = count.saturating_add(count_sign);
            }
            _ => {
                return Err(MaterializedViewError::Unsupported(
                    "aggregate state does not match spec".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn numeric_value(row: &Row, column: Option<usize>) -> Result<f64, MaterializedViewError> {
    let column = column.ok_or_else(|| {
        MaterializedViewError::Unsupported("aggregate column is required".to_owned())
    })?;
    match row.get(column) {
        Some(Value::Int(value)) => value
            .to_string()
            .parse::<f64>()
            .map_err(|error| MaterializedViewError::Serde(error.to_string())),
        Some(Value::Float(value)) if value.is_finite() => Ok(*value),
        Some(_) => Err(MaterializedViewError::Unsupported(
            "aggregate column must be numeric".to_owned(),
        )),
        None => Err(MaterializedViewError::Unsupported(
            "aggregate column is out of range".to_owned(),
        )),
    }
}

fn group_key(value: &Value) -> Result<String, MaterializedViewError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| MaterializedViewError::Serde(error.to_string()))?;
    Ok(hex_bytes(&bytes))
}

fn group_is_empty(group: &GroupState) -> bool {
    group.row_count == Some(0)
}

fn state_to_rows(
    spec: &MaterializedViewSpec,
    state: &MaterializedViewState,
) -> MaterializedViewRows {
    let mut rows = state
        .groups
        .values()
        .map(|group| {
            let mut row = vec![group.group_value.clone()];
            row.extend(
                spec.aggregates
                    .iter()
                    .zip(&group.aggregates)
                    .map(|(_, value)| aggregate_to_value(value)),
            );
            row
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| serde_json::to_vec(row).unwrap_or_default());
    MaterializedViewRows {
        fresh_to_lsn: state.fresh_to_lsn,
        rows,
    }
}

fn aggregate_to_value(value: &AggregateValue) -> Value {
    match value {
        AggregateValue::Count(count) => Value::Int(*count),
        AggregateValue::Sum(sum) => Value::Float(*sum),
        AggregateValue::Avg { sum, count } if *count > 0 => {
            let divisor = count.to_string().parse::<f64>().unwrap_or(f64::INFINITY);
            Value::Float(*sum / divisor)
        }
        AggregateValue::Avg { .. } => Value::Null,
    }
}

fn hook_matches_op(hook: &HookSpec, scope: Option<u32>, op: &Op) -> bool {
    match op {
        Op::Put { table, key, .. } | Op::Delete { table, key } => {
            hook_matches_parts(hook, scope, table, key)
        }
    }
}

/// `scope` is the resolved catalog id for `Collection`, `VectorCollection` and
/// `Graph` targets (see [`resolve_hook_scope`]).
fn hook_matches_parts(hook: &HookSpec, scope: Option<u32>, table: &str, key: &[u8]) -> bool {
    let in_scope = |id: Option<u32>| scope.is_some() && id == scope;
    match &hook.target {
        HookTarget::AnyUser => !table.starts_with("__") && !is_derived_index_table(table),
        HookTarget::Keyspace(expected) => expected == table,
        HookTarget::Table(expected) => {
            matches!(table, REL_ROWS_TABLE | REL_COLUMNAR_SEGMENTS_TABLE)
                && parse_table_name_from_key(key).is_some_and(|actual| &actual == expected)
        }
        HookTarget::Collection(_) => table == DOCUMENT_TABLE && in_scope(parse_collection_id(key)),
        HookTarget::VectorCollection(_) => {
            table == VECTOR_TABLE && in_scope(parse_collection_id(key))
        }
        HookTarget::FullTextIndex(expected) => {
            matches!(table, FULL_TEXT_DOCS_TABLE | FULL_TEXT_POSTINGS_TABLE)
                && parse_len_prefixed_name(key).is_some_and(|actual| &actual == expected)
        }
        HookTarget::TimeSeries(expected) => {
            matches!(table, TIME_SERIES_CHUNKS_TABLE | TIME_SERIES_LATEST_TABLE)
                && parse_len_prefixed_name(key).is_some_and(|actual| &actual == expected)
        }
        HookTarget::Graph(_) => {
            matches!(table, GRAPH_OUT_EDGES_TABLE | GRAPH_IN_EDGES_TABLE)
                && in_scope(parse_graph_id(key))
        }
        HookTarget::GeoIndex(expected) => {
            matches!(table, GEO_POINTS_TABLE | GEO_INDEX_TABLE)
                && parse_len_prefixed_name(key).is_some_and(|actual| &actual == expected)
        }
    }
}

fn is_derived_index_table(table: &str) -> bool {
    matches!(
        table,
        REL_INDEX_TABLE
            | INDEX_TABLE
            | FULL_TEXT_DOCS_TABLE
            | FULL_TEXT_POSTINGS_TABLE
            | GEO_POINTS_TABLE
            | GEO_INDEX_TABLE
    )
}

fn hook_delivery_key(name: &str, lsn: Lsn) -> Bytes {
    let mut key = Vec::with_capacity(8 + name.len() + 8);
    key.extend_from_slice(&(name.len() as u64).to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key.extend_from_slice(&lsn.to_be_bytes());
    key
}

fn state_key(key: &[u8]) -> String {
    hex_bytes(key)
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0F)]));
    }
    out
}

impl From<FeedError> for DbError {
    fn from(error: FeedError) -> Self {
        Self::Cdc(error)
    }
}

impl From<SubscriptionError> for DbError {
    fn from(error: SubscriptionError) -> Self {
        Self::Subscription(error)
    }
}

impl From<MaterializedViewError> for DbError {
    fn from(error: MaterializedViewError) -> Self {
        Self::MaterializedView(error)
    }
}

impl From<HookError> for DbError {
    fn from(error: HookError) -> Self {
        Self::Hook(error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        AggregateKind, AggregateSpec, ChangeOp, ChangefeedFilter, ChangefeedOptions,
        ChangefeedTarget, HookAction, HookSpec, HookTarget, MaterializedViewSpec, ResumeToken,
        SimplePredicate, SubscriptionConfig, poll_changefeed,
    };
    use crate::{
        db::{DbConfig, Profile, create_database},
        model::Value,
        phase30::{InternalTransportConfig, InternalTransportSecurity},
        query::{ColumnDef, ColumnType, TableSchema},
        repl::{
            ClusterBootstrap, CpClusterConfig, CpRaft, RaftNode, ReadConsistency, ReplError,
            Replication,
        },
        security::{AuthzPolicy, Permission, Principal, Resource, Role},
        storage::MemEngine,
    };

    fn users_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    ty: ColumnType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "category".to_owned(),
                    ty: ColumnType::Str,
                    nullable: false,
                },
                ColumnDef {
                    name: "amount".to_owned(),
                    ty: ColumnType::Int,
                    nullable: false,
                },
            ],
            primary_key: 0,
        }
    }

    #[test]
    fn changefeed_emits_committed_table_changes_and_resumes()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let users = database.create_table("users", Some(users_schema()), Vec::new())?;
        users.insert(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Int(10),
        ])?;

        let filter = ChangefeedFilter {
            target: ChangefeedTarget::Table("users".to_owned()),
        };
        let (events, token) = database.poll_changefeed(
            &ResumeToken::default(),
            &filter,
            &ChangefeedOptions::default(),
            100,
        )?;

        assert!(
            events
                .iter()
                .any(|event| matches!(event.op, ChangeOp::Upsert { .. }))
        );
        assert!(events.iter().all(|event| {
            matches!(
                event.target,
                super::LogicalTarget::Table(_) | super::LogicalTarget::Database
            )
        }));

        let (again, again_token) =
            database.poll_changefeed(&token, &filter, &ChangefeedOptions::default(), 100)?;
        assert!(again.is_empty(), "expected an empty value, got {again:?}");
        assert_eq!(again_token, token);
        Ok(())
    }

    #[test]
    fn subscription_rbac_and_ack_are_persistent() -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table("users", Some(users_schema()), Vec::new())?;

        let config = SubscriptionConfig {
            name: "sub-users".to_owned(),
            filter: ChangefeedFilter {
                target: ChangefeedTarget::Table("users".to_owned()),
            },
            start: ResumeToken::default(),
            buffer_limit: 8,
            ack_timeout_ms: 1_000,
        };

        assert!(
            database
                .create_subscription_as(&Principal::new("alice"), config.clone())
                .is_err()
        );

        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
        ]));
        let principal = Principal::new("alice").with_role("reader");
        let state = database.create_subscription_as(&principal, config)?;
        assert_eq!(state.last_ack.lsn, 0);

        let token = ResumeToken::new("default", 7);
        let acked = database.ack_subscription_as(&principal, "sub-users", token.clone())?;
        assert_eq!(acked.last_ack, token);
        assert_eq!(
            database
                .subscription_state_as(&principal, "sub-users")?
                .last_ack,
            token
        );
        Ok(())
    }

    #[test]
    fn materialized_view_refreshes_incrementally() -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let users = database.create_table("users", Some(users_schema()), Vec::new())?;
        users.insert(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Int(10),
        ])?;
        users.insert(vec![
            Value::Int(2),
            Value::Str("b".to_owned()),
            Value::Int(20),
        ])?;

        let spec = MaterializedViewSpec {
            name: "users_by_category".to_owned(),
            source_table: "users".to_owned(),
            filter: None,
            group_by: 1,
            aggregates: vec![
                AggregateSpec {
                    output_name: "count".to_owned(),
                    kind: AggregateKind::Count,
                    column: None,
                },
                AggregateSpec {
                    output_name: "sum".to_owned(),
                    kind: AggregateKind::Sum,
                    column: Some(2),
                },
                AggregateSpec {
                    output_name: "avg".to_owned(),
                    kind: AggregateKind::Avg,
                    column: Some(2),
                },
            ],
        };
        let initial = database.create_materialized_view(&spec)?;
        assert_eq!(initial.rows.len(), 2);

        users.insert(vec![
            Value::Int(3),
            Value::Str("a".to_owned()),
            Value::Int(30),
        ])?;
        let refreshed = database.refresh_materialized_view_to_current("users_by_category")?;
        let group_a = refreshed
            .rows
            .iter()
            .find(|row| row.first() == Some(&Value::Str("a".to_owned())))
            .ok_or("missing group a")?;

        assert_eq!(group_a[1], Value::Int(2));
        assert_eq!(group_a[2], Value::Float(40.0));
        assert_eq!(group_a[3], Value::Float(20.0));
        assert!(refreshed.fresh_to_lsn > initial.fresh_to_lsn);
        Ok(())
    }

    #[test]
    fn materialized_view_rejects_unsupported_shape() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let spec = MaterializedViewSpec {
            name: "bad".to_owned(),
            source_table: "users".to_owned(),
            filter: Some(SimplePredicate {
                column: 0,
                equals: Value::Int(1),
            }),
            group_by: 0,
            aggregates: Vec::new(),
        };

        assert!(database.create_materialized_view(&spec).is_err());
        Ok(())
    }

    #[test]
    fn before_hook_rejects_write_and_after_hook_records_delivery()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let users = database.create_table("users", Some(users_schema()), Vec::new())?;
        database.register_hook(&HookSpec::before(
            "small-values",
            HookTarget::Table("users".to_owned()),
            HookAction::MaxValueBytes(1),
            1_000,
        ))?;

        assert!(
            users
                .insert(vec![
                    Value::Int(1),
                    Value::Str("a".to_owned()),
                    Value::Int(10)
                ])
                .is_err()
        );

        database.unregister_hook("small-values")?;
        database.register_hook(&HookSpec::after(
            "after-users",
            HookTarget::Table("users".to_owned()),
            HookAction::RecordAfterCommit,
            1_000,
        ))?;
        users.insert(vec![
            Value::Int(2),
            Value::Str("a".to_owned()),
            Value::Int(10),
        ])?;

        let deliveries = database.range(
            super::HOOK_DELIVERIES_TABLE,
            &[],
            &[],
            ReadConsistency::Strong,
        )?;
        assert_eq!(deliveries.len(), 1);
        Ok(())
    }

    #[test]
    fn cp_no_quorum_does_not_emit_uncommitted_events() -> Result<(), Box<dyn std::error::Error>> {
        let cluster = CpClusterConfig::new(
            1,
            "127.0.0.1:7101",
            vec![
                RaftNode::new(1, "127.0.0.1:7101"),
                RaftNode::new(2, "127.0.0.1:7102"),
                RaftNode::new(3, "127.0.0.1:7103"),
            ],
        )
        .with_bootstrap(ClusterBootstrap::Initialize)
        .with_transport(InternalTransportConfig::new(
            "127.0.0.1:7101",
            InternalTransportSecurity::PlaintextForTests,
        ));
        let raft = CpRaft::new(MemEngine::new(), cluster)?;
        raft.set_available_voters_for_tests([1])?;

        assert!(matches!(
            raft.propose(crate::repl::Op::Put {
                table: "t".to_owned(),
                key: b"k".to_vec(),
                value: b"v".to_vec()
            }),
            Err(ReplError::NoQuorum)
        ));
        raft.set_available_voters_for_tests([1, 2])?;

        let (events, _) = poll_changefeed(
            &raft,
            &BTreeMap::new(),
            &ResumeToken::default(),
            &ChangefeedFilter::default(),
            &ChangefeedOptions {
                include_system: true,
            },
            100,
        )?;
        assert!(events.is_empty(), "expected an empty value, got {events:?}");
        Ok(())
    }

    mod bounded_history {
        use std::{
            collections::BTreeMap,
            sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        };

        use crate::{
            cdc::{
                ChangeOp, ChangefeedFilter, ChangefeedOptions, FeedError, HOOK_DELIVERIES_TABLE,
                HookAction, HookError, HookSpec, HookTarget, ResumeToken, SubscriptionConfig,
                commit_records_between, current_resume_token, deliver_after_commit_hooks,
                poll_changefeed, poll_changefeed_page, register_hook,
            },
            db::{DbConfig, Profile, create_database},
            repl::{Op, ReadConsistency, ReplError, Replication, SingleNode},
            storage::{Bytes, MemEngine, StorageEngine, StorageError, WriteTransaction},
            txn::{self, WriteAuthorization},
        };

        /// Counts commit-log rows read and can truncate LSNs 1..=5 right before
        /// the first commit-log range read, simulating a concurrent history GC.
        struct LogProbe {
            inner: SingleNode<MemEngine>,
            commit_log_rows: AtomicUsize,
            truncate_on_first_range: AtomicBool,
        }

        impl LogProbe {
            fn with_commits(count: u8, truncate_on_first_range: bool) -> Result<Self, ReplError> {
                let inner = SingleNode::new(MemEngine::new());
                for id in 0..count {
                    inner.propose(Op::Put {
                        table: "t".to_owned(),
                        key: vec![id],
                        value: vec![id],
                    })?;
                }
                Ok(Self {
                    inner,
                    commit_log_rows: AtomicUsize::new(0),
                    truncate_on_first_range: AtomicBool::new(truncate_on_first_range),
                })
            }

            fn take_rows(&self) -> usize {
                self.commit_log_rows.swap(0, Ordering::SeqCst)
            }

            fn truncate_prefix(&self, record_floor: bool) -> Result<(), ReplError> {
                let mut write = self.inner.storage().begin_write()?;
                for lsn in 1..=5 {
                    write.delete(txn::COMMIT_LOG_TABLE, &txn::commit_log_key(lsn))?;
                }
                if record_floor {
                    write.put(
                        txn::TXN_META_TABLE,
                        txn::history::COMMIT_LOG_FLOOR_KEY,
                        &txn::encode_txn_id(6),
                    )?;
                }
                write.commit()?;
                Ok(())
            }
        }

        impl Replication for LogProbe {
            fn propose(&self, op: Op) -> Result<(), ReplError> {
                self.inner.propose(op)
            }

            fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
                self.inner.propose_batch(ops)
            }

            fn propose_authorized_batch(
                &self,
                ops: Vec<Op>,
                authorization: WriteAuthorization,
            ) -> Result<(), ReplError> {
                self.inner.propose_authorized_batch(ops, authorization)
            }

            fn read(
                &self,
                table: &str,
                key: &[u8],
                consistency: ReadConsistency,
            ) -> Result<Option<Bytes>, ReplError> {
                self.inner.read(table, key, consistency)
            }

            fn range(
                &self,
                table: &str,
                start: &[u8],
                end: &[u8],
                consistency: ReadConsistency,
            ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
                if table == txn::COMMIT_LOG_TABLE
                    && self.truncate_on_first_range.swap(false, Ordering::SeqCst)
                {
                    self.truncate_prefix(true)?;
                }
                let rows = self.inner.range(table, start, end, consistency)?;
                if table == txn::COMMIT_LOG_TABLE {
                    self.commit_log_rows.fetch_add(rows.len(), Ordering::SeqCst);
                }
                Ok(rows)
            }
        }

        fn system_options() -> ChangefeedOptions {
            ChangefeedOptions {
                include_system: true,
            }
        }

        #[test]
        fn changefeed_page_reads_bounded_records() -> Result<(), Box<dyn std::error::Error>> {
            let repl = LogProbe::with_commits(40, false)?;
            let catalog = BTreeMap::new();
            let filter = ChangefeedFilter::default();
            let options = system_options();

            repl.take_rows();
            let page = poll_changefeed_page(
                &repl,
                &catalog,
                &ResumeToken::default(),
                &filter,
                &options,
                5,
            )?;
            assert!(page.has_more);
            assert_eq!(page.next.lsn, 5);
            let commits = page
                .events
                .iter()
                .filter(|event| matches!(event.op, ChangeOp::TxCommit))
                .count();
            assert_eq!(commits, 5);
            assert!(repl.take_rows() <= 6);

            let (all, last) = poll_changefeed(
                &repl,
                &catalog,
                &ResumeToken::default(),
                &filter,
                &options,
                1_000,
            )?;
            let mut paged = Vec::new();
            let mut token = ResumeToken::default();
            loop {
                let page = poll_changefeed_page(&repl, &catalog, &token, &filter, &options, 5)?;
                paged.extend(page.events);
                token = page.next;
                if !page.has_more {
                    break;
                }
            }
            assert_eq!(paged, all);
            assert_eq!(token, last);
            assert_eq!(last.lsn, 40);
            Ok(())
        }

        #[test]
        fn changefeed_poll_detects_truncated_prefix() -> Result<(), Box<dyn std::error::Error>> {
            let catalog = BTreeMap::new();
            let filter = ChangefeedFilter::default();
            let options = system_options();

            let truncated = LogProbe::with_commits(10, false)?;
            truncated.truncate_prefix(false)?;
            assert!(matches!(
                poll_changefeed(
                    &truncated,
                    &catalog,
                    &ResumeToken::default(),
                    &filter,
                    &options,
                    100
                ),
                Err(FeedError::TokenExpired {
                    requested: 0,
                    earliest: 6
                })
            ));
            let (_, next) = poll_changefeed(
                &truncated,
                &catalog,
                &ResumeToken::new("default", 5),
                &filter,
                &options,
                100,
            )?;
            assert_eq!(next.lsn, 10);

            // The truncation lands after the expiry check passed; the leading-gap
            // recheck must still refuse to skip LSNs 1..=5.
            let racing = LogProbe::with_commits(10, true)?;
            assert!(matches!(
                poll_changefeed(
                    &racing,
                    &catalog,
                    &ResumeToken::default(),
                    &filter,
                    &options,
                    100
                ),
                Err(FeedError::TokenExpired {
                    requested: 0,
                    earliest: 6
                })
            ));
            Ok(())
        }

        #[test]
        fn hook_commit_reads_fail_closed_on_missing_records()
        -> Result<(), Box<dyn std::error::Error>> {
            let lsns = |records: &[txn::CommitLogRecord]| {
                records
                    .iter()
                    .map(|record| record.txn_id)
                    .collect::<Vec<_>>()
            };

            let intact = LogProbe::with_commits(10, false)?;
            assert_eq!(
                lsns(&commit_records_between(&intact, 0, 10)?),
                (1..=10).collect::<Vec<_>>()
            );

            let truncated = LogProbe::with_commits(10, false)?;
            truncated.truncate_prefix(true)?;
            assert!(matches!(
                commit_records_between(&truncated, 0, 10),
                Err(FeedError::TokenExpired {
                    requested: 0,
                    earliest: 6
                })
            ));
            assert_eq!(
                lsns(&commit_records_between(&truncated, 5, 10)?),
                (6..=10).collect::<Vec<_>>()
            );

            // The truncation lands after the floor check passed.
            let racing = LogProbe::with_commits(10, true)?;
            assert!(matches!(
                commit_records_between(&racing, 0, 10),
                Err(FeedError::TokenExpired {
                    requested: 0,
                    earliest: 6
                })
            ));

            // A hole above the floor is corruption, never a silently skipped record.
            let holed = LogProbe::with_commits(10, false)?;
            let mut write = holed.inner.storage().begin_write()?;
            write.delete(txn::COMMIT_LOG_TABLE, &txn::commit_log_key(7))?;
            write.commit()?;
            assert!(matches!(
                commit_records_between(&holed, 5, 10),
                Err(FeedError::Storage(StorageError::Corruption(_)))
            ));

            // After-commit hook delivery surfaces the racing truncation and records nothing.
            let hooked = LogProbe::with_commits(10, false)?;
            register_hook(
                &hooked,
                &HookSpec::after(
                    "audit",
                    HookTarget::AnyUser,
                    HookAction::RecordAfterCommit,
                    1_000,
                ),
            )?;
            hooked.truncate_on_first_range.store(true, Ordering::SeqCst);
            let delivered = deliver_after_commit_hooks(&hooked, 0, 10);
            assert!(
                matches!(&delivered, Err(HookError::Serde(message)) if message.contains("expired")),
                "{delivered:?}"
            );
            assert!(
                hooked
                    .range(HOOK_DELIVERIES_TABLE, &[], &[], ReadConsistency::Strong)?
                    .is_empty(),
                "expected an empty value, got {:?}",
                hooked.range(HOOK_DELIVERIES_TABLE, &[], &[], ReadConsistency::Strong)?
            );
            Ok(())
        }

        #[test]
        fn ack_subscription_is_noop_for_unchanged_token() -> Result<(), Box<dyn std::error::Error>>
        {
            let database = create_database(DbConfig::new(Profile::InMemory))?;
            database.create_subscription(SubscriptionConfig::new("sub", ResumeToken::default()))?;
            let token = ResumeToken::new("default", 1);
            let first = database.ack_subscription("sub", token.clone())?;
            let lsn = current_resume_token(&database)?.lsn;

            let again = database.ack_subscription("sub", token)?;

            assert_eq!(again, first);
            assert_eq!(current_resume_token(&database)?.lsn, lsn);
            assert_eq!(database.subscription_state("sub")?, first);
            Ok(())
        }
    }

    /// System rows (`LogicalTarget::System`) need `System:Admin` on every principal surface:
    /// changefeed polls, subscriptions and SQL.
    mod system_rows {
        use tokio::sync::mpsc;

        use crate::{
            cdc::{
                ChangeEvent, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedTarget,
                LogicalTarget, ResumeToken, SubscriptionConfig, TransactionBoundary,
                push_subscription_once, push_subscription_once_as, retain_events_visible_to,
            },
            db::{AdminCredentialRecord, Database, DbConfig, DbError, Profile, create_database},
            extension::{LimitPolicy, MaskingPolicy, POLICIES_TABLE, PolicyConfig},
            model::{FieldPath, Value, decode_value},
            query::{ColumnDef, ColumnType, REL_INDEX_TABLE, RelIndexSpec, TableSchema},
            security::{
                AUDIT_TABLE, AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry,
                Resource, Role,
            },
        };

        const ADMIN_AUTH: &str = "__admin_auth";
        const AUTHZ_POLICY: &str = "__authz_policy";
        const PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$marker-salt$marker-hash";
        const SECRET_NAME: &str = "Ada-Secret-Name";

        fn policy() -> AuthzPolicy {
            AuthzPolicy::new([
                Role::new("sysadmin")
                    .grant(Resource::System, Permission::Admin)
                    .grant(Resource::Database, Permission::Read)
                    .grant(Resource::Database, Permission::Admin),
                Role::new("system_admin").grant(Resource::System, Permission::Admin),
                Role::new("db_admin").grant(Resource::Database, Permission::Admin),
                Role::new("reader").grant(Resource::Database, Permission::Read),
                Role::new("users_reader")
                    .grant(Resource::Table("users".to_owned()), Permission::Read),
            ])
        }

        fn root() -> Principal {
            Principal::new("root").with_role("sysadmin")
        }

        fn alice() -> Principal {
            Principal::new("alice").with_role("reader")
        }

        /// A database whose commit log holds an admin credential, the persisted RBAC state,
        /// audit events, a masking policy and a `users` row whose masked `name` is indexed.
        fn seeded_database() -> Result<Database, Box<dyn std::error::Error>> {
            let mut database = create_database(DbConfig::new(Profile::InMemory))?;
            database.create_table(
                "users",
                Some(TableSchema::new(
                    vec![
                        ColumnDef::new("id", ColumnType::Int, false),
                        ColumnDef::new("name", ColumnType::Str, false),
                        ColumnDef::new("age", ColumnType::Int, false),
                    ],
                    0,
                )),
                vec![RelIndexSpec::new(1, 1)],
            )?;
            database.set_authz_policy(policy());
            database.set_audit_enabled_as(&root(), true)?;
            database.set_security_state_as(&root(), policy(), PrincipalRegistry::new())?;
            database.set_admin_credential(&AdminCredentialRecord {
                username: "admin".to_owned(),
                password_hash: PASSWORD_HASH.to_owned(),
                updated_at_millis: 1,
            })?;
            database.set_policy_config(&PolicyConfig {
                validations: Vec::new(),
                masking: vec![MaskingPolicy {
                    resource: Resource::Table("users".to_owned()),
                    paths: vec![FieldPath::new(["name"])],
                    replacement: Value::Str("***".to_owned()),
                }],
                row_policies: Vec::new(),
                limits: LimitPolicy::default(),
            })?;
            database.table("users")?.insert(vec![
                Value::Int(1),
                Value::Str(SECRET_NAME.to_owned()),
                Value::Int(37),
            ])?;
            Ok(database)
        }

        fn all() -> ChangefeedFilter {
            ChangefeedFilter::default()
        }

        fn system(table: &str) -> ChangefeedFilter {
            ChangefeedFilter {
                target: ChangefeedTarget::System(table.to_owned()),
            }
        }

        fn include_system() -> ChangefeedOptions {
            ChangefeedOptions {
                include_system: true,
            }
        }

        /// Polls with both `poll_changefeed_as` and `poll_changefeed_page_as`, checks that they
        /// agree, and returns the events.
        fn poll_as(
            database: &Database,
            principal: &Principal,
            filter: &ChangefeedFilter,
            options: &ChangefeedOptions,
        ) -> Result<Vec<ChangeEvent>, DbError> {
            let (events, _) = database.poll_changefeed_as(
                principal,
                &ResumeToken::default(),
                filter,
                options,
                1_000,
            )?;
            let page = database.poll_changefeed_page_as(
                principal,
                &ResumeToken::default(),
                filter,
                options,
                1_000,
            )?;
            assert_eq!(page.events, events);
            Ok(events)
        }

        fn assert_denied_both_ways(
            database: &Database,
            principal: &Principal,
            filter: &ChangefeedFilter,
            options: &ChangefeedOptions,
        ) {
            let polled = database.poll_changefeed_as(
                principal,
                &ResumeToken::default(),
                filter,
                options,
                1_000,
            );
            assert!(matches!(polled, Err(DbError::AuthzDenied(_))), "{polled:?}");
            let paged = database.poll_changefeed_page_as(
                principal,
                &ResumeToken::default(),
                filter,
                options,
                1_000,
            );
            assert!(matches!(paged, Err(DbError::AuthzDenied(_))), "{paged:?}");
        }

        fn system_tables(events: &[ChangeEvent]) -> Vec<&str> {
            events
                .iter()
                .filter_map(|event| match &event.target {
                    LogicalTarget::System(table) => Some(table.as_str()),
                    _ => None,
                })
                .collect()
        }

        /// Whether any event key or value contains `needle`.
        fn carries(events: &[ChangeEvent], needle: &str) -> bool {
            let needle = needle.as_bytes();
            events.iter().any(|event| match &event.op {
                ChangeOp::Upsert { key, value_after } => [key, value_after]
                    .iter()
                    .any(|bytes| bytes.windows(needle.len()).any(|window| window == needle)),
                ChangeOp::Delete { key } => {
                    key.windows(needle.len()).any(|window| window == needle)
                }
                ChangeOp::TxBegin | ChangeOp::TxCommit | ChangeOp::Ddl { .. } => false,
            })
        }

        fn denied_poll_events(database: &Database, principal: &str) -> Result<usize, DbError> {
            Ok(database
                .audit_events()?
                .iter()
                .filter(|event| {
                    event.action == "poll_changefeed"
                        && event.outcome == AuditOutcome::Denied
                        && event.resource == Resource::System
                        && event.principal.as_deref() == Some(principal)
                })
                .count())
        }

        #[test]
        fn include_system_requires_system_admin_and_denials_are_audited()
        -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;

            assert_denied_both_ways(&database, &alice(), &all(), &include_system());
            assert_eq!(denied_poll_events(&database, "alice")?, 2);

            // A table filter never matched system rows, but `include_system` is now refused with
            // it too: the options are checked before the filter target is looked at.
            let users_reader = Principal::new("bob").with_role("users_reader");
            let users = ChangefeedFilter {
                target: ChangefeedTarget::Table("users".to_owned()),
            };
            assert!(
                poll_as(
                    &database,
                    &users_reader,
                    &users,
                    &ChangefeedOptions::default()
                )
                .is_ok()
            );
            assert_denied_both_ways(&database, &users_reader, &users, &include_system());
            assert_eq!(denied_poll_events(&database, "bob")?, 2);
            Ok(())
        }

        #[test]
        fn system_admin_reads_credentials_rbac_and_audit_rows()
        -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;

            let events = poll_as(&database, &root(), &all(), &include_system())?;
            let tables = system_tables(&events);
            for table in [
                ADMIN_AUTH,
                AUTHZ_POLICY,
                AUDIT_TABLE,
                POLICIES_TABLE,
                REL_INDEX_TABLE,
            ] {
                assert!(tables.contains(&table), "missing {table} in {tables:?}");
            }
            assert!(carries(&events, PASSWORD_HASH));
            assert!(carries(&events, SECRET_NAME));

            // `System:Admin` alone reads one system keyspace by name.
            let system_admin = Principal::new("ops").with_role("system_admin");
            let credentials = poll_as(
                &database,
                &system_admin,
                &system(ADMIN_AUTH),
                &include_system(),
            )?;
            assert!(carries(&credentials, PASSWORD_HASH));
            assert!(
                system_tables(&credentials)
                    .iter()
                    .all(|table| *table == ADMIN_AUTH)
            );
            Ok(())
        }

        #[test]
        fn database_admin_grant_covers_system_rows() -> Result<(), Box<dyn std::error::Error>> {
            // `Role::allows` lets a grant on `Resource::Database` satisfy the same permission
            // on every resource, `Resource::System` included.
            let database = seeded_database()?;
            let db_admin = Principal::new("dba").with_role("db_admin");

            let events = poll_as(&database, &db_admin, &system(ADMIN_AUTH), &include_system())?;

            assert!(system_tables(&events).contains(&ADMIN_AUTH));
            assert!(carries(&events, PASSWORD_HASH));
            Ok(())
        }

        #[test]
        fn polls_without_system_rows_are_unchanged() -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;

            let events = poll_as(&database, &alice(), &all(), &ChangefeedOptions::default())?;
            assert!(system_tables(&events).is_empty(), "{events:?}");
            let masked = events
                .iter()
                .find_map(|event| match (&event.target, &event.op) {
                    (LogicalTarget::Table(table), ChangeOp::Upsert { value_after, .. })
                        if table == "users" =>
                    {
                        Some(value_after)
                    }
                    _ => None,
                })
                .ok_or("missing users upsert")?;
            assert_eq!(
                decode_value(masked)?,
                Value::Array(vec![
                    Value::Int(1),
                    Value::Str("***".to_owned()),
                    Value::Int(37)
                ])
            );
            // Neither the credential nor the masked value reaches the reader, not even through
            // the `rel_indexes` key that indexes the masked column.
            assert!(!carries(&events, PASSWORD_HASH));
            assert!(!carries(&events, SECRET_NAME));
            assert_eq!(denied_poll_events(&database, "alice")?, 0);

            // A table-only reader keeps its table feed and still cannot use the `All` filter.
            let users_reader = Principal::new("bob").with_role("users_reader");
            let users = ChangefeedFilter {
                target: ChangefeedTarget::Table("users".to_owned()),
            };
            let table_events = poll_as(
                &database,
                &users_reader,
                &users,
                &ChangefeedOptions::default(),
            )?;
            assert!(!table_events.is_empty(), "expected a non-empty value");
            assert!(!carries(&table_events, SECRET_NAME));
            assert_denied_both_ways(
                &database,
                &users_reader,
                &all(),
                &ChangefeedOptions::default(),
            );
            Ok(())
        }

        #[test]
        fn system_targets_still_require_system_admin() -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;

            for table in [ADMIN_AUTH, AUTHZ_POLICY, AUDIT_TABLE, REL_INDEX_TABLE] {
                assert_denied_both_ways(&database, &alice(), &system(table), &include_system());
                assert_denied_both_ways(
                    &database,
                    &alice(),
                    &system(table),
                    &ChangefeedOptions::default(),
                );
            }
            assert_eq!(denied_poll_events(&database, "alice")?, 16);
            Ok(())
        }

        #[test]
        fn system_rows_are_dropped_for_principals_without_system_admin() {
            let event = |target: LogicalTarget| ChangeEvent {
                lsn: 1,
                tx_id: 1,
                at: std::time::SystemTime::UNIX_EPOCH,
                hlc: None,
                target,
                op: ChangeOp::Upsert {
                    key: b"admin".to_vec(),
                    value_after: PASSWORD_HASH.as_bytes().to_vec(),
                },
                transaction_boundary: TransactionBoundary::Data,
            };
            let events = vec![
                event(LogicalTarget::Table("users".to_owned())),
                event(LogicalTarget::System(ADMIN_AUTH.to_owned())),
                event(LogicalTarget::System(REL_INDEX_TABLE.to_owned())),
            ];
            let authz = policy();

            for principal in [
                alice(),
                Principal::new("bob").with_role("users_reader"),
                Principal::new("nobody"),
            ] {
                let visible = retain_events_visible_to(&authz, &principal, events.clone());
                assert_eq!(visible, events[..1], "{principal:?}");
            }
            for principal in [
                root(),
                Principal::new("ops").with_role("system_admin"),
                Principal::new("dba").with_role("db_admin"),
            ] {
                let visible = retain_events_visible_to(&authz, &principal, events.clone());
                assert_eq!(visible, events, "{principal:?}");
            }
        }

        #[tokio::test]
        async fn subscriptions_never_deliver_system_rows() -> Result<(), Box<dyn std::error::Error>>
        {
            let database = seeded_database()?;
            let config = SubscriptionConfig::new("feed", ResumeToken::default());
            database.create_subscription_as(&alice(), config)?;
            let mut system_feed = SubscriptionConfig::new("system-feed", ResumeToken::default());
            system_feed.filter = system(ADMIN_AUTH);
            assert!(
                database
                    .create_subscription_as(&alice(), system_feed)
                    .is_err()
            );

            let (sender, mut receiver) = mpsc::channel(4_096);
            assert!(matches!(
                push_subscription_once(&database, database.catalog(), "feed", &sender).await,
                Err(crate::cdc::SubscriptionError::PrincipalRequired)
            ));
            push_subscription_once_as(&database, &alice(), "feed", &sender).await?;
            drop(sender);
            let mut delivered = Vec::new();
            while let Some(event) = receiver.recv().await {
                delivered.push(event);
            }

            assert!(
                delivered
                    .iter()
                    .any(|event| matches!(event.target, LogicalTarget::Table(_)))
            );
            assert!(system_tables(&delivered).is_empty(), "{delivered:?}");
            assert!(!carries(&delivered, PASSWORD_HASH));
            assert!(!carries(&delivered, SECRET_NAME));
            Ok(())
        }

        #[tokio::test]
        async fn principal_subscription_rechecks_revoked_read()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut database = seeded_database()?;
            database.create_subscription_as(
                &alice(),
                SubscriptionConfig::new("feed", ResumeToken::default()),
            )?;
            let token = database.subscription_state("feed")?.last_ack;
            database.set_authz_policy(AuthzPolicy::default());
            let (sender, mut receiver) = mpsc::channel(4_096);
            assert!(matches!(
                push_subscription_once_as(&database, &alice(), "feed", &sender).await,
                Err(crate::cdc::SubscriptionError::Authz(_))
            ));
            assert!(receiver.try_recv().is_err());
            assert_eq!(database.subscription_state("feed")?.last_ack, token);
            Ok(())
        }

        #[tokio::test]
        async fn principal_worker_rechecks_live_permissions()
        -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;
            let start = crate::cdc::current_resume_token(&database)?;
            database.create_subscription_as(&alice(), SubscriptionConfig::new("feed", start))?;
            let database = std::sync::Arc::new(std::sync::RwLock::new(database));
            let (handle, mut receiver) = crate::cdc::SubscriptionWorker::start_as(
                database.clone(),
                alice(),
                "feed",
                crate::phase30::CdcWorkerConfig {
                    poll_interval_ms: 1,
                    ..crate::phase30::CdcWorkerConfig::default()
                },
            )?;
            database
                .write()
                .map_err(|_| "poisoned test lock")?
                .set_authz_policy(AuthzPolicy::default());
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(2), handle.join()).await?;
            assert!(matches!(
                result,
                Err(crate::cdc::SubscriptionError::Authz(_))
            ));
            while let Ok(event) = receiver.try_recv() {
                assert!(!matches!(
                    event.op,
                    ChangeOp::Upsert { .. } | ChangeOp::Delete { .. }
                ));
            }
            Ok(())
        }

        #[tokio::test]
        async fn registered_subscription_worker_observes_removed_mapping()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut database = seeded_database()?;
            database.set_principal_registry(
                PrincipalRegistry::new().with_principal("pg-alice", alice()),
            );
            let start = crate::cdc::current_resume_token(&database)?;
            database.create_subscription_as(&alice(), SubscriptionConfig::new("feed", start))?;
            let database = std::sync::Arc::new(std::sync::RwLock::new(database));
            let (handle, _receiver) = crate::cdc::SubscriptionWorker::start_for_user(
                database.clone(),
                "pg-alice",
                "feed",
                crate::phase30::CdcWorkerConfig {
                    poll_interval_ms: 1,
                    ..crate::phase30::CdcWorkerConfig::default()
                },
            )?;
            database
                .write()
                .map_err(|_| "poisoned test lock")?
                .set_principal_registry(PrincipalRegistry::new());
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(2), handle.join()).await?;
            assert!(matches!(
                result,
                Err(crate::cdc::SubscriptionError::Authz(_)
                    | crate::cdc::SubscriptionError::PrincipalMismatch)
            ));
            Ok(())
        }

        #[tokio::test]
        async fn subscription_owner_cannot_take_over_other_or_trusted_cursor()
        -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;
            let config = SubscriptionConfig::new("feed", ResumeToken::default());
            database.create_subscription_as(&alice(), config.clone())?;
            let bob = Principal::new("bob").with_role("reader");
            assert!(database.create_subscription_as(&bob, config).is_err());
            assert!(
                database
                    .ack_subscription_as(&bob, "feed", ResumeToken::default())
                    .is_err()
            );
            assert!(database.subscription_state_as(&bob, "feed").is_err());
            let trusted = SubscriptionConfig::new("trusted", ResumeToken::default());
            database.create_subscription(trusted.clone())?;
            assert!(database.create_subscription_as(&alice(), trusted).is_err());
            let (sender, _receiver) = mpsc::channel(4_096);
            assert!(matches!(
                push_subscription_once_as(&database, &alice(), "trusted", &sender).await,
                Err(crate::cdc::SubscriptionError::PrincipalMismatch)
            ));
            Ok(())
        }

        #[test]
        fn principal_continuous_query_preserves_ownership_and_ack_on_reapply()
        -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;
            let spec = crate::continuous::ContinuousQuerySpec {
                name: "watched".to_owned(),
                sql: "SELECT * FROM users".to_owned(),
                filter: ChangefeedFilter {
                    target: ChangefeedTarget::Table("users".to_owned()),
                },
                start: ResumeToken::default(),
                buffer_limit: 100,
            };
            database.create_continuous_query_as(&alice(), &spec)?;
            assert_eq!(
                database.subscription_state("watched")?.principal,
                Some(alice())
            );
            let ack = crate::cdc::current_resume_token(&database)?;
            database.ack_continuous_query_as(&alice(), "watched", ack.clone())?;
            let reapplied = database.create_continuous_query_as(&alice(), &spec)?;
            assert_eq!(reapplied.last_ack, ack);
            assert_eq!(database.subscription_state("watched")?.last_ack, ack);
            assert!(
                database
                    .create_continuous_query_as(&Principal::new("bob").with_role("reader"), &spec,)
                    .is_err()
            );
            assert_eq!(
                database.subscription_state("watched")?.principal,
                Some(alice())
            );
            Ok(())
        }

        #[tokio::test]
        async fn legacy_subscription_requires_explicit_adoption_without_cursor_reset()
        -> Result<(), Box<dyn std::error::Error>> {
            let database = seeded_database()?;
            let config = SubscriptionConfig::new("legacy", ResumeToken::default());
            let state = database.create_subscription(config)?;
            let token = crate::cdc::current_resume_token(&database)?;
            database.ack_subscription("legacy", token.clone())?;
            let mut legacy = serde_json::to_value(&state)?;
            legacy
                .as_object_mut()
                .ok_or("subscription must be object")?
                .remove("principal");
            legacy
                .as_object_mut()
                .ok_or("subscription must be object")?
                .remove("trusted");
            legacy["last_ack"] = serde_json::to_value(&token)?;
            let decoded: crate::cdc::SubscriptionState = serde_json::from_value(legacy)?;
            crate::cdc::write_subscription(&database, &decoded)?;
            let (sender, _receiver) = mpsc::channel(4_096);
            assert!(matches!(
                push_subscription_once(&database, database.catalog(), "legacy", &sender).await,
                Err(crate::cdc::SubscriptionError::PrincipalRequired)
            ));
            assert!(
                crate::cdc::adopt_subscription_as(
                    &database,
                    database.authz_policy(),
                    &alice(),
                    &alice(),
                    "legacy",
                )
                .is_err()
            );
            let adopted = crate::cdc::adopt_subscription_as(
                &database,
                database.authz_policy(),
                &root(),
                &alice(),
                "legacy",
            )?;
            assert_eq!(adopted.last_ack, token);
            assert_eq!(adopted.principal, Some(alice()));
            assert!(!adopted.trusted);
            push_subscription_once_as(&database, &alice(), "legacy", &sender).await?;
            Ok(())
        }

        #[tokio::test]
        async fn sql_cannot_read_system_keyspaces() -> Result<(), Box<dyn std::error::Error>> {
            // `query_as` serves PostgreSQL wire, the Control Plane `/sql` and the SDKs. Only
            // catalog objects are SQL tables, so a system keyspace name is not one.
            let database = seeded_database()?;

            for table in [
                ADMIN_AUTH,
                AUTHZ_POLICY,
                AUDIT_TABLE,
                POLICIES_TABLE,
                REL_INDEX_TABLE,
            ] {
                for principal in [alice(), root()] {
                    let result = database
                        .query_as(&principal, &format!("SELECT * FROM {table}"))
                        .await;
                    let Err(error) = result else {
                        panic!("{table} answered SQL for {principal:?}: {result:?}");
                    };
                    assert!(!error.to_string().contains(PASSWORD_HASH), "{error}");
                }
            }
            Ok(())
        }
    }
}
