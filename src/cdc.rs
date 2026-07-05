use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    backup::{Lsn, TimelineId},
    db::{CatalogEntry, DbError},
    geo::{GEO_INDEX_TABLE, GEO_POINTS_TABLE},
    graph::{GRAPH_IN_EDGES_TABLE, GRAPH_OUT_EDGES_TABLE},
    model::{DOCUMENT_TABLE, INDEX_TABLE, Value, decode_value},
    observability,
    phase30::{CdcWorkerConfig, HlcTimestamp},
    query::{REL_COLUMNAR_SEGMENTS_TABLE, REL_INDEX_TABLE, REL_ROWS_TABLE, Row, decode_row_bytes},
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, propose_system,
        propose_system_batch,
    },
    security::{AuthzError, AuthzPolicy, Permission, Principal, Resource},
    storage::{Bytes, StorageError},
    text::{FULL_TEXT_DOCS_TABLE, FULL_TEXT_META_TABLE, FULL_TEXT_POSTINGS_TABLE},
    timeseries::{TIME_SERIES_CHUNKS_TABLE, TIME_SERIES_LATEST_TABLE, TIME_SERIES_META_TABLE},
    txn::{self, CommitLogRecord, TxnId, WriteSet},
    vector::VECTOR_TABLE,
};

pub const CDC_SUBSCRIPTIONS_TABLE: &str = "__cdc_subscriptions";
pub const CDC_META_TABLE: &str = "__cdc_meta";
pub const CDC_TIMELINES_TABLE: &str = "__cdc_timelines";
pub const MATERIALIZED_VIEWS_TABLE: &str = "__materialized_views";
pub const MATERIALIZED_VIEW_STATE_TABLE: &str = "__materialized_view_state";
pub const HOOKS_TABLE: &str = "__hooks";
pub const HOOK_DELIVERIES_TABLE: &str = "__hook_deliveries";

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

#[derive(thiserror::Error, Debug)]
pub enum HookError {
    #[error("hook rejected write: {0}")]
    Rejected(String),

    #[error("hook timed out: {0}")]
    Timeout(String),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

pub struct HookedReplication {
    inner: Arc<dyn Replication>,
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
        let _ = subscription_state(repl.as_ref(), &name)?;
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

impl HookedReplication {
    #[must_use]
    pub fn new(inner: Arc<dyn Replication>) -> Self {
        Self { inner }
    }
}

impl Replication for HookedReplication {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        validate_before_hooks(self.inner.as_ref(), &ops).map_err(hook_to_repl_error)?;
        let before = current_lsn(self.inner.as_ref())?;
        self.inner.propose_batch(ops)?;
        let after = current_lsn(self.inner.as_ref())?;
        deliver_after_hooks(self.inner.as_ref(), before, after).map_err(hook_to_repl_error)?;
        Ok(())
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.inner.propose_authorized_batch(ops, authorization)
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        validate_before_hooks(self.inner.as_ref(), &batch.ops).map_err(hook_to_repl_error)?;
        let before = current_lsn(self.inner.as_ref())?;
        self.inner.propose_conditional_batch(batch)?;
        let after = current_lsn(self.inner.as_ref())?;
        deliver_after_hooks(self.inner.as_ref(), before, after).map_err(hook_to_repl_error)?;
        Ok(())
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
        self.inner.range(table, start, end, consistency)
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        self.inner.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
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

    let current = current_lsn(repl)?;
    let mut records = commit_records_between(repl, from.lsn, current)?;
    let limit = max.max(1);
    let has_more = records.len() > limit;
    if has_more {
        records.truncate(limit);
    }

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
    authorize_filter_access(authz, principal, &config.filter)?;
    let now = SystemTime::now();
    let state = SubscriptionState {
        last_ack: config.start.clone(),
        config,
        created_at: now,
        updated_at: now,
    };
    write_subscription(repl, &state)?;
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
    let state = SubscriptionState {
        last_ack: config.start.clone(),
        config,
        created_at: now,
        updated_at: now,
    };
    write_subscription(repl, &state)?;
    Ok(state)
}

/// Acknowledges a subscription position.
///
/// # Errors
/// Fails when the subscription is missing or metadata writes fail.
pub fn ack_subscription(
    repl: &dyn Replication,
    name: &str,
    token: ResumeToken,
) -> Result<SubscriptionState, SubscriptionError> {
    let mut state = read_subscription(repl, name)?
        .ok_or_else(|| SubscriptionError::MissingSubscription(name.to_owned()))?;
    state.last_ack = token;
    state.updated_at = SystemTime::now();
    write_subscription(repl, &state)?;
    Ok(state)
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
/// # Errors
/// Fails when the spec is unsupported, source table is missing, or metadata writes fail.
pub fn create_materialized_view(
    database: &crate::db::Database,
    spec: &MaterializedViewSpec,
) -> Result<MaterializedViewRows, MaterializedViewError> {
    validate_view_spec(spec)?;
    let _table = database
        .table(&spec.source_table)
        .map_err(|error| MaterializedViewError::Unsupported(error.to_string()))?;
    let mut state = MaterializedViewState {
        fresh_to_lsn: 0,
        source_rows: BTreeMap::new(),
        groups: BTreeMap::new(),
    };
    let token = ResumeToken::new(timeline_id(database)?, 0);
    let filter = ChangefeedFilter {
        target: ChangefeedTarget::Table(spec.source_table.clone()),
    };
    let next = apply_view_changefeed(database, spec, &mut state, token, &filter)?;
    state.fresh_to_lsn = next.lsn;
    write_view(database, spec, &state)?;
    Ok(state_to_rows(spec, &state))
}

/// Refreshes a materialized view to the database current LSN.
///
/// # Errors
/// Fails when view metadata is corrupt, changefeed read fails, or state writes fail.
pub fn refresh_materialized_view(
    database: &crate::db::Database,
    name: &str,
) -> Result<MaterializedViewRows, MaterializedViewError> {
    let spec = read_view_spec(database, name)?;
    let mut state = read_view_state(database, name)?;
    let token = ResumeToken::new(timeline_id(database)?, state.fresh_to_lsn);
    let filter = ChangefeedFilter {
        target: ChangefeedTarget::Table(spec.source_table.clone()),
    };
    let next = apply_view_changefeed(database, &spec, &mut state, token, &filter)?;
    state.fresh_to_lsn = next.lsn;
    write_view(database, &spec, &state)?;
    observability::record_materialized_view_refresh(name, state.fresh_to_lsn);
    Ok(state_to_rows(&spec, &state))
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
    let state = read_view_state_from_repl(repl, name)?;
    Ok(state_to_rows(&spec, &state))
}

/// Persists a hook.
///
/// # Errors
/// Fails when hook metadata cannot be written.
pub fn register_hook(repl: &dyn Replication, hook: &HookSpec) -> Result<(), HookError> {
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

pub(crate) fn validate_before_hooks(repl: &dyn Replication, ops: &[Op]) -> Result<(), HookError> {
    let hooks = read_hooks(repl)?;
    for hook in hooks
        .iter()
        .filter(|hook| hook.enabled && hook.kind == HookKind::BeforeCommit)
    {
        let started = Instant::now();
        for op in ops {
            if hook_matches_op(hook, op) {
                apply_before_hook(hook, op)?;
            }
            if started.elapsed() > Duration::from_millis(hook.timeout_ms) {
                return Err(HookError::Timeout(hook.name.clone()));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_write_set_hooks(
    repl: &dyn Replication,
    write_set: &WriteSet,
) -> Result<(), HookError> {
    let ops = write_set
        .iter()
        .map(|((table, key), value)| match value {
            Some(value) => Op::Put {
                table: table.clone(),
                key: key.clone(),
                value: value.clone(),
            },
            None => Op::Delete {
                table: table.clone(),
                key: key.clone(),
            },
        })
        .collect::<Vec<_>>();
    validate_before_hooks(repl, &ops)
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
        for record in &records {
            let event_count = record
                .writes
                .iter()
                .filter(|write| hook_matches_parts(&hook, &write.table, &write.key))
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

fn hook_to_repl_error(error: HookError) -> ReplError {
    let (kind, message) = match error {
        HookError::Rejected(message) => ("rejected", format!("hook rejected write: {message}")),
        HookError::Timeout(message) => ("timeout", format!("hook timed out: {message}")),
        HookError::Repl(error) => ("replication", format!("replication: {error}")),
        HookError::Serde(message) => ("serde", format!("metadata serialization: {message}")),
    };
    observability::record_hook_failure(kind);
    ReplError::Unsupported(message)
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
        ChangefeedTarget::System(_) => {
            authz.authorize(principal, &Resource::System, Permission::Admin)
        }
    }
}

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
    let Some(bytes) = repl.read(
        CDC_SUBSCRIPTIONS_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| SubscriptionError::Serde(error.to_string()))
}

fn current_lsn<R: Replication + ?Sized>(repl: &R) -> Result<Lsn, ReplError> {
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
    let rows = repl.range(txn::COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)?;
    let Some((key, _)) = rows.first() else {
        return Ok(1);
    };
    txn::decode_txn_id(key).map_err(Into::into)
}

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
    let start_key = txn::commit_log_key(first);
    let end_key = end.checked_add(1).map_or([0xFF; 8], txn::commit_log_key);
    repl.range(
        txn::COMMIT_LOG_TABLE,
        &start_key,
        &end_key,
        ReadConsistency::Strong,
    )?
    .into_iter()
    .map(|(_, value)| txn::decode_commit_log_record(&value).map_err(Into::into))
    .collect()
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
) -> Result<(), MaterializedViewError> {
    propose_system_batch(
        repl,
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
) -> Result<MaterializedViewState, MaterializedViewError> {
    let Some(bytes) = repl.read(
        MATERIALIZED_VIEW_STATE_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Err(MaterializedViewError::MissingView(name.to_owned()));
    };
    serde_json::from_slice(&bytes).map_err(|error| MaterializedViewError::Serde(error.to_string()))
}

fn read_view_spec(
    database: &crate::db::Database,
    name: &str,
) -> Result<MaterializedViewSpec, MaterializedViewError> {
    read_view_spec_from_repl(database, name)
}

fn read_view_state(
    database: &crate::db::Database,
    name: &str,
) -> Result<MaterializedViewState, MaterializedViewError> {
    read_view_state_from_repl(database, name)
}

fn apply_view_changefeed(
    database: &crate::db::Database,
    spec: &MaterializedViewSpec,
    state: &mut MaterializedViewState,
    mut token: ResumeToken,
    filter: &ChangefeedFilter,
) -> Result<ResumeToken, MaterializedViewError> {
    loop {
        let page = poll_changefeed_page(
            database,
            database.catalog(),
            &token,
            filter,
            &ChangefeedOptions::default(),
            MATERIALIZED_VIEW_CDC_WINDOW,
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
    });
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
                *sum += numeric_value(row, spec.column)? * numeric_sign;
            }
            (AggregateKind::Avg, AggregateValue::Avg { sum, count }) => {
                *sum += numeric_value(row, spec.column)? * numeric_sign;
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
    group.aggregates.iter().all(|value| match value {
        AggregateValue::Count(count) | AggregateValue::Avg { count, .. } => *count <= 0,
        AggregateValue::Sum(_) => false,
    })
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

fn hook_matches_op(hook: &HookSpec, op: &Op) -> bool {
    match op {
        Op::Put { table, key, .. } | Op::Delete { table, key } => {
            hook_matches_parts(hook, table, key)
        }
    }
}

fn hook_matches_parts(hook: &HookSpec, table: &str, key: &[u8]) -> bool {
    match &hook.target {
        HookTarget::AnyUser => !table.starts_with("__") && !is_derived_index_table(table),
        HookTarget::Keyspace(expected) => expected == table,
        HookTarget::Table(expected) => {
            matches!(table, REL_ROWS_TABLE | REL_COLUMNAR_SEGMENTS_TABLE)
                && parse_table_name_from_key(key).is_some_and(|actual| &actual == expected)
        }
        HookTarget::Collection(_) => table == DOCUMENT_TABLE,
        HookTarget::VectorCollection(_) => table == VECTOR_TABLE,
        HookTarget::FullTextIndex(expected) => {
            matches!(table, FULL_TEXT_DOCS_TABLE | FULL_TEXT_POSTINGS_TABLE)
                && parse_len_prefixed_name(key).is_some_and(|actual| &actual == expected)
        }
        HookTarget::TimeSeries(expected) => {
            matches!(table, TIME_SERIES_CHUNKS_TABLE | TIME_SERIES_LATEST_TABLE)
                && parse_len_prefixed_name(key).is_some_and(|actual| &actual == expected)
        }
        HookTarget::Graph(_) => matches!(table, GRAPH_OUT_EDGES_TABLE | GRAPH_IN_EDGES_TABLE),
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
        assert!(again.is_empty());
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
        assert!(events.is_empty());
        Ok(())
    }
}
