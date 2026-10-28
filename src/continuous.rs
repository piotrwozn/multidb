use std::time::SystemTime;

use crate::{
    backup::Lsn,
    cdc::{self, ChangefeedFilter, ResumeToken, SubscriptionConfig, SubscriptionState},
    extension::{AbiVersion, UdfBudget},
    model::Value,
    query::{Row, TableSchema},
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition,
        propose_system, propose_system_batch, propose_system_conditional_batch,
    },
    security::{AuthzError, AuthzPolicy, Principal},
    storage::{Bytes, StorageError},
};

pub const CONTINUOUS_QUERIES_TABLE: &str = "__continuous_queries";
pub const CONTINUOUS_QUERY_STATE_TABLE: &str = "__continuous_query_state";
pub const OUTBOX_CONNECTORS_TABLE: &str = "__outbox_connectors";
pub const OUTBOX_EVENTS_TABLE: &str = "__outbox_events";
pub const WASM_TRIGGERS_TABLE: &str = "__wasm_triggers";
pub const WASM_PROCEDURES_TABLE: &str = "__wasm_procedures";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ContinuousQuerySpec {
    pub name: String,
    pub sql: String,
    pub filter: ChangefeedFilter,
    pub start: ResumeToken,
    pub buffer_limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ContinuousQueryState {
    pub name: String,
    pub last_ack: ResumeToken,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct OutboxConnectorSpec {
    pub name: String,
    pub filter: ChangefeedFilter,
    pub sink: OutboxSink,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum OutboxSink {
    InternalTable,
    Webhook { endpoint: String },
    Kafka { topic: String },
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct OutboxEvent {
    pub connector: String,
    pub lsn: Lsn,
    pub target_key: Bytes,
    pub payload: Value,
    pub delivered: bool,
    pub created_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TriggerSpec {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub module_hash: String,
    pub entry: String,
    pub budget: UdfBudget,
    pub abi: AbiVersion,
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TriggerTiming {
    Before,
    /// Runs after the commit. Only `accept` counts as success: any other result or
    /// error is recorded in [`crate::cdc::POST_COMMIT_FAILURES_TABLE`] and never
    /// fails the committed write.
    After,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ProcedureSpec {
    pub name: String,
    pub module_hash: String,
    pub entry: String,
    pub budget: UdfBudget,
    pub abi: AbiVersion,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ProcedureResult {
    Rows { schema: TableSchema, rows: Vec<Row> },
    AffectedRows(usize),
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum TriggerOutcome {
    Accept,
    Reject(String),
    Replace(Value),
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ProcedureCommand {
    Put {
        table: String,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        table: String,
        key: Bytes,
    },
    Rows {
        schema: TableSchema,
        rows: Vec<Row>,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum ContinuousError {
    #[error("authorization: {0}")]
    Authz(#[from] AuthzError),

    #[error("subscription: {0}")]
    Subscription(#[from] cdc::SubscriptionError),

    #[error("missing continuous query: {0}")]
    MissingContinuousQuery(String),

    #[error("missing procedure: {0}")]
    MissingProcedure(String),

    #[error("missing trigger: {0}")]
    MissingTrigger(String),

    #[error("invalid continuous object: {0}")]
    Invalid(String),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

impl ContinuousQuerySpec {
    #[must_use]
    pub fn subscription_config(&self) -> SubscriptionConfig {
        SubscriptionConfig {
            name: self.name.clone(),
            filter: self.filter.clone(),
            start: self.start.clone(),
            buffer_limit: self.buffer_limit,
            ack_timeout_ms: 30_000,
        }
    }
}

/// Persists continuous-query metadata and its durable offset state.
/// # Errors
/// Fails when the name or SQL is invalid, serialization fails, or replication rejects metadata.
pub fn create_continuous_query(
    repl: &dyn Replication,
    spec: &ContinuousQuerySpec,
) -> Result<ContinuousQueryState, ContinuousError> {
    ensure_commit_log(repl, "continuous queries")?;
    validate_name(&spec.name)?;
    if spec.sql.trim().is_empty() {
        return Err(ContinuousError::Invalid(
            "continuous query SQL is required".to_owned(),
        ));
    }
    let now = SystemTime::now();
    let state = ContinuousQueryState {
        name: spec.name.clone(),
        last_ack: spec.start.clone(),
        created_at: now,
        updated_at: now,
    };
    propose_system_batch(
        repl,
        vec![
            put_json(CONTINUOUS_QUERIES_TABLE, spec.name.as_bytes(), spec)?,
            put_json(CONTINUOUS_QUERY_STATE_TABLE, spec.name.as_bytes(), &state)?,
        ],
    )?;
    Ok(state)
}

/// Creates a principal-bound continuous query and its subscription in one commit.
/// Reapplying the query preserves both durable acknowledgement positions. Delivery must
/// use the principal-aware CDC APIs, which check current grants and policies again.
/// # Errors
/// Fails when the caller lacks Read, another principal owns the subscription, the spec
/// is invalid, or reading/writing the durable metadata fails.
pub fn create_continuous_query_as(
    repl: &dyn Replication,
    authz: &AuthzPolicy,
    principal: &Principal,
    spec: &ContinuousQuerySpec,
) -> Result<ContinuousQueryState, ContinuousError> {
    ensure_commit_log(repl, "continuous queries")?;
    cdc::authorize_filter_access(authz, principal, &spec.filter)?;
    validate_name(&spec.name)?;
    if spec.sql.trim().is_empty() {
        return Err(ContinuousError::Invalid(
            "continuous query SQL is required".to_owned(),
        ));
    }
    let previous_query = query_state_snapshot(repl, &spec.name)?;
    let previous_subscription = cdc::subscription_snapshot(repl, &spec.name)?;
    if let Some((state, _)) = &previous_subscription {
        cdc::require_subscription_owner(state, principal)?;
    }
    let now = SystemTime::now();
    let state = ContinuousQueryState {
        name: spec.name.clone(),
        last_ack: previous_query
            .as_ref()
            .map_or_else(|| spec.start.clone(), |(state, _)| state.last_ack.clone()),
        created_at: previous_query
            .as_ref()
            .map_or(now, |(state, _)| state.created_at),
        updated_at: now,
    };
    let subscription = SubscriptionState {
        principal: Some(principal.clone()),
        trusted: false,
        config: spec.subscription_config(),
        last_ack: previous_subscription
            .as_ref()
            .map_or_else(|| spec.start.clone(), |(state, _)| state.last_ack.clone()),
        created_at: previous_subscription
            .as_ref()
            .map_or(now, |(state, _)| state.created_at),
        updated_at: now,
    };
    propose_system_conditional_batch(
        repl,
        ConditionalBatch::new(
            vec![
                WriteCondition::ValueEquals {
                    table: CONTINUOUS_QUERY_STATE_TABLE.to_owned(),
                    key: spec.name.as_bytes().to_vec(),
                    expected: previous_query.map(|(_, bytes)| bytes),
                },
                WriteCondition::ValueEquals {
                    table: cdc::CDC_SUBSCRIPTIONS_TABLE.to_owned(),
                    key: spec.name.as_bytes().to_vec(),
                    expected: previous_subscription.map(|(_, bytes)| bytes),
                },
            ],
            vec![
                put_json(CONTINUOUS_QUERIES_TABLE, spec.name.as_bytes(), spec)?,
                put_json(CONTINUOUS_QUERY_STATE_TABLE, spec.name.as_bytes(), &state)?,
                put_json(
                    cdc::CDC_SUBSCRIPTIONS_TABLE,
                    spec.name.as_bytes(),
                    &subscription,
                )?,
            ],
        ),
    )?;
    Ok(state)
}

fn ensure_commit_log(repl: &dyn Replication, feature: &str) -> Result<(), ContinuousError> {
    if !repl.supports_commit_log() {
        return Err(ReplError::Unsupported(format!(
            "{feature} require replication with a commit log"
        ))
        .into());
    }
    Ok(())
}

/// Advances the durable offset for one continuous query.
/// # Errors
/// Fails when the query is missing, metadata is corrupt, or replication rejects the update.
pub fn ack_continuous_query(
    repl: &dyn Replication,
    name: &str,
    token: ResumeToken,
) -> Result<ContinuousQueryState, ContinuousError> {
    ensure_commit_log(repl, "continuous queries")?;
    let (mut state, expected) = query_state_snapshot(repl, name)?
        .ok_or_else(|| ContinuousError::MissingContinuousQuery(name.to_owned()))?;
    cdc::validate_ack_advance(&state.last_ack, &token)?;
    if state.last_ack == token {
        return Ok(state);
    }
    state.last_ack = token;
    state.updated_at = SystemTime::now();
    propose_system_conditional_batch(
        repl,
        ConditionalBatch::new(
            vec![WriteCondition::ValueEquals {
                table: CONTINUOUS_QUERY_STATE_TABLE.to_owned(),
                key: name.as_bytes().to_vec(),
                expected: Some(expected),
            }],
            vec![put_json(
                CONTINUOUS_QUERY_STATE_TABLE,
                name.as_bytes(),
                &state,
            )?],
        ),
    )?;
    Ok(state)
}

/// Advances the query offset and its subscription cursor in one conditional commit.
pub(crate) fn ack_continuous_query_and_subscription(
    repl: &dyn Replication,
    name: &str,
    token: ResumeToken,
) -> Result<ContinuousQueryState, ContinuousError> {
    ack_query_and_subscription(repl, name, token, |_| Ok(()))
}

/// Authorizes the exact owner/filter snapshot updated by the conditional commit.
pub(crate) fn ack_continuous_query_and_subscription_as(
    repl: &dyn Replication,
    authz: &AuthzPolicy,
    principal: &Principal,
    name: &str,
    token: ResumeToken,
) -> Result<ContinuousQueryState, ContinuousError> {
    ack_query_and_subscription(repl, name, token, |state| {
        cdc::require_subscription_owner(state, principal)?;
        cdc::authorize_filter_access(authz, principal, &state.config.filter)?;
        Ok(())
    })
}

fn ack_query_and_subscription(
    repl: &dyn Replication,
    name: &str,
    token: ResumeToken,
    authorize: impl FnOnce(&SubscriptionState) -> Result<(), ContinuousError>,
) -> Result<ContinuousQueryState, ContinuousError> {
    ensure_commit_log(repl, "continuous queries")?;
    let (mut state, query_bytes) = query_state_snapshot(repl, name)?
        .ok_or_else(|| ContinuousError::MissingContinuousQuery(name.to_owned()))?;
    let (mut subscription, subscription_bytes) = cdc::subscription_snapshot(repl, name)?
        .ok_or_else(|| cdc::SubscriptionError::MissingSubscription(name.to_owned()))?;
    authorize(&subscription)?;
    cdc::validate_ack_advance(&state.last_ack, &token)?;
    cdc::validate_ack_advance(&subscription.last_ack, &token)?;
    if state.last_ack == token && subscription.last_ack == token {
        return Ok(state);
    }
    state.last_ack = token.clone();
    subscription.last_ack = token;
    let now = SystemTime::now();
    state.updated_at = now;
    subscription.updated_at = now;
    propose_system_conditional_batch(
        repl,
        ConditionalBatch::new(
            vec![
                WriteCondition::ValueEquals {
                    table: CONTINUOUS_QUERY_STATE_TABLE.to_owned(),
                    key: name.as_bytes().to_vec(),
                    expected: Some(query_bytes),
                },
                WriteCondition::ValueEquals {
                    table: cdc::CDC_SUBSCRIPTIONS_TABLE.to_owned(),
                    key: name.as_bytes().to_vec(),
                    expected: Some(subscription_bytes),
                },
            ],
            vec![
                put_json(CONTINUOUS_QUERY_STATE_TABLE, name.as_bytes(), &state)?,
                put_json(cdc::CDC_SUBSCRIPTIONS_TABLE, name.as_bytes(), &subscription)?,
            ],
        ),
    )?;
    Ok(state)
}

fn query_state_snapshot(
    repl: &dyn Replication,
    name: &str,
) -> Result<Option<(ContinuousQueryState, Bytes)>, ContinuousError> {
    let Some(bytes) = repl.read(
        CONTINUOUS_QUERY_STATE_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Ok(None);
    };
    let state = serde_json::from_slice(&bytes)
        .map_err(|error| ContinuousError::Serde(error.to_string()))?;
    Ok(Some((state, bytes)))
}

/// Reads durable continuous-query state.
/// # Errors
/// Fails when the query is missing, metadata is corrupt, or replication rejects the read.
pub fn read_continuous_query_state(
    repl: &dyn Replication,
    name: &str,
) -> Result<ContinuousQueryState, ContinuousError> {
    let Some(bytes) = repl.read(
        CONTINUOUS_QUERY_STATE_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Err(ContinuousError::MissingContinuousQuery(name.to_owned()));
    };
    serde_json::from_slice(&bytes).map_err(|error| ContinuousError::Serde(error.to_string()))
}

/// Persists an outbox connector descriptor.
/// # Errors
/// Fails when the name is invalid, serialization fails, or replication rejects metadata.
pub fn register_outbox_connector(
    repl: &dyn Replication,
    spec: &OutboxConnectorSpec,
) -> Result<(), ContinuousError> {
    ensure_commit_log(repl, "outbox connectors")?;
    validate_name(&spec.name)?;
    propose_system(
        repl,
        put_json(OUTBOX_CONNECTORS_TABLE, spec.name.as_bytes(), spec)?,
    )?;
    Ok(())
}

/// Enqueues one durable outbox event.
/// # Errors
/// Fails when serialization fails or replication rejects the write.
pub fn enqueue_outbox_event(
    repl: &dyn Replication,
    event: &OutboxEvent,
) -> Result<(), ContinuousError> {
    propose_system(
        repl,
        put_json(OUTBOX_EVENTS_TABLE, &outbox_key(event), event)?,
    )?;
    Ok(())
}

/// Persists a WASM trigger descriptor.
/// # Errors
/// Fails when the name is invalid, serialization fails, or replication rejects metadata.
pub fn register_trigger(repl: &dyn Replication, spec: &TriggerSpec) -> Result<(), ContinuousError> {
    validate_name(&spec.name)?;
    propose_system(
        repl,
        put_json(WASM_TRIGGERS_TABLE, spec.name.as_bytes(), spec)?,
    )?;
    Ok(())
}

/// Reads all registered WASM trigger descriptors.
/// # Errors
/// Fails when metadata is corrupt or replication rejects the scan.
pub fn read_triggers(repl: &dyn Replication) -> Result<Vec<TriggerSpec>, ContinuousError> {
    repl.range(WASM_TRIGGERS_TABLE, &[], &[], ReadConsistency::Strong)?
        .into_iter()
        .map(|(_, value)| {
            serde_json::from_slice(&value)
                .map_err(|error| ContinuousError::Serde(error.to_string()))
        })
        .collect()
}

/// Persists a WASM procedure descriptor.
/// # Errors
/// Fails when the name is invalid, serialization fails, or replication rejects metadata.
pub fn register_procedure(
    repl: &dyn Replication,
    spec: &ProcedureSpec,
) -> Result<(), ContinuousError> {
    validate_name(&spec.name)?;
    propose_system(
        repl,
        put_json(WASM_PROCEDURES_TABLE, spec.name.as_bytes(), spec)?,
    )?;
    Ok(())
}

/// Reads one WASM procedure descriptor by name.
/// # Errors
/// Fails when the procedure is missing, metadata is corrupt, or replication rejects the read.
pub fn read_procedure(
    repl: &dyn Replication,
    name: &str,
) -> Result<ProcedureSpec, ContinuousError> {
    let Some(bytes) = repl.read(
        WASM_PROCEDURES_TABLE,
        name.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Err(ContinuousError::MissingProcedure(name.to_owned()));
    };
    serde_json::from_slice(&bytes).map_err(|error| ContinuousError::Serde(error.to_string()))
}

/// Converts a WASM procedure return value into host-validated commands.
/// # Errors
/// Fails when the return value is not the supported declarative command format.
pub fn procedure_commands_from_value(
    value: Value,
) -> Result<Vec<ProcedureCommand>, ContinuousError> {
    let Value::Array(values) = value else {
        return Err(ContinuousError::Invalid(
            "procedure must return an array of command objects".to_owned(),
        ));
    };
    values
        .into_iter()
        .map(procedure_command_from_value)
        .collect()
}

/// Converts a WASM trigger return value into a host action.
/// # Errors
/// Fails when the return value is not a supported trigger outcome object.
pub fn trigger_outcome_from_value(value: Value) -> Result<TriggerOutcome, ContinuousError> {
    let Value::Object(fields) = value else {
        return Err(ContinuousError::Invalid(
            "trigger must return an object".to_owned(),
        ));
    };
    let action = fields
        .get("action")
        .and_then(value_as_str)
        .ok_or_else(|| ContinuousError::Invalid("trigger outcome action is required".to_owned()))?;
    match action {
        "accept" => Ok(TriggerOutcome::Accept),
        "reject" => Ok(TriggerOutcome::Reject(
            fields
                .get("message")
                .and_then(value_as_str)
                .unwrap_or("trigger rejected write")
                .to_owned(),
        )),
        "replace" => fields
            .get("value")
            .cloned()
            .map(TriggerOutcome::Replace)
            .ok_or_else(|| ContinuousError::Invalid("replace outcome needs value".to_owned())),
        other => Err(ContinuousError::Invalid(format!(
            "unknown trigger outcome {other}"
        ))),
    }
}

fn procedure_command_from_value(value: Value) -> Result<ProcedureCommand, ContinuousError> {
    let Value::Object(fields) = value else {
        return Err(ContinuousError::Invalid(
            "procedure command must be an object".to_owned(),
        ));
    };
    let action = fields.get("action").and_then(value_as_str).ok_or_else(|| {
        ContinuousError::Invalid("procedure command action is required".to_owned())
    })?;
    match action {
        "put" => Ok(ProcedureCommand::Put {
            table: object_string(&fields, "table")?,
            key: object_bytes(&fields, "key")?,
            value: object_bytes(&fields, "value")?,
        }),
        "delete" => Ok(ProcedureCommand::Delete {
            table: object_string(&fields, "table")?,
            key: object_bytes(&fields, "key")?,
        }),
        other => Err(ContinuousError::Invalid(format!(
            "unsupported procedure command {other}"
        ))),
    }
}

fn object_string(
    fields: &std::collections::BTreeMap<String, Value>,
    key: &str,
) -> Result<String, ContinuousError> {
    fields
        .get(key)
        .and_then(value_as_str)
        .map(str::to_owned)
        .ok_or_else(|| ContinuousError::Invalid(format!("missing string field {key}")))
}

fn object_bytes(
    fields: &std::collections::BTreeMap<String, Value>,
    key: &str,
) -> Result<Bytes, ContinuousError> {
    match fields.get(key) {
        Some(Value::Bytes(value)) => Ok(value.clone()),
        Some(Value::Str(value)) => Ok(value.as_bytes().to_vec()),
        _ => Err(ContinuousError::Invalid(format!(
            "missing bytes field {key}"
        ))),
    }
}

fn value_as_str(value: &Value) -> Option<&str> {
    match value {
        Value::Str(value) => Some(value),
        _ => None,
    }
}

fn put_json<T: serde::Serialize>(
    table: &str,
    key: &[u8],
    value: &T,
) -> Result<Op, ContinuousError> {
    Ok(Op::Put {
        table: table.to_owned(),
        key: key.to_vec(),
        value: serde_json::to_vec(value)
            .map_err(|error| ContinuousError::Serde(error.to_string()))?,
    })
}

fn outbox_key(event: &OutboxEvent) -> Bytes {
    let mut key = Vec::new();
    key.extend_from_slice(event.connector.as_bytes());
    key.push(0);
    key.extend_from_slice(&event.lsn.to_be_bytes());
    key.push(0);
    key.extend_from_slice(&event.target_key);
    key
}

fn validate_name(name: &str) -> Result<(), ContinuousError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(ContinuousError::Invalid("empty name".to_owned()));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(ContinuousError::Invalid(format!("invalid name {name}")));
    }
    Ok(())
}

#[cfg(test)]
mod capability_tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        geo::{GeoError, GeoIndex, GeoIndexConfig},
        model::{CollectionId, FieldPath},
        repl::ApDynamo,
        storage::MemEngine,
        text::{FullTextIndex, FullTextIndexConfig, TextError},
    };

    #[test]
    fn low_level_ap_apis_reject_commit_log_features_before_writing()
    -> Result<(), Box<dyn std::error::Error>> {
        let repl = ApDynamo::new_for_test(MemEngine::new(), 1);
        let spec = ContinuousQuerySpec {
            name: "watched".to_owned(),
            sql: "SELECT 1".to_owned(),
            filter: ChangefeedFilter::default(),
            start: ResumeToken::default(),
            buffer_limit: 16,
        };
        assert!(matches!(
            create_continuous_query(&repl, &spec),
            Err(ContinuousError::Repl(ReplError::Unsupported(_)))
        ));
        assert!(
            repl.read(
                CONTINUOUS_QUERIES_TABLE,
                b"watched",
                ReadConsistency::Strong
            )?
            .is_none()
        );
        assert!(
            repl.read(
                CONTINUOUS_QUERY_STATE_TABLE,
                b"watched",
                ReadConsistency::Strong
            )?
            .is_none()
        );
        assert!(matches!(
            ack_continuous_query(&repl, "watched", ResumeToken::default()),
            Err(ContinuousError::Repl(ReplError::Unsupported(_)))
        ));
        let outbox = OutboxConnectorSpec {
            name: "events".to_owned(),
            filter: ChangefeedFilter::default(),
            sink: OutboxSink::InternalTable,
            enabled: true,
        };
        assert!(matches!(
            register_outbox_connector(&repl, &outbox),
            Err(ContinuousError::Repl(ReplError::Unsupported(_)))
        ));
        assert!(
            repl.read(OUTBOX_CONNECTORS_TABLE, b"events", ReadConsistency::Strong)?
                .is_none()
        );

        let text = FullTextIndex::new(
            &repl,
            FullTextIndexConfig::collection(
                "words",
                CollectionId::new(7),
                FieldPath::new(["body"]),
            ),
        )?;
        assert!(matches!(
            text.refresh_incremental(&BTreeMap::new()),
            Err(TextError::Repl(ReplError::Unsupported(_)))
        ));
        let geo = GeoIndex::new(
            &repl,
            GeoIndexConfig::new("places", CollectionId::new(7), FieldPath::new(["point"])),
        )?;
        assert!(matches!(
            geo.refresh_incremental(&BTreeMap::new()),
            Err(GeoError::Repl(ReplError::Unsupported(_)))
        ));

        // Full rebuilds remain supported without reading a nonexistent commit log.
        assert_eq!(text.refresh_full()?.indexed_documents, 0);
        assert_eq!(geo.refresh_full()?, 0);
        Ok(())
    }
}
