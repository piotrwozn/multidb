//! Database-side helpers for spec provisioning (`crate::provision`): creating a table whose
//! physical layout differs from the database layout, so an analytics table can be columnar
//! inside a row-class database; the read-only views of the live database a provisioning plan is
//! computed from; and the writes an apply makes: the RBAC state in one batch, the audit flag for
//! local operators, creates that allocate ids, the provision record and its history, the
//! profile manifest and provisioning audit events; and re-applying the caller's audit key to a
//! handle opened from a spec.

use super::{
    AuditConfig, CatalogEntry, ConfigError, Database, DatabaseRepl, DbError, KEY_PROFILE_MANIFEST,
    KEY_SCHEMA_VERSION, PROVISION_TABLE, PersistedProfileManifest, ProbedMetadata,
    QueryGovernorOverrides, SCHEMA_VERSION, SCHEMA_VERSION_FRAMED, SecurityConfig, catalog_put_op,
    current_lsn_from_repl, meta_put_op, parse_audit_key_file, read_metadata_from_repl,
    read_security_from_repl, unmet_profile_floors, validate_manifest,
};
use crate::graph::GraphId;
use crate::model::{
    CollectionId, DOCUMENT_TABLE, IndexSpec, collection_range_bounds, document_id_from_key,
};
use crate::provision::{
    ProvisionActor, ProvisionHistoryEntry, ProvisionRecord,
    record::{RECORD_KEY, RecordState, decode_record},
};
use crate::query::{DocField, RelIndexSpec, RelTable, TableLayout, TableSchema, schema_put_op};
use crate::repl::{Op, ReadConsistency, Replication, propose_system_batch};
use crate::security::{
    AUDIT_TABLE, AuditEvent, AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry,
    Resource,
};
use crate::storage::StorageError;
use crate::txn::TxnId;
use crate::vector::{HnswParams, VectorCollectionConfig, VectorMetric};

/// Documents read per batch by `Database::document_count`.
const COUNT_BATCH_ROWS: usize = 1_024;
/// The key prefix of provision history entries in `__provision`: `history/{seq:020}`.
const HISTORY_PREFIX: &[u8] = b"history/";
/// The end of the history key range: the byte after `/` is `0`.
const HISTORY_END: &[u8] = b"history0";
/// History entries kept; the oldest are deleted when a new one is written.
pub(crate) const HISTORY_LIMIT: usize = 100;

/// One history row of `__provision`: its sequence, key and value.
type HistoryRow = (u64, Vec<u8>, Vec<u8>);

impl Database {
    /// The provision record of this database: what provisioning manages, the spec it last
    /// applied and how that apply ended. `None` for a database provisioning never touched.
    /// # Errors
    /// Fails when the record cannot be read, and with `DbError::Serde` when it is unreadable:
    /// written by a newer build (`record_version`) or corrupt.
    pub fn provision_state(&self) -> Result<Option<ProvisionRecord>, DbError> {
        match self.read_provision_record()? {
            RecordState::None => Ok(None),
            RecordState::Readable(record) => Ok(Some(*record)),
            RecordState::Unreadable(reason) => Err(DbError::Serde(format!(
                "the provision record is unreadable: {reason}"
            ))),
        }
    }

    /// Reads `__provision.record`. A record that does not decode, or that a newer build wrote,
    /// is `Unreadable` rather than an error, so a plan can report it.
    pub(crate) fn read_provision_record(&self) -> Result<RecordState, DbError> {
        let bytes =
            self.replication_ref()
                .read(PROVISION_TABLE, RECORD_KEY, ReadConsistency::Strong)?;
        Ok(bytes.map_or(RecordState::None, |bytes| decode_record(&bytes)))
    }

    /// This handle was opened with an encryption config, so values are encrypted at rest.
    pub(crate) const fn encryption_configured(&self) -> bool {
        self.encryption.is_some()
    }

    /// The LSN of the newest commit, 0 before the first one. An apply reads it around a step
    /// to tell whether the step committed anything before it failed.
    /// # Errors
    /// Fails when `__txn_meta` cannot be read or decoded.
    pub(crate) fn commit_lsn(&self) -> Result<TxnId, DbError> {
        Ok(current_lsn_from_repl(self.replication_ref())?)
    }

    /// The history GC horizon: `AS OF` reads and temporal retentions below it are refused. 0
    /// when GC never pruned.
    /// # Errors
    /// Fails when `__txn_meta` cannot be read or decoded.
    pub(crate) fn history_horizon(&self) -> Result<TxnId, DbError> {
        Ok(crate::txn::history::history_horizon(
            self.replication_ref(),
        )?)
    }

    /// The query governor knobs `override_query_governor` (or `set_query_governor`) set for this
    /// handle, as `multidb serve` does for each of `MULTIDB_MAX_CONCURRENT_QUERIES` and
    /// `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` that is set: those knobs are the caller's, and a restart
    /// sets them again. The others come from the profile manifest.
    pub(crate) const fn query_governor_overrides(&self) -> QueryGovernorOverrides {
        self.query_governor_overrides
    }

    /// Audit events are chained with a configured key rather than the built-in development key.
    pub(crate) const fn audit_key_configured(&self) -> bool {
        self.audit_config.key_path.is_some()
    }

    /// The audit key and anchor this handle records with.
    pub(crate) const fn audit_config(&self) -> &AuditConfig {
        &self.audit_config
    }

    /// Makes this handle chain audit events with the key and anchor of `audit`. The audit
    /// config is not persisted, and `open_database_with_ops` does not apply
    /// `OperationalConfig.security.audit`, so `provision::open_from_spec` re-applies the
    /// caller's config to the handle it opened; without it the handle falls back to the
    /// development key, and the chain no longer verifies. The key file is read and parsed first,
    /// so an unusable key fails here rather than at the next audit event.
    /// # Errors
    /// Fails with `Storage` when the key file cannot be read and with `AuditIntegrity` when it
    /// holds neither 32 raw bytes nor 64 hex characters.
    pub(crate) fn apply_audit_config(&mut self, audit: AuditConfig) -> Result<(), DbError> {
        if let Some(path) = &audit.key_path {
            let bytes = std::fs::read(path).map_err(StorageError::Io)?;
            parse_audit_key_file(&bytes).map_err(DbError::AuditIntegrity)?;
        }
        self.audit_config = audit;
        Ok(())
    }

    /// Whether the newest audit event verifies under the key this handle chains audit events
    /// with, so the events an apply appends keep `verify_audit_chain` green. `None` for an empty
    /// chain; `Some(false)` when the chain was written with another key (a configured key read
    /// by a handle that falls back to the development key, or the reverse) or its head event is
    /// missing. Reads every audit row once and decodes only the newest, unless the clock went
    /// backwards and the head event is not the last row.
    /// # Errors
    /// Fails when storage or the audit key file cannot be read, or an audit row cannot be
    /// decoded.
    pub(crate) fn audit_tail_matches_key(&self) -> Result<Option<bool>, DbError> {
        let head = self.audit_head()?;
        if head.sequence == 0 {
            return Ok(None);
        }
        let key = self.audit_key()?;
        let mut newest = None;
        self.repl.scan_range_batches(
            AUDIT_TABLE,
            &[],
            &[0xFF],
            ReadConsistency::Strong,
            COUNT_BATCH_ROWS,
            &|| false,
            &mut |batch| {
                if let Some((_, value)) = batch.last() {
                    newest = Some(value.clone());
                }
                Ok(true)
            },
        )?;
        let is_head = |event: &AuditEvent| {
            event
                .integrity
                .as_ref()
                .is_some_and(|integrity| integrity.sequence == head.sequence)
        };
        let mut tail = newest
            .and_then(|bytes| serde_json::from_slice::<AuditEvent>(&bytes).ok())
            .filter(is_head);
        if tail.is_none() {
            // Events are keyed by time, so after a clock step back the head is an earlier row.
            tail = self.audit_events()?.into_iter().find(is_head);
        }
        let Some(integrity) = tail.as_ref().and_then(|event| event.integrity.as_ref()) else {
            return Ok(Some(false));
        };
        let verifies = head.hash.as_deref() == Some(integrity.hash.as_str())
            && tail.as_ref().is_some_and(|event| {
                event
                    .verify_integrity_link(integrity.previous_hash.as_deref(), head.sequence, &key)
                    .is_ok()
            });
        Ok(Some(verifies))
    }

    /// The RBAC policy, principals and audit flag persisted in `__authz_policy`, which every
    /// opener loads. A database that never persisted them reads as `SecurityConfig::default()`.
    pub(crate) fn persisted_security_state(&self) -> Result<SecurityConfig, DbError> {
        Ok(read_security_from_repl(self.replication_ref())?.unwrap_or_default())
    }

    /// The metadata of this single-node database as `inspect_database_metadata` reports it for
    /// a file: storage profile, replication, layout, schema version, manifest and whether
    /// values are compression-framed (in `__meta`, or by this handle's engine).
    /// `unmet_floors` lists the manifest floors this handle does not meet, as the file probe
    /// does: `floors.audit` when audit is off, and `floors.encryption_at_rest` when the handle is
    /// not encrypted.
    /// # Errors
    /// Fails with `Unsupported` for CP, AP and sharded replication backends, which provisioning
    /// does not support, and when `__meta` cannot be read.
    pub(crate) fn probed_metadata(&self) -> Result<ProbedMetadata, DbError> {
        let DatabaseRepl::SingleNode(repl) = &self.repl else {
            return Err(ConfigError::Unsupported(
                "provisioning supports single-node databases only; this database uses a CP, \
                 AP or sharded replication backend"
                    .to_owned(),
            )
            .into());
        };
        let read = read_metadata_from_repl(repl.as_ref())?;
        let unmet_floors = read.manifest.as_ref().map_or_else(Vec::new, |manifest| {
            unmet_profile_floors(
                manifest.knobs.floors,
                self.audit_enabled(),
                self.encryption_configured(),
            )
        });
        Ok(ProbedMetadata {
            schema_version: read.metadata.schema_version,
            storage_profile: read.metadata.profile,
            replication: read.metadata.replication,
            layout: read.metadata.layout,
            manifest: read.manifest,
            values_framed: read.values_framed || repl.storage().is_value_framed(),
            unmet_floors,
        })
    }

    /// Counts the documents of a collection by key, in batches, without decoding them.
    /// # Errors
    /// Fails when storage cannot be read.
    pub(crate) fn document_count(&self, collection_id: CollectionId) -> Result<u64, DbError> {
        let (start, end) = collection_range_bounds(collection_id);
        let mut count = 0_u64;
        self.repl.scan_range_batches(
            DOCUMENT_TABLE,
            &start,
            &end,
            ReadConsistency::Strong,
            COUNT_BATCH_ROWS,
            &|| false,
            &mut |batch| {
                let documents = batch
                    .iter()
                    .filter(|(key, _)| document_id_from_key(key).is_some())
                    .count();
                count = count.saturating_add(u64::try_from(documents).unwrap_or(u64::MAX));
                Ok(true)
            },
        )?;
        Ok(count)
    }

    /// Creates a named relational table with an explicit physical layout and persists it in the
    /// catalog. The layout is stored with the table (`CatalogEntry::Table.layout`) and honoured
    /// by every later open, whatever the database layout is. A columnar table needs a schema and
    /// takes no B-tree indexes.
    /// # Errors
    /// Fails when the name collides, the schema, indexes or layout are invalid, or storage
    /// rejects the metadata.
    pub fn create_table_with_layout(
        &mut self,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
        layout: TableLayout,
    ) -> Result<RelTable, DbError> {
        self.ensure_non_tensor_model_allowed("create_table")?;
        let name = name.into();
        self.ensure_name_available(&name)?;
        let table = RelTable::handle_with_layout(
            self.replication_handle(),
            name.clone(),
            schema,
            indexes,
            layout,
        )?;

        let mut ops = Vec::new();
        if let Some(schema) = table.schema() {
            ops.push(schema_put_op(table.name(), schema)?);
        }

        let entry = CatalogEntry::Table {
            indexes: table.indexes().to_vec(),
            layout,
        };
        ops.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, ops)?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();

        Ok(table)
    }

    /// Creates a table with an explicit physical layout after checking `Admin` on the database.
    /// Audited like [`Database::create_table_as`], with the layout in the event detail.
    /// # Errors
    /// Fails when authorization or table creation fails.
    pub fn create_table_with_layout_as(
        &mut self,
        principal: &Principal,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
        layout: TableLayout,
    ) -> Result<RelTable, DbError> {
        let name = name.into();
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_table",
        )?;
        let table = self.create_table_with_layout(name.clone(), schema, indexes, layout)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_table",
            Resource::Table(name),
            AuditOutcome::Succeeded,
            Some(layout_detail(layout)),
        ))?;
        Ok(table)
    }
}

impl Database {
    /// The history of applies in `__provision`, oldest first; at most 100 entries are kept.
    /// # Errors
    /// Fails when the history cannot be read, and with `DbError::Serde` when an entry or its key
    /// does not decode.
    pub fn provision_history(&self) -> Result<Vec<ProvisionHistoryEntry>, DbError> {
        self.provision_history_rows()?
            .into_iter()
            .map(|(_, _, bytes)| {
                serde_json::from_slice(&bytes).map_err(|error| {
                    DbError::Serde(format!(
                        "a provision history entry does not decode: {error}"
                    ))
                })
            })
            .collect()
    }

    /// Replaces the RBAC policy and the principal registry after checking system admin
    /// permission. Both are persisted in one batch, with the audit flag, so no reader ever sees
    /// the new policy with the old principals. Audited as `set_security_state`.
    /// # Errors
    /// Fails when authorization, persistence or the audit write fails.
    pub fn set_security_state_as(
        &mut self,
        principal: &Principal,
        policy: AuthzPolicy,
        principals: PrincipalRegistry,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "set_security_state",
        )?;
        let detail = security_state_detail(&policy, &principals);
        self.set_security_state(policy, principals)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "set_security_state",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&detail),
        ))
    }

    /// Replaces the RBAC policy and the principal registry and persists both, with the audit
    /// flag, in one batch. Not audited: the caller audits the change.
    /// # Errors
    /// Fails when the security state cannot be persisted.
    pub(crate) fn set_security_state(
        &mut self,
        policy: AuthzPolicy,
        principals: PrincipalRegistry,
    ) -> Result<(), DbError> {
        self.authz = policy;
        self.principals = principals;
        self.persist_security_state()
    }

    /// Turns audit on or off for a local provisioning operator, which relies on filesystem trust
    /// rather than RBAC. Behaves like [`Database::set_audit_enabled_as`] after its authorization
    /// check: `floors.audit` refuses disabling, the disable event is written before the flag and
    /// the enable event after it, each with `actor_label` as the principal name.
    /// # Errors
    /// Fails as `set_audit_enabled_as` does, except for authorization.
    pub(crate) fn set_audit_enabled_by_provisioning(
        &mut self,
        actor_label: &str,
        enabled: bool,
    ) -> Result<(), DbError> {
        self.change_audit_enabled(&Principal::new(actor_label), enabled)
    }

    /// Checks that `principal` may apply a provisioning plan: `System:Admin` and
    /// `Database:Admin`, each audited when denied.
    /// # Errors
    /// Fails with `AuthzDenied` when either permission is missing.
    pub(crate) fn authorize_provision_actor(&self, principal: &Principal) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "provision_apply",
        )?;
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "provision_apply",
        )
    }

    /// Checks that `principal` may read provisioning plans, drift, state or the live spec:
    /// `System:Admin`, audited as `action` when denied. A plan shows the live catalog, RBAC and
    /// guarantees, so it needs the permission the security endpoints need.
    /// # Errors
    /// Fails with `AuthzDenied` when the permission is missing.
    pub(crate) fn authorize_provision_read(
        &self,
        principal: &Principal,
        action: &str,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(principal, &Resource::System, Permission::Admin, action)
    }

    /// Creates a document collection with the next available collection id after checking
    /// `Admin` on the database, audited like [`Database::create_collection_as`]. Returns the
    /// allocated id.
    /// # Errors
    /// Fails when authorization or collection creation fails.
    pub(crate) fn create_collection_auto_id_as(
        &mut self,
        principal: &Principal,
        name: &str,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    ) -> Result<CollectionId, DbError> {
        let collection_id = CollectionId::new(self.catalog_next_collection_id());
        let _ = self.create_collection_as(principal, name, collection_id, fields, indexes)?;
        Ok(collection_id)
    }

    /// Creates a vector collection with the next available collection id after checking
    /// `Admin` on the database, audited like [`Database::create_vector_collection_as`]. Returns
    /// the allocated id.
    /// # Errors
    /// Fails when authorization or vector collection creation fails.
    pub(crate) fn create_vector_collection_auto_id_as(
        &mut self,
        principal: &Principal,
        name: &str,
        dim: usize,
        metric: VectorMetric,
        hnsw: HnswParams,
    ) -> Result<CollectionId, DbError> {
        let collection_id = CollectionId::new(self.catalog_next_collection_id());
        let config = VectorCollectionConfig::new(collection_id, dim)
            .with_metric(metric)
            .with_hnsw(hnsw);
        let _ = self.create_vector_collection_as(principal, name, config)?;
        Ok(collection_id)
    }

    /// Creates a graph with the next available graph id after checking `Admin` on the
    /// database, audited like [`Database::create_graph_as`]. Returns the allocated id.
    /// # Errors
    /// Fails when authorization or graph creation fails.
    pub(crate) fn create_graph_auto_id_as(
        &mut self,
        principal: &Principal,
        name: &str,
    ) -> Result<GraphId, DbError> {
        let graph_id = GraphId::new(self.catalog_next_graph_id());
        let _ = self.create_graph_as(principal, name, graph_id)?;
        Ok(graph_id)
    }

    /// Records a provisioning audit event with the actor as its principal (a local operator's
    /// label is the principal name). Unlike other audit writes it fails when audit is disabled,
    /// because an apply must never mutate a database without its audit trail.
    /// # Errors
    /// Fails with `AuditIntegrity` when audit is disabled, and when the audit write fails.
    pub(crate) fn record_provision_event(
        &self,
        actor: &ProvisionActor,
        action: &str,
        resource: Resource,
        outcome: AuditOutcome,
        detail: &str,
    ) -> Result<(), DbError> {
        if !self.audit_enabled {
            return Err(DbError::AuditIntegrity(format!(
                "{action} requires audit to be enabled"
            )));
        }
        let principal = actor.audit_principal();
        self.record_audit(&AuditEvent::new(
            Some(&principal),
            action,
            resource,
            outcome,
            Some(detail),
        ))
    }

    /// Writes the provision record and, when given, appends a history entry under the next
    /// `history/{seq:020}` key, deleting the oldest entries beyond 100, all in one batch.
    /// # Errors
    /// Fails when the record cannot be serialized, the history cannot be read, or storage
    /// rejects the batch.
    pub(crate) fn write_provision_record(
        &self,
        record: &ProvisionRecord,
        history: Option<&ProvisionHistoryEntry>,
    ) -> Result<(), DbError> {
        let mut ops = vec![Op::Put {
            table: PROVISION_TABLE.to_owned(),
            key: RECORD_KEY.to_vec(),
            value: serde_json::to_vec(record).map_err(|error| DbError::Serde(error.to_string()))?,
        }];
        if let Some(entry) = history {
            let rows = self.provision_history_rows()?;
            let next = match rows.last() {
                Some((sequence, _, _)) => sequence.checked_add(1).ok_or_else(|| {
                    DbError::Serde("the provision history sequence overflowed".to_owned())
                })?,
                None => 1,
            };
            let mut key = HISTORY_PREFIX.to_vec();
            key.extend_from_slice(format!("{next:020}").as_bytes());
            ops.push(Op::Put {
                table: PROVISION_TABLE.to_owned(),
                key,
                value: serde_json::to_vec(entry)
                    .map_err(|error| DbError::Serde(error.to_string()))?,
            });
            let excess = rows.len().saturating_sub(HISTORY_LIMIT - 1);
            ops.extend(rows.into_iter().take(excess).map(|(_, key, _)| Op::Delete {
                table: PROVISION_TABLE.to_owned(),
                key,
            }));
        }
        propose_system_batch(&self.repl, ops)?;
        Ok(())
    }

    /// Writes `__meta.profile_manifest` and `__meta.schema_version` in one batch: the profile
    /// manifest an adoption, retune or storage format upgrade leaves. The manifest is validated
    /// against the storage profile of the database first, `schema_version` must be 2 exactly
    /// when the manifest frames values, value framing is never turned off, and the floors the
    /// runtime can check must hold, as `create_database_with_profile` requires. Only the
    /// metadata changes: the knobs and the framing apply from the next open, and values written
    /// before a format upgrade stay readable because frames are recognised by their magic.
    /// # Errors
    /// Fails with `Unsupported` for databases that are not single-node, with
    /// `ProfileManifestInvalid` for an invalid manifest or schema version, with
    /// `StorageFormatMismatch` when the database already frames values and the manifest does not,
    /// with `ProfileFloorNotMet` when the manifest sets `floors.encryption_at_rest` for a handle
    /// without encryption or `floors.audit` while audit is off, and when storage rejects the
    /// batch.
    pub(crate) fn write_profile_manifest(
        &self,
        manifest: &PersistedProfileManifest,
        schema_version: u32,
    ) -> Result<(), DbError> {
        let current = self.probed_metadata()?;
        validate_manifest(manifest, current.storage_profile)?;
        let framing = manifest.storage_format.value_framing;
        let expected = if framing {
            SCHEMA_VERSION_FRAMED
        } else {
            SCHEMA_VERSION
        };
        if schema_version != expected {
            return Err(ConfigError::ProfileManifestInvalid(format!(
                "a manifest {} value framing needs schema_version {expected}, not \
                 {schema_version}",
                if framing { "with" } else { "without" }
            ))
            .into());
        }
        let framed_now = current.values_framed
            || current
                .manifest
                .as_ref()
                .is_some_and(|manifest| manifest.storage_format.value_framing);
        if framed_now && !framing {
            return Err(ConfigError::StorageFormatMismatch(
                "the database frames its values, and value framing can never be turned off"
                    .to_owned(),
            )
            .into());
        }
        let floors = manifest.knobs.floors;
        if floors.encryption_at_rest && !self.encryption_configured() {
            return Err(ConfigError::ProfileFloorNotMet {
                floor: "floors.encryption_at_rest",
                reason: "the database is not encrypted at rest, so its manifest cannot require \
                         encryption"
                    .to_owned(),
            }
            .into());
        }
        if floors.audit && !self.audit_enabled {
            return Err(ConfigError::ProfileFloorNotMet {
                floor: "floors.audit",
                reason: "audit is off, so the manifest cannot require it".to_owned(),
            }
            .into());
        }
        propose_system_batch(
            &self.repl,
            vec![
                meta_put_op(KEY_PROFILE_MANIFEST, manifest)?,
                meta_put_op(KEY_SCHEMA_VERSION, &schema_version)?,
            ],
        )?;
        Ok(())
    }

    /// The history rows of `__provision` in key order: sequence, key and value.
    fn provision_history_rows(&self) -> Result<Vec<HistoryRow>, DbError> {
        self.replication_ref()
            .range(
                PROVISION_TABLE,
                HISTORY_PREFIX,
                HISTORY_END,
                ReadConsistency::Strong,
            )?
            .into_iter()
            .map(|(key, value)| {
                let sequence = key
                    .strip_prefix(HISTORY_PREFIX)
                    .and_then(|digits| std::str::from_utf8(digits).ok())
                    .filter(|digits| digits.len() == 20)
                    .and_then(|digits| digits.parse::<u64>().ok())
                    .ok_or_else(|| {
                        DbError::Serde(format!(
                            "{} is not a provision history key",
                            String::from_utf8_lossy(&key)
                        ))
                    })?;
                Ok((sequence, key, value))
            })
            .collect()
    }
}

/// The audit detail of a security state change: how many roles and principals it holds.
fn security_state_detail(policy: &AuthzPolicy, principals: &PrincipalRegistry) -> String {
    format!(
        "roles: {} principals: {}",
        policy.roles().len(),
        principals.principals().len()
    )
}

const fn layout_detail(layout: TableLayout) -> &'static str {
    match layout {
        TableLayout::Row => "layout: row",
        TableLayout::Columnar => "layout: columnar",
    }
}
