//! Database-side helpers for spec provisioning (`crate::provision`): creating a table whose
//! physical layout differs from the database layout, so an analytics table can be columnar
//! inside a row-class database, and the read-only views of the live database a provisioning
//! plan is computed from.

use super::{
    AuditConfig, CatalogEntry, ConfigError, Database, DatabaseRepl, DbError, PROVISION_TABLE,
    ProbedMetadata, SecurityConfig, catalog_put_op, read_metadata_from_repl,
    read_security_from_repl,
};
use crate::model::{CollectionId, DOCUMENT_TABLE, collection_range_bounds, document_id_from_key};
use crate::provision::{
    ProvisionRecord,
    record::{RECORD_KEY, RecordState, decode_record},
};
use crate::query::{RelIndexSpec, RelTable, TableLayout, TableSchema, schema_put_op};
use crate::repl::{ReadConsistency, Replication, propose_system_batch};
use crate::security::{AuditEvent, AuditOutcome, Permission, Principal, Resource};

/// Documents read per batch by `Database::document_count`.
const COUNT_BATCH_ROWS: usize = 1_024;

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

    /// Audit events are chained with a configured key rather than the built-in development key.
    pub(crate) const fn audit_key_configured(&self) -> bool {
        self.audit_config.key_path.is_some()
    }

    /// The audit key and anchor this handle records with.
    pub(crate) const fn audit_config(&self) -> &AuditConfig {
        &self.audit_config
    }

    /// The RBAC policy, principals and audit flag persisted in `__authz_policy`, which the next
    /// `open_database_with_ops` loads. A database that never persisted them reads as
    /// `SecurityConfig::default()`.
    pub(crate) fn persisted_security_state(&self) -> Result<SecurityConfig, DbError> {
        Ok(read_security_from_repl(self.replication_ref())?.unwrap_or_default())
    }

    /// The metadata of this single-node database as `inspect_database_metadata` reports it for
    /// a file: storage profile, replication, layout, schema version, manifest and whether
    /// values are compression-framed (in `__meta`, or by this handle's engine).
    /// `unmet_floors` lists `floors.encryption_at_rest` when the manifest sets it and this
    /// handle is not encrypted.
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
        let unmet_floors = read
            .manifest
            .as_ref()
            .filter(|manifest| {
                manifest.knobs.floors.encryption_at_rest && !self.encryption_configured()
            })
            .map(|_| vec!["floors.encryption_at_rest"])
            .unwrap_or_default();
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

const fn layout_detail(layout: TableLayout) -> &'static str {
    match layout {
        TableLayout::Row => "layout: row",
        TableLayout::Columnar => "layout: columnar",
    }
}
