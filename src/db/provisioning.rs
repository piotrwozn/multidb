//! Database-side helpers for spec provisioning (`crate::provision`): creating a table whose
//! physical layout differs from the database layout, so an analytics table can be columnar
//! inside a row-class database.

use super::{CatalogEntry, Database, DbError, catalog_put_op};
use crate::query::{RelIndexSpec, RelTable, TableLayout, TableSchema, schema_put_op};
use crate::repl::propose_system_batch;
use crate::security::{AuditEvent, AuditOutcome, Permission, Principal, Resource};

impl Database {
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
