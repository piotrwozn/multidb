//! Physical projection: the catalog objects a [`DatabaseSpec`] describes, as the runtime types
//! the `Database::create_*` APIs take.
//!
//! Each collection maps to exactly one container object (document collection, table, vector
//! collection, time series or graph), followed by the objects it derives: full-text and geo
//! indexes on a document collection, and the `<table>_history` table of a system-versioned
//! table. Defaults are pinned (HNSW 16/200/48, full-text language `simple` with refresh lag 1,
//! geo precision 6, index ids at position + 1, chunk and retention from the profile knobs), so
//! a later build projects the same objects from the same spec.

use std::collections::BTreeMap;

use serde_json::{Value as JsonValue, json};

use super::record::ObjectKind;
use crate::config_spec::{
    CertificationImpact, CheckMode, ColumnSpec, ColumnTypeSpec, DatabaseSpec, LayoutKnob,
    PlannedDocIndex, PlannedField, PlannedKind, PlannedObject, PlannedTableIndex, ResolvedProfile,
    SpecPath, ValidationIssue, ValidationSeverity, VectorMetricSpec, plan_catalog,
};
use crate::db::CatalogEntry;
use crate::graph::GraphId;
use crate::model::{CollectionId, FieldPath, IndexId, IndexSpec};
use crate::query::{
    ColumnDef, ColumnType, DocField, DocFieldSource, RelIndexExpression, RelIndexSpec, TableLayout,
    TableSchema,
};
use crate::text::TextSource;
use crate::timeseries::TimeSeriesConfig;
use crate::vector::{HnswParams, VectorMetric};

/// One catalog object provisioning creates or matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredObject {
    /// The catalog name: the collection name, or a derived or explicit index or history name.
    pub name: String,
    /// The spec collection the object comes from.
    pub spec_collection: String,
    pub kind: DesiredKind,
}

/// The runtime parameters of a [`DesiredObject`]. Only what a spec controls is here; for
/// example vector quantization keeps its runtime default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DesiredKind {
    /// `create_collection` (pinned id) or `create_collection_auto_id`.
    Document {
        collection_id: Option<CollectionId>,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    },
    /// `create_table_with_layout`; `schema` is `None` for a schemaless row table.
    Table {
        layout: TableLayout,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
    },
    /// `create_vector_collection` (pinned id) or `create_vector_collection_auto_id`.
    Vector {
        collection_id: Option<CollectionId>,
        dim: usize,
        metric: VectorMetric,
        hnsw: HnswParams,
    },
    /// `create_time_series` with [`time_series_config`].
    TimeSeries {
        chunk_millis: i64,
        retention_millis: Option<i64>,
        max_future_skew_millis: i64,
    },
    /// `create_graph` (pinned id) or `create_graph_auto_id`.
    Graph { graph_id: Option<GraphId> },
    /// `create_full_text_index` on the document collection `source`.
    FullText {
        source: String,
        path: FieldPath,
        language: String,
        refresh_lag_target: u64,
    },
    /// `create_geo_index` on the document collection `source`.
    Geo {
        source: String,
        path: FieldPath,
        precision: u8,
    },
    /// `enable_system_versioning` on `base_table`, which registers `<base_table>_history`.
    History {
        base_table: String,
        keep_history: bool,
    },
}

impl DesiredKind {
    /// The kind as a lowercase slug: `document`, `table`, `vector`, `time_series`, `graph`,
    /// `full_text`, `geo` or `history`.
    #[must_use]
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Document { .. } => "document",
            Self::Table { .. } => "table",
            Self::Vector { .. } => "vector",
            Self::TimeSeries { .. } => "time_series",
            Self::Graph { .. } => "graph",
            Self::FullText { .. } => "full_text",
            Self::Geo { .. } => "geo",
            Self::History { .. } => "history",
        }
    }
}

impl DesiredObject {
    /// The parameters in spec vocabulary (the keys of `collections[].physical`), with every
    /// default filled in. Stable across builds, so plans can show and diff them.
    #[must_use]
    pub fn params_json(&self) -> JsonValue {
        match &self.kind {
            DesiredKind::Document {
                collection_id,
                fields,
                indexes,
            } => document_json(collection_id.map(CollectionId::as_u32), fields, indexes),
            DesiredKind::Table {
                layout,
                schema,
                indexes,
            } => table_json(*layout, schema.as_ref(), indexes),
            DesiredKind::Vector {
                collection_id,
                dim,
                metric,
                hnsw,
            } => vector_json(collection_id.map(CollectionId::as_u32), *dim, *metric, hnsw),
            DesiredKind::TimeSeries {
                chunk_millis,
                retention_millis,
                max_future_skew_millis,
            } => time_series_json(*chunk_millis, *retention_millis, *max_future_skew_millis),
            DesiredKind::Graph { graph_id } => graph_json(graph_id.map(GraphId::as_u32)),
            DesiredKind::FullText {
                source,
                path,
                language,
                refresh_lag_target,
            } => full_text_json(source, path, language, *refresh_lag_target),
            DesiredKind::Geo {
                source,
                path,
                precision,
            } => geo_json(source, path, *precision),
            DesiredKind::History {
                base_table,
                keep_history,
            } => history_json(base_table, *keep_history),
        }
    }
}

impl DesiredKind {
    /// The kind in the vocabulary of provision records and plans.
    #[must_use]
    pub const fn object_kind(&self) -> ObjectKind {
        match self {
            Self::Document { .. } => ObjectKind::Document,
            Self::Table { .. } => ObjectKind::Table,
            Self::Vector { .. } => ObjectKind::Vector,
            Self::TimeSeries { .. } => ObjectKind::TimeSeries,
            Self::Graph { .. } => ObjectKind::Graph,
            Self::FullText { .. } => ObjectKind::FullText,
            Self::Geo { .. } => ObjectKind::Geo,
            Self::History { .. } => ObjectKind::History,
        }
    }
}

/// The kind of a live catalog entry.
pub(crate) const fn entry_kind(entry: &CatalogEntry) -> ObjectKind {
    match entry {
        CatalogEntry::Collection { .. } => ObjectKind::Document,
        CatalogEntry::Table { .. } => ObjectKind::Table,
        CatalogEntry::Vector { .. } => ObjectKind::Vector,
        CatalogEntry::FullTextIndex { .. } => ObjectKind::FullText,
        CatalogEntry::TimeSeries { .. } => ObjectKind::TimeSeries,
        CatalogEntry::Graph { .. } => ObjectKind::Graph,
        CatalogEntry::GeoIndex { .. } => ObjectKind::Geo,
        CatalogEntry::ForeignTable { .. } => ObjectKind::ForeignTable,
        CatalogEntry::MaterializedView { .. } => ObjectKind::MaterializedView,
        CatalogEntry::TemporalTable { .. } => ObjectKind::History,
    }
}

/// The parameters of a live catalog entry in the vocabulary of [`DesiredObject::params_json`],
/// so a plan can show what differs. Full-text and geo sources are named by the live collection
/// that holds their collection id (`#<id>` when none does). A partial table index, which a spec
/// cannot express, adds `"partial_indexes": true`.
pub(crate) fn live_params_json(
    name: &str,
    entry: &CatalogEntry,
    catalog: &BTreeMap<String, CatalogEntry>,
    schemas: &BTreeMap<String, TableSchema>,
) -> JsonValue {
    match entry {
        CatalogEntry::Collection {
            collection_id,
            fields,
            indexes,
        } => document_json(Some(collection_id.as_u32()), fields, indexes),
        CatalogEntry::Table { indexes, layout } => {
            let mut params = table_json(*layout, schemas.get(name), indexes);
            if indexes.iter().any(|index| index.predicate.is_some()) {
                params["partial_indexes"] = json!(true);
            }
            params
        }
        CatalogEntry::Vector {
            collection_id,
            dim,
            metric,
            hnsw,
            ..
        } => vector_json(Some(collection_id.as_u32()), *dim, *metric, hnsw),
        CatalogEntry::FullTextIndex { config } => {
            let TextSource::Collection {
                collection_id,
                path,
            } = &config.source;
            full_text_json(
                &source_name(catalog, *collection_id),
                path,
                &config.language,
                config.refresh_lag_target,
            )
        }
        CatalogEntry::TimeSeries { config } => time_series_json(
            config.chunk_millis,
            config.retention_millis,
            config.max_future_skew_millis,
        ),
        CatalogEntry::Graph { graph_id } => graph_json(Some(graph_id.as_u32())),
        CatalogEntry::GeoIndex { config } => geo_json(
            &source_name(catalog, config.collection_id),
            &config.path,
            config.precision,
        ),
        CatalogEntry::ForeignTable { .. } => json!({ "kind": "foreign_table" }),
        CatalogEntry::MaterializedView { .. } => json!({ "kind": "materialized_view" }),
        CatalogEntry::TemporalTable {
            base_table,
            retention,
            ..
        } => history_json(base_table, retention.keep_history),
    }
}

/// The name of the live document collection with `collection_id`, or `#<id>`.
pub(crate) fn source_name(
    catalog: &BTreeMap<String, CatalogEntry>,
    collection_id: CollectionId,
) -> String {
    catalog
        .iter()
        .find_map(|(name, entry)| match entry {
            CatalogEntry::Collection {
                collection_id: id, ..
            } if *id == collection_id => Some(name.clone()),
            _ => None,
        })
        .unwrap_or_else(|| format!("#{}", collection_id.as_u32()))
}

fn document_json(
    collection_id: Option<u32>,
    fields: &[DocField],
    indexes: &[IndexSpec],
) -> JsonValue {
    json!({
        "kind": "document",
        "collection_id": collection_id,
        "fields": fields.iter().map(field_json).collect::<Vec<_>>(),
        "document_indexes": indexes
            .iter()
            .map(|index| json!({ "path": path_text(&index.path), "id": index.id.as_u32() }))
            .collect::<Vec<_>>(),
    })
}

fn vector_json(
    collection_id: Option<u32>,
    dim: usize,
    metric: VectorMetric,
    hnsw: &HnswParams,
) -> JsonValue {
    json!({
        "kind": "vector",
        "collection_id": collection_id,
        "dim": dim,
        "metric": match metric {
            VectorMetric::Cosine => "cosine",
            VectorMetric::L2 => "l2",
        },
        "hnsw": {
            "m": hnsw.m,
            "ef_construction": hnsw.ef_construction,
            "ef_search": hnsw.ef_search,
        },
    })
}

/// No future-skew bound (`i64::MAX`) is `null`, as in a spec: plans and drift go to JavaScript
/// clients, which cannot represent `i64::MAX`.
fn time_series_json(
    chunk_millis: i64,
    retention_millis: Option<i64>,
    max_future_skew_millis: i64,
) -> JsonValue {
    json!({
        "kind": "time_series",
        "chunk_millis": chunk_millis,
        "retention_millis": retention_millis,
        "max_future_skew_millis": (max_future_skew_millis != i64::MAX).then_some(max_future_skew_millis),
    })
}

fn graph_json(graph_id: Option<u32>) -> JsonValue {
    json!({ "kind": "graph", "graph_id": graph_id })
}

fn full_text_json(
    source: &str,
    path: &FieldPath,
    language: &str,
    refresh_lag_target: u64,
) -> JsonValue {
    json!({
        "kind": "full_text",
        "source": source,
        "path": path_text(path),
        "language": language,
        "refresh_lag_target": refresh_lag_target,
    })
}

fn geo_json(source: &str, path: &FieldPath, precision: u8) -> JsonValue {
    json!({
        "kind": "geo",
        "source": source,
        "path": path_text(path),
        "precision": precision,
    })
}

fn history_json(base_table: &str, keep_history: bool) -> JsonValue {
    json!({
        "kind": "history",
        "base_table": base_table,
        "keep_history": keep_history,
    })
}

/// The [`TimeSeriesConfig`] of a time-series object, or `None` for any other kind. The only
/// place provisioning builds one, so a new runtime field changes one line.
#[must_use]
pub fn time_series_config(name: &str, desired: &DesiredKind) -> Option<TimeSeriesConfig> {
    let DesiredKind::TimeSeries {
        chunk_millis,
        retention_millis,
        max_future_skew_millis,
    } = desired
    else {
        return None;
    };
    let config = TimeSeriesConfig::new(name)
        .with_chunk_millis(*chunk_millis)
        .with_max_future_skew_millis(*max_future_skew_millis);
    Some(match retention_millis {
        Some(retention) => config.with_retention_millis(*retention),
        None => config,
    })
}

/// Projects `spec` onto the catalog objects provisioning would create, in spec order with each
/// collection's derived objects right after it.
///
/// Projection is always strict, whatever the spec version: the version 2 checks are Errors,
/// name and size problems that version 1 validation only warns about are Errors, and so is
/// `PHYSICAL_PARAMETER_REQUIRED` (a version 1 collection with a `document` index but no
/// `physical.document_indexes`, or a vector collection without `physical.dim`). `resolved` is
/// the spec's resolved profile; it supplies the database layout and the history and time-series
/// defaults.
///
/// # Errors
/// Returns every blocking issue when any object cannot be built from the spec.
pub fn project(
    spec: &DatabaseSpec,
    resolved: &ResolvedProfile,
) -> Result<Vec<DesiredObject>, Vec<ValidationIssue>> {
    let plan = plan_catalog(spec, Some(resolved), CheckMode::Provisioning);
    let errors = plan
        .issues
        .into_iter()
        .filter(|issue| issue.severity == ValidationSeverity::Error)
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(errors);
    }
    let mut objects = Vec::with_capacity(plan.objects.len());
    let mut errors = Vec::new();
    for planned in plan.objects {
        match desired_object(planned) {
            Ok(object) => objects.push(object),
            Err(issue) => errors.push(issue),
        }
    }
    if errors.is_empty() {
        Ok(objects)
    } else {
        Err(errors)
    }
}

fn desired_object(planned: PlannedObject) -> Result<DesiredObject, ValidationIssue> {
    let PlannedObject {
        name,
        spec_collection,
        path,
        kind,
    } = planned;
    let kind = match kind {
        PlannedKind::Document {
            collection_id,
            fields,
            indexes,
        } => DesiredKind::Document {
            collection_id: collection_id.map(CollectionId::new),
            fields: fields.into_iter().map(doc_field).collect(),
            indexes: indexes.into_iter().map(doc_index).collect(),
        },
        PlannedKind::Table {
            layout,
            columns,
            primary_key,
            indexes,
        } => table_kind(&name, &path, layout, columns, primary_key, indexes)?,
        PlannedKind::Vector {
            collection_id,
            dim,
            metric,
            m,
            ef_construction,
            ef_search,
        } => DesiredKind::Vector {
            collection_id: collection_id.map(CollectionId::new),
            dim: to_usize(dim),
            metric: match metric {
                VectorMetricSpec::Cosine => VectorMetric::Cosine,
                VectorMetricSpec::L2 => VectorMetric::L2,
            },
            hnsw: HnswParams {
                m: to_usize(m),
                ef_construction: to_usize(ef_construction),
                ef_search: to_usize(ef_search),
            },
        },
        PlannedKind::TimeSeries {
            chunk_millis,
            retention_millis,
            max_future_skew_millis,
        } => DesiredKind::TimeSeries {
            chunk_millis,
            retention_millis,
            max_future_skew_millis,
        },
        PlannedKind::Graph { graph_id } => DesiredKind::Graph {
            graph_id: graph_id.map(GraphId::new),
        },
        PlannedKind::FullText {
            source,
            path,
            language,
            refresh_lag_target,
        } => DesiredKind::FullText {
            source,
            path: FieldPath::new(path),
            language,
            refresh_lag_target,
        },
        PlannedKind::Geo {
            source,
            path,
            precision,
        } => DesiredKind::Geo {
            source,
            path: FieldPath::new(path),
            precision,
        },
        PlannedKind::History {
            base_table,
            keep_history,
        } => DesiredKind::History {
            base_table,
            keep_history,
        },
    };
    Ok(DesiredObject {
        name,
        spec_collection,
        kind,
    })
}

fn table_kind(
    name: &str,
    path: &str,
    layout: LayoutKnob,
    columns: Vec<ColumnSpec>,
    primary_key: Option<usize>,
    indexes: Vec<PlannedTableIndex>,
) -> Result<DesiredKind, ValidationIssue> {
    let schema = match (columns.is_empty(), primary_key) {
        (true, _) => None,
        (false, Some(primary_key)) => Some(TableSchema::new(
            columns.into_iter().map(column_def).collect(),
            primary_key,
        )),
        (false, None) => {
            return Err(projection_error(
                path,
                format!("table {name} has columns but no resolved primary key"),
            ));
        }
    };
    Ok(DesiredKind::Table {
        layout: match layout {
            LayoutKnob::Row => TableLayout::Row,
            LayoutKnob::Columnar => TableLayout::Columnar,
        },
        schema,
        indexes: indexes.into_iter().map(table_index).collect(),
    })
}

fn projection_error(path: &str, message: String) -> ValidationIssue {
    ValidationIssue {
        code: "PHYSICAL_PARAMETER_REQUIRED".to_owned(),
        severity: ValidationSeverity::Error,
        path: path.to_owned(),
        message,
        suggestion: "Fix the physical parameters of the collection and re-run validation."
            .to_owned(),
        certification_impact: CertificationImpact::BlocksCertified,
    }
}

fn to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

const fn column_type(ty: ColumnTypeSpec) -> ColumnType {
    match ty {
        ColumnTypeSpec::Int => ColumnType::Int,
        ColumnTypeSpec::Float => ColumnType::Float,
        ColumnTypeSpec::Str => ColumnType::Str,
        ColumnTypeSpec::Bool => ColumnType::Bool,
        ColumnTypeSpec::Bytes => ColumnType::Bytes,
    }
}

const fn column_type_slug(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Int => "int",
        ColumnType::Float => "float",
        ColumnType::Str => "str",
        ColumnType::Bool => "bool",
        ColumnType::Bytes => "bytes",
        ColumnType::Null => "null",
    }
}

fn doc_field(field: PlannedField) -> DocField {
    let source = match field.source {
        SpecPath::DocumentId => DocFieldSource::DocumentId,
        SpecPath::Field(segments) => DocFieldSource::Path(FieldPath::new(segments)),
    };
    DocField::new(field.name, source, column_type(field.ty))
}

fn doc_index(index: PlannedDocIndex) -> IndexSpec {
    IndexSpec::new(IndexId::new(index.id), FieldPath::new(index.path))
}

fn column_def(column: ColumnSpec) -> ColumnDef {
    ColumnDef::new(column.name, column_type(column.ty), column.nullable)
}

fn table_index(index: PlannedTableIndex) -> RelIndexSpec {
    let spec = if index.lower_ascii {
        RelIndexSpec::lower_ascii(index.id, index.column)
    } else {
        RelIndexSpec::new(index.id, index.column)
    };
    spec.with_include(index.include)
}

fn path_text(path: &FieldPath) -> String {
    path.segments().join(".")
}

fn field_json(field: &DocField) -> JsonValue {
    json!({
        "name": field.name,
        "path": match &field.source {
            DocFieldSource::DocumentId => crate::config_spec::DOCUMENT_ID_PATH.to_owned(),
            DocFieldSource::Path(path) => path_text(path),
        },
        "type": column_type_slug(&field.ty),
    })
}

fn table_json(
    layout: TableLayout,
    schema: Option<&TableSchema>,
    indexes: &[RelIndexSpec],
) -> JsonValue {
    let column_name = |column: usize| -> JsonValue {
        schema
            .and_then(|schema| schema.columns.get(column))
            .map_or_else(|| json!(column), |column| json!(column.name))
    };
    json!({
        "kind": "table",
        "layout": match layout {
            TableLayout::Row => "row",
            TableLayout::Columnar => "columnar",
        },
        "columns": schema
            .map(|schema| {
                schema
                    .columns
                    .iter()
                    .map(|column| json!({
                        "name": column.name,
                        "type": column_type_slug(&column.ty),
                        "nullable": column.nullable,
                    }))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        "primary_key": schema.map(|schema| column_name(schema.primary_key)),
        "table_indexes": indexes
            .iter()
            .map(|index| json!({
                "column": column_name(index.column),
                "expression": match index.expression {
                    RelIndexExpression::Column(_) => "column",
                    RelIndexExpression::LowerAscii(_) => "lower_ascii",
                },
                "include": index.include.iter().map(|column| column_name(*column)).collect::<Vec<_>>(),
                "id": index.id,
            }))
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value as JsonValue, json};

    use super::{DesiredKind, DesiredObject, project, time_series_config};
    use crate::config_spec::{
        CollectionRole, CollectionRoleSpec, DATABASE_SPEC_VERSION_V2, DatabaseSpec, PhysicalKind,
        ResolvedProfile, ValidationIssue, ValidationSeverity, allowed_kinds,
        collection_role_definitions, resolve_profile,
        spec_io::{SpecFormat, decode_spec_bytes},
    };
    use crate::db::{DbConfig, Profile};
    use crate::model::{CollectionId, FieldPath, IndexId, IndexSpec};
    use crate::query::{ColumnType, DocField, RelIndexSpec, TableLayout};
    use crate::vector::{HnswParams, VectorMetric};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn v2_spec(
        profile: &str,
        collections: &[JsonValue],
    ) -> Result<DatabaseSpec, serde_json::Error> {
        let mut spec =
            DatabaseSpec::from_db_config("app", &DbConfig::on_disk(Profile::Balanced, "app.redb"));
        spec.version = DATABASE_SPEC_VERSION_V2;
        spec.profile = profile.to_owned();
        spec.collections = collections
            .iter()
            .map(|collection| {
                let mut collection = collection.clone();
                collection["domain"] = json!("primary");
                serde_json::from_value::<CollectionRoleSpec>(collection)
            })
            .collect::<Result<_, _>>()?;
        Ok(spec)
    }

    fn resolved(spec: &DatabaseSpec) -> Result<ResolvedProfile, Box<dyn std::error::Error>> {
        resolve_profile(spec)
            .resolved()
            .cloned()
            .ok_or_else(|| "the test profile resolves".into())
    }

    fn projected(spec: &DatabaseSpec) -> Result<Vec<DesiredObject>, Box<dyn std::error::Error>> {
        project(spec, &resolved(spec)?).map_err(|issues| format!("{issues:#?}").into())
    }

    fn rejected(spec: &DatabaseSpec) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
        match project(spec, &resolved(spec)?) {
            Ok(objects) => Err(format!("expected issues, projected {objects:#?}").into()),
            Err(issues) => {
                assert!(
                    issues
                        .iter()
                        .all(|issue| issue.severity == ValidationSeverity::Error),
                    "{issues:#?}"
                );
                Ok(codes(&issues))
            }
        }
    }

    fn codes(issues: &[ValidationIssue]) -> Vec<(String, String)> {
        issues
            .iter()
            .map(|issue| (issue.code.clone(), issue.path.clone()))
            .collect()
    }

    fn pair(code: &str, path: &str) -> (String, String) {
        (code.to_owned(), path.to_owned())
    }

    const fn role_slug(role: CollectionRole) -> &'static str {
        match role {
            CollectionRole::DocumentEntity => "document_entity",
            CollectionRole::KeyValue => "key_value",
            CollectionRole::EventLog => "event_log",
            CollectionRole::VectorMemory => "vector_memory",
            CollectionRole::Cache => "cache",
            CollectionRole::Audit => "audit",
            CollectionRole::Graph => "graph",
            CollectionRole::Analytics => "analytics",
            CollectionRole::TimeSeries => "time_series",
        }
    }

    /// A minimal collection of `kind` and the projection it must give.
    fn golden(kind: PhysicalKind) -> (JsonValue, JsonValue, JsonValue) {
        match kind {
            PhysicalKind::Document => (
                json!({ "kind": "document" }),
                json!(["primary"]),
                json!({
                    "kind": "document",
                    "collection_id": null,
                    "fields": [{ "name": "id", "path": "$id", "type": "bytes" }],
                    "document_indexes": [],
                }),
            ),
            PhysicalKind::Table => (
                json!({
                    "kind": "table",
                    "columns": [
                        { "name": "id", "type": "str" },
                        { "name": "amount", "type": "float", "nullable": true }
                    ],
                    "primary_key": "id"
                }),
                json!(["primary"]),
                json!({
                    "kind": "table",
                    "layout": "row",
                    "columns": [
                        { "name": "id", "type": "str", "nullable": false },
                        { "name": "amount", "type": "float", "nullable": true }
                    ],
                    "primary_key": "id",
                    "table_indexes": [],
                }),
            ),
            PhysicalKind::Vector => (
                json!({ "dim": 3 }),
                json!(["vector"]),
                json!({
                    "kind": "vector",
                    "collection_id": null,
                    "dim": 3,
                    "metric": "cosine",
                    "hnsw": { "m": 16, "ef_construction": 200, "ef_search": 48 },
                }),
            ),
            PhysicalKind::TimeSeries => (
                json!({}),
                json!(["time_series"]),
                json!({
                    "kind": "time_series",
                    "chunk_millis": 3_600_000,
                    "retention_millis": null,
                    "max_future_skew_millis": 300_000,
                }),
            ),
            PhysicalKind::Graph => (
                json!({}),
                json!(["graph"]),
                json!({ "kind": "graph", "graph_id": null }),
            ),
        }
    }

    const ALL_KINDS: [PhysicalKind; 5] = [
        PhysicalKind::Document,
        PhysicalKind::Table,
        PhysicalKind::Vector,
        PhysicalKind::TimeSeries,
        PhysicalKind::Graph,
    ];

    #[test]
    fn every_role_and_allowed_kind_projects_its_golden_object() -> TestResult {
        let mut covered = 0;
        for definition in collection_role_definitions() {
            let role = definition.role;
            for kind in allowed_kinds(role) {
                let (physical, indexes, expected) = golden(*kind);
                let mut physical = physical;
                if role == CollectionRole::Analytics {
                    // Analytics tables are columnar unless the spec says otherwise.
                    physical["layout"] = json!("row");
                }
                let spec = v2_spec(
                    "balanced",
                    &[json!({
                        "name": "items",
                        "role": role_slug(role),
                        "indexes": indexes,
                        "physical": physical,
                    })],
                )?;

                let objects = projected(&spec)?;

                assert_eq!(objects.len(), 1, "{role:?} {kind:?}: {objects:#?}");
                assert_eq!(objects[0].name, "items");
                assert_eq!(objects[0].spec_collection, "items");
                assert_eq!(objects[0].params_json(), expected, "{role:?} {kind:?}");
                covered += 1;
            }
        }
        assert_eq!(covered, 14);
        Ok(())
    }

    #[test]
    fn kinds_outside_the_role_are_rejected() -> TestResult {
        for definition in collection_role_definitions() {
            let role = definition.role;
            for kind in ALL_KINDS
                .iter()
                .filter(|kind| !allowed_kinds(role).contains(kind))
            {
                let (mut physical, indexes, _) = golden(*kind);
                physical["kind"] = serde_json::to_value(kind)?;
                let spec = v2_spec(
                    "balanced",
                    &[json!({
                        "name": "items",
                        "role": role_slug(role),
                        "indexes": indexes,
                        "physical": physical,
                    })],
                )?;

                let issues = rejected(&spec)?;

                assert!(
                    issues.contains(&pair(
                        "PHYSICAL_KIND_INCOMPATIBLE_WITH_ROLE",
                        "collections[0].physical.kind"
                    )),
                    "{role:?} {kind:?}: {issues:?}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn analytics_tables_default_to_columnar_and_others_to_the_database_layout() -> TestResult {
        let columns =
            json!([{ "name": "id", "type": "str" }, { "name": "value", "type": "float" }]);
        let collections = [
            json!({
                "name": "rollups",
                "role": "analytics",
                "indexes": ["columnar"],
                "physical": { "columns": columns, "primary_key": "id" },
            }),
            json!({
                "name": "ledger",
                "role": "event_log",
                "indexes": ["primary"],
                "physical": { "kind": "table", "columns": columns, "primary_key": "id" },
            }),
        ];

        let balanced = projected(&v2_spec("balanced", &collections)?)?;
        let analytical = projected(&v2_spec("analytics_columnar", &collections)?)?;

        let layouts = |objects: &[DesiredObject]| {
            objects
                .iter()
                .map(|object| match &object.kind {
                    DesiredKind::Table { layout, .. } => Some(*layout),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            layouts(&balanced),
            [Some(TableLayout::Columnar), Some(TableLayout::Row)]
        );
        assert_eq!(
            layouts(&analytical),
            [Some(TableLayout::Columnar), Some(TableLayout::Columnar)]
        );
        Ok(())
    }

    #[test]
    fn derived_names_follow_their_source() -> TestResult {
        let spec = v2_spec(
            "balanced",
            &[
                json!({
                    "name": "orders",
                    "role": "document_entity",
                    "indexes": ["primary", "document", "full_text", "geo"],
                    "physical": {
                        "fields": [{ "name": "customer_id", "path": "customer.id", "type": "str" }],
                        "document_indexes": [{ "path": "customer.id" }],
                        "full_text": [{ "path": "notes" }, { "path": "summary", "name": "orders_search" }],
                        "geo": [{ "path": "shipping.location", "precision": 7 }]
                    },
                }),
                json!({
                    "name": "ledger",
                    "role": "event_log",
                    "indexes": ["primary"],
                    "physical": {
                        "kind": "table",
                        "columns": [{ "name": "entry_id", "type": "int" }],
                        "primary_key": "entry_id",
                        "system_versioning": true
                    },
                }),
            ],
        )?;

        let objects = projected(&spec)?;
        let names = objects
            .iter()
            .map(|object| (object.name.as_str(), object.kind.slug()))
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                ("orders", "document"),
                ("orders_notes_fts", "full_text"),
                ("orders_search", "full_text"),
                ("orders_shipping_location_geo", "geo"),
                ("ledger", "table"),
                ("ledger_history", "history"),
            ]
        );
        assert_eq!(
            objects[3].params_json(),
            json!({
                "kind": "geo",
                "source": "orders",
                "path": "shipping.location",
                "precision": 7,
            })
        );
        assert_eq!(
            objects[5].kind,
            DesiredKind::History {
                base_table: "ledger".to_owned(),
                keep_history: true,
            }
        );
        assert!(
            objects
                .iter()
                .all(|object| object.spec_collection == object.name
                    || object.name.starts_with(&object.spec_collection))
        );
        Ok(())
    }

    #[test]
    fn derived_and_explicit_names_collide_in_one_namespace() -> TestResult {
        let spec = v2_spec(
            "balanced",
            &[
                json!({
                    "name": "orders",
                    "role": "document_entity",
                    "indexes": ["full_text", "geo"],
                    "physical": {
                        "full_text": [{ "path": "notes" }],
                        "geo": [{ "path": "spot", "name": "places" }]
                    },
                }),
                json!({ "name": "orders_notes_fts", "role": "key_value", "indexes": ["primary"] }),
                json!({ "name": "places", "role": "key_value", "indexes": ["primary"] }),
            ],
        )?;

        let issues = rejected(&spec)?;

        assert_eq!(
            issues,
            [
                pair("OBJECT_NAME_COLLISION", "collections[1].name"),
                pair("OBJECT_NAME_COLLISION", "collections[2].name"),
            ]
        );
        Ok(())
    }

    #[test]
    fn path_rules_are_enforced() -> TestResult {
        let too_deep = ["a"; 17].join(".");
        for (physical, path) in [
            (
                json!({ "document_indexes": [{ "path": "a..b" }] }),
                "collections[0].physical.document_indexes[0].path",
            ),
            (
                json!({ "document_indexes": [{ "path": "1abc" }] }),
                "collections[0].physical.document_indexes[0].path",
            ),
            (
                json!({ "document_indexes": [{ "path": "$id" }] }),
                "collections[0].physical.document_indexes[0].path",
            ),
            (
                json!({ "document_indexes": [{ "path": too_deep }] }),
                "collections[0].physical.document_indexes[0].path",
            ),
            (
                json!({
                    "document_indexes": [{ "path": "a" }],
                    "fields": [{ "name": "id", "path": "$id", "type": "str" }]
                }),
                "collections[0].physical.fields[0].type",
            ),
            (
                json!({
                    "document_indexes": [{ "path": "a" }],
                    "fields": [{ "name": "spot", "path": "geo-point", "type": "str" }]
                }),
                "collections[0].physical.fields[0].path",
            ),
        ] {
            let spec = v2_spec(
                "balanced",
                &[json!({
                    "name": "items",
                    "role": "document_entity",
                    "indexes": ["document"],
                    "physical": physical,
                })],
            )?;

            assert_eq!(
                rejected(&spec)?,
                [pair("PROVISION_PATH_INVALID", path)],
                "{physical}"
            );
        }

        let deepest = ["_a1"; 16].join(".");
        let spec = v2_spec(
            "balanced",
            &[json!({
                "name": "items",
                "role": "document_entity",
                "indexes": ["document"],
                "physical": {
                    "fields": [
                        { "name": "id", "path": "$id", "type": "bytes" },
                        { "name": "deep", "path": deepest, "type": "int" }
                    ],
                    "document_indexes": [{ "path": "_owner.Name_2" }]
                },
            })],
        )?;
        let objects = projected(&spec)?;
        let DesiredKind::Document {
            fields, indexes, ..
        } = &objects[0].kind
        else {
            return Err("expected a document collection".into());
        };
        assert_eq!(fields[0], DocField::document_id("id"));
        assert_eq!(fields[1].ty, ColumnType::Int);
        assert_eq!(
            indexes,
            &[IndexSpec::new(
                IndexId::new(1),
                FieldPath::new(["_owner", "Name_2"])
            )]
        );
        Ok(())
    }

    #[test]
    fn index_ids_default_to_position_plus_one_unless_pinned() -> TestResult {
        let spec = v2_spec(
            "balanced",
            &[
                json!({
                    "name": "items",
                    "role": "document_entity",
                    "indexes": ["document"],
                    "physical": {
                        "collection_id": 40,
                        "document_indexes": [{ "path": "a" }, { "path": "b", "id": 7 }, { "path": "c" }]
                    },
                }),
                json!({
                    "name": "rows",
                    "role": "key_value",
                    "indexes": ["document"],
                    "physical": {
                        "kind": "table",
                        "columns": [{ "name": "k", "type": "str" }, { "name": "v", "type": "str" }],
                        "primary_key": "k",
                        "table_indexes": [
                            { "column": "v", "id": 9 },
                            { "column": "v", "expression": "lower_ascii", "include": ["k"] }
                        ]
                    },
                }),
                json!({ "name": "links", "role": "graph", "indexes": ["graph"], "physical": { "graph_id": 3 } }),
            ],
        )?;

        let objects = projected(&spec)?;

        let DesiredKind::Document {
            collection_id,
            indexes,
            ..
        } = &objects[0].kind
        else {
            return Err("expected a document collection".into());
        };
        assert_eq!(*collection_id, Some(CollectionId::new(40)));
        assert_eq!(
            indexes
                .iter()
                .map(|index| index.id.as_u32())
                .collect::<Vec<_>>(),
            [1, 7, 3]
        );
        let DesiredKind::Table { indexes, .. } = &objects[1].kind else {
            return Err("expected a table".into());
        };
        assert_eq!(
            indexes,
            &[
                RelIndexSpec::new(9, 1),
                RelIndexSpec::lower_ascii(2, 1).with_include(vec![0]),
            ]
        );
        assert_eq!(
            objects[2].kind,
            DesiredKind::Graph {
                graph_id: Some(crate::graph::GraphId::new(3)),
            }
        );

        let conflicting = v2_spec(
            "balanced",
            &[
                json!({
                    "name": "items",
                    "role": "document_entity",
                    "indexes": ["document"],
                    "physical": {
                        "collection_id": 5,
                        "document_indexes": [{ "path": "a" }, { "path": "b", "id": 1 }]
                    },
                }),
                json!({
                    "name": "memories",
                    "role": "vector_memory",
                    "indexes": ["vector"],
                    "physical": { "dim": 4, "collection_id": 5 },
                }),
            ],
        )?;
        assert_eq!(
            rejected(&conflicting)?,
            [
                pair("ID_CONFLICT", "collections[0].physical.document_indexes[1]"),
                pair("ID_CONFLICT", "collections[1].physical.collection_id"),
            ]
        );
        Ok(())
    }

    #[test]
    fn defaults_are_pinned() -> TestResult {
        let spec = v2_spec(
            "balanced",
            &[
                json!({
                    "name": "notes",
                    "role": "document_entity",
                    "indexes": ["full_text", "geo"],
                    "physical": { "full_text": [{ "path": "body" }], "geo": [{ "path": "at" }] },
                }),
                json!({ "name": "memories", "role": "vector_memory", "indexes": ["vector"], "physical": { "dim": 8 } }),
                json!({ "name": "metrics", "role": "time_series", "indexes": ["time_series"] }),
            ],
        )?;

        let objects = projected(&spec)?;

        assert_eq!(
            objects
                .iter()
                .map(|object| object.kind.clone())
                .collect::<Vec<_>>(),
            [
                DesiredKind::Document {
                    collection_id: None,
                    fields: vec![DocField::document_id("id")],
                    indexes: Vec::new(),
                },
                DesiredKind::FullText {
                    source: "notes".to_owned(),
                    path: FieldPath::new(["body"]),
                    language: "simple".to_owned(),
                    refresh_lag_target: 1,
                },
                DesiredKind::Geo {
                    source: "notes".to_owned(),
                    path: FieldPath::new(["at"]),
                    precision: 6,
                },
                DesiredKind::Vector {
                    collection_id: None,
                    dim: 8,
                    metric: VectorMetric::Cosine,
                    hnsw: HnswParams {
                        m: 16,
                        ef_construction: 200,
                        ef_search: 48,
                    },
                },
                DesiredKind::TimeSeries {
                    chunk_millis: 3_600_000,
                    retention_millis: None,
                    max_future_skew_millis: crate::timeseries::DEFAULT_MAX_FUTURE_SKEW_MILLIS,
                },
            ]
        );
        assert_eq!(
            HnswParams::default(),
            HnswParams {
                m: 16,
                ef_construction: 200,
                ef_search: 48,
            }
        );
        let config = time_series_config("metrics", &objects[4].kind).ok_or("time series")?;
        assert_eq!(config.chunk_millis, 3_600_000);
        assert_eq!(config.retention_millis, None);
        assert_eq!(
            config.max_future_skew_millis,
            crate::timeseries::DEFAULT_MAX_FUTURE_SKEW_MILLIS
        );
        assert!(time_series_config("notes", &objects[0].kind).is_none());
        Ok(())
    }

    #[test]
    fn time_series_and_history_follow_the_profile_knobs() -> TestResult {
        let mut spec = v2_spec(
            "metrics_store",
            &[
                json!({ "name": "metrics", "role": "time_series", "indexes": ["time_series"] }),
                json!({
                    "name": "ledger",
                    "role": "event_log",
                    "indexes": ["primary"],
                    "physical": {
                        "kind": "table",
                        "columns": [{ "name": "id", "type": "int" }],
                        "primary_key": "id"
                    },
                }),
                json!({ "name": "loose", "role": "key_value", "indexes": ["primary"], "physical": { "kind": "table" } }),
            ],
        )?;
        spec.custom_profile = Some(serde_json::from_value(json!({
            "extends": "balanced",
            "knobs": {
                "time_series": { "default_chunk_millis": 60_000, "default_retention_millis": 600_000 },
                "history": { "system_versioning": "typed_tables", "keep_history": false }
            }
        }))?);

        let objects = projected(&spec)?;

        assert_eq!(
            objects
                .iter()
                .map(|object| object.name.as_str())
                .collect::<Vec<_>>(),
            ["metrics", "ledger", "ledger_history", "loose"]
        );
        let config = time_series_config("metrics", &objects[0].kind).ok_or("time series")?;
        assert_eq!(
            (config.chunk_millis, config.retention_millis),
            (60_000, Some(600_000))
        );
        assert_eq!(
            objects[2].kind,
            DesiredKind::History {
                base_table: "ledger".to_owned(),
                keep_history: false,
            }
        );
        Ok(())
    }

    /// A number is the bound; `null` and `i64::MAX` both mean no bound, which `params_json`
    /// shows as `null`, since JavaScript clients cannot represent `i64::MAX`.
    #[test]
    fn an_explicit_future_skew_bound_reaches_the_time_series_config() -> TestResult {
        let skewed = |skew: JsonValue| {
            v2_spec(
                "balanced",
                &[json!({
                    "name": "metrics", "role": "time_series", "indexes": ["time_series"],
                    "physical": { "max_future_skew_millis": skew }
                })],
            )
        };
        for (skew, params, bound) in [
            (json!(0), json!(0), 0),
            (json!(5_000), json!(5_000), 5_000),
            (json!(i64::MAX), JsonValue::Null, i64::MAX),
            (JsonValue::Null, JsonValue::Null, i64::MAX),
        ] {
            let objects = projected(&skewed(skew.clone())?)?;
            assert_eq!(
                objects[0].params_json()["max_future_skew_millis"],
                params,
                "{skew}"
            );
            let config = time_series_config("metrics", &objects[0].kind).ok_or("time series")?;
            assert_eq!(config.max_future_skew_millis, bound, "{skew}");
        }
        assert_eq!(
            rejected(&skewed(json!(-1))?)?,
            [pair(
                "TIME_SERIES_PARAMS_INVALID",
                "collections[0].physical.max_future_skew_millis"
            )]
        );
        Ok(())
    }

    #[test]
    fn version_1_specs_project_only_when_nothing_is_missing() -> TestResult {
        let mut primary_only =
            DatabaseSpec::from_db_config("app", &DbConfig::on_disk(Profile::Balanced, "app.redb"));
        primary_only.collections = vec![
            serde_json::from_value(json!({
                "name": "settings", "role": "key_value", "domain": "primary", "indexes": ["primary"]
            }))?,
            serde_json::from_value(json!({
                "name": "events", "role": "event_log", "domain": "primary", "indexes": ["primary"]
            }))?,
        ];
        let objects = projected(&primary_only)?;
        assert_eq!(
            objects
                .iter()
                .map(|object| (object.name.as_str(), object.kind.slug()))
                .collect::<Vec<_>>(),
            [("settings", "document"), ("events", "document")]
        );

        let game_save = decode_spec_bytes(
            include_bytes!("../../tests/fixtures/spec-v1/game-save.yaml"),
            SpecFormat::Yaml,
        )?;
        let issues = match project(&game_save, &resolved(&game_save)?) {
            Ok(objects) => return Err(format!("game-save projected: {objects:#?}").into()),
            Err(issues) => issues,
        };
        assert_eq!(
            codes(&issues),
            [pair(
                "PHYSICAL_PARAMETER_REQUIRED",
                "collections[0].physical.document_indexes"
            )]
        );
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
        assert!(
            issues[0].suggestion.contains("multidb config upgrade"),
            "{:?}",
            issues[0]
        );

        let ai_memory = decode_spec_bytes(
            include_bytes!("../../tests/fixtures/spec-v1/ai-memory.yaml"),
            SpecFormat::Yaml,
        )?;
        let issues = match project(&ai_memory, &resolved(&ai_memory)?) {
            Ok(objects) => return Err(format!("ai-memory projected: {objects:#?}").into()),
            Err(issues) => issues,
        };
        assert_eq!(
            codes(&issues),
            [
                pair("PHYSICAL_PARAMETER_REQUIRED", "collections[0].physical.dim"),
                pair(
                    "PHYSICAL_PARAMETER_REQUIRED",
                    "collections[1].physical.document_indexes"
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn version_1_name_warnings_block_projection() -> TestResult {
        let mut spec =
            DatabaseSpec::from_db_config("app", &DbConfig::on_disk(Profile::Balanced, "app.redb"));
        spec.collections = vec![serde_json::from_value(json!({
            "name": "order-items", "role": "key_value", "domain": "primary", "indexes": ["primary"]
        }))?];

        assert_eq!(
            rejected(&spec)?,
            [pair("COLLECTION_NAME_INVALID", "collections[0].name")]
        );
        Ok(())
    }
}
