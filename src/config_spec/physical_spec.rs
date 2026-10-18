//! Spec version 2 physical parameters (`collections[].physical`): the flat
//! [`PhysicalObjectSpec`], its pinned defaults, derived object names and path rules, plus the
//! catalog plan that validation and `provision::physical::project` share, so both apply the
//! same rules. Nothing here touches a database.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    CertificationImpact, CollectionIndexKind, CollectionRole, CollectionRoleSpec, DatabaseSpec,
    LayoutKnob, ResolvedProfile, SystemVersioningKnob, ValidationIssue, ValidationSeverity,
    suggest, validation_issue,
};
use crate::db::is_valid_catalog_name;

/// HNSW `m` when `physical.hnsw.m` is not set.
pub const DEFAULT_HNSW_M: u32 = 16;
/// HNSW `ef_construction` when `physical.hnsw.ef_construction` is not set.
pub const DEFAULT_HNSW_EF_CONSTRUCTION: u32 = 200;
/// HNSW `ef_search` when `physical.hnsw.ef_search` is not set.
pub const DEFAULT_HNSW_EF_SEARCH: u32 = 48;
/// Full-text language when `physical.full_text[].language` is not set; the only one accepted.
pub const DEFAULT_FULL_TEXT_LANGUAGE: &str = "simple";
/// Full-text refresh lag target when `physical.full_text[].refresh_lag_target` is not set.
pub const DEFAULT_REFRESH_LAG_TARGET: u64 = 1;
/// Geohash precision when `physical.geo[].precision` is not set.
pub const DEFAULT_GEO_PRECISION: u8 = 6;
/// The largest vector dimension a spec may declare.
pub const MAX_VECTOR_DIM: u32 = 65_536;
/// The narrowest time-series chunk a spec may declare: one second.
pub const MIN_CHUNK_MILLIS: i64 = 1_000;
/// The widest time-series chunk a spec may declare: 31 days.
pub const MAX_CHUNK_MILLIS: i64 = 2_678_400_000;
/// Time-series future-skew bound when `physical.max_future_skew_millis` is not set: the
/// runtime default for new collections, [`crate::timeseries::DEFAULT_MAX_FUTURE_SKEW_MILLIS`].
pub const DEFAULT_MAX_FUTURE_SKEW_MILLIS: i64 = crate::timeseries::DEFAULT_MAX_FUTURE_SKEW_MILLIS;
/// The most segments a field path may have.
pub const MAX_PATH_SEGMENTS: usize = 16;
/// The field path of the generated document id, allowed for `physical.fields` only.
pub const DOCUMENT_ID_PATH: &str = "$id";

/// Size limits of one spec. Version 2 reports a breach as an Error, version 1 as a Warning;
/// provisioning always refuses it.
pub const MAX_COLLECTIONS: usize = 1_024;
/// The most catalog objects (collections plus derived indexes and history tables) of one spec.
pub const MAX_CATALOG_OBJECTS: usize = 4_096;
/// The most fields or columns of one collection.
pub const MAX_FIELDS_PER_COLLECTION: usize = 256;
/// The most document or table indexes of one collection.
pub const MAX_INDEXES_PER_COLLECTION: usize = 64;
/// The most full-text entries of one collection.
pub const MAX_FULL_TEXT_PER_COLLECTION: usize = 16;
/// The most geo entries of one collection.
pub const MAX_GEO_PER_COLLECTION: usize = 16;
/// The most entries of `overrides` and of `operation_hints`.
pub const MAX_MAP_ENTRIES: usize = 512;
/// The longest `overrides` or `operation_hints` key, in bytes.
pub const MAX_MAP_KEY_BYTES: usize = 256;
/// The longest `overrides` or `operation_hints` value, in bytes.
pub const MAX_MAP_VALUE_BYTES: usize = 4_096;

/// The runtime object a collection becomes. The default depends on the role
/// ([`allowed_kinds`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalKind {
    Document,
    Table,
    Vector,
    TimeSeries,
    Graph,
}

/// The value type of a document field or table column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnTypeSpec {
    Int,
    Float,
    Str,
    Bool,
    Bytes,
}

/// `collections[].physical`: the parameters provisioning needs to build a collection. One flat
/// struct for every kind; validation reports a field set on a kind it does not apply to as
/// `PHYSICAL_FIELD_NOT_APPLICABLE`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalObjectSpec {
    /// Every kind; defaults to the first of [`allowed_kinds`] for the role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<PhysicalKind>,
    /// Document and vector: a pinned collection id. Unpinned ids are allocated at apply time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_id: Option<u32>,
    /// Document: SQL-visible fields. Empty means one field `id` with the document id.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<DocFieldSpec>,
    /// Document: secondary indexes, required by the `document` index kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub document_indexes: Vec<DocIndexSpec>,
    /// Document: full-text indexes, required by the `full_text` index kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub full_text: Vec<FullTextSpec>,
    /// Document: geo indexes, required by the `geo` index kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub geo: Vec<GeoSpec>,
    /// Table: the physical layout. Defaults to columnar for analytics and to the database
    /// layout otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<LayoutKnob>,
    /// Table: typed columns. Empty means a schemaless row table.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnSpec>,
    /// Table: the primary key column, required when `columns` are given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,
    /// Table: B-tree indexes of a row table, required by the `document` index kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_indexes: Vec<TableIndexSpec>,
    /// Typed row table: keep system-versioned history in `<table>_history`. Defaults to the
    /// profile knob `history.system_versioning`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_versioning: Option<bool>,
    /// Typed row table with history: defaults to the profile knob `history.keep_history`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_history: Option<bool>,
    /// Vector: the dimension, 1..=65536. Required, and fixed at creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dim: Option<u32>,
    /// Vector: the distance metric, cosine by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<VectorMetricSpec>,
    /// Vector: HNSW parameters, 16/200/48 by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hnsw: Option<HnswSpec>,
    /// Time series: the chunk width. Defaults to `time_series.default_chunk_millis`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_millis: Option<i64>,
    /// Time series: the retention. Defaults to `time_series.default_retention_millis` when the
    /// field is left out; an explicit `null` is refused, since it could mean either that default
    /// or no retention.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "retention_not_null"
    )]
    pub retention_millis: Option<i64>,
    /// Time series: how far ahead of the server clock a point's timestamp may lie, at least 0;
    /// later points are refused. Defaults to [`DEFAULT_MAX_FUTURE_SKEW_MILLIS`], and is fixed
    /// at creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_future_skew_millis: Option<i64>,
    /// Graph: a pinned graph id. Unpinned ids are allocated at apply time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_id: Option<u32>,
}

/// `physical.retention_millis` when it is present: a number. `null` is refused instead of
/// falling back to the profile knob, which could set a retention a spec author who wrote `null`
/// for "keep forever" does not expect. A field that is left out never reaches this function.
fn retention_not_null<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<i64>::deserialize(deserializer)? {
        Some(retention) => Ok(Some(retention)),
        None => Err(serde::de::Error::custom(
            "physical.retention_millis cannot be null: leave it out to use the profile knob \
             time_series.default_retention_millis (a profile whose knob is null keeps data \
             without retention), or give the retention in milliseconds",
        )),
    }
}

/// A SQL-visible document field: a dotted `path` into the document, or `$id`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DocFieldSpec {
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub ty: ColumnTypeSpec,
}

/// A document secondary index. The id defaults to its position + 1.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DocIndexSpec {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,
}

/// A full-text index on a document path. The name defaults to
/// [`full_text_index_name`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FullTextSpec {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_lag_target: Option<u64>,
}

/// A geo index on a document path holding a point. The name defaults to [`geo_index_name`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeoSpec {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Geohash precision, 1..=12.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
}

/// A typed table column.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnTypeSpec,
    #[serde(default, skip_serializing_if = "is_false")]
    pub nullable: bool,
}

/// A B-tree index of a typed row table. The id defaults to its position + 1.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableIndexSpec {
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<TableIndexExpressionSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,
}

/// What a table index keys on: the column value, or its ASCII-lowercased string.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableIndexExpressionSpec {
    #[default]
    Column,
    LowerAscii,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorMetricSpec {
    #[default]
    Cosine,
    L2,
}

/// HNSW parameters of a vector collection; unset values take the pinned defaults.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HnswSpec {
    /// 1..=255.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub m: Option<u32>,
    /// At least `m`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<u32>,
    /// At least 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<u32>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

/// The physical kinds a role may use, default first.
#[must_use]
pub const fn allowed_kinds(role: CollectionRole) -> &'static [PhysicalKind] {
    match role {
        CollectionRole::DocumentEntity
        | CollectionRole::KeyValue
        | CollectionRole::EventLog
        | CollectionRole::Cache
        | CollectionRole::Audit => &[PhysicalKind::Document, PhysicalKind::Table],
        CollectionRole::VectorMemory => &[PhysicalKind::Vector],
        CollectionRole::Graph => &[PhysicalKind::Graph],
        CollectionRole::Analytics => &[PhysicalKind::Table],
        CollectionRole::TimeSeries => &[PhysicalKind::TimeSeries],
    }
}

/// The derived name of a full-text index: `<collection>_<path with . replaced by _>_fts`.
#[must_use]
pub fn full_text_index_name(collection: &str, path: &str) -> String {
    format!("{collection}_{}_fts", path.replace('.', "_"))
}

/// The derived name of a geo index: `<collection>_<path with . replaced by _>_geo`.
#[must_use]
pub fn geo_index_name(collection: &str, path: &str) -> String {
    format!("{collection}_{}_geo", path.replace('.', "_"))
}

/// The history table system versioning registers for `table`: `<table>_history`.
#[must_use]
pub fn history_table_name(table: &str) -> String {
    format!("{table}_history")
}

/// A parsed field path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpecPath {
    /// `$id`: the generated document id.
    DocumentId,
    /// Dotted segments, each `[A-Za-z_][A-Za-z0-9_]*`.
    Field(Vec<String>),
}

/// Parses a dotted field path: 1..=16 segments, each `[A-Za-z_][A-Za-z0-9_]*`. `$id` is the
/// document id and is accepted only where `allow_document_id` is set.
pub(crate) fn parse_spec_path(path: &str, allow_document_id: bool) -> Result<SpecPath, String> {
    if path == DOCUMENT_ID_PATH {
        return if allow_document_id {
            Ok(SpecPath::DocumentId)
        } else {
            Err("$id names the document id and is allowed in physical.fields only".to_owned())
        };
    }
    let segments = path.split('.').collect::<Vec<_>>();
    if segments.len() > MAX_PATH_SEGMENTS {
        return Err(format!(
            "has {} segments; at most {MAX_PATH_SEGMENTS} are allowed",
            segments.len()
        ));
    }
    for segment in &segments {
        let mut chars = segment.chars();
        let valid = chars
            .next()
            .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
            && chars.all(|character| character == '_' || character.is_ascii_alphanumeric());
        if !valid {
            return Err(format!(
                "segment {segment:?} must match [A-Za-z_][A-Za-z0-9_]*"
            ));
        }
    }
    Ok(SpecPath::Field(
        segments.into_iter().map(str::to_owned).collect(),
    ))
}

/// Which rules a catalog plan applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckMode {
    /// Version 1 validation: name and size Warnings only; `physical` is gated elsewhere.
    V1,
    /// Version 2 validation: Errors, physical checks where `physical` is present and
    /// `PHYSICAL_PARAMETER_REQUIRED` as Advice.
    V2,
    /// Provisioning: every check is an Error, with or without `physical`.
    Provisioning,
}

/// The catalog objects a spec describes, in spec order with dependents after their source, and
/// the issues found while planning them.
pub(crate) struct CatalogPlan {
    pub objects: Vec<PlannedObject>,
    pub issues: Vec<ValidationIssue>,
}

/// One catalog object of the plan, in spec terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedObject {
    pub name: String,
    /// The spec collection the object comes from.
    pub spec_collection: String,
    /// Where the object is declared, for issue paths.
    pub path: String,
    pub kind: PlannedKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PlannedKind {
    Document {
        collection_id: Option<u32>,
        fields: Vec<PlannedField>,
        indexes: Vec<PlannedDocIndex>,
    },
    Table {
        layout: LayoutKnob,
        columns: Vec<ColumnSpec>,
        primary_key: Option<usize>,
        indexes: Vec<PlannedTableIndex>,
    },
    Vector {
        collection_id: Option<u32>,
        dim: u32,
        metric: VectorMetricSpec,
        m: u32,
        ef_construction: u32,
        ef_search: u32,
    },
    TimeSeries {
        chunk_millis: i64,
        retention_millis: Option<i64>,
        max_future_skew_millis: i64,
    },
    Graph {
        graph_id: Option<u32>,
    },
    FullText {
        source: String,
        path: Vec<String>,
        language: String,
        refresh_lag_target: u64,
    },
    Geo {
        source: String,
        path: Vec<String>,
        precision: u8,
    },
    History {
        base_table: String,
        keep_history: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedField {
    pub name: String,
    pub source: SpecPath,
    pub ty: ColumnTypeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedDocIndex {
    pub id: u32,
    pub path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedTableIndex {
    pub id: u32,
    pub column: usize,
    pub lower_ascii: bool,
    pub include: Vec<usize>,
}

/// How an issue's severity follows the mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// Names and size limits: a Warning in version 1, an Error otherwise.
    Limit,
    /// Physical parameters: an Error where checked (version 2 with `physical`, provisioning).
    Physical,
    /// A missing physical parameter: Advice in version 2, an Error in provisioning.
    ParameterRequired,
}

/// Plans the catalog objects of `spec` and checks names, size limits and physical parameters.
/// `resolved` supplies the profile defaults (database layout, history and time-series knobs);
/// without it the preset defaults apply.
pub(crate) fn plan_catalog(
    spec: &DatabaseSpec,
    resolved: Option<&ResolvedProfile>,
    mode: CheckMode,
) -> CatalogPlan {
    let mut planner = Planner {
        mode,
        resolved,
        objects: Vec::new(),
        issues: Vec::new(),
    };
    planner.check_spec_limits(spec);
    for (index, collection) in spec.collections.iter().enumerate() {
        planner.collection(index, collection);
    }
    planner.check_ids(spec);
    planner.check_names();
    CatalogPlan {
        objects: planner.objects,
        issues: planner.issues,
    }
}

struct Planner<'a> {
    mode: CheckMode,
    resolved: Option<&'a ResolvedProfile>,
    objects: Vec<PlannedObject>,
    issues: Vec<ValidationIssue>,
}

/// The kind-specific context of one collection.
struct CollectionContext<'a> {
    index: usize,
    collection: &'a CollectionRoleSpec,
    physical: &'a PhysicalObjectSpec,
    /// Physical checks run for this collection (`physical` is present, or provisioning).
    checked: bool,
}

impl CollectionContext<'_> {
    fn path(&self, field: &str) -> String {
        format!("collections[{}].physical.{field}", self.index)
    }

    fn has_index(&self, kind: CollectionIndexKind) -> bool {
        self.collection.indexes.contains(&kind)
    }
}

impl Planner<'_> {
    fn push(
        &mut self,
        class: Class,
        checked: bool,
        code: &str,
        path: impl Into<String>,
        message: impl Into<String>,
        suggestion: impl Into<String>,
    ) {
        let (severity, impact) = match (class, self.mode) {
            (Class::Limit, CheckMode::V1) => {
                (ValidationSeverity::Warning, CertificationImpact::None)
            }
            (Class::Physical | Class::ParameterRequired, CheckMode::V1) => return,
            (Class::Physical, _) if !checked => return,
            (Class::ParameterRequired, CheckMode::V2) => {
                (ValidationSeverity::Advice, CertificationImpact::None)
            }
            _ => (
                ValidationSeverity::Error,
                CertificationImpact::BlocksCertified,
            ),
        };
        self.issues.push(validation_issue(
            code, severity, path, message, suggestion, impact,
        ));
    }

    fn limit(&mut self, path: impl Into<String>, message: impl Into<String>) {
        self.push(
            Class::Limit,
            true,
            "SPEC_LIMIT_EXCEEDED",
            path,
            message,
            "Split the configuration or remove entries; the limits are in docs/provisioning.md.",
        );
    }

    fn check_spec_limits(&mut self, spec: &DatabaseSpec) {
        if spec.collections.len() > MAX_COLLECTIONS {
            self.limit(
                "collections",
                format!(
                    "the spec declares {} collections; at most {MAX_COLLECTIONS} are allowed",
                    spec.collections.len()
                ),
            );
        }
        for (name, map) in [
            ("overrides", &spec.overrides),
            ("operation_hints", &spec.operation_hints),
        ] {
            if map.len() > MAX_MAP_ENTRIES {
                self.limit(
                    name,
                    format!(
                        "{name} has {} entries; at most {MAX_MAP_ENTRIES} are allowed",
                        map.len()
                    ),
                );
            }
            if let Some(key) = map.keys().find(|key| key.len() > MAX_MAP_KEY_BYTES) {
                self.limit(
                    name,
                    format!(
                        "{name} has a key of {} bytes; keys are at most {MAX_MAP_KEY_BYTES} bytes",
                        key.len()
                    ),
                );
            }
            if let Some((key, value)) = map.iter().find(|(key, value)| {
                key.len() <= MAX_MAP_KEY_BYTES && value.len() > MAX_MAP_VALUE_BYTES
            }) {
                self.limit(
                    format!("{name}.{key}"),
                    format!(
                        "{name}.{key} is {} bytes long; values are at most {MAX_MAP_VALUE_BYTES} bytes",
                        value.len()
                    ),
                );
            }
        }
    }

    fn collection(&mut self, index: usize, collection: &CollectionRoleSpec) {
        // Version 1 never reads `physical`: the version gate reports it.
        let declared = match self.mode {
            CheckMode::V1 => None,
            CheckMode::V2 | CheckMode::Provisioning => collection.physical.as_ref(),
        };
        let default = PhysicalObjectSpec::default();
        let context = CollectionContext {
            index,
            collection,
            physical: declared.unwrap_or(&default),
            checked: declared.is_some() || self.mode == CheckMode::Provisioning,
        };
        let allowed = allowed_kinds(collection.role);
        let mut kind = allowed[0];
        if let Some(requested) = context.physical.kind {
            if allowed.contains(&requested) {
                kind = requested;
            } else {
                self.push(
                    Class::Physical,
                    context.checked,
                    "PHYSICAL_KIND_INCOMPATIBLE_WITH_ROLE",
                    context.path("kind"),
                    format!(
                        "collection {} has role {}, which cannot be a {} object",
                        collection.name,
                        role_slug(collection.role),
                        kind_slug(requested)
                    ),
                    format!(
                        "Use one of: {}.",
                        allowed
                            .iter()
                            .map(|kind| kind_slug(*kind))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                );
                // Kind-specific checks would only repeat the mismatch; the default kind keeps
                // the collection name in the catalog plan.
                self.objects.push(PlannedObject {
                    name: collection.name.clone(),
                    spec_collection: collection.name.clone(),
                    path: format!("collections[{index}].name"),
                    kind: default_container(kind),
                });
                return;
            }
        }

        self.check_applicable_fields(&context, kind);
        match kind {
            PhysicalKind::Document => self.document(&context),
            PhysicalKind::Table => self.table(&context),
            PhysicalKind::Vector => self.vector(&context),
            PhysicalKind::TimeSeries => self.time_series(&context),
            PhysicalKind::Graph => self.graph(&context),
        }
    }

    /// `PHYSICAL_FIELD_NOT_APPLICABLE` for every field set on a kind it does not apply to.
    fn check_applicable_fields(&mut self, context: &CollectionContext<'_>, kind: PhysicalKind) {
        use PhysicalKind::{Document, Graph, Table, TimeSeries, Vector};
        let physical = context.physical;
        let fields: [(&str, bool, &[PhysicalKind]); 18] = [
            (
                "collection_id",
                physical.collection_id.is_some(),
                &[Document, Vector],
            ),
            ("fields", !physical.fields.is_empty(), &[Document]),
            (
                "document_indexes",
                !physical.document_indexes.is_empty(),
                &[Document],
            ),
            ("full_text", !physical.full_text.is_empty(), &[Document]),
            ("geo", !physical.geo.is_empty(), &[Document]),
            ("layout", physical.layout.is_some(), &[Table]),
            ("columns", !physical.columns.is_empty(), &[Table]),
            ("primary_key", physical.primary_key.is_some(), &[Table]),
            (
                "table_indexes",
                !physical.table_indexes.is_empty(),
                &[Table],
            ),
            (
                "system_versioning",
                physical.system_versioning.is_some(),
                &[Table],
            ),
            ("keep_history", physical.keep_history.is_some(), &[Table]),
            ("dim", physical.dim.is_some(), &[Vector]),
            ("metric", physical.metric.is_some(), &[Vector]),
            ("hnsw", physical.hnsw.is_some(), &[Vector]),
            (
                "chunk_millis",
                physical.chunk_millis.is_some(),
                &[TimeSeries],
            ),
            (
                "retention_millis",
                physical.retention_millis.is_some(),
                &[TimeSeries],
            ),
            (
                "max_future_skew_millis",
                physical.max_future_skew_millis.is_some(),
                &[TimeSeries],
            ),
            ("graph_id", physical.graph_id.is_some(), &[Graph]),
        ];
        for (field, set, kinds) in fields {
            if set && !kinds.contains(&kind) {
                self.push(
                    Class::Physical,
                    context.checked,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    context.path(field),
                    format!(
                        "physical.{field} applies to {} objects, but collection {} is a {} object",
                        kinds
                            .iter()
                            .map(|kind| kind_slug(*kind))
                            .collect::<Vec<_>>()
                            .join(" and "),
                        context.collection.name,
                        kind_slug(kind)
                    ),
                    format!("Remove physical.{field}, or change physical.kind."),
                );
            }
        }
    }

    /// `PHYSICAL_FIELD_NOT_APPLICABLE` at `indexes[j]` for an index kind the object cannot have.
    fn index_not_applicable(
        &mut self,
        context: &CollectionContext<'_>,
        position: usize,
        code: &str,
        reason: &str,
    ) {
        self.push(
            Class::Physical,
            context.checked,
            code,
            format!("collections[{}].indexes[{position}]", context.index),
            format!(
                "collection {} lists index kind {}, but {reason}",
                context.collection.name,
                index_slug(context.collection.indexes[position])
            ),
            "Remove the index kind, or change physical.kind or physical.layout.",
        );
    }

    fn parameter_required(
        &mut self,
        context: &CollectionContext<'_>,
        field: &str,
        message: String,
    ) {
        self.push(
            Class::ParameterRequired,
            true,
            "PHYSICAL_PARAMETER_REQUIRED",
            context.path(field),
            message,
            format!(
                "Add physical.{field} to the collection in a version 2 spec; multidb config \
                 upgrade converts a version 1 spec and lists the parameters to add."
            ),
        );
    }

    fn index_kind_missing(&mut self, context: &CollectionContext<'_>, field: &str, kind: &str) {
        self.push(
            Class::Physical,
            context.checked,
            "PROVISION_INDEX_KIND_MISSING",
            context.path(field),
            format!(
                "collection {} sets physical.{field}, but its indexes do not list {kind}",
                context.collection.name
            ),
            format!("Add {kind} to the collection indexes, or remove physical.{field}."),
        );
    }

    fn path_invalid(&mut self, context: &CollectionContext<'_>, path: String, reason: &str) {
        self.push(
            Class::Physical,
            context.checked,
            "PROVISION_PATH_INVALID",
            path,
            format!("collection {}: the path {reason}", context.collection.name),
            "Use dotted segments of letters, digits and _, starting with a letter or _ \
             (at most 16 segments).",
        );
    }

    fn count_limit(
        &mut self,
        context: &CollectionContext<'_>,
        field: &str,
        count: usize,
        max: usize,
    ) {
        if count > max {
            self.limit(
                context.path(field),
                format!(
                    "collection {} has {count} physical.{field} entries; at most {max} are allowed",
                    context.collection.name
                ),
            );
        }
    }

    fn document(&mut self, context: &CollectionContext<'_>) {
        let physical = context.physical;
        let name = &context.collection.name;
        for (position, kind) in context.collection.indexes.iter().enumerate() {
            match kind {
                CollectionIndexKind::Primary
                | CollectionIndexKind::Document
                | CollectionIndexKind::FullText
                | CollectionIndexKind::Geo => {}
                CollectionIndexKind::Vector
                | CollectionIndexKind::Graph
                | CollectionIndexKind::Columnar
                | CollectionIndexKind::TimeSeries => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "document collections have no such index",
                ),
            }
        }
        for (has, entries, field, kind) in [
            (
                context.has_index(CollectionIndexKind::Document),
                physical.document_indexes.len(),
                "document_indexes",
                "document",
            ),
            (
                context.has_index(CollectionIndexKind::FullText),
                physical.full_text.len(),
                "full_text",
                "full_text",
            ),
            (
                context.has_index(CollectionIndexKind::Geo),
                physical.geo.len(),
                "geo",
                "geo",
            ),
        ] {
            if has && entries == 0 {
                self.parameter_required(
                    context,
                    field,
                    format!(
                        "collection {name} lists index kind {kind}, but physical.{field} \
                         declares no index, so provisioning cannot build it"
                    ),
                );
            } else if !has && entries > 0 {
                self.index_kind_missing(context, field, kind);
            }
        }
        self.count_limit(
            context,
            "fields",
            physical.fields.len(),
            MAX_FIELDS_PER_COLLECTION,
        );
        self.count_limit(
            context,
            "document_indexes",
            physical.document_indexes.len(),
            MAX_INDEXES_PER_COLLECTION,
        );
        self.count_limit(
            context,
            "full_text",
            physical.full_text.len(),
            MAX_FULL_TEXT_PER_COLLECTION,
        );
        self.count_limit(context, "geo", physical.geo.len(), MAX_GEO_PER_COLLECTION);

        let fields = self.document_fields(context);
        let mut indexes = Vec::with_capacity(physical.document_indexes.len());
        let mut ids = BTreeSet::new();
        for (position, index) in physical.document_indexes.iter().enumerate() {
            let id = index.id.unwrap_or_else(|| position_id(position));
            if !ids.insert(id) {
                self.id_conflict(
                    context,
                    context.path(&format!("document_indexes[{position}]")),
                    format!("document index id {id} is used twice in collection {name}"),
                );
            }
            match parse_spec_path(&index.path, false) {
                Ok(SpecPath::Field(segments)) => {
                    indexes.push(PlannedDocIndex { id, path: segments });
                }
                Ok(SpecPath::DocumentId) => {}
                Err(reason) => self.path_invalid(
                    context,
                    context.path(&format!("document_indexes[{position}].path")),
                    &reason,
                ),
            }
        }
        self.objects.push(PlannedObject {
            name: name.clone(),
            spec_collection: name.clone(),
            path: format!("collections[{}].name", context.index),
            kind: PlannedKind::Document {
                collection_id: physical.collection_id,
                fields,
                indexes,
            },
        });

        for (position, entry) in physical.full_text.iter().enumerate() {
            let entry_path = context.path(&format!("full_text[{position}]"));
            let Some(path) = self.dependent_path(context, &entry.path, &entry_path) else {
                continue;
            };
            let language = entry
                .language
                .clone()
                .unwrap_or_else(|| DEFAULT_FULL_TEXT_LANGUAGE.to_owned());
            if language != DEFAULT_FULL_TEXT_LANGUAGE {
                self.push(
                    Class::Physical,
                    context.checked,
                    "PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME",
                    format!("{entry_path}.language"),
                    format!(
                        "full-text language {language:?} is not supported: this build \
                         tokenizes every language the same way"
                    ),
                    "Remove language, or set it to simple.",
                );
            }
            let (object_name, object_path) = match &entry.name {
                Some(explicit) => (explicit.clone(), format!("{entry_path}.name")),
                None => (
                    full_text_index_name(name, &entry.path),
                    format!("{entry_path}.path"),
                ),
            };
            self.objects.push(PlannedObject {
                name: object_name,
                spec_collection: name.clone(),
                path: object_path,
                kind: PlannedKind::FullText {
                    source: name.clone(),
                    path,
                    language,
                    refresh_lag_target: entry
                        .refresh_lag_target
                        .unwrap_or(DEFAULT_REFRESH_LAG_TARGET),
                },
            });
        }

        for (position, entry) in physical.geo.iter().enumerate() {
            let entry_path = context.path(&format!("geo[{position}]"));
            let Some(path) = self.dependent_path(context, &entry.path, &entry_path) else {
                continue;
            };
            let precision = entry.precision.unwrap_or(DEFAULT_GEO_PRECISION);
            if !(1..=12).contains(&precision) {
                self.push(
                    Class::Physical,
                    context.checked,
                    "GEO_PARAMS_OUT_OF_RANGE",
                    format!("{entry_path}.precision"),
                    format!("geo precision {precision} is outside 1..=12"),
                    "Set precision between 1 and 12 (6 by default).",
                );
            }
            let (object_name, object_path) = match &entry.name {
                Some(explicit) => (explicit.clone(), format!("{entry_path}.name")),
                None => (
                    geo_index_name(name, &entry.path),
                    format!("{entry_path}.path"),
                ),
            };
            self.objects.push(PlannedObject {
                name: object_name,
                spec_collection: name.clone(),
                path: object_path,
                kind: PlannedKind::Geo {
                    source: name.clone(),
                    path,
                    precision,
                },
            });
        }
    }

    /// The parsed path of a full-text or geo entry; `None` after reporting an invalid path.
    fn dependent_path(
        &mut self,
        context: &CollectionContext<'_>,
        path: &str,
        entry_path: &str,
    ) -> Option<Vec<String>> {
        match parse_spec_path(path, false) {
            Ok(SpecPath::Field(segments)) => Some(segments),
            Ok(SpecPath::DocumentId) => None,
            Err(reason) => {
                self.path_invalid(context, format!("{entry_path}.path"), &reason);
                None
            }
        }
    }

    fn document_fields(&mut self, context: &CollectionContext<'_>) -> Vec<PlannedField> {
        let physical = context.physical;
        if physical.fields.is_empty() {
            return vec![PlannedField {
                name: "id".to_owned(),
                source: SpecPath::DocumentId,
                ty: ColumnTypeSpec::Bytes,
            }];
        }
        let mut names = BTreeSet::new();
        let mut fields = Vec::with_capacity(physical.fields.len());
        for (position, field) in physical.fields.iter().enumerate() {
            let field_path = context.path(&format!("fields[{position}]"));
            self.check_column_name(
                context,
                &field.name,
                &format!("{field_path}.name"),
                &mut names,
            );
            match parse_spec_path(&field.path, true) {
                Ok(SpecPath::DocumentId) if field.ty != ColumnTypeSpec::Bytes => {
                    self.path_invalid(
                        context,
                        format!("{field_path}.type"),
                        "$id is the 16-byte document id, so its field must have type bytes",
                    );
                }
                Ok(source) => fields.push(PlannedField {
                    name: field.name.clone(),
                    source,
                    ty: field.ty,
                }),
                Err(reason) => {
                    self.path_invalid(context, format!("{field_path}.path"), &reason);
                }
            }
        }
        fields
    }

    /// Field and column names: a catalog-style identifier, unique within the object.
    fn check_column_name(
        &mut self,
        context: &CollectionContext<'_>,
        name: &str,
        path: &str,
        names: &mut BTreeSet<String>,
    ) {
        if !is_valid_catalog_name(name) {
            self.push(
                Class::Physical,
                context.checked,
                "PHYSICAL_NAME_INVALID",
                path,
                format!(
                    "collection {}: {name:?} is not a valid field or column name",
                    context.collection.name
                ),
                "Use letters, digits and _, starting with a letter or _ (at most 128 bytes).",
            );
        }
        if !names.insert(name.to_owned()) {
            self.push(
                Class::Physical,
                context.checked,
                "TABLE_COLUMN_DUPLICATE",
                path,
                format!(
                    "collection {} declares {name} twice",
                    context.collection.name
                ),
                "Give every field or column a unique name.",
            );
        }
    }

    fn effective_layout(&self, context: &CollectionContext<'_>) -> LayoutKnob {
        context.physical.layout.unwrap_or_else(|| {
            if context.collection.role == CollectionRole::Analytics {
                LayoutKnob::Columnar
            } else {
                self.resolved
                    .map_or(LayoutKnob::Row, |resolved| resolved.knobs.storage.layout)
            }
        })
    }

    fn table(&mut self, context: &CollectionContext<'_>) {
        let physical = context.physical;
        let name = &context.collection.name;
        let layout = self.effective_layout(context);
        let columnar = layout == LayoutKnob::Columnar;
        for (position, kind) in context.collection.indexes.iter().enumerate() {
            match kind {
                CollectionIndexKind::Primary => {}
                CollectionIndexKind::Document if !columnar => {}
                CollectionIndexKind::Columnar if columnar => {}
                CollectionIndexKind::Document => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "columnar tables take no B-tree indexes",
                ),
                CollectionIndexKind::Columnar => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "the table has a row layout",
                ),
                CollectionIndexKind::FullText | CollectionIndexKind::Geo => self
                    .index_not_applicable(
                        context,
                        position,
                        "INDEX_SOURCE_NOT_DOCUMENT",
                        "full-text and geo indexes need a document collection as their source",
                    ),
                CollectionIndexKind::Vector
                | CollectionIndexKind::Graph
                | CollectionIndexKind::TimeSeries => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "tables have no such index",
                ),
            }
        }
        self.count_limit(
            context,
            "columns",
            physical.columns.len(),
            MAX_FIELDS_PER_COLLECTION,
        );
        self.count_limit(
            context,
            "table_indexes",
            physical.table_indexes.len(),
            MAX_INDEXES_PER_COLLECTION,
        );

        let mut names = BTreeSet::new();
        for (position, column) in physical.columns.iter().enumerate() {
            self.check_column_name(
                context,
                &column.name,
                &context.path(&format!("columns[{position}].name")),
                &mut names,
            );
        }
        let typed = !physical.columns.is_empty();
        if columnar && !typed {
            self.parameter_required(
                context,
                "columns",
                format!(
                    "table {name} is columnar, so it needs physical.columns and \
                     physical.primary_key"
                ),
            );
        } else if !typed
            && (physical.system_versioning == Some(true) || physical.keep_history.is_some())
        {
            self.parameter_required(
                context,
                "columns",
                format!(
                    "table {name} asks for system-versioned history, which needs a typed \
                     table: set physical.columns and physical.primary_key"
                ),
            );
        }
        let primary_key = self.primary_key(context);

        let wants_document = context.has_index(CollectionIndexKind::Document);
        if columnar {
            if !physical.table_indexes.is_empty() {
                self.push(
                    Class::Physical,
                    context.checked,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    context.path("table_indexes"),
                    format!("table {name} is columnar, and columnar tables take no B-tree indexes"),
                    "Remove physical.table_indexes, or set physical.layout to row.",
                );
            }
        } else if wants_document && physical.table_indexes.is_empty() {
            self.parameter_required(
                context,
                "table_indexes",
                format!(
                    "table {name} lists index kind document, but physical.table_indexes \
                     declares no index, so provisioning cannot build it"
                ),
            );
        } else if !wants_document && !physical.table_indexes.is_empty() {
            self.index_kind_missing(context, "table_indexes", "document");
        }
        let indexes = if columnar {
            Vec::new()
        } else {
            self.table_indexes(context)
        };

        let knob_versioning = self.resolved.is_some_and(|resolved| {
            resolved.knobs.history.system_versioning == SystemVersioningKnob::TypedTables
        });
        let versioning = match physical.system_versioning {
            Some(true) if columnar => {
                self.push(
                    Class::Physical,
                    context.checked,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    context.path("system_versioning"),
                    format!(
                        "table {name} is columnar; system-versioned history is kept for row \
                         tables only"
                    ),
                    "Remove physical.system_versioning, or set physical.layout to row.",
                );
                false
            }
            Some(explicit) => explicit && typed,
            None => knob_versioning && typed && !columnar,
        };
        if typed && !versioning && physical.keep_history.is_some() {
            self.push(
                Class::Physical,
                context.checked,
                "PHYSICAL_FIELD_NOT_APPLICABLE",
                context.path("keep_history"),
                format!("table {name} keeps no system-versioned history"),
                "Remove physical.keep_history, or set physical.system_versioning: true.",
            );
        }

        self.objects.push(PlannedObject {
            name: name.clone(),
            spec_collection: name.clone(),
            path: format!("collections[{}].name", context.index),
            kind: PlannedKind::Table {
                layout,
                columns: physical.columns.clone(),
                primary_key,
                indexes,
            },
        });
        if versioning {
            let keep_history = physical.keep_history.unwrap_or_else(|| {
                self.resolved
                    .is_none_or(|resolved| resolved.knobs.history.keep_history)
            });
            self.objects.push(PlannedObject {
                name: history_table_name(name),
                spec_collection: name.clone(),
                path: if physical.system_versioning.is_some() {
                    context.path("system_versioning")
                } else {
                    format!("collections[{}].name", context.index)
                },
                kind: PlannedKind::History {
                    base_table: name.clone(),
                    keep_history,
                },
            });
        }
    }

    fn primary_key(&mut self, context: &CollectionContext<'_>) -> Option<usize> {
        let physical = context.physical;
        let name = &context.collection.name;
        let columns = column_names(physical);
        match &physical.primary_key {
            None if !columns.is_empty() => {
                self.push(
                    Class::Physical,
                    context.checked,
                    "TABLE_PRIMARY_KEY_UNKNOWN",
                    context.path("primary_key"),
                    format!("table {name} declares columns but no primary_key"),
                    "Set physical.primary_key to one of the columns.",
                );
                None
            }
            None => None,
            Some(key) => {
                let position = columns.iter().position(|column| column == key);
                if position.is_none() {
                    self.push(
                        Class::Physical,
                        context.checked,
                        "TABLE_PRIMARY_KEY_UNKNOWN",
                        context.path("primary_key"),
                        format!("table {name} has no column {key} for its primary key"),
                        column_suggestion(key, &columns),
                    );
                }
                position
            }
        }
    }

    fn table_indexes(&mut self, context: &CollectionContext<'_>) -> Vec<PlannedTableIndex> {
        let physical = context.physical;
        let name = &context.collection.name;
        let columns = column_names(physical);
        let mut ids = BTreeSet::new();
        let mut indexes = Vec::with_capacity(physical.table_indexes.len());
        for (position, index) in physical.table_indexes.iter().enumerate() {
            let index_path = context.path(&format!("table_indexes[{position}]"));
            let id = index.id.unwrap_or_else(|| position_id(position));
            if !ids.insert(id) {
                self.id_conflict(
                    context,
                    index_path.clone(),
                    format!("table index id {id} is used twice in table {name}"),
                );
            }
            let column = self.index_column(
                context,
                &columns,
                &index.column,
                format!("{index_path}.column"),
            );
            let mut include = Vec::with_capacity(index.include.len());
            for (include_position, included) in index.include.iter().enumerate() {
                if let Some(position) = self.index_column(
                    context,
                    &columns,
                    included,
                    format!("{index_path}.include[{include_position}]"),
                ) {
                    include.push(position);
                }
            }
            let lower_ascii = index.expression == Some(TableIndexExpressionSpec::LowerAscii);
            if let Some(column) = column {
                if lower_ascii && physical.columns[column].ty != ColumnTypeSpec::Str {
                    self.push(
                        Class::Physical,
                        context.checked,
                        "PHYSICAL_FIELD_NOT_APPLICABLE",
                        format!("{index_path}.expression"),
                        format!(
                            "lower_ascii indexes need a str column, but {}.{} is not one",
                            name, index.column
                        ),
                        "Use expression: column, or index a str column.",
                    );
                }
                indexes.push(PlannedTableIndex {
                    id,
                    column,
                    lower_ascii,
                    include,
                });
            }
        }
        indexes
    }

    fn index_column(
        &mut self,
        context: &CollectionContext<'_>,
        columns: &[&str],
        column: &str,
        path: String,
    ) -> Option<usize> {
        let position = columns.iter().position(|candidate| *candidate == column);
        if position.is_none() {
            self.push(
                Class::Physical,
                context.checked,
                "TABLE_INDEX_COLUMN_UNKNOWN",
                path,
                if columns.is_empty() {
                    format!(
                        "table {} indexes column {column}, but declares no columns",
                        context.collection.name
                    )
                } else {
                    format!("table {} has no column {column}", context.collection.name)
                },
                column_suggestion(column, columns),
            );
        }
        position
    }

    fn vector(&mut self, context: &CollectionContext<'_>) {
        let physical = context.physical;
        let name = &context.collection.name;
        for (position, kind) in context.collection.indexes.iter().enumerate() {
            match kind {
                CollectionIndexKind::Primary | CollectionIndexKind::Vector => {}
                CollectionIndexKind::FullText | CollectionIndexKind::Geo => self
                    .index_not_applicable(
                        context,
                        position,
                        "INDEX_SOURCE_NOT_DOCUMENT",
                        "full-text and geo indexes need a document collection as their source",
                    ),
                CollectionIndexKind::Document
                | CollectionIndexKind::Graph
                | CollectionIndexKind::Columnar
                | CollectionIndexKind::TimeSeries => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "vector collections have no such index",
                ),
            }
        }
        let dim = match physical.dim {
            None => {
                self.parameter_required(
                    context,
                    "dim",
                    format!(
                        "vector collection {name} needs physical.dim, which is fixed at \
                         creation"
                    ),
                );
                0
            }
            Some(dim) => {
                if !(1..=MAX_VECTOR_DIM).contains(&dim) {
                    self.push(
                        Class::Physical,
                        context.checked,
                        "VECTOR_DIM_OUT_OF_RANGE",
                        context.path("dim"),
                        format!("vector dimension {dim} is outside 1..={MAX_VECTOR_DIM}"),
                        "Set physical.dim to the dimension of your embedding model.",
                    );
                }
                dim
            }
        };
        let hnsw = physical.hnsw.unwrap_or_default();
        let m = hnsw.m.unwrap_or(DEFAULT_HNSW_M);
        let ef_construction = hnsw.ef_construction.unwrap_or(DEFAULT_HNSW_EF_CONSTRUCTION);
        let ef_search = hnsw.ef_search.unwrap_or(DEFAULT_HNSW_EF_SEARCH);
        for (field, valid, rule) in [
            (
                "m",
                (1..=255).contains(&m),
                format!("m is {m}; it must be in 1..=255"),
            ),
            (
                "ef_construction",
                ef_construction >= m,
                format!("ef_construction is {ef_construction}; it must be at least m ({m})"),
            ),
            (
                "ef_search",
                ef_search >= 1,
                "ef_search is 0; it must be at least 1".to_owned(),
            ),
        ] {
            if !valid {
                self.push(
                    Class::Physical,
                    context.checked,
                    "VECTOR_PARAMS_OUT_OF_RANGE",
                    context.path(&format!("hnsw.{field}")),
                    format!("vector collection {name}: {rule}"),
                    "Use m in 1..=255, ef_construction >= m and ef_search >= 1 (defaults \
                     16/200/48).",
                );
            }
        }
        self.objects.push(PlannedObject {
            name: name.clone(),
            spec_collection: name.clone(),
            path: format!("collections[{}].name", context.index),
            kind: PlannedKind::Vector {
                collection_id: physical.collection_id,
                dim,
                metric: physical.metric.unwrap_or_default(),
                m,
                ef_construction,
                ef_search,
            },
        });
    }

    fn time_series(&mut self, context: &CollectionContext<'_>) {
        let physical = context.physical;
        let name = &context.collection.name;
        for (position, kind) in context.collection.indexes.iter().enumerate() {
            match kind {
                CollectionIndexKind::Primary | CollectionIndexKind::TimeSeries => {}
                CollectionIndexKind::FullText | CollectionIndexKind::Geo => self
                    .index_not_applicable(
                        context,
                        position,
                        "INDEX_SOURCE_NOT_DOCUMENT",
                        "full-text and geo indexes need a document collection as their source",
                    ),
                CollectionIndexKind::Document
                | CollectionIndexKind::Vector
                | CollectionIndexKind::Graph
                | CollectionIndexKind::Columnar => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "time series have no such index",
                ),
            }
        }
        let knobs = self.resolved.map(|resolved| resolved.knobs.time_series);
        let chunk_millis = physical
            .chunk_millis
            .unwrap_or_else(|| knobs.map_or(3_600_000, |knobs| knobs.default_chunk_millis));
        let retention_millis = physical
            .retention_millis
            .or_else(|| knobs.and_then(|knobs| knobs.default_retention_millis));
        if physical.chunk_millis.is_some()
            && !(MIN_CHUNK_MILLIS..=MAX_CHUNK_MILLIS).contains(&chunk_millis)
        {
            self.time_series_invalid(
                context,
                "chunk_millis",
                format!(
                    "time series {name} has chunk_millis {chunk_millis}, outside \
                     {MIN_CHUNK_MILLIS}..={MAX_CHUNK_MILLIS} (one second to 31 days)"
                ),
            );
        }
        if let Some(retention) = retention_millis {
            let explicit = physical.retention_millis.is_some() || physical.chunk_millis.is_some();
            let field = if physical.retention_millis.is_some() {
                "retention_millis"
            } else {
                "chunk_millis"
            };
            if explicit && retention <= 0 {
                self.time_series_invalid(
                    context,
                    field,
                    format!(
                        "time series {name} has retention_millis {retention}; it must be positive"
                    ),
                );
            } else if explicit && retention < chunk_millis {
                self.time_series_invalid(
                    context,
                    field,
                    format!(
                        "time series {name} keeps {retention} ms, less than one chunk of \
                         {chunk_millis} ms"
                    ),
                );
            }
        }
        let max_future_skew_millis = physical
            .max_future_skew_millis
            .unwrap_or(DEFAULT_MAX_FUTURE_SKEW_MILLIS);
        if max_future_skew_millis < 0 {
            self.time_series_invalid(
                context,
                "max_future_skew_millis",
                format!(
                    "time series {name} has max_future_skew_millis {max_future_skew_millis}; it \
                     cannot be negative"
                ),
            );
        }
        self.objects.push(PlannedObject {
            name: name.clone(),
            spec_collection: name.clone(),
            path: format!("collections[{}].name", context.index),
            kind: PlannedKind::TimeSeries {
                chunk_millis,
                retention_millis,
                max_future_skew_millis,
            },
        });
    }

    fn time_series_invalid(
        &mut self,
        context: &CollectionContext<'_>,
        field: &str,
        message: String,
    ) {
        self.push(
            Class::Physical,
            context.checked,
            "TIME_SERIES_PARAMS_INVALID",
            context.path(field),
            message,
            "Use chunk_millis between 1000 and 2678400000, a positive retention_millis of at \
             least one chunk (or none), and a max_future_skew_millis of at least 0.",
        );
    }

    fn graph(&mut self, context: &CollectionContext<'_>) {
        for (position, kind) in context.collection.indexes.iter().enumerate() {
            match kind {
                CollectionIndexKind::Primary | CollectionIndexKind::Graph => {}
                CollectionIndexKind::FullText | CollectionIndexKind::Geo => self
                    .index_not_applicable(
                        context,
                        position,
                        "INDEX_SOURCE_NOT_DOCUMENT",
                        "full-text and geo indexes need a document collection as their source",
                    ),
                CollectionIndexKind::Document
                | CollectionIndexKind::Vector
                | CollectionIndexKind::Columnar
                | CollectionIndexKind::TimeSeries => self.index_not_applicable(
                    context,
                    position,
                    "PHYSICAL_FIELD_NOT_APPLICABLE",
                    "graphs have no such index",
                ),
            }
        }
        self.objects.push(PlannedObject {
            name: context.collection.name.clone(),
            spec_collection: context.collection.name.clone(),
            path: format!("collections[{}].name", context.index),
            kind: PlannedKind::Graph {
                graph_id: context.physical.graph_id,
            },
        });
    }

    fn id_conflict(&mut self, context: &CollectionContext<'_>, path: String, message: String) {
        self.push(
            Class::Physical,
            context.checked,
            "ID_CONFLICT",
            path,
            message,
            "Give every pinned id a unique value, or leave it unset to use the default.",
        );
    }

    /// `ID_CONFLICT` for pinned collection ids (shared by document and vector collections) and
    /// pinned graph ids used twice.
    fn check_ids(&mut self, spec: &DatabaseSpec) {
        let mut collection_ids = BTreeMap::new();
        let mut graph_ids = BTreeMap::new();
        let mut conflicts = Vec::new();
        for object in &self.objects {
            let (ids, id, field) = match &object.kind {
                PlannedKind::Document {
                    collection_id: Some(id),
                    ..
                }
                | PlannedKind::Vector {
                    collection_id: Some(id),
                    ..
                } => (&mut collection_ids, *id, "collection_id"),
                PlannedKind::Graph { graph_id: Some(id) } => (&mut graph_ids, *id, "graph_id"),
                _ => continue,
            };
            if let Some(first) = ids.insert(id, object.name.clone()) {
                conflicts.push((object.spec_collection.clone(), field, id, first));
            }
        }
        for (collection, field, id, first) in conflicts {
            let Some(index) = spec
                .collections
                .iter()
                .position(|candidate| candidate.name == collection)
            else {
                continue;
            };
            let checked =
                spec.collections[index].physical.is_some() || self.mode == CheckMode::Provisioning;
            self.push(
                Class::Physical,
                checked,
                "ID_CONFLICT",
                format!("collections[{index}].physical.{field}"),
                format!("collections {first} and {collection} both pin {field} {id}"),
                "Give every pinned id a unique value, or leave it unset to allocate one.",
            );
        }
    }

    /// Catalog names: the runtime rule, no `__` prefix, one namespace for every object, and the
    /// total object count.
    fn check_names(&mut self) {
        let mut seen = BTreeMap::new();
        let mut issues = Vec::new();
        for object in &self.objects {
            if object.name.starts_with("__") {
                issues.push((
                    "COLLECTION_NAME_RESERVED",
                    object.path.clone(),
                    format!(
                        "{} starts with __, which is reserved for system tables",
                        object.name
                    ),
                    "Rename the object so it does not start with __.".to_owned(),
                ));
            } else if !is_valid_catalog_name(&object.name) {
                issues.push((
                    "COLLECTION_NAME_INVALID",
                    object.path.clone(),
                    format!(
                        "{:?} is not a valid catalog name{}",
                        truncate_name(&object.name),
                        if object.name.len() > 128 {
                            format!(" ({} bytes; at most 128)", object.name.len())
                        } else {
                            String::new()
                        }
                    ),
                    if object.name == object.spec_collection {
                        "Use letters, digits and _, starting with a letter or _ (at most 128 \
                         bytes)."
                            .to_owned()
                    } else {
                        "Give the index an explicit name, or use a shorter collection name or \
                         path (at most 128 bytes)."
                            .to_owned()
                    },
                ));
            }
            if let Some(first) = seen.insert(object.name.clone(), object.path.clone()) {
                issues.push((
                    "OBJECT_NAME_COLLISION",
                    object.path.clone(),
                    format!(
                        "{} is declared at {first} and again here; collections, indexes and \
                         history tables share one namespace",
                        object.name
                    ),
                    "Rename one of the objects, or give the index an explicit name.".to_owned(),
                ));
            }
        }
        if self.objects.len() > MAX_CATALOG_OBJECTS {
            issues.push((
                "SPEC_LIMIT_EXCEEDED",
                "collections".to_owned(),
                format!(
                    "the spec describes {} catalog objects; at most {MAX_CATALOG_OBJECTS} are \
                     allowed",
                    self.objects.len()
                ),
                "Split the configuration or remove entries; the limits are in \
                 docs/provisioning.md."
                    .to_owned(),
            ));
        }
        for (code, path, message, suggestion) in issues {
            self.push(Class::Limit, true, code, path, message, suggestion);
        }
    }
}

const fn position_id(position: usize) -> u32 {
    // Positions are bounded by the spec size limit, far below u32::MAX.
    #[allow(clippy::cast_possible_truncation)]
    let id = position as u32;
    id.saturating_add(1)
}

fn default_container(kind: PhysicalKind) -> PlannedKind {
    match kind {
        PhysicalKind::Document => PlannedKind::Document {
            collection_id: None,
            fields: Vec::new(),
            indexes: Vec::new(),
        },
        PhysicalKind::Table => PlannedKind::Table {
            layout: LayoutKnob::Row,
            columns: Vec::new(),
            primary_key: None,
            indexes: Vec::new(),
        },
        PhysicalKind::Vector => PlannedKind::Vector {
            collection_id: None,
            dim: 0,
            metric: VectorMetricSpec::Cosine,
            m: DEFAULT_HNSW_M,
            ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
            ef_search: DEFAULT_HNSW_EF_SEARCH,
        },
        PhysicalKind::TimeSeries => PlannedKind::TimeSeries {
            chunk_millis: 3_600_000,
            retention_millis: None,
            max_future_skew_millis: DEFAULT_MAX_FUTURE_SKEW_MILLIS,
        },
        PhysicalKind::Graph => PlannedKind::Graph { graph_id: None },
    }
}

fn column_names(physical: &PhysicalObjectSpec) -> Vec<&str> {
    physical
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

fn column_suggestion(column: &str, columns: &[&str]) -> String {
    match suggest::closest(column, columns.iter().copied(), 2) {
        Some(candidate) => format!("Did you mean {candidate}?"),
        None if columns.is_empty() => "Declare the table's physical.columns first.".to_owned(),
        None => format!("Use one of: {}.", columns.join(", ")),
    }
}

/// At most 64 characters of a name, for messages about overlong names.
fn truncate_name(name: &str) -> String {
    let mut truncated = name.chars().take(64).collect::<String>();
    if truncated.len() < name.len() {
        truncated.push('…');
    }
    truncated
}

pub(crate) const fn kind_slug(kind: PhysicalKind) -> &'static str {
    match kind {
        PhysicalKind::Document => "document",
        PhysicalKind::Table => "table",
        PhysicalKind::Vector => "vector",
        PhysicalKind::TimeSeries => "time_series",
        PhysicalKind::Graph => "graph",
    }
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

const fn index_slug(kind: CollectionIndexKind) -> &'static str {
    match kind {
        CollectionIndexKind::Primary => "primary",
        CollectionIndexKind::Document => "document",
        CollectionIndexKind::Vector => "vector",
        CollectionIndexKind::Graph => "graph",
        CollectionIndexKind::FullText => "full_text",
        CollectionIndexKind::Columnar => "columnar",
        CollectionIndexKind::TimeSeries => "time_series",
        CollectionIndexKind::Geo => "geo",
    }
}
