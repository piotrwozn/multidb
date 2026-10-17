//! Describing a live database as a spec.
//!
//! A database provisioning applied a spec to exports that spec, from its record. Any other
//! database gets a spec inferred from its catalog, profile manifest, RBAC and audit state, with
//! every id and index pinned, so planning the exported spec against the database only adopts
//! what is there. What a spec cannot express (foreign tables, materialized views, null-typed
//! fields, partial indexes, system grants) is left out and listed in the notes and in
//! `operation_hints["export.unsupported_objects"]`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use super::ProvisionError;
use super::live::LiveState;
use super::physical::{self, DesiredObject, source_name};
use super::plan::{knob_leaves, object_resource};
use super::record::{ProvisionRecordStatus, RecordState};
use crate::config_spec::{
    CollectionIndexKind, CollectionRole, CollectionRoleSpec, ColumnSpec, ColumnTypeSpec,
    CustomProfileSpec, DATABASE_SPEC_VERSION_V2, DEFAULT_FULL_TEXT_LANGUAGE, DEFAULT_GEO_PRECISION,
    DEFAULT_REFRESH_LAG_TARGET, DOCUMENT_ID_PATH, DatabaseSpec, DocFieldSpec, DocIndexSpec,
    ExtensionManifestRef, ExtensionStability, FloorOverrides, FullTextSpec, GeoSpec, GrantScope,
    GrantSpec, GuaranteeValidator, HnswSpec, LayoutKnob, MAX_GRANTS_PER_ROLE, MAX_MAP_VALUE_BYTES,
    PermissionSpec, PhysicalKind, PhysicalObjectSpec, PrincipalSpec, ProfileKnobOverrides,
    RESERVED_PRINCIPAL_NAMES, RESERVED_ROLE_NAMES, ResolvedProfile, ResolvedProfileKind,
    RoleSetDelta, RoleSpec, SecuritySpec, SupportStatus, TableIndexExpressionSpec, TableIndexSpec,
    ValidationSeverity, VectorMetricSpec, built_in_extension_manifest, full_text_index_name,
    geo_index_name, history_table_name, index_required_extensions, resolve_named_profile,
    resolve_profile, role_required_extensions,
};
use crate::db::{CatalogEntry, Database, PersistedProfileManifest};
use crate::query::{ColumnType, DocFieldSource, RelIndexExpression, TableLayout, TableSchema};
use crate::security::{Permission, Resource};
use crate::text::TextSource;
use crate::vector::VectorMetric;

/// Where an exported spec comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportSource {
    /// The spec the last apply recorded in `__provision`.
    AppliedRecord,
    /// Inferred from the live catalog, profile manifest, RBAC and audit state.
    Inferred,
}

/// A live database described as a spec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportedSpec {
    pub spec: DatabaseSpec,
    pub source: ExportSource,
    /// What the export could not express, assumed or should be checked by hand.
    pub notes: Vec<String>,
}

/// Exports `db` as a spec: the applied spec of its provision record when it has one, otherwise
/// a version 2 spec inferred from the live database. Never writes.
/// # Errors
/// Fails with `RecordUnreadable` when the provision record cannot be read (a newer build wrote
/// it, or it is corrupt), and as `plan_provision` when the live state cannot be read.
pub fn export_spec(db: &Database) -> Result<ExportedSpec, ProvisionError> {
    let live = LiveState::capture(db)?;
    match &live.record {
        RecordState::Unreadable(reason) => Err(ProvisionError::RecordUnreadable(reason.clone())),
        RecordState::Readable(record) => {
            let mut notes = vec![
                "the spec the last apply recorded in __provision; objects created outside \
                 provisioning since then are not in it"
                    .to_owned(),
            ];
            if record.status != ProvisionRecordStatus::Applied {
                notes.push(format!(
                    "the last apply ({}) did not finish; this is the spec it was applying",
                    record.plan_id
                ));
            }
            Ok(ExportedSpec {
                spec: record.applied_spec.clone(),
                source: ExportSource::AppliedRecord,
                notes,
            })
        }
        RecordState::None => Ok(infer(db, &live)),
    }
}

/// What an inference collects besides the spec.
#[derive(Default)]
struct Findings {
    notes: Vec<String>,
    /// Objects, fields, indexes and grants a spec cannot express.
    unsupported: Vec<String>,
    /// `<collection>=<role>` for every inferred collection.
    inferred_roles: Vec<String>,
}

fn infer(db: &Database, live: &LiveState) -> ExportedSpec {
    let mut findings = Findings::default();
    let name = db
        .config()
        .path
        .as_ref()
        .and_then(|path| path.file_stem())
        .map(|stem| stem.to_string_lossy().into_owned())
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| "database".to_owned());
    let mut spec = DatabaseSpec::from_db_config(name, db.config());
    spec.version = DATABASE_SPEC_VERSION_V2;

    let manifest = live.metadata.manifest.as_ref();
    match manifest {
        Some(manifest) => {
            spec.profile.clone_from(&manifest.profile);
            if manifest.kind == ResolvedProfileKind::Custom {
                spec.custom_profile = custom_profile(manifest, &mut findings);
            }
        }
        None => findings.notes.push(format!(
            "the database has no profile manifest; profile {} is the alias of its storage \
             profile, and adopting it writes the manifest",
            spec.profile
        )),
    }

    let floors = manifest
        .map(|manifest| manifest.knobs.floors)
        .unwrap_or_default();
    spec.guarantees.audit.enabled = live.audit_enabled;
    spec.guarantees.encryption.at_rest = live.encryption_configured;
    spec.guarantees.backup.enabled = floors.backup;
    spec.guarantees.backup.pitr = floors.pitr;
    if floors.backup || floors.pitr {
        findings.notes.push(
            "guarantees.backup is set because the profile floors require it; backups are \
             operator-managed, so schedule multidb backup yourself"
                .to_owned(),
        );
    } else {
        findings.notes.push(
            "guarantees.backup is false: backups are operator-managed (multidb backup); set it \
             to true when you run them"
                .to_owned(),
        );
    }
    if floors.audit && !live.audit_enabled {
        findings.notes.push(
            "audit is disabled, although the profile floor floors.audit requires it".to_owned(),
        );
    }

    let resolved = resolve_profile(&spec).resolved().cloned();
    spec.collections = infer_collections(live, resolved.as_ref(), &mut findings);
    spec.extensions = required_extensions(&spec.collections);
    if let Some(resolved) = &resolved {
        match physical::project(&spec, resolved) {
            Ok(objects) => spec.security = infer_security(live, &objects, &mut findings),
            Err(issues) => findings.notes.push(format!(
                "RBAC was not exported, because the inferred collections do not project: {}",
                issue_codes(&issues)
            )),
        }
    } else {
        findings.notes.push(format!(
            "RBAC was not exported, because profile {} does not resolve",
            spec.profile
        ));
    }

    hint(
        &mut spec,
        "export.inferred_roles",
        &findings.inferred_roles,
        &mut findings.notes,
    );
    let unsupported = findings.unsupported.clone();
    if !unsupported.is_empty() {
        findings.notes.push(format!(
            "left out, because a spec cannot express them: {}",
            unsupported.join(", ")
        ));
        hint(
            &mut spec,
            "export.unsupported_objects",
            &unsupported,
            &mut findings.notes,
        );
    }

    let report = GuaranteeValidator::validate(&spec);
    if !report.valid {
        findings.notes.push(format!(
            "the exported spec does not validate: {}",
            issue_codes(&report.issues)
        ));
    }
    ExportedSpec {
        spec,
        source: ExportSource::Inferred,
        notes: findings.notes,
    }
}

fn issue_codes(issues: &[crate::config_spec::ValidationIssue]) -> String {
    issues
        .iter()
        .filter(|issue| issue.severity == ValidationSeverity::Error)
        .map(|issue| format!("{} at {}", issue.code, issue.path))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Records `values` under `operation_hints[key]`, joined with `, ` and cut to the value limit.
fn hint(spec: &mut DatabaseSpec, key: &str, values: &[String], notes: &mut Vec<String>) {
    if values.is_empty() {
        return;
    }
    let mut text = String::new();
    for (index, value) in values.iter().enumerate() {
        let separator = if text.is_empty() { "" } else { ", " };
        let rest = values.len() - index;
        let suffix = format!(" (+{rest} more)");
        if text.len() + separator.len() + value.len() + suffix.len() > MAX_MAP_VALUE_BYTES {
            text.push_str(&suffix);
            notes.push(format!(
                "operation_hints[{key}] lists the first {index} of {} entries",
                values.len()
            ));
            break;
        }
        text.push_str(separator);
        text.push_str(value);
    }
    spec.operation_hints.insert(key.to_owned(), text);
}

/// Rebuilds the `custom_profile` of a custom manifest: the knobs, floors and roles that differ
/// from the profile it extends. The description is not persisted and is lost.
fn custom_profile(
    manifest: &PersistedProfileManifest,
    findings: &mut Findings,
) -> Option<CustomProfileSpec> {
    let Some(extends) = manifest.extends.clone() else {
        findings.notes.push(format!(
            "custom profile {} records no base profile, so custom_profile was not exported",
            manifest.profile
        ));
        return None;
    };
    let Some(base) = resolve_named_profile(&extends, &ProfileKnobOverrides::default())
        .resolved()
        .cloned()
    else {
        findings.notes.push(format!(
            "the base profile {extends} of custom profile {} does not resolve in this build",
            manifest.profile
        ));
        return None;
    };
    let base_leaves = knob_leaves(&base.knobs);
    let mut knobs = Map::new();
    let mut floors = FloorOverrides::default();
    for (path, value) in knob_leaves(&manifest.knobs) {
        if base_leaves.get(&path) == Some(&value) || path == "compatible_roles" {
            continue;
        }
        if let Some(floor) = path.strip_prefix("floors.") {
            let raised = Some(value.as_bool() == Some(true));
            match floor {
                "audit" => floors.audit = raised,
                "backup" => floors.backup = raised,
                "pitr" => floors.pitr = raised,
                "encryption_at_rest" => floors.encryption_at_rest = raised,
                _ => {}
            }
            continue;
        }
        insert_path(&mut knobs, &path, value);
    }
    let knobs = match serde_json::from_value::<ProfileKnobOverrides>(JsonValue::Object(knobs)) {
        Ok(knobs) => knobs,
        Err(error) => {
            findings.notes.push(format!(
                "the knobs of custom profile {} cannot be expressed as overrides: {error}",
                manifest.profile
            ));
            return None;
        }
    };
    let add = manifest
        .knobs
        .compatible_roles
        .difference(&base.knobs.compatible_roles)
        .copied()
        .collect();
    let remove = base
        .knobs
        .compatible_roles
        .difference(&manifest.knobs.compatible_roles)
        .copied()
        .collect();
    let custom = CustomProfileSpec {
        extends,
        description: None,
        knobs,
        floors,
        compatible_roles: RoleSetDelta { add, remove },
    };
    findings.notes.push(format!(
        "custom_profile is rebuilt from the profile manifest of {}; its description is not \
         persisted",
        manifest.profile
    ));
    Some(custom)
}

fn insert_path(map: &mut Map<String, JsonValue>, path: &str, value: JsonValue) {
    match path.split_once('.') {
        None => {
            map.insert(path.to_owned(), value);
        }
        Some((head, rest)) => {
            let entry = map
                .entry(head.to_owned())
                .or_insert_with(|| JsonValue::Object(Map::new()));
            if let JsonValue::Object(child) = entry {
                insert_path(child, rest, value);
            }
        }
    }
}

/// One collection per container object, in catalog order, with full-text and geo indexes and
/// history attached to their source.
fn infer_collections(
    live: &LiveState,
    resolved: Option<&ResolvedProfile>,
    findings: &mut Findings,
) -> Vec<CollectionRoleSpec> {
    let mut collections = BTreeMap::new();
    for (name, entry) in &live.catalog {
        if name.starts_with("__") {
            findings.unsupported.push(name.clone());
            continue;
        }
        if let Some(collection) = container_collection(name, entry, live, resolved, findings) {
            findings
                .inferred_roles
                .push(format!("{name}={}", role_slug(collection.role)));
            collections.insert(name.clone(), collection);
        }
    }
    for (name, entry) in &live.catalog {
        attach_dependent(name, entry, live, &mut collections, findings);
    }
    // A typed row table without history says so, so the profile knob does not add one.
    for collection in collections.values_mut() {
        if let Some(physical) = collection.physical.as_mut()
            && physical.layout == Some(LayoutKnob::Row)
            && !physical.columns.is_empty()
            && physical.system_versioning.is_none()
        {
            physical.system_versioning = Some(false);
        }
    }
    collections.into_values().collect()
}

/// The collection a container entry becomes; `None` for dependents and for what a spec cannot
/// express.
fn container_collection(
    name: &str,
    entry: &CatalogEntry,
    live: &LiveState,
    resolved: Option<&ResolvedProfile>,
    findings: &mut Findings,
) -> Option<CollectionRoleSpec> {
    match entry {
        CatalogEntry::Collection {
            collection_id,
            fields,
            indexes,
        } => Some(document_collection(
            name,
            collection_id.as_u32(),
            fields,
            indexes,
            findings,
        )),
        CatalogEntry::Vector {
            collection_id,
            dim,
            metric,
            hnsw,
            ..
        } => Some(collection_spec(
            name,
            CollectionRole::VectorMemory,
            vec![CollectionIndexKind::Vector],
            PhysicalObjectSpec {
                kind: Some(PhysicalKind::Vector),
                collection_id: Some(collection_id.as_u32()),
                dim: u32::try_from(*dim).ok(),
                metric: Some(match metric {
                    VectorMetric::Cosine => VectorMetricSpec::Cosine,
                    VectorMetric::L2 => VectorMetricSpec::L2,
                }),
                hnsw: Some(HnswSpec {
                    m: u32::try_from(hnsw.m).ok(),
                    ef_construction: u32::try_from(hnsw.ef_construction).ok(),
                    ef_search: u32::try_from(hnsw.ef_search).ok(),
                }),
                ..PhysicalObjectSpec::default()
            },
        )),
        CatalogEntry::Table { indexes, layout } => table_collection(
            name,
            *layout,
            live.table_schemas.get(name),
            indexes,
            findings,
        ),
        CatalogEntry::TimeSeries { config } => {
            let knob_retention =
                resolved.and_then(|resolved| resolved.knobs.time_series.default_retention_millis);
            if config.retention_millis.is_none() && knob_retention.is_some() {
                findings.notes.push(format!(
                    "time series {name} keeps every point, but a spec cannot express \"no \
                     retention\" when the profile sets time_series.default_retention_millis"
                ));
            }
            Some(collection_spec(
                name,
                CollectionRole::TimeSeries,
                vec![CollectionIndexKind::TimeSeries],
                PhysicalObjectSpec {
                    kind: Some(PhysicalKind::TimeSeries),
                    chunk_millis: Some(config.chunk_millis),
                    retention_millis: config.retention_millis,
                    ..PhysicalObjectSpec::default()
                },
            ))
        }
        CatalogEntry::Graph { graph_id } => Some(collection_spec(
            name,
            CollectionRole::Graph,
            vec![CollectionIndexKind::Graph],
            PhysicalObjectSpec {
                kind: Some(PhysicalKind::Graph),
                graph_id: Some(graph_id.as_u32()),
                ..PhysicalObjectSpec::default()
            },
        )),
        CatalogEntry::ForeignTable { .. } | CatalogEntry::MaterializedView { .. } => {
            findings.unsupported.push(name.to_owned());
            None
        }
        CatalogEntry::FullTextIndex { .. }
        | CatalogEntry::GeoIndex { .. }
        | CatalogEntry::TemporalTable { .. } => None,
    }
}

/// Attaches a full-text index, geo index or history table to the collection it derives from.
fn attach_dependent(
    name: &str,
    entry: &CatalogEntry,
    live: &LiveState,
    collections: &mut BTreeMap<String, CollectionRoleSpec>,
    findings: &mut Findings,
) {
    match entry {
        CatalogEntry::FullTextIndex { config } => {
            let TextSource::Collection {
                collection_id,
                path,
            } = &config.source;
            let source = source_name(&live.catalog, *collection_id);
            let path = path.segments().join(".");
            let Some(physical) = document_physical(collections, &source) else {
                findings.unsupported.push(name.to_owned());
                return;
            };
            physical.full_text.push(FullTextSpec {
                name: (name != full_text_index_name(&source, &path)).then(|| name.to_owned()),
                path,
                language: (config.language != DEFAULT_FULL_TEXT_LANGUAGE)
                    .then(|| config.language.clone()),
                refresh_lag_target: (config.refresh_lag_target != DEFAULT_REFRESH_LAG_TARGET)
                    .then_some(config.refresh_lag_target),
            });
            add_index(collections, &source, CollectionIndexKind::FullText);
        }
        CatalogEntry::GeoIndex { config } => {
            let source = source_name(&live.catalog, config.collection_id);
            let path = config.path.segments().join(".");
            let Some(physical) = document_physical(collections, &source) else {
                findings.unsupported.push(name.to_owned());
                return;
            };
            physical.geo.push(GeoSpec {
                name: (name != geo_index_name(&source, &path)).then(|| name.to_owned()),
                path,
                precision: (config.precision != DEFAULT_GEO_PRECISION).then_some(config.precision),
            });
            add_index(collections, &source, CollectionIndexKind::Geo);
        }
        CatalogEntry::TemporalTable {
            base_table,
            retention,
            ..
        } => {
            let physical = collections
                .get_mut(base_table)
                .and_then(|collection| collection.physical.as_mut())
                .filter(|physical| {
                    physical.layout == Some(LayoutKnob::Row) && !physical.columns.is_empty()
                });
            match physical {
                Some(physical) if name == history_table_name(base_table) => {
                    physical.system_versioning = Some(true);
                    physical.keep_history = Some(retention.keep_history);
                }
                _ => findings.unsupported.push(name.to_owned()),
            }
        }
        _ => {}
    }
}

fn collection_spec(
    name: &str,
    role: CollectionRole,
    indexes: Vec<CollectionIndexKind>,
    physical: PhysicalObjectSpec,
) -> CollectionRoleSpec {
    CollectionRoleSpec {
        name: name.to_owned(),
        role,
        domain: "primary".to_owned(),
        indexes,
        physical: Some(physical),
    }
}

fn document_physical<'c>(
    collections: &'c mut BTreeMap<String, CollectionRoleSpec>,
    source: &str,
) -> Option<&'c mut PhysicalObjectSpec> {
    collections
        .get_mut(source)
        .and_then(|collection| collection.physical.as_mut())
        .filter(|physical| physical.kind == Some(PhysicalKind::Document))
}

fn add_index(
    collections: &mut BTreeMap<String, CollectionRoleSpec>,
    source: &str,
    kind: CollectionIndexKind,
) {
    if let Some(collection) = collections.get_mut(source)
        && !collection.indexes.contains(&kind)
    {
        collection.indexes.push(kind);
    }
}

fn document_collection(
    name: &str,
    collection_id: u32,
    fields: &[crate::query::DocField],
    indexes: &[crate::model::IndexSpec],
    findings: &mut Findings,
) -> CollectionRoleSpec {
    let mut physical = PhysicalObjectSpec {
        kind: Some(PhysicalKind::Document),
        collection_id: Some(collection_id),
        ..PhysicalObjectSpec::default()
    };
    for field in fields {
        let Some(ty) = column_type(&field.ty) else {
            findings.unsupported.push(format!("{name}.{}", field.name));
            continue;
        };
        physical.fields.push(DocFieldSpec {
            name: field.name.clone(),
            path: match &field.source {
                DocFieldSource::DocumentId => DOCUMENT_ID_PATH.to_owned(),
                DocFieldSource::Path(path) => path.segments().join("."),
            },
            ty,
        });
    }
    for index in indexes {
        physical.document_indexes.push(DocIndexSpec {
            path: index.path.segments().join("."),
            id: Some(index.id.as_u32()),
        });
    }
    let mut kinds = vec![CollectionIndexKind::Primary];
    if !physical.document_indexes.is_empty() {
        kinds.push(CollectionIndexKind::Document);
    }
    collection_spec(name, CollectionRole::DocumentEntity, kinds, physical)
}

fn table_collection(
    name: &str,
    layout: TableLayout,
    schema: Option<&TableSchema>,
    indexes: &[crate::query::RelIndexSpec],
    findings: &mut Findings,
) -> Option<CollectionRoleSpec> {
    let mut physical = PhysicalObjectSpec {
        kind: Some(PhysicalKind::Table),
        layout: Some(LayoutKnob::from(layout)),
        ..PhysicalObjectSpec::default()
    };
    if let Some(schema) = schema {
        for column in &schema.columns {
            let Some(ty) = column_type(&column.ty) else {
                findings.unsupported.push(format!("{name}.{}", column.name));
                return None;
            };
            physical.columns.push(ColumnSpec {
                name: column.name.clone(),
                ty,
                nullable: column.nullable,
            });
        }
        physical.primary_key = schema
            .columns
            .get(schema.primary_key)
            .map(|column| column.name.clone());
    }
    let column_name = |column: usize| -> Option<String> {
        schema
            .and_then(|schema| schema.columns.get(column))
            .map(|column| column.name.clone())
    };
    for index in indexes {
        let (expression, column) = match index.expression {
            RelIndexExpression::Column(column) => (None, column),
            RelIndexExpression::LowerAscii(column) => {
                (Some(TableIndexExpressionSpec::LowerAscii), column)
            }
        };
        let include = index
            .include
            .iter()
            .map(|column| column_name(*column))
            .collect::<Option<Vec<_>>>();
        let (Some(column), Some(include), None) = (column_name(column), include, &index.predicate)
        else {
            findings
                .unsupported
                .push(format!("{name}.index_{}", index.id));
            return None;
        };
        physical.table_indexes.push(TableIndexSpec {
            column,
            expression,
            include,
            id: Some(index.id),
        });
    }
    let (role, kinds) = if layout == TableLayout::Columnar {
        (
            CollectionRole::Analytics,
            vec![CollectionIndexKind::Columnar],
        )
    } else {
        let mut kinds = vec![CollectionIndexKind::Primary];
        if !physical.table_indexes.is_empty() {
            kinds.push(CollectionIndexKind::Document);
        }
        (CollectionRole::DocumentEntity, kinds)
    };
    Some(collection_spec(name, role, kinds, physical))
}

const fn column_type(ty: &ColumnType) -> Option<ColumnTypeSpec> {
    match ty {
        ColumnType::Int => Some(ColumnTypeSpec::Int),
        ColumnType::Float => Some(ColumnTypeSpec::Float),
        ColumnType::Str => Some(ColumnTypeSpec::Str),
        ColumnType::Bool => Some(ColumnTypeSpec::Bool),
        ColumnType::Bytes => Some(ColumnTypeSpec::Bytes),
        ColumnType::Null => None,
    }
}

/// The built-in extensions the inferred collections need.
fn required_extensions(collections: &[CollectionRoleSpec]) -> Vec<ExtensionManifestRef> {
    let mut names = BTreeSet::new();
    for collection in collections {
        for extension in role_required_extensions(collection.role) {
            names.insert((*extension).to_owned());
        }
        names.extend(index_required_extensions(&collection.indexes));
    }
    names
        .into_iter()
        .filter_map(|name| {
            built_in_extension_manifest(&name).map(|manifest| ExtensionManifestRef {
                name,
                version: manifest.version.clone(),
                stability: if manifest.status == SupportStatus::Experimental {
                    ExtensionStability::Experimental
                } else {
                    ExtensionStability::Stable
                },
            })
        })
        .collect()
}

/// The live RBAC state minus the bootstrap administrator. A role with a grant the spec cannot
/// express (system, or an object the spec does not declare with that kind) is left out, and so
/// is a principal with a role that is.
fn infer_security(
    live: &LiveState,
    objects: &[DesiredObject],
    findings: &mut Findings,
) -> Option<SecuritySpec> {
    let resources = objects
        .iter()
        .map(|object| {
            (
                object_resource(&object.name, &object.kind),
                object.name.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut exported_roles = BTreeSet::new();
    let mut roles = Vec::new();
    for role in live.authz.roles().values() {
        let name = role.name();
        if RESERVED_ROLE_NAMES.contains(&name) {
            continue;
        }
        if !valid_role_name(name) {
            findings.unsupported.push(format!("role {name}"));
            continue;
        }
        let mut grants = Vec::new();
        let mut expressible = role.grants().len() <= MAX_GRANTS_PER_ROLE;
        for (resource, permissions) in role.grants() {
            let (object, scope) = match resource {
                Resource::Database => (None, Some(GrantScope::Database)),
                Resource::System => {
                    expressible = false;
                    break;
                }
                other => {
                    let Some(object) = resources.get(other) else {
                        expressible = false;
                        break;
                    };
                    (Some(object.clone()), None)
                }
            };
            grants.push(GrantSpec {
                object,
                resource: scope,
                permissions: permissions
                    .iter()
                    .map(|permission| match permission {
                        Permission::Read => PermissionSpec::Read,
                        Permission::Write => PermissionSpec::Write,
                        Permission::Admin => PermissionSpec::Admin,
                    })
                    .collect(),
            });
        }
        if !expressible {
            findings.unsupported.push(format!("role {name}"));
            continue;
        }
        exported_roles.insert(name.to_owned());
        roles.push(RoleSpec {
            name: name.to_owned(),
            grants,
        });
    }

    let mut principals = Vec::new();
    for (user, principal) in live.principals.principals() {
        let reserved = RESERVED_PRINCIPAL_NAMES.contains(&user.as_str())
            || RESERVED_PRINCIPAL_NAMES.contains(&principal.name());
        if reserved {
            continue;
        }
        let expressible = valid_principal_name(user)
            && valid_principal_name(principal.name())
            && principal
                .roles()
                .iter()
                .all(|role| exported_roles.contains(role));
        if !expressible {
            findings.unsupported.push(format!("principal {user}"));
            continue;
        }
        principals.push(PrincipalSpec {
            user: user.clone(),
            principal: (principal.name() != user).then(|| principal.name().to_owned()),
            roles: principal.roles().iter().cloned().collect(),
        });
    }
    if roles.is_empty() && principals.is_empty() {
        None
    } else {
        Some(SecuritySpec { roles, principals })
    }
}

/// `^[a-z][a-z0-9_]{0,63}$`, as `security.roles[].name` requires.
fn valid_role_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|first| first.is_ascii_lowercase())
        && name.len() <= 64
        && chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

/// 1 to 128 printable ASCII characters, as `security.principals[]` requires.
fn valid_principal_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|character| character.is_ascii_graphic())
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
