use serde_json::{Map, Value as JsonValue, json};

use super::{
    ALL_INDEX_KINDS, ColumnTypeSpec, DATABASE_SPEC_VERSION_V2, DEFAULT_FULL_TEXT_LANGUAGE,
    GrantScope, KNOB_REGISTRY, KnobType, LayoutKnob, MAX_CHUNK_MILLIS, MAX_COLLECTIONS,
    MAX_FIELDS_PER_COLLECTION, MAX_FULL_TEXT_PER_COLLECTION, MAX_GEO_PER_COLLECTION,
    MAX_GRANTS_PER_ROLE, MAX_INDEXES_PER_COLLECTION, MAX_PRINCIPALS, MAX_ROLES, MAX_VECTOR_DIM,
    MIN_CHUNK_MILLIS, PermissionSpec, PhysicalKind, TableIndexExpressionSpec, UnmanagedPolicy,
    VectorMetricSpec, collection_role_definitions, database_spec_v1_schema, describe_value,
};

/// Every `PhysicalKind`, in declaration order; the schema enum is generated from it.
const PHYSICAL_KINDS: &[PhysicalKind] = &[
    PhysicalKind::Document,
    PhysicalKind::Table,
    PhysicalKind::Vector,
    PhysicalKind::TimeSeries,
    PhysicalKind::Graph,
];
const COLUMN_TYPES: &[ColumnTypeSpec] = &[
    ColumnTypeSpec::Int,
    ColumnTypeSpec::Float,
    ColumnTypeSpec::Str,
    ColumnTypeSpec::Bool,
    ColumnTypeSpec::Bytes,
];
const LAYOUTS: &[LayoutKnob] = &[LayoutKnob::Row, LayoutKnob::Columnar];
const TABLE_INDEX_EXPRESSIONS: &[TableIndexExpressionSpec] = &[
    TableIndexExpressionSpec::Column,
    TableIndexExpressionSpec::LowerAscii,
];
const VECTOR_METRICS: &[VectorMetricSpec] = &[VectorMetricSpec::Cosine, VectorMetricSpec::L2];
const GRANT_SCOPES: &[GrantScope] = &[GrantScope::Database];
const PERMISSIONS: &[PermissionSpec] = &[
    PermissionSpec::Read,
    PermissionSpec::Write,
    PermissionSpec::Admin,
];
const UNMANAGED_POLICIES: &[UnmanagedPolicy] = &[
    UnmanagedPolicy::Ignore,
    UnmanagedPolicy::Report,
    UnmanagedPolicy::Fail,
];
/// The largest catalog name, in bytes (`db::is_valid_catalog_name`).
const MAX_NAME_BYTES: u32 = 128;

/// The JSON schema of a version 2 `DatabaseSpec`: the version 1 schema plus `custom_profile`,
/// whose knob bounds are generated from `KNOB_REGISTRY`, `security`, `provisioning`,
/// `collections[].physical` and the `geo` index kind. The version 1 schema is unchanged.
#[must_use]
pub fn database_spec_v2_schema() -> JsonValue {
    let mut schema = database_spec_v1_schema();
    schema["$id"] = json!("https://multidb.dev/schemas/database-spec-v2.schema.json");
    schema["title"] = json!("MultiDB DatabaseSpec v2");
    schema["properties"]["version"] = json!({ "const": DATABASE_SPEC_VERSION_V2 });
    schema["properties"]["collections"]["maxItems"] = json!(MAX_COLLECTIONS);
    schema["properties"]["custom_profile"] = json!({ "$ref": "#/$defs/custom_profile" });
    schema["properties"]["security"] = json!({ "$ref": "#/$defs/security" });
    schema["properties"]["provisioning"] = json!({ "$ref": "#/$defs/provisioning" });

    let roles = role_enum();
    schema["$defs"]["collection_role"]["properties"]["role"] = roles.clone();
    schema["$defs"]["collection_role"]["properties"]["indexes"]["items"] = index_enum();
    schema["$defs"]["collection_role"]["properties"]["physical"] =
        json!({ "$ref": "#/$defs/physical" });
    schema["$defs"]["physical"] = physical_schema();
    schema["$defs"]["security"] = security_schema();
    schema["$defs"]["provisioning"] = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "unmanaged": {
                "description": "What a plan does with catalog objects the spec does not declare (default report).",
                "type": "string",
                "enum": slugs(UNMANAGED_POLICIES)
            }
        }
    });
    schema["$defs"]["custom_profile"] = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["extends"],
        "properties": {
            "extends": { "type": "string", "minLength": 1 },
            "description": { "type": "string", "maxLength": 500 },
            "knobs": { "$ref": "#/$defs/profile_knob_overrides" },
            "floors": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "audit": { "type": "boolean" },
                    "backup": { "type": "boolean" },
                    "pitr": { "type": "boolean" },
                    "encryption_at_rest": { "type": "boolean" }
                }
            },
            "compatible_roles": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "add": { "type": "array", "items": roles.clone() },
                    "remove": { "type": "array", "items": roles }
                }
            }
        }
    });
    schema["$defs"]["profile_knob_overrides"] = knob_overrides_schema();
    schema
}

/// The serde names of `values`, in order.
fn slugs<T: serde::Serialize>(values: &[T]) -> Vec<JsonValue> {
    values
        .iter()
        .map(|value| JsonValue::from(describe_value(value).trim_matches('"')))
        .collect()
}

fn string_enum<T: serde::Serialize>(description: &str, values: &[T]) -> JsonValue {
    json!({ "description": description, "type": "string", "enum": slugs(values) })
}

fn name_schema(description: &str) -> JsonValue {
    json!({
        "description": description,
        "type": "string",
        "minLength": 1,
        "maxLength": MAX_NAME_BYTES
    })
}

fn id_schema(description: &str) -> JsonValue {
    json!({ "description": description, "type": "integer", "minimum": 0, "maximum": u32::MAX })
}

fn path_schema(description: &str) -> JsonValue {
    json!({ "description": description, "type": "string", "minLength": 1 })
}

/// `collections[].physical`: one flat object; each property names the kinds it applies to.
fn physical_schema() -> JsonValue {
    let column_type = string_enum("Value type.", COLUMN_TYPES);
    json!({
        "description": "How provisioning builds the collection. Every property is optional and applies to the kinds its description names (PHYSICAL_FIELD_NOT_APPLICABLE otherwise).",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "kind": string_enum(
                "All kinds: the runtime object. Defaults to the first kind the role allows.",
                PHYSICAL_KINDS,
            ),
            "collection_id": id_schema("document, vector: a pinned collection id; unpinned ids are allocated at creation."),
            "fields": {
                "description": "document: SQL-visible fields; empty means one field id holding the document id.",
                "type": "array",
                "maxItems": MAX_FIELDS_PER_COLLECTION,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "path", "type"],
                    "properties": {
                        "name": name_schema("Field name."),
                        "path": path_schema("Dotted document path, or $id for the document id (type bytes)."),
                        "type": column_type.clone()
                    }
                }
            },
            "document_indexes": {
                "description": "document: secondary indexes; required by the document index kind.",
                "type": "array",
                "maxItems": MAX_INDEXES_PER_COLLECTION,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["path"],
                    "properties": {
                        "path": path_schema("Dotted document path."),
                        "id": id_schema("Index id; defaults to position + 1.")
                    }
                }
            },
            "full_text": {
                "description": "document: full-text indexes, named <collection>_<path>_fts unless name is set; required by the full_text index kind.",
                "type": "array",
                "maxItems": MAX_FULL_TEXT_PER_COLLECTION,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["path"],
                    "properties": {
                        "path": path_schema("Dotted document path of the text."),
                        "name": name_schema("Index name."),
                        "language": {
                            "description": "Tokenizer language; only simple in this build.",
                            "type": "string",
                            "enum": [DEFAULT_FULL_TEXT_LANGUAGE]
                        },
                        "refresh_lag_target": {
                            "description": "Refresh lag target (default 1).",
                            "type": "integer",
                            "minimum": 0
                        }
                    }
                }
            },
            "geo": {
                "description": "document: geo indexes, named <collection>_<path>_geo unless name is set; required by the geo index kind.",
                "type": "array",
                "maxItems": MAX_GEO_PER_COLLECTION,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["path"],
                    "properties": {
                        "path": path_schema("Dotted document path of the point."),
                        "name": name_schema("Index name."),
                        "precision": {
                            "description": "Geohash precision (default 6).",
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 12
                        }
                    }
                }
            },
            "layout": string_enum(
                "table: physical layout; defaults to columnar for analytics and to the database layout otherwise.",
                LAYOUTS,
            ),
            "columns": {
                "description": "table: typed columns; empty means a schemaless row table.",
                "type": "array",
                "maxItems": MAX_FIELDS_PER_COLLECTION,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "type"],
                    "properties": {
                        "name": name_schema("Column name."),
                        "type": column_type,
                        "nullable": { "description": "Default false.", "type": "boolean" }
                    }
                }
            },
            "primary_key": name_schema("table: the primary key column; required when columns are given."),
            "table_indexes": {
                "description": "table: B-tree indexes of a row table; required by the document index kind.",
                "type": "array",
                "maxItems": MAX_INDEXES_PER_COLLECTION,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["column"],
                    "properties": {
                        "column": name_schema("Indexed column."),
                        "expression": string_enum(
                            "What the index keys on (default column); lower_ascii needs a str column.",
                            TABLE_INDEX_EXPRESSIONS,
                        ),
                        "include": {
                            "description": "Covered columns.",
                            "type": "array",
                            "items": name_schema("Column name.")
                        },
                        "id": id_schema("Index id; defaults to position + 1.")
                    }
                }
            },
            "system_versioning": {
                "description": "table: keep system-versioned history in <table>_history (typed row tables); defaults to the history.system_versioning knob.",
                "type": "boolean"
            },
            "keep_history": {
                "description": "table: keep history rows; defaults to the history.keep_history knob.",
                "type": "boolean"
            },
            "dim": {
                "description": "vector: the dimension, fixed at creation; required.",
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_VECTOR_DIM
            },
            "metric": string_enum("vector: distance metric (default cosine).", VECTOR_METRICS),
            "hnsw": {
                "description": "vector: HNSW parameters (default 16/200/48); ef_construction must be at least m.",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "m": { "type": "integer", "minimum": 1, "maximum": 255 },
                    "ef_construction": { "type": "integer", "minimum": 1, "maximum": u32::MAX },
                    "ef_search": { "type": "integer", "minimum": 1, "maximum": u32::MAX }
                }
            },
            "chunk_millis": {
                "description": "time_series: chunk width; defaults to the time_series.default_chunk_millis knob.",
                "type": "integer",
                "minimum": MIN_CHUNK_MILLIS,
                "maximum": MAX_CHUNK_MILLIS
            },
            "retention_millis": {
                "description": "time_series: retention, at least one chunk; defaults to the time_series.default_retention_millis knob.",
                "type": "integer",
                "minimum": 1
            },
            "max_future_skew_millis": {
                "description": "time_series: reserved; refused until the runtime supports a future-skew bound.",
                "type": "integer"
            },
            "graph_id": id_schema("graph: a pinned graph id; unpinned ids are allocated at creation.")
        }
    })
}

/// `security`: managed roles and principals. A grant names exactly one of `object` or
/// `resource`; there is no system scope.
fn security_schema() -> JsonValue {
    json!({
        "description": "Roles and principals provisioning manages; others are never changed. The role admin and the principals admin and root are reserved.",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "roles": {
                "type": "array",
                "maxItems": MAX_ROLES,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "grants"],
                    "properties": {
                        "name": {
                            "description": "Role name, ^[a-z][a-z0-9_]{0,63}$.",
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 64
                        },
                        "grants": {
                            "type": "array",
                            "maxItems": MAX_GRANTS_PER_ROLE,
                            "items": {
                                "description": "Exactly one of object or resource.",
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["permissions"],
                                "properties": {
                                    "object": name_schema("A collection, or a full-text, geo or history object of the spec."),
                                    "resource": string_enum("The whole database.", GRANT_SCOPES),
                                    "permissions": {
                                        "type": "array",
                                        "items": string_enum("Permission.", PERMISSIONS)
                                    }
                                },
                                "oneOf": [
                                    { "required": ["object"] },
                                    { "required": ["resource"] }
                                ]
                            }
                        }
                    }
                }
            },
            "principals": {
                "type": "array",
                "maxItems": MAX_PRINCIPALS,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["user", "roles"],
                    "properties": {
                        "user": name_schema("Login user name."),
                        "principal": name_schema("Principal name; defaults to the user."),
                        "roles": {
                            "type": "array",
                            "items": {
                                "description": "A role of security.roles.",
                                "type": "string",
                                "minLength": 1
                            }
                        }
                    }
                }
            }
        }
    })
}

fn role_enum() -> JsonValue {
    let roles = collection_role_definitions()
        .iter()
        .map(|definition| JsonValue::from(definition.slug))
        .collect::<Vec<_>>();
    json!({ "type": "string", "enum": roles })
}

fn index_enum() -> JsonValue {
    json!({ "type": "string", "enum": slugs(ALL_INDEX_KINDS) })
}

/// One object per knob section, with one property per registry leaf. Floors and roles have
/// their own blocks in `custom_profile`, so they are not knobs here.
fn knob_overrides_schema() -> JsonValue {
    let mut sections = Map::new();
    for definition in KNOB_REGISTRY {
        let Some((section, leaf)) = definition.path.split_once('.') else {
            continue;
        };
        if section == "floors" {
            continue;
        }
        let mut property = knob_value_schema(definition.value_type);
        property["description"] = json!(definition.description);
        let entry = sections.entry(section.to_owned()).or_insert_with(|| {
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            })
        });
        entry["properties"][leaf] = property;
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": sections
    })
}

fn knob_value_schema(value_type: KnobType) -> JsonValue {
    match value_type {
        KnobType::Enum(values) => json!({ "type": "string", "enum": values }),
        KnobType::U32 { min, max } => json!({ "type": "integer", "minimum": min, "maximum": max }),
        KnobType::U64 { min, max } => json!({ "type": "integer", "minimum": min, "maximum": max }),
        KnobType::I64 { min, max } => json!({ "type": "integer", "minimum": min, "maximum": max }),
        KnobType::AutoOrU32 { min, max } => keyword_or_integer("auto", min, max),
        KnobType::LimitU32 { min, max } => keyword_or_integer("unlimited", min, max),
        KnobType::OptI64 { min, max } => {
            json!({ "type": ["integer", "null"], "minimum": min, "maximum": max })
        }
        KnobType::Bool => json!({ "type": "boolean" }),
        KnobType::RoleSet => json!({ "type": "array", "items": role_enum() }),
    }
}

/// `"auto"` or `"unlimited"` as a `const` branch, so validators report precise errors.
fn keyword_or_integer(keyword: &str, min: u32, max: u32) -> JsonValue {
    json!({
        "oneOf": [
            { "const": keyword },
            { "type": "integer", "minimum": min, "maximum": max }
        ]
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::{Value as JsonValue, json};

    use super::{
        COLUMN_TYPES, GRANT_SCOPES, LAYOUTS, PERMISSIONS, PHYSICAL_KINDS, TABLE_INDEX_EXPRESSIONS,
        UNMANAGED_POLICIES, VECTOR_METRICS, database_spec_v2_schema,
    };
    use crate::config_spec::{
        ALL_INDEX_KINDS, CollectionIndexKind, ColumnTypeSpec, GrantScope, GuaranteeValidator,
        KNOB_REGISTRY, KnobType, LayoutKnob, PermissionSpec, PhysicalKind,
        TableIndexExpressionSpec, UnmanagedPolicy, ValidationSeverity, VectorMetricSpec,
        database_spec_v1_schema,
        spec_io::{SpecFormat, decode_spec_bytes},
    };

    /// Forces `ALL_INDEX_KINDS` to be updated when an index kind is added.
    const fn index_kind_position(kind: CollectionIndexKind) -> usize {
        match kind {
            CollectionIndexKind::Primary => 0,
            CollectionIndexKind::Document => 1,
            CollectionIndexKind::Vector => 2,
            CollectionIndexKind::Graph => 3,
            CollectionIndexKind::FullText => 4,
            CollectionIndexKind::Columnar => 5,
            CollectionIndexKind::TimeSeries => 6,
            CollectionIndexKind::Geo => 7,
        }
    }

    /// Forces the generated physical, security and provisioning enums to list every variant.
    #[test]
    fn generated_enum_tables_list_every_variant() {
        let kind = |kind: PhysicalKind| match kind {
            PhysicalKind::Document => 0,
            PhysicalKind::Table => 1,
            PhysicalKind::Vector => 2,
            PhysicalKind::TimeSeries => 3,
            PhysicalKind::Graph => 4,
        };
        let column = |ty: ColumnTypeSpec| match ty {
            ColumnTypeSpec::Int => 0,
            ColumnTypeSpec::Float => 1,
            ColumnTypeSpec::Str => 2,
            ColumnTypeSpec::Bool => 3,
            ColumnTypeSpec::Bytes => 4,
        };
        let layout = |layout: LayoutKnob| match layout {
            LayoutKnob::Row => 0,
            LayoutKnob::Columnar => 1,
        };
        let expression = |expression: TableIndexExpressionSpec| match expression {
            TableIndexExpressionSpec::Column => 0,
            TableIndexExpressionSpec::LowerAscii => 1,
        };
        let metric = |metric: VectorMetricSpec| match metric {
            VectorMetricSpec::Cosine => 0,
            VectorMetricSpec::L2 => 1,
        };
        let scope = |scope: GrantScope| match scope {
            GrantScope::Database => 0,
        };
        let permission = |permission: PermissionSpec| match permission {
            PermissionSpec::Read => 0,
            PermissionSpec::Write => 1,
            PermissionSpec::Admin => 2,
        };
        let policy = |policy: UnmanagedPolicy| match policy {
            UnmanagedPolicy::Ignore => 0,
            UnmanagedPolicy::Report => 1,
            UnmanagedPolicy::Fail => 2,
        };
        for (position, value) in PHYSICAL_KINDS.iter().enumerate() {
            assert_eq!(kind(*value), position);
        }
        for (position, value) in COLUMN_TYPES.iter().enumerate() {
            assert_eq!(column(*value), position);
        }
        for (position, value) in LAYOUTS.iter().enumerate() {
            assert_eq!(layout(*value), position);
        }
        for (position, value) in TABLE_INDEX_EXPRESSIONS.iter().enumerate() {
            assert_eq!(expression(*value), position);
        }
        for (position, value) in VECTOR_METRICS.iter().enumerate() {
            assert_eq!(metric(*value), position);
        }
        for (position, value) in GRANT_SCOPES.iter().enumerate() {
            assert_eq!(scope(*value), position);
        }
        for (position, value) in PERMISSIONS.iter().enumerate() {
            assert_eq!(permission(*value), position);
        }
        for (position, value) in UNMANAGED_POLICIES.iter().enumerate() {
            assert_eq!(policy(*value), position);
        }
        assert_eq!(
            [
                PHYSICAL_KINDS.len(),
                COLUMN_TYPES.len(),
                LAYOUTS.len(),
                TABLE_INDEX_EXPRESSIONS.len(),
                VECTOR_METRICS.len(),
                GRANT_SCOPES.len(),
                PERMISSIONS.len(),
                UNMANAGED_POLICIES.len()
            ],
            [5, 5, 2, 2, 2, 1, 3, 3]
        );
    }

    #[test]
    fn v2_schema_snapshot_matches_checked_in_file() -> Result<(), Box<dyn std::error::Error>> {
        let expected = serde_json::from_str::<JsonValue>(include_str!(
            "../../docs/schemas/database-spec-v2.schema.json"
        ))?;

        assert_eq!(database_spec_v2_schema(), expected);
        Ok(())
    }

    #[test]
    fn v2_schema_keeps_the_v1_shape_and_requires_version_2() {
        let v1 = database_spec_v1_schema();
        let v2 = database_spec_v2_schema();

        assert_eq!(v2["required"], v1["required"]);
        assert_eq!(v2["properties"]["version"], json!({ "const": 2 }));
        assert_eq!(v2["additionalProperties"], json!(false));
        assert_eq!(
            v2["$id"],
            json!("https://multidb.dev/schemas/database-spec-v2.schema.json")
        );
        assert_eq!(
            v2["$defs"]["custom_profile"]["required"],
            json!(["extends"])
        );
        assert_eq!(
            v2["$defs"]["custom_profile"]["properties"]["description"]["maxLength"],
            json!(500)
        );
        for (name, definition) in v1["$defs"].as_object().into_iter().flatten() {
            if name != "collection_role" {
                assert_eq!(&v2["$defs"][name], definition, "{name}");
            }
        }
    }

    #[test]
    fn v2_schema_enums_are_generated_and_equal_v1() {
        let v1 = database_spec_v1_schema();
        let v2 = database_spec_v2_schema();
        let role_path = "/$defs/collection_role/properties/role/enum";
        let index_path = "/$defs/collection_role/properties/indexes/items/enum";

        assert_eq!(v2.pointer(role_path), v1.pointer(role_path));
        let mut v1_indexes = v1
            .pointer(index_path)
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        v1_indexes.push(json!("geo"));
        assert_eq!(v2.pointer(index_path), Some(&JsonValue::Array(v1_indexes)));
        assert_eq!(
            v2.pointer(role_path)
                .and_then(JsonValue::as_array)
                .map(Vec::len),
            Some(9)
        );
        for (position, kind) in ALL_INDEX_KINDS.iter().enumerate() {
            assert_eq!(index_kind_position(*kind), position);
        }
        assert_eq!(
            v2.pointer(index_path)
                .and_then(JsonValue::as_array)
                .map(Vec::len),
            Some(ALL_INDEX_KINDS.len())
        );
        assert_eq!(
            v2.pointer(
                "/$defs/custom_profile/properties/compatible_roles/properties/add/items/enum"
            ),
            v1.pointer(role_path)
        );
    }

    #[test]
    fn every_registry_knob_is_in_the_v2_schema_with_its_bounds() {
        let schema = database_spec_v2_schema();
        let knobs = &schema["$defs"]["profile_knob_overrides"];
        let mut covered = 0;
        for definition in KNOB_REGISTRY {
            let pointer = match definition.path.split_once('.') {
                Some(("floors", leaf)) => {
                    format!("/$defs/custom_profile/properties/floors/properties/{leaf}")
                }
                Some((section, leaf)) => {
                    format!("/$defs/profile_knob_overrides/properties/{section}/properties/{leaf}")
                }
                None => format!("/$defs/custom_profile/properties/{}", definition.path),
            };
            let Some(property) = schema.pointer(&pointer) else {
                panic!("{} is missing from the v2 schema", definition.path);
            };
            covered += 1;
            let integer = match definition.value_type {
                KnobType::AutoOrU32 { .. } | KnobType::LimitU32 { .. } => {
                    let keyword = if matches!(definition.value_type, KnobType::AutoOrU32 { .. }) {
                        "auto"
                    } else {
                        "unlimited"
                    };
                    assert_eq!(property["oneOf"][0], json!({ "const": keyword }));
                    &property["oneOf"][1]
                }
                _ => property,
            };
            let bounds = match definition.value_type {
                KnobType::U32 { min, max }
                | KnobType::AutoOrU32 { min, max }
                | KnobType::LimitU32 { min, max } => Some((json!(min), json!(max))),
                KnobType::U64 { min, max } => Some((json!(min), json!(max))),
                KnobType::I64 { min, max } | KnobType::OptI64 { min, max } => {
                    Some((json!(min), json!(max)))
                }
                KnobType::Enum(values) => {
                    assert_eq!(property["enum"], json!(values), "{}", definition.path);
                    None
                }
                KnobType::Bool | KnobType::RoleSet => None,
            };
            if let Some((min, max)) = bounds {
                assert_eq!(integer["minimum"], min, "{}", definition.path);
                assert_eq!(integer["maximum"], max, "{}", definition.path);
            }
        }
        assert_eq!(covered, KNOB_REGISTRY.len());
        assert_eq!(knobs["additionalProperties"], json!(false));
        for section in knobs["properties"].as_object().into_iter().flatten() {
            assert_eq!(
                section.1["additionalProperties"],
                json!(false),
                "{}",
                section.0
            );
        }
    }

    /// Checks a JSON value against the subset of JSON schema these schemas use: `$ref`,
    /// `required`, `properties`, `additionalProperties`, `items`, `enum`, `const`, `oneOf`
    /// (combined with sibling keywords), `type`, integer bounds, `minLength`, `maxLength` and
    /// `maxItems`.
    fn conforms(
        root: &JsonValue,
        schema: &JsonValue,
        value: &JsonValue,
        path: &str,
    ) -> Result<(), String> {
        if let Some(reference) = schema["$ref"].as_str() {
            let pointer = reference.trim_start_matches('#');
            let target = root
                .pointer(pointer)
                .ok_or_else(|| format!("{path}: unresolved $ref {reference}"))?;
            return conforms(root, target, value, path);
        }
        if let Some(branches) = schema["oneOf"].as_array() {
            let matching = branches
                .iter()
                .filter(|branch| conforms(root, branch, value, path).is_ok())
                .count();
            if matching != 1 {
                return Err(format!("{path}: {value} matches {matching} oneOf branches"));
            }
        }
        if let Some(expected) = schema.get("const")
            && expected != value
        {
            return Err(format!("{path}: expected {expected}, found {value}"));
        }
        if let Some(allowed) = schema["enum"].as_array()
            && !allowed.contains(value)
        {
            return Err(format!("{path}: {value} is not one of {allowed:?}"));
        }
        check_type(schema, value, path)?;
        if let Some(number) = value.as_i64()
            && (schema["minimum"]
                .as_i64()
                .is_some_and(|minimum| number < minimum)
                || schema["maximum"]
                    .as_i64()
                    .is_some_and(|maximum| number > maximum))
        {
            return Err(format!("{path}: {number} is out of range"));
        }
        if let Some(text) = value.as_str() {
            let length = text.chars().count();
            if bound(schema, "minLength").is_some_and(|minimum| length < minimum)
                || bound(schema, "maxLength").is_some_and(|maximum| length > maximum)
            {
                return Err(format!("{path}: {text:?} has a length out of range"));
            }
        }
        if let (Some(items), Some(maximum)) = (value.as_array(), bound(schema, "maxItems"))
            && items.len() > maximum
        {
            return Err(format!("{path}: more than {maximum} items"));
        }
        if let JsonValue::Object(object) = value {
            for required in schema["required"].as_array().into_iter().flatten() {
                let key = required.as_str().unwrap_or_default();
                if !object.contains_key(key) {
                    return Err(format!("{path}: missing required {key}"));
                }
            }
            for (key, child) in object {
                let child_path = format!("{path}.{key}");
                match schema["properties"].get(key) {
                    Some(property) => conforms(root, property, child, &child_path)?,
                    None => match &schema["additionalProperties"] {
                        JsonValue::Bool(false) => {
                            return Err(format!("{child_path}: not allowed by the schema"));
                        }
                        JsonValue::Object(_) => {
                            conforms(root, &schema["additionalProperties"], child, &child_path)?;
                        }
                        _ => {}
                    },
                }
            }
        }
        if let (JsonValue::Array(items), Some(item_schema)) = (value, schema.get("items")) {
            for (index, item) in items.iter().enumerate() {
                conforms(root, item_schema, item, &format!("{path}[{index}]"))?;
            }
        }
        Ok(())
    }

    fn bound(schema: &JsonValue, keyword: &str) -> Option<usize> {
        schema[keyword]
            .as_u64()
            .and_then(|bound| usize::try_from(bound).ok())
    }

    fn check_type(schema: &JsonValue, value: &JsonValue, path: &str) -> Result<(), String> {
        let types = match &schema["type"] {
            JsonValue::String(name) => vec![name.as_str()],
            JsonValue::Array(names) => names.iter().filter_map(JsonValue::as_str).collect(),
            _ => return Ok(()),
        };
        let actual = match value {
            JsonValue::Null => "null",
            JsonValue::Bool(_) => "boolean",
            JsonValue::Number(number) if number.is_i64() || number.is_u64() => "integer",
            JsonValue::Number(_) => "number",
            JsonValue::String(_) => "string",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        };
        if types.contains(&actual) {
            Ok(())
        } else {
            Err(format!("{path}: expected {types:?}, found {actual}"))
        }
    }

    #[test]
    fn checked_in_specs_conform_to_the_schema_of_their_version()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut documents = Vec::new();
        for entry in std::fs::read_dir(root.join("examples"))? {
            let spec = entry?.path().join("multidb.yaml");
            if spec.is_file() {
                documents.push(spec);
            }
        }
        for entry in std::fs::read_dir(root.join("tests/fixtures/spec-v1"))? {
            let path = entry?.path();
            let yaml = path
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("yaml"));
            let reserialized = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.ends_with(".reserialized"));
            if yaml && !reserialized {
                documents.push(path);
            }
        }
        documents.sort();
        assert!(
            documents
                .iter()
                .any(|path| path.ends_with("examples/custom-profile/multidb.yaml")),
            "{documents:?}"
        );
        assert!(documents.len() >= 11, "{documents:?}");

        let v1 = database_spec_v1_schema();
        let v2 = database_spec_v2_schema();
        let mut versions = Vec::new();
        for path in &documents {
            let value = serde_norway::from_str::<JsonValue>(&std::fs::read_to_string(path)?)?;
            let schema = match value["version"].as_u64() {
                Some(1) => &v1,
                Some(2) => &v2,
                other => panic!("{} declares version {other:?}", path.display()),
            };
            versions.push(value["version"].as_u64());
            conforms(schema, schema, &value, "$")
                .map_err(|error| format!("{}: {error}", path.display()))?;
        }
        assert!(versions.contains(&Some(1)) && versions.contains(&Some(2)));

        // The checker is not vacuous: unknown keys, bad bounds and v2 fields in v1 fail.
        let mut custom = serde_norway::from_str::<JsonValue>(&std::fs::read_to_string(
            root.join("examples/custom-profile/multidb.yaml"),
        )?)?;
        assert!(conforms(&v1, &v1, &custom, "$").is_err());
        custom["custom_profile"]["knobs"]["cache"]["max_entires"] = json!(1);
        assert!(conforms(&v2, &v2, &custom, "$").is_err());
        custom["custom_profile"]["knobs"]["cache"] = json!({ "max_entries": 2_000_000 });
        assert!(conforms(&v2, &v2, &custom, "$").is_err());
        custom["custom_profile"]["knobs"]["cache"] = json!({ "max_entries": 8_192 });
        custom["custom_profile"]["knobs"]["query"] = json!({ "max_concurrent_queries": "Auto" });
        assert!(conforms(&v2, &v2, &custom, "$").is_err());
        custom["custom_profile"]["knobs"]["query"] = json!({ "max_concurrent_queries": "auto" });
        assert!(conforms(&v2, &v2, &custom, "$").is_ok());
        Ok(())
    }

    /// Every object schema below `node` refuses unknown properties.
    fn assert_closed_objects(node: &JsonValue, path: &str) {
        match node {
            JsonValue::Object(map) => {
                if map.get("type") == Some(&json!("object")) {
                    assert_eq!(
                        map.get("additionalProperties"),
                        Some(&json!(false)),
                        "{path}"
                    );
                }
                for (key, child) in map {
                    assert_closed_objects(child, &format!("{path}/{key}"));
                }
            }
            JsonValue::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    assert_closed_objects(child, &format!("{path}/{index}"));
                }
            }
            _ => {}
        }
    }

    #[test]
    fn v2_schema_describes_physical_security_and_provisioning()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = database_spec_v2_schema();
        for name in ["physical", "security", "provisioning"] {
            let definition = &schema["$defs"][name];
            assert_eq!(definition["type"], json!("object"), "{name}");
            assert!(definition.get("required").is_none(), "{name}");
            assert_closed_objects(definition, name);
        }
        assert_eq!(
            schema["properties"]["security"],
            json!({ "$ref": "#/$defs/security" })
        );
        assert_eq!(
            schema["properties"]["provisioning"],
            json!({ "$ref": "#/$defs/provisioning" })
        );
        assert_eq!(
            schema["$defs"]["collection_role"]["properties"]["physical"],
            json!({ "$ref": "#/$defs/physical" })
        );
        let physical = schema["$defs"]["physical"]["properties"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        let documented = serde_json::to_value(crate::config_spec::PhysicalObjectSpec {
            kind: Some(PhysicalKind::Document),
            collection_id: Some(1),
            fields: vec![serde_json::from_value(
                json!({ "name": "a", "path": "a", "type": "str" }),
            )?],
            document_indexes: vec![crate::config_spec::DocIndexSpec {
                path: "a".to_owned(),
                id: None,
            }],
            full_text: vec![crate::config_spec::FullTextSpec {
                path: "a".to_owned(),
                name: None,
                language: None,
                refresh_lag_target: None,
            }],
            geo: vec![crate::config_spec::GeoSpec {
                path: "a".to_owned(),
                name: None,
                precision: None,
            }],
            layout: Some(LayoutKnob::Row),
            columns: vec![crate::config_spec::ColumnSpec {
                name: "a".to_owned(),
                ty: ColumnTypeSpec::Str,
                nullable: true,
            }],
            primary_key: Some("a".to_owned()),
            table_indexes: vec![crate::config_spec::TableIndexSpec {
                column: "a".to_owned(),
                expression: None,
                include: Vec::new(),
                id: None,
            }],
            system_versioning: Some(true),
            keep_history: Some(true),
            dim: Some(1),
            metric: Some(VectorMetricSpec::L2),
            hnsw: Some(crate::config_spec::HnswSpec::default()),
            chunk_millis: Some(1_000),
            retention_millis: Some(1_000),
            max_future_skew_millis: Some(0),
            graph_id: Some(1),
        })?;
        let fields = documented.as_object().cloned().unwrap_or_default();
        assert_eq!(
            physical.keys().collect::<Vec<_>>(),
            fields.keys().collect::<Vec<_>>(),
            "every PhysicalObjectSpec field has a schema property"
        );
        for (field, property) in &physical {
            let description = property["description"].as_str().unwrap_or_default();
            assert!(!description.is_empty(), "{field} has no description");
        }
        assert_eq!(
            schema.pointer("/$defs/security/properties/roles/items/properties/grants/items/oneOf"),
            Some(&json!([{ "required": ["object"] }, { "required": ["resource"] }]))
        );
        assert_eq!(
            schema.pointer(
                "/$defs/security/properties/roles/items/properties/grants/items/properties/resource/enum"
            ),
            Some(&json!(["database"]))
        );
        Ok(())
    }

    /// The provisioning example of `docs/provisioning.md`.
    const PROVISIONING_EXAMPLE: &str = r"
version: 2
name: orders
profile: orders_durable
custom_profile: { extends: high_durability, knobs: { cache: { max_entries: 8192 } } }
deployment: { mode: single_node, storage_path: orders.redb }
defaults: { consistency_domain: primary, replication: cp }
guarantees:
  write_ack: quorum
  conflict_resolution: none
  backup: { enabled: true, pitr: true }
  encryption: { at_rest: false }
  audit: { enabled: true }
  sensitive_data: false
  strict_cross_domain_transactions: false
domains: [ { name: primary, mode: strong_cp } ]
collections:
  - name: orders
    role: document_entity
    domain: primary
    indexes: [primary, document, full_text, geo]
    physical:
      fields: [ { name: customer_id, path: customer.id, type: str }, { name: total, path: total, type: float } ]
      document_indexes: [ { path: customer.id }, { path: status } ]
      full_text: [ { path: notes } ]
      geo: [ { path: shipping.location, precision: 7 } ]
  - name: ledger
    role: event_log
    domain: primary
    indexes: [primary, document]
    physical:
      kind: table
      columns: [ { name: entry_id, type: int }, { name: order_id, type: str }, { name: amount, type: float } ]
      primary_key: entry_id
      table_indexes: [ { column: order_id } ]
      system_versioning: true
  - name: memories
    role: vector_memory
    domain: primary
    indexes: [vector]
    physical: { dim: 1536, metric: cosine }
  - name: daily_rollups
    role: analytics
    domain: primary
    indexes: [columnar]
    physical: { columns: [ { name: id, type: str }, { name: day, type: str }, { name: revenue, type: float } ], primary_key: id }
security:
  roles:
    - name: order_writer
      grants:
        - { object: orders, permissions: [read, write] }
        - { object: orders_notes_fts, permissions: [read] }
        - { object: ledger, permissions: [read, write] }
  principals: [ { user: orders_svc, roles: [order_writer] } ]
provisioning: { unmanaged: report }
extensions:
  - { name: audit, version: 1.0.0, stability: stable }
  - { name: cdc, version: 1.0.0, stability: stable }
  - { name: document_index, version: 1.0.0, stability: stable }
  - { name: full_text, version: 1.0.0, stability: stable }
  - { name: vector_hnsw, version: 1.0.0, stability: stable }
  - { name: columnar_layout, version: 1.0.0, stability: experimental }
overrides: {}
operation_hints: {}
";

    #[test]
    fn provisioning_example_conforms_and_validates() -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_norway::from_str::<JsonValue>(PROVISIONING_EXAMPLE)?;
        let schema = database_spec_v2_schema();
        conforms(&schema, &schema, &value, "$")?;
        let spec = decode_spec_bytes(PROVISIONING_EXAMPLE.as_bytes(), SpecFormat::Yaml)?;

        let report = GuaranteeValidator::validate(&spec);

        assert!(report.valid, "{:#?}", report.issues);
        assert!(
            report
                .issues
                .iter()
                .all(|issue| issue.severity != ValidationSeverity::Error
                    && !issue.code.starts_with("PHYSICAL_")
                    && !issue.code.starts_with("SECURITY_")),
            "{:#?}",
            report.issues
        );

        // The checker covers the new keywords: oneOf with siblings, lengths and item counts.
        let mut both = value.clone();
        both["security"]["roles"][0]["grants"][0]["resource"] = json!("database");
        assert!(conforms(&schema, &schema, &both, "$").is_err());
        let mut neither = value.clone();
        neither["security"]["roles"][0]["grants"][0] = json!({ "permissions": ["read"] });
        assert!(conforms(&schema, &schema, &neither, "$").is_err());
        let mut unknown = value.clone();
        unknown["security"]["roles"][0]["grants"][0]["scope"] = json!("system");
        assert!(conforms(&schema, &schema, &unknown, "$").is_err());
        let mut long = value.clone();
        long["collections"][0]["physical"]["full_text"][0]["name"] = json!("n".repeat(129));
        assert!(conforms(&schema, &schema, &long, "$").is_err());
        let mut many = value.clone();
        many["collections"][0]["physical"]["full_text"] =
            JsonValue::Array(vec![json!({ "path": "notes" }); 17]);
        assert!(conforms(&schema, &schema, &many, "$").is_err());
        let mut dim = value;
        dim["collections"][2]["physical"]["dim"] = json!(65_537);
        assert!(conforms(&schema, &schema, &dim, "$").is_err());
        Ok(())
    }
}
