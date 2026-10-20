use serde_json::{Map, Value as JsonValue, json};

use super::{
    ALL_INDEX_KINDS, DATABASE_SPEC_VERSION_V2, KNOB_REGISTRY, KnobType,
    collection_role_definitions, database_spec_v1_schema, describe_value,
};

/// The JSON schema of a version 2 `DatabaseSpec`: the version 1 schema plus `custom_profile`,
/// whose knob bounds are generated from `KNOB_REGISTRY`. The version 1 schema is unchanged.
#[must_use]
pub fn database_spec_v2_schema() -> JsonValue {
    let mut schema = database_spec_v1_schema();
    schema["$id"] = json!("https://multidb.dev/schemas/database-spec-v2.schema.json");
    schema["title"] = json!("MultiDB DatabaseSpec v2");
    schema["properties"]["version"] = json!({ "const": DATABASE_SPEC_VERSION_V2 });
    schema["properties"]["custom_profile"] = json!({ "$ref": "#/$defs/custom_profile" });

    let roles = role_enum();
    schema["$defs"]["collection_role"]["properties"]["role"] = roles.clone();
    schema["$defs"]["collection_role"]["properties"]["indexes"]["items"] = index_enum();
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

fn role_enum() -> JsonValue {
    let roles = collection_role_definitions()
        .iter()
        .map(|definition| JsonValue::from(definition.slug))
        .collect::<Vec<_>>();
    json!({ "type": "string", "enum": roles })
}

fn index_enum() -> JsonValue {
    let kinds = ALL_INDEX_KINDS
        .iter()
        .map(|kind| JsonValue::from(describe_value(kind).trim_matches('"')))
        .collect::<Vec<_>>();
    json!({ "type": "string", "enum": kinds })
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

    use super::database_spec_v2_schema;
    use crate::config_spec::{
        ALL_INDEX_KINDS, CollectionIndexKind, KNOB_REGISTRY, KnobType, database_spec_v1_schema,
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
        }
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
        assert_eq!(v2.pointer(index_path), v1.pointer(index_path));
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
    /// `required`, `properties`, `additionalProperties`, `items`, `enum`, `const`, `oneOf`,
    /// `type` and integer bounds.
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
            return if matching == 1 {
                Ok(())
            } else {
                Err(format!("{path}: {value} matches {matching} oneOf branches"))
            };
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
}
