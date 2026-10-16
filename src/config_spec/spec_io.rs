//! Bounded reading and decoding of `DatabaseSpec` documents.
//!
//! Every spec the CLI and the Control Plane read goes through this module. A spec is at most
//! [`MAX_SPEC_BYTES`]. Decoding first peeks at the document without building it, which reads
//! the version and bounds YAML alias expansion, and only then decodes the typed spec from the
//! original text, so serde errors keep their line and column.

use std::{
    fmt,
    fs::{self, File},
    io::{self, Read},
    ops::RangeInclusive,
    path::Path,
};

use serde::de::{self, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, Visitor};
use serde_json::Value as JsonValue;

use super::{
    CertificationImpact, DatabaseSpec, SUPPORTED_SPEC_VERSIONS, SupportStatus, ValidationReport,
    ValidationSeverity, validation_issue,
};

/// Largest accepted spec document, the same as the Control Plane body limit.
pub const MAX_SPEC_BYTES: usize = 1_048_576;
/// Most YAML nodes the peek visits, counting every alias expansion. An alias-free document has
/// at most about one node per source byte, so only alias expansion can reach this.
pub const MAX_SPEC_NODES: usize = 2 * MAX_SPEC_BYTES;
/// Most scalar bytes the peek visits, counting every alias expansion. Decoded scalars are never
/// longer than their source, so only alias expansion can reach this.
pub const MAX_SPEC_EXPANDED_BYTES: usize = 2 * MAX_SPEC_BYTES;

/// Spec document encodings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpecFormat {
    Yaml,
    Json,
}

#[derive(thiserror::Error, Debug)]
pub enum SpecIoError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("DatabaseSpec is {found} bytes; the limit is {limit} bytes")]
    TooLarge { limit: usize, found: u64 },

    #[error(
        "DatabaseSpec expands to more than {limit} YAML nodes or scalar bytes through anchors \
         and aliases"
    )]
    TooComplex { limit: usize },

    #[error("DatabaseSpec has no usable version: {detail}")]
    MissingVersion { detail: String },

    #[error("unsupported DatabaseSpec version: {found} (supported {supported:?})")]
    UnsupportedVersion {
        found: i128,
        supported: RangeInclusive<u32>,
    },

    #[error("{0}")]
    Yaml(#[from] serde_norway::Error),

    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

impl SpecIoError {
    /// The validation report for size, complexity and version errors, which are reported like
    /// any other invalid spec (CLI exit 2, HTTP 422). `None` for I/O, syntax and decode errors.
    #[must_use]
    pub fn validation_report(&self) -> Option<ValidationReport> {
        let (code, path, suggestion) = match self {
            Self::TooLarge { .. } => (
                "SPEC_TOO_LARGE",
                "$",
                "Keep the DatabaseSpec under 1 MiB; move bulk data out of the spec.",
            ),
            Self::TooComplex { .. } => (
                "SPEC_TOO_COMPLEX",
                "$",
                "Write the repeated values out, or use fewer YAML anchors and aliases.",
            ),
            Self::MissingVersion { .. } => (
                "MISSING_VERSION",
                "version",
                "Start the document with version: 1 or version: 2.",
            ),
            Self::UnsupportedVersion { .. } => (
                "UNSUPPORTED_VERSION",
                "version",
                "Use version 1 or 2, or upgrade multidb to read newer DatabaseSpec versions.",
            ),
            Self::Io(_) | Self::Yaml(_) | Self::Json(_) => return None,
        };
        Some(ValidationReport {
            valid: false,
            status: SupportStatus::Invalid,
            issues: vec![validation_issue(
                code,
                ValidationSeverity::Error,
                path,
                self.to_string(),
                suggestion,
                CertificationImpact::BlocksCertified,
            )],
        })
    }
}

/// `.yaml` and `.yml` (any case) are YAML; every other path is JSON.
#[must_use]
pub fn spec_format_for_path(path: &Path) -> SpecFormat {
    let yaml = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("yaml") || extension.eq_ignore_ascii_case("yml")
        });
    if yaml {
        SpecFormat::Yaml
    } else {
        SpecFormat::Json
    }
}

/// Reads at most [`MAX_SPEC_BYTES`] from `path` and decodes it in the format its extension
/// selects.
pub fn read_spec_file(path: &Path) -> Result<DatabaseSpec, SpecIoError> {
    let limit = u64::try_from(MAX_SPEC_BYTES).unwrap_or(u64::MAX);
    let mut bytes = Vec::new();
    File::open(path)?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SPEC_BYTES {
        // Pipes, FIFOs and procfs files report length 0; the bytes read are a lower bound.
        let read = byte_count(&bytes);
        let found = fs::metadata(path).map_or(read, |meta| meta.len().max(read));
        return Err(SpecIoError::TooLarge {
            limit: MAX_SPEC_BYTES,
            found,
        });
    }
    if std::str::from_utf8(&bytes).is_err() {
        return Err(SpecIoError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )));
    }
    decode_spec_bytes(&bytes, spec_format_for_path(path))
}

/// Decodes a spec document: size check, bounded version peek, then the typed decode.
pub fn decode_spec_bytes(bytes: &[u8], format: SpecFormat) -> Result<DatabaseSpec, SpecIoError> {
    if bytes.len() > MAX_SPEC_BYTES {
        return Err(SpecIoError::TooLarge {
            limit: MAX_SPEC_BYTES,
            found: byte_count(bytes),
        });
    }
    let version = match format {
        SpecFormat::Yaml => peek_yaml(bytes)?,
        SpecFormat::Json => peek_json(bytes)?,
    };
    check_version(version)?;
    match format {
        SpecFormat::Yaml => Ok(serde_norway::from_slice(bytes)?),
        SpecFormat::Json => Ok(serde_json::from_slice(bytes)?),
    }
}

/// Decodes a spec that is already a JSON value (the Control Plane, whose body limit already
/// bounds the size): version check, then the typed decode.
pub fn decode_spec_value(value: JsonValue) -> Result<DatabaseSpec, SpecIoError> {
    let version = match &value {
        JsonValue::Object(object) => match object.get("version") {
            None => VersionField::Missing,
            Some(version) => version
                .as_i64()
                .map(i128::from)
                .or_else(|| version.as_u64().map(i128::from))
                .map_or(VersionField::NotInteger, VersionField::Integer),
        },
        _ => VersionField::NotMapping,
    };
    check_version(version)?;
    Ok(serde_json::from_value(value)?)
}

fn byte_count(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

/// The top-level `version` field as the peek saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VersionField {
    NotMapping,
    Missing,
    NotInteger,
    Integer(i128),
}

fn check_version(version: VersionField) -> Result<(), SpecIoError> {
    let detail = match version {
        VersionField::Integer(found) => {
            let supported = SUPPORTED_SPEC_VERSIONS;
            let known = u32::try_from(found).is_ok_and(|found| supported.contains(&found));
            return if known {
                Ok(())
            } else {
                Err(SpecIoError::UnsupportedVersion { found, supported })
            };
        }
        VersionField::NotMapping => "the document root must be a mapping with a version field",
        VersionField::Missing => "the document has no version field",
        VersionField::NotInteger => "version must be an integer",
    };
    Err(SpecIoError::MissingVersion {
        detail: detail.to_owned(),
    })
}

fn peek_yaml(bytes: &[u8]) -> Result<VersionField, SpecIoError> {
    let mut budget = Budget::default();
    let peeked = Peek {
        budget: &mut budget,
        root: true,
    }
    .deserialize(serde_norway::Deserializer::from_slice(bytes));
    match peeked {
        Ok(node) => Ok(node.version_field()),
        Err(_) if budget.exceeded => Err(SpecIoError::TooComplex {
            limit: MAX_SPEC_NODES,
        }),
        Err(error) => Err(SpecIoError::Yaml(error)),
    }
}

fn peek_json(bytes: &[u8]) -> Result<VersionField, SpecIoError> {
    let mut budget = Budget::default();
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let peeked = Peek {
        budget: &mut budget,
        root: true,
    }
    .deserialize(&mut deserializer)
    .and_then(|node| deserializer.end().map(|()| node));
    match peeked {
        Ok(node) => Ok(node.version_field()),
        Err(_) if budget.exceeded => Err(SpecIoError::TooComplex {
            limit: MAX_SPEC_NODES,
        }),
        Err(error) => Err(SpecIoError::Json(error)),
    }
}

/// Nodes and scalar bytes the peek has visited so far.
#[derive(Debug, Default)]
struct Budget {
    nodes: usize,
    bytes: usize,
    exceeded: bool,
}

impl Budget {
    fn charge<E: de::Error>(&mut self, scalar_bytes: usize) -> Result<(), E> {
        self.nodes = self.nodes.saturating_add(1);
        self.bytes = self.bytes.saturating_add(scalar_bytes);
        if self.nodes > MAX_SPEC_NODES || self.bytes > MAX_SPEC_EXPANDED_BYTES {
            self.exceeded = true;
            return Err(E::custom("DatabaseSpec expansion budget exceeded"));
        }
        Ok(())
    }
}

/// What the peek remembers about one node: nothing but integers, the key `version` and, for
/// the root mapping, its version field.
#[derive(Clone, Copy, Debug)]
enum PeekedNode {
    Integer(i128),
    VersionKey,
    Root(VersionField),
    Other,
}

impl PeekedNode {
    const fn version_field(self) -> VersionField {
        match self {
            Self::Root(version) => version,
            Self::Integer(_) | Self::VersionKey | Self::Other => VersionField::NotMapping,
        }
    }
}

/// A count-only visitor in the shape of `serde::de::IgnoredAny`, except that it visits nested
/// values through `deserialize_any`, so YAML aliases are expanded exactly as a typed decode
/// expands them, and every visited node and scalar byte is charged to the budget.
struct Peek<'a> {
    budget: &'a mut Budget,
    root: bool,
}

impl Peek<'_> {
    fn nested(&mut self) -> Peek<'_> {
        Peek {
            budget: &mut *self.budget,
            root: false,
        }
    }
}

impl<'de> DeserializeSeed<'de> for Peek<'_> {
    type Value = PeekedNode;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<PeekedNode, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Peek<'_> {
    type Value = PeekedNode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DatabaseSpec document")
    }

    fn visit_bool<E: de::Error>(self, _value: bool) -> Result<PeekedNode, E> {
        self.budget.charge(0)?;
        Ok(PeekedNode::Other)
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<PeekedNode, E> {
        self.visit_i128(i128::from(value))
    }

    fn visit_i128<E: de::Error>(self, value: i128) -> Result<PeekedNode, E> {
        self.budget.charge(0)?;
        Ok(PeekedNode::Integer(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<PeekedNode, E> {
        self.visit_i128(i128::from(value))
    }

    fn visit_u128<E: de::Error>(self, value: u128) -> Result<PeekedNode, E> {
        self.visit_i128(i128::try_from(value).unwrap_or(i128::MAX))
    }

    fn visit_f64<E: de::Error>(self, _value: f64) -> Result<PeekedNode, E> {
        self.budget.charge(0)?;
        Ok(PeekedNode::Other)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<PeekedNode, E> {
        self.budget.charge(value.len())?;
        Ok(if value == "version" {
            PeekedNode::VersionKey
        } else {
            PeekedNode::Other
        })
    }

    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<PeekedNode, E> {
        self.budget.charge(value.len())?;
        Ok(PeekedNode::Other)
    }

    fn visit_none<E: de::Error>(self) -> Result<PeekedNode, E> {
        self.budget.charge(0)?;
        Ok(PeekedNode::Other)
    }

    fn visit_unit<E: de::Error>(self) -> Result<PeekedNode, E> {
        self.budget.charge(0)?;
        Ok(PeekedNode::Other)
    }

    fn visit_some<D: Deserializer<'de>>(mut self, deserializer: D) -> Result<PeekedNode, D::Error> {
        self.budget.charge(0)?;
        self.nested().deserialize(deserializer)?;
        Ok(PeekedNode::Other)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        mut self,
        deserializer: D,
    ) -> Result<PeekedNode, D::Error> {
        self.budget.charge(0)?;
        self.nested().deserialize(deserializer)?;
        Ok(PeekedNode::Other)
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<PeekedNode, A::Error> {
        self.budget.charge(0)?;
        while seq.next_element_seed(self.nested())?.is_some() {}
        Ok(PeekedNode::Other)
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<PeekedNode, A::Error> {
        self.budget.charge(0)?;
        let mut version = VersionField::Missing;
        while let Some(key) = map.next_key_seed(self.nested())? {
            let value = map.next_value_seed(self.nested())?;
            if self.root
                && matches!(key, PeekedNode::VersionKey)
                && version == VersionField::Missing
            {
                version = match value {
                    PeekedNode::Integer(found) => VersionField::Integer(found),
                    PeekedNode::VersionKey | PeekedNode::Root(_) | PeekedNode::Other => {
                        VersionField::NotInteger
                    }
                };
            }
        }
        Ok(if self.root {
            PeekedNode::Root(version)
        } else {
            PeekedNode::Other
        })
    }

    fn visit_enum<A: EnumAccess<'de>>(mut self, data: A) -> Result<PeekedNode, A::Error> {
        self.budget.charge(0)?;
        let (_, variant) = data.variant_seed(self.nested())?;
        de::VariantAccess::newtype_variant_seed(variant, self.nested())?;
        Ok(PeekedNode::Other)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use serde_json::json;

    use super::{
        MAX_SPEC_BYTES, SpecFormat, SpecIoError, decode_spec_bytes, decode_spec_value,
        read_spec_file, spec_format_for_path,
    };
    use crate::{
        config_spec::{DatabaseSpec, GuaranteeValidator, ValidationSeverity},
        db::{DbConfig, Profile},
    };

    fn base_spec() -> DatabaseSpec {
        DatabaseSpec::from_db_config(
            "orders",
            &DbConfig::on_disk(Profile::Transactional, "orders.redb"),
        )
    }

    fn report_code(error: &SpecIoError) -> Option<String> {
        error
            .validation_report()
            .and_then(|report| report.issues.first().map(|issue| issue.code.clone()))
    }

    /// A YAML document that is a valid spec except for `extra`, which follows `version` and
    /// carries the payload under test.
    fn yaml_with(extra: &str) -> String {
        format!("version: 1\nextra: {extra}\n")
    }

    #[test]
    fn spec_format_follows_the_extension() {
        assert_eq!(
            spec_format_for_path("a/spec.yaml".as_ref()),
            SpecFormat::Yaml
        );
        assert_eq!(spec_format_for_path("spec.YML".as_ref()), SpecFormat::Yaml);
        assert_eq!(spec_format_for_path("spec.json".as_ref()), SpecFormat::Json);
        assert_eq!(spec_format_for_path("spec".as_ref()), SpecFormat::Json);
    }

    #[test]
    fn oversized_specs_are_rejected_before_parsing() -> Result<(), Box<dyn std::error::Error>> {
        // Not a parseable document at all: the size check must come first.
        let bytes = vec![b'{'; MAX_SPEC_BYTES + 1];
        let error = decode_spec_bytes(&bytes, SpecFormat::Json)
            .err()
            .ok_or("oversized bytes must fail")?;
        assert!(
            matches!(
                error,
                SpecIoError::TooLarge {
                    limit: MAX_SPEC_BYTES,
                    found
                } if found == u64::try_from(MAX_SPEC_BYTES + 1)?
            ),
            "{error:?}"
        );
        assert_eq!(report_code(&error).as_deref(), Some("SPEC_TOO_LARGE"));

        let temp = tempfile::tempdir()?;
        let path = temp.path().join("huge.yaml");
        std::fs::write(&path, vec![b'#'; MAX_SPEC_BYTES + 10])?;
        let error = read_spec_file(&path)
            .err()
            .ok_or("oversized file must fail")?;
        assert!(
            matches!(error, SpecIoError::TooLarge { found, .. } if found == u64::try_from(MAX_SPEC_BYTES + 10)?),
            "{error:?}"
        );

        let exact = temp.path().join("exact.json");
        std::fs::write(&exact, vec![b' '; MAX_SPEC_BYTES])?;
        let error = read_spec_file(&exact).err().ok_or("blank JSON must fail")?;
        assert!(matches!(error, SpecIoError::Json(_)), "{error:?}");
        Ok(())
    }

    #[test]
    fn missing_and_non_integer_versions_are_reported() -> Result<(), Box<dyn std::error::Error>> {
        let mut without = serde_json::to_value(base_spec())?;
        without
            .as_object_mut()
            .ok_or("spec must be an object")?
            .remove("version");
        let mut as_string = serde_json::to_value(base_spec())?;
        as_string["version"] = json!("1");

        for value in [without, as_string, json!(["version", 1]), json!(1)] {
            let bytes = serde_json::to_vec(&value)?;
            for error in [
                decode_spec_value(value.clone())
                    .err()
                    .ok_or("value must fail")?,
                decode_spec_bytes(&bytes, SpecFormat::Json)
                    .err()
                    .ok_or("JSON must fail")?,
            ] {
                assert!(
                    matches!(error, SpecIoError::MissingVersion { .. }),
                    "{error:?}"
                );
                let report = error.validation_report().ok_or("missing report")?;
                assert!(!report.valid);
                assert_eq!(report.issues[0].code, "MISSING_VERSION");
                assert_eq!(report.issues[0].path, "version");
                assert_eq!(report.issues[0].severity, ValidationSeverity::Error);
            }
        }

        for yaml in ["version: \"1\"\nname: x\n", "- version\n- 1\n", "name: x\n"] {
            let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
                .err()
                .ok_or("YAML must fail")?;
            assert_eq!(report_code(&error).as_deref(), Some("MISSING_VERSION"));
        }
        Ok(())
    }

    #[test]
    fn unsupported_versions_are_reported_verbatim() -> Result<(), Box<dyn std::error::Error>> {
        for (found, yaml) in [
            (3, "version: 3\n"),
            (-1, "version: -1\n"),
            (4_294_967_296, "version: 4294967296\n"),
        ] {
            let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
                .err()
                .ok_or("version must fail")?;
            assert!(
                matches!(error, SpecIoError::UnsupportedVersion { found: seen, ref supported } if seen == found && *supported == (1..=2)),
                "{error:?}"
            );
            let report = error.validation_report().ok_or("missing report")?;
            assert_eq!(report.issues[0].code, "UNSUPPORTED_VERSION");
            assert!(report.issues[0].message.contains("(supported 1..=2)"));
        }

        let mut value = serde_json::to_value(base_spec())?;
        value["version"] = json!(3);
        let error = decode_spec_value(value)
            .err()
            .ok_or("version 3 must fail")?;
        assert_eq!(report_code(&error).as_deref(), Some("UNSUPPORTED_VERSION"));
        Ok(())
    }

    #[test]
    fn yaml_and_json_decode_to_the_same_spec() -> Result<(), Box<dyn std::error::Error>> {
        let mut spec = base_spec();
        spec.version = 2;
        spec.profile = "orders_durable".to_owned();
        spec.custom_profile = Some(serde_json::from_value(json!({
            "extends": "high_durability",
            "knobs": { "cache": { "max_entries": 8192 } }
        }))?);

        let from_yaml =
            decode_spec_bytes(serde_norway::to_string(&spec)?.as_bytes(), SpecFormat::Yaml)?;
        let from_json = decode_spec_bytes(&serde_json::to_vec(&spec)?, SpecFormat::Json)?;
        let from_value = decode_spec_value(serde_json::to_value(&spec)?)?;

        assert_eq!(from_yaml, spec);
        assert_eq!(from_json, spec);
        assert_eq!(from_value, spec);
        Ok(())
    }

    #[test]
    fn v2_fields_in_a_v1_document_are_rejected_at_their_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut spec = base_spec();
        spec.profile = "orders_durable".to_owned();
        spec.custom_profile = Some(serde_json::from_value(json!({ "extends": "secure_app" }))?);

        let decoded =
            decode_spec_bytes(serde_norway::to_string(&spec)?.as_bytes(), SpecFormat::Yaml)?;
        let report = GuaranteeValidator::validate(&decoded);

        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| {
            issue.code == "FIELD_REQUIRES_SPEC_V2" && issue.path == "custom_profile"
        }));
        Ok(())
    }

    #[test]
    fn typed_decode_errors_keep_their_yaml_position() -> Result<(), Box<dyn std::error::Error>> {
        let mut yaml = serde_norway::to_string(&base_spec())?;
        yaml.push_str("unexpected: true\n");

        let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
            .err()
            .ok_or("unknown field must fail")?;

        assert!(matches!(error, SpecIoError::Yaml(_)), "{error:?}");
        assert!(error.validation_report().is_none());
        let message = error.to_string();
        assert!(message.contains("unexpected"), "{message}");
        assert!(message.contains("line"), "{message}");
        Ok(())
    }

    #[test]
    fn nested_alias_bombs_hit_the_yaml_repetition_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut yaml =
            String::from("version: 1\na0: &a0 [lol, lol, lol, lol, lol, lol, lol, lol, lol]\n");
        for level in 1..10 {
            let previous = level - 1;
            writeln!(
                yaml,
                "a{level}: &a{level} [*a{previous}, *a{previous}, *a{previous}, *a{previous}, \
                 *a{previous}, *a{previous}, *a{previous}, *a{previous}, *a{previous}]"
            )?;
        }

        let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
            .err()
            .ok_or("alias bomb must fail")?;

        assert!(matches!(error, SpecIoError::Yaml(_)), "{error:?}");
        assert!(
            error.to_string().contains("repetition limit exceeded"),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn alias_fan_out_below_the_repetition_limit_hits_the_node_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        // 50 000 anchored items referenced 100 times: few alias jumps, 5 million nodes.
        let items = vec!["0"; 50_000].join(",");
        let aliases = vec!["*big"; 100].join(", ");
        let yaml = yaml_with(&format!("{{big: &big [{items}], copies: [{aliases}]}}"));
        assert!(yaml.len() < MAX_SPEC_BYTES);

        let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
            .err()
            .ok_or("alias fan-out must fail")?;

        assert!(matches!(error, SpecIoError::TooComplex { .. }), "{error:?}");
        assert_eq!(report_code(&error).as_deref(), Some("SPEC_TOO_COMPLEX"));
        Ok(())
    }

    #[test]
    fn aliased_large_scalars_hit_the_byte_budget() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = "x".repeat(900 * 1024);
        let aliases = vec!["*big"; 4_000].join(", ");
        let yaml = yaml_with(&format!("{{big: &big {scalar}, copies: [{aliases}]}}"));
        assert!(yaml.len() < MAX_SPEC_BYTES);

        let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
            .err()
            .ok_or("aliased scalars must fail")?;

        assert!(matches!(error, SpecIoError::TooComplex { .. }), "{error:?}");
        Ok(())
    }

    #[test]
    fn dense_alias_free_documents_pass_the_peek() -> Result<(), Box<dyn std::error::Error>> {
        // About one node per two source bytes, close to the size limit and without aliases.
        let items = vec!["0"; 500_000].join(",");
        let yaml = yaml_with(&format!("[{items}]"));
        assert!(yaml.len() > MAX_SPEC_BYTES - 64 * 1024 && yaml.len() <= MAX_SPEC_BYTES);

        let error = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)
            .err()
            .ok_or("the extra field must fail the typed decode")?;

        // The peek passes; only the typed decode refuses the unknown field.
        assert!(matches!(error, SpecIoError::Yaml(_)), "{error:?}");
        assert!(error.to_string().contains("extra"), "{error}");

        let mut dense = serde_norway::to_string(&base_spec())?;
        let overrides = (0..40_000)
            .map(|index| format!("k{index}: v"))
            .collect::<Vec<_>>()
            .join(", ");
        dense = dense.replace("overrides: {}", &format!("overrides: {{{overrides}}}"));
        assert!(dense.len() < MAX_SPEC_BYTES);
        let decoded = decode_spec_bytes(dense.as_bytes(), SpecFormat::Yaml)?;
        assert_eq!(decoded.overrides.len(), 40_000);
        Ok(())
    }
}
