use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value as JsonValue};

use super::{
    profile::{FloorOverrides, ProfileKnobOverrides},
    suggest,
};

const KIB: u32 = 1_024;
const MIB: u32 = 1_024 * KIB;
const MIB_U64: u64 = 1_024 * 1_024;
const GIB_U64: u64 = 1_024 * MIB_U64;
const TIB_U64: u64 = 1_024 * GIB_U64;
const PLANNED_WRITE_PIPELINE: KnobEnforcement = KnobEnforcement::Planned {
    tracking: "#11 single write pipeline",
};

/// One profile knob: its value domain, when it takes effect, where the runtime reads it and
/// whether this build enforces it. `KNOB_REGISTRY` is the single source of truth for knobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct KnobDefinition {
    pub path: &'static str,
    pub value_type: KnobType,
    pub timing: KnobTiming,
    pub sink: &'static str,
    pub enforcement: KnobEnforcement,
    pub after_creation: ChangePolicy,
    pub description: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnobType {
    Enum(&'static [&'static str]),
    U32 { min: u32, max: u32 },
    U64 { min: u64, max: u64 },
    I64 { min: i64, max: i64 },
    AutoOrU32 { min: u32, max: u32 },
    LimitU32 { min: u32, max: u32 },
    Bool,
    OptI64 { min: i64, max: i64 },
    RoleSet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnobTiming {
    /// Fixed when the database is created.
    Creation,
    /// Persisted with the database and applied at every open.
    EveryOpen,
    /// Applied by provisioning when it creates objects.
    Provisioning,
    /// A default for generated specs (guided init, templates).
    GenerationDefault,
    /// Checked by spec validation only.
    Validation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnobEnforcement {
    Enforced,
    /// Accepted by the model but not enforced until `tracking` lands; only the preset value is
    /// accepted.
    Planned {
        tracking: &'static str,
    },
    /// Recorded and reported, applied by a later consumer.
    Declarative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangePolicy {
    /// Cannot change after creation.
    Unsupported,
    /// Can change between opens.
    Retune,
    /// Turning it on is a forward-only on-disk format change.
    FormatUpgrade,
    /// Changed by provisioning for objects it creates.
    Provisioning,
    /// Has no runtime state; changing it only changes validation or generated specs.
    NotApplicable,
}

/// A knob name that decodes but has no runtime implementation yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ReservedKnob {
    pub path: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KnobAssignment {
    pub path: &'static str,
    /// The value in its JSON form, as it appears under `custom_profile.knobs` or
    /// `custom_profile.floors`; `null` means inherit.
    pub value: JsonValue,
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum KnobAssignmentError {
    #[error("knob assignment {input} must have the form path=value")]
    MissingValue { input: String },

    #[error(
        "unknown profile knob {path}{}",
        .suggestion.map_or_else(String::new, |candidate| format!("; did you mean {candidate}?"))
    )]
    UnknownKnob {
        path: String,
        suggestion: Option<&'static str>,
    },

    #[error("profile knob {path} is not supported by this runtime ({reason})")]
    Reserved {
        path: &'static str,
        reason: &'static str,
    },

    #[error(
        "{path} is a role set, not a single value; edit custom_profile.compatible_roles.add or \
         custom_profile.compatible_roles.remove in the spec"
    )]
    RoleSet { path: &'static str },

    #[error("invalid value {value} for {path}: expected {expected}")]
    InvalidValue {
        path: &'static str,
        value: String,
        expected: String,
    },
}

pub const KNOB_REGISTRY: &[KnobDefinition] = &[
    KnobDefinition {
        path: "storage.medium",
        value_type: KnobType::Enum(&["durable", "volatile"]),
        timing: KnobTiming::Creation,
        sink: "__meta.profile -> engine_for (memory or redb engine)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Unsupported,
        description: "Whether data lives in a durable redb file or only in memory.",
    },
    KnobDefinition {
        path: "storage.layout",
        value_type: KnobType::Enum(&["row", "columnar"]),
        timing: KnobTiming::Creation,
        sink: "__meta.layout / layout_for (default layout for new tables)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Unsupported,
        description: "Default table layout of the database.",
    },
    KnobDefinition {
        path: "durability.commit",
        value_type: KnobType::Enum(&["immediate", "two_phase"]),
        timing: KnobTiming::Creation,
        sink: "redb two-phase commit (storage profile HighDurability)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Unsupported,
        description: "Commit protocol of every redb write transaction.",
    },
    KnobDefinition {
        path: "durability.group_commit_window_ms",
        value_type: KnobType::U32 { min: 0, max: 50 },
        timing: KnobTiming::EveryOpen,
        sink: "IoConfig.group_commit_window_ms -> SingleNode::with_group_commit_window",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "How long single-node commits wait to be batched together.",
    },
    KnobDefinition {
        path: "compression.algorithm",
        value_type: KnobType::Enum(&["none", "lz4", "zstd"]),
        timing: KnobTiming::Creation,
        sink: "CompressionConfig.algorithm -> CompressedEngine value framing",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::FormatUpgrade,
        description: "Value compression; anything but none turns on value framing \
                      (schema_version 2). Switching lz4 and zstd later is a retune.",
    },
    KnobDefinition {
        path: "compression.min_bytes",
        value_type: KnobType::U32 { min: 64, max: MIB },
        timing: KnobTiming::EveryOpen,
        sink: "CompressionConfig.min_bytes",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Values shorter than this are stored in a raw frame.",
    },
    KnobDefinition {
        path: "compression.zstd_level",
        value_type: KnobType::U32 { min: 1, max: 19 },
        timing: KnobTiming::EveryOpen,
        sink: "CompressionConfig.zstd_level",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "zstd compression level; only used with algorithm zstd.",
    },
    KnobDefinition {
        path: "cache.enabled",
        value_type: KnobType::Bool,
        timing: KnobTiming::EveryOpen,
        sink: "CacheConfig.enabled -> PerformanceCache::new",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Whether the read cache is on.",
    },
    KnobDefinition {
        path: "cache.max_entries",
        value_type: KnobType::U32 { min: 0, max: MIB },
        timing: KnobTiming::EveryOpen,
        sink: "CacheConfig.max_capacity -> PerformanceCache::new",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Maximum number of cached values.",
    },
    KnobDefinition {
        path: "query.max_concurrent_queries",
        value_type: KnobType::AutoOrU32 { min: 1, max: 4_096 },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.max_concurrent_queries -> query semaphore",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Query permits; auto is four per available thread.",
    },
    KnobDefinition {
        path: "query.queue_timeout_ms",
        value_type: KnobType::U32 {
            min: 1,
            max: 60_000,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.queue_timeout_ms -> acquire_query_permit",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "How long a query waits for a permit before it is rejected.",
    },
    KnobDefinition {
        path: "query.timeout_ms",
        value_type: KnobType::U32 {
            min: 100,
            max: 3_600_000,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.timeout_ms -> SQL execution timeout",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "SQL execution timeout.",
    },
    KnobDefinition {
        path: "query.memory_limit_bytes",
        value_type: KnobType::U64 {
            min: 64 * MIB_U64,
            max: 64 * GIB_U64,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.memory_limit_bytes -> DataFusion memory pool",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Memory pool of one SQL query.",
    },
    KnobDefinition {
        path: "query.spill_limit_bytes",
        value_type: KnobType::U64 {
            min: 0,
            max: TIB_U64,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.spill_limit_bytes -> DataFusion temp directory limit",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Maximum spill-to-disk size of one SQL query.",
    },
    KnobDefinition {
        path: "query.target_partitions",
        value_type: KnobType::AutoOrU32 { min: 1, max: 256 },
        timing: KnobTiming::EveryOpen,
        sink: "ParallelismConfig.target_partitions -> DataFusion target partitions",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "DataFusion partitions; auto is the storage profile default.",
    },
    KnobDefinition {
        path: "query.batch_rows",
        value_type: KnobType::U32 { min: 1, max: MIB },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.batch_rows -> DataFusion batch size",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Rows per record batch.",
    },
    KnobDefinition {
        path: "query.max_sql_bytes",
        value_type: KnobType::U32 {
            min: KIB,
            max: 16 * MIB,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.max_sql_bytes -> SQL parser limits",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Maximum SQL text size.",
    },
    KnobDefinition {
        path: "query.max_statements",
        value_type: KnobType::U32 { min: 1, max: 1_024 },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.max_statements -> SQL parser limits",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Maximum statements per SQL request.",
    },
    KnobDefinition {
        path: "query.max_values_rows",
        value_type: KnobType::U32 {
            min: 1,
            max: 1_000_000,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.max_values_rows -> SQL parser limits",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "Maximum rows in one VALUES list.",
    },
    KnobDefinition {
        path: "query.parser_recursion_limit",
        value_type: KnobType::U32 {
            min: 16,
            max: 1_024,
        },
        timing: KnobTiming::EveryOpen,
        sink: "QueryExecutionConfig.parser_recursion_limit -> SQL parser limits",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::Retune,
        description: "SQL parser recursion depth.",
    },
    KnobDefinition {
        path: "limits.max_value_bytes",
        value_type: KnobType::LimitU32 {
            min: KIB,
            max: 64 * MIB,
        },
        timing: KnobTiming::EveryOpen,
        sink: "LimitPolicy.max_value_bytes (no user write path checks it yet)",
        enforcement: PLANNED_WRITE_PIPELINE,
        after_creation: ChangePolicy::Provisioning,
        description: "Largest accepted user value; only unlimited is accepted until #11.",
    },
    KnobDefinition {
        path: "limits.max_batch_ops",
        value_type: KnobType::LimitU32 {
            min: 1,
            max: 1_000_000,
        },
        timing: KnobTiming::EveryOpen,
        sink: "LimitPolicy.max_batch_ops (no user write path checks it yet)",
        enforcement: PLANNED_WRITE_PIPELINE,
        after_creation: ChangePolicy::Provisioning,
        description: "Largest accepted write batch; only unlimited is accepted until #11.",
    },
    KnobDefinition {
        path: "history.system_versioning",
        value_type: KnobType::Enum(&["off", "typed_tables"]),
        timing: KnobTiming::Provisioning,
        sink: "enable_system_versioning on typed tables that provisioning creates",
        enforcement: KnobEnforcement::Declarative,
        after_creation: ChangePolicy::Provisioning,
        description: "Whether provisioned typed tables keep system-versioned history.",
    },
    KnobDefinition {
        path: "history.keep_history",
        value_type: KnobType::Bool,
        timing: KnobTiming::Provisioning,
        sink: "TemporalRetention.keep_history of provisioned typed tables",
        enforcement: KnobEnforcement::Declarative,
        after_creation: ChangePolicy::Provisioning,
        description: "Whether system-versioned history is kept.",
    },
    KnobDefinition {
        path: "time_series.default_chunk_millis",
        value_type: KnobType::I64 {
            min: 1_000,
            max: 2_678_400_000,
        },
        timing: KnobTiming::Provisioning,
        sink: "TimeSeriesConfig.chunk_millis default of provisioned series",
        enforcement: KnobEnforcement::Declarative,
        after_creation: ChangePolicy::Provisioning,
        description: "Default chunk width of time series.",
    },
    KnobDefinition {
        path: "time_series.default_retention_millis",
        value_type: KnobType::OptI64 {
            min: 1,
            max: i64::MAX,
        },
        timing: KnobTiming::Provisioning,
        sink: "TimeSeriesConfig.retention_millis default of provisioned series",
        enforcement: KnobEnforcement::Declarative,
        after_creation: ChangePolicy::Provisioning,
        description: "Default retention of time series; null keeps data forever and a value \
                      must be at least one chunk.",
    },
    KnobDefinition {
        path: "consistency.default_domain",
        value_type: KnobType::Enum(&["local_snapshot", "strong_cp", "eventual_ap"]),
        timing: KnobTiming::GenerationDefault,
        sink: "guided init, templates and domain advice",
        enforcement: KnobEnforcement::Declarative,
        after_creation: ChangePolicy::NotApplicable,
        description: "Consistency domain that generated specs start from.",
    },
    KnobDefinition {
        path: "floors.audit",
        value_type: KnobType::Bool,
        timing: KnobTiming::Validation,
        sink: "GuaranteeValidator GUARANTEE_BELOW_PROFILE_FLOOR (guarantees.audit.enabled)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::NotApplicable,
        description: "Requires guarantees.audit.enabled; may only be raised.",
    },
    KnobDefinition {
        path: "floors.backup",
        value_type: KnobType::Bool,
        timing: KnobTiming::Validation,
        sink: "GuaranteeValidator GUARANTEE_BELOW_PROFILE_FLOOR or PRODUCTION_BACKUP_DISABLED \
               (guarantees.backup.enabled)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::NotApplicable,
        description: "Requires guarantees.backup.enabled; may only be raised.",
    },
    KnobDefinition {
        path: "floors.pitr",
        value_type: KnobType::Bool,
        timing: KnobTiming::Validation,
        sink: "GuaranteeValidator GUARANTEE_BELOW_PROFILE_FLOOR (guarantees.backup.pitr)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::NotApplicable,
        description: "Requires guarantees.backup.pitr; may only be raised.",
    },
    KnobDefinition {
        path: "floors.encryption_at_rest",
        value_type: KnobType::Bool,
        timing: KnobTiming::Validation,
        sink: "GuaranteeValidator GUARANTEE_BELOW_PROFILE_FLOOR (guarantees.encryption.at_rest)",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::NotApplicable,
        description: "Requires guarantees.encryption.at_rest; may only be raised.",
    },
    KnobDefinition {
        path: "compatible_roles",
        value_type: KnobType::RoleSet,
        timing: KnobTiming::Validation,
        sink: "GuaranteeValidator ROLE_OUTSIDE_PROFILE_CERTIFICATION",
        enforcement: KnobEnforcement::Enforced,
        after_creation: ChangePolicy::NotApplicable,
        description: "Collection roles the profile supports; changed with add and remove.",
    },
];

pub const RESERVED_KNOBS: &[ReservedKnob] = &[
    ReservedKnob {
        path: "history.mvcc_retention_millis",
        reason: "waits for MVCC and commit-log GC (#12)",
    },
    ReservedKnob {
        path: "cache.ttl_millis",
        reason: "no runtime implementation",
    },
];

impl KnobType {
    /// The accepted values in a short human form, for example `auto | 1..=4096`.
    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::Enum(values) => values.join(" | "),
            Self::U32 { min, max } => format!("{min}..={max}"),
            Self::U64 { min, max } => format!("{min}..={max}"),
            Self::I64 { min, max } => format!("{min}..={max}"),
            Self::AutoOrU32 { min, max } => format!("auto | {min}..={max}"),
            Self::LimitU32 { min, max } => format!("unlimited | {min}..={max}"),
            Self::Bool => "true | false".to_owned(),
            Self::OptI64 { min, max } => format!("null | {min}..={max}"),
            Self::RoleSet => "a set of collection roles".to_owned(),
        }
    }

    /// Whether a serialized knob leaf lies inside the bounds. `None` when the type has no bounds
    /// or the leaf is a keyword (`auto`, `unlimited`, `null`).
    pub(crate) fn contains(self, leaf: &JsonValue) -> Option<bool> {
        match self {
            Self::U32 { min, max } | Self::AutoOrU32 { min, max } | Self::LimitU32 { min, max } => {
                leaf.as_u64()
                    .map(|value| (u64::from(min)..=u64::from(max)).contains(&value))
            }
            Self::U64 { min, max } => leaf.as_u64().map(|value| (min..=max).contains(&value)),
            Self::I64 { min, max } | Self::OptI64 { min, max } => {
                leaf.as_i64().map(|value| (min..=max).contains(&value))
            }
            Self::Enum(_) | Self::Bool | Self::RoleSet => None,
        }
    }
}

impl KnobAssignment {
    /// Writes the value into the matching override set: `floors.*` into `floors`, every other
    /// knob into `knobs`.
    pub fn apply(
        &self,
        knobs: &mut ProfileKnobOverrides,
        floors: &mut FloorOverrides,
    ) -> Result<(), KnobAssignmentError> {
        match self.path.split_once('.') {
            Some(("floors", leaf)) => *floors = self.with_leaf(&*floors, None, leaf)?,
            Some((section, leaf)) => *knobs = self.with_leaf(&*knobs, Some(section), leaf)?,
            None => return Err(KnobAssignmentError::RoleSet { path: self.path }),
        }
        Ok(())
    }

    fn with_leaf<T: Serialize + DeserializeOwned>(
        &self,
        target: &T,
        section: Option<&str>,
        leaf: &str,
    ) -> Result<T, KnobAssignmentError> {
        let invalid = |reason: String| KnobAssignmentError::InvalidValue {
            path: self.path,
            value: self.value.to_string(),
            expected: reason,
        };
        let mut document =
            serde_json::to_value(target).map_err(|error| invalid(error.to_string()))?;
        let mut object = document
            .as_object_mut()
            .ok_or_else(|| invalid("an object of overrides".to_owned()))?;
        if let Some(section) = section {
            object = object
                .entry(section.to_owned())
                .or_insert_with(|| JsonValue::Object(Map::new()))
                .as_object_mut()
                .ok_or_else(|| invalid("an object of overrides".to_owned()))?;
        }
        object.insert(leaf.to_owned(), self.value.clone());
        serde_json::from_value(document).map_err(|error| invalid(error.to_string()))
    }
}

/// Looks up a knob by its registry path, for example `cache.max_entries`.
#[must_use]
pub fn knob_definition(path: &str) -> Option<&'static KnobDefinition> {
    KNOB_REGISTRY
        .iter()
        .find(|definition| definition.path == path)
}

pub(crate) fn reserved_knob(path: &str) -> Option<&'static ReservedKnob> {
    RESERVED_KNOBS.iter().find(|knob| knob.path == path)
}

/// Parses `path=value`, for example `cache.max_entries=8192`, against `KNOB_REGISTRY`.
///
/// Values are checked for type and representability; ranges are checked by the resolver.
pub fn parse_knob_assignment(input: &str) -> Result<KnobAssignment, KnobAssignmentError> {
    let Some((path, raw)) = input.split_once('=') else {
        return Err(KnobAssignmentError::MissingValue {
            input: input.to_owned(),
        });
    };
    let path = path.trim();
    let raw = raw.trim();
    if let Some(reserved) = reserved_knob(path) {
        return Err(KnobAssignmentError::Reserved {
            path: reserved.path,
            reason: reserved.reason,
        });
    }
    let Some(definition) = knob_definition(path) else {
        return Err(KnobAssignmentError::UnknownKnob {
            path: path.to_owned(),
            suggestion: suggest::closest(
                path,
                KNOB_REGISTRY.iter().map(|definition| definition.path),
                2,
            ),
        });
    };

    let assignment = KnobAssignment {
        path: definition.path,
        value: parse_knob_value(definition, raw)?,
    };
    // Prove the value fits the override field (for example a u16 knob) before returning it.
    assignment.apply(
        &mut ProfileKnobOverrides::default(),
        &mut FloorOverrides::default(),
    )?;
    Ok(assignment)
}

fn parse_knob_value(
    definition: &KnobDefinition,
    raw: &str,
) -> Result<JsonValue, KnobAssignmentError> {
    let invalid = || KnobAssignmentError::InvalidValue {
        path: definition.path,
        value: raw.to_owned(),
        expected: definition.value_type.describe(),
    };
    let unsigned = || {
        raw.parse::<u64>()
            .map(JsonValue::from)
            .map_err(|_| invalid())
    };
    let signed = || {
        raw.parse::<i64>()
            .map(JsonValue::from)
            .map_err(|_| invalid())
    };
    match definition.value_type {
        KnobType::Enum(values) => {
            if values.contains(&raw) {
                Ok(JsonValue::from(raw))
            } else {
                Err(invalid())
            }
        }
        KnobType::AutoOrU32 { .. } if raw == "auto" => Ok(JsonValue::from(raw)),
        KnobType::LimitU32 { .. } if raw == "unlimited" => Ok(JsonValue::from(raw)),
        KnobType::OptI64 { .. } if raw == "null" => Ok(JsonValue::Null),
        KnobType::U32 { .. }
        | KnobType::U64 { .. }
        | KnobType::AutoOrU32 { .. }
        | KnobType::LimitU32 { .. } => unsigned(),
        KnobType::I64 { .. } | KnobType::OptI64 { .. } => signed(),
        KnobType::Bool => match raw {
            "true" => Ok(JsonValue::Bool(true)),
            "false" => Ok(JsonValue::Bool(false)),
            _ => Err(invalid()),
        },
        KnobType::RoleSet => Err(KnobAssignmentError::RoleSet {
            path: definition.path,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::Value as JsonValue;

    use super::{
        KNOB_REGISTRY, KnobAssignmentError, KnobEnforcement, KnobTiming, RESERVED_KNOBS,
        knob_definition, parse_knob_assignment,
    };
    use crate::config_spec::{
        AutoOr, CommitMode, FloorOverrides, Limit, ProfileKnobOverrides,
        profile::{lookup_profile_name, preset_knobs},
    };

    fn leaf_paths(prefix: &str, value: &JsonValue, paths: &mut BTreeSet<String>) {
        match value {
            JsonValue::Object(object) => {
                for (key, child) in object {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    leaf_paths(&path, child, paths);
                }
            }
            _ => {
                paths.insert(prefix.to_owned());
            }
        }
    }

    #[test]
    fn registry_matches_the_profile_knob_leaves() -> Result<(), Box<dyn std::error::Error>> {
        let Some((product, variant)) = lookup_profile_name("balanced") else {
            panic!("balanced must resolve");
        };
        let mut leaves = BTreeSet::new();
        leaf_paths(
            "",
            &serde_json::to_value(preset_knobs(product, variant))?,
            &mut leaves,
        );
        let registry = KNOB_REGISTRY
            .iter()
            .map(|definition| definition.path.to_owned())
            .collect::<BTreeSet<_>>();

        assert_eq!(registry, leaves);
        assert_eq!(registry.len(), KNOB_REGISTRY.len());
        Ok(())
    }

    #[test]
    fn registry_paths_are_unique_grouped_and_described() {
        let mut sections = Vec::new();
        let mut paths = BTreeSet::new();
        for definition in KNOB_REGISTRY {
            assert!(
                paths.insert(definition.path),
                "duplicate {}",
                definition.path
            );
            assert!(!definition.sink.trim().is_empty());
            assert!(!definition.description.trim().is_empty());
            let section = definition
                .path
                .split_once('.')
                .map_or(definition.path, |(section, _)| section);
            if sections.last() != Some(&section) {
                assert!(!sections.contains(&section), "section {section} is split");
                sections.push(section);
            }
            if section == "limits" {
                assert!(matches!(
                    definition.enforcement,
                    KnobEnforcement::Planned { .. }
                ));
            }
            if matches!(section, "history" | "time_series") {
                assert_eq!(definition.timing, KnobTiming::Provisioning);
                assert_eq!(definition.enforcement, KnobEnforcement::Declarative);
            }
        }
        for reserved in RESERVED_KNOBS {
            assert!(knob_definition(reserved.path).is_none());
            assert!(!reserved.reason.trim().is_empty());
        }
    }

    #[test]
    fn profiles_doc_lists_every_registry_and_reserved_knob() {
        let doc = include_str!("../../docs/profiles.md");
        for path in KNOB_REGISTRY
            .iter()
            .map(|definition| definition.path)
            .chain(RESERVED_KNOBS.iter().map(|knob| knob.path))
        {
            assert!(
                doc.contains(&format!("`{path}`")),
                "docs/profiles.md does not document {path}"
            );
        }
    }

    #[test]
    fn knob_assignments_parse_and_apply() -> Result<(), Box<dyn std::error::Error>> {
        let mut knobs = ProfileKnobOverrides::default();
        let mut floors = FloorOverrides::default();
        for input in [
            "cache.max_entries=8192",
            "query.max_concurrent_queries=auto",
            "floors.backup=true",
            " durability.commit = two_phase ",
            "limits.max_value_bytes=unlimited",
            "time_series.default_chunk_millis=60000",
            "time_series.default_retention_millis=null",
        ] {
            parse_knob_assignment(input)?.apply(&mut knobs, &mut floors)?;
        }

        assert_eq!(knobs.cache.and_then(|cache| cache.max_entries), Some(8_192));
        assert_eq!(
            knobs.query.and_then(|query| query.max_concurrent_queries),
            Some(AutoOr::Auto)
        );
        assert_eq!(
            knobs.durability.and_then(|durability| durability.commit),
            Some(CommitMode::TwoPhase)
        );
        assert_eq!(
            knobs.limits.and_then(|limits| limits.max_value_bytes),
            Some(Limit::Unlimited)
        );
        assert_eq!(
            knobs
                .time_series
                .and_then(|time_series| time_series.default_chunk_millis),
            Some(60_000)
        );
        assert_eq!(floors.backup, Some(true));

        parse_knob_assignment("cache.max_entries=1024")?.apply(&mut knobs, &mut floors)?;
        assert_eq!(knobs.cache.and_then(|cache| cache.max_entries), Some(1_024));
        Ok(())
    }

    #[test]
    fn bad_knob_assignments_explain_what_is_accepted() {
        let typo = parse_knob_assignment("cache.max_entires=1");
        let Err(KnobAssignmentError::UnknownKnob { suggestion, .. }) = &typo else {
            panic!("typo should be an unknown knob: {typo:?}");
        };
        assert_eq!(*suggestion, Some("cache.max_entries"));
        assert!(
            typo.as_ref()
                .err()
                .is_some_and(|error| error.to_string().contains("did you mean cache.max_entries"))
        );

        for reserved in ["cache.ttl_millis=1", "history.mvcc_retention_millis=1"] {
            assert!(matches!(
                parse_knob_assignment(reserved),
                Err(KnobAssignmentError::Reserved { .. })
            ));
        }
        let roles = parse_knob_assignment("compatible_roles=graph");
        assert!(matches!(roles, Err(KnobAssignmentError::RoleSet { .. })));
        assert!(
            roles
                .err()
                .is_some_and(|error| error.to_string().contains("compatible_roles.add"))
        );

        let commit = parse_knob_assignment("durability.commit=fast");
        let Err(KnobAssignmentError::InvalidValue { expected, .. }) = &commit else {
            panic!("fast is not a commit mode: {commit:?}");
        };
        assert!(expected.contains("immediate") && expected.contains("two_phase"));

        for invalid in [
            "durability.group_commit_window_ms=70000",
            "cache.max_entries=-1",
            "cache.enabled=yes",
            "query.max_concurrent_queries=unlimited",
            "limits.max_batch_ops=auto",
        ] {
            assert!(
                matches!(
                    parse_knob_assignment(invalid),
                    Err(KnobAssignmentError::InvalidValue { .. })
                ),
                "{invalid} must be rejected"
            );
        }
        assert!(matches!(
            parse_knob_assignment("cache.max_entries"),
            Err(KnobAssignmentError::MissingValue { .. })
        ));
    }
}
