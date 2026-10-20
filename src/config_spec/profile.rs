use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Unexpected, Visitor},
};

use super::{
    CertificationImpact, CollectionRole, ConsistencyMode, DATABASE_SPEC_VERSION_V2, DatabaseSpec,
    ProfileSpec, SupportStatus, ValidationIssue, ValidationSeverity, built_in_profile,
    built_in_profiles, custom_profile_requires_spec_v2, hex_prefix, knobs::KNOB_REGISTRY, suggest,
    update_hash_with_json, validation_issue,
};
use crate::{
    db::Profile,
    performance::{CompressionAlgorithm, PerformanceConfig, QueryExecutionConfig},
    query::TableLayout,
};

/// Maximum length of `custom_profile.description`, in characters.
const MAX_DESCRIPTION_CHARS: usize = 500;
/// Maximum edit distance for did-you-mean suggestions.
const SUGGESTION_DISTANCE: usize = 2;
/// Knob paths whose preset value comes from the variant table rather than the product preset.
const VARIANT_KNOB_PATHS: &[&str] = &[
    "storage.medium",
    "storage.layout",
    "durability.commit",
    "durability.group_commit_window_ms",
    "cache.enabled",
    "cache.max_entries",
    "query.memory_limit_bytes",
];
/// Prefix of every knob path inside a spec document.
const KNOB_PATH_PREFIX: &str = "custom_profile.knobs";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMedium {
    Durable,
    Volatile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutKnob {
    Row,
    Columnar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitMode {
    Immediate,
    TwoPhase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionKnob {
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemVersioningKnob {
    Off,
    TypedTables,
}

/// A knob that is either `"auto"` (the runtime default) or an explicit integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutoOr<T> {
    Auto,
    Value(T),
}

/// A knob that is either `"unlimited"` or an explicit integer limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit<T> {
    Unlimited,
    Value(T),
}

/// Fully resolved profile knobs: every leaf is concrete.
///
/// # Evolving the persisted form
///
/// These knobs, every section struct below and `db::PersistedProfileManifest` are persisted in
/// `__meta.profile_manifest`, decoded with `deny_unknown_fields`, and every read recomputes the
/// manifest digest from the knobs as serialized here. Databases written by older builds must
/// keep opening, so:
///
/// - a new leaf needs a `#[serde(default)]` equal to the value every preset had before the leaf
///   existed (what older manifests ran with), and must not be serialized at that value
///   (`skip_serializing_if`); otherwise older manifests fail to decode or their digest no longer
///   matches. An older binary refuses a manifest that sets the leaf to another value, which is
///   the intended fail-closed outcome;
/// - removing or renaming a leaf, or changing its value form, needs a `manifest_version` bump and
///   a migration that reads the older version;
/// - the digest hashes the serialized JSON bytes (`profile_digest`), so reordering fields or
///   changing a serde attribute (rename, representation) here, in a section struct or on
///   `Profile` changes every stored digest even when the decoded values stay the same; it needs
///   the same version bump, with the older digest recomputed the way that version serialized;
/// - `profile_manifest_v1_golden_still_decodes_and_validates` in `src/db/tests.rs` pins a
///   manifest written by the first build with manifests and must keep passing unchanged.
///
/// See docs/profiles.md, "Evolving the manifest".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileKnobs {
    pub storage: StorageKnobs,
    pub durability: DurabilityKnobs,
    pub compression: CompressionKnobs,
    pub cache: CacheKnobs,
    pub query: QueryKnobs,
    pub limits: LimitKnobs,
    pub history: HistoryKnobs,
    pub time_series: TimeSeriesKnobs,
    pub consistency: ConsistencyKnobs,
    pub floors: GuaranteeFloors,
    pub compatible_roles: BTreeSet<CollectionRole>,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageKnobs {
    pub medium: StorageMedium,
    pub layout: LayoutKnob,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurabilityKnobs {
    pub commit: CommitMode,
    pub group_commit_window_ms: u16,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompressionKnobs {
    pub algorithm: CompressionKnob,
    pub min_bytes: u32,
    pub zstd_level: u8,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheKnobs {
    pub enabled: bool,
    pub max_entries: u32,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryKnobs {
    pub max_concurrent_queries: AutoOr<u32>,
    pub queue_timeout_ms: u32,
    pub timeout_ms: u32,
    pub memory_limit_bytes: u64,
    pub spill_limit_bytes: u64,
    pub target_partitions: AutoOr<u16>,
    pub batch_rows: u32,
    pub max_sql_bytes: u32,
    pub max_statements: u16,
    pub max_values_rows: u32,
    pub parser_recursion_limit: u16,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitKnobs {
    pub max_value_bytes: Limit<u32>,
    pub max_batch_ops: Limit<u32>,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryKnobs {
    pub system_versioning: SystemVersioningKnob,
    pub keep_history: bool,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeSeriesKnobs {
    pub default_chunk_millis: i64,
    pub default_retention_millis: Option<i64>,
}

/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsistencyKnobs {
    pub default_domain: ConsistencyMode,
}

/// Guarantees a profile requires. A custom profile may raise them, never lower them.
/// Persisted in profile manifests: evolve it only as [`ProfileKnobs`] describes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct GuaranteeFloors {
    pub audit: bool,
    pub backup: bool,
    pub pitr: bool,
    pub encryption_at_rest: bool,
}

/// Sparse knob overrides of `custom_profile.knobs`. Absent leaves inherit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProfileKnobOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durability: Option<DurabilityOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<CompressionOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<LimitOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<HistoryOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_series: Option<TimeSeriesOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistency: Option<ConsistencyOverrides>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub medium: Option<StorageMedium>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout: Option<LayoutKnob>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DurabilityOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<CommitMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_commit_window_ms: Option<u16>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompressionOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<CompressionKnob>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_bytes: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zstd_level: Option<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_entries: Option<u32>,
    /// Reserved: decodes so validation can report that no runtime implements it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_millis: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_queries: Option<AutoOr<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_timeout_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spill_limit_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_partitions: Option<AutoOr<u16>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_rows: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sql_bytes: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_statements: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_values_rows: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser_recursion_limit: Option<u16>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value_bytes: Option<Limit<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_batch_ops: Option<Limit<u32>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_versioning: Option<SystemVersioningKnob>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_history: Option<bool>,
    /// Reserved: history GC is manual (`multidb maintenance gc`) and retains a number of LSNs,
    /// not a duration, so this duration knob has no runtime sink.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mvcc_retention_millis: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TimeSeriesOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_chunk_millis: Option<i64>,
    /// Absent and `null` both inherit; every preset retention is `null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_retention_millis: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsistencyOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_domain: Option<ConsistencyMode>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FloorOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pitr: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_at_rest: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoleSetDelta {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<CollectionRole>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<CollectionRole>,
}

/// `DatabaseSpec.custom_profile`: a user profile that extends one built-in slug or alias.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomProfileSpec {
    pub extends: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "ProfileKnobOverrides::is_empty")]
    pub knobs: ProfileKnobOverrides,
    #[serde(default, skip_serializing_if = "FloorOverrides::is_empty")]
    pub floors: FloorOverrides,
    #[serde(default, skip_serializing_if = "RoleSetDelta::is_empty")]
    pub compatible_roles: RoleSetDelta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedProfileKind {
    BuiltIn,
    Variant,
    Custom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnobSource {
    Preset,
    Variant,
    Override,
    Derived,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedProfile {
    /// `spec.profile`: the built-in slug or alias, or the custom profile slug.
    pub slug: String,
    pub kind: ResolvedProfileKind,
    pub product_profile: String,
    pub variant: String,
    /// The `extends` value as written; custom profiles only.
    pub extends: Option<String>,
    pub storage_profile: Profile,
    pub status: SupportStatus,
    pub knobs: ProfileKnobs,
    /// Where each knob value came from, keyed by registry path.
    pub provenance: BTreeMap<String, KnobSource>,
    pub digest: String,
}

/// A built-in variant: one product profile pinned 1:1 to a storage `Profile`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ProfileVariant {
    pub product: &'static str,
    pub name: &'static str,
    pub canonical: bool,
    pub storage_profile: Profile,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProfileResolution {
    /// The profile resolved; `issues` holds only Warnings and Advice.
    Resolved {
        profile: Box<ResolvedProfile>,
        issues: Vec<ValidationIssue>,
    },
    /// A version 1 document names a profile the resolver cannot describe.
    Unresolved {
        reason: String,
        issues: Vec<ValidationIssue>,
    },
    /// At least one issue is an Error.
    Invalid(Vec<ValidationIssue>),
}

const PROFILE_VARIANTS: &[ProfileVariant] = &[
    ProfileVariant {
        product: "game_local_balanced",
        name: "balanced",
        canonical: true,
        storage_profile: Profile::Balanced,
    },
    ProfileVariant {
        product: "game_local_balanced",
        name: "in_memory",
        canonical: false,
        storage_profile: Profile::InMemory,
    },
    ProfileVariant {
        product: "desktop_app_embedded",
        name: "document",
        canonical: true,
        storage_profile: Profile::Document,
    },
    ProfileVariant {
        product: "ai_agent_memory",
        name: "vector",
        canonical: true,
        storage_profile: Profile::Vector,
    },
    ProfileVariant {
        product: "secure_app",
        name: "transactional",
        canonical: true,
        storage_profile: Profile::Transactional,
    },
    ProfileVariant {
        product: "secure_app",
        name: "high_durability",
        canonical: false,
        storage_profile: Profile::HighDurability,
    },
    ProfileVariant {
        product: "production_cp",
        name: "production_cp",
        canonical: true,
        storage_profile: Profile::Transactional,
    },
    ProfileVariant {
        product: "analytics_columnar",
        name: "analytical",
        canonical: true,
        storage_profile: Profile::Analytical,
    },
    ProfileVariant {
        product: "analytics_columnar",
        name: "time_series",
        canonical: false,
        storage_profile: Profile::TimeSeries,
    },
];

/// The per-variant columns of the preset table (`docs/profiles.md`).
struct VariantColumns {
    medium: StorageMedium,
    layout: LayoutKnob,
    commit: CommitMode,
    group_commit_window_ms: u16,
    cache_enabled: bool,
    cache_max_entries: u32,
    memory_limit_bytes: u64,
}

struct ResolveContext<'a> {
    strict: bool,
    storage_path: Option<&'a str>,
}

struct ProfileIdentity {
    slug: String,
    kind: ResolvedProfileKind,
    extends: Option<String>,
}

impl<T: Serialize> Serialize for AutoOr<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Auto => serializer.serialize_str("auto"),
            Self::Value(value) => value.serialize(serializer),
        }
    }
}

impl<T: Serialize> Serialize for Limit<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unlimited => serializer.serialize_str("unlimited"),
            Self::Value(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for AutoOr<u32> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match keyword_or_integer(deserializer, "auto", u64::from(u32::MAX))? {
            None => Ok(Self::Auto),
            Some(value) => u32::try_from(value)
                .map(Self::Value)
                .map_err(de::Error::custom),
        }
    }
}

impl<'de> Deserialize<'de> for AutoOr<u16> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match keyword_or_integer(deserializer, "auto", u64::from(u16::MAX))? {
            None => Ok(Self::Auto),
            Some(value) => u16::try_from(value)
                .map(Self::Value)
                .map_err(de::Error::custom),
        }
    }
}

impl<'de> Deserialize<'de> for Limit<u32> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match keyword_or_integer(deserializer, "unlimited", u64::from(u32::MAX))? {
            None => Ok(Self::Unlimited),
            Some(value) => u32::try_from(value)
                .map(Self::Value)
                .map_err(de::Error::custom),
        }
    }
}

/// Accepts exactly `keyword` (returned as `None`) or an unsigned integer up to `max`.
fn keyword_or_integer<'de, D: Deserializer<'de>>(
    deserializer: D,
    keyword: &'static str,
    max: u64,
) -> Result<Option<u64>, D::Error> {
    deserializer.deserialize_any(KeywordOrInteger { keyword, max })
}

struct KeywordOrInteger {
    keyword: &'static str,
    max: u64,
}

impl Visitor<'_> for KeywordOrInteger {
    type Value = Option<u64>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "the string \"{}\" or an unsigned integer up to {}",
            self.keyword, self.max
        )
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        if value == self.keyword {
            Ok(None)
        } else {
            Err(E::invalid_value(Unexpected::Str(value), &self))
        }
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        if value <= self.max {
            Ok(Some(value))
        } else {
            Err(E::invalid_value(Unexpected::Unsigned(value), &self))
        }
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        match u64::try_from(value) {
            Ok(value) => self.visit_u64(value),
            Err(_) => Err(E::invalid_value(Unexpected::Signed(value), &self)),
        }
    }
}

impl From<LayoutKnob> for TableLayout {
    fn from(value: LayoutKnob) -> Self {
        match value {
            LayoutKnob::Row => Self::Row,
            LayoutKnob::Columnar => Self::Columnar,
        }
    }
}

impl From<TableLayout> for LayoutKnob {
    fn from(value: TableLayout) -> Self {
        match value {
            TableLayout::Row => Self::Row,
            TableLayout::Columnar => Self::Columnar,
        }
    }
}

impl From<CompressionKnob> for CompressionAlgorithm {
    fn from(value: CompressionKnob) -> Self {
        match value {
            CompressionKnob::None => Self::None,
            CompressionKnob::Lz4 => Self::Lz4,
            CompressionKnob::Zstd => Self::Zstd,
        }
    }
}

impl From<CompressionAlgorithm> for CompressionKnob {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::None => Self::None,
            CompressionAlgorithm::Lz4 => Self::Lz4,
            CompressionAlgorithm::Zstd => Self::Zstd,
        }
    }
}

impl ProfileKnobOverrides {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage.as_ref().is_none_or(StorageOverrides::is_empty)
            && self
                .durability
                .as_ref()
                .is_none_or(DurabilityOverrides::is_empty)
            && self
                .compression
                .as_ref()
                .is_none_or(CompressionOverrides::is_empty)
            && self.cache.as_ref().is_none_or(CacheOverrides::is_empty)
            && self.query.as_ref().is_none_or(QueryOverrides::is_empty)
            && self.limits.as_ref().is_none_or(LimitOverrides::is_empty)
            && self.history.as_ref().is_none_or(HistoryOverrides::is_empty)
            && self
                .time_series
                .as_ref()
                .is_none_or(TimeSeriesOverrides::is_empty)
            && self
                .consistency
                .as_ref()
                .is_none_or(ConsistencyOverrides::is_empty)
    }
}

impl StorageOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.medium.is_none() && self.layout.is_none()
    }
}

impl DurabilityOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.commit.is_none() && self.group_commit_window_ms.is_none()
    }
}

impl CompressionOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.algorithm.is_none() && self.min_bytes.is_none() && self.zstd_level.is_none()
    }
}

impl CacheOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.max_entries.is_none() && self.ttl_millis.is_none()
    }
}

impl QueryOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_concurrent_queries.is_none()
            && self.queue_timeout_ms.is_none()
            && self.timeout_ms.is_none()
            && self.memory_limit_bytes.is_none()
            && self.spill_limit_bytes.is_none()
            && self.target_partitions.is_none()
            && self.batch_rows.is_none()
            && self.max_sql_bytes.is_none()
            && self.max_statements.is_none()
            && self.max_values_rows.is_none()
            && self.parser_recursion_limit.is_none()
    }
}

impl LimitOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_value_bytes.is_none() && self.max_batch_ops.is_none()
    }
}

impl HistoryOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.system_versioning.is_none()
            && self.keep_history.is_none()
            && self.mvcc_retention_millis.is_none()
    }
}

impl TimeSeriesOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.default_chunk_millis.is_none() && self.default_retention_millis.is_none()
    }
}

impl ConsistencyOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.default_domain.is_none()
    }
}

impl FloorOverrides {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.audit.is_none()
            && self.backup.is_none()
            && self.pitr.is_none()
            && self.encryption_at_rest.is_none()
    }
}

impl RoleSetDelta {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }
}

impl ResolvedProfile {
    /// The runtime performance configuration these knobs produce:
    /// `PerformanceConfig::for_profile(storage_profile)` overlaid with the knobs.
    #[must_use]
    pub fn performance_config(&self) -> PerformanceConfig {
        PerformanceConfig::for_profile(self.storage_profile).with_profile_knobs(&self.knobs)
    }
}

impl ProfileResolution {
    #[must_use]
    pub fn resolved(&self) -> Option<&ResolvedProfile> {
        match self {
            Self::Resolved { profile, .. } => Some(profile),
            Self::Unresolved { .. } | Self::Invalid(_) => None,
        }
    }

    #[must_use]
    pub fn issues(&self) -> &[ValidationIssue] {
        match self {
            Self::Resolved { issues, .. }
            | Self::Unresolved { issues, .. }
            | Self::Invalid(issues) => issues,
        }
    }
}

/// Every built-in variant, grouped by product. Exactly one variant per product is canonical.
#[must_use]
pub const fn profile_variants() -> &'static [ProfileVariant] {
    PROFILE_VARIANTS
}

/// Resolves `spec.profile` (and `spec.custom_profile`) into concrete knobs.
///
/// Version 1 documents keep their lenient behaviour: unknown names are `Unresolved`, and
/// new checks are Warnings or Advice. Version 2 documents get the same checks as Errors.
/// A version 1 document that sets `custom_profile` is `Invalid` with `FIELD_REQUIRES_SPEC_V2`.
#[must_use]
pub fn resolve_profile(spec: &DatabaseSpec) -> ProfileResolution {
    let context = ResolveContext {
        strict: spec.version >= DATABASE_SPEC_VERSION_V2,
        storage_path: spec.deployment.storage_path.as_deref(),
    };

    if let Some(custom) = &spec.custom_profile {
        if !context.strict {
            return ProfileResolution::Invalid(vec![custom_profile_requires_spec_v2()]);
        }
        return resolve_custom(&spec.profile, custom, &context);
    }

    let key = spec.profile.trim();
    let Some((product, variant)) = built_in_profile(key)
        .and_then(|product| variant_for_name(product, key).map(|variant| (product, variant)))
    else {
        return unknown_profile(&spec.profile, &context);
    };
    let identity = ProfileIdentity {
        slug: key.to_owned(),
        kind: built_in_kind(product, key),
        extends: None,
    };
    assemble(
        identity,
        product,
        variant,
        Overlay::default(),
        Vec::new(),
        &context,
    )
}

/// Resolves a built-in slug or alias (snake or kebab case) with optional knob overrides,
/// as a version 2 document would. Used for what-if views such as `multidb profile show`.
#[must_use]
pub fn resolve_named_profile(name: &str, overrides: &ProfileKnobOverrides) -> ProfileResolution {
    let context = ResolveContext {
        strict: true,
        storage_path: None,
    };
    let Some((product, variant)) = lookup_profile_name(name) else {
        return ProfileResolution::Invalid(vec![unknown_profile_issue(name, true)]);
    };
    let key = normalize_profile_name(name);
    if overrides.is_empty() {
        let identity = ProfileIdentity {
            kind: built_in_kind(product, &key),
            slug: key,
            extends: None,
        };
        return assemble(
            identity,
            product,
            variant,
            Overlay::default(),
            Vec::new(),
            &context,
        );
    }

    let custom = CustomProfileSpec {
        extends: name.trim().to_owned(),
        description: None,
        knobs: overrides.clone(),
        floors: FloorOverrides::default(),
        compatible_roles: RoleSetDelta::default(),
    };
    resolve_custom(&format!("{key}_custom"), &custom, &context)
}

/// Lowercases, trims and maps `-` to `_`, so `Secure-App` and `secure_app` are the same name.
pub(crate) fn normalize_profile_name(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('-', "_")
}

/// Looks up a product slug (selecting its canonical variant) or an alias (selecting the variant
/// of the same name) after normalisation.
pub(crate) fn lookup_profile_name(
    name: &str,
) -> Option<(&'static ProfileSpec, &'static ProfileVariant)> {
    let key = normalize_profile_name(name);
    let product = built_in_profile(&key)?;
    variant_for_name(product, &key).map(|variant| (product, variant))
}

/// Finds the variant named exactly `variant` of the product slug named exactly `product`, as a
/// persisted profile manifest records them. No normalisation and no alias lookup.
pub(crate) fn product_variant(
    product: &str,
    variant: &str,
) -> Option<(&'static ProfileSpec, &'static ProfileVariant)> {
    let spec = built_in_profile(product).filter(|spec| spec.slug == product)?;
    PROFILE_VARIANTS
        .iter()
        .find(|candidate| candidate.product == spec.slug && candidate.name == variant)
        .map(|candidate| (spec, candidate))
}

/// Builds the preset knobs of one variant from the variant table and the product preset.
pub(crate) fn preset_knobs(product: &ProfileSpec, variant: &ProfileVariant) -> ProfileKnobs {
    let columns = variant_columns(variant.storage_profile);
    ProfileKnobs {
        storage: StorageKnobs {
            medium: columns.medium,
            layout: columns.layout,
        },
        durability: DurabilityKnobs {
            commit: columns.commit,
            group_commit_window_ms: columns.group_commit_window_ms,
        },
        compression: CompressionKnobs {
            algorithm: CompressionKnob::None,
            min_bytes: 256,
            zstd_level: 3,
        },
        cache: CacheKnobs {
            enabled: columns.cache_enabled,
            max_entries: columns.cache_max_entries,
        },
        query: QueryKnobs {
            max_concurrent_queries: AutoOr::Auto,
            queue_timeout_ms: saturating_u32(QueryExecutionConfig::DEFAULT_QUEUE_TIMEOUT_MS),
            timeout_ms: saturating_u32(QueryExecutionConfig::DEFAULT_TIMEOUT_MS),
            memory_limit_bytes: columns.memory_limit_bytes,
            spill_limit_bytes: QueryExecutionConfig::DEFAULT_SPILL_LIMIT_BYTES,
            target_partitions: AutoOr::Auto,
            batch_rows: saturating_u32_from_usize(QueryExecutionConfig::DEFAULT_BATCH_ROWS),
            max_sql_bytes: saturating_u32_from_usize(QueryExecutionConfig::DEFAULT_MAX_SQL_BYTES),
            max_statements: saturating_u16_from_usize(QueryExecutionConfig::DEFAULT_MAX_STATEMENTS),
            max_values_rows: saturating_u32_from_usize(
                QueryExecutionConfig::DEFAULT_MAX_VALUES_ROWS,
            ),
            parser_recursion_limit: saturating_u16_from_usize(
                QueryExecutionConfig::DEFAULT_PARSER_RECURSION_LIMIT,
            ),
        },
        limits: LimitKnobs {
            max_value_bytes: Limit::Unlimited,
            max_batch_ops: Limit::Unlimited,
        },
        history: HistoryKnobs {
            system_versioning: SystemVersioningKnob::Off,
            keep_history: true,
        },
        time_series: TimeSeriesKnobs {
            default_chunk_millis: 3_600_000,
            default_retention_millis: None,
        },
        consistency: ConsistencyKnobs {
            default_domain: product.default_domain,
        },
        floors: GuaranteeFloors {
            backup: product.slug == "production_cp",
            ..GuaranteeFloors::default()
        },
        compatible_roles: product.compatible_roles.iter().copied().collect(),
    }
}

/// The storage class a durable, row-layout, immediate-commit profile of `product` uses when its
/// variant's own storage class does not fit those knobs.
pub(crate) fn durable_row_profile(product: &ProfileSpec) -> Profile {
    match product.slug {
        "desktop_app_embedded" => Profile::Document,
        "ai_agent_memory" => Profile::Vector,
        "secure_app" | "production_cp" => Profile::Transactional,
        "analytics_columnar" => Profile::TimeSeries,
        _ => Profile::Balanced,
    }
}

/// Derives the storage `Profile` from the knobs (the capability matrix in `docs/profiles.md`).
pub(crate) fn derive_storage_profile(
    knobs: &ProfileKnobs,
    variant: &ProfileVariant,
    product: &ProfileSpec,
) -> Result<Profile, ValidationIssue> {
    let window = knobs.durability.group_commit_window_ms;
    match (
        knobs.storage.medium,
        knobs.storage.layout,
        knobs.durability.commit,
    ) {
        (StorageMedium::Volatile, LayoutKnob::Row, CommitMode::Immediate) if window == 0 => {
            Ok(Profile::InMemory)
        }
        (StorageMedium::Volatile, LayoutKnob::Row, CommitMode::Immediate) => {
            Err(combination_issue(
                "durability.group_commit_window_ms",
                "a volatile medium has no commit log to batch, so group_commit_window_ms must be 0",
                "Set durability.group_commit_window_ms to 0, or extend in_memory without changing it.",
            ))
        }
        (StorageMedium::Volatile, LayoutKnob::Columnar, _) => Err(combination_issue(
            "storage.layout",
            "columnar layout requires a durable medium",
            "Extend analytical for a durable columnar profile, or keep row layout and extend in_memory.",
        )),
        (StorageMedium::Volatile, LayoutKnob::Row, CommitMode::TwoPhase) => Err(combination_issue(
            "durability.commit",
            "two-phase commit requires a durable medium",
            "Extend high_durability for two-phase commit, or keep commit immediate and extend in_memory.",
        )),
        (StorageMedium::Durable, LayoutKnob::Row, CommitMode::TwoPhase) if window == 0 => {
            Ok(Profile::HighDurability)
        }
        (StorageMedium::Durable, LayoutKnob::Row, CommitMode::TwoPhase) => Err(combination_issue(
            "durability.group_commit_window_ms",
            "two-phase commit writes directly, so group_commit_window_ms must be 0",
            "Set durability.group_commit_window_ms to 0, or extend high_durability without changing it.",
        )),
        (StorageMedium::Durable, LayoutKnob::Columnar, CommitMode::TwoPhase) => {
            Err(combination_issue(
                "storage.layout",
                "two-phase commit is only available with row layout",
                "Extend high_durability for two-phase commit, or analytical for columnar layout.",
            ))
        }
        (StorageMedium::Durable, LayoutKnob::Columnar, CommitMode::Immediate) => {
            Ok(Profile::Analytical)
        }
        (StorageMedium::Durable, LayoutKnob::Row, CommitMode::Immediate) => {
            Ok(match variant.storage_profile {
                Profile::Transactional
                | Profile::Document
                | Profile::Vector
                | Profile::TimeSeries
                | Profile::Balanced => variant.storage_profile,
                Profile::InMemory | Profile::HighDurability | Profile::Analytical => {
                    durable_row_profile(product)
                }
            })
        }
    }
}

/// The resolver's Error checks that need only the final knobs: registry bounds, conflicts and
/// planned `limits.*` other than `unlimited`. Reading a persisted manifest re-runs them, so a
/// hand-built or edited manifest cannot carry knobs the resolver would refuse.
pub(crate) fn check_knob_values(knobs: &ProfileKnobs) -> Vec<ValidationIssue> {
    let mut issues = check_knob_ranges(knobs);
    check_knob_conflicts(knobs, &mut issues);
    for (path, value) in [
        ("limits.max_value_bytes", knobs.limits.max_value_bytes),
        ("limits.max_batch_ops", knobs.limits.max_batch_ops),
    ] {
        if matches!(value, Limit::Value(_)) {
            issues.push(limit_not_enforced(path));
        }
    }
    issues
}

/// `PROFILE_KNOB_NOT_ENFORCED` for a planned `limits.*` knob set to a value.
fn limit_not_enforced(path: &str) -> ValidationIssue {
    validation_issue(
        "PROFILE_KNOB_NOT_ENFORCED",
        ValidationSeverity::Error,
        format!("{KNOB_PATH_PREFIX}.{path}"),
        format!(
            "{path} is planned but not enforced: only unlimited is accepted because the write \
             pipeline does not check it yet"
        ),
        format!("Remove {path} or set it to unlimited."),
        CertificationImpact::BlocksCertified,
    )
}

/// Checks every numeric knob against its `KNOB_REGISTRY` bounds.
fn check_knob_ranges(knobs: &ProfileKnobs) -> Vec<ValidationIssue> {
    let value = match serde_json::to_value(knobs) {
        Ok(value) => value,
        Err(error) => {
            return vec![validation_issue(
                "PROFILE_KNOB_OUT_OF_RANGE",
                ValidationSeverity::Error,
                KNOB_PATH_PREFIX,
                format!("profile knobs could not be checked: {error}"),
                "Report this as a bug; resolved knobs must always serialize.",
                CertificationImpact::BlocksCertified,
            )];
        }
    };

    let mut issues = Vec::new();
    for definition in KNOB_REGISTRY {
        let pointer = format!("/{}", definition.path.replace('.', "/"));
        let Some(leaf) = value.pointer(&pointer) else {
            continue;
        };
        if definition.value_type.contains(leaf) == Some(false) {
            issues.push(validation_issue(
                "PROFILE_KNOB_OUT_OF_RANGE",
                ValidationSeverity::Error,
                format!("{KNOB_PATH_PREFIX}.{}", definition.path),
                format!(
                    "{} = {leaf} is outside the supported range {}",
                    definition.path,
                    definition.value_type.describe()
                ),
                format!("Use a value in {}.", definition.value_type.describe()),
                CertificationImpact::BlocksCertified,
            ));
        }
    }
    issues
}

fn resolve_custom(
    slug: &str,
    custom: &CustomProfileSpec,
    context: &ResolveContext<'_>,
) -> ProfileResolution {
    let mut issues = Vec::new();
    if !is_valid_custom_slug(slug) {
        issues.push(validation_issue(
            "CUSTOM_PROFILE_SLUG_INVALID",
            ValidationSeverity::Error,
            "profile",
            format!("custom profile slug {slug} does not match ^[a-z][a-z0-9_]{{2,63}}$"),
            "Use 3 to 64 lowercase letters, digits or underscores, starting with a letter.",
            CertificationImpact::BlocksCertified,
        ));
    }
    let normalized = normalize_profile_name(slug);
    if profile_name_candidates().any(|candidate| candidate == normalized) {
        issues.push(validation_issue(
            "CUSTOM_PROFILE_SHADOWS_BUILTIN",
            ValidationSeverity::Error,
            "profile",
            format!("custom profile slug {slug} shadows the built-in profile name {normalized}"),
            format!(
                "Pick a slug of your own (for example {normalized}_custom) and keep {normalized} in custom_profile.extends."
            ),
            CertificationImpact::BlocksCertified,
        ));
    }
    if let Some(description) = &custom.description {
        let length = description.chars().count();
        if length > MAX_DESCRIPTION_CHARS {
            issues.push(validation_issue(
                "CUSTOM_PROFILE_DESCRIPTION_TOO_LONG",
                ValidationSeverity::Error,
                "custom_profile.description",
                format!(
                    "custom profile description has {length} characters; the limit is {MAX_DESCRIPTION_CHARS}"
                ),
                "Shorten the description to at most 500 characters.",
                CertificationImpact::BlocksCertified,
            ));
        }
    }

    let Some((product, variant)) = lookup_profile_name(&custom.extends) else {
        issues.push(validation_issue(
            "CUSTOM_PROFILE_EXTENDS_UNKNOWN",
            ValidationSeverity::Error,
            "custom_profile.extends",
            format!(
                "custom_profile.extends names {}, which is not a built-in profile slug or alias",
                custom.extends
            ),
            profile_suggestion(&custom.extends),
            CertificationImpact::BlocksCertified,
        ));
        return ProfileResolution::Invalid(issues);
    };

    let identity = ProfileIdentity {
        slug: slug.to_owned(),
        kind: ResolvedProfileKind::Custom,
        extends: Some(custom.extends.clone()),
    };
    let overlay = Overlay {
        knobs: &custom.knobs,
        floors: &custom.floors,
        roles: &custom.compatible_roles,
    };
    assemble(identity, product, variant, overlay, issues, context)
}

#[derive(Clone, Copy)]
struct Overlay<'a> {
    knobs: &'a ProfileKnobOverrides,
    floors: &'a FloorOverrides,
    roles: &'a RoleSetDelta,
}

impl Default for Overlay<'_> {
    fn default() -> Self {
        const NO_KNOBS: &ProfileKnobOverrides = &ProfileKnobOverrides {
            storage: None,
            durability: None,
            compression: None,
            cache: None,
            query: None,
            limits: None,
            history: None,
            time_series: None,
            consistency: None,
        };
        const NO_FLOORS: &FloorOverrides = &FloorOverrides {
            audit: None,
            backup: None,
            pitr: None,
            encryption_at_rest: None,
        };
        const NO_ROLES: &RoleSetDelta = &RoleSetDelta {
            add: Vec::new(),
            remove: Vec::new(),
        };
        Self {
            knobs: NO_KNOBS,
            floors: NO_FLOORS,
            roles: NO_ROLES,
        }
    }
}

fn assemble(
    identity: ProfileIdentity,
    product: &'static ProfileSpec,
    variant: &'static ProfileVariant,
    overlay: Overlay<'_>,
    mut issues: Vec<ValidationIssue>,
    context: &ResolveContext<'_>,
) -> ProfileResolution {
    let mut knobs = preset_knobs(product, variant);
    let mut provenance = preset_provenance();
    overlay_knobs(&mut knobs, overlay.knobs, &mut provenance, &mut issues);
    overlay_floors(
        &mut knobs.floors,
        *overlay.floors,
        &mut provenance,
        &mut issues,
    );
    apply_role_delta(
        &mut knobs.compatible_roles,
        overlay.roles,
        &mut provenance,
        &mut issues,
    );

    let storage_profile = match derive_storage_profile(&knobs, variant, product) {
        Ok(storage_profile) => Some(storage_profile),
        Err(issue) => {
            issues.push(issue);
            None
        }
    };
    issues.extend(check_knob_ranges(&knobs));
    check_knob_conflicts(&knobs, &mut issues);
    check_irrelevant_knobs(&knobs, overlay.knobs, &mut issues);
    check_deployment(&knobs, context, &mut issues);
    if knobs.compression.algorithm != CompressionKnob::None {
        issues.push(validation_issue(
            "PROFILE_FORMAT_REQUIRES_NEW_BINARY",
            ValidationSeverity::Advice,
            format!("{KNOB_PATH_PREFIX}.compression.algorithm"),
            "compression turns on value framing: the database is created with schema_version 2, \
             which older binaries refuse to open",
            "Keep compression.algorithm none if older binaries must read the database.",
            CertificationImpact::None,
        ));
    }

    let status = if identity.kind == ResolvedProfileKind::Custom {
        let overridden = provenance
            .iter()
            .filter(|(_, source)| **source == KnobSource::Override)
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>();
        issues.push(validation_issue(
            "CUSTOM_PROFILE_DEFINED",
            ValidationSeverity::Advice,
            "custom_profile",
            format!(
                "custom profile {} extends {} (variant {} of {}) and overrides: {}",
                identity.slug,
                identity.extends.as_deref().unwrap_or(variant.name),
                variant.name,
                product.slug,
                if overridden.is_empty() {
                    "nothing".to_owned()
                } else {
                    overridden.join(", ")
                }
            ),
            "Custom profiles are supported as Custom; use a built-in profile for catalog certification.",
            CertificationImpact::DowngradesToCustom,
        ));
        SupportStatus::Custom
    } else {
        product.status
    };

    let Some(storage_profile) = storage_profile else {
        return ProfileResolution::Invalid(issues);
    };
    if issues
        .iter()
        .any(|issue| issue.severity == ValidationSeverity::Error)
    {
        return ProfileResolution::Invalid(issues);
    }

    let digest = profile_digest(&knobs, storage_profile);
    ProfileResolution::Resolved {
        profile: Box::new(ResolvedProfile {
            slug: identity.slug,
            kind: identity.kind,
            product_profile: product.slug.to_owned(),
            variant: variant.name.to_owned(),
            extends: identity.extends,
            storage_profile,
            status,
            knobs,
            provenance,
            digest,
        }),
        issues,
    }
}

fn overlay_knobs(
    knobs: &mut ProfileKnobs,
    overrides: &ProfileKnobOverrides,
    provenance: &mut BTreeMap<String, KnobSource>,
    issues: &mut Vec<ValidationIssue>,
) {
    if let Some(storage) = &overrides.storage {
        take(
            &mut knobs.storage.medium,
            storage.medium,
            "storage.medium",
            provenance,
        );
        take(
            &mut knobs.storage.layout,
            storage.layout,
            "storage.layout",
            provenance,
        );
    }
    if let Some(durability) = &overrides.durability {
        take(
            &mut knobs.durability.commit,
            durability.commit,
            "durability.commit",
            provenance,
        );
        take(
            &mut knobs.durability.group_commit_window_ms,
            durability.group_commit_window_ms,
            "durability.group_commit_window_ms",
            provenance,
        );
    }
    if let Some(compression) = &overrides.compression {
        take(
            &mut knobs.compression.algorithm,
            compression.algorithm,
            "compression.algorithm",
            provenance,
        );
        take(
            &mut knobs.compression.min_bytes,
            compression.min_bytes,
            "compression.min_bytes",
            provenance,
        );
        take(
            &mut knobs.compression.zstd_level,
            compression.zstd_level,
            "compression.zstd_level",
            provenance,
        );
    }
    if let Some(cache) = &overrides.cache {
        take(
            &mut knobs.cache.enabled,
            cache.enabled,
            "cache.enabled",
            provenance,
        );
        take(
            &mut knobs.cache.max_entries,
            cache.max_entries,
            "cache.max_entries",
            provenance,
        );
        if cache.ttl_millis.is_some() {
            issues.push(reserved_knob_issue("cache.ttl_millis"));
        }
    }
    if let Some(query) = &overrides.query {
        overlay_query_knobs(&mut knobs.query, query, provenance);
    }
    if let Some(limits) = &overrides.limits {
        for (path, target, value) in [
            (
                "limits.max_value_bytes",
                &mut knobs.limits.max_value_bytes,
                limits.max_value_bytes,
            ),
            (
                "limits.max_batch_ops",
                &mut knobs.limits.max_batch_ops,
                limits.max_batch_ops,
            ),
        ] {
            take(target, value, path, provenance);
            if matches!(value, Some(Limit::Value(_))) {
                issues.push(limit_not_enforced(path));
            }
        }
    }
    if let Some(history) = &overrides.history {
        take(
            &mut knobs.history.system_versioning,
            history.system_versioning,
            "history.system_versioning",
            provenance,
        );
        take(
            &mut knobs.history.keep_history,
            history.keep_history,
            "history.keep_history",
            provenance,
        );
        if history.mvcc_retention_millis.is_some() {
            issues.push(reserved_knob_issue("history.mvcc_retention_millis"));
        }
    }
    if let Some(time_series) = &overrides.time_series {
        take(
            &mut knobs.time_series.default_chunk_millis,
            time_series.default_chunk_millis,
            "time_series.default_chunk_millis",
            provenance,
        );
        take(
            &mut knobs.time_series.default_retention_millis,
            time_series.default_retention_millis.map(Some),
            "time_series.default_retention_millis",
            provenance,
        );
    }
    if let Some(consistency) = &overrides.consistency {
        take(
            &mut knobs.consistency.default_domain,
            consistency.default_domain,
            "consistency.default_domain",
            provenance,
        );
    }
}

fn overlay_query_knobs(
    knobs: &mut QueryKnobs,
    query: &QueryOverrides,
    provenance: &mut BTreeMap<String, KnobSource>,
) {
    take(
        &mut knobs.max_concurrent_queries,
        query.max_concurrent_queries,
        "query.max_concurrent_queries",
        provenance,
    );
    take(
        &mut knobs.target_partitions,
        query.target_partitions,
        "query.target_partitions",
        provenance,
    );
    for (path, target, value) in [
        (
            "query.memory_limit_bytes",
            &mut knobs.memory_limit_bytes,
            query.memory_limit_bytes,
        ),
        (
            "query.spill_limit_bytes",
            &mut knobs.spill_limit_bytes,
            query.spill_limit_bytes,
        ),
    ] {
        take(target, value, path, provenance);
    }
    for (path, target, value) in [
        (
            "query.queue_timeout_ms",
            &mut knobs.queue_timeout_ms,
            query.queue_timeout_ms,
        ),
        ("query.timeout_ms", &mut knobs.timeout_ms, query.timeout_ms),
        ("query.batch_rows", &mut knobs.batch_rows, query.batch_rows),
        (
            "query.max_sql_bytes",
            &mut knobs.max_sql_bytes,
            query.max_sql_bytes,
        ),
        (
            "query.max_values_rows",
            &mut knobs.max_values_rows,
            query.max_values_rows,
        ),
    ] {
        take(target, value, path, provenance);
    }
    for (path, target, value) in [
        (
            "query.max_statements",
            &mut knobs.max_statements,
            query.max_statements,
        ),
        (
            "query.parser_recursion_limit",
            &mut knobs.parser_recursion_limit,
            query.parser_recursion_limit,
        ),
    ] {
        take(target, value, path, provenance);
    }
}

fn overlay_floors(
    floors: &mut GuaranteeFloors,
    overrides: FloorOverrides,
    provenance: &mut BTreeMap<String, KnobSource>,
    issues: &mut Vec<ValidationIssue>,
) {
    for (name, current, requested) in [
        ("audit", &mut floors.audit, overrides.audit),
        ("backup", &mut floors.backup, overrides.backup),
        ("pitr", &mut floors.pitr, overrides.pitr),
        (
            "encryption_at_rest",
            &mut floors.encryption_at_rest,
            overrides.encryption_at_rest,
        ),
    ] {
        let Some(requested) = requested else {
            continue;
        };
        if *current && !requested {
            issues.push(validation_issue(
                "PROFILE_FLOOR_WEAKENED",
                ValidationSeverity::Error,
                format!("custom_profile.floors.{name}"),
                format!("floor {name} is required by the extended profile and cannot be lowered"),
                format!("Remove custom_profile.floors.{name} or set it to true."),
                CertificationImpact::BlocksCertified,
            ));
        } else {
            *current = requested;
            provenance.insert(format!("floors.{name}"), KnobSource::Override);
        }
    }
}

fn apply_role_delta(
    roles: &mut BTreeSet<CollectionRole>,
    delta: &RoleSetDelta,
    provenance: &mut BTreeMap<String, KnobSource>,
    issues: &mut Vec<ValidationIssue>,
) {
    if delta.is_empty() {
        return;
    }
    let conflicting = delta
        .add
        .iter()
        .copied()
        .filter(|role| delta.remove.contains(role))
        .collect::<BTreeSet<_>>();
    if !conflicting.is_empty() {
        issues.push(validation_issue(
            "PROFILE_KNOB_CONFLICT",
            ValidationSeverity::Error,
            "custom_profile.compatible_roles",
            format!(
                "roles {} are listed in both compatible_roles.add and compatible_roles.remove",
                describe_roles(conflicting.iter().copied())
            ),
            "List each role in add or in remove, not both.",
            CertificationImpact::BlocksCertified,
        ));
    }
    roles.extend(delta.add.iter().copied());
    for role in &delta.remove {
        roles.remove(role);
    }
    provenance.insert("compatible_roles".to_owned(), KnobSource::Override);
}

fn check_knob_conflicts(knobs: &ProfileKnobs, issues: &mut Vec<ValidationIssue>) {
    if knobs.cache.enabled && knobs.cache.max_entries == 0 {
        issues.push(validation_issue(
            "PROFILE_KNOB_CONFLICT",
            ValidationSeverity::Error,
            format!("{KNOB_PATH_PREFIX}.cache.max_entries"),
            "cache.enabled is true but cache.max_entries is 0, so the cache could never hold an entry",
            "Set cache.max_entries above 0, or set cache.enabled to false.",
            CertificationImpact::BlocksCertified,
        ));
    }
    if let Some(retention) = knobs.time_series.default_retention_millis
        && retention < knobs.time_series.default_chunk_millis
    {
        issues.push(validation_issue(
            "PROFILE_KNOB_CONFLICT",
            ValidationSeverity::Error,
            format!("{KNOB_PATH_PREFIX}.time_series.default_retention_millis"),
            format!(
                "time_series.default_retention_millis {retention} is shorter than \
                 time_series.default_chunk_millis {}",
                knobs.time_series.default_chunk_millis
            ),
            "Keep at least one chunk of retention, or set default_retention_millis to null.",
            CertificationImpact::BlocksCertified,
        ));
    }
}

fn check_irrelevant_knobs(
    knobs: &ProfileKnobs,
    overrides: &ProfileKnobOverrides,
    issues: &mut Vec<ValidationIssue>,
) {
    let Some(compression) = &overrides.compression else {
        return;
    };
    if compression.zstd_level.is_some() && knobs.compression.algorithm != CompressionKnob::Zstd {
        issues.push(irrelevant_knob_issue(
            "compression.zstd_level",
            "zstd_level only applies when compression.algorithm is zstd",
        ));
    }
    if compression.min_bytes.is_some() && knobs.compression.algorithm == CompressionKnob::None {
        issues.push(irrelevant_knob_issue(
            "compression.min_bytes",
            "min_bytes has no effect while compression.algorithm is none",
        ));
    }
}

fn check_deployment(
    knobs: &ProfileKnobs,
    context: &ResolveContext<'_>,
    issues: &mut Vec<ValidationIssue>,
) {
    if knobs.storage.medium != StorageMedium::Volatile {
        return;
    }
    let Some(path) = context.storage_path else {
        return;
    };
    let (severity, impact) = if context.strict {
        (
            ValidationSeverity::Error,
            CertificationImpact::BlocksCertified,
        )
    } else {
        (ValidationSeverity::Warning, CertificationImpact::None)
    };
    issues.push(validation_issue(
        "PROFILE_DEPLOYMENT_MISMATCH",
        severity,
        "deployment.storage_path",
        format!("the profile keeps data in volatile memory, but deployment.storage_path is {path}"),
        "Set deployment.storage_path to null for a volatile profile, or use a durable storage.medium.",
        impact,
    ));
}

fn take<T>(
    target: &mut T,
    value: Option<T>,
    path: &str,
    provenance: &mut BTreeMap<String, KnobSource>,
) {
    if let Some(value) = value {
        *target = value;
        provenance.insert(path.to_owned(), KnobSource::Override);
    }
}

fn preset_provenance() -> BTreeMap<String, KnobSource> {
    KNOB_REGISTRY
        .iter()
        .map(|definition| {
            let source = if VARIANT_KNOB_PATHS.contains(&definition.path)
                || definition.path.starts_with("floors.")
            {
                KnobSource::Variant
            } else {
                KnobSource::Preset
            };
            (definition.path.to_owned(), source)
        })
        .collect()
}

const fn variant_columns(profile: Profile) -> VariantColumns {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    match profile {
        Profile::InMemory => VariantColumns {
            medium: StorageMedium::Volatile,
            layout: LayoutKnob::Row,
            commit: CommitMode::Immediate,
            group_commit_window_ms: 0,
            cache_enabled: false,
            cache_max_entries: 0,
            memory_limit_bytes: 512 * MIB,
        },
        Profile::HighDurability => {
            durable_columns(LayoutKnob::Row, CommitMode::TwoPhase, 0, 2_048, 768 * MIB)
        }
        Profile::Analytical => durable_columns(
            LayoutKnob::Columnar,
            CommitMode::Immediate,
            0,
            4_096,
            2 * GIB,
        ),
        Profile::Vector => {
            durable_columns(LayoutKnob::Row, CommitMode::Immediate, 1, 8_192, 2 * GIB)
        }
        Profile::Transactional | Profile::Document | Profile::TimeSeries | Profile::Balanced => {
            durable_columns(LayoutKnob::Row, CommitMode::Immediate, 2, 2_048, GIB)
        }
    }
}

const fn durable_columns(
    layout: LayoutKnob,
    commit: CommitMode,
    group_commit_window_ms: u16,
    cache_max_entries: u32,
    memory_limit_bytes: u64,
) -> VariantColumns {
    VariantColumns {
        medium: StorageMedium::Durable,
        layout,
        commit,
        group_commit_window_ms,
        cache_enabled: true,
        cache_max_entries,
        memory_limit_bytes,
    }
}

fn variant_for_name(product: &ProfileSpec, key: &str) -> Option<&'static ProfileVariant> {
    PROFILE_VARIANTS.iter().find(|variant| {
        variant.product == product.slug
            && if product.slug == key {
                variant.canonical
            } else {
                variant.name == key
            }
    })
}

fn built_in_kind(product: &ProfileSpec, key: &str) -> ResolvedProfileKind {
    if product.slug == key {
        ResolvedProfileKind::BuiltIn
    } else {
        ResolvedProfileKind::Variant
    }
}

fn profile_name_candidates() -> impl Iterator<Item = &'static str> {
    built_in_profiles()
        .iter()
        .flat_map(|profile| std::iter::once(profile.slug).chain(profile.aliases.iter().copied()))
}

/// The built-in product slug or alias closest to `name` after normalisation, within edit
/// distance 2; `None` when nothing is that close. Used for did-you-mean suggestions.
#[must_use]
pub fn suggest_profile_name(name: &str) -> Option<&'static str> {
    suggest::closest(
        &normalize_profile_name(name),
        profile_name_candidates(),
        SUGGESTION_DISTANCE,
    )
}

fn profile_suggestion(name: &str) -> String {
    suggest_profile_name(name).map_or_else(
        || {
            format!(
                "Use a built-in profile slug or alias: {}.",
                profile_name_candidates().collect::<Vec<_>>().join(", ")
            )
        },
        |candidate| format!("Did you mean {candidate}?"),
    )
}

fn unknown_profile(name: &str, context: &ResolveContext<'_>) -> ProfileResolution {
    if context.strict {
        return ProfileResolution::Invalid(vec![unknown_profile_issue(name, true)]);
    }
    ProfileResolution::Unresolved {
        reason: format!("profile {name} is not a built-in profile slug or alias"),
        issues: vec![unknown_profile_issue(name, false)],
    }
}

fn unknown_profile_issue(name: &str, strict: bool) -> ValidationIssue {
    if strict {
        validation_issue(
            "PROFILE_UNKNOWN",
            ValidationSeverity::Error,
            "profile",
            format!("profile {name} is not a built-in profile slug or alias"),
            format!(
                "{} To define your own profile, add custom_profile with extends.",
                profile_suggestion(name)
            ),
            CertificationImpact::BlocksCertified,
        )
    } else {
        validation_issue(
            "PROFILE_NOT_PROVISIONABLE",
            ValidationSeverity::Advice,
            "profile",
            format!("profile {name} has no knobs, so no database can be built from it"),
            "Use a built-in profile slug or alias, or run multidb config upgrade and define custom_profile.",
            CertificationImpact::None,
        )
    }
}

fn combination_issue(path: &str, message: &str, suggestion: &str) -> ValidationIssue {
    validation_issue(
        "PROFILE_KNOB_COMBINATION_UNSUPPORTED",
        ValidationSeverity::Error,
        format!("{KNOB_PATH_PREFIX}.{path}"),
        message,
        suggestion,
        CertificationImpact::BlocksCertified,
    )
}

fn reserved_knob_issue(path: &str) -> ValidationIssue {
    let reason = super::knobs::reserved_knob(path).map_or("reserved", |knob| knob.reason);
    validation_issue(
        "PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME",
        ValidationSeverity::Error,
        format!("{KNOB_PATH_PREFIX}.{path}"),
        format!("{path} is a reserved knob that this runtime does not support ({reason})"),
        format!("Remove {path} from custom_profile.knobs."),
        CertificationImpact::BlocksCertified,
    )
}

fn irrelevant_knob_issue(path: &str, message: &str) -> ValidationIssue {
    validation_issue(
        "PROFILE_KNOB_IRRELEVANT",
        ValidationSeverity::Advice,
        format!("{KNOB_PATH_PREFIX}.{path}"),
        message,
        format!("Remove {path} or change compression.algorithm."),
        CertificationImpact::None,
    )
}

fn is_valid_custom_slug(slug: &str) -> bool {
    (3..=64).contains(&slug.len())
        && slug.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && slug
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn describe_roles(roles: impl Iterator<Item = CollectionRole>) -> String {
    roles
        .map(|role| super::describe_value(&role).trim_matches('"').to_owned())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `prof_` digest of resolved knobs and their storage class. `PersistedProfileManifest`
/// stores it, and every manifest read recomputes it from the decoded knobs.
pub(crate) fn profile_digest(knobs: &ProfileKnobs, storage_profile: Profile) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"multidb-profile-v1");
    update_hash_with_json(&mut hasher, knobs);
    update_hash_with_json(&mut hasher, &storage_profile);
    let digest = hasher.finalize();
    format!("prof_{}", hex_prefix(digest.as_bytes(), 16))
}

fn saturating_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn saturating_u32_from_usize(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn saturating_u16_from_usize(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::{Map, Value as JsonValue, json};

    use super::{
        AutoOr, CommitMode, CustomProfileSpec, FloorOverrides, KnobSource, Limit,
        ProfileKnobOverrides, ProfileResolution, ResolvedProfile, ResolvedProfileKind,
        RoleSetDelta, StorageMedium, lookup_profile_name, profile_name_candidates,
        profile_variants, resolve_named_profile, resolve_profile, suggest_profile_name,
    };
    use crate::{
        config_spec::{
            CollectionRole, DatabaseSpec, KNOB_REGISTRY, KnobType, SupportStatus,
            ValidationSeverity, built_in_profiles, profile_slug,
        },
        db::{DbConfig, Profile, layout_for},
        performance::{CompressionAlgorithm, CompressionConfig, PerformanceConfig},
        query::TableLayout,
    };

    fn spec(version: u32, profile: &str, custom: Option<CustomProfileSpec>) -> DatabaseSpec {
        let mut spec = DatabaseSpec::from_db_config(
            "orders",
            &DbConfig::on_disk(Profile::Balanced, "orders.redb"),
        );
        spec.version = version;
        spec.profile = profile.to_owned();
        spec.custom_profile = custom;
        spec
    }

    fn custom(extends: &str) -> CustomProfileSpec {
        CustomProfileSpec {
            extends: extends.to_owned(),
            description: None,
            knobs: ProfileKnobOverrides::default(),
            floors: FloorOverrides::default(),
            compatible_roles: RoleSetDelta::default(),
        }
    }

    fn custom_with_knobs(
        extends: &str,
        knobs: JsonValue,
    ) -> Result<CustomProfileSpec, serde_json::Error> {
        Ok(CustomProfileSpec {
            knobs: serde_json::from_value(knobs)?,
            ..custom(extends)
        })
    }

    /// Resolves a version 2 spec whose path is left to the embedding application, so volatile
    /// variants do not trip the deployment check.
    fn resolve_custom_spec(custom: CustomProfileSpec) -> ProfileResolution {
        let mut spec = spec(2, "orders_custom", Some(custom));
        spec.deployment.storage_path = None;
        resolve_profile(&spec)
    }

    fn codes(resolution: &ProfileResolution) -> BTreeSet<&str> {
        resolution
            .issues()
            .iter()
            .map(|issue| issue.code.as_str())
            .collect()
    }

    fn resolved(resolution: &ProfileResolution) -> &ResolvedProfile {
        let Some(profile) = resolution.resolved() else {
            panic!("profile should resolve: {:?}", resolution.issues());
        };
        profile
    }

    fn assert_invalid_with(resolution: &ProfileResolution, code: &str) {
        assert!(
            matches!(resolution, ProfileResolution::Invalid(_)),
            "{code} should make the profile invalid: {resolution:?}"
        );
        assert!(
            codes(resolution).contains(code),
            "missing {code}: {:?}",
            resolution.issues()
        );
    }

    #[test]
    fn truth_table_matches_runtime_defaults_for_every_storage_profile() {
        for storage_profile in Profile::all() {
            let resolution = resolve_named_profile(
                profile_slug(storage_profile),
                &ProfileKnobOverrides::default(),
            );
            let profile = resolved(&resolution);
            let mut expected = PerformanceConfig::for_profile(storage_profile);
            expected.compression = CompressionConfig {
                algorithm: CompressionAlgorithm::None,
                min_bytes: 256,
                zstd_level: 3,
            };

            assert_eq!(profile.storage_profile, storage_profile);
            assert_eq!(
                profile.performance_config(),
                expected,
                "{storage_profile:?}"
            );
            assert_eq!(
                TableLayout::from(profile.knobs.storage.layout),
                layout_for(storage_profile)
            );
            assert_eq!(
                profile.knobs.durability.commit == CommitMode::TwoPhase,
                storage_profile == Profile::HighDurability
            );
            assert_eq!(
                profile.knobs.storage.medium == StorageMedium::Volatile,
                storage_profile == Profile::InMemory
            );
            assert!(resolution.issues().is_empty(), "{:?}", resolution.issues());
        }
    }

    #[test]
    fn db_config_specs_resolve_to_their_own_storage_profile() {
        for storage_profile in Profile::all() {
            let config = if storage_profile == Profile::InMemory {
                DbConfig::new(storage_profile)
            } else {
                DbConfig::on_disk(storage_profile, format!("{storage_profile:?}.redb"))
            };
            let spec = DatabaseSpec::from_db_config("roundtrip", &config);
            let resolution = resolve_profile(&spec);
            let profile = resolved(&resolution);

            assert_eq!(profile.storage_profile, storage_profile);
            assert_eq!(profile.kind, ResolvedProfileKind::Variant);
            assert_eq!(profile.variant, spec.profile);
            assert!(resolution.issues().is_empty(), "{:?}", resolution.issues());
        }
    }

    #[test]
    fn product_slugs_resolve_to_their_canonical_variant() {
        for product in built_in_profiles() {
            let Some(canonical) = profile_variants()
                .iter()
                .find(|variant| variant.product == product.slug && variant.canonical)
            else {
                panic!("{} has no canonical variant", product.slug);
            };
            let resolution = resolve_profile(&spec(1, product.slug, None));
            let profile = resolved(&resolution);

            assert_eq!(profile.kind, ResolvedProfileKind::BuiltIn);
            assert_eq!(profile.product_profile, product.slug);
            assert_eq!(profile.variant, canonical.name);
            assert_eq!(profile.storage_profile, canonical.storage_profile);
            assert_eq!(profile.status, product.status);
            assert_eq!(profile.extends, None);
        }
    }

    #[test]
    fn variant_table_pins_every_alias_to_one_variant() {
        assert_eq!(profile_variants().len(), 9);
        for product in built_in_profiles() {
            let variants = profile_variants()
                .iter()
                .filter(|variant| variant.product == product.slug)
                .collect::<Vec<_>>();
            assert_eq!(
                variants.iter().filter(|variant| variant.canonical).count(),
                1,
                "{} needs exactly one canonical variant",
                product.slug
            );
            for alias in product.aliases {
                assert_eq!(
                    variants
                        .iter()
                        .filter(|variant| variant.name == *alias)
                        .count(),
                    1,
                    "alias {alias} must map to exactly one variant"
                );
            }
            for variant in &variants {
                assert!(
                    product.aliases.contains(&variant.name) || variant.name == product.slug,
                    "variant {} is neither an alias nor the slug of {}",
                    variant.name,
                    product.slug
                );
            }
        }
        let Some(production) = profile_variants()
            .iter()
            .find(|variant| variant.product == "production_cp")
        else {
            panic!("production_cp has no variant");
        };
        assert_eq!(production.name, "production_cp");
        assert_eq!(production.storage_profile, Profile::Transactional);
    }

    #[test]
    fn extends_accepts_every_slug_and_alias_in_snake_and_kebab_case() {
        for name in profile_name_candidates() {
            let Some((product, variant)) = lookup_profile_name(name) else {
                panic!("built-in name {name} must resolve");
            };
            for spelling in [name.to_owned(), name.replace('_', "-"), name.to_uppercase()] {
                let resolution = resolve_custom_spec(custom(&spelling));
                let profile = resolved(&resolution);

                assert_eq!(profile.kind, ResolvedProfileKind::Custom);
                assert_eq!(profile.product_profile, product.slug);
                assert_eq!(profile.variant, variant.name);
                assert_eq!(profile.storage_profile, variant.storage_profile);
                assert_eq!(profile.extends.as_deref(), Some(spelling.as_str()));
                assert_eq!(profile.status, SupportStatus::Custom);
            }
        }
    }

    #[test]
    fn provenance_covers_every_registry_path() -> Result<(), Box<dyn std::error::Error>> {
        let registry = KNOB_REGISTRY
            .iter()
            .map(|definition| definition.path.to_owned())
            .collect::<BTreeSet<_>>();
        let built_in = resolve_named_profile("high_durability", &ProfileKnobOverrides::default());
        let custom_resolution = resolve_custom_spec(custom_with_knobs(
            "high_durability",
            json!({ "cache": { "max_entries": 8192 } }),
        )?);

        for resolution in [&built_in, &custom_resolution] {
            let profile = resolved(resolution);
            assert_eq!(
                profile.provenance.keys().cloned().collect::<BTreeSet<_>>(),
                registry
            );
        }
        let built_in = resolved(&built_in);
        assert_eq!(
            built_in.provenance.get("durability.commit"),
            Some(&KnobSource::Variant)
        );
        assert_eq!(
            built_in.provenance.get("query.batch_rows"),
            Some(&KnobSource::Preset)
        );
        assert_eq!(
            resolved(&custom_resolution)
                .provenance
                .get("cache.max_entries"),
            Some(&KnobSource::Override)
        );
        Ok(())
    }

    #[test]
    fn variant_digests_are_pinned() {
        let expected = BTreeMap::from([
            ("analytical", "prof_6bbeb7de9d60dc51bfe0825a022ca57a"),
            ("balanced", "prof_e498f362e9bb92d86a3ac7ca46c8cb97"),
            ("document", "prof_be8edb80801f69b141e1401e49a9b8ca"),
            ("high_durability", "prof_b9a7301c8cee9db72a94bff57c8252ee"),
            ("in_memory", "prof_1c3feed6bd67a5b7980ad9cd00f93dca"),
            ("production_cp", "prof_86086a41acabf41b8dfa17ecf8740259"),
            ("time_series", "prof_373cb12425fec567784d723165191e80"),
            ("transactional", "prof_cb09bd41e007dca8bafe38d995e88dcb"),
            ("vector", "prof_291fadb87b72ed0b578aab6b30797ddf"),
        ]);
        let actual = profile_variants()
            .iter()
            .map(|variant| {
                let resolution =
                    resolve_named_profile(variant.name, &ProfileKnobOverrides::default());
                (variant.name, resolved(&resolution).digest.clone())
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            actual.values().collect::<BTreeSet<_>>().len(),
            9,
            "{actual:?}"
        );
        for (name, digest) in &actual {
            assert_eq!(Some(&digest.as_str()), expected.get(name), "{actual:#?}");
        }
    }

    #[test]
    fn unknown_extends_suggests_the_closest_built_in_name() {
        let resolution = resolve_custom_spec(custom("high_durabilty"));

        assert_invalid_with(&resolution, "CUSTOM_PROFILE_EXTENDS_UNKNOWN");
        let Some(issue) = resolution
            .issues()
            .iter()
            .find(|issue| issue.code == "CUSTOM_PROFILE_EXTENDS_UNKNOWN")
        else {
            panic!("missing extends issue");
        };
        assert_eq!(issue.path, "custom_profile.extends");
        assert!(issue.suggestion.contains("high_durability"), "{issue:?}");
    }

    #[test]
    fn suggest_profile_name_normalises_and_stays_within_distance_two() {
        assert_eq!(
            suggest_profile_name("high_durabilty"),
            Some("high_durability")
        );
        assert_eq!(suggest_profile_name("Secure-Ap"), Some("secure_app"));
        assert_eq!(suggest_profile_name(" vector "), Some("vector"));
        assert_eq!(suggest_profile_name("nothing_like_it"), None);
    }

    #[test]
    fn custom_slugs_must_be_valid_and_must_not_shadow_built_ins() {
        for slug in ["secure_app", "vector", "secure-app", "High_Durability"] {
            let resolution = resolve_profile(&spec(2, slug, Some(custom("balanced"))));
            assert_invalid_with(&resolution, "CUSTOM_PROFILE_SHADOWS_BUILTIN");
        }

        let too_long = format!("a{}", "b".repeat(64));
        for slug in ["ab", "Orders", "1orders", "orders-db", too_long.as_str()] {
            let resolution = resolve_profile(&spec(2, slug, Some(custom("balanced"))));
            assert_invalid_with(&resolution, "CUSTOM_PROFILE_SLUG_INVALID");
        }

        let longest = format!("a{}", "b".repeat(63));
        for slug in ["abc", "orders_2", longest.as_str()] {
            let resolution = resolve_profile(&spec(2, slug, Some(custom("balanced"))));
            assert_eq!(resolved(&resolution).slug, slug);
        }
    }

    #[test]
    fn custom_profile_description_is_limited_to_500_characters() {
        let mut long = custom("balanced");
        long.description = Some("é".repeat(501));
        assert_invalid_with(
            &resolve_custom_spec(long),
            "CUSTOM_PROFILE_DESCRIPTION_TOO_LONG",
        );

        let mut limit = custom("balanced");
        limit.description = Some("é".repeat(500));
        assert!(resolve_custom_spec(limit).resolved().is_some());
    }

    #[test]
    fn registry_bounds_are_enforced_at_every_edge() {
        for definition in KNOB_REGISTRY {
            let (min, max) = match definition.value_type {
                KnobType::U32 { min, max } | KnobType::AutoOrU32 { min, max } => {
                    (i128::from(min), i128::from(max))
                }
                KnobType::U64 { min, max } => (i128::from(min), i128::from(max)),
                KnobType::I64 { min, max } | KnobType::OptI64 { min, max } => {
                    (i128::from(min), i128::from(max))
                }
                KnobType::LimitU32 { .. }
                | KnobType::Enum(_)
                | KnobType::Bool
                | KnobType::RoleSet => continue,
            };
            let Some((section, leaf)) = definition.path.split_once('.') else {
                panic!("numeric knob {} needs a section", definition.path);
            };
            for (candidate, in_range) in
                [(min - 1, false), (min, true), (max, true), (max + 1, false)]
            {
                let value = if let Ok(value) = i64::try_from(candidate) {
                    JsonValue::from(value)
                } else if let Ok(value) = u64::try_from(candidate) {
                    JsonValue::from(value)
                } else {
                    continue;
                };
                let overrides = JsonValue::Object(Map::from_iter([(
                    section.to_owned(),
                    JsonValue::Object(Map::from_iter([(leaf.to_owned(), value)])),
                )]));
                let Ok(overrides) = serde_json::from_value::<ProfileKnobOverrides>(overrides)
                else {
                    assert!(!in_range, "{} = {candidate} must decode", definition.path);
                    continue;
                };
                let resolution = resolve_named_profile("balanced", &overrides);
                let out_of_range = resolution.issues().iter().any(|issue| {
                    issue.code == "PROFILE_KNOB_OUT_OF_RANGE"
                        && issue.path.ends_with(definition.path)
                });

                assert_eq!(
                    out_of_range,
                    !in_range,
                    "{} = {candidate}: {:?}",
                    definition.path,
                    resolution.issues()
                );
            }
        }
    }

    #[test]
    fn planned_limits_reject_every_integer_and_accept_unlimited()
    -> Result<(), Box<dyn std::error::Error>> {
        for definition in KNOB_REGISTRY {
            let KnobType::LimitU32 { min, max } = definition.value_type else {
                continue;
            };
            let Some(leaf) = definition.path.strip_prefix("limits.") else {
                panic!("limit knob {} must live under limits", definition.path);
            };
            for value in [min - 1, min, max, max + 1] {
                let overrides = serde_json::from_value::<ProfileKnobOverrides>(
                    json!({ "limits": { leaf: value } }),
                )?;
                let resolution = resolve_named_profile("balanced", &overrides);
                assert_invalid_with(&resolution, "PROFILE_KNOB_NOT_ENFORCED");
            }

            let overrides = serde_json::from_value::<ProfileKnobOverrides>(
                json!({ "limits": { leaf: "unlimited" } }),
            )?;
            let resolution = resolve_named_profile("balanced", &overrides);
            assert!(!codes(&resolution).contains("PROFILE_KNOB_NOT_ENFORCED"));
            assert_eq!(
                resolved(&resolution).knobs.limits.max_value_bytes,
                Limit::Unlimited
            );
        }
        Ok(())
    }

    #[test]
    fn unsupported_knob_combinations_are_refused() -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            (
                "in_memory",
                json!({ "storage": { "layout": "columnar" } }),
                "analytical",
            ),
            (
                "in_memory",
                json!({ "durability": { "commit": "two_phase" } }),
                "high_durability",
            ),
            (
                "in_memory",
                json!({ "durability": { "group_commit_window_ms": 2 } }),
                "in_memory",
            ),
            (
                "high_durability",
                json!({ "storage": { "layout": "columnar" } }),
                "analytical",
            ),
            (
                "high_durability",
                json!({ "durability": { "group_commit_window_ms": 2 } }),
                "high_durability",
            ),
        ];
        for (extends, knobs, hint) in cases {
            let resolution = resolve_custom_spec(custom_with_knobs(extends, knobs.clone())?);
            assert_invalid_with(&resolution, "PROFILE_KNOB_COMBINATION_UNSUPPORTED");
            assert!(
                resolution.issues().iter().any(|issue| {
                    issue.code == "PROFILE_KNOB_COMBINATION_UNSUPPORTED"
                        && issue.suggestion.contains(hint)
                }),
                "{extends} {knobs}: {:?}",
                resolution.issues()
            );
        }

        let accepted = [
            (
                "in_memory",
                json!({ "storage": { "medium": "durable" } }),
                Profile::Balanced,
            ),
            (
                "high_durability",
                json!({ "durability": { "commit": "immediate" } }),
                Profile::Transactional,
            ),
            (
                "analytical",
                json!({ "storage": { "layout": "row" } }),
                Profile::TimeSeries,
            ),
            (
                "balanced",
                json!({ "storage": { "layout": "columnar" } }),
                Profile::Analytical,
            ),
            (
                "transactional",
                json!({ "durability": { "commit": "two_phase", "group_commit_window_ms": 0 } }),
                Profile::HighDurability,
            ),
            (
                "document",
                json!({ "storage": { "medium": "volatile" }, "durability": { "group_commit_window_ms": 0 } }),
                Profile::InMemory,
            ),
        ];
        for (extends, knobs, storage_profile) in accepted {
            let resolution = resolve_custom_spec(custom_with_knobs(extends, knobs)?);
            assert_eq!(resolved(&resolution).storage_profile, storage_profile);
        }
        Ok(())
    }

    #[test]
    fn conflicting_knobs_are_refused() -> Result<(), Box<dyn std::error::Error>> {
        for (extends, knobs) in [
            ("balanced", json!({ "cache": { "max_entries": 0 } })),
            (
                "time_series",
                json!({ "time_series": { "default_retention_millis": 1_000 } }),
            ),
        ] {
            let resolution = resolve_custom_spec(custom_with_knobs(extends, knobs)?);
            assert_invalid_with(&resolution, "PROFILE_KNOB_CONFLICT");
        }

        let mut both = custom("balanced");
        both.compatible_roles = RoleSetDelta {
            add: vec![CollectionRole::Graph],
            remove: vec![CollectionRole::Graph],
        };
        let resolution = resolve_custom_spec(both);
        assert_invalid_with(&resolution, "PROFILE_KNOB_CONFLICT");
        assert!(
            resolution
                .issues()
                .iter()
                .any(|issue| issue.path == "custom_profile.compatible_roles")
        );

        let resolution = resolve_custom_spec(custom_with_knobs(
            "time_series",
            json!({ "time_series": { "default_retention_millis": 7_200_000 } }),
        )?);
        assert_eq!(
            resolved(&resolution)
                .knobs
                .time_series
                .default_retention_millis,
            Some(7_200_000)
        );
        Ok(())
    }

    #[test]
    fn floors_can_be_raised_but_not_lowered() {
        let mut raised = custom("balanced");
        raised.floors.audit = Some(true);
        raised.floors.pitr = Some(true);
        let resolution = resolve_custom_spec(raised);
        let profile = resolved(&resolution);
        assert!(profile.knobs.floors.audit);
        assert!(profile.knobs.floors.pitr);
        assert_eq!(
            profile.provenance.get("floors.audit"),
            Some(&KnobSource::Override)
        );

        let mut lowered = custom("production_cp");
        lowered.floors.backup = Some(false);
        let resolution = resolve_custom_spec(lowered);
        assert_invalid_with(&resolution, "PROFILE_FLOOR_WEAKENED");

        let mut kept = custom("production_cp");
        kept.floors.backup = Some(true);
        assert!(resolved(&resolve_custom_spec(kept)).knobs.floors.backup);
    }

    #[test]
    fn reserved_knobs_are_not_supported_by_the_runtime() -> Result<(), Box<dyn std::error::Error>> {
        for knobs in [
            json!({ "cache": { "ttl_millis": 60_000 } }),
            json!({ "history": { "mvcc_retention_millis": 60_000 } }),
        ] {
            let resolution = resolve_custom_spec(custom_with_knobs("balanced", knobs)?);
            assert_invalid_with(&resolution, "PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME");
        }
        Ok(())
    }

    #[test]
    fn compatible_roles_add_and_remove_are_applied() {
        let mut delta = custom("high_durability");
        delta.compatible_roles = RoleSetDelta {
            add: vec![CollectionRole::TimeSeries],
            remove: vec![CollectionRole::EventLog],
        };
        let resolution = resolve_custom_spec(delta);
        let roles = &resolved(&resolution).knobs.compatible_roles;

        assert!(roles.contains(&CollectionRole::TimeSeries));
        assert!(!roles.contains(&CollectionRole::EventLog));
        assert!(roles.contains(&CollectionRole::Audit));
        assert_eq!(
            resolved(&resolution).provenance.get("compatible_roles"),
            Some(&KnobSource::Override)
        );
    }

    #[test]
    fn volatile_profiles_with_a_storage_path_are_an_error_only_in_v2() {
        let lenient = resolve_profile(&spec(1, "in_memory", None));
        let issue = resolved(&lenient)
            .storage_profile
            .eq(&Profile::InMemory)
            .then(|| {
                lenient
                    .issues()
                    .iter()
                    .find(|issue| issue.code == "PROFILE_DEPLOYMENT_MISMATCH")
            })
            .flatten();
        let Some(issue) = issue else {
            panic!("v1 in_memory with a path should warn: {lenient:?}");
        };
        assert_eq!(issue.severity, ValidationSeverity::Warning);
        assert_eq!(issue.path, "deployment.storage_path");

        let strict = resolve_profile(&spec(2, "in_memory", None));
        assert_invalid_with(&strict, "PROFILE_DEPLOYMENT_MISMATCH");

        let mut no_path = spec(2, "in_memory", None);
        no_path.deployment.storage_path = None;
        assert!(
            resolve_profile(&no_path).issues().is_empty(),
            "expected an empty value, got {:?}",
            resolve_profile(&no_path).issues()
        );
    }

    #[test]
    fn irrelevant_knobs_and_format_changes_are_advised() -> Result<(), Box<dyn std::error::Error>> {
        let resolution = resolve_custom_spec(custom_with_knobs(
            "balanced",
            json!({ "compression": { "algorithm": "lz4", "zstd_level": 5 } }),
        )?);
        assert!(resolution.resolved().is_some());
        assert!(codes(&resolution).contains("PROFILE_KNOB_IRRELEVANT"));
        assert!(codes(&resolution).contains("PROFILE_FORMAT_REQUIRES_NEW_BINARY"));

        let resolution = resolve_custom_spec(custom_with_knobs(
            "balanced",
            json!({ "compression": { "min_bytes": 512 } }),
        )?);
        assert!(codes(&resolution).contains("PROFILE_KNOB_IRRELEVANT"));
        assert!(!codes(&resolution).contains("PROFILE_FORMAT_REQUIRES_NEW_BINARY"));

        let resolution = resolve_custom_spec(custom_with_knobs(
            "balanced",
            json!({ "compression": { "algorithm": "zstd", "zstd_level": 5 } }),
        )?);
        assert!(!codes(&resolution).contains("PROFILE_KNOB_IRRELEVANT"));
        assert!(codes(&resolution).contains("PROFILE_FORMAT_REQUIRES_NEW_BINARY"));
        assert!(
            resolution
                .issues()
                .iter()
                .all(|issue| issue.severity == ValidationSeverity::Advice)
        );
        Ok(())
    }

    #[test]
    fn custom_profiles_are_custom_and_list_their_overrides()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut orders = custom_with_knobs(
            "high_durability",
            json!({
                "compression": { "algorithm": "zstd", "zstd_level": 3, "min_bytes": 512 },
                "cache": { "max_entries": 8192 },
                "query": { "max_concurrent_queries": "auto", "timeout_ms": 10_000 },
                "history": { "system_versioning": "typed_tables", "keep_history": true }
            }),
        )?;
        orders.description = Some("Durable order store with zstd and a larger cache.".to_owned());
        orders.floors.audit = Some(true);
        orders.floors.backup = Some(true);
        orders.compatible_roles.add.push(CollectionRole::TimeSeries);
        let resolution = resolve_profile(&spec(2, "orders_durable", Some(orders)));
        let profile = resolved(&resolution);

        assert_eq!(profile.slug, "orders_durable");
        assert_eq!(profile.kind, ResolvedProfileKind::Custom);
        assert_eq!(profile.product_profile, "secure_app");
        assert_eq!(profile.variant, "high_durability");
        assert_eq!(profile.storage_profile, Profile::HighDurability);
        assert_eq!(profile.status, SupportStatus::Custom);
        assert_eq!(profile.knobs.query.max_concurrent_queries, AutoOr::Auto);
        let performance = profile.performance_config();
        assert_eq!(performance.cache.max_capacity, 8_192);
        assert_eq!(performance.query.timeout_ms, 10_000);
        assert_eq!(
            performance.compression.algorithm,
            CompressionAlgorithm::Zstd
        );
        assert_eq!(performance.compression.min_bytes, 512);
        assert_eq!(performance.io.group_commit_window_ms, 0);

        let Some(defined) = resolution
            .issues()
            .iter()
            .find(|issue| issue.code == "CUSTOM_PROFILE_DEFINED")
        else {
            panic!("missing CUSTOM_PROFILE_DEFINED: {:?}", resolution.issues());
        };
        assert_eq!(defined.severity, ValidationSeverity::Advice);
        for path in [
            "cache.max_entries",
            "compression.algorithm",
            "compression.min_bytes",
            "floors.audit",
            "compatible_roles",
            "history.system_versioning",
            "query.timeout_ms",
        ] {
            assert!(
                defined.message.contains(path),
                "{path}: {}",
                defined.message
            );
        }
        assert!(!defined.message.contains("durability.commit"));
        assert_eq!(
            profile.provenance.get("durability.commit"),
            Some(&KnobSource::Variant)
        );
        Ok(())
    }

    #[test]
    fn named_profile_overrides_resolve_as_a_custom_what_if()
    -> Result<(), Box<dyn std::error::Error>> {
        let overrides = serde_json::from_value::<ProfileKnobOverrides>(json!({
            "cache": { "max_entries": 8192 },
            "query": { "max_concurrent_queries": 3 },
            "durability": { "group_commit_window_ms": 5 }
        }))?;
        let resolution = resolve_named_profile("Game-Local-Balanced", &overrides);
        let profile = resolved(&resolution);
        let performance = profile.performance_config();

        assert_eq!(profile.kind, ResolvedProfileKind::Custom);
        assert_eq!(profile.slug, "game_local_balanced_custom");
        assert_eq!(profile.variant, "balanced");
        assert_eq!(performance.cache.max_capacity, 8_192);
        assert_eq!(performance.query.max_concurrent_queries, 3);
        assert_eq!(performance.io.group_commit_window_ms, 5);

        let variant = resolve_named_profile("high-durability", &ProfileKnobOverrides::default());
        assert_eq!(resolved(&variant).kind, ResolvedProfileKind::Variant);
        assert_eq!(resolved(&variant).slug, "high_durability");
        assert_eq!(resolved(&variant).status, SupportStatus::Stable);
        Ok(())
    }

    #[test]
    fn unknown_profiles_are_unresolved_in_v1_and_invalid_in_v2() {
        let lenient = resolve_profile(&spec(1, "unknown_profile", None));
        let ProfileResolution::Unresolved { issues, .. } = &lenient else {
            panic!("v1 unknown profile should be unresolved: {lenient:?}");
        };
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "PROFILE_NOT_PROVISIONABLE");
        assert_eq!(issues[0].severity, ValidationSeverity::Advice);

        let kebab_v1 = resolve_profile(&spec(1, "secure-app", None));
        assert!(matches!(kebab_v1, ProfileResolution::Unresolved { .. }));

        let strict = resolve_profile(&spec(2, "secure-app", None));
        assert_invalid_with(&strict, "PROFILE_UNKNOWN");
        assert!(strict.issues()[0].suggestion.contains("secure_app"));

        let named = resolve_named_profile("vectr", &ProfileKnobOverrides::default());
        assert_invalid_with(&named, "PROFILE_UNKNOWN");
        assert!(named.issues()[0].suggestion.contains("vector"));
    }

    #[test]
    fn custom_profile_in_a_v1_document_is_invalid() {
        let resolution = resolve_profile(&spec(1, "orders_custom", Some(custom("balanced"))));
        assert_invalid_with(&resolution, "FIELD_REQUIRES_SPEC_V2");
        assert_eq!(resolution.issues().len(), 1);
        assert_eq!(resolution.issues()[0].path, "custom_profile");
        assert_eq!(resolution.issues()[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn keyword_or_integer_knobs_round_trip_and_reject_other_input()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(serde_json::to_value(AutoOr::<u32>::Auto)?, json!("auto"));
        assert_eq!(serde_json::to_value(AutoOr::Value(7_u32))?, json!(7));
        assert_eq!(
            serde_json::to_value(Limit::<u32>::Unlimited)?,
            json!("unlimited")
        );
        assert_eq!(serde_json::to_value(Limit::Value(4_096_u32))?, json!(4_096));
        assert_eq!(
            serde_json::from_value::<AutoOr<u32>>(json!("auto"))?,
            AutoOr::Auto
        );
        assert_eq!(
            serde_json::from_value::<AutoOr<u16>>(json!(16))?,
            AutoOr::Value(16)
        );
        assert_eq!(
            serde_json::from_value::<Limit<u32>>(json!("unlimited"))?,
            Limit::Unlimited
        );
        assert_eq!(serde_norway::from_str::<AutoOr<u32>>("auto")?, AutoOr::Auto);
        assert_eq!(
            serde_norway::from_str::<Limit<u32>>("4096")?,
            Limit::Value(4_096)
        );

        for bad in [json!("Auto"), json!(-1), json!(1.5), json!("unlimited")] {
            let Err(error) = serde_json::from_value::<AutoOr<u32>>(bad.clone()) else {
                panic!("{bad} must be rejected");
            };
            let message = error.to_string();
            assert!(
                message.contains("\"auto\"") && message.contains("unsigned integer"),
                "{message}"
            );
        }
        for bad in [json!("Unlimited"), json!(-1), json!(1.5), json!("auto")] {
            let Err(error) = serde_json::from_value::<Limit<u32>>(bad.clone()) else {
                panic!("{bad} must be rejected");
            };
            let message = error.to_string();
            assert!(
                message.contains("\"unlimited\"") && message.contains("unsigned integer"),
                "{message}"
            );
        }
        assert!(serde_json::from_value::<AutoOr<u16>>(json!(70_000)).is_err());
        Ok(())
    }

    #[test]
    fn knob_overrides_reject_typos_and_treat_null_retention_as_inherit()
    -> Result<(), Box<dyn std::error::Error>> {
        let Err(error) = serde_norway::from_str::<ProfileKnobOverrides>("cache: {max_entires: 1}")
        else {
            panic!("a misspelled knob must not decode");
        };
        assert!(error.to_string().contains("max_entires"), "{error}");

        let null = serde_json::from_value::<ProfileKnobOverrides>(json!({
            "time_series": { "default_retention_millis": null }
        }))?;
        let absent = serde_json::from_value::<ProfileKnobOverrides>(json!({ "time_series": {} }))?;
        assert_eq!(null, absent);
        assert!(null.is_empty());

        let resolution = resolve_custom_spec(CustomProfileSpec {
            knobs: null,
            ..custom("time_series")
        });
        let profile = resolved(&resolution);
        assert_eq!(profile.knobs.time_series.default_retention_millis, None);
        assert_eq!(
            profile
                .provenance
                .get("time_series.default_retention_millis"),
            Some(&KnobSource::Preset)
        );
        Ok(())
    }
}
