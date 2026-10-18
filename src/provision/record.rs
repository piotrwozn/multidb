//! The provision record: what provisioning owns in a database, stored in the system table
//! `__provision` under the key `record`, with one history entry per apply under
//! `history/{seq:020}`.
//!
//! The record is the only thing that makes an object managed: adoption, release and drift all
//! read it. It is decoded with `deny_unknown_fields`, and `record_version` is checked before the
//! typed decode, so a record written by a newer build is reported as unreadable instead of being
//! misread. Fields added later need `#[serde(default)]`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::guarantees::GuaranteeEnforcement;
use crate::config_spec::DatabaseSpec;

/// The only `ProvisionRecord.record_version` this build reads and writes.
pub const RECORD_VERSION: u32 = 1;
/// The key of the record in `__provision`.
pub(crate) const RECORD_KEY: &[u8] = b"record";

/// Where the last apply got to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionRecordStatus {
    /// An apply started and has not finished: the process stopped, or the record write after
    /// the last step failed.
    InProgress,
    Applied,
    /// A step failed; `failed_step` names it.
    Failed,
}

/// The kind of a catalog object, in spec vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Document,
    Table,
    Vector,
    TimeSeries,
    Graph,
    FullText,
    Geo,
    /// The `<table>_history` table of a system-versioned table.
    History,
    /// A live object provisioning never creates.
    ForeignTable,
    /// A live object provisioning never creates.
    MaterializedView,
}

impl ObjectKind {
    /// The kind as a lowercase slug, as `DesiredKind::slug` spells it.
    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Table => "table",
            Self::Vector => "vector",
            Self::TimeSeries => "time_series",
            Self::Graph => "graph",
            Self::FullText => "full_text",
            Self::Geo => "geo",
            Self::History => "history",
            Self::ForeignTable => "foreign_table",
            Self::MaterializedView => "materialized_view",
        }
    }
}

/// One catalog object provisioning owns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedObject {
    /// The spec collection the object comes from.
    pub spec_collection: String,
    pub kind: ObjectKind,
    /// The object existed and matched the spec when provisioning took ownership.
    pub adopted: bool,
    /// The collection or graph id the object got when it was created or adopted. Later plans
    /// compare the live id against it, also when the spec does not pin one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_id: Option<u32>,
}

/// The provision record of a database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionRecord {
    pub record_version: u32,
    pub status: ProvisionRecordStatus,
    /// The `prov_` id of the plan the last apply confirmed.
    pub plan_id: String,
    /// `config_spec::spec_digest` of `applied_spec`.
    pub spec_digest: String,
    /// The spec the last apply converged to (or, while `in_progress` or `failed`, was
    /// converging to).
    pub applied_spec: DatabaseSpec,
    /// While the record is `in_progress` or `failed`: the spec the last finished apply converged
    /// to, which plans compare declared guarantees and metadata against, so a weakening the
    /// unfinished apply confirmed still needs its flag. `None` when no apply finished; an
    /// `applied` record leaves it out, since `applied_spec` is that spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_spec: Option<DatabaseSpec>,
    /// The `prof_` digest of the resolved profile of `applied_spec`.
    pub profile_digest: String,
    /// Managed catalog objects by name.
    #[serde(default)]
    pub objects: BTreeMap<String, ManagedObject>,
    /// Managed role names.
    #[serde(default)]
    pub managed_roles: BTreeSet<String>,
    /// Managed principals, by login user.
    #[serde(default)]
    pub managed_principals: BTreeSet<String>,
    /// Objects a spec stopped declaring that provisioning released: they and their data still
    /// exist, unmanaged.
    #[serde(default)]
    pub released_objects: BTreeSet<String>,
    /// How each guarantee was enforced when the record was written.
    #[serde(default)]
    pub guarantees: Vec<GuaranteeEnforcement>,
    /// The `pstep-` ids the last apply completed.
    #[serde(default)]
    pub completed_steps: Vec<String>,
    #[serde(default)]
    pub failed_step: Option<String>,
    /// The `--allow-*` flags the last apply ran with.
    #[serde(default)]
    pub flags: Vec<String>,
    /// Reserved for seeds; this build never sets it.
    #[serde(default)]
    pub seed_digest: Option<String>,
    pub applied_at_millis: u64,
    /// The actor of the last apply: a principal name or a local operator label.
    pub applied_by: String,
    /// The crate version that wrote the record.
    pub binary_version: String,
}

impl ProvisionRecord {
    /// The spec the last finished apply converged to: `applied_spec` once the record is
    /// `applied`, else `last_applied_spec`.
    #[must_use]
    pub fn last_applied(&self) -> Option<&DatabaseSpec> {
        if self.status == ProvisionRecordStatus::Applied {
            Some(&self.applied_spec)
        } else {
            self.last_applied_spec.as_ref()
        }
    }
}

/// One past apply, kept under `history/{seq:020}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionHistoryEntry {
    pub plan_id: String,
    pub status: ProvisionRecordStatus,
    #[serde(default)]
    pub flags: Vec<String>,
    pub actor: String,
    pub at_millis: u64,
}

/// What `__provision.record` holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) enum RecordState {
    /// The database was never provisioned.
    None,
    Readable(Box<ProvisionRecord>),
    /// A record a newer build wrote, or a corrupt one. Provisioning refuses to plan against it.
    Unreadable(String),
}

impl RecordState {
    /// The record, when one is readable.
    pub(crate) fn readable(&self) -> Option<&ProvisionRecord> {
        match self {
            Self::Readable(record) => Some(record),
            Self::None | Self::Unreadable(_) => None,
        }
    }
}

/// Decodes a stored record. `record_version` is read before the typed decode, so a record of a
/// newer version is unreadable for that reason rather than for an unknown field.
pub(crate) fn decode_record(bytes: &[u8]) -> RecordState {
    let value: JsonValue = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(error) => return RecordState::Unreadable(format!("the record is not JSON: {error}")),
    };
    match value.get("record_version").and_then(JsonValue::as_u64) {
        Some(found) if found == u64::from(RECORD_VERSION) => {}
        Some(found) => {
            return RecordState::Unreadable(format!(
                "record_version {found} was written by a newer build; this build reads version \
                 {RECORD_VERSION}"
            ));
        }
        None => {
            return RecordState::Unreadable(
                "record_version is missing or not a non-negative integer".to_owned(),
            );
        }
    }
    match serde_json::from_value(value) {
        Ok(record) => RecordState::Readable(Box::new(record)),
        Err(error) => RecordState::Unreadable(format!("the record does not decode: {error}")),
    }
}
