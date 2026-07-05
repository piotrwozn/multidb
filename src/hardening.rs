use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::storage::{Bytes, ReadTransaction, StorageEngine, StorageError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardeningSeed(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvariantSeverity {
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvariantViolation {
    pub invariant: String,
    pub detail: String,
    pub severity: InvariantSeverity,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvariantReport {
    checked: usize,
    violations: Vec<InvariantViolation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEvent {
    pub step: u64,
    pub actor: String,
    pub action: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceDigest {
    pub hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DowngradePolicy {
    AllowedUntilNewFormatWritten,
    RestoreFromBackup,
    NotSupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentFormat {
    pub name: String,
    pub version_field: String,
    pub current_version: u32,
    pub min_read_version: u32,
    pub downgrade: DowngradePolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatRegistry {
    formats: Vec<PersistentFormat>,
}

impl HardeningSeed {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }
}

impl InvariantViolation {
    #[must_use]
    pub fn error(invariant: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            invariant: invariant.into(),
            detail: detail.into(),
            severity: InvariantSeverity::Error,
        }
    }

    #[must_use]
    pub fn warning(invariant: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            invariant: invariant.into(),
            detail: detail.into(),
            severity: InvariantSeverity::Warning,
        }
    }
}

impl InvariantReport {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            checked: 0,
            violations: Vec::new(),
        }
    }

    #[must_use]
    pub fn ok(invariant: impl Into<String>) -> Self {
        let mut report = Self::new();
        report.record_ok(invariant);
        report
    }

    pub fn record_ok(&mut self, _invariant: impl Into<String>) {
        self.checked = self.checked.saturating_add(1);
    }

    pub fn push(&mut self, violation: InvariantViolation) {
        self.checked = self.checked.saturating_add(1);
        self.violations.push(violation);
    }

    pub fn merge(&mut self, other: Self) {
        self.checked = self.checked.saturating_add(other.checked);
        self.violations.extend(other.violations);
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.violations
            .iter()
            .all(|violation| violation.severity == InvariantSeverity::Warning)
    }

    #[must_use]
    pub const fn checked(&self) -> usize {
        self.checked
    }

    #[must_use]
    pub fn violations(&self) -> &[InvariantViolation] {
        &self.violations
    }
}

impl TraceEvent {
    #[must_use]
    pub fn new(
        step: u64,
        actor: impl Into<String>,
        action: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            step,
            actor: actor.into(),
            action: action.into(),
            detail: detail.into(),
        }
    }
}

impl PersistentFormat {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        version_field: impl Into<String>,
        current_version: u32,
        min_read_version: u32,
        downgrade: DowngradePolicy,
    ) -> Self {
        Self {
            name: name.into(),
            version_field: version_field.into(),
            current_version,
            min_read_version,
            downgrade,
        }
    }

    #[must_use]
    pub fn accepts(&self, version: u32) -> bool {
        version >= self.min_read_version && version <= self.current_version
    }

    #[must_use]
    pub fn future_version_message(&self, found: u32) -> Option<String> {
        (found > self.current_version).then(|| {
            format!(
                "{} format version {found} requires multidb format support >= {}",
                self.name, found
            )
        })
    }
}

impl FormatRegistry {
    #[must_use]
    pub fn new(formats: Vec<PersistentFormat>) -> Self {
        Self { formats }
    }

    #[must_use]
    pub fn phase_24_default() -> Self {
        Self::new(vec![
            PersistentFormat::new(
                "database metadata",
                "schema_version",
                1,
                1,
                DowngradePolicy::AllowedUntilNewFormatWritten,
            ),
            PersistentFormat::new(
                "value codec",
                "MDBV/v1; legacy JSON read-only migration source",
                1,
                1,
                DowngradePolicy::RestoreFromBackup,
            ),
            PersistentFormat::new(
                "backup manifest",
                "format_version",
                1,
                1,
                DowngradePolicy::RestoreFromBackup,
            ),
            PersistentFormat::new(
                "txn versions",
                "__txn_versions/value-u64-be",
                1,
                1,
                DowngradePolicy::AllowedUntilNewFormatWritten,
            ),
            PersistentFormat::new(
                "ap versions",
                "__ap_versions/json",
                1,
                1,
                DowngradePolicy::RestoreFromBackup,
            ),
            PersistentFormat::new(
                "shard map",
                "version",
                1,
                1,
                DowngradePolicy::AllowedUntilNewFormatWritten,
            ),
            PersistentFormat::new(
                "cloud tier pointer",
                "MULTIDB_TIERED_SEGMENT_V1",
                1,
                1,
                DowngradePolicy::RestoreFromBackup,
            ),
            PersistentFormat::new(
                "cloud segment metadata",
                "__cloud_segments/json",
                1,
                1,
                DowngradePolicy::RestoreFromBackup,
            ),
            PersistentFormat::new(
                "audit event",
                "integrity hash chain",
                1,
                1,
                DowngradePolicy::RestoreFromBackup,
            ),
            PersistentFormat::new(
                "extension registry",
                "abi version",
                1,
                1,
                DowngradePolicy::NotSupported,
            ),
            PersistentFormat::new(
                "optimizer stats",
                "stats_version",
                1,
                1,
                DowngradePolicy::AllowedUntilNewFormatWritten,
            ),
        ])
    }

    #[must_use]
    pub fn formats(&self) -> &[PersistentFormat] {
        &self.formats
    }

    #[must_use]
    pub fn find(&self, name: &str) -> Option<&PersistentFormat> {
        self.formats.iter().find(|format| format.name == name)
    }

    #[must_use]
    pub fn check_completeness(&self, required: &[&str]) -> InvariantReport {
        let mut report = InvariantReport::new();
        for name in required {
            if self.find(name).is_some() {
                report.record_ok(format!("format registry contains {name}"));
            } else {
                report.push(InvariantViolation::error(
                    "format registry completeness",
                    format!("missing persistent format {name}"),
                ));
            }
        }
        report
    }
}

impl Default for FormatRegistry {
    fn default() -> Self {
        Self::phase_24_default()
    }
}

#[must_use]
pub fn trace_digest(events: &[TraceEvent]) -> TraceDigest {
    let mut hasher = blake3::Hasher::new();
    for event in events {
        hasher.update(&event.step.to_be_bytes());
        hasher.update(event.actor.as_bytes());
        hasher.update(&[0]);
        hasher.update(event.action.as_bytes());
        hasher.update(&[0]);
        hasher.update(event.detail.as_bytes());
        hasher.update(&[0xff]);
    }
    TraceDigest {
        hex: hasher.finalize().to_hex().to_string(),
    }
}

/// Compares storage contents with a naive model for known tables.
/// # Errors
/// Fails when the storage engine rejects a read.
pub fn check_storage_matches_model<S: StorageEngine>(
    storage: &S,
    model: &BTreeMap<String, BTreeMap<Bytes, Bytes>>,
) -> Result<InvariantReport, StorageError> {
    let read = storage.begin_read()?;
    check_snapshot_matches_model(&read, model)
}

/// Compares a read snapshot with a naive model for known tables.
/// # Errors
/// Fails when the snapshot rejects a range read.
pub fn check_snapshot_matches_model<R: ReadTransaction>(
    read: &R,
    model: &BTreeMap<String, BTreeMap<Bytes, Bytes>>,
) -> Result<InvariantReport, StorageError> {
    let mut report = InvariantReport::new();
    for (table, expected) in model {
        let actual = read
            .range(table, &[], &[])?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if &actual == expected {
            report.record_ok(format!("storage table {table} matches model"));
        } else {
            report.push(InvariantViolation::error(
                "storage matches model",
                format!(
                    "table {table} expected {} rows, found {} rows",
                    expected.len(),
                    actual.len()
                ),
            ));
        }
    }
    Ok(report)
}

#[must_use]
pub fn check_sorted_unique_rows(rows: &[(Bytes, Bytes)]) -> InvariantReport {
    let mut report = InvariantReport::new();
    let mut previous: Option<&[u8]> = None;
    for (key, _) in rows {
        if let Some(previous_key) = previous
            && previous_key >= key.as_slice()
        {
            report.push(InvariantViolation::error(
                "sorted unique rows",
                "keys are duplicated or out of byte order",
            ));
            return report;
        }
        previous = Some(key);
    }
    report.record_ok("sorted unique rows");
    report
}

#[must_use]
pub fn check_index_entries_reference_data(
    data_keys: &[Bytes],
    indexed_primary_keys: &[Bytes],
) -> InvariantReport {
    let data = data_keys.iter().collect::<std::collections::BTreeSet<_>>();
    let mut report = InvariantReport::new();
    for key in indexed_primary_keys {
        if data.contains(key) {
            report.record_ok("index entry references data");
        } else {
            report.push(InvariantViolation::error(
                "index subset data",
                format!("index references missing primary key {}", hex_bytes(key)),
            ));
        }
    }
    if indexed_primary_keys.is_empty() {
        report.record_ok("index subset data");
    }
    report
}

#[must_use]
pub fn check_backup_manifest_version(found: u32, registry: &FormatRegistry) -> InvariantReport {
    let mut report = InvariantReport::new();
    let Some(format) = registry.find("backup manifest") else {
        report.push(InvariantViolation::error(
            "backup manifest version",
            "backup manifest format is missing from registry",
        ));
        return report;
    };
    if format.accepts(found) {
        report.record_ok("backup manifest version");
    } else {
        report.push(InvariantViolation::error(
            "backup manifest version",
            format
                .future_version_message(found)
                .unwrap_or_else(|| format!("backup manifest version {found} is too old")),
        ));
    }
    report
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        hardening::{
            FormatRegistry, check_backup_manifest_version, check_index_entries_reference_data,
            check_sorted_unique_rows, check_storage_matches_model, trace_digest,
        },
        storage::{MemEngine, StorageEngine, WriteTransaction},
    };

    use super::TraceEvent;

    #[test]
    fn trace_digest_is_deterministic() {
        let events = vec![
            TraceEvent::new(1, "node-a", "put", "k"),
            TraceEvent::new(2, "node-a", "commit", "ok"),
        ];
        assert_eq!(trace_digest(&events), trace_digest(&events));
    }

    #[test]
    fn storage_model_invariant_detects_mismatch() -> Result<(), Box<dyn std::error::Error>> {
        let storage = MemEngine::new();
        let mut write = storage.begin_write()?;
        write.put("t", b"k", b"v")?;
        write.commit()?;

        let mut expected = BTreeMap::new();
        expected.insert(
            "t".to_owned(),
            BTreeMap::from([(b"k".to_vec(), b"v".to_vec())]),
        );
        assert!(check_storage_matches_model(&storage, &expected)?.is_ok());

        expected.insert(
            "t".to_owned(),
            BTreeMap::from([(b"k".to_vec(), b"x".to_vec())]),
        );
        assert!(!check_storage_matches_model(&storage, &expected)?.is_ok());
        Ok(())
    }

    #[test]
    fn sorted_unique_and_index_invariants_report_errors() {
        let sorted = vec![(b"a".to_vec(), Vec::new()), (b"b".to_vec(), Vec::new())];
        assert!(check_sorted_unique_rows(&sorted).is_ok());

        let unsorted = vec![(b"b".to_vec(), Vec::new()), (b"a".to_vec(), Vec::new())];
        assert!(!check_sorted_unique_rows(&unsorted).is_ok());

        let report = check_index_entries_reference_data(&[b"1".to_vec()], &[b"2".to_vec()]);
        assert!(!report.is_ok());
    }

    #[test]
    fn format_registry_covers_phase_24_formats_and_rejects_future_manifest() {
        let registry = FormatRegistry::default();
        let report = registry.check_completeness(&[
            "database metadata",
            "value codec",
            "backup manifest",
            "txn versions",
            "ap versions",
            "shard map",
            "cloud tier pointer",
            "cloud segment metadata",
            "audit event",
            "extension registry",
            "optimizer stats",
        ]);
        assert!(report.is_ok());
        assert!(!check_backup_manifest_version(99, &registry).is_ok());
    }
}
