//! Runtime binding of resolved profiles: the `__meta.profile_manifest` record, the
//! single-node creator that writes it, the restore target built from a backed-up manifest and
//! the read-only metadata probe.

use std::{
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{
    AUTHZ_TABLE, ConfigError, Database, DbConfig, KEY_AUDIT_ENABLED, KEY_LAYOUT, KEY_PROFILE,
    KEY_PROFILE_MANIFEST, KEY_REPLICATION, KEY_SCHEMA_VERSION, KEY_SCHEMA_VERSION_NAME, META_TABLE,
    OperationalConfig, Profile, ReplicationKind, SCHEMA_VERSION, SCHEMA_VERSION_FRAMED,
    decode_meta_value, decode_metadata, engine_for, engine_for_operational, layout_for,
    read_catalog, read_metadata, validate, validate_operational, write_json, write_metadata,
    write_security_to_repl,
};
use crate::config_spec::{
    CompiledPolicy, CompressionKnob, DeploymentMode, DeploymentSpec, GuaranteeFloors, ProfileKnobs,
    ResolvedProfile, ResolvedProfileKind, TopologySpec, check_knob_values, derive_storage_profile,
    product_variant, profile_digest,
};
use crate::performance::{CompressionConfig, PerformanceConfig};
use crate::query::TableLayout;
use crate::storage::{
    AnyEngine, Bytes, EngineKind, ExistingRedbOpen, ReadTransaction, RedbEngine, RedbReadOnly,
    StorageEngine, StorageError, WriteTransaction,
};

/// The only `PersistedProfileManifest.manifest_version` this build reads and writes.
pub(crate) const MANIFEST_VERSION: u32 = 1;

/// How the values of a profile-manifest database are stored. Persisted with the manifest:
/// evolve it as [`PersistedProfileManifest`] describes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageFormat {
    /// Every value carries a compression frame (`MDBC1`). Requires `schema_version` 2 and can
    /// never be turned off again.
    pub value_framing: bool,
    pub compression: CompressionConfig,
}

/// The resolved profile a database was created with. Stored in `__meta.profile_manifest`,
/// re-validated on every read and re-applied by every single-node opener.
///
/// Every database created with a profile depends on this exact shape (`deny_unknown_fields`,
/// with the digest recomputed from `knobs` on every read). A new field, here or in
/// [`ProfileKnobs`], needs a serde default equal to what older manifests mean and must not be
/// serialized at that default; removing or renaming a field, or changing a value form, needs a
/// [`manifest_version`](Self::manifest_version) bump with a migration that reads the older
/// version. The digest hashes the serialized knob bytes, so reordering knob fields or changing
/// their serde attributes needs that bump too. The golden test
/// `profile_manifest_v1_golden_still_decodes_and_validates` pins a manifest of version 1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedProfileManifest {
    pub manifest_version: u32,
    /// The built-in slug or alias, or the custom profile slug.
    pub profile: String,
    pub kind: ResolvedProfileKind,
    pub product_profile: String,
    pub variant: String,
    pub extends: Option<String>,
    pub storage_profile: Profile,
    pub knobs: ProfileKnobs,
    pub storage_format: StorageFormat,
    pub digest: String,
    /// The crate version that wrote the manifest.
    pub written_by: String,
}

/// A resolved profile bound to one concrete single-node database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeProfileBinding {
    pub db_config: DbConfig,
    /// The runtime performance the knobs produce; equal to what every later open rebuilds from
    /// the manifest.
    pub performance: PerformanceConfig,
    pub manifest: PersistedProfileManifest,
}

/// What `inspect_database_metadata` found at a path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MetadataProbe {
    /// Nothing exists at the path.
    NotFound,
    /// The file exists but holds no database metadata yet.
    Uninitialized,
    Initialized(Box<ProbedMetadata>),
    /// Another handle, in this or another process, holds the database open for writing.
    Locked,
    /// The file was not closed cleanly (a crash or a killed process) and needs redb's crash
    /// recovery, which only a read-write open runs; `recover_database_file` runs it.
    NeedsRecovery,
    Unreadable {
        reason: String,
    },
}

/// The metadata of an initialized database, as `inspect_database_metadata` read it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProbedMetadata {
    pub schema_version: u32,
    pub storage_profile: Profile,
    pub replication: ReplicationKind,
    pub layout: TableLayout,
    /// `None` for a legacy database created without a profile.
    pub manifest: Option<PersistedProfileManifest>,
    /// `__meta` values are compression-framed.
    pub values_framed: bool,
    /// Manifest floors the file visibly does not meet, in registry order. The probe reads
    /// `__meta` without a key, and an encrypted engine encrypts `__meta` too, so a readable
    /// manifest that sets `floors.encryption_at_rest` belongs to an unencrypted file, such as a
    /// restore that was allowed to miss the floor (docs/profiles.md, "Backup and restore");
    /// `floors.audit` is listed when the stored audit flag is off. Empty without a manifest.
    pub unmet_floors: Vec<&'static str>,
}

impl ResolvedProfile {
    /// Binds these knobs to a single-node database: its `DbConfig`, the runtime performance and
    /// the manifest `create_database_with_profile` persists. Compression other than `none`
    /// turns value framing on.
    /// # Errors
    /// Fails when `path` contradicts the storage class: a volatile class with a path, or a
    /// durable class without one.
    pub fn bind(
        &self,
        replication: ReplicationKind,
        path: Option<PathBuf>,
    ) -> Result<RuntimeProfileBinding, ConfigError> {
        let db_config = DbConfig {
            profile: self.storage_profile,
            replication,
            path,
        };
        validate(&db_config)?;

        let manifest = PersistedProfileManifest {
            manifest_version: MANIFEST_VERSION,
            profile: self.slug.clone(),
            kind: self.kind,
            product_profile: self.product_profile.clone(),
            variant: self.variant.clone(),
            extends: self.extends.clone(),
            storage_profile: self.storage_profile,
            knobs: self.knobs.clone(),
            storage_format: StorageFormat {
                value_framing: self.knobs.compression.algorithm != CompressionKnob::None,
                compression: self.performance_config().compression,
            },
            digest: self.digest.clone(),
            written_by: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let performance = performance_for_manifest(&manifest);
        Ok(RuntimeProfileBinding {
            db_config,
            performance,
            manifest,
        })
    }
}

impl CompiledPolicy {
    /// Binds the resolved profile of this policy to a single-node database at
    /// `path_override`, or else at `deployment.storage_path`.
    /// # Errors
    /// Fails with `Unsupported` when the profile did not resolve, for cluster deployments and
    /// for any topology other than one replica and one shard; otherwise as
    /// [`ResolvedProfile::bind`]. In particular a volatile storage class with either path is
    /// `VolatileWithPath`, although a version 1 spec only warns about it
    /// (`PROFILE_DEPLOYMENT_MISMATCH`): binding never drops a path the caller gave.
    pub fn binding(
        &self,
        deployment: &DeploymentSpec,
        path_override: Option<PathBuf>,
    ) -> Result<RuntimeProfileBinding, ConfigError> {
        let Some(resolved) = &self.resolved_profile else {
            return Err(ConfigError::Unsupported(format!(
                "profile {} did not resolve to built-in knobs, so it cannot be bound to a database",
                self.storage_profile
            )));
        };
        if deployment.mode == DeploymentMode::Cluster {
            return Err(ConfigError::Unsupported(
                "cluster deployments cannot be bound to a database in this build; profile \
                 manifests are single-node only"
                    .to_owned(),
            ));
        }
        if self.topology != TopologySpec::default() {
            return Err(ConfigError::Unsupported(format!(
                "topology with {} replicas and {} shards cannot be bound to a database in this \
                 build; profile manifests need 1 replica and 1 shard",
                self.topology.replica_count, self.topology.shard_count
            )));
        }

        let path = path_override.or_else(|| deployment.storage_path.as_deref().map(PathBuf::from));
        resolved.bind(self.replication_kind.into(), path)
    }
}

/// Creates a new single-node database from a profile binding and persists its manifest.
///
/// Unlike `create_database`, this never opens an existing database. The profile owns the
/// performance configuration, so `ops.performance` must be `None`; the other operational
/// settings apply as in `create_database_with_ops`, including writing `ops.security`. The
/// operational settings must meet the profile floors the runtime can check (see
/// `check_profile_floors`), so no manifest claims a floor its database does not meet.
/// # Errors
/// Fails with `AlreadyInitialized` when the target already holds database metadata, with
/// `Unsupported` when `ops.performance` is set, with `ProfileFloorNotMet` when `ops` does not
/// meet a floor, with `ProfileManifestInvalid` when the binding is inconsistent, and as
/// `create_database_with_ops` otherwise. Nothing is created when it fails before the metadata
/// write.
pub fn create_database_with_profile(
    binding: &RuntimeProfileBinding,
    ops: OperationalConfig,
) -> Result<Database, ConfigError> {
    if ops.performance.is_some() {
        return Err(ConfigError::Unsupported(
            "create_database_with_profile takes performance from the profile; leave \
             OperationalConfig.performance unset"
                .to_owned(),
        ));
    }
    check_profile_floors(binding.manifest.knobs.floors, &ops)?;
    let config = binding.db_config.clone();
    validate_operational(&config, &ops)?;
    validate_manifest(&binding.manifest, config.profile)?;
    if binding.performance != performance_for_manifest(&binding.manifest) {
        return Err(ConfigError::ProfileManifestInvalid(
            "the binding performance differs from what its manifest produces at every open; \
             build bindings with ResolvedProfile::bind"
                .to_owned(),
        ));
    }

    let mut ops = ops;
    ops.performance = Some(binding.performance.clone());
    let mut engine = engine_for_operational(&config, &ops)?;
    let storage_format = &binding.manifest.storage_format;
    if storage_format.value_framing && !engine.is_value_framed() {
        // `engine_for_operational` leaves out the wrapper for algorithm `none`; framing still
        // applies, as RAW frames.
        engine = engine.with_value_framing(storage_format.compression.clone());
    }

    match read_metadata(&engine) {
        Ok(_) => return Err(ConfigError::AlreadyInitialized),
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {}
        Err(error) => return Err(error),
    }
    write_profile_metadata(&engine, &config, &binding.manifest)?;

    let catalog = read_catalog(&engine)?;
    let mut database =
        Database::new_with_performance(config, engine, catalog, binding.performance.clone())?;
    database.encryption.clone_from(&ops.encryption);
    write_security_to_repl(database.replication_ref(), &ops.security)?;
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(ops.security);
    Ok(database)
}

/// Refuses operational settings that do not meet a profile floor the runtime can check at
/// creation: `encryption_at_rest` needs `ops.encryption`, and `audit` needs
/// `ops.security.audit_enabled`. `backup` and `pitr` are operator-managed (`multidb backup`, no
/// scheduler), so no runtime setting provides them; validation checks them against the spec
/// guarantees (`GUARANTEE_BELOW_PROFILE_FLOOR`) instead.
fn check_profile_floors(
    floors: GuaranteeFloors,
    ops: &OperationalConfig,
) -> Result<(), ConfigError> {
    if floors.encryption_at_rest && ops.encryption.is_none() {
        return Err(ConfigError::ProfileFloorNotMet {
            floor: "floors.encryption_at_rest",
            reason: "the profile requires encryption at rest, but OperationalConfig.encryption \
                     is unset; supply an encryption config"
                .to_owned(),
        });
    }
    if floors.audit && !ops.security.audit_enabled {
        return Err(ConfigError::ProfileFloorNotMet {
            floor: "floors.audit",
            reason: "the profile requires audit, but OperationalConfig.security.audit_enabled is \
                     false"
                .to_owned(),
        });
    }
    Ok(())
}

/// The manifest floors a database does not meet, in registry order: `floors.audit` when audit
/// is off, and `floors.encryption_at_rest` when its values are not encrypted at rest. `backup`
/// and `pitr` are operator-managed, so no database state meets or misses them.
pub(crate) fn unmet_profile_floors(
    floors: GuaranteeFloors,
    audit_enabled: bool,
    encrypted_at_rest: bool,
) -> Vec<&'static str> {
    [
        (floors.audit && !audit_enabled, "floors.audit"),
        (
            floors.encryption_at_rest && !encrypted_at_rest,
            "floors.encryption_at_rest",
        ),
    ]
    .into_iter()
    .filter_map(|(unmet, floor)| unmet.then_some(floor))
    .collect()
}

/// Logs a warning for every floor of `manifest` that the database a single-node opener just
/// opened does not meet: its engine does not encrypt values, such as a restore that was allowed
/// to miss `floors.encryption_at_rest`, or audit is off. Opening still succeeds; `profile show
/// --db` lists the same floors (`ProbedMetadata.unmet_floors`).
pub(crate) fn warn_unmet_profile_floors(
    manifest: Option<&PersistedProfileManifest>,
    database: &Database,
) {
    let Some(manifest) = manifest else {
        return;
    };
    let encrypted_at_rest = matches!(
        database.engine_kind(),
        EngineKind::EncryptedMemory
            | EngineKind::EncryptedRedb
            | EngineKind::CompressedEncryptedMemory
            | EngineKind::CompressedEncryptedRedb
    );
    for floor in unmet_profile_floors(
        manifest.knobs.floors,
        database.audit_enabled(),
        encrypted_at_rest,
    ) {
        tracing::warn!(
            profile = %manifest.profile,
            floor,
            "the database does not meet profile floor {floor} of its manifest; see \
             docs/profiles.md, \"Backup and restore\""
        );
    }
}

/// Reads a stored `__authz_policy.audit_enabled` flag, which every opener reads as on when it
/// is missing.
fn stored_audit_enabled(raw: Option<Bytes>) -> Result<bool, ConfigError> {
    raw.map_or(Ok(true), |raw| {
        decode_meta_value(&raw, "__authz_policy.audit_enabled", &mut false)
    })
}

/// Creates the empty target database of a restore, framed the way the backed-up profile
/// manifest says.
///
/// With a manifest, the target gets the engine and metadata `create_database_with_profile`
/// gives it: value framing with the manifest compression when the manifest records framing,
/// and `schema_version`, the storage profile, replication, layout and the manifest in one write
/// transaction. Without one, it is what `create_database` creates on an empty target. The
/// restored `__meta` records overwrite this metadata afterwards. Unlike
/// `create_database_with_profile`, it does not check the manifest floors: like every restore
/// target, the engine is not encrypted, also when the manifest sets `floors.encryption_at_rest`,
/// so restore checks the floors before it calls this and refuses unless the caller allows
/// unmet floors (docs/profiles.md, "Backup and restore").
/// # Errors
/// Fails with `ProfileManifestInvalid` when the manifest does not describe `config.profile`,
/// with `AlreadyInitialized` when the target already holds database metadata, and as
/// `engine_for` otherwise.
pub(crate) fn create_restore_target(
    config: DbConfig,
    manifest: Option<&PersistedProfileManifest>,
) -> Result<Database, ConfigError> {
    if let Some(manifest) = manifest {
        validate_manifest(manifest, config.profile)?;
    }
    let mut engine = engine_for(&config)?;
    if let Some(storage_format) = manifest
        .map(|manifest| &manifest.storage_format)
        .filter(|storage_format| storage_format.value_framing)
    {
        engine = engine.with_value_framing(storage_format.compression.clone());
    }

    match read_metadata(&engine) {
        Ok(_) => return Err(ConfigError::AlreadyInitialized),
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {}
        Err(error) => return Err(error),
    }
    let performance = if let Some(manifest) = manifest {
        write_profile_metadata(&engine, &config, manifest)?;
        performance_for_manifest(manifest)
    } else {
        write_metadata(&engine, &config)?;
        PerformanceConfig::for_profile(config.profile)
    };

    let catalog = read_catalog(&engine)?;
    Database::new_with_performance(config, engine, catalog, performance)
}

/// Reads the metadata of the database file at `path` without creating or writing anything.
///
/// A missing path is `NotFound`, a path that is not a regular file (a directory, for example)
/// `Unreadable` and an empty file `Uninitialized`, all without opening it.
/// Otherwise the existing redb file is opened read-only (`redb::ReadOnlyDatabase`, a shared
/// lock and no write access), `__meta` is read through the frame-tolerant reader, as is the
/// stored audit flag when the manifest sets `floors.audit`, and the handle is dropped before
/// returning. A file that was not closed cleanly is `NeedsRecovery`: redb recovers it only on a
/// read-write open, which the probe never does.
#[must_use]
pub fn inspect_database_metadata(path: &Path) -> MetadataProbe {
    let length = match std::fs::metadata(path) {
        // A directory, FIFO or device is never a database file; opening a FIFO could block.
        Ok(metadata) if !metadata.is_file() => {
            return MetadataProbe::Unreadable {
                reason: format!("{} is not a regular file", path.display()),
            };
        }
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return MetadataProbe::NotFound,
        Err(error) => {
            return MetadataProbe::Unreadable {
                reason: error.to_string(),
            };
        }
    };
    if length == 0 {
        return MetadataProbe::Uninitialized;
    }

    let database = match RedbReadOnly::open(path) {
        ExistingRedbOpen::Opened(database) => database,
        ExistingRedbOpen::Locked => return MetadataProbe::Locked,
        ExistingRedbOpen::NeedsRepair => return MetadataProbe::NeedsRecovery,
        ExistingRedbOpen::Failed(error) => {
            return MetadataProbe::Unreadable {
                reason: error.to_string(),
            };
        }
    };
    let read = database
        .begin_read()
        .map_err(ConfigError::from)
        .and_then(|txn| {
            let read = decode_metadata(|key| Ok(txn.get(META_TABLE, key)?))?;
            let unmet_floors = match &read.manifest {
                // `__meta` was read without a key, so the file is not encrypted at rest.
                Some(manifest) => unmet_profile_floors(
                    manifest.knobs.floors,
                    !manifest.knobs.floors.audit
                        || stored_audit_enabled(txn.get(AUTHZ_TABLE, KEY_AUDIT_ENABLED)?)?,
                    false,
                ),
                None => Vec::new(),
            };
            Ok((read, unmet_floors))
        });
    let probe = match read {
        Ok((read, unmet_floors)) => MetadataProbe::Initialized(Box::new(ProbedMetadata {
            schema_version: read.metadata.schema_version,
            storage_profile: read.metadata.profile,
            replication: read.metadata.replication,
            layout: read.metadata.layout,
            manifest: read.manifest,
            values_framed: read.values_framed,
            unmet_floors,
        })),
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            MetadataProbe::Uninitialized
        }
        Err(error) => MetadataProbe::Unreadable {
            reason: error.to_string(),
        },
    };
    drop(database);
    probe
}

/// Runs redb's crash recovery on the existing database file at `path`, for a file that
/// `inspect_database_metadata` reports as `NeedsRecovery`.
///
/// The file is opened read-write without ever being created, which restores its last committed
/// state exactly as every opener does before it reads anything, and is closed again. No database
/// metadata is read or written, so the storage profile, replication and manifest stay as they
/// were, and `inspect_database_metadata` can read the file afterwards. A file that was closed
/// cleanly is left untouched.
/// # Errors
/// Fails with `Storage` when the path is missing, is not a regular file (checked before it is
/// opened) or not a redb file, another handle holds the database open, or redb cannot recover it.
pub fn recover_database_file(path: &Path) -> Result<(), ConfigError> {
    // A directory, FIFO or device is never a database file; opening a FIFO could block.
    if !std::fs::metadata(path)
        .map_err(StorageError::from)?
        .is_file()
    {
        return Err(StorageError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a regular file", path.display()),
        ))
        .into());
    }
    match RedbReadOnly::open(path) {
        ExistingRedbOpen::Opened(_) => Ok(()),
        ExistingRedbOpen::Failed(error) => Err(error.into()),
        // A locked file fails in the read-write open with the error every opener reports.
        ExistingRedbOpen::NeedsRepair | ExistingRedbOpen::Locked => {
            Ok(RedbEngine::recover_existing(path)?)
        }
    }
}

/// The runtime performance a manifest database runs with when no `OperationalConfig`
/// overrides it: the storage profile's defaults overlaid with the knobs, and the persisted
/// compression when values are framed.
pub(crate) fn performance_for_manifest(manifest: &PersistedProfileManifest) -> PerformanceConfig {
    let mut performance = PerformanceConfig::for_profile(manifest.storage_profile)
        .with_profile_knobs(&manifest.knobs);
    if manifest.storage_format.value_framing {
        performance.compression = manifest.storage_format.compression.clone();
    }
    performance
}

/// Decodes `__meta.profile_manifest`. `manifest_version` is checked before the typed decode, so
/// a newer manifest is reported as such rather than as an unknown field.
pub(crate) fn decode_profile_manifest(
    bytes: &[u8],
) -> Result<PersistedProfileManifest, ConfigError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| ConfigError::ProfileManifestInvalid(format!("not JSON: {error}")))?;
    let Some(version) = value.get("manifest_version") else {
        return Err(ConfigError::ProfileManifestInvalid(
            "manifest_version is missing".to_owned(),
        ));
    };
    match version.as_u64() {
        Some(found) if found == u64::from(MANIFEST_VERSION) => {}
        Some(found) => {
            return Err(ConfigError::UnsupportedProfileManifest {
                found: u32::try_from(found).unwrap_or(u32::MAX),
            });
        }
        None => {
            return Err(ConfigError::ProfileManifestInvalid(format!(
                "manifest_version must be a non-negative integer, found {version}"
            )));
        }
    }
    serde_json::from_value(value)
        .map_err(|error| ConfigError::ProfileManifestInvalid(error.to_string()))
}

/// Re-validates a manifest before it drives the runtime: it must describe the storage profile
/// recorded in `__meta`, the knobs must pass the resolver's value checks (registry bounds, knob
/// conflicts, planned `limits.*` left `unlimited`), the knobs must derive that storage class
/// under the recorded variant, the storage format must agree with the compression knobs, and
/// the recorded digest must equal the digest of the decoded knobs, so an in-range hand edit is
/// refused too.
pub(crate) fn validate_manifest(
    manifest: &PersistedProfileManifest,
    profile: Profile,
) -> Result<(), ConfigError> {
    let invalid = |message: String| -> Result<(), ConfigError> {
        Err(ConfigError::ProfileManifestInvalid(message))
    };

    if manifest.storage_profile != profile {
        return invalid(format!(
            "manifest storage_profile {:?} disagrees with __meta profile {profile:?}",
            manifest.storage_profile
        ));
    }
    if let Some(issue) = check_knob_values(&manifest.knobs).first() {
        return invalid(format!("{}: {}", issue.path, issue.message));
    }
    let Some((product, variant)) = product_variant(&manifest.product_profile, &manifest.variant)
    else {
        return invalid(format!(
            "variant {} of product profile {} is not a built-in variant",
            manifest.variant, manifest.product_profile
        ));
    };
    match derive_storage_profile(&manifest.knobs, variant, product) {
        Ok(derived) if derived == manifest.storage_profile => {}
        Ok(derived) => {
            return invalid(format!(
                "the knobs derive storage class {derived:?}, but the manifest records {:?}",
                manifest.storage_profile
            ));
        }
        Err(issue) => return invalid(format!("{}: {}", issue.path, issue.message)),
    }

    let knob_compression = PerformanceConfig::for_profile(manifest.storage_profile)
        .with_profile_knobs(&manifest.knobs)
        .compression;
    if manifest.storage_format.compression != knob_compression {
        return invalid(
            "storage_format.compression disagrees with the compression knobs".to_owned(),
        );
    }
    if manifest.knobs.compression.algorithm != CompressionKnob::None
        && !manifest.storage_format.value_framing
    {
        return invalid("compression other than none requires value framing".to_owned());
    }
    let digest = profile_digest(&manifest.knobs, manifest.storage_profile);
    if manifest.digest != digest {
        return invalid(format!(
            "digest {} does not match the knobs, which digest to {digest}; the manifest was \
             edited or is corrupt",
            manifest.digest
        ));
    }
    Ok(())
}

/// Writes the metadata of a new profile-manifest database in one write transaction.
fn write_profile_metadata(
    engine: &AnyEngine,
    config: &DbConfig,
    manifest: &PersistedProfileManifest,
) -> Result<(), ConfigError> {
    let schema_version = if manifest.storage_format.value_framing {
        SCHEMA_VERSION_FRAMED
    } else {
        SCHEMA_VERSION
    };
    let mut txn = engine.begin_write()?;
    write_json(&mut txn, KEY_SCHEMA_VERSION, &schema_version)?;
    write_json(&mut txn, KEY_PROFILE, &config.profile)?;
    write_json(&mut txn, KEY_REPLICATION, &config.replication)?;
    write_json(&mut txn, KEY_LAYOUT, &layout_for(config.profile))?;
    write_json(&mut txn, KEY_PROFILE_MANIFEST, manifest)?;
    txn.commit()?;
    Ok(())
}
