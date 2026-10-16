//! Runtime binding of resolved profiles: the `__meta.profile_manifest` record, the
//! single-node creator that writes it and the read-only metadata probe.

use std::{
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{
    ConfigError, Database, DbConfig, KEY_LAYOUT, KEY_PROFILE, KEY_PROFILE_MANIFEST,
    KEY_REPLICATION, KEY_SCHEMA_VERSION, KEY_SCHEMA_VERSION_NAME, OperationalConfig, Profile,
    ReplicationKind, SCHEMA_VERSION, SCHEMA_VERSION_FRAMED, engine_for_operational, layout_for,
    read_catalog, read_metadata, validate, validate_operational, write_json,
    write_security_to_repl,
};
use crate::config_spec::{
    CompiledPolicy, CompressionKnob, DeploymentMode, DeploymentSpec, ProfileKnobs, ResolvedProfile,
    ResolvedProfileKind, TopologySpec, check_knob_ranges, derive_storage_profile, product_variant,
};
use crate::performance::{CompressionConfig, PerformanceConfig};
use crate::query::TableLayout;
use crate::storage::{AnyEngine, ExistingRedbOpen, RedbEngine, StorageEngine, WriteTransaction};

/// The only `PersistedProfileManifest.manifest_version` this build reads and writes.
const MANIFEST_VERSION: u32 = 1;

/// How the values of a profile-manifest database are stored.
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
    /// Another handle, in this or another process, holds the database open.
    Locked,
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
    /// `path_override`, or else at `deployment.storage_path`. The path is used only for a
    /// durable storage class.
    /// # Errors
    /// Fails with `Unsupported` when the profile did not resolve, for cluster deployments and
    /// for any topology other than one replica and one shard; otherwise as
    /// [`ResolvedProfile::bind`].
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

        let path = if resolved.storage_profile.is_on_disk() {
            path_override.or_else(|| deployment.storage_path.as_deref().map(PathBuf::from))
        } else {
            None
        };
        resolved.bind(self.replication_kind.into(), path)
    }
}

/// Creates a new single-node database from a profile binding and persists its manifest.
///
/// Unlike `create_database`, this never opens an existing database. The profile owns the
/// performance configuration, so `ops.performance` must be `None`; the other operational
/// settings apply as in `create_database_with_ops`, including writing `ops.security`.
/// # Errors
/// Fails with `AlreadyInitialized` when the target already holds database metadata, with
/// `Unsupported` when `ops.performance` is set, with `ProfileManifestInvalid` when the binding
/// is inconsistent, and as `create_database_with_ops` otherwise.
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

/// Reads the metadata of the database file at `path` without creating anything.
///
/// A missing path is `NotFound` and an empty file `Uninitialized`, both without opening it.
/// Otherwise the existing redb file is opened, `__meta` is read through the frame-tolerant
/// reader and the handle is dropped before returning.
#[must_use]
pub fn inspect_database_metadata(path: &Path) -> MetadataProbe {
    let length = match std::fs::metadata(path) {
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

    let engine = match RedbEngine::open_existing(path, false) {
        ExistingRedbOpen::Opened(engine) => engine,
        ExistingRedbOpen::Locked => return MetadataProbe::Locked,
        ExistingRedbOpen::Failed(error) => {
            return MetadataProbe::Unreadable {
                reason: error.to_string(),
            };
        }
    };
    let probe = match read_metadata(&engine) {
        Ok(read) => MetadataProbe::Initialized(Box::new(ProbedMetadata {
            schema_version: read.metadata.schema_version,
            storage_profile: read.metadata.profile,
            replication: read.metadata.replication,
            layout: read.metadata.layout,
            manifest: read.manifest,
            values_framed: read.values_framed,
        })),
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            MetadataProbe::Uninitialized
        }
        Err(error) => MetadataProbe::Unreadable {
            reason: error.to_string(),
        },
    };
    drop(engine);
    probe
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
/// recorded in `__meta`, every knob must be in range, the knobs must derive that storage class
/// under the recorded variant, and the storage format must agree with the compression knobs.
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
    if let Some(issue) = check_knob_ranges(&manifest.knobs).first() {
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
