#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::json;

use crate::config_spec::{
    CollectionIndexKind, CollectionRole, CollectionRoleSpec, ColumnSpec, ColumnTypeSpec,
    ConsistencyMode, DATABASE_SPEC_VERSION_V2, DatabaseDefaults, DatabaseSpec, DeploymentMode,
    DeploymentSpec, DocIndexSpec, ExtensionManifestRef, ExtensionStability, GuaranteeSpec,
    GuaranteeValidator, PhysicalKind, PhysicalObjectSpec, ReplicationMode, SupportStatus,
    TopologySpec, ValidationReport, VectorMetricSpec, built_in_extension_manifest,
    built_in_profile, collection_role_definition, consistency_domain_definition,
    index_required_extensions, role_required_extensions,
};

const DEFAULT_DOMAIN: &str = "primary";
/// The spec version built-in templates emit: version 2, because they carry the `physical`
/// parameters provisioning needs. The version 1 documents they emitted before are frozen under
/// `tests/fixtures/spec-v1`.
pub const TEMPLATE_SPEC_VERSION: u32 = DATABASE_SPEC_VERSION_V2;
/// One hour, the chunk width of the built-in time series.
const HOUR_MILLIS: i64 = 3_600_000;
/// Thirty days, the retention of the desktop usage metrics.
const THIRTY_DAYS_MILLIS: i64 = 2_592_000_000;
/// The embedding dimension of the AI memory template. It is fixed at creation.
const AI_MEMORY_DIM: u32 = 1_536;

const PRIMARY_INDEX: &[CollectionIndexKind] = &[CollectionIndexKind::Primary];
const DOCUMENT_INDEX: &[CollectionIndexKind] = &[CollectionIndexKind::Document];
const VECTOR_INDEX: &[CollectionIndexKind] = &[CollectionIndexKind::Vector];
const COLUMNAR_INDEX: &[CollectionIndexKind] = &[CollectionIndexKind::Columnar];
const TIME_SERIES_INDEX: &[CollectionIndexKind] = &[CollectionIndexKind::TimeSeries];

const GAME_COLLECTIONS: &[TemplateCollection] = &[
    TemplateCollection {
        name: "saves",
        role: CollectionRole::DocumentEntity,
        indexes: DOCUMENT_INDEX,
        description: "Versioned save slots and game-world snapshots.",
    },
    TemplateCollection {
        name: "player_state",
        role: CollectionRole::KeyValue,
        indexes: PRIMARY_INDEX,
        description: "Fast direct lookup for the current player profile.",
    },
    TemplateCollection {
        name: "session_events",
        role: CollectionRole::EventLog,
        indexes: PRIMARY_INDEX,
        description: "Replayable local events for debugging and sync bridges.",
    },
    TemplateCollection {
        name: "asset_cache",
        role: CollectionRole::Cache,
        indexes: PRIMARY_INDEX,
        description: "Regenerable cache records safe to rebuild.",
    },
];

const DESKTOP_COLLECTIONS: &[TemplateCollection] = &[
    TemplateCollection {
        name: "documents",
        role: CollectionRole::DocumentEntity,
        indexes: DOCUMENT_INDEX,
        description: "Durable user documents and settings payloads.",
    },
    TemplateCollection {
        name: "settings",
        role: CollectionRole::KeyValue,
        indexes: PRIMARY_INDEX,
        description: "Application preferences addressed by stable keys.",
    },
    TemplateCollection {
        name: "audit_log",
        role: CollectionRole::Audit,
        indexes: PRIMARY_INDEX,
        description: "Tamper-evident local operator and sync audit records.",
    },
    TemplateCollection {
        name: "usage_metrics",
        role: CollectionRole::TimeSeries,
        indexes: TIME_SERIES_INDEX,
        description: "Local time-series telemetry with bounded retention.",
    },
];

const AI_COLLECTIONS: &[TemplateCollection] = &[
    TemplateCollection {
        name: "memories",
        role: CollectionRole::VectorMemory,
        indexes: VECTOR_INDEX,
        description: "Embedding memory for similarity lookup.",
    },
    TemplateCollection {
        name: "facts",
        role: CollectionRole::DocumentEntity,
        indexes: DOCUMENT_INDEX,
        description: "Structured facts and long-term notes.",
    },
    TemplateCollection {
        name: "conversation_events",
        role: CollectionRole::EventLog,
        indexes: PRIMARY_INDEX,
        description: "Replayable interaction history.",
    },
    TemplateCollection {
        name: "scratch_cache",
        role: CollectionRole::Cache,
        indexes: PRIMARY_INDEX,
        description: "Regenerable short-lived context.",
    },
];

const SECURE_COLLECTIONS: &[TemplateCollection] = &[
    TemplateCollection {
        name: "tenants",
        role: CollectionRole::DocumentEntity,
        indexes: DOCUMENT_INDEX,
        description: "Tenant metadata and account state.",
    },
    TemplateCollection {
        name: "app_state",
        role: CollectionRole::KeyValue,
        indexes: PRIMARY_INDEX,
        description: "Transactional application control records.",
    },
    TemplateCollection {
        name: "audit_log",
        role: CollectionRole::Audit,
        indexes: PRIMARY_INDEX,
        description: "Security audit trail owned by the core audit path.",
    },
    TemplateCollection {
        name: "outbox",
        role: CollectionRole::EventLog,
        indexes: PRIMARY_INDEX,
        description: "Durable integration events for external delivery.",
    },
];

const ANALYTICS_COLLECTIONS: &[TemplateCollection] = &[
    TemplateCollection {
        name: "events",
        role: CollectionRole::EventLog,
        indexes: PRIMARY_INDEX,
        description: "Append-oriented raw facts for replay and CDC.",
    },
    TemplateCollection {
        name: "metrics",
        role: CollectionRole::TimeSeries,
        indexes: TIME_SERIES_INDEX,
        description: "Timestamped measurements for rollups.",
    },
    TemplateCollection {
        name: "aggregates",
        role: CollectionRole::Analytics,
        indexes: COLUMNAR_INDEX,
        description: "Columnar aggregates for scans and reporting.",
    },
];

const BUILT_IN_TEMPLATES: &[TemplateSpec] = &[
    TemplateSpec {
        slug: "game-save",
        aliases: &["game", "game_save"],
        title: "Game Save",
        profile: "game_local_balanced",
        description: "Local-first save data for games and lightweight embedded state.",
        why_profile: "Uses the certified local snapshot profile so game state remains simple, fast and explicit about not claiming cross-node quorum semantics.",
        known_limits: &[
            "No cross-device sync is configured by default.",
            "Cache data must be safe to rebuild.",
            "Use the generated event log as an integration boundary rather than as a remote replication claim.",
            "The app chooses the database path; init --apply creates <out dir>/<name>.redb unless --db is given.",
        ],
        collections: GAME_COLLECTIONS,
    },
    TemplateSpec {
        slug: "desktop-embedded",
        aliases: &["desktop", "desktop_embedded"],
        title: "Desktop Embedded",
        profile: "desktop_app_embedded",
        description: "Durable embedded storage for desktop applications.",
        why_profile: "Uses the certified embedded desktop profile to keep local durability, document data and audit visibility in one validated spec.",
        known_limits: &[
            "The template is local-first and does not configure remote cluster membership.",
            "Application migrations still go through config plan and operator review.",
        ],
        collections: DESKTOP_COLLECTIONS,
    },
    TemplateSpec {
        slug: "ai-memory",
        aliases: &["ai", "ai_memory", "agent-memory"],
        title: "AI Memory",
        profile: "ai_agent_memory",
        description: "Vector memory plus document facts and replayable context.",
        why_profile: "Uses the stable AI memory profile because vector collections, document facts, event context and cache data are all part of its support catalog.",
        known_limits: &[
            "Embedding generation is intentionally outside the database.",
            "Vector indexing can lag writes; inspect explain config before treating results as strongly fresh.",
            "dim is fixed at creation; edit physical.dim to match your embedding model before --apply.",
        ],
        collections: AI_COLLECTIONS,
    },
    TemplateSpec {
        slug: "secure-saas",
        aliases: &["secure", "secure_saas", "saas"],
        title: "Secure SaaS",
        profile: "secure_app",
        description: "Security-focused transactional application state.",
        why_profile: "Uses the stable secure_app profile with strong CP intent, backup, PITR, encryption and audit enabled while keeping enterprise SLA concerns explicit.",
        known_limits: &[
            "The template does not configure Kubernetes automation, multi-region placement or enterprise SLA.",
            "Encryption at rest is provisioned through the Rust API only: init --apply, config apply --spec, multidb serve spec mode and the Control Plane refuse this spec with GUARANTEE_NOT_ENFORCEABLE.",
            "Provisioning creates and adopts objects but never drops, alters or re-encrypts them; config apply --plan stays confirm/audit-only.",
        ],
        collections: SECURE_COLLECTIONS,
    },
    TemplateSpec {
        slug: "analytics",
        aliases: &["analytics-columnar", "analytics_columnar"],
        title: "Analytics",
        profile: "analytics_columnar",
        description: "Columnar analytics starter for events, metrics and aggregate scans.",
        why_profile: "Uses the stable analytics profile to make event logs, time-series and columnar aggregate intent explicit in DatabaseSpec.",
        known_limits: &[
            "columnar_layout is currently Experimental in the extension catalog.",
            "This starter is optimized for read-heavy analytical paths, not OLTP writes.",
            "Table columns are fixed at creation; edit aggregates physical.columns before --apply.",
        ],
        collections: ANALYTICS_COLLECTIONS,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct TemplateSpec {
    pub slug: &'static str,
    pub aliases: &'static [&'static str],
    pub title: &'static str,
    pub profile: &'static str,
    pub description: &'static str,
    pub why_profile: &'static str,
    pub known_limits: &'static [&'static str],
    pub collections: &'static [TemplateCollection],
}

impl TemplateSpec {
    /// The `physical` parameters the template gives `collection`, so provisioning can build it;
    /// `None` for a collection that needs none (a document collection with only the `primary`
    /// index) and for unknown names.
    #[must_use]
    pub fn physical(&self, collection: &str) -> Option<PhysicalObjectSpec> {
        template_physical(self.slug, collection)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct TemplateCollection {
    pub name: &'static str,
    pub role: CollectionRole,
    pub indexes: &'static [CollectionIndexKind],
    pub description: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TemplateFile {
    pub path: String,
    pub contents: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateMaterialization {
    pub template_slug: &'static str,
    pub spec: DatabaseSpec,
    pub validation: ValidationReport,
    pub files: Vec<TemplateFile>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateOutputFormat {
    Json,
    Yaml,
}

#[derive(thiserror::Error, Debug)]
pub enum TemplateError {
    #[error("unknown template {name}; run multidb template list")]
    UnknownTemplate { name: String },

    #[error("template {template} references unknown profile {profile}")]
    UnknownProfile {
        template: &'static str,
        profile: &'static str,
    },

    #[error("template {template} references unknown extension {extension}")]
    UnknownExtension {
        template: &'static str,
        extension: String,
    },

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("yaml: {0}")]
    Yaml(#[from] serde_norway::Error),
}

impl TemplateOutputFormat {
    #[must_use]
    pub const fn spec_file_name(self) -> &'static str {
        match self {
            Self::Json => "multidb.json",
            Self::Yaml => "multidb.yaml",
        }
    }
}

#[must_use]
pub const fn built_in_templates() -> &'static [TemplateSpec] {
    BUILT_IN_TEMPLATES
}

#[must_use]
pub fn built_in_template(slug_or_alias: &str) -> Option<&'static TemplateSpec> {
    let key = normalize_template_key(slug_or_alias);
    built_in_templates()
        .iter()
        .find(|template| template.slug == key || template.aliases.iter().any(|alias| *alias == key))
}

pub fn materialize_template(
    slug_or_alias: &str,
    name: &str,
    format: TemplateOutputFormat,
) -> Result<TemplateMaterialization, TemplateError> {
    let template =
        built_in_template(slug_or_alias).ok_or_else(|| TemplateError::UnknownTemplate {
            name: slug_or_alias.to_owned(),
        })?;
    let spec = template_database_spec(template, name)?;
    let validation = GuaranteeValidator::validate(&spec);
    let spec_contents = match format {
        TemplateOutputFormat::Json => serde_json::to_string_pretty(&spec)?,
        TemplateOutputFormat::Yaml => serde_norway::to_string(&spec)?,
    };
    let files = vec![
        TemplateFile {
            path: format.spec_file_name().to_owned(),
            contents: spec_contents,
        },
        TemplateFile {
            path: "README.md".to_owned(),
            contents: template_readme(template, &spec, &validation)?,
        },
        TemplateFile {
            path: "seed.json".to_owned(),
            contents: template_seed_json(template)?,
        },
        TemplateFile {
            path: "smoke.ps1".to_owned(),
            contents: template_smoke_script(format.spec_file_name()),
        },
    ];

    Ok(TemplateMaterialization {
        template_slug: template.slug,
        spec,
        validation,
        files,
    })
}

fn template_database_spec(
    template: &'static TemplateSpec,
    name: &str,
) -> Result<DatabaseSpec, TemplateError> {
    let Some(profile) = built_in_profile(template.profile) else {
        return Err(TemplateError::UnknownProfile {
            template: template.slug,
            profile: template.profile,
        });
    };
    let replication = replication_for_domain(profile.default_domain);
    let mut guarantees = GuaranteeSpec::for_replication(replication);
    apply_template_guarantees(template.slug, &mut guarantees);
    let durable_storage = template.slug != "game-save";

    Ok(DatabaseSpec {
        version: TEMPLATE_SPEC_VERSION,
        name: name.to_owned(),
        profile: profile.slug.to_owned(),
        deployment: DeploymentSpec {
            mode: if durable_storage {
                DeploymentMode::SingleNode
            } else {
                DeploymentMode::Embedded
            },
            storage_path: durable_storage.then(|| storage_file_name(name)),
        },
        topology: TopologySpec::default(),
        defaults: DatabaseDefaults {
            consistency_domain: DEFAULT_DOMAIN.to_owned(),
            replication,
        },
        guarantees,
        domains: vec![crate::config_spec::ConsistencyDomainSpec {
            name: DEFAULT_DOMAIN.to_owned(),
            mode: profile.default_domain,
        }],
        collections: template_collections(template),
        extensions: template_extensions(template)?,
        overrides: BTreeMap::new(),
        operation_hints: template_operation_hints(template),
        custom_profile: None,
        security: None,
        provisioning: None,
    })
}

fn apply_template_guarantees(template_slug: &str, guarantees: &mut GuaranteeSpec) {
    if template_slug == "secure-saas" {
        guarantees.backup.enabled = true;
        guarantees.backup.pitr = true;
        guarantees.encryption.at_rest = true;
        guarantees.audit.enabled = true;
        guarantees.sensitive_data = true;
        guarantees.strict_cross_domain_transactions = true;
    }
}

fn template_collections(template: &TemplateSpec) -> Vec<CollectionRoleSpec> {
    template
        .collections
        .iter()
        .map(|collection| CollectionRoleSpec {
            name: collection.name.to_owned(),
            role: collection.role,
            domain: DEFAULT_DOMAIN.to_owned(),
            indexes: collection.indexes.to_vec(),
            physical: template_physical(template.slug, collection.name),
        })
        .collect()
}

/// The physical parameters of the built-in templates: a document index path for every
/// `document` index kind, the vector dimension and metric, the time-series chunk width and
/// retention, and the columns of the analytics table. Everything else takes the documented
/// defaults (`docs/provisioning.md`).
fn template_physical(template_slug: &str, collection: &str) -> Option<PhysicalObjectSpec> {
    let document_index = |path: &str| PhysicalObjectSpec {
        document_indexes: vec![DocIndexSpec {
            path: path.to_owned(),
            id: None,
        }],
        ..PhysicalObjectSpec::default()
    };
    let column = |name: &str, ty: ColumnTypeSpec| ColumnSpec {
        name: name.to_owned(),
        ty,
        nullable: false,
    };
    match (template_slug, collection) {
        ("game-save", "saves") => Some(document_index("slot")),
        ("desktop-embedded", "documents") => Some(document_index("title")),
        ("desktop-embedded", "usage_metrics") => Some(PhysicalObjectSpec {
            chunk_millis: Some(HOUR_MILLIS),
            retention_millis: Some(THIRTY_DAYS_MILLIS),
            ..PhysicalObjectSpec::default()
        }),
        ("ai-memory", "memories") => Some(PhysicalObjectSpec {
            dim: Some(AI_MEMORY_DIM),
            metric: Some(VectorMetricSpec::Cosine),
            ..PhysicalObjectSpec::default()
        }),
        ("ai-memory", "facts") => Some(document_index("subject")),
        ("secure-saas", "tenants") => Some(document_index("tenant_id")),
        ("analytics", "metrics") => Some(PhysicalObjectSpec {
            chunk_millis: Some(HOUR_MILLIS),
            ..PhysicalObjectSpec::default()
        }),
        ("analytics", "aggregates") => Some(PhysicalObjectSpec {
            kind: Some(PhysicalKind::Table),
            columns: vec![
                column("id", ColumnTypeSpec::Str),
                column("bucket", ColumnTypeSpec::Str),
                column("metric", ColumnTypeSpec::Str),
                column("value", ColumnTypeSpec::Float),
            ],
            primary_key: Some("id".to_owned()),
            ..PhysicalObjectSpec::default()
        }),
        _ => None,
    }
}

fn template_extensions(
    template: &'static TemplateSpec,
) -> Result<Vec<ExtensionManifestRef>, TemplateError> {
    let mut extension_names = BTreeSet::new();
    for collection in template.collections {
        for extension in role_required_extensions(collection.role) {
            extension_names.insert((*extension).to_owned());
        }
        for extension in index_required_extensions(collection.indexes) {
            extension_names.insert(extension);
        }
    }

    let mut extensions = Vec::with_capacity(extension_names.len());
    for extension in extension_names {
        let Some(manifest) = built_in_extension_manifest(&extension) else {
            return Err(TemplateError::UnknownExtension {
                template: template.slug,
                extension,
            });
        };
        extensions.push(ExtensionManifestRef {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            stability: if manifest.status == SupportStatus::Experimental {
                ExtensionStability::Experimental
            } else {
                ExtensionStability::Stable
            },
        });
    }
    Ok(extensions)
}

fn template_operation_hints(template: &TemplateSpec) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("template.slug".to_owned(), template.slug.to_owned()),
        (
            "template.profile_reason".to_owned(),
            template.why_profile.to_owned(),
        ),
    ])
}

const fn replication_for_domain(domain: ConsistencyMode) -> ReplicationMode {
    match domain {
        ConsistencyMode::EventualAp => ReplicationMode::Ap,
        ConsistencyMode::LocalSnapshot | ConsistencyMode::StrongCp => ReplicationMode::Cp,
    }
}

fn storage_file_name(name: &str) -> String {
    let mut file_name = String::with_capacity(name.len() + ".redb".len());
    for character in name.chars() {
        if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
            file_name.push(character.to_ascii_lowercase());
        } else {
            file_name.push('_');
        }
    }
    if file_name.is_empty() {
        file_name.push_str("multidb");
    }
    file_name.push_str(".redb");
    file_name
}

fn template_readme(
    template: &TemplateSpec,
    spec: &DatabaseSpec,
    validation: &ValidationReport,
) -> Result<String, TemplateError> {
    let mut lines = vec![
        format!("# {} Template", template.title),
        String::new(),
        template.description.to_owned(),
        String::new(),
        "## Why This Profile".to_owned(),
        String::new(),
        template.why_profile.to_owned(),
        String::new(),
        "## Generated Contract".to_owned(),
        String::new(),
        format!("- template: {}", template.slug),
        format!("- spec_version: {}", spec.version),
        format!("- profile: {}", spec.profile),
        format!("- validation_status: {:?}", validation.status),
        format!("- validation_valid: {}", validation.valid),
        format!(
            "- consistency_domain: {} ({})",
            spec.defaults.consistency_domain,
            domain_slug(spec.domains[0].mode)
        ),
        format!("- replication: {:?}", spec.defaults.replication),
        String::new(),
        "## Collections".to_owned(),
        String::new(),
    ];

    for collection in template.collections {
        lines.push(format!(
            "- {}: role={} indexes={} - {}",
            collection.name,
            role_slug(collection.role),
            collection
                .indexes
                .iter()
                .map(|index| index_slug(*index))
                .collect::<Vec<_>>()
                .join(","),
            collection.description
        ));
        if let Some(physical) = template.physical(collection.name) {
            lines.push(format!(
                "  - physical: `{}`",
                serde_json::to_string(&physical)?
            ));
        }
    }

    lines.extend([
        String::new(),
        "## Validate And Explain".to_owned(),
        String::new(),
        "```powershell".to_owned(),
        ".\\smoke.ps1".to_owned(),
        "multidb config validate --spec .\\multidb.yaml".to_owned(),
        "multidb config explain --spec .\\multidb.yaml".to_owned(),
        "```".to_owned(),
        String::new(),
    ]);
    lines.extend(template_provisioning_section(template, spec));
    lines.extend(["## Known Limits".to_owned(), String::new()]);
    lines.extend(
        template
            .known_limits
            .iter()
            .map(|limit| format!("- {limit}")),
    );
    lines.push(String::new());
    lines.push(
        "Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter."
            .to_owned(),
    );
    lines.push(String::new());
    Ok(lines.join("\n"))
}

/// The README section on building the database the spec describes, with the guarantee statuses
/// a CLI plan reports for it.
fn template_provisioning_section(template: &TemplateSpec, spec: &DatabaseSpec) -> Vec<String> {
    let db_file = spec
        .deployment
        .storage_path
        .clone()
        .unwrap_or_else(|| storage_file_name(&spec.name));
    let mut lines = vec![
        "## Provision".to_owned(),
        String::new(),
        "This spec is version 2: it carries the physical parameters provisioning needs to build"
            .to_owned(),
        "every collection (see `docs/provisioning.md`).".to_owned(),
        String::new(),
    ];
    if spec.guarantees.encryption.at_rest {
        lines.extend([
            "It requests encryption at rest, which this build provisions through the Rust API"
                .to_owned(),
            "only: `config plan --spec` on the CLI is blocked with `GUARANTEE_NOT_ENFORCEABLE`,"
                .to_owned(),
            "and `init --apply`, `config apply --spec`, `multidb serve` spec mode and the Control"
                .to_owned(),
            "Plane refuse it. Create the database with `provision::provision_new_database` on the"
                .to_owned(),
            "`rust_api` surface, with `ProvisionEnvironment.encryption` set to".to_owned(),
            "`EncryptionConfig::local_envelope(<keyring>, <kek>)`. The dry run shows the reason:"
                .to_owned(),
            String::new(),
            "```powershell".to_owned(),
            format!("multidb config plan --spec .\\multidb.yaml --db .\\{db_file}"),
            "```".to_owned(),
        ]);
    } else {
        lines.extend([
            format!(
                "`multidb init --guided --template {} --name <name> --out <dir> --apply` writes",
                template.slug
            ),
            "these files and creates the database in one step. From this directory:".to_owned(),
            String::new(),
            "```powershell".to_owned(),
            format!("multidb config plan --spec .\\multidb.yaml --db .\\{db_file}"),
            format!("multidb config apply --spec .\\multidb.yaml --db .\\{db_file} --auto-approve"),
            format!("multidb config drift --spec .\\multidb.yaml --db .\\{db_file}"),
            "```".to_owned(),
            String::new(),
            "`config plan` is a dry run. `config apply --auto-approve` applies a plan that only"
                .to_owned(),
            "creates, adopts or records, and a second apply is a noop. `config drift` exits 3"
                .to_owned(),
            "when the database no longer matches the spec.".to_owned(),
        ]);
    }
    lines.extend([
        String::new(),
        "Guarantee statuses a CLI plan reports for this spec:".to_owned(),
        String::new(),
        "- `guarantees.audit.enabled`: `enforced` with `MULTIDB_AUDIT_KEY_PATH`; without it,"
            .to_owned(),
        "  local CLI usage reports `enforced_with_development_key` (`AUDIT_DEVELOPMENT_KEY`)."
            .to_owned(),
        "  Production `multidb serve` requires an existing private audit key and, for durable"
            .to_owned(),
        "  profiles, `MULTIDB_AUDIT_ANCHOR_PATH`. The Control Plane inherits these runtime"
            .to_owned(),
        "  settings. Existing audit history requires its original signing key; changing the"
            .to_owned(),
        "  path does not re-sign it. Follow the repository's `docs/docker.md` audit setup."
            .to_owned(),
    ]);
    if spec.guarantees.backup.enabled || spec.guarantees.backup.pitr {
        lines.extend([
            "- `guarantees.backup.enabled` and `guarantees.backup.pitr`: `operator_managed`. This"
                .to_owned(),
            "  build has no backup scheduler, so the plan prints the backup and restore runbook"
                .to_owned(),
            "  (`GUARANTEE_OPERATOR_MANAGED`).".to_owned(),
        ]);
    } else {
        lines.push(
            "- `guarantees.backup.enabled` and `guarantees.backup.pitr`: `not_requested`."
                .to_owned(),
        );
    }
    if spec.guarantees.encryption.at_rest {
        lines.push(
            "- `guarantees.encryption.at_rest`: `not_enforceable` on the CLI, `enforced` on the Rust API with a `LocalEnvelope` key."
                .to_owned(),
        );
    } else {
        lines.push("- `guarantees.encryption.at_rest`: `not_requested`.".to_owned());
    }
    lines.push(String::new());
    lines
}

fn template_seed_json(template: &TemplateSpec) -> Result<String, TemplateError> {
    let seed = match template.slug {
        "game-save" => json!({
            "template": template.slug,
            "records": {
                "saves": [{ "slot": "slot-1", "level": "intro", "playtime_seconds": 180 }],
                "player_state": [{ "key": "current_profile", "value": { "display_name": "Ada", "difficulty": "normal" } }],
                "session_events": [{ "event": "save_created", "slot": "slot-1" }]
            }
        }),
        "desktop-embedded" => json!({
            "template": template.slug,
            "records": {
                "documents": [{ "id": "welcome", "title": "Welcome", "body": "Edit locally." }],
                "settings": [{ "key": "theme", "value": "system" }],
                "usage_metrics": [{ "timestamp": "2026-01-01T00:00:00Z", "name": "open", "value": 1 }]
            }
        }),
        "ai-memory" => json!({
            "template": template.slug,
            "records": {
                "memories": [{ "id": "memory-1", "embedding": [0.1, 0.2, 0.3], "text": "User prefers concise plans." }],
                "facts": [{ "subject": "project", "predicate": "uses", "object": "DatabaseSpec" }],
                "conversation_events": [{ "event": "memory_seeded", "memory_id": "memory-1" }]
            }
        }),
        "secure-saas" => json!({
            "template": template.slug,
            "records": {
                "tenants": [{ "tenant_id": "tenant-demo", "plan": "starter" }],
                "app_state": [{ "key": "billing_mode", "value": "test" }],
                "outbox": [{ "event": "tenant_created", "tenant_id": "tenant-demo" }]
            }
        }),
        "analytics" => json!({
            "template": template.slug,
            "records": {
                "events": [{ "event": "page_view", "path": "/docs", "tenant": "demo" }],
                "metrics": [{ "timestamp": "2026-01-01T00:00:00Z", "name": "latency_ms", "value": 42 }],
                "aggregates": [{ "bucket": "2026-01-01", "metric": "page_views", "value": 1 }]
            }
        }),
        _ => json!({ "template": template.slug, "records": {} }),
    };
    serde_json::to_string_pretty(&seed).map_err(Into::into)
}

fn template_smoke_script(spec_file_name: &str) -> String {
    format!(
        r#"$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$IsWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)
$ExeName = if ($IsWindowsHost) {{ "multidb.exe" }} else {{ "multidb" }}
$Bin = Join-Path (Join-Path $RepoRoot "target") (Join-Path "debug" $ExeName)

if (-not (Test-Path $Bin)) {{
    Push-Location $RepoRoot
    cargo build --bin multidb
    Pop-Location
}}

$Spec = Join-Path $PSScriptRoot "{spec_file_name}"
& $Bin config validate --spec $Spec
if ($LASTEXITCODE -ne 0) {{ throw "template config validation failed" }}
& $Bin config explain --spec $Spec --json | ConvertFrom-Json | Out-Null
if ($LASTEXITCODE -ne 0) {{ throw "template config explain failed" }}
Get-Content (Join-Path $PSScriptRoot "seed.json") -Raw | ConvertFrom-Json | Out-Null
"template smoke ok: {spec_file_name}"
"#
    )
}

fn role_slug(role: CollectionRole) -> &'static str {
    collection_role_definition(role).map_or("unknown", |definition| definition.slug)
}

fn domain_slug(domain: ConsistencyMode) -> &'static str {
    consistency_domain_definition(domain).map_or("unknown", |definition| definition.slug)
}

const fn index_slug(index: CollectionIndexKind) -> &'static str {
    match index {
        CollectionIndexKind::Primary => "primary",
        CollectionIndexKind::Document => "document",
        CollectionIndexKind::Vector => "vector",
        CollectionIndexKind::Graph => "graph",
        CollectionIndexKind::FullText => "full_text",
        CollectionIndexKind::Columnar => "columnar",
        CollectionIndexKind::TimeSeries => "time_series",
        CollectionIndexKind::Geo => "geo",
    }
}

fn normalize_template_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    use super::{
        TEMPLATE_SPEC_VERSION, TemplateOutputFormat, built_in_template, built_in_templates,
        materialize_template, storage_file_name,
    };
    use crate::config_spec::{
        ConfigExplainer, DATABASE_SPEC_VERSION_V2, DatabaseSpec, ExtensionStability,
        GuaranteeValidator, ValidationSeverity, VectorMetricSpec, resolve_profile,
        spec_io::{SpecFormat, decode_spec_bytes},
    };
    use crate::db::EncryptionConfig;
    use crate::provision::{
        Confirmation, EnforcementStatus, PlanOutcome, ProvisionEnvironment, ProvisionOptions,
        ProvisionOutcome, ProvisionPlan, ProvisionSurface, ProvisionTargetRef, StartupDecision,
        StartupMode, open_from_spec, physical::project, plan_provision, provision_new_database,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn repo_file(relative: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
    }

    fn error_codes(plan: &ProvisionPlan) -> BTreeSet<&str> {
        plan.issues
            .iter()
            .filter(|issue| issue.severity == ValidationSeverity::Error)
            .map(|issue| issue.code.as_str())
            .collect()
    }

    fn plan_new(
        spec: &DatabaseSpec,
        path: &Path,
        env: &ProvisionEnvironment,
    ) -> Result<ProvisionPlan, Box<dyn std::error::Error>> {
        Ok(plan_provision(
            spec,
            ProvisionTargetRef::Path(path),
            &ProvisionOptions::default(),
            env,
        )?)
    }

    #[test]
    fn catalog_contains_phase_forty_seven_templates() {
        let slugs = built_in_templates()
            .iter()
            .map(|template| template.slug)
            .collect::<Vec<_>>();

        assert_eq!(
            slugs,
            vec![
                "game-save",
                "desktop-embedded",
                "ai-memory",
                "secure-saas",
                "analytics",
            ]
        );
    }

    #[test]
    fn aliases_resolve_to_templates() {
        let Some(game) = built_in_template("game_save") else {
            panic!("game_save alias must resolve");
        };
        assert_eq!(game.slug, "game-save");

        let Some(ai) = built_in_template("agent-memory") else {
            panic!("agent-memory alias must resolve");
        };
        assert_eq!(ai.slug, "ai-memory");
    }

    #[test]
    fn generated_specs_validate_and_explain() -> TestResult {
        assert_eq!(TEMPLATE_SPEC_VERSION, DATABASE_SPEC_VERSION_V2);
        for template in built_in_templates() {
            let materialized =
                materialize_template(template.slug, "Template Smoke", TemplateOutputFormat::Yaml)?;
            assert_eq!(materialized.spec.version, DATABASE_SPEC_VERSION_V2);
            let report = GuaranteeValidator::validate(&materialized.spec);
            assert!(
                report.valid,
                "{} should validate: {:?}",
                template.slug, report
            );
            assert_eq!(report, materialized.validation);
            // The physical parameters are complete: version 2 validation has no Advice that a
            // parameter is missing, and the YAML round-trips through the bounded reader.
            assert!(
                report
                    .issues
                    .iter()
                    .all(|issue| issue.code != "PHYSICAL_PARAMETER_REQUIRED"),
                "{}: {:?}",
                template.slug,
                report.issues
            );
            let decoded =
                decode_spec_bytes(materialized.files[0].contents.as_bytes(), SpecFormat::Yaml)?;
            assert_eq!(decoded, materialized.spec);
            let resolved = resolve_profile(&materialized.spec)
                .resolved()
                .cloned()
                .ok_or("the template profile resolves")?;
            let objects = project(&materialized.spec, &resolved)
                .map_err(|issues| format!("{}: {issues:#?}", template.slug))?;
            assert_eq!(
                objects.len(),
                template.collections.len(),
                "{}",
                template.slug
            );
            let explain = ConfigExplainer::explain(&materialized.spec);
            assert!(explain.validation.valid);
            assert!(explain.compiled_policy.is_some());
            assert!(
                explain
                    .decisions
                    .iter()
                    .any(|decision| decision.path == "$.compiled_policy.storage_profile"),
                "{} should explain the compiled storage profile",
                template.slug
            );
        }
        Ok(())
    }

    #[test]
    fn templates_carry_the_physical_parameters_provisioning_needs() -> TestResult {
        let ai = materialize_template("ai-memory", "Agent", TemplateOutputFormat::Yaml)?;
        let memories = ai.spec.collections[0]
            .physical
            .as_ref()
            .ok_or("memories physical")?;
        assert_eq!(memories.dim, Some(1_536));
        assert_eq!(memories.metric, Some(VectorMetricSpec::Cosine));
        let Some(template) = built_in_template("ai-memory") else {
            panic!("ai-memory must resolve");
        };
        assert_eq!(template.physical("memories").as_ref(), Some(memories));
        assert_eq!(template.physical("scratch_cache"), None);
        assert_eq!(template.physical("unknown"), None);

        let analytics = materialize_template("analytics", "Metrics", TemplateOutputFormat::Yaml)?;
        let aggregates = analytics.spec.collections[2]
            .physical
            .as_ref()
            .ok_or("aggregates physical")?;
        assert_eq!(aggregates.primary_key.as_deref(), Some("id"));
        assert_eq!(
            aggregates
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "bucket", "metric", "value"]
        );
        assert!(
            aggregates.table_indexes.is_empty(),
            "expected an empty value, got {:?}",
            aggregates.table_indexes
        );

        let desktop = materialize_template("desktop-embedded", "Desk", TemplateOutputFormat::Yaml)?;
        let usage = desktop.spec.collections[3]
            .physical
            .as_ref()
            .ok_or("usage_metrics physical")?;
        assert_eq!(usage.chunk_millis, Some(3_600_000));
        assert_eq!(usage.retention_millis, Some(2_592_000_000));

        // Only collections with a parameter-bearing index kind carry `physical`.
        let game = materialize_template("game-save", "Game", TemplateOutputFormat::Yaml)?;
        assert_eq!(
            game.spec
                .collections
                .iter()
                .map(|collection| collection.physical.is_some())
                .collect::<Vec<_>>(),
            [true, false, false, false]
        );
        assert_eq!(game.spec.deployment.storage_path, None);
        Ok(())
    }

    #[test]
    fn required_extensions_are_explicit() -> TestResult {
        let ai = materialize_template("ai-memory", "Agent", TemplateOutputFormat::Yaml)?;
        let extension_names = ai
            .spec
            .extensions
            .iter()
            .map(|extension| extension.name.as_str())
            .collect::<Vec<_>>();
        assert!(extension_names.contains(&"vector_hnsw"));
        assert!(extension_names.contains(&"document_index"));
        assert!(extension_names.contains(&"cdc"));

        let analytics = materialize_template("analytics", "Metrics", TemplateOutputFormat::Yaml)?;
        let Some(columnar) = analytics
            .spec
            .extensions
            .iter()
            .find(|extension| extension.name == "columnar_layout")
        else {
            panic!("analytics template must reference columnar_layout");
        };
        assert_eq!(columnar.stability, ExtensionStability::Experimental);

        // The physical parameters add no extension: every template requires exactly the
        // extensions its frozen version 1 spec did.
        for template in built_in_templates() {
            let frozen = decode_spec_bytes(
                &std::fs::read(repo_file(&format!(
                    "tests/fixtures/spec-v1/{}.yaml",
                    template.slug
                )))?,
                SpecFormat::Yaml,
            )?;
            let materialized =
                materialize_template(template.slug, &frozen.name, TemplateOutputFormat::Yaml)?;
            assert_eq!(
                materialized.spec.extensions, frozen.extensions,
                "{}",
                template.slug
            );
        }
        Ok(())
    }

    #[test]
    fn templates_plan_ready_on_the_cli_except_secure_saas() -> TestResult {
        let temp_dir = tempfile::tempdir()?;
        let cli = ProvisionEnvironment::new(ProvisionSurface::Cli);
        for slug in ["game-save", "desktop-embedded", "ai-memory", "analytics"] {
            let materialized = materialize_template(slug, "Plan Me", TemplateOutputFormat::Yaml)?;
            let path = temp_dir.path().join(format!("{slug}.redb"));
            let plan = plan_new(&materialized.spec, &path, &cli)?;
            assert_eq!(
                plan.outcome,
                PlanOutcome::Ready,
                "{slug}: {:#?}",
                plan.issues
            );
            assert!(plan.auto_approvable, "{slug}");
            assert!(!path.exists(), "{slug}: a dry run creates no file");
        }

        // secure-saas asks for encryption at rest: the CLI cannot enforce it, and that is the
        // only blocker. The Rust API with a LocalEnvelope key can.
        let secure = materialize_template("secure-saas", "Secure", TemplateOutputFormat::Yaml)?;
        let path = temp_dir.path().join("secure.redb");
        let blocked = plan_new(&secure.spec, &path, &cli)?;
        assert_eq!(blocked.outcome, PlanOutcome::Blocked);
        assert!(blocked.validation.valid);
        assert_eq!(
            error_codes(&blocked),
            BTreeSet::from(["GUARANTEE_NOT_ENFORCEABLE"]),
            "{:#?}",
            blocked.issues
        );
        for surface in [ProvisionSurface::Serve, ProvisionSurface::ControlPlane] {
            let plan = plan_new(&secure.spec, &path, &ProvisionEnvironment::new(surface))?;
            assert_eq!(
                error_codes(&plan),
                BTreeSet::from(["GUARANTEE_NOT_ENFORCEABLE"]),
                "{surface:?}"
            );
        }
        let rust_api = ProvisionEnvironment {
            encryption: Some(EncryptionConfig::local_envelope(
                temp_dir.path().join("keyring.json"),
                temp_dir.path().join("kek.bin"),
            )),
            ..ProvisionEnvironment::new(ProvisionSurface::RustApi)
        };
        let ready = plan_new(&secure.spec, &path, &rust_api)?;
        assert_eq!(ready.outcome, PlanOutcome::Ready, "{:#?}", ready.issues);
        assert!(error_codes(&ready).is_empty());
        Ok(())
    }

    #[test]
    fn templates_provision_and_converge_to_a_noop() -> TestResult {
        let temp_dir = tempfile::tempdir()?;
        let cli = ProvisionEnvironment::new(ProvisionSurface::Cli);
        for slug in ["game-save", "desktop-embedded", "ai-memory", "analytics"] {
            let materialized =
                materialize_template(slug, "Converge Me", TemplateOutputFormat::Yaml)?;
            let path = temp_dir.path().join(storage_file_name(slug));
            let (db, report) = provision_new_database(
                &materialized.spec,
                Some(&path),
                &Confirmation::AutoApproveAdditive,
                &ProvisionOptions::default(),
                &cli,
            )?;
            assert_eq!(
                report.status,
                ProvisionOutcome::Applied,
                "{slug}: {report:#?}"
            );
            for collection in &materialized.spec.collections {
                assert!(
                    db.catalog().contains_key(&collection.name),
                    "{slug}: {}",
                    collection.name
                );
            }
            let again = plan_provision(
                &materialized.spec,
                ProvisionTargetRef::Open(&db),
                &ProvisionOptions::default(),
                &cli,
            )?;
            assert_eq!(
                again.outcome,
                PlanOutcome::Noop,
                "{slug}: {:#?}",
                again.steps
            );
        }

        let secure = materialize_template("secure-saas", "Secure", TemplateOutputFormat::Yaml)?;
        let kek_path = temp_dir.path().join("kek.bin");
        std::fs::write(&kek_path, [7_u8; 32])?;
        let rust_api = ProvisionEnvironment {
            encryption: Some(EncryptionConfig::local_envelope(
                temp_dir.path().join("keyring.json"),
                &kek_path,
            )),
            ..ProvisionEnvironment::new(ProvisionSurface::RustApi)
        };
        let secure_path = temp_dir.path().join("secure.redb");
        let (db, report) = provision_new_database(
            &secure.spec,
            Some(&secure_path),
            &Confirmation::AutoApproveAdditive,
            &ProvisionOptions::default(),
            &rust_api,
        )?;
        assert_eq!(report.status, ProvisionOutcome::Applied, "{report:#?}");
        assert!(db.catalog().contains_key("tenants"));
        let again = plan_provision(
            &secure.spec,
            ProvisionTargetRef::Open(&db),
            &ProvisionOptions::default(),
            &rust_api,
        )?;
        assert_eq!(again.outcome, PlanOutcome::Noop, "{:#?}", again.steps);
        assert!(again.guarantees.iter().any(|entry| {
            entry.path == "guarantees.encryption.at_rest"
                && entry.status == EnforcementStatus::Enforced
        }));
        drop(db);

        // The documented embedded route: open_from_spec opens it with the key and starts it.
        let (db, started) = open_from_spec(
            &secure.spec,
            Some(&secure_path),
            StartupMode::Verify,
            &ProvisionOptions::default(),
            &rust_api,
        )?;
        assert_eq!(started.decision, StartupDecision::Start);
        assert!(db.catalog().contains_key("tenants"));
        Ok(())
    }

    #[test]
    fn checked_in_examples_equal_the_materialized_templates() -> TestResult {
        for template in built_in_templates() {
            let path = repo_file(&format!("examples/{}/multidb.yaml", template.slug));
            let checked_in = std::fs::read_to_string(&path)?;
            let spec = decode_spec_bytes(checked_in.as_bytes(), SpecFormat::Yaml)?;
            let materialized =
                materialize_template(template.slug, &spec.name, TemplateOutputFormat::Yaml)?;
            assert_eq!(
                checked_in,
                materialized.files[0].contents,
                "{} is stale; regenerate it with multidb init --guided --template {} --name \"{}\"",
                path.display(),
                template.slug,
                spec.name
            );
            // The seed and smoke generators did not change with spec version 2. The checked-in
            // seeds end with a newline, so they are compared as JSON.
            let read_example = |file: &str| {
                std::fs::read_to_string(repo_file(&format!("examples/{}/{file}", template.slug)))
            };
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&read_example("seed.json")?)?,
                serde_json::from_str::<serde_json::Value>(&materialized.files[2].contents)?,
                "{} seed.json",
                template.slug
            );
            assert_eq!(
                read_example("smoke.ps1")?,
                materialized.files[3].contents,
                "{} smoke.ps1",
                template.slug
            );
        }
        Ok(())
    }

    #[test]
    fn provisioning_example_validates_projects_and_plans() -> TestResult {
        let spec = decode_spec_bytes(
            include_bytes!("../examples/provisioning/multidb.yaml"),
            SpecFormat::Yaml,
        )?;
        assert_eq!(spec.version, DATABASE_SPEC_VERSION_V2);
        let report = GuaranteeValidator::validate(&spec);
        assert!(report.valid, "{report:#?}");
        assert!(
            report
                .issues
                .iter()
                .all(|issue| issue.severity != ValidationSeverity::Error),
            "{report:#?}"
        );
        let resolved = resolve_profile(&spec)
            .resolved()
            .cloned()
            .ok_or("the example profile resolves")?;
        let objects = project(&spec, &resolved).map_err(|issues| format!("{issues:#?}"))?;
        let names = objects
            .iter()
            .map(|object| (object.name.as_str(), object.kind.slug()))
            .collect::<Vec<_>>();
        for expected in [
            ("orders", "document"),
            ("orders_notes_fts", "full_text"),
            ("orders_shipping_location_geo", "geo"),
            ("ledger", "table"),
            ("ledger_history", "history"),
            ("memories", "vector"),
            ("daily_rollups", "table"),
        ] {
            assert!(names.contains(&expected), "{expected:?} in {names:?}");
        }

        let temp_dir = tempfile::tempdir()?;
        let plan = plan_new(
            &spec,
            &temp_dir.path().join("orders.redb"),
            &ProvisionEnvironment::new(ProvisionSurface::Cli),
        )?;
        assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
        Ok(())
    }

    #[test]
    fn readme_describes_provisioning_and_guarantee_statuses() -> TestResult {
        let game = materialize_template("game-save", "Arcade Save", TemplateOutputFormat::Yaml)?;
        let readme = &game.files[1].contents;
        assert!(readme.contains("- spec_version: 2"), "{readme}");
        assert!(readme.contains("## Provision"), "{readme}");
        assert!(
            readme.contains("multidb config drift --spec .\\multidb.yaml --db .\\arcade_save.redb"),
            "{readme}"
        );
        assert!(
            readme.contains("`enforced_with_development_key`"),
            "{readme}"
        );
        assert!(readme.contains("pitr`: `not_requested`"), "{readme}");
        assert!(readme.contains(r#"physical: `{"document_indexes":[{"path":"slot"}]}`"#));

        let secure =
            materialize_template("secure-saas", "Secure SaaS", TemplateOutputFormat::Yaml)?;
        let readme = &secure.files[1].contents;
        assert!(readme.contains("GUARANTEE_NOT_ENFORCEABLE"), "{readme}");
        assert!(readme.contains("`operator_managed`"), "{readme}");
        assert!(!readme.contains("--auto-approve"), "{readme}");
        Ok(())
    }

    #[test]
    fn materialized_files_have_expected_names() -> TestResult {
        let yaml = materialize_template("secure-saas", "Secure", TemplateOutputFormat::Yaml)?;
        let yaml_paths = yaml
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            yaml_paths,
            vec!["multidb.yaml", "README.md", "seed.json", "smoke.ps1"]
        );

        let json = materialize_template("secure-saas", "Secure", TemplateOutputFormat::Json)?;
        let json_paths = json
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(json_paths[0], "multidb.json");
        Ok(())
    }
}
