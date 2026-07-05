#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::json;

use crate::config_spec::{
    CollectionIndexKind, CollectionRole, CollectionRoleSpec, ConsistencyMode, DatabaseDefaults,
    DatabaseSpec, DeploymentMode, DeploymentSpec, ExtensionManifestRef, ExtensionStability,
    GuaranteeSpec, GuaranteeValidator, ReplicationMode, SupportStatus, TopologySpec,
    ValidationReport, built_in_extension_manifest, built_in_profile, collection_role_definition,
    consistency_domain_definition,
};

const DEFAULT_DOMAIN: &str = "primary";
const TEMPLATE_SPEC_VERSION: u32 = crate::config_spec::DATABASE_SPEC_VERSION;

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
            "Physical config apply remains confirm/audit-only in v1.",
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
    Yaml(#[from] serde_yaml::Error),
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
        TemplateOutputFormat::Yaml => serde_yaml::to_string(&spec)?,
    };
    let files = vec![
        TemplateFile {
            path: format.spec_file_name().to_owned(),
            contents: spec_contents,
        },
        TemplateFile {
            path: "README.md".to_owned(),
            contents: template_readme(template, &spec, &validation),
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
        })
        .collect()
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

fn role_required_extensions(role: CollectionRole) -> &'static [&'static str] {
    match role {
        CollectionRole::EventLog => &["cdc"],
        CollectionRole::VectorMemory => &["vector_hnsw"],
        CollectionRole::Audit => &["audit"],
        CollectionRole::Graph => &["graph_index"],
        CollectionRole::Analytics => &["columnar_layout"],
        CollectionRole::TimeSeries => &["time_series"],
        CollectionRole::DocumentEntity | CollectionRole::KeyValue | CollectionRole::Cache => &[],
    }
}

fn index_required_extensions(indexes: &[CollectionIndexKind]) -> Vec<String> {
    let mut required = BTreeSet::new();
    for index in indexes {
        match index {
            CollectionIndexKind::Document => {
                required.insert("document_index");
            }
            CollectionIndexKind::Vector => {
                required.insert("vector_hnsw");
            }
            CollectionIndexKind::Graph => {
                required.insert("graph_index");
            }
            CollectionIndexKind::FullText => {
                required.insert("full_text");
            }
            CollectionIndexKind::Columnar => {
                required.insert("columnar_layout");
            }
            CollectionIndexKind::TimeSeries => {
                required.insert("time_series");
            }
            CollectionIndexKind::Primary => {}
        }
    }
    required.into_iter().map(str::to_owned).collect()
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
) -> String {
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
        "## Known Limits".to_owned(),
        String::new(),
    ]);
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
    lines.join("\n")
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
    }
}

fn normalize_template_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

#[cfg(test)]
mod tests {
    use super::{
        TemplateOutputFormat, built_in_template, built_in_templates, materialize_template,
    };
    use crate::config_spec::{ConfigExplainer, ExtensionStability, GuaranteeValidator};

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
    fn generated_specs_validate_and_explain() -> Result<(), Box<dyn std::error::Error>> {
        for template in built_in_templates() {
            let materialized =
                materialize_template(template.slug, "Template Smoke", TemplateOutputFormat::Yaml)?;
            let report = GuaranteeValidator::validate(&materialized.spec);
            assert!(
                report.valid,
                "{} should validate: {:?}",
                template.slug, report
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
    fn required_extensions_are_explicit() -> Result<(), Box<dyn std::error::Error>> {
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
        Ok(())
    }

    #[test]
    fn materialized_files_have_expected_names() -> Result<(), Box<dyn std::error::Error>> {
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
