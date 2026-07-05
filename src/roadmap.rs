#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadinessStatus {
    /// The phase has implementation evidence and no known gaps in this report.
    Complete,
    /// The phase has implemented surface area, but production work remains.
    ProductionGap,
    /// Future roadmap work; not a claim about implemented runtime behavior.
    Deferred,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhaseReadiness {
    pub phase: u8,
    pub title: &'static str,
    pub status: ReadinessStatus,
    pub evidence: &'static [&'static str],
    pub gaps: &'static [&'static str],
}

const PHASES: &[PhaseReadiness] = &[
    PhaseReadiness {
        phase: 0,
        title: "foundation",
        status: ReadinessStatus::Complete,
        evidence: &[
            "deny.toml",
            "scripts/check.ps1",
            "scripts/license-deny-smoke.ps1",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 1,
        title: "storage",
        status: ReadinessStatus::Complete,
        evidence: &[
            "storage conformance tests",
            "storage doctest compile-fail contracts",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 2,
        title: "profiles and replication seam",
        status: ReadinessStatus::Complete,
        evidence: &["profile metadata tests", "SingleNode replication tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 3,
        title: "documents",
        status: ReadinessStatus::Complete,
        evidence: &["document collection tests", "Value round-trip tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 4,
        title: "document indexes",
        status: ReadinessStatus::Complete,
        evidence: &["index scan tests", "batch atomicity tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 5,
        title: "relational and sql",
        status: ReadinessStatus::Complete,
        evidence: &["SQL subset tests", "DataFusion provider tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 6,
        title: "multi-model",
        status: ReadinessStatus::Complete,
        evidence: &["catalog tests", "cross-model query tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 7,
        title: "single-node acid",
        status: ReadinessStatus::Complete,
        evidence: &[
            "snapshot isolation tests",
            "write conflict tests",
            "retry tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 8,
        title: "columnar analytics",
        status: ReadinessStatus::Complete,
        evidence: &[
            "Parquet round-trip tests",
            "columnar SQL tests",
            "criterion benchmark",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 9,
        title: "vectors",
        status: ReadinessStatus::Complete,
        evidence: &["HNSW tests", "rebuild tests", "metric ranking tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 10,
        title: "network",
        status: ReadinessStatus::Complete,
        evidence: &["TLS/SCRAM tests", "PG wire subset tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 11,
        title: "cp and ap replication",
        status: ReadinessStatus::Complete,
        evidence: &["CP/AP contract tests", "conflict tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 12,
        title: "self-healing",
        status: ReadinessStatus::Complete,
        evidence: &["health state tests", "healing policy tests"],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 13,
        title: "sharding",
        status: ReadinessStatus::Complete,
        evidence: &[
            "routing tests",
            "scatter-gather tests",
            "2PC skeleton tests",
            "durable 2PC recovery tests",
            "docs/phase-13-distributed-transaction-recovery.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 14,
        title: "operations",
        status: ReadinessStatus::Complete,
        evidence: &[
            "RBAC tests",
            "audit hash-chain tests",
            "encryption tests",
            "metrics tests",
            "docs/phase-14-ops-ga.md",
            "ops/kind/multidb-kind.yaml",
            "ops/helm/multidb",
            "ops/vault/dev-policy.hcl",
            "ops/minio/backup-target.env.example",
            "scripts/ops-smoke.ps1",
            "scripts/upgrade-smoke.ps1",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 15,
        title: "cost optimizer",
        status: ReadinessStatus::Complete,
        evidence: &[
            "ANALYZE stats tests",
            "cost-based index-vs-scan tests",
            "plan cache tests",
            "EXPLAIN ANALYZE feedback tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 16,
        title: "performance engineering",
        status: ReadinessStatus::Complete,
        evidence: &[
            "performance config defaults",
            "compressed storage decorator tests",
            "group commit tests",
            "columnar multi-segment tests",
            "performance benchmark scripts",
            "baselines/perf/local-smoke.json",
            "baselines/perf/ci-gate.json",
            "baselines/perf/release-baseline.json",
            "release workflow performance gate",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 17,
        title: "backup and pitr",
        status: ReadinessStatus::Complete,
        evidence: &[
            "logical commit log",
            "full backup tests",
            "incremental PITR tests",
            "backup verify tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 18,
        title: "cdc and materialized views",
        status: ReadinessStatus::Complete,
        evidence: &[
            "commit-log changefeed tests",
            "subscription RBAC and ack tests",
            "incremental materialized view tests",
            "before/after hook tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 19,
        title: "additional models",
        status: ReadinessStatus::Complete,
        evidence: &[
            "full-text BM25-style tests",
            "time-series chunk codec tests",
            "graph traversal tests",
            "geo haversine tests",
            "phase 19 catalog and SQL helper tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 20,
        title: "extensibility",
        status: ReadinessStatus::Complete,
        evidence: &[
            "WASM UDF sandbox tests",
            "codec plugin conformance tests",
            "collation sort-key tests",
            "policy validation and masking tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 21,
        title: "self-tuning",
        status: ReadinessStatus::Complete,
        evidence: &[
            "workload profiler tests",
            "index advisor tests",
            "tuning policy envelope tests",
            "tuning cooldown and max_changes_per_hour tests",
            "RegressionGate rollback and audit tests",
            "reprofile planning-only visibility tests",
            "system workload/advice views",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 22,
        title: "ecosystem compatibility",
        status: ReadinessStatus::Complete,
        evidence: &[
            "minimal pg_catalog and information_schema tests",
            "SQLSTATE compatibility mapping tests",
            "CSV/JSONL migration tests",
            "Mongo BSON mapping tests",
            "admin status tests",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 23,
        title: "cloud architecture",
        status: ReadinessStatus::Complete,
        evidence: &[
            "tiered columnar segment tests",
            "tiering recovery state tests",
            "object-store backup/restore tests",
            "backup GC tests preserving PITR parent and descendant chains",
            "tenant quota bootstrap and delete accounting tests",
            "tenant concurrency fairness tests",
            "cloud lease TTL, owner, heartbeat and break-lease tests",
            "guarded resume session and one-shot hibernation marker tests",
            "compute/storage separation ADR",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 24,
        title: "hardening",
        status: ReadinessStatus::Complete,
        evidence: &[
            "hardening invariant tests",
            "SimStorage conformance and fault-injection tests",
            "format registry tests",
            "fuzz target scaffold",
            "phase 24 hardening docs",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 25,
        title: "canonical value and key format",
        status: ReadinessStatus::Complete,
        evidence: &[
            "canonical value decode coverage",
            "key encoding helpers and fuzz target",
            "docs/phase-25-canonical-format.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 26,
        title: "transaction core",
        status: ReadinessStatus::Complete,
        evidence: &[
            "transaction module tests for binary commit log, MVCC GC, HLC and deterministic clocking",
            "storage conformance tests for stale-snapshot Conflict and writer serialization",
            "transaction facade tests for savepoints, retry loops, write skew and phantom rejection",
            "redb durability reopen tests and compressed decode-limit tests",
            "docs/phase-26-transaction-core.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 27,
        title: "cryptography and audit",
        status: ReadinessStatus::Complete,
        evidence: &[
            "encrypted storage nonce/version tests",
            "audit integrity tests",
            "Vault dev KEK provider test",
            "key rotation and crypto-shred tests",
            "docs/phase-27-cryptography-audit.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 28,
        title: "query engine production ready",
        status: ReadinessStatus::Complete,
        evidence: &[
            "limited SQL parser and query resource governor",
            "fast-path/DataFusion semantic regression tests",
            "plan cache invalidation and fingerprint tests",
            "reservoir statistics regression tests",
            "information_schema projection/filter tests",
            "CTE/UNION RBAC coverage tests",
            "batch replication scan API",
            "scripts/check.ps1",
            "scripts/perf.ps1 -Rows 1000 -Output target/perf/phase28.json",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 29,
        title: "multi-model consistency and safe extensibility",
        status: ReadinessStatus::Complete,
        evidence: &[
            "phase29 catalog, parser, vector and hook tests",
            "WASM sandbox limit coverage",
            "docs/phase-29-multi-model-consistency.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 30,
        title: "distributed production readiness",
        status: ReadinessStatus::Complete,
        evidence: &[
            "durable 2PC participant/coordinator records",
            "cross-shard batch commit through prepare/decision/finish",
            "CP participant recovery state",
            "AP strong range quorum merge and sibling read-repair",
            "AP Merkle-diff anti-entropy and ordered hinted handoff",
            "internal length-prefixed TCP transport with mTLS config, AP RPC, frame limits, and flow-control",
            "pgwire connection limits, connection timeout, auth rate-limit, and blocking describe inference",
            "CDC paged poll API, timeline fork mapping, managed subscription worker, durable push offsets, and windowed materialized views",
            "durable HLC commit metadata, AP region-aware placement preference, and bounded hint/anti-entropy backlogs",
            "CP OpenRaft type config/read-index gate metadata and managed self-healing runner",
            "stable CP cluster APIs for start/shutdown/status/recovery/membership/leader transfer",
            "internal Raft append/vote/pre-vote/snapshot and cluster-admin RPC frames",
            "docs/phase-30-production-ready.md",
            "cargo test --lib",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 31,
        title: "advanced indexes and vector-columnar execution",
        status: ReadinessStatus::Complete,
        evidence: &[
            "advanced RelIndexSpec serde compatibility",
            "partial/expression/covering/bitmap index tests",
            "filtered vector kNN, quantization, DiskANN-style rerank, and Euclidean L2 tests",
            "columnar segment metadata and zone-map skip tests",
            "Gorilla time-series compression ratio test",
            "docs/phase-31-production-ready.md",
            "scripts/perf.ps1 -Rows 1000 -Output target/perf/phase31.json",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 32,
        title: "federation temporal continuous and wasm procedures",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public federation/temporal/continuous modules",
            "foreign CSV query provider test",
            "MVCC AS OF LSN and RetentionExpired test",
            "cataloged materialized view query test",
            "continuous query and outbox metadata test",
            "WASM before/after trigger firing tests",
            "deterministic fail-closed SQL DDL declaration tests",
            "docs/phase-32-production-ready.md",
            "cargo test --lib",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 33,
        title: "formal verification and distributed testing",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public verification module with executable contracts and History checker",
            "StorageModel oracle and deterministic SimStorage scenarios",
            "storage conformance across Mem/redb/Sim/Compressed/Encrypted/Any wrappers",
            "Stateright bounded CP quorum and 2PC safety tests",
            "Jepsen-style in-process linearizability checker catches injected split-brain",
            "pg_copy_text/keyenc_successor/internal_request_frame fuzz targets",
            "nightly-verification workflow for model/fuzz/Miri/TSan/perf-gate smoke",
            "docs/phase-33-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 34,
        title: "production ready",
        status: ReadinessStatus::Complete,
        evidence: &[
            "import batch/resume/reject regression tests",
            "bounded metrics, histogram and live readiness tests",
            "cloud lease/fencing, PITR GC and quota accounting tests",
            "tuning cooldown, max_changes and automatic rollback tests",
            "pinned CI with MSRV, audit, vet, SBOM and release attestation workflow",
            "docs/phase-34-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 35,
        title: "roadmap honesty and repo baseline",
        status: ReadinessStatus::Complete,
        evidence: &[
            "docs/phase-35-roadmap-baseline.md",
            "docs/source-baseline.md",
            "readiness metadata covers phases 0-48",
            "repo baseline and local artifact policy documented",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 36,
        title: "configuration specification",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public config_spec module with DatabaseSpec v1",
            "DatabaseSpec JSON serde and structural validation tests",
            "DbConfig compatibility import tests",
            "docs/schemas/database-spec-v1.schema.json",
            "docs/phase-36-configuration-spec.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 37,
        title: "profiles roles and consistency domains",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public config_spec support catalog with ProfileSpec, CollectionRoleDefinition and ConsistencyDomainDefinition",
            "support status derivation tests for Certified, Custom, Invalid and Experimental outcomes",
            "legacy DbConfig profile aliases resolve through the product catalog",
            "docs/phase-37-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 38,
        title: "guarantee validator and policy compiler",
        status: ReadinessStatus::Complete,
        evidence: &[
            "DatabaseSpec v1 guarantee, collection index and extension stability contract",
            "GuaranteeValidator validation matrix tests",
            "PolicyCompiler deterministic compile tests",
            "multidb config validate CLI tests",
            "docs/phase-38-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 39,
        title: "explain config and migration planner",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public config_spec ExplainConfigReport and MigrationPlan contracts",
            "deterministic migration dry-run and apply-confirmation tests",
            "multidb config explain/plan/apply CLI tests",
            "docs/phase-39-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 40,
        title: "cli product layer",
        status: ReadinessStatus::Complete,
        evidence: &[
            "multidb init --guided YAML/JSON generation tests",
            "multidb profile/role/domain list text and JSON tests",
            "multidb config validate/explain/plan YAML input tests",
            "multidb explain config alias and config plan --out tests",
            "docs/phase-40-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 41,
        title: "control plane api",
        status: ReadinessStatus::Complete,
        evidence: &[
            "authenticated admin router for /config, /profiles, /roles, /domains, /extensions, /advice and /studio",
            "Database::confirm_config_apply_as audited no-op contract",
            "admin HTTP envelope, auth, RBAC and apply no-mutation tests",
            "docs/phase-41-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 42,
        title: "multidb studio",
        status: ReadinessStatus::Complete,
        evidence: &[
            "studio React/Vite application over the Phase 41 Control Plane API",
            "Studio API client, validation, migration dry-run, catalog, extensions and advice views",
            "Studio Vitest component tests and Playwright smoke test",
            "scripts/studio-check.ps1",
            "docs/phase-42-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 43,
        title: "extension manifest and marketplace contract",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public ExtensionManifest contract and manifest validator",
            "built-in extension manifests and deterministic extension catalog compilation",
            "admin /extensions full manifest catalog",
            "Studio extension manifest registry and UI panel rendering",
            "docs/phase-43-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 44,
        title: "runtime advisor v2",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public runtime_advisor module with RuntimeAdviceReport and decision memory",
            "Runtime Advisor combines index advice, planner feedback, guarantee validation and migration dry-runs",
            "admin /advice, /advice/plan and /advice/decision endpoints",
            "multidb advice list/plan/reject CLI tests",
            "Studio Runtime Advisor recommendation cards",
            "docs/phase-44-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 45,
        title: "cluster ga",
        status: ReadinessStatus::Complete,
        evidence: &[
            "live OpenRaft runtime with durable log/state machine/snapshot storage",
            "CP internal transport Raft RPC and cluster-admin RPC frames",
            "Phase 45 Cluster GA smoke for leader handoff, minority write rejection, durable membership metadata, and read-index",
            "scripts/cluster-ga-smoke.ps1",
            "docs/phase-45-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 46,
        title: "performance truth",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public performance truth envelope types and profile thresholds",
            "scripts/perf.ps1 profile-aware envelope reports",
            "scripts/perf_gate.ps1 release baseline gate with summary output",
            "scripts/perf_trend.ps1 trend dashboard JSON",
            "baselines/perf/local-smoke.json",
            "baselines/perf/ci-gate.json",
            "baselines/perf/release-baseline.json",
            "CI ci-gate artifact and release workflow performance blocker",
            "docs/phase-46-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 47,
        title: "sdks examples and templates",
        status: ReadinessStatus::Complete,
        evidence: &[
            "public templates module with built-in template catalog",
            "multidb template list/explain and init --guided --template CLI",
            "examples/game-save",
            "examples/desktop-embedded",
            "examples/ai-memory",
            "examples/secure-saas",
            "examples/analytics",
            "scripts/templates-smoke.ps1",
            "docs/phase-47-production-ready.md",
            "docs/sdk-templates.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 48,
        title: "public preview packaging",
        status: ReadinessStatus::Complete,
        evidence: &[
            "docs/phase-48-production-ready.md",
            "docs/public-preview.md",
            "scripts/preview-smoke.ps1",
            ".github/workflows/release.yml public preview smoke",
            "docs/phase-0-42-ga-support-matrix.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 49,
        title: "docker runtime",
        status: ReadinessStatus::Complete,
        evidence: &[
            "multidb serve shared Control Plane, Studio and PostgreSQL wire runtime",
            "Dockerfile multi-stage runtime image with non-root final stage",
            "docker-compose.yml local-dev quickstart",
            "scripts/docker-smoke.ps1 image build/run/restart smoke",
            "ops/helm/multidb Docker-aligned runtime values, PVC and Secret references",
            "docs/docker.md",
            "docs/phase-49-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 50,
        title: "admin login and sessions",
        status: ReadinessStatus::Complete,
        evidence: &[
            "admin /auth/login, /auth/logout and /auth/change-password endpoints",
            "durable Argon2id admin credential in __admin_auth",
            "in-memory hashed session token store with TTL and logout invalidation",
            "legacy MULTIDB_ADMIN_TOKEN compatibility for automation",
            "Studio password login, logout and 401 session-expiry handling",
            "Docker Compose, Helm and docker smoke use admin password with legacy token check",
            "docs/phase-50-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 51,
        title: "studio full ui",
        status: ReadinessStatus::Complete,
        evidence: &[
            "Studio dashboard health/readiness, catalog metrics and recent audit events",
            "Studio auth UX distinguishes login failure, session expiry and forbidden errors",
            "Data Explorer bounded pagination, JSON preflight validation and exact destructive confirmations",
            "SQL Console in-memory history with table rendering for row results",
            "Audit filters and event detail expansion",
            "Runtime Advisor plan and reject-decision UI over existing Control Plane endpoints",
            "Security RBAC dirty state, validation and broad-admin warnings",
            "desktop and mobile Playwright Studio operator flow",
            "docs/phase-51-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 52,
        title: "sdk api ecosystem",
        status: ReadinessStatus::Complete,
        evidence: &[
            "docs/openapi/control-plane-v1.openapi.json served by /openapi.json and /api/openapi.json",
            "Control Plane operation registry with auth and stability metadata",
            "official publish-ready SDK packages under sdk/typescript, sdk/python, sdk/go and sdk/rust",
            "Studio imports the local @multidb/client TypeScript SDK",
            "examples/sdk TypeScript, Python, Go and Rust first-app flows",
            "scripts/sdk-smoke.ps1",
            "scripts/sdk-examples-smoke.ps1",
            "docs/sdk-api-ecosystem.md",
            "docs/sdk-generated-clients.md",
            "docs/phase-52-production-ready.md",
        ],
        gaps: &[],
    },
    PhaseReadiness {
        phase: 53,
        title: "ga hardening",
        status: ReadinessStatus::Complete,
        evidence: &[
            "Admin login rate-limit config and neutral lockout/audit tests",
            "MULTIDB_ADMIN_LOGIN_MAX_FAILURES, WINDOW_SECONDS and LOCKOUT_SECONDS runtime env contract",
            "release workflow builds Linux and Windows binaries, signs blobs, publishes checksums and provenance",
            "release workflow publishes signed GHCR Docker image without latest tag",
            "scripts/phase53-ga-smoke.ps1",
            "SDK compatibility constants for Control Plane API v1 and minimum MultiDB version",
            "docs/ga-support-matrix.md",
            "docs/release-and-versioning.md",
            "docs/release-checklist.md",
            "docs/phase-53-production-ready.md",
        ],
        gaps: &[],
    },
];

#[must_use]
pub const fn readiness_report() -> &'static [PhaseReadiness] {
    PHASES
}

#[must_use]
pub fn phase_readiness(phase: u8) -> Option<&'static PhaseReadiness> {
    PHASES.iter().find(|entry| entry.phase == phase)
}

pub fn production_gaps() -> impl Iterator<Item = &'static PhaseReadiness> {
    PHASES
        .iter()
        .filter(|entry| entry.status == ReadinessStatus::ProductionGap)
}

#[cfg(test)]
mod tests {
    use super::{ReadinessStatus, phase_readiness, production_gaps, readiness_report};

    #[test]
    fn report_covers_phases_zero_through_fifty_three() {
        let phases = readiness_report();
        assert_eq!(phases.len(), 54);
        for expected in 0_u8..=53 {
            assert!(
                phase_readiness(expected).is_some(),
                "missing phase {expected}"
            );
        }
    }

    #[test]
    fn every_phase_has_evidence_or_an_explicit_gap() {
        for phase in readiness_report() {
            assert!(!phase.evidence.is_empty() || !phase.gaps.is_empty());
        }
    }

    #[test]
    fn phase_lookup_returns_the_requested_entry() {
        let Some(phase) = phase_readiness(20) else {
            panic!("phase 20 must exist");
        };
        assert_eq!(phase.title, "extensibility");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
    }

    #[test]
    fn phases_zero_through_forty_four_are_complete_with_empty_gaps() {
        for phase in 0_u8..=44 {
            let Some(readiness) = phase_readiness(phase) else {
                panic!("phase {phase} must exist");
            };
            assert_eq!(
                readiness.status,
                ReadinessStatus::Complete,
                "phase {phase} must be complete"
            );
            assert!(
                readiness.gaps.is_empty(),
                "phase {phase} must not have readiness gaps"
            );
        }
    }

    #[test]
    fn phase_thirty_four_is_marked_complete() {
        let Some(phase) = phase_readiness(34) else {
            panic!("phase 34 must exist");
        };
        assert_eq!(phase.title, "production ready");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
    }

    #[test]
    fn phase_thirty_five_is_marked_complete() {
        let Some(phase) = phase_readiness(35) else {
            panic!("phase 35 must exist");
        };
        assert_eq!(phase.title, "roadmap honesty and repo baseline");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
    }

    #[test]
    fn phase_thirty_six_is_marked_complete() {
        let Some(phase) = phase_readiness(36) else {
            panic!("phase 36 must exist");
        };
        assert_eq!(phase.title, "configuration specification");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("DatabaseSpec"))
        );
    }

    #[test]
    fn phase_thirty_seven_is_marked_complete() {
        let Some(phase) = phase_readiness(37) else {
            panic!("phase 37 must exist");
        };
        assert_eq!(phase.title, "profiles roles and consistency domains");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("ProfileSpec"))
        );
    }

    #[test]
    fn phase_thirty_eight_is_marked_complete() {
        let Some(phase) = phase_readiness(38) else {
            panic!("phase 38 must exist");
        };
        assert_eq!(phase.title, "guarantee validator and policy compiler");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("GuaranteeValidator"))
        );
    }

    #[test]
    fn phase_thirty_nine_is_marked_complete() {
        let Some(phase) = phase_readiness(39) else {
            panic!("phase 39 must exist");
        };
        assert_eq!(phase.title, "explain config and migration planner");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("MigrationPlan"))
        );
    }

    #[test]
    fn phase_forty_is_marked_complete() {
        let Some(phase) = phase_readiness(40) else {
            panic!("phase 40 must exist");
        };
        assert_eq!(phase.title, "cli product layer");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("init --guided"))
        );
    }

    #[test]
    fn phase_forty_one_is_marked_complete() {
        let Some(phase) = phase_readiness(41) else {
            panic!("phase 41 must exist");
        };
        assert_eq!(phase.title, "control plane api");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("confirm_config_apply_as"))
        );
    }

    #[test]
    fn phase_forty_two_is_marked_complete() {
        let Some(phase) = phase_readiness(42) else {
            panic!("phase 42 must exist");
        };
        assert_eq!(phase.title, "multidb studio");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(phase.evidence.iter().any(|entry| entry.contains("Studio")));
    }

    #[test]
    fn phase_forty_three_is_marked_complete() {
        let Some(phase) = phase_readiness(43) else {
            panic!("phase 43 must exist");
        };
        assert_eq!(phase.title, "extension manifest and marketplace contract");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("ExtensionManifest"))
        );
    }

    #[test]
    fn phase_forty_four_is_marked_complete() {
        let Some(phase) = phase_readiness(44) else {
            panic!("phase 44 must exist");
        };
        assert_eq!(phase.title, "runtime advisor v2");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("RuntimeAdviceReport"))
        );
    }

    #[test]
    fn phase_forty_five_is_marked_complete() {
        let Some(phase) = phase_readiness(45) else {
            panic!("phase 45 must exist");
        };
        assert_eq!(phase.title, "cluster ga");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("cluster-ga-smoke.ps1"))
        );
    }

    #[test]
    fn phase_forty_six_is_marked_complete() {
        let Some(phase) = phase_readiness(46) else {
            panic!("phase 46 must exist");
        };
        assert_eq!(phase.title, "performance truth");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("perf_trend.ps1"))
        );
    }

    #[test]
    fn phase_forty_seven_is_marked_complete() {
        let Some(phase) = phase_readiness(47) else {
            panic!("phase 47 must exist");
        };
        assert_eq!(phase.title, "sdks examples and templates");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("templates module"))
        );
    }

    #[test]
    fn phase_forty_eight_is_marked_complete() {
        let Some(phase) = phase_readiness(48) else {
            panic!("phase 48 must exist");
        };
        assert_eq!(phase.title, "public preview packaging");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("preview-smoke.ps1"))
        );
    }

    #[test]
    fn phase_forty_nine_is_marked_complete() {
        let Some(phase) = phase_readiness(49) else {
            panic!("phase 49 must exist");
        };
        assert_eq!(phase.title, "docker runtime");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("docker-smoke.ps1"))
        );
    }

    #[test]
    fn phase_fifty_is_marked_complete() {
        let Some(phase) = phase_readiness(50) else {
            panic!("phase 50 must exist");
        };
        assert_eq!(phase.title, "admin login and sessions");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("/auth/login"))
        );
    }

    #[test]
    fn phase_fifty_one_is_marked_complete() {
        let Some(phase) = phase_readiness(51) else {
            panic!("phase 51 must exist");
        };
        assert_eq!(phase.title, "studio full ui");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("phase-51-production-ready.md"))
        );
    }

    #[test]
    fn phase_fifty_two_is_marked_complete() {
        let Some(phase) = phase_readiness(52) else {
            panic!("phase 52 must exist");
        };
        assert_eq!(phase.title, "sdk api ecosystem");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("sdk-smoke.ps1"))
        );
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("control-plane-v1.openapi.json"))
        );
    }

    #[test]
    fn phase_fifty_three_is_marked_complete() {
        let Some(phase) = phase_readiness(53) else {
            panic!("phase 53 must exist");
        };
        assert_eq!(phase.title, "ga hardening");
        assert_eq!(phase.status, ReadinessStatus::Complete);
        assert!(phase.gaps.is_empty());
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("phase53-ga-smoke.ps1"))
        );
        assert!(
            phase
                .evidence
                .iter()
                .any(|entry| entry.contains("GHCR Docker image"))
        );
    }

    #[test]
    fn production_gaps_are_closed_for_the_ga_matrix() {
        assert_eq!(production_gaps().count(), 0);
    }

    #[test]
    fn public_docs_cover_phase_thirty_five_and_forty_eight() {
        let phase_35 = include_str!("../docs/phase-35-roadmap-baseline.md");
        let phase_48 = include_str!("../docs/phase-48-production-ready.md");
        assert!(phase_35.contains("Phase 35"));
        assert!(phase_35.contains("outside the public source baseline"));
        assert!(phase_48.contains("Phase 48"));
        assert!(phase_48.contains("public preview"));
    }

    #[test]
    fn post_preview_public_docs_cover_phase_forty_nine_through_fifty_three() {
        let phase_49 = include_str!("../docs/phase-49-production-ready.md");
        let phase_50 = include_str!("../docs/phase-50-production-ready.md");
        let phase_51 = include_str!("../docs/phase-51-production-ready.md");
        let phase_52 = include_str!("../docs/phase-52-production-ready.md");
        let phase_53 = include_str!("../docs/phase-53-production-ready.md");
        assert!(phase_49.contains("Phase 49"));
        assert!(phase_50.contains("Phase 50"));
        assert!(phase_51.contains("Phase 51"));
        assert!(phase_52.contains("Phase 52"));
        assert!(phase_53.contains("Phase 53"));
    }
}
