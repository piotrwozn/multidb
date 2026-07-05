import type {
  AuditResponse,
  AdminStatus,
  AuthMe,
  CatalogResponse,
  DatabaseSpec,
  DomainSummary,
  ExtensionSummary,
  LoginResponse,
  MigrationPlan,
  ProfileSummary,
  SecurityState,
  RoleSummary,
  RuntimeAdviceReport,
  StudioManifest,
  ValidationReport,
} from "../types";

export const statusFixture: AdminStatus = {
  server_version: "0.1.0-test",
  uptime_millis: 65_000,
  profile: "local",
  replication: "single_node",
  layout: "row",
  engine: "Memory",
  catalog_objects: 3,
  shard_count: 1,
};

export const authFixture: AuthMe = {
  principal: "admin",
  roles: ["admin"],
  system_admin: true,
  database_admin: true,
  insecure_local_admin: true,
};

export const loginFixture: LoginResponse = {
  token: "mda1_session_test",
  expires_at: "2026-07-05T12:00:00Z",
  expires_at_millis: 1_783_252_800_000,
  principal: "admin",
  roles: ["admin"],
};

export const catalogFixture: CatalogResponse = {
  objects: [
    {
      name: "users",
      kind: "table",
      entry: { Table: { indexes: [], layout: "Row" } },
      schema: {
        columns: [
          { name: "id", ty: "Int", nullable: false },
          { name: "name", ty: "Str", nullable: false },
        ],
        primary_key: 0,
      },
      row_count: 1,
    },
    {
      name: "profiles",
      kind: "collection",
      entry: { Collection: { collection_id: 1, fields: [], indexes: [] } },
      row_count: 1,
    },
  ],
};

export const securityFixture: SecurityState = {
  audit_enabled: true,
  roles: [
    {
      name: "admin",
      grants: [
        { resource: "System", permission: "Admin" },
        { resource: "Database", permission: "Admin" },
      ],
    },
  ],
  principals: [
    {
      user: "admin",
      principal: "admin",
      roles: ["admin"],
    },
  ],
};

export const auditFixture: AuditResponse = {
  events: [
    {
      id: 1,
      at_millis: 1_720_000_000_000,
      principal: "admin",
      action: "login",
      resource: "System",
      outcome: "Succeeded",
      detail: "dev",
    },
  ],
};

export const configFixture: DatabaseSpec = {
  version: 1,
  name: "studio-test",
  profile: "durable",
  deployment: {
    mode: "single_node",
    storage_path: null,
  },
  topology: {
    replica_count: 1,
    shard_count: 1,
  },
  defaults: {
    consistency_domain: "primary",
    replication: "cp",
  },
  guarantees: {
    write_ack: "quorum",
    conflict_resolution: "none",
    backup: { enabled: false, pitr: false },
    encryption: { at_rest: false },
    audit: { enabled: true },
    sensitive_data: false,
    strict_cross_domain_transactions: false,
  },
  collections: [
    {
      name: "users",
      role: "document_entity",
      domain: "primary",
      indexes: ["document"],
    },
  ],
  domains: [{ name: "primary", mode: "local_snapshot" }],
  extensions: [],
  overrides: {},
  operation_hints: {},
};

export const manifestFixture: StudioManifest = {
  api_version: 1,
  physical_migration_supported: false,
  config_apply_data_mutated: false,
  endpoints: [
    "POST /auth/login",
    "GET /auth/me",
    "POST /auth/logout",
    "POST /auth/change-password",
    "GET /catalog",
    "POST /sql",
    "GET /config",
    "POST /config/validate",
    "POST /config/plan",
    "POST /config/apply",
    "POST /builder/table",
    "POST /builder/collection",
    "POST /builder/vector",
    "POST /builder/time-series",
    "POST /builder/full-text",
    "POST /builder/geo",
    "POST /builder/graph",
    "GET /security",
    "POST /security",
    "GET /audit",
    "GET /profiles",
    "GET /roles",
    "GET /domains",
    "GET /extensions",
    "GET /advice",
    "POST /advice/plan",
    "POST /advice/decision",
    "GET /studio",
  ],
  capabilities: [
    "admin_login",
    "admin_sessions",
    "admin_change_password",
    "auth_me",
    "catalog_browser",
    "sql_console",
    "table_row_crud",
    "document_crud",
    "vector_crud",
    "time_series_crud",
    "resource_builder",
    "full_text_builder",
    "geo_builder",
    "graph_builder",
    "security_rbac",
    "audit_log",
    "config_view",
    "config_validate",
    "migration_dry_run",
    "catalog_view",
    "advisor_read_only",
    "runtime_advisor_v2",
    "advisor_decision_memory",
  ],
};

export const adviceFixture: RuntimeAdviceReport = {
  schema_version: 1,
  generated_at_millis: 1_720_000_000_000,
  auto_apply_enabled: false,
  sources: [
    {
      name: "workload_profiler",
      status: "active",
      detail: "system.workload observations",
    },
  ],
  suppressed_recommendations: 0,
  recommendations: [
    {
      id: "index-create-users-age",
      code: "CREATE_INDEX",
      message: "Create an index for users.age",
      rationale: "fingerprint users-age examined 2000 rows for 1 returned rows",
      cost: {
        summary: "Physical index build after operator approval; dry-run is metadata-only.",
        write_amplification: "medium",
        disk: "medium",
        cpu: "medium",
        operator_effort: "low",
      },
      risk: "medium",
      expected_gain: "Estimated scan cost drops from 2000.0 to 20.0.",
      rollback_conditions: ["Drop the new index if write latency regresses."],
      dry_run: {
        plan_id: "plan-advice-1",
        operation_hint: "advisor.index.create.users.age",
        cli_command:
          "multidb advice plan --advice-id index-create-users-age --db <path> --profile <profile>",
        control_plane_endpoint: "/advice/plan",
        plan: {
          plan_id: "plan-advice-1",
          valid: true,
          apply_supported: true,
          current_validation: {
            valid: true,
            status: "Certified",
            issues: [],
          },
          desired_validation: {
            valid: true,
            status: "Certified",
            issues: [],
          },
          steps: [],
          impact: {
            downtime: "none",
            disk: "low",
            cpu: "low",
            risk: "low",
            requires_backup: false,
            requires_downtime: false,
            notes: [],
          },
          rollback: {
            possible: true,
            description: "Remove the added metadata key.",
            steps: [],
          },
          required_confirmation: "plan-advice-1",
        },
      },
      status: "proposed",
    },
  ],
};

export const profilesFixture: ProfileSummary[] = [
  {
    slug: "durable",
    aliases: ["default"],
    status: "Certified",
    description: "Local durable profile",
    default_domain: "local_snapshot",
    compatible_roles: ["document_entity"],
  },
];

export const rolesFixture: RoleSummary[] = [
  {
    slug: "document_entity",
    status: "Certified",
    description: "Document entity role",
    required_capabilities: ["document"],
    constraints: ["primary index required"],
  },
];

export const domainsFixture: DomainSummary[] = [
  {
    slug: "local_snapshot",
    status: "Certified",
    guarantees: ["snapshot reads"],
    limits: ["single node"],
  },
];

export const extensionsFixture: ExtensionSummary[] = [
  {
    slug: "full_text",
    status: "Stable",
    source: "builtin",
    description: "Full text capability",
    manifest: {
      name: "full_text",
      version: "1.0.0",
      compatible_multidb: ">=0.1.0",
      status: "Stable",
      provides: {
        types: ["text_document"],
        indexes: ["full_text"],
        operators: ["match_text"],
        storage_strategies: [],
      },
      registries: {
        types: [
          {
            id: "text_document",
            status: "Stable",
            required_capabilities: ["text-search"],
            description: "Full-text document metadata type.",
          },
        ],
        indexes: [
          {
            id: "full_text",
            status: "Stable",
            required_capabilities: ["full-text"],
            description: "Full-text inverted index.",
          },
        ],
        operators: [
          {
            id: "match_text",
            status: "Stable",
            required_capabilities: ["text-search"],
            description: "Full-text search operator.",
          },
        ],
        storage_strategies: [],
      },
      capabilities: ["full-text", "text-search"],
      config_schema: { type: "object", additionalProperties: false },
      limitations: ["Derived text indexes can lag source writes."],
      migrations: [
        {
          id: "full-text-1",
          from: "1.0.0",
          to: "1.0.0",
          kind: "metadata",
          requires_downtime: false,
          notes: [],
        },
      ],
      ui_panels: [
        {
          id: "full-text-indexes",
          title: "Full text",
          route: "/extensions/full_text",
          required_capabilities: ["full-text"],
        },
      ],
      core_boundary: {
        wal: "core_owned",
        transactions: "core_owned",
        recovery: "core_owned",
        security: "core_owned",
        rbac: "core_owned",
      },
    },
  },
];

export const validationFixture: ValidationReport = {
  valid: false,
  status: "Invalid",
  issues: [
    {
      code: "backup_required",
      severity: "error",
      path: "$.guarantees.backup",
      message: "missing backup coverage",
      suggestion: "enable backup coverage before certification",
    },
  ],
};

export const migrationPlanFixture: MigrationPlan = {
  plan_id: "plan-1",
  valid: true,
  apply_supported: false,
  current_validation: { valid: true, status: "Certified", issues: [] },
  desired_validation: { valid: true, status: "Certified", issues: [] },
  steps: [
    {
      step_id: "step-1",
      kind: "change_topology",
      path: "$.topology.replica_count",
      action: "change replica count from 1 to 3",
      impact: {
        downtime: "none",
        disk: "low",
        cpu: "low",
        risk: "medium",
        requires_backup: true,
        requires_downtime: false,
        notes: [],
      },
      rollback: "restore previous replica count",
      requires_confirmation: true,
      supported: false,
    },
  ],
  impact: {
    downtime: "none",
    disk: "low",
    cpu: "low",
    risk: "medium",
    requires_backup: true,
    requires_downtime: false,
    notes: ["operator review required"],
  },
  rollback: {
    possible: true,
    description: "Restore previous backup guarantee.",
    steps: ["Revert desired spec."],
  },
  required_confirmation: "plan-1",
};
