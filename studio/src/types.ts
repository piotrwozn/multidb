export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export type JsonObject = { [key: string]: JsonValue };

export type ApiErrorBody = {
  code: string;
  message: string;
};

export type ApiEnvelope<T> =
  | {
      ok: true;
      data: T;
    }
  | {
      ok: false;
      error: ApiErrorBody;
    };

export type AdminStatus = {
  server_version: string;
  uptime_millis: number;
  profile: JsonValue;
  replication: JsonValue;
  layout: JsonValue;
  engine: string;
  catalog_objects: number;
  shard_count: number;
};

export type HealthResponse = {
  ok: boolean;
  status: string;
};

export type StudioManifest = {
  api_version: number;
  physical_migration_supported: boolean;
  config_apply_data_mutated: boolean;
  endpoints: string[];
  capabilities: string[];
};

export type DeploymentMode = "embedded" | "single_node" | "cluster";
export type ReplicationMode = "cp" | "ap";
export type ConsistencyMode = "local_snapshot" | "strong_cp" | "eventual_ap";
export type WriteAck = "local" | "quorum" | "all";
export type ConflictResolution =
  | "none"
  | "last_write_wins"
  | "vector_clock"
  | "crdt"
  | "custom";
export type CollectionRole =
  | "document_entity"
  | "key_value"
  | "event_log"
  | "vector_memory"
  | "cache"
  | "audit"
  | "graph"
  | "analytics"
  | "time_series";
export type CollectionIndexKind =
  | "primary"
  | "document"
  | "vector"
  | "graph"
  | "full_text"
  | "columnar"
  | "time_series";

export type DatabaseSpec = JsonObject & {
  version: number;
  name: string;
  profile: string;
  deployment: JsonObject & {
    mode: DeploymentMode;
    storage_path: string | null;
  };
  topology: JsonObject & {
    replica_count: number;
    shard_count: number;
  };
  defaults: JsonObject & {
    consistency_domain: string;
    replication: ReplicationMode;
  };
  guarantees: JsonObject & {
    write_ack: WriteAck;
    conflict_resolution: ConflictResolution;
    backup: JsonObject & { enabled: boolean; pitr: boolean };
    encryption: JsonObject & { at_rest: boolean };
    audit: JsonObject & { enabled: boolean };
    sensitive_data: boolean;
    strict_cross_domain_transactions: boolean;
  };
  domains: (JsonObject & { name: string; mode: ConsistencyMode })[];
  collections: (
    JsonObject & {
      name: string;
      role: CollectionRole;
      domain: string;
      indexes: CollectionIndexKind[];
    }
  )[];
  extensions: JsonValue[];
  overrides: JsonObject;
  operation_hints: JsonObject;
};

export type AuthMe = {
  principal: string;
  roles: string[];
  system_admin: boolean;
  database_admin: boolean;
  insecure_local_admin: boolean;
};

export type LoginResponse = {
  token: string;
  expires_at: string;
  expires_at_millis: number;
  principal: string;
  roles: string[];
};

export type ColumnType = "Int" | "Float" | "Str" | "Bool" | "Bytes" | "Null";

export type ColumnDef = {
  name: string;
  ty: ColumnType;
  nullable: boolean;
};

export type TableSchema = {
  columns: ColumnDef[];
  primary_key: number;
};

export type Row = JsonValue[];

export type CatalogObjectSummary = {
  name: string;
  kind: string;
  entry: JsonObject;
  schema?: TableSchema | null;
  row_count?: number | null;
};

export type CatalogResponse = {
  objects: CatalogObjectSummary[];
};

export type SqlResult =
  | {
      kind: "rows";
      columns: string[];
      rows: Row[];
    }
  | {
      kind: "affected_rows";
      affected_rows: number;
    };

export type SqlResponse = {
  output: SqlResult;
};

export type TableRowsResponse = {
  table: string;
  schema?: TableSchema | null;
  rows: Row[];
  offset: number;
  limit: number;
  returned: number;
  has_more: boolean;
  next_offset?: number | null;
  capped: boolean;
};

export type DocumentSummary = {
  id: string;
  document: JsonValue;
};

export type DocumentListResponse = {
  collection: string;
  documents: DocumentSummary[];
  offset: number;
  limit: number;
  returned: number;
  has_more: boolean;
  next_offset?: number | null;
  capped: boolean;
};

export type GrantSummary = {
  resource: JsonValue;
  permission: "Read" | "Write" | "Admin";
};

export type SecurityRoleSummary = {
  name: string;
  grants: GrantSummary[];
};

export type PrincipalSummary = {
  user: string;
  principal: string;
  roles: string[];
};

export type SecurityState = {
  roles: SecurityRoleSummary[];
  principals: PrincipalSummary[];
  audit_enabled: boolean;
};

export type AuditEvent = {
  id: number;
  at_millis: number;
  principal?: string | null;
  action: string;
  resource: JsonValue;
  outcome: string;
  detail?: string | null;
  integrity?: JsonValue;
};

export type AuditResponse = {
  events: AuditEvent[];
};

export type ValidationSeverity = "error" | "warning" | "advice";

export type ValidationIssue = {
  code: string;
  severity: ValidationSeverity;
  path: string;
  message: string;
  suggestion: string;
  certification_impact?: JsonValue;
};

export type ValidationReport = {
  valid: boolean;
  status: string;
  issues: ValidationIssue[];
};

export type PlanImpact = {
  downtime: string;
  disk: string;
  cpu: string;
  risk: string;
  requires_backup: boolean;
  requires_downtime: boolean;
  notes: string[];
};

export type RollbackPlan = {
  possible: boolean;
  description: string;
  steps: string[];
};

export type MigrationStep = {
  step_id: string;
  kind: string;
  path: string;
  action: string;
  impact: PlanImpact;
  rollback: string;
  requires_confirmation: boolean;
  supported: boolean;
};

export type MigrationPlan = {
  plan_id: string;
  valid: boolean;
  apply_supported: boolean;
  current_validation: ValidationReport;
  desired_validation: ValidationReport;
  steps: MigrationStep[];
  impact: PlanImpact;
  rollback: RollbackPlan;
  required_confirmation: string;
};

export type AdviceCost = {
  summary: string;
  write_amplification: string;
  disk: string;
  cpu: string;
  operator_effort: string;
};

export type MigrationPlanRef = {
  plan_id: string;
  operation_hint: string;
  cli_command: string;
  control_plane_endpoint: string;
  plan: MigrationPlan;
};

export type RuntimeAdviceStatus =
  | "proposed"
  | "accepted"
  | "rejected"
  | "applied"
  | "superseded"
  | "suppressed";

export type RuntimeAdviceDecision = {
  advice_id: string;
  status: RuntimeAdviceStatus;
  reason: string;
  decided_by: string;
  decided_at_millis: number;
  suppress_until_millis?: number | null;
};

export type RuntimeAdvice = {
  id: string;
  code: string;
  message: string;
  rationale: string;
  cost: AdviceCost;
  risk: string;
  expected_gain: string;
  rollback_conditions: string[];
  dry_run: MigrationPlanRef;
  status: RuntimeAdviceStatus;
};

export type AdviceSource = {
  name: string;
  status: string;
  detail: string;
};

export type RuntimeAdviceReport = {
  schema_version: number;
  generated_at_millis: number;
  auto_apply_enabled: boolean;
  sources: AdviceSource[];
  suppressed_recommendations: number;
  recommendations: RuntimeAdvice[];
};

export type ProfileSummary = {
  slug: string;
  aliases: string[];
  status: string;
  description: string;
  default_domain: string;
  compatible_roles: string[];
};

export type RoleSummary = {
  slug: string;
  status: string;
  description: string;
  required_capabilities: string[];
  constraints: string[];
};

export type DomainSummary = {
  slug: string;
  status: string;
  guarantees: string[];
  limits: string[];
};

export type ExtensionProvides = {
  types: string[];
  indexes: string[];
  operators: string[];
  storage_strategies: string[];
};

export type ExtensionRegistryEntry = {
  id: string;
  status: string;
  required_capabilities: string[];
  description: string;
};

export type ExtensionRegistries = {
  types: ExtensionRegistryEntry[];
  indexes: ExtensionRegistryEntry[];
  operators: ExtensionRegistryEntry[];
  storage_strategies: ExtensionRegistryEntry[];
};

export type ExtensionMigration = {
  id: string;
  from: string;
  to: string;
  kind: string;
  requires_downtime: boolean;
  notes: string[];
};

export type ExtensionUiPanel = {
  id: string;
  title: string;
  route: string;
  required_capabilities: string[];
};

export type ExtensionCoreBoundary = {
  wal: string;
  transactions: string;
  recovery: string;
  security: string;
  rbac: string;
};

export type ExtensionManifest = {
  name: string;
  version: string;
  compatible_multidb: string;
  status: string;
  provides: ExtensionProvides;
  registries: ExtensionRegistries;
  capabilities: string[];
  config_schema: JsonValue;
  limitations: string[];
  migrations: ExtensionMigration[];
  ui_panels: ExtensionUiPanel[];
  core_boundary: ExtensionCoreBoundary;
};

export type ExtensionSummary = {
  slug: string;
  status: string;
  source: string;
  description: string;
  manifest: ExtensionManifest;
};

export type LoadedStudioData = {
  health: HealthResponse;
  readiness: HealthResponse;
  auth: AuthMe;
  status: AdminStatus;
  catalog: CatalogResponse;
  security: SecurityState;
  audit: AuditResponse;
  config: DatabaseSpec;
  manifest: StudioManifest;
  profiles: ProfileSummary[];
  roles: RoleSummary[];
  domains: DomainSummary[];
  extensions: ExtensionSummary[];
  advice?: RuntimeAdviceReport;
  adviceError?: string;
};
