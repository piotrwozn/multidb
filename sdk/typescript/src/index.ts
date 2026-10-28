export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export type JsonObject = { [key: string]: JsonValue };

/** Bounded vector-seeded graph retrieval. The server assembles context locally, without an LLM. */
export type GraphRagRequest = {
  vector_collection: string;
  graph: string;
  query_vector: number[];
  /** Default: 8. */
  seed_k?: number;
  /** Default: 2. */
  max_depth?: number;
  /** Default: 64. */
  max_nodes?: number;
  /** Default: 256. */
  max_edges?: number;
  /** Default: 16384 UTF-8 bytes. */
  max_context_bytes?: number;
  /** Default: ["related"]. */
  labels?: string[];
  /** Metadata path. Default: ["text"]. */
  text_field?: string[];
  /** Metadata path. Default: ["source"]. */
  source_field?: string[];
};

export type GraphRagSource = {
  document_id: string;
  text: string;
  source: string | null;
  seed_distance: number;
  depth: number;
  seed_id: string;
};

export type GraphRagRelationship = {
  source: string;
  label: string;
  target: string;
};

export type GraphRagResult = {
  context: string;
  sources: GraphRagSource[];
  relationships: GraphRagRelationship[];
  truncated: boolean;
};

/** GraphRAG node names are the lowercase 32-character hex ids returned by insertVector. */
export type GraphEdgeRequest = {
  source: string;
  label: string;
  target: string;
  props?: JsonValue;
};

export type GraphEdgeWriteResult = { written: true };
export type VectorInsertResult = { id: string };

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

export type ControlPlaneOperation = {
  method: string;
  path: string;
  operation_id: string;
  auth_required: boolean;
  stability: "stable" | "preview" | string;
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
  openapi_endpoint: string;
  physical_migration_supported: boolean;
  config_apply_data_mutated: boolean;
  /** True when the server provisions specs (`/provision/*`, preview). */
  provisioning_supported?: boolean;
  endpoints: string[];
  operations: ControlPlaneOperation[];
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
  | "time_series"
  /** Spec version 2 only; needs `physical.geo`. */
  | "geo";

/**
 * `DatabaseSpec.custom_profile` (spec version 2): a profile that extends one built-in
 * product slug or alias. `knobs` holds only the overridden knob leaves, keyed by section
 * (for example `{ cache: { max_entries: 8192 } }`); `docs/profiles.md` lists every knob.
 * Floors can be raised but never lowered.
 */
export type CustomProfileSpec = JsonObject & {
  extends: string;
  description?: string;
  knobs?: JsonObject;
  floors?: JsonObject & {
    audit?: boolean;
    backup?: boolean;
    pitr?: boolean;
    encryption_at_rest?: boolean;
  };
  compatible_roles?: JsonObject & {
    add?: CollectionRole[];
    remove?: CollectionRole[];
  };
};

export type PhysicalKind = "document" | "table" | "vector" | "time_series" | "graph";
export type PhysicalColumnType = "int" | "float" | "str" | "bool" | "bytes";

/**
 * `collections[].physical` (spec version 2): the parameters provisioning needs to build a
 * collection. One flat object for every kind; `docs/provisioning.md` lists which fields apply
 * to which kind and their defaults.
 */
export type PhysicalObjectSpec = JsonObject & {
  kind?: PhysicalKind;
  collection_id?: number;
  graph_id?: number;
  fields?: (JsonObject & { name: string; path: string; type: PhysicalColumnType })[];
  document_indexes?: (JsonObject & { path: string; id?: number })[];
  full_text?: (JsonObject & {
    path: string;
    name?: string;
    language?: string;
    refresh_lag_target?: number;
  })[];
  geo?: (JsonObject & { path: string; name?: string; precision?: number })[];
  layout?: "row" | "columnar";
  columns?: (JsonObject & { name: string; type: PhysicalColumnType; nullable?: boolean })[];
  primary_key?: string;
  table_indexes?: (JsonObject & {
    column: string;
    expression?: "column" | "lower_ascii";
    include?: string[];
    id?: number;
  })[];
  system_versioning?: boolean;
  keep_history?: boolean;
  dim?: number;
  metric?: "cosine" | "l2";
  hnsw?: JsonObject & { m?: number; ef_construction?: number; ef_search?: number };
  chunk_millis?: number;
  retention_millis?: number;
  /** Left out: 300000. `null`: no bound, as export writes a series that accepts any future timestamp. */
  max_future_skew_millis?: number | null;
};

export type PermissionSpec = "read" | "write" | "admin";

/**
 * `security` (spec version 2): managed roles and principals. A grant names one spec object
 * (`object`) or the whole database (`resource: "database"`); there are no secrets and no
 * system scope.
 */
export type SecuritySpec = JsonObject & {
  roles?: (JsonObject & {
    name: string;
    grants: (JsonObject & {
      object?: string;
      resource?: "database";
      permissions: PermissionSpec[];
    })[];
  })[];
  principals?: (JsonObject & { user: string; principal?: string; roles: string[] })[];
};

export type UnmanagedPolicy = "ignore" | "report" | "fail";

/** `provisioning` (spec version 2): how a plan treats live objects the spec does not declare. */
export type ProvisioningPolicySpec = JsonObject & {
  unmanaged?: UnmanagedPolicy;
};

export type DatabaseSpec = JsonObject & {
  /** Spec version: 1, or 2 for specs that use `custom_profile`, `security`, `provisioning`,
   * `collections[].physical` or the `geo` index kind. */
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
      /** Version 2 only. */
      physical?: PhysicalObjectSpec;
    }
  )[];
  extensions: JsonValue[];
  overrides: JsonObject;
  operation_hints: JsonObject;
  /** Version 2 only. */
  custom_profile?: CustomProfileSpec;
  /** Version 2 only. */
  security?: SecuritySpec;
  /** Version 2 only. */
  provisioning?: ProvisioningPolicySpec;
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

/** A built-in variant of a product profile, pinned to one storage profile. */
export type ProfileVariantSummary = {
  name: string;
  storage_profile: string;
  canonical: boolean;
};

export type ProfileSummary = {
  slug: string;
  aliases: string[];
  status: string;
  description: string;
  default_domain: string;
  compatible_roles: string[];
  /** The product's variants; exactly one is canonical. */
  variants?: ProfileVariantSummary[];
  /** The preset knobs of the canonical variant. */
  knobs?: JsonObject;
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

export type ApplyCheckReport = {
  plan_id: string;
  status: "confirmed" | "rejected" | "unsupported";
  valid: boolean;
  confirmation_matched: boolean;
  audit_recorded: boolean;
  data_mutated: boolean;
  message: string;
};

/** Per-run provisioning options; part of the `prov_` id, so an apply repeats the plan's. */
export type ProvisionOptions = {
  allow_weakening?: string[];
  allow_destructive?: string[];
  allow_format_upgrade?: boolean;
  unmanaged?: UnmanagedPolicy;
};

export type ProvisionSurface = "rust_api" | "cli" | "serve" | "control_plane";

export type StepSafety =
  | "additive"
  | "metadata"
  | "retune"
  | "strengthening"
  | "weakening"
  | "destructive"
  | "format_upgrade"
  | "unsupported";

export type ProvisionStepKind =
  | "create_database"
  | "adopt_profile"
  | "retune_profile"
  | "upgrade_storage_format"
  | "enable_audit"
  | "disable_audit"
  | "create_collection"
  | "create_vector_collection"
  | "create_table"
  | "create_time_series"
  | "create_graph"
  | "adopt_object"
  | "create_full_text_index"
  | "create_geo_index"
  | "enable_system_versioning"
  | "apply_security"
  | "release_object"
  | "record_guarantees"
  | "record_metadata"
  | "record_applied_spec"
  | "noop";

export type ProvisionStep = {
  /** `pstep-001`, `pstep-002`, ... in execution order. */
  step_id: string;
  kind: ProvisionStepKind;
  path: string;
  object?: string | null;
  action: string;
  safety: StepSafety;
  impact: PlanImpact;
  supported: boolean;
  /** `allow_weakening:<path>`, `allow_destructive:<path>` or `allow_format_upgrade`. */
  requires_flag?: string | null;
};

export type PlanOutcome = "noop" | "ready" | "needs_flags" | "blocked";

export type EnforcementStatus =
  | "enforced"
  | "enforced_with_development_key"
  | "trivially_satisfied"
  | "operator_managed"
  | "not_enforced"
  | "not_enforceable"
  | "not_requested";

/** How one guarantee of a spec is enforced on the surface that provisions it. */
export type GuaranteeEnforcement = {
  path: string;
  requested: string;
  status: EnforcementStatus;
  detail: string;
  remediation: string;
};

export type DriftItem = {
  path: string;
  kind: string;
  expected: string;
  actual: string;
  blocking: boolean;
  resolution: string;
};

export type DriftReport = {
  /** Not blocked, and every item is information only. */
  in_sync: boolean;
  items: DriftItem[];
};

export type PlannedObject = {
  name: string;
  spec_collection?: string | null;
  kind: string;
  state: "create" | "satisfied" | "adopt" | "conflict" | "release" | "unmanaged";
  params: JsonValue;
  diff: (JsonObject & { field: string })[];
};

/** A provisioning plan (`POST /provision/plan`); see `docs/provisioning.md`. */
export type ProvisionPlan = {
  plan_format: number;
  /** `prov_` and 32 hex characters. */
  plan_id: string;
  surface: ProvisionSurface;
  target: JsonObject & { kind: "new_database" | "existing_database" };
  spec_digest: string;
  live_fingerprint?: string | null;
  resolved_profile?: JsonObject | null;
  validation: ValidationReport;
  issues: ValidationIssue[];
  objects: PlannedObject[];
  guarantees: GuaranteeEnforcement[];
  drift: DriftReport;
  steps: ProvisionStep[];
  spec_migration?: MigrationPlan | null;
  outcome: PlanOutcome;
  required_flags: string[];
  auto_approvable: boolean;
  impact: PlanImpact;
  rollback: RollbackPlan;
  /** The `plan_id` an apply must confirm; `null` for a noop. */
  required_confirmation?: string | null;
};

export type ProvisionOutcome =
  | "applied"
  | "noop"
  | "rejected"
  | "blocked"
  | "plan_changed"
  | "partially_applied";

export type AppliedStep = {
  step_id: string;
  kind: ProvisionStepKind;
  object?: string | null;
  outcome: "applied" | "already_satisfied" | "failed";
  detail: string;
  error_kind?: string;
};

/** What an apply did (`POST /provision/apply`). */
export type ProvisionApplyReport = {
  status: ProvisionOutcome;
  check: ApplyCheckReport;
  plan_id: string;
  applied_steps: AppliedStep[];
  failed_step?: string | null;
  record?: JsonObject | null;
  warnings: ValidationIssue[];
  /** A retune or the storage format upgrade takes effect at the next restart. */
  session_pending_restart: boolean;
};

export type ProvisionRecord = JsonObject & {
  record_version: number;
  status: "in_progress" | "applied" | "failed";
  plan_id: string;
  spec_digest: string;
};

/** `GET /provision/state`. */
export type ProvisionState = {
  provisioned: boolean;
  record: ProvisionRecord | null;
  history: JsonObject[];
  manifest: JsonObject | null;
  live_fingerprint: string;
  /** The spec `multidb serve` started from in spec mode. */
  spec_mode: { spec_digest: string; apply_mode: "verify" | "create" | "additive" } | null;
};

/** `GET /config/live`: the served database described as a spec. */
export type LiveConfig = {
  spec: DatabaseSpec;
  source: "applied_record" | "inferred";
  notes: string[];
  live_fingerprint: string;
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

export class ControlPlaneError extends Error {
  readonly status: number;
  readonly code: string;
  readonly body?: unknown;

  constructor(
    message: string,
    options: { status: number; code: string; body?: unknown },
  ) {
    super(message);
    this.name = "ControlPlaneError";
    this.status = options.status;
    this.code = options.code;
    this.body = options.body;
  }
}

export type ControlPlaneClientOptions = {
  baseUrl?: string;
  token?: string;
  principal?: string | undefined;
  fetchImpl?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
};

export type ConfigPlanRequest = {
  current: JsonObject;
  desired: JsonObject;
};

export const CONTROL_PLANE_API_VERSION = 1;
export const MIN_MULTIDB_VERSION = "0.1.0";

const DEFAULT_BASE_URL = "http://127.0.0.1:8080/api";

const trimTrailingSlash = (value: string): string => value.replace(/\/+$/, "");

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isEnvelope = <T>(value: unknown): value is ApiEnvelope<T> =>
  isObject(value) && typeof value.ok === "boolean";

export const defaultApiBase = (): string => DEFAULT_BASE_URL;

const provisionBody = (
  spec: JsonObject,
  options: ProvisionOptions | undefined,
): { spec: JsonObject; options?: ProvisionOptions } =>
  options === undefined ? { spec } : { spec, options };

export class ControlPlaneClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly principal: string | undefined;
  private readonly fetchImpl: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

  constructor(options: ControlPlaneClientOptions = {}) {
    this.baseUrl = trimTrailingSlash(options.baseUrl || DEFAULT_BASE_URL);
    this.token = options.token?.trim() ?? "";
    this.principal = options.principal?.trim() || undefined;
    this.fetchImpl = options.fetchImpl ?? ((input, init) => fetch(input, init));
  }

  withToken(token: string, principal?: string): ControlPlaneClient {
    const options: ControlPlaneClientOptions = {
      baseUrl: this.baseUrl,
      token,
      fetchImpl: this.fetchImpl,
    };
    const nextPrincipal = principal ?? this.principal;
    if (nextPrincipal !== undefined) {
      options.principal = nextPrincipal;
    }
    return new ControlPlaneClient(options);
  }

  openApi(): Promise<JsonObject> {
    return this.rawRequest<JsonObject>("/openapi.json", { auth: false });
  }

  status(): Promise<AdminStatus> {
    return this.request<AdminStatus>("/status");
  }

  health(): Promise<HealthResponse> {
    return this.rawRequest<HealthResponse>("/health", { auth: false });
  }

  ready(): Promise<HealthResponse> {
    return this.rawRequest<HealthResponse>("/ready", { auth: false });
  }

  metrics(): Promise<string> {
    return this.textRequest("/metrics");
  }

  login(username: string, password: string): Promise<LoginResponse> {
    return this.request<LoginResponse>("/auth/login", {
      method: "POST",
      body: { username, password },
      auth: false,
    });
  }

  logout(): Promise<JsonObject> {
    return this.request<JsonObject>("/auth/logout", { method: "POST" });
  }

  changePassword(currentPassword: string, newPassword: string): Promise<JsonObject> {
    return this.request<JsonObject>("/auth/change-password", {
      method: "POST",
      body: {
        current_password: currentPassword,
        new_password: newPassword,
      },
    });
  }

  authMe(): Promise<AuthMe> {
    return this.request<AuthMe>("/auth/me");
  }

  catalog(): Promise<CatalogResponse> {
    return this.request<CatalogResponse>("/catalog");
  }

  security(): Promise<SecurityState> {
    return this.request<SecurityState>("/security");
  }

  saveSecurity(security: SecurityState): Promise<SecurityState> {
    return this.request<SecurityState>("/security", {
      method: "POST",
      body: security,
    });
  }

  audit(): Promise<AuditResponse> {
    return this.request<AuditResponse>("/audit");
  }

  config(): Promise<DatabaseSpec> {
    return this.request<DatabaseSpec>("/config");
  }

  manifest(): Promise<StudioManifest> {
    return this.request<StudioManifest>("/studio");
  }

  profiles(): Promise<ProfileSummary[]> {
    return this.request<ProfileSummary[]>("/profiles");
  }

  roles(): Promise<RoleSummary[]> {
    return this.request<RoleSummary[]>("/roles");
  }

  domains(): Promise<DomainSummary[]> {
    return this.request<DomainSummary[]>("/domains");
  }

  extensions(): Promise<ExtensionSummary[]> {
    return this.request<ExtensionSummary[]>("/extensions");
  }

  advice(): Promise<RuntimeAdviceReport> {
    return this.request<RuntimeAdviceReport>("/advice");
  }

  advicePlan(adviceId: string): Promise<MigrationPlan> {
    return this.request<MigrationPlan>("/advice/plan", {
      method: "POST",
      body: { advice_id: adviceId },
    });
  }

  recordAdviceDecision(
    adviceId: string,
    status: RuntimeAdviceStatus,
    reason: string,
  ): Promise<RuntimeAdviceDecision> {
    return this.request<RuntimeAdviceDecision>("/advice/decision", {
      method: "POST",
      body: { advice_id: adviceId, status, reason },
    });
  }

  sql(sql: string): Promise<SqlResponse> {
    return this.request<SqlResponse>("/sql", {
      method: "POST",
      body: { sql },
    });
  }

  tableRows(table: string, options: { offset?: number; limit?: number } = {}): Promise<TableRowsResponse> {
    const params = new URLSearchParams({
      offset: String(options.offset ?? 0),
      limit: String(options.limit ?? 100),
    });
    return this.request<TableRowsResponse>(
      `/data/tables/${encodeURIComponent(table)}/rows?${params.toString()}`,
    );
  }

  insertTableRow(table: string, row: JsonValue[]): Promise<JsonObject> {
    return this.request<JsonObject>(`/data/tables/${encodeURIComponent(table)}/rows`, {
      method: "POST",
      body: { row },
    });
  }

  updateTableRow(table: string, row: JsonValue[]): Promise<JsonObject> {
    return this.request<JsonObject>(`/data/tables/${encodeURIComponent(table)}/rows`, {
      method: "PUT",
      body: { row },
    });
  }

  deleteTableRow(table: string, primaryKey: JsonValue, confirm: string): Promise<JsonObject> {
    return this.request<JsonObject>(`/data/tables/${encodeURIComponent(table)}/rows`, {
      method: "DELETE",
      body: { primary_key: primaryKey, confirm },
    });
  }

  documents(collection: string, options: { offset?: number; limit?: number } = {}): Promise<DocumentListResponse> {
    const params = new URLSearchParams({
      offset: String(options.offset ?? 0),
      limit: String(options.limit ?? 100),
    });
    return this.request<DocumentListResponse>(
      `/data/collections/${encodeURIComponent(collection)}/documents?${params.toString()}`,
    );
  }

  createDocument(collection: string, document: JsonValue): Promise<{ id: string }> {
    return this.request<{ id: string }>(
      `/data/collections/${encodeURIComponent(collection)}/documents`,
      { method: "POST", body: { document } },
    );
  }

  updateDocument(collection: string, id: string, document: JsonValue): Promise<JsonObject> {
    return this.request<JsonObject>(
      `/data/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(id)}`,
      { method: "PUT", body: { document } },
    );
  }

  deleteDocument(collection: string, id: string, confirm: string): Promise<JsonObject> {
    return this.request<JsonObject>(
      `/data/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(id)}`,
      { method: "DELETE", body: { confirm } },
    );
  }

  createTable(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/table", { method: "POST", body: request });
  }

  createCollection(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/collection", { method: "POST", body: request });
  }

  createVector(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/vector", { method: "POST", body: request });
  }

  createTimeSeries(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/time-series", { method: "POST", body: request });
  }

  createFullText(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/full-text", { method: "POST", body: request });
  }

  createGeoIndex(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/geo", { method: "POST", body: request });
  }

  createGraph(request: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>("/builder/graph", { method: "POST", body: request });
  }

  addGraphEdge(graph: string, request: GraphEdgeRequest): Promise<GraphEdgeWriteResult> {
    return this.request<GraphEdgeWriteResult>(`/data/graphs/${encodeURIComponent(graph)}/edges`, {
      method: "POST",
      body: request,
    });
  }

  graphRagQuery(request: GraphRagRequest): Promise<GraphRagResult> {
    return this.request<GraphRagResult>("/graphrag/query", { method: "POST", body: request });
  }

  insertVector(collection: string, metadata: JsonValue, vector: number[]): Promise<VectorInsertResult> {
    return this.request<VectorInsertResult>(`/data/vectors/${encodeURIComponent(collection)}/vectors`, {
      method: "POST",
      body: { metadata, vector },
    });
  }

  searchVector(collection: string, vector: number[], k: number): Promise<JsonObject> {
    return this.request<JsonObject>(`/data/vectors/${encodeURIComponent(collection)}/search`, {
      method: "POST",
      body: { vector, k },
    });
  }

  timeSeriesPoints(collection: string, series: string, start: number, end: number): Promise<JsonObject> {
    const params = new URLSearchParams({ series, start: String(start), end: String(end) });
    return this.request<JsonObject>(
      `/data/time-series/${encodeURIComponent(collection)}/points?${params.toString()}`,
    );
  }

  insertTimeSeriesPoint(collection: string, series: string, point: JsonObject): Promise<JsonObject> {
    return this.request<JsonObject>(
      `/data/time-series/${encodeURIComponent(collection)}/points`,
      { method: "POST", body: { series, point } },
    );
  }

  validate(spec: JsonObject): Promise<ValidationReport> {
    return this.request<ValidationReport>("/config/validate", {
      method: "POST",
      body: spec,
    });
  }

  plan(request: ConfigPlanRequest): Promise<MigrationPlan> {
    return this.request<MigrationPlan>("/config/plan", {
      method: "POST",
      body: request,
    });
  }

  apply(plan: MigrationPlan, confirm: string): Promise<JsonObject> {
    return this.request<JsonObject>("/config/apply", {
      method: "POST",
      body: { plan, confirm },
    });
  }

  /** What provisioning manages in the served database (preview). */
  provisionState(): Promise<ProvisionState> {
    return this.request<ProvisionState>("/provision/state");
  }

  /**
   * Plans `spec` against the served database (preview, a dry run). A blocked plan is HTTP 422:
   * the thrown `ControlPlaneError` carries the plan as `body.data`.
   */
  provisionPlan(spec: JsonObject, options?: ProvisionOptions): Promise<ProvisionPlan> {
    return this.request<ProvisionPlan>("/provision/plan", {
      method: "POST",
      body: provisionBody(spec, options),
    });
  }

  /**
   * Applies `spec` when `confirm` is the `prov_` id of its current plan (preview). The server
   * recomputes the plan; pass the options the plan was computed with.
   */
  provisionApply(
    spec: JsonObject,
    confirm: string,
    options?: ProvisionOptions,
  ): Promise<ProvisionApplyReport> {
    return this.request<ProvisionApplyReport>("/provision/apply", {
      method: "POST",
      body: { ...provisionBody(spec, options), confirm },
    });
  }

  /** How the served database differs from `spec` (preview). */
  provisionDrift(spec: JsonObject, options?: ProvisionOptions): Promise<DriftReport> {
    return this.request<DriftReport>("/provision/drift", {
      method: "POST",
      body: provisionBody(spec, options),
    });
  }

  /** The served database described as a spec (preview). */
  liveConfig(): Promise<LiveConfig> {
    return this.request<LiveConfig>("/config/live");
  }

  async loadStudioData(): Promise<LoadedStudioData> {
    const [
      health,
      readiness,
      auth,
      status,
      catalog,
      security,
      audit,
      config,
      manifest,
      profiles,
      roles,
      domains,
      extensions,
    ] = await Promise.all([
      this.health(),
      this.ready(),
      this.authMe(),
      this.status(),
      this.catalog(),
      this.security(),
      this.audit(),
      this.config(),
      this.manifest(),
      this.profiles(),
      this.roles(),
      this.domains(),
      this.extensions(),
    ]);

    try {
      const advice = await this.advice();
      return { health, readiness, auth, status, catalog, security, audit, config, manifest, profiles, roles, domains, extensions, advice };
    } catch (error) {
      return { health, readiness, auth, status, catalog, security, audit, config, manifest, profiles, roles, domains, extensions, adviceError: errorMessage(error) };
    }
  }

  private async request<T>(
    path: string,
    options: { method?: "GET" | "POST" | "PUT" | "DELETE"; body?: unknown; auth?: boolean } = {},
  ): Promise<T> {
    const response = await this.fetchJson(path, options);
    if (!isEnvelope<T>(response.payload)) {
      throw new ControlPlaneError("Control Plane returned an invalid envelope", {
        status: response.status,
        code: "invalid_envelope",
        body: response.payload,
      });
    }
    if (!response.payload.ok) {
      throw new ControlPlaneError(response.payload.error.message, {
        status: response.status,
        code: response.payload.error.code,
        body: response.payload,
      });
    }
    return response.payload.data;
  }

  private async rawRequest<T>(
    path: string,
    options: { method?: "GET" | "POST" | "PUT" | "DELETE"; body?: unknown; auth?: boolean } = {},
  ): Promise<T> {
    return (await this.fetchJson(path, options)).payload as T;
  }

  private async textRequest(path: string): Promise<string> {
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method: "GET",
      headers: this.headers(undefined, true),
    });
    return response.text();
  }

  private async fetchJson(
    path: string,
    options: { method?: "GET" | "POST" | "PUT" | "DELETE"; body?: unknown; auth?: boolean },
  ): Promise<{ status: number; payload: unknown }> {
    const init: RequestInit = {
      method: options.method ?? "GET",
      headers: this.headers(options.body, options.auth !== false),
    };
    if (options.body !== undefined) {
      init.body = JSON.stringify(options.body);
    }
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, init);
    try {
      return { status: response.status, payload: await response.json() };
    } catch {
      throw new ControlPlaneError("Control Plane did not return JSON", {
        status: response.status,
        code: "invalid_json",
      });
    }
  }

  private headers(body: unknown, auth: boolean): Record<string, string> {
    const headers: Record<string, string> = { Accept: "application/json" };
    if (auth) {
      headers.Authorization = `Bearer ${this.token}`;
    }
    if (auth && this.principal !== undefined) {
      headers["x-multidb-principal"] = this.principal;
    }
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
    }
    return headers;
  }
}

export const parseJsonObject = (text: string): JsonObject => {
  const parsed: unknown = JSON.parse(text);
  if (!isObject(parsed)) {
    throw new Error("Expected a JSON object at the top level.");
  }
  return parsed as JsonObject;
};

export const stringifyJson = (value: JsonValue): string =>
  JSON.stringify(value, null, 2);

export const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);
