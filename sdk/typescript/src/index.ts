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

  insertVector(collection: string, metadata: JsonValue, vector: number[]): Promise<JsonObject> {
    return this.request<JsonObject>(`/data/vectors/${encodeURIComponent(collection)}/vectors`, {
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
