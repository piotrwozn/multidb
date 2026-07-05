import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  ClipboardCheck,
  Database,
  FileJson,
  Hammer,
  KeyRound,
  Layers3,
  ListChecks,
  Loader2,
  LogOut,
  Moon,
  Play,
  Plus,
  RefreshCw,
  ScrollText,
  Search,
  ShieldCheck,
  Sun,
  Table2,
  TerminalSquare,
  Trash2,
  Users,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import type { ComponentType, ReactNode } from "react";

import {
  ControlPlaneError,
  ControlPlaneClient,
  defaultApiBase,
  errorMessage,
  parseJsonObject,
  stringifyJson,
} from "./api";
import type {
  AuditEvent,
  CatalogObjectSummary,
  CollectionIndexKind,
  CollectionRole,
  ConflictResolution,
  ConsistencyMode,
  DatabaseSpec,
  DeploymentMode,
  DocumentListResponse,
  JsonObject,
  JsonValue,
  LoadedStudioData,
  MigrationPlan,
  ReplicationMode,
  Row,
  SecurityState,
  SqlResponse,
  TableRowsResponse,
  ValidationIssue,
  ValidationReport,
  WriteAck,
} from "./types";

type ViewId =
  | "overview"
  | "data"
  | "sql"
  | "builder"
  | "security"
  | "audit"
  | "advice"
  | "config";

type BuilderKind =
  | "table"
  | "collection"
  | "vector"
  | "time_series"
  | "full_text"
  | "geo"
  | "graph"
  | "database"
  | "custom_profile";

type ThemeMode = "light" | "dark";

type ResourceKind =
  | "Database"
  | "System"
  | "Table"
  | "Collection"
  | "VectorCollection"
  | "FullTextIndex"
  | "TimeSeries"
  | "Graph"
  | "GeoIndex";

type NavItem = {
  id: ViewId;
  label: string;
  icon: ComponentType<{ size?: number; "aria-hidden"?: boolean }>;
};

const navItems: NavItem[] = [
  { id: "overview", label: "Overview", icon: Activity },
  { id: "data", label: "Data Explorer", icon: Table2 },
  { id: "sql", label: "SQL Console", icon: TerminalSquare },
  { id: "builder", label: "Builder", icon: Hammer },
  { id: "security", label: "Security", icon: Users },
  { id: "audit", label: "Audit", icon: ScrollText },
  { id: "advice", label: "Advice", icon: ShieldCheck },
  { id: "config", label: "Config", icon: FileJson },
];

const builderKinds: BuilderKind[] = [
  "table",
  "collection",
  "vector",
  "time_series",
  "full_text",
  "geo",
  "graph",
  "database",
  "custom_profile",
];

const resourceKinds: ResourceKind[] = [
  "Database",
  "System",
  "Table",
  "Collection",
  "VectorCollection",
  "FullTextIndex",
  "TimeSeries",
  "Graph",
  "GeoIndex",
];

const permissionOptions: ("Read" | "Write" | "Admin")[] = ["Read", "Write", "Admin"];

const deploymentModes: DeploymentMode[] = ["embedded", "single_node", "cluster"];
const replicationModes: ReplicationMode[] = ["cp", "ap"];
const consistencyModes: ConsistencyMode[] = [
  "local_snapshot",
  "strong_cp",
  "eventual_ap",
];
const writeAckOptions: WriteAck[] = ["local", "quorum", "all"];
const conflictOptions: ConflictResolution[] = [
  "none",
  "last_write_wins",
  "vector_clock",
  "crdt",
  "custom",
];
const collectionRoles: CollectionRole[] = [
  "document_entity",
  "key_value",
  "event_log",
  "vector_memory",
  "cache",
  "audit",
  "graph",
  "analytics",
  "time_series",
];
const collectionIndexes: CollectionIndexKind[] = [
  "primary",
  "document",
  "vector",
  "graph",
  "full_text",
  "columnar",
  "time_series",
];

const deploymentHelp: Record<DeploymentMode, string> = {
  embedded:
    "Runs inside one process. Use for desktop/dev flows; risk: no extra runtime replica.",
  single_node:
    "One server owns the database. Use for simple production or staging; risk: replica_count must stay 1.",
  cluster:
    "Multiple nodes are planned by policy. Use for HA or scale-out; risk: this release creates plan and audit only.",
};

const replicationHelp: Record<ReplicationMode, string> = {
  cp: "Consistency-first replication. Use when correctness beats availability; cluster CP needs odd quorum replicas.",
  ap: "Availability-first replication. Use for eventually consistent workloads; risk: conflicts need an explicit policy.",
};

const consistencyHelp: Record<ConsistencyMode, string> = {
  local_snapshot:
    "Local snapshot semantics. Use for embedded or single-node resources; risk: not a cross-node guarantee.",
  strong_cp:
    "Strong CP semantics. Use for accounts, security, money and admin state; risk: quorum can reject writes during partitions.",
  eventual_ap:
    "Eventual AP semantics. Use for caches, feeds and telemetry; risk: conflicts must be resolved.",
};

const writeAckHelp: Record<WriteAck, string> = {
  local: "Ack after local write. Fastest; risk: weakest durability if the node fails.",
  quorum: "Ack after quorum. Good default for CP clusters; risk: needs enough healthy replicas.",
  all: "Ack after every replica. Strongest durability; risk: slowest and least tolerant of slow nodes.",
};

const conflictHelp: Record<ConflictResolution, string> = {
  none: "No conflict resolver. Use only when conflicts cannot happen; risk: AP domains will be rejected.",
  last_write_wins:
    "Latest timestamp wins. Simple operational default; risk: can discard concurrent updates.",
  vector_clock:
    "Tracks causality. Use for AP documents and sync; risk: callers may need merge handling.",
  crdt: "Mergeable data types. Use for counters/sets/collab state; risk: data model must fit CRDT semantics.",
  custom:
    "Application resolver. Use for domain-specific merges; risk: operator must deploy resolver logic.",
};

const defaultColumns = stringifyJson([
  { name: "id", ty: "Int", nullable: false },
  { name: "name", ty: "Str", nullable: false },
]);

const defaultCollectionFields = stringifyJson([
  { name: "id", source: "DocumentId", ty: "Bytes" },
  { name: "name", source: { Path: ["name"] }, ty: "Str" },
]);

const shortJson = (value: JsonValue): string =>
  typeof value === "string" ? value : JSON.stringify(value);

const parseJsonValue = (value: string): JsonValue => JSON.parse(value) as JsonValue;

const jsonInputError = (
  value: string,
  validate?: (parsed: unknown) => string | undefined,
): string | undefined => {
  try {
    const parsed: unknown = JSON.parse(value);
    return validate?.(parsed);
  } catch (error) {
    return errorMessage(error);
  }
};

const rowJsonError = (value: string): string | undefined =>
  jsonInputError(value, (parsed) =>
    Array.isArray(parsed) ? undefined : "Expected a JSON array for a table row.",
  );

const vectorJsonError = (value: string): string | undefined =>
  jsonInputError(value, (parsed) =>
    Array.isArray(parsed) && parsed.every((item) => typeof item === "number")
      ? undefined
      : "Expected a JSON array of numbers.",
  );

const objectJsonError = (value: string): string | undefined =>
  jsonInputError(value, (parsed) =>
    typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
      ? undefined
      : "Expected a JSON object.",
  );

const parseJsonArray = (value: string): number[] => {
  const parsed = JSON.parse(value);
  if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "number")) {
    throw new Error("Expected a JSON array of numbers.");
  }
  return parsed as number[];
};

const formatUptime = (millis: number): string => {
  const seconds = Math.floor(millis / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
};

const normalizeDatabaseSpec = (spec: DatabaseSpec): DatabaseSpec => ({
  ...spec,
  version: spec.version ?? 1,
  name: spec.name ?? "database",
  profile: spec.profile ?? "durable",
  deployment: spec.deployment ?? { mode: "single_node", storage_path: null },
  topology: spec.topology ?? { replica_count: 1, shard_count: 1 },
  defaults: spec.defaults ?? { consistency_domain: "primary", replication: "cp" },
  guarantees: spec.guarantees ?? {
    write_ack: "quorum",
    conflict_resolution: "none",
    backup: { enabled: false, pitr: false },
    encryption: { at_rest: false },
    audit: { enabled: true },
    sensitive_data: false,
    strict_cross_domain_transactions: false,
  },
  domains: spec.domains ?? [{ name: "primary", mode: "local_snapshot" }],
  collections: spec.collections ?? [],
  extensions: spec.extensions ?? [],
  overrides: spec.overrides ?? {},
  operation_hints: spec.operation_hints ?? {},
});

const cloneDatabaseSpec = (spec: DatabaseSpec): DatabaseSpec =>
  JSON.parse(JSON.stringify(spec)) as DatabaseSpec;

const parseDesiredDatabaseSpec = (
  desiredSpec: string,
  fallback: DatabaseSpec,
): { spec: DatabaseSpec; error?: string } => {
  try {
    return {
      spec: normalizeDatabaseSpec(parseJsonObject(desiredSpec) as DatabaseSpec),
    };
  } catch (parseError) {
    return {
      spec: normalizeDatabaseSpec(fallback),
      error: errorMessage(parseError),
    };
  }
};

const positiveIntegerFromInput = (value: string, fallback: number): number => {
  if (value.trim().length === 0) return 0;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(0, Math.trunc(parsed));
};

const optionLabel = (value: string): string =>
  value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());

const readStoredTheme = (): ThemeMode => {
  try {
    return localStorage.getItem("multidb.theme") === "dark" ? "dark" : "light";
  } catch {
    return "light";
  }
};

const storeTheme = (theme: ThemeMode) => {
  try {
    localStorage.setItem("multidb.theme", theme);
  } catch {
    // Local storage can be disabled in embedded browsers; the in-memory state is enough.
  }
};

const resourceFromParts = (kind: ResourceKind, name: string): JsonValue => {
  if (kind === "Database" || kind === "System") return kind;
  return { [kind]: name.trim() || "resource_name" } as JsonObject;
};

const syncSecurityJson = (state: SecurityState): string =>
  stringifyJson(state as unknown as JsonValue);

const cloneSecurityState = (state: SecurityState): SecurityState =>
  JSON.parse(JSON.stringify(state)) as SecurityState;

const controlPlaneErrorMessage = (error: unknown): string => {
  if (error instanceof ControlPlaneError) {
    if (error.status === 403) {
      return `Forbidden (${error.code}): ${error.message}`;
    }
    return `${error.code}: ${error.message}`;
  }
  return errorMessage(error);
};

const databaseSpecForBuilder = ({
  name,
  profile,
  deploymentMode,
  storagePath,
  replication,
  replicaCount,
  shardCount,
  backupEnabled,
  auditEnabled,
}: {
  name: string;
  profile: string;
  deploymentMode: DeploymentMode;
  storagePath: string;
  replication: ReplicationMode;
  replicaCount: number;
  shardCount: number;
  backupEnabled: boolean;
  auditEnabled: boolean;
}): DatabaseSpec =>
  normalizeDatabaseSpec({
    version: 1,
    name: name.trim() || "new_database",
    profile: profile.trim() || "balanced",
    deployment: {
      mode: deploymentMode,
      storage_path: storagePath.trim().length === 0 ? null : storagePath.trim(),
    },
    topology: {
      replica_count: replicaCount,
      shard_count: shardCount,
    },
    defaults: {
      consistency_domain: "primary",
      replication,
    },
    guarantees: {
      write_ack: replication === "cp" ? "quorum" : "local",
      conflict_resolution: replication === "ap" ? "vector_clock" : "none",
      backup: { enabled: backupEnabled, pitr: backupEnabled },
      encryption: { at_rest: false },
      audit: { enabled: auditEnabled },
      sensitive_data: false,
      strict_cross_domain_transactions: replication === "cp",
    },
    domains: [
      {
        name: "primary",
        mode: replication === "ap" ? "eventual_ap" : "strong_cp",
      },
    ],
    collections: [],
    extensions: [],
    overrides: {},
    operation_hints: {
      "studio.builder": "database",
    },
  } as DatabaseSpec);

const issueCountFor = (
  report: ValidationReport | undefined,
  prefixes: string[],
): number =>
  report?.issues.filter((issue) => {
    const path = issue.path.replace(/^\$\./, "");
    return prefixes.some((prefix) => path === prefix || path.startsWith(`${prefix}.`));
  }).length ?? 0;

export default function App() {
  const [baseUrl, setBaseUrl] = useState(defaultApiBase());
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [client, setClient] = useState<ControlPlaneClient>();
  const [data, setData] = useState<LoadedStudioData>();
  const [view, setView] = useState<ViewId>("overview");
  const [desiredSpec, setDesiredSpec] = useState("");
  const [validation, setValidation] = useState<ValidationReport>();
  const [plan, setPlan] = useState<MigrationPlan>();
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState<string>();
  const [error, setError] = useState<string>();
  const [theme, setTheme] = useState<ThemeMode>(readStoredTheme);

  const toggleTheme = () => {
    setTheme((current) => {
      const next = current === "dark" ? "light" : "dark";
      storeTheme(next);
      return next;
    });
  };

  const clearSession = (message?: string) => {
    setClient(undefined);
    setData(undefined);
    setPassword("");
    if (message !== undefined) {
      setError(message);
    }
  };

  const handleClientError = (error: unknown) => {
    if (error instanceof ControlPlaneError && error.status === 401) {
      clearSession("Session expired. Sign in again.");
      return;
    }
    setError(controlPlaneErrorMessage(error));
  };

  const reportClientError = (error?: unknown) => {
    if (error === undefined) {
      setError(undefined);
      return;
    }
    handleClientError(error);
  };

  const connect = async () => {
    const authClient = new ControlPlaneClient({ baseUrl });
    setLoading(true);
    setError(undefined);
    try {
      const session = await authClient.login(username.trim(), password);
      const nextClient = new ControlPlaneClient({
        baseUrl,
        token: session.token,
      });
      const loaded = await nextClient.loadStudioData();
      setClient(nextClient);
      setData(loaded);
      setPassword("");
      setDesiredSpec(stringifyJson(loaded.config));
      setView("overview");
    } catch (loadError) {
      if (loadError instanceof ControlPlaneError && loadError.status === 401) {
        setError("Sign in failed. Check the admin username and password.");
      } else {
        setError(controlPlaneErrorMessage(loadError));
      }
      setClient(undefined);
      setData(undefined);
    } finally {
      setLoading(false);
    }
  };

  const refresh = async () => {
    if (client === undefined) return;
    setActionLoading("refresh");
    setError(undefined);
    try {
      const loaded = await client.loadStudioData();
      setData(loaded);
      setDesiredSpec((current) =>
        current.trim().length === 0 ? stringifyJson(loaded.config) : current,
      );
    } catch (refreshError) {
      handleClientError(refreshError);
    } finally {
      setActionLoading(undefined);
    }
  };

  const validateCurrent = async () => {
    if (client === undefined || data === undefined) return;
    setActionLoading("validate");
    setError(undefined);
    try {
      const desired = normalizeDatabaseSpec(parseJsonObject(desiredSpec) as DatabaseSpec);
      setValidation(await client.validate(desired));
      setView("config");
    } catch (validationError) {
      handleClientError(validationError);
    } finally {
      setActionLoading(undefined);
    }
  };

  const buildDryRun = async () => {
    if (client === undefined || data === undefined) return;
    setActionLoading("plan");
    setError(undefined);
    try {
      const desired = normalizeDatabaseSpec(parseJsonObject(desiredSpec) as DatabaseSpec);
      setPlan(await client.plan({ current: data.config, desired }));
      setView("config");
    } catch (planError) {
      handleClientError(planError);
    } finally {
      setActionLoading(undefined);
    }
  };

  const logout = async () => {
    if (client !== undefined) {
      try {
        await client.logout();
      } catch {
        // A failed logout still clears the browser-held session.
      }
    }
    clearSession();
  };

  const canConnect =
    baseUrl.trim().length > 0 &&
    username.trim().length > 0 &&
    password.length > 0 &&
    !loading;

  if (data === undefined || client === undefined) {
    return (
      <main className="login-shell" data-theme={theme}>
        <section className="login-panel" aria-label="Control Plane connection">
          <div className="login-toolbar">
            <button
              className="secondary-button"
              type="button"
              onClick={toggleTheme}
              aria-label="Toggle dark mode"
            >
              {theme === "dark" ? <Sun size={16} aria-hidden /> : <Moon size={16} aria-hidden />}
              {theme === "dark" ? "Light" : "Dark"}
            </button>
          </div>
          <div>
            <p className="eyebrow">MultiDB Studio</p>
            <h1>Admin v2</h1>
            <p className="supporting-copy">
              Sign in with the admin account to start a short Control Plane session.
            </p>
          </div>
          <label>
            <span>API base</span>
            <input value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} />
          </label>
          <label>
            <span>Username</span>
            <input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              autoComplete="username"
            />
          </label>
          <label>
            <span>Password</span>
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              type="password"
              autoComplete="current-password"
            />
          </label>
          <button className="primary-button" type="button" disabled={!canConnect} onClick={connect}>
            {loading ? <Loader2 size={18} aria-hidden /> : <KeyRound size={18} aria-hidden />}
            Sign in
          </button>
          {error !== undefined && <Notice tone="error" message={error} />}
        </section>
      </main>
    );
  }

  return (
    <main className="admin-shell" data-theme={theme}>
      <aside className="sidebar">
        <div className="brand-block">
          <Database size={22} aria-hidden />
          <div>
            <strong>MultiDB</strong>
            <span>Studio Admin v2</span>
          </div>
        </div>
        <nav aria-label="Admin views">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                className={view === item.id ? "nav-item active" : "nav-item"}
                type="button"
                onClick={() => setView(item.id)}
              >
                <Icon size={17} aria-hidden />
                {item.label}
              </button>
            );
          })}
        </nav>
      </aside>

      <section className="workspace">
        <header className="command-bar">
          <div>
            <p className="eyebrow">Connected as {data.auth.principal}</p>
            <h1>{viewTitle(view)}</h1>
          </div>
          <div className="bar-actions">
            <StatusChip active={data.auth.system_admin} label="System admin" />
            <StatusChip active={data.auth.database_admin} label="Database admin" />
            {data.auth.insecure_local_admin && <span className="tag tone-warning">local dev</span>}
            <button
              className="icon-button"
              type="button"
              onClick={toggleTheme}
              title={theme === "dark" ? "Light mode" : "Dark mode"}
              aria-label={theme === "dark" ? "Light mode" : "Dark mode"}
            >
              {theme === "dark" ? <Sun size={18} aria-hidden /> : <Moon size={18} aria-hidden />}
            </button>
            <button
              className="icon-button"
              type="button"
              onClick={refresh}
              disabled={actionLoading !== undefined}
              title="Refresh"
              aria-label="Refresh"
            >
              {actionLoading === "refresh" ? (
                <Loader2 size={18} aria-hidden />
              ) : (
                <RefreshCw size={18} aria-hidden />
              )}
            </button>
            <button
              className="icon-button"
              type="button"
              onClick={logout}
              title="Logout"
              aria-label="Logout"
            >
              <LogOut size={18} aria-hidden />
            </button>
          </div>
        </header>

        {error !== undefined && <Notice tone="error" message={error} />}

        {view === "overview" && <Overview data={data} />}
        {view === "data" && <DataExplorer client={client} data={data} onError={reportClientError} />}
        {view === "sql" && <SqlConsole client={client} onError={reportClientError} />}
        {view === "builder" && (
          <Builder client={client} data={data} onError={reportClientError} onCreated={refresh} />
        )}
        {view === "security" && (
          <SecurityPanel
            client={client}
            security={data.security}
            onError={reportClientError}
            onSaved={(security) => setData({ ...data, security })}
          />
        )}
        {view === "audit" && <AuditPanel events={data.audit.events} />}
        {view === "advice" && (
          <AdviceView client={client} data={data} onError={reportClientError} />
        )}
        {view === "config" && (
          <ConfigAndMigration
            data={data}
            desiredSpec={desiredSpec}
            validation={validation}
            plan={plan}
            actionLoading={actionLoading}
            onDesiredSpecChange={setDesiredSpec}
            onValidate={validateCurrent}
            onPlan={buildDryRun}
          />
        )}
      </section>
    </main>
  );
}

function Overview({ data }: { data: LoadedStudioData }) {
  const recentAudit = data.audit.events.slice(0, 5);
  return (
    <div className="view-stack">
      <section className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Runtime</p>
            <h2>Cluster Snapshot</h2>
          </div>
          <span className={data.health.ok && data.readiness.ok ? "status-pill" : "status-pill muted"}>
            <CheckCircle2 size={16} aria-hidden />
            {data.health.ok && data.readiness.ok ? "Healthy" : "Attention"}
          </span>
        </div>
        <div className="metric-grid">
          <Metric label="Health" value={data.health.status} />
          <Metric label="Readiness" value={data.readiness.status} />
          <Metric label="Server" value={data.status.server_version} />
          <Metric label="Uptime" value={formatUptime(data.status.uptime_millis)} />
          <Metric label="Profile" value={shortJson(data.status.profile)} />
          <Metric label="Replication" value={shortJson(data.status.replication)} />
          <Metric label="Layout" value={shortJson(data.status.layout)} />
          <Metric label="Engine" value={data.status.engine} />
          <Metric label="Catalog objects" value={String(data.catalog.objects.length)} />
          <Metric label="Shards" value={String(data.status.shard_count)} />
          <Metric label="Audit events" value={String(data.audit.events.length)} />
          <Metric label="Studio API" value={`v${data.manifest.api_version}`} />
        </div>
      </section>
      <section className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Catalog</p>
            <h2>Resource Map</h2>
          </div>
        </div>
        <div className="resource-grid">
          {data.catalog.objects.length === 0 ? (
            <EmptyState icon={Layers3} text="No catalog objects yet" />
          ) : (
            data.catalog.objects.map((object) => (
              <ResourceCard object={object} key={object.name} />
            ))
          )}
        </div>
      </section>
      <section className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Audit</p>
            <h2>Recent Events</h2>
          </div>
          <span className="tag">{recentAudit.length} shown</span>
        </div>
        <div className="table-list">
          {recentAudit.length === 0 ? (
            <EmptyState icon={ScrollText} text="No audit events recorded" />
          ) : (
            recentAudit.map((event) => (
              <article className="row-item" key={event.id}>
                <div>
                  <strong>{event.action}</strong>
                  <p>{event.principal ?? "system"} - {event.outcome}</p>
                  <small>{resourceLabel(event.resource)} {event.detail ?? ""}</small>
                </div>
                <span className="tag">{new Date(event.at_millis).toLocaleTimeString()}</span>
              </article>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function DataExplorer({
  client,
  data,
  onError,
}: {
  client: ControlPlaneClient;
  data: LoadedStudioData;
  onError: (error?: unknown) => void;
}) {
  const dataObjects = data.catalog.objects.filter((object) =>
    ["table", "collection", "vector", "time_series"].includes(object.kind),
  );
  const [selectedName, setSelectedName] = useState(dataObjects[0]?.name ?? "");
  const selected = dataObjects.find((object) => object.name === selectedName);
  const [tableRows, setTableRows] = useState<TableRowsResponse>();
  const [documents, setDocuments] = useState<DocumentListResponse>();
  const [editor, setEditor] = useState("[1, \"Ada\"]");
  const [primaryKey, setPrimaryKey] = useState("1");
  const [docId, setDocId] = useState("");
  const [confirm, setConfirm] = useState("");
  const [metadataEditor, setMetadataEditor] = useState("{\"label\":\"Ada\"}");
  const [vectorEditor, setVectorEditor] = useState("[1, 0, 0]");
  const [vectorK, setVectorK] = useState(5);
  const [seriesName, setSeriesName] = useState("cpu");
  const [pointEditor, setPointEditor] = useState(() =>
    stringifyJson({ timestamp_millis: Date.now(), value: 0.42 }),
  );
  const [rangeStart, setRangeStart] = useState(() => String(Date.now() - 60 * 60 * 1000));
  const [rangeEnd, setRangeEnd] = useState(() => String(Date.now() + 60 * 1000));
  const [operationResult, setOperationResult] = useState<JsonValue>();
  const [loading, setLoading] = useState(false);
  const [pageOffset, setPageOffset] = useState(0);
  const [pageLimit, setPageLimit] = useState(100);

  const loadSelected = async (offsetOverride = pageOffset) => {
    if (selected === undefined) return;
    setLoading(true);
    onError(undefined);
    try {
      if (selected.kind === "table") {
        const rows = await client.tableRows(selected.name, {
          offset: offsetOverride,
          limit: pageLimit,
        });
        setTableRows(rows);
        setPageOffset(rows.offset);
        setDocuments(undefined);
        setOperationResult(undefined);
      }
      if (selected.kind === "collection") {
        const docs = await client.documents(selected.name, {
          offset: offsetOverride,
          limit: pageLimit,
        });
        setDocuments(docs);
        setPageOffset(docs.offset);
        setTableRows(undefined);
        setOperationResult(undefined);
      }
      if (selected.kind === "time_series") {
        const points = await client.timeSeriesPoints(
          selected.name,
          seriesName,
          Number(rangeStart),
          Number(rangeEnd),
        );
        setTableRows(undefined);
        setDocuments(undefined);
        setOperationResult(points as unknown as JsonValue);
      }
      if (selected.kind === "vector") {
        setTableRows(undefined);
        setDocuments(undefined);
      }
    } catch (error) {
      onError(error);
    } finally {
      setLoading(false);
    }
  };

  const selectResource = (name: string) => {
    setSelectedName(name);
    setPageOffset(0);
    setTableRows(undefined);
    setDocuments(undefined);
    setOperationResult(undefined);
  };

  const loadPreviousPage = () => {
    const previous = Math.max(0, pageOffset - pageLimit);
    void loadSelected(previous);
  };

  const loadNextPage = () => {
    const next = tableRows?.next_offset ?? documents?.next_offset;
    if (next !== undefined && next !== null) {
      void loadSelected(next);
    }
  };

  const pageInfo = tableRows ?? documents;
  const canPage = selected?.kind === "table" || selected?.kind === "collection";
  const tableRowError = rowJsonError(editor);
  const primaryKeyError = jsonInputError(primaryKey);
  const documentJsonError = jsonInputError(editor);
  const metadataJsonError = jsonInputError(metadataEditor);
  const vectorError = vectorJsonError(vectorEditor);
  const pointJsonError = objectJsonError(pointEditor);
  const timeRangeError =
    Number.isFinite(Number(rangeStart)) && Number.isFinite(Number(rangeEnd))
      ? undefined
      : "Range start and end must be numeric millisecond timestamps.";

  const writeTable = async (mode: "insert" | "update") => {
    if (selected === undefined) return;
    try {
      if (tableRowError !== undefined) {
        throw new Error(tableRowError);
      }
      const row = JSON.parse(editor) as Row;
      if (mode === "insert") {
        await client.insertTableRow(selected.name, row);
      } else {
        await client.updateTableRow(selected.name, row);
      }
      await loadSelected();
    } catch (error) {
      onError(error);
    }
  };

  const insertVector = async () => {
    if (selected === undefined) return;
    try {
      if (metadataJsonError !== undefined) {
        throw new Error(metadataJsonError);
      }
      if (vectorError !== undefined) {
        throw new Error(vectorError);
      }
      const result = await client.insertVector(
        selected.name,
        parseJsonValue(metadataEditor),
        parseJsonArray(vectorEditor),
      );
      setOperationResult(result as unknown as JsonValue);
    } catch (error) {
      onError(error);
    }
  };

  const searchVector = async () => {
    if (selected === undefined) return;
    try {
      if (vectorError !== undefined) {
        throw new Error(vectorError);
      }
      const result = await client.searchVector(selected.name, parseJsonArray(vectorEditor), vectorK);
      setOperationResult(result as unknown as JsonValue);
    } catch (error) {
      onError(error);
    }
  };

  const insertTimeSeriesPoint = async () => {
    if (selected === undefined) return;
    try {
      if (pointJsonError !== undefined) {
        throw new Error(pointJsonError);
      }
      const result = await client.insertTimeSeriesPoint(
        selected.name,
        seriesName,
        parseJsonObject(pointEditor),
      );
      setOperationResult(result as unknown as JsonValue);
    } catch (error) {
      onError(error);
    }
  };

  const loadTimeSeriesRange = async () => {
    if (selected === undefined) return;
    try {
      if (timeRangeError !== undefined) {
        throw new Error(timeRangeError);
      }
      const result = await client.timeSeriesPoints(
        selected.name,
        seriesName,
        Number(rangeStart),
        Number(rangeEnd),
      );
      setOperationResult(result as unknown as JsonValue);
    } catch (error) {
      onError(error);
    }
  };

  const deleteRow = async () => {
    if (selected === undefined) return;
    try {
      if (primaryKeyError !== undefined) {
        throw new Error(primaryKeyError);
      }
      if (confirm !== selected.name) {
        throw new Error(`Type ${selected.name} to confirm row deletion.`);
      }
      await client.deleteTableRow(selected.name, JSON.parse(primaryKey), confirm);
      setConfirm("");
      await loadSelected();
    } catch (error) {
      onError(error);
    }
  };

  const writeDocument = async (mode: "create" | "update") => {
    if (selected === undefined) return;
    try {
      if (documentJsonError !== undefined) {
        throw new Error(documentJsonError);
      }
      if (mode === "update" && docId.trim().length === 0) {
        throw new Error("Choose or type a document id before updating.");
      }
      const document = JSON.parse(editor) as JsonValue;
      if (mode === "create") {
        const created = await client.createDocument(selected.name, document);
        setDocId(created.id);
      } else {
        await client.updateDocument(selected.name, docId, document);
      }
      await loadSelected();
    } catch (error) {
      onError(error);
    }
  };

  const deleteDocument = async () => {
    if (selected === undefined) return;
    try {
      if (docId.trim().length === 0) {
        throw new Error("Choose or type a document id before deleting.");
      }
      if (confirm !== docId) {
        throw new Error("Type the document id to confirm deletion.");
      }
      await client.deleteDocument(selected.name, docId, confirm);
      setConfirm("");
      await loadSelected();
    } catch (error) {
      onError(error);
    }
  };

  return (
    <section className="panel split-panel">
      <div className="resource-list">
        <div className="section-heading compact">
          <div>
            <p className="eyebrow">Resources</p>
            <h2>Data Explorer</h2>
          </div>
          <button className="icon-button" type="button" onClick={() => void loadSelected()} disabled={loading}>
            {loading ? <Loader2 size={17} aria-hidden /> : <Search size={17} aria-hidden />}
          </button>
        </div>
        {dataObjects.map((object) => (
          <button
            className={selectedName === object.name ? "resource-button active" : "resource-button"}
            key={object.name}
            type="button"
            aria-label={`${object.name} ${object.kind}`}
            onClick={() => selectResource(object.name)}
          >
            <strong>{object.name}</strong>
            <span>{object.kind}</span>
          </button>
        ))}
      </div>
      <div className="editor-surface">
        {selected === undefined ? (
          <EmptyState icon={Table2} text="Create a table or collection in Builder" />
        ) : (
          <>
            <div className="section-heading">
              <div>
                <p className="eyebrow">{selected.kind}</p>
                <h2>{selected.name}</h2>
              </div>
              <span className="tag">{selected.row_count ?? 0} records in catalog</span>
            </div>
            {canPage && (
              <DataPageControls
                limit={pageLimit}
                page={pageInfo}
                loading={loading}
                onLimitChange={(limit) => {
                  setPageLimit(limit);
                  setPageOffset(0);
                }}
                onLoad={() => void loadSelected(0)}
                onPrevious={loadPreviousPage}
                onNext={loadNextPage}
              />
            )}
            {selected.kind === "table" && (
              <TableCrud
                tableName={selected.name}
                rows={tableRows}
                editor={editor}
                primaryKey={primaryKey}
                confirm={confirm}
                editorError={tableRowError}
                primaryKeyError={primaryKeyError}
                onEditorChange={setEditor}
                onPrimaryKeyChange={setPrimaryKey}
                onConfirmChange={setConfirm}
                onInsert={() => void writeTable("insert")}
                onUpdate={() => void writeTable("update")}
                onDelete={() => void deleteRow()}
              />
            )}
            {selected.kind === "collection" && (
              <DocumentCrud
                documents={documents}
                editor={editor}
                docId={docId}
                confirm={confirm}
                editorError={documentJsonError}
                onEditorChange={setEditor}
                onDocIdChange={setDocId}
                onConfirmChange={setConfirm}
                onCreate={() => void writeDocument("create")}
                onUpdate={() => void writeDocument("update")}
                onDelete={() => void deleteDocument()}
              />
            )}
            {selected.kind === "vector" && (
              <VectorCrud
                metadata={metadataEditor}
                vector={vectorEditor}
                k={vectorK}
                result={operationResult}
                metadataError={metadataJsonError}
                vectorError={vectorError}
                onMetadataChange={setMetadataEditor}
                onVectorChange={setVectorEditor}
                onKChange={setVectorK}
                onInsert={() => void insertVector()}
                onSearch={() => void searchVector()}
              />
            )}
            {selected.kind === "time_series" && (
              <TimeSeriesCrud
                series={seriesName}
                point={pointEditor}
                start={rangeStart}
                end={rangeEnd}
                result={operationResult}
                pointError={pointJsonError}
                rangeError={timeRangeError}
                onSeriesChange={setSeriesName}
                onPointChange={setPointEditor}
                onStartChange={setRangeStart}
                onEndChange={setRangeEnd}
                onInsert={() => void insertTimeSeriesPoint()}
                onLoad={() => void loadTimeSeriesRange()}
              />
            )}
          </>
        )}
      </div>
    </section>
  );
}

function DataPageControls({
  limit,
  page,
  loading,
  onLimitChange,
  onLoad,
  onPrevious,
  onNext,
}: {
  limit: number;
  page?: TableRowsResponse | DocumentListResponse;
  loading: boolean;
  onLimitChange: (limit: number) => void;
  onLoad: () => void;
  onPrevious: () => void;
  onNext: () => void;
}) {
  return (
    <div className="data-page-controls">
      <HelpText
        title="Production-safe browsing"
        text="Explorer loads one bounded page at a time. Use filters/SQL for targeted work; avoid full scans on production data."
      />
      <div className="form-grid four-columns">
        <label>
          <span>Page limit</span>
          <input
            type="number"
            min={1}
            max={1000}
            value={limit}
            onChange={(event) =>
              onLimitChange(positiveIntegerFromInput(event.target.value, limit) || 1)
            }
          />
        </label>
        <Metric label="Offset" value={String(page?.offset ?? 0)} />
        <Metric label="Loaded" value={String(page?.returned ?? 0)} />
        <Metric label="More" value={page?.has_more ? "yes" : "no"} />
      </div>
      {page?.capped === true && (
        <Notice tone="warning" message="Requested limit was capped by the server safety limit." />
      )}
      <div className="button-row">
        <button className="secondary-button" type="button" onClick={onLoad} disabled={loading}>
          {loading ? <Loader2 size={16} aria-hidden /> : <Search size={16} aria-hidden />}
          Load page
        </button>
        <button
          className="secondary-button"
          type="button"
          onClick={onPrevious}
          disabled={loading || (page?.offset ?? 0) === 0}
        >
          Previous
        </button>
        <button
          className="secondary-button"
          type="button"
          onClick={onNext}
          disabled={loading || page?.has_more !== true}
        >
          Next
        </button>
      </div>
    </div>
  );
}

function TableCrud({
  tableName,
  rows,
  editor,
  primaryKey,
  confirm,
  editorError,
  primaryKeyError,
  onEditorChange,
  onPrimaryKeyChange,
  onConfirmChange,
  onInsert,
  onUpdate,
  onDelete,
}: {
  tableName: string;
  rows?: TableRowsResponse;
  editor: string;
  primaryKey: string;
  confirm: string;
  editorError?: string;
  primaryKeyError?: string;
  onEditorChange: (value: string) => void;
  onPrimaryKeyChange: (value: string) => void;
  onConfirmChange: (value: string) => void;
  onInsert: () => void;
  onUpdate: () => void;
  onDelete: () => void;
}) {
  const canWrite = editorError === undefined;
  const canDelete =
    primaryKeyError === undefined && primaryKey.trim().length > 0 && confirm === tableName;
  return (
    <div className="crud-grid">
      <div className="table-frame">
        <table>
          <thead>
            <tr>
              {(rows?.schema?.columns ?? []).map((column) => (
                <th key={column.name}>{column.name}</th>
              ))}
              {rows?.schema === null || rows?.schema === undefined ? <th>row</th> : null}
            </tr>
          </thead>
          <tbody>
            {(rows?.rows ?? []).map((row, index) => (
              <tr key={`${index}-${JSON.stringify(row)}`}>
                {rows?.schema?.columns.map((column, columnIndex) => (
                  <td key={column.name}>{shortJson(row[columnIndex])}</td>
                )) ?? <td>{stringifyJson(row)}</td>}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="form-stack">
        <label>
          <span>Row JSON</span>
          <textarea value={editor} onChange={(event) => onEditorChange(event.target.value)} />
        </label>
        {editorError !== undefined && <Notice tone="error" message={editorError} />}
        <div className="button-row">
          <button className="primary-button" type="button" onClick={onInsert} disabled={!canWrite}>Insert</button>
          <button className="secondary-button" type="button" onClick={onUpdate} disabled={!canWrite}>Update</button>
        </div>
        <label>
          <span>Primary key JSON</span>
          <input value={primaryKey} onChange={(event) => onPrimaryKeyChange(event.target.value)} />
        </label>
        {primaryKeyError !== undefined && <Notice tone="error" message={primaryKeyError} />}
        <label>
          <span>Confirm table name</span>
          <input value={confirm} onChange={(event) => onConfirmChange(event.target.value)} />
        </label>
        <button className="danger-button" type="button" onClick={onDelete} disabled={!canDelete}>
          <Trash2 size={16} aria-hidden />
          Delete row
        </button>
      </div>
    </div>
  );
}

function DocumentCrud({
  documents,
  editor,
  docId,
  confirm,
  editorError,
  onEditorChange,
  onDocIdChange,
  onConfirmChange,
  onCreate,
  onUpdate,
  onDelete,
}: {
  documents?: DocumentListResponse;
  editor: string;
  docId: string;
  confirm: string;
  editorError?: string;
  onEditorChange: (value: string) => void;
  onDocIdChange: (value: string) => void;
  onConfirmChange: (value: string) => void;
  onCreate: () => void;
  onUpdate: () => void;
  onDelete: () => void;
}) {
  const canWrite = editorError === undefined;
  const hasDocumentId = docId.trim().length > 0;
  const canDelete = hasDocumentId && confirm === docId;
  return (
    <div className="crud-grid">
      <div className="document-list">
        {(documents?.documents ?? []).map((document) => (
          <button
            className="document-row"
            key={document.id}
            type="button"
            onClick={() => {
              onDocIdChange(document.id);
              onEditorChange(stringifyJson(document.document));
            }}
          >
            <strong>{document.id}</strong>
            <code>{shortJson(document.document)}</code>
          </button>
        ))}
      </div>
      <div className="form-stack">
        <label>
          <span>Document JSON</span>
          <textarea value={editor} onChange={(event) => onEditorChange(event.target.value)} />
        </label>
        {editorError !== undefined && <Notice tone="error" message={editorError} />}
        <label>
          <span>Document id</span>
          <input value={docId} onChange={(event) => onDocIdChange(event.target.value)} />
        </label>
        <div className="button-row">
          <button className="primary-button" type="button" onClick={onCreate} disabled={!canWrite}>Create</button>
          <button className="secondary-button" type="button" onClick={onUpdate} disabled={!canWrite || !hasDocumentId}>Update</button>
        </div>
        <label>
          <span>Confirm document id</span>
          <input value={confirm} onChange={(event) => onConfirmChange(event.target.value)} />
        </label>
        <button className="danger-button" type="button" onClick={onDelete} disabled={!canDelete}>
          <Trash2 size={16} aria-hidden />
          Delete document
        </button>
      </div>
    </div>
  );
}

function VectorCrud({
  metadata,
  vector,
  k,
  result,
  metadataError,
  vectorError,
  onMetadataChange,
  onVectorChange,
  onKChange,
  onInsert,
  onSearch,
}: {
  metadata: string;
  vector: string;
  k: number;
  result?: JsonValue;
  metadataError?: string;
  vectorError?: string;
  onMetadataChange: (value: string) => void;
  onVectorChange: (value: string) => void;
  onKChange: (value: number) => void;
  onInsert: () => void;
  onSearch: () => void;
}) {
  const vectorValid = vectorError === undefined;
  return (
    <div className="crud-grid">
      <div className="model-guide">
        <HelpText
          title="Vector collection"
          text="Insert embeddings with metadata, then search by nearest vectors. Dimension must match the collection."
        />
        {result !== undefined && <pre className="json-view">{stringifyJson(result)}</pre>}
      </div>
      <div className="form-stack">
        <label>
          <span>Metadata JSON</span>
          <textarea value={metadata} onChange={(event) => onMetadataChange(event.target.value)} />
        </label>
        {metadataError !== undefined && <Notice tone="error" message={metadataError} />}
        <label>
          <span>Vector JSON</span>
          <textarea value={vector} onChange={(event) => onVectorChange(event.target.value)} />
        </label>
        {vectorError !== undefined && <Notice tone="error" message={vectorError} />}
        <label>
          <span>Search k</span>
          <input type="number" value={k} onChange={(event) => onKChange(Number(event.target.value))} />
        </label>
        <div className="button-row">
          <button className="primary-button" type="button" onClick={onInsert} disabled={metadataError !== undefined || !vectorValid}>Insert vector</button>
          <button className="secondary-button" type="button" onClick={onSearch} disabled={!vectorValid}>Search</button>
        </div>
      </div>
    </div>
  );
}

function TimeSeriesCrud({
  series,
  point,
  start,
  end,
  result,
  pointError,
  rangeError,
  onSeriesChange,
  onPointChange,
  onStartChange,
  onEndChange,
  onInsert,
  onLoad,
}: {
  series: string;
  point: string;
  start: string;
  end: string;
  result?: JsonValue;
  pointError?: string;
  rangeError?: string;
  onSeriesChange: (value: string) => void;
  onPointChange: (value: string) => void;
  onStartChange: (value: string) => void;
  onEndChange: (value: string) => void;
  onInsert: () => void;
  onLoad: () => void;
}) {
  const hasSeries = series.trim().length > 0;
  return (
    <div className="crud-grid">
      <div className="model-guide">
        <HelpText
          title="Time-series points"
          text="Write timestamped numeric points into a named series, then range scan by millisecond timestamps."
        />
        {result !== undefined && <pre className="json-view">{stringifyJson(result)}</pre>}
      </div>
      <div className="form-stack">
        <label>
          <span>Series key</span>
          <input value={series} onChange={(event) => onSeriesChange(event.target.value)} />
        </label>
        <label>
          <span>Point JSON</span>
          <textarea value={point} onChange={(event) => onPointChange(event.target.value)} />
        </label>
        {pointError !== undefined && <Notice tone="error" message={pointError} />}
        <div className="field-row">
          <label>
            <span>Range start ms</span>
            <input value={start} onChange={(event) => onStartChange(event.target.value)} />
          </label>
          <label>
            <span>Range end ms</span>
            <input value={end} onChange={(event) => onEndChange(event.target.value)} />
          </label>
        </div>
        {rangeError !== undefined && <Notice tone="error" message={rangeError} />}
        <div className="button-row">
          <button className="primary-button" type="button" onClick={onInsert} disabled={!hasSeries || pointError !== undefined}>Insert point</button>
          <button className="secondary-button" type="button" onClick={onLoad} disabled={!hasSeries || rangeError !== undefined}>Load range</button>
        </div>
      </div>
    </div>
  );
}

function SqlConsole({
  client,
  onError,
}: {
  client: ControlPlaneClient;
  onError: (error?: unknown) => void;
}) {
  const [sql, setSql] = useState("SELECT * FROM information_schema.tables");
  const [result, setResult] = useState<SqlResponse>();
  const [history, setHistory] = useState<string[]>([]);
  const [sqlError, setSqlError] = useState<string>();
  const run = async () => {
    onError(undefined);
    setSqlError(undefined);
    try {
      const response = await client.sql(sql);
      setResult(response);
      setHistory((current) => {
        const next = [sql, ...current.filter((item) => item !== sql)];
        return next.slice(0, 8);
      });
    } catch (error) {
      setSqlError(controlPlaneErrorMessage(error));
      onError(error);
    }
  };
  return (
    <section className="panel sql-layout">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Expert</p>
          <h2>SQL Console</h2>
        </div>
        <button className="primary-button" type="button" onClick={run}>
          <Play size={16} aria-hidden />
          Run
        </button>
      </div>
      <textarea className="sql-editor" value={sql} onChange={(event) => setSql(event.target.value)} />
      {history.length > 0 && (
        <div className="history-list" aria-label="SQL history">
          {history.map((item) => (
            <button className="resource-button" key={item} type="button" onClick={() => setSql(item)}>
              <strong>{item}</strong>
            </button>
          ))}
        </div>
      )}
      {sqlError !== undefined && <Notice tone="error" message={sqlError} />}
      {result !== undefined && <SqlResultView response={result} />}
    </section>
  );
}

function SqlResultView({ response }: { response: SqlResponse }) {
  const output = response.output;
  if (output.kind !== "rows") {
    return <pre className="json-view">{stringifyJson(output as unknown as JsonValue)}</pre>;
  }
  if (output.rows.length === 0) {
    return <EmptyState icon={Table2} text="Query returned no rows" />;
  }
  return (
    <div className="table-frame">
      <table>
        <thead>
          <tr>
            {output.columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {output.rows.map((row, rowIndex) => (
            <tr key={`${rowIndex}-${JSON.stringify(row)}`}>
              {output.columns.map((column, columnIndex) => (
                <td key={column}>{shortJson(row[columnIndex])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Builder({
  client,
  data,
  onError,
  onCreated,
}: {
  client: ControlPlaneClient;
  data: LoadedStudioData;
  onError: (error?: unknown) => void;
  onCreated: () => Promise<void>;
}) {
  const [kind, setKind] = useState<BuilderKind>("table");
  const [name, setName] = useState("users");
  const [columns, setColumns] = useState(defaultColumns);
  const [primaryKey, setPrimaryKey] = useState(0);
  const [fields, setFields] = useState(defaultCollectionFields);
  const [collectionId, setCollectionId] = useState(1);
  const [fieldPath, setFieldPath] = useState("body");
  const [language, setLanguage] = useState("simple");
  const [refreshLag, setRefreshLag] = useState(1);
  const [geoPrecision, setGeoPrecision] = useState(6);
  const [graphId, setGraphId] = useState(1);
  const [dimension, setDimension] = useState(3);
  const [chunkMillis, setChunkMillis] = useState(60_000);
  const [databaseProfile, setDatabaseProfile] = useState(data.config.profile);
  const [databaseDeployment, setDatabaseDeployment] =
    useState<DeploymentMode>(data.config.deployment.mode);
  const [databaseStoragePath, setDatabaseStoragePath] = useState("target\\new-database.redb");
  const [databaseReplication, setDatabaseReplication] =
    useState<ReplicationMode>(data.config.defaults.replication);
  const [databaseReplicas, setDatabaseReplicas] = useState(data.config.topology.replica_count);
  const [databaseShards, setDatabaseShards] = useState(data.config.topology.shard_count);
  const [databaseBackup, setDatabaseBackup] = useState(data.config.guarantees.backup.enabled);
  const [databaseAudit, setDatabaseAudit] = useState(data.config.guarantees.audit.enabled);
  const [customProfileSlug, setCustomProfileSlug] = useState("ops_custom_profile");
  const [customProfileDescription, setCustomProfileDescription] = useState(
    "Custom operator-managed profile for this environment.",
  );
  const [customProfileBase, setCustomProfileBase] = useState(data.config.profile);
  const [result, setResult] = useState<JsonObject>();

  const request = useMemo<JsonObject>(() => {
    try {
      const path = fieldPath
        .split(".")
        .map((part) => part.trim())
        .filter(Boolean);
      if (kind === "table") {
        return {
          name,
          schema: {
            columns: JSON.parse(columns) as JsonValue,
            primary_key: primaryKey,
          },
          indexes: [],
        } as unknown as JsonObject;
      }
      if (kind === "collection") {
        return { name, fields: JSON.parse(fields) as JsonValue, indexes: [] } as unknown as JsonObject;
      }
      if (kind === "vector") {
        return {
          name,
          dim: dimension,
          metric: "Cosine",
          hnsw: { m: 16, ef_construction: 200, ef_search: 48 },
        } as unknown as JsonObject;
      }
      if (kind === "time_series") {
        return { name, chunk_millis: chunkMillis, retention_millis: null } as unknown as JsonObject;
      }
      if (kind === "full_text") {
        return {
          name,
          collection_id: collectionId,
          path,
          language,
          refresh_lag_target: refreshLag,
        } as unknown as JsonObject;
      }
      if (kind === "geo") {
        return {
          name,
          collection_id: collectionId,
          path,
          precision: geoPrecision,
        } as unknown as JsonObject;
      }
      if (kind === "database") {
        return databaseSpecForBuilder({
          name,
          profile: databaseProfile,
          deploymentMode: databaseDeployment,
          storagePath: databaseStoragePath,
          replication: databaseReplication,
          replicaCount: databaseReplicas,
          shardCount: databaseShards,
          backupEnabled: databaseBackup,
          auditEnabled: databaseAudit,
        }) as unknown as JsonObject;
      }
      if (kind === "custom_profile") {
        const spec = databaseSpecForBuilder({
          name: `${customProfileSlug}_example`,
          profile: customProfileSlug,
          deploymentMode: databaseDeployment,
          storagePath: databaseStoragePath,
          replication: databaseReplication,
          replicaCount: databaseReplicas,
          shardCount: databaseShards,
          backupEnabled: databaseBackup,
          auditEnabled: databaseAudit,
        });
        spec.overrides = {
          ...spec.overrides,
          "profile.custom.base": customProfileBase,
          "profile.custom.description": customProfileDescription,
        };
        spec.operation_hints = {
          ...spec.operation_hints,
          "studio.builder": "custom_profile",
          "profile.custom.slug": customProfileSlug,
        };
        return {
          profile: {
            slug: customProfileSlug,
            base_profile: customProfileBase,
            description: customProfileDescription,
            status: "Custom",
            note: "Custom profiles are validated as operator-managed DatabaseSpec metadata.",
          },
          example_database_spec: spec,
        } as unknown as JsonObject;
      }
      return { name, graph_id: graphId } as unknown as JsonObject;
    } catch (error) {
      return { error: errorMessage(error) } as JsonObject;
    }
  }, [
    chunkMillis,
    collectionId,
    columns,
    customProfileBase,
    customProfileDescription,
    customProfileSlug,
    databaseAudit,
    databaseBackup,
    databaseDeployment,
    databaseProfile,
    databaseReplicas,
    databaseReplication,
    databaseShards,
    databaseStoragePath,
    dimension,
    fieldPath,
    fields,
    geoPrecision,
    graphId,
    kind,
    language,
    name,
    primaryKey,
    refreshLag,
  ]);

  const requestError = typeof request.error === "string" ? request.error : undefined;

  const create = async () => {
    onError(undefined);
    try {
      if (requestError !== undefined) {
        throw new Error(requestError);
      }
      let created: JsonObject;
      if (kind === "table") created = await client.createTable(request);
      else if (kind === "collection") created = await client.createCollection(request);
      else if (kind === "vector") created = await client.createVector(request);
      else if (kind === "time_series") created = await client.createTimeSeries(request);
      else if (kind === "full_text") created = await client.createFullText(request);
      else if (kind === "geo") created = await client.createGeoIndex(request);
      else if (kind === "graph") created = await client.createGraph(request);
      else {
        const spec =
          kind === "database"
            ? (request as DatabaseSpec)
            : ((request.example_database_spec as unknown) as DatabaseSpec);
        const validation = await client.validate(spec);
        const plan = await client.plan({ current: data.config, desired: spec });
        created = {
          prepared: true,
          physical_create_supported: false,
          kind,
          message:
            "Spec prepared, validated and dry-run planned. Physical database/profile switching is not enabled in this runtime.",
          validation: validation as unknown as JsonValue,
          plan: plan as unknown as JsonValue,
          spec: spec as unknown as JsonValue,
        } as JsonObject;
      }
      setResult(created);
      if (!["database", "custom_profile"].includes(kind)) {
        await onCreated();
      }
    } catch (error) {
      onError(error);
    }
  };

  return (
    <section className="panel builder-layout">
      <div className="wizard">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Wizard + expert</p>
            <h2>Resource Builder</h2>
          </div>
          <button className="primary-button" type="button" onClick={create}>
            <Hammer size={16} aria-hidden />
            Create
          </button>
        </div>
        <div className="segmented">
          {builderKinds.map((option) => (
            <button
              className={kind === option ? "active" : ""}
              key={option}
              type="button"
              onClick={() => setKind(option)}
            >
              {option}
            </button>
          ))}
        </div>
        {kind !== "custom_profile" && (
          <label>
            <span>{kind === "database" ? "Database name" : "Name"}</span>
            <input value={name} onChange={(event) => setName(event.target.value)} />
          </label>
        )}
        {kind === "table" && (
          <>
            <HelpText title="Table schema" text="Typed columns make CRUD forms safer and let SQL validate row shape before writes." />
            <label>
              <span>Columns JSON</span>
              <textarea value={columns} onChange={(event) => setColumns(event.target.value)} />
            </label>
            <label>
              <span>Primary key column index</span>
              <input
                type="number"
                value={primaryKey}
                onChange={(event) => setPrimaryKey(Number(event.target.value))}
              />
            </label>
          </>
        )}
        {kind === "collection" && (
          <>
            <HelpText title="Document fields" text="Fields describe indexed document paths and help admins understand searchable shape." />
            <label>
              <span>Fields JSON</span>
              <textarea value={fields} onChange={(event) => setFields(event.target.value)} />
            </label>
          </>
        )}
        {kind === "vector" && (
          <>
            <HelpText title="Vector dimension" text="Dimension must match every embedded vector inserted into this collection." />
            <label>
              <span>Dimension</span>
              <input
                type="number"
                value={dimension}
                onChange={(event) => setDimension(Number(event.target.value))}
              />
            </label>
          </>
        )}
        {kind === "time_series" && (
          <>
            <HelpText title="Chunk window" text="Chunk size controls write grouping and range scan granularity." />
            <label>
              <span>Chunk millis</span>
              <input
                type="number"
                value={chunkMillis}
                onChange={(event) => setChunkMillis(Number(event.target.value))}
              />
            </label>
          </>
        )}
        {kind === "full_text" && (
          <>
            <HelpText title="Full-text index" text="Indexes text from a document collection path so SQL match queries can rank documents." />
            <CollectionSourceFields
              collectionId={collectionId}
              fieldPath={fieldPath}
              onCollectionIdChange={setCollectionId}
              onFieldPathChange={setFieldPath}
            />
            <div className="field-row">
              <label>
                <span>Language</span>
                <input value={language} onChange={(event) => setLanguage(event.target.value)} />
              </label>
              <label>
                <span>Refresh lag target</span>
                <input
                  type="number"
                  value={refreshLag}
                  onChange={(event) => setRefreshLag(Number(event.target.value))}
                />
              </label>
            </div>
          </>
        )}
        {kind === "geo" && (
          <>
            <HelpText title="Geo index" text="Indexes lon/lat points from documents for radius and bounding-box lookups." />
            <CollectionSourceFields
              collectionId={collectionId}
              fieldPath={fieldPath}
              onCollectionIdChange={setCollectionId}
              onFieldPathChange={setFieldPath}
            />
            <label>
              <span>Geohash precision</span>
              <input
                type="number"
                value={geoPrecision}
                onChange={(event) => setGeoPrecision(Number(event.target.value))}
              />
            </label>
          </>
        )}
        {kind === "graph" && (
          <>
            <HelpText title="Graph" text="Creates a named graph for edges and traversals. Graph id must be unique inside the database." />
            <label>
              <span>Graph id</span>
              <input
                type="number"
                value={graphId}
                onChange={(event) => setGraphId(Number(event.target.value))}
              />
            </label>
          </>
        )}
        {kind === "database" && (
          <>
            <HelpText
              title="Database spec"
              text="Creates a desired DatabaseSpec for a new database and runs validation/dry-run. This runtime does not physically switch databases yet."
            />
            <div className="form-grid two-columns">
              <label>
                <span>Profile</span>
                <select
                  value={databaseProfile}
                  onChange={(event) => setDatabaseProfile(event.target.value)}
                >
                  {data.profiles.map((profile) => (
                    <option key={profile.slug} value={profile.slug}>
                      {profile.slug} - {profile.status}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Storage path</span>
                <input
                  value={databaseStoragePath}
                  onChange={(event) => setDatabaseStoragePath(event.target.value)}
                />
              </label>
              <label>
                <span>Deployment</span>
                <select
                  value={databaseDeployment}
                  onChange={(event) => setDatabaseDeployment(event.target.value as DeploymentMode)}
                >
                  {deploymentModes.map((mode) => (
                    <option key={mode} value={mode}>
                      {optionLabel(mode)}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Replication</span>
                <select
                  value={databaseReplication}
                  onChange={(event) => setDatabaseReplication(event.target.value as ReplicationMode)}
                >
                  {replicationModes.map((mode) => (
                    <option key={mode} value={mode}>
                      {mode.toUpperCase()}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Replica count</span>
                <input
                  type="number"
                  min={0}
                  value={databaseReplicas}
                  onChange={(event) =>
                    setDatabaseReplicas(positiveIntegerFromInput(event.target.value, databaseReplicas))
                  }
                />
              </label>
              <label>
                <span>Shard count</span>
                <input
                  type="number"
                  min={0}
                  value={databaseShards}
                  onChange={(event) =>
                    setDatabaseShards(positiveIntegerFromInput(event.target.value, databaseShards))
                  }
                />
              </label>
            </div>
            <div className="toggle-grid">
              <ToggleControl
                label="Backups"
                checked={databaseBackup}
                help="Enable for production specs; risk: disabled backups may downgrade certification."
                onChange={setDatabaseBackup}
              />
              <ToggleControl
                label="Audit"
                checked={databaseAudit}
                help="Enable for admin-controlled databases; risk: disabled audit hides config/user changes."
                onChange={setDatabaseAudit}
              />
            </div>
          </>
        )}
        {kind === "custom_profile" && (
          <>
            <HelpText
              title="Custom profile"
              text="Defines an operator-managed profile as DatabaseSpec metadata. Validation will mark it Custom, and dry-run records the profile change without physical runtime switching."
            />
            <div className="form-grid two-columns">
              <label>
                <span>Profile slug</span>
                <input
                  value={customProfileSlug}
                  onChange={(event) => setCustomProfileSlug(event.target.value)}
                />
              </label>
              <label>
                <span>Base profile</span>
                <select
                  value={customProfileBase}
                  onChange={(event) => setCustomProfileBase(event.target.value)}
                >
                  {data.profiles.map((profile) => (
                    <option key={profile.slug} value={profile.slug}>
                      {profile.slug} - {profile.status}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <label>
              <span>Description</span>
              <textarea
                className="compact-textarea"
                value={customProfileDescription}
                onChange={(event) => setCustomProfileDescription(event.target.value)}
              />
            </label>
            <div className="form-grid two-columns">
              <label>
                <span>Deployment</span>
                <select
                  value={databaseDeployment}
                  onChange={(event) => setDatabaseDeployment(event.target.value as DeploymentMode)}
                >
                  {deploymentModes.map((mode) => (
                    <option key={mode} value={mode}>
                      {optionLabel(mode)}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Replication</span>
                <select
                  value={databaseReplication}
                  onChange={(event) => setDatabaseReplication(event.target.value as ReplicationMode)}
                >
                  {replicationModes.map((mode) => (
                    <option key={mode} value={mode}>
                      {mode.toUpperCase()}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Replica count</span>
                <input
                  type="number"
                  min={0}
                  value={databaseReplicas}
                  onChange={(event) =>
                    setDatabaseReplicas(positiveIntegerFromInput(event.target.value, databaseReplicas))
                  }
                />
              </label>
              <label>
                <span>Shard count</span>
                <input
                  type="number"
                  min={0}
                  value={databaseShards}
                  onChange={(event) =>
                    setDatabaseShards(positiveIntegerFromInput(event.target.value, databaseShards))
                  }
                />
              </label>
            </div>
          </>
        )}
      </div>
      <div>
        <h3>Request Preview</h3>
        {requestError !== undefined && <Notice tone="error" message={requestError} />}
        <pre className="json-view">{stringifyJson(request)}</pre>
        {result !== undefined && (
          <>
            <h3>Result</h3>
            <pre className="json-view">{stringifyJson(result)}</pre>
          </>
        )}
      </div>
    </section>
  );
}

const securityValidationIssues = (state: SecurityState): string[] => {
  const issues: string[] = [];
  const roleNames = new Set<string>();
  for (const role of state.roles) {
    const name = role.name.trim();
    if (name.length === 0) {
      issues.push("Role names cannot be empty.");
    }
    if (roleNames.has(name)) {
      issues.push(`Duplicate role: ${name}.`);
    }
    roleNames.add(name);
  }
  for (const principal of state.principals) {
    if (principal.user.trim().length === 0 || principal.principal.trim().length === 0) {
      issues.push("User and principal names cannot be empty.");
    }
    for (const roleName of principal.roles) {
      if (!roleNames.has(roleName)) {
        issues.push(`${principal.user} references missing role ${roleName}.`);
      }
    }
  }
  return [...new Set(issues)];
};

const isBroadAdminGrant = (grant: { resource: JsonValue; permission: string }): boolean =>
  grant.permission === "Admin" &&
  (grant.resource === "System" || grant.resource === "Database");

function SecurityPanel({
  client,
  security,
  onError,
  onSaved,
}: {
  client: ControlPlaneClient;
  security: SecurityState;
  onError: (error?: unknown) => void;
  onSaved: (security: SecurityState) => void;
}) {
  const [draftSecurity, setDraftSecurity] = useState<SecurityState>(() =>
    cloneSecurityState(security),
  );
  const [expertSecurity, setExpertSecurity] = useState(syncSecurityJson(security));
  const [newRoleName, setNewRoleName] = useState("operator");
  const [newUser, setNewUser] = useState("new-user");
  const [newPrincipal, setNewPrincipal] = useState("new-user");
  const [newPrincipalRoles, setNewPrincipalRoles] = useState<string[]>([]);
  const [grantRole, setGrantRole] = useState(security.roles[0]?.name ?? "admin");
  const [grantResourceKind, setGrantResourceKind] = useState<ResourceKind>("Database");
  const [grantResourceName, setGrantResourceName] = useState("users");
  const [grantPermission, setGrantPermission] =
    useState<"Read" | "Write" | "Admin">("Read");
  const dirty = syncSecurityJson(draftSecurity) !== syncSecurityJson(security);
  const validationIssues = securityValidationIssues(draftSecurity);
  const broadAdminGrants = draftSecurity.roles.flatMap((role) =>
    role.grants
      .filter(isBroadAdminGrant)
      .map((grant) => `${role.name}: ${resourceLabel(grant.resource)} ${grant.permission}`),
  );

  useEffect(() => {
    const next = cloneSecurityState(security);
    setDraftSecurity(next);
    setExpertSecurity(syncSecurityJson(next));
    setGrantRole((current) =>
      next.roles.some((role) => role.name === current)
        ? current
        : next.roles[0]?.name ?? "admin",
    );
  }, [security]);

  const updateDraft = (mutator: (draft: SecurityState) => void) => {
    setDraftSecurity((current) => {
      const next = cloneSecurityState(current);
      mutator(next);
      setExpertSecurity(syncSecurityJson(next));
      return next;
    });
  };

  const save = async () => {
    onError(undefined);
    try {
      const next = await client.saveSecurity(draftSecurity);
      setExpertSecurity(syncSecurityJson(next));
      setDraftSecurity(cloneSecurityState(next));
      onSaved(next);
    } catch (error) {
      onError(error);
    }
  };

  const applyExpertJson = () => {
    try {
      const parsed = JSON.parse(expertSecurity) as SecurityState;
      const next = {
        ...parsed,
        audit_enabled: parsed.audit_enabled ?? draftSecurity.audit_enabled,
      };
      setDraftSecurity(next);
      setExpertSecurity(syncSecurityJson(next));
      onError(undefined);
    } catch (error) {
      onError(error);
    }
  };

  const addRole = () => {
    const name = newRoleName.trim();
    if (name.length === 0) return;
    updateDraft((draft) => {
      if (!draft.roles.some((role) => role.name === name)) {
        draft.roles.push({ name, grants: [] });
      }
    });
    setGrantRole(name);
  };

  const removeRole = (name: string) => {
    updateDraft((draft) => {
      draft.roles = draft.roles.filter((role) => role.name !== name);
      draft.principals = draft.principals.map((principal) => ({
        ...principal,
        roles: principal.roles.filter((role) => role !== name),
      }));
    });
  };

  const addPrincipal = () => {
    const user = newUser.trim();
    const principal = newPrincipal.trim();
    if (user.length === 0 || principal.length === 0) return;
    updateDraft((draft) => {
      const nextPrincipal = {
        user,
        principal,
        roles: newPrincipalRoles.filter((role) =>
          draft.roles.some((candidate) => candidate.name === role),
        ),
      };
      const existing = draft.principals.findIndex((item) => item.user === user);
      if (existing >= 0) {
        draft.principals[existing] = nextPrincipal;
      } else {
        draft.principals.push(nextPrincipal);
      }
    });
  };

  const removePrincipal = (user: string) => {
    updateDraft((draft) => {
      draft.principals = draft.principals.filter((principal) => principal.user !== user);
    });
  };

  const togglePrincipalRole = (user: string, roleName: string, checked: boolean) => {
    updateDraft((draft) => {
      const principal = draft.principals.find((item) => item.user === user);
      if (principal === undefined) return;
      const roles = new Set(principal.roles);
      if (checked) roles.add(roleName);
      else roles.delete(roleName);
      principal.roles = draft.roles
        .map((role) => role.name)
        .filter((candidate) => roles.has(candidate));
    });
  };

  const toggleNewPrincipalRole = (roleName: string, checked: boolean) => {
    setNewPrincipalRoles((current) => {
      const roles = new Set(current);
      if (checked) roles.add(roleName);
      else roles.delete(roleName);
      return draftSecurity.roles
        .map((role) => role.name)
        .filter((candidate) => roles.has(candidate));
    });
  };

  const addGrant = () => {
    updateDraft((draft) => {
      const role = draft.roles.find((candidate) => candidate.name === grantRole);
      if (role === undefined) return;
      role.grants.push({
        resource: resourceFromParts(grantResourceKind, grantResourceName),
        permission: grantPermission,
      });
    });
  };

  const removeGrant = (roleName: string, index: number) => {
    updateDraft((draft) => {
      const role = draft.roles.find((candidate) => candidate.name === roleName);
      role?.grants.splice(index, 1);
    });
  };

  return (
    <section className="panel security-layout">
      <div className="config-form">
        <div className="section-heading">
          <div>
            <p className="eyebrow">RBAC</p>
            <h2>Roles and Principals</h2>
            <p className="supporting-copy">
              Create users, map principals and grant scoped permissions without editing JSON.
            </p>
          </div>
          <button
            className="primary-button"
            type="button"
            onClick={save}
            disabled={!dirty || validationIssues.length > 0}
          >
            <ShieldCheck size={16} aria-hidden />
            Save changes
          </button>
        </div>
        {dirty ? (
          <Notice tone="info" message="Security policy has unsaved changes." />
        ) : (
          <Notice tone="info" message="Security policy is saved." />
        )}
        {validationIssues.map((issue) => (
          <Notice tone="error" message={issue} key={issue} />
        ))}
        {broadAdminGrants.length > 0 && (
          <Notice
            tone="warning"
            message={`Broad admin grants require operator review: ${broadAdminGrants.join(", ")}`}
          />
        )}
        <HelpText
          title="User vs principal"
          text="User is the registry key used by Studio, principal is the runtime identity checked by RBAC. This panel manages roles and grants, not passwords."
        />

        <ConfigSection
          title="Create User"
          description="Add or update a user/principal mapping and assign roles."
          issueCount={0}
        >
          <div className="form-grid two-columns">
            <label>
              <span>User key</span>
              <input value={newUser} onChange={(event) => setNewUser(event.target.value)} />
            </label>
            <label>
              <span>Principal name</span>
              <input
                value={newPrincipal}
                onChange={(event) => setNewPrincipal(event.target.value)}
              />
            </label>
          </div>
          <div className="chip-row" aria-label="Roles for new user">
            {draftSecurity.roles.map((role) => (
              <label className="chip-toggle" key={role.name}>
                <input
                  type="checkbox"
                  checked={newPrincipalRoles.includes(role.name)}
                  onChange={(event) =>
                    toggleNewPrincipalRole(role.name, event.target.checked)
                  }
                />
                <span>{role.name}</span>
              </label>
            ))}
          </div>
          <button className="secondary-button" type="button" onClick={addPrincipal}>
            <Plus size={16} aria-hidden />
            Add user
          </button>
        </ConfigSection>

        <ConfigSection
          title="Create Role"
          description="Roles collect permissions and can be attached to many users."
          issueCount={0}
        >
          <div className="field-row">
            <label>
              <span>Role name</span>
              <input value={newRoleName} onChange={(event) => setNewRoleName(event.target.value)} />
            </label>
            <div className="button-row align-end">
              <button className="secondary-button" type="button" onClick={addRole}>
                <Plus size={16} aria-hidden />
                Add role
              </button>
            </div>
          </div>
        </ConfigSection>

        <ConfigSection
          title="Grant Builder"
          description="Attach Read, Write or Admin permissions to a resource scope."
          issueCount={0}
        >
          <div className="form-grid four-columns">
            <label>
              <span>Role</span>
              <select value={grantRole} onChange={(event) => setGrantRole(event.target.value)}>
                {draftSecurity.roles.map((role) => (
                  <option key={role.name} value={role.name}>
                    {role.name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Resource type</span>
              <select
                value={grantResourceKind}
                onChange={(event) => setGrantResourceKind(event.target.value as ResourceKind)}
              >
                {resourceKinds.map((kind) => (
                  <option key={kind} value={kind}>
                    {kind}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Resource name</span>
              <input
                value={grantResourceName}
                disabled={grantResourceKind === "Database" || grantResourceKind === "System"}
                onChange={(event) => setGrantResourceName(event.target.value)}
              />
            </label>
            <label>
              <span>Permission</span>
              <select
                value={grantPermission}
                onChange={(event) =>
                  setGrantPermission(event.target.value as "Read" | "Write" | "Admin")
                }
              >
                {permissionOptions.map((permission) => (
                  <option key={permission} value={permission}>
                    {permission}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <HelpText
            title="Scopes"
            text="Database grants can cover broad data access; System Admin can change RBAC/config and should be reserved for operators."
          />
          <button className="secondary-button" type="button" onClick={addGrant}>
            <Plus size={16} aria-hidden />
            Add grant
          </button>
        </ConfigSection>

        <ConfigSection
          title="Users"
          description="Existing users and their assigned roles."
          issueCount={0}
        >
          <div className="rbac-grid">
            {draftSecurity.principals.map((principal) => (
              <article className="row-item" key={principal.user}>
                <div>
                  <strong>{principal.user}</strong>
                  <p>{principal.principal} {"->"} {principal.roles.join(", ") || "no roles"}</p>
                  <div className="chip-row">
                    {draftSecurity.roles.map((role) => (
                      <label className="chip-toggle" key={`${principal.user}-${role.name}`}>
                        <input
                          type="checkbox"
                          checked={principal.roles.includes(role.name)}
                          onChange={(event) =>
                            togglePrincipalRole(principal.user, role.name, event.target.checked)
                          }
                        />
                        <span>{role.name}</span>
                      </label>
                    ))}
                  </div>
                </div>
                <button
                  className="icon-button"
                  type="button"
                  title="Remove user"
                  aria-label={`Remove user ${principal.user}`}
                  onClick={() => removePrincipal(principal.user)}
                >
                  <Trash2 size={16} aria-hidden />
                </button>
              </article>
            ))}
          </div>
        </ConfigSection>

        <ConfigSection
          title="Roles"
          description="Role grants and delete controls."
          issueCount={0}
        >
          <div className="rbac-grid">
            {draftSecurity.roles.map((role) => (
              <article className="row-item" key={role.name}>
                <div>
                  <strong>{role.name}</strong>
                  {role.grants.length === 0 ? (
                    <p>No grants yet</p>
                  ) : (
                    <div className="grant-list">
                      {role.grants.map((grant, index) => (
                        <span className="grant-chip" key={`${role.name}-${index}`}>
                          {resourceLabel(grant.resource)}:{grant.permission}
                          <button
                            type="button"
                            aria-label={`Remove grant ${index + 1} from ${role.name}`}
                            onClick={() => removeGrant(role.name, index)}
                          >
                            <Trash2 size={12} aria-hidden />
                          </button>
                        </span>
                      ))}
                    </div>
                  )}
                </div>
                <button
                  className="icon-button"
                  type="button"
                  title="Remove role"
                  aria-label={`Remove role ${role.name}`}
                  onClick={() => removeRole(role.name)}
                >
                  <Trash2 size={16} aria-hidden />
                </button>
              </article>
            ))}
          </div>
        </ConfigSection>
      </div>
      <div>
        <div className="section-heading compact">
          <div>
            <p className="eyebrow">Expert</p>
            <h2>Security JSON</h2>
          </div>
          <button className="secondary-button" type="button" onClick={applyExpertJson}>
            Apply JSON
          </button>
        </div>
        <label>
          <span>Security policy preview</span>
          <textarea
            value={expertSecurity}
            onChange={(event) => setExpertSecurity(event.target.value)}
          />
        </label>
      </div>
    </section>
  );
}

function CollectionSourceFields({
  collectionId,
  fieldPath,
  onCollectionIdChange,
  onFieldPathChange,
}: {
  collectionId: number;
  fieldPath: string;
  onCollectionIdChange: (value: number) => void;
  onFieldPathChange: (value: string) => void;
}) {
  return (
    <div className="field-row">
      <label>
        <span>Source collection id</span>
        <input
          type="number"
          value={collectionId}
          onChange={(event) => onCollectionIdChange(Number(event.target.value))}
        />
      </label>
      <label>
        <span>Field path</span>
        <input value={fieldPath} onChange={(event) => onFieldPathChange(event.target.value)} />
      </label>
    </div>
  );
}

function AuditPanel({ events }: { events: AuditEvent[] }) {
  const [actionFilter, setActionFilter] = useState("");
  const [principalFilter, setPrincipalFilter] = useState("");
  const [outcomeFilter, setOutcomeFilter] = useState("");
  const filteredEvents = events.filter((event) => {
    const actionMatches =
      actionFilter.trim().length === 0 ||
      event.action.toLowerCase().includes(actionFilter.trim().toLowerCase());
    const principalMatches =
      principalFilter.trim().length === 0 ||
      (event.principal ?? "system")
        .toLowerCase()
        .includes(principalFilter.trim().toLowerCase());
    const outcomeMatches =
      outcomeFilter.trim().length === 0 ||
      event.outcome.toLowerCase().includes(outcomeFilter.trim().toLowerCase());
    return actionMatches && principalMatches && outcomeMatches;
  });
  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Tamper-evident log</p>
          <h2>Audit</h2>
        </div>
        <span className="tag">{filteredEvents.length} of {events.length} events</span>
      </div>
      <div className="form-grid three-columns audit-filters">
        <label>
          <span>Action filter</span>
          <input value={actionFilter} onChange={(event) => setActionFilter(event.target.value)} />
        </label>
        <label>
          <span>Principal filter</span>
          <input
            value={principalFilter}
            onChange={(event) => setPrincipalFilter(event.target.value)}
          />
        </label>
        <label>
          <span>Outcome filter</span>
          <input value={outcomeFilter} onChange={(event) => setOutcomeFilter(event.target.value)} />
        </label>
      </div>
      <div className="table-list">
        {filteredEvents.length === 0 ? (
          <EmptyState icon={ScrollText} text="No audit events match the filters" />
        ) : (
          filteredEvents.map((event) => (
            <details className="audit-event" key={event.id}>
              <summary>
                <span>
                  <strong>{event.action}</strong>
                  <small>{event.principal ?? "system"} - {event.outcome}</small>
                </span>
                <span className="tag">{new Date(event.at_millis).toLocaleTimeString()}</span>
              </summary>
              <pre className="json-view">{stringifyJson(event as unknown as JsonValue)}</pre>
            </details>
          ))
        )}
      </div>
    </section>
  );
}

function AdviceView({
  client,
  data,
  onError,
}: {
  client: ControlPlaneClient;
  data: LoadedStudioData;
  onError: (error?: unknown) => void;
}) {
  const report = data.advice;
  const [plans, setPlans] = useState<Record<string, MigrationPlan>>({});
  const [decisions, setDecisions] = useState<Record<string, string>>({});
  const [reasons, setReasons] = useState<Record<string, string>>({});
  const [loadingAdvice, setLoadingAdvice] = useState<string>();

  const loadPlan = async (adviceId: string) => {
    setLoadingAdvice(`plan:${adviceId}`);
    onError(undefined);
    try {
      const plan = await client.advicePlan(adviceId);
      setPlans((current) => ({ ...current, [adviceId]: plan }));
    } catch (error) {
      onError(error);
    } finally {
      setLoadingAdvice(undefined);
    }
  };

  const rejectAdvice = async (adviceId: string) => {
    const reason = reasons[adviceId]?.trim() ?? "";
    if (reason.length === 0) {
      onError(new Error("Reject requires a reason."));
      return;
    }
    setLoadingAdvice(`reject:${adviceId}`);
    onError(undefined);
    try {
      const decision = await client.recordAdviceDecision(adviceId, "rejected", reason);
      setDecisions((current) => ({
        ...current,
        [adviceId]: `${decision.status} by ${decision.decided_by}`,
      }));
    } catch (error) {
      onError(error);
    } finally {
      setLoadingAdvice(undefined);
    }
  };

  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Advisor</p>
          <h2>Runtime Advice</h2>
        </div>
      </div>
      {data.adviceError !== undefined ? (
        <Notice tone="warning" message={data.adviceError} />
      ) : report === undefined || report.recommendations.length === 0 ? (
        <EmptyState icon={ShieldCheck} text="No runtime recommendations" />
      ) : (
        <div className="advice-list">
          {report.recommendations.map((advice) => (
            <article className="advice-card" key={advice.id}>
              <div className="advice-header">
                <div>
                  <span className="tag">{advice.code}</span>
                  <h3>{advice.message}</h3>
                </div>
                <span className="status-pill">{advice.status}</span>
              </div>
              <p>{advice.rationale}</p>
              <div className="manifest-row">
                <Detail label="Risk" value={advice.risk} />
                <Detail label="Plan" value={advice.dry_run.plan_id} />
                <Detail label="Hint" value={advice.dry_run.operation_hint} />
              </div>
              <div className="manifest-row">
                <Detail label="Expected gain" value={advice.expected_gain} />
                <Detail label="Disk" value={advice.cost.disk} />
                <Detail label="Effort" value={advice.cost.operator_effort} />
              </div>
              <div className="button-row">
                <button
                  className="secondary-button"
                  type="button"
                  onClick={() => void loadPlan(advice.id)}
                  disabled={loadingAdvice !== undefined}
                >
                  {loadingAdvice === `plan:${advice.id}` ? (
                    <Loader2 size={16} aria-hidden />
                  ) : (
                    <ListChecks size={16} aria-hidden />
                  )}
                  Load plan
                </button>
              </div>
              {plans[advice.id] !== undefined && <PlanSummary plan={plans[advice.id]} />}
              <div className="form-grid two-columns advice-decision">
                <label>
                  <span>Reject reason</span>
                  <input
                    value={reasons[advice.id] ?? ""}
                    onChange={(event) =>
                      setReasons((current) => ({ ...current, [advice.id]: event.target.value }))
                    }
                  />
                </label>
                <div className="button-row align-end">
                  <button
                    className="danger-button"
                    type="button"
                    onClick={() => void rejectAdvice(advice.id)}
                    disabled={
                      loadingAdvice !== undefined ||
                      (reasons[advice.id]?.trim().length ?? 0) === 0
                    }
                  >
                    {loadingAdvice === `reject:${advice.id}` ? (
                      <Loader2 size={16} aria-hidden />
                    ) : (
                      <Trash2 size={16} aria-hidden />
                    )}
                    Reject
                  </button>
                </div>
              </div>
              {decisions[advice.id] !== undefined && (
                <Notice tone="info" message={`Decision recorded: ${decisions[advice.id]}`} />
              )}
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function ConfigAndMigration({
  data,
  desiredSpec,
  validation,
  plan,
  actionLoading,
  onDesiredSpecChange,
  onValidate,
  onPlan,
}: {
  data: LoadedStudioData;
  desiredSpec: string;
  validation?: ValidationReport;
  plan?: MigrationPlan;
  actionLoading?: string;
  onDesiredSpecChange: (value: string) => void;
  onValidate: () => void;
  onPlan: () => void;
}) {
  const desired = parseDesiredDatabaseSpec(desiredSpec, data.config);
  const spec = desired.spec;
  const selectedProfile = data.profiles.find((profile) => profile.slug === spec.profile);
  const selectedDeploymentHelp = deploymentHelp[spec.deployment.mode];
  const selectedReplicationHelp = replicationHelp[spec.defaults.replication];
  const selectedWriteAckHelp = writeAckHelp[spec.guarantees.write_ack];
  const selectedConflictHelp = conflictHelp[spec.guarantees.conflict_resolution];
  const domainNames = spec.domains.map((domain) => domain.name).filter(Boolean);
  const profileOptions = data.profiles.some((profile) => profile.slug === spec.profile)
    ? data.profiles
    : [
        {
          slug: spec.profile,
          aliases: [],
          status: "Custom",
          description: "Custom profile from the desired JSON.",
          default_domain: spec.defaults.consistency_domain,
          compatible_roles: [],
        },
        ...data.profiles,
      ];

  const updateSpec = (mutator: (draft: DatabaseSpec) => void) => {
    const next = cloneDatabaseSpec(spec);
    mutator(next);
    onDesiredSpecChange(stringifyJson(next));
  };

  const updateGuarantee = <K extends keyof DatabaseSpec["guarantees"]>(
    key: K,
    value: DatabaseSpec["guarantees"][K],
  ) => {
    updateSpec((draft) => {
      draft.guarantees[key] = value;
    });
  };

  const setCollectionIndex = (
    collectionIndex: number,
    indexKind: CollectionIndexKind,
    enabled: boolean,
  ) => {
    updateSpec((draft) => {
      const collection = draft.collections[collectionIndex];
      if (collection === undefined) return;
      const nextIndexes = new Set(collection.indexes);
      if (enabled) {
        nextIndexes.add(indexKind);
      } else {
        nextIndexes.delete(indexKind);
      }
      collection.indexes = collectionIndexes.filter((kind) => nextIndexes.has(kind));
    });
  };

  return (
    <section className="panel">
      <div className="config-page">
        <div className="section-heading">
          <div>
            <p className="eyebrow">DatabaseSpec</p>
            <h2>Config and Migration</h2>
            <p className="supporting-copy">
              Build the desired config with guided controls; expert JSON stays synced as a fallback.
            </p>
          </div>
          <div className="button-row">
            <button className="secondary-button" type="button" onClick={onValidate}>
              {actionLoading === "validate" ? <Loader2 size={16} aria-hidden /> : <ClipboardCheck size={16} aria-hidden />}
              Validate
            </button>
            <button className="secondary-button" type="button" onClick={onPlan}>
              {actionLoading === "plan" ? <Loader2 size={16} aria-hidden /> : <ListChecks size={16} aria-hidden />}
              Build Dry Run
            </button>
          </div>
        </div>
        <Notice
          tone="info"
          message="Topology/profile/replication changes are dry-run and audit only in this build; physical runtime switch remains unsupported."
        />
        {desired.error !== undefined && (
          <Notice tone="error" message={`Expert JSON is not valid yet: ${desired.error}`} />
        )}
        {validation !== undefined && <ValidationSummary report={validation} />}
        {plan !== undefined && <PlanSummary plan={plan} />}
        <div className="config-layout">
          <div className="config-form">
            <ConfigSection
              title="General"
              description="Identity and product profile for this database."
              issueCount={issueCountFor(validation, ["name", "profile"])}
            >
              <div className="form-grid two-columns">
                <label>
                  <span>Database name</span>
                  <input
                    value={spec.name}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.name = event.target.value;
                      })
                    }
                  />
                </label>
                <label>
                  <span>Profile</span>
                  <select
                    value={spec.profile}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.profile = event.target.value;
                      })
                    }
                  >
                    {profileOptions.map((profile) => (
                      <option key={profile.slug} value={profile.slug}>
                        {profile.slug} - {profile.status}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <HelpText
                title={selectedProfile?.slug ?? spec.profile}
                text={
                  selectedProfile?.description ??
                  "Custom profile. Use when an operator owns the compatibility contract; risk: validation may downgrade certification."
                }
              />
            </ConfigSection>

            <ConfigSection
              title="Deployment"
              description="Where the database should run and where its files live."
              issueCount={issueCountFor(validation, ["deployment"])}
            >
              <div className="form-grid two-columns">
                <label>
                  <span>Deployment mode</span>
                  <select
                    value={spec.deployment.mode}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.deployment.mode = event.target.value as DeploymentMode;
                      })
                    }
                  >
                    {deploymentModes.map((mode) => (
                      <option key={mode} value={mode}>
                        {optionLabel(mode)}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>Storage path</span>
                  <input
                    value={spec.deployment.storage_path ?? ""}
                    placeholder="memory or data.redb"
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.deployment.storage_path =
                          event.target.value.trim().length === 0 ? null : event.target.value;
                      })
                    }
                  />
                </label>
              </div>
              <HelpText
                title={optionLabel(spec.deployment.mode)}
                text={`${selectedDeploymentHelp} Storage path empty means in-memory/managed by the runtime profile.`}
              />
            </ConfigSection>

            <ConfigSection
              title="Topology"
              description="Replica and shard intent used by validation, planning and audit."
              issueCount={issueCountFor(validation, ["topology"])}
            >
              <div className="form-grid two-columns">
                <label>
                  <span>Replica count</span>
                  <input
                    type="number"
                    min={0}
                    max={65535}
                    step={1}
                    value={spec.topology.replica_count}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.topology.replica_count = positiveIntegerFromInput(
                          event.target.value,
                          draft.topology.replica_count,
                        );
                      })
                    }
                  />
                </label>
                <label>
                  <span>Shard count</span>
                  <input
                    type="number"
                    min={0}
                    max={65535}
                    step={1}
                    value={spec.topology.shard_count}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.topology.shard_count = positiveIntegerFromInput(
                          event.target.value,
                          draft.topology.shard_count,
                        );
                      })
                    }
                  />
                </label>
              </div>
              <HelpText
                title="Replica and shard policy"
                text="Replica count is target node redundancy. Embedded/single-node must be 1; cluster CP needs odd 3+; cluster AP needs 2+. Shards describe data placement intent and currently produce dry-run/audit only."
              />
            </ConfigSection>

            <ConfigSection
              title="Replication"
              description="Default consistency domain and replication model for new resources."
              issueCount={issueCountFor(validation, ["defaults", "domains"])}
            >
              <div className="form-grid two-columns">
                <label>
                  <span>Replication mode</span>
                  <select
                    value={spec.defaults.replication}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.defaults.replication = event.target.value as ReplicationMode;
                      })
                    }
                  >
                    {replicationModes.map((mode) => (
                      <option key={mode} value={mode}>
                        {mode.toUpperCase()}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>Default domain</span>
                  <select
                    value={spec.defaults.consistency_domain}
                    onChange={(event) =>
                      updateSpec((draft) => {
                        draft.defaults.consistency_domain = event.target.value;
                      })
                    }
                  >
                    {domainNames.length === 0 ? (
                      <option value={spec.defaults.consistency_domain}>
                        {spec.defaults.consistency_domain}
                      </option>
                    ) : (
                      domainNames.map((domain) => (
                        <option key={domain} value={domain}>
                          {domain}
                        </option>
                      ))
                    )}
                  </select>
                </label>
              </div>
              <HelpText
                title={spec.defaults.replication.toUpperCase()}
                text={selectedReplicationHelp}
              />
            </ConfigSection>

            <ConfigSection
              title="Guarantees"
              description="Durability, conflict, security and audit promises."
              issueCount={issueCountFor(validation, ["guarantees"])}
            >
              <div className="form-grid two-columns">
                <label>
                  <span>Write acknowledgement</span>
                  <select
                    value={spec.guarantees.write_ack}
                    onChange={(event) =>
                      updateGuarantee("write_ack", event.target.value as WriteAck)
                    }
                  >
                    {writeAckOptions.map((option) => (
                      <option key={option} value={option}>
                        {optionLabel(option)}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>Conflict resolution</span>
                  <select
                    value={spec.guarantees.conflict_resolution}
                    onChange={(event) =>
                      updateGuarantee(
                        "conflict_resolution",
                        event.target.value as ConflictResolution,
                      )
                    }
                  >
                    {conflictOptions.map((option) => (
                      <option key={option} value={option}>
                        {optionLabel(option)}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="toggle-grid">
                <ToggleControl
                  label="Backups"
                  checked={spec.guarantees.backup.enabled}
                  help="Enables backup coverage. Use for production; risk: disabled backups can block certified profiles."
                  onChange={(checked) =>
                    updateSpec((draft) => {
                      draft.guarantees.backup.enabled = checked;
                    })
                  }
                />
                <ToggleControl
                  label="Point-in-time recovery"
                  checked={spec.guarantees.backup.pitr}
                  help="Keeps restore points across time. Use for regulated or high-value data; risk: more storage."
                  onChange={(checked) =>
                    updateSpec((draft) => {
                      draft.guarantees.backup.pitr = checked;
                    })
                  }
                />
                <ToggleControl
                  label="Encryption at rest"
                  checked={spec.guarantees.encryption.at_rest}
                  help="Encrypts stored data. Use for sensitive data; risk: key management must be ready."
                  onChange={(checked) =>
                    updateSpec((draft) => {
                      draft.guarantees.encryption.at_rest = checked;
                    })
                  }
                />
                <ToggleControl
                  label="Audit log"
                  checked={spec.guarantees.audit.enabled}
                  help="Records admin and config events. Use for production/admin panels; risk: disabling hides accountability."
                  onChange={(checked) =>
                    updateSpec((draft) => {
                      draft.guarantees.audit.enabled = checked;
                    })
                  }
                />
                <ToggleControl
                  label="Sensitive data"
                  checked={spec.guarantees.sensitive_data}
                  help="Marks data as sensitive. Use for PII/secrets; risk: validator expects encryption and stronger controls."
                  onChange={(checked) =>
                    updateGuarantee("sensitive_data", checked)
                  }
                />
                <ToggleControl
                  label="Strict cross-domain transactions"
                  checked={spec.guarantees.strict_cross_domain_transactions}
                  help="Blocks mixed guarantee transactions unless safe. Use for CP/AP separation; risk: stricter writes."
                  onChange={(checked) =>
                    updateGuarantee("strict_cross_domain_transactions", checked)
                  }
                />
              </div>
              <div className="help-grid">
                <HelpText title={optionLabel(spec.guarantees.write_ack)} text={selectedWriteAckHelp} />
                <HelpText
                  title={optionLabel(spec.guarantees.conflict_resolution)}
                  text={selectedConflictHelp}
                />
              </div>
            </ConfigSection>

            <ConfigSection
              title="Domains"
              description="Named consistency zones used by collections."
              issueCount={issueCountFor(validation, ["domains"])}
            >
              <div className="resource-list">
                {spec.domains.map((domain, index) => (
                  <div className="config-editor-row" key={`${domain.name}-${index}`}>
                    <label>
                      <span>Domain name</span>
                      <input
                        value={domain.name}
                        onChange={(event) =>
                          updateSpec((draft) => {
                            draft.domains[index].name = event.target.value;
                          })
                        }
                      />
                    </label>
                    <label>
                      <span>Domain mode</span>
                      <select
                        value={domain.mode}
                        onChange={(event) =>
                          updateSpec((draft) => {
                            draft.domains[index].mode = event.target.value as ConsistencyMode;
                          })
                        }
                      >
                        {consistencyModes.map((mode) => (
                          <option key={mode} value={mode}>
                            {optionLabel(mode)}
                          </option>
                        ))}
                      </select>
                    </label>
                    <button
                      className="icon-button"
                      type="button"
                      title="Remove domain"
                      aria-label={`Remove domain ${domain.name || index + 1}`}
                      onClick={() =>
                        updateSpec((draft) => {
                          draft.domains.splice(index, 1);
                        })
                      }
                    >
                      <Trash2 size={16} aria-hidden />
                    </button>
                    <HelpText title={optionLabel(domain.mode)} text={consistencyHelp[domain.mode]} />
                  </div>
                ))}
              </div>
              <button
                className="secondary-button"
                type="button"
                onClick={() =>
                  updateSpec((draft) => {
                    draft.domains.push({ name: `domain_${draft.domains.length + 1}`, mode: "local_snapshot" });
                  })
                }
              >
                <Plus size={16} aria-hidden />
                Add domain
              </button>
            </ConfigSection>

            <ConfigSection
              title="Collections"
              description="Resource intent, consistency domain and indexes."
              issueCount={issueCountFor(validation, ["collections"])}
            >
              <div className="resource-list">
                {spec.collections.length === 0 ? (
                  <EmptyState icon={Layers3} text="No collection policies yet" />
                ) : (
                  spec.collections.map((collection, index) => (
                    <div className="collection-editor" key={`${collection.name}-${index}`}>
                      <div className="form-grid three-columns">
                        <label>
                          <span>Collection name</span>
                          <input
                            value={collection.name}
                            onChange={(event) =>
                              updateSpec((draft) => {
                                draft.collections[index].name = event.target.value;
                              })
                            }
                          />
                        </label>
                        <label>
                          <span>Role</span>
                          <select
                            value={collection.role}
                            onChange={(event) =>
                              updateSpec((draft) => {
                                draft.collections[index].role =
                                  event.target.value as CollectionRole;
                              })
                            }
                          >
                            {collectionRoles.map((role) => (
                              <option key={role} value={role}>
                                {optionLabel(role)}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>Domain</span>
                          <select
                            value={collection.domain}
                            onChange={(event) =>
                              updateSpec((draft) => {
                                draft.collections[index].domain = event.target.value;
                              })
                            }
                          >
                            {domainNames.length === 0 ? (
                              <option value={collection.domain}>{collection.domain}</option>
                            ) : (
                              domainNames.map((domain) => (
                                <option key={domain} value={domain}>
                                  {domain}
                                </option>
                              ))
                            )}
                          </select>
                        </label>
                      </div>
                      <div className="chip-row" aria-label={`Indexes for ${collection.name || index + 1}`}>
                        {collectionIndexes.map((indexKind) => (
                          <label className="chip-toggle" key={indexKind}>
                            <input
                              type="checkbox"
                              checked={collection.indexes.includes(indexKind)}
                              onChange={(event) =>
                                setCollectionIndex(index, indexKind, event.target.checked)
                              }
                            />
                            <span>{optionLabel(indexKind)}</span>
                          </label>
                        ))}
                      </div>
                      <div className="button-row">
                        <button
                          className="danger-button"
                          type="button"
                          onClick={() =>
                            updateSpec((draft) => {
                              draft.collections.splice(index, 1);
                            })
                          }
                        >
                          <Trash2 size={16} aria-hidden />
                          Remove collection
                        </button>
                      </div>
                    </div>
                  ))
                )}
              </div>
              <button
                className="secondary-button"
                type="button"
                onClick={() =>
                  updateSpec((draft) => {
                    draft.collections.push({
                      name: `collection_${draft.collections.length + 1}`,
                      role: "document_entity",
                      domain: draft.defaults.consistency_domain,
                      indexes: ["document"],
                    });
                  })
                }
              >
                <Plus size={16} aria-hidden />
                Add collection
              </button>
            </ConfigSection>
          </div>

          <aside className="config-preview">
            <ConfigSection
              title="Preview"
              description="Current runtime config and desired spec side by side."
              issueCount={0}
            >
              <h3>Current config</h3>
              <pre className="json-view">{stringifyJson(data.config)}</pre>
              <h3>Desired preview</h3>
              <pre className="json-view">{stringifyJson(spec)}</pre>
            </ConfigSection>
            <ConfigSection
              title="Expert JSON"
              description="Fallback editor for fields that do not have a first-class control yet."
              issueCount={desired.error === undefined ? 0 : 1}
            >
              <label>
                <span>Desired config JSON</span>
                <textarea
                  value={desiredSpec}
                  onChange={(event) => onDesiredSpecChange(event.target.value)}
                />
              </label>
            </ConfigSection>
          </aside>
        </div>
      </div>
    </section>
  );
}

function ConfigSection({
  title,
  description,
  issueCount,
  children,
}: {
  title: string;
  description: string;
  issueCount: number;
  children: ReactNode;
}) {
  return (
    <section className="config-section">
      <div className="section-heading compact">
        <div>
          <h3>{title}</h3>
          <p className="supporting-copy">{description}</p>
        </div>
        {issueCount > 0 && <span className="tag tone-warning">{issueCount} issue</span>}
      </div>
      {children}
    </section>
  );
}

function ToggleControl({
  label,
  checked,
  help,
  onChange,
}: {
  label: string;
  checked: boolean;
  help: string;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="toggle-control">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      <span>
        <strong>{label}</strong>
        <small>{help}</small>
      </span>
    </label>
  );
}

function ValidationSummary({ report }: { report: ValidationReport }) {
  return (
    <div className={report.valid ? "validation validation-ok" : "validation validation-error"}>
      <div className="validation-header">
        {report.valid ? <CheckCircle2 size={18} aria-hidden /> : <AlertTriangle size={18} aria-hidden />}
        <strong>{report.valid ? "Validation passed" : "Validation failed"}</strong>
        <span>{report.status}</span>
      </div>
      {report.issues.length > 0 && <IssueList issues={report.issues} />}
    </div>
  );
}

function IssueList({ issues }: { issues: ValidationIssue[] }) {
  return (
    <div className="issue-list">
      {issues.map((issue) => (
        <article className={`issue ${severityClass(issue.severity)}`} key={`${issue.code}-${issue.path}`}>
          <div>
            <strong>{issue.code}</strong>
            <code>{issue.path}</code>
          </div>
          <p>{issue.message}</p>
          <span>{issue.suggestion}</span>
        </article>
      ))}
    </div>
  );
}

function PlanSummary({ plan }: { plan: MigrationPlan }) {
  return (
    <div className="plan-summary">
      <div className="plan-header">
        <div>
          <span className="eyebrow">Plan ID</span>
          <strong>{plan.plan_id}</strong>
        </div>
        <span className={plan.valid ? "tag tone-info" : "tag tone-error"}>
          {plan.valid ? "valid" : "invalid"}
        </span>
        <span className={plan.apply_supported ? "tag tone-info" : "tag tone-warning"}>
          apply_supported={String(plan.apply_supported)}
        </span>
      </div>
      <div className="manifest-row">
        <Detail label="Risk" value={plan.impact.risk} />
        <Detail label="Downtime" value={plan.impact.downtime} />
        <Detail label="Rollback" value={plan.rollback.description} />
      </div>
      {!plan.apply_supported && (
        <Notice
          tone="warning"
          message="This plan is safe to review and audit, but apply is disabled for unsupported physical changes such as topology, profile or replication."
        />
      )}
      {plan.steps.length > 0 && (
        <div className="step-list">
          {plan.steps.map((step) => (
            <article className="step-item" key={step.step_id}>
              <div>
                <strong>{optionLabel(step.kind)}</strong>
                <code>{step.path}</code>
                <p>{step.action}</p>
              </div>
              <span className={step.supported ? "tag tone-info" : "tag tone-warning"}>
                supported={String(step.supported)}
              </span>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}

function ResourceCard({ object }: { object: CatalogObjectSummary }) {
  return (
    <article className="resource-card">
      <div>
        <strong>{object.name}</strong>
        <span>{object.kind}</span>
      </div>
      <p>{object.row_count ?? 0} records</p>
    </article>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="detail">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function HelpText({ title, text }: { title: string; text: string }) {
  return (
    <div className="help-text">
      <strong>{title}</strong>
      <span>{text}</span>
    </div>
  );
}

function Notice({ tone, message }: { tone: "error" | "warning" | "info"; message: string }) {
  const icon = tone === "error" ? AlertTriangle : CheckCircle2;
  const Icon = icon;
  return (
    <div className={`notice tone-${tone}`} role={tone === "error" ? "alert" : "status"}>
      <Icon size={18} aria-hidden />
      <span>{message}</span>
    </div>
  );
}

function EmptyState({
  icon,
  text,
}: {
  icon: ComponentType<{ size?: number; "aria-hidden"?: boolean }>;
  text: string;
}) {
  const Icon = icon;
  return (
    <div className="empty-state">
      <Icon size={24} aria-hidden />
      <span>{text}</span>
    </div>
  );
}

function StatusChip({ active, label }: { active: boolean; label: string }) {
  return <span className={active ? "status-pill" : "status-pill muted"}>{label}</span>;
}

function viewTitle(view: ViewId): string {
  return navItems.find((item) => item.id === view)?.label ?? "Studio";
}

function severityClass(severity: string): string {
  if (severity === "error") return "tone-error";
  if (severity === "warning") return "tone-warning";
  return "tone-info";
}

function resourceLabel(resource: JsonValue): string {
  if (typeof resource === "string") return resource;
  if (typeof resource === "object" && resource !== null) return JSON.stringify(resource);
  return String(resource);
}
