import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import type { DatabaseSpec } from "./types";
import {
  auditFixture,
  authFixture,
  catalogFixture,
  adviceFixture,
  configFixture,
  domainsFixture,
  extensionsFixture,
  loginFixture,
  manifestFixture,
  migrationPlanFixture,
  profilesFixture,
  rolesFixture,
  securityFixture,
  statusFixture,
  validationFixture,
} from "./test/fixtures";

const envelope = (data: unknown) =>
  new Response(JSON.stringify({ ok: true, data }), {
    headers: { "Content-Type": "application/json" },
  });

const failedEnvelope = (status: number, code: string, message: string) =>
  new Response(JSON.stringify({ ok: false, error: { code, message } }), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const installFetch = (
  override?: (
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    path: string,
    method: string,
  ) => Response | undefined,
) => {
  const fetchMock = vi.fn(
    async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const method = init?.method ?? "GET";
      const path = new URL(String(input), "http://studio.test").pathname.replace(
        /^\/api/,
        "",
      );
      const overridden = override?.(input, init, path, method);
      if (overridden !== undefined) {
        return overridden;
      }
      if (method === "GET" && path === "/health") {
        return new Response(JSON.stringify({ ok: true, status: "healthy" }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (method === "GET" && path === "/ready") {
        return new Response(JSON.stringify({ ok: true, status: "ready" }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (method === "POST" && path === "/auth/login") {
        return envelope(loginFixture);
      }
      if (method === "POST" && path === "/auth/logout") {
        return envelope({});
      }
      if (method === "GET" && path === "/status") {
        return envelope(statusFixture);
      }
      if (method === "GET" && path === "/auth/me") {
        return envelope(authFixture);
      }
      if (method === "GET" && path === "/catalog") {
        return envelope(catalogFixture);
      }
      if (method === "GET" && path === "/security") {
        return envelope(securityFixture);
      }
      if (method === "GET" && path === "/audit") {
        return envelope(auditFixture);
      }
      if (method === "GET" && path === "/config") {
        return envelope(configFixture);
      }
      if (method === "GET" && path === "/studio") {
        return envelope(manifestFixture);
      }
      if (method === "GET" && path === "/profiles") {
        return envelope(profilesFixture);
      }
      if (method === "GET" && path === "/roles") {
        return envelope(rolesFixture);
      }
      if (method === "GET" && path === "/domains") {
        return envelope(domainsFixture);
      }
      if (method === "GET" && path === "/extensions") {
        return envelope(extensionsFixture);
      }
      if (method === "GET" && path === "/advice") {
        return envelope(adviceFixture);
      }
      if (method === "GET" && path === "/data/tables/users/rows") {
        return envelope({
          table: "users",
          schema: catalogFixture.objects[0].schema,
          rows: [[1, "Ada"]],
          offset: 0,
          limit: 100,
          returned: 1,
          has_more: false,
          next_offset: null,
          capped: false,
        });
      }
      if (method === "DELETE" && path === "/data/tables/users/rows") {
        return envelope({});
      }
      if (method === "GET" && path === "/data/collections/profiles/documents") {
        return envelope({
          collection: "profiles",
          documents: [{ id: "doc-1", document: { name: "Ada" } }],
          offset: 0,
          limit: 100,
          returned: 1,
          has_more: false,
          next_offset: null,
          capped: false,
        });
      }
      if (method === "DELETE" && path === "/data/collections/profiles/documents/doc-1") {
        return envelope({});
      }
      if (method === "POST" && path === "/sql") {
        return envelope({
          output: { kind: "affected_rows", affected_rows: 1 },
        });
      }
      if (method === "POST" && path === "/advice/plan") {
        return envelope(migrationPlanFixture);
      }
      if (method === "POST" && path === "/advice/decision") {
        return envelope({
          advice_id: "index-create-users-age",
          status: "rejected",
          reason: "defer until low traffic",
          decided_by: "admin",
          decided_at_millis: 1_720_000_000_500,
        });
      }
      if (method === "POST" && path === "/config/validate") {
        return envelope(validationFixture);
      }
      if (method === "POST" && path === "/config/plan") {
        return envelope(migrationPlanFixture);
      }
      return new Response(
        JSON.stringify({
          ok: false,
          error: { code: "not_found", message: `${method} ${path}` },
        }),
        { status: 404 },
      );
    },
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
};

const connect = async () => {
  const user = userEvent.setup();
  render(<App />);
  await user.type(screen.getByLabelText("Password"), "secret");
  await user.click(screen.getByRole("button", { name: /sign in/i }));
  await screen.findByText("0.1.0-test");
  return user;
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("App", () => {
  it("loads the operator overview from the Control Plane", async () => {
    installFetch();

    await connect();

    expect(screen.getByText("0.1.0-test")).toBeInTheDocument();
    expect(screen.getByText("users")).toBeInTheDocument();
    expect(screen.getByText("System admin")).toBeInTheDocument();
  });

  it("logs in with a password-backed session and omits principal spoofing", async () => {
    const fetchMock = installFetch();

    await connect();

    const loginCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/auth/login" && init?.method === "POST";
    });
    expect(loginCall).toBeDefined();
    if (loginCall === undefined) {
      throw new Error("missing /auth/login call");
    }
    const loginHeaders = loginCall[1]?.headers as Record<string, string>;
    expect(loginHeaders.Authorization).toBeUndefined();
    expect(JSON.parse(String(loginCall[1]?.body))).toEqual({
      username: "admin",
      password: "secret",
    });

    const authMeCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/auth/me" && init?.method !== "POST";
    });
    expect(authMeCall).toBeDefined();
    if (authMeCall === undefined) {
      throw new Error("missing /auth/me call");
    }
    const authHeaders = authMeCall[1]?.headers as Record<string, string>;
    expect(authHeaders.Authorization).toBe(`Bearer ${loginFixture.token}`);
    expect(authHeaders["x-multidb-principal"]).toBeUndefined();
  });

  it("shows a neutral login failure without creating a session", async () => {
    installFetch((_input, _init, path, method) => {
      if (method === "POST" && path === "/auth/login") {
        return failedEnvelope(401, "unauthorized", "invalid credentials");
      }
      return undefined;
    });
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText("Password"), "wrong");
    await user.click(screen.getByRole("button", { name: /sign in/i }));

    expect(await screen.findByText("Sign in failed. Check the admin username and password.")).toBeInTheDocument();
    expect(screen.queryByText("0.1.0-test")).not.toBeInTheDocument();
  });

  it("clears the session on 401 session expiry", async () => {
    let statusCalls = 0;
    installFetch((_input, _init, path, method) => {
      if (method === "GET" && path === "/status") {
        statusCalls += 1;
        if (statusCalls > 1) {
          return failedEnvelope(401, "unauthorized", "session expired");
        }
      }
      return undefined;
    });
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /refresh/i }));

    expect(await screen.findByText("Session expired. Sign in again.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /sign in/i })).toBeInTheDocument();
  });

  it("shows 403 forbidden without clearing the session", async () => {
    installFetch((_input, _init, path, method) => {
      if (method === "POST" && path === "/config/validate") {
        return failedEnvelope(403, "forbidden", "system admin required");
      }
      return undefined;
    });
    const user = await connect();
    await user.click(screen.getByRole("button", { name: /^config$/i }));
    await user.click(screen.getByRole("button", { name: /^validate$/i }));

    expect(await screen.findByText("Forbidden (forbidden): system admin required")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /logout/i })).toBeInTheDocument();
  });

  it("renders validation errors as explicit failure state", async () => {
    installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /^config$/i }));
    await user.click(screen.getByRole("button", { name: /^validate$/i }));

    expect(await screen.findByText("Validation failed")).toBeInTheDocument();
    expect(screen.getByText("missing backup coverage")).toBeInTheDocument();
    expect(screen.queryByText("Validation passed")).not.toBeInTheDocument();
  });

  it("submits current and desired specs to the migration dry-run endpoint", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /^config$/i }));
    await user.selectOptions(screen.getByLabelText("Deployment mode"), "cluster");
    await user.selectOptions(screen.getByLabelText("Replication mode"), "cp");
    await user.clear(screen.getByLabelText("Replica count"));
    await user.type(screen.getByLabelText("Replica count"), "3");
    await user.click(screen.getByRole("button", { name: /build dry run/i }));

    expect(await screen.findByText("plan-1")).toBeInTheDocument();
    const planCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/config/plan" && init?.method === "POST";
    });
    expect(planCall).toBeDefined();
    if (planCall === undefined) {
      throw new Error("missing /config/plan call");
    }
    const body = JSON.parse(String(planCall[1]?.body)) as {
      current: unknown;
      desired: unknown;
    };
    expect(body.current).toEqual(configFixture);
    expect(body.desired).toMatchObject({
      deployment: { mode: "cluster" },
      defaults: { replication: "cp" },
      topology: { replica_count: 3 },
    });
  });

  it("runs SQL through the typed console endpoint", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /sql console/i }));
    await user.click(screen.getByRole("button", { name: /^run$/i }));

    const sqlCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/sql" && init?.method === "POST";
    });
    expect(sqlCall).toBeDefined();
  });

  it("renders SQL rows as a table and keeps in-memory history", async () => {
    installFetch((_input, _init, path, method) => {
      if (method === "POST" && path === "/sql") {
        return envelope({
          output: {
            kind: "rows",
            columns: ["table_name", "rows"],
            rows: [["users", 1]],
          },
        });
      }
      return undefined;
    });
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /sql console/i }));
    await user.click(screen.getByRole("button", { name: /^run$/i }));

    expect(await screen.findByText("table_name")).toBeInTheDocument();
    expect(screen.getByLabelText("SQL history")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /information_schema\.tables/i })).toBeInTheDocument();
  });

  it("loads Data Explorer rows with bounded pagination parameters", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /data explorer/i }));
    await user.click(screen.getByRole("button", { name: /load page/i }));

    expect(await screen.findByText("Ada")).toBeInTheDocument();
    const rowsCall = fetchMock.mock.calls.find(([input, init]) => {
      const url = new URL(String(input), "http://studio.test");
      return url.pathname === "/api/data/tables/users/rows" && init?.method !== "POST";
    });
    expect(rowsCall).toBeDefined();
    if (rowsCall === undefined) {
      throw new Error("missing paged rows call");
    }
    const url = new URL(String(rowsCall[0]), "http://studio.test");
    expect(url.searchParams.get("limit")).toBe("100");
    expect(url.searchParams.get("offset")).toBe("0");
  });

  it("requires exact destructive confirmations in Data Explorer", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /data explorer/i }));
    await user.click(screen.getByRole("button", { name: /load page/i }));

    const deleteRow = await screen.findByRole("button", { name: /delete row/i });
    expect(deleteRow).toBeDisabled();
    await user.type(screen.getByLabelText("Confirm table name"), "users");
    await user.click(deleteRow);

    const rowDeleteCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/data/tables/users/rows" && init?.method === "DELETE";
    });
    expect(rowDeleteCall).toBeDefined();
    if (rowDeleteCall === undefined) {
      throw new Error("missing table delete call");
    }
    expect(JSON.parse(String(rowDeleteCall[1]?.body))).toMatchObject({
      primary_key: 1,
      confirm: "users",
    });

    await user.click(screen.getByRole("button", { name: /profiles collection/i }));
    await user.click(screen.getByRole("button", { name: /load page/i }));
    await user.click(await screen.findByRole("button", { name: /doc-1/i }));
    const deleteDocument = screen.getByRole("button", { name: /delete document/i });
    expect(deleteDocument).toBeDisabled();
    await user.type(screen.getByLabelText("Confirm document id"), "doc-1");
    await user.click(deleteDocument);

    const docDeleteCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/data/collections/profiles/documents/doc-1" && init?.method === "DELETE";
    });
    expect(docDeleteCall).toBeDefined();
  });

  it("renders security and audit operations", async () => {
    installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /security/i }));

    expect(screen.getByText("admin -> admin")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /audit/i }));
    expect(screen.getByText("login")).toBeInTheDocument();
  });

  it("filters audit events and shows event details", async () => {
    installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /audit/i }));
    await user.type(screen.getByLabelText("Action filter"), "missing-action");

    expect(screen.getByText("No audit events match the filters")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Action filter"));
    await user.type(screen.getByLabelText("Action filter"), "login");
    await user.click(screen.getByText("login"));

    expect(screen.getByText(/"action": "login"/)).toBeInTheDocument();
  });

  it("saves users, roles and grants from the guided RBAC form", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /security/i }));
    await user.click(screen.getByRole("button", { name: /add user/i }));
    await user.click(screen.getByRole("button", { name: /add grant/i }));
    await user.click(screen.getByRole("button", { name: /save changes/i }));

    const saveCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/security" && init?.method === "POST";
    });
    expect(saveCall).toBeDefined();
    if (saveCall === undefined) {
      throw new Error("missing /security save call");
    }
    const body = JSON.parse(String(saveCall[1]?.body)) as typeof securityFixture;
    expect(body.principals.some((principal) => principal.user === "new-user")).toBe(true);
    expect(
      body.roles
        .find((role) => role.name === "admin")
        ?.grants.some((grant) => grant.resource === "Database" && grant.permission === "Read"),
    ).toBe(true);
  });

  it("builds a database spec dry-run from the resource builder", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /builder/i }));
    await user.click(screen.getByRole("button", { name: /^database$/i }));
    await user.clear(screen.getByLabelText("Database name"));
    await user.type(screen.getByLabelText("Database name"), "tenant_a");
    await user.click(screen.getByRole("button", { name: /^create$/i }));

    expect(await screen.findByText(/physical database\/profile switching/i)).toBeInTheDocument();
    const planCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/config/plan" && init?.method === "POST";
    });
    expect(planCall).toBeDefined();
    if (planCall === undefined) {
      throw new Error("missing /config/plan call");
    }
    const body = JSON.parse(String(planCall[1]?.body)) as {
      desired: { name: string; topology: { replica_count: number } };
    };
    expect(body.desired.name).toBe("tenant_a");
    expect(body.desired.topology.replica_count).toBe(1);
  });

  it("builds a version 2 custom_profile spec from the custom-profile builder", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /builder/i }));
    await user.click(screen.getByRole("button", { name: /^custom_profile$/i }));
    await user.clear(screen.getByLabelText("Profile slug"));
    await user.type(screen.getByLabelText("Profile slug"), "orders_durable");
    await user.selectOptions(screen.getByLabelText("Extends"), "game_local_balanced");
    await user.selectOptions(screen.getByLabelText(/durability\.commit/), "immediate");
    await user.selectOptions(screen.getByLabelText(/compression\.algorithm/), "zstd");
    await user.type(screen.getByLabelText(/cache\.max_entries/), "8192");
    await user.click(screen.getByRole("button", { name: /^create$/i }));

    expect(
      await screen.findByText(/databases are created from custom profiles by provisioning/i),
    ).toBeInTheDocument();
    const bodyOf = (endpoint: string): unknown => {
      const call = fetchMock.mock.calls.find(([input, init]) => {
        const path = new URL(String(input), "http://studio.test").pathname;
        return path === endpoint && init?.method === "POST";
      });
      if (call === undefined) {
        throw new Error(`missing ${endpoint} call`);
      }
      return JSON.parse(String(call[1]?.body));
    };
    const validated = bodyOf("/api/config/validate") as DatabaseSpec;
    const planned = bodyOf("/api/config/plan") as { current: unknown; desired: DatabaseSpec };
    expect(planned.current).toEqual(configFixture);
    expect(planned.desired).toEqual(validated);
    expect(planned.desired.version).toBe(2);
    expect(planned.desired.profile).toBe("orders_durable");
    expect(planned.desired.custom_profile).toEqual({
      extends: "game_local_balanced",
      description: "Custom operator-managed profile for this environment.",
      knobs: {
        durability: { commit: "immediate" },
        compression: { algorithm: "zstd" },
        cache: { max_entries: 8192 },
      },
    });
    expect(planned.desired.deployment.storage_path).not.toBeNull();
    expect(planned.desired.overrides).toEqual({});
    expect(planned.desired.operation_hints).toEqual({ "studio.builder": "custom_profile" });
    expect(JSON.stringify(planned.desired)).not.toContain("profile.custom.");
  });

  it("clears the storage path when a custom profile extends an in-memory variant", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /builder/i }));
    await user.click(screen.getByRole("button", { name: /^custom_profile$/i }));
    await user.selectOptions(screen.getByLabelText("Extends"), "in_memory");
    expect(screen.getByText(/in-memory profiles keep no files/i)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /^create$/i }));

    await screen.findByText(/databases are created from custom profiles by provisioning/i);
    const planCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/config/plan" && init?.method === "POST";
    });
    if (planCall === undefined) {
      throw new Error("missing /config/plan call");
    }
    const body = JSON.parse(String(planCall[1]?.body)) as { desired: DatabaseSpec };
    expect(body.desired.version).toBe(2);
    expect(body.desired.custom_profile?.extends).toBe("in_memory");
    expect(body.desired.custom_profile?.knobs).toBeUndefined();
    expect(body.desired.deployment).toEqual({ mode: "embedded", storage_path: null });
  });

  it("falls back to the balanced profile when the desired spec has none", async () => {
    const { profile: _omitted, ...withoutProfile } = configFixture;
    const fetchMock = installFetch((_input, _init, path, method) =>
      method === "GET" && path === "/config" ? envelope(withoutProfile) : undefined,
    );
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /^config$/i }));
    await user.click(screen.getByRole("button", { name: /^validate$/i }));

    await screen.findByText("Validation failed");
    const validateCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/config/validate" && init?.method === "POST";
    });
    if (validateCall === undefined) {
      throw new Error("missing /config/validate call");
    }
    const body = JSON.parse(String(validateCall[1]?.body)) as DatabaseSpec;
    expect(body.profile).toBe("balanced");
  });

  it("toggles dark mode from the command bar", async () => {
    installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /dark mode/i }));

    expect(screen.getByRole("button", { name: /light mode/i })).toBeInTheDocument();
  });

  it("renders Runtime Advisor recommendations as dry-run cards", async () => {
    installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /advice/i }));

    expect(screen.getByText("CREATE_INDEX")).toBeInTheDocument();
    expect(screen.getByText("Create an index for users.age")).toBeInTheDocument();
    expect(screen.getByText("advisor.index.create.users.age")).toBeInTheDocument();
    expect(screen.getByText("plan-advice-1")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /apply/i })).not.toBeInTheDocument();
  });

  it("plans and rejects Runtime Advisor recommendations through typed endpoints", async () => {
    const fetchMock = installFetch();
    const user = await connect();

    await user.click(screen.getByRole("button", { name: /advice/i }));
    await user.click(screen.getByRole("button", { name: /load plan/i }));
    expect(await screen.findByText("plan-1")).toBeInTheDocument();

    await user.type(screen.getByLabelText("Reject reason"), "defer until low traffic");
    await user.click(screen.getByRole("button", { name: /^reject$/i }));
    expect(await screen.findByText(/Decision recorded: rejected by admin/i)).toBeInTheDocument();

    const planCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/advice/plan" && init?.method === "POST";
    });
    const decisionCall = fetchMock.mock.calls.find(([input, init]) => {
      const path = new URL(String(input), "http://studio.test").pathname;
      return path === "/api/advice/decision" && init?.method === "POST";
    });
    expect(planCall).toBeDefined();
    expect(decisionCall).toBeDefined();
    if (planCall === undefined || decisionCall === undefined) {
      throw new Error("missing advice calls");
    }
    expect(JSON.parse(String(planCall[1]?.body))).toEqual({
      advice_id: "index-create-users-age",
    });
    expect(JSON.parse(String(decisionCall[1]?.body))).toEqual({
      advice_id: "index-create-users-age",
      status: "rejected",
      reason: "defer until low traffic",
    });
  });
});
