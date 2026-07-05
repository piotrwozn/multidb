import { describe, expect, it, vi } from "vitest";

import { ControlPlaneClient, parseJsonObject } from "./api";
import { statusFixture, validationFixture } from "./test/fixtures";

const jsonResponse = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

describe("ControlPlaneClient", () => {
  it("unwraps successful Control Plane envelopes", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse({ ok: true, data: statusFixture }),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "secret",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await expect(client.status()).resolves.toEqual(statusFixture);
  });

  it("sends bearer auth and an in-memory principal header", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse({ ok: true, data: statusFixture }),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "secret",
      principal: "operator",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await client.status();

    const [, init] = fetchMock.mock.calls[0]!;
    const headers = init?.headers as Record<string, string>;
    expect(headers.Authorization).toBe("Bearer secret");
    expect(headers["x-multidb-principal"]).toBe("operator");
  });

  it("logs in without a bearer token", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse({
          ok: true,
          data: {
            token: "mda1_session_token",
            expires_at: "2026-07-05T12:00:00Z",
            expires_at_millis: 1,
            principal: "admin",
            roles: ["admin"],
          },
        }),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await client.login("admin", "secret");

    const [, init] = fetchMock.mock.calls[0]!;
    const headers = init?.headers as Record<string, string>;
    expect(headers.Authorization).toBeUndefined();
    expect(init?.body).toBe(JSON.stringify({ username: "admin", password: "secret" }));
  });

  it("does not send a principal header for session clients by default", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse({ ok: true, data: statusFixture }),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "mda1_session_token",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await client.status();

    const [, init] = fetchMock.mock.calls[0]!;
    const headers = init?.headers as Record<string, string>;
    expect(headers.Authorization).toBe("Bearer mda1_session_token");
    expect(headers["x-multidb-principal"]).toBeUndefined();
  });

  it("throws visible errors for failed envelopes", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse(
          {
            ok: false,
            error: { code: "unauthorized", message: "missing token" },
          },
          401,
        ),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "bad",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await expect(client.status()).rejects.toMatchObject({
      status: 401,
      code: "unauthorized",
      message: "missing token",
    });
  });

  it("surfaces forbidden envelopes from protected actions", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse(
          {
            ok: false,
            error: { code: "forbidden", message: "system admin required" },
          },
          403,
        ),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "secret",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await expect(client.validate({ version: 1 })).rejects.toMatchObject({
      status: 403,
      code: "forbidden",
      message: "system admin required",
    });
  });

  it("rejects invalid envelopes before reading data", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse({ data: statusFixture }),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "secret",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await expect(client.status()).rejects.toMatchObject({
      status: 200,
      code: "invalid_envelope",
    });
  });

  it("rejects non-JSON responses with response status", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        new Response("not json", {
          status: 502,
          headers: { "Content-Type": "text/plain" },
        }),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "secret",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await expect(client.status()).rejects.toMatchObject({
      status: 502,
      code: "invalid_json",
    });
  });

  it("accepts valid validation reports carried by 422 responses", async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> =>
        jsonResponse({ ok: true, data: validationFixture }, 422),
    );
    const client = new ControlPlaneClient({
      baseUrl: "/api",
      token: "secret",
      fetchImpl: fetchMock as unknown as typeof fetch,
    });

    await expect(client.validate({ version: 1 })).resolves.toEqual(
      validationFixture,
    );
  });

  it("parses top-level JSON objects only for desired specs", () => {
    expect(parseJsonObject("{\"version\":1}")).toEqual({ version: 1 });
    expect(() => parseJsonObject("[1,2,3]")).toThrow("Expected a JSON object");
  });
});
