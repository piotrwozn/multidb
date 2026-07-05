import assert from "node:assert/strict";
import test from "node:test";
import {
  CONTROL_PLANE_API_VERSION,
  ControlPlaneClient,
  ControlPlaneError,
  MIN_MULTIDB_VERSION,
  defaultApiBase,
} from "../dist/index.js";

const jsonResponse = (status, payload) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });

test("defaults to the Docker Control Plane API base URL", () => {
  assert.equal(defaultApiBase(), "http://127.0.0.1:8080/api");
  assert.equal(CONTROL_PLANE_API_VERSION, 1);
  assert.equal(MIN_MULTIDB_VERSION, "0.1.0");
});

test("maps success and error envelopes", async () => {
  const calls = [];
  const client = new ControlPlaneClient({
    baseUrl: "http://unit.test/api",
    fetchImpl: async (input, init) => {
      calls.push({ input: String(input), init });
      if (String(input).endsWith("/status")) {
        return jsonResponse(200, {
          ok: true,
          data: {
            server_version: "test",
            uptime_millis: 1,
            profile: {},
            replication: {},
            layout: {},
            engine: "Memory",
            catalog_objects: 0,
            shard_count: 1,
          },
        });
      }
      return jsonResponse(401, {
        ok: false,
        error: { code: "unauthorized", message: "unauthorized" },
      });
    },
    token: "secret",
  });

  assert.equal((await client.status()).server_version, "test");
  await assert.rejects(() => client.authMe(), (error) => {
    assert.ok(error instanceof ControlPlaneError);
    assert.equal(error.status, 401);
    assert.equal(error.code, "unauthorized");
    return true;
  });
  assert.equal(calls[0].init.headers.Authorization, "Bearer secret");
});

test("detects invalid envelopes", async () => {
  const client = new ControlPlaneClient({
    fetchImpl: async () => jsonResponse(200, { status: "not-an-envelope" }),
  });
  await assert.rejects(() => client.status(), { code: "invalid_envelope" });
});
