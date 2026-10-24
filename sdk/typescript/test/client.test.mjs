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

test("provisioning methods send the documented paths and bodies", async () => {
  const calls = [];
  const client = new ControlPlaneClient({
    baseUrl: "http://unit.test/api",
    token: "secret",
    principal: "root",
    fetchImpl: async (input, init) => {
      calls.push({
        url: String(input),
        method: init.method,
        body: init.body === undefined ? undefined : JSON.parse(init.body),
        headers: init.headers,
      });
      return jsonResponse(200, { ok: true, data: { echoed: true } });
    },
  });
  const spec = { version: 2, name: "shop" };
  const options = { allow_destructive: ["collections.orders"] };

  await client.provisionState();
  await client.provisionPlan(spec);
  await client.provisionPlan(spec, options);
  await client.provisionApply(spec, "prov_0123", options);
  await client.provisionDrift(spec, { unmanaged: "fail" });
  await client.liveConfig();

  assert.deepEqual(
    calls.map((call) => [call.method, call.url.replace("http://unit.test/api", ""), call.body]),
    [
      ["GET", "/provision/state", undefined],
      ["POST", "/provision/plan", { spec }],
      ["POST", "/provision/plan", { spec, options }],
      ["POST", "/provision/apply", { spec, options, confirm: "prov_0123" }],
      ["POST", "/provision/drift", { spec, options: { unmanaged: "fail" } }],
      ["GET", "/config/live", undefined],
    ],
  );
  assert.equal(calls[3].headers.Authorization, "Bearer secret");
  assert.equal(calls[3].headers["x-multidb-principal"], "root");
});

test("a blocked provisioning plan surfaces the plan on the error", async () => {
  const plan = { plan_id: "prov_0123", outcome: "blocked" };
  const client = new ControlPlaneClient({
    fetchImpl: async () =>
      jsonResponse(422, {
        ok: false,
        error: { code: "provision_blocked", message: "blocked" },
        data: plan,
      }),
  });
  await assert.rejects(() => client.provisionPlan({ version: 2 }), (error) => {
    assert.ok(error instanceof ControlPlaneError);
    assert.equal(error.status, 422);
    assert.equal(error.code, "provision_blocked");
    assert.deepEqual(error.body.data, plan);
    return true;
  });
});
