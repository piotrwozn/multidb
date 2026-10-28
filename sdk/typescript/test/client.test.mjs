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

test("GraphRAG and graph edge methods preserve payloads, results, auth and encoded paths", async () => {
  const calls = [];
  const source = "01".repeat(16);
  const target = "02".repeat(16);
  const result = {
    context: "A retrieved passage",
    sources: [{ document_id: source, text: "A retrieved passage", source: null,
      seed_distance: 0.25, depth: 0, seed_id: source }],
    relationships: [{ source, label: "related", target }],
    truncated: false,
  };
  const client = new ControlPlaneClient({
    baseUrl: "http://unit.test/api/",
    token: "session-token",
    principal: "reader",
    fetchImpl: async (input, init) => {
      calls.push({ url: String(input), ...init, body: JSON.parse(init.body) });
      return jsonResponse(200, {
        ok: true,
        data: String(input).endsWith("/graphrag/query") ? result : { written: true },
      });
    },
  });
  const minimal = { vector_collection: "chunks", graph: "knowledge", query_vector: [1, 0, 0] };
  assert.deepEqual(await client.graphRagQuery(minimal), result);
  const bounded = { ...minimal, seed_k: 1, max_depth: 2, max_nodes: 4, max_edges: 8,
    max_context_bytes: 1024, labels: ["related", "cites"],
    text_field: ["content", "text"], source_field: ["provenance", "url"] };
  assert.deepEqual(await client.graphRagQuery(bounded), result);
  const edge = { source, label: "related", target, props: { confidence: 0.9, tags: ["demo"], note: null } };
  assert.deepEqual(await client.addGraphEdge("knowledge /?", edge), { written: true });
  assert.deepEqual(await client.addGraphEdge("knowledge", { source, label: "cites", target }), { written: true });
  assert.deepEqual(calls.map(({ url, method, body }) => ({ url, method, body })), [
    { url: "http://unit.test/api/graphrag/query", method: "POST", body: minimal },
    { url: "http://unit.test/api/graphrag/query", method: "POST", body: bounded },
    { url: "http://unit.test/api/data/graphs/knowledge%20%2F%3F/edges", method: "POST", body: edge },
    { url: "http://unit.test/api/data/graphs/knowledge/edges", method: "POST", body: { source, label: "cites", target } },
  ]);
  for (const { headers } of calls) {
    assert.equal(headers.Authorization, "Bearer session-token");
    assert.equal(headers["x-multidb-principal"], "reader");
    assert.equal(headers["Content-Type"], "application/json");
    assert.equal(headers.Accept, "application/json");
  }
});

test("GraphRAG and graph edge methods preserve server errors", async () => {
  for (const [status, code] of [[400, "invalid_value"], [403, "forbidden"], [404, "not_found"]]) {
    const payload = { ok: false, error: { code, message: "request refused" }, data: { reason: "fixture" } };
    const client = new ControlPlaneClient({ fetchImpl: async () => jsonResponse(status, payload) });
    for (const operation of [
      () => client.graphRagQuery({ vector_collection: "chunks", graph: "knowledge", query_vector: [1] }),
      () => client.addGraphEdge("knowledge", { source: "01".repeat(16), label: "related", target: "02".repeat(16) }),
    ]) {
      await assert.rejects(operation, (error) => {
        assert.ok(error instanceof ControlPlaneError);
        assert.equal(error.status, status);
        assert.equal(error.code, code);
        assert.equal(error.message, "request refused");
        assert.deepEqual(error.body, payload);
        return true;
      });
    }
  }
});
