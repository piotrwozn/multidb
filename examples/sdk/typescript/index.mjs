import { ControlPlaneClient } from "@multidb/client";

const baseUrl = process.env.MULTIDB_CONTROL_PLANE_URL ?? "http://127.0.0.1:8080/api";
const password = process.env.MULTIDB_ADMIN_PASSWORD ?? "local-dev-admin-password";
const stamp = `ts_${Date.now()}`;

const client = new ControlPlaneClient({ baseUrl });
const session = await client.login("admin", password);
const db = client.withToken(session.token);

try {
  const table = `sdk_users_${stamp}`;
  await db.createTable({
    name: table,
    schema: {
      columns: [
        { name: "id", ty: "Int", nullable: false },
        { name: "name", ty: "Str", nullable: false },
      ],
      primary_key: 0,
    },
    indexes: [],
  });
  await db.insertTableRow(table, [1, "Ada"]);
  await db.sql(`SELECT * FROM ${table}`);

  const collection = `sdk_docs_${stamp}`;
  await db.createCollection({
    name: collection,
    fields: [{ name: "name", source: { Path: ["name"] }, ty: "Str" }],
    indexes: [],
  });
  await db.createDocument(collection, { name: "Ada" });

  const vectors = `sdk_vectors_${stamp}`;
  await db.createVector({ name: vectors, dim: 3 });
  await db.insertVector(vectors, { label: "Ada" }, [1, 0, 0]);
  await db.searchVector(vectors, [1, 0, 0], 1);

  const series = `sdk_series_${stamp}`;
  await db.createTimeSeries({ name: series, chunk_millis: 60000, retention_millis: null });
  const now = Date.now();
  await db.insertTimeSeriesPoint(series, "default", { timestamp_millis: now, value: 42 });
  await db.timeSeriesPoints(series, "default", now - 1, now + 1);

  console.log("TypeScript SDK example completed");
} finally {
  await db.logout();
}
