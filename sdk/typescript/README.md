# @multidb/client

TypeScript client for the MultiDB Control Plane API v1.

```ts
import { ControlPlaneClient } from "@multidb/client";

const client = new ControlPlaneClient();
const session = await client.login("admin", process.env.MULTIDB_ADMIN_PASSWORD ?? "");
const authed = client.withToken(session.token);
console.log(await authed.status());
await authed.logout();
```

The default base URL is `http://127.0.0.1:8080/api`. Browser apps can pass a
same-origin base URL such as `/api`.

## GraphRAG retrieval

Create a vector collection with passage metadata, connect the returned document
ids in a graph, and retrieve context. This example uses the authenticated
`authed` client above and fresh collection/graph names:

```ts
await authed.createVector({ name: "sdk_chunks", dim: 3 });
const first = await authed.insertVector(
  "sdk_chunks",
  { text: "MultiDB stores vectors with document metadata.", source: "guide/vectors" },
  [1, 0, 0],
);
const second = await authed.insertVector(
  "sdk_chunks",
  { text: "Graph edges connect related passages.", source: "guide/graphs" },
  [0.8, 0.2, 0],
);
await authed.createGraph({ name: "sdk_knowledge", graph_id: 41 });
await authed.addGraphEdge("sdk_knowledge", {
  source: first.id,
  label: "related",
  target: second.id,
  props: { reason: "same guide" },
});
const result = await authed.graphRagQuery({
  vector_collection: "sdk_chunks",
  graph: "sdk_knowledge",
  query_vector: [1, 0, 0],
  seed_k: 1,
  max_depth: 1,
});
console.log(result.context);
console.log(result.sources, result.relationships, result.truncated);
```

Graph nodes use the lowercase 32-character hexadecimal `id` returned by
`insertVector`. Passage text and provenance come from vector metadata fields
`text` and `source`; `text_field` and `source_field` can select nested paths.
Each source includes its `document_id`, `text`, nullable `source`,
`seed_distance`, `depth` and `seed_id`. Relationships contain `source`,
`label` and `target`; `truncated` reports that a retrieval limit was reached.

The server defaults are `seed_k: 8`, `max_depth: 2`, `max_nodes: 64`,
`max_edges: 256`, `max_context_bytes: 16384`, `labels: ["related"]`,
`text_field: ["text"]` and `source_field: ["source"]`. Omitted options are
left to the server. This retrieves
and assembles context locally; it does not generate embeddings or call an LLM.
The caller needs read access to the vector collection and graph; adding edges
needs write access to the graph.

`GraphRagRequest`, `GraphRagResult`, `GraphRagSource`, `GraphRagRelationship`,
`GraphEdgeRequest` and `GraphEdgeWriteResult` are exported types. Server
validation and authorization failures are raised as `ControlPlaneError`, with
their HTTP `status`, `code` and original response `body`.

Run the SDK checks from this directory with `npm ci` and `npm test`.
