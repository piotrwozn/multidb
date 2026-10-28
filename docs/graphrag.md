# GraphRAG retrieval

GraphRAG starts from semantically close passages in a vector collection, follows
their directed graph relationships, and returns a cited context with source
provenance. It runs locally in the Rust engine and through the preview Control
Plane API. TypeScript, Python, Go and Rust clients expose the same operation.

The caller supplies query/document embeddings. The result is retrieval context
for an application to use when answering a question; embedding generation,
entity extraction and language-model answer generation belong to the application.

## Run the example

From a checkout:

```sh
cargo run --locked --example graphrag
```

The two-dimensional fixture deliberately places a linked passage far from the
query. A single vector seed retrieves the first passage, then one graph hop adds
the second. The example prints numbered citations and each source's graph depth.
It needs no service, model download or credentials.

## Build a knowledge graph over HTTP

Requests below use the authenticated `/api` base URL of `multidb serve`. Set
`Authorization: Bearer <token>` and `Content-Type: application/json`; normal
principal selection and RBAC apply. The SDK READMEs contain executable client
workflows for these requests.

1. Create a vector collection with `POST /api/builder/vector`:

   ```json
   {"name":"knowledge","dim":2,"metric":"L2"}
   ```

2. Insert passages through `POST /api/data/vectors/knowledge/vectors`:

   ```json
   {"metadata":{"text":"MultiDB stores document embeddings.","source":"guide/vectors"},"vector":[1,0]}
   ```

   ```json
   {"metadata":{"text":"A graph edge connects related passages.","source":"guide/graphs"},"vector":[0,1]}
   ```

   Save the `data.id` from each response. A graph node representing a passage is
   exactly that lowercase, 32-character hexadecimal vector document id. It is
   scoped by the chosen vector collection; retrieval never reads another
   collection to resolve a graph node.

3. Create a graph through `POST /api/builder/graph`:

   ```json
   {"name":"relations","graph_id":41}
   ```

4. Link the returned ids through `POST /api/data/graphs/relations/edges`:

   ```json
   {"source":"<first-id>","label":"related","target":"<second-id>","props":{"reason":"same guide"}}
   ```

   This adds or replaces one directed edge and returns `data: {"written":true}`.
   The edge endpoint also supports general string node ids for other graph uses;
   GraphRAG follows canonical passage ids only. Edge properties are stored but
   are not included in retrieval context or provenance.

## Retrieve context

`POST /api/graphrag/query` accepts:

```json
{
  "vector_collection":"knowledge",
  "graph":"relations",
  "query_vector":[1,0],
  "seed_k":1,
  "max_depth":1,
  "labels":["related"]
}
```

The successful envelope contains:

```json
{
  "ok":true,
  "data":{
    "context":"[1] MultiDB stores document embeddings.\n\n[2] A graph edge connects related passages.",
    "sources":[
      {"document_id":"<first-id>","text":"MultiDB stores document embeddings.","source":"guide/vectors","seed_distance":0,"depth":0,"seed_id":"<first-id>"},
      {"document_id":"<second-id>","text":"A graph edge connects related passages.","source":"guide/graphs","seed_distance":0,"depth":1,"seed_id":"<first-id>"}
    ],
    "relationships":[{"source":"<first-id>","label":"related","target":"<second-id>"}],
    "truncated":false
  }
}
```

The placeholders above stand for real ids; they are not valid graph ids.
Citation `[n]` corresponds to `sources[n-1]`. `seed_distance` is the vector
distance of the originating seed, not a newly computed similarity for a linked
passage. `depth` is the number of graph hops from that seed. The source location
may be `null` when metadata has no string at the selected source path.

Seeds are ordered by distance and document id. Expansion is breadth-first over
sorted labels and storage-key ordered edges, with cycle detection and document
deduplication. Relationships refer only to returned, visible passages. An empty
result is successful and has empty context, sources and relationships.

## Limits and metadata paths

| Field | Default | Allowed values |
| --- | --- | --- |
| `query_vector` | required | 1–16384 finite numbers; dimension must match the collection |
| `seed_k` | 8 | 1–64, no greater than `max_nodes` |
| `max_depth` | 2 | 0–8; zero retrieves vector seeds only |
| `max_nodes` | 64 | 1–1024 distinct metadata lookups, including hidden/missing nodes |
| `max_edges` | 256 | 1–10000 examined edge records, including duplicates/cycles/invalid ids |
| `max_context_bytes` | 16384 | 1–1048576 bytes of UTF-8 context, including citation separators |
| `labels` | `["related"]` | 1–16 nonempty labels, at most 128 bytes each |
| `text_field` | `["text"]` | A nonempty path of up to 16 nonempty segments, at most 128 bytes per segment |
| `source_field` | `["source"]` | Same path limits; for example `["provenance","url"]` |

Resource names are nonempty and at most 256 bytes. Names, labels and path
segments reject control characters. Unknown request fields are rejected.

Vector candidate overfetch is bounded by `max_nodes`; candidates hidden by a
policy or without usable text can leave fewer than `seed_k` seeds. GraphRAG uses
bounded approximate HNSW retrieval for indexes larger than the candidate budget
and does not fall back to an exhaustive vector scan for those indexes. When the
entire indexed set, including tombstones, fits within `max_nodes`, GraphRAG
searches that bounded in-memory set exactly. Existing index loading/rebuilding
and the index's internal search work are separate from the metadata/edge budgets.

When a budget stops retrieval, `truncated` is true. Edge truncation is
conservative when a scan exactly fills its budget. Context clipping preserves
UTF-8 and clips the returned source text to the same text used in the citation.
Each source location is separately capped at 1024 bytes. The byte limit covers
`context`, not JSON escaping or the entire response envelope. Larger limits
increase work and response size; they do not guarantee more relevant evidence.

## Authorization and consistency

- Querying needs `Read` on both `VectorCollection("knowledge")` and
  `Graph("relations")`. Both checks run before resource lookup. Creating
  resources uses existing builder permissions; adding an edge needs graph Write.
- Vector row policies inspect stored metadata. Masking then changes the metadata
  used for text and source extraction. Hidden, missing or textless documents
  are traversal barriers, so a hidden intermediate cannot reveal its neighbors.
- Graph edge row/masking policies currently cause GraphRAG to fail closed with
  a policy error. There is no inferred policy mapping from arbitrary edge
  properties onto passage fields.
- Rust applications call `Database::graph_rag_as` for principals. The trusted
  `Database::graph_rag` and low-level `graphrag::retrieve` do not authenticate a
  caller; the low-level function accepts an explicit visibility callback.
- GraphRAG shares the SQL concurrency governor. A synchronous retrieval refuses
  saturation immediately with HTTP 503 `busy` and releases its slot on success
  or failure.
- The preview requires native bounded scans. Embedded/single-node databases are
  supported; AP and sharded handles are rejected before retrieval. A retrieval
  observes live state across several reads and is not a cross-model snapshot.
  Concurrent changes may remove passages between vector search and expansion.

HTTP errors use the usual envelope: 400 for invalid input, 401 for a missing
or invalid token, 403 for authorization/policy refusal, 404 for a missing
resource, 422 `graphrag_unsupported` for an unsupported backend, and 503 `busy`
for a saturated query governor. Storage and
index read errors fail the request instead of returning partial successful data.

The exact request and response schemas are in
[`openapi/control-plane-v1.openapi.json`](openapi/control-plane-v1.openapi.json).
