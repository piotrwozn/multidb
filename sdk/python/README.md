# multidb-client

Synchronous Python client for the MultiDB Control Plane API v1.

## Quick start

```python
from multidb_client import ControlPlaneClient

client = ControlPlaneClient()
session = client.login("admin", "local-dev-admin-password")
authed = client.with_token(session["token"])
print(authed.status())
authed.logout()
```

The default base URL is `http://127.0.0.1:8080/api`.

Override it when the control plane is running elsewhere:

```python
from multidb_client import ControlPlaneClient

client = ControlPlaneClient(base_url="http://localhost:8080/api")
```

## Common operations

Create a table, insert a row, and run SQL:

```python
table = "sdk_users"
authed.create_table(
    {
        "name": table,
        "schema": {
            "columns": [
                {"name": "id", "ty": "Int", "nullable": False},
                {"name": "name", "ty": "Str", "nullable": False},
            ],
            "primary_key": 0,
        },
        "indexes": [],
    }
)
authed.insert_table_row(table, [1, "Ada"])
print(authed.sql(f"SELECT * FROM {table}"))
```

Create and query a document collection:

```python
collection = "sdk_docs"
authed.create_collection(
    {
        "name": collection,
        "fields": [{"name": "name", "source": {"Path": ["name"]}, "ty": "Str"}],
        "indexes": [],
    }
)
authed.create_document(collection, {"name": "Ada"})
print(authed.documents(collection))
```

Create a vector collection and search it:

```python
vectors = "sdk_vectors"
authed.create_vector({"name": vectors, "dim": 3})
authed.insert_vector(vectors, {"label": "Ada"}, [1.0, 0.0, 0.0])
print(authed.search_vector(vectors, [1.0, 0.0, 0.0], 1))
```

## GraphRAG retrieval

Create a vector collection with passage metadata, link the returned document
ids in a graph, and retrieve context. Use the authenticated `authed` client
above and fresh collection/graph names:

```python
authed.create_vector({"name": "sdk_chunks", "dim": 3})
first = authed.insert_vector(
    "sdk_chunks",
    {"text": "MultiDB stores vectors with document metadata.", "source": "guide/vectors"},
    [1.0, 0.0, 0.0],
)
second = authed.insert_vector(
    "sdk_chunks",
    {"text": "Graph edges connect related passages.", "source": "guide/graphs"},
    [0.8, 0.2, 0.0],
)
authed.create_graph({"name": "sdk_knowledge", "graph_id": 41})
authed.add_graph_edge("sdk_knowledge", {
    "source": first["id"],
    "label": "related",
    "target": second["id"],
    "props": {"reason": "same guide"},
})
result = authed.graph_rag_query({
    "vector_collection": "sdk_chunks",
    "graph": "sdk_knowledge",
    "query_vector": [1.0, 0.0, 0.0],
    "seed_k": 1,
    "max_depth": 1,
})
print(result["context"])
print(result["sources"], result["relationships"], result["truncated"])
```

Graph nodes use the lowercase 32-character hexadecimal `id` returned by
`insert_vector`. Passage text and provenance come from vector metadata fields
`text` and `source`; `text_field` and `source_field` can select nested paths.
Each source contains `document_id`, `text`, nullable `source`,
`seed_distance`, `depth` and `seed_id`. Relationships contain `source`,
`label` and `target`; `truncated` reports that a retrieval limit was reached.

The server defaults are `seed_k=8`, `max_depth=2`, `max_nodes=64`,
`max_edges=256`, `max_context_bytes=16384`, `labels=["related"]`,
`text_field=["text"]` and `source_field=["source"]`. Omitted options are
left to the server. Retrieval assembles context locally; it does not generate
embeddings or call an LLM. The caller needs read access to the vector
collection and graph; adding edges needs write access to the graph.

The exported `GraphRagRequest`, `GraphRagResult`, `GraphRagSource`,
`GraphRagRelationship`, `GraphEdgeRequest` and `GraphEdgeWriteResult` are
`TypedDict` types and work with ordinary dictionaries. Server validation and
authorization errors raise `ControlPlaneError`, preserving `status`, `code`
and the original response `body`.

See the runnable end-to-end example at
[`examples/sdk/python/example.py`](../../examples/sdk/python/example.py).

## Running tests

From the repository root:

```bash
PYTHONPATH=sdk/python/src python3 -m unittest discover sdk/python/tests
```
