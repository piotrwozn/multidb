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

See the runnable end-to-end example at
[`examples/sdk/python/example.py`](../../examples/sdk/python/example.py).

## Running tests

From the repository root:

```bash
PYTHONPATH=sdk/python/src python3 -m unittest discover sdk/python/tests
```
