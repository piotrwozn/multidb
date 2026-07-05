import os
import time

from multidb_client import ControlPlaneClient


def main() -> None:
    base_url = os.environ.get("MULTIDB_CONTROL_PLANE_URL", "http://127.0.0.1:8080/api")
    password = os.environ.get("MULTIDB_ADMIN_PASSWORD", "local-dev-admin-password")
    stamp = f"py_{int(time.time() * 1000)}"

    client = ControlPlaneClient(base_url=base_url)
    session = client.login("admin", password)
    db = client.with_token(session["token"])

    try:
        table = f"sdk_users_{stamp}"
        db.create_table(
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
        db.insert_table_row(table, [1, "Ada"])
        db.sql(f"SELECT * FROM {table}")

        collection = f"sdk_docs_{stamp}"
        db.create_collection(
            {
                "name": collection,
                "fields": [{"name": "name", "source": {"Path": ["name"]}, "ty": "Str"}],
                "indexes": [],
            }
        )
        db.create_document(collection, {"name": "Ada"})

        vectors = f"sdk_vectors_{stamp}"
        db.create_vector({"name": vectors, "dim": 3})
        db.insert_vector(vectors, {"label": "Ada"}, [1.0, 0.0, 0.0])
        db.search_vector(vectors, [1.0, 0.0, 0.0], 1)

        series = f"sdk_series_{stamp}"
        db.create_time_series({"name": series, "chunk_millis": 60000, "retention_millis": None})
        now = int(time.time() * 1000)
        db.insert_time_series_point(series, "default", {"timestamp_millis": now, "value": 42.0})
        db.time_series_points(series, "default", now - 1, now + 1)
        print("Python SDK example completed")
    finally:
        db.logout()


if __name__ == "__main__":
    main()
