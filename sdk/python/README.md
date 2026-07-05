# multidb-client

Synchronous Python client for the MultiDB Control Plane API v1.

```python
from multidb_client import ControlPlaneClient

client = ControlPlaneClient()
session = client.login("admin", "local-dev-admin-password")
authed = client.with_token(session["token"])
print(authed.status())
authed.logout()
```

The default base URL is `http://127.0.0.1:8080/api`.
