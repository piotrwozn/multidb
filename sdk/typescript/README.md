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
