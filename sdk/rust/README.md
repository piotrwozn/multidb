# multidb-client

Blocking Rust client for the MultiDB Control Plane API v1.

```rust
use multidb_client::ControlPlaneClient;

let client = ControlPlaneClient::new();
let session = client.login("admin", "local-dev-admin-password")?;
let authed = client.with_token(session.token);
println!("{:?}", authed.status()?);
authed.logout()?;
# Ok::<(), multidb_client::ControlPlaneError>(())
```

The default base URL is `http://127.0.0.1:8080/api`.
