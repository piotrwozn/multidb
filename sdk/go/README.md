# MultiDB Go Client

Context-aware Go client for the MultiDB Control Plane API v1.

```go
client := multidb.NewClient()
session, err := client.Login(ctx, "admin", "local-dev-admin-password")
if err != nil {
    panic(err)
}
authed := client.WithToken(session.Token)
status, err := authed.Status(ctx)
```

The default base URL is `http://127.0.0.1:8080/api`.
