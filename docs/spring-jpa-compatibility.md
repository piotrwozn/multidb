# Spring JPA Compatibility Boundary

Spring JPA support is a client compatibility smoke layer over MultiDB's
PostgreSQL wire surface. It is not a Rust rewrite, a second persistence model,
or a Java-side storage contract.

## Target Contract

- Spring Boot connects through JDBC to the PostgreSQL wire endpoint.
- Hibernate/JPA may run a small smoke entity lifecycle: connect, create or use a
  simple table, insert, query by id, update, delete, and rollback.
- Pass/fail criteria live at the wire/client boundary: authentication, SQLSTATE
  mapping, prepared statements, basic catalog introspection, and transaction
  behavior.
- Multi-model APIs, storage engines, replication, and catalog ownership remain
  in the Rust core.

## Non-Goals

- No Java storage adapter.
- No parallel ORM metadata catalog inside MultiDB.
- No JPA-specific durable format.
- No Docker dependency for proving the compatibility contract; Docker can wrap
  the smoke test later after the binary/runtime contract is stable.

## Suggested Future Smoke

Add a minimal Spring Boot test app only after the PostgreSQL wire endpoint,
runtime initialization, and source baseline are stable. The app should be small
enough to run as a compatibility check, not as a product surface.
