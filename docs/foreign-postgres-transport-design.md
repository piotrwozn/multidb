# PostgreSQL foreign transport

PostgreSQL and MultiDB pgwire foreign sources use a private guarded transport.
The serialized `ForeignSource`, secret references, and SDK requests are unchanged.

Set `MULTIDB_FOREIGN_PG_CA_CERT_PATH` to a trusted PEM CA bundle to enable
certificate-chain and DNS/IP SAN verification. With this bundle, TCP connections
using `sslmode=prefer` or `sslmode=require` require verified TLS and never fall
back to plaintext. `sslmode=require` without a CA bundle fails explicitly.
An unknown CA, expired certificate, wrong name, or server refusing TLS causes
connection failure. The implementation selects rustls's ring provider explicitly,
uses its safe TLS protocol defaults, and does not bypass certificate verification.
The client advertises PostgreSQL ALPN and disables TLS resumption.

Explicit `sslmode=disable` and Unix sockets remain available. Without a CA
bundle, `sslmode=prefer` retains the earlier plaintext behavior. Unix sockets do
not use TLS; requiring TLS or channel binding on them fails explicitly.
`channel_binding=require` is preserved by delegating the verified stream's
channel binding to tokio-postgres. It cannot be satisfied by plaintext or trust
authentication. Direct TLS negotiation requires the verified TCP path.

Host and hostaddr lists, per-host ports, DNS/IP failover, random host ordering,
connect timeout, socket keepalive, supported platform TCP options, startup options,
and `target_session_attrs` remain supported. Invalid list cardinality or unknown
connection modes fail explicitly. A CA bundle is limited to 1 MiB and a secret
connection string to 64 KiB. Neither its contents nor peer error text are placed
in public errors or connection logs.

After TLS decryption, the transport stages each five-byte PostgreSQL backend
header **before** exposing it to the client's codec. Negative/truncated lengths
are rejected. A frame is limited to 8 MiB and may have a smaller limit derived
from the statement's available shared memory. The connection holds a reservation
covering the codec's in-progress buffer, bounded driver response backlog, and
decoded row overhead; only one frame is exposed per read. Startup and control
messages have an additional cumulative 1 MiB wire limit and their retained
metadata is charged to the statement. These checks also apply to plaintext.

SCRAM server-first challenges are staged in an 8 KiB authentication buffer.
Their iteration count must be a single positive integer no greater than
1,000,000. This prevents a small malicious challenge from asking the synchronous
client PBKDF2 loop to perform billions of iterations. Normal 4,096-iteration and
stronger 600,000-iteration challenges fit this limit.

Shared cancellation and deadlines cover DNS, connection attempts, TLS,
authentication, and subsequent row reads. Oversized frames fail with a typed
resource-limit error; they are never silently truncated. LIMIT 0 validates the
requested column indexes without resolving a secret or opening a socket. Dropping
the scan aborts its owned connection task; the stream retains its wire reservation
until that task is actually dropped.

Tests cover split and invalid headers, an advertised oversized frame without a
payload, authentication work limits, repeated control messages, a paused consumer,
ordinary plaintext and Unix rows, verified DNS/IP TLS, a wrong SAN and untrusted
CA, TLS refusal without downgrade, required channel binding, cancellation and
reservation release, and a password sentinel in malformed configuration and
hostile peer error text. Their execution belongs to the repository's shared test
run; the existence of these tests is not an independent security certification.

Remote scans fetch only the projected columns and columns needed by local
residual filters; duplicate projections are applied once after reconstruction.
An empty projection uses `SELECT 1` and preserves row count. Values use typed
bind parameters, including binary BYTEA, and null equality uses `IS NULL`.
Text ordering stays local because PostgreSQL collation may differ from Rust
ordering. Text equality and floating-point ordering retain a residual check;
LIMIT is applied after that check.

For guarded SQL without an explicitly pushed LIMIT, `remote_scan_limit` is a
completeness boundary: the scanner requests/checks one additional matching row
and returns a typed resource-limit error if the source would be truncated.
It never supplies a partial relation as a successful COUNT or filtered result.
A complete source of exactly the configured size succeeds. Legacy unguarded
source APIs retain their explicit capped-result contract.
