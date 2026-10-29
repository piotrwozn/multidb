# Internal transport identity and certificate rotation

The production internal transport requires mutual TLS. `insecure-transport`
exposes `PlaintextForTests` explicitly for isolated test deployments; the normal
build has no plaintext transport variant and never retries TLS over plaintext.

Each node needs its own leaf certificate and private key, a trusted CA bundle,
and an explicit admission map. A trusted CA signature alone does not admit a
peer. Each map entry binds one nonzero `NodeId` to an exact leaf certificate and
the DNS name or IP address in that certificate's SAN. A leaf certificate cannot
be assigned to two node IDs. Private keys are never distributed as peer pins.

For example, the TLS portion of node 1's configuration is:

```json
{
  "cert_path": "/etc/multidb/node-1.pem",
  "key_path": "/etc/multidb/node-1.key",
  "ca_cert_path": "/etc/multidb/cluster-ca.pem",
  "local_node": 1,
  "peers": {
    "2": {
      "server_name": "node-2.db.example",
      "cert_path": "/etc/multidb/peers/node-2.pem"
    },
    "3": {
      "server_name": "node-3.db.example",
      "cert_path": "/etc/multidb/peers/node-3.pem"
    }
  },
  "crl_paths": ["/etc/multidb/cluster.crl.pem"]
}
```

This is `phase30::InternalTlsConfig`, used with
`InternalTransportSecurity::Mtls` and `InternalTransportConfig::new`. Each peer
entry is `InternalPeerTlsConfig`. The transport's separate address map locates a
node; `server_name` verifies its identity even when connecting by IP. Certificates
used in both directions need server-auth and client-auth usage. The local node
ID must match the cluster constructor's node ID. CA bundles can include roots
and intermediates; `cert_path` starts with the node's leaf, followed by its chain
when needed. Peer pin files start with the expected leaf certificate.

Both sides require the `multidb-internal/1` ALPN and verify the certificate
chain, certificate usage, leaf pin and configured SAN. The receiving side checks
claimed request senders against the certified node before dispatch. The CP
endpoint also checks the candidate/leader and term inside the OpenRaft payload.
AP repair may forward versions originating at other nodes, so a version's origin
is not confused with the authenticated connection sender. All admitted nodes
are trusted cluster participants; this admission list is not a tenant or SQL
authorization boundary.

Rustls's safe TLS 1.2 and TLS 1.3 suites remain enabled. TLS 1.3 early data is
disabled, and session resumption is disabled so new connections perform current
certificate and CRL checks. TLS protects channel records against replay; this
does not make retries of application operations exactly once. Raft terms and AP
version clocks continue to determine protocol outcomes.

Responses have a write-and-shutdown deadline, including when the peer stops
reading. Wire encoding stops at the configured frame cap instead of allocating
the entire encoded response first. Native AP full-version and Merkle RPCs visit
records incrementally and reject a response exceeding the wire cap or the
64 MiB conservative decoded-allocation budget; they never silently truncate a
full-result request. Bounded range pages are available for larger datasets.
The limits cover retained response data and wire buffers, not total process RSS
or the materialization of a single stored value. Custom Rust endpoint
implementations must override the visitor methods to obtain native streaming
behavior; their default visitors preserve the legacy eager APIs. Synchronous
endpoint execution is awaited while holding the connection permit; timing out
network I/O is not presented as cancellation of an already running storage
operation.

Guarded AP point, multi-key and range reads share the query's `ReadBudget` with
their shard merge heads, decoded siblings and network frames. A frame length is
checked against the remaining budget before allocating its wire buffer; an
allocation-free postcard pass checks container counts and reserves decoded
storage before serde runs. A returned page keeps that reservation until its
records are dropped. The peer receives explicit wire and decoded caps and
rejects an oversized entry rather than returning a shortened successful page.
Stored JSON siblings also undergo a typed allocation preflight before decoding.
Custom backends retain their legacy APIs and advertise guarded support only
when their borrowed/guarded methods implement this contract.

For a strong AP range over `N` nodes with replication factor `RF` and the point
read quorum `R`, at least `N - RF + R` nodes must respond. This ensures every
possible `RF` replica set has at least `R` responders. Merge and repair consider
only each key's actual replica set. A range can therefore require more live
nodes than a point read when `N > RF`; returning a partial range as a successful
strong read would lose this guarantee. There is no cross-node snapshot or
serializable-range claim. Point reads still contact the key's preference list
and keep the existing consistency, sibling and tombstone behavior.

## Migrating the old configuration

Replace the old single `server_name` with `local_node` and a `peers` map covering
the peers the node must admit or contact. Give every node its own certificate,
copy only each peer's public certificate to its pin path, and assign the correct
SAN name to each entry. A shared legacy node certificate must be reissued as
distinct node certificates. The old format, absent pins, unknown peers and
identity mismatches fail closed; there is no compatibility downgrade to
CA-only authentication. `crl_paths` can be omitted when no CRLs are configured.

## Rotation and revocation

Certificates, admission maps and CRLs are loaded when a client/server or cluster
handle is constructed; changing files does not update an existing handle.
Recreate the affected handles or restart the processes to apply changes. Keep
private-key files readable only by the node's service identity. Peer certificate
files contain public material and do not require another node's private key.

An exact leaf pin changes on certificate renewal, including renewal with the
same key. There is one admitted leaf per node, so coordinate peer-pin updates
with the node's certificate replacement. Update and restart voters one at a
time while retaining a mutually admitted majority; plan for the rotating node
to be temporarily isolated and for leader reelection. This API does not provide
automatic reload or overlapping old/new pins. Back up the prior configuration
for a deliberate rollback rather than disabling certificate verification.

To revoke a leaf, remove its admission entry or install an issuer-signed PEM CRL
and recreate the relevant handles. When CRLs are configured, missing chain
revocation status, invalid CRLs and expired CRLs fail closed. CRLs do not revoke
a trust anchor: remove a compromised CA from the CA bundle, replace affected
node certificates and pins, and recreate handles. Revocation is not pushed to
already running handles; coordinate process termination when immediate
connection revocation is required.

## Qualification

`internal_transport::mtls_tests` generates disposable fixture keys at test time
and exercises real handshakes, including wrong pins, sender impersonation, SAN
mismatches in both directions, CA removal, CRL revocation, missing ALPN and
plaintext downgrade attempts. No production credentials are checked in.

`scripts/cluster-smoke.ps1` also runs
`mtls_three_process_failover_and_quorum_smoke`: three separate processes use
durable redb storage and real mTLS connections, then lose a leader, lose quorum,
restart two members and verify acknowledged data. The isolated minority must reject writes
and strong reads. A timed-out write can still commit after quorum returns; the
test does not claim a timeout guarantees rollback. The test runs on loopback;
it is not evidence of separate-host, WAN, firewall-partition or adversarial
network qualification.
