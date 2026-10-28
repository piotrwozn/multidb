# Executable qualification and acceptance

Qualification accepts a declared scenario on an exact artifact. It does not
rename a preview feature or approve a production release. The release owner
must reconcile all applicable reports with the supported scope in
[production readiness](../production-readiness.md).

## Local execution

Use Linux, Python 3.10 or newer, an already built default-feature release binary,
and a new private output directory. The runner never builds, downloads, starts
Docker, attaches to an existing server, or opens an operator database. Its HTTP
and pgwire ports are loopback-only. Disposable credentials are public test
values. Keep other load generators and compilers off the host during a run.

```sh
python3 scripts/qualification/qualify.py \
  --binary /absolute/immutable/multidb --revision <source-commit> \
  --profile local-15m --output /absolute/new/qualification-local --plan

python3 scripts/qualification/qualify.py \
  --binary /absolute/immutable/multidb --revision <source-commit> \
  --profile local-15m --output /absolute/new/qualification-local
```

The first command prints a plan without launching anything. The execution
creates `plan.json` before any server work, then `report.json`, request samples,
resource samples, command logs and hashes of all retained evidence. Exit zero
means the required **local scenario** sections were accepted. Exit two means a
failed or blocked scenario; inspect its section statuses. Failures are retained.
Output paths cannot be reused. The runner owns and cleans up only its processes;
its campaign lock is shared with the competitor benchmark driver.

Profiles live in [profiles.json](../../scripts/qualification/profiles.json):

| Profile | Predeclared workload and scope |
| --- | --- |
| `smoke` | 20 seconds, two concurrent workers, 1,000 rows, 256-byte payloads and two slow clients. A short functional/CI exercise, not a production soak. |
| `local-15m` | 900 seconds, eight workers, 100,000 rows, 256-byte payloads, 16 slow clients and a maximum of 20 requests/second per worker. At least 72,000 successful measured requests; no unexpected errors. Minimum host capacity is four effective CPUs and 8 GiB RAM; the intended audit host has 16 GiB. |
| `cold` | The local 15-minute profile after stopping the owned server, syncing and advising eviction of its database file, then independently checking residency with Linux `mincore`. At most 5% of the file's pages may remain resident before restart. This describes the fixture's page cache, not device/controller caches. It naturally warms after restart. |
| `larger-than-ram` | One-hour, eight-worker profile with an explicit operator-sized fixture. Logical source payload bytes must exceed **observed** effective physical/cgroup RAM, and backing-read I/O during measured load must be at least that RAM size. Required disk and declared directory ceiling must also cover fixture plus recovery copies. Default fixture parameters deliberately block on the audit host; they do not simulate a larger-than-RAM result. |

The local profile predeclares point-read p95/p99 ceilings of 250/1,000 ms,
write and large-result ceilings of 1,000/2,000 ms, a 2 GiB process RSS ceiling,
256 open descriptors, 1 GiB allocated output-directory disk and at least
512 MiB free disk. The smoke profile allows wider latency limits. Inspect the
machine-readable profile for all limits. Request latencies include client
decoding and the operation's correctness check. They are workload SLOs, not
isolated engine timings. A small p99 sample remains a descriptive observation.

Use `--profile-overrides <json-file>` to declare different values **before**
execution. The file can override existing profile keys; unknown keys, nonfinite
limits, allowed errors, or a relaxed cold-residency threshold are rejected.
Changing a limit after seeing a result requires a fresh campaign and disclosure.
For a real larger-than-RAM exercise, increase `rows`, `payload_bytes`, the disk
ceiling and duration on a suitable host, keeping response batches below the
64 MiB response bound. Source payload size alone, a declared memory budget, or
software GPU emulation cannot qualify the hardware gates. A large fixture that
does not generate the predeclared backing-read pressure remains blocked.

## What the local runner checks

Workers own disjoint key partitions, so an acknowledged write has an independent
last-version oracle without cross-worker response-order ambiguity. It mixes
point reads, full-record upserts, larger result sets and actual SQL authorization
denials. A no-role principal must receive exactly `403 forbidden` without data.
All source rows, scalar types, payloads and final acknowledged key versions are
checked before load, after load and after restoration; logical hashes must agree.

Normal clients reconnect every 50 operations. Separate clients send partial
HTTP headers and pgwire startup packets slowly, and disconnect immediately after
transmitting complete read-only HTTP requests. They never issue uncertain
writes. This exercises cleanup and service responsiveness under bounded slow
connections. It does not qualify authenticated pgwire load, every malicious
wire frame, an Internet-facing proxy, or an unlimited connection attack.

The server explicitly opts into online MVCC GC. An `AS OF LSN 0` request must be
readable before fixture mutations and rejected as retention-expired after the
concurrent workload, before offline GC. Merely setting an environment variable
does not pass the GC section. After a clean shutdown, the runner performs actual
offline GC, full backup, verification and restoration into an absent target,
then checks complete logical contents. Encryption, key recovery and incremental
LSN/time PITR are separate evidence from `examples/backup_recovery_drill.rs`.

The public backup CLI requires exclusive database-file access. Consequently
`concurrent_backup` is explicitly **blocked** in this local scope; the runner
does not copy a live redb file or claim online backup. A deployment that promises
backup under uninterrupted write traffic needs a separately supported and tested
snapshot/backup workflow. Offline backup downtime and RPO/RTO need operator
acceptance.

RSS includes `/proc` high-water marks. Descriptors and allocated directory disk
are sampled; brief descriptor peaks between samples are not certified. Offline
maintenance commands are also sampled and terminated if their resource limits
are crossed. Host free disk, fixture/backup growth, latency, errors and evidence
hashes are retained. Queue-depth telemetry, power loss, disk controller failure,
and a 24-hour intended-environment soak remain separate gates.

## External hardware and production hooks

The standalone [external runner](../../scripts/qualification/external.py)
creates a contract with an **empty** command. An empty command stays `unrun`.
It never turns absence of a physical GPU or a cluster into acceptance.

```sh
python3 scripts/qualification/external.py template \
  --gate physical_gpu --binary /absolute/multidb --revision <source-commit> \
  --output /absolute/new/gpu-plan.json

# Configure a reviewed argv command in gpu-plan.json on the target machine.
python3 scripts/qualification/external.py run \
  --plan /absolute/gpu-plan.json --output /absolute/new/gpu-results
```

Available gates are `physical_gpu`, `cluster` and `production_environment`.
The hook is an explicitly configured argv array and executes without shell
evaluation by this runner. Use environment/secret files for credentials; do not
put secrets in argv or evidence. Treat hook code as trusted operator code.
Commands get the absolute paths in `MULTIDB_QUALIFICATION_PLAN` and
`MULTIDB_QUALIFICATION_REPORT`, plus the criteria hash in
`MULTIDB_QUALIFICATION_CRITERIA_SHA256`.

The command writes a JSON object to `MULTIDB_QUALIFICATION_REPORT` with:

- `schema_version: 1`, the exact `gate`, `artifact_sha256`, `source_revision`
  and `criteria_sha256` copied from the executed contract;
- `status`, UTC `started_at_utc` and `finished_at_utc`, and one `checks` object
  per mandatory contract check, each with its status and supporting details;
- actual `environment` facts and relative `evidence` entries containing `path`
  and `sha256`; raw logs/results must exist inside the new output directory;
- `operator_acceptance` with the actual reviewer, evidence ticket and acceptance
  timestamp after measurement. Leave this empty until a reviewer has accepted
  the evidence. The automated collector cannot provide that review.

GPU evidence must identify physical adapters, vendor/model, driver, API and
device ID. Software adapters are rejected. Required checks cover numerical
references, device loss, deadlines, restart, memory pressure and Docker device
passthrough. Cluster evidence requires at least three distinct hosts with
process and certificate identities, and actual failover/partition/prepared
recovery/durable-decision/unique-ID/identity checks. Production-environment
evidence identifies hardware, storage, filesystem, deployment and SLO owner;
its command must execute for at least 24 hours, with declared load, storage
failure, disk-full, RPO/RTO, upgrade/rollback, alerting and independent security
review evidence. Report timestamps alone cannot satisfy the measured duration.

All mandatory checks, exact artifact hashes and raw-file hashes must match.
Missing, partial, software-only, malformed, unreviewed or traversing/symlink
evidence is blocked. A command failing or timing out fails the hook, and its
owned process group is stopped. Hashes bind bytes; they do not establish that an
external author or asserted reviewer is trustworthy. Release acceptance still
requires the responsible operator to review the actual environment and logs.

Tooling regressions can be run without a database:

```sh
python3 -m unittest discover -s scripts/qualification -p 'test_*.py'
```
