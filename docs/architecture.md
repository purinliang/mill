# Architecture

Mill turns one trusted container image and one JSONL input into durable,
independent tasks. PostgreSQL stores control-plane state, object storage holds
data, and Kubernetes schedules containers. Planned behavior is identified
explicitly.

## System view

```text
User --REST--> Job service -------------> PostgreSQL
                  |
                  +--> partition input --> logical byte ranges
                  |
                  `-- gRPC <---------- Execution replicas
                                             |
                                             v
                                      Kubernetes API
                                             |
                                      Job --> workload Pod
                                             |
                                      ranged input/output
                                             |
                                      file:// or s3://
```

The Job service owns submission, partitioning, state transitions, progress,
and all PostgreSQL access. Execution replicas claim attempts through gRPC,
create Kubernetes Jobs, and report observations. They never access Mill's
database directly. Kubernetes owns placement and container lifecycle; Mill
does not implement a cluster scheduler.

## Domain model

- A **job** applies one executable to one immutable input.
- A **logical shard** is a complete-record byte range in that input.
- A **task** is the durable obligation to process one job shard. The pair
  `(job_id, shard_index)` is unique.
- An **attempt** is one execution generation of a task. Retries create new
  attempts while preserving previous terminal history.
- An **executable** is a trusted OCI image and unchanged user arguments.
- A **result** is the output URI of a successful attempt. Its bytes remain in
  object storage rather than PostgreSQL.

Task input and output URIs are derived from their owning job and attempt. A
retry writes to a new attempt path, so it cannot overwrite an earlier result.

## Submission and partitioning

`POST /jobs` requires an idempotency key, executable, and input URI. Repeating
the same normalized request returns the original job; changing the request
under the same key conflicts.

The current partitioner scans JSONL twice. The first pass validates records,
counts them, and computes a SHA-256 identity. The second pass chooses complete
record boundaries without loading the dataset into memory. The input must not
change after submission.

```text
target tasks = min(records, parallelism * 4, 10,000)
records per task = ceil(records / target tasks)
```

Parallelism is server policy captured on the job. The number of tasks may be
larger so short and long shards can share the available execution slots.

## Execution and recovery

PostgreSQL and Kubernetes cannot share one transaction. Mill therefore stores
intent first and makes external operations discoverable and repeatable:

1. Claim an eligible task under the job's parallelism limit.
2. Create a starting attempt and lease in one database transaction.
3. Create or find the deterministic Kubernetes Job for that attempt.
4. Record its Kubernetes UID and observe it until terminal.
5. Complete or fail the attempt and update task and job progress atomically.

Kubernetes-native retries are disabled. Mill permits at most three attempts
per task with a durable five-second retry delay. A failed attempt output is
never returned as a successful task result.

An execution replica owns an attempt through a 15-second lease and fencing
token. After expiry, another replica may receive a new token for the same
attempt and Kubernetes Job. State changes require the current token, preventing
a stale replica from completing work after takeover.

## Interfaces

The public HTTP boundary provides job submission, status, liveness, and
database-backed readiness. The internal Protobuf/gRPC boundary provides task
claims, leases, attempt transitions, and Kubernetes identity recording.

Each workload attempt receives the following CLI contract before the user's
original arguments:

```text
--job-id, --task-id, --shard-index
--input-uri, --input-start-byte, --input-end-byte
--output-uri -- <user arguments>
```

Workloads read only their assigned range and exit zero only after publishing a
complete result. `internal/objectstore` provides streaming and ranged access
for `file://` and `s3://` URIs; it is a library, not another service.

## Resource policy

Users select a server-defined class rather than raw Kubernetes quantities:

| Class | Memory request and limit | CPU request and limit |
| --- | ---: | ---: |
| `small` | 128 MiB | `100m` / `1` |
| `medium` | 512 MiB | `100m` / `1` |
| `large` | 2 GiB | `100m` / `1` |

Resolved values are stored with the job so a later configuration change does
not alter its retries.

## Availability boundary

Implemented local demonstrations survive one Job or execution Pod loss while
the kind node, PostgreSQL, network, and object store remain healthy. Durable
leases preserve attempt and Kubernetes Job identity during execution takeover.
This is Pod-level recovery, not infrastructure high availability.

The planned three-node deployment uses three K3s/etcd voters, three
CloudNativePG instances, and AWS S3. A two-node majority must continue after
one node fails, while an isolated minority must refuse authoritative writes.
See the [AWS deployment](deployment/aws.md) and
[availability](deployment/availability.md).

## Non-goals

V1 does not include untrusted workload isolation, multi-tenancy, arbitrary
input formats, generic aggregation, streaming pipelines, a custom scheduler,
cross-region recovery, or production availability claims. Kafka, Redis, and a
service mesh require a concrete future problem before introduction.

## Code structure

```text
cmd/                    service composition roots
api/proto/              internal RPC schema
deploy/                 Kubernetes definitions
examples/               trusted demonstration workloads
internal/job/           submission, state, and adapters
internal/execution/     leases and Kubernetes reconciliation
internal/objectstore/   file and S3 access
internal/workload/      workload CLI contract
migrations/             PostgreSQL schema history
scripts/                executable development operations
test/integration/       cross-boundary tests
```

Package ownership and dependencies are documented in the nearest package
README. Installation, tests, and operational commands are intentionally kept
out of this architecture document. See [Setup](development/setup.md),
[Testing](development/testing.md), and [Deployment](deployment/local.md).
