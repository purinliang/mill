# Mill architecture

This document describes Mill's implemented architecture and the deliberately
staged service and availability design. Anything marked **planned** is not a
current capability.

## Responsibilities

Mill owns the batch abstraction: job intent, logical tasks, attempts, retry
policy, reconciliation, progress, and result locations. PostgreSQL owns durable
metadata, S3 or local files hold data, and Kubernetes owns placement and
container lifecycle. Mill does not implement a cluster scheduler, database
election, or arbitrary workload aggregation.

The current deployment is one Go process containing the HTTP API, job service,
streaming planner, and optional coordinator. The execution domain and gRPC
adapter now establish a tested code boundary, but they are not yet separately
deployed services.

```text
User
  |
  | POST /jobs or GET /jobs/{id}
  v
Mill process
  |
  +--> JSONL planner --> input object --> logical ranges
  |
  +--> PostgreSQL --> jobs, tasks, attempts
  |
  `--> coordinator --> Kubernetes API --> Job --> Pod
                                                 |
                                                 +--> ranged input
                                                 `--> unique output
                                                       file:// or s3://
```

## Domain model

- **Job** is one request to apply an executable to an entire input. It owns the
  input identity, output root, chosen parallelism, tasks, and aggregate state.
- **Input** is one JSONL object. Each line is an independent record.
- **Logical shard** is a contiguous half-open byte range `[start, end)` aligned
  to complete JSONL records. Mill stores offsets rather than copied shard files.
- **Task** is the durable obligation to process one shard for one job. The pair
  `(job_id, shard_index)` is unique.
- **Attempt** is one execution generation of a task. A retry creates another
  attempt and preserves earlier terminal history.
- **Executable** is a trusted OCI image plus its unchanged user arguments.
- **Result** is the output of one successful attempt. PostgreSQL stores its URI;
  the bytes remain in file or object storage.

The implemented records are:

```text
Job
  id                    UUIDv7
  idempotency key       unique submission identity
  executable            image reference and arguments
  input                 URI, SHA-256, and record count
  output root           configured root / jobs / job ID
  parallelism           server value captured at submission
  state and task count  durable progress metadata

Task
  id                    UUIDv7
  job ID                owning job
  shard index           zero-based position
  input range           [start byte, end byte)
  state                 pending/running/completed/failed
  available_at          earliest retry claim time

Attempt
  id                    UUIDv7
  task ID               logical task
  attempt number        increasing within that task
  executor              execution backend
  state                 starting/running/completed/failed
  external ID           Kubernetes Job UID after creation
  lease                 owner, fencing token, and expiry
  lifecycle             timestamps and optional failure
```

Input and output URIs are not repeated on tasks. A task reads its job input.
Each attempt output is derived as
`tasks/<shard-index>/attempts/<attempt-id>/result.jsonl`, preventing retries
from overwriting each other.

## Submission and planning

The implemented request is intentionally small:

```json
{
  "executable": {
    "image": "mill/word-count:dev",
    "args": []
  },
  "input": {
    "uri": "s3://mill-input/records.jsonl"
  }
}
```

`Idempotency-Key` is required. Replaying the same key and normalized submission
returns the original job; changing the submission conflicts. JSONL is currently
the only format. Shard size and parallelism are server policy rather than
request fields.

The planner streams the input twice without retaining the dataset or every line
offset in memory. The first pass validates JSONL, counts records, and calculates
SHA-256. The second chooses complete-record boundaries. The input must remain
immutable after submission; S3 version IDs and runtime checksum enforcement are
not implemented.

The current heuristic targets four waves of work:

```text
target tasks = min(record count, parallelism * 4, 10,000)
records per task = ceil(record count / target tasks)
actual tasks = ceil(record count / records per task)
```

Each record is limited to 16 MiB. A 100-record input at parallelism three
usually produces 12 tasks, while only three attempts may be active. For S3,
two complete planning reads are intentionally accepted in this prototype;
metadata-assisted or one-pass planning is deferred until measurements justify
the added complexity.

## Workload contract

Every task attempt receives Mill-owned CLI flags followed by a mandatory `--`
separator and the executable's original arguments:

```text
--job-id <job-id>
--task-id <task-id>
--shard-index <zero-based-index>
--input-uri <absolute-file-or-s3-uri>
--input-start-byte <inclusive-offset>
--input-end-byte <exclusive-offset>
--output-uri <absolute-file-or-s3-uri>
--
<user executable arguments>
```

A conforming workload reads only its assigned range, treats the input as
immutable, publishes only its assigned output, and exits zero only after the
complete result is visible. The contract does not use environment variables
for per-attempt arguments. Storage SDK configuration and credentials may be
provided through the Pod environment because they describe the runtime, not
the individual invocation.

## Storage boundary

`internal/objectstore` implements a URI-oriented adapter using local files and
the AWS SDK for Go v2:

- whole-object reads support streaming planning;
- ranged reads translate `[start, end)` into an S3 HTTP byte range;
- complete outputs are published through atomic local rename or S3 PutObject;
- a custom endpoint and path-style addressing support local S3-compatible
  services; and
- an empty S3 configuration retains file-only operation.

For local S3 tasks, the Kubernetes adapter injects region and endpoint values
and may reference a Kubernetes Secret containing AWS credential environment
variables. Real AWS should prefer workload identity over copying control-plane
credentials. Mill assumes trusted workloads in V1, but credentials should
still be limited to the required input/output namespaces.

The storage abstraction is not a network service. It is a small client library
used by the planner and reference workload.

## Durable execution and reconciliation

PostgreSQL and Kubernetes cannot share one transaction. Mill therefore persists
intent first and treats every external call as retryable or ambiguous:

1. Claim a pending task under its job's parallelism limit.
2. Create a `starting` attempt and mark the task running in one transaction.
3. Create or discover the deterministic Kubernetes Job for that attempt.
4. Record its UID and transition the attempt to `running`.
5. Observe only terminal Kubernetes Job conditions.
6. Atomically finish the attempt, task, and, where applicable, job.

Kubernetes-native retries are disabled with `backoffLimit: 0` and
`restartPolicy: Never`; Mill owns the retry history. A terminal failure waits
five durable seconds before a retry becomes eligible. A task receives at most
three attempts. Waiting retries consume no active slot.

API timeouts and missing running Jobs are ambiguous observations, not proof that
execution did not occur. Stable Job names and recorded UIDs allow the
coordinator to rediscover the same execution after its process restarts. Mill
does not silently launch a replacement for a missing running Job.

Each process uses a random executor instance identity. A newly created attempt
receives a 15-second lease, owner, and UUID fencing token in the same transaction
that marks its task running. Every coordinator tick renews leases it owns and
may atomically take over unowned or expired attempts using `FOR UPDATE SKIP
LOCKED`. A takeover creates a new token but preserves the attempt ID and
deterministic Kubernetes Job name. All attempt state mutations require the
current unexpired token, so a stale process cannot finish work after ownership
has moved. Exact terminal-transition replays with the same token remain
idempotent.

This database ownership mechanism is implemented. The process-level restart
demo and the simultaneous two-process failover demo exercise expiry, takeover,
and stable external identity. A packaged Kubernetes multi-replica deployment
remains planned.

## Service boundary

The approved next distributed-service shape has only two long-lived Mill
services:

```text
Client --REST--> Job service replicas --PostgreSQL--> metadata
                         ^
                         |
                  Protobuf/gRPC
                         |
                 Executor replicas --Kubernetes API--> workload Jobs
```

The **Job service** owns the public API, job state machine, planner, and all
metadata database access. The **executor service** owns Kubernetes creation and
observation and never accesses Mill tables directly. A separate planner service
is unjustified while planning is a bounded streaming operation inside the Job
workflow.

`internal/execution` now owns the backend-independent attempt model and the
store contract consumed by the coordinator and Kubernetes adapter. HTTP
submission, JSONL planning, and PostgreSQL implementation remain in
`internal/job`; file count was not by itself a reason to split them.

The versioned gRPC API is defined in
`api/proto/mill/execution/v1/execution.proto`. It exposes the implemented lease
domain operations rather than database CRUD:

- lease active attempts owned by an executor replica;
- claim the next eligible attempt;
- record a Kubernetes external identity;
- complete an attempt; and
- fail an attempt with a bounded reason.

The Job-side adapter owns the 15-second lease policy; it is deliberately absent
from executor requests. Every mutation carries an attempt ID and fencing token.
The client applies a per-call deadline, maps concurrency and state failures back
to domain errors, and treats other transport failures as ambiguous. A `bufconn`
test proves schema conversion, server-owned lease policy, and error mapping
without opening a network port.

The current `cmd/mill` process still connects the coordinator directly to the
PostgreSQL repository through a small adapter. Separate Job and executor
entrypoints, a real gRPC listener, transport credentials, authorization, and a
multi-Pod deployment are still planned. Lease expiry transfers observation
ownership; it does not create a new attempt. An explicit expected version may
be added only if the existing fencing and state guards prove insufficient for
safely retrying an unknown RPC outcome.

## Planned workload resource classes

An optional top-level `resource_class` will select a server-defined profile:

```json
{
  "executable": {"image": "mill/word-count:dev", "args": []},
  "input": {"uri": "s3://mill-input/records.jsonl"},
  "resource_class": "large"
}
```

Omission will mean `small`. The Job service will persist the class and resolved
requests/limits so configuration changes cannot alter a retry. The initial
memory profiles are:

| Class | Memory request and limit |
| --- | ---: |
| `small` | 128 MiB |
| `medium` | 512 MiB |
| `large` | 2 GiB |

Users will not submit raw Kubernetes resource strings. CPU values and operator
configuration will be decided when this milestone is implemented. Current
Kubernetes attempts still use fixed resources.

The Job and executor services should normally request 64 MiB and limit at
128 MiB. Their memory may scale with explicitly bounded concurrent requests,
RPCs, and workers, but not dataset size or historical job count. PostgreSQL is
budgeted separately, initially around a 256 MiB request and 512 MiB limit per
instance. These are planned starting measurements, not capacity guarantees.

## Availability progression

Availability statements name the exact failure being tested.

### Two-laptop replica availability — planned

One laptop runs a K3s server and the other a K3s agent. Two Job-service and two
executor replicas are spread across the nodes. CloudNativePG runs a primary and
standby with synchronous replication set to availability-oriented
`dataDurability: preferred`.

This stage targets one Mill Pod/process failure or one controlled PostgreSQL Pod
failure while the Kubernetes control plane and network remain healthy. If the
standby is unavailable, writes may continue; overlapping failures can therefore
lose recently acknowledged metadata. It does not claim survival of an entire
laptop or an ambiguous network partition.

### Three-node quorum availability — planned

Three independent nodes each run a K3s server/etcd voter and one PostgreSQL
instance. PostgreSQL uses one primary, two standbys, synchronous `ANY 1`,
`dataDurability: required`, and failover quorum. Losing one node leaves two
Kubernetes voters and at least one synchronized database copy.

The minority or uncertain side must stop accepting authoritative writes. Mill
does not implement voting: etcd owns Kubernetes consensus and CloudNativePG
owns PostgreSQL promotion. Full-stack availability also requires an available
S3 service; a single local object-storage container cannot support a whole-node
availability claim.

## Explicitly deferred

- asynchronous planning and preparation recovery;
- raw user-configurable Kubernetes resources;
- arbitrary input formats or partitioning languages;
- generic result aggregation or content validation;
- automatic deletion of historical Jobs and attempt outputs;
- multi-tenant authorization and untrusted-code isolation;
- database or object-store consensus implemented by Mill; and
- production availability, security, or scalability claims.
