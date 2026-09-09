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

The implemented runtime boundary has two Go executables. `cmd/mill-job`
contains the HTTP API, Job service, dataset partitioner, PostgreSQL repository,
and optional internal gRPC listener. It does not import the coordinator or
Kubernetes adapter. `cmd/mill-execution` runs the coordinator and Kubernetes
adapter against that API without a PostgreSQL dependency. The full batch and
execution-failover paths have been demonstrated across this boundary. Both
processes are packaged as minimal images and deployed as Kubernetes services
in the local demonstration.

```text
User
  |
  | POST /jobs or GET /jobs/{id}
  v
Job service (`mill-job`)
  |
  +--> dataset partitioner --> input object --> logical ranges
  |
  +--> PostgreSQL --> jobs, tasks, attempts
  |
  `-- gRPC --> execution service replica(s) (`mill-execution`)
                                      |
                                      `--> Kubernetes API --> Job --> Pod
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

## Submission and partitioning

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

The partitioner streams the input twice without retaining the dataset or every
line offset in memory. The first pass validates JSONL, counts records, and
calculates SHA-256. The second chooses complete-record boundaries. The input
must remain immutable after submission; S3 version IDs and runtime checksum
enforcement are not implemented.

The current heuristic targets four waves of work:

```text
target tasks = min(record count, parallelism * 4, 10,000)
records per task = ceil(record count / target tasks)
actual tasks = ceil(record count / records per task)
```

Each record is limited to 16 MiB. A 100-record input at parallelism three
usually produces 12 tasks, while only three attempts may be active. For S3,
two complete partitioning reads are intentionally accepted in this prototype;
metadata-assisted or one-pass partitioning is deferred until measurements
justify the added complexity.

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

- whole-object reads support streaming partitioning;
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
used by the partitioner and reference workload.

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

Each execution process uses a random instance identity. A newly created attempt
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

The implemented distributed-service shape has only two long-lived Mill
services:

```text
Client --REST--> Job service replicas --PostgreSQL--> metadata
                         ^
                         |
                  Protobuf/gRPC
                         |
                Execution replicas --Kubernetes API--> workload Jobs
```

The **Job service** owns the public API, job state machine, partitioner, and all
metadata database access. The **execution service** owns Kubernetes creation and
observation and never accesses Mill tables directly. A separate partition
service is unjustified while partitioning remains a bounded streaming
operation inside the Job workflow.

The package-level ownership and dependency graphs are documented beside the
code in the [Job package](../internal/job/README.md) and
[execution package](../internal/execution/README.md). Those package boundaries
make the two service responsibilities visible without creating more services.

The versioned gRPC API is defined in
`api/proto/mill/execution/v1/execution.proto`. It exposes the implemented lease
domain operations rather than database CRUD:

- lease active attempts owned by an execution replica;
- claim the next eligible attempt;
- record a Kubernetes external identity;
- complete an attempt; and
- fail an attempt with a bounded reason.

The Job-side adapter owns the 15-second lease policy and deliberately omits it
from execution-service requests. Every mutation carries both an attempt ID and
the current fencing token. The client applies a per-call deadline, maps
concurrency and state failures back to domain errors, and treats other
transport failures as ambiguous. A `bufconn` test proves schema conversion,
server-owned lease policy, and error mapping without opening a network port.

The current `cmd/mill-job` process serves the Job-side API when
`MILL_GRPC_ADDR` is set. Messages are limited to 1 MiB, and the gRPC server
shuts down with the HTTP server. This listener has no transport credentials;
bind it only to a trusted local or cluster-internal address.
`cmd/mill-execution` uses a bounded, deadline-bearing gRPC client, has no
PostgreSQL configuration or dependency, and retries later coordinator ticks
when the Job service is temporarily unavailable. The 12-task batch runs through
one Job process and standalone execution service replicas; the failover mode
kills the active lease owner and proves fenced takeover by a surviving replica.
Minimal non-root images now package both services. The execution service
explicitly selects either a local kubeconfig context or the standard in-cluster
service-account configuration; selecting both or neither is an error. Local
namespace-scoped RBAC and one- or two-replica Pod deployments are implemented.
Service authentication remains planned. Lease expiry transfers observation
ownership without creating a new attempt. Add an explicit expected version
only if the existing fencing and state guards prove insufficient for safely
retrying an unknown RPC outcome.

### Runtime separation implementation path

Implement the boundary as small runnable slices rather than another broad
package refactor:

1. **Serve the Job-side RPC API — implemented.** Keep `cmd/mill-job` as the Job
   service for now. It runs REST and optional gRPC listeners together,
   registers `executionrpc.Server`, limits messages to 1 MiB, and shuts both
   listeners down gracefully. PostgreSQL remains reachable only from this
   process.
2. **Add a separate execution process — implemented.** The
   `cmd/mill-execution` command has an execution instance identity,
   `executionrpc.Client`, coordinator loop, and Kubernetes client. Its
   dependency graph contains neither `internal/job` nor PostgreSQL, and
   temporary RPC failures leave it running for a later tick.
3. **Prove a complete split-process batch — implemented.** Run the existing
   12-task example through one Job process and two execution processes.
   Preserve bounded parallelism and exact output comparison, and verify
   execution processes have no database configuration.
4. **Remove the direct execution path — implemented.** The in-process
   coordinator, `repositoryExecutionStore`, and `MILL_EXECUTOR` have been
   removed from the Job service rather than maintained as a second mode.
5. **Prove execution failover — implemented locally.** Record active attempt
   IDs, lease tokens, Kubernetes Job names, and UIDs. Kill one execution
   process with `SIGKILL`, wait for lease expiry, and verify that the survivor
   receives new fencing tokens. Attempt IDs, Job names, and UIDs must remain
   unchanged, and all 12 tasks must finish without duplicate attempts or
   Kubernetes Jobs being created.

After step 5, the two-service system was reviewed before further feature work.
The review traced one submitted job through REST, partitioning, PostgreSQL,
gRPC, reconciliation, Kubernetes, and output publication. A first
learning-oriented refactor then made those boundaries visible in the package
tree: the job core depends on the `Store` and `DatasetPartitioner` contracts,
while HTTP, partitioning, and PostgreSQL remain adapters. Attempt persistence
moved beside the execution domain. These package boundaries do not create
additional deployed services.

This checkpoint demonstrates execution-process availability and a real service
boundary. It does not demonstrate complete infrastructure availability. A Job
service replica can reconnect execution replicas through a Kubernetes Service,
but PostgreSQL remains a failure point until database replication is complete.
Single-node kind remains a node-level failure point until the multi-node stage.

### Refactor checkpoint

The first refactor checkpoint follows the Milestone 7 test expansion and makes
existing service boundaries easier to learn. Perform a second overall review
after the three-node quorum milestone. Milestone 8 should provide evidence from
process failure, controlled database promotion, physical node loss, and
minority isolation before another broad reshaping of the code.

Begin with a package/dependency inventory and an end-to-end walkthrough.
Review service composition, lifecycle and shutdown, package ownership,
deployment manifests, RPC retry classification, reconciliation boundaries,
database assumptions, observability, and test/demo duplication. Delete obsolete
paths before introducing abstractions, keep behavior unchanged in refactor
commits, and preserve all process, Pod, database, node-loss, fencing, and quorum
tests throughout the work. This is not permission for a ground-up rewrite or
speculative frameworks.

Short reviews and necessary corrections still occur after every implementation
slice; only the broad consolidation is deferred.

## Workload resource classes

An optional top-level `resource_class` selects a server-defined profile:

```json
{
  "executable": {"image": "mill/word-count:dev", "args": []},
  "input": {"uri": "s3://mill-input/records.jsonl"},
  "resource_class": "large"
}
```

Omission means `small`. The Job service persists the class and resolved
requests/limits so configuration changes cannot alter a retry. The implemented
memory profiles are:

| Class | Memory request and limit |
| --- | ---: |
| `small` | 128 MiB |
| `medium` | 512 MiB |
| `large` | 2 GiB |

All three classes request `100m` CPU and limit CPU to `1`; these values remain
server policy rather than user input. Users do not submit raw Kubernetes
resource strings. Execution replicas receive resolved integer resources
through gRPC and construct the corresponding Kubernetes quantities.

The Job and execution services should normally request 64 MiB and limit at
128 MiB. Their memory may scale with explicitly bounded concurrent requests,
RPCs, and workers, but not dataset size or historical job count. PostgreSQL is
budgeted separately, initially around a 256 MiB request and 512 MiB limit per
instance. These are planned starting measurements, not capacity guarantees.

## Availability progression

Availability statements name the exact failure being tested.

### Local single-node deployment baseline — implemented

The first packaged deployment creates `mill-system` for the Job and execution
services, and `mill-workloads` for generated Kubernetes Jobs. One ClusterIP
Service exposes the Job service's REST and plaintext internal gRPC ports. The
Job Pod does not mount a Kubernetes service-account token. The execution
service uses an in-cluster token and a Role in `mill-workloads` limited to
`create` and `get` on `batch/jobs`; it cannot list or delete Jobs, read Pods
or Secrets, or access resources cluster-wide.

The Job service alone receives the PostgreSQL URL and its object-store
credentials. The execution service receives only the Job address, target
namespace, and workload object-store routing. Static local workload
credentials live in a separate Secret in `mill-workloads`; referencing that
Secret in a Job does not grant the execution service permission to read it.

Both Deployments have one replica and depend on externally managed PostgreSQL
and S3-compatible storage. The kind-specific images use `imagePullPolicy:
Never`. This baseline proves Pod startup, PostgreSQL-backed readiness, service
discovery, in-cluster configuration, and the RBAC boundary. It makes no
availability claim.

`scripts/demo-word-count-deployed.sh` proves the complete boundary using unique
temporary namespaces and disposable PostgreSQL/S3 fixtures. It submits 12
logical tasks through the deployed REST endpoint, leases them through deployed
gRPC and execution Pods, observes 12 S3-backed Kubernetes Jobs at bounded
parallelism three, and verifies the merged result exactly. This extends the
deployment claim to end-to-end correctness, but not availability.

Its `--execution-failover` mode is the first narrow availability proof. On the
same kind node, it scales the execution Deployment to two replicas, proves the
standby cannot acquire valid leases, deletes the active owner Pod, and waits for
lease takeover. Recovery is valid only when fencing tokens and the execution
owner change while task IDs, attempt IDs, attempt numbers, external UIDs,
Kubernetes Job names, and Job UIDs remain stable. The batch must finish with no
second attempts and exact output. This proves tolerance of one execution Pod
deletion while the Job service, PostgreSQL, Kubernetes API/node, network, and
object storage stay healthy; it does not prove any of those dependencies are
available under failure.

The complementary `--job-failover` mode starts with one Job
endpoint, scales its Deployment to two ready replicas, and deletes the original
Pod while attempts are live. A retrying REST client reconnects, and the
execution service's existing gRPC client must reconnect through the ClusterIP
Service and finish the same leases. Task IDs, attempt IDs, attempt numbers,
external UIDs, lease owner, fencing tokens, Kubernetes Job names, and Job UIDs
must remain stable. This proves tolerance of one stateless Job Pod failure
while PostgreSQL, the execution service, Kubernetes API/node, network, and
object storage stay healthy.

### Two-laptop replica availability — planned

One laptop runs a K3s server and the other a K3s agent. Two Job and two
execution replicas are spread across the nodes. CloudNativePG runs a primary and
standby with synchronous replication set to availability-oriented
`dataDurability: preferred`.

This stage targets one Mill Pod/process failure or one controlled PostgreSQL Pod
failure while the Kubernetes control plane and network remain healthy. If the
standby is unavailable, writes may continue; overlapping failures can therefore
lose recently acknowledged metadata. It does not claim survival of an entire
laptop or an ambiguous network partition.

The deployable two-node profile is implemented but not yet exercised. It uses
required hostname anti-affinity for both Mill service replicas and both
PostgreSQL instances, PodDisruptionBudgets for Mill, K3s `local-path` PVCs, and
CloudNativePG synchronous `ANY 1` with `dataDurability: preferred`.

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

The corresponding three-instance manifest is implemented and validated by the
CloudNativePG 1.30.0 admission webhook. It enables required `ANY 1` synchronous
replication and failover quorum, but no three-node runtime claim exists until
the K3s/etcd topology and failure scenarios have actually passed.

## Explicitly deferred

- asynchronous partitioning and preparation recovery;
- raw user-configurable Kubernetes resources;
- arbitrary input formats or partitioning languages;
- generic result aggregation or content validation;
- automatic deletion of historical Jobs and attempt outputs;
- multi-tenant authorization and untrusted-code isolation;
- database or object-store consensus implemented by Mill; and
- production availability, security, or scalability claims.
