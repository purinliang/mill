# Mill

Mill is a planned distributed batch execution system for running trusted OCI
container workloads over pre-partitioned datasets. A submitted job identifies a
workload image and a manifest of independent dataset shards; Mill turns those
shards into parallel tasks, delegates container execution to Kubernetes, tracks
durable state in PostgreSQL, and stores inputs and outputs in Amazon S3.

## Motivation

Many batch workloads apply the same computation independently to a large number
of inputs. The useful system problem is not writing another cluster scheduler,
but providing a job-level control plane around those executions: accepting a
job, expanding it into work, recording progress, handling failures, and making
results discoverable. Mill is intended to explore that problem with the
smallest architecture that demonstrates distributed batch execution clearly.

## Scope

Mill V1 is planned to support:

- submitting a batch job through an HTTP/REST API;
- selecting a trusted OCI image containing the workload;
- referencing a pre-partitioned dataset through a shard manifest;
- storing manifests, dataset shards, and task outputs in S3;
- storing jobs, tasks, and execution state in PostgreSQL;
- creating one independently executable task per shard;
- running tasks as Kubernetes-managed container workloads with configurable
  parallelism;
- tracking task states of `pending`, `running`, `completed`, and `failed`;
- reporting aggregate job progress;
- retrying failed tasks; and
- retrieving job status and output locations.

## Non-goals

V1 will not attempt to provide:

- arbitrary untrusted-code sandboxing or multi-tenant isolation;
- a custom cluster scheduler or replacement for Kubernetes placement;
- automatic partitioning or interpretation of arbitrary dataset formats;
- streaming execution or interdependent task graphs;
- Kafka, Redis, a service mesh, or multiple databases without a demonstrated
  requirement;
- a public gRPC API without a concrete internal-service need; or
- production-readiness, unlimited scale, or comprehensive cloud operations.

## High-level architecture

```text
User
  |
  | submit/query job (HTTP/REST)
  v
API --------------------------> PostgreSQL
                                 jobs, tasks, attempts, desired/observed state
                                      ^
                                      | reconcile durable intent
                                      v
                                 Coordinator
                                      |
                                      | create/observe native work
                                      v
                                 Kubernetes
                                      |
                                      +--> workload Pods <--> S3 inputs/outputs
```

The API and coordinator are logical responsibilities, not a commitment to
separate services. The first control plane should remain a single Go process
unless operational evidence justifies splitting it. PostgreSQL is the durable
source of truth for job and task metadata. S3 holds large datasets and outputs.
Kubernetes owns container placement, resource allocation, and pod lifecycle;
Mill owns the job abstraction, task intent, reconciliation, and progress view.

Kubernetes Indexed Jobs are a promising fit for shard-parallel execution and
will be evaluated before introducing any custom scheduling mechanism.

## Module view

Mill is one Go module and, for V1, one deployable control-plane process. The
modules below are responsibility boundaries inside that process; they are not
microservices. Status describes the repository today.

| Module | Status | Responsibility |
| --- | --- | --- |
| Process composition | Implemented | Read configuration, connect dependencies, assemble HTTP routes, and manage startup and graceful shutdown. |
| Health API | Implemented | Report process liveness and PostgreSQL-backed readiness. |
| Job API and model | Partially implemented | Validate job submissions, expose create/get operations, and report task-state counts. Execution-driven state changes remain planned. |
| Job persistence | Partially implemented | Store immutable job submission data, task intent, and concurrency-safe idempotency in PostgreSQL. Attempt and result persistence remain planned. |
| Manifest materialization | Implemented locally | Strictly decode a bounded local JSON manifest, record its checksum, and transactionally create one durable task per ordered shard. S3 manifest loading remains planned. |
| Coordinator | Planned | Reconcile durable task and attempt intent with an execution backend. It may initially use a local test executor before Kubernetes. |
| Kubernetes adapter | Planned | Create and observe native Kubernetes workloads without taking over scheduling. |
| Object-storage adapter | Planned | Access manifests and result metadata through storage-neutral boundaries, with S3 as the V1 cloud implementation. |

The HTTP handler calls the job capability, which owns its validation and
persistence rules. PostgreSQL remains the durable boundary. Future coordination
will operate from durable jobs, tasks, and attempts rather than directly from an
HTTP request, so work survives a client disconnect or control-plane restart.

```text
cmd/mill (configuration and process lifecycle)
    |
    +--> health HTTP handlers --> PostgreSQL readiness check
    |
    +--> job HTTP handlers --> job service
                                  |
                         +--------+--------+
                         |                 |
                         v                 v
               local manifest loader   PostgreSQL repository
                                             |
                                             v
                                        durable tasks
                                             |
                                             v
                                    planned coordinator
                                             |
                                             v
                                    Kubernetes adapter
```

## Repository structure

The current layout is intentionally shallow. Planned packages and directories
are not created until they contain a concrete implementation.

```text
cmd/mill/
  main.go                         process assembly, configuration, HTTP server,
                                  health routes, and graceful shutdown
  main_test.go                    hermetic process and health behavior tests
  main_integration_test.go        PostgreSQL startup/readiness integration test
internal/job/
  model.go                        job API and domain data types
  validation.go                   submission, identifier, and URI rules
  handler.go                      HTTP transport for job operations
  service.go                      submission and materialization workflow
  manifest.go                     bounded local JSON manifest loader
  repository.go                   PostgreSQL jobs, tasks, and idempotency
  *_test.go                       unit and PostgreSQL integration coverage
migrations/
  000001_create_jobs.sql          initial durable job schema
  000002_create_tasks.sql         manifest identity and durable task schema
README.md                         architecture, behavior, and development guide
AGENTS.md                         contribution and agent operating conventions
go.mod, go.sum                    Go module and dependency definitions
```

New behavior should normally enter through the narrowest existing capability.
For example, task materialization belongs with the job capability while it is
small and cohesive. A separate package is justified only when a boundary gains
an independent lifecycle or a concrete adapter contract, such as Kubernetes or
object storage. Process wiring stays in `cmd/mill`; business state transitions
and persistence invariants do not.

## Proposed correctness model

The following model is the starting architecture, not an implemented schema or
API contract. Its purpose is to keep retries and crash recovery consistent as
the implementation is introduced.

### Durable records

- A **job** holds an immutable submission snapshot: resolved image digest,
  versioned or checksummed manifest identity, output root, execution policy, and
  request identity. Its preparation status distinguishes a job whose shards are
  still being materialized from one with zero tasks.
- A **task** is the logical obligation to process one immutable shard. There is
  at most one task for each `(job, shard index)`, and its user-visible state is
  `pending`, `running`, `completed`, or `failed`.
- An **attempt** records one Mill-controlled execution generation for a task,
  including its Kubernetes resource identity and observed outcome. Retrying a
  task creates another attempt rather than erasing the previous failure.
- A **result** identifies output from the successful attempt. PostgreSQL stores
  its location and metadata; the output bytes remain in S3.

This distinction does not imply four separate services or elaborate domain
layers. It is a persistence and correctness boundary: a logical task can outlive
any particular Kubernetes Pod, Job, coordinator process, or retry.

### Submission and materialization

Job creation should accept an idempotency key or equivalent client request ID.
Repeating an identical request returns the original job; reusing the key for a
different request is rejected.

Before execution, Mill should resolve the image to an immutable digest; image
resolution is still planned. The local prototype records the exact manifest
bytes as a SHA-256 checksum. It then materializes tasks idempotently, using the
`(job, shard index)` identity and one database transaction so a crash cannot
commit a partial task set. The final shard count is persisted before the job is
treated as runnable. Local manifests are limited to 4 MiB and 10,000 shards.

### Dispatch and reconciliation

PostgreSQL records desired work before the coordinator calls Kubernetes. There
is no atomic transaction across PostgreSQL and the Kubernetes API, so correctness
comes from reconciliation:

1. Claim durable pending work and create an attempt.
2. Create Kubernetes work with deterministic names and Mill job/task/attempt
   labels.
3. Record the observed Kubernetes UID and lifecycle state.
4. Periodically compare durable intent with Kubernetes resources and repair
   incomplete steps.

Creating or observing the same attempt repeatedly must be safe. A crash before
Kubernetes creation leaves an attempt that can be dispatched; a crash after
creation but before the database update rediscovers the deterministically named
resource instead of launching duplicate intended work. Watches may reduce
latency, but periodic reconciliation remains the recovery mechanism.

V1 may run one coordinator instance. Multiple active coordinators require an
explicit PostgreSQL-backed claim or lease protocol before they are supported.

### Execution, retries, and outputs

Mill owns retry eligibility, retry budgets, and the durable attempt history.
Kubernetes owns Pod placement and lifecycle. Kubernetes-level retry settings
must be chosen deliberately so that native retries and Mill retries do not
silently multiply one another.

Execution is at least once. Kubernetes may start replacement or, rarely,
overlapping Pods for the same logical work, so a workload must tolerate
duplicate execution. Each physical execution writes to an attempt- or
execution-specific S3 prefix and exits successfully only after its output is
complete. Mill marks the task `completed` only after selecting and recording a
successful result. Failed or superseded output can be retained temporarily for
diagnosis and removed later by lifecycle policy.

The expected task transitions are:

```text
pending --> running --> completed
               |
               v
             failed --> pending (new retry attempt)
```

Transitions must reject stale observations, for example by checking the active
attempt or record version. A job is preparing while shards are materialized,
running while tasks remain active or retryable, completed when every task is
completed, and failed when preparation fails or at least one task exhausts its
retry policy.

### Kubernetes execution primitive

The preferred first investigation is one Kubernetes Indexed Job for a set of
tasks, with completion indexes mapped to an immutable task list and native
parallelism limiting active Pods. It is acceptable only if Mill can reliably
attribute per-index failure, execution identity, and output while preserving the
attempt model above.

The fallback is one Kubernetes Job per task with Mill limiting the number of
active Jobs. That mapping is simpler but creates more Kubernetes resources and
is suitable only within explicit V1 task-count limits. This is workload
admission, not container placement; Kubernetes remains the scheduler in either
design. The choice will be recorded during Milestone 3 after testing both
failure and recovery behavior.

### Access boundaries

Trusted workloads do not need an untrusted-code sandbox, but they should not
inherit control-plane credentials. The coordinator receives only the Kubernetes
permissions needed to manage Mill-owned resources. Workload Pods receive only
the S3 permissions required for their assigned inputs and output prefix. Local
development should preserve the same separation with development credentials or
test doubles.

## Core concepts

- **Job**: A user request to run one workload over every shard in a dataset. It
  groups tasks and exposes aggregate progress and result locations.
- **Dataset**: A pre-partitioned input collection. V1 may represent it with a
  manifest containing an ordered list of S3 shard locations.
- **Shard**: One independently processable input object or location from the
  dataset manifest. Mill does not interpret the shard's data format.
- **Task**: Mill's durable record of the work for one job and one shard. A task
  moves through explicit execution states and may be retried after failure.
- **Attempt**: One durable execution generation for a task. Attempts preserve
  retry history and connect Mill state to Kubernetes resources.
- **Workload/container**: A trusted OCI image that implements the user
  computation. Mill will define a small contract for passing task identity,
  input location, and output location to it.
- **Result**: Output written by a task to its assigned S3 location, with that
  location recorded in Mill's metadata.

## Proposed technology stack

| Concern | Direction |
| --- | --- |
| Control plane | Go 1.27.x, initially one process |
| External API | HTTP/REST |
| Durable metadata | PostgreSQL 18.x through pgx v5 |
| Dataset and result storage | Amazon S3 |
| Workload packaging | OCI images, commonly built with Docker |
| Distributed execution | Native Kubernetes Jobs/Pods |
| Local Kubernetes | kind, k3d, or another justified lightweight option |
| Cloud demonstration | AWS, likely Amazon EKS |
| Infrastructure as Code | Terraform when cloud deployment requires it |
| Testing | Go unit tests, integration tests, then Kubernetes end-to-end tests |

Structured logs, Prometheus, Grafana, and OpenTelemetry are candidates, not
baseline dependencies. They should be introduced only in response to an actual
diagnostic or measurement requirement.

## Local dataset manifest

The implemented manifest contract is deliberately small. It is a JSON object
with version `1` and an ordered, non-empty `shards` array:

```json
{
  "version": 1,
  "shards": [
    {"uri": "file:///tmp/mill-demo/shard-000.json"},
    {"uri": "file:///tmp/mill-demo/shard-001.json"},
    {"uri": "file:///tmp/mill-demo/shard-002.json"}
  ]
}
```

The array position is the stable zero-based shard index. Mill creates one task
per entry and assigns outputs below `<job-output-uri>/tasks/<shard-index>/`.
Duplicate shard URIs are allowed because two positions may intentionally refer
to the same input. The local loader accepts only absolute `file://` URIs, does
not open the shard files, rejects unknown JSON fields, and limits a manifest to
4 MiB and 10,000 shards. These are prototype limits, not scalability claims.

The SHA-256 recorded on the job identifies the exact manifest bytes that were
materialized. Once materialization succeeds, an idempotent replay returns the
stored job without requiring the manifest file to remain available.

Task creation and the transition from `preparing` to `running` commit in one
PostgreSQL transaction. A process failure can therefore leave a job preparing,
but cannot commit only part of its task set. The current recovery trigger is an
idempotent replay of the same submission; automatic background reconciliation
of preparing jobs remains planned.

## Local quick start

The current development slice requires Go 1.27 and a reachable PostgreSQL 18
database. It opens a pgx connection pool and verifies the database connection
before accepting HTTP traffic. Numbered SQL migrations create the job and task
metadata schema.

Create an empty development database using your local PostgreSQL installation:

```bash
createdb mill
export MILL_DATABASE_URL='postgresql:///mill'
psql "$MILL_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/000001_create_jobs.sql
psql "$MILL_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/000002_create_tasks.sql
```

Create a local manifest. The shard files are references in this slice and do
not need to exist yet:

```bash
mkdir -p /tmp/mill-demo
printf '%s\n' '{' \
  '  "version": 1,' \
  '  "shards": [' \
  '    {"uri": "file:///tmp/mill-demo/shard-000.json"},' \
  '    {"uri": "file:///tmp/mill-demo/shard-001.json"},' \
  '    {"uri": "file:///tmp/mill-demo/shard-002.json"}' \
  '  ]' \
  '}' > /tmp/mill-demo/manifest.json
```

Set a local output root and start Mill. The prototype derives a stable output
URI for each job but does not create the directory or write output yet:

```bash
export MILL_OUTPUT_ROOT_URI='file:///tmp/mill-output'
go run ./cmd/mill
```

The server listens on `:8080` by default. Set `MILL_HTTP_ADDR` to use another
address. A TCP connection URL can include the database user, password, host,
and port; keep real credentials in the environment and out of the repository.

The health endpoints have distinct purposes:

- `GET /healthz` and `GET /livez` report that the HTTP process is alive without
  depending on PostgreSQL.
- `GET /readyz` checks PostgreSQL and returns HTTP `503` while the dependency is
  unavailable.

In another shell:

```bash
curl http://localhost:8080/livez
curl http://localhost:8080/readyz
```

The readiness response is:

```json
{"status":"ready"}
```

Submit a job in another shell. The image remains a reference only. Mill reads
the manifest, records its checksum, and creates the pending task records, but it
does not inspect the image, open shard files, or execute tasks yet.

```bash
curl --include --request POST http://localhost:8080/jobs \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: demo-job-001' \
  --data '{
    "workload": {
      "image": "mill/example:dev",
      "args": ["--mode", "fast"]
    },
    "dataset": {
      "manifest_uri": "file:///tmp/mill-demo/manifest.json"
    }
  }'
```

The first submission returns HTTP `201`; repeating the identical request returns
HTTP `200` and the same job. Reusing the key with different input returns HTTP
`409`. The response reports `state: "running"`, the manifest checksum, and task
progress such as three total and three pending. Retrieve the stored job using
the UUID from the response:

```bash
curl http://localhost:8080/jobs/<job-id>
```

Run the hermetic tests with:

```bash
go test ./...
```

To include PostgreSQL persistence and concurrency tests, create a disposable
database, apply the migration, and point the test-specific variable at it:

```bash
createdb mill_test
psql 'postgresql:///mill_test' -v ON_ERROR_STOP=1 \
  -f migrations/000001_create_jobs.sql
psql 'postgresql:///mill_test' -v ON_ERROR_STOP=1 \
  -f migrations/000002_create_tasks.sql
MILL_TEST_DATABASE_URL='postgresql:///mill_test' go test ./...
```

## Execution lifecycle

1. A user submits an idempotent request containing a trusted image reference, a
   dataset manifest location, and execution settings. Mill derives the job's
   output namespace from its configured storage root and generated job ID.
2. Mill resolves immutable input identities, persists the preparing job, and
   idempotently materializes one `pending` task for each shard.
3. The coordinator records an attempt before representing runnable work with a
   native Kubernetes primitive.
4. Workload containers receive task and execution identity plus input and output
   locations, read their shard from S3, and write isolated results to S3.
5. Mill reconciles Kubernetes observations into attempt and task state without
   relying on one event delivery or one coordinator process lifetime.
6. A successful output is recorded before the task becomes `completed`; an
   unsuccessful attempt makes the task `failed` or eligible for another attempt.
7. Job progress is calculated from the finalized task count, and status
   responses expose committed result locations.

## Development milestones

### Milestone 0 — Project foundation

Document goals, non-goals, terminology, architectural boundaries, the job and
task model, the conceptual lifecycle, milestones, and repository conventions.
No application implementation is part of this milestone.

### Milestone 1 — Local single-process control plane

Implement the minimum Go domain model and REST API required to create and query
jobs. Establish job, task, attempt, and result persistence; idempotent submission
and task materialization; explicit state transitions; and restart-safe
reconciliation against a mock or local executor before Kubernetes integration.

### Milestone 2 — Container workload contract

Define and document the minimal stable interface between Mill and workload
containers, covering immutable job/task/execution identity, S3 input/output
locations, duplicate execution, and successful output completion. Provide a
small demonstration workload to verify the contract without expanding Mill's
scope.

### Milestone 3 — Kubernetes execution

Evaluate Kubernetes Indexed Jobs against the task model, then integrate the
smallest suitable native Job/Pod approach. Support configurable parallelism and
reconcile Kubernetes lifecycle information into Mill's attempt and task state.
Configure Kubernetes and Mill retry ownership explicitly.

### Milestone 4 — Reliable execution

Add retry backoff and policy controls, cleanup behavior, and fault-injection
coverage. Test recovery after task, coordinator, and Kubernetes failures,
including duplicate execution and every dispatch crash window. Failures must be
observable and must not silently lose durable job intent.

### Milestone 5 — AWS deployment

Create a disposable demonstration environment using S3, PostgreSQL, and likely
Amazon EKS. Introduce Terraform only when the deployment design is understood,
and make teardown and cost control explicit.

### Milestone 6 — Evaluation

Run one or more simple independent-shard workloads, such as a Monte Carlo
simulation, parameter sweep, or bulk file transformation. Measure throughput,
elapsed time, scaling with parallelism, and failure recovery. The workload is a
test vehicle; Mill remains the project under evaluation.

## Current status

Mill is in Milestone 1. The Go control plane establishes a PostgreSQL connection
pool, exposes liveness and database-backed readiness probes, and shuts down
cleanly. Numbered migrations define durable `jobs` and `tasks`. `POST /jobs`
persists a submission with concurrency-safe idempotency, strictly reads a local
JSON manifest, and transactionally creates one pending task per ordered shard.
`GET /jobs/{id}` retrieves the job, manifest checksum, output location, and
task-state counts after restart. Image inspection, shard access, task execution,
attempts, results, S3, Kubernetes, retries, and the complete workload contract
remain planned.

## Local and cloud development philosophy

Development starts locally so domain behavior, persistence, and reconciliation
can be tested quickly and deterministically. Kubernetes and AWS integration
should be added in layers only after the preceding local behavior is understood.
A real AWS deployment is intended as a reproducible, temporary demonstration,
not as the default development environment. Cloud resources should be easy to
create for an evaluation and destroy afterward to limit cost.
