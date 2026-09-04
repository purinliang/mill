# Mill

Mill is a learning-oriented distributed batch execution system for running a
trusted OCI container over independent parts of a dataset. A user submits an
executable image and one input; Mill plans logical shards, stores durable job
and task state in PostgreSQL, and will later ask Kubernetes to run copies of the
image in parallel. Large cloud inputs and outputs will live in Amazon S3.

Mill is in active development and is not production-ready. Its reference
workload can be built and run manually with Docker, but the control plane does
not execute containers yet.

## Motivation

Many batch workloads apply the same computation independently to sections of a
large input. The interesting system problem is the control plane around that
work: create a job, divide it into durable tasks, limit concurrency, recover
from failures, report progress, and make outputs discoverable. Mill explores
those ideas while relying on PostgreSQL for durable state and Kubernetes for
container placement rather than rebuilding either system.

## Scope

V1 is intended to support:

- submitting a job through an HTTP/REST API;
- selecting a trusted OCI image and its ordinary application arguments;
- accepting one JSON Lines (JSONL) input and planning independent logical
  shards at record boundaries;
- storing datasets and outputs in S3 in the cloud;
- storing job, task, attempt, and result metadata in PostgreSQL;
- executing tasks as Kubernetes-managed container workloads with configurable
  parallelism;
- tracking `pending`, `running`, `completed`, and `failed` task states;
- reporting aggregate job progress;
- retrying failed tasks; and
- retrieving job state and generated output locations.

The local prototype uses `file://` input and output URIs. S3, container
execution, and Kubernetes integration are planned rather than implemented.

## Non-goals

V1 will not provide:

- arbitrary untrusted-code sandboxing or multi-tenant isolation;
- arbitrary data-format discovery or a general-purpose partitioning language;
- a custom cluster scheduler or replacement for Kubernetes placement;
- streaming pipelines or interdependent task graphs;
- Kafka, Redis, a service mesh, multiple databases, or separate deployable
  services without a concrete requirement;
- a public gRPC API without a demonstrated internal-service need; or
- production-readiness or unbounded scalability claims.

## High-level architecture

```text
User
  |
  | POST /jobs: executable + one JSONL input
  v
Single Go control plane
  |
  +--> partition planner --> logical byte ranges at JSONL record boundaries
  |
  +--> PostgreSQL
  |      jobs, tasks, durable execution state
  |
  `--> coordinator (planned)
          |
          `--> Kubernetes Jobs/Pods (planned)
                  |
                  +--> read input range from file/S3
                  `--> write result to file/S3
```

The API, planner, and coordinator are responsibilities inside one control-plane
process, not separate microservices. PostgreSQL is the durable source of truth
for control-plane metadata. Object storage holds large data. Kubernetes will
own Pod placement, resource allocation, and container lifecycle; Mill owns job
intent, logical tasks, retries, reconciliation, and progress.

Kubernetes Indexed Jobs are the first execution primitive to evaluate. Mill
should use one Kubernetes Job per task only if Indexed Jobs cannot expose the
per-shard execution and failure information that the task model needs.

## Core concepts

- **Job**: one request to apply an executable to an entire input. It owns the
  input identity, generated output root, chosen parallelism, tasks, and
  aggregate progress.
- **Dataset/input**: one JSONL resource in the initial implementation. Each line
  is one independent record. More formats require an explicit later design.
- **Logical shard**: a contiguous byte range `[start, end)` whose boundaries are
  also JSONL record boundaries. Logical shards refer to one input rather than
  copying it into physical shard files.
- **Task**: the durable obligation to process one logical shard for one job. The
  pair `(job_id, shard_index)` is unique, so `job-1/shard-0` and
  `job-2/shard-0` are different tasks.
- **Attempt**: one durable execution generation of a task. A future retry will
  create a new attempt instead of erasing failure history.
- **Executable/container**: the trusted OCI image and user arguments. The image
  is only recorded today; image resolution and execution are planned.
- **Result**: output from a successful attempt. Output bytes belong in object
  storage; PostgreSQL stores only their identity and metadata.

## Current data model

The implemented durable records are deliberately small:

```text
Job
  id                    UUIDv7
  idempotency key       unique client request identity
  executable            image reference + arguments
  input                 URI + SHA-256 + record count
  output root           generated from configured root + job ID
  parallelism           server setting captured at submission time
  state/task count      durable materialization and progress metadata

Task
  id                    UUIDv7
  job ID                owning job
  shard index           stable zero-based position within the job
  input range           [start byte, end byte)
  state                 pending/running/completed/failed

Attempt
  id                    UUIDv7
  task ID               logical task being executed
  attempt number        monotonically increasing within the task
  executor              execution backend identity
  state                 starting/running/completed/failed
  external ID           nullable runtime resource identity
  lifecycle metadata    timestamps and an optional failure message
```

Input and output URIs are not repeated on every task. A task reads the job's
input URI. Each attempt's output URI is derived from the job output root, shard
index, and attempt ID, so overlapping or retried executions cannot overwrite
one another.

The job/task distinction is important: a job is the user's whole batch request,
while a task is one independently executable piece of that request. A future
Kubernetes Pod is an attempt to perform a task, not the task itself.

## Logical JSONL sharding

The current planner accepts an absolute local `file://` URI and validates that:

- the target is a regular, non-empty file;
- each line contains exactly one valid JSON value;
- each record is at most 16 MiB; and
- the input does not change while the plan is being created.

The planner scans the file without loading the whole dataset into memory. It
records an SHA-256 checksum and record count, then creates contiguous byte
ranges on line boundaries. This is logical sharding: Mill stores offsets, not
copied shard files.

The number of tasks is an internal scheduling choice, distinct from the number
of concurrent workers. The first learning heuristic targets four waves of work:

```text
target tasks = min(record count, parallelism * 4, 10,000)
```

For example, 100 records and `MILL_PARALLELISM=3` produce 12 logical tasks, of
which at most three will eventually run at once. The factor and limits are
prototype policy, not API promises; the user does not need to choose a shard
size.

Planning currently performs two local scans: one to count and identify the
input, and one to select balanced record boundaries without retaining every
line offset in memory. A future S3 planner must make the I/O cost explicit and
may use object metadata or a different one-pass strategy while preserving the
same durable task model.

## Submission API

The current request shape keeps execution policy out of the user-facing job:

```json
{
  "executable": {
    "image": "mill/jsonl-copy:dev",
    "args": ["--mode", "fast"]
  },
  "input": {
    "uri": "file:///tmp/mill-demo/records.jsonl"
  }
}
```

JSONL is the only format, so there is no redundant `format` field. Shard size
and parallelism are not request fields. Mill generates the output root and
returns it with the job. The workload contract uses `--` to keep Mill-owned
flags separate from arbitrary user executable arguments.

`Idempotency-Key` is required. Repeating the same key and submission returns the
original job; using the key for different executable arguments or input URI is
rejected. Once task materialization succeeds, an identical replay does not need
to reopen the input.

Task creation and the job transition out of `preparing` commit in one
PostgreSQL transaction. A crash can leave a job in `preparing`, but cannot
commit only part of its task set. Replaying the same submission currently
retries preparation; background recovery is planned.

For now, job state `running` means that materialization finished and the job has
unfinished tasks; it does not claim that a container is currently active. Task
counts provide the more precise execution view.

## Workload CLI contract

Mill-owned arguments come first, followed by a mandatory `--` separator and the
user's executable arguments:

```text
--job-id <job-id>
--task-id <task-id>
--shard-index <zero-based-index>
--input-uri <absolute-uri>
--input-start-byte <inclusive-offset>
--input-end-byte <exclusive-offset>
--output-uri <absolute-uri>
--
<user executable arguments>
```

The half-open byte range `[start, end)` contains complete JSONL records. The
output URI names the exact file/object for this execution, not merely a shared
directory. Mill derives it below the job output root as
`tasks/<shard-index>/attempts/<attempt-id>/result.jsonl`.

A conforming workload should:

- read only its assigned input range;
- treat the input as immutable for the duration of the job;
- write only to its assigned output URI;
- exit successfully only after publishing a complete output; and
- return a non-zero exit status for a failed execution.

The separator is always present, even when the user supplied no arguments. It
makes ownership positional: flags before it belong to Mill and arguments after
it belong unchanged to the executable. A user argument may therefore have the
same spelling as a Mill flag without ambiguity. No equivalent environment
variables are defined in this contract.

`cmd/mill-jsonl-copy` is the first reference workload. It supports local
`file://` URIs, copies exactly the assigned range to its output atomically, and
performs no business computation. Its Docker image contains a statically linked
binary in a scratch filesystem and runs as non-root UID/GID `65532`. It proves
the protocol; the control plane does not launch it yet.

## Module view

Mill is one Go module with one control-plane process. The reference workload is
a separate executable, not another service. These are code responsibility
boundaries, not microservices.

| Module | Status | Responsibility |
| --- | --- | --- |
| Process composition | Implemented | Read environment configuration, connect PostgreSQL, assemble routes, and shut down cleanly. |
| Health API | Implemented | Report process liveness and PostgreSQL-backed readiness. |
| Job HTTP API/model | Partially implemented | Validate, create, and retrieve jobs; report persisted task counts. Execution-driven transitions remain planned. |
| JSONL partition planner | Implemented locally | Validate local input, calculate identity, and create logical record-aligned byte ranges. S3 access remains planned. |
| PostgreSQL repository | Partially implemented | Persist idempotent jobs, materialize logical tasks, claim work, and transition attempts. Result publication remains planned. |
| Workload CLI contract | Implemented locally | Serialize and parse task identity, logical input range, output URI, and separated user arguments. |
| JSONL copy workload | Implemented locally and in Docker | Demonstrate that one process/container reads and atomically writes exactly one assigned local range. |
| Coordinator | Planned | Reconcile durable tasks and attempts with an execution backend. |
| Kubernetes adapter | Planned | Create and observe native Kubernetes work without taking over scheduling. |
| Object-storage adapter | Planned | Read S3 inputs and expose result metadata after local behavior is understood. |

```text
cmd/mill (configuration and process lifecycle)
    |
    +--> health handlers --> PostgreSQL readiness
    |
    `--> job HTTP handler --> job service
                                |
                         +------+------+
                         |             |
                         v             v
                  JSONL planner   PostgreSQL repository
                                       |
                                       v
                                  durable tasks
                                       |
                                       v
                              coordinator (planned)
                                       |
                                       v
                              Kubernetes (planned)
```

## Repository structure

Only concrete implementation paths exist:

```text
cmd/mill/
  main.go                         configuration, assembly, HTTP lifecycle
  main_test.go                    process and health unit tests
  main_integration_test.go        PostgreSQL readiness integration test
cmd/mill-jsonl-copy/
  main.go                         local reference workload for one byte range
  Dockerfile                      multi-stage non-root reference image
internal/job/
  model.go                        API and domain data types
  validation.go                   submission, identifier, and URI rules
  partition.go                    local JSONL logical-shard planner
  service.go                      idempotent planning/materialization workflow
  repository.go                   PostgreSQL job/task persistence
  attempt_repository.go           task claiming and attempt transitions
  handler.go                      HTTP transport
  *_test.go                       unit and PostgreSQL integration coverage
internal/workload/
  contract.go                     Mill CLI argument builder and parser
migrations/
  000001_create_jobs.sql          initial job schema
  000002_create_tasks.sql         first task schema
  000003_refactor_job_input.sql   executable/input names and byte-range tasks
  000004_create_attempts.sql      durable execution attempts
README.md                         architecture and development guide
AGENTS.md                         contribution and agent conventions
.dockerignore                     files excluded from Docker build context
go.mod, go.sum                    Go module and dependency definitions
```

Process wiring belongs in `cmd/mill`. Cohesive job behavior remains in
`internal/job` while it is small. A Kubernetes or S3 package should appear only
when that adapter has real behavior; empty architectural placeholders are not
useful.

## Correctness and execution design

### Durable intent and reconciliation

Mill persists execution intent before an external runtime call. PostgreSQL and
Docker or Kubernetes do not share a transaction, so execution must eventually
be reconciled rather than treated as an exactly-once request:

1. Lock a job with available parallelism and claim its oldest pending task.
2. Create a `starting` attempt and mark the task `running` in one transaction.
3. Ask the execution backend to create the external container or Pod.
4. Record its external identity and transition the attempt to `running`.
5. Atomically finish the attempt, task, and, when appropriate, job.
6. Reconcile attempts left between these steps after a process or API failure.

The repository implements the durable transitions in steps 1, 2, 4, and 5.
External execution and reconciliation remain planned. PostgreSQL row locks make
claims concurrency-safe, enforce each job's captured parallelism, and allow at
most one active attempt per task. Running multiple coordinators is deferred
until ownership and recovery behavior are explicitly designed.

### Retries and outputs

Execution will be at least once, so a workload must tolerate duplicate
execution. Each attempt writes to its own output location and should publish
completion only after its output is complete. A future retry will create a new
attempt while retaining terminal attempt history. Until retry policy is added,
a failed attempt immediately fails its task and job.

```text
pending --> running --> completed
               |
               v
             failed --> pending (future retry creates a new attempt)
```

Kubernetes-native retries and Mill retries must be configured together so they
do not multiply unexpectedly.

### Credential boundary

Trusted workloads still should not inherit control-plane credentials. The
control plane receives only the Kubernetes permissions needed for Mill-owned
resources. Workload Pods receive only the object-storage access needed for
their assigned input and output namespace.

## Proposed technology stack

| Concern | Direction |
| --- | --- |
| Control plane | Go 1.27.x, one process initially |
| External API | HTTP/REST |
| Durable metadata | PostgreSQL 18.x through pgx v5 |
| Dataset and output storage | Local files now; Amazon S3 planned |
| Workload packaging | OCI images, commonly built with Docker |
| Distributed execution | Native Kubernetes Jobs/Pods, planned |
| Local Kubernetes | kind or k3d after the container contract exists |
| Cloud demonstration | AWS, likely Amazon EKS |
| Infrastructure as Code | Terraform when cloud deployment needs it |
| Testing | Go unit tests, PostgreSQL integration tests, later Kubernetes end-to-end tests |

Logging, metrics, dashboards, and tracing should be added only when a concrete
diagnostic or evaluation requirement justifies them.

## Local quick start

The current slice requires Go 1.27 and PostgreSQL 18. Apply all numbered
migrations to a local development database:

```bash
createdb mill
export MILL_DATABASE_URL='postgresql:///mill'
psql "$MILL_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/000001_create_jobs.sql \
  -f migrations/000002_create_tasks.sql \
  -f migrations/000003_refactor_job_input.sql \
  -f migrations/000004_create_attempts.sql
```

Create an input containing 100 independent JSON records:

```bash
mkdir -p /tmp/mill-demo
awk 'BEGIN { for (i = 0; i < 100; i++) print "{\"value\":" i "}" }' \
  > /tmp/mill-demo/records.jsonl
```

Start Mill with a local output root and desired future worker concurrency:

```bash
export MILL_OUTPUT_ROOT_URI='file:///tmp/mill-output'
export MILL_PARALLELISM=3
go run ./cmd/mill
```

The default HTTP address is `:8080`; override it with `MILL_HTTP_ADDR`.
`MILL_DATABASE_URL` may use either a local Unix socket or TCP connection. Keep
credentials in the environment and out of the repository.

Health endpoints:

- `GET /healthz` and `GET /livez` report HTTP process liveness;
- `GET /readyz` verifies PostgreSQL connectivity and returns `503` if it is
  unavailable.

Submit the input. The image is recorded but not run:

```bash
curl --include --request POST http://localhost:8080/jobs \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: demo-job-001' \
  --data '{
    "executable": {
      "image": "mill/jsonl-copy:dev",
      "args": []
    },
    "input": {
      "uri": "file:///tmp/mill-demo/records.jsonl"
    }
  }'
```

The first request returns `201`; an identical replay returns `200` and the same
job. With 100 records and parallelism 3, progress reports 12 pending logical
tasks. The response also includes the input checksum and generated output root.
Retrieve it again with:

```bash
curl http://localhost:8080/jobs/<job-id>
```

The reference workload can be invoked manually against the first 12-byte JSONL
record. This exercises the contract but does not change job or task state:

```bash
go run ./cmd/mill-jsonl-copy \
  --job-id 0198b7c9-1d24-7000-8000-000000000001 \
  --task-id 0198b7c9-1d24-7000-8000-000000000002 \
  --shard-index 0 \
  --input-uri file:///tmp/mill-demo/records.jsonl \
  --input-start-byte 0 \
  --input-end-byte 12 \
  --output-uri file:///tmp/mill-output/manual-task-0.jsonl \
  --
```

Build the same workload as an OCI image:

```bash
docker build \
  --file cmd/mill-jsonl-copy/Dockerfile \
  --tag mill/jsonl-copy:dev \
  .
```

Run it with a read-only input mount and a separate writable output mount. The
host UID/GID override lets the non-root process write a host-owned result:

```bash
mkdir -p /tmp/mill-output/container-demo
docker run --rm \
  --read-only \
  --network none \
  --user "$(id -u):$(id -g)" \
  --mount type=bind,source=/tmp/mill-demo/records.jsonl,target=/data/records.jsonl,readonly \
  --mount type=bind,source=/tmp/mill-output/container-demo,target=/data/output \
  mill/jsonl-copy:dev \
  --job-id 0198b7c9-1d24-7000-8000-000000000001 \
  --task-id 0198b7c9-1d24-7000-8000-000000000002 \
  --shard-index 0 \
  --input-uri file:///data/records.jsonl \
  --input-start-byte 0 \
  --input-end-byte 12 \
  --output-uri file:///data/output/task-0.jsonl \
  --
```

The `file://` URIs refer to paths inside the container. The bind mounts are the
adapter between host storage and that container-visible namespace. S3-backed
execution will not need this path translation.

Run hermetic tests:

```bash
go test ./...
```

Run PostgreSQL integration tests against a disposable migrated database:

```bash
createdb mill_test
psql 'postgresql:///mill_test' -v ON_ERROR_STOP=1 \
  -f migrations/000001_create_jobs.sql \
  -f migrations/000002_create_tasks.sql \
  -f migrations/000003_refactor_job_input.sql \
  -f migrations/000004_create_attempts.sql
MILL_TEST_DATABASE_URL='postgresql:///mill_test' go test -race ./...
```

## Execution lifecycle

1. The user submits an idempotent request containing an executable and one
   JSONL input URI.
2. Mill validates and identifies the input, chooses a task count from its
   configured parallelism, and plans record-aligned logical ranges.
3. Mill persists the job and atomically creates one pending task per range.
4. A coordinator claims a task and durably creates an attempt before asking a
   local Docker adapter, or later Kubernetes, to run it.
5. A workload container receives task identity, its input range, and a derived
   attempt output location through a small CLI contract.
6. Mill reconciles Kubernetes observations into durable attempt and task state.
7. Successful output is recorded before completion; failed work may create a
   new attempt according to retry policy.
8. Job status aggregates the finalized task set and exposes output locations.

Steps 1–3 and the durable claim/transition part of step 4 are implemented. The
CLI shape used by step 5 and a standalone reference workload are implemented,
but no coordinator or execution adapter wires them together yet.

## Development milestones

### Milestone 0 — Project foundation

Define goals, non-goals, terminology, architecture, lifecycle, and repository
conventions. **Implemented.**

### Milestone 1 — Local single-process control plane

Implement create/get job APIs, PostgreSQL persistence, internal JSONL logical
sharding, durable task materialization, and persisted progress. **Implemented.**

### Milestone 2 — Container workload contract

Define the smallest stable CLI interface for job/task identity, input byte
ranges, and output location. Build one demonstration image and exercise it
locally before involving Kubernetes. **Implemented:** the CLI protocol,
reference executable, minimal image, and constrained manual Docker execution
are tested. Control-plane execution belongs to the next integration slice.

### Milestone 3 — Kubernetes execution

Evaluate Indexed Jobs, integrate the smallest suitable native primitive,
enforce configured parallelism, and reconcile Pod outcomes. **Planned.**

### Milestone 4 — Reliable execution

Add retries, backoff, attempt history, stale-observation protection, cleanup,
and fault/recovery tests for dispatch crash windows. **Planned.**

### Milestone 5 — AWS deployment

Add S3 and a disposable demonstration environment using PostgreSQL and likely
EKS. Introduce Terraform with explicit teardown and cost control. **Planned.**

### Milestone 6 — Evaluation

Run an independent-record workload and measure throughput, elapsed time,
parallel scaling, and failure recovery. The workload is a test vehicle; Mill is
the project being evaluated. **Planned.**

## Current status

Mill has completed Milestone 2. Implemented behavior includes:

- one Go HTTP process with liveness and PostgreSQL-backed readiness;
- `POST /jobs` and `GET /jobs/{id}`;
- concurrency-safe idempotent submission;
- local JSONL validation, identity, record counting, and logical byte-range
  planning;
- atomic PostgreSQL job/task materialization;
- persisted task-state progress after restart;
- concurrency-safe task claims that enforce each job's parallelism;
- durable attempt lifecycle transitions and atomic task/job completion;
- a typed workload CLI argument contract; and
- a local JSONL copy executable and non-root OCI image that process one assigned
  range.

The control plane still does not launch the executable. Image inspection, task
execution coordination, result publication, automatic recovery, S3, a Docker
execution adapter, Kubernetes, and retries are not implemented. Until retries
exist, one failed attempt immediately fails its task and job.

## Local and cloud development philosophy

Development starts locally so each domain and persistence decision can be
learned and tested in isolation. The next layer should be a small local Docker
execution adapter, then Kubernetes, then AWS. A cloud environment should be
reproducible and disposable rather than the default development setup.
