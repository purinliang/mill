# Mill

Mill is a learning-oriented distributed batch execution system for running a
trusted OCI container over independent parts of a dataset. A user submits an
executable image and one input; Mill plans logical shards, stores durable job
and task state in PostgreSQL, and can ask Kubernetes to run copies of the
image in parallel. Large cloud inputs and outputs will later live in Amazon S3.

Mill is in active development and is not production-ready. The current
execution prototype runs on local kind with explicitly staged files and an
optional coordinator in the HTTP process.

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

The local prototype uses `file://` input and output URIs and native Kubernetes
Jobs on a configured node. S3, automatic task retries, and cloud storage remain
planned.

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
  `--> coordinator (optional, same process)
          |
          `--> Kubernetes Job per attempt
                  |
                  +--> read input range from file/S3
                  `--> write result to file/S3
```

The API, planner, and coordinator are responsibilities inside one control-plane
process, not separate microservices. PostgreSQL is the durable source of truth
for control-plane metadata. Object storage holds large data. Kubernetes will
own Pod placement, resource allocation, and container lifecycle; Mill owns job
intent, logical tasks, retries, reconciliation, and progress.

The local Kubernetes adapter creates one Kubernetes Job for each Mill
attempt. That maps unique task arguments, output locations, and retry history
directly to independently observable resources. Mill limits how many
attempts are active; Kubernetes places their Pods. Indexed Jobs remain a
later option if a concrete need justifies adding a shared shard-manifest lookup
contract.

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
- **Attempt**: one durable execution generation of a task. A retry
  creates a new attempt instead of erasing failure history.
- **Executable/container**: the trusted OCI image and user arguments. The local
  adapter requires the image to be loaded into kind before submission.
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
  available_at          earliest time a pending task may be claimed

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
records per task = ceil(record count / target tasks)
actual tasks = ceil(record count / records per task)
```

For example, 100 records and `MILL_PARALLELISM=3` produce 12 logical tasks, of
which at most three attempts run at once with execution enabled. These are
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

`examples/jsonl-copy/cmd/jsonl-copy` is the first reference workload. It supports
local `file://` URIs, copies exactly the assigned range to its output atomically, and
performs no business computation. Its Docker image contains a statically linked
binary in a scratch filesystem and runs as non-root UID/GID `65532`. It proves
the protocol and can also run through the Kubernetes adapter when its image is
loaded and its input/output paths are staged.

## Module view

Mill is one Go module with one control-plane process. The reference workload is
a separate executable, not another service. These are code responsibility
boundaries, not microservices.

| Module | Status | Responsibility |
| --- | --- | --- |
| Process composition | Implemented | Read environment configuration, connect PostgreSQL, assemble routes, and shut down cleanly. |
| Health API | Implemented | Report process liveness and PostgreSQL-backed readiness. |
| Job HTTP API/model | Implemented locally | Validate, create, and retrieve jobs; report persisted task counts and successful output URIs after completion. |
| JSONL partition planner | Implemented locally | Validate local input, calculate identity, and create logical record-aligned byte ranges. S3 access remains planned. |
| PostgreSQL repository | Partially implemented | Persist idempotent jobs, materialize logical tasks, claim work, and transition attempts. Result publication remains planned. |
| Workload CLI contract | Implemented locally | Serialize and parse task identity, logical input range, output URI, and separated user arguments. |
| JSONL copy workload | Implemented locally and in Docker | Demonstrate that one process/container reads and atomically writes exactly one assigned local range. |
| Word-count workload | Implemented locally, in Docker, and as a manual kind Job | Count words in one assigned range, verify Pod output against a local run, and merge partial results for demonstration. |
| Coordinator | Implemented locally | Observe active attempts, claim pending tasks within PostgreSQL concurrency limits, and persist completion/failure. |
| Kubernetes adapter | Implemented for kind | Create and observe one native Job per attempt, with staged node-local mounts. |
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
                              coordinator (optional)
                                       |
                                       v
                              Kubernetes adapter --> Jobs/Pods
```

## Repository structure

Only concrete implementation paths exist:

```text
cmd/mill/
  main.go                         configuration, assembly, HTTP lifecycle
  execution.go                    optional coordinator loop and ownership lock
  main_test.go                    process and health unit tests
  main_integration_test.go        PostgreSQL readiness integration test
examples/jsonl-copy/cmd/jsonl-copy/
  main.go                         local reference workload for one byte range
  Dockerfile                      multi-stage non-root reference image
examples/word-count/
  cmd/word-count/
    main.go                       word-count mapper for one assigned range
    Dockerfile                    multi-stage non-root mapper image
  cmd/merge/
    main.go                       local demonstration result merger
  cmd/fault-injection/
    main.go                       test-only fail-once/always mapper wrapper
    Dockerfile                    wrapper plus unchanged word-count executable
  wordcount.go                    tokenization, counting, and merge behavior
  wordcount_test.go               workload behavior tests
  walden-economy.txt              committed plain-text Chapter 1 source
  record-config.json              deterministic demo record-grouping config
  generate/                       reproducible JSONL input generator
  job.yaml.template               single-task kind Job arguments and mounts
  README.md                       demo contract and provenance
internal/job/
  model.go                        API and domain data types
  validation.go                   submission, identifier, and URI rules
  partition.go                    local JSONL logical-shard planner
  service.go                      idempotent planning/materialization workflow
  repository.go                   PostgreSQL job/task persistence
  attempt_repository.go           task claiming, attempt transitions, retry policy
  execution_repository.go         reconstruct active attempts and list results
  handler.go                      HTTP transport
  *_test.go                       unit and PostgreSQL integration coverage
internal/workload/
  contract.go                     Mill CLI argument builder and parser
internal/coordinator/
  coordinator.go                  observe attempts and replenish free slots
internal/kubernetes/
  executor.go                     native Jobs, identity checks, and path mounts
migrations/
  000001_create_jobs.sql          initial job schema
  000002_create_tasks.sql         first task schema
  000003_refactor_job_input.sql   executable/input names and byte-range tasks
  000004_create_attempts.sql      durable execution attempts
  000005_add_task_retry_time.sql  durable retry eligibility timestamp
scripts/
  setup                           repeatable local kind environment setup
  demo-word-count-single-task      run one kind task and verify its output
  demo-word-count-batch            submit a full batch, observe, copy, and merge
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

These steps are wired together for the local Kubernetes prototype. PostgreSQL
row locks enforce each job's captured parallelism and at most one active
attempt per task. A dedicated PostgreSQL session advisory lock admits one
coordinator per database. Loss of that connection stops the process.

The coordinator polls once per second, reconstructing active attempts from the
database before claiming more tasks. Job names derive from attempt IDs, so an
uncertain create response or a restart can rediscover the same resource. Mill
records the Kubernetes UID and checks identity on later observations. Only
terminal `Complete`/`Failed` Job conditions release an execution slot. A
missing running Job or changed UID is reported as an error and retains its
slot for investigation. Do not delete active Jobs or change storage/cluster
configuration while recovering attempts.

The batch demo includes a live process-boundary check: it sends SIGKILL to Mill
with a full initial wave running, leaves PostgreSQL and Kubernetes untouched,
then starts a new Mill process with the same configuration. The demo compares
durable attempt IDs and recorded Kubernetes UIDs before and after restart,
checks that no replacement attempts were created, and verifies the completed
batch output. This demonstrates recovery from control-plane process loss; it
does not simulate database, cluster, node, or network failure.

This is basic local reconciliation, not full fault tolerance. Recovery of
`preparing` jobs, coordinator failover during network
partitions, and deleted-resource recovery remain future work.

### Retries and outputs

Execution can repeat, so a workload must tolerate duplicate
execution. Each attempt writes to its own output location and should publish
completion only after its output is complete. A retry creates a new attempt,
Kubernetes Job, and output path while retaining terminal attempt history.

The fixed prototype policy is **three total attempts per task**, with a
**five-second delay** after each observed terminal failure. These are server
constants, not submission options. Failed attempts stay `failed`; a task with
remaining attempts returns to `pending`, and its job stays `running`.
`tasks.available_at` persists the earliest next claim time in PostgreSQL.
Waiting retries count as pending progress, consume no running slot, and survive
process restart without an in-memory timer. A due retry still obeys parallelism.

```text
Task:    pending --> running --> completed
            ^          |
            |          +--> failed       (attempt limit reached)
            +----------+                 (retry available after delay)

Attempt: starting --> running --> completed / failed (terminal)
```

When a task exhausts the limit, Mill fails the task and job and stops claiming
pending work for that job. Already active attempts continue to be observed;
they cannot schedule more retries after the job fails. A repeated failure
observation does not reset the delay or affect a newer attempt. Only successful
attempt outputs appear in completed job results; partial failed outputs remain
on disk for inspection, not aggregation. This is not exactly-once execution or
protection against duplicate external side effects.

Kubernetes-native retries remain disabled (`backoffLimit: 0`,
`restartPolicy: Never`); Mill owns the retry budget. API timeouts and missing
running Jobs remain ambiguous observations, not terminal execution failures.
All definitive failures currently receive the same bounded policy, including
non-retryable configuration errors. Failure classification, configurable or
exponential backoff, automatic cleanup, and a public attempt-history API are
deliberately deferred. Attempt history is inspectable in PostgreSQL and logs.

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
| Distributed execution | Native Kubernetes Jobs/Pods through the official Go client |
| Local Kubernetes | kind, with versions pinned by `scripts/setup` |
| Cloud demonstration | AWS, likely Amazon EKS |
| Infrastructure as Code | Terraform when cloud deployment needs it |
| Testing | Go unit tests, PostgreSQL integration tests, later Kubernetes end-to-end tests |

Logging, metrics, dashboards, and tracing should be added only when a concrete
diagnostic or evaluation requirement justifies them.

## Local Kubernetes setup

Docker Engine is a machine-level prerequisite because installing its service
and configuring daemon permissions requires an explicit host decision. After
Docker works, Linux amd64 developers can prepare the remaining local Kubernetes
environment with one idempotent command:

```bash
./scripts/setup
```

The script verifies Docker access, installs the pinned `kind` and `kubectl`
versions under `${MILL_TOOLS_DIR:-$HOME/.local/bin}` when an exact version is
not already available, creates or reuses the `mill` kind cluster, selects its
kubectl context, waits for the node, and prints status. Downloads are verified
against their published SHA-256 checksums. It does not install Docker, change
Docker permissions, replace a cluster created with another node image, or
delete resources.

The cluster can be removed explicitly when it is no longer needed:

```bash
kind delete cluster --name mill
```

The setup command prepares the cluster. Execution is enabled separately with
`MILL_EXECUTOR=kubernetes`; the batch demonstration below configures it.

## Local quick start

The current slice requires Go 1.27 and PostgreSQL 18. Apply all numbered
migrations to a fresh local development database:

```bash
createdb mill
export MILL_DATABASE_URL='postgresql:///mill'
psql "$MILL_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/000001_create_jobs.sql \
  -f migrations/000002_create_tasks.sql \
  -f migrations/000003_refactor_job_input.sql \
  -f migrations/000004_create_attempts.sql \
  -f migrations/000005_add_task_retry_time.sql
```

For an existing database already at migration 000004, apply **only**
`migrations/000005_add_task_retry_time.sql` once before starting the updated
coordinator. Earlier migrations are not re-runnable. Existing terminal jobs
remain terminal; the retry change does not resurrect failed work. The demo
script applies all migrations automatically to its fresh private database.

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
go run ./examples/jsonl-copy/cmd/jsonl-copy \
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
  --file examples/jsonl-copy/cmd/jsonl-copy/Dockerfile \
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

### Word-count demonstration

The word-count example starts from a committed plain-text chapter and produces
one disposable JSONL input file:

```bash
go run ./examples/word-count/generate
```

The fixed configuration turns the first 60 parsed paragraphs into 12
deterministic, variable-size JSONL records at
`/tmp/mill-word-count/walden-economy.jsonl`. Submit that one file to Mill; do
not split it into shard files. With `MILL_PARALLELISM=3`, the planner creates 12
logical byte-range tasks. The Kubernetes adapter executes at most
three attempts concurrently, allowing shorter work to free capacity for the
next pending task.

The mapper and local result merger are independently testable. The control
plane launches mapper tasks; the example script merges their outputs. See
[`examples/word-count/README.md`](examples/word-count/README.md) for the token
rules and the boundary between demo input preparation and Mill's internal
partitioning.

With the local kind cluster ready, run one word-count task in a Pod:

```bash
./scripts/demo-word-count-single-task
```

The script stages the input inside the kind node, mounts it read-only, runs
the first record's byte range, and copies the output back for comparison with
a local run. It prints the saved result and rendered manifest paths. Each run
retains its Job and files for inspection. This manual smoke test does not use
the PostgreSQL task lifecycle.

For the complete batch through Mill's HTTP API and PostgreSQL coordinator:

```bash
./scripts/demo-word-count-batch
```

This starts private, temporary PostgreSQL 18 and Mill processes, submits the
12-record input, and processes 12 tasks with parallelism three. The script
merges all successful task outputs and verifies the result against a local
full-input count. It prints the output path and stops its processes on exit;
database files, Kubernetes Jobs, and results remain available for inspection.
Go, Docker, kind, kubectl, PostgreSQL 18 tools (`initdb`, `pg_ctl`, `psql`),
`curl`, and `jq` must be on PATH. Set `MILL_DEMO_PORT` if port 18080 is occupied.

`MILL_PARALLELISM=2 ./scripts/demo-word-count-batch` uses two active attempts.
The existing heuristic then groups the same 12 records into six tasks of two
records each. Record count and task count need not be identical.

To exercise automatic retries using a test-only wrapper around word-count:

```bash
./scripts/demo-word-count-batch --failure once
./scripts/demo-word-count-batch --failure always
```

Both target shard 0 deterministically. `once` fails its first attempt, then
verifies a successful retry and exact final counts despite a deliberately
incorrect failed-attempt output. `always` verifies the job fails after exactly
three attempts without publishing a successful job result. Each run saves
`attempts.json`, failed Pod logs, and final `status.json` in its printed run
directory. See [the example guide](examples/word-count/README.md#inject-a-failure-and-observe-retries)
for the wrapper's marker behavior. The normal mapper and workload CLI contract
are unchanged.

To exercise coordinator process recovery independently of workload failure:

```bash
./scripts/demo-word-count-batch --restart-coordinator
```

The test wrapper delays the initial Pods for 15 seconds. The script waits until
three attempts have durable external IDs, saves PostgreSQL and Kubernetes
identity snapshots, kills only the Mill child process with SIGKILL, and proves
the database and Jobs remain available. After restarting Mill, it requires the
attempt IDs, task IDs, attempt numbers, and Kubernetes Job UIDs to match. It
then verifies exactly 12 first attempts completed and the merged count equals
the local baseline. Recovery snapshots and the combined two-process server log
remain in the printed run directory.

### Enabling the local coordinator

The batch script sets these variables in addition to the normal database,
HTTP address, output root, and parallelism configuration:

| Variable | Purpose |
| --- | --- |
| `MILL_EXECUTOR=kubernetes` | Enable execution; unset keeps the API-only mode. |
| `MILL_KUBE_CONTEXT` | Explicit kubeconfig context, such as `kind-mill`. |
| `MILL_KUBE_NAMESPACE` | Namespace for Mill Jobs, such as `default`. |
| `MILL_KUBE_NODE` | Node holding staged data, such as `mill-control-plane`. |
| `MILL_LOCAL_ROOT` | Absolute local directory containing `input/` and `output/`. |
| `MILL_NODE_ROOT` | Corresponding absolute directory inside that node. |

Input URIs must be below `MILL_LOCAL_ROOT/input`; the configured output root
must be at or below `MILL_LOCAL_ROOT/output`. Stage identical input bytes under
`MILL_NODE_ROOT/input` before submission and keep them immutable. The node's
output directory must be writable by UID/GID 65532. Pods mount these directories
as `/data` (read-only) and `/output` (writable). This is node-local storage, not
a multi-node shared filesystem.

The adapter requires preloaded images (`imagePullPolicy: Never`), disables
native retries, and sets a five-minute attempt deadline. CPU/memory requests
and limits are small fixed prototype defaults. Successful workloads must
publish their assigned output before exiting zero. `GET /jobs/{id}` includes
ordered `results` with task IDs, attempt IDs, shard indices, and output URIs
after the whole job completes. The demo copies node outputs back to those
local paths; generic storage verification and aggregation remain planned.

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
  -f migrations/000004_create_attempts.sql \
  -f migrations/000005_add_task_retry_time.sql
MILL_TEST_DATABASE_URL='postgresql:///mill_test' go test -race ./...
```

## Execution lifecycle

1. The user submits an idempotent request containing an executable and one
   JSONL input URI.
2. Mill validates and identifies the input, chooses a task count from its
   configured parallelism, and plans record-aligned logical ranges.
3. Mill persists the job and atomically creates one pending task per range.
4. A coordinator claims a task and durably creates an attempt before asking
   Kubernetes to run it.
5. A workload container receives task identity, its input range, and a derived
   attempt output location through a small CLI contract.
6. Mill reconciles Kubernetes observations into durable attempt and task state.
7. A successful workload exits only after publishing its output; Mill records
   completion. A terminal execution failure queues a delayed retry; exhausting
   the three-attempt limit fails the task and job.
8. Job status aggregates the finalized task set and exposes output locations.

These steps are implemented for the staged local kind demonstration. Generic
output verification and cloud storage remain planned.

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
are tested.

### Milestone 3 — Kubernetes execution

**Implemented for the local demo:** one native Job per attempt, configured
parallelism, restart rediscovery, terminal outcome tracking, and output URI
listing. Staging uses one kind node; multi-node storage remains future work.

### Milestone 4 — Reliable execution

**In progress:** bounded retries, durable fixed-delay backoff, retained attempt
history, idempotent failure transitions, deterministic transient/permanent
failure demonstrations, and live coordinator-process restart recovery are
implemented. Dispatch crash-window expansion, runtime deletion recovery, and
cleanup remain planned.

### Milestone 5 — AWS deployment

Add S3 and a disposable demonstration environment using PostgreSQL and likely
EKS. Introduce Terraform with explicit teardown and cost control. **Planned.**

### Milestone 6 — Evaluation

Run an independent-record workload and measure throughput, elapsed time,
parallel scaling, and failure recovery. The workload is a test vehicle; Mill is
the project being evaluated. **Planned.**

## Current status

Mill has a working local Milestone 3 demonstration and the first Milestone 4
retry slice. Implemented behavior includes:

- one Go HTTP process with liveness and PostgreSQL-backed readiness;
- `POST /jobs` and `GET /jobs/{id}`;
- concurrency-safe idempotent submission;
- local JSONL validation, identity, record counting, and logical byte-range
  planning;
- atomic PostgreSQL job/task materialization;
- persisted task-state progress after restart;
- concurrency-safe task claims that enforce each job's parallelism;
- durable attempt lifecycle transitions and atomic task/job completion;
- bounded task retries with durable delay, fresh attempt outputs, and
  deterministic failure injection demos;
- a typed workload CLI argument contract;
- local JSONL-copy and word-count executables with non-root OCI images that
  process one assigned range;
- a deterministic Walden word-count input generator and local partial-result
  merger;
- a repeatable one-task kind smoke test that compares Pod and local outputs;
- an optional in-process coordinator and Kubernetes adapter;
- durable attempt rediscovery, stable Kubernetes identities, and a single
  coordinator ownership lock;
- live SIGKILL/restart recovery that reuses running attempts and Kubernetes
  Jobs without duplicate attempts;
- a batch demo that submits, executes, and merges all tasks, with final output
  verified against a local count; and
- an idempotent local kind and kubectl setup command.

Generic image inspection, storage-level output verification/aggregation, S3,
and full failure recovery are not implemented. Exhausting a task's retry budget
fails its job. Already active tasks are still observed to completion, while
pending tasks stop dispatching. Network partition failover, database or cluster
loss, deletion of a running Job, and crashes in narrower dispatch windows still
need dedicated end-to-end tests.

## Local and cloud development philosophy

Development starts locally so each domain and persistence decision can be
learned and tested in isolation. Manual Docker runs validate the container
contract; local kind runs exercise task coordination before adding recovery
policies and AWS. A cloud environment should be reproducible and disposable
rather than the default development setup.
