# Mill

Mill is a learning-oriented distributed batch execution system for running a
trusted OCI image over independent parts of a JSON Lines dataset. A user
submits an executable and one input URI; Mill validates and divides the input
into logical byte-range tasks, stores durable execution state in PostgreSQL,
and runs task attempts as Kubernetes Jobs. Inputs and outputs can use local
files or S3-compatible object storage.

Mill is an active technical-validation project, not a production service. Its
purpose is to make distributed-system behavior—durable intent, bounded
parallelism, retries, reconciliation, shared storage, and eventually service
and database failover—small enough to inspect and test.

## Why Mill

The workload computation is intentionally ordinary. The interesting problem is
the control plane around it: turning one request into durable independent work,
running that work concurrently, recovering when processes fail, and exposing
only successful outputs. PostgreSQL owns metadata and state transitions, S3
holds large data, and Kubernetes owns container placement and lifecycle. Mill
does not replace any of those systems.

## Current architecture

```text
User
  |
  | REST/JSON: submit or inspect a job
  v
Mill (one Go process today)
  |
  +--> streaming JSONL planner --> record-aligned byte ranges
  |
  +--> PostgreSQL --> jobs, tasks, attempts, retry eligibility
  |
  `--> coordinator --> Kubernetes Job per attempt
                           |
                           `--> workload Pod
                                  +--> ranged file/S3 input
                                  `--> per-attempt file/S3 output
```

The HTTP API and coordinator are still deployed together. The coordinator
persists an attempt before creating its deterministic Kubernetes Job, then
reconciles Kubernetes observations back into PostgreSQL. Durable per-attempt
leases fence stale coordinators and allow another process to take over an
expired lease while preserving the attempt and Kubernetes Job identity. For
S3-backed jobs, Pods need no hostPath volume or fixed-node selector and can use
shared object storage from any eligible node.

The planned service architecture separates a replicated Job service from
replicated executor workers. The Job service will retain the planner and sole
ownership of metadata tables; executors will claim and report leased attempts
through an internal Protobuf/gRPC API. This is planned, not implemented. See
[Architecture](docs/architecture.md) for the domain model, correctness rules,
resource-class proposal, and availability design.

## V1 scope

- Submit and retrieve jobs through HTTP/REST.
- Accept a trusted OCI image and one JSONL input.
- Plan record-aligned logical shards internally.
- Store metadata and execution state in PostgreSQL.
- Store datasets and attempt outputs through `file://` or `s3://` URIs.
- Execute attempts as Kubernetes Jobs with bounded parallelism.
- Track pending, running, completed, and failed tasks.
- Retry terminal failures up to three total attempts.
- Return successful attempt output locations after job completion.

V1 does not include untrusted-code sandboxing, multi-tenant isolation, arbitrary
data formats, streaming pipelines, task graphs, a custom scheduler, Kafka,
Redis, a service mesh, or production-readiness claims.

## Quick start

Prepare the pinned local kind and kubectl environment after Docker Engine is
installed and accessible:

```bash
./scripts/setup
```

Run the complete batch demonstration with node-local files:

```bash
./scripts/demo-word-count-batch
```

Run two Mill processes and kill the active coordinator:

```bash
./scripts/demo-word-count-batch --replica-failover
```

Run the shared-storage demonstration:

```bash
./scripts/demo-word-count-s3
```

The S3 demonstration starts disposable PostgreSQL and S3-compatible SeaweedFS
processes, submits 12 logical tasks, runs at most three Pods concurrently, and
verifies the merged S3 results against a local full-input count. It retains its
printed result directory and Kubernetes Jobs for inspection while removing its
temporary credentials and storage container.

Run the unit test suite with:

```bash
go test ./...
```

Installation, API-only operation, environment variables, demonstrations,
integration tests, and the repository layout are documented in
[Development](docs/development.md).

## Current status

Implemented:

- one Go HTTP process with liveness and PostgreSQL-backed readiness;
- idempotent `POST /jobs` and `GET /jobs/{id}` endpoints;
- streaming JSONL validation, SHA-256 identity, and logical byte-range planning;
- local-file and S3-compatible input/output adapters;
- atomic job/task materialization and durable progress;
- concurrency-safe task claims and attempt state transitions;
- one native Kubernetes Job per attempt through the official Go client;
- bounded retries with durable five-second delay and separate attempt outputs;
- durable attempt leases, renewal, expiry takeover, and stale-owner fencing;
- deterministic Kubernetes identity and coordinator restart reconciliation;
- trusted workload CLI contract and non-root example images;
- local, container, single-task, full-batch, retry, restart, replica-failover,
  and S3-backed word-count demonstrations; and
- exact result verification against a local baseline.

Not implemented:

- separate Job and executor services or gRPC;
- a packaged Kubernetes multi-replica deployment;
- named workload resource classes;
- replicated PostgreSQL or multi-node K3s deployment;
- network-partition or physical-node failure tests;
- generic aggregation or validation of arbitrary workload outputs;
- AWS/EKS deployment, Terraform, or CI/CD; and
- production security, operations, or availability guarantees.

## Milestones

### 0 — Foundation — implemented

Define goals, non-goals, terminology, architecture, lifecycle, and repository
conventions.

### 1 — Local control plane — implemented

Create and retrieve jobs, persist metadata in PostgreSQL, plan JSONL shards,
materialize tasks, and report progress.

### 2 — Workload contract — implemented

Define a stable CLI contract for one task attempt, build trusted reference
images, and verify assigned byte-range behavior.

### 3 — Kubernetes execution — implemented locally

Create and observe one native Kubernetes Job per Mill attempt, enforce job
parallelism, and expose successful output URIs.

### 4 — Reliable execution — in progress

Bound retries, preserve attempt history, delay retry eligibility durably, and
recover the same Kubernetes identities after coordinator process loss. Use
durable leases to renew or transfer attempt ownership and reject stale state
changes. Wider dispatch crash windows, resource deletion, long API stalls, and
network ambiguity remain.

### 5 — Shared object storage — implemented locally

Read and plan JSONL through S3-compatible storage, use HTTP byte-range requests
inside workload Pods, and publish unique attempt outputs without hostPath or
node pinning. Real AWS S3 remains untested.

### 6 — Service boundary and resource classes — planned

Split the current process into a replicated Job service and replicated executor
workers. Keep planning inside the Job service, make it the sole metadata owner,
and expose the existing lease operations through a versioned Protobuf/gRPC
domain API. Add optional named
`small`, `medium`, and `large` workload classes; persist their resolved
resources so retries remain stable.

### 7 — Two-laptop replica availability — planned

Use one K3s server and one K3s agent. Spread two Job-service replicas and two
executor replicas across the laptops. Run a CloudNativePG primary and standby
with availability-oriented synchronous replication. Demonstrate individual
Mill Pod failure and controlled PostgreSQL Pod promotion. This stage will not
claim whole-laptop or network-partition tolerance.

### 8 — Three-node quorum availability — planned

Add a third independent failure domain, run three K3s server/etcd voters, and
place one PostgreSQL instance on each node. Use required synchronous replication
and failover quorum, then test one physical-node loss and an isolated minority
without conflicting writers or duplicate attempts. Full-stack claims also
require replicated object storage or AWS S3.

### 9 — CI/CD and disposable AWS deployment — planned

Run formatting and tests continuously. Make Terraform deployment and teardown
manual, deploy a temporary AWS demonstration, collect evidence, and destroy all
billable resources afterward.

### 10 — Evaluation — planned

Measure throughput, scaling with parallelism, failure interruption,
reconciliation time, and memory use for different workload classes.

## Development philosophy

Each milestone should prove one behavior locally before adding another failure
boundary. A service exists only when it has distinct ownership or scaling
needs; a Go package does not automatically become a Pod. Availability claims
must name the exact failure survived. Cloud infrastructure should be
reproducible, manually activated, and disposable.

Further documentation:

- [Architecture](docs/architecture.md)
- [Development and demonstrations](docs/development.md)
- [Word-count example](examples/word-count/README.md)
- [Agent and contribution conventions](AGENTS.md)
