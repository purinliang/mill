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
Job service
  |
  +--> streaming JSONL planner --> record-aligned byte ranges
  |
  +--> PostgreSQL --> jobs, tasks, attempts, retry eligibility
  |
  `-- gRPC --> executor replica(s) --> Kubernetes Job per attempt
                                         |
                                         `--> workload Pod
                                                +--> ranged file/S3 input
                                                `--> per-attempt file/S3 output
```

The Job service owns REST, planning, PostgreSQL, and the internal execution API;
it does not import the coordinator or Kubernetes adapter. Standalone executor
replicas access execution state exclusively through bounded Protobuf/gRPC calls.
An executor persists an attempt through the Job service before creating its
deterministic Kubernetes Job, then reconciles Kubernetes observations back into
PostgreSQL. Durable per-attempt leases fence stale executors and allow a replica
to take over an expired lease while preserving the attempt and Kubernetes Job
identity. For S3-backed jobs, Pods need no hostPath volume or fixed-node selector
and can use shared object storage from any eligible node. See
[Architecture](docs/architecture.md) for the domain model, correctness rules,
resource-class policy, and availability design. The physical-node procedure
and required evidence are in the [Availability runbook](docs/availability-runbook.md).

## V1 scope

- Submit and retrieve jobs through HTTP/REST.
- Accept a trusted OCI image and one JSONL input.
- Plan record-aligned logical shards internally.
- Store metadata and execution state in PostgreSQL.
- Store datasets and attempt outputs through `file://` or `s3://` URIs.
- Execute attempts as Kubernetes Jobs with bounded parallelism.
- Select a server-defined `small`, `medium`, or `large` workload resource class.
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

Build the two Mill control-plane images and verify their runtime identities:

```bash
./scripts/build-control-plane-images
```

This produces `mill/job-service:dev` and `mill/executor:dev`. No Kubernetes
deployment is created by this command.

After preparing a Pod-reachable, migrated PostgreSQL database and shared S3
storage, deploy the single-replica local control plane with:

```bash
export MILL_DATABASE_URL='postgresql://mill:password@pod-reachable-host:5432/mill'
export MILL_OUTPUT_ROOT_URI='s3://mill-output'
export AWS_REGION='us-east-1'
./scripts/deploy-local-control-plane
```

This local deployment is a Milestone 7 baseline, not an HA configuration. See
[Development](docs/development.md) for its storage, credential, and cleanup
requirements.

Run the complete batch demonstration with node-local files:

```bash
./scripts/demo-word-count-batch
```

Run the same batch through one Job-service process and two standalone executor
replicas communicating over gRPC:

```bash
./scripts/demo-word-count-batch --split-process
```

Run two executor replicas and kill the active lease owner:

```bash
./scripts/demo-word-count-batch --replica-failover
```

Run the shared-storage demonstration:

```bash
./scripts/demo-word-count-s3
```

Run the same 12-task S3 workload with both Mill services deployed as Pods:

```bash
./scripts/demo-word-count-deployed
```

Delete an active executor Pod and prove fenced takeover by another replica:

```bash
./scripts/demo-word-count-deployed --executor-failover
```

Delete the original Job-service Pod and prove REST/gRPC reconnection:

```bash
./scripts/demo-word-count-deployed --job-service-failover
```

The S3 demonstration starts disposable PostgreSQL and S3-compatible SeaweedFS
processes, submits 12 logical tasks, runs at most three Pods concurrently, and
verifies the merged S3 results against a local full-input count. It retains its
printed result directory and Kubernetes Jobs for inspection while removing its
temporary credentials and storage container.

The deployed variation runs both Mill services as Pods in unique namespaces,
captures their diagnostics, and removes only its own namespaces and fixture
containers after exact result verification. Its failover mode scales the
executor Deployment to two replicas and proves recovery from one active
executor Pod deletion. Its Job-service mode similarly scales that Deployment,
deletes the original REST/gRPC endpoint, and proves reconnection through the
Service. Neither mode tests database, storage, node, or network failure.

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
- backend-independent execution types and store contract;
- versioned execution Protobuf schema and tested gRPC client/server adapters;
- optional Job-side gRPC listener with bounded messages and graceful shutdown;
- separately runnable executor process with no PostgreSQL dependency;
- minimal non-root OCI images for the Job service and executor;
- explicit executor support for either a kubeconfig context or in-cluster
  service-account credentials;
- a single-node kind deployment with separate control-plane/workload
  namespaces, health probes, resource bounds, and namespace-scoped executor
  RBAC;
- demonstrated 12-task split-process execution through one Job service and two
  live executor replicas;
- demonstrated executor-process failover with lease-token replacement and
  stable attempt and Kubernetes Job identities;
- demonstrated active executor Pod deletion and fenced takeover by another
  deployed replica without duplicate attempts or Kubernetes Jobs;
- demonstrated Job-service Pod deletion with REST/gRPC reconnection and stable
  durable execution identities;
- deterministic Kubernetes identity and executor restart reconciliation;
- durable workload resource classes propagated through gRPC to Kubernetes;
- trusted workload CLI contract and non-root example images;
- local, container, single-task, full-batch, retry, restart, replica-failover,
  S3-backed, and fully deployed word-count demonstrations; and
- exact result verification against a local baseline.

Not implemented:

- service authentication for the internal gRPC boundary;
- multi-node replica placement and anti-affinity configuration;
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
recover the same Kubernetes identities after executor process loss. Use
durable leases to renew or transfer attempt ownership and reject stale state
changes. Wider dispatch crash windows, resource deletion, long API stalls, and
network ambiguity remain.

### 5 — Shared object storage — implemented locally

Read and plan JSONL through S3-compatible storage, use HTTP byte-range requests
inside workload Pods, and publish unique attempt outputs without hostPath or
node pinning. Real AWS S3 remains untested.

### 6 — Service boundary and resource classes — implemented locally

The pure execution domain contract, versioned Protobuf/gRPC adapters, Job-side
listener, standalone executor, removal of the direct path, and executor
failover proof are implemented. Optional named `small`, `medium`, and `large`
workload classes persist their resolved CPU and memory resources so retries
remain stable if server profiles change later.

### 7 — Two-laptop replica availability — in progress

Use one K3s server and one K3s agent. Spread two Job-service replicas and two
executor replicas across the laptops. Run a CloudNativePG primary and standby
with availability-oriented synchronous replication. Demonstrate individual
Mill Pod failure and controlled PostgreSQL Pod promotion. This stage will not
claim whole-laptop or network-partition tolerance.

The single-node prerequisite is implemented: both packaged services run as
one-replica Deployments, communicate through a ClusterIP Service, and isolate
workload Jobs in a namespace where the executor may only create and get Jobs.
The complete 12-task S3 workload has run through these deployed services with
exact result verification and bounded parallelism. A single-node test also
scales the executor to two replicas, deletes the Pod that owns three active
leases, and proves takeover with new fencing tokens while attempt IDs,
Kubernetes Job names, and Job UIDs remain unchanged. Another single-node test
deletes the original Job-service Pod and proves the REST client and executor's
gRPC connection recover through the Service without changing durable work.
Multi-node replica placement, K3s installation, and database replication remain
to be exercised. The first deployable manifests are now defined under
`deploy/kubernetes/availability`: two Mill replicas per service, required
hostname anti-affinity, disruption budgets, and distinct two- and three-node
CloudNativePG profiles. Both profiles pass Kubernetes and CloudNativePG 1.30.0
admission validation; neither has yet been run on multiple nodes.

Continue with focused reviews after each slice, but defer the overall
architecture and code-ownership refactor until after Milestone 8.

### 8 — Three-node quorum availability — planned

Add a third independent failure domain, run three K3s server/etcd voters, and
place one PostgreSQL instance on each node. Use required synchronous replication
and failover quorum, then test one physical-node loss and an isolated minority
without conflicting writers or duplicate attempts. Full-stack claims also
require replicated object storage or AWS S3.

The three-instance CloudNativePG manifest is implemented and admission-tested,
but the three-node runtime and its failure evidence remain planned.

After this milestone, perform the overall review and refactor using evidence
from both the two-laptop and three-node systems. Preserve Pod failure, database
promotion, node-loss, and minority-isolation evidence as regression tests.

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
