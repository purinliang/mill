# Mill

Mill is a learning-oriented distributed batch execution system for running a
trusted OCI image over independent parts of a JSON Lines dataset. A user
submits an executable and one input URI; Mill validates and partitions the
input into logical byte-range tasks, stores durable state in PostgreSQL,
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
                         +-----------------------+
User ------ REST/JSON -->| Job service           |--> PostgreSQL
Execution ------ gRPC -->| REST + gRPC endpoints |--> dataset partitioner
                         +-----------------------+       |
       |                                                 `--> JSONL ranges
       v
Kubernetes Job per attempt --> workload Pod
                                  +--> ranged file/S3 input
                                  `--> per-attempt file/S3 output
```

The Job service owns REST, partitioning, PostgreSQL, and the internal execution
API; it does not import the coordinator or Kubernetes adapter. Standalone
execution replicas access state exclusively through bounded Protobuf/gRPC
calls. An execution replica persists an attempt through the Job service before
creating its deterministic Kubernetes Job, then reconciles Kubernetes
observations back into PostgreSQL. Durable per-attempt leases fence stale
execution replicas and allow another replica to take over an expired lease
while preserving the attempt and Kubernetes Job identity. For S3-backed jobs,
Pods need no hostPath volume or fixed-node selector and can use shared object
storage from any eligible node. See
[Architecture](docs/architecture.md) for the domain model, correctness rules,
resource-class policy, and availability design. The physical-node procedure
and required evidence are in the [Availability runbook](docs/availability-runbook.md).

## V1 scope

- Submit and retrieve jobs through HTTP/REST.
- Accept a trusted OCI image and one JSONL input.
- Partition the input into record-aligned logical shards internally.
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
./scripts/setup.sh
```

Build the two Mill control-plane images and verify their runtime identities:

```bash
./scripts/build-control-plane-images.sh
```

This produces `mill/job:dev` and `mill/execution:dev`. No Kubernetes
deployment is created by this command.

After preparing a Pod-reachable, migrated PostgreSQL database and shared S3
storage, deploy the single-replica local control plane with:

```bash
export MILL_DATABASE_URL='postgresql://mill:password@pod-reachable-host:5432/mill'
export MILL_OUTPUT_ROOT_URI='s3://mill-output'
export AWS_REGION='us-east-1'
./scripts/deploy-local-control-plane.sh
```

This is a single-node local baseline, not an HA configuration. See
[Development](docs/development.md) for storage, credentials, and cleanup.

Run the complete batch demonstration with node-local files:

```bash
./scripts/demo-word-count-batch.sh
```

Run the same batch through one Job process and two standalone execution
replicas communicating over gRPC:

```bash
./scripts/demo-word-count-batch.sh --split-process
```

Run two execution replicas and kill the active lease owner:

```bash
./scripts/demo-word-count-batch.sh --replica-failover
```

Run the shared-storage demonstration:

```bash
./scripts/demo-word-count-s3.sh
```

Run the same 12-task S3 workload with both Mill services deployed as Pods:

```bash
./scripts/demo-word-count-deployed.sh
```

Delete an active execution Pod and prove fenced takeover by another replica:

```bash
./scripts/demo-word-count-deployed.sh --execution-failover
```

Delete the original Job Pod and prove REST/gRPC reconnection:

```bash
./scripts/demo-word-count-deployed.sh --job-failover
```

The S3 demonstration starts disposable PostgreSQL and S3-compatible SeaweedFS
processes, submits 12 logical tasks, runs at most three Pods concurrently, and
verifies the merged S3 results against a local full-input count. It retains its
printed result directory and Kubernetes Jobs for inspection while removing its
temporary credentials and storage container.

The deployed variation runs both Mill services as Pods in unique namespaces,
captures their diagnostics, and removes only its own namespaces and fixture
containers after exact result verification. Its failover mode scales the
execution Deployment to two replicas and proves recovery from one active
execution Pod deletion. Its Job mode similarly scales that Deployment,
deletes the original REST/gRPC endpoint, and proves reconnection through the
Service. Neither mode tests database, storage, node, or network failure.

Run the unit test suite with:

```bash
go test ./...
```

Installation, API-only operation, environment variables, demonstrations,
integration tests, and the repository layout are documented in
[Development](docs/development.md).

## Status

The local control plane, split Job and execution services, Kubernetes task
execution, retries, leases, S3-compatible storage, and single-Pod failure
demonstrations are implemented. Physical multi-node availability, AWS
deployment, and production hardening are not. See the
[roadmap](docs/roadmap.md) for detailed evidence and remaining milestones.

## Documentation

See [`docs/`](docs/) for architecture, roadmap, and developer operations.
Package-specific responsibilities and agent instructions live in the nearest
`README.md` and `AGENTS.md` beside the code.
