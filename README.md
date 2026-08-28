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
API ----------------------> PostgreSQL
  |                          jobs, tasks, durable execution state
  v
Coordinator
  |
  | create and reconcile native Kubernetes work
  v
Kubernetes
  +--> task 0 container --> S3 shard 0 / task 0 output
  +--> task 1 container --> S3 shard 1 / task 1 output
  +--> task 2 container --> S3 shard 2 / task 2 output
  `--> ...
```

The API and coordinator are logical responsibilities, not a commitment to
separate services. The first control plane should remain a single Go process
unless operational evidence justifies splitting it. PostgreSQL is the durable
source of truth for job and task metadata. S3 holds large datasets and outputs.
Kubernetes owns container placement, resource allocation, and pod lifecycle;
Mill owns the job abstraction, task intent, reconciliation, and progress view.

Kubernetes Indexed Jobs are a promising fit for shard-parallel execution and
will be evaluated before introducing any custom scheduling mechanism.

## Core concepts

- **Job**: A user request to run one workload over every shard in a dataset. It
  groups tasks and exposes aggregate progress and result locations.
- **Dataset**: A pre-partitioned input collection. V1 may represent it with a
  manifest containing an ordered list of S3 shard locations.
- **Shard**: One independently processable input object or location from the
  dataset manifest. Mill does not interpret the shard's data format.
- **Task**: Mill's durable record of the work for one job and one shard. A task
  moves through explicit execution states and may be retried after failure.
- **Workload/container**: A trusted OCI image that implements the user
  computation. Mill will define a small contract for passing task identity,
  input location, and output location to it.
- **Result**: Output written by a task to its assigned S3 location, with that
  location recorded in Mill's metadata.

## Proposed technology stack

| Concern | Direction |
| --- | --- |
| Control plane | Go, initially one process |
| External API | HTTP/REST |
| Durable metadata | PostgreSQL |
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

## Execution lifecycle

1. A user submits a trusted image reference, a dataset manifest location, an
   output destination, and execution settings such as parallelism.
2. Mill validates the request and persists the job in PostgreSQL.
3. Mill reads the manifest and creates one `pending` task for each shard.
4. The coordinator represents the pending work with native Kubernetes
   primitives and respects the configured parallelism.
5. Workload containers receive task identity plus input and output locations,
   read their shard from S3, and write results to S3.
6. Mill reconciles Kubernetes execution with durable task state, recording each
   task as `running`, then `completed` or `failed`.
7. Job progress is reported from task counts, and status responses expose known
   output locations.
8. Failed tasks may be retried. Retry limits, backoff, and attempt persistence
   will be defined during the reliability milestone rather than assumed now.

## Development milestones

### Milestone 0 — Project foundation

Document goals, non-goals, terminology, architectural boundaries, the job and
task model, the conceptual lifecycle, milestones, and repository conventions.
No application implementation is part of this milestone.

### Milestone 1 — Local single-process control plane

Implement the minimum Go domain model and REST API required to create and query
jobs. Load pre-partitioned shard descriptions, persist metadata in PostgreSQL,
and use a mock or local executor so control-plane behavior can be tested before
Kubernetes integration.

### Milestone 2 — Container workload contract

Define and document the minimal stable interface between Mill and workload
containers, covering job/task identity and S3 input/output locations. Provide a
small demonstration workload to verify the contract without expanding Mill's
scope.

### Milestone 3 — Kubernetes execution

Evaluate Kubernetes Indexed Jobs against the task model, then integrate the
smallest suitable native Job/Pod approach. Support configurable parallelism and
reconcile Kubernetes lifecycle information into Mill's task state.

### Milestone 4 — Reliable execution

Specify retry policy and execution attempts, make required state transitions
idempotent, and test recovery after task, coordinator, and Kubernetes failures.
Failures must be observable and must not silently lose durable job intent.

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

Mill is at project initialization. The architectural plan and development
conventions are documented, but no API, control plane, persistence layer,
executor, deployment, or workload contract has been implemented. All runtime
capabilities described above are planned.

## Local and cloud development philosophy

Development starts locally so domain behavior, persistence, and reconciliation
can be tested quickly and deterministically. Kubernetes and AWS integration
should be added in layers only after the preceding local behavior is understood.
A real AWS deployment is intended as a reproducible, temporary demonstration,
not as the default development environment. Cloud resources should be easy to
create for an evaluation and destroy afterward to limit cost.
