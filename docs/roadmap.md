# Roadmap

This file preserves milestone intent and evidence as the implementation
changes. An **implemented locally** milestone has local evidence only; it does
not make a production, cloud, or physical-node availability claim.

## Current status

Implemented:

- HTTP submission, status, liveness, and PostgreSQL-backed readiness;
- streaming JSONL validation, identity, and logical partitioning;
- durable jobs, tasks, attempts, retries, leases, and fencing;
- native Kubernetes Job execution with deterministic attempt identity;
- a Protobuf/gRPC boundary between Job and execution services;
- local-file and S3-compatible inputs and outputs;
- workload resource classes and non-root example images; and
- deterministic process, Pod, retry, and output demonstrations.

Not yet demonstrated on the target environment:

- physical multi-node placement and PostgreSQL replication;
- node loss or network-minority behavior;
- real AWS S3, EC2, ECR, and Terraform deployment;
- CI/CD and measured scaling; and
- production security or availability guarantees.

## 0 — Foundation — implemented

Defined goals, non-goals, terminology, architecture, lifecycle, and repository
conventions before adding application code.

## 1 — Local control plane — implemented

Added job submission and retrieval, PostgreSQL persistence, JSONL validation,
logical partitioning, task materialization, and durable progress.

## 2 — Workload contract — implemented

Defined stable task arguments for identity, input range, output URI, and
unchanged user arguments. Reference images verify the byte-range contract.

## 3 — Kubernetes execution — implemented locally

Added one native Kubernetes Job per attempt, bounded job parallelism, resource
limits, successful output locations, and deterministic external identities.

## 4 — Reliable execution — in progress

Mill retains attempt history, waits five durable seconds before retry, and
allows three attempts per task. Execution replicas lease active attempts,
renew ownership, take over expired leases, and fence stale writers.

Process restart, concurrent-replica takeover, and Pod deletion have preserved
attempt and Kubernetes Job identities. Wider Kubernetes API ambiguity,
resource deletion, long stalls, and network partitions remain unproven.

## 5 — Shared object storage — implemented locally

Added streaming and ranged S3-compatible access and unique attempt outputs.
The complete batch runs without hostPath or node pinning against disposable
SeaweedFS. Real AWS S3 remains untested.

## 6 — Service boundary and resources — implemented locally

Separated the Job and execution processes through versioned Protobuf/gRPC.
Only the Job service accesses PostgreSQL; execution replicas own Kubernetes
reconciliation. Server-defined resource classes persist resolved values so a
retry is not changed by later configuration.

## 7 — Replica availability — in progress

The single-node deployment runs both Mill services as Pods. Tests delete an
active execution Pod or Job Pod and require takeover or reconnection without
new attempts or Kubernetes Jobs.

A two-node kind simulation also passed synchronous PostgreSQL promotion and
exact output recovery. The K3s installer, two-node manifests, and destructive
acceptance runner exist, but they have not run on two physical laptops. This
milestone therefore claims Pod recovery only, not whole-node tolerance.

The operational procedure and required evidence are in the
[availability](deployment/availability.md).

## 8 — Disposable AWS three-node quorum — planned

Use Terraform to provision three EC2 nodes across three Availability Zones.
Run one K3s server/etcd voter and one CloudNativePG instance on each node. Use
S3 for input and output objects and ECR for immutable images.

First verify the local workload against real S3. Then run the 12-task batch on
the cluster and test Pod loss, PostgreSQL primary loss, one EC2-node loss, and
minority isolation. The majority must continue, the minority must refuse
authoritative writes, PostgreSQL must retain one writer, and execution
identities and final output must remain exact.

The three-instance database manifest is admission-tested, but the AWS
infrastructure and runtime evidence do not exist yet. See the target
[AWS deployment](deployment/aws.md).

After this milestone, perform the next broad architecture review using the new
Pod, database, node-loss, and minority-isolation evidence.

## 9 — CI/CD and deployment automation — planned

Continuously run formatting and tests. Make installation, evidence capture,
and teardown repeatable while keeping billable Terraform apply and destroy
manual.

## 10 — Evaluation — planned

Measure throughput, scaling with parallelism, failure interruption,
reconciliation time, and memory use for each workload class.
