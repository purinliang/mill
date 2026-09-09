# Mill roadmap

This document records the staged development path. A milestone marked
**implemented locally** has local evidence only; it is not a production or
cloud availability claim.

## Current status

Implemented:

- HTTP liveness, PostgreSQL-backed readiness, submission, and status;
- streaming JSONL validation, identity, and logical partitioning;
- local-file and S3-compatible input and output adapters;
- atomic task materialization and durable progress;
- concurrency-safe claims, retries, attempt history, leases, and fencing;
- native Kubernetes Job execution with deterministic attempt identity;
- a versioned Protobuf/gRPC boundary between Job and execution services;
- workload resource classes propagated from submission to Kubernetes;
- minimal non-root images and a single-node kind deployment; and
- exact-result demonstrations for local, S3-compatible, split-process, restart,
  retry, execution Pod failure, and Job Pod failure paths.

Not implemented or not yet demonstrated on the target environment:

- internal gRPC authentication;
- physical multi-node replica placement and PostgreSQL replication;
- network-partition or physical-node failure tests;
- arbitrary workload-output aggregation;
- AWS/EKS, Terraform, or CI/CD; and
- production security, operations, or availability guarantees.

## 0 — Foundation — implemented

Define goals, non-goals, terminology, architecture, lifecycle, and repository
conventions.

## 1 — Local control plane — implemented

Create and retrieve jobs, persist metadata in PostgreSQL, partition JSONL
input, materialize tasks, and report progress.

## 2 — Workload contract — implemented

Define a stable CLI contract for one task attempt, build trusted reference
images, and verify assigned byte-range behavior.

## 3 — Kubernetes execution — implemented locally

Create and observe one native Kubernetes Job per Mill attempt, enforce job
parallelism, and expose successful output URIs.

## 4 — Reliable execution — in progress

Bound retries, preserve attempt history, delay retry eligibility durably, and
recover the same Kubernetes identities after execution process loss. Durable
leases renew or transfer attempt ownership and reject stale state changes.
Wider dispatch crash windows, resource deletion, long API stalls, and network
ambiguity remain.

## 5 — Shared object storage — implemented locally

Read and partition JSONL through S3-compatible storage, use byte-range requests
inside workload Pods, and publish unique attempt outputs without hostPath or
node pinning. Real AWS S3 remains untested.

## 6 — Service boundary and resource classes — implemented locally

The execution domain contract, versioned Protobuf/gRPC adapters, Job-side
listener, standalone execution service, direct-path removal, and execution
failover proof are implemented. Named workload classes persist resolved CPU
and memory values so retries remain stable if server profiles change.

## 7 — Two-laptop replica availability — in progress

Use one K3s server and one K3s agent. Spread two Job replicas and two execution
replicas across the laptops. Run a CloudNativePG primary and standby with
availability-oriented synchronous replication. Demonstrate individual Mill
Pod failure and controlled PostgreSQL Pod promotion. This stage will not claim
whole-laptop or network-partition tolerance.

The single-node prerequisite is implemented. Both services run as Deployments
and communicate through a ClusterIP Service. Workload Jobs are isolated in a
namespace where the execution service may only create and get Jobs. The
12-task S3 demonstration verifies exact results and bounded parallelism.

Single-node tests have also demonstrated execution Pod deletion with fenced
lease takeover and Job Pod deletion with REST/gRPC reconnection. Attempt IDs,
Kubernetes Job names, Job UIDs, and durable work remain stable.

Pinned role-based K3s installation, availability manifests, and a destructive
two-node acceptance runner are implemented but have not run on two physical
laptops. A two-node kind simulation passed Mill Pod deletion, synchronous
standby promotion, connection recovery, stable execution identities, and
byte-exact output. This validates the manifests, not a physical-laptop failure
domain.

The manifests under `deploy/kubernetes/availability` define two replicas of
each Mill service, required hostname anti-affinity, disruption budgets, and
separate two- and three-node CloudNativePG profiles. Both profiles pass
Kubernetes and CloudNativePG 1.30.0 admission validation.

Continue focused reviews after each slice, but defer the next overall
architecture refactor until Milestone 8 supplies new failure evidence.

## 8 — Three-node quorum availability — planned

Add a third independent failure domain, run three K3s server/etcd voters, and
place one PostgreSQL instance on each node. Use required synchronous
replication and failover quorum, then test one physical-node loss and an
isolated minority without conflicting writers or duplicate attempts.
Full-stack claims also require replicated object storage or AWS S3.

The three-instance CloudNativePG manifest is implemented and admission-tested,
but the three-node runtime and failure evidence remain planned.

After this milestone, perform the overall review using evidence from both the
two-laptop and three-node systems. Preserve Pod failure, database promotion,
node-loss, and minority-isolation evidence as regression tests.

## 9 — CI/CD and disposable AWS deployment — planned

Run formatting and tests continuously. Make Terraform deployment and teardown
manual, deploy a temporary AWS demonstration, collect evidence, and destroy
all billable resources afterward.

## 10 — Evaluation — planned

Measure throughput, scaling with parallelism, failure interruption,
reconciliation time, and memory use for different workload classes.
