# Word-count demonstration

This trusted workload demonstrates Mill's partitioning, bounded parallelism,
shared storage, retries, and recovery. It is a correctness fixture rather than
a throughput benchmark.

## Input and output

`walden-economy.txt` contains Chapter 1 of Henry David Thoreau's *Walden*, from
[Project Gutenberg eBook 205](https://www.gutenberg.org/ebooks/205). Generate
the JSONL input with:

```bash
go run ./examples/word-count/generate
```

`record-config.json` deterministically groups the first 60 parsed paragraphs
into 12 variable-size JSONL records. The generated file is not committed. This
is input preparation, not Mill partitioning: the user submits one JSONL object,
and Mill creates logical byte ranges without copying physical shard files.

The mapper lowercases ASCII alphanumeric tokens. A single hyphen remains only
when it joins two token segments, so `well-known` is one word. Each task writes
sorted records such as:

```json
{"word":"well-known","count":2}
```

The local `cmd/merge` command combines partial counts after every Mill task
succeeds. Aggregation remains example behavior rather than control-plane logic.

## Demonstrations

Run one manually configured Kubernetes task:

```bash
./scripts/demo-word-count-single-task.sh
```

This compares one Pod's output with a local invocation. It does not submit a
Mill job or use PostgreSQL.

Run the complete 12-task batch through the Job and execution services:

```bash
./scripts/demo-word-count-batch.sh
```

The default parallelism is three. The script starts disposable PostgreSQL,
submits one job, runs Kubernetes Jobs, merges successful outputs, and compares
the result with a local full-input count.

Useful batch variants are:

```bash
./scripts/demo-word-count-batch.sh --split-process
./scripts/demo-word-count-batch.sh --restart-coordinator
./scripts/demo-word-count-batch.sh --replica-failover
./scripts/demo-word-count-batch.sh --failure once
./scripts/demo-word-count-batch.sh --failure always
MILL_DEMO_RESOURCE_CLASS=medium ./scripts/demo-word-count-batch.sh
```

These respectively exercise two execution replicas, process restart, lease
takeover, recoverable failure, retry exhaustion, and workload resource policy.
Failure injection is deterministic: shard zero fails once or on every attempt.
Mill allows three attempts per task and excludes failed outputs.

Run the batch through a temporary S3-compatible service:

```bash
./scripts/demo-word-count-s3.sh
```

This verifies ranged S3 reads, unique attempt outputs, and scheduling without
hostPath volumes or fixed-node selection.

Run both Mill services as Kubernetes Deployments:

```bash
./scripts/demo-word-count-deployed.sh
./scripts/demo-word-count-deployed.sh --execution-failover
./scripts/demo-word-count-deployed.sh --job-failover
```

The failure variants delete one active service Pod and require the surviving
replica to finish the same task attempts and Kubernetes Jobs. PostgreSQL,
object storage, and the single kind node remain healthy, so these are Pod-level
recovery tests rather than infrastructure availability claims.

## Results and inspection

Every script prints its temporary run directory and cleanup commands. Depending
on the mode, the directory contains final status, task and attempt snapshots,
service logs, Kubernetes identities, and merged counts. Successful modes pass
only when the merged result matches the local baseline byte-for-byte.

Completed Kubernetes Jobs may be retained for inspection:

```bash
kubectl --context kind-mill -n default get jobs,pods \
  -l mill.dev/job-id=<job-id>
kubectl --context kind-mill -n default delete jobs \
  -l mill.dev/job-id=<job-id>
```

Multi-node deployment and failure evidence are documented in the
[availability](../../docs/deployment/availability.md).
