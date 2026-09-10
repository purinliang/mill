# Mill

Mill is a learning-oriented distributed batch system that runs a trusted OCI
image over independent parts of one JSON Lines dataset. It partitions the
input into logical byte-range tasks, stores durable state in PostgreSQL, and
runs task attempts as Kubernetes Jobs. Data remains in local or S3-compatible
object storage.

Mill is an active technical-validation project, not a production service. Its
purpose is to make durable intent, bounded parallelism, retries,
reconciliation, and failure recovery observable and testable.

## Architecture

```text
User --REST--> Job service -----------> PostgreSQL
                  |
                  `-- gRPC <--------- Execution replicas
                                           |
                                           v
                                    Kubernetes Jobs
                                           |
                                    workload Pods
                                           |
                                      file or S3
```

The Job service owns submission, partitioning, progress, and durable lease
transitions in PostgreSQL. Execution replicas use those leases while
reconciling Kubernetes. Fencing lets another replica take over an expired
lease without creating a new attempt or Kubernetes Job. Kubernetes remains
responsible for container placement and lifecycle.

See [Architecture](docs/architecture.md) for the domain model and correctness
rules.

## V1 scope

- Submit and retrieve jobs through HTTP/REST.
- Accept a trusted OCI image and one JSONL input.
- Partition input into record-aligned logical shards.
- Store metadata and execution state in PostgreSQL.
- Store datasets and outputs through `file://` or `s3://` URIs.
- Execute Kubernetes Jobs with bounded parallelism and resource classes.
- Track task states, retry failures, and return successful output locations.

V1 excludes untrusted-code isolation, multi-tenancy, arbitrary data formats,
generic aggregation, streaming pipelines, custom scheduling, Kafka, Redis,
service mesh, and production-readiness claims.

## Quick start

Prepare the pinned local kind and kubectl environment after installing Docker:

```bash
./scripts/setup.sh
go test ./...
./scripts/demo-word-count-batch.sh
```

Other useful demonstrations are:

```bash
./scripts/demo-word-count-s3.sh
./scripts/demo-word-count-deployed.sh
./scripts/demo-word-count-deployed.sh --execution-failover
./scripts/demo-word-count-deployed.sh --job-failover
```

See [Setup](docs/development/setup.md),
[Testing](docs/development/testing.md), and
[Local deployment](docs/deployment/local.md) for operational instructions.

## Status

Implemented locally: REST submission and status, JSONL partitioning,
PostgreSQL state, Kubernetes execution, retries, leases and fencing, gRPC
service separation, S3-compatible storage, resource classes, and single-Pod
failure demonstrations.

Planned: real AWS S3, three-node K3s and PostgreSQL quorum evidence, CI/CD,
evaluation, and production hardening. See the [roadmap](docs/roadmap.md) and
planned [AWS deployment](docs/deployment/aws.md).

## Documentation

- [Architecture](docs/architecture.md)
- [Setup](docs/development/setup.md)
- [Testing](docs/development/testing.md)
- [Local deployment](docs/deployment/local.md)
- [Roadmap](docs/roadmap.md)
- [Availability](docs/deployment/availability.md)
- [Agent and contribution rules](AGENTS.md)
