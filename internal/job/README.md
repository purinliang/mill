# Job package

`internal/job` owns submission policy, dataset partitioning orchestration,
durable Job status, and the contracts required by that workflow.

```text
HTTP adapter
    |
    v
job.Service
    +--> normalize and validate submission
    +--> resolve workload resource class
    +--> DatasetPartitioner --> JSONL adapter --> object store
    `--> Store -------------------------------> PostgreSQL adapter
```

The core package describes what the Job workflow needs. Adapter packages
describe how HTTP, JSONL, and PostgreSQL provide those capabilities. Package
boundaries do not create additional deployed services.

## Creation flow

`Service.Create` performs the following ordered workflow:

1. Normalize and validate the submitted executable, input, and resource class.
2. Look for an existing job with the same idempotency key.
3. Partition the input into stable, record-aligned logical shards.
4. Resolve the named resource class into concrete CPU and memory values.
5. Persist a preparing job, then atomically materialize its tasks.

PostgreSQL persists resolved values but does not choose resource policy.
Kubernetes execution starts only after task materialization.

## File map

- `job.go` defines durable Job status, progress, output, and results.
- `submission.go` defines user intent and named resource classes.
- `service.go` implements job creation and status retrieval.
- `partition.go` defines the partitioning port and logical shard model.
- `store.go` defines the durable persistence port used by the service.
- `validation.go` contains Job-domain normalization and validation.
- `httpapi/` translates REST requests and responses.
- `partition/` implements streaming JSONL partitioning.
- `postgres/` persists jobs, tasks, progress, and results.

Unit and within-package tests remain beside these packages. PostgreSQL-backed
and cross-package workflows live under `test/integration`.
