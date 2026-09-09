# Job package instructions

These instructions apply to `internal/job` and its subpackages.

- Keep Job-domain types, validation, defaults, resource policy, and workflow
  ordering in the root package.
- Keep interfaces at the consumer boundary. `Service` owns `Store` and
  `DatasetPartitioner`; adapters implement them.
- Keep HTTP limited to transport translation. Do not place submission policy
  or persistence decisions in handlers.
- Keep PostgreSQL limited to durable storage and atomic state changes. It must
  receive resolved workload resources rather than choose a resource class.
- Keep partitioning in `partition/`. The public adapter implements dataset
  partitioning; JSONL scanning remains a private format detail.
- Do not import coordinator or Kubernetes packages into the Job core.
- Preserve idempotent submission and atomic task materialization.
- Test public workflow behavior with small fakes. Test PostgreSQL transactions,
  constraints, concurrency, and replay against disposable PostgreSQL.
- Put cross-package or complete lifecycle tests under `test/integration`, not
  in this package.

See [README.md](README.md) for the package graph and file responsibilities.
