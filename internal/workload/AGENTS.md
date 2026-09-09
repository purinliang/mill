# Workload contract instructions

These instructions apply to `internal/workload`.

- Keep the contract language-neutral, minimal, and stable.
- Preserve the mandatory `--` separator and unchanged user arguments.
- Validate identity, non-negative shard indexes, non-empty half-open ranges,
  and absolute input and output URIs.
- Do not add Job-service, PostgreSQL, Kubernetes, or example computation here.
- Treat flag changes as compatibility changes and update documentation and
  reference workload tests together.

See [README.md](README.md) for the contract graph.
