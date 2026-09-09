# Execution package instructions

These instructions apply to `internal/execution` and its subpackages.

- Keep attempt types and errors independent of PostgreSQL, gRPC, and
  Kubernetes. Keep the Store port with its coordinator consumer.
- Keep coordinator policy backend-independent. Reconcile active attempts before
  claiming new work, and bound the number of claims per tick.
- Keep the runnable execution service free of PostgreSQL configuration. It
  reaches durable state exclusively through the gRPC Store adapter.
- Preserve deterministic Kubernetes Job names and recorded external UIDs.
- Treat timeouts, missing running Jobs, and API failures as ambiguous
  observations. Do not silently create replacements.
- Preserve renewable leases, token replacement on takeover, and fencing on
  every mutation. Takeover keeps the same attempt identity.
- Keep Kubernetes retries disabled while Mill owns retry policy.
- Keep Kubernetes code limited to runtime translation and observation.
- Keep gRPC messages bounded and deadline-bearing, and map domain failures
  without exposing database details.
- Put PostgreSQL-backed, RPC round-trip, process-boundary, and Kubernetes API
  adapter tests under `test/integration`.

See [README.md](README.md) for the package graph and file responsibilities.
