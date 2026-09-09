# Execution package

`internal/execution` owns backend-independent attempt identity, ownership, and
the durable operations required by execution replicas.

```text
mill-execution
      |
      v
 coordinator.Tick
      +--> execution.Store --> gRPC client --> Job service --> PostgreSQL
      `--> Runtime ---------> Kubernetes adapter --> Job --> workload Pod
```

The execution process does not connect to PostgreSQL. Its `Store`
implementation is a bounded gRPC client. The Job service hosts the gRPC server
and uses the PostgreSQL execution adapter behind it.

## Reconciliation flow

On every tick, the coordinator first renews or takes over active attempt
leases, reconciles their Kubernetes Jobs, and then claims new work while slots
are available. A runtime observation may record an external Job identity,
complete an attempt, or fail it.

Attempt IDs and Kubernetes Job names remain stable during lease takeover. A
new owner receives a new fencing token, and stale tokens cannot mutate state.
Ambiguous Kubernetes or RPC errors are retried as observations; they do not
prove that a replacement attempt is safe.

## File map

- `attempt.go` defines attempt identity, lifecycle, and leased invocation.
- `workload.go` defines executable and resolved resource requirements.
- `errors.go` defines transport-independent execution failures.
- `coordinator/` owns observation order, bounded claims, and its Store port.
- `kubernetes/` creates and observes native Kubernetes Jobs.
- `postgres/` implements leases, fencing, transitions, and retries.
- `rpc/` carries the execution Store contract over Protobuf/gRPC.

Unit and within-package tests remain beside these packages. PostgreSQL-backed
and cross-package workflows live under `test/integration`.
