# Developing Mill

This guide covers the implemented local environment, demonstrations, tests,
configuration, and repository organization. Planned multi-service and
availability work is described in [Architecture](architecture.md).

## Prerequisites

- Linux amd64 for the automated local-tool setup;
- Go 1.27.x;
- PostgreSQL 18 client/server tools for local and integration runs;
- Docker Engine with daemon access;
- `curl`, `jq`, `openssl`, and ordinary POSIX command-line tools; and
- kind and kubectl, which `scripts/setup` can install at pinned versions.

Docker is a machine-level prerequisite. The setup script does not install the
daemon, modify group membership, replace an incompatible cluster, or delete
resources.

## Local Kubernetes setup

Run:

```bash
./scripts/setup
```

The script installs pinned kind and kubectl binaries under
`${MILL_TOOLS_DIR:-$HOME/.local/bin}` when necessary, verifies downloaded
checksums, creates or reuses the `mill` cluster, selects `kind-mill`, and waits
for its node. It is idempotent and intentionally non-destructive.

Remove the local cluster explicitly when it is no longer needed:

```bash
kind delete cluster --name mill
```

## API-only local run

Create and migrate a local database:

```bash
createdb mill
export MILL_DATABASE_URL='postgresql:///mill'
for migration in migrations/*.sql; do
  psql "$MILL_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration"
done
```

Create an input and start Mill without an executor:

```bash
mkdir -p /tmp/mill-demo /tmp/mill-output
awk 'BEGIN { for (i = 0; i < 100; i++) print "{\"value\":" i "}" }' \
  > /tmp/mill-demo/records.jsonl

export MILL_OUTPUT_ROOT_URI='file:///tmp/mill-output'
export MILL_PARALLELISM=3
export MILL_GRPC_ADDR='127.0.0.1:9090'
go run ./cmd/mill
```

The default HTTP address is `:8080`; set `MILL_HTTP_ADDR` to override it. The
gRPC listener is disabled when `MILL_GRPC_ADDR` is empty. It currently has no
transport authentication and should bind only to a trusted local or
cluster-internal address. Submit a job from another shell:

```bash
curl --include --request POST http://localhost:8080/jobs \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: demo-job-001' \
  --data '{
    "executable": {"image": "mill/jsonl-copy:dev", "args": []},
    "input": {"uri": "file:///tmp/mill-demo/records.jsonl"}
  }'
```

The first request returns `201`; an identical replay returns `200` with the
same job. Retrieve status with:

```bash
curl http://localhost:8080/jobs/<job-id>
```

Health endpoints are:

- `GET /healthz` and `GET /livez` for process liveness;
- `GET /readyz` for PostgreSQL-backed readiness.

## Demonstrations

### One manually configured task

```bash
./scripts/demo-word-count-single-task
```

This stages one input range in the kind node, renders a Kubernetes Job, and
compares its output with a local run. It does not use PostgreSQL task claims.

### Full node-local batch

```bash
./scripts/demo-word-count-batch
```

This starts private temporary PostgreSQL and Mill processes, submits the
generated 12-record Walden input, executes the planned logical tasks with
bounded concurrency, merges successful outputs, and compares them to a local
full-input result. It uses hostPath storage on the single kind node.

Use two active attempts with:

```bash
MILL_PARALLELISM=2 ./scripts/demo-word-count-batch
```

Exercise deterministic task failure and retry exhaustion:

```bash
./scripts/demo-word-count-batch --failure once
./scripts/demo-word-count-batch --failure always
```

Exercise coordinator process recovery while PostgreSQL and Kubernetes continue:

```bash
./scripts/demo-word-count-batch --restart-coordinator
```

The replacement process waits for the 15-second attempt leases to expire, takes
over with new fencing tokens, and observes the same attempt IDs and Kubernetes
Job UIDs. The script records attempt history, failure logs, and identity
snapshots in its printed temporary directory. Kubernetes Jobs remain until
explicitly removed.

Exercise two live coordinators and survivor takeover:

```bash
./scripts/demo-word-count-batch --replica-failover
```

The script first lets one process own three delayed attempts, then starts a
second process on another loopback HTTP port. It proves that the second process
cannot change the unexpired leases or create duplicate Jobs, kills the primary
with SIGKILL, and verifies that the survivor receives new fencing tokens for
the same attempt IDs and Kubernetes UIDs. The remaining shards complete through
the survivor. Set `MILL_DEMO_SECONDARY_PORT` when the default primary port plus
one is unavailable.

### Full S3-compatible batch

```bash
./scripts/demo-word-count-s3
```

This is the shared-storage vertical slice. It:

1. generates one 12-record JSONL input;
2. builds and loads the word-count image;
3. starts pinned SeaweedFS on Docker's kind network with runtime-generated
   credentials and pre-created input/output buckets;
4. uploads the input and starts a private PostgreSQL database;
5. submits an `s3://` input with an `s3://` output root;
6. runs 12 Kubernetes Jobs with default parallelism three;
7. verifies Jobs contain no hostPath volumes or node selector;
8. downloads and merges only the successful result URIs; and
9. compares the result byte-for-byte with the local baseline.

The storage container and credential Secret are removed on exit. Generated
input, downloaded outputs, status, logs, and storage data remain in the printed
directory. Completed Kubernetes Jobs remain for inspection:

```bash
kubectl --context kind-mill -n default get jobs,pods \
  -l mill.dev/job-id=<job-id>
kubectl --context kind-mill -n default delete jobs \
  -l mill.dev/job-id=<job-id>
```

The S3 service is a local compatibility fixture, not a production storage
deployment or availability claim. Override its HTTP port with
`MILL_DEMO_PORT`; the default is `18081`.

See [the word-count guide](../examples/word-count/README.md) for tokenization,
input provenance, deterministic record grouping, and result-merging behavior.

## Configuration

Job-process variables:

| Variable | Purpose |
| --- | --- |
| `MILL_DATABASE_URL` | Required PostgreSQL connection URL. |
| `MILL_OUTPUT_ROOT_URI` | Required `file://` or `s3://` root. |
| `MILL_PARALLELISM` | Required job concurrency captured at submission. |
| `MILL_HTTP_ADDR` | Optional listen address; default `:8080`. |
| `MILL_GRPC_ADDR` | Optional internal execution gRPC listen address; empty disables it. |
| `AWS_REGION` | Enables S3 in the planner/workload storage adapter. |
| `MILL_S3_ENDPOINT` | Optional custom S3-compatible endpoint. |

Enable the current in-process Kubernetes coordinator with:

| Variable | Purpose |
| --- | --- |
| `MILL_EXECUTOR=kubernetes` | Enable task execution. |
| `MILL_KUBE_CONTEXT` | Explicit kubeconfig context. |
| `MILL_KUBE_NAMESPACE` | Namespace for Jobs. |

Alternatively, run the coordinator as a separate process. Start `cmd/mill`
with `MILL_GRPC_ADDR` set and leave `MILL_EXECUTOR` empty. In another shell,
configure and start the executor:

```bash
export MILL_JOB_GRPC_TARGET='127.0.0.1:9090'
export MILL_KUBE_CONTEXT='kind-mill'
export MILL_KUBE_NAMESPACE='default'
go run ./cmd/mill-executor
```

The standalone executor uses these RPC variables:

| Variable | Purpose |
| --- | --- |
| `MILL_JOB_GRPC_TARGET` | Required Job-service gRPC target. |
| `MILL_EXECUTION_RPC_TIMEOUT` | Optional per-call timeout; default `3s`, range `100ms`–`30s`. |

The current local-file Kubernetes path also needs the node/root variables below;
S3 tasks need the workload storage variables. The gRPC connection is currently
plaintext and must remain on a trusted local or cluster-internal network. The
full 12-task demonstration still uses the in-process path until its dedicated
split-process mode is implemented.

Node-local file tasks additionally require:

| Variable | Purpose |
| --- | --- |
| `MILL_KUBE_NODE` | Node containing staged files. |
| `MILL_LOCAL_ROOT` | Host root containing `input/` and `output/`. |
| `MILL_NODE_ROOT` | Corresponding root inside the kind node. |

S3-backed workload Pods use:

| Variable | Purpose |
| --- | --- |
| `MILL_WORKLOAD_S3_REGION` | Region injected as `AWS_REGION`. |
| `MILL_WORKLOAD_S3_ENDPOINT` | Optional endpoint reachable from Pods. |
| `MILL_WORKLOAD_S3_CREDENTIALS_SECRET` | Optional Secret exposed to the trusted workload as environment variables. |

The control plane obtains AWS credentials through the AWS SDK's normal
credential chain. The local demonstration uses ephemeral environment values.
On AWS, prefer workload identity and narrow object permissions instead of
copying long-lived credentials.

## Tests

Run all hermetic tests:

```bash
go test ./...
```

Prepare a disposable migrated database and enable PostgreSQL integration tests:

```bash
createdb mill_test
for migration in migrations/*.sql; do
  psql 'postgresql:///mill_test' -v ON_ERROR_STOP=1 -f "$migration"
done
MILL_TEST_DATABASE_URL='postgresql:///mill_test' go test -race ./...
```

Tests requiring PostgreSQL skip when `MILL_TEST_DATABASE_URL` is absent.
Kubernetes demonstrations are explicit scripts rather than part of the normal
unit suite.

## Protobuf generation

Generated Go bindings are committed, so ordinary builds and tests do not need
`protoc`. When the execution schema changes, install the matching generators
and regenerate from the repository root:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
PATH="$(go env GOPATH)/bin:$PATH" protoc -I api/proto \
  --go_out=. --go_opt=module=github.com/purinliang/mill \
  --go-grpc_out=. --go-grpc_opt=module=github.com/purinliang/mill \
  api/proto/mill/execution/v1/execution.proto
```

The checked-in files record the generator and compiler versions in their
headers. Review both the schema and generated diff together.

## Repository structure

```text
cmd/mill/
  main.go                         process composition and HTTP lifecycle
  grpc.go                         optional bounded execution gRPC listener
  execution.go                    coordinator lifecycle and direct store adapter
cmd/mill-executor/
  main.go                         standalone gRPC-to-Kubernetes coordinator
api/proto/mill/execution/v1/
  execution.proto                 versioned internal lease/state RPC schema
docs/
  architecture.md                 domain, correctness, and availability design
  development.md                  local setup, demos, tests, and structure
examples/jsonl-copy/
  cmd/jsonl-copy/                 minimal range-copy workload and Dockerfile
examples/word-count/
  cmd/word-count/                 S3/file mapper and Dockerfile
  cmd/merge/                      example-specific local result merger
  cmd/fault-injection/            deterministic test wrapper and Dockerfile
  generate/                       reproducible JSONL input generator
  walden-economy.txt              committed source fixture
  record-config.json              deterministic grouping configuration
  job.yaml.template               manual single-task manifest template
internal/job/
  model.go                        public job/submission model
  validation.go                   submission and URI normalization
  partition.go                    streaming JSONL logical-shard planner
  repository.go                   PostgreSQL job/task persistence
  attempt_repository.go           claims, fenced transitions, and retry policy
  execution_repository.go         lease renewal/takeover and successful results
  handler.go                      HTTP transport
internal/execution/
  model.go                        executor-facing attempt and executable model
  store.go                        durable executor store contract
internal/executionrpc/
  client.go                       deadline-bound execution Store client
  server.go                       Job-side backend and gRPC status mapping
  convert.go                      domain/Protobuf conversion
  v1/                             generated versioned Go bindings
internal/coordinator/
  coordinator.go                  observe active attempts and fill free slots
internal/kubernetes/
  executor.go                     create and observe native Kubernetes Jobs
internal/objectstore/
  store.go                        file and S3-compatible object access
internal/workload/
  contract.go                     language-neutral CLI protocol implementation
migrations/                       ordered PostgreSQL schema and lease history
scripts/
  setup                           pinned local kind/kubectl preparation
  demo-word-count-single-task     one manual Kubernetes task
  demo-word-count-batch           complete node-local control-plane batch
  demo-word-count-s3              complete shared-storage batch
README.md                         concise project entry point and roadmap
AGENTS.md                         engineering, Git, and agent conventions
```

Keep Mill as one Go module. Package boundaries are not automatically deployment
boundaries. Example executables remain under `examples`; top-level `cmd` is
reserved for Mill-owned services.

The lightweight Git workflow and commit conventions are defined in
[AGENTS.md](../AGENTS.md).
