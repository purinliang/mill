# Development

This guide covers the implemented local environment, demonstrations, tests,
configuration, and repository organization. Availability design is described
in [Architecture](architecture.md), and staged work is in the
[roadmap](roadmap.md).

## Prerequisites

- Linux amd64 for the automated local-tool setup;
- Go 1.27.x;
- PostgreSQL 18 client/server tools for local and integration runs;
- Docker Engine with daemon access;
- `curl`, `jq`, `openssl`, and ordinary POSIX command-line tools; and
- kind and kubectl, which `scripts/setup.sh` can install at pinned versions.

Docker is a machine-level prerequisite. The setup script does not install the
daemon, modify group membership, replace an incompatible cluster, or delete
resources.

## Local Kubernetes setup

Run:

```bash
./scripts/setup.sh
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

Create an input and start the Job service:

```bash
mkdir -p /tmp/mill-demo /tmp/mill-output
awk 'BEGIN { for (i = 0; i < 100; i++) print "{\"value\":" i "}" }' \
  > /tmp/mill-demo/records.jsonl

export MILL_OUTPUT_ROOT_URI='file:///tmp/mill-output'
export MILL_PARALLELISM=3
export MILL_GRPC_ADDR='127.0.0.1:9090'
go run ./cmd/mill-job
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
same job. Without a separately running execution service, its tasks remain
pending. Retrieve status with:

```bash
curl http://localhost:8080/jobs/<job-id>
```

Health endpoints are:

- `GET /healthz` and `GET /livez` for process liveness;
- `GET /readyz` for PostgreSQL-backed readiness.

## Demonstrations

### One manually configured task

```bash
./scripts/demo-word-count-single-task.sh
```

This stages one input range in the kind node, renders a Kubernetes Job, and
compares its output with a local run. It does not use PostgreSQL task claims.

### Full node-local batch

```bash
./scripts/demo-word-count-batch.sh
```

This starts private temporary PostgreSQL, Job, and standalone execution service
processes, submits the generated 12-record Walden input, executes the resulting
logical tasks with bounded concurrency, merges successful outputs, and compares
them to a local full-input result. It uses hostPath storage on the single kind
node.

Run the same batch with a second live execution replica:

```bash
./scripts/demo-word-count-batch.sh --split-process
```

All modes use the gRPC service boundary. Execution replicas receive no
PostgreSQL configuration and access execution state only through the Job
service. The script verifies that all 12 task outputs match the local baseline
and no more than the configured number of workload Pods run concurrently. One
execution replica may own all active leases while the other remains available
as standby. Execution replicas provide reconciliation availability, while
workload Pods provide computation parallelism. Override the gRPC port with
`MILL_DEMO_GRPC_PORT` when necessary.

Use two active attempts with:

```bash
MILL_PARALLELISM=2 ./scripts/demo-word-count-batch.sh
```

Exercise a workload resource class and verify every generated Pod template:

```bash
MILL_DEMO_RESOURCE_CLASS=medium ./scripts/demo-word-count-batch.sh
```

The accepted values are `small`, `medium`, and `large`; omission defaults to
`small` in the API. The demo checks both the returned resolved resources and
the Kubernetes CPU and memory requests/limits.

Exercise deterministic task failure and retry exhaustion:

```bash
./scripts/demo-word-count-batch.sh --failure once
./scripts/demo-word-count-batch.sh --failure always
```

Exercise execution process recovery while the Job service, PostgreSQL, and
Kubernetes continue:

```bash
./scripts/demo-word-count-batch.sh --restart-coordinator
```

The replacement execution process waits for the 15-second attempt leases to
expire, takes over with new fencing tokens, and observes the same attempt IDs
and Kubernetes Job UIDs. The script records attempt history, failure logs, and
identity snapshots in its printed temporary directory. It retains Kubernetes
Jobs until the user explicitly removes them.

Exercise two live execution replicas and survivor takeover:

```bash
./scripts/demo-word-count-batch.sh --replica-failover
```

The script first lets one execution process own three delayed attempts, then
starts a second process against the same Job gRPC endpoint. It proves that the
second process cannot change the unexpired leases or create duplicate Jobs,
kills the lease owner with SIGKILL, and verifies that the survivor receives
new fencing tokens for the same attempt IDs and Kubernetes UIDs. The Job
service remains available while the survivor completes the remaining shards.

### Full S3-compatible batch

```bash
./scripts/demo-word-count-s3.sh
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

### Full batch through deployed Mill services

```bash
./scripts/demo-word-count-deployed.sh
```

This is the single-node deployment proof. It creates unique temporary system
and workload namespaces, starts disposable PostgreSQL 18 and SeaweedFS
containers on kind's Docker network, migrates the database, and calls
`scripts/deploy-local-control-plane.sh`. The API is reached through a temporary
port-forward; override its local port with `MILL_DEMO_PORT`, whose default is
`18083`.

The demo submits the same 12-task S3 workload, enforces parallelism three,
checks that generated Jobs have no hostPath or node selector, downloads only
successful result URIs, and compares the merged counts with a local full-input
result. It then captures service logs, Kubernetes resource snapshots, Job
manifests, database logs, and storage logs in the printed run directory.

Each run uses unique namespaces and container names. On exit it removes only
those namespaces and containers, so an existing `mill-system` deployment is
not overwritten. The disposable database and object store are correctness
fixtures; this test demonstrates a fully deployed control plane, not service,
node, database, or storage availability.

To test one execution Pod failure on the same single-node cluster, run:

```bash
./scripts/demo-word-count-deployed.sh --execution-failover
```

This mode uses a deterministic 15-second delay for the first three workload
attempts. It waits until one execution replica owns all three leases, scales
the Deployment from one replica to two, and verifies that the standby neither
steals the live leases nor creates replacement Jobs. It then records the owner
and standby logs, deletes the owning Pod, waits for that Pod to disappear, and
accepts takeover only when another running execution replica holds new fencing
tokens for the same task IDs, attempt IDs, attempt numbers, external UIDs,
Kubernetes Job names, and Job UIDs.

The run must still finish with exactly 12 first attempts, two available
execution replicas, peak workload parallelism three, and output identical to
the local baseline. The printed directory retains `failover-attempts-*.json`,
`failover-kubernetes-*.json`, and the execution logs around the failure. This is
evidence for execution Pod recovery while the Job service, PostgreSQL,
Kubernetes control plane/node, network, and object store remain healthy; it is
not a full high-availability claim.

To test one Job Pod failure, run:

```bash
./scripts/demo-word-count-deployed.sh --job-failover
```

This mode also delays the first three attempts. It records the original
attempt and Kubernetes Job identities, scales the Job Deployment from
one ready replica to two, and deletes the original Pod—the only gRPC endpoint
that existed when the execution service connected. The external port-forward
is recreated to model a retrying REST client. The test passes only when the
REST API becomes reachable and the execution service reconnects through the
ClusterIP Service. The original lease owner and fencing tokens must complete
the same attempts; all 12 first attempts must finish with exact output. Its
`job-*.json` and Pod-specific logs provide the failure evidence.

This proves that one stateless Job Pod may fail while another ready replica,
PostgreSQL, the execution service, Kubernetes node/API, network, and object
store remain healthy. It does not demonstrate tolerance of database, node, or
network-partition failure.

### Availability manifests

The multi-node profiles live in `deploy/kubernetes/availability`. Install the
pinned CloudNativePG 1.30.0 operator into the intended context first:

```bash
MILL_KUBE_CONTEXT=<context> ./scripts/install-cloudnative-pg.sh
```

The installer downloads the official release manifest, verifies its pinned
SHA-256 digest, applies it server-side, and waits for the controller. The
directory then provides:

- `namespaces-rbac.yaml` for the system, workload, and database namespaces and
  the existing namespace-scoped execution-service permissions;
- `control-plane.yaml` for two Job and two execution replicas, required
  hostname anti-affinity, and one-replica disruption budgets;
- `postgres-two-node.yaml` for two PostgreSQL instances with synchronous
  `ANY 1` and `dataDurability: preferred`; and
- `postgres-three-node.yaml` for three instances with synchronous `ANY 1`,
  `dataDurability: required`, and failover quorum.

The PostgreSQL profiles have the same resource name and are alternatives; do
not apply both. Each requires a `kubernetes.io/basic-auth` Secret named
`mill-database-credentials` in `mill-database`, and each uses K3s's
`local-path` storage class. The control-plane manifest similarly expects its
configuration Secrets and node-reachable images. The
`scripts/deploy-availability.sh` script validates the topology, creates the
Secrets, applies migrations, and rolls out the services.

Required anti-affinity intentionally leaves replicas Pending when the cluster
has too few distinct hostnames. Weakening it to make a one-node test green
would invalidate the failure-domain claim. The two-node profile favors write
availability if its standby disappears; the three-node profile favors
acknowledged-write durability and stops rather than promoting an unsafe
minority.

Machine installation, firewall requirements, profile deployment, and the
evidence checklist are maintained in the
[Availability runbook](availability-runbook.md).

See [the word-count guide](../examples/word-count/README.md) for tokenization,
input provenance, deterministic record grouping, and result-merging behavior.

## Control-plane images

Build both Mill service images from the repository root:

```bash
./scripts/build-control-plane-images.sh
```

The script builds `mill/job:dev` from `cmd/mill-job/Dockerfile` and builds
`mill/execution:dev` from `cmd/mill-execution/Dockerfile`. Override the tags
with `MILL_JOB_IMAGE` and `MILL_EXECUTION_IMAGE`. It inspects both results and
fails unless both have the expected entrypoint and numeric non-root identity
`65532:65532`.

Both images contain only a statically linked service binary and CA
certificates. Building them does not load them into kind or create Kubernetes
resources.

## Local control-plane deployment

The first deployment baseline runs one Job Pod and one execution Pod in
kind. It requires an existing PostgreSQL database that is already migrated and
reachable from Pods, plus an `s3://` output root. It does not deploy PostgreSQL
or object storage.

```bash
export MILL_DATABASE_URL='postgresql://mill:password@pod-reachable-host:5432/mill'
export MILL_OUTPUT_ROOT_URI='s3://mill-output'
export AWS_REGION='us-east-1'

# Optional for a local S3-compatible endpoint:
export MILL_S3_ENDPOINT='http://storage-address:8333'
export MILL_WORKLOAD_S3_ENDPOINT="$MILL_S3_ENDPOINT"
export AWS_ACCESS_KEY_ID='local-access-key'
export AWS_SECRET_ACCESS_KEY='local-secret-key'

./scripts/deploy-local-control-plane.sh
```

Do not use `127.0.0.1` for PostgreSQL or an S3 endpoint unless that service is
inside the same Pod: inside a container it refers to that container. The script
builds and loads both service images, creates `mill-system` and
`mill-workloads`, applies configuration as Kubernetes Secrets, restarts the
Deployments, and waits for both rollouts. Re-running it updates the local
deployment without deleting its namespaces.

The Job Pod receives the database URL and control-plane S3 credentials.
The execution Pod does not receive them. Its service account may only create and
get Jobs in `mill-workloads`; it cannot list or delete Jobs, read Pods, or read
Secrets. When static AWS credentials are supplied for this local setup, a
separate `mill-workload-storage` Secret is created in the workload namespace.
Workload Pods may reference this Secret without granting the execution service
permission to read its contents.

The checked-in manifests are kind-specific: they use local `:dev` images with
`imagePullPolicy: Never`, one replica per service, plaintext cluster-internal
gRPC, and no database or object-store deployment. They establish a runnable
Pod/RBAC baseline but provide no replica, node, database, or storage
availability.

`MILL_SYSTEM_NAMESPACE` and `MILL_WORKLOAD_NAMESPACE` may override the default
namespaces, and `MILL_KUBE_CONTEXT` and `MILL_KIND_CLUSTER` may select another
local kind cluster. Namespace overrides must be distinct DNS labels. The
deployed demonstration uses these options to isolate every run.

Inspect the deployment and API:

```bash
kubectl --context kind-mill -n mill-system get deployments,pods,service
kubectl --context kind-mill -n mill-system logs deployment/mill-execution
kubectl --context kind-mill -n mill-system port-forward service/mill-job 8080:8080
curl http://127.0.0.1:8080/readyz
```

Remove only these local Mill namespaces when finished, then stop separately
managed PostgreSQL or object-storage processes yourself:

```bash
kubectl --context kind-mill delete namespace mill-system mill-workloads
```

## Configuration

Job-process variables:

| Variable | Purpose |
| --- | --- |
| `MILL_DATABASE_URL` | Required PostgreSQL connection URL. |
| `MILL_OUTPUT_ROOT_URI` | Required `file://` or `s3://` root. |
| `MILL_PARALLELISM` | Required job concurrency captured at submission. |
| `MILL_HTTP_ADDR` | Optional listen address; default `:8080`. |
| `MILL_GRPC_ADDR` | Optional internal execution gRPC listen address; empty disables it. |
| `AWS_REGION` | Enables S3 in the partitioner/workload storage adapter. |
| `MILL_S3_ENDPOINT` | Optional custom S3-compatible endpoint. |

Task execution always runs in the standalone execution service. Start
`cmd/mill-job` with `MILL_GRPC_ADDR` set, then configure and start the
execution service in another shell:

```bash
export MILL_JOB_GRPC_TARGET='127.0.0.1:9090'
export MILL_KUBE_CONTEXT='kind-mill'
export MILL_KUBE_NAMESPACE='default'
go run ./cmd/mill-execution
```

The standalone execution service uses these RPC variables:

| Variable | Purpose |
| --- | --- |
| `MILL_JOB_GRPC_TARGET` | Required Job gRPC target. |
| `MILL_EXECUTION_RPC_TIMEOUT` | Optional per-call timeout; default `3s`, range `100ms`–`30s`. |
| `MILL_KUBE_CONTEXT` | Kubeconfig context for local execution. |
| `MILL_KUBE_IN_CLUSTER` | Use Pod credentials when exactly `true`. |
| `MILL_KUBE_NAMESPACE` | Namespace for Jobs. |

Configure exactly one Kubernetes client mode. Set `MILL_KUBE_CONTEXT` for a
locally running execution service, or set `MILL_KUBE_IN_CLUSTER=true` when it
runs as a Pod. In-cluster mode uses client-go's standard service-account CA,
token, and API address. It does not itself create or grant the required RBAC;
that belongs to the deployment configuration.

The current local-file Kubernetes path also needs the node/root variables below;
S3 tasks need the workload storage variables. The gRPC connection is currently
plaintext and must remain on a trusted local or cluster-internal network. The
full 12-task and S3-compatible demonstrations both use this external process.

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

Report coverage for handwritten Go code with:

```bash
./scripts/test-coverage.sh
```

This command still compiles and exercises the committed Protobuf bindings
through Mill's RPC tests, but excludes generated `*.pb.go` statements from the
reported coverage percentage. Test the handwritten RPC client/server adapters
and domain behavior rather than generated getters and descriptors.

Coverage is weighted by statements, not averaged across files or packages. The
hermetic run skips PostgreSQL integration tests, so its total does not represent
coverage of the durable repository. Mill uses small `Store` fakes when testing
HTTP, coordinator, and transport behavior, but tests repository SQL against a
real PostgreSQL instance instead of mocking expected SQL calls.

Prepare a disposable migrated database and enable PostgreSQL integration tests:

```bash
createdb mill_test
for migration in migrations/*.sql; do
  psql 'postgresql:///mill_test' -v ON_ERROR_STOP=1 -f "$migration"
done
MILL_TEST_DATABASE_URL='postgresql:///mill_test' go test -race ./...
```

With the same environment variable set, include PostgreSQL behavior in the
handwritten coverage report:

```bash
MILL_TEST_DATABASE_URL='postgresql:///mill_test' ./scripts/test-coverage.sh
```

Tests requiring PostgreSQL skip when `MILL_TEST_DATABASE_URL` is absent.
The opt-in suite also builds and launches the real Job executable,
verifies liveness and readiness, submits and replays a job through HTTP,
claims and transitions an attempt through the public gRPC API, observes the
durable progress through HTTP, and requires graceful SIGTERM shutdown. Direct
database access is limited to removing its fixture afterward.
The hermetic suite separately builds and launches the real execution
executable against in-memory gRPC and Kubernetes HTTP test servers. It verifies
that a claimed attempt becomes a correctly addressed Kubernetes Job and that
the execution service reports the returned Job UID before shutting down
cleanly. No cluster is required for this process-boundary test.
Kubernetes demonstrations are explicit scripts rather than part of the normal
unit suite.

## Protobuf generation

Generated Go bindings are committed, so ordinary builds and tests do not need
`protoc`. When the execution schema changes, install the matching generators
and regenerate from the repository root:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
PATH="$(go env GOPATH)/bin:$PATH" protoc -I . \
  --go_out=. --go_opt=module=github.com/purinliang/mill \
  --go-grpc_out=. --go-grpc_opt=module=github.com/purinliang/mill \
  api/proto/mill/execution/v1/execution.proto
```

The checked-in files record the generator and compiler versions in their
headers. Review both the schema and generated diff together.

## Repository structure

```text
cmd/                    runnable Mill service composition roots
api/proto/              versioned internal RPC schemas
deploy/                 Kubernetes deployment definitions
docs/                   system design and developer operations
examples/               trusted workloads and demonstrations
internal/job/            Job workflow, policy, ports, and adapters
internal/execution/      attempt reconciliation, ownership, and adapters
internal/objectstore/    file and S3-compatible object access
internal/workload/       stable workload command-line contract
migrations/             ordered PostgreSQL schema history
scripts/                repeatable setup, deployment, and demo commands
test/integration/        database, transport, adapter, and process tests
```

Keep Mill as one Go module. Package boundaries are not automatically deployment
boundaries. Example executables remain under `examples`; top-level `cmd` is
reserved for Mill-owned services.

Detailed package graphs and file responsibilities are maintained beside the
code:

- [Job package](../internal/job/README.md)
- [Execution package](../internal/execution/README.md)
- [Object-store package](../internal/objectstore/README.md)
- [Workload contract](../internal/workload/README.md)
- [Integration tests](../test/integration/README.md)

The lightweight Git workflow and commit conventions are defined in
[AGENTS.md](../AGENTS.md).
