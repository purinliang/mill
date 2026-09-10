# Setup

This guide prepares the local development environment and starts Mill's two
services as local processes.

## Prerequisites

- Linux amd64 for the automated tool setup;
- Go 1.27.x and PostgreSQL 18 client/server tools;
- Docker Engine with daemon access; and
- `curl`, `jq`, `openssl`, kind, and kubectl.

Install or verify the pinned kind and kubectl versions, create the local `mill`
cluster, and select its context:

```bash
./scripts/setup.sh
```

The script is idempotent and does not install Docker, change group membership,
replace an incompatible cluster, or delete resources. Remove the cluster only
when it is no longer needed:

```bash
kind delete cluster --name mill
```

## Run the services locally

Create and migrate a PostgreSQL database:

```bash
mkdir -p /tmp/mill-demo /tmp/mill-output
printf '%s\n' '{"value":1}' '{"value":2}' > /tmp/mill-demo/records.jsonl
createdb mill
export MILL_DATABASE_URL='postgresql:///mill'
for migration in migrations/*.sql; do
  psql "$MILL_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration"
done
```

Start the Job service:

```bash
export MILL_OUTPUT_ROOT_URI='file:///tmp/mill-output'
export MILL_PARALLELISM=3
export MILL_GRPC_ADDR='127.0.0.1:9090'
go run ./cmd/mill-job
```

Start the execution service in another shell:

```bash
export MILL_JOB_GRPC_TARGET='127.0.0.1:9090'
export MILL_KUBE_CONTEXT='kind-mill'
export MILL_KUBE_NAMESPACE='default'
go run ./cmd/mill-execution
```

The execution process requires exactly one Kubernetes mode: a kubeconfig
context locally, or `MILL_KUBE_IN_CLUSTER=true` when running as a Pod.

## Configuration reference

The Job service requires `MILL_DATABASE_URL`, `MILL_OUTPUT_ROOT_URI`, and
`MILL_PARALLELISM`. `MILL_HTTP_ADDR` defaults to `:8080`.
`MILL_GRPC_ADDR` enables the internal listener.

The execution service requires `MILL_JOB_GRPC_TARGET` and
`MILL_KUBE_NAMESPACE`. `MILL_EXECUTION_RPC_TIMEOUT` controls its per-call
deadline. Local execution uses `MILL_KUBE_CONTEXT`; in-cluster execution uses
`MILL_KUBE_IN_CLUSTER=true`.

S3 configuration uses `AWS_REGION` and the AWS SDK credential chain.
`MILL_S3_ENDPOINT` selects a compatible control-plane endpoint. Workload Pods
use `MILL_WORKLOAD_S3_REGION`, `MILL_WORKLOAD_S3_ENDPOINT`, and optionally
`MILL_WORKLOAD_S3_CREDENTIALS_SECRET`.

Node-local tasks additionally use `MILL_KUBE_NODE`, `MILL_LOCAL_ROOT`, and
`MILL_NODE_ROOT`. Do not use `127.0.0.1` for a service outside the current Pod;
inside a container it refers to that container.

The gRPC boundary is currently plaintext. Bind it only to a trusted local or
cluster-internal network. Never commit database or object-store credentials.

## HTTP smoke check

The Job service exposes `/healthz` and `/livez` for liveness and `/readyz` for
PostgreSQL-backed readiness. Submit and inspect a job with:

```bash
curl --request POST http://127.0.0.1:8080/jobs \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: demo-job-001' \
  --data '{
    "executable":{"image":"mill/jsonl-copy:dev","args":[]},
    "input":{"uri":"file:///tmp/mill-demo/records.jsonl"}
  }'

curl http://127.0.0.1:8080/jobs/<job-id>
```
