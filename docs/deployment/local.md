# Local

This guide deploys the Job and execution services into the existing local kind
cluster. PostgreSQL and S3-compatible storage must already be migrated,
reachable from Pods, and managed separately.

## Build service images

```bash
./scripts/build-control-plane-images.sh
```

The script builds `mill/job:dev` and `mill/execution:dev`, then verifies their
entrypoints and non-root `65532:65532` identity. It does not deploy them.

## Deploy

```bash
export MILL_DATABASE_URL='postgresql://mill:password@host:5432/mill'
export MILL_OUTPUT_ROOT_URI='s3://mill-output'
export AWS_REGION='us-east-1'

# Only for a local S3-compatible service:
export MILL_S3_ENDPOINT='http://storage-address:8333'
export MILL_WORKLOAD_S3_ENDPOINT="$MILL_S3_ENDPOINT"
export AWS_ACCESS_KEY_ID='local-access-key'
export AWS_SECRET_ACCESS_KEY='local-secret-key'

./scripts/deploy-local-control-plane.sh
```

The script builds and loads the images, creates `mill-system` and
`mill-workloads`, applies Secrets, and waits for both Deployments. Re-running
it updates the deployment without deleting namespaces.

The Job Pod receives PostgreSQL and control-plane storage configuration. The
execution Pod receives neither database nor storage credentials. Its service
account can create and get Jobs only in `mill-workloads`. Trusted workload Pods
may receive a separate storage Secret.

The local manifests use one replica per service, plaintext internal gRPC,
`imagePullPolicy: Never`, and no database or object-store deployment. They prove
the deployment boundary but provide no node or infrastructure availability.

## Inspect and remove

```bash
kubectl --context kind-mill -n mill-system get deployments,pods,service
kubectl --context kind-mill -n mill-system logs deployment/mill-execution
kubectl --context kind-mill -n mill-system \
  port-forward service/mill-job 8080:8080
curl http://127.0.0.1:8080/readyz
```

Remove only Mill's local namespaces when finished:

```bash
kubectl --context kind-mill delete namespace mill-system mill-workloads
```

Stop separately managed PostgreSQL and object-storage processes yourself. For
replicated deployments, use [Availability](availability.md).
