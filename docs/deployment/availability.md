# Availability

This guide exercises the two-node and three-voter profiles. Passing on kind
validates manifests; only a run across independent machines validates a
physical failure domain. The preferred three-node environment is described in
[AWS deployment](aws.md).

## Prepare K3s nodes

Every machine needs a unique hostname, stable address, synchronized time, and
reliable storage. Allow TCP 6443 between nodes, TCP 2379–2380 between server
voters, and the selected K3s Pod-network ports.

Create one uncommitted cluster token and transfer it securely to each node:

```bash
umask 077
openssl rand -hex 32 > mill-k3s-token
```

Initialize the first server:

```bash
sudo env MILL_NODE_NAME=mill-a \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_TLS_SAN=<stable-api-address> \
  ./scripts/install-k3s-node.sh server-init
```

For the two-laptop profile, join the second machine as an agent:

```bash
sudo env MILL_NODE_NAME=mill-b \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_SERVER_URL=https://<server-address>:6443 \
  ./scripts/install-k3s-node.sh agent
```

For the quorum profile, join two additional machines as servers:

```bash
sudo env MILL_NODE_NAME=mill-b \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_SERVER_URL=https://<server-address>:6443 \
  K3S_TLS_SAN=<stable-api-address> \
  ./scripts/install-k3s-node.sh server-join

sudo env MILL_NODE_NAME=mill-c \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_SERVER_URL=https://<server-address>:6443 \
  K3S_TLS_SAN=<stable-api-address> \
  ./scripts/install-k3s-node.sh server-join
```

All servers must use the same K3s network and component flags.

## Verify the topology

Configure kubectl for the cluster, then verify roles and failure domains:

```bash
kubectl get nodes -o wide
kubectl get nodes \
  -L kubernetes.io/hostname,node-role.kubernetes.io/control-plane
```

The two-node profile requires two Ready hostnames. The quorum profile requires
three Ready nodes with control-plane and etcd roles.

All nodes need the same immutable images. Use a registry for AWS or import one
Docker archive into every laptop K3s node. Shared input and output storage must
be reachable from every workload Pod. A storage service on one laptop does not
support a whole-node availability claim.

## Deploy Mill

Install the pinned CloudNativePG operator:

```bash
MILL_KUBE_CONTEXT=<context> ./scripts/install-cloudnative-pg.sh
```

Select exactly one database profile and deploy:

```bash
export MILL_KUBE_CONTEXT=<context>
export MILL_AVAILABILITY_PROFILE=three-node  # or two-node
export MILL_DATABASE_PASSWORD="$(openssl rand -hex 24)"
export MILL_JOB_IMAGE=<registry>/mill/job:<immutable-tag>
export MILL_EXECUTION_IMAGE=<registry>/mill/execution:<immutable-tag>
export MILL_OUTPUT_ROOT_URI=s3://<output-bucket>
export AWS_REGION=<region>
./scripts/deploy-availability.sh
```

For a compatible local S3 service, also set `MILL_S3_ENDPOINT` and
`MILL_WORKLOAD_S3_ENDPOINT`. Supply scoped temporary credentials through the
current environment; never commit them. Do not switch an existing PostgreSQL
resource between two- and three-node profiles.

Required anti-affinity intentionally leaves replicas Pending when too few
hostnames exist. Do not weaken it to make an invalid topology appear healthy.

## Run the failure exercise

```bash
export MILL_WORKLOAD_IMAGE=<registry>/mill/word-count-fault:<immutable-tag>
export MILL_DEMO_INPUT_ROOT_URI=s3://<input-bucket>
./scripts/demo-availability.sh
```

The exercise holds one active wave while deleting an execution Pod, a Job Pod,
and the PostgreSQL primary Pod. It must complete the same attempts and
Kubernetes Jobs and produce byte-exact output.

For the three-node milestone, additionally stop one physical node and isolate
one minority node. The remaining two voters must continue, the minority must
reject authoritative writes, and PostgreSQL must expose exactly one writer.
Never force database promotion to make an unsafe test pass.

## Preserve evidence

Keep the run directory until it has been reviewed. It should contain:

- node and Pod placement before and after failure;
- etcd membership and PostgreSQL replication state;
- primary identity and promotion timing;
- task, attempt, fencing-token, Kubernetes Job, and UID snapshots;
- service and database logs; and
- the exact final-output comparison.

After review, remove only resources created for the exercise. Delete temporary
credentials and image archives, then destroy the disposable AWS environment or
stop the local storage fixtures explicitly.
