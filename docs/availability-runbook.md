# Availability runbook

This runbook prepares the Milestone 7 two-laptop profile and Milestone 8
three-node quorum profile. It does not turn an untested topology into an
availability claim: retain the output of every check and failure exercise.

## Shared prerequisites

Each Linux machine needs a unique hostname, a stable LAN address, synchronized
time, and passwordless reachability to the other nodes on the required K3s
ports. At minimum allow TCP 6443 from joining nodes, UDP 8472 between all nodes
for Flannel VXLAN, and TCP 2379–2380 between server nodes in the three-voter
profile. Slow or unreliable disks are unsuitable for etcd and PostgreSQL.

Create one cluster token on a trusted machine without committing it:

```bash
umask 077
openssl rand -hex 32 > mill-k3s-token
chmod 600 mill-k3s-token
```

Move that file to each node over a trusted channel and delete the copies when
installation is complete. `scripts/install-k3s-node` verifies a pinned official
K3s installer before running it. It installs K3s `v1.36.3+k3s1`; review the
upstream release notes before changing the pin.

## Two-laptop profile

The first laptop owns the only Kubernetes server/control plane. Initialize its
embedded etcd datastore:

```bash
sudo env MILL_NODE_NAME=mill-a \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_TLS_SAN=<server-lan-ip> \
  ./scripts/install-k3s-node server-init
```

Join the second laptop as an agent:

```bash
sudo env MILL_NODE_NAME=mill-b \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_SERVER_URL=https://<server-lan-ip>:6443 \
  ./scripts/install-k3s-node agent
```

This topology can survive one Mill Pod or a controlled PostgreSQL primary Pod
failure while the server laptop remains healthy. It cannot survive loss of the
server laptop or safely decide ambiguous network partitions.

## Three-voter profile

Initialize the first K3s server as above. Join the other two machines as server
and etcd voters, not agents:

```bash
sudo env MILL_NODE_NAME=mill-b \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_SERVER_URL=https://<first-server-lan-ip>:6443 \
  K3S_TLS_SAN=<stable-api-address> \
  ./scripts/install-k3s-node server-join

sudo env MILL_NODE_NAME=mill-c \
  K3S_TOKEN_FILE=/secure/path/mill-k3s-token \
  K3S_SERVER_URL=https://<first-server-lan-ip>:6443 \
  K3S_TLS_SAN=<stable-api-address> \
  ./scripts/install-k3s-node server-join
```

All server nodes must use the same network- and component-related K3s flags.
The stable API address may initially be a controlled DNS name or virtual IP,
but its own availability must be included in any whole-node claim.

## Verify the cluster

Configure a local kubeconfig context that reaches the server LAN address, then
check the exact roles and hostname failure domains:

```bash
kubectl --context <context> get nodes -o wide
kubectl --context <context> get nodes \
  -L kubernetes.io/hostname,node-role.kubernetes.io/control-plane
```

The two-node profile requires two distinct ready hostnames. The three-node
profile requires three ready nodes that all show control-plane and etcd roles.

## Deploy PostgreSQL and Mill

First install the pinned CloudNativePG operator:

```bash
MILL_KUBE_CONTEXT=<context> ./scripts/install-cloudnative-pg
```

Push `mill/job-service` and `mill/executor` images to a registry reachable by
every node, and use S3 or another S3-compatible endpoint reachable from every
Pod. Then deploy one profile:

```bash
export MILL_KUBE_CONTEXT=<context>
export MILL_AVAILABILITY_PROFILE=two-node  # or three-node
export MILL_DATABASE_PASSWORD="$(openssl rand -hex 24)"
export MILL_JOB_IMAGE=<registry>/mill/job-service:<immutable-tag>
export MILL_EXECUTOR_IMAGE=<registry>/mill/executor:<immutable-tag>
export MILL_OUTPUT_ROOT_URI=s3://<output-bucket>
export AWS_REGION=<region>
export AWS_ACCESS_KEY_ID=<temporary-or-scoped-key>
export AWS_SECRET_ACCESS_KEY=<secret>
./scripts/deploy-availability
```

Use `MILL_S3_ENDPOINT` and `MILL_WORKLOAD_S3_ENDPOINT` for a compatible service.
The script creates Kubernetes Secrets, deploys the selected PostgreSQL profile,
waits for the writer Service, applies Mill migrations, and rolls out two
anti-affined replicas of each Mill service. It never prints the database or S3
credentials. The password and cloud credentials remain in your current shell;
unset them when the exercise finishes.

Do not switch a live `mill-postgres` resource from the three-node manifest to
the two-node manifest: that would intentionally remove a database instance.

## Evidence required before milestone completion

For Milestone 7, record Pod/node placement, synchronous-replication state, the
current primary, one active Mill batch, deletion of either Mill Pod type, and a
controlled deletion of the PostgreSQL primary. The batch must finish with
stable attempt/Job identities and exact output.

For Milestone 8, additionally record three K3s server/etcd voters, three
PostgreSQL instances on distinct nodes, failover-quorum status, loss of one
physical node, and isolation of a minority. The two-node majority must continue
without duplicate attempts or conflicting writers; the isolated minority must
not accept authoritative writes. Available external S3 is part of the test.

Never force PostgreSQL promotion to make a failed quorum test pass. A refusal
to promote an unsafe minority is the intended result.
