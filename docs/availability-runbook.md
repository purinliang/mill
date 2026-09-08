# Availability runbook

This runbook prepares the Milestone 7 two-laptop profile and Milestone 8
three-node quorum profile. It does not turn an untested topology into an
availability claim: retain the output of every check and failure exercise.

The two-node workflow and acceptance runner have passed on two kind nodes on
one laptop. That validates orchestration behavior but not the physical failure
domain required to complete Milestone 7.

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

On the first laptop, copy K3s's administrator kubeconfig without changing the
root-owned original, replace its loopback API address with the server's LAN
address, and give its context a distinct name:

```bash
mkdir -p "$HOME/.kube"
sudo install -o "$USER" -g "$(id -gn)" -m 600 \
  /etc/rancher/k3s/k3s.yaml "$HOME/.kube/mill-k3s.yaml"

kubectl --kubeconfig "$HOME/.kube/mill-k3s.yaml" \
  config set-cluster default --server=https://<server-lan-ip>:6443
kubectl --kubeconfig "$HOME/.kube/mill-k3s.yaml" \
  config rename-context default mill-k3s
export KUBECONFIG="$HOME/.kube/mill-k3s.yaml"
```

Then check the exact roles and hostname failure domains:

```bash
kubectl --context <context> get nodes -o wide
kubectl --context <context> get nodes \
  -L kubernetes.io/hostname,node-role.kubernetes.io/control-plane
```

The two-node profile requires two distinct ready hostnames. The three-node
profile requires three ready nodes that all show control-plane and etcd roles.

## Make demonstration images available

Using a registry with immutable tags is the normal multi-node path. For the
two-laptop learning demonstration, the same images may instead be imported
directly into K3s on both nodes. Build one archive on the first laptop:

```bash
MILL_JOB_IMAGE=mill/job-service:m7 \
MILL_EXECUTOR_IMAGE=mill/executor:m7 \
  ./scripts/build-control-plane-images

docker build \
  --file examples/word-count/cmd/fault-injection/Dockerfile \
  --tag mill/word-count-fault:m7 .

docker save --output /tmp/mill-m7-images.tar \
  mill/job-service:m7 mill/executor:m7 mill/word-count-fault:m7
sudo k3s ctr images import /tmp/mill-m7-images.tar
scp /tmp/mill-m7-images.tar <second-laptop>:/tmp/mill-m7-images.tar
```

On the second laptop, import the transferred archive:

```bash
sudo k3s ctr images import /tmp/mill-m7-images.tar
```

The workload uses `IfNotPresent`: a node uses the imported image when present
and otherwise tries its configured registry. Use unique immutable tags for a
real registry; do not reuse `m7` after changing the binaries.

## Start temporary shared object storage

External AWS S3 is valid. For a LAN-only demonstration, the first laptop may
run the same small S3-compatible fixture used by local tests. It is deliberately
not redundant, so Milestone 7 does not claim object-storage or laptop failure
tolerance:

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID="mill$(openssl rand -hex 8)"
export AWS_SECRET_ACCESS_KEY="$(openssl rand -hex 32)"

docker run --detach --name mill-m7-storage \
  --publish 8333:8333 \
  --env "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  --env "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  --env 'S3_BUCKET=mill-input,mill-output' \
  chrislusf/seaweedfs:4.44 mini -dir=/data

export MILL_S3_ENDPOINT=http://<first-laptop-lan-ip>:8333
curl "$MILL_S3_ENDPOINT/"
```

Allow TCP 8333 from the second laptop and the K3s Pod network only for the
duration of the exercise. Keep these generated credentials in the same shell
for deployment and verification.

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

For the registry-free two-laptop commands above, use:

```bash
export MILL_KUBE_CONTEXT=mill-k3s
export MILL_AVAILABILITY_PROFILE=two-node
export MILL_DATABASE_PASSWORD="$(openssl rand -hex 24)"
export MILL_JOB_IMAGE=mill/job-service:m7
export MILL_EXECUTOR_IMAGE=mill/executor:m7
export MILL_OUTPUT_ROOT_URI=s3://mill-output
export MILL_WORKLOAD_S3_ENDPOINT="$MILL_S3_ENDPOINT"
./scripts/deploy-availability
```

After it reports two replicas of each component on distinct nodes, run the
destructive acceptance exercise:

```bash
export MILL_WORKLOAD_IMAGE=mill/word-count-fault:m7
export MILL_DEMO_INPUT_ROOT_URI=s3://mill-input
./scripts/demo-availability
```

The demonstration creates one 12-task batch and, while its first wave is held
open, deletes an active executor Pod, its current Job-service endpoint Pod, and
the CloudNativePG primary Pod. It requires fenced executor takeover, REST and
database reconnection, a different promoted primary, unchanged attempt and
Kubernetes Job identities, exactly 12 first attempts, and byte-exact merged
output. It does not delete the job or its objects afterward. All snapshots and
logs are retained in the printed `/tmp/mill-availability.*` directory.

The demonstration profiles bound `stopDelay` and `switchoverDelay` to 30
seconds and reserve 15 seconds for smart shutdown. These values make the
failure exercise observable on small machines but favor recovery time over the
operator's conservative production defaults. Reassess them from measured RPO
and RTO requirements before treating either profile as production guidance.

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

After preserving the evidence, remove the temporary object-store container
explicitly with `docker rm -f mill-m7-storage` and delete the transferred image
archives from both laptops. Do not delete the K3s cluster until the evidence
has been reviewed.
