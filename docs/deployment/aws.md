# AWS

This planned deployment combines Mill's first AWS environment with its
three-node quorum demonstration. It is not implemented and makes no production
availability claim.

## Architecture

```text
                              AWS Region

Client --> stable entry point (NLB or equivalent)
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
       EC2 node A     EC2 node B     EC2 node C
          AZ A           AZ B           AZ C
       K3s + etcd     K3s + etcd     K3s + etcd
       PostgreSQL     PostgreSQL     PostgreSQL
          |              |              |
          `--------------+--------------'
                         |
             Kubernetes schedules across nodes
                Job-service replicas
             execution-service replicas
                 workload Job Pods
                         |
                 +-------+-------+
                 |               |
                 v               v
             Amazon S3       Amazon ECR
          input and output   OCI images
```

Each EC2 node is a K3s server and etcd voter. Three voters retain a majority
after one node fails. CloudNativePG places one PostgreSQL instance on each node
and owns synchronous replication and safe primary promotion. Mill does not
implement either consensus mechanism.

S3 stores datasets and task outputs so data remains accessible from surviving
nodes. ECR distributes immutable Mill and workload images. Multiple Job and
execution replicas are spread across nodes through Kubernetes anti-affinity.

## Delivery order

1. Provision the VPC, three EC2 nodes, security groups, IAM roles, S3, ECR,
   and the stable entry point with Terraform.
2. Push immutable images and verify the existing local demonstration against
   real S3 before introducing cluster failures.
3. Form the three-server K3s cluster, install CloudNativePG, and deploy the
   existing three-instance PostgreSQL profile and Mill replicas.
4. Run the 12-task word-count batch, then exercise Pod, database-primary, and
   EC2-node failures while recording the result.
5. Review the evidence and manually destroy all billable resources.

## Completion evidence

Milestone 8 is complete only when the demonstration records:

- three Ready K3s server and etcd voters in distinct Availability Zones;
- one healthy PostgreSQL primary and two synchronized replicas;
- Mill replicas placed on different nodes;
- continued operation of the two-node majority after one EC2 node stops;
- refusal by an isolated minority to accept authoritative writes;
- stable task, attempt, and Kubernetes Job identities during recovery;
- no conflicting PostgreSQL writer or duplicate workload execution; and
- a byte-exact final result whose inputs and outputs remain in S3.

## Deliberate constraints

- Use K3s rather than EKS so the project exposes control-plane quorum and
  avoids an additional managed-cluster charge.
- Use CloudNativePG rather than RDS so PostgreSQL replication and promotion
  remain part of the failure exercise.
- Start near two vCPUs and 4 GiB per EC2 node, then adjust from measurements.
  Use a small workload class during the quorum test.
- Use scoped IAM roles and temporary credentials. Never commit AWS access
  keys, account identifiers, generated state, or secrets.
- Keep Terraform apply, acceptance testing, and destroy manual. CI must not
  create billable infrastructure for an ordinary commit.
- Defer autoscaling, cross-region recovery, untrusted workloads, and
  production hardening.

The operational cluster and failure procedure are documented in
[Availability](availability.md). Milestone state and later automation work
remain in the [roadmap](../roadmap.md).
