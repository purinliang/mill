# AGENTS.md

This file contains repository-wide instructions for coding agents working on
Mill. Follow the current milestone and preserve Mill's deliberately small V1
scope.

## Current project state

Mill currently has a Job process and a standalone execution process. The
Job service validates and plans JSONL inputs and stores jobs, logical tasks,
attempts, retry eligibility, and progress in PostgreSQL. Execution replicas use
gRPC to lease that work and launch one native Kubernetes Job for each attempt.
The workload CLI contract, local-file execution, bounded retries, attempt
history, deterministic Kubernetes identity, and execution restart reconciliation
are implemented.

The object-storage adapter supports `file://` and `s3://`. S3-backed attempts
perform ranged reads and publish unique outputs without hostPath mounts or node
pinning. `scripts/demo-word-count-batch.sh` exercises the complete node-local
control plane; its failure and restart modes test retry exhaustion and process
recovery. Per-attempt PostgreSQL leases now provide renewable ownership,
expired-owner takeover, and fencing tokens for all state mutations.
The batch demo's `--replica-failover` mode runs two execution processes
concurrently. It rejects premature lease stealing, kills the primary, and
verifies fenced takeover of the same attempts and Kubernetes Jobs.
`scripts/demo-word-count-s3.sh` proves the shared-storage path against a
disposable S3-compatible service and exact local baseline.
`scripts/demo-word-count-deployed.sh` proves the same 12-task flow through
deployed Job and execution Pods in unique temporary namespaces, then
removes only its owned cluster resources and fixture containers while retaining
diagnostics. Its `--execution-failover` mode scales the execution Deployment to
two, deletes the active lease owner's Pod, and proves fenced takeover without
changing attempt or Kubernetes Job identities. `scripts/setup.sh` provides a
repeatable local kind environment. Its `--job-failover` mode separately
deletes the original REST/gRPC Pod after adding a ready replica and proves both
client paths reconnect without changing durable execution identity.

The backend-independent execution model and store contract have been extracted
from `internal/job`. A versioned Protobuf schema and gRPC client/server adapters
now carry the implemented lease operations with server-owned lease duration,
fencing tokens, deadlines, and domain error mapping. They are tested over an
in-memory transport. `cmd/mill-job` can serve the Job-side API on an optional
1 MiB bounded gRPC listener with graceful shutdown and no longer imports
coordinator or Kubernetes packages. `cmd/mill-execution` is the only runnable
gRPC-to-Kubernetes coordinator and has no Job-package or PostgreSQL dependency.
The batch demo's `--split-process` mode proves the 12-task flow
through one Job service and two live standalone execution replicas. One replica
may own all current leases while the other remains standby; Kubernetes workload
Pods, not execution replicas, provide task parallelism. Jobs select an optional
`small`, `medium`, or `large` resource class; the Job service persists resolved
CPU and memory values and execution replicas receive them through gRPC. The
`--replica-failover` mode proves survivor takeover across this boundary while
preserving attempt and Kubernetes Job identities.

Minimal non-root OCI images package the Job and execution services. The
execution service supports either an explicit kubeconfig context or standard
in-cluster service-account credentials, but never both. A local kind manifest
runs one replica of each service, separates control-plane and workload
namespaces, and limits the execution service to creating and getting Jobs. It
depends on externally managed, Pod-reachable PostgreSQL and S3-compatible
storage and makes no availability claim. Workload image inspection, generic
output verification/aggregation, wider fault recovery, PostgreSQL replication,
and multi-node availability remain planned. Add implementation only in small,
explicitly requested increments. Do not add more Dockerfiles, Kubernetes
manifests, CI workflows, Terraform, or unrelated infrastructure unless a later
task explicitly requires them.

`deploy/kubernetes/availability` contains the explicitly requested multi-node
foundation. Preserve the distinction between its alternative profiles: two
PostgreSQL instances use `dataDurability: preferred` for the limited
two-laptop demonstration; three instances use `dataDurability: required` plus
failover quorum. Both database and Mill replicas require distinct hostname
failure domains. Do not weaken anti-affinity to make an undersized cluster
appear available, and do not claim either profile has passed until its runtime
evidence exists.

Preserve `scripts/install-k3s-node.sh` as an explicit role-based installer. Keep
the K3s version and tagged installer digest pinned together; never accept a
join token as a command-line argument or commit it. Preserve
`scripts/deploy-availability.sh` checks for distinct ready hostname domains,
alternative database profiles, URL-safe database credentials, externally
reachable images/storage, migration-before-control-plane ordering, and Secret
redaction. Render requested images before creating Pods; required anti-affinity
must not deadlock a fresh deployment or rolling update. Preserve the
checksum-verified migration ledger and single-transaction application rather
than making historical DDL silently repeatable.

Preserve `scripts/demo-availability.sh` as the destructive two-node acceptance
proof. It must refuse fewer than two hostname failure domains, require all
three replica pairs to be ready and anti-affined before mutation, and retain
evidence. Execution, Job, and PostgreSQL primary Pod deletion must occur
against one active 12-task batch. Require fenced takeover, a changed database
primary, stable attempt and Kubernetes Job identities, exactly one attempt per
task, restored replicas, synchronous streaming, and exact merged output. Do
not describe this as node, partition, or object-storage tolerance.
The database shutdown bounds intentionally favor a short demonstration RTO;
keep that tradeoff explicit and do not present the values as production
defaults.

`scripts/demo-word-count-single-task.sh` runs one manual word-count Job with
staged node-local input and verifies its output against a local run. It uses
`examples/word-count/job.yaml.template`; it does not claim or transition
PostgreSQL tasks. Keep this demonstration distinct from the
control-plane Kubernetes adapter.

Do not describe planned behavior as implemented. Update the status in
`README.md` whenever a milestone materially changes the repository's actual
capabilities.

## Scope discipline

Before implementing a feature, answer:

1. What concrete problem does this solve?
2. Is it required for the current milestone?
3. Can Kubernetes, PostgreSQL, or S3 already solve it?
4. Does it introduce unnecessary distributed-system complexity?

Avoid speculative infrastructure. In particular, do not introduce Kafka,
Redis, a service mesh, extra databases, or extra services merely for technology
coverage. Add a dependency only when its concrete benefit outweighs its
operational and maintenance cost.

## Architecture and engineering principles

- Prefer simple designs over premature abstraction.
- Keep control-plane responsibilities separate from workload execution. Mill
  manages job intent and state; workload containers perform user computation.
- Treat PostgreSQL as the durable source of truth for job and task metadata
  unless a deliberate architecture change is documented.
- Store large datasets and task outputs in S3 or compatible object storage.
  Never store large binary datasets in PostgreSQL.
- Keep object access URI-oriented. Planning may stream a whole input; workload
  attempts must read only their assigned range and publish only their unique
  output. Preserve file-backed tests while S3 is the shared-storage direction.
- Keep one input URI and generated output root on the job. A logical task owns
  its shard index and input byte range; do not duplicate calculable input or
  output URIs on every task.
- Keep user-facing submission simple: `executable` plus `input`. JSONL is the
  only current format, partition sizing is internal policy, and parallelism is
  server configuration captured durably on each job.
- Keep workload resources server-defined. Accept only the named resource
  classes, default omission to `small`, and persist both the class and resolved
  integer requests/limits on the job. Pass resolved values—not policy names—to
  execution replicas, and retain the job's persisted settings for every retry.
- Use explicit, validated state transitions. Make transitions idempotent where
  retries, reconciliation, or process restarts can repeat an operation.
- Persist a `starting` attempt and mark its task active in one transaction
  before calling an external runtime. Permit at most one active attempt per
  task and retain terminal attempts as execution history.
- Keep retry decisions and the next eligible claim time durable and atomic with
  the failed attempt transition. A failed attempt remains failed; only its task
  returns to pending while budget remains. Delayed retries consume no active
  slot and must obey job parallelism when claimed. Never retry pending work for
  a terminal job, or let a stale attempt observation alter a newer attempt.
- Keep the initial retry policy fixed at three total attempts and five seconds
  between observed failure and eligibility. Changes to policy affect running
  jobs too; introduce persisted per-job configuration before configurable policy.
  Keep Kubernetes retries disabled while Mill owns the budget. Do not treat
  timeouts or missing running Jobs as safe evidence to launch replacements.
- Reconstruct active attempts from PostgreSQL on each coordinator tick. Use a
  deterministic Kubernetes Job name per attempt and verify its labels and UID.
  Treat API errors as ambiguous observations; do not fail/retry a task merely
  because a request timed out. Only terminal Job conditions release slots.
- Give every coordinator process a unique instance identity. Acquire and renew
  durable per-attempt leases in PostgreSQL, replace the fencing token on
  takeover, and require the current token for every state mutation. Lease expiry
  transfers observation ownership of the same attempt; it does not authorize a
  new attempt. Missing running Jobs and identity mismatches require
  investigation; never silently recreate them.
- Preserve `scripts/demo-word-count-batch.sh --restart-coordinator` as a real
  process-boundary recovery test. It must use SIGKILL only on the child
  execution PID, keep the Job service, PostgreSQL, and Kubernetes alive,
  compare stable attempt IDs and Job UIDs, reject duplicate attempts, and
  still verify the complete workload output.
- Preserve `scripts/demo-word-count-batch.sh --replica-failover` as the
  simultaneous-process lease test. Both processes must overlap before SIGKILL;
  the standby must not steal live leases, and takeover must change fencing
  tokens without changing attempt IDs, external UIDs, or Kubernetes Jobs.
- Preserve `scripts/demo-word-count-batch.sh --split-process` as the runtime
  boundary test. The Job service must not start an in-process coordinator, two
  standalone execution processes must remain live, neither process may receive
  PostgreSQL configuration, and all task outputs must match the local baseline.
- Preserve `scripts/demo-word-count-deployed.sh` as the packaged control-plane
  proof. Each run must use unique namespaces, keep the execution service free of
  PostgreSQL credentials, execute exactly 12 S3-backed tasks with bounded
  parallelism, reject hostPath/node placement, compare exact output, retain
  diagnostics, and remove only resources created by that run.
- Preserve `scripts/demo-word-count-deployed.sh --execution-failover` as the
  single-node execution Pod failure proof. The standby must first respect live
  leases; deletion must target the Pod whose instance owns the initial leases;
  takeover must replace lease owners and fencing tokens while preserving task,
  attempt, external UID, Kubernetes Job name, and Job UID identities. Require
  exactly 12 first attempts, restored replica availability, and exact output.
  Do not describe this as Job, database, storage, node, or partition
  availability.
- Preserve `scripts/demo-word-count-deployed.sh --job-failover` as the
  single-node Job Pod failure proof. Begin with one endpoint, add and
  verify a ready standby, delete the original Pod, and require both retrying
  REST access and the existing execution-service gRPC client to recover through
  the Service. Preserve lease owner/token and attempt/Job identities, exactly 12
  first attempts, restored replica availability, and exact output. Do not
  describe this as database, storage, node, or partition availability.
- Do not implement a custom cluster scheduler when Kubernetes provides a
  suitable primitive. Initially map one Mill attempt to one Kubernetes Job so
  its arguments, output, and retry history remain independently observable.
  Reconsider Indexed Jobs only for a concrete requirement that justifies a
  shared shard-manifest lookup contract.
- Use `IfNotPresent` for workload images so a multi-node cluster can pull a
  trusted registry image while local demonstrations can use images imported
  into each node. Do not use `Never` outside an explicitly manual fixture.
- Keep the workload/container contract minimal and stable. Changes to it require
  documentation and compatibility consideration. Mill-owned CLI flags precede
  a mandatory `--`; arguments after it belong unchanged to the executable.
- Make failures observable, reproducible, and testable. Do not silently discard
  reconciliation errors or ambiguous execution state.
- Prefer deterministic tests where practical. Add fault and recovery tests as
  distributed behavior is introduced.
- The approved deployment boundary is one replicated Job service and
  replicated execution service. Keep partitioning inside the Job service and
  do not create a separate partition service without measured independent
  scaling need.
  Do not split other packages into services merely to increase Pod count.
- As the service-boundary milestone proceeds, make the Job service the sole
  owner of Mill metadata tables. Execution replicas must access the implemented
  lease operations through a versioned Protobuf/gRPC domain API with deadlines,
  fencing tokens, state guards, and idempotent mutations. Do not use gRPC as a
  durable queue or change the workload CLI contract to gRPC.
- Availability claims must identify their failure domain. Two laptops may
  demonstrate individual Pod/process and controlled primary/standby failure;
  do not describe that as whole-node or network-partition tolerance. Quorum-safe
  node loss requires three independent voters and available object storage.
- Prefer consistency to conflicting writes during an ambiguous partition.
  Delegate Kubernetes consensus to etcd and PostgreSQL promotion to the chosen
  database operator; Mill must not implement either election.
- Assume trusted workloads for V1. Do not expand a feature into arbitrary
  untrusted-code sandboxing or multi-tenant security without an explicit scope
  change.

## Go code quality

When Go implementation begins:

- follow standard Go project conventions and keep packages cohesive;
- use `gofmt` on changed Go files;
- run `go test ./...` before considering a change complete;
- measure coverage with `scripts/test-coverage.sh`; it excludes generated
  `*.pb.go` statements while retaining those files in compilation and testing;
- avoid interfaces that do not provide a current testing or substitution need;
- return errors explicitly and wrap them with useful operational context;
- prefer standard-library solutions when they are reasonable; and
- add or update tests with every behavior change.

Keep domain decisions visible in the code. Avoid generic frameworks that hide
job, task, shard, attempt, or state-transition semantics.

## Code organization

- Keep Mill as one Go module with the implemented Job and execution
  binaries. A logical module is not automatically a service; do not create more
  service boundaries without a concrete operational reason.
- Keep `cmd/mill-job` as the composition root: environment configuration,
  dependency construction, route assembly, process lifecycle, and shutdown
  belong there. Do not put job or execution policy in `main.go`.
- Organize `internal` by cohesive capability, not by generic technical layers.
  Keep job policy and required contracts in `internal/job`, then place concrete
  adapters in `internal/job/httpapi`, `internal/job/partition`, and
  `internal/job/postgres`. Keep attempt persistence in
  `internal/execution/postgres`, beside the execution domain whose transitions
  and ownership rules it implements.
- Introduce a new package only for a concrete boundary with a distinct purpose,
  such as a Kubernetes adapter or object-storage adapter. Do not pre-create
  empty packages or speculative `common`, `util`, `service`, or `manager`
  layers.
- Keep dataset partitioning in `internal/job/partition`. Its public
  `partitioner.go` implements the partitioner port owned by the job workflow;
  `jsonl.go` keeps current format-specific scanning private. Logical shard
  boundaries must be contiguous, non-empty, and aligned to complete records.
- Keep `internal/objectstore` limited to file and S3-compatible access. A custom
  endpoint is a local-development concern; use normal AWS SDK endpoint and
  credential resolution in AWS. Close read bodies and require seekable bodies
  for the current complete-object upload path.
- Keep the exported object-store API in `internal/objectstore/store.go`.
  URI parsing and the file and S3 backends belong in separate files with
  lowercase implementation names. Backends receive only validated locations.
- Keep backend-independent task observation/claim logic in
  `internal/execution/coordinator`, Kubernetes types and API calls in
  `internal/execution/kubernetes`, and their lifecycle/configuration in
  `cmd/mill-execution/main.go`. Word-count aggregation stays in the example.
- Require the execution service to select exactly one Kubernetes credential
  source. Outside the cluster, use an explicit kubeconfig context. Inside it,
  use client-go's standard service-account configuration. Do not silently fall
  back between clusters. Keep namespace selection explicit in both modes.
  Grant only the RBAC operations the execution service actually uses.
- Keep the local deployment's Mill services in `mill-system` and generated Jobs
  in `mill-workloads`. The Job service must not mount a service-account token.
  The execution Role remains namespace-scoped to `create` and `get` Jobs unless
  a concrete implemented operation requires another verb or resource. Never
  provide PostgreSQL credentials to the execution service or grant it
  permission to read workload Secrets.
- Treat `deploy/kubernetes/local` as a kind-only, single-replica baseline. It
  uses preloaded development images and external PostgreSQL/S3; do not reuse it
  to claim K3s, database, node, or object-storage availability. Keep secret
  values out of manifests and Git.
- Preserve the first boundary refactor completed after Milestone 7: domain
  packages own policy and interfaces, while HTTP, JSONL, PostgreSQL, gRPC, and
  Kubernetes packages implement adapters. Defer another broad architecture
  refactor until Milestone 8 provides new failure evidence.
- Keep execution-facing attempt types, sentinel domain failures, and the
  transport-independent store contract in `internal/execution`. Coordinator
  and Kubernetes packages must not import `internal/job`.
- Keep the versioned schema under `api/proto/mill/execution/v1` and transport
  adapters in `internal/execution/rpc`. Lease duration is Job policy,
  never an execution-service request field. Commit generated bindings with
  schema changes and do not hand-edit them. The RPC client package must not
  pull in `internal/job` or PostgreSQL transitively.
- Keep the language-neutral CLI protocol and its Go serialization/parser in
  `internal/workload`. Reserve top-level `cmd` for Mill's own executables.
  Example executable entrypoints belong under `examples/<name>/cmd/<command>`,
  alongside demonstration-specific computation, inputs, generators, and
  documentation under `examples/<name>`. All must remain
  separate from control-plane behavior. Introduce a top-level `workloads`
  package only if Mill later owns reusable workload implementations beyond
  examples.
- Keep failure injection in test/demo wrappers, not production execution policy
  or word-count computation. Prefer deterministic cases first. The example's
  shared fail-once marker is a test fixture, not Mill's retry state. Aggregate
  only successful attempt outputs returned by Mill; never glob all attempts.
- Keep a reference workload's Dockerfile beside its command. Prefer a
  multi-stage build and a minimal non-root runtime image; do not place build
  tools in the final workload image.
- Keep each Mill service Dockerfile beside its top-level `cmd` entrypoint. The
  final service images contain only the static binary and CA certificates, run
  as `65532:65532`, and are built together by the
  `scripts/build-control-plane-images.sh` script. Image creation is distinct
  from loading or deploying an image.
- Commit small, stable source fixtures and deterministic generation
  configuration when they explain a demonstration. Do not commit generated
  JSONL inputs, task outputs, or other reproducible artifacts.
- Keep interfaces at the consumer boundary and add them only for an existing
  substitute. For example, the job HTTP handler owns the small store interface
  used by its tests; the concrete PostgreSQL repository does not need an
  interface merely because it accesses a database.
- Keep numbered SQL migrations in `migrations`. After a migration has been
  shared or applied outside a disposable local database, correct the schema
  with a new migration instead of rewriting history.
- Keep `scripts/setup.sh` idempotent and non-destructive. It may install pinned
  user-space development tools and create or reuse the named local cluster, but
  must not silently install Docker, change host permissions, replace clusters,
  or delete resources.
- Co-locate unit tests with the package under test. Name external-dependency
  tests clearly as integration tests and make them opt-in when they require a
  developer-managed service.
- Update the module view, repository structure, and current status in
  `README.md` when a change makes any of them materially inaccurate. Keep
  detailed design in `docs/architecture.md` and operational commands and
  structure in `docs/development.md`. Document planned paths as planned; do not
  create placeholder files for them.

## Testing expectations

- Test behavior through exported methods, APIs, and process boundaries. Do not
  call unexported production functions directly merely to increase coverage;
  exercise their effects through the public contract instead.
- Unit-test domain validation, progress calculation, and allowed state
  transitions.
- Integration-test PostgreSQL behavior and transaction boundaries once
  persistence exists.
- Use small store fakes to test consumers of durable state. Test the PostgreSQL
  repository itself against real disposable PostgreSQL rather than mocking SQL
  calls; row locks, constraints, transactions, and concurrent claims are part
  of the behavior Mill relies on.
- Test the workload contract independently of orchestration.
- Test object storage with a bounded fake S3 endpoint and preserve the live
  `scripts/demo-word-count-s3.sh` check for ranged reads, output publication,
  and absence of node-local mounts.
- Add Kubernetes end-to-end tests only when Kubernetes execution is introduced.
- Cover retries, duplicate reconciliation, partial failure, and restart recovery
  during the reliability milestone.
- Test lease contention, renewal, expiry takeover, and stale-token rejection
  against PostgreSQL. Preserve the process SIGKILL demonstration as an
  end-to-end check of stable attempt and Kubernetes identities.
- Keep tests hermetic where practical, and document any required external
  service or cluster.
- Test composition roots as child processes through their public transports;
  do not call private `main`/`run` helpers for coverage. Bound startup and
  shutdown waits, capture diagnostics, and always terminate child processes.

## Change review and handoff

- Keep each change focused on one coherent, observable behavior.
- At handoff, explain what changed, why it changed, what the tests demonstrate,
  and which relevant behavior remains untested.
- Review distributed behavior in terms of ownership, durable state, concurrency,
  retries, reconciliation, and failure boundaries rather than incidental
  plumbing.
- Preserve concise `why` comments beside code that protects a non-obvious
  invariant. Do not add comments that merely restate the next line.
- Treat tests as executable evidence for claims about retries, crash recovery,
  persistence ordering, concurrency, and availability.

## Git workflow

Use a lightweight GitFlow-style workflow:

```text
feature/*, fix/*, or docs/*
              |
              | rebase, then --ff-only
              v
           develop
              |
              | --no-ff at a working milestone
              v
             main
```

### Branch rules

- `main` contains stable project history and working milestones. Do not develop
  directly on it.
- `develop` is the integration branch for completed work. Do not use it as a
  working branch.
- Create each working branch from an up-to-date `develop` using
  `feature/<short-description>`, `fix/<short-description>`, or
  `docs/<short-description>`.
- Keep one task or coherent change on each working branch, and keep the branch
  short-lived.
- Rebase a working branch onto current `develop` before integration. Do not add
  repeated "merge develop" commits to working branches.
- Never force-push `main` or `develop`. Use `--force-with-lease`, never
  `--force`, on a rebased working branch only when its rewrite is intentional.
- Delete working branches after they are merged.
- Do not add release or hotfix branches unless the project develops a concrete
  need for them.

### Merge rules

- Rebase a completed working branch onto current `develop`, then integrate it
  with `git merge --ff-only <branch>`. Working-branch integration stays linear;
  do not create merge commits for small feature, fix, or documentation branches.
- Do not squash coherent commits merely to reduce the commit count. Clean up
  temporary or low-value agent/WIP commits before the final rebase when they
  would obscure the branch's intent.
- Merge `develop` into `main` with `--no-ff` only when a meaningful working
  milestone is reached.
- Run the relevant tests and inspect the complete diff before either integration
  merge. Record material test limitations in the handoff.
- Use a Conventional Commit-style subject for an intentional merge commit when
  Git does not generate one that clearly identifies the merged branch.

For AI-assisted work, an agent may create, commit, rebase, and push its working
branch as part of an authorized task. The agent must not merge into or push
`develop` or `main` unless the user explicitly asks for that integration. Before
integrating, report the branch, commits, changed files, tests, and any unresolved
risks so the maintainer can inspect the unit of work.

When repository hosting rules are configured, protect `main` and `develop` from
force-push and deletion. A solo maintainer may review a pushed working branch
and then perform the rebase and fast-forward integration locally. If pull
requests are required, use a rebase-style merge that keeps `develop` linear;
GitHub does not expose a true fast-forward-only pull-request merge. Add required
status checks only when the repository has CI checks worth enforcing.

Prefer clean, focused commits. Do not mix unrelated refactors with behavior
changes.

## Commit messages

Use Conventional Commits:

```text
<type>(<optional-scope>): <imperative summary>
```

Recommended types are `feat`, `fix`, `refactor`, `test`, `docs`, `build`, `ci`,
and `chore`; `style`, `perf`, `revert` may be used when they describe the change
more accurately. A scope is optional when the repository-wide context is
clearer without one. Keep the subject concise and imperative, and do not end it
with a period. Add a body when the reason or impact is not obvious. Mark a
breaking change with `!` before the colon and describe it in a
`BREAKING CHANGE:` footer.

Examples:

```text
feat(api): add job creation endpoint
feat(execution): launch indexed Kubernetes jobs
feat(storage): persist task state in PostgreSQL
test(execution): cover failed task retry
docs(readme): document execution lifecycle
refactor(job): simplify task state transitions
```

## Repository hygiene

- Do not commit credentials, secrets, local environment files, or cloud account
  identifiers.
- Do not commit generated files or build artifacts unless the repository later
  documents a specific reason to version them.
- Keep commits limited to the task in scope and preserve unrelated user changes.
- Document new operational prerequisites and developer commands when they are
  introduced.
