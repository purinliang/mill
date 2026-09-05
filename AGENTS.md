# AGENTS.md

This file contains repository-wide instructions for coding agents working on
Mill. Follow the current milestone and preserve Mill's deliberately small V1
scope.

## Current project state

Mill has completed Milestone 2's container workload contract. Milestone 1's
HTTP process, PostgreSQL connection/readiness behavior, create/get API, local
JSONL logical sharding, and durable job/task materialization are implemented.
CLI argument serialization, local JSONL-copy and word-count reference workloads,
their minimal non-root Docker images, and a deterministic word-count input
generator are also implemented. An optional coordinator now launches native
Kubernetes Jobs through the official Go client, using PostgreSQL task claims
and attempt transitions. The local adapter uses staged input/output paths on
one kind node, and successful job status includes attempt output URIs.
`scripts/demo-word-count-batch` exercises the HTTP API, PostgreSQL, concurrent
Pods, and example-specific result merging. Mill now retries terminal task
failures up to three total attempts with a durable five-second delay. The batch
demo's `--failure once|always` modes exercise recovery and retry exhaustion with
a test-only workload wrapper. Workload image inspection, generic output
verification/aggregation, full fault recovery, and S3 remain planned.
`scripts/setup` provides a repeatable local kind environment.
Add implementation only in small,
explicitly requested increments. Do not add more Dockerfiles, Kubernetes
manifests, CI workflows, Terraform, or unrelated infrastructure unless a later
task requires them.

`scripts/demo-word-count-single-task` runs one manual word-count Job with staged
node-local input and verifies its output against a local run. It uses
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
- Keep one input URI and generated output root on the job. A logical task owns
  its shard index and input byte range; do not duplicate calculable input or
  output URIs on every task.
- Keep user-facing submission simple: `executable` plus `input`. JSONL is the
  only current format, partition sizing is internal policy, and parallelism is
  server configuration captured durably on each job.
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
- Keep one coordinator per database using the dedicated advisory-lock
  connection. Preserve the same cluster and storage configuration on restart.
  Missing running Jobs and identity mismatches require investigation; never
  silently recreate them.
- Do not implement a custom cluster scheduler when Kubernetes provides a
  suitable primitive. Initially map one Mill attempt to one Kubernetes Job so
  its arguments, output, and retry history remain independently observable.
  Reconsider Indexed Jobs only for a concrete requirement that justifies a
  shared shard-manifest lookup contract.
- Keep the workload/container contract minimal and stable. Changes to it require
  documentation and compatibility consideration. Mill-owned CLI flags precede
  a mandatory `--`; arguments after it belong unchanged to the executable.
- Make failures observable, reproducible, and testable. Do not silently discard
  reconciliation errors or ambiguous execution state.
- Prefer deterministic tests where practical. Add fault and recovery tests as
  distributed behavior is introduced.
- Keep the API and coordinator as logical boundaries; do not split them into
  deployable services without a concrete operational reason.
- Assume trusted workloads for V1. Do not expand a feature into arbitrary
  untrusted-code sandboxing or multi-tenant security without an explicit scope
  change.

## Go code quality

When Go implementation begins:

- follow standard Go project conventions and keep packages cohesive;
- use `gofmt` on changed Go files;
- run `go test ./...` before considering a change complete;
- avoid interfaces that do not provide a current testing or substitution need;
- return errors explicitly and wrap them with useful operational context;
- prefer standard-library solutions when they are reasonable; and
- add or update tests with every behavior change.

Keep domain decisions visible in the code. Avoid generic frameworks that hide
job, task, shard, attempt, or state-transition semantics.

## Code organization

- Keep Mill as one Go module and one control-plane binary until a concrete
  deployment or ownership boundary requires otherwise. A logical module is not
  automatically a service.
- Keep `cmd/mill` as the composition root: environment configuration,
  dependency construction, route assembly, process lifecycle, and shutdown
  belong there. Do not put job or execution policy in `main.go`.
- Organize `internal` by cohesive capability, not by generic technical layers.
  The current `internal/job` package may contain its model, validation, HTTP
  handler, and PostgreSQL repository while that keeps the job behavior easy to
  understand in one place.
- Introduce a new package only for a concrete boundary with a distinct purpose,
  such as a Kubernetes adapter or object-storage adapter. Do not pre-create
  empty packages or speculative `common`, `util`, `service`, or `manager`
  layers.
- Keep local JSONL partition planning in `internal/job/partition.go` while it is
  part of the cohesive job-creation workflow. Logical shard boundaries must be
  contiguous, non-empty, and aligned to complete JSONL records.
- Keep backend-independent task observation/claim logic in
  `internal/coordinator`, Kubernetes types and API calls in
  `internal/kubernetes`, and their lifecycle/configuration in
  `cmd/mill/execution.go`. Word-count aggregation stays in the example.
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
- Keep `scripts/setup` idempotent and non-destructive. It may install pinned
  user-space development tools and create or reuse the named local cluster, but
  must not silently install Docker, change host permissions, replace clusters,
  or delete resources.
- Co-locate unit tests with the package under test. Name external-dependency
  tests clearly as integration tests and make them opt-in when they require a
  developer-managed service.
- Update the module view, repository structure, and current status in
  `README.md` when a change makes any of them materially inaccurate. Document
  planned paths as planned; do not create placeholder files for them.

## Testing expectations

- Unit-test domain validation, progress calculation, and allowed state
  transitions.
- Integration-test PostgreSQL behavior and transaction boundaries once
  persistence exists.
- Test the workload contract independently of orchestration.
- Add Kubernetes end-to-end tests only when Kubernetes execution is introduced.
- Cover retries, duplicate reconciliation, partial failure, and restart recovery
  during the reliability milestone.
- Keep tests hermetic where practical, and document any required external
  service or cluster.

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
feat(executor): launch indexed Kubernetes jobs
feat(storage): persist task state in PostgreSQL
test(executor): cover failed task retry
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
