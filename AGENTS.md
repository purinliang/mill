# AGENTS.md

This file contains repository-wide instructions for coding agents working on
Mill. Follow the current milestone and preserve Mill's deliberately small V1
scope.

## Current project state

Mill is in Milestone 1: local single-process control plane. The project
foundation, HTTP process, PostgreSQL connection/readiness behavior, numbered
`jobs` and `tasks` migrations, and create/get job API are implemented. Job
submission validates one local JSONL input and transactionally materializes
pending tasks containing logical byte ranges at record boundaries. Workload
image inspection, task execution, attempts, results, automatic recovery,
retries, S3, Docker integration, and Kubernetes are not implemented. Add
implementation only in small, explicitly requested increments. Do not add
Dockerfiles, Kubernetes manifests, CI workflows, Terraform, or unrelated
infrastructure unless a later task requires them.

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
- Do not implement a custom cluster scheduler when Kubernetes provides a
  suitable primitive. Evaluate native Jobs, including Indexed Jobs, first.
- Keep the workload/container contract minimal and stable. Changes to it require
  documentation and compatibility consideration.
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
- Keep interfaces at the consumer boundary and add them only for an existing
  substitute. For example, the job HTTP handler owns the small store interface
  used by its tests; the concrete PostgreSQL repository does not need an
  interface merely because it accesses a database.
- Keep numbered SQL migrations in `migrations`. After a migration has been
  shared or applied outside a disposable local database, correct the schema
  with a new migration instead of rewriting history.
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
