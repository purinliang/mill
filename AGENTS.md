# AGENTS.md

This file contains repository-wide instructions for coding agents working on
Mill. Follow the current milestone and preserve Mill's deliberately small V1
scope.

## Current project state

Mill is in Milestone 0: project foundation. During the initialization task, only
`README.md` and `AGENTS.md` may be created or modified. Do not add application
code, source directories, Dockerfiles, Kubernetes manifests, CI workflows,
Terraform, migrations, or other implementation files unless a later task
explicitly begins an implementation milestone.

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
- Store large datasets, manifests where appropriate, and task outputs in S3 or
  compatible object storage. Never store large binary datasets in PostgreSQL.
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
feature/* or fix/*
        |
        v
     develop
        |
        v
       main
```

- `main` contains stable project history and working milestones.
- `develop` is the normal integration branch for active development once
  implementation begins.
- Use short-lived `feature/<short-description>` branches for features.
- Use short-lived `fix/<short-description>` branches for fixes.
- Use `docs/<short-description>` branches for documentation-only work when a
  separate branch is useful.
- Merge completed, milestone-quality feature work into `develop`.
- Merge `develop` into `main` when the repository reaches a meaningful working
  milestone.
- Do not add release or hotfix machinery unless the project develops a concrete
  need for it.

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
