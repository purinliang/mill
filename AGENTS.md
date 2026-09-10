# Mill repository instructions

These repository-wide instructions apply to every coding agent working on
Mill. Follow the current milestone and preserve the deliberately small,
learning-oriented scope.

Read the nearest package `README.md` and `AGENTS.md` before changing a package.
The root [README](README.md) summarizes the project. System design lives in
[Architecture](docs/architecture.md), staged work in the
[roadmap](docs/roadmap.md), and operational guides are grouped under
`docs/development` and `docs/deployment`.

## Scope discipline

Before implementing a feature, answer:

1. What concrete problem does this solve?
2. Is it required for the current milestone?
3. Can Kubernetes, PostgreSQL, or S3 already solve it?
4. Does it introduce unnecessary distributed-system complexity?

Avoid speculative infrastructure. Do not introduce Kafka, Redis, a service
mesh, extra databases, or extra services merely for technology coverage. Add
a dependency only when its concrete benefit outweighs its operational and
maintenance cost.

## Engineering principles

- Prefer simple designs over premature abstraction.
- Keep control-plane state management separate from workload execution.
- Treat PostgreSQL as the durable source of truth for Mill metadata unless a
  deliberate architecture change is documented.
- Store large datasets and outputs in S3-compatible object storage, never in
  PostgreSQL.
- Use explicit state transitions and make repeated reconciliation safe.
- Do not implement scheduling, consensus, or database election mechanisms
  already provided by Kubernetes, etcd, or the database operator.
- Keep workload/container contracts minimal and stable.
- Make failures observable, reproducible, and testable.
- Prefer deterministic tests and add fault-recovery tests with new distributed
  behavior.
- Keep availability claims tied to the exact failure domain demonstrated.
- Assume trusted workloads in V1; do not claim production readiness or
  multi-tenant isolation.
- Do not create a service merely because a Go package exists. A service needs
  a concrete ownership, scaling, or failure-isolation reason.

Package-specific rules live with their code:

- [Job](internal/job/README.md)
- [Execution](internal/execution/README.md)
- [Object storage](internal/objectstore/README.md)
- [Workload contract](internal/workload/README.md)
- [Integration tests](test/integration/README.md)

## Go code quality

- Follow standard Go conventions and keep packages cohesive.
- Run `gofmt` on changed Go files.
- Run `go test ./...` before considering a change complete.
- Measure coverage with `scripts/test-coverage.sh`; generated Protobuf
  statements are excluded from the percentage but remain compiled and tested.
- Avoid interfaces without a current consumer, test, or substitution need.
- Return errors explicitly and wrap them with useful operational context.
- Prefer standard-library solutions where reasonable.
- Add or update tests with every behavior change.
- Keep comments focused on contracts and non-obvious reasons, not line-by-line
  restatements of code.

## Test organization

- Keep unit and within-package tests beside the production package.
- Put Go tests requiring PostgreSQL, an external-system protocol, or multiple
  package/process boundaries in `test/integration`. Do not add `_integration`
  to filenames there.
- Put executable test and demonstration runners in `scripts`, using a `.sh`
  extension. Test logic, fixtures, and assertions belong in Go tests when
  practical.
- Test through exported methods, transports, and process boundaries. Do not
  expose private production helpers merely to increase coverage.
- Test PostgreSQL behavior against a disposable real PostgreSQL instance, not
  a mocked SQL layer; locks, constraints, and transactions are part of the
  behavior under test.
- Keep tests hermetic where practical and document required external systems.
- Bound process startup and shutdown waits, retain useful diagnostics, and
  always clean up child processes owned by a test.

## Change review and handoff

- Keep each change focused on one coherent, observable behavior.
- Preserve unrelated user changes in a dirty worktree.
- Inspect the complete diff and run proportionate tests before integration.
- At handoff, state what changed, why, what was tested, and any remaining risk.
- Treat tests as executable evidence for claims about concurrency, retries,
  crash recovery, persistence ordering, and availability.

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

- `main` contains stable working milestones; do not develop directly on it.
- `develop` is the integration branch; do not use it as a working branch.
- Create `feature/<short-description>`, `fix/<short-description>`, or
  `docs/<short-description>` from an up-to-date `develop`.
- Keep one coherent change per short-lived working branch.
- Rebase a working branch onto `develop` before integration; avoid repeated
  merge commits from `develop` into the working branch.
- Never force-push `main` or `develop`. Use `--force-with-lease` only for an
  intentional rewrite of a working branch.
- Delete working branches after integration.

### Merge rules

- Integrate a rebased working branch into `develop` with `--ff-only`.
- Merge `develop` into `main` with `--no-ff` only at a meaningful working
  milestone.
- Do not squash coherent commits only to reduce commit count. Remove temporary
  or low-value WIP commits when they obscure the branch history.
- An agent may create, commit, rebase, and push its working branch during an
  authorized task. It must not merge into or push `develop` or `main` without
  explicit user approval.

When hosting rules exist, protect `main` and `develop` from deletion and force
pushes. If pull requests are required, use rebase-style integration to keep
`develop` linear; GitHub does not provide a true fast-forward-only PR merge.

## Commit messages

Use Conventional Commits:

```text
<type>(<optional-scope>): <imperative summary>
```

Recommended types are `feat`, `fix`, `refactor`, `test`, `docs`, `build`, `ci`,
and `chore`. Keep the subject concise and imperative without a trailing period.
Use a body when the reason or impact is not obvious.

Examples:

```text
feat(api): add job creation endpoint
feat(execution): launch Kubernetes jobs
test(execution): cover failed task retry
docs(readme): document execution lifecycle
refactor(job): simplify task state transitions
```

## Repository hygiene

- Do not commit credentials, secrets, local environment files, cloud account
  identifiers, generated files, or build artifacts.
- Keep commits limited to the requested task.
- Document new operational prerequisites and developer commands.
