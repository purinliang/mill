# Integration tests

This directory contains tests that require PostgreSQL or assemble more than
one package, transport, process, or external-system protocol boundary.

```text
postgres/    repositories and workflows -----> PostgreSQL
process/     compiled service binaries ------> public transports
rpc/         execution client and server ----> in-memory gRPC
kubernetes/  runtime adapter ----------------> fake Kubernetes API
```

Unit and within-package tests stay beside their production packages. Tests in
this directory use exported APIs and skip PostgreSQL-backed cases when
`MILL_TEST_DATABASE_URL` is not set.

Each subdirectory is an independent Go test package named for an architecture
boundary. Files inside it are named for behavior and do not repeat the
directory name or an `_integration` suffix. Shared values and constructors
remain inside their owning test package in `fixtures_test.go`.

Executable environment setup and suite runners belong under `scripts/`, not
in this directory.
