# Integration tests

This directory contains tests that require PostgreSQL or assemble more than
one package, transport, process, or external-system protocol boundary.

```text
Job repository ------------------------------> PostgreSQL
Job process ------------ REST + gRPC --------> PostgreSQL
execution process ------ gRPC + HTTP --------> fake Kubernetes API
Kubernetes adapter --------------------------> fake Kubernetes API
```

Unit and within-package tests stay beside their production packages. Tests in
this directory use exported APIs and skip PostgreSQL-backed cases when
`MILL_TEST_DATABASE_URL` is not set.

Files are named for the behavior or boundary they test. Because the directory
already establishes the integration-test scope, filenames do not repeat an
`_integration` suffix. Shared test values and constructors live in explicitly
named fixture files. Executable environment setup and suite runners belong
under `scripts/`, not in this directory.
