# Cross-package integration tests

This directory contains tests that require PostgreSQL or assemble more than
one Mill package, transport, or process boundary.

```text
Job service --> Job PostgreSQL adapter
     |
     `-------- completed status <-------- execution PostgreSQL adapter
```

Unit and within-package tests stay beside their production packages. Tests in
this directory use exported APIs and skip PostgreSQL-backed cases when
`MILL_TEST_DATABASE_URL` is not set.

Files are named for the behavior or boundary they test. Because the directory
already establishes the integration-test scope, filenames do not repeat an
`_integration` suffix. Executable environment setup and suite runners belong
under `scripts/`, not in this directory.
