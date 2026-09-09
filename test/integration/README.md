# Cross-package integration tests

This directory contains tests that assemble more than one Mill package or
service boundary.

```text
Job service --> Job PostgreSQL adapter
     |
     `-------- completed status <-------- execution PostgreSQL adapter
```

Single-package unit and adapter tests stay beside their production packages.
Tests here use only exported APIs and require `MILL_TEST_DATABASE_URL` when
PostgreSQL behavior is involved.
