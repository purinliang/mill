# Object-store package instructions

These instructions apply to `internal/objectstore`.

- Keep `Store` as the only exported storage entry point.
- Validate and parse URIs before dispatching to a backend.
- Keep file and S3 implementation functions private.
- Close all opened bodies and preserve half-open range semantics.
- Require a seekable body for the current complete-object upload path.
- Do not add distributed locking here. Callers own unique output locations.
- Use normal AWS credential and endpoint resolution in AWS; custom endpoints
  exist for local S3-compatible testing.
- Test file and S3 behavior separately through exported Store methods.

See [README.md](README.md) for the package contract and graph.
