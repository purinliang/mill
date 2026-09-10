# Testing

## Unit and hermetic tests

Run the normal suite without external infrastructure:

```bash
go test ./...
```

Report coverage for handwritten Go code with:

```bash
./scripts/test-coverage.sh
```

Generated Protobuf statements are excluded from the percentage but remain
compiled and exercised. Test behavior through exported APIs rather than
generated getters or private helpers.

## PostgreSQL integration tests

Repository tests use a real disposable PostgreSQL database because locks,
constraints, and transactions are behavior under test. Prepare one with:

```bash
createdb mill_test
for migration in migrations/*.sql; do
  psql 'postgresql:///mill_test' -v ON_ERROR_STOP=1 -f "$migration"
done
```

Then run the complete race-enabled suite:

```bash
MILL_TEST_DATABASE_URL='postgresql:///mill_test' go test -race ./...
```

The PostgreSQL tests skip when `MILL_TEST_DATABASE_URL` is absent. Tests under
`test/integration` cover database, RPC, Kubernetes-protocol, and real-process
boundaries. They do not require a live Kubernetes cluster.

Kubernetes demonstrations are explicit scripts rather than part of the normal
suite. The [word-count guide](../../examples/word-count/README.md) lists them.

## Protobuf generation

Generated bindings are committed. Regenerate them only when the execution
schema changes:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
PATH="$(go env GOPATH)/bin:$PATH" protoc -I . \
  --go_out=. --go_opt=module=github.com/purinliang/mill \
  --go-grpc_out=. --go-grpc_opt=module=github.com/purinliang/mill \
  api/proto/mill/execution/v1/execution.proto
```

Review the schema and generated diff together. Before integration, run the Go
test suite, `go vet ./...`, and `git diff --check`.
