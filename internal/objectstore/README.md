# Object-store package

`internal/objectstore` provides one URI-based API for local files and
S3-compatible objects.

```text
Store.Open / OpenRange / Put
              |
              v
       parse and validate URI
          /             \
     file backend     S3 backend
```

`store.go` is the public package boundary. `location.go` validates absolute
`file://` and `s3://` locations before `file.go` or `s3.go` receives the parsed
location.

The Store is safe for concurrent use, but it does not coordinate two writers
targeting the same URI. Mill avoids that conflict by assigning each attempt a
unique output URI. `Put` publishes one complete object and a later writer may
replace an earlier object if callers violate that ownership rule.

Local-file tests verify filesystem behavior. S3 tests use a bounded fake
endpoint; the live S3-compatible demonstration supplies end-to-end evidence.
