# Workload contract package

`internal/workload` defines the stable command-line boundary between Mill and a
trusted workload container. It does not schedule or execute containers.

```text
Mill attempt
    |
    | CommandArgs
    v
container entrypoint -- Mill flags -- user executable arguments
    ^
    | ParseArgs
trusted workload
```

Mill passes job and task identity, shard index, input URI, half-open input byte
range, and the unique attempt output URI as named flags. A required `--`
separator precedes the user-provided executable arguments, which are preserved
unchanged.

`contract.go` owns serialization, parsing, and validation. Example workloads
under `examples/` consume this package; control-plane policy does not belong
here.
