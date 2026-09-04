# Word-count demonstration

`walden-economy.txt` contains the complete Chapter 1, “Economy,” from Henry
David Thoreau's *Walden*. It was extracted from
[Project Gutenberg eBook 205](https://www.gutenberg.org/ebooks/205), which
identifies the ebook as public domain in the United States. Keeping the source
in the repository makes the demonstration reproducible and independent of the
network.

The source is plain text, while Mill's initial input contract is JSON Lines
(JSONL). Generate one JSONL input file with:

```bash
go run ./examples/word-count/generate
```

The default command reads the first 60 of the chapter's 136 parsed paragraph
blocks (about 44.1%) and writes:

```text
/tmp/mill-word-count/walden-economy.jsonl
```

The generated file is intentionally not committed. `record-config.json`
records the deterministic grouping algorithm, seed, boundary probability, and
paragraph limit. With the checked-in source and configuration, the generator
always combines the 60 paragraphs into the same 12 variable-size JSONL
records. A record may contain several paragraphs; no text is reordered or
discarded.

This grouping is demonstration input preparation, not Mill partitioning. The
user submits the one generated JSONL file. Mill then scans its record
boundaries and creates logical tasks that refer to byte ranges in that same
file; it does not create 12 physical shard files. With server parallelism 3,
the current planning heuristic creates 12 tasks, and a future execution adapter
will keep at most three active at once.

The mapper extracts lowercase ASCII alphanumeric tokens. A single ASCII hyphen
is retained only when it joins alphanumeric token segments, so `well-known` is
one token while punctuation and repeated, leading, or trailing hyphens are
separators. Each task produces sorted JSONL records of the form:

```json
{"word":"well-known","count":2}
```

`cmd/mill-word-count-merge` is a local demonstration reducer. It combines any
number of partial count files without placing word-count-specific behavior in
Mill's control plane. Automated Kubernetes execution and final result
aggregation are not implemented yet.

## Run one task in kind

With Go, Docker, kind, kubectl, and the existing `mill` cluster ready, run from
the repository root:

```bash
./scripts/demo-word-count-k8s
```

The script generates a fresh input, builds and loads `mill/word-count:dev`,
and runs one Kubernetes Job for the first JSONL record. It compares the Pod's
output byte-for-byte with a local invocation over the same range. A successful
run prints `PASS`, the Job and Pod status, and local output and manifest paths.
The workload writes its result to a file, so an empty successful Pod log is
expected.

The filesystem path crosses two boundaries:

```text
Laptop: /tmp/mill-word-count-k8s.<random>/input.jsonl
  | copy the whole JSONL input into the existing kind node
  v
Node: /var/local/<run-name>/input.jsonl
  | read-only hostPath mount
  v
Pod: /data/input.jsonl  -- read only [0, end of first record)

Pod: /output/task-0/result.jsonl
  | writable hostPath mount
  v
Node: /var/local/<run-name>/output/task-0/result.jsonl
  | copy result back
  v
Laptop: /tmp/mill-word-count-k8s.<random>/pod-result.jsonl
```

Copying into the node creates a snapshot; later laptop edits are not reflected
in it. The node is a Docker container, and Kubernetes
[`hostPath`](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath)
refers to that node's filesystem. The existing cluster does not need to be
recreated. These files are local to one node; this storage arrangement is for
the kind demonstration, and is not shared storage for a multi-node deployment.

`job.yaml.template` makes the Pod arguments and mounts visible. The script
fills in a fresh run name and byte offset and saves the rendered manifest
outside the repository. The Pod uses the loaded image with
[`imagePullPolicy: Never`](https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster),
runs as UID/GID 65532, and is assigned to the node containing the files. It has
a 120-second execution deadline and no automatic retries.

Each invocation keeps its own Job, files, and results for inspection, including
on failure. The script prints an exact command to delete its Job and Pod when
finished. That command leaves the data directories intact; local results can
be retained independently of the cluster. Repeated runs consume additional
disk space until their artifacts are removed.

This smoke test uses demonstration IDs and a record boundary chosen by the
script. It does not submit a Mill job, claim PostgreSQL tasks, or update their
state. The next implementation step connects that execution behavior to Mill's
coordinator so all 12 tasks can run with parallelism three.
