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
the current planning heuristic creates 12 tasks, and the execution adapter
keeps at most three attempts active at once.

The mapper extracts lowercase ASCII alphanumeric tokens. A single ASCII hyphen
is retained only when it joins alphanumeric token segments, so `well-known` is
one token while punctuation and repeated, leading, or trailing hyphens are
separators. Each task produces sorted JSONL records of the form:

```json
{"word":"well-known","count":2}
```

`examples/word-count/cmd/merge` is a local demonstration reducer. It combines any
number of partial count files without placing word-count-specific behavior in
Mill's control plane. The batch demo runs mapper tasks through Mill and invokes
this merger locally after all tasks succeed.

## Run one task in kind

With Go, Docker, kind, kubectl, and the existing `mill` cluster ready, run from
the repository root:

```bash
./scripts/demo-word-count-single-task
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
state. For the coordinator-driven execution, use the full-batch script below.

## Run the whole batch through Mill

```bash
./scripts/demo-word-count-batch
```

This requires Go, Docker, kind, kubectl, PostgreSQL 18 tools (`initdb`, `pg_ctl`,
`psql`), `curl`, and `jq` on PATH. It creates a private PostgreSQL instance that
listens only on a Unix socket in the fresh run directory. No existing database
is modified. Mill starts on `127.0.0.1:18080`; choose another port with
`MILL_DEMO_PORT` if necessary.

The script stages the full input on the kind node, submits it through
`POST /jobs`, and displays progress from `GET /jobs/{id}`. Mill creates 12
logical tasks and executes at most three attempts concurrently. Each attempt
gets its own Kubernetes Job and output file. A completed task frees a slot
independently of the others.

After completion, job status lists the 12 successful output URIs. The script
copies the results back, combines them into `counts.jsonl`, and checks that
they match a local count over the entire input. It prints the fresh output
directory, final result, and Kubernetes inspection/cleanup commands. Mill and
the private PostgreSQL server stop when the script exits; all files and
Kubernetes resources remain for inspection. `server.log` records claims,
dispatches, completions, and failures; `status.json` contains final API status.
This is a correctness demonstration, not a throughput benchmark: the input is
small and Pod startup dominates execution time.

For two active attempts instead:

```bash
MILL_PARALLELISM=2 ./scripts/demo-word-count-batch
```

With this configuration the current planner produces six tasks, each covering
two of the 12 records. The result still covers the entire input. The demo uses
the task count returned by Mill rather than choosing its own shards.

The coordinator uses one Job per attempt and preserves durable state across
process restarts. Native retries are disabled; Mill owns a three-attempt budget
per task with a five-second delay between observed failure and retry eligibility.
Generic result aggregation remains future work; this script performs only the
word-count-specific merge.

## Inject a failure and observe retries

Start with the recoverable case:

```bash
./scripts/demo-word-count-batch --failure once
```

The script builds `mill/word-count-fault:dev`, a separate image containing the
normal mapper and `cmd/fault-injection` wrapper. The wrapper receives `once` as
an executable argument after Mill's `--` separator. **Only shard 0** fails on
its first invocation; other shards immediately delegate to the normal mapper.
No randomness is involved, so repeating the demo exercises the same failure.

The wrapper uses an atomic directory creation at
`/output/.mill-faults/<job-id>/<task-id>` to remember the first invocation across
replacement Pods. On its first invocation it leaves a deliberately incorrect
partial result at the assigned attempt output URI and exits 1. The next
invocation sees the marker and runs word-count normally with unchanged Mill
arguments and no test-only arguments. The marker is specific to this local
shared-filesystem test: **Mill does not read it or use it to decide retries**.
Fresh demo jobs have distinct IDs and cannot inherit a previous job's marker.

Mill observes the failed Kubernetes Job, retains attempt 1 as `failed`, and
returns its task to `pending` with a durable five-second delay. Another pending
task can use the freed slot. When eligible and capacity is available, the same
task gets attempt 2 with a new Kubernetes Job and output URI. A failed attempt
does not count as a failed task while retries remain.

With the default parallelism 3, the expected history is:

| Shard | Attempt 1 | Attempt 2 | Final task state |
| --- | --- | --- | --- |
| 0 | failed | completed | completed |
| 1–11 | completed | not needed | completed |

There are 12 logical tasks, 13 attempts, and at most three active attempts.
The script merges only the 12 successful output URIs listed by Mill and checks
the exact full-input result. This also proves the failed attempt's deliberately
wrong partial result was excluded.

Then check exhaustion:

```bash
./scripts/demo-word-count-batch --failure always
```

Here shard 0 exits 1 on every invocation. Mill stops after three total attempts
(two retries) and marks the task and job failed. Pending tasks stop dispatching;
already active attempts are still observed until terminal. The demo expects
this failure and prints `PASS` only when the limit and absence of successful
job results are verified. Other tasks may already have produced valid outputs,
but the script does not publish a merged result for a failed job.

Both modes check persisted retry delays and observed parallelism, verify failed
Pod logs contain the injected error, and save `attempts.json` alongside
`status.json`, `server.log`, and `failure-<attempt-id>.log`. The printed attempt
table distinguishes attempt state from final task state. The private database
stops at exit, so these snapshots remain readable without restarting it.

To inspect the recoverable case after the script prints its run directory:

```bash
jq '.[] | select(.shard_index == 0)' /tmp/mill-batch.<run>/attempts.json
```

Replace `<run>` with the actual suffix. The demo retains Kubernetes resources,
node files, and local files as before. These cases test terminal workload
failure, not coordinator crashes, network partitions, or deleted running Jobs.
