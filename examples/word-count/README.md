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
