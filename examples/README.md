# SmartPipeline examples

Eight runnable pipelines, each written from the point of view of a different
team using SmartPipeline. They are deliberately heavier than unit tests: they
combine I/O, CPU work, large payloads, retries, failure paths and uneven stage
speeds, and print throughput, stage timings and peak RSS.

The package itself has no dependencies. The crawler and webhook examples use
`requests` (the local webhook example needs no internet access); the ML example
needs the optional `numpy` and `scikit-learn` packages; the `dump_es_ids.py`
script needs `elasticsearch`. The rest use the standard library only.

## Run

From the repository root, with the project virtualenv:

```bash
.venv/bin/python examples/news_crawler.py --items 60
```

`examples/_common.py` adds the repository root to `sys.path`, so a source
checkout works without installing the package. The seven data pipelines accept
`--items`, `--workers` and `--queue-size`; where relevant they also accept
`--executor threads|processes` or a scenario-specific option. Use `--help`
for the exact flags. `failure_drill.py` runs without arguments.

## The scenarios

| Script | Persona / use case | Main concurrency | What it stresses |
|---|---|---|---|
| `news_crawler.py` | Data journalist building a Wikipedia digest | threads | Real HTTP I/O, retries/backoff, batch feature hashing, slow API feeding fast stages |
| `log_lake_etl.py` | SRE turning access logs into per-minute stats | processes or threads | Fast inline parsing, CPU-heavy batch stage, malformed lines, bounded queues, IPC overhead |
| `blob_compactor.py` | Storage engineer deduplicating uploaded blobs | threads or processes | Disk I/O, zlib, 100+ MB payloads, tiny queues, missing/unreadable files, SQLite writer |
| `fraud_stream.py` | Payments ML engineer scoring live transactions | `process_async` | Producer backpressure, per-item callbacks, flaky gateway retries, batch model, shutdown |
| `genome_kmers.py` | Bioinformatics researcher computing k-mer spectra | processes or threads | Pure-Python CPU work, GIL behaviour, large strings, process pickling, batch merge |
| `webhook_dispatcher.py` | SaaS platform delivering customer webhooks | threads | Local flaky HTTP server, retries, per-thread sessions, dead-letter queue, audit sink |
| `ml_batch_inference.py` | ML platform engineer scoring feature rows | threads or processes | numpy/scikit-learn model, batch `predict_proba`, pickled model in workers, JSONL predictions |
| `failure_drill.py` | Release engineer running a game day | mixed | Broken batch contract, critical vs soft errors, cancellation from another thread, queue size 1 |

Quick reference:

```bash
.venv/bin/python examples/news_crawler.py --items 60 --workers 8
.venv/bin/python examples/log_lake_etl.py --items 30000 --workers 4
.venv/bin/python examples/log_lake_etl.py --items 30000 --executor threads
.venv/bin/python examples/blob_compactor.py --items 120 --workers 4
.venv/bin/python examples/blob_compactor.py --items 120 --executor processes
.venv/bin/python examples/fraud_stream.py --items 3000 --workers 4
.venv/bin/python examples/genome_kmers.py --items 160 --workers 4
.venv/bin/python examples/genome_kmers.py --items 40 --executor threads
.venv/bin/python examples/webhook_dispatcher.py --items 1000 --workers 8
.venv/bin/python examples/ml_batch_inference.py --items 50000 --workers 2
.venv/bin/python examples/ml_batch_inference.py --items 5000 --executor processes
.venv/bin/python examples/benchmark.py
.venv/bin/python examples/failure_drill.py
```

## Choosing an executor

The process executor is not automatically faster: every item crosses a
manager-backed queue and is pickled. On the development machine this transport
cost is roughly 0.2--0.5 ms per item, independent of the stage work. Use the
break-even from `examples/benchmark.py` as a starting point:

| Work per item | Usually better |
|---|---|
| I/O wait (HTTP, disk, database) | threads |
| numpy/scikit-learn/zlib, which release the GIL | threads |
| pure-Python CPU below ~0.2 ms | inline or threads |
| pure-Python CPU around 1 ms or more | processes |
| few large items | processes |

A `BatchStage` groups items inside a worker but they still cross queues one by
one: it amortizes model/API calls, not process transport. If a process pipeline
is dominated by transport, send fewer, larger **chunk items** (one item carrying
many records) and keep a per-record status inside the chunk. Adjacent CPU steps
can also be combined into one process stage to remove a queue boundary.

`examples/log_lake_etl.py` is the small-item side of the trade-off;
`examples/genome_kmers.py` is the large-item side.

## Reading the output

Every pipeline prints a summary like:

```
items=1000 elapsed=3.92s throughput=254.9 items/s
soft_errors=10 critical_errors=0
peak_rss=46MB child_peak_rss=0MB
  deliver: total=30.64s avg=30.64ms max=221.9ms
  ...
```

- `total` is the sum of the per-item stage timings across all workers, so it
  can exceed wall time when a stage runs concurrently. On threads it also
  includes time spent waiting for the GIL, so it is not CPU time.
- `child_peak_rss` is the peak RSS of the largest child process (`RUSAGE_CHILDREN`).
  It is meaningful only after an executor using processes has been shut down.
- A critical error makes the item skip all downstream stages but `run()` still
  yields it; a soft error skips only the stage that raised it. Count both when
  comparing source and sink totals.
- The examples use small custom error managers where errors are expected:
  the default manager logs a full traceback for every soft error, which is too
  noisy for a log file with 0.2% malformed lines.
- With processes, an `ErrorManager` is copied into each worker just like a stage;
  its instance counters are per worker. Read the total from
  `Pipeline.soft_errors_count` / `Pipeline.critical_errors_count`, or from the
  delivered items with `item.soft_errors()` / `item.critical_errors()`.
