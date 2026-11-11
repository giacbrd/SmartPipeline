# AGENTS.md

SmartPipeline is a zero-dependency, single-package Python **library** for producer-consumer
data pipelines. No app/server/CLI: `smartpipeline/` is the only shipped package.

## Commands

No `pyproject.toml`/Makefile/tox — setup.py + pre-commit + raw pytest.
Use repo venv binaries (`.venv`, Python 3.14; has pytest, flake8, mypy, black, isort).
Always run tests from repo root (`tests/` is a package: `from tests.utils import ...`).

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest -q tests/pipeline/test_concurrent.py::test_concurrency_errors  # single test
.venv/bin/flake8 .             # .flake8 ignores E501,W503, excludes docs/*
.venv/bin/mypy smartpipeline  # same target as pre-commit (pass_filenames: false)
.venv/bin/coverage run -m pytest -v --timeout=180 --timeout-method=signal   # CI entrypoint
```

CI (`.github/workflows/tests.yml`, py 3.12–3.15 after `ulimit -n 8192`): only the first
flake8 invocation gates the build (`--select=E9,F63,F7,F82`; the second is `--exit-zero`
advisory) → `mypy smartpipeline` → `coverage run -m pytest`.
The pytest timeout uses the "signal" method on purpose: the "thread" one kills the process
with `os._exit()`, which discards the coverage data of the whole matrix leg.
`setup.py` requires `python_requires=">=3.12"` (`ExceptionGroup`, `list[...]` OK).

Suite takes minutes: concurrency tests spawn real processes and sleep. Iterate on focused
tests, run the full suite before finishing.

## Architecture

Execution flows `pipeline.py -> segments.py -> runners.py`; read in that order.

- `pipeline.py` — `Pipeline`: `set_source()` (a `Source` or an iterable of items), `append()` (stage **instance or class**, everything after `stage` keyword-only, plus `init_args`/`init_kwargs` for classes), `build()`,
  `run()`, `process()`/`process_async()`/`get_item()`/`stop()`, `close()`.
  `append()` only records a `StageSpec`: an instance stage exists and is named
  immediately, while `build()` constructs class-based stages one by one in order.
  `StageSpec.retry_manager` builds the stage `RetryManager` once, shared by `process()`
  and the segments.
  `_stages` is empty until `build()`; `get_stage()` returns a class-based stage only after it.
  `PipelineState` (`INITIAL → BUILT → RUNNING → CLOSED`) is the lifecycle contract.
  `Pipeline` keeps `_state`, one `_lock` and one `_closed` `Event`, plus a single
  engine handle (`_engine`) and the started-stages list. `process()` runs the stages
  directly with no containers and no queues. `run()`/`process_async()` compile segments
  and start an `Engine`; `run()` wraps everything in `try/finally: close()`.
  `close()` takes the transition to `CLOSED` under the lock (a second caller waits on
  `_closed`), sets `cancel`, joins the asynchronous runner thread, then shuts the engine
  down (join workers, drain queues, run `on_end` under the engine stage lock, executors
  and the Manager), and finally sets `_closed`.
- `segments.py` — per-run wiring (`InlineSegment`, `ConcurrentSegment`, `compile_segments`,
  `Engine`). A pipeline compiles into segments with queues only *between* them:
  a concurrent stage (a batch stage is always one) becomes one `ConcurrentSegment` with N
  workers on a thread or process executor; a maximal run of `concurrency=0` stages becomes
  one `InlineSegment` executed as a plain chain by one driver. A source thread pulls and puts
  except for a single inline pipeline, which pulls from the `Source` directly.
  Middle inline segments run on one dedicated thread each, the last inline segment runs on
  the caller's thread inside `run()`. One `Stop` per consumer worker ends flow: a worker
  exits on `Stop` (flushing its batch) without forwarding it; the coordinator waits until
  every worker of segment k is done and puts one `Stop` per worker of k+1 into the next queue.
  One `cancel` `Event` (from the `Manager` when any segment uses processes) aborts every blocking
  `get`/`put` (`get_or_abort`/`put_or_abort` with `CONCURRENCY_WAIT`); both helpers treat a dead
  manager as an abort, so a concurrent `close()` never raises `EOFError`/`OSError` on a queue. Every worker future gets a
  done-callback that records the error and sets `cancel`; the run loop raises via
  `raise_collected_errors` as soon as `cancel` is observed.
- `runners.py` — `worker_loop(stage, in_queue, out_queue, error_manager, retry_manager, cancel,
  logs_queue)`: plain stages pull one blocking item, batch stages pull up to `size`/`timeout`
  (waiting in `CONCURRENCY_WAIT` slices, so `cancel` is never delayed by a long `timeout`);
  results go with `put_or_abort`; `Stop` flushes and exits without forwarding; processing errors
  propagate (the callback sets `cancel`); process workers call `on_start`/`on_end`. `process` and
  `process_batch` keep timing, retry/backoff and `RetryError`; `BatchOutputError` stays
  non-retryable, and a batch whose items all carry a critical error skips `process_batch`.
- `stage.py` — `Source` (`pop()` + `self.stop()` to end), `Stage.process`,
  `BatchStage.process_batch` (`__init__(size, timeout)`); `on_start`/`on_end` hooks.
- `item.py` — `Item` (generic `Item[MyData]`; `.data` is writable); internal `Stop`
  sentinel ends flow.
- `error/` — `SoftError`/`CriticalError`/`RetryError`; `ErrorManager` (**critical errors
  are skipped by default** — `raise_on_critical_error()` to raise).
- `defaults.py` — `CONCURRENCY_WAIT=0.1`, `MAX_QUEUES_SIZE=1000`.

`smartpipeline/__init__.py` exports the public names (`Pipeline`, `Stage`, `BatchStage`,
`Source`, `Item`, `ErrorManager`, `SoftError`, `CriticalError` via `__all__`) — examples
import from the top level (`from smartpipeline import Pipeline`).

Payloads are generic: `Item[MyData]`/`Stage[MyData]`/`Source[MyData]`/`Pipeline[MyData]`;
`Item()` infers `Item[Dict[str, Any]]`. Concurrency uses
`executor: Literal["threads","processes"]` (default `"threads"`), not a `parallel` bool.
`append(..., retryable_errors, max_retries, backoff)` retries; after the final failed
attempt one `RetryError` soft error is attached to the item.

## Invariants agents get wrong

- `build()` only validates/prepares: it raises `ValueError` with no stages, constructs
  class-based stages one by one and stores them, but never starts workers or calls `on_start`.
  `run()`/`process_async()` compile segments and start an `Engine`; `run()` raises `ValueError`
  with no source. Stage names must be unique.
- `run()`/`process()`/`process_async()` auto-call `build()` if skipped, but call it
  explicitly after appending anyway. A pipeline is one-shot: after `run()` or
  `process_async()` it is `CLOSED`; `process()` may be called repeatedly while
  `BUILT`. `close()` is idempotent and runs `on_end`/resource release via the engine;
  a second `close()` waits on `_closed` instead of returning early.
- `append()`/`set_error_manager()` raise `PipelineStateError` after `build()`;
  `set_source()` is still allowed on `BUILT`. `append(..., init_args, init_kwargs)` is only valid
  with a stage class.
  `process()` raises `ValueError` with any concurrent stage (use
  `run()`/`process_async()`), runs stages directly with no queues, and never returns `Stop`;
  it raises `PipelineStateError` after `stop()`, and so does `process_async()` (an
  `Engine` never re-runs the `on_start` of stages already started by `process()`).
- `concurrency=0` means inline (no executor workers). A middle inline run executes on one
  dedicated driver thread, the last inline run executes on the caller's thread. `BatchStage` with
  `concurrency < 1` is silently forced to `concurrency=1, executor="threads"`.
  `BatchStage.process_batch()` must return exactly one item per input item; a mismatch
  raises a fatal `BatchOutputError` (never retried).
- `executor="processes"` uses `spawn`; stage instances are pickled into workers.
  Non-picklable resources (files, sockets, models) go in `on_start()`, not `__init__`
  (see `SerializableStage`/`SerializableErrorManager` in `tests/utils.py`). The main
  error manager starts after process workers are submitted (workers serialize a pre-start
  copy), so its `on_start` may also open non-picklable resources.
- `on_start`/`on_end` failures are fatal and bypass `ErrorManager` (no `Item` to attach
  to): single failure re-raised as-is, several as `ExceptionGroup`. `on_end` runs only
  for stages whose `on_start` completed, including on start failures (process workers run
  theirs in `worker_loop`). Never swallow termination errors or the pipeline hangs silently;
  `run()` preserves the original failure when `close()` also fails.
- `count` is the number of items delivered by `run()`/`process_async()` (yielded or via
  `get_item()`), including items with soft errors; `process()` items are not counted.
- All cross-process state via one `multiprocessing.Manager()` owned by the `Engine`
  (queues, `cancel` `Event`, logs queue) when any segment uses processes — don't swap in raw mp
  primitives. Cross-process logging needs the `LogsReceiver` wiring; process workers set it up
  from `logs_queue`.

## Tests

- Never invent ad-hoc stages/sources: reuse `tests/utils.py` + fixtures in
  `tests/conftest.py`; `get_pipeline()` already sets `raise_on_critical_error()`.
- Never consume `pipeline.run()` directly — a stuck pipeline hangs the suite. Use
  `run_with_timeout(pipeline)` (fails instead of hanging).
- Annotate new public functions (`py.typed` shipped, mypy enforced).

## Release / docs / style

- Version triple-sync: `setup.py` (`version=`), `smartpipeline/__init__.py`
  (`__version__`), `docs/conf.py` (`release`) + `CHANGELOG.md` entry. Git Flow off
  `develop` (see `CONTRIBUTING.md`).
- New public API needs an `autoclass` entry in `docs/api.rst`. Don't reintroduce removed
  aliases (`Item.payload`, `DataItem`, `append_stage*`) or add new ones.
- Each module starts with `__author__ = "Giacomo Berardi <giacbrd.com>"`.
- The venv's `black` matches the pre-commit pin (26.5.1), whose one-line empty
  definitions need `E704` ignored: check `git status` after formatting anyway.
