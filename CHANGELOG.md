# SmartPipeline changelog

## Version 1.0.0 [Unreleased]

### Breaking changes

- The pipeline has an explicit lifecycle `INITIAL -> BUILT -> RUNNING -> CLOSED`,
  exposed by the `Pipeline.state` property and the `PipelineState` enum.
  `build()` validates, constructs stages and stores them; stage `on_start` hooks
  and workers start in `run()` / `process_async()`; `close()` is idempotent,
  terminates workers, drains queues, runs the paired `on_end` hooks and releases
  resources. A pipeline is one-shot: after `run()` or `process_async()` it is
  closed and cannot run again, while `process()` can be called repeatedly on a
  built pipeline. `append()` and `set_error_manager()` raise `PipelineStateError`
  after `build()`; `set_source()` is still accepted on a built pipeline which has
  not started.
  Migration: always `build()` before running; create a new pipeline instead of
  re-running a closed one; use `close()` instead of the removed `shutdown()`.
- `Pipeline.append()` accepts either a stage instance or a stage class, with
  everything after `stage` keyword-only and `init_args`/`init_kwargs` for classes
  (renamed from `args`/`kwargs`). An instance exists immediately, while a
  class-based stage does not exist before `build()`: `build()` constructs the
  classes one by one in order and stores them. `Pipeline.append_concurrently()`
  has been removed; custom stage runners (`run(runner=...)`) have been removed
  with the concurrency engine rewrite.
  Migration: rename `args`/`kwargs` to `init_args`/`init_kwargs`; pass the stage
  class to `append()` instead of `append_concurrently()`; delete any custom
  stage runner.
- The item payload is now generic. Use `Item[MyData]`, `Stage[MyData]` /
  `BatchStage[MyData]`, `Source[MyData]` and `Pipeline[MyData]` to type the
  payload (`TypedDict`, dataclass, primitive, ...) so static checkers like
  `mypy` verify the code. `Item()` infers `Item[Dict[str, Any]]`; a custom
  payload always requires explicit data (`Item[MyData](...)`), and heterogeneous
  dict literals need an explicit `Item[Dict[str, Any]]({...})`. `Item.data` is
  now writable (`item.data = new_value`, required for immutable payloads) and
  `None` is kept as a valid payload instead of being replaced by the default
  empty dict. `Item.id` falls back to an `id` attribute for non-dict payloads.
  `FilePathItem` / `LocalFilesSource` are `Item[Dict[str, Any]]` based.
  Migration: parameterize items, stages, sources and pipelines; replace
  `Item.payload` with `Item.data`, `set_metadata`/`get_metadata` with direct
  `metadata` dict access, `DataItem` with `Item`, and
  `append_stage`/`append_stage_concurrently` with `append()`.
- The `parallel: bool` parameter of `Pipeline.append` has been replaced by
  `executor: Literal["threads", "processes"]` (`"threads"` by default).
  Migration: update `parallel=True` to `executor="processes"` and
  `parallel=False` to `executor="threads"`.

### Added

- Top-level exports: `Pipeline`, `Stage`, `BatchStage`, `Source`, `Item`,
  `ErrorManager`, `SoftError` and `CriticalError` can be imported directly from
  `smartpipeline`.
- The public `StageSpec` dataclass, describing a stage instance or class and its
  pipeline configuration.
- The concurrency engine has been rewritten: pipelines compile into segments
  with queues only between them, a single end signal, a single cancellation
  signal, and errors reported via worker futures.
- `Pipeline.set_source()` accepts any iterable of items (e.g. a generator)
  besides a `Source`, stopping when the iterable is exhausted.
- `Pipeline.count` is the number of items delivered by `run()` or
  `process_async()`, including items with soft errors; items processed one by
  one with `process()` are not counted.
- `Pipeline.soft_errors_count` and `Pipeline.critical_errors_count` count the
  errors attached to the same delivered items, so global error totals remain
  available when the error manager runs as a copy in worker processes.
- `StageSpec.retry_manager` exposes the retry configuration of a stage, built once and
  shared by every item and every execution mode.
- `BatchStage.process_batch()` output length is validated: it must return exactly
  one item for every input item, and a mismatch raises the public
  `BatchOutputError` and stops the pipeline. Batch stages always run with
  `concurrency=1` on threads.
- Retry backoff no longer sleeps after the final failed attempt; after all
  attempts a single `RetryError` soft error is attached to the item, carrying the
  attempt details.
- `Item.add_soft_error()` and `add_critical_error()` handle subclasses of
  `SoftError` and `CriticalError` correctly.
- Exceptions raised by a `process_async()` callback are delivered by `get_item()`
  instead of silently killing the runner; `run()` also cleans up on
  `KeyboardInterrupt`. A failed start runs `on_end` only for the stages whose
  `on_start` completed.

### Fixed

- No execution blocks indefinitely on a queue anymore: blocking puts and gets
  abort when the pipeline is cancelled (on error or `close()`). This fixes
  several hangs and lost items on errors in concurrent stages, including full
  queues, concurrent batch stages, and the process log forwarding.
- Worker failures are reported via futures and never silently lost, and problems
  during termination never mask the original exception.
- A missing source is checked before any worker starts, so `run()` with no
  source fails fast instead of hanging; a failed stage initialization is reported
  again instead of hanging a second `build()` or `run()`.
- `Pipeline.process()` raises `ValueError` for pipelines with concurrent stages
  instead of hanging (use `run()` or `process_async()` for concurrency), raises
  `PipelineStateError` after `stop()`, and never returns an end-of-stream item.
- A second concurrent `Pipeline.close()` waits for the first one to finish
  instead of returning while resources are still being released.
- `Pipeline.get_item()` raises `PipelineStateError` after the asynchronous run
  has ended and its output has been drained, instead of blocking forever.
- `Pipeline.close()` called from another thread ends a running `run()` cleanly: the
  engine (queues, executors and the multiprocessing manager) is released only after the
  consuming loop stopped, and a queue whose manager is already gone aborts the get/put
  instead of raising `EOFError` or a broken pipe error.
- A source calling `stop()` on the same `pop()` returning its last item does not lose
  that item anymore.
- Running a pipeline which already processed items with `process()` does not start the
  stages a second time: `on_start`/`on_end` stay paired.
- `close()` does not wait on an asynchronous runner blocked over a full output queue.
- A failed start phase releases executors and manager even when a stage `on_end` fails
  too, and the original start error is never masked by the rollback.
- A batch stage is skipped without calling `process_batch()` when every item of its
  batch already carries a critical error, and a batch stage waiting for a full batch
  gives up as soon as the pipeline is cancelled, instead of after its whole `timeout`.
- `Pipeline.process_async()` raises `PipelineStateError` after `stop()` instead of
  accepting items which would never be processed.

## Version 0.7.0 [2023-09-27]

- Mypy checked

## Version 0.6.0 [2022-12-21]

- Logging now works properly
- Tests coverage
- Refactoring, also of class/method names

## Version 0.5.0 [2022-08-01]

- retry policy on stage failures on items
- `on_end` method on stages called at pipeline termination
- documentation fixes

## Version 0.4.0 [2020-06-22]

- critical bug fixes on concurrency
- documentation on multiprocessing
- an example script
- BREAKING CHANGES:
  - `use_threads` parameter has been substituted with `parallel`, values are inverted
  - item timings are now in seconds
  - `get_exception` of an `Error` without associated Exception now returns `None`
  - `Error` class is now called `SoftError`, `Error` becomes the general base class
  - `build()` must be used at the end of the methods chain in `Pipeline` construction

## Version 0.3.0 [2020-04-22]

- Heavy refactoring
- Documentation
- Pipeline queues size
- Critical fixes on concurrency

## Version 0.2.0 [2019-09-24]

- Batch stages

## Version 0.1.0 [2019-04-04]

- First stable release
