# SmartPipeline changelog

## Unreleased

- BREAKING CHANGE: the item payload is now generic. Use `Item[MyData]`,
  `Stage[MyData]` / `BatchStage[MyData]`, `Source[MyData]` and `Pipeline[MyData]`
  to type the payload (`TypedDict`, dataclass, primitive, ...) so static checkers
  like `mypy` verify the code. `Item()` infers `Item[Dict[str, Any]]`;
  a custom payload always requires explicit data (`Item[MyData](...)`),
  and heterogeneous dict literals need an explicit `Item[Dict[str, Any]]({...})`.
  `Item.data` is now writable (`item.data = new_value`, required for immutable
  payloads). `Item.id` falls back to an `id` attribute for non-dict payloads.
  `FilePathItem` / `LocalFilesSource` are `Item[Dict[str, Any]]` based.
  Removed deprecated aliases: `Item.payload`, `Item.set_metadata`/`get_metadata`,
  `DataItem`, `Pipeline.append_stage`/`append_stage_concurrently`

- BREAKING CHANGE: the `parallel: bool` parameter of `Pipeline.append`,
  `Pipeline.append_concurrently` (and of the stage containers) has been replaced by
  `executor: Literal["threads", "processes"]` (`"threads"` by default, `"processes"`
  for multiprocessing). Update `parallel=True` to `executor="processes"` and
  `parallel=False` to `executor="threads"`. The `ConcurrentContainer.parallel`
  property is now `ConcurrentContainer.executor`

- Fixed a deadlock on errors in concurrent stages: with `raise_on_critical_error` a failing
  concurrent stage kills its runners, and any execution blocked putting an item in a full
  queue (the main thread when a stage without concurrency produces items for it, a stage
  runner or the source thread) was waiting forever for consumers which were not able to
  consume anymore, so the pipeline never terminated
- No execution blocks indefinitely on a queue anymore: puts and the last wait in `process`
  recurrently check the container termination and a new pipeline-wide event which is set by
  a stage runner that exits because of an error
- Errors while retrieving the finally processed items now terminate the pipeline as any
  other error, and problems in the termination never mask the original exception
- `stage_runner` and `batch_stage_runner` accept a new `fatal_event` argument,
  custom stage runners must accept it
- Fixed a deadlock on termination of concurrent batch stages: they were excluded from the
  termination of the other concurrent stages, so a failing one could leave the pipeline stuck
  with items in its queues
- Termination no longer waits for the item count of a stage to reach the one of the source,
  which could never happen for a stage which doesn't deliver all the items it receives: it
  waits for the `Stop` item instead, the only reliable "everything has been produced" signal
- Stopping the logs receiver doesn't join its queue anymore, which could wait forever for
  records logged after the sentinel (by a stage which was still dying)
- Fixed a concurrent batch stage which was sending the `Stop` item before the items of the
  batch which received it: those items were then dropped by the pipeline termination, which
  could lose items and never terminate

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
