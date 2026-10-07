# SmartPipeline changelog

## Unreleased

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
- `stage_runner` and `batch_stage_runner` accept a new optional `fatal_event` argument,
  custom stage runners keep working without it
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
