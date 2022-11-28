# SmartPipeline changelog

## Version 0.6.0 [2022-11-28]

- Logging now roks properly
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
