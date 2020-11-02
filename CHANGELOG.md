# SmartPipeline changelog

## Version 0.4.0 [2020-06-22]

- BREAKING CHANGES:
  - `use_threads` parameter has been substituted with `parallel`, values are inverted
  - item timings are now in seconds
  - `get_exception` of an `Error` without associated Exception now returns `None`
  - `Error` class is now called `SoftError`

## Version 0.3.0 [2020-04-22]

- Heavy refactoring
- Documentation
- Pipeline queues size
- Critical fixes on concurrency

## Version 0.2.0 [2019-09-24]

- Batch stages

## Version 0.1.0 [2019-04-04]

- First stable release
