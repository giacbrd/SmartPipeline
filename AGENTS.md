# AGENTS.md

SmartPipeline is a zero-dependency, single-package Python **library** for building producer-consumer
data pipelines. There is no app, no server, no CLI: `smartpipeline/` is the only shipped package and
everything else is docs/tests/examples.

## Commands

No `pyproject.toml`, no Makefile, no tox — everything is setup.py + pre-commit + raw pytest.
Use the repo venv binaries (`.venv`, Python 3.14; has pytest, flake8, mypy, black, isort).

```bash
# tests (always run from repo root: tests/ is a package and imports use `from tests.utils import ...`)
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest -q tests/pipeline/test_concurrent.py::test_errors   # single test
.venv/bin/python -m pytest -q --durations=10 tests/          # find slow tests

# CI order is: flake8 -> mypy -> pytest
.venv/bin/flake8 .             # .flake8 ignores E501 and W503, excludes docs/*
.venv/bin/mypy smartpipeline  # same target as pre-commit (pass_filenames: false)
.venv/bin/black . && .venv/bin/isort --profile black .
.venv/bin/coverage run -m pytest -v   # CI entrypoint
```

CI (`.github/workflows/tests.yml`) runs **flake8 → mypy → coverage/pytest** on Python 3.9–3.14
after `ulimit -n 8192`. Keep code 3.9-compatible even though `setup.py` advertises up to 3.14:
no `match`, no PEP 604 unions except under `from __future__ import annotations`.

**The suite is slow (minutes), not because of heavy setup but because concurrency tests spawn real
processes and sleep.** `tests/pipeline/test_concurrent.py` and `test_batch_concurrent.py` use
`parallel=True` everywhere. Iterate on focused files/tests, run the whole suite before finishing.

## Architecture: where things live

Execution flows `pipeline.py -> containers.py -> runners.py`. Read these in that order before changing
pipeline behaviour.

- `smartpipeline/pipeline.py` — `Pipeline`: the builder + public API. `append()` (stage instance),
  `append_concurrently()` (stage **class** + `args`/`kwargs`, rejects instances), `set_source()`,
  `set_error_manager()`, `build()`, `run()`, `process()`/`process_async()`/`get_item()`/`stop()`.
- `smartpipeline/containers.py` — per-stage wiring objects: `SourceContainer`, `StageContainer`,
  `BatchStageContainer`, `ConcurrentStageContainer`, `BatchConcurrentStageContainer`. `build()` blocks
  in `_wait_executors` until stage construction and container linking finish, then sets the shared
  fatal event and starts concurrent runners.
- `smartpipeline/runners.py` — the actual worker loops (`process`, `process_batch`, `stage_runner`,
  `batch_stage_runner`).
- `smartpipeline/stage.py` — `Source` (`pop()` + `self.stop()`), `Stage` (`process`), `BatchStage`
  (`process_batch`, `__init__(size, timeout)`), plus `AliveMixin` (name/logger) and `ConstructorMixin`
  (`on_start`/`on_end`).
- `smartpipeline/item.py` — `Item` (`data`, `metadata`, per-stage timings, soft/critical errors) and the
  internal `Stop` sentinel item that signals flow end.
- `smartpipeline/error/` — `exceptions.py` (`Error` -> `SoftError` / `CriticalError` / `RetryError`),
  `handling.py` (`ErrorManager`, `RetryManager`).
- `smartpipeline/helpers.py` — `LocalFilesSource`, `FilePathItem`.
- `smartpipeline/defaults.py` — tuning constants: `CONCURRENCY_WAIT=0.1`, `MAX_QUEUES_SIZE=1000`,
  `DATA_SNIPPET_SIZE=100`.

`smartpipeline/__init__.py` exports **nothing** (only `__author__` and `__version__`). There is no
`from smartpipeline import Pipeline` — every user imports from a submodule
(`from smartpipeline.pipeline import Pipeline`, `from smartpipeline.stage import Stage`, etc.).
`examples/dump_es_ids.py` shows the intended import style.

## Non-obvious invariants

- **`build()` must be called after appending and before `run()`/`process()`**; it blocks until all
  stages finish initializing and raises `ValueError` if no stage was appended. `run()` raises
  `ValueError` if no source was set. Stage names must be unique.
- **`concurrency=0` means inline execution** (no thread/process). A `BatchStage` with `concurrency < 1`
  is silently forced to `concurrency=1, parallel=False` (two `FIXME`s in `pipeline.py`: `append` and
  `append_concurrently`).
- **`parallel=True` uses the `spawn` start method and stage instances are copied into workers.** Anything
  non-picklable (files, sockets, models) must be created in `on_start()`, not `__init__` — see
  `SerializableStage`/`SerializableErrorManager` in `tests/utils.py` for the pattern.
- **All cross-process state goes through one `multiprocessing.Manager()`** (`SyncManager`): queues,
  events, counters. This is deliberate (see the comment in `pipeline._new_mp_queue` explaining why not
  `multiprocessing.Queue`). Don't swap in raw mp primitives.
- **Cross-process logging** needs the `LogsReceiver`/`QueueHandler` wiring
  (`pipeline._start_logs_receiver` + `_stage_initialization_with_logger`); stage loggers are named after
  the stage name.
- Timing uses `time.perf_counter`; item timings are **seconds** per stage name (`item.set_timing`).

## Version bump checklist

The version string is duplicated in three places and must stay in sync: `setup.py` (`version=`),
`smartpipeline/__init__.py` (`__version__`), `docs/conf.py` (`release`). Also update `CHANGELOG.md`,
which documents breaking changes per release (this project uses Git Flow branches off `develop`, per
`CONTRIBUTING.md`).

## Public API changes

Anything added to the public surface should get an `autoclass` entry in `docs/api.rst` (docs are built
by Read the Docs from `docs/conf.py`, deps pinned in `docs/requirements.txt`, e.g. `docutils<0.17`)
and a `CHANGELOG.md` entry. Deprecated aliases still exist and must be kept working: `Item.payload`,
`Item.set_metadata`/`get_metadata`, `DataItem`, `Pipeline.append_stage`,
`Pipeline.append_stage_concurrently`.

## Style conventions

- Each module starts with `__author__ = "Giacomo Berardi <giacbrd.com>"` (below imports, per
  `.pre-commit-config.yaml`/`black`).
- Tests never define ad-hoc stages/sources: reuse the shared ones in `tests/utils.py` and the fixtures in
  `tests/conftest.py` (`text_samples_fx`, `items_generator_fx`, `file_directory_source_fx`).
  `tests/utils.py:get_pipeline()` builds a pipeline with `raise_on_critical_error()` already set.
- Never consume `pipeline.run()` directly in a test: a stuck pipeline hangs the suite. Use
  `tests/utils.py:run_with_timeout(pipeline)`, which fails the test instead of hanging.
- Annotate new public functions — the package ships `py.typed` and CI runs `mypy`.
