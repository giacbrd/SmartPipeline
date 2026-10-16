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
.venv/bin/coverage run -m pytest -v   # CI entrypoint
```

CI (`.github/workflows/tests.yml`, py 3.12–3.14 after `ulimit -n 8192`): only the first
flake8 invocation gates the build (`--select=E9,F63,F7,F82`; the second is `--exit-zero`
advisory) → `mypy smartpipeline` → `coverage run -m pytest`.
`setup.py` requires `python_requires=">=3.12"` (`ExceptionGroup`, `list[...]` OK).

Suite takes minutes: concurrency tests spawn real processes and sleep. Iterate on focused
tests, run the full suite before finishing.

## Architecture

Execution flows `pipeline.py -> containers.py -> runners.py`; read in that order.

- `pipeline.py` — `Pipeline`: `set_source()`, `append()` (stage **instance**),
  `append_concurrently()` (stage **class** + `args`/`kwargs`, rejects instances),
  `build()`, `run()`, `process()`/`process_async()`/`get_item()`/`stop()`.
- `containers.py` — per-stage wiring (`SourceContainer`, `StageContainer`,
  `BatchStageContainer`, concurrent variants).
- `runners.py` — worker loops; fatal errors surface via `raise_collected_errors`
  (`utils.py`): single error re-raised as-is, several as `ExceptionGroup`.
- `stage.py` — `Source` (`pop()` + `self.stop()` to end), `Stage.process`,
  `BatchStage.process_batch` (`__init__(size, timeout)`); `on_start`/`on_end` hooks.
- `item.py` — `Item` (generic `Item[MyData]`; `.data` is writable); internal `Stop`
  sentinel ends flow.
- `error/` — `SoftError`/`CriticalError`/`RetryError`; `ErrorManager` (**critical errors
  are skipped by default** — `raise_on_critical_error()` to raise).
- `defaults.py` — `CONCURRENCY_WAIT=0.1`, `MAX_QUEUES_SIZE=1000`.

`smartpipeline/__init__.py` exports nothing (`__author__`, `__version__` only) — always
import from submodules (`from smartpipeline.pipeline import Pipeline`).

Payloads are generic: `Item[MyData]`/`Stage[MyData]`/`Source[MyData]`/`Pipeline[MyData]`;
`Item()` infers `Item[Dict[str, Any]]`. Concurrency uses
`executor: Literal["threads","processes"]` (default `"threads"`), not a `parallel` bool.
`append(..., retryable_errors, max_retries, backoff)` retries then raises `RetryError`.

## Invariants agents get wrong

- `run()`/`process()`/`process_async()` auto-call `build()` if skipped — but call
  `build()` explicitly after appending anyway; it raises `ValueError` with no stages,
  and `run()` raises `ValueError` with no source. Stage names must be unique.
- `concurrency=0` means inline (no threads/processes). `BatchStage` with
  `concurrency < 1` is silently forced to `concurrency=1, executor="threads"`.
- `executor="processes"` uses `spawn`; stage instances are pickled into workers.
  Non-picklable resources (files, sockets, models) go in `on_start()`, not `__init__`
  (see `SerializableStage`/`SerializableErrorManager` in `tests/utils.py`).
- `on_start`/`on_end` failures are fatal and bypass `ErrorManager` (no `Item` to attach
  to): single failure re-raised as-is, several as `ExceptionGroup`. Never swallow
  termination errors or the pipeline hangs silently.
- All cross-process state via one `multiprocessing.Manager()` (`_new_mp_queue` etc.) —
  don't swap in raw mp primitives. Cross-process logging needs the `LogsReceiver` wiring.

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
- The venv's `black` (26.x) is newer than the pre-commit pin (23.7.0): check `git status`
  after formatting and revert unrelated hunks.
