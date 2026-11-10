# SmartPipeline 1.0.0 — pre-release plan

Ordered tasks for finishing the codebase before release. Each task is meant
to be one independent LLM session and one commit. Run the tasks **in order**, because
later tasks assume the earlier ones are merged.

## Rules for every task

- Commands, test conventions (`run_with_timeout`, `get_pipeline`,
  reuse `tests/utils.py`) and invariants apply unless a task explicitly changes them.
- Gates before committing (all must pass):
  ```bash
  .venv/bin/flake8 . && .venv/bin/mypy smartpipeline && .venv/bin/python -m pytest -q --timeout=180 --timeout-method=signal
  ```
  Iterate on focused tests and run the full suite once at the end.
- Stay inside the task scope. If you find a problem that belongs to another task, write
  it under "Notes" at the end of this file. Don't fix it.
- Any public behaviour change gets a line in the `CHANGELOG.md` 1.0.0 entry and, when it
  is user-visible, an update in `docs/`. Any new public name gets an entry in `docs/api.rst`.
- Every bug fix starts with a failing test that reproduces it.
- Commit message: `T<id>: <title>`. Tick the checkbox of the task in this file.

---

## Phase 0 — Decisions and safety net

### [x] T0.1 Maintainer decisions (human, not LLM)
Answer every question below inline (`Decision: ...`). Tasks marked **[D-x]** follow
the answer, and skip themselves when the answer is "no".

- **D-1** After `stop()`, should `process()` raise `PipelineStateError`?
  Decision: yes
- **D-2** Once an async run has ended and the output is drained, should `get_item()` raise
  `PipelineStateError` instead of blocking forever?
  Decision: yes
- **D-3** Should we remove parallel construction of stage classes in `build()`, along
  with `Pipeline(max_init_workers=...)`? Classes would still be accepted by `append()`,
  just constructed one by one. Heavy setup belongs in `on_start`, and that already runs
  sequentially.
  Decision: yes
- **D-4** Redesign: an inline stage (`concurrency=0`) placed *between* two concurrent
  stages would run on a dedicated pipeline thread instead of the caller's thread. It
  still has a single instance, sequential calls, and `on_start`/`on_end` in the main
  process. Is this acceptable?
  Decision: yes
- **D-5** Should custom stage runners (`ConcurrentStageContainer.run(runner=...)`,
  the `stage_runner` signature) become private?
  Decision: yes
- **D-6** Should `set_source()` accept any `Iterable[Item[DataT]]` besides a `Source`?
  This is not breaking.
  Decision: yes
- **D-7** Should critical errors raise by default, with an opt-out to skip them?
  This is breaking.
  Decision: no
- **D-8** In `append()`: should every parameter after `stage` become keyword-only, and
  should `args`/`kwargs` be renamed to `init_args`/`init_kwargs`? This is breaking.
  Decision: yes to all
- **D-9** Should the main public names be exported from `smartpipeline/__init__.py`?
  The names are `Pipeline`, `Stage`, `BatchStage`, `Source`, `Item`, `ErrorManager`,
  `SoftError`, `CriticalError`. This is not breaking.
  Decision: yes

### [x] T0.2 Public-contract test suite
**Goal:** a suite that only uses the public API and must stay green through the
redesign in Phase 3. The existing tests touch many privates (`_containers`,
`_terminate_all`, `_start_stages`, `_fatal_event`, `_lifecycle_lock`, ...), so they
won't survive it.

**Do:** create `tests/pipeline/test_contract.py`. Use only `Pipeline`, `Stage`,
`BatchStage`, `Source`, `Item`, `ErrorManager`, the exceptions and the `tests/utils.py`
helpers. Each scenario is parametrized over inline, `threads` and `processes` where it
makes sense:
1. All items delivered, and `count` matches, for these layouts: all inline;
   concurrent first; concurrent last; inline/concurrent/inline/concurrent; batch stage
   in the middle; batch stage last.
2. Soft errors are attached and the item is delivered. Critical errors are skipped by
   default, and raised with `raise_on_critical_error()`.
3. A stage that raises while queues are full (`max_queues_size=1`, a slow consumer)
   makes `run()` raise within a timeout, with no hang.
4. `on_start` fails on stage k: the error is raised, `on_end` runs exactly for
   stages `< k`, and no workers are left alive. Also cover `on_end` failing, and two
   hooks failing at once (`ExceptionGroup`).
5. `BatchOutputError` stops the pipeline. Retry gives one `RetryError` after
   `max_retries+1` attempts.
6. Lifecycle: the state sequence; `append` after `build` raises; `run` twice raises;
   `close()` is idempotent; `close()` from another thread during `run()` ends the
   generator; breaking out of a `run()` loop early releases resources (the pipeline is
   `CLOSED`).
7. `process()` called repeatedly, and `process()` with a concurrent stage raises
   `ValueError`. `process_async` + `get_item` with a callback, and a callback that raises.

**Acceptance:** the file has no `._` attribute access (`grep -n "\._" tests/pipeline/test_contract.py` is empty)
and it passes on the current code.

### [x] T0.3 Baseline benchmark
Add `examples/benchmark.py`, a plain script that is not a test. It times 20k items
through: an inline pipeline; one `threads` stage with concurrency 2; one `processes`
stage with concurrency 2; and a batch stage of size 100. Record the numbers you get
under "Notes" below. Phase 3 must not regress them by more than 20%.

---

## Phase 1 — Bug fixes on the current code

### [x] T1.1 `process()` after `stop()` returns the internal `Stop` item [D-1]
**Repro:** `p = Pipeline().append("a", S()).build(); p.process(Item()); p.stop(); p.process(Item()); p.process(Item())`.
The third call returns `Stop`, and the item it was given is left stuck in
`SourceContainer._internal_queue`.
**Fix:** in `Pipeline.process()`, raise `PipelineStateError` when the source container
is stopped, before calling `prepend_item`. Also add a guard so `process()` never
returns a `Stop` instance.
**Tests:** add the repro to `tests/pipeline/test_pipeline.py` and to the contract suite.

### [x] T1.2 Concurrent `close()` calls are not serialized
`Pipeline.close()` checks and sets `_closing` outside `_lifecycle_lock`. A second
caller returns immediately, before resources have been released.
**Fix:** a second `close()` waits until the first one has finished
(`_termination_done`, or a new `_closed` `Event` set at the end of `close()`).
**Test:** two threads call `close()` on a running concurrent pipeline. After both
return, the state is `CLOSED`, `_sync_manager is None`, and `on_end` was called exactly once.

### [x] T1.3 `get_item()` blocks forever after the async run ended [D-2]
**Fix:** when the async runner thread (`_start_pipeline_executor.pipeline_runner`)
ends, it puts an end-of-stream sentinel into `_out_queue`. When `get_item()` reads it,
it puts it back (so later calls see it too) and raises `PipelineStateError("asynchronous run ended")`.
**Test:** `process_async` 2 items, `stop()`, then `get_item()` ×2 returns the items
and a third call raises within 1s.

### [x] T1.4 A fatal runner error can be silently lost
`Pipeline._wait_for_runner_error()` polls 10 times and then only logs a warning. Its
callers then return `None`, or the item that was given to them.
**Fix:** after the retries, raise
`RuntimeError("a stage runner failed without reporting its error")`.
**Test:** set `_fatal_event` with no failed future and check that the error is raised.
This is a unit test in `tests/pipeline/test_lifecycle.py`; it may use privates.

---

## Phase 2 — Clean-ups that survive the redesign

Only touch code that Phase 3 keeps: `item.py`, `error/`, `stage.py`, the retry logic, and
the `Pipeline` public surface. Don't polish `containers.py` or the runner loops,
because Phase 3 replaces them.

### [x] T2.1 Remove parallel stage construction [D-3]
- In `pipeline.py`, `_construct_stages` becomes `[spec.construct() for spec in specs]`,
  wrapped so it still collects every error with `raise_collected_errors`.
- Remove `max_init_workers` and the `ThreadPoolExecutor` import.
- Update the `append()` and `build()` docstrings, `docs/usage.rst`, `CHANGELOG.md` and
  `AGENTS.md` (which currently says "constructs class-based stages concurrently").
- Delete or adapt tests that check construction runs concurrently.

### [x] T2.2 One retry implementation
`runners.process` and `runners.process_batch` duplicate the retry, backoff and
`RetryError` logic. Extract:
`_retry(call: Callable[[], T], retry_manager) -> T | RetryError`. It retries only
`retry_manager.retryable_errors`, sleeps `2**(n-1) * backoff` between attempts (never
after the last one), and returns the built `RetryError` (with message and
`with_exception`) on exhaustion.
`process` and `process_batch` keep their timing and error-manager handling, and
`BatchOutputError` stays non-retryable.
**Acceptance:** the retry tests pass unchanged, and `runners.py` loses about 40 lines.

### [x] T2.3 Deduplicate `Item.add_soft_error` / `add_critical_error`
Use one private helper parametrized by `(error_cls, forbidden_cls)`. Keep the
`ValueError` messages and the `__cause__`→`with_exception` preservation exactly as
they are.

### [x] T2.4 Remove dead and leftover code in the `Pipeline` surface
- `append()` and `_wire_containers()` both compute `_has_concurrent_stages` and
  `_enqueue_source`. Keep only the build-time computation. `process()` must call
  `_ensure_built()` before checking `_has_concurrent_stages` (keep the
  `ValueError`).
- `runners.OnEndErrors` (a NamedTuple with `__bool__`) becomes a plain
  `list[Exception]`.
- Delete comments that tell the history of the code ("Keep the old append() behavior",
  "used to be mixed into the container", "backward-compatible ...") and keep the
  comments that explain *why*. Applies to `pipeline.py`, `item.py`, `stage.py`, `error/`.

---

## Phase 3 — Redesign of the execution core

### Target design (shared by T3.1–T3.6)

Problem today: the caller's thread loops round-robin over every container, and two
end signals coexist (`Stop` items and per-container terminate events). Termination is
therefore inferred by polling queue emptiness, counters, and a terminator thread. That
is the source of most of the flags and polling sleeps.

New model:
- **Segments.** A pipeline compiles into an ordered list of segments:
  - `ConcurrentSegment`: one concurrent stage, with N workers on a thread or process
    executor. A batch stage is always one of these.
  - `InlineSegment`: a maximal run of consecutive `concurrency=0` stages, executed as
    a plain function chain by one driver.
  Queues exist only *between* segments.
- **Who drives what:**
  - When the first segment is concurrent, a source thread pulls from the `Source` and
    puts into queue 0. Otherwise the first inline segment pulls from the source
    directly.
  - The last inline segment, when the pipeline ends with one, runs on the caller's
    thread inside `run()`. Middle inline segments run on one dedicated thread each [D-4].
  - The caller's thread only consumes the final queue (or runs the final inline
    segment) and yields items.
- **One end signal: `Stop`, counted per worker.** The producer of a segment's input
  sends one `Stop` per consumer worker. A worker exits when it reads a `Stop` (after
  flushing its batch). A coordinator, `Segment.join_then_forward()`, waits until *all*
  workers of segment k are done and then puts N(k+1) `Stop`s into queue k+1. Every item
  of segment k is therefore already enqueued before the first `Stop`, so ordering is
  correct with no counters and no terminate events.
- **One cancel signal.** A single `cancel` `Event`, from the `Manager` when any
  segment uses processes. Every blocking get/put loops with a `CONCURRENCY_WAIT`
  timeout and gives up when `cancel` is set (reuse `utils.put_or_abort`, and add a
  symmetric `get_or_abort`).
- **Errors via futures, not polling.** Every worker future gets an `add_done_callback`.
  On an exception it records the error in a thread-safe list and sets `cancel`. The run
  loop raises the recorded errors with `raise_collected_errors` as soon as `cancel` is
  observed.
  `on_start` failures in workers travel the same path. Drop `has_started_counter`:
  start errors surface at the first iteration of `run()`.
- **Lifecycle.** `Pipeline` keeps `_state`, one `_lock` and one `_closed` `Event`.
  `close()` works as follows:
  1. under the lock, take the transition to `CLOSED`; a second caller waits on `_closed`;
  2. set `cancel`;
  3. join the source and driver threads, and wait on the futures;
  4. drain the queues;
  5. run `on_end` for main-process stages whose `on_start` completed;
  6. run `error_manager.on_end`;
  7. shut down the executors and the Manager;
  8. set `_closed`.

  A normal end of `run()` is a graceful wait for the final `Stop` followed by the same
  `close()`. The `run()` generator wraps everything in `try/finally: close()`.
- **`process()`** gets no containers and no queues: it is
  `for stage in stages: item = runners.process(stage, item, em, rm)`, after a one-time
  `on_start` and with the critical-error check.
- **`process_async()`** reuses the engine with an internal `queue.Queue` as the source.
  `stop()` ends it.

Things to delete by the end of Phase 3:
- the `_ConcurrencyAdapter`, the two forwarding container classes, and
  `BatchStageContainer` with its buffering;
- `SourceContainer.prepend_item`'s `_next_item` juggling;
- the terminate events, the item counters of containers, `_all_empty`,
  `_all_terminated`, the terminator thread, `queues_join`, `_abort_pending_termination`,
  `_wait_for_runner_error` and `_get_processed_or_abort`;
- most of the lifecycle flags (`_stages_started`, `_close_requested`, `_closing`,
  `_run_finished`, `_terminated_all`, `_termination_done`, `_resources_released`,
  `_error_manager_started`).

Invariants to keep: everything in `AGENTS.md` "Invariants agents get wrong" except the
inline-thread placement [D-4]; `spawn`; a single `Manager`; `LogsReceiver` for process
logs; worker copies running their own `on_start`/`on_end`; the main error manager
starting after process workers are submitted.

### [x] T3.1 Synchronous `process()` without containers
Implement `process()` as described above, in `pipeline.py`. The old containers remain
in use for `run()` and `process_async()`.
**Acceptance:** all `process()` tests pass, including T1.1. `process()` no longer
touches `_source_container` or `_containers[...].process()`.

### [x] T3.2 Unified worker loop
In `runners.py`, replace `stage_runner` and `batch_stage_runner` with one
`worker_loop(stage, in_queue, out_queue, error_manager, retry_manager, cancel, logs_queue)`:
- it pulls up to `size` items, where `size=1` and blocking (no timeout) means a plain
  `Stage`, and `stage.size` / `stage.timeout` apply to a `BatchStage`;
- it processes them with `process` / `process_batch`;
- it puts the results with `put_or_abort(..., cancel.is_set)`;
- on `Stop` it flushes the pending batch and exits; it does **not** forward `Stop`,
  because the coordinator does that;
- on a processing exception it lets the exception propagate (the done-callback sets
  `cancel`);
- in a worker process it calls `on_start` at entry and `on_end` in a `finally`, logging
  `on_end` errors when another error is already propagating and raising them otherwise.

Keep `process`/`process_batch` (now using T2.2). Add unit tests in
`tests/test_runners.py`: delivery, batch flush on timeout, batch flush on `Stop`,
exit on `cancel` while blocked on a full queue, and the `on_end` cases.
The old runners may stay until T3.3 removes them.

### [x] T3.3 Segments, coordinator and new `run()`
Rewrite `containers.py` as `segments.py`, or keep the file name and replace its
content, with:
- `InlineSegment(stages)`;
- `ConcurrentSegment(stage, concurrency, executor, ...)`, which owns the executor,
  input queue and futures;
- a `compile_segments(specs, stages, ...)` function;
- the coordinator described in the target design.

Switch `Pipeline.run()` and `process_async()` to it and implement the new `close()`.
Delete the old containers, the old runners and the obsolete `Pipeline` internals listed
above, together with their now-meaningless tests (`tests/test_containers.py` and parts of
`test_lifecycle.py`/`test_runners.py`). Replace them with segment unit tests
only where the contract suite doesn't already cover the behaviour.
**Acceptance:**
- the contract suite (T0.2) passes unchanged;
- `grep -rn "time.sleep" smartpipeline` shows only `RetryManager` backoff and
  `get/put_or_abort` timeouts;
- `pipeline.py` + `segments.py` + `runners.py` total under 1300 lines, against about
  2750 today.

### [x] T3.4 Lifecycle simplification audit
After T3.3, check every attribute assigned in `Pipeline.__init__`. Each one must be
configuration, the state, the lock/events, the engine handle, or the
started-stages list. Remove the rest. Test: concurrent `close()` (T1.2),
`close()` during `on_start` of a slow stage (use `tests/utils.py` helpers), and
`KeyboardInterrupt` simulated inside a stage on a `threads` pipeline. In every case:
no hang, `on_end` pairing holds, and `threading.active_count()` returns to its baseline.

### [x] T3.5 Stress and performance check
- Run the contract suite 20 times:
  `for i in $(seq 20); do .venv/bin/python -m pytest -q tests/pipeline/test_contract.py -p no:randomly || break; done`.
  Every failure is a bug to fix in this task.
- Run `examples/benchmark.py` and compare it with the T0.3 baseline. A regression above
  20% must be fixed or justified under "Notes".

### [x] T3.6 Documentation of the new internals
- Rewrite the `AGENTS.md` "Architecture" and "Invariants" sections (the flow becomes
  `pipeline.py -> segments.py -> runners.py`).
- Update the concurrency explanations in `docs/concurrency.rst`, and redraw or remove
  `docs/_static/concurrency.png` if it shows the old wiring.
- Remove CHANGELOG lines that name deleted internals (`fatal_event` runner argument,
  `_fake_container`, `ConcurrentContainer.executor`, ...) and say once that the
  concurrency engine has been rewritten [D-5].

---

## Phase 4 — Public API improvements (each gated by its decision)

### [x] T4.1 Iterable sources [D-6]
`set_source(source: Source[DataT] | Iterable[Item[DataT]])`. An iterable is wrapped in
a private `_IterableSource(Source)` whose `pop()` calls `next()` and calls `stop()` on
`StopIteration`. Update the README example and `docs/usage.rst` to use a generator, and
add `test_typed.py` coverage.

### [ ] T4.2 Raise on critical errors by default [D-7]
Make `ErrorManager` raise by default. Add `skip_on_critical_error()` to opt out, and
remove `raise_on_critical_error()`/`no_skip_on_critical_error()`, or keep
`raise_on_critical_error` as a no-op if the maintainer prefers. Update
`tests/utils.get_pipeline`, the docs, the CHANGELOG (BREAKING) and `AGENTS.md`.

### [x] T4.3 Keyword-only `append()` parameters [D-8]
Put `*` after `stage` in both overloads and in the implementation, and optionally rename
`args`/`kwargs`. Fix every call site in tests, docs and examples
(`grep -rn "append(" docs examples tests README.rst`).

### [x] T4.4 Top-level exports [D-9]
Export the agreed names from `smartpipeline/__init__.py` with an `__all__`. Avoid an
import cycle (check `python -c "import smartpipeline"`). Update the `AGENTS.md` rule
"exports nothing", the README imports, and add a test.

---

## Phase 5 — Release

### [ ] T5.1 Docs pass
Read `README.rst`, `docs/usage.rst`, `docs/concurrency.rst` and `docs/api.rst`
completely against the final code. Every code snippet must run, so extract them into a
throwaway script and execute them, using stub stages for network/model calls. Every
public class needs an `autoclass` entry. Build with `sphinx-build -W docs /tmp/docs-out`
if Sphinx is available.

### [ ] T5.2 CHANGELOG consolidation
Rewrite the 1.0.0 entry for users, in three sections: **Breaking changes** (each with a
migration line), **Added**, **Fixed**. No private names. Merge duplicated statements:
the entry currently repeats the lifecycle/one-shot rules and the hang fixes several times.

### [ ] T5.3 Version and release
Sync the version in `setup.py`, `smartpipeline/__init__.py` and `docs/conf.py`, and set
the release date in `CHANGELOG.md`. Check that the CI matrix is green on all Python
versions, then follow `CONTRIBUTING.md` (Git Flow release branch from `develop`).

---

## Out of scope for 1.0 (record, don't do)
- asyncio stages/executor.
- Order-preserving concurrent stages.
- Making the silent `BatchStage` `concurrency<1 → 1` promotion explicit.

## Notes
<!-- tasks append findings and benchmark numbers here -->

### Phase 4 (T4.1, T4.3, T4.4 done; T4.2 skipped)

- T4.2 skipped per D-7 ("no"): critical errors stay skipped by default, no code change.

### T0.3 baseline benchmark (2026-11-05, `PYTHONPATH=. .venv/bin/python examples/benchmark.py`, 20k items)

- inline: 20000 items in 0.05s (436582 items/s)
- threads-2: 20000 items in 0.25s (81627 items/s)
- processes-2: 20000 items in 4.77s (4191 items/s)
- batch-100: 20000 items in 1.12s (17855 items/s)

Phase 3 must not regress these by more than 20%.

### T3.5 stress and performance check (measured after the post-review fixes, same machine)

- Contract suite run 20 consecutive times: 20/20 green (47 passed each, no flakes).
- Full suite: 188 passed, 2 skipped. `flake8` / `mypy` / `black` / `isort --profile black` clean.
- 20k items, vs the T0.3 baseline (no regression, all layouts faster):
  - inline: 736813 items/s (baseline 436582, +69%)
  - threads-2: 257229 items/s (baseline 81627, +215%)
  - processes-2: 5095 items/s (baseline 4191, +22%)
  - batch-100: 297811 items/s (baseline 17855, +1568%)

### Phase 3 acceptance deviations (recorded, not fixed)

- T3.3 line target missed: `pipeline.py + segments.py + runners.py` is 1636 lines
  (from 2721 pre-Phase-3, -40%), above the planned <1300. Every structural item on
  the delete list is gone; what remains over target is API docstrings and typing
  kept for Phase 5, plus the `process_async()` queue/consumer plumbing. Further
  reduction would be docstring deletion, not code.
- T3.3/T3.4: `_error_manager_started` and the async plumbing attributes
  (`_async_queue`, `_out_queue`, `_pipeline_executor`, `_async_stopped`, `_stopped`,
  `_build_error`) survived the "remove the rest" audit: each is required to pair
  `on_end` after partial `on_start` in `process()` mode or to drive `process_async()`;
  Engine owns its own copies for `run()` mode.
- Design deviation from the target section: the first inline segment is fed by the
  source thread + queue like any other segment (only a single-inline pipeline pulls
  from the `Source` directly on the caller thread). Uniform wiring, simpler than the
  plan text; AGENTS.md documents the implemented behaviour.

### Post-review fixes (unpushed T3 commits were reviewed, then fixed in `T3: post-review fixes...`)

- `Engine.start()` cancelled the already-submitted workers when a later start step
  (notably `error_manager.on_start()`, which the invariants say can fail) raised,
  instead of hanging in `executor.shutdown(wait=True)`; test for both executors.
- `run()` / `process_async()` re-check `PipelineState.BUILT` under the lock: a second
  starter now raises `PipelineStateError` instead of double-starting the engine, and
  the engine (and its `Manager`) is created only after that check.
- `close()` is annotated and its no-engine branch shares `run_stage_on_ends` with
  the engine instead of duplicating the `on_end` pairing logic; `iter_results` is
  annotated (it was silently unchecked by mypy) and its three near-duplicate loops
  are merged into two; `worker_loop` dispatch deduplicated; dead counter classes
  removed from `utils.py`; stale `terminator` test name and comments updated;
  `_AsyncSource.pop` uses `CONCURRENCY_WAIT` instead of a literal.
- Noted limitation (commented in `Engine.shutdown`): a worker stuck inside user
  code cannot be interrupted; `close(timeout)` only bounds the async-consumer join.
