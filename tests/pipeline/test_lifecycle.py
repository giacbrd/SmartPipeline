import logging
import time
from concurrent.futures import Future
from queue import Queue
from threading import Event, Thread
from unittest import mock

import pytest

from smartpipeline.containers import ConcurrentStageContainer, StageContainer
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.pipeline import Pipeline
from smartpipeline.utils import ThreadCounter
from tests.utils import (
    BrokenInitStage,
    BrokenOnEndBatchStage,
    BrokenOnEndErrorManager,
    BrokenOnEndStage,
    BrokenOnStartErrorManager,
    BrokenProcessAndEndStage,
    ErrorSerializableStage,
    ErrorSource,
    RandomTextSource,
    SlowInitializer,
    SlowOnEndStage,
    TextGenerator,
    TextReverser,
    cleanup_on_exit,
    get_pipeline,
    next_with_timeout,
    run_with_timeout,
    text_item,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


class _ScriptedConcurrentContainer(ConcurrentStageContainer):
    """
    Concurrent container double with scripted failures, for covering the pipeline
    termination and build cleanup paths that real executions never hit
    deterministically (a raising `terminate`/`shutdown`/`run`, non-empty queues).
    """

    def __init__(self, *args, fail_on=(), **kwargs):
        super().__init__(*args, **kwargs)
        self._fail_on = set(fail_on)
        self._queues_empty_calls = 0

    def run(self):
        if "run" in self._fail_on:
            raise RuntimeError("test container run failure")

    def terminate(self):
        if "terminate" in self._fail_on:
            raise RuntimeError("test container terminate failure")
        super().terminate()

    def shutdown(self):
        if "shutdown" in self._fail_on:
            raise RuntimeError("test container shutdown failure")
        super().shutdown()

    def queues_empty(self):
        self._queues_empty_calls += 1
        if "queues_empty" in self._fail_on and self._queues_empty_calls == 1:
            return False
        return super().queues_empty()


def _scripted_container(pipeline, fail_on=()):
    container = _ScriptedConcurrentContainer(
        "scripted",
        TextReverser(),
        ErrorManager(),
        RetryManager(),
        Queue,
        ThreadCounter,
        Event,
        concurrency=1,
        fail_on=fail_on,
    )
    pipeline._containers["scripted"] = container
    return container


def test_on_start_error_manager_raises_on_build():
    """Error-manager on_start failures abort the build with cleanup."""
    pipeline = get_pipeline().set_error_manager(BrokenOnStartErrorManager())
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="on_start"):
            (
                pipeline.set_source(RandomTextSource(3))
                .append("stage", TextReverser(), concurrency=1, executor="processes")
                .build()
            )


@pytest.mark.parametrize(
    "stage,kwargs,timeout,match",
    [
        pytest.param(BrokenOnEndStage(), {}, 30.0, "test on_end failure", id="inline"),
        pytest.param(
            BrokenOnEndStage(),
            {"concurrency": 1},
            30.0,
            "test on_end failure",
            id="threads",
        ),
        pytest.param(
            BrokenOnEndStage(),
            {"concurrency": 1, "executor": "processes"},
            40.0,
            "test on_end failure",
            id="processes",
        ),
        pytest.param(
            BrokenOnEndBatchStage(),
            {"concurrency": 1, "executor": "processes"},
            40.0,
            "test batch on_end failure",
            id="batch-processes",
        ),
    ],
)
def test_on_end_failure_raises_on_run(stage, kwargs, timeout, match):
    """Stage on_end failures raise on run, inline and concurrent."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", stage, **kwargs)
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match=match):
            run_with_timeout(pipeline, timeout=timeout)


def test_on_end_error_manager_raises_on_run():
    """Error-manager on_end failures raise on run."""
    pipeline = (
        get_pipeline()
        .set_error_manager(BrokenOnEndErrorManager())
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser())
        .build()
    )
    with pytest.raises(RuntimeError, match="test error manager on_end failure"):
        run_with_timeout(pipeline)


def test_on_end_both_hooks_fail_groups_errors():
    """Stage and error-manager on_end failures group into one ExceptionGroup."""
    pipeline = (
        get_pipeline()
        .set_error_manager(BrokenOnEndErrorManager())
        .set_source(RandomTextSource(2))
        .append("boom", BrokenOnEndStage(), concurrency=1)
        .build()
    )
    with pytest.raises(ExceptionGroup, match="termination failed") as exc_info:
        run_with_timeout(pipeline)
    assert len(exc_info.value.exceptions) == 2
    assert all(isinstance(e, RuntimeError) for e in exc_info.value.exceptions)


@pytest.mark.parametrize("concurrency", [0, 1])
def test_process_error_preserved_when_on_end_fails(concurrency):
    """Process errors surface even when on_end fails too, inline and concurrent."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append(
            "bad",
            BrokenProcessAndEndStage(),
            concurrency=concurrency,
            executor="processes",
        )
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(ValueError, match="test process failure"):
            run_with_timeout(pipeline, timeout=40)


def test_concurrent_init_failure_raises_on_build():
    """Stage __init__ failures raise on build with cleanup."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("broken", BrokenInitStage)
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="test init failure"):
            pipeline.build()


def test_concurrent_on_start_failure_raises_on_build():
    """Stage on_start failures raise on build with cleanup."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("broken", ErrorSerializableStage, concurrency=1)
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(IOError):
            pipeline.build()


def test_build_waits_for_slow_concurrent_init():
    """Build waits for slow concurrent stage initialization."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("slow", SlowInitializer, args=[1])
    )
    with cleanup_on_exit(pipeline):
        pipeline.build()
        assert pipeline._executors_ready


def test_concurrent_linker_returns_on_init_errors():
    """Init failures surface even when more stages are appended after the broken one."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("broken", BrokenInitStage)
        .append_concurrently("second", TextReverser)
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="test init failure"):
            pipeline.build()


def test_terminator_error_surfaces_during_run():
    """Slow on_end failures surface while the run terminates."""
    pipeline = (
        get_pipeline().set_source(RandomTextSource(2)).append("slow", SlowOnEndStage())
    )
    for i in range(30):
        pipeline.append(f"reverser-{i}", TextReverser())
    pipeline.build()
    with pytest.raises(RuntimeError, match="slow on_end"):
        run_with_timeout(pipeline, timeout=60)


def test_terminate_all_waits_for_non_empty_queues():
    """_terminate_all drains non-empty queues before terminating containers."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser(), concurrency=1)
    )
    container = _scripted_container(pipeline, fail_on=("queues_empty",))
    pipeline._terminate_all()
    assert container._queues_empty_calls == 2


def test_terminate_all_collects_terminate_error():
    """Container terminate failures raise from _terminate_all."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser(), concurrency=1)
    )
    _scripted_container(pipeline, fail_on=("terminate",))
    with pytest.raises(RuntimeError, match="test container terminate failure"):
        pipeline._terminate_all()


def test_build_cleanup_survives_container_failures():
    """Build cleans up containers when their startup fails."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser(), concurrency=1)
    )
    _scripted_container(pipeline, fail_on=("run", "terminate", "shutdown"))
    with pytest.raises(RuntimeError, match="test container run failure"):
        pipeline.build()


def test_wait_for_stopped_waits_for_stop():
    """_wait_for_stopped blocks until the container stops."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(1))
        .append("reverser", TextReverser())
        .build()
    )
    container = pipeline._containers["reverser"]
    assert not container.is_stopped()
    stopped = Event()

    def _waiter():
        pipeline._wait_for_stopped(container)
        stopped.set()

    waiter = Thread(target=_waiter)
    waiter.start()
    time.sleep(0.5)
    assert not stopped.is_set()
    container.stop()
    waiter.join(timeout=5)
    assert stopped.is_set()


def test_build_waits_for_pending_container_init():
    """Build waits for containers that are still under construction."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser())
    )
    # simulate a stage which is still under concurrent construction
    pipeline._containers["pending"] = pipeline._fake_container

    def _finish_init():
        time.sleep(0.3)
        pipeline._containers["pending"] = StageContainer(
            "pending", TextReverser(), ErrorManager(), RetryManager()
        )

    finisher = Thread(target=_finish_init)
    finisher.start()
    try:
        pipeline.build()
    finally:
        finisher.join(timeout=5)
    assert pipeline._executors_ready


def test_source_errors():
    """Source failures raise and stop the source across executors."""
    for concurrency, executor in ((0, "threads"), (2, "threads"), (2, "processes")):
        source = ErrorSource()
        pipeline = (
            Pipeline()
            .set_source(source)
            .append(
                "text_generator",
                TextGenerator(),
                concurrency=concurrency,
                executor=executor,
            )
            .build()
        )
        run = pipeline.run()
        if concurrency == 0:
            assert next_with_timeout(run, pipeline=pipeline)
            assert next_with_timeout(run, pipeline=pipeline)
            assert next_with_timeout(run, pipeline=pipeline)
        assert not source.is_stopped
        with pytest.raises(ValueError):
            # in case of concurrent pipeline the exception can raise immediately, or after we consume some item
            for _ in range(3):
                next_with_timeout(run, pipeline=pipeline)
        assert source.is_stopped


def test_new_shared_event_dispatch():
    """Shared events come from the sync manager when one is present."""
    pipeline = Pipeline()
    event = pipeline._new_shared_event()
    assert isinstance(event, type(Event()))
    with mock.patch.object(
        pipeline, "_new_mp_event", return_value="mp-event"
    ) as mp_event:
        pipeline._sync_manager = mock.sentinel.manager
        assert pipeline._new_shared_event() == "mp-event"
        mp_event.assert_called_once_with()


def test_pipeline_is_fatal():
    """_is_fatal reflects whether the fatal event fired."""
    pipeline = Pipeline()
    assert not pipeline._is_fatal()
    pipeline._fatal_event = Event()
    assert not pipeline._is_fatal()
    pipeline._fatal_event.set()
    assert pipeline._is_fatal()


def test_check_errors_and_wait_for_runner_error():
    """Runner failures surface through _check_errors and _wait_for_runner_error."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    # no concurrent container: nothing to raise, just sleeps
    pipeline._wait_for_runner_error(retries=2, wait_seconds=0.01)
    container = ConcurrentStageContainer(
        "boom",
        TextReverser(),
        ErrorManager(),
        RetryManager(),
        Queue,
        ThreadCounter,
        Event,
        concurrency=1,
    )
    future: Future = Future()
    future.set_exception(ValueError("runner boom"))
    container._futures = [future]
    pipeline._containers["boom"] = container
    with pytest.raises(ValueError):
        pipeline._check_errors()
    with pytest.raises(ValueError):
        pipeline._wait_for_runner_error(retries=1, wait_seconds=0.01)


def test_get_processed_or_abort():
    """_get_processed_or_abort returns items, propagates runner errors, or aborts silently."""
    pipeline = Pipeline()

    class _FakeContainer:
        def __init__(self, items):
            self._items = list(items)

        def get_processed(self, block=True, timeout=None):
            return self._items.pop(0) if self._items else None

    assert pipeline._get_processed_or_abort(_FakeContainer([text_item()])) is not None
    assert (
        pipeline._get_processed_or_abort(_FakeContainer([None, text_item()]))
        is not None
    )
    pipeline._fatal_event = Event()
    pipeline._fatal_event.set()
    with mock.patch.object(
        pipeline, "_wait_for_runner_error", side_effect=RuntimeError("runner")
    ):
        with pytest.raises(RuntimeError):
            pipeline._get_processed_or_abort(_FakeContainer([None]))
    with mock.patch.object(pipeline, "_wait_for_runner_error", return_value=None):
        assert pipeline._get_processed_or_abort(_FakeContainer([None])) is None


def test_process_propagates_runner_error():
    """process() raises when a runner has already failed."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with mock.patch.object(pipeline, "_is_fatal", return_value=True):
        with mock.patch.object(
            pipeline,
            "_wait_for_runner_error",
            side_effect=RuntimeError("runner down"),
        ):
            with pytest.raises(RuntimeError):
                pipeline.process(text_item())


def test_exit_run_does_not_mask_termination_errors(caplog):
    """_exit_run counts the run and logs stop failures instead of masking them."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with mock.patch.object(pipeline, "stop", side_effect=RuntimeError("stop boom")):
        with caplog.at_level(logging.ERROR):
            pipeline._exit_run(None, None)
    assert pipeline.count == 1
    assert any(
        "problems in terminating" in record.msg.lower() for record in caplog.records
    )


def test_run_generator_close_terminates():
    """Closing run() early stops the source container."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(5))
        .append("reverser", TextReverser())
        .build()
    )
    gen = pipeline.run()
    first = next_with_timeout(gen, pipeline=pipeline)
    assert first.data["text"]
    gen.close()
    assert pipeline._source_container.is_stopped()


def test_exit_run_joins_terminator_thread():
    """_exit_run joins the source and terminator threads."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(1))
        .append("reverser", TextReverser())
        .build()
    )
    source_thread = Thread(target=lambda: None)
    terminator_thread = Thread(target=lambda: None)
    source_thread.start()
    terminator_thread.start()
    source_thread.join()
    terminator_thread.join()
    pipeline._exit_run(source_thread, terminator_thread)
    assert pipeline.count == 1


def test_wait_for_runner_error_warns_when_no_error_surfaced(caplog):
    """A missing runner error warns instead of hanging the waiter."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with caplog.at_level(logging.WARNING):
        pipeline._wait_for_runner_error(retries=2, wait_seconds=0.01)
    assert any("runner error" in record.msg.lower() for record in caplog.records)
