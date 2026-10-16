import time
from queue import Queue
from threading import Event, Thread

import pytest

from smartpipeline.containers import ConcurrentStageContainer, StageContainer
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.runners import OnEndErrors
from smartpipeline.utils import ThreadCounter, raise_collected_errors
from tests.utils import (
    BrokenInitStage,
    BrokenOnEndBatchStage,
    BrokenOnEndErrorManager,
    BrokenOnEndStage,
    BrokenOnStartErrorManager,
    BrokenProcessAndEndStage,
    ErrorSerializableStage,
    RandomTextSource,
    SlowInitializer,
    SlowOnEndStage,
    TextReverser,
    get_pipeline,
    run_with_timeout,
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


def test_on_end_errors_type():
    assert not OnEndErrors(None, None)
    assert OnEndErrors(None, None).exceptions == []
    first, second = RuntimeError("first"), ValueError("second")
    assert OnEndErrors(first, None).exceptions == [first]
    assert OnEndErrors(None, second).exceptions == [second]
    result = OnEndErrors(first, second)
    assert result
    assert result.exceptions == [first, second]


def test_raise_collected_errors():
    assert raise_collected_errors([], "nothing") is None
    error = RuntimeError("single")
    with pytest.raises(RuntimeError, match="single"):
        raise_collected_errors([error], "single")
    with pytest.raises(ExceptionGroup, match="many") as exc_info:
        raise_collected_errors([RuntimeError("a"), ValueError("b")], "many")
    assert len(exc_info.value.exceptions) == 2


def test_on_start_error_manager_raises_on_build():
    pipeline = get_pipeline().set_error_manager(BrokenOnStartErrorManager())
    try:
        with pytest.raises(RuntimeError, match="on_start"):
            (
                pipeline.set_source(RandomTextSource(3))
                .append("stage", TextReverser(), concurrency=1, executor="processes")
                .build()
            )
    finally:
        try:
            pipeline._stop_logs_receiver()
        except Exception:
            pass
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_on_end_inline_raises_on_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("reverser", TextReverser())
        .append("boom", BrokenOnEndStage())
        .build()
    )
    with pytest.raises(RuntimeError, match="test on_end failure"):
        run_with_timeout(pipeline)


def test_on_end_thread_raises_on_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", BrokenOnEndStage(), concurrency=1)
        .build()
    )
    with pytest.raises(RuntimeError, match="test on_end failure"):
        run_with_timeout(pipeline)


def test_on_end_processes_raises_on_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", BrokenOnEndStage(), concurrency=1, executor="processes")
        .build()
    )
    try:
        with pytest.raises(RuntimeError, match="test on_end failure"):
            run_with_timeout(pipeline, timeout=40)
    finally:
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_on_end_batch_processes_raises_on_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", BrokenOnEndBatchStage(), concurrency=1, executor="processes")
        .build()
    )
    try:
        with pytest.raises(RuntimeError, match="test batch on_end failure"):
            run_with_timeout(pipeline, timeout=40)
    finally:
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_on_end_error_manager_raises_on_run():
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
    try:
        with pytest.raises(ValueError, match="test process failure"):
            run_with_timeout(pipeline, timeout=40)
    finally:
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_concurrent_init_failure_raises_on_build():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("broken", BrokenInitStage)
    )
    try:
        with pytest.raises(RuntimeError, match="test init failure"):
            pipeline.build()
    finally:
        try:
            pipeline._stop_logs_receiver()
        except Exception:
            pass
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_concurrent_on_start_failure_raises_on_build():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("broken", ErrorSerializableStage, concurrency=1)
    )
    try:
        with pytest.raises(IOError):
            pipeline.build()
    finally:
        try:
            pipeline._stop_logs_receiver()
        except Exception:
            pass
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_build_waits_for_slow_concurrent_init():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("slow", SlowInitializer, args=[1])
    )
    try:
        pipeline.build()
        assert pipeline._executors_ready
    finally:
        try:
            pipeline._stop_logs_receiver()
        except Exception:
            pass
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_concurrent_linker_returns_on_init_errors():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_concurrently("broken", BrokenInitStage)
        .append_concurrently("second", TextReverser)
    )
    try:
        with pytest.raises(RuntimeError, match="test init failure"):
            pipeline.build()
    finally:
        try:
            pipeline._stop_logs_receiver()
        except Exception:
            pass
        try:
            pipeline.shutdown()
        except Exception:
            pass


def test_terminator_error_surfaces_during_run():
    pipeline = (
        get_pipeline().set_source(RandomTextSource(2)).append("slow", SlowOnEndStage())
    )
    for i in range(30):
        pipeline.append(f"reverser-{i}", TextReverser())
    pipeline.build()
    with pytest.raises(RuntimeError, match="slow on_end"):
        run_with_timeout(pipeline, timeout=60)


def test_terminate_all_waits_for_non_empty_queues():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser(), concurrency=1)
    )
    container = _scripted_container(pipeline, fail_on=("queues_empty",))
    pipeline._terminate_all()
    assert container._queues_empty_calls == 2


def test_terminate_all_collects_terminate_error():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser(), concurrency=1)
    )
    _scripted_container(pipeline, fail_on=("terminate",))
    with pytest.raises(RuntimeError, match="test container terminate failure"):
        pipeline._terminate_all()


def test_build_cleanup_survives_container_failures():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser(), concurrency=1)
    )
    _scripted_container(pipeline, fail_on=("run", "terminate", "shutdown"))
    with pytest.raises(RuntimeError, match="test container run failure"):
        pipeline.build()


def test_wait_for_stopped_waits_for_stop():
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
