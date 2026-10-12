import pytest

from smartpipeline.runners import OnEndErrors
from smartpipeline.utils import raise_collected_errors
from tests.utils import (
    BrokenInitStage,
    BrokenOnEndBatchStage,
    BrokenOnEndErrorManager,
    BrokenOnEndStage,
    BrokenOnStartErrorManager,
    BrokenProcessAndEndStage,
    ErrorSerializableStage,
    RandomTextSource,
    TextReverser,
    get_pipeline,
    run_with_timeout,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


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
