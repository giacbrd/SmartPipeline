__author__ = "Giacomo Berardi <giacbrd.com>"

"""Public-contract suite for SmartPipeline.

These tests use only the public pipeline surface and the shared test helpers,
so they stay valid while the execution engine changes. Each scenario runs on
inline stages and on thread and process executors where that changes behavior.
"""

import threading
import time

import pytest

from smartpipeline.error.exceptions import BatchOutputError, RetryError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.item import Item
from smartpipeline.pipeline import Pipeline, PipelineState, PipelineStateError
from smartpipeline.stage import Stage
from tests.utils import (
    PIPELINE_TIMEOUT,
    BatchTextReverser,
    BrokenOnEndErrorManager,
    BrokenOnEndStage,
    CriticalIOErrorStage,
    CustomizableBrokenStage,
    ErrorSerializableStage,
    ErrorStage,
    ExceptionStage,
    LifecycleStage,
    RandomTextSource,
    TextDuplicator,
    TextReverser,
    TimeWaster,
    TruncatingBatchStage,
    check_items,
    get_item_with_timeout,
    get_pipeline,
    mark_item_checked,
    next_with_timeout,
    run_with_timeout,
    text_item,
)


class FailAfter(Stage):
    """Succeed a fixed number of items, then fail on every later one."""

    def __init__(self, successes=10):
        self.succeed = successes
        self.seen = 0

    def process(self, item):
        self.seen = self.seen + 1
        if self.seen > self.succeed:
            raise RuntimeError("test fail after")
        return item


def wait_for_thread_settle(baseline, timeout=10.0):
    """Wait until worker threads started by a run have exited."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if threading.active_count() <= baseline:
            return
        time.sleep(0.05)
    raise AssertionError("worker threads did not exit in time")


def concurrent_kwargs(mode):
    """Concurrency arguments for a mode name used across this suite."""
    if mode == "inline":
        return {}
    return {"concurrency": 1, "executor": mode}


def make_delivery_pipeline(layout, executor, count):
    """Build a delivery pipeline for one layout with N source items."""
    pipeline = get_pipeline().set_source(RandomTextSource(count))
    if layout == "all_inline":
        pipeline.append("first", TextReverser())
        pipeline.append("second", TextDuplicator())
    elif layout == "concurrent_first":
        pipeline.append("first", TextReverser(), concurrency=2, executor=executor)
        pipeline.append("second", TextDuplicator())
    elif layout == "concurrent_last":
        pipeline.append("first", TextReverser())
        pipeline.append("second", TextDuplicator(), concurrency=2, executor=executor)
    elif layout == "alternating":
        pipeline.append("first", TextReverser())
        pipeline.append("second", TextReverser(), concurrency=2, executor=executor)
        pipeline.append("third", TextDuplicator())
        pipeline.append("fourth", TextDuplicator(), concurrency=2, executor=executor)
    elif layout == "batch_middle":
        pipeline.append("first", TextReverser())
        pipeline.append(
            "batch", BatchTextReverser(size=5), concurrency=2, executor=executor
        )
        pipeline.append("last", TextDuplicator())
    elif layout == "batch_last":
        pipeline.append("first", TextReverser())
        pipeline.append(
            "batch", BatchTextReverser(size=5), concurrency=2, executor=executor
        )
    else:
        raise ValueError("unknown layout")
    return pipeline


def test_delivery_all_inline():
    """An all-inline pipeline delivers every item and counts them."""
    count = 20
    pipeline = make_delivery_pipeline("all_inline", "threads", count)
    items = run_with_timeout(pipeline)
    check_items(items, count, pipeline)
    for item in items:
        assert item.get_timing("first") is not None
        assert item.get_timing("second") is not None


@pytest.mark.parametrize(
    "layout",
    [
        "concurrent_first",
        "concurrent_last",
        "alternating",
        "batch_middle",
        "batch_last",
    ],
)
@pytest.mark.parametrize("executor", ["threads", "processes"])
def test_delivery_concurrent_layouts(layout, executor):
    """Every layout delivers all items on threads and on processes."""
    count = 20
    pipeline = make_delivery_pipeline(layout, executor, count)
    items = run_with_timeout(pipeline)
    check_items(items, count, pipeline)
    assert pipeline.state is PipelineState.CLOSED


@pytest.mark.parametrize("mode", ["inline", "threads", "processes"])
def test_soft_errors_are_delivered(mode):
    """Soft errors attach to the item without dropping it."""
    kwargs = concurrent_kwargs(mode)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser(), **kwargs)
        .append("failing", ErrorStage(), **kwargs)
    )
    items = run_with_timeout(pipeline)
    assert len(items) == 10
    assert pipeline.count == 10
    for item in items:
        assert item.has_soft_errors()
        assert item.get_timing("reverser") is not None
        assert item.get_timing("failing") is not None


@pytest.mark.parametrize("mode", ["inline", "threads", "processes"])
def test_critical_errors_are_skipped_by_default(mode):
    """Critical errors skip downstream stages but still deliver the item."""
    kwargs = concurrent_kwargs(mode)
    pipeline = (
        Pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser(), **kwargs)
        .append("critical", CriticalIOErrorStage(), **kwargs)
        .append("after", TextDuplicator(), **kwargs)
    )
    items = run_with_timeout(pipeline)
    assert len(items) == 10
    assert pipeline.count == 10
    for item in items:
        assert item.has_critical_errors()
        assert item.get_timing("reverser") is not None
        assert item.get_timing("critical") is not None
        assert item.get_timing("after") is None


@pytest.mark.parametrize("mode", ["inline", "threads", "processes"])
def test_critical_errors_raise_when_enabled(mode):
    """With raise on critical, the run surfaces the failure."""
    kwargs = concurrent_kwargs(mode)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser(), **kwargs)
        .append("critical", ExceptionStage(), **kwargs)
    )
    with pytest.raises(Exception, match="test exception"):
        run_with_timeout(pipeline)


@pytest.mark.parametrize("executor", ["threads", "processes"])
def test_failing_stage_with_full_queues_raises(executor):
    """A failure while queues are full raises instead of hanging."""
    pipeline = (
        get_pipeline(max_queues_size=1)
        .set_source(RandomTextSource(50))
        .append("failing", FailAfter(10), concurrency=2, executor=executor)
        .append("slow", TimeWaster(0.05), concurrency=1, executor=executor)
    )
    with pytest.raises(RuntimeError, match="test fail after"):
        run_with_timeout(pipeline, timeout=30.0)
    assert pipeline.state is PipelineState.CLOSED


@pytest.mark.parametrize("concurrency", [0, 1])
def test_on_start_failure_pairs_on_end(concurrency):
    """A failed on_start runs on_end only for the stages before it."""
    baseline = threading.active_count()
    events = []
    kwargs = {} if concurrency == 0 else {"concurrency": 1}
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("first", LifecycleStage(events), **kwargs)
        .append("second", LifecycleStage(events, fail_on_start=True), **kwargs)
        .append("third", LifecycleStage(events), **kwargs)
    )
    with pytest.raises(RuntimeError, match="failed on_start"):
        run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    assert "first:start" in events
    assert "first:end" in events
    assert "second:start" in events
    assert "second:end" not in events
    assert not [e for e in events if e.startswith("third")]
    assert not [e for e in events if e.startswith("error-manager")]
    wait_for_thread_settle(baseline)


def test_on_start_failure_processes():
    """A worker on_start failure surfaces and closes the pipeline."""
    baseline = threading.active_count()
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append(
            "failing", ErrorSerializableStage(), concurrency=1, executor="processes"
        )
    )
    with pytest.raises(IOError):
        run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    wait_for_thread_settle(baseline)


@pytest.mark.parametrize("mode", ["inline", "threads", "processes"])
def test_on_end_failure_raises(mode):
    """An on_end failure surfaces at the end of the run."""
    kwargs = concurrent_kwargs(mode)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", BrokenOnEndStage(), **kwargs)
    )
    with pytest.raises(RuntimeError, match="test on_end failure"):
        run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED


@pytest.mark.parametrize("mode", ["inline", "threads"])
def test_two_on_end_failures_group(mode):
    """Two failing on_end hooks report together as one group."""
    kwargs = concurrent_kwargs(mode)
    pipeline = (
        get_pipeline()
        .set_error_manager(BrokenOnEndErrorManager())
        .set_source(RandomTextSource(2))
        .append("boom", BrokenOnEndStage(), **kwargs)
    )
    with pytest.raises(ExceptionGroup) as exc_info:
        run_with_timeout(pipeline)
    assert len(exc_info.value.exceptions) == 2
    assert pipeline.state is PipelineState.CLOSED


@pytest.mark.parametrize("executor", ["threads", "processes"])
def test_batch_output_error_stops_pipeline(executor):
    """A batch stage returning the wrong count stops the pipeline."""
    pipeline = (
        Pipeline()
        .set_source(RandomTextSource(3))
        .append("short", TruncatingBatchStage(size=3), concurrency=1, executor=executor)
    )
    with pytest.raises(BatchOutputError, match="exactly one item per input item"):
        run_with_timeout(pipeline)


@pytest.mark.parametrize("mode", ["inline", "threads", "processes"])
def test_retry_gives_single_retry_error(mode):
    """Exhausted retries attach exactly one RetryError per item."""
    kwargs = {
        "retryable_errors": (ValueError,),
        "max_retries": 2,
        "backoff": 0,
    }
    kwargs.update(concurrent_kwargs(mode))
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("flaky", CustomizableBrokenStage([ValueError]), **kwargs)
    )
    items = run_with_timeout(pipeline)
    assert len(items) == 3
    assert pipeline.count == 3
    for item in items:
        assert not item.has_critical_errors()
        soft = list(item.soft_errors())
        assert len(soft) == 1
        assert isinstance(soft[0], RetryError)
        assert "failed after 3 attempts" in str(soft[0])


def test_lifecycle_state_sequence():
    """A run walks INITIAL to BUILT to RUNNING to CLOSED."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("reverser", TextReverser())
    )
    assert pipeline.state is PipelineState.INITIAL
    pipeline.build()
    assert pipeline.state is PipelineState.BUILT
    run = pipeline.run()
    try:
        first = next_with_timeout(run, pipeline=pipeline)
        assert first.data["text"]
        assert pipeline.state is PipelineState.RUNNING
        items = [first]
        while True:
            try:
                items.append(next_with_timeout(run, pipeline=pipeline))
            except StopIteration:
                break
        assert len(items) == 3
    finally:
        run.close()
    assert pipeline.state is PipelineState.CLOSED
    assert pipeline.count == 3


def test_append_after_build_raises():
    """Builder calls are rejected once the pipeline is built."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with pytest.raises(PipelineStateError):
        pipeline.append("other", TextReverser())
    with pytest.raises(PipelineStateError):
        pipeline.set_error_manager(ErrorManager())
    pipeline.close()


def test_run_twice_raises():
    """A pipeline runs once: a second run is rejected."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("reverser", TextReverser())
    )
    items = run_with_timeout(pipeline)
    assert len(items) == 3
    assert pipeline.state is PipelineState.CLOSED
    with pytest.raises(PipelineStateError):
        run_with_timeout(pipeline)


def test_close_is_idempotent():
    """Closing twice is safe, before and after a run."""
    pipeline = get_pipeline().append("reverser", TextReverser())
    pipeline.close()
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", TextReverser())
    )
    run_with_timeout(pipeline)
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED


def test_close_from_another_thread_ends_run():
    """close() from another thread ends a running generator."""
    pipeline = (
        get_pipeline(max_queues_size=1)
        .set_source(RandomTextSource(200))
        .append("reverser", TextReverser(), concurrency=1)
    )
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        assert pipeline.state is PipelineState.RUNNING
        closer = threading.Thread(target=pipeline.close)
        closer.start()
        closer.join(PIPELINE_TIMEOUT)
        assert not closer.is_alive()
        assert list(run) == []
    finally:
        run.close()
    assert pipeline.state is PipelineState.CLOSED


def test_early_break_releases_pipeline():
    """Closing the run generator early closes the pipeline."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(20))
        .append("slow", TimeWaster(0.05), concurrency=2)
    )
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        run.close()
    finally:
        run.close()
    deadline = time.monotonic() + PIPELINE_TIMEOUT
    while pipeline.state is not PipelineState.CLOSED:
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_process_repeatedly():
    """process() runs one item at a time and counts nothing."""
    pipeline = get_pipeline().append("reverser", TextReverser())
    first = pipeline.process(text_item("hello"))
    second = pipeline.process(text_item("world"))
    assert first.data["text"] == "olleh"
    assert second.data["text"] == "dlrow"
    assert pipeline.state is PipelineState.BUILT
    assert pipeline.count == 0
    pipeline.close()
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED


@pytest.mark.parametrize("executor", ["threads", "processes"])
def test_process_rejects_concurrent_stages(executor):
    """process() refuses pipelines with concurrent stages."""
    pipeline = get_pipeline().append(
        "reverser", TextReverser(), concurrency=1, executor=executor
    )
    with pytest.raises(ValueError, match="concurrent"):
        pipeline.process(text_item())
    assert pipeline.state is PipelineState.INITIAL
    pipeline.close()


def test_process_async_with_callback():
    """process_async() delivers items with their callbacks run."""
    pipeline = get_pipeline().append("reverser", TextReverser())
    first = text_item("hello")
    second = text_item("world")
    pipeline.process_async(first, callback=mark_item_checked)
    pipeline.process_async(second, callback=mark_item_checked)
    pipeline.stop()
    got = [get_item_with_timeout(pipeline), get_item_with_timeout(pipeline)]
    texts = sorted([item.data["text"] for item in got])
    assert texts == ["dlrow", "olleh"]
    for item in got:
        assert item.metadata["check"] is True
    assert pipeline.count == 2


def test_process_async_callback_error_is_delivered():
    """A failing callback surfaces through get_item."""
    pipeline = get_pipeline().append("reverser", TextReverser())

    def failing(item: Item):
        raise RuntimeError("callback failed")

    pipeline.process_async(text_item("hello"), callback=failing)
    pipeline.stop()
    with pytest.raises(RuntimeError, match="callback failed"):
        get_item_with_timeout(pipeline)
