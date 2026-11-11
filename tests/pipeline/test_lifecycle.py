import logging
import multiprocessing
import threading
import time
from threading import Thread

import pytest

from smartpipeline.error.handling import ErrorManager
from smartpipeline.item import Stop
from smartpipeline.pipeline import Pipeline, PipelineState, PipelineStateError
from tests.utils import (
    PIPELINE_TIMEOUT,
    QUEUE_TIMEOUT,
    BrokenInitStage,
    BrokenOnEndBatchStage,
    BrokenOnEndErrorManager,
    BrokenOnEndStage,
    BrokenOnStartErrorManager,
    BrokenProcessAndEndStage,
    ErrorSerializableStage,
    ErrorSource,
    LifecycleErrorManager,
    LifecycleStage,
    RandomTextSource,
    SlowInitializer,
    SlowOnEndStage,
    TextGenerator,
    TextReverser,
    TimeWaster,
    build_with_timeout,
    cleanup_on_exit,
    get_item_with_timeout,
    get_pipeline,
    next_with_timeout,
    run_with_timeout,
    text_item,
    wait_for_state,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_on_start_error_manager_raises_on_run():
    """Error-manager on_start failures abort the run, not the build."""
    pipeline = (
        get_pipeline()
        .set_error_manager(BrokenOnStartErrorManager())
        .set_source(RandomTextSource(3))
        .append("stage", TextReverser(), concurrency=1, executor="processes")
    )
    with cleanup_on_exit(pipeline):
        pipeline.build()
        with pytest.raises(RuntimeError, match="on_start"):
            run_with_timeout(pipeline)
        assert pipeline.state is PipelineState.CLOSED


@pytest.mark.parametrize(
    "stage,kwargs,match",
    [
        pytest.param(BrokenOnEndStage(), {}, "test on_end failure", id="inline"),
        pytest.param(
            BrokenOnEndStage(),
            {"concurrency": 1},
            "test on_end failure",
            id="threads",
        ),
        pytest.param(
            BrokenOnEndStage(),
            {"concurrency": 1, "executor": "processes"},
            "test on_end failure",
            id="processes",
        ),
        pytest.param(
            BrokenOnEndBatchStage(),
            {"concurrency": 1, "executor": "processes"},
            "test batch on_end failure",
            id="batch-processes",
        ),
    ],
)
def test_on_end_failure_raises_on_run(stage, kwargs, match):
    """Stage on_end failures raise on run, inline and concurrent."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", stage, **kwargs)
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match=match):
            run_with_timeout(pipeline)


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
            run_with_timeout(pipeline)


def test_concurrent_init_failure_raises_on_build():
    """Stage __init__ failures raise on build with cleanup."""
    pipeline = (
        get_pipeline().set_source(RandomTextSource(2)).append("broken", BrokenInitStage)
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="test init failure"):
            pipeline.build()


def test_build_groups_stage_construction_errors():
    """Every stage construction failure is reported, not only the first one."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("broken0", BrokenInitStage)
        .append("broken1", BrokenInitStage)
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(
            ExceptionGroup, match="stage construction failed"
        ) as exc_info:
            pipeline.build()
        assert len(exc_info.value.exceptions) == 2


def test_repeated_build_after_init_failure_raises_again():
    """A second build after a failed initialization raises again instead of hanging."""
    pipeline = (
        get_pipeline().set_source(RandomTextSource(2)).append("broken", BrokenInitStage)
    )
    with cleanup_on_exit(pipeline):
        # the first build fails on the broken stage initialization
        with pytest.raises(RuntimeError, match="test init failure"):
            build_with_timeout(pipeline, timeout=10)
        # the failed stage container will never be ready: any further build must report
        # the failure again instead of waiting for it indefinitely
        with pytest.raises(RuntimeError, match="test init failure"):
            build_with_timeout(pipeline, timeout=10)
        with pytest.raises(RuntimeError, match="test init failure"):
            run_with_timeout(pipeline, timeout=10)


def test_concurrent_on_start_failure_raises_on_run():
    """Stage on_start failures raise on run, while build only prepares."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("broken", ErrorSerializableStage, concurrency=1)
    )
    with cleanup_on_exit(pipeline):
        pipeline.build()
        with pytest.raises(IOError):
            run_with_timeout(pipeline)
        assert pipeline.state is PipelineState.CLOSED


def test_build_waits_for_slow_concurrent_init():
    """Build waits for slow concurrent stage initialization."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("slow", SlowInitializer, init_args=[1])
    )
    with cleanup_on_exit(pipeline):
        pipeline.build()
        assert pipeline.state is PipelineState.BUILT
        assert pipeline.get_stage("slow") is not None


def test_build_surfaces_stage_construction_errors():
    """Init failures surface even when more stages are appended after the broken one."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("broken", BrokenInitStage)
        .append("second", TextReverser)
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="test init failure"):
            pipeline.build()


def test_on_end_error_surfaces_during_run():
    """Slow on_end failures surface while the run terminates."""
    pipeline = (
        get_pipeline().set_source(RandomTextSource(2)).append("slow", SlowOnEndStage())
    )
    for i in range(30):
        pipeline.append(f"reverser-{i}", TextReverser())
    pipeline.build()
    with pytest.raises(RuntimeError, match="slow on_end"):
        run_with_timeout(pipeline)


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


def _lifecycle_pipeline(events):
    return (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("tracked", LifecycleStage(events))
    )


def test_build_only_prepares_without_starting_stages():
    """build() wires the pipeline but does not run on_start or start workers."""
    events = []
    pipeline = _lifecycle_pipeline(events).build()
    assert pipeline.state is PipelineState.BUILT
    assert events == []
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    # no on_end without a paired on_start
    assert events == []


def test_close_from_initial_does_not_start_stages():
    """Closing an unbuilt pipeline leaves every lifecycle hook untouched."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    assert events == []
    with pytest.raises(PipelineStateError):
        pipeline.build()


def test_state_transition_matrix():
    """A run walks the pipeline through INITIAL, BUILT, RUNNING and CLOSED."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    assert pipeline.state is PipelineState.INITIAL
    pipeline.build()
    assert pipeline.state is PipelineState.BUILT
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        assert pipeline.state is PipelineState.RUNNING
    finally:
        pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    run.close()
    assert pipeline.state is PipelineState.CLOSED


def test_run_lifecycle_hooks_are_paired():
    """run() starts the stage once, processes every item and ends the stage once."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    items = run_with_timeout(pipeline)
    assert len(items) == 3
    assert pipeline.state is PipelineState.CLOSED
    assert events[0] == "tracked:start"
    assert events[-1] == "tracked:end"
    assert events.count("tracked:start") == 1
    assert events.count("tracked:end") == 1
    assert events.count("tracked:process") == 3


def test_run_is_one_shot():
    """A completed run closes the pipeline for every further execution."""
    pipeline = _lifecycle_pipeline([])
    run_with_timeout(pipeline)
    with pytest.raises(PipelineStateError):
        run_with_timeout(pipeline)
    with pytest.raises(PipelineStateError):
        pipeline.process(text_item())
    with pytest.raises(PipelineStateError):
        pipeline.process_async(text_item())
    with pytest.raises(PipelineStateError):
        pipeline.build()


def test_process_can_be_repeated_until_close():
    """process() runs one item at a time on a built pipeline and counts nothing."""
    events = []
    pipeline = get_pipeline().append("tracked", LifecycleStage(events))
    first = pipeline.process(text_item("a"))
    second = pipeline.process(text_item("b"))
    assert first.data["text"] == "a"
    assert second.data["text"] == "b"
    assert pipeline.state is PipelineState.BUILT
    assert pipeline.count == 0
    assert events.count("tracked:start") == 1
    assert events.count("tracked:process") == 2
    pipeline.close()
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    assert events.count("tracked:end") == 1


def test_count_counts_delivered_run_items():
    """count is the number of items delivered by run()."""
    pipeline = _lifecycle_pipeline([])
    items = run_with_timeout(pipeline)
    assert len(items) == 3
    assert pipeline.count == 3


def test_process_async_accepts_items_while_running():
    """process_async() can enqueue more items while the pipeline is running."""
    pipeline = get_pipeline().append("reverser", TextReverser())
    with cleanup_on_exit(pipeline):
        pipeline.process_async(text_item("a"))
        wait_for_state(pipeline, PipelineState.RUNNING)
        pipeline.process_async(text_item("b"))
        pipeline.stop()
        texts = {get_item_with_timeout(pipeline).data["text"] for _ in range(2)}
        assert texts == {"a", "b"}
        wait_for_state(pipeline, PipelineState.CLOSED)
        assert pipeline.count == 2


def test_process_async_rejected_after_close():
    """process_async() cannot enqueue items on a closed pipeline."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    pipeline.close()
    with pytest.raises(PipelineStateError):
        pipeline.process_async(text_item())


def test_get_item_after_async_run_end_raises():
    """get_item() raises after the async run drained instead of blocking."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    pipeline.process_async(text_item("hello"))
    pipeline.process_async(text_item("world"))
    pipeline.stop()
    texts = {get_item_with_timeout(pipeline).data["text"] for _ in range(2)}
    assert texts == {"olleh", "dlrow"}
    outcome: dict = {}

    def _get():
        try:
            pipeline.get_item()
        except BaseException as e:  # noqa: BLE001 - record whatever surfaces
            outcome["error"] = e

    waiter = Thread(target=_get, daemon=True)
    waiter.start()
    waiter.join(timeout=1.0)
    assert not waiter.is_alive()
    assert isinstance(outcome.get("error"), PipelineStateError)
    assert "asynchronous run ended" in str(outcome["error"])
    with pytest.raises(PipelineStateError, match="asynchronous run ended"):
        get_item_with_timeout(pipeline, timeout=5.0)
    pipeline.close()

    # get_item() after close() and after an async callback failure must also raise
    # instead of blocking forever
    closed_async = get_pipeline().append("reverser", TextReverser()).build()
    closed_async.process_async(text_item("hello"))
    assert get_item_with_timeout(closed_async).data["text"] == "olleh"
    closed_async.close()
    with pytest.raises(PipelineStateError, match="asynchronous run ended"):
        get_item_with_timeout(closed_async, timeout=5.0)

    failed_async = get_pipeline().append("reverser", TextReverser()).build()
    failed_async.process_async(
        text_item("hello"),
        callback=lambda _: (_ for _ in ()).throw(RuntimeError("callback boom")),
    )
    failed_async.stop()
    with pytest.raises(RuntimeError, match="callback boom"):
        get_item_with_timeout(failed_async, timeout=5.0)
    with pytest.raises(PipelineStateError, match="asynchronous run ended"):
        get_item_with_timeout(failed_async, timeout=5.0)


def test_builder_methods_rejected_after_build():
    """Builder methods are rejected once the pipeline is built."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with pytest.raises(PipelineStateError, match="state 'built'"):
        pipeline.append("other", TextReverser())
    with pytest.raises(PipelineStateError, match="state 'built'"):
        pipeline.append("other", TextReverser)
    with pytest.raises(PipelineStateError, match="state 'built'"):
        pipeline.set_error_manager(ErrorManager())
    # a built pipeline without a source is not a dead end
    pipeline.set_source(RandomTextSource(1))
    assert pipeline.build() is pipeline
    pipeline.close()


def test_builder_methods_rejected_while_running():
    """Builder methods and single-item processing are rejected during a run."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        assert pipeline.state is PipelineState.RUNNING
        actions = (
            lambda: pipeline.append("other", TextReverser()),
            lambda: pipeline.append("other", TextReverser),
            lambda: pipeline.set_error_manager(ErrorManager()),
            lambda: pipeline.set_source(RandomTextSource(1)),
            pipeline.build,
            lambda: next(pipeline.run()),
            lambda: pipeline.process(text_item()),
        )
        for action in actions:
            with pytest.raises(PipelineStateError):
                action()
    finally:
        pipeline.close()
        run.close()
    assert pipeline.state is PipelineState.CLOSED


def test_builder_methods_rejected_after_close():
    """A closed pipeline rejects every configuration and execution method."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    pipeline.close()
    actions = (
        lambda: pipeline.append("other", TextReverser()),
        lambda: pipeline.append("other", TextReverser),
        lambda: pipeline.set_error_manager(ErrorManager()),
        lambda: pipeline.set_source(RandomTextSource(1)),
        pipeline.build,
        lambda: next(pipeline.run()),
        lambda: pipeline.process(text_item()),
        lambda: pipeline.process_async(text_item()),
    )
    for action in actions:
        with pytest.raises(PipelineStateError):
            action()


def test_close_from_running_terminates_and_counts_delivered():
    """close() terminates a running pipeline, running on_end once."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        assert pipeline.state is PipelineState.RUNNING
        pipeline.close()
        assert pipeline.state is PipelineState.CLOSED
        assert pipeline.count == 1
        assert events.count("tracked:start") == 1
        assert events.count("tracked:end") == 1
    finally:
        run.close()
    pipeline.close()
    assert events.count("tracked:end") == 1


def test_start_failure_pairs_on_end_with_started_stages():
    """A stage which failed on_start has no on_end, while earlier stages do."""
    events = []
    pipeline = (
        get_pipeline()
        .set_error_manager(LifecycleErrorManager(events))
        .set_source(RandomTextSource(1))
        .append("ok", LifecycleStage(events))
        .append("broken", LifecycleStage(events, fail_on_start=True))
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="broken failed on_start"):
            run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    assert "ok:start" in events
    assert "ok:end" in events
    assert "broken:start" in events
    assert "broken:end" not in events
    # the error manager on_start never completed, so it has no on_end either
    assert "error-manager:start" not in events
    assert "error-manager:end" not in events


def test_error_manager_start_failure_pairs_stage_on_end():
    """A failed error-manager on_start still closes the stages already started."""
    events = []
    pipeline = (
        get_pipeline()
        .set_error_manager(LifecycleErrorManager(events, fail_on_start=True))
        .set_source(RandomTextSource(1))
        .append("ok", LifecycleStage(events))
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="error manager on_start"):
            run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    assert "ok:start" in events
    assert "ok:end" in events
    assert "error-manager:start" in events
    assert "error-manager:end" not in events


@pytest.mark.parametrize("executor", ("threads", "processes"))
def test_error_manager_start_failure_cancels_started_workers(executor):
    """An error-manager on_start failure after worker submission cancels the run."""
    events = []
    pipeline = (
        get_pipeline()
        .set_error_manager(LifecycleErrorManager(events, fail_on_start=True))
        .set_source(RandomTextSource(2))
        .append("tracked", LifecycleStage(events), concurrency=2, executor=executor)
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="error manager on_start"):
            run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    if executor == "threads":
        assert "tracked:start" in events
        assert "tracked:end" in events


def test_source_can_be_set_after_build():
    """A source can be attached to a built pipeline before it starts."""
    events = []
    pipeline = get_pipeline().append("tracked", LifecycleStage(events)).build()
    pipeline.set_source(RandomTextSource(2))
    items = run_with_timeout(pipeline)
    assert len(items) == 2
    assert pipeline.count == 2


def test_run_returns_when_close_is_requested():
    """A run generator resumed after close() ends without delivering more items."""
    events = []
    pipeline = _lifecycle_pipeline(events)
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        pipeline.close()
        assert list(run) == []
    finally:
        run.close()
    assert pipeline.state is PipelineState.CLOSED


def _call_with_timeout(func, timeout: float = PIPELINE_TIMEOUT) -> bool:
    """Run ``func`` in a daemon thread, return True when it ended in time."""
    thread = Thread(target=func, daemon=True)
    thread.start()
    thread.join(timeout)
    return not thread.is_alive()


def test_closing_a_run_early_after_stop_reached_the_output_does_not_hang():
    """Breaking out of run() while the termination drains pending items ends."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(4))
        .append("slow", TimeWaster(0.5), concurrency=5)
        .build()
    )
    with cleanup_on_exit(pipeline):
        run = pipeline.run()
        assert next_with_timeout(run, pipeline=pipeline)
        assert _call_with_timeout(run.close)
        assert pipeline.state is PipelineState.CLOSED


def test_close_from_another_thread_after_an_early_break_does_not_hang():
    """close() from another thread ends while a run is abandoned mid-termination."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(4))
        .append("slow", TimeWaster(0.5), concurrency=5)
        .build()
    )
    with cleanup_on_exit(pipeline):
        run = pipeline.run()
        assert next_with_timeout(run, pipeline=pipeline)
        assert _call_with_timeout(pipeline.close)
        assert pipeline.state is PipelineState.CLOSED
        run.close()


def _active_baseline() -> int:
    # Let daemon threads from previous tests exit.
    deadline = time.monotonic() + 10.0
    baseline = threading.active_count()
    while time.monotonic() < deadline:
        time.sleep(0.05)
        current = threading.active_count()
        if current <= baseline:
            return current
        baseline = current
    return threading.active_count()


def test_concurrent_close_calls_are_serialized():
    """A second close() waits for the first instead of returning early."""
    baseline = _active_baseline()
    entered = threading.Event()
    release = threading.Event()
    events = []

    class BlockingOnEndStage(TextReverser):
        def on_end(self):
            events.append("end-start")
            entered.set()
            assert release.wait(timeout=PIPELINE_TIMEOUT)
            events.append("end-done")

    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", BlockingOnEndStage(), concurrency=1)
        .build()
    )
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        first_done = threading.Event()
        second_done = threading.Event()

        def _first():
            pipeline.close()
            first_done.set()

        def _second():
            pipeline.close()
            second_done.set()

        first = threading.Thread(target=_first, daemon=True)
        first.start()
        assert entered.wait(PIPELINE_TIMEOUT)
        second = threading.Thread(target=_second, daemon=True)
        second.start()
        time.sleep(0.3)
        assert not second_done.is_set()
        assert not first_done.is_set()
        release.set()
        first.join(PIPELINE_TIMEOUT)
        second.join(PIPELINE_TIMEOUT)
        assert not first.is_alive()
        assert not second.is_alive()
        assert pipeline.state is PipelineState.CLOSED
        assert events == ["end-start", "end-done"]
    finally:
        release.set()
        run.close()
    assert pipeline.state is PipelineState.CLOSED
    deadline = time.monotonic() + 10.0
    while threading.active_count() > baseline:
        assert time.monotonic() < deadline
        time.sleep(0.05)


def test_close_during_slow_on_start_pairs_on_end():
    """close() racing with a slow on_start terminates without hanging."""
    baseline = _active_baseline()
    events = []
    started = threading.Event()
    release = threading.Event()

    class SlowStartStage(TextReverser):
        def on_start(self):
            events.append("start")
            started.set()
            assert release.wait(timeout=PIPELINE_TIMEOUT)

        def on_end(self):
            events.append("end")

    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("slow", SlowStartStage())
        .build()
    )
    runner = threading.Thread(target=lambda: list(pipeline.run()), daemon=True)
    runner.start()
    try:
        assert started.wait(PIPELINE_TIMEOUT)
        closer = threading.Thread(target=pipeline.close, daemon=True)
        closer.start()
        release.set()
        closer.join(PIPELINE_TIMEOUT)
        assert not closer.is_alive()
        runner.join(PIPELINE_TIMEOUT)
        assert not runner.is_alive()
        assert pipeline.state is PipelineState.CLOSED
        assert events.count("start") == 1
        assert events.count("end") == 1
    finally:
        release.set()
        pipeline.close()
    deadline = time.monotonic() + 10.0
    while threading.active_count() > baseline:
        assert time.monotonic() < deadline
        time.sleep(0.05)


def test_keyboard_interrupt_in_threads_stage_cleans_up():
    """A KeyboardInterrupt inside a threads stage ends the run without hanging."""
    baseline = _active_baseline()
    events = []

    class InterruptStage(TextReverser):
        def __init__(self):
            self._seen = 0

        def on_start(self):
            events.append("start")

        def on_end(self):
            events.append("end")

        def process(self, item):
            self._seen += 1
            if self._seen == 2:
                raise KeyboardInterrupt("test interrupt")
            return super().process(item)

    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("interrupt", InterruptStage(), concurrency=2)
        .build()
    )
    try:
        with pytest.raises(KeyboardInterrupt):
            run_with_timeout(pipeline, timeout=PIPELINE_TIMEOUT)
    finally:
        pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    assert events.count("start") >= 1
    assert events.count("end") >= 1
    deadline = time.monotonic() + 10.0
    while threading.active_count() > baseline:
        assert time.monotonic() < deadline
        time.sleep(0.05)


def test_run_does_not_double_start_a_starting_pipeline():
    """A second run() while the first is still starting raises instead of starting a second engine."""
    started = threading.Event()
    release = threading.Event()

    class SlowStartStage(TextReverser):
        def on_start(self):
            started.set()
            assert release.wait(timeout=PIPELINE_TIMEOUT)

    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(1))
        .append("slow", SlowStartStage())
        .build()
    )
    outcome: dict[str, object] = {}

    def _run(tag: str) -> None:
        try:
            outcome[tag] = list(pipeline.run())
        except BaseException as e:
            outcome[tag] = e

    with cleanup_on_exit(pipeline):
        runner = Thread(target=_run, args=("first",), daemon=True)
        runner.start()
        assert started.wait(PIPELINE_TIMEOUT)
        challenger = Thread(target=_run, args=("second",), daemon=True)
        challenger.start()
        time.sleep(0.2)  # let the challenger queue on the starting pipeline's lock
        release.set()
        challenger.join(PIPELINE_TIMEOUT)
        runner.join(PIPELINE_TIMEOUT)
        assert not challenger.is_alive() and not runner.is_alive()
    assert isinstance(outcome["second"], PipelineStateError)
    assert pipeline.state is PipelineState.CLOSED


def test_start_rollback_releases_workers_when_on_end_fails():
    """A failing on_end during the start rollback still releases executors and manager."""
    baseline = {child.pid for child in multiprocessing.active_children()}
    pipeline = (
        get_pipeline()
        .set_error_manager(BrokenOnStartErrorManager())
        .set_source(RandomTextSource(3))
        .append("boom", BrokenOnEndStage())
        .append("workers", TextReverser(), concurrency=2, executor="processes")
        .build()
    )
    with cleanup_on_exit(pipeline):
        with pytest.raises(RuntimeError, match="error manager on_start"):
            run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    leaked = [
        child
        for child in multiprocessing.active_children()
        if child.pid not in baseline and child.is_alive()
    ]
    assert not leaked, f"the failed start leaked processes: {leaked}"


def test_source_stop_failure_never_breaks_the_shutdown():
    """Neither stop() nor close() propagates a source which fails while stopping."""

    class BrokenStopSource(RandomTextSource):
        def stop(self):
            super().stop()
            raise RuntimeError("test source stop failure")

    pipeline = (
        get_pipeline()
        .set_source(BrokenStopSource(3))
        .append("reverser", TextReverser())
        .build()
    )
    pipeline.stop()
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED


def test_close_runs_the_on_ends_of_a_pipeline_without_engine():
    """A pipeline which only processed items has no engine: close() ends its stages."""
    pipeline = get_pipeline().append("boom", BrokenOnEndStage()).build()
    pipeline.process(text_item())
    with pytest.raises(RuntimeError, match="test on_end failure"):
        pipeline.close()
    assert pipeline.state is PipelineState.CLOSED


def test_close_warns_when_the_background_runner_is_stuck(caplog):
    """A runner ignoring the cancellation is reported, never waited for forever."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    stuck = threading.Event()
    blocker = Thread(target=stuck.wait, daemon=True)
    blocker.start()
    pipeline._pipeline_executor = blocker
    try:
        with caplog.at_level(logging.WARNING):
            pipeline._join_background(0.01)
        assert "did not end" in caplog.text
    finally:
        stuck.set()
        blocker.join(QUEUE_TIMEOUT)


def test_process_start_failure_ends_the_stages_which_started():
    """A failing on_start closes the pipeline, ending the stages which did start."""
    events: list = []
    pipeline = (
        get_pipeline()
        .set_error_manager(LifecycleErrorManager(events))
        .append("first", LifecycleStage(events))
        .append("boom", LifecycleStage(events, fail_on_start=True))
        .build()
    )
    with pytest.raises(RuntimeError, match="failed on_start"):
        pipeline.process(text_item())
    # the error manager never started, so its on_end is skipped
    assert events == ["first:start", "boom:start", "first:end"]
    assert pipeline.state is PipelineState.CLOSED


def test_process_refuses_items_when_close_wins_the_start_race():
    """A pipeline closed while its stages start never processes another item."""
    events: list = []

    class ClosingOnStartStage(TextReverser):
        def __init__(self, closer):
            super().__init__()
            self._closer = closer

        def on_start(self):
            events.append("closer:start")
            self._closer()

        def on_end(self):
            events.append("closer:end")

        def process(self, item):
            raise AssertionError("a closed pipeline cannot process items")

    pipeline = (
        get_pipeline()
        .set_error_manager(LifecycleErrorManager(events))
        .append("first", LifecycleStage(events))
    )
    pipeline.append("closer", ClosingOnStartStage(pipeline.close))
    pipeline.append("third", LifecycleStage(events)).build()
    with pytest.raises(PipelineStateError, match="The pipeline is closed"):
        pipeline.process(text_item())
    assert events == ["first:start", "closer:start", "first:end", "closer:end"]


def test_process_refuses_an_item_which_became_a_stop():
    """A stage turning its item into a Stop ends the pipeline for process()."""

    class StopReturningStage(TextReverser):
        def process(self, item):
            return Stop()

    pipeline = get_pipeline().append("stopper", StopReturningStage()).build()
    with pytest.raises(PipelineStateError, match="source is stopped"):
        pipeline.process(text_item())


def test_process_async_start_failure_closes_the_pipeline():
    """A failing on_start closes an asynchronous run which never started."""
    events: list = []
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", LifecycleStage(events, fail_on_start=True), concurrency=1)
        .build()
    )
    with pytest.raises(RuntimeError, match="failed on_start"):
        pipeline.process_async(text_item())
    assert pipeline.state is PipelineState.CLOSED


def test_async_run_logs_the_on_end_failure_of_its_own_close(caplog):
    """An asynchronous run has no waiting caller: its close failure is only logged."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("boom", BrokenOnEndStage(), concurrency=1)
        .build()
    )
    with cleanup_on_exit(pipeline):
        pipeline.process_async(text_item())
        assert get_item_with_timeout(pipeline).data["text"] == "hello"
        with caplog.at_level(logging.ERROR):
            pipeline.stop()
            with pytest.raises(PipelineStateError, match="asynchronous run ended"):
                get_item_with_timeout(pipeline)
            assert pipeline._closed.wait(PIPELINE_TIMEOUT)
            # the consumer closes its own run: wait for its thread to log the failure
            pipeline._pipeline_executor.join(PIPELINE_TIMEOUT)
        assert "Problems in closing an async run" in caplog.text


def test_concurrent_process_async_startup_serializes():
    """Concurrent initial process_async() calls wait for engine startup."""
    started = threading.Event()
    release = threading.Event()

    class SlowStartStage(TextReverser):
        def on_start(self):
            started.set()
            assert release.wait(timeout=PIPELINE_TIMEOUT)

    pipeline = get_pipeline().append("slow", SlowStartStage()).build()
    second_done = threading.Event()
    with cleanup_on_exit(pipeline):
        first = Thread(
            target=lambda: pipeline.process_async(text_item("a")), daemon=True
        )
        first.start()
        assert started.wait(PIPELINE_TIMEOUT)
        second = Thread(
            target=lambda: (
                pipeline.process_async(text_item("b")),
                second_done.set(),
            ),
            daemon=True,
        )
        second.start()
        time.sleep(0.1)
        assert not second_done.is_set()
        release.set()
        first.join(PIPELINE_TIMEOUT)
        second.join(PIPELINE_TIMEOUT)
        pipeline.stop()
        texts = {get_item_with_timeout(pipeline).data["text"] for _ in range(2)}
        assert texts == {"a", "b"}


def test_close_during_inline_source_pop_skips_stage_process():
    """Closing while an inline run is in source.pop() never calls process after on_end."""
    in_pop = threading.Event()
    release_pop = threading.Event()

    class BlockingSource(RandomTextSource):
        def __init__(self):
            super().__init__(2)

        def pop(self):
            if self.counter == 1:
                in_pop.set()
                assert release_pop.wait(timeout=PIPELINE_TIMEOUT)
            return super().pop()

    events: list = []
    pipeline = (
        get_pipeline()
        .set_source(BlockingSource())
        .append("tracked", LifecycleStage(events))
        .build()
    )
    run = pipeline.run()
    try:
        assert next_with_timeout(run, pipeline=pipeline)
        consumer = Thread(target=lambda: list(run), daemon=True)
        consumer.start()
        assert in_pop.wait(PIPELINE_TIMEOUT)
        pipeline.close()
        release_pop.set()
        consumer.join(PIPELINE_TIMEOUT)
        assert not consumer.is_alive()
    finally:
        release_pop.set()
        run.close()
    assert events == ["tracked:start", "tracked:process", "tracked:end"]
