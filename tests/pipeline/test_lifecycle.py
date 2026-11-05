import logging
import time
from threading import Thread

import pytest

from smartpipeline.error.handling import ErrorManager
from smartpipeline.pipeline import Pipeline, PipelineState, PipelineStateError
from tests.utils import (
    PIPELINE_TIMEOUT,
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
        .append("slow", SlowInitializer, args=[1])
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


def test_terminator_error_surfaces_during_run():
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


def _active_baseline():
    import threading
    import time

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




def _active_baseline():
    import threading
    import time

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
    import threading

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
        import time as _time

        _time.sleep(0.3)
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
    deadline = __import__("time").monotonic() + 10.0
    while __import__("threading").active_count() > baseline:
        assert __import__("time").monotonic() < deadline
        __import__("time").sleep(0.05)


def test_close_during_slow_on_start_pairs_on_end():
    """close() racing with a slow on_start terminates without hanging."""
    import threading
    import time

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
    import threading
    import time

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
        with __import__("pytest").raises(KeyboardInterrupt):
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
