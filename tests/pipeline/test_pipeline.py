import logging
import traceback
from typing import Any, Dict

import pytest

from smartpipeline.error.exceptions import RetryError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.item import Item
from smartpipeline.pipeline import (
    Pipeline,
    PipelineState,
    PipelineStateError,
    StageSpec,
)
from smartpipeline.stage import Stage
from tests.utils import (
    BatchTextReverser,
    CriticalIOErrorStage,
    CustomException,
    CustomizableBrokenStage,
    ErrorStage,
    ExceptionStage,
    RandomTextSource,
    TextDuplicator,
    TextReverser,
    get_item_with_timeout,
    get_pipeline,
    run_with_timeout,
    text_item,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

logger = logging.getLogger(__name__)


def test_run():
    """An inline pipeline runs 10 items with timings and rejects duplicate stage names."""
    pipeline: Pipeline[Dict[str, Any]] = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("duplicator", TextDuplicator())
        .build()
    )
    assert pipeline.name.startswith("Pipeline-")
    assert isinstance(pipeline.get_stage("duplicator"), TextDuplicator)
    for item in run_with_timeout(pipeline):
        assert len([x for x in item.data.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert pipeline.count == 10
    with pytest.raises(ValueError):
        Pipeline().append("reverser", TextReverser()).append("reverser", TextReverser())


def test_soft_errors_are_logged_and_flow(caplog):
    """Soft errors attach per item with timings while downstream stages still run."""
    error_manager = ErrorManager()
    pipeline = (
        get_pipeline()
        .set_error_manager(error_manager)
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error", ErrorStage())
        .build()
    )
    for item in run_with_timeout(pipeline):
        assert item.has_soft_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert all(
        "has generated an error" in record.msg.lower()
        and "stage error" == str(record.args[0]).lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    assert pipeline.count == 10


def test_soft_errors_with_downstream_stages(caplog):
    """Soft errors do not stop downstream stages from processing the item."""
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error", ErrorStage())
        .append("duplicator", TextDuplicator())
        .build()
    )
    for item in run_with_timeout(pipeline):
        assert item.has_soft_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.data.keys())
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert all(
        "has generated an error" in record.msg.lower()
        and "stage error" == str(record.args[0]).lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    assert pipeline.count == 10


def test_critical_io_error_skips_downstream():
    """Critical IO errors attach the cause and skip downstream stages."""
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error1", CriticalIOErrorStage())
        .build()
    )
    for item in run_with_timeout(pipeline):
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), IOError)
            assert str(error) == "test pipeline critical IO error"


def test_critical_error_skips_downstream(caplog):
    """Critical errors skip downstream stages and are logged."""
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error1", ExceptionStage())
        .append("error2", ErrorStage())
        .build()
    )
    for item in run_with_timeout(pipeline):
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1")
        assert not item.get_timing("error2")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert (
                str(error) == "Exception: test exception"
                and "Exception" in str(error)
                and str(error.get_exception()) == "test exception"
                and str(error) != "test pipeline error"
            )
    assert all(
        "has generated an error" in record.msg.lower()
        and "stage error1" == str(record.args[0]).lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    assert pipeline.count == 10


def test_raise_on_critical_error_stops_run():
    """A critical error raises out of run when raise_on_critical_error is set."""
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error", ExceptionStage())
        .build()
    )
    with pytest.raises(Exception):
        try:
            for _ in run_with_timeout(pipeline):
                pass
        except Exception:
            assert 'Exception("test exception")' in traceback.format_exc()
            raise


def test_no_skip_on_critical_error_runs_downstream():
    """Critical errors still run downstream stages when skipping is disabled."""
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager().no_skip_on_critical_error())
        .set_source(RandomTextSource(10))
        .append("reverser1", TextReverser())
        .append("error", ExceptionStage())
        .append("reverser2", TextReverser())
        .build()
    )
    for item in run_with_timeout(pipeline):
        assert item.get_timing("reverser1")
        assert item.get_timing("error")
        assert item.get_timing("reverser2")
    assert pipeline.count == 10


def test_build():
    """Test the build pattern with non-conventional order of the method calls"""
    pipeline1: Pipeline[Dict[str, Any]] = (
        Pipeline[Dict[str, Any]]()
        .append("reverser", TextReverser())
        .append("duplicator", TextDuplicator())
        .set_source(RandomTextSource(10))
        .build()
    )
    pipeline2: Pipeline[Dict[str, Any]] = (
        Pipeline[Dict[str, Any]]()
        .append("reverser", TextReverser())
        .set_source(RandomTextSource(10))
        .append("duplicator", TextDuplicator())
        .build()
    )
    for pipeline in (pipeline1, pipeline2):
        for item in run_with_timeout(pipeline):
            assert len([x for x in item.data.keys() if x.startswith("text")]) == 2
            assert item.get_timing("reverser")
            assert item.get_timing("duplicator")
        assert pipeline.count == 10

    pipeline3 = (
        Pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .set_error_manager(ErrorManager())
        .append("error", ErrorStage())
        .build()
    )
    pipeline4 = (
        Pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error", ErrorStage())
        .set_error_manager(ErrorManager())
        .build()
    )
    for pipeline in (
        pipeline3,
        pipeline4,
    ):
        for item in run_with_timeout(pipeline):
            assert item.has_soft_errors()
        assert pipeline.count == 10


def test_retryable_stage(items_generator_fx):
    """Retryable stages turn failures into RetryError soft errors; bad configs raise."""
    # default behaviour
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append("broken_stage", CustomizableBrokenStage([ValueError]))
        .append("reverser1", TextReverser())
    )
    item = pipeline.process(item)
    assert not item.has_soft_errors()  # no soft errors
    critical_errors = list(item.critical_errors())
    assert len(critical_errors) == 1 and isinstance(
        critical_errors[0].get_exception(), ValueError
    )

    # not defining any error to which retry on, so the pipeline behaviour should not change
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage",
            CustomizableBrokenStage([ValueError]),
            backoff=1,
            max_retries=1,
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = pipeline.process(item)
    assert not item.has_soft_errors()  # no soft errors
    critical_errors = list(item.critical_errors())
    assert len(critical_errors) == 1 and isinstance(
        critical_errors[0].get_exception(), ValueError
    )

    # defining some retryable_errors for which the stage is force to try the reprocessing of the item
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage",
            CustomizableBrokenStage([ValueError]),
            backoff=1,
            max_retries=1,
            retryable_errors=(ValueError,),
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = pipeline.process(item)
    assert not item.has_critical_errors()
    soft_errors = list(item.soft_errors())
    assert len(soft_errors) == 1 and all(
        isinstance(err, RetryError) and isinstance(err.get_exception(), ValueError)
        for err in soft_errors
    )

    # 0 retries
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage",
            CustomizableBrokenStage([ValueError]),
            backoff=0,
            max_retries=0,
            retryable_errors=(ValueError,),
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = pipeline.process(item)
    assert not item.has_critical_errors()
    soft_errors = list(item.soft_errors())
    assert len(soft_errors) == 1 and all(
        isinstance(err, RetryError) and isinstance(err.get_exception(), ValueError)
        for err in soft_errors
    )

    # mixing retryable and critical errors
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage0",
            CustomizableBrokenStage([CustomException]),
            backoff=1,
            max_retries=1,
            retryable_errors=(ValueError, CustomException),
        )
        .append("reverser1", TextReverser())
        .append(
            "broken_stage1",
            CustomizableBrokenStage([CustomException]),
            backoff=1,
            max_retries=2,
            retryable_errors=(ValueError,),
        )
        .build()
    )

    item = pipeline.process(item)
    critical_errors = list(item.critical_errors())
    assert len(critical_errors) == 1 and (
        critical_errors[0].get_exception(),
        CustomException,
    )
    soft_errors = list(item.soft_errors())
    assert len(soft_errors) == 1 and all(
        isinstance(err, RetryError) and isinstance(err.get_exception(), CustomException)
        for err in soft_errors
    )
    with pytest.raises(ValueError):
        Pipeline().append(
            "broken_stage",
            CustomizableBrokenStage([CustomException]),
            backoff=1,
            max_retries=1,
            retryable_errors=[ValueError, CustomException],
        )
    with pytest.raises(ValueError):
        Pipeline().append(
            "broken_stage",
            CustomizableBrokenStage([CustomException]),
            backoff="1.2",
            max_retries=1,
            retryable_errors=(ValueError, CustomException),
        )
    with pytest.raises(ValueError):
        Pipeline().append(
            "broken_stage",
            CustomizableBrokenStage([CustomException]),
            backoff=1,
            max_retries=2.5,
            retryable_errors=(ValueError, CustomException),
        )


def test_retry_on_different_errors(items_generator_fx):
    """Retries report one soft error carrying all the failed attempts."""
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage",
            CustomizableBrokenStage([CustomException, ValueError]),
            backoff=0,
            max_retries=2,
            retryable_errors=(CustomException, ValueError),
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = pipeline.process(item)
    soft_errors = list(item.soft_errors())
    assert len(soft_errors) == 1
    error = soft_errors[0]
    assert isinstance(error, RetryError) and error.get_stage() == "broken_stage"
    assert isinstance(error.get_exception(), CustomException)
    assert "failed after 3 attempts" in str(error)
    assert "CustomException" in str(error) and "ValueError" in str(error)


def test_exponential_backoff_retry_strategy(items_generator_fx):
    """Retry backoff delays grow exponentially with max_retries."""
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage",
            CustomizableBrokenStage([CustomException]),
            backoff=1,
            max_retries=0,
            retryable_errors=(CustomException,),
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = pipeline.process(item)
    assert item.get_timing("broken_stage") < 1
    item = next(items_generator_fx)
    pipeline = (
        Pipeline()
        .append("reverser0", TextReverser())
        .append(
            "broken_stage",
            CustomizableBrokenStage([CustomException]),
            backoff=1,
            max_retries=3,
            retryable_errors=(CustomException,),
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = pipeline.process(item)
    assert 7 <= item.get_timing("broken_stage") <= 8
    soft_errors = list(item.soft_errors())
    assert len(soft_errors) == 1
    assert all(
        isinstance(err, RetryError) and isinstance(err.get_exception(), CustomException)
        for err in soft_errors
    )


def test_implicit_build_on_first_use():
    """run() and process_async() build the pipeline implicitly on first use."""
    pipeline: Pipeline[Dict[str, Any]] = (
        get_pipeline()
        .set_source(RandomTextSource(3))
        .append("reverser", TextReverser())
    )
    assert pipeline.state is PipelineState.INITIAL
    items = run_with_timeout(pipeline)
    assert pipeline.state is PipelineState.CLOSED
    assert len(items) == 3

    pipeline = get_pipeline().append("reverser", TextReverser())
    assert pipeline.state is PipelineState.INITIAL
    item = Item()
    item.data["text"] = "hello"
    pipeline.process_async(item)
    pipeline.stop()
    assert get_item_with_timeout(pipeline).data["text"] == "olleh"


def test_append_names_a_stage_instance_immediately():
    """An instance-based stage knows its pipeline name before build()."""
    stage = TextReverser()
    get_pipeline().append("reverser", stage)
    assert stage.name == "reverser"


def test_append_stage_class_is_constructed_at_build():
    """append() accepts a stage class and defers its construction to build()."""
    events = []

    class TrackingStage(Stage):
        def __init__(self):
            events.append("init")

        def process(self, item):
            return item

    pipeline = get_pipeline().append("tracked", TrackingStage)
    assert events == []
    assert pipeline.get_stage("tracked") is None
    pipeline.build()
    assert events == ["init"]
    assert isinstance(pipeline.get_stage("tracked"), TrackingStage)
    pipeline.close()


def test_append_stage_class_with_init_args_and_init_kwargs():
    """append() forwards constructor arguments to a class-based stage."""
    pipeline = get_pipeline().append("reverser", TextReverser, init_args=[1])
    assert pipeline.process(text_item("hello")).data["text"] == "olleh"
    pipeline = get_pipeline().append(
        "reverser", TextReverser, init_kwargs={"cycles": 1}
    )
    assert pipeline.process(text_item("hello")).data["text"] == "olleh"


def test_append_rejects_constructor_args_for_an_instance():
    """Constructor arguments are only valid together with a stage class."""
    with pytest.raises(ValueError, match="stage class"):
        get_pipeline().append("reverser", TextReverser(), init_args=[1])


def test_append_concurrently_is_removed():
    """The old append_concurrently() alias is gone: use append() with a class."""
    assert not hasattr(Pipeline(), "append_concurrently")


@pytest.mark.parametrize(
    "stage, kwargs",
    [
        (TextReverser(), {"concurrency": 1}),
        (BatchTextReverser(size=2), {}),
    ],
    ids=["thread", "batch"],
)
def test_process_rejects_concurrent_stages(stage, kwargs):
    """process() fails fast instead of hanging on a concurrent stage."""
    pipeline = get_pipeline().append("stage", stage, **kwargs)
    with pytest.raises(ValueError, match="concurrent"):
        pipeline.process(text_item())
    # the guard runs after build, so the pipeline is built but no stage has started
    assert pipeline.state is PipelineState.BUILT


def test_process_async_callback_error_is_delivered():
    """Callback failures surface through get_item instead of killing the runner."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()

    def failing_callback(item):
        raise RuntimeError("callback failed")

    pipeline.process_async(text_item("hello"), callback=failing_callback)
    pipeline.stop()
    with pytest.raises(RuntimeError, match="callback failed"):
        get_item_with_timeout(pipeline)


def test_close_before_build_with_class_stage():
    """close() releases a pipeline whose class-based stage was never constructed."""
    pipeline = get_pipeline().append("reverser", TextReverser)
    pipeline.close()
    assert pipeline.state is PipelineState.CLOSED
    assert pipeline._stages == {}


def test_process_async_without_callback():
    """process_async without a callback still delivers the item to get_item."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    pipeline.process_async(text_item("hello"))
    pipeline.stop()
    result = get_item_with_timeout(pipeline)
    assert result.data["text"] == "olleh"


def test_stage_spec_requires_exactly_one_stage():
    """A stage spec holds either an instance or a class, never both or neither."""
    with pytest.raises(ValueError, match="exactly one"):
        StageSpec(name="empty")
    with pytest.raises(ValueError, match="exactly one"):
        StageSpec(name="both", stage=TextReverser(), stage_class=TextReverser)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"concurrency": -1}, "concurrency"),
        ({"concurrency": "1"}, "concurrency"),
        ({"executor": "invalid"}, "executor"),
    ],
)
def test_append_rejects_invalid_execution_options(kwargs, match):
    """append() validates concurrency and executor values."""
    with pytest.raises(ValueError, match=match):
        get_pipeline().append("stage", TextReverser(), **kwargs)


def test_append_rejects_non_stage_types():
    """append() rejects classes and instances which are not stages."""
    with pytest.raises(ValueError, match="extend Stage"):
        get_pipeline().append("stage", int)
    with pytest.raises(ValueError, match="instance of Stage"):
        get_pipeline().append("stage", object())


def test_async_run_error_is_delivered_by_get_item():
    """An error raised by the asynchronous run loop reaches get_item()."""
    pipeline = get_pipeline().append("broken", CustomizableBrokenStage([ValueError]))
    pipeline.process_async(text_item())
    with pytest.raises(ValueError, match="exception"):
        get_item_with_timeout(pipeline)


def test_process_after_stop_raises():
    """process() after stop() raises instead of returning the Stop signal."""
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    pipeline.process(text_item("hello"))
    pipeline.stop()
    with pytest.raises(PipelineStateError):
        pipeline.process(text_item("hello"))
    with pytest.raises(PipelineStateError):
        pipeline.process(text_item("hello"))
    pipeline.close()
