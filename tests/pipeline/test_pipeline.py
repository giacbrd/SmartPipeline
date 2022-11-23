import logging
import traceback

import pytest

from smartpipeline.error.exceptions import RetryError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.pipeline import Pipeline
from tests.utils import (
    CriticalIOErrorStage,
    CustomException,
    CustomizableBrokenStage,
    ErrorStage,
    ExceptionStage,
    RandomTextSource,
    TextDuplicator,
    TextReverser,
    get_pipeline,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

logger = logging.getLogger(__name__)


def test_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("duplicator", TextDuplicator())
        .build()
    )
    assert pipeline.name.startswith("Pipeline-")
    assert str(pipeline._source_container)
    assert isinstance(pipeline.get_stage("duplicator"), TextDuplicator)
    for container in pipeline._containers.values():
        assert container.name
        assert str(container)
    for item in pipeline.run():
        assert len([x for x in item.data.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert pipeline.count == 10
    with pytest.raises(ValueError):
        Pipeline().append("reverser", TextReverser()).append("reverser", TextReverser())


def test_errors(caplog):
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error", ErrorStage())
        .build()
    )
    error_manager = ErrorManager()
    pipeline.set_error_manager(error_manager)
    assert all(c.error_manager == error_manager for c in pipeline._containers.values())
    for item in pipeline.run():
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
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error", ErrorStage())
        .append("duplicator", TextDuplicator())
        .build()
    )
    caplog.clear()
    for item in pipeline.run():
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
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error1", CriticalIOErrorStage())
        .build()
    )
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), IOError)
            assert str(error) == "test pipeline critical IO error"
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", TextReverser())
        .append("error1", ExceptionStage())
        .append("error2", ErrorStage())
        .build()
    )
    caplog.clear()
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1")
        assert not item.get_timing("error2")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert (
                str(error) == "test exception"
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
    with pytest.raises(Exception):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append("reverser", TextReverser())
            .append("error", ExceptionStage())
            .build()
        )
        try:
            for _ in pipeline.run():
                pass
        except Exception:
            assert 'Exception("test exception")' in traceback.format_exc()
            raise
        assert pipeline.count == 1
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager().no_skip_on_critical_error())
        .set_source(RandomTextSource(10))
        .append("reverser1", TextReverser())
        .append("error", ExceptionStage())
        .append("reverser2", TextReverser())
        .build()
    )
    for item in pipeline.run():
        assert item.get_timing("reverser1")
        assert item.get_timing("error")
        assert item.get_timing("reverser2")
    assert pipeline.count == 10


def test_retryable_stage(items_generator_fx):
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
    assert len(soft_errors) == 2 and all(
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
    assert len(soft_errors) == 2 and all(
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
    assert len(soft_errors) == 3
    assert all(
        isinstance(err, RetryError) and err.get_stage() == "broken_stage"
        for err in soft_errors
    )
    assert isinstance(soft_errors[0].get_exception(), CustomException) and isinstance(
        soft_errors[2].get_exception(), CustomException
    )
    assert isinstance(soft_errors[1].get_exception(), ValueError)


def test_exponential_backoff_retry_strategy(items_generator_fx):
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
    assert 15 <= item.get_timing("broken_stage") <= 16
    soft_errors = list(item.soft_errors())
    assert len(soft_errors) == 4
    assert all(
        isinstance(err, RetryError) and isinstance(err.get_exception(), CustomException)
        for err in soft_errors
    )
