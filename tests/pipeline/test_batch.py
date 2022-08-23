import copy
import logging

import pytest

from smartpipeline.error.exceptions import RetryError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.pipeline import Pipeline
from tests.utils import (
    RandomTextSource,
    TextDuplicator,
    TextReverser,
    ErrorStage,
    ExceptionStage,
    BatchTextReverser,
    BatchTextDuplicator,
    BatchExceptionStage,
    BatchErrorStage,
    CustomizableBrokenBatchStage,
    CustomException,
    get_pipeline,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

logger = logging.getLogger(__name__)


def test_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(35))
        .append_stage("reverser", BatchTextReverser())
        .append_stage("duplicator", TextDuplicator())
        .append_stage("batch_duplicator", BatchTextDuplicator())
        .build()
    )
    for item in pipeline.run():
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 3
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert pipeline.count == 35
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(40))
        .append_stage("reverser", TextReverser())
        .append_stage("duplicator", TextDuplicator())
        .append_stage("batch_duplicator", BatchTextDuplicator(check_batch=True))
        .build()
    )
    for _ in pipeline.run():
        pass
    assert pipeline.count == 40


def test_run_different_sizes():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_stage("reverser", BatchTextReverser(size=4))
        .append_stage("duplicator", BatchTextDuplicator(size=20))
        .build()
    )
    n = 0
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert n == 1 == pipeline.count - 1
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(6))
        .append_stage("reverser", BatchTextReverser(size=1))
        .append_stage("duplicator", BatchTextDuplicator(size=20))
        .build()
    )
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert n == 5 == pipeline.count - 1


def test_errors(caplog):
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(22))
        .append_stage("reverser", BatchTextReverser(size=5))
        .append_stage("error", ErrorStage())
        .build()
    )
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert pipeline.count == 22
    assert all(
        "stage error has generated an error" in record.msg.lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(28))
        .append_stage("reverser", BatchTextReverser(size=8))
        .append_stage("error", ErrorStage())
        .append_stage("duplicator", BatchTextDuplicator(size=5))
        .build()
    )
    caplog.clear()
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert pipeline.count == 28
    assert any(caplog.records)
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append_stage("reverser", BatchTextReverser(size=3))
        .append_stage("error1", ExceptionStage())
        .append_stage("error2", ErrorStage())
        .build()
    )
    caplog.clear()
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1") >= 0.0003
        assert not item.get_timing("error2")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert (
                str(error.get_exception()) == "test exception"
                and str(error) != "test pipeline error"
            )
    assert pipeline.count == 10
    assert any(caplog.records)
    with pytest.raises(Exception):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", BatchTextReverser(size=4))
            .append_stage("error", ExceptionStage())
            .build()
        )
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager().no_skip_on_critical_error())
        .set_source(RandomTextSource(10))
        .append_stage("reverser1", BatchTextReverser(size=1))
        .append_stage("error", ExceptionStage())
        .append_stage("reverser2", BatchTextReverser(size=4))
        .build()
    )
    for item in pipeline.run():
        assert item.get_timing("reverser1")
        assert item.get_timing("error")
        assert item.get_timing("reverser2")
    assert pipeline.count == 10


def test_batch_errors(caplog):
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(22))
        .append_stage("reverser", BatchTextReverser(size=5))
        .append_stage("error", BatchErrorStage(size=3))
        .build()
    )
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert pipeline.count == 22
    assert any(caplog.records)
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(28))
        .append_stage("reverser", BatchTextReverser(size=8))
        .append_stage("error", BatchErrorStage(size=7))
        .append_stage("duplicator", BatchTextDuplicator(size=5))
        .build()
    )
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert pipeline.count == 28
    assert any(caplog.records)
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append_stage("reverser", BatchTextReverser(size=3))
        .append_stage("error1", BatchExceptionStage(size=7))
        .append_stage("error2", BatchErrorStage(size=1))
        .build()
    )
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1") >= 0.0003
        assert not item.get_timing("error2")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert (
                str(error.get_exception()) == "test exception"
                and str(error) != "test pipeline error"
            )
    assert pipeline.count == 10
    assert any(caplog.records)
    with pytest.raises(Exception):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", BatchTextReverser(size=4))
            .append_stage("error", BatchExceptionStage(size=3))
            .build()
        )
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1


def test_retryable_batch_stages(items_generator_fx):
    pipeline = (
        Pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("reverser0", TextReverser())
        .append_stage(
            "broken_batch_stage",
            CustomizableBrokenBatchStage(
                size=10, timeout=5, exceptions_to_raise=[CustomException, ValueError]
            ),
            backoff=0.01,
            max_retries=1,
            retryable_errors=(CustomException, ValueError),
        )
        .append_stage("reverser1", TextReverser())
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process(copy.deepcopy(item))
    items = list(pipeline.run())
    for item in items:
        assert not item.has_critical_errors()
        assert all(
            isinstance(err, RetryError)
            and isinstance(err.get_exception(), (CustomException, ValueError))
            and err.get_stage() == "broken_batch_stage"
            for err in item.soft_errors()
        )