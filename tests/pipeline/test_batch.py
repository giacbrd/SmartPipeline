import copy
import logging

import pytest

from smartpipeline.error.exceptions import RetryError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.pipeline import Pipeline
from tests.utils import (
    BatchErrorStage,
    BatchExceptionStage,
    BatchTextDuplicator,
    BatchTextReverser,
    CustomException,
    CustomizableBrokenBatchStage,
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
        .set_source(RandomTextSource(35))
        .append("reverser", BatchTextReverser())
        .append("duplicator", TextDuplicator())
        .append("batch_duplicator", BatchTextDuplicator())
        .build()
    )
    for item in pipeline.run():
        assert len([x for x in item.data.keys() if x.startswith("text")]) == 3
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert pipeline.count == 35
    for _ in range(3):
        assert pipeline._containers["batch_duplicator"].get_processed() is None
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(40))
        .append("reverser", TextReverser())
        .append("duplicator", TextDuplicator())
        .append("batch_duplicator", BatchTextDuplicator(check_batch=True))
        .build()
    )
    for _ in pipeline.run():
        pass
    assert pipeline.count == 40


def test_run_different_sizes():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append("reverser", BatchTextReverser(size=4))
        .append("duplicator", BatchTextDuplicator(size=20))
        .build()
    )
    n = 0
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.data.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert n == 1 == pipeline.count - 1
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(6))
        .append("reverser", BatchTextReverser(size=1))
        .append("duplicator", BatchTextDuplicator(size=20))
        .build()
    )
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.data.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert n == 5 == pipeline.count - 1


def test_errors(caplog):
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(22))
        .append("reverser", BatchTextReverser(size=5))
        .append("error", ErrorStage())
        .build()
    )
    for item in pipeline.run():
        assert item.has_soft_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error")
        error = next(item.soft_errors())
        assert error.get_exception() is None
        assert str(error) == "test pipeline error"
    assert pipeline.count == 22
    assert all(
        "has generated an error" in record.msg.lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(28))
        .append("reverser", BatchTextReverser(size=8))
        .append("error", ErrorStage())
        .append("duplicator", BatchTextDuplicator(size=5))
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
    assert pipeline.count == 28
    assert any(caplog.records)
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append("reverser", BatchTextReverser(size=3))
        .append("error1", ExceptionStage())
        .append("error2", ErrorStage())
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
            .append("reverser", BatchTextReverser(size=4))
            .append("error", ExceptionStage())
            .build()
        )
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    pipeline = (
        get_pipeline()
        .set_error_manager(ErrorManager().no_skip_on_critical_error())
        .set_source(RandomTextSource(10))
        .append("reverser1", BatchTextReverser(size=1))
        .append("error", ExceptionStage())
        .append("reverser2", BatchTextReverser(size=4))
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
        .append("reverser", BatchTextReverser(size=5))
        .append("error", BatchErrorStage(size=3))
        .build()
    )
    for item in pipeline.run():
        assert item.has_soft_errors()
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
        .append("reverser", BatchTextReverser(size=8))
        .append("error", BatchErrorStage(size=7))
        .append("duplicator", BatchTextDuplicator(size=5))
        .build()
    )
    for item in pipeline.run():
        assert item.has_soft_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.data.keys())
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
        .append("reverser", BatchTextReverser(size=3))
        .append("error1", BatchExceptionStage(size=7))
        .append("error2", BatchErrorStage(size=1))
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
            .append("reverser", BatchTextReverser(size=4))
            .append("error", BatchExceptionStage(size=3))
            .build()
        )
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1


def test_retryable_batch_stages(items_generator_fx):
    pipeline = (
        Pipeline()
        .set_source(RandomTextSource(10))
        .append("reverser0", TextReverser())
        .append(
            "broken_batch_stage1",
            CustomizableBrokenBatchStage(
                size=10, timeout=5, exceptions_to_raise=[CustomException, ValueError]
            ),
            backoff=0.01,
            max_retries=1,
            retryable_errors=(CustomException, ValueError),
        )
        .append(
            "broken_batch_stage2",
            CustomizableBrokenBatchStage(
                size=10, timeout=5, exceptions_to_raise=[IOError]
            ),
            backoff=0,
            max_retries=0,
            retryable_errors=(IOError,),
        )
        .append("reverser1", TextReverser())
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process(copy.deepcopy(item))
    items = list(pipeline.run())
    for item in items:
        assert not item.has_critical_errors()
        assert all(
            isinstance(err, RetryError)
            and isinstance(err.get_exception(), (CustomException, ValueError, IOError))
            and err.get_stage() == "broken_batch_stage1"
            or err.get_stage() == "broken_batch_stage2"
            for err in item.soft_errors()
        )
