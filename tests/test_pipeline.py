import copy
import logging
import time
import traceback

import pytest

from smartpipeline.error.handling import ErrorManager
from smartpipeline.pipeline import Pipeline
from tests.utils import (
    RandomTextSource,
    TextDuplicator,
    TextReverser,
    ErrorStage,
    ExceptionStage,
    TimeWaster,
    SerializableStage,
    CriticalIOErrorStage,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

logger = logging.getLogger(__name__)


def _pipeline(*args, **kwargs):
    return Pipeline(*args, **kwargs).set_error_manager(
        ErrorManager().raise_on_critical_error()
    )


def test_run():
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser())
        .append_stage("duplicator", TextDuplicator())
        .build()
    )
    for item in pipeline.run():
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert pipeline.count == 10


def test_errors(caplog):
    pipeline = (
        _pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser())
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
    assert all(
        "stage error has generated an error" in record.msg.lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    assert pipeline.count == 10
    pipeline = (
        _pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser())
        .append_stage("error", ErrorStage())
        .append_stage("duplicator", TextDuplicator())
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
    assert all(
        "stage error has generated an error" in record.msg.lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    assert pipeline.count == 10
    pipeline = (
        _pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser())
        .append_stage("error1", CriticalIOErrorStage())
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
        _pipeline()
        .set_error_manager(ErrorManager())
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser())
        .append_stage("error1", ExceptionStage())
        .append_stage("error2", ErrorStage())
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
        "stage error1 has generated an error" in record.msg.lower()
        for record in caplog.records
        if record.levelno == logging.ERROR
    )
    assert pipeline.count == 10
    with pytest.raises(Exception):
        pipeline = (
            _pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", TextReverser())
            .append_stage("error", ExceptionStage())
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
        _pipeline()
        .set_error_manager(ErrorManager().no_skip_on_critical_error())
        .set_source(RandomTextSource(10))
        .append_stage("reverser1", TextReverser())
        .append_stage("error", ExceptionStage())
        .append_stage("reverser2", TextReverser())
        .build()
    )
    for item in pipeline.run():
        assert item.get_timing("reverser1")
        assert item.get_timing("error")
        assert item.get_timing("reverser2")
    assert pipeline.count == 10


def _check(items, num, pipeline=None):
    diff = frozenset(range(1, num + 1)).difference(
        item.payload["count"] for item in items
    )
    assert not diff, "Not found items: {}".format(", ".join(str(x) for x in diff))
    assert len(items) == num
    if pipeline:
        assert num == pipeline.count


def test_concurrent_run():
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage("reverser1", TextReverser(), concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=0)
        .append_stage("duplicator", TextDuplicator(), concurrency=2, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=0)
        .append_stage("reverser1", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=False)
        .append_stage("duplicator", TextDuplicator(), concurrency=1, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("duplicator0", TextDuplicator(), concurrency=0)
        .append_stage("reverser", TextReverser(), concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser", TextReverser(), concurrency=0)
        .append_stage("duplicator0", TextDuplicator(), concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=1)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_queue_sizes():
    pipeline = (
        _pipeline(max_queues_size=1)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage("reverser1", TextReverser(), concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline(max_queues_size=1)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=0)
        .append_stage("duplicator", TextDuplicator(), concurrency=2, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline(max_queues_size=3)
        .set_source(RandomTextSource(100))
        .append_stage("waster", TimeWaster(0.02), concurrency=2)
        .append_stage("reverser", TimeWaster(0.04), concurrency=1, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline(max_queues_size=0)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage("reverser1", TextReverser(), concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline(max_queues_size=0)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=0)
        .append_stage("duplicator", TextDuplicator(), concurrency=2, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_concurrency_errors():
    with pytest.raises(Exception):
        pipeline = (
            _pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", TextReverser(), concurrency=1)
            .append_stage("error", ExceptionStage(), concurrency=1)
            .build()
        )
        try:
            for _ in pipeline.run():
                pass
        except Exception:
            tb = traceback.format_exc()
            assert 'Exception("test exception")' in tb
            assert "CriticalError" in tb
            raise
        assert pipeline.count == 1
    with pytest.raises(Exception):
        pipeline = (
            _pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", TextReverser(), concurrency=1, parallel=True)
            .append_stage("error", ExceptionStage(), concurrency=1, parallel=True)
            .build()
        )
        try:
            for _ in pipeline.run():
                pass
        except Exception:
            tb = traceback.format_exc()
            assert 'Exception("test exception")' in tb
            assert "CriticalError" in tb
            raise
        assert pipeline.count == 1
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser(), concurrency=1)
        .append_stage("error1", ErrorStage(), concurrency=1, parallel=True)
        .append_stage("error2", ErrorStage(), concurrency=2)
        .append_stage("duplicator", TextDuplicator(), concurrency=1, parallel=True)
        .build()
    )
    for item in pipeline.run():
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert len(list(item.soft_errors())) == 2
        assert item.get_timing("error1")
        assert item.get_timing("error2")
    assert pipeline.count == 10


def test_concurrent_constructions():
    """test `on_fork` method"""
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("error", SerializableStage(), concurrency=2, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=1, parallel=True)
        .build()
    )
    for item in pipeline.run():
        assert item.payload.get("file")
        assert item.get_timing("reverser1")
        assert item.get_timing("reverser2")
    assert pipeline.count == 10


def test_concurrent_initialization():
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage_concurrently(
            "reverser0", TextReverser, kwargs={"cycles": 3}, concurrency=2
        )
        .append_stage_concurrently("reverser1", TextReverser, args=[5], concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage_concurrently(
            "reverser0", TextReverser, concurrency=2, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[10], concurrency=1, parallel=True
        )
        .append_stage_concurrently("reverser2", TextReverser, concurrency=0)
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[10], concurrency=2, parallel=True
        )
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=0)
        .append_stage_concurrently("reverser1", TextReverser, concurrency=1)
        .append_stage("duplicator", TextDuplicator(10), concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage_concurrently("duplicator0", TextDuplicator, concurrency=0)
        .append_stage("reverser", TextReverser(), concurrency=0)
        .append_stage_concurrently("duplicator1", TextDuplicator, concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser", TextReverser(12), concurrency=0)
        .append_stage_concurrently("duplicator0", TextDuplicator, concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=1)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        _pipeline(max_init_workers=1)
        .set_source(RandomTextSource(100))
        .append_stage_concurrently(
            "reverser0", TextReverser, args=[20], concurrency=1, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[20], concurrency=1, parallel=False
        )
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[20], concurrency=1, parallel=True
        )
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


# one core machines can have problems with this test
# TODO it has problems also on dual core machine
@pytest.mark.skip
def test_huge_run():
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(200))
        .append_stage("reverser0", TextReverser(15000), concurrency=4, parallel=True)
        .append_stage("reverser1", TextReverser(15000), concurrency=4, parallel=True)
        .append_stage("reverser2", TextReverser(15000), concurrency=4, parallel=True)
        .append_stage("duplicator", TextDuplicator(15000), concurrency=4, parallel=True)
        .build()
    )
    runner = pipeline.run()
    start_time = time.time()
    items = list(runner)
    elapsed1 = time.time() - start_time
    logger.debug("Time for strongly parallel: {}".format(elapsed1))
    _check(items, 200)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(200))
        .append_stage("reverser0", TextReverser(15000), concurrency=1, parallel=True)
        .append_stage("reverser1", TextReverser(15000), concurrency=2, parallel=True)
        .append_stage("reverser2", TextReverser(15000), concurrency=1, parallel=True)
        .append_stage("duplicator", TextDuplicator(15000), concurrency=1, parallel=True)
        .build()
    )
    runner = pipeline.run()
    start_time = time.time()
    items = list(runner)
    elapsed2 = time.time() - start_time
    logger.debug("Time for mildly parallel: {}".format(elapsed2))
    _check(items, 200)
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(200))
        .append_stage("reverser0", TextReverser(15000), concurrency=0)
        .append_stage("reverser1", TextReverser(15000), concurrency=0)
        .append_stage("reverser2", TextReverser(15000), concurrency=0)
        .append_stage("duplicator", TextDuplicator(15000), concurrency=0)
        .build()
    )
    runner = pipeline.run()
    start_time = time.time()
    items = list(runner)
    elapsed3 = time.time() - start_time
    logger.debug("Time for sequential: {}".format(elapsed3))
    _check(items, 200)
    assert elapsed3 > elapsed1
    assert elapsed3 > elapsed2
    assert elapsed2 > elapsed1


def test_run_times():
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("waster0", TimeWaster(0.2), concurrency=1)
        .append_stage("waster1", TimeWaster(0.2), concurrency=1)
        .append_stage("waster2", TimeWaster(0.2), concurrency=1)
        .append_stage("waster3", TimeWaster(0.2), concurrency=1)
        .build()
    )
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elapsed0 = time.time() - start_time
    logger.debug("Time for multi-threading: {}".format(elapsed0))
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("waster0", TimeWaster(0.2), concurrency=1, parallel=True)
        .append_stage("waster1", TimeWaster(0.2), concurrency=1, parallel=True)
        .append_stage("waster2", TimeWaster(0.2), concurrency=1, parallel=True)
        .append_stage("waster3", TimeWaster(0.2), concurrency=1, parallel=True)
        .build()
    )
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elapsed1 = time.time() - start_time
    logger.debug("Time for multi-process: {}".format(elapsed1))
    pipeline = (
        _pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("waster0", TimeWaster(0.2), concurrency=0)
        .append_stage("waster1", TimeWaster(0.2), concurrency=0)
        .append_stage("waster2", TimeWaster(0.2), concurrency=0)
        .append_stage("waster3", TimeWaster(0.2), concurrency=0)
        .build()
    )
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elapsed2 = time.time() - start_time
    logger.debug("Time for sequential: {}".format(elapsed2))
    assert elapsed2 > elapsed0
    assert elapsed2 > elapsed1


def test_single_items(items_generator_fx):
    pipeline = (
        _pipeline()
        .append_stage("reverser0", TextReverser())
        .append_stage("reverser1", TextReverser())
        .append_stage("reverser2", TextReverser())
        .append_stage("duplicator", TextDuplicator())
        .build()
    )
    item = next(items_generator_fx)
    result = pipeline.process(copy.deepcopy(item))
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        _pipeline()
        .append_stage_concurrently(
            "reverser0", TextReverser, kwargs={"cycles": 3}, concurrency=2
        )
        .append_stage_concurrently("reverser1", TextReverser, args=[5], concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        _pipeline()
        .append_stage_concurrently(
            "reverser0", TextReverser, concurrency=2, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[9], concurrency=1, parallel=True
        )
        .append_stage_concurrently("reverser2", TextReverser, concurrency=0)
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[10], concurrency=2, parallel=True
        )
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        _pipeline()
        .append_stage("reverser0", TextReverser(), concurrency=0)
        .append_stage_concurrently("reverser1", TextReverser, concurrency=1)
        .append_stage("duplicator", TextDuplicator(10), concurrency=0)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] == item.payload["text"]

    pipeline = (
        _pipeline()
        .append_stage_concurrently("duplicator0", TextDuplicator)
        .append_stage("reverser", TextReverser())
        .append_stage_concurrently("duplicator1", TextDuplicator)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        _pipeline()
        .append_stage("reverser", TextReverser(11), concurrency=0)
        .append_stage_concurrently("duplicator0", TextDuplicator, concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=1)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        _pipeline(max_init_workers=1)
        .append_stage_concurrently(
            "reverser0", TextReverser, args=[20], concurrency=1, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[20], concurrency=1, parallel=False
        )
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[20], concurrency=1, parallel=True
        )
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] == item.payload["text"]
    assert len(result.payload.keys()) > len(item.payload.keys())
