import copy
import logging
import time

import pytest

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
    TimeWaster,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

logger = logging.getLogger(__name__)


def _pipeline(*args, **kwargs):
    return Pipeline(*args, **kwargs).set_error_manager(
        ErrorManager().raise_on_critical_error()
    )


def test_run():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(35))
    pipeline.append_stage("reverser", BatchTextReverser())
    pipeline.append_stage("duplicator", TextDuplicator())
    pipeline.append_stage("batch_duplicator", BatchTextDuplicator())
    for item in pipeline.run():
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 3
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert pipeline.count == 35
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(40))
    pipeline.append_stage("reverser", TextReverser())
    pipeline.append_stage("duplicator", TextDuplicator())
    pipeline.append_stage("batch_duplicator", BatchTextDuplicator(check_batch=True))
    for _ in pipeline.run():
        pass
    assert pipeline.count == 40


def test_run_different_sizes():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(2))
    pipeline.append_stage("reverser", BatchTextReverser(size=4))
    pipeline.append_stage("duplicator", BatchTextDuplicator(size=20))
    n = 0
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert n == 1 == pipeline.count - 1
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(6))
    pipeline.append_stage("reverser", BatchTextReverser(size=1))
    pipeline.append_stage("duplicator", BatchTextDuplicator(size=20))
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.payload.keys() if x.startswith("text")]) == 2
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
    assert n == 5 == pipeline.count - 1


def test_errors(caplog):
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(RandomTextSource(22))
    pipeline.append_stage("reverser", BatchTextReverser(size=5))
    pipeline.append_stage("error", ErrorStage())
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error")
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == "test pipeline error"
    assert pipeline.count == 22
    assert any(caplog.records)
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(RandomTextSource(28))
    pipeline.append_stage("reverser", BatchTextReverser(size=8))
    pipeline.append_stage("error", ErrorStage())
    pipeline.append_stage("duplicator", BatchTextDuplicator(size=5))
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert item.get_timing("error")
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == "test pipeline error"
    assert pipeline.count == 28
    assert any(caplog.records)
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(RandomTextSource(10))
    pipeline.append_stage("reverser", BatchTextReverser(size=3))
    pipeline.append_stage("error1", ExceptionStage())
    pipeline.append_stage("error2", ErrorStage())
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1") >= 0.3
        assert not item.get_timing("error2")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert (
                str(error) == "test pipeline critical error"
                or str(error) == "test exception"
            )
    assert pipeline.count == 10
    assert any(caplog.records)
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(RandomTextSource(10))
        pipeline.append_stage("reverser", BatchTextReverser(size=4))
        pipeline.append_stage("error", ExceptionStage())
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager().no_skip_on_critical_error())
    pipeline.set_source(RandomTextSource(10))
    pipeline.append_stage("reverser1", BatchTextReverser(size=1))
    pipeline.append_stage("error", ExceptionStage())
    pipeline.append_stage("reverser2", BatchTextReverser(size=4))
    for item in pipeline.run():
        assert item.get_timing("reverser1")
        assert item.get_timing("error")
        assert item.get_timing("reverser2")
    assert pipeline.count == 10


def test_batch_errors(caplog):
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(RandomTextSource(22))
    pipeline.append_stage("reverser", BatchTextReverser(size=5))
    pipeline.append_stage("error", BatchErrorStage(size=3))
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error")
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == "test pipeline error"
    assert pipeline.count == 22
    assert any(caplog.records)
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(RandomTextSource(28))
    pipeline.append_stage("reverser", BatchTextReverser(size=8))
    pipeline.append_stage("error", BatchErrorStage(size=7))
    pipeline.append_stage("duplicator", BatchTextDuplicator(size=5))
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert item.get_timing("error")
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == "test pipeline error"
    assert pipeline.count == 28
    assert any(caplog.records)
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(RandomTextSource(10))
    pipeline.append_stage("reverser", BatchTextReverser(size=3))
    pipeline.append_stage("error1", BatchExceptionStage(size=7))
    pipeline.append_stage("error2", BatchErrorStage(size=1))
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing("reverser")
        assert item.get_timing("error1") >= 0.3
        assert not item.get_timing("error2")
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert (
                str(error) == "test pipeline critical error"
                or str(error) == "test exception"
            )
    assert pipeline.count == 10
    assert any(caplog.records)
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(RandomTextSource(10))
        pipeline.append_stage("reverser", BatchTextReverser(size=4))
        pipeline.append_stage("error", BatchExceptionStage(size=3))
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1


def _check(items, num, pipeline=None):
    diff = frozenset(range(1, num + 1)).difference(
        item.payload["count"] for item in items
    )
    assert not diff, "Not found items: {}".format(", ".join(str(x) for x in diff))
    if pipeline:
        assert len(items) == num == pipeline.count


def test_concurrent_run():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(96))
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=2)
    pipeline.append_stage("reverser1", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 96, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(81))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=2, use_threads=False
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(), concurrency=1, use_threads=False
    )
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=0)
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(), concurrency=2, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 81, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(92))
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("reverser1", BatchTextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 92, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(83))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=1, use_threads=False
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(), concurrency=1, use_threads=True
    )
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(), concurrency=1, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 83, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(101))
    pipeline.append_stage("duplicator0", BatchTextDuplicator(), concurrency=0)
    pipeline.append_stage("reverser", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("duplicator1", BatchTextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 101, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage("reverser", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("duplicator0", BatchTextDuplicator(), concurrency=0)
    pipeline.append_stage(
        "duplicator1", BatchTextDuplicator(check_batch=True), concurrency=1
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_queue_sizes():
    pipeline = _pipeline(max_queues_size=1)
    pipeline.set_source(RandomTextSource(93))
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=2)
    pipeline.append_stage("reverser1", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 93, pipeline)
    pipeline = _pipeline(max_queues_size=1)
    pipeline.set_source(RandomTextSource(79))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=2, use_threads=False
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(), concurrency=1, use_threads=False
    )
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=0)
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(), concurrency=2, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 79, pipeline)
    pipeline = _pipeline(max_queues_size=2)
    pipeline.set_source(RandomTextSource(131))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=2, use_threads=False
    )
    pipeline.append_stage("waster1", TimeWaster(0.02), concurrency=2, use_threads=False)
    pipeline.append_stage("reverser1", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("waster2", TimeWaster(0.03), concurrency=2)
    items = list(pipeline.run())
    _check(items, 131, pipeline)
    pipeline = _pipeline(max_queues_size=0)
    pipeline.set_source(RandomTextSource(4))
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=2)
    pipeline.append_stage("reverser1", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 4, pipeline)
    pipeline = _pipeline(max_queues_size=0)
    pipeline.set_source(RandomTextSource(11))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=2, use_threads=False
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(), concurrency=1, use_threads=False
    )
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=0)
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(), concurrency=2, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 11, pipeline)


def test_mixed_concurrent_run():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(96))
    pipeline.append_stage("reverser0", TextReverser(), concurrency=2)
    pipeline.append_stage("reverser1", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("reverser2", TextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 96, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(81))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=2, use_threads=False
    )
    pipeline.append_stage("reverser1", TextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage("reverser2", TextReverser(), concurrency=0)
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(), concurrency=2, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 81, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(92))
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=0)
    pipeline.append_stage("reverser1", TextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 92, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(83))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(), concurrency=1, use_threads=False
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(), concurrency=1, use_threads=True
    )
    pipeline.append_stage(
        "duplicator", TextDuplicator(), concurrency=1, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 83, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage("duplicator0", TextDuplicator(), concurrency=0)
    pipeline.append_stage("reverser", TextReverser(), concurrency=0)
    pipeline.append_stage(
        "duplicator1", BatchTextDuplicator(check_batch=True), concurrency=0
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage("duplicator0", TextDuplicator(), concurrency=2)
    pipeline.append_stage("reverser", TextReverser(), concurrency=2)
    pipeline.append_stage("duplicator1", BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_concurrency_errors():
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(RandomTextSource(29))
        pipeline.append_stage("reverser", BatchTextReverser(size=7), concurrency=1)
        pipeline.append_stage("error", ExceptionStage(), concurrency=1)
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(RandomTextSource(10))
        pipeline.append_stage(
            "reverser", BatchTextReverser(size=3), concurrency=1, use_threads=False
        )
        pipeline.append_stage(
            "error", ExceptionStage(), concurrency=1, use_threads=False
        )
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(29))
    pipeline.append_stage("reverser", BatchTextReverser(size=3), concurrency=3)
    pipeline.append_stage("error", ErrorStage(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(size=7), concurrency=2)
    for item in pipeline.run():
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert item.get_timing("error")
    assert pipeline.count == 29
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(RandomTextSource(29))
        pipeline.append_stage("reverser", BatchTextReverser(size=7), concurrency=1)
        pipeline.append_stage("error", BatchExceptionStage(size=3), concurrency=1)
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(RandomTextSource(10))
        pipeline.append_stage(
            "reverser", BatchTextReverser(size=3), concurrency=1, use_threads=False
        )
        pipeline.append_stage(
            "error", BatchExceptionStage(size=1), concurrency=1, use_threads=False
        )
        for _ in pipeline.run():
            pass
        assert pipeline.count == 1
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(29))
    pipeline.append_stage("reverser", BatchTextReverser(size=3), concurrency=3)
    pipeline.append_stage("error", BatchErrorStage(size=13), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(size=7), concurrency=2)
    for item in pipeline.run():
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert item.get_timing("error")
    assert pipeline.count == 29


def test_concurrent_initialization():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage_concurrently(
        "reverser0", BatchTextReverser, kwargs={"cycles": 3}, concurrency=2
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[5], concurrency=0
    )
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage_concurrently(
        "reverser0", BatchTextReverser, concurrency=2, use_threads=False
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[10], concurrency=1, use_threads=False
    )
    pipeline.append_stage_concurrently("reverser2", BatchTextReverser, concurrency=0)
    pipeline.append_stage_concurrently(
        "duplicator", BatchTextDuplicator, args=[10], concurrency=2, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=0)
    pipeline.append_stage_concurrently("reverser1", BatchTextReverser, concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(10), concurrency=0)
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage_concurrently(
        "duplicator0", BatchTextDuplicator, concurrency=0
    )
    pipeline.append_stage("reverser", BatchTextReverser(), concurrency=0)
    pipeline.append_stage_concurrently(
        "duplicator1", BatchTextDuplicator, concurrency=0
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage("reverser", BatchTextReverser(12), concurrency=0)
    pipeline.append_stage_concurrently(
        "duplicator0", BatchTextDuplicator, concurrency=0
    )
    pipeline.append_stage("duplicator1", BatchTextDuplicator(), concurrency=1)
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = _pipeline(max_init_workers=1)
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage_concurrently(
        "reverser0", BatchTextReverser, args=[20], concurrency=1, use_threads=False
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[20], concurrency=1, use_threads=True
    )
    pipeline.append_stage_concurrently(
        "duplicator", BatchTextDuplicator, args=[20], concurrency=1, use_threads=False
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_huge_run():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(200))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(10000), concurrency=2, use_threads=False
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(10000), concurrency=3, use_threads=False
    )
    pipeline.append_stage(
        "reverser2", BatchTextReverser(10000), concurrency=1, use_threads=False
    )
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(10000), concurrency=2, use_threads=False
    )
    start_time = time.time()
    items = list(pipeline.run())
    elapsed1 = time.time() - start_time
    logger.debug("Time for parallel: {}".format(elapsed1))
    _check(items, 200)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(200))
    pipeline.append_stage("reverser0", BatchTextReverser(10000), concurrency=0)
    pipeline.append_stage("reverser1", BatchTextReverser(10000), concurrency=0)
    pipeline.append_stage("reverser2", BatchTextReverser(10000), concurrency=0)
    pipeline.append_stage("duplicator", BatchTextDuplicator(10000), concurrency=0)
    start_time = time.time()
    items = list(pipeline.run())
    elapsed2 = time.time() - start_time
    logger.debug("Time for sequential: {}".format(elapsed2))
    _check(items, 200)
    assert elapsed2 > elapsed1


def test_timeouts():
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(timeout=1, size=120), concurrency=0
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(timeout=1, size=120), concurrency=0
    )
    pipeline.append_stage(
        "reverser2", BatchTextReverser(timeout=0, size=120), concurrency=0
    )
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(timeout=1, size=120), concurrency=0
    )
    start_time = time.time()
    items = list(pipeline.run())
    elapsed = time.time() - start_time
    assert 3 <= round(elapsed)
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage(
        "reverser0", BatchTextReverser(timeout=1, size=120), concurrency=1
    )
    pipeline.append_stage(
        "reverser1", BatchTextReverser(timeout=2, size=120), concurrency=1
    )
    pipeline.append_stage(
        "reverser2", BatchTextReverser(timeout=0, size=120), concurrency=1
    )
    pipeline.append_stage(
        "duplicator", BatchTextDuplicator(timeout=1, size=120), concurrency=1
    )
    start_time = time.time()
    items = list(pipeline.run())
    elapsed = time.time() - start_time
    assert 4 <= round(elapsed)
    _check(items, 100, pipeline)
    pipeline = _pipeline()
    pipeline.set_source(RandomTextSource(100))
    pipeline.append_stage(
        "reverser0",
        BatchTextReverser(timeout=1, size=120),
        concurrency=1,
        use_threads=False,
    )
    pipeline.append_stage(
        "reverser1",
        BatchTextReverser(timeout=0, size=120),
        concurrency=1,
        use_threads=False,
    )
    pipeline.append_stage(
        "reverser2",
        BatchTextReverser(timeout=1, size=120),
        concurrency=1,
        use_threads=False,
    )
    pipeline.append_stage(
        "duplicator",
        BatchTextDuplicator(timeout=0, size=120),
        concurrency=1,
        use_threads=False,
    )
    start_time = time.time()
    items = list(pipeline.run())
    elapsed = time.time() - start_time
    assert 2 <= round(elapsed)
    _check(items, 100, pipeline)


def test_single_items(items_generator_fx):
    pipeline = _pipeline()
    pipeline.append_stage("reverser0", BatchTextReverser())
    pipeline.append_stage("reverser1", BatchTextReverser())
    pipeline.append_stage("reverser2", BatchTextReverser())
    pipeline.append_stage("duplicator", BatchTextDuplicator())
    item = next(items_generator_fx)
    for _ in range(88):
        pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = _pipeline()
    pipeline.append_stage_concurrently(
        "reverser0", BatchTextReverser, kwargs={"cycles": 3}, concurrency=2
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[5], concurrency=0
    )
    pipeline.append_stage("reverser2", BatchTextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = _pipeline()
    pipeline.append_stage_concurrently(
        "reverser0", BatchTextReverser, concurrency=2, use_threads=False
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[9], concurrency=1, use_threads=False
    )
    pipeline.append_stage_concurrently("reverser2", BatchTextReverser, concurrency=0)
    pipeline.append_stage_concurrently(
        "duplicator", BatchTextDuplicator, args=[10], concurrency=2, use_threads=False
    )
    item = next(items_generator_fx)
    for _ in range(88):
        pipeline.process_async(item)
    for _ in range(88):
        result = pipeline.get_item()
        assert result.id == item.id
        assert result.payload["text"]
        assert result.payload["text"] != item.payload["text"]
    pipeline.stop()

    pipeline = _pipeline()
    pipeline.append_stage("reverser0", BatchTextReverser(), concurrency=0)
    pipeline.append_stage_concurrently("reverser1", BatchTextReverser, concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(10), concurrency=0)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] == item.payload["text"]

    pipeline = _pipeline()
    pipeline.append_stage_concurrently("duplicator0", BatchTextDuplicator)
    pipeline.append_stage("reverser", BatchTextReverser())
    pipeline.append_stage_concurrently("duplicator1", BatchTextDuplicator)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = _pipeline()
    pipeline.append_stage("reverser", BatchTextReverser(11), concurrency=0)
    pipeline.append_stage_concurrently(
        "duplicator0", BatchTextDuplicator, concurrency=0
    )
    pipeline.append_stage("duplicator1", BatchTextDuplicator(), concurrency=1)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = _pipeline(max_init_workers=1)
    pipeline.append_stage_concurrently(
        "reverser0", BatchTextReverser, args=[20], concurrency=1, use_threads=False
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[20], concurrency=1, use_threads=True
    )
    pipeline.append_stage_concurrently(
        "duplicator", BatchTextDuplicator, args=[20], concurrency=1, use_threads=False
    )
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] == item.payload["text"]
    assert len(result.payload.keys()) > len(item.payload.keys())


def test_mixed_single_items(items_generator_fx):
    pipeline = _pipeline()
    pipeline.append_stage("reverser0", BatchTextReverser())
    pipeline.append_stage("reverser1", TextReverser())
    pipeline.append_stage("reverser2", BatchTextReverser())
    pipeline.append_stage("duplicator", TextDuplicator())
    item = next(items_generator_fx)
    for _ in range(88):
        pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = _pipeline()
    pipeline.append_stage_concurrently(
        "reverser0", TextReverser, kwargs={"cycles": 3}, concurrency=2
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[5], concurrency=0
    )
    pipeline.append_stage("reverser2", TextReverser(), concurrency=1)
    pipeline.append_stage("duplicator", BatchTextDuplicator(), concurrency=2)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = _pipeline()
    pipeline.append_stage_concurrently(
        "reverser0", TextReverser, concurrency=2, use_threads=False
    )
    pipeline.append_stage_concurrently(
        "reverser1", BatchTextReverser, args=[9], concurrency=1, use_threads=False
    )
    pipeline.append_stage_concurrently("reverser2", TextReverser, concurrency=0)
    pipeline.append_stage_concurrently(
        "duplicator", BatchTextDuplicator, args=[10], concurrency=2, use_threads=False
    )
    item = next(items_generator_fx)
    for _ in range(88):
        pipeline.process_async(item)
    for _ in range(88):
        result = pipeline.get_item()
        assert result.id == item.id
        assert result.payload["text"]
        assert result.payload["text"] != item.payload["text"]
    pipeline.stop()
