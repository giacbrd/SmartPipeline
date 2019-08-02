import copy
import logging
import time

import pytest

from smartpipeline.error import ErrorManager
from smartpipeline.pipeline import Pipeline
from tests.utils import FakeSource, TextDuplicator, TextReverser, ErrorStage, ExceptionStage, \
    BatchTextReverser, BatchTextDuplicator

__author__ = 'Giacomo Berardi <giacbrd.com>'

logger = logging.getLogger(__name__)


def _pipeline():
    return Pipeline().set_error_manager(ErrorManager().raise_on_critical_error())


def test_run():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(35))
    pipeline.append_stage('reverser', BatchTextReverser())
    pipeline.append_stage('duplicator', TextDuplicator())
    pipeline.append_stage('batch_duplicator', BatchTextDuplicator())
    for item in pipeline.run():
        assert len([x for x in item.payload.keys() if x.startswith('text')]) == 3
        assert item.get_timing('reverser')
        assert item.get_timing('duplicator')
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(40))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('duplicator', TextDuplicator())
    pipeline.append_stage('batch_duplicator', BatchTextDuplicator(check_batch=True))
    for _ in pipeline.run():
        pass


def test_run_different_sizes():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(2))
    pipeline.append_stage('reverser', BatchTextReverser(size=4))
    pipeline.append_stage('duplicator', BatchTextDuplicator(size=20))
    n = 0
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.payload.keys() if x.startswith('text')]) == 2
        assert item.get_timing('reverser')
        assert item.get_timing('duplicator')
    assert n == 1
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(6))
    pipeline.append_stage('reverser', BatchTextReverser(size=1))
    pipeline.append_stage('duplicator', BatchTextDuplicator(size=20))
    for n, item in enumerate(pipeline.run()):
        assert len([x for x in item.payload.keys() if x.startswith('text')]) == 2
        assert item.get_timing('reverser')
        assert item.get_timing('duplicator')
    assert n == 5


def test_error(caplog):
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(FakeSource(22))
    pipeline.append_stage('reverser', BatchTextReverser(size=5))
    pipeline.append_stage('error', ErrorStage())
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing('reverser')
        assert item.get_timing('error')
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == 'test pipeline error'
    assert any(caplog.records)
    pipeline = _pipeline()
    pipeline.set_error_manager(ErrorManager())
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', BatchTextReverser(size=3))
    pipeline.append_stage('error1', ExceptionStage())
    pipeline.append_stage('error2', ErrorStage())
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing('reverser')
        assert item.get_timing('error1') >= 0.3
        assert not item.get_timing('error2')
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert str(error) == 'test pipeline critical error' or str(error) == 'test exception'
    assert any(caplog.records)
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(FakeSource(10))
        pipeline.append_stage('reverser', TextReverser())
        pipeline.append_stage('error2', ExceptionStage())
        for _ in pipeline.run():
            pass


def _check(items, num):
    diff = frozenset(range(1, num + 1)).difference(item.payload['count'] for item in items)
    assert not diff, 'Not found items: {}'.format(', '.join(str(x) for x in diff))


def test_concurrent_run():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(96))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=2)
    pipeline.append_stage('reverser1', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('reverser2', BatchTextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 96)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(81))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=2, use_threads=False)
    pipeline.append_stage('reverser1', BatchTextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser2', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=2, use_threads=False)
    items = list(pipeline.run())
    _check(items, 81)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(92))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('reverser1', BatchTextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 92)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(83))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser1', BatchTextReverser(), concurrency=1, use_threads=True)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=1, use_threads=False)
    items = list(pipeline.run())
    _check(items, 83)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(101))
    pipeline.append_stage('duplicator0', BatchTextDuplicator(), concurrency=0)
    pipeline.append_stage('reverser', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('duplicator1', BatchTextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 101)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('duplicator0', BatchTextDuplicator(), concurrency=0)
    pipeline.append_stage('duplicator1', BatchTextDuplicator(check_batch=True), concurrency=1)
    items = list(pipeline.run())
    _check(items, 100)


def test_mixed_concurrent_run():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(96))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=2)
    pipeline.append_stage('reverser1', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 96)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(81))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=2, use_threads=False)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=0)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=2, use_threads=False)
    items = list(pipeline.run())
    _check(items, 81)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(92))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=0)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 92)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(83))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser1', BatchTextReverser(), concurrency=1, use_threads=True)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=1, use_threads=False)
    items = list(pipeline.run())
    _check(items, 83)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('duplicator0', TextDuplicator(), concurrency=0)
    pipeline.append_stage('reverser', TextReverser(), concurrency=0)
    pipeline.append_stage('duplicator1', BatchTextDuplicator(check_batch=True), concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('duplicator0', TextDuplicator(), concurrency=2)
    pipeline.append_stage('reverser', TextReverser(), concurrency=2)
    pipeline.append_stage('duplicator1', BatchTextDuplicator(check_batch=True), concurrency=2)
    items = list(pipeline.run())
    _check(items, 100)


def test_concurrency_errors():
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(FakeSource(10))
        pipeline.append_stage('reverser', BatchTextReverser(), concurrency=1)
        pipeline.append_stage('error2', ExceptionStage(), concurrency=1)
        for _ in pipeline.run():
            pass
    with pytest.raises(Exception):
        pipeline = _pipeline()
        pipeline.set_source(FakeSource(10))
        pipeline.append_stage('reverser', BatchTextReverser(), concurrency=1, use_threads=False)
        pipeline.append_stage('error2', ExceptionStage(), concurrency=1, use_threads=False)
        for _ in pipeline.run():
            pass


def test_concurrent_initialization():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('reverser0', BatchTextReverser, kwargs={'cycles': 3}, concurrency=2)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, args=[5], concurrency=0)
    pipeline.append_stage('reverser2', BatchTextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('reverser0', BatchTextReverser, concurrency=2, use_threads=False)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, args=[10], concurrency=1, use_threads=False)
    pipeline.append_stage_concurrently('reverser2', BatchTextReverser, concurrency=0)
    pipeline.append_stage_concurrently('duplicator', BatchTextDuplicator, args=[10], concurrency=2, use_threads=False)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=0)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(10), concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('duplicator0', BatchTextDuplicator, concurrency=0)
    pipeline.append_stage('reverser', BatchTextReverser(), concurrency=0)
    pipeline.append_stage_concurrently('duplicator1', BatchTextDuplicator, concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser', BatchTextReverser(12), concurrency=0)
    pipeline.append_stage_concurrently('duplicator0', BatchTextDuplicator, concurrency=0)
    pipeline.append_stage('duplicator1', BatchTextDuplicator(), concurrency=1)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = _pipeline().set_max_init_workers(1)
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('reverser0', BatchTextReverser, args=[20], concurrency=1, use_threads=False)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, args=[20], concurrency=1, use_threads=True)
    pipeline.append_stage_concurrently('duplicator', BatchTextDuplicator, args=[20], concurrency=1, use_threads=False)
    items = list(pipeline.run())
    _check(items, 100)


def test_huge_run():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(200))
    pipeline.append_stage('reverser0', BatchTextReverser(10000), concurrency=2, use_threads=False)
    pipeline.append_stage('reverser1', BatchTextReverser(10000), concurrency=3, use_threads=False)
    pipeline.append_stage('reverser2', BatchTextReverser(10000), concurrency=1, use_threads=False)
    pipeline.append_stage('duplicator', BatchTextDuplicator(10000), concurrency=2, use_threads=False)
    start_time = time.time()
    items = list(pipeline.run())
    elasped1 = time.time() - start_time
    logger.debug('Time for parallel: {}'.format(elasped1))
    _check(items, 200)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(200))
    pipeline.append_stage('reverser0', BatchTextReverser(10000), concurrency=0)
    pipeline.append_stage('reverser1', BatchTextReverser(10000), concurrency=0)
    pipeline.append_stage('reverser2', BatchTextReverser(10000), concurrency=0)
    pipeline.append_stage('duplicator', BatchTextDuplicator(10000), concurrency=0)
    start_time = time.time()
    items = list(pipeline.run())
    elasped2 = time.time() - start_time
    logger.debug('Time for sequential: {}'.format(elasped2))
    _check(items, 200)
    assert elasped2 > elasped1


def test_timeouts():
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', BatchTextReverser(timeout=2), concurrency=0)
    pipeline.append_stage('reverser1', BatchTextReverser(timeout=2), concurrency=0)
    pipeline.append_stage('reverser2', BatchTextReverser(timeout=2), concurrency=0)
    pipeline.append_stage('duplicator', BatchTextDuplicator(timeout=2), concurrency=0)
    start_time = time.time()
    items = list(pipeline.run())
    elasped0 = time.time() - start_time
    assert elasped0 < 8
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', BatchTextReverser(timeout=2), concurrency=1)
    pipeline.append_stage('reverser1', BatchTextReverser(timeout=2), concurrency=1)
    pipeline.append_stage('reverser2', BatchTextReverser(timeout=2), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(timeout=2), concurrency=1)
    start_time = time.time()
    items = list(pipeline.run())
    elasped0 = time.time() - start_time
    assert elasped0 < 8
    _check(items, 100)
    pipeline = _pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', BatchTextReverser(timeout=2), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser1', BatchTextReverser(timeout=2), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser2', BatchTextReverser(timeout=2), concurrency=1, use_threads=False)
    pipeline.append_stage('duplicator', BatchTextDuplicator(timeout=2), concurrency=1, use_threads=False)
    start_time = time.time()
    items = list(pipeline.run())
    elasped0 = time.time() - start_time
    assert elasped0 < 8
    _check(items, 100)


def test_single_items(items_generator_fx):
    pipeline = _pipeline()
    pipeline.append_stage('reverser0', BatchTextReverser())
    pipeline.append_stage('reverser1', BatchTextReverser())
    pipeline.append_stage('reverser2', BatchTextReverser())
    pipeline.append_stage('duplicator', BatchTextDuplicator())
    item = next(items_generator_fx)
    result = pipeline.process(copy.deepcopy(item))
    assert result.id == item.id
    assert result.payload['text'] != item.payload['text']

    pipeline = _pipeline()
    pipeline.append_stage_concurrently('reverser0', BatchTextReverser, kwargs={'cycles': 3}, concurrency=2)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, args=[5], concurrency=0)
    pipeline.append_stage('reverser2', BatchTextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(), concurrency=2)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload['text'] != item.payload['text']

    pipeline = _pipeline()
    pipeline.append_stage_concurrently('reverser0', BatchTextReverser, concurrency=2, use_threads=False)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, args=[9], concurrency=1, use_threads=False)
    pipeline.append_stage_concurrently('reverser2', BatchTextReverser, concurrency=0)
    pipeline.append_stage_concurrently('duplicator', BatchTextDuplicator, args=[10], concurrency=2, use_threads=False)
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload['text'] != item.payload['text']

    pipeline = _pipeline()
    pipeline.append_stage('reverser0', BatchTextReverser(), concurrency=0)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, concurrency=1)
    pipeline.append_stage('duplicator', BatchTextDuplicator(10), concurrency=0)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload['text'] == item.payload['text']

    pipeline = _pipeline()
    pipeline.append_stage_concurrently('duplicator0', BatchTextDuplicator)
    pipeline.append_stage('reverser', BatchTextReverser())
    pipeline.append_stage_concurrently('duplicator1', BatchTextDuplicator)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload['text'] != item.payload['text']

    pipeline = _pipeline()
    pipeline.append_stage('reverser', BatchTextReverser(11), concurrency=0)
    pipeline.append_stage_concurrently('duplicator0', BatchTextDuplicator, concurrency=0)
    pipeline.append_stage('duplicator1', BatchTextDuplicator(), concurrency=1)
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload['text'] != item.payload['text']

    pipeline = _pipeline().set_max_init_workers(1)
    pipeline.append_stage_concurrently('reverser0', BatchTextReverser, args=[20], concurrency=1, use_threads=False)
    pipeline.append_stage_concurrently('reverser1', BatchTextReverser, args=[20], concurrency=1, use_threads=True)
    pipeline.append_stage_concurrently('duplicator', BatchTextDuplicator, args=[20], concurrency=1, use_threads=False)
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload['text'] == item.payload['text']
    assert len(result.payload.keys()) > len(item.payload.keys())
