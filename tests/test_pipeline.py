import logging
import time

from smartpipeline.pipeline import Pipeline
from tests.utils import FakeSource, TextDuplicator, TextReverser, ErrorStage, CriticalErrorStage, ExceptionStage, \
    TimeWaster

__author__ = 'Giacomo Berardi <giacbrd.com>'


logger = logging.getLogger(__name__)


def test_run():
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('duplicator', TextDuplicator())
    for item in pipeline.run():
        assert len([x for x in item.payload.keys() if x.startswith('text')]) == 2
        assert item.get_timing('reverser')
        assert item.get_timing('duplicator')


def test_error(caplog):
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('error', ErrorStage())
    for item in pipeline.run():
        assert item.has_errors()
        assert item.get_timing('reverser')
        assert item.get_timing('error')
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == 'test pipeline error'
    assert any(caplog.records)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('error1', CriticalErrorStage())
    pipeline.append_stage('error2', ExceptionStage())
    for item in pipeline.run():
        assert item.has_critical_errors()
        assert item.get_timing('reverser')
        assert item.get_timing('error1')
        assert item.get_timing('error2') >= 0.5
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert str(error) == 'test pipeline critical error' or str(error) == 'test exception'
    assert any(caplog.records)


def _check(items, num):
    diff = frozenset(range(1, num+1)).difference(item.payload['count'] for item in items)
    assert not diff, 'Not found items {}'.format(diff)


def test_concurrent_run():
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=2)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=0)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=2, use_threads=False)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=0)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=2, use_threads=False)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=0)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=1, use_threads=False)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1, use_threads=True)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=1, use_threads=False)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('duplicator0', TextDuplicator(), concurrency=0)
    pipeline.append_stage('reverser', TextReverser(), concurrency=0)
    pipeline.append_stage('duplicator1', TextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser', TextReverser(), concurrency=0)
    pipeline.append_stage('duplicator0', TextDuplicator(), concurrency=0)
    pipeline.append_stage('duplicator1', TextDuplicator(), concurrency=1)
    items = list(pipeline.run())
    _check(items, 100)


def test_concurrent_initialization():
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('reverser0', TextReverser, kwargs={'cycles': 3}, concurrency=2)
    pipeline.append_stage_concurrently('reverser1', TextReverser, args=[5], concurrency=0)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('reverser0', TextReverser, concurrency=2, use_threads=False)
    pipeline.append_stage_concurrently('reverser1', TextReverser, args=[10], concurrency=1, use_threads=False)
    pipeline.append_stage_concurrently('reverser2', TextReverser, concurrency=0)
    pipeline.append_stage_concurrently('duplicator', TextDuplicator, args=[10], concurrency=2, use_threads=False)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=0)
    pipeline.append_stage_concurrently('reverser1', TextReverser, concurrency=1)
    pipeline.append_stage('duplicator', TextDuplicator(10), concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('duplicator0', TextDuplicator, concurrency=0)
    pipeline.append_stage('reverser', TextReverser(), concurrency=0)
    pipeline.append_stage_concurrently('duplicator1', TextDuplicator, concurrency=0)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser', TextReverser(12), concurrency=0)
    pipeline.append_stage_concurrently('duplicator0', TextDuplicator, concurrency=0)
    pipeline.append_stage('duplicator1', TextDuplicator(), concurrency=1)
    items = list(pipeline.run())
    _check(items, 100)
    pipeline = Pipeline().set_max_init_workers(1)
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage_concurrently('reverser0', TextReverser, args=[20], concurrency=1, use_threads=False)
    pipeline.append_stage_concurrently('reverser1', TextReverser, args=[20], concurrency=1, use_threads=True)
    pipeline.append_stage_concurrently('duplicator', TextDuplicator, args=[20], concurrency=1, use_threads=False)
    items = list(pipeline.run())
    _check(items, 100)


def test_huge_run():
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(200))
    pipeline.append_stage('reverser0', TextReverser(10000), concurrency=2, use_threads=False)
    pipeline.append_stage('reverser1', TextReverser(10000), concurrency=3, use_threads=False)
    pipeline.append_stage('reverser2', TextReverser(10000), concurrency=1, use_threads=False)
    pipeline.append_stage('duplicator', TextDuplicator(10000), concurrency=2, use_threads=False)
    start_time = time.time()
    items = list(pipeline.run())
    elasped1 = time.time() - start_time
    logger.debug('Time for parallel: {}'.format(elasped1))
    _check(items, 200)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(200))
    pipeline.append_stage('reverser0', TextReverser(10000), concurrency=0)
    pipeline.append_stage('reverser1', TextReverser(10000), concurrency=0)
    pipeline.append_stage('reverser2', TextReverser(10000), concurrency=0)
    pipeline.append_stage('duplicator', TextDuplicator(10000), concurrency=0)
    start_time = time.time()
    items = list(pipeline.run())
    elasped2 = time.time() - start_time
    logger.debug('Time for sequential: {}'.format(elasped2))
    _check(items, 200)
    assert elasped2 > elasped1


def test_run_times():
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('waster0', TimeWaster(0.2), concurrency=1)
    pipeline.append_stage('waster1', TimeWaster(0.2), concurrency=1)
    pipeline.append_stage('waster2', TimeWaster(0.2), concurrency=1)
    pipeline.append_stage('waster3', TimeWaster(0.2), concurrency=1)
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elasped0 = time.time() - start_time
    logger.debug('Time for multi-threading: {}'.format(elasped0))
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('waster0', TimeWaster(0.2), concurrency=1, use_threads=False)
    pipeline.append_stage('waster1', TimeWaster(0.2), concurrency=1, use_threads=False)
    pipeline.append_stage('waster2', TimeWaster(0.2), concurrency=1, use_threads=False)
    pipeline.append_stage('waster3', TimeWaster(0.2), concurrency=1, use_threads=False)
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elasped1 = time.time() - start_time
    logger.debug('Time for multi-process: {}'.format(elasped1))
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('waster0', TimeWaster(0.2), concurrency=0)
    pipeline.append_stage('waster1', TimeWaster(0.2), concurrency=0)
    pipeline.append_stage('waster2', TimeWaster(0.2), concurrency=0)
    pipeline.append_stage('waster3', TimeWaster(0.2), concurrency=0)
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elasped2 = time.time() - start_time
    logger.debug('Time for sequential: {}'.format(elasped2))
    assert elasped2 > elasped0
    assert elasped2 > elasped1
