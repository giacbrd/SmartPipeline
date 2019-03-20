from smartpipeline.pipeline import Pipeline
from tests.utils import FakeSource, TextDuplicator, TextReverser, ErrorStage, CriticalErrorStage, ExceptionStage, \
    TimeWaster

__author__ = 'Giacomo Berardi <giacbrd.com>'


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


def test_concurrent_run():

    def _check(items):
        assert len(items) == 100, 'Not found items {}'.format(
            set(range(1, 101)).difference(item.payload['count'] for item in items))
        assert any(item.payload['count'] == 1 for item in items)
        assert any(item.payload['count'] == 100 for item in items)

    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=2)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=0)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=2)
    items = list(pipeline.run())
    _check(items)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=2, use_threads=True)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1, use_threads=True)
    pipeline.append_stage('reverser2', TextReverser(), concurrency=0, use_threads=True)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=2, use_threads=True)
    items = list(pipeline.run())
    _check(items)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(100))
    pipeline.append_stage('reverser0', TextReverser(), concurrency=0)
    pipeline.append_stage('reverser1', TextReverser(), concurrency=1)
    pipeline.append_stage('duplicator', TextDuplicator(), concurrency=0)
    items = list(pipeline.run())
    _check(items)
