from smartpipeline.pipeline import Pipeline
from tests.utils import FakeSource, TextDuplicator, TextReverser, ErrorStage, CriticalErrorStage, ExceptionStage

__author__ = 'Giacomo Berardi <giacbrd.com>'


def test_run():
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('duplicator', TextDuplicator())
    for item in pipeline.run():
        assert len([x for x in item.payload.keys() if x.startswith('text')]) == 2


def test_error(caplog):
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('error', ErrorStage())
    for item in pipeline.run():
        assert item.has_errors()
        error = next(item.errors())
        assert isinstance(error.get_exception(), Exception)
        assert str(error) == 'test pipeline error'
    assert any(caplog.records)
    pipeline = Pipeline()
    pipeline.set_source(FakeSource(10))
    pipeline.append_stage('reverser', TextReverser())
    pipeline.append_stage('error', CriticalErrorStage())
    pipeline.append_stage('error', ExceptionStage())
    for item in pipeline.run():
        assert item.has_critical_errors()
        for error in item.critical_errors():
            assert isinstance(error.get_exception(), Exception)
            assert str(error) == 'test pipeline critical error' or str(error) == 'test exception'
    assert any(caplog.records)

