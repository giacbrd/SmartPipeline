import pytest

from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.item import Item
from smartpipeline.pipeline import Pipeline
from tests.utils import ErrorSource, TextGenerator, TextReverser

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_manager(caplog):
    manager = ErrorManager()
    stage = TextReverser()
    item = Item()
    manager.handle(SoftError(), stage, item)
    assert any(caplog.records)
    assert item.has_soft_errors()
    assert not item.has_critical_errors()
    item = Item()
    manager.handle(CriticalError(), stage, item)
    assert not item.has_soft_errors()
    assert item.has_critical_errors()
    assert any(caplog.records)
    item = Item()
    manager.handle(ValueError(), stage, item)
    manager.handle(KeyError(), stage, item)
    manager.handle(KeyError(), stage, item)
    assert not item.has_soft_errors()
    assert item.has_critical_errors()
    assert len(list(item.critical_errors())) == 3
    for record in caplog.records:
        assert "has generated an error" in record.message


def test_critical_errors(caplog):
    stage = TextReverser()
    manager = ErrorManager()
    item = Item()
    error = CriticalError()
    error.with_exception(Exception())
    managed_critical_error = manager.handle(error, stage, item)
    assert not item.has_soft_errors()
    assert item.has_critical_errors()
    assert isinstance(
        next(item.critical_errors()).get_exception(),
        type(managed_critical_error.get_exception()),
    )
    assert any(caplog.records)
    manager = ErrorManager().raise_on_critical_error()
    item = Item()
    with pytest.raises(CriticalError):
        manager.handle(CriticalError(), stage, item)
    with pytest.raises(Exception):
        error = CriticalError().with_exception(Exception())
        manager.handle(error, stage, item)
    assert any(caplog.records)
    assert item.has_critical_errors()
    assert not item.has_soft_errors()
    manager = ErrorManager().no_skip_on_critical_error()
    item = Item()
    assert manager.handle(CriticalError(), stage, item) is None
    assert not item.has_soft_errors()
    assert item.has_critical_errors()
    assert any(caplog.records)
    item = Item()
    manager.handle(ValueError(), stage, item)
    manager.handle(KeyError(), stage, item)
    manager.handle(KeyError(), stage, item)
    assert not item.has_soft_errors()
    assert item.has_critical_errors()
    assert len(list(item.critical_errors())) == 3
    for record in caplog.records:
        assert "has generated an error" in record.message


def test_source_errors():
    source = ErrorSource()
    pipeline = (
        Pipeline().set_source(source).append("text_generator", TextGenerator()).build()
    )
    run = pipeline.run()
    assert next(run)
    assert next(run)
    assert next(run)
    with pytest.raises(ValueError):
        next(run)
