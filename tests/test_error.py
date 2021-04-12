import pytest

from smartpipeline.error.handling import ErrorManager
from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.item import DataItem
from tests.utils import TextReverser

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_manager(caplog):
    manager = ErrorManager()
    stage = TextReverser()
    item = DataItem()
    manager.handle(SoftError(), stage, item)
    assert any(caplog.records)
    assert item.has_errors()
    assert not item.has_critical_errors()
    item = DataItem()
    manager.handle(CriticalError(), stage, item)
    assert not item.has_errors()
    assert item.has_critical_errors()
    assert any(caplog.records)
    item = DataItem()
    manager.handle(ValueError(), stage, item)
    manager.handle(KeyError(), stage, item)
    manager.handle(KeyError(), stage, item)
    assert not item.has_errors()
    assert item.has_critical_errors()
    assert len(list(item.critical_errors())) == 3
    for record in caplog.records:
        assert "has generated an error" in record.message


def test_critical_errors(caplog):
    stage = TextReverser()
    manager = ErrorManager()
    item = DataItem()
    error = CriticalError()
    error.with_exception(Exception())
    managed_critical_error = manager.handle(error, stage, item)
    assert not item.has_errors()
    assert item.has_critical_errors()
    assert isinstance(
        next(item.critical_errors()).get_exception(),
        type(managed_critical_error.get_exception()),
    )
    assert any(caplog.records)
    manager = ErrorManager().raise_on_critical_error()
    item = DataItem()
    with pytest.raises(CriticalError):
        manager.handle(CriticalError(), stage, item)
    with pytest.raises(Exception):
        error = CriticalError().with_exception(Exception())
        manager.handle(error, stage, item)
    assert any(caplog.records)
    assert item.has_critical_errors()
    assert not item.has_errors()
    manager = ErrorManager().no_skip_on_critical_error()
    item = DataItem()
    assert manager.handle(CriticalError(), stage, item) is None
    assert not item.has_errors()
    assert item.has_critical_errors()
    assert any(caplog.records)
    item = DataItem()
    manager.handle(ValueError(), stage, item)
    manager.handle(KeyError(), stage, item)
    manager.handle(KeyError(), stage, item)
    assert not item.has_errors()
    assert item.has_critical_errors()
    assert len(list(item.critical_errors())) == 3
    for record in caplog.records:
        assert "has generated an error" in record.message
