import pytest

from smartpipeline.error.handling import ErrorManager
from smartpipeline.error.exceptions import Error, CriticalError
from smartpipeline.item import DataItem
from tests.utils import TextReverser

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_manager(caplog):
    manager = ErrorManager()
    stage = TextReverser()
    item = DataItem()
    manager.handle(Error(), stage, item)
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
        assert "stage" in record.message


def test_critical_errors(caplog):
    stage = TextReverser()
    manager = ErrorManager()
    item = DataItem()
    managed_error = manager.handle(CriticalError(), stage, item)
    assert not item.has_errors()
    assert item.has_critical_errors()
    assert type(next(item.critical_errors()).get_exception()) == type(managed_error)
    assert any(caplog.records)
    manager = ErrorManager().raise_on_critical_error()
    item = DataItem()
    with pytest.raises(Exception):
        manager.handle(CriticalError(), stage, item)
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
        assert "stage" in record.message
