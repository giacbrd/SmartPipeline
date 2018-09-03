
from smartpipeline.error import ErrorManager, Error, CriticalError
from smartpipeline.stage import DataItem
from tests.utils import TextReverser

__author__ = 'Giacomo Berardi <giacbrd.com>'


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
        assert 'stage' in record.message


def test_logger():
    pass