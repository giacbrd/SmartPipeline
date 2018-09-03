import pytest

from smartpipeline.error import CriticalError, Error
from smartpipeline.stage import DataItem, FileItem
from tests.utils import TextReverser

__author__ = 'Giacomo Berardi <giacbrd.com>'


def test_data():
    item = DataItem()
    assert item.id
    item.payload['text'] = 'prova'
    item.payload['id'] = '666'
    item.set_metadata('source', 'remote')
    item.set_metadata('version', 3)
    assert item.id == '666'
    assert item.get_metadata('source') == 'remote'
    assert item.get_metadata('version') == 3
    assert not item.get_metadata('head')


def test_error():
    item = DataItem()
    stage = TextReverser()
    item.add_error(stage, ValueError('value error'))
    item.add_error(stage, KeyError('key error'))
    item.add_critical_error(stage, KeyError('key error'))
    assert item.has_critical_errors()
    assert item.has_errors()
    assert len(list(item.errors())) == 2
    assert len(list(item.critical_errors())) == 1
    stage = TextReverser()
    item.add_error(stage, Error())
    item.add_critical_error(stage, CriticalError())
    with pytest.raises(ValueError):
        item.add_error(stage, CriticalError())
    with pytest.raises(ValueError):
        item.add_critical_error(stage, Error())


def test_fileitem():
    item = FileItem('/path/to/file')
    assert item.id
    assert not item.payload
    assert item.path in str(item)
