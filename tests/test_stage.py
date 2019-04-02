import time

import pytest

from smartpipeline.error import CriticalError, Error, ErrorManager
from smartpipeline.executors import SourceContainer, StageContainer, ConcurrentStageContainer
from smartpipeline.helpers import FilePathItem
from smartpipeline.stage import DataItem, Stop
from tests.utils import TextReverser, ListSource, TextGenerator

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
    item = FilePathItem('/path/to/file')
    assert item.id
    assert not item.payload
    assert item.path in str(item)


def test_source_container():
    data = [DataItem() for _ in range(100)]
    for i, item in enumerate(data):
        item.set_metadata('id', i+1)
    container = SourceContainer()
    assert not container.is_set()
    source = ListSource(data)
    container.set(source)
    assert not container.is_stopped()
    assert container.is_set()
    item = container.get_item()
    assert item.get_metadata('id') == 1
    while not isinstance(item, Stop):
        assert not container.is_stopped()
        item = container.get_item()
    assert container.is_stopped()
    container = SourceContainer()
    source = ListSource(data)
    container.set(source)
    item = DataItem()
    item.set_metadata('id', 1001)
    container.prepend_item(item)
    item = DataItem()
    item.set_metadata('id', 1002)
    container.prepend_item(item)
    assert container.get_item().get_metadata('id') == 1001
    assert container.get_item().get_metadata('id') == 1002
    assert container.get_item().get_metadata('id') == 1
    assert container.get_item().get_metadata('id') == 2
    item = DataItem()
    item.set_metadata('id', 1003)
    container.prepend_item(item)
    assert container.get_item().get_metadata('id') == 1003
    assert container.get_item().get_metadata('id') == 3
    assert not container.is_stopped()
    container.init_queue()
    queue = container.out_queue
    item = DataItem()
    item.set_metadata('id', 1004)
    queue.put(item)
    assert container.get_item().get_metadata('id') == 1004
    container.pop_into_queue()
    assert container.get_item().get_metadata('id') == 4


def test_stage_container():
    simple_item = DataItem()
    simple_item.payload['text'] = 'hello world'
    source = SourceContainer()
    source.set(ListSource([DataItem() for _ in range(20)]))
    previous = StageContainer('test0', TextGenerator(), ErrorManager())
    previous.set_previous_stage(source)
    container = StageContainer('test1', TextReverser(), ErrorManager())
    container.set_previous_stage(previous)
    previous.process()
    item1 = container.process()
    item2 = container.get_item()
    assert item1 and item2
    assert item1 == item2
    previous.process()
    item3 = container.process()
    item4 = container.get_item()
    assert item3 and item4
    assert item1 != item3
    assert item3 == item4
    assert not container.is_stopped() and not container.is_terminated()
    container.init_queue()
    queue = container.out_queue
    queue.put(item4)
    assert item4.payload == container.get_item().payload
    source = SourceContainer()
    source.set(ListSource([simple_item]))
    container.set_previous_stage(source)
    assert container.process()
    assert isinstance(container.process(), Stop)
    assert container.is_stopped() and container.is_terminated()

    container = ConcurrentStageContainer('test2', TextReverser(), ErrorManager())
    container.set_previous_stage(previous)
    container.run()
    previous.process()
    item5 = container.get_item(block=True)
    assert item5
    previous.process()
    item6 = container.get_item(block=True)
    assert item6
    assert item5 != item6
    assert not container.is_stopped() and not container.is_terminated()
    queue = container.out_queue
    queue.put(item6)
    assert item6.payload == container.get_item().payload
    container.terminate()
    container.shutdown()

    source = SourceContainer()
    source.set(ListSource([simple_item]))
    container = ConcurrentStageContainer('test2', TextReverser(), ErrorManager())
    container.set_previous_stage(source)
    container.run()
    source.pop_into_queue()
    assert container.get_item(block=True)
    source.pop_into_queue()
    assert isinstance(container.get_item(block=True), Stop)
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()
