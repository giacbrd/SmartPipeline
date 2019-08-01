import time
from multiprocessing import Manager

import pytest

from smartpipeline.error import CriticalError, Error, ErrorManager
from smartpipeline.executors import SourceContainer, StageContainer, ConcurrentStageContainer, BatchStageContainer, \
    BatchConcurrentStageContainer
from smartpipeline.helpers import FilePathItem
from smartpipeline.stage import DataItem, Stop
from tests.utils import TextReverser, ListSource, TextGenerator, BatchTextReverser, BatchTextGenerator

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
    manager = Manager()
    data = [DataItem() for _ in range(100)]
    for i, item in enumerate(data):
        item.set_metadata('id', i+1)
    container = SourceContainer()
    assert not container.is_set()
    source = ListSource(data)
    container.set(source)
    assert not container.is_stopped()
    assert container.is_set()
    item = container.get_processed()
    assert item.get_metadata('id') == 1
    while not isinstance(item, Stop):
        assert not container.is_stopped()
        item = container.get_processed()
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
    assert container.get_processed().get_metadata('id') == 1001
    assert container.get_processed().get_metadata('id') == 1002
    assert container.get_processed().get_metadata('id') == 1
    assert container.get_processed().get_metadata('id') == 2
    item = DataItem()
    item.set_metadata('id', 1003)
    container.prepend_item(item)
    assert container.get_processed().get_metadata('id') == 1003
    assert container.get_processed().get_metadata('id') == 3
    assert not container.is_stopped()
    container.init_queue(manager.Queue)
    queue = container.out_queue
    item = DataItem()
    item.set_metadata('id', 1004)
    queue.put(item)
    assert container.get_processed().get_metadata('id') == 1004
    container.pop_into_queue()
    assert container.get_processed().get_metadata('id') == 4


def test_stage_container():
    manager = Manager()
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
    item2 = container.get_processed()
    assert item1 and item2
    assert item1 == item2
    previous.process()
    item3 = container.process()
    item4 = container.get_processed()
    assert item3 and item4
    assert item1 != item3
    assert item3 == item4
    assert not container.is_stopped() and not container.is_terminated()
    container.init_queue(manager.Queue)
    queue = container.out_queue
    queue.put(item4)
    assert item4.payload == container.get_processed().payload
    source = SourceContainer()
    source.set(ListSource([simple_item]))
    container.set_previous_stage(source)
    assert container.process()
    assert isinstance(container.process(), Stop)
    assert container.is_stopped() and container.is_terminated()

    container = ConcurrentStageContainer('test2', TextReverser(), ErrorManager(), manager.Queue)
    container.set_previous_stage(previous)
    container.run(manager.Event)
    previous.process()
    item5 = container.get_processed(block=True)
    assert item5
    previous.process()
    item6 = container.get_processed(block=True)
    assert item6
    assert item5 != item6
    assert not container.is_stopped() and not container.is_terminated()
    queue = container.out_queue
    queue.put(item6)
    assert item6.payload == container.get_processed().payload
    container.terminate()
    container.shutdown()

    source = SourceContainer()
    source.set(ListSource([simple_item]))
    container = ConcurrentStageContainer('test2', TextReverser(), ErrorManager(), manager.Queue)
    container.set_previous_stage(source)
    container.run(manager.Event)
    source.pop_into_queue()
    assert container.get_processed(block=True)
    source.pop_into_queue()
    assert isinstance(container.get_processed(block=True), Stop)
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()


def _get_items(container):
    while True:
        item = container.get_processed()
        if item is None or isinstance(item, Stop):
            break
        yield item


def test_batch_stage_container1():
    manager = Manager()
    simple_item = DataItem()
    simple_item.payload['text'] = 'hello world'
    source = SourceContainer()
    source.set(ListSource([DataItem() for _ in range(200)]))
    previous = BatchStageContainer('test0', BatchTextGenerator(), ErrorManager())
    previous.set_previous_stage(source)
    container = BatchStageContainer('test1', BatchTextReverser(), ErrorManager())
    container.set_previous_stage(previous)
    previous.process()
    items1 = container.process()
    items2 = list(_get_items(container))
    assert all(items1) and all(items2)
    assert all(item.payload.get('text') for item in items1)
    assert all(item.payload.get('text') for item in items2)
    assert items1 == items2
    previous.process()
    items3 = container.process()
    items4 = list(_get_items(container))
    assert all(items3) and all(items4)
    assert all(item.payload.get('text') for item in items3)
    assert all(item.payload.get('text') for item in items4)
    assert items1 != items3
    assert items3 == items4
    assert not container.is_stopped() and not container.is_terminated()
    container.init_queue(manager.Queue)
    queue = container.out_queue
    for item in items4:
        queue.put(item)
    result = list(_get_items(container))
    for i, item in enumerate(items4):
        assert item.payload == result[i].payload


def test_batch_stage_container2():
    source = SourceContainer()
    container = BatchStageContainer('test1', BatchTextReverser(), ErrorManager())
    items = [DataItem() for _ in range(10)]
    for item in items:
        item.payload['text'] = 'something'
    source.set(ListSource(items))
    container.set_previous_stage(source)
    processed = container.process()
    assert len(processed) == 10 and not any(isinstance(item, Stop) for item in processed)
    reprocessed = container.process()
    assert any(isinstance(item, Stop) for item in reprocessed)
    assert container.is_stopped() and container.is_terminated()


def test_batch_concurrent_stage_container1():
    manager = Manager()
    source = SourceContainer()
    source.set(ListSource([DataItem() for _ in range(200)]))
    previous = BatchStageContainer('test0', BatchTextGenerator(), ErrorManager())
    previous.set_previous_stage(source)
    container = BatchConcurrentStageContainer('test2', BatchTextReverser(timeout=1.), ErrorManager(), manager.Queue)
    container.set_previous_stage(previous)
    container.run(manager.Event)
    for _ in range(10):
        previous.process()
    items5 = list(_get_items(container))
    assert items5 and all(items5)
    for _ in range(11):
        previous.process()
    items6 = list(_get_items(container))
    assert items6 and all(items6)
    assert all(item.payload.get('text') for item in items5)
    assert all(item.payload.get('text') for item in items6)
    assert items5 != items6
    assert not container.is_stopped() and not container.is_terminated()
    container.empty_queues()
    container.terminate()

    container = BatchConcurrentStageContainer('test2', BatchTextReverser(timeout=0.), ErrorManager(), manager.Queue)
    container.run(manager.Event)
    queue = container.out_queue
    for item in items6:
        queue.put(item)
    result = list(_get_items(container))
    for i, item in enumerate(items6):
        assert item.payload == result[i].payload, 'On item {}'.format(i)
    container.terminate()


def test_batch_concurrent_stage_container2():
    manager = Manager()
    source = SourceContainer()
    items = [DataItem() for _ in range(10)]
    for item in items:
        item.payload['text'] = 'something'
    source.set(ListSource(items))
    container = BatchConcurrentStageContainer('test3', BatchTextGenerator(), ErrorManager(), manager.Queue)
    container.set_previous_stage(source)
    container.run(manager.Event)
    for _ in range(10):
        source.pop_into_queue()
    time.sleep(2)
    assert list(_get_items(container))
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()