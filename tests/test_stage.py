import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
from queue import Queue
from threading import Event

import pytest

from smartpipeline.containers import (
    BatchConcurrentStageContainer,
    BatchStageContainer,
    ConcurrentStageContainer,
    SourceContainer,
    StageContainer,
)
from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.executors import batch_stage_executor, stage_executor
from smartpipeline.helpers import FilePathItem
from smartpipeline.item import DataItem, Stop
from smartpipeline.utils import ProcessCounter, ThreadCounter
from tests.utils import (
    BatchTextGenerator,
    BatchTextReverser,
    ListSource,
    SerializableBatchStage,
    SerializableErrorManager,
    SerializableStage,
    TextGenerator,
    TextReverser,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_data():
    item = DataItem()
    assert item.id
    item.payload["text"] = "prova"
    item.payload["id"] = "666"
    item.set_metadata("source", "remote")
    item.set_metadata("version", 3)
    assert set(item.metadata_fields) == {"source", "version"}
    assert item.id == "666"
    assert item.get_metadata("source") == "remote"
    assert item.get_metadata("version") == 3
    assert not item.get_metadata("head")
    assert item.payload == item.d


def test_error():
    item = DataItem()
    stage = TextReverser()
    item.add_soft_error(stage.name, ValueError("value error"))
    item.add_soft_error(stage.name, KeyError("key error"))
    item.add_critical_error(stage.name, KeyError("key error"))
    assert item.has_critical_errors()
    assert item.has_errors()
    assert len(list(item.soft_errors())) == 2
    assert len(list(item.critical_errors())) == 1
    stage = TextReverser()
    item.add_soft_error(stage.name, SoftError())
    item.add_critical_error(stage.name, CriticalError())
    with pytest.raises(ValueError):
        item.add_soft_error(stage.name, CriticalError())
    with pytest.raises(ValueError):
        item.add_critical_error(stage.name, SoftError())


def test_fileitem():
    item = FilePathItem("/path/to/file")
    assert item.id
    assert not item.payload
    assert item.path in str(item)


def test_source_container():
    manager = Manager()
    data = [DataItem() for _ in range(100)]
    for i, item in enumerate(data):
        item.set_metadata("id", i + 1)
    container = SourceContainer()
    assert str(container)
    assert not container.is_set()
    source = ListSource(data)
    container.set(source)
    assert not container.is_stopped()
    assert container.is_set()
    item = container.get_processed()
    assert item.get_metadata("id") == 1
    while not isinstance(item, Stop):
        assert not container.is_stopped()
        item = container.get_processed()
    assert container.is_stopped()

    container = SourceContainer()
    source = ListSource(data)
    container.set(source)
    item = DataItem()
    item.set_metadata("id", 1001)
    container.prepend_item(item)
    item = DataItem()
    item.set_metadata("id", 1002)
    container.prepend_item(item)
    assert container.get_processed().get_metadata("id") == 1001
    assert container.get_processed().get_metadata("id") == 1002
    assert container.get_processed().get_metadata("id") == 1
    assert container.get_processed().get_metadata("id") == 2
    item = DataItem()
    item.set_metadata("id", 1003)
    container.prepend_item(item)
    assert container.get_processed().get_metadata("id") == 1003
    assert container.get_processed().get_metadata("id") == 3
    assert not container.is_stopped()
    container.init_queue(manager.Queue)
    queue = container.out_queue
    item = DataItem()
    item.set_metadata("id", 1004)
    queue.put(item)
    assert container.get_processed().get_metadata("id") == 1004
    container.pop_into_queue()
    assert container.get_processed().get_metadata("id") == 4

    container = SourceContainer()
    source = ListSource([])
    container.set(source)
    container.prepend_item(DataItem())
    container.stop()
    assert isinstance(container.get_processed(), DataItem)
    assert isinstance(container.get_processed(), Stop)


def test_stage_container():
    manager = Manager()
    simple_item = DataItem()
    simple_item.payload["text"] = "hello world"
    source = SourceContainer()
    source.set(ListSource([DataItem() for _ in range(20)]))
    previous = StageContainer("test0", TextGenerator(), ErrorManager(), RetryManager())
    previous.set_previous(source)
    container = StageContainer("test1", TextReverser(), ErrorManager(), RetryManager())
    container.set_previous(previous)
    assert str(container)
    assert container.name
    previous.process()
    assert container.count() == 0
    item1 = container.process()
    item2 = container.get_processed()
    assert item1 and item2
    assert item1 == item2
    assert len(list(item1.timed_stages)) == 2
    assert container.count() == 1
    previous.process()
    item3 = container.process()
    item4 = container.get_processed()
    assert container.count() == 2
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
    container.set_previous(source)
    assert container.process()
    last = container.process()
    assert isinstance(last, Stop)
    assert str(last)
    assert container.is_stopped() and not container.is_terminated()

    container = ConcurrentStageContainer(
        "test2",
        TextReverser(),
        ErrorManager(),
        RetryManager(),
        manager.Queue,
        lambda: ProcessCounter(manager),
        manager.Event,
    )
    container.set_previous(previous)
    container.run()
    previous.process()
    item5 = container.get_processed(block=True)
    assert item5
    assert list(item5.timed_stages)
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
    del container

    source = SourceContainer()
    source.set(ListSource([simple_item]))
    container = ConcurrentStageContainer(
        "test2",
        TextReverser(),
        ErrorManager(),
        RetryManager(),
        manager.Queue,
        lambda: ProcessCounter(manager),
        manager.Event,
    )
    container.set_previous(source)
    container.run()
    source.pop_into_queue()
    assert container.get_processed(block=True)
    source.pop_into_queue()
    assert isinstance(container.get_processed(block=True), Stop)
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()
    del container


def test_executors():
    manager = Manager()
    terminated = manager.Event()
    in_queue = manager.Queue()
    out_queue = manager.Queue()

    in_queue.put(DataItem())
    item = DataItem()
    item.payload["file"] = "f"
    in_queue.put(item)
    terminated.set()
    with pytest.raises(ValueError) as excinfo:
        stage_executor(
            SerializableStage(),
            in_queue,
            out_queue,
            SerializableErrorManager().raise_on_critical_error(),
            RetryManager(),
            terminated,
            ProcessCounter(manager),
            ProcessCounter(manager),
            manager.Queue(),
        )
    assert "bad item" in str(excinfo.value)
    assert out_queue.get(block=True).payload["file"]

    terminated = manager.Event()
    for _ in range(100):
        in_queue.put(DataItem())
    item = DataItem()
    item.payload["file"] = "f"
    in_queue.put(item)
    terminated.set()
    with pytest.raises(ValueError) as excinfo:
        batch_stage_executor(
            SerializableBatchStage(100, 1),
            in_queue,
            out_queue,
            SerializableErrorManager().raise_on_critical_error(),
            RetryManager(),
            terminated,
            ProcessCounter(manager),
            ProcessCounter(manager),
            manager.Queue(),
        )
    assert "bad item" in str(excinfo.value)
    for _ in range(100):
        assert out_queue.get(block=True).payload["file"]
    assert out_queue.empty()

    terminated = manager.Event()
    for _ in range(100):
        in_queue.put(DataItem())
    item = DataItem()
    item.payload["file"] = "f"
    in_queue.put(item)
    terminated.set()
    with pytest.raises(ValueError) as excinfo:
        batch_stage_executor(
            SerializableBatchStage(100, 1),
            in_queue,
            out_queue,
            SerializableErrorManager().raise_on_critical_error(),
            RetryManager(),
            terminated,
            ProcessCounter(manager),
            ProcessCounter(manager),
            manager.Queue(),
        )
    assert "bad item" in str(excinfo.value)
    for _ in range(100):
        assert out_queue.get(block=True).payload["file"]
    assert out_queue.empty()

    # also test when running concurrently, both on processes and threads, that is how batch stages always run
    terminated = manager.Event()
    future = ProcessPoolExecutor().submit(
        batch_stage_executor,
        SerializableBatchStage(100, 1),
        in_queue,
        out_queue,
        SerializableErrorManager().raise_on_critical_error(),
        RetryManager(),
        terminated,
        ProcessCounter(manager),
        ProcessCounter(manager),
        manager.Queue(),
    )
    for _ in range(150):
        in_queue.put(DataItem())
    for _ in range(150):
        assert out_queue.get(block=True).payload["file"]
    item = DataItem()
    item.payload["file"] = "f"
    in_queue.put(item)
    assert str(future.exception()) == "bad item"
    terminated.set()
    future.cancel()

    terminated = Event()
    in_queue = Queue()
    out_queue = Queue()
    stage = SerializableBatchStage(100, 1)
    stage.on_start()
    future = ThreadPoolExecutor().submit(
        batch_stage_executor,
        stage,
        in_queue,
        out_queue,
        SerializableErrorManager().raise_on_critical_error(),
        RetryManager(),
        terminated,
        ThreadCounter(),
        ThreadCounter(),
        Queue(),
    )
    for _ in range(150):
        in_queue.put(DataItem())
    for _ in range(150):
        assert out_queue.get(block=True).payload["file"]
    item = DataItem()
    item.payload["file"] = "f"
    in_queue.put(item)
    assert str(future.exception()) == "bad item"
    terminated.set()
    future.cancel()


def _get_items(container):
    while True:
        item = container.get_processed()
        if item is None or isinstance(item, Stop):
            break
        yield item


def test_batch_stage_container1():
    manager = Manager()
    simple_item = DataItem()
    simple_item.payload["text"] = "hello world"
    source = SourceContainer()
    source.set(ListSource([DataItem() for _ in range(200)]))
    previous = BatchStageContainer(
        "test0", BatchTextGenerator(), ErrorManager(), RetryManager()
    )
    previous.set_previous(source)
    container = BatchStageContainer(
        "test1", BatchTextReverser(), ErrorManager(), RetryManager()
    )
    container.set_previous(previous)
    previous.process()
    items1 = container.process()
    assert len(items1) == container.count()
    items2 = list(_get_items(container))
    assert all(items1) and all(items2)
    assert all(item.payload.get("text") for item in items1)
    assert all(list(item.timed_stages) for item in items1)
    assert all(item.payload.get("text") for item in items2)
    assert items1 == items2
    previous.process()
    items3 = container.process()
    items4 = list(_get_items(container))
    assert all(items3) and all(items4)
    assert all(item.payload.get("text") for item in items3)
    assert all(item.payload.get("text") for item in items4)
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
    assert container.get_processed() is None


def test_batch_stage_container2():
    source = SourceContainer()
    container = BatchStageContainer(
        "test1", BatchTextReverser(), ErrorManager(), RetryManager()
    )
    items = [DataItem() for _ in range(10)]
    for item in items:
        item.payload["text"] = "something"
    source.set(ListSource(items))
    container.set_previous(source)
    processed = container.process()
    assert len(processed) == 10 and not any(
        isinstance(item, Stop) for item in processed
    )
    reprocessed = container.process()
    assert any(isinstance(item, Stop) for item in reprocessed)
    assert container.is_stopped() and not container.is_terminated()


def test_batch_concurrent_stage_container1():
    manager = Manager()
    source = SourceContainer()
    source.set(ListSource([DataItem() for _ in range(200)]))
    previous = BatchStageContainer(
        "test0", BatchTextGenerator(), ErrorManager(), RetryManager()
    )
    previous.set_previous(source)
    container = BatchConcurrentStageContainer(
        "test2",
        BatchTextReverser(timeout=1.0),
        ErrorManager(),
        RetryManager(),
        manager.Queue,
        lambda: ProcessCounter(manager),
        manager.Event,
    )
    container.set_previous(previous)
    container.run()
    for _ in range(10):
        previous.process()
    items5 = list(_get_items(container))
    assert items5 and all(items5)
    assert all(list(item.timed_stages) for item in items5)
    assert container.count() == len(items5)
    for _ in range(11):
        previous.process()
    items6 = list(_get_items(container))
    assert items6 and all(items6)
    assert all(item.payload.get("text") for item in items5)
    assert all(item.payload.get("text") for item in items6)
    assert items5 != items6
    assert not container.is_stopped() and not container.is_terminated()
    container.empty_queues()
    container.terminate()
    del container

    container = BatchConcurrentStageContainer(
        "test2",
        BatchTextReverser(timeout=0.0),
        ErrorManager(),
        RetryManager(),
        manager.Queue,
        lambda: ProcessCounter(manager),
        manager.Event,
    )
    container.set_previous(previous)
    container.run()
    queue = container.out_queue
    for item in items6:
        queue.put(item)
    result = list(_get_items(container))
    for i, item in enumerate(items6):
        assert item.payload == result[i].payload, "On item {}".format(i)
    container.terminate()
    del container


def test_batch_concurrent_stage_container2():
    manager = Manager()
    source = SourceContainer()
    items = [DataItem() for _ in range(10)]
    for item in items:
        item.payload["text"] = "something"
    source.set(ListSource(items))
    container = BatchConcurrentStageContainer(
        "test3",
        BatchTextGenerator(),
        ErrorManager(),
        RetryManager(),
        manager.Queue,
        lambda: ProcessCounter(manager),
        manager.Event,
    )
    container.set_previous(source)
    container.run()
    for _ in range(10):
        source.pop_into_queue()
    time.sleep(2)
    assert list(_get_items(container))
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()
    del container
