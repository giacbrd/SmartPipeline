import logging
import time
from concurrent.futures import Future
from multiprocessing import Manager
from queue import Queue
from threading import Event

from smartpipeline.containers import (
    BatchConcurrentStageContainer,
    BatchStageContainer,
    ConcurrentStageContainer,
    SourceContainer,
    StageContainer,
)
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.utils import ProcessCounter, ThreadCounter
from tests.utils import (
    QUEUE_TIMEOUT,
    BatchTextGenerator,
    BatchTextReverser,
    ErrorSource,
    ListSource,
    TextGenerator,
    TextReverser,
    text_item,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def _get_items(container):
    while True:
        item = container.get_processed()
        if item is None or isinstance(item, Stop):
            break
        yield item


def _wait_for_count(container, expected, timeout=30.0):
    """Wait until a concurrent container has seen the expected number of items.

    A concurrent runner puts each item in the output queue before increasing
    its counter, and it produces asynchronously, so draining the output right
    after feeding the input can stop at the first empty queue while items are
    still in flight (and observe a counter lagging one put behind).
    Poll the counter up to `timeout` seconds so the test observes quiescence.
    """
    start = time.time()
    while container.count() < expected:
        if time.time() - start > timeout:
            break
        time.sleep(0.05)
    return container.count()


def _hundred_items():
    data = [Item() for _ in range(100)]
    for i, item in enumerate(data):
        item.metadata["id"] = i + 1
    return data


def test_source_container_streams_until_stop():
    """SourceContainer streams items in order until Stop and reports stopped."""
    data = _hundred_items()
    container = SourceContainer()
    assert str(container)
    assert not container.is_set()
    container.set(ListSource(data))
    assert not container.is_stopped()
    assert container.is_set()
    item = container.get_processed()
    assert item.metadata["id"] == 1
    while not isinstance(item, Stop):
        assert not container.is_stopped()
        item = container.get_processed()
    assert container.is_stopped()


def test_source_container_prepends_items():
    """SourceContainer serves prepended items before the source stream."""
    data = _hundred_items()
    container = SourceContainer()
    container.set(ListSource(data))
    item = Item()
    item.metadata["id"] = 1001
    container.prepend_item(item)
    item = Item()
    item.metadata["id"] = 1002
    container.prepend_item(item)
    assert container.get_processed().metadata["id"] == 1001
    assert container.get_processed().metadata["id"] == 1002
    assert container.get_processed().metadata["id"] == 1
    assert container.get_processed().metadata["id"] == 2
    item = Item()
    item.metadata["id"] = 1003
    container.prepend_item(item)
    assert container.get_processed().metadata["id"] == 1003
    assert container.get_processed().metadata["id"] == 3
    assert not container.is_stopped()


def test_source_container_is_set_needs_a_source():
    """The output queue alone does not make the source container able to produce items."""
    container = SourceContainer()
    assert not container.is_set()
    # the output queue is created when a concurrent stage is linked to the container,
    # but it does not provide any item to produce
    container.init_queue(Queue)
    assert not container.is_set()
    container.prepend_item(Item())
    assert container.is_set()


def test_source_container_queued_pop_and_stop():
    """SourceContainer pops into queues and stops with a prepended item left."""
    manager = Manager()
    data = _hundred_items()
    container = SourceContainer()
    container.set(ListSource(data))
    container.init_queue(manager.Queue)
    queue = container.out_queue
    item = Item()
    item.metadata["id"] = 1004
    queue.put(item)
    assert container.get_processed().metadata["id"] == 1004
    container.pop_into_queue(Queue())
    assert container.get_processed().metadata["id"] == 1

    container = SourceContainer()
    container.set(ListSource([]))
    container.prepend_item(Item())
    container.stop()
    assert isinstance(container.get_processed(), Item)
    assert isinstance(container.get_processed(), Stop)


def test_stage_container_inline_and_concurrent():
    """StageContainer processes inline items; the concurrent variant runs and terminates."""
    manager = Manager()
    simple_item = Item()
    simple_item.data["text"] = "hello world"
    source = SourceContainer()
    source.set(ListSource([Item() for _ in range(20)]))
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
    assert item4.data == container.get_processed().data
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
    item5 = container.get_processed(block=True, timeout=QUEUE_TIMEOUT)
    assert item5
    assert list(item5.timed_stages)
    previous.process()
    item6 = container.get_processed(block=True, timeout=QUEUE_TIMEOUT)
    assert item6
    assert item5 != item6
    assert not container.is_stopped() and not container.is_terminated()
    queue = container.out_queue
    queue.put(item6)
    assert item6.data == container.get_processed().data
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
    source_errors = Queue()
    source.pop_into_queue(source_errors)
    assert container.get_processed(block=True, timeout=QUEUE_TIMEOUT) is not None
    source.pop_into_queue(source_errors)
    assert isinstance(container.get_processed(block=True, timeout=QUEUE_TIMEOUT), Stop)
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()
    del container


def test_batch_stage_container_sequential_batches():
    """BatchStageContainer processes sequential batches and serves queued items."""
    manager = Manager()
    simple_item = Item()
    simple_item.data["text"] = "hello world"
    source = SourceContainer()
    source.set(ListSource([Item() for _ in range(200)]))
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
    assert all(item.data.get("text") for item in items1)
    assert all(list(item.timed_stages) for item in items1)
    assert all(item.data.get("text") for item in items2)
    assert items1 == items2
    previous.process()
    items3 = container.process()
    items4 = list(_get_items(container))
    assert all(items3) and all(items4)
    assert all(item.data.get("text") for item in items3)
    assert all(item.data.get("text") for item in items4)
    assert items1 != items3
    assert items3 == items4
    assert not container.is_stopped() and not container.is_terminated()
    container.init_queue(manager.Queue)
    queue = container.out_queue
    for item in items4:
        queue.put(item)
    result = list(_get_items(container))
    for i, item in enumerate(items4):
        assert item.data == result[i].data
    assert container.get_processed() is None


def test_batch_stage_container_stop_propagation():
    """BatchStageContainer forwards Stop once the source is exhausted."""
    source = SourceContainer()
    container = BatchStageContainer(
        "test1", BatchTextReverser(), ErrorManager(), RetryManager()
    )
    items = [Item() for _ in range(10)]
    for item in items:
        item.data["text"] = "something"
    source.set(ListSource(items))
    container.set_previous(source)
    processed = container.process()
    assert len(processed) == 10 and not any(
        isinstance(item, Stop) for item in processed
    )
    reprocessed = container.process()
    assert any(isinstance(item, Stop) for item in reprocessed)
    assert container.is_stopped() and not container.is_terminated()


def test_batch_concurrent_stage_container_throughput():
    """BatchConcurrentStageContainer processes 200 items with async counting."""
    manager = Manager()
    source = SourceContainer()
    source.set(ListSource([Item() for _ in range(200)]))
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
    assert _wait_for_count(container, 100) == 100
    items5 = list(_get_items(container))
    assert items5 and all(items5)
    assert all(list(item.timed_stages) for item in items5)
    assert container.count() == len(items5) == 100
    for _ in range(11):
        previous.process()
    assert _wait_for_count(container, 200) == 200
    items6 = list(_get_items(container))
    assert items6 and all(items6)
    assert all(item.data.get("text") for item in items5)
    assert all(item.data.get("text") for item in items6)
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
        assert item.data == result[i].data, "On item {}".format(i)
    container.terminate()
    del container


def test_batch_concurrent_stage_container_termination():
    """BatchConcurrentStageContainer terminates and reports termination."""
    manager = Manager()
    source = SourceContainer()
    items = [Item() for _ in range(10)]
    for item in items:
        item.data["text"] = "something"
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
    source_errors = Queue()
    for _ in range(10):
        source.pop_into_queue(source_errors)
    time.sleep(2)
    assert list(_get_items(container))
    container.terminate()
    source.prepend_item(None)
    time.sleep(1)
    assert container.is_terminated()
    container.shutdown()
    del container


def test_fatal_event_mixin():
    """Containers expose the shared fatal event and observe when it fires."""
    container = StageContainer(
        "fatal-test", TextReverser(), ErrorManager(), RetryManager()
    )
    assert container.fatal_event is None
    assert not container.is_fatal()
    event = Event()
    container.set_fatal_event(event)
    assert container.fatal_event is event
    assert not container.is_fatal()
    assert not container.has_aborted_put()
    event.set()
    assert container.is_fatal()


def test_stage_container_put_abort():
    """StageContainer drops output and flags the abort once fatal is set."""
    container = StageContainer(
        "put-abort", TextReverser(), ErrorManager(), RetryManager()
    )
    container.init_queue(lambda: Queue(maxsize=1))
    container.out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    container.set_fatal_event(fatal)
    container._put_item(text_item())
    assert container.has_aborted_put()


def test_batch_container_put_abort_breaks():
    """BatchStageContainer aborts a batch put on fatal, dropping the tail."""
    container = BatchStageContainer(
        "batch-put-abort", BatchTextGenerator(size=2), ErrorManager(), RetryManager()
    )
    container.init_queue(lambda: Queue(maxsize=1))
    container.out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    container.set_fatal_event(fatal)
    container._put_item([text_item(), text_item()])
    assert container.has_aborted_put()
    # the second item of the batch is dropped with the abort
    assert container.out_queue.qsize() == 1


def test_empty_out_queue():
    """empty_out_queue drains the output queue and tolerates a missing one."""
    container = StageContainer(
        "empty-test", TextReverser(), ErrorManager(), RetryManager()
    )
    # no output queue: nothing to do, must not raise
    container.empty_out_queue()
    container.init_queue(Queue)
    container.out_queue.put(text_item())
    container.out_queue.put(text_item())
    container.empty_out_queue()
    assert container.out_queue.empty()


def test_source_pop_into_queue_aborts_on_fatal():
    """SourceContainer stops instead of pushing once fatal is set."""
    container = SourceContainer()
    container.set(ListSource([text_item() for _ in range(5)]))
    container.init_queue(lambda: Queue(maxsize=1))
    container.out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    container.set_fatal_event(fatal)
    container.pop_into_queue(Queue())
    assert container._stop_sent is True


def test_source_pop_into_queue_reports_errors():
    """SourceContainer routes source pop failures to the errors queue."""
    container = SourceContainer()
    container.set(ErrorSource())
    container.init_queue(Queue)
    errors: Queue = Queue()
    container.pop_into_queue(errors)
    assert isinstance(errors.get_nowait(), ValueError)


def test_concurrent_shutdown_oserror(caplog):
    """Concurrent shutdown failures are logged, not raised."""
    container = ConcurrentStageContainer(
        "shutdown-test",
        TextReverser(),
        ErrorManager(),
        RetryManager(),
        Queue,
        ThreadCounter,
        Event,
        concurrency=1,
    )
    future: Future = Future()
    future.set_result(None)
    container._futures = [future]

    class _BoomExecutor:
        def shutdown(self):
            raise OSError("boom")

    container._stage_executor = _BoomExecutor()
    with caplog.at_level(logging.WARNING):
        container.shutdown()
    assert any("shutting down" in record.msg.lower() for record in caplog.records)
