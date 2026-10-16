from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager, get_context
from queue import Queue
from threading import Event

import pytest

from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.runners import OnEndErrors, batch_stage_runner, stage_runner
from smartpipeline.utils import ProcessCounter, ThreadCounter
from tests.utils import (
    BatchTextGenerator,
    CustomizableBrokenBatchStage,
    CustomizableBrokenStage,
    SerializableBatchStage,
    SerializableErrorManager,
    SerializableStage,
    TextReverser,
    text_item,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_on_end_errors_type():
    """OnEndErrors collects stage and error-manager termination failures."""
    assert not OnEndErrors(None, None)
    assert OnEndErrors(None, None).exceptions == []
    first, second = RuntimeError("first"), ValueError("second")
    assert OnEndErrors(first, None).exceptions == [first]
    assert OnEndErrors(None, second).exceptions == [second]
    result = OnEndErrors(first, second)
    assert result
    assert result.exceptions == [first, second]


def test_runners_with_shared_counters():
    """Stage and batch runners drain queues with shared counters, on threads and processes."""
    manager = Manager()
    terminated = manager.Event()
    in_queue = manager.Queue()
    out_queue = manager.Queue()

    in_queue.put(Item())
    item = Item()
    item.data["file"] = "f"
    in_queue.put(item)
    terminated.set()
    with pytest.raises(ValueError) as excinfo:
        stage_runner(
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
    assert out_queue.get(block=True).data["file"]

    terminated = manager.Event()
    for _ in range(100):
        in_queue.put(Item())
    item = Item()
    item.data["file"] = "f"
    in_queue.put(item)
    terminated.set()
    with pytest.raises(ValueError) as excinfo:
        batch_stage_runner(
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
        assert out_queue.get(block=True).data["file"]
    assert out_queue.empty()

    terminated = manager.Event()
    for _ in range(100):
        in_queue.put(Item())
    item = Item()
    item.data["file"] = "f"
    in_queue.put(item)
    terminated.set()
    with pytest.raises(ValueError) as excinfo:
        batch_stage_runner(
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
        assert out_queue.get(block=True).data["file"]
    assert out_queue.empty()

    # also test when running concurrently, both on processes and threads, that is how batch stages always run
    terminated = manager.Event()
    future = ProcessPoolExecutor(mp_context=get_context("spawn")).submit(
        batch_stage_runner,
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
        in_queue.put(Item())
    for _ in range(150):
        assert out_queue.get(block=True).data["file"]
    item = Item()
    item.data["file"] = "f"
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
        batch_stage_runner,
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
        in_queue.put(Item())
    for _ in range(150):
        assert out_queue.get(block=True).data["file"]
    item = Item()
    item.data["file"] = "f"
    in_queue.put(item)
    assert str(future.exception()) == "bad item"
    terminated.set()
    future.cancel()


def test_stage_runner_sets_fatal_on_error():
    """A failing stage runner raises and sets the fatal event."""
    in_queue: Queue = Queue()
    in_queue.put(Item())
    terminated = Event()
    terminated.set()
    fatal = Event()
    with pytest.raises(ValueError):
        stage_runner(
            CustomizableBrokenStage([ValueError]),
            in_queue,
            Queue(),
            ErrorManager().raise_on_critical_error(),
            RetryManager(),
            terminated,
            ThreadCounter(),
            ThreadCounter(),
            None,
            fatal_event=fatal,
        )
    assert fatal.is_set()


def test_stage_runner_aborts_put_on_fatal():
    """A stage runner skips output puts once fatal is set."""
    in_queue: Queue = Queue()
    in_queue.put(text_item())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    counter = ThreadCounter()
    stage_runner(
        TextReverser(),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
        ThreadCounter(),
        counter,
        None,
        fatal_event=fatal,
    )
    assert out_queue.qsize() == 1
    assert counter.value == 0


def test_stage_runner_aborts_stop_on_fatal():
    """A stage runner skips Stop forwarding once fatal is set."""
    in_queue: Queue = Queue()
    in_queue.put(Stop())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    stage_runner(
        TextReverser(),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
        ThreadCounter(),
        ThreadCounter(),
        None,
        fatal_event=fatal,
    )
    assert out_queue.qsize() == 1


def test_batch_runner_sets_fatal_on_error():
    """A failing batch runner raises and sets the fatal event."""
    in_queue: Queue = Queue()
    in_queue.put(Item())
    in_queue.put(Item())
    fatal = Event()
    with pytest.raises(ValueError):
        batch_stage_runner(
            CustomizableBrokenBatchStage(2, 0.01, [ValueError]),
            in_queue,
            Queue(),
            ErrorManager().raise_on_critical_error(),
            RetryManager(),
            Event(),
            ThreadCounter(),
            ThreadCounter(),
            None,
            fatal_event=fatal,
        )
    assert fatal.is_set()


def test_batch_runner_aborts_put_on_fatal():
    """A batch runner skips output puts once fatal is set."""
    in_queue: Queue = Queue()
    in_queue.put(text_item())
    in_queue.put(text_item())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    counter = ThreadCounter()
    batch_stage_runner(
        BatchTextGenerator(size=2, timeout=0.01),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
        ThreadCounter(),
        counter,
        None,
        fatal_event=fatal,
    )
    assert out_queue.qsize() == 1
    assert counter.value == 0


def test_batch_runner_aborts_stop_on_fatal():
    """A batch runner skips Stop forwarding once fatal is set."""
    in_queue: Queue = Queue()
    in_queue.put(Stop())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item())
    fatal = Event()
    fatal.set()
    batch_stage_runner(
        BatchTextGenerator(size=2, timeout=0.01),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
        ThreadCounter(),
        ThreadCounter(),
        None,
        fatal_event=fatal,
    )
    assert out_queue.qsize() == 1
