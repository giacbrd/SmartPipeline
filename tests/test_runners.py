import logging
import threading
import time
from queue import Queue
from threading import Event

import pytest

from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.runners import worker_loop
from tests.utils import (
    QUEUE_TIMEOUT,
    BatchTextReverser,
    BrokenOnEndStage,
    BrokenProcessAndEndStage,
    LifecycleErrorManager,
    LifecycleStage,
    TextReverser,
    text_item,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_worker_loop_delivers_and_exits_on_stop():
    """A plain worker delivers every item and exits on Stop without forwarding it."""
    in_queue: Queue = Queue()
    out_queue: Queue = Queue()
    for text in ("hello", "world"):
        in_queue.put(text_item(text))
    in_queue.put(Stop())
    worker_loop(
        TextReverser(),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
    )
    assert out_queue.qsize() == 2
    assert out_queue.get_nowait().data["text"] == "olleh"
    assert out_queue.get_nowait().data["text"] == "dlrow"
    assert out_queue.empty()


def test_worker_loop_batch_flushes_on_stop():
    """A batch worker flushes its pending batch when Stop arrives."""
    in_queue: Queue = Queue()
    out_queue: Queue = Queue()
    for _ in range(3):
        in_queue.put(text_item("hello"))
    in_queue.put(Stop())
    worker_loop(
        BatchTextReverser(size=10, timeout=0.01),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
    )
    assert out_queue.qsize() == 3
    for _ in range(3):
        assert out_queue.get_nowait().data["text"] == "olleh"
    assert out_queue.empty()


def test_worker_loop_batch_flushes_on_timeout():
    """A batch worker flushes a partial batch after its timeout, then exits on Stop."""
    in_queue: Queue = Queue()
    out_queue: Queue = Queue()
    cancel = Event()
    for _ in range(3):
        in_queue.put(text_item("hello"))
    worker = threading.Thread(
        target=worker_loop,
        args=(
            BatchTextReverser(size=10, timeout=0.05),
            in_queue,
            out_queue,
            ErrorManager(),
            RetryManager(),
            cancel,
        ),
        daemon=True,
    )
    worker.start()
    deadline = time.monotonic() + QUEUE_TIMEOUT
    while out_queue.qsize() < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert out_queue.qsize() == 3
    in_queue.put(Stop())
    worker.join(timeout=QUEUE_TIMEOUT)
    assert not worker.is_alive()
    assert out_queue.qsize() == 3


def test_worker_loop_exits_on_cancel_while_blocked():
    """A worker blocked on a full output queue exits once cancel is set."""
    in_queue: Queue = Queue()
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item("full"))
    in_queue.put(text_item("hello"))
    cancel = Event()
    worker = threading.Thread(
        target=worker_loop,
        args=(
            TextReverser(),
            in_queue,
            out_queue,
            ErrorManager(),
            RetryManager(),
            cancel,
        ),
        daemon=True,
    )
    worker.start()
    time.sleep(0.2)
    assert worker.is_alive()
    cancel.set()
    worker.join(timeout=QUEUE_TIMEOUT)
    assert not worker.is_alive()
    assert out_queue.qsize() == 1


def test_worker_loop_process_lifecycle_pairs_on_start_on_end():
    """A process worker runs on_start at entry and on_end on exit."""
    events: list = []
    in_queue: Queue = Queue()
    out_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    in_queue.put(Stop())
    worker_loop(
        LifecycleStage(events),
        in_queue,
        out_queue,
        LifecycleErrorManager(events),
        RetryManager(),
        Event(),
        Queue(),
    )
    assert "error-manager:start" in events
    assert any(e.endswith(":start") and "track" not in e for e in events)
    assert out_queue.qsize() == 1
    assert "error-manager:end" in events
    assert any(e.endswith(":end") for e in events)


def test_worker_loop_on_start_failure_propagates_without_on_end():
    """A failing on_start propagates and skips on_end."""
    events: list = []
    in_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    with pytest.raises(RuntimeError, match="failed on_start"):
        worker_loop(
            LifecycleStage(events, fail_on_start=True),
            in_queue,
            Queue(),
            ErrorManager(),
            RetryManager(),
            Event(),
            Queue(),
        )
    assert any(e.endswith(":start") for e in events)
    assert not [e for e in events if e.endswith(":end")]


def test_worker_loop_on_end_failure_raises():
    """An on_end failure on a clean exit is raised as fatal."""
    in_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    in_queue.put(Stop())
    with pytest.raises(RuntimeError, match="test on_end failure"):
        worker_loop(
            BrokenOnEndStage(),
            in_queue,
            Queue(),
            ErrorManager(),
            RetryManager(),
            Event(),
            Queue(),
        )


def test_worker_loop_logs_on_end_failure_after_process_error(caplog):
    """A process worker logs on_end without masking the processing error."""
    in_queue: Queue = Queue()
    in_queue.put(Item())
    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError, match="test process failure"):
            worker_loop(
                BrokenProcessAndEndStage(),
                in_queue,
                Queue(),
                ErrorManager().raise_on_critical_error(),
                RetryManager(),
                Event(),
                Queue(),
            )
    assert any(
        "processing error" in record.getMessage().lower() for record in caplog.records
    )
