import logging
import threading
import time
from queue import Queue
from threading import Event

import pytest

from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.runners import process, worker_loop
from tests.utils import (
    QUEUE_TIMEOUT,
    BatchTextReverser,
    BrokenOnEndErrorManager,
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
    """A process worker starts the error manager first and ends it last."""
    events: list = []
    in_queue: Queue = Queue()
    out_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    in_queue.put(Stop())
    stage = LifecycleStage(events)
    stage.set_name("track")
    worker_loop(
        stage,
        in_queue,
        out_queue,
        LifecycleErrorManager(events),
        RetryManager(),
        Event(),
        Queue(),
    )
    assert out_queue.qsize() == 1
    # the stage ends before the error manager, whose resources it may still need to
    # flush its own work: the same order the main-process teardown uses
    assert events == [
        "error-manager:start",
        "track:start",
        "track:process",
        "track:end",
        "error-manager:end",
    ]


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


def test_worker_loop_on_end_failure_raises(caplog):
    """An on_end failure on a clean exit is raised as fatal."""
    in_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    in_queue.put(Stop())
    with caplog.at_level(logging.ERROR):
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
    assert any(
        record.exc_info is not None and "test on_end failure" in str(record.exc_info[1])
        for record in caplog.records
    )


def test_worker_loop_error_manager_on_end_failure_raises():
    """An error-manager on_end failure is fatal too, even after a successful run."""
    in_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    in_queue.put(Stop())
    with pytest.raises(RuntimeError, match="test error manager on_end failure"):
        worker_loop(
            TextReverser(),
            in_queue,
            Queue(),
            BrokenOnEndErrorManager(),
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


def test_process_rejects_stage_returning_none():
    """A stage returning None records a ValueError critical error on the item."""

    class NoneStage(TextReverser):
        def process(self, item):
            return None

    item = process(NoneStage(), text_item("hello"), ErrorManager(), RetryManager())
    assert item.has_critical_errors()
    assert isinstance(next(item.critical_errors()).get_exception(), ValueError)


def test_batch_worker_exits_when_queue_disconnects():
    """A batch worker whose input queue loses its manager mid-batch exits cleanly."""

    class _DisconnectAfterFirstQueue(Queue):
        def __init__(self):
            super().__init__()
            self._reads = 0

        def get(self, *args, **kwargs):
            self._reads += 1
            if self._reads > 1:
                raise EOFError
            return super().get(*args, **kwargs)

    in_queue = _DisconnectAfterFirstQueue()
    out_queue: Queue = Queue()
    in_queue.put(text_item("hello"))
    worker_loop(
        BatchTextReverser(size=4, timeout=1.0),
        in_queue,
        out_queue,
        ErrorManager(),
        RetryManager(),
        Event(),
    )
    assert out_queue.empty()
