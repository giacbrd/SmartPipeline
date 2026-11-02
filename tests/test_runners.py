import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager, get_context
from queue import Queue
from threading import Event

import pytest

from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.runners import (
    OnEndErrors,
    _call_on_end_after_abort,
    _call_on_end_best_effort,
    _call_on_start,
    _raise_on_end_errors,
    batch_stage_runner,
    stage_runner,
)
from smartpipeline.utils import ProcessCounter, ThreadCounter
from tests.utils import (
    QUEUE_TIMEOUT,
    BatchTextGenerator,
    BrokenOnEndErrorManager,
    BrokenOnEndStage,
    BrokenProcessAndEndBatchStage,
    BrokenProcessAndEndStage,
    CustomizableBrokenBatchStage,
    CustomizableBrokenStage,
    ErrorSerializableStage,
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
    """Stage and batch runners drain managed queues with shared counters."""
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
    assert out_queue.get(block=True, timeout=QUEUE_TIMEOUT).data["file"]

    for _ in range(2):
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
            assert out_queue.get(block=True, timeout=QUEUE_TIMEOUT).data["file"]
        assert out_queue.empty()


def test_batch_runner_in_executors():
    """Batch runners drain queues inside process and thread executors."""
    manager = Manager()
    in_queue = manager.Queue()
    out_queue = manager.Queue()

    # batch stages always run concurrently, both on processes and threads
    terminated = manager.Event()
    with ProcessPoolExecutor(mp_context=get_context("spawn")) as executor:
        future = executor.submit(
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
            assert out_queue.get(block=True, timeout=QUEUE_TIMEOUT).data["file"]
        item = Item()
        item.data["file"] = "f"
        in_queue.put(item)
        assert str(future.exception(timeout=QUEUE_TIMEOUT)) == "bad item"
        terminated.set()

    terminated = Event()
    in_queue = Queue()
    out_queue = Queue()
    stage = SerializableBatchStage(100, 1)
    stage.on_start()
    with ThreadPoolExecutor() as executor:
        future = executor.submit(
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
            assert out_queue.get(block=True, timeout=QUEUE_TIMEOUT).data["file"]
        item = Item()
        item.data["file"] = "f"
        in_queue.put(item)
        assert str(future.exception(timeout=QUEUE_TIMEOUT)) == "bad item"
        terminated.set()


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


def test_stage_runner_aborts_output_on_fatal():
    """A stage runner drops items and Stop instead of blocking on a full queue once fatal is set."""
    fatal = Event()
    fatal.set()
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item())
    counter = ThreadCounter()
    in_queue: Queue = Queue()
    in_queue.put(text_item())
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
    in_queue = Queue()
    in_queue.put(Stop())
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


def test_batch_runner_aborts_output_on_fatal():
    """A batch runner drops batches and Stop instead of blocking on a full queue once fatal is set."""
    fatal = Event()
    fatal.set()
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(text_item())
    counter = ThreadCounter()
    in_queue: Queue = Queue()
    in_queue.put(text_item())
    in_queue.put(text_item())
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
    in_queue = Queue()
    in_queue.put(Stop())
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


def test_on_start_failure_sets_fatal_and_raises():
    """A worker on_start failure raises and unblocks the shared fatal event."""
    fatal = Event()
    with pytest.raises(IOError):
        _call_on_start(ErrorManager(), ErrorSerializableStage(), fatal)
    assert fatal.is_set()


def test_on_end_helpers_report_both_failures(caplog):
    """Both on_end hooks are attempted, their errors reported, the abort path only logs."""
    fatal = Event()
    result = _call_on_end_best_effort(BrokenOnEndErrorManager(), BrokenOnEndStage())
    assert len(result.exceptions) == 2
    with pytest.raises(ExceptionGroup, match="failed on_end"):
        _raise_on_end_errors(result, BrokenOnEndStage(), fatal)
    assert fatal.is_set()
    with caplog.at_level(logging.ERROR):
        _call_on_end_after_abort(True, BrokenOnEndErrorManager(), BrokenOnEndStage())
    assert any(
        "aborted put" in record.getMessage().lower() for record in caplog.records
    )


def test_stage_runner_logs_on_end_failure_after_process_error(caplog):
    """A process worker logs its on_end failure without masking the process error."""
    manager = Manager()
    in_queue: Queue = Queue()
    in_queue.put(Item())
    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError, match="test process failure"):
            stage_runner(
                BrokenProcessAndEndStage(),
                in_queue,
                Queue(),
                ErrorManager().raise_on_critical_error(),
                RetryManager(),
                Event(),
                ProcessCounter(manager),
                ProcessCounter(manager),
                None,
            )
    assert any(
        "processing error" in record.getMessage().lower() for record in caplog.records
    )


def test_batch_runner_logs_on_end_failure_after_process_error(caplog):
    """A batch process worker logs its on_end failure without masking the process error."""
    manager = Manager()
    in_queue: Queue = Queue()
    in_queue.put(Item())
    in_queue.put(Item())
    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError, match="test batch process failure"):
            batch_stage_runner(
                BrokenProcessAndEndBatchStage(),
                in_queue,
                Queue(),
                ErrorManager().raise_on_critical_error(),
                RetryManager(),
                Event(),
                ProcessCounter(manager),
                ProcessCounter(manager),
                None,
            )
    assert any(
        "processing error" in record.getMessage().lower() for record in caplog.records
    )
