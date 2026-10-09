"""Unit tests for the error-handling paths introduced with the fatal-event rework.

These cover small defensive branches that integration tests never hit
deterministically (termination masking, logs-receiver edge cases, aborted
puts, runner fatal events), which is where the coverage drop comes from.
"""

import logging
import threading
import time
from concurrent.futures import Future
from queue import Queue
from threading import Event
from unittest import mock

import pytest

from smartpipeline.containers import (
    BatchStageContainer,
    ConcurrentStageContainer,
    SourceContainer,
    StageContainer,
)
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.pipeline import Pipeline
from smartpipeline.runners import batch_stage_runner, stage_runner
from smartpipeline.utils import LogsReceiver, ThreadCounter, put_or_abort
from tests.utils import (
    BatchTextGenerator,
    CustomizableBrokenBatchStage,
    CustomizableBrokenStage,
    ErrorSource,
    ListSource,
    RandomTextSource,
    TextReverser,
    get_pipeline,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def _text_item(text="hello"):
    item = Item()
    item.data["text"] = text
    return item


def test_put_or_abort_success():
    q: Queue = Queue(maxsize=1)
    assert put_or_abort(q, _text_item(), timeout=0.01) is True
    assert q.qsize() == 1


def test_put_or_abort_aborts_when_guard_set():
    q: Queue = Queue(maxsize=1)
    q.put(_text_item())
    assert put_or_abort(q, _text_item(), lambda: True, timeout=0.01) is False
    assert q.qsize() == 1


def test_put_or_abort_retries_until_space():
    q: Queue = Queue(maxsize=1)
    q.put(_text_item())

    def _free():
        time.sleep(0.05)
        q.get_nowait()
        q.task_done()

    releaser = threading.Thread(target=_free, daemon=True)
    releaser.start()
    assert put_or_abort(q, _text_item(), lambda: False, timeout=0.01) is True
    releaser.join(timeout=2)


def test_fatal_event_mixin():
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
    container = StageContainer(
        "put-abort", TextReverser(), ErrorManager(), RetryManager()
    )
    container.init_queue(lambda: Queue(maxsize=1))
    container.out_queue.put(_text_item())
    fatal = Event()
    fatal.set()
    container.set_fatal_event(fatal)
    container._put_item(_text_item())
    assert container.has_aborted_put()


def test_batch_container_put_abort_breaks():
    container = BatchStageContainer(
        "batch-put-abort", BatchTextGenerator(size=2), ErrorManager(), RetryManager()
    )
    container.init_queue(lambda: Queue(maxsize=1))
    container.out_queue.put(_text_item())
    fatal = Event()
    fatal.set()
    container.set_fatal_event(fatal)
    container._put_item([_text_item(), _text_item()])
    assert container.has_aborted_put()
    # the second item of the batch is dropped with the abort
    assert container.out_queue.qsize() == 1


def test_empty_out_queue():
    container = StageContainer(
        "empty-test", TextReverser(), ErrorManager(), RetryManager()
    )
    # no output queue: nothing to do, must not raise
    container.empty_out_queue()
    container.init_queue(Queue)
    container.out_queue.put(_text_item())
    container.out_queue.put(_text_item())
    container.empty_out_queue()
    assert container.out_queue.empty()


def test_source_pop_into_queue_aborts_on_fatal():
    container = SourceContainer()
    container.set(ListSource([_text_item() for _ in range(5)]))
    container.init_queue(lambda: Queue(maxsize=1))
    container.out_queue.put(_text_item())
    fatal = Event()
    fatal.set()
    container.set_fatal_event(fatal)
    container.pop_into_queue(Queue())
    assert container._stop_sent is True


def test_source_pop_into_queue_reports_errors():
    container = SourceContainer()
    container.set(ErrorSource())
    container.init_queue(Queue)
    errors: Queue = Queue()
    container.pop_into_queue(errors)
    assert isinstance(errors.get_nowait(), ValueError)


def test_new_shared_event_dispatch():
    pipeline = Pipeline()
    event = pipeline._new_shared_event()
    assert isinstance(event, type(Event()))
    with mock.patch.object(
        pipeline, "_new_mp_event", return_value="mp-event"
    ) as mp_event:
        pipeline._sync_manager = mock.sentinel.manager  # type: ignore
        assert pipeline._new_shared_event() == "mp-event"
        mp_event.assert_called_once_with()


def test_pipeline_is_fatal():
    pipeline = Pipeline()
    assert not pipeline._is_fatal()
    pipeline._fatal_event = Event()
    assert not pipeline._is_fatal()
    pipeline._fatal_event.set()
    assert pipeline._is_fatal()


def test_check_errors_and_wait_for_runner_error():
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    # no concurrent container: nothing to raise, just sleeps
    pipeline._wait_for_runner_error(retries=2, wait_seconds=0.01)
    container = ConcurrentStageContainer(
        "boom",
        TextReverser(),
        ErrorManager(),
        RetryManager(),
        Queue,
        ThreadCounter,
        Event,
        concurrency=1,
    )
    future: Future = Future()
    future.set_exception(ValueError("runner boom"))
    container._futures = [future]
    pipeline._containers["boom"] = container
    with pytest.raises(ValueError):
        pipeline._check_errors()
    with pytest.raises(ValueError):
        pipeline._wait_for_runner_error(retries=1, wait_seconds=0.01)


def test_get_processed_or_abort():
    pipeline = Pipeline()

    class _FakeContainer:
        def __init__(self, items):
            self._items = list(items)

        def get_processed(self, block=True, timeout=None):
            return self._items.pop(0) if self._items else None

    assert pipeline._get_processed_or_abort(_FakeContainer([_text_item()])) is not None  # type: ignore
    assert pipeline._get_processed_or_abort(_FakeContainer([None, _text_item()])) is not None  # type: ignore
    pipeline._fatal_event = Event()
    pipeline._fatal_event.set()
    with mock.patch.object(
        pipeline, "_wait_for_runner_error", side_effect=RuntimeError("runner")
    ):
        with pytest.raises(RuntimeError):
            pipeline._get_processed_or_abort(_FakeContainer([None]))  # type: ignore
    with mock.patch.object(pipeline, "_wait_for_runner_error", return_value=None):
        assert pipeline._get_processed_or_abort(_FakeContainer([None])) is None  # type: ignore


def test_process_propagates_runner_error():
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with mock.patch.object(pipeline, "_is_fatal", return_value=True):
        with mock.patch.object(
            pipeline,
            "_wait_for_runner_error",
            side_effect=RuntimeError("runner down"),
        ):
            with pytest.raises(RuntimeError):
                pipeline.process(_text_item())


def test_exit_run_does_not_mask_termination_errors(caplog):
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    with mock.patch.object(pipeline, "stop", side_effect=RuntimeError("stop boom")):
        with caplog.at_level(logging.ERROR):
            pipeline._exit_run(None, None)
    assert pipeline.count == 1
    assert any(
        "problems in terminating" in record.msg.lower() for record in caplog.records
    )


def test_run_generator_close_terminates():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(5))
        .append("reverser", TextReverser())
        .build()
    )
    gen = pipeline.run()
    first = next(gen)
    assert first.data["text"]
    gen.close()
    assert pipeline._source_container.is_stopped()


def test_logs_receiver_eof_ends_thread():
    class _EOFQueue(Queue):
        def get(self, *args, **kwargs):
            raise EOFError

    receiver = LogsReceiver(_EOFQueue())
    receiver.start()
    receiver._thread.join(timeout=2)
    assert not receiver._thread.is_alive()
    receiver.stop()
    assert receiver._thread is None


def test_logs_receiver_stop_timeout_warns(caplog):
    done = Event()
    sleeper = threading.Thread(target=lambda: done.wait(10), daemon=True)
    sleeper.start()
    receiver = LogsReceiver(Queue(), stop_timeout=0.01)
    receiver._thread = sleeper  # type: ignore
    try:
        with caplog.at_level(logging.WARNING):
            receiver.stop()
        assert receiver._thread is sleeper
        assert any("did not end" in record.msg for record in caplog.records)
    finally:
        done.set()
        sleeper.join(timeout=2)


def test_logs_receiver_double_start():
    receiver = LogsReceiver(Queue())
    receiver.start()
    first = receiver._thread
    receiver.start()
    assert receiver._thread is first
    receiver.stop()
    assert receiver._thread is None


def test_concurrent_shutdown_oserror(caplog):
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

    container._stage_executor = _BoomExecutor()  # type: ignore
    with caplog.at_level(logging.WARNING):
        container.shutdown()
    assert any("shutting down" in record.msg.lower() for record in caplog.records)


def test_stage_runner_sets_fatal_on_error():
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
    in_queue: Queue = Queue()
    in_queue.put(_text_item())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(_text_item())
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
    in_queue: Queue = Queue()
    in_queue.put(Stop())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(_text_item())
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
    in_queue: Queue = Queue()
    in_queue.put(_text_item())
    in_queue.put(_text_item())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(_text_item())
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
    in_queue: Queue = Queue()
    in_queue.put(Stop())
    out_queue: Queue = Queue(maxsize=1)
    out_queue.put(_text_item())
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


def test_process_async_without_callback():
    pipeline = get_pipeline().append("reverser", TextReverser()).build()
    pipeline.process_async(_text_item("hello"))
    pipeline.stop()
    result = pipeline.get_item()
    assert result.data["text"] == "olleh"
