"""Unit tests for the engine internals which no public run can trigger deterministically.

The abort paths of the pipeline threads (source, inline drivers, coordinator) and the
cleanup of broken executors, queues and futures: they must always end quietly, because a
driver thread dying with an exception would end the run with no error to report.
"""

import logging
import threading
from queue import Queue

import pytest

from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Stop
from smartpipeline.segments import ConcurrentSegment, Engine, InlineSegment
from tests.utils import (
    CriticalIOErrorStage,
    CustomizableBrokenStage,
    ErrorSource,
    ListSource,
    TextReverser,
    text_item,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


class _CancelledFuture:
    """A future cancelled before running, which therefore reports no error."""

    def cancelled(self):
        return True

    def exception(self):
        raise AssertionError("a cancelled future is never read")


class _UnreadableFuture:
    """A future which cannot be read, like one of a process pool going down."""

    def cancelled(self):
        return False

    def exception(self):
        raise RuntimeError("the future is unreadable")


class _UncancellableFuture:
    """A future refusing to be cancelled on shutdown."""

    def cancel(self):
        raise RuntimeError("already running")


class _CancelAfter:
    """A cancel flag which trips after the given number of checks."""

    def __init__(self, checks: int):
        self._checks = checks

    def is_set(self) -> bool:
        if self._checks > 0:
            self._checks -= 1
            return False
        return True

    def set(self) -> None:
        self._checks = 0


class _GoneQueue(Queue):
    """A queue which loses its unfinished-tasks tracking, like one of a dead manager."""

    def task_done(self):
        raise EOFError


class _ClosedQueue(Queue):
    """A closed queue: readable until its manager is gone, then it refuses any use."""

    def get_nowait(self):
        raise ValueError("Queue is closed")


class _FailingShutdown:
    """An executor, logs receiver or manager which fails while being shut down."""

    def shutdown(self, *args, **kwargs):
        raise RuntimeError("shutdown failed")

    def stop(self):
        raise RuntimeError("stop failed")


def _inline_segment(*stages) -> InlineSegment:
    return InlineSegment(
        names=[stage.name for stage in stages],
        stages=list(stages),
        retry_managers=[RetryManager() for _ in stages],
    )


def _engine(segments, error_manager=None, source=None) -> Engine:
    return Engine(
        segments,
        error_manager or ErrorManager(),
        source if source is not None else ListSource([]),
    )


def test_done_callback_ignores_a_cancelled_future():
    """A future which never ran is not an error."""
    engine = _engine([])
    engine._done_callback(_CancelledFuture())
    assert not engine.errors
    assert not engine.cancel.is_set()


def test_done_callback_records_an_unreadable_future():
    """A future which cannot be read is recorded as an error, never raised here."""
    engine = _engine([])
    engine._done_callback(_UnreadableFuture())
    assert [type(e) for e in engine.errors] == [RuntimeError]
    assert engine.cancel.is_set()


def test_source_loop_exits_when_the_run_is_cancelled():
    """A cancelled run pulls nothing else from its source."""
    seg = _inline_segment(TextReverser())
    seg.input_queue = Queue()
    engine = _engine([seg], source=ListSource([text_item()]))
    engine.cancel.set()
    engine._source_loop()
    assert seg.input_queue.empty()


def test_inline_driver_exits_when_its_get_is_aborted():
    """A driver which loses its input queue ends without producing anything."""
    seg = _inline_segment(TextReverser())
    seg.input_queue = Queue()
    seg.output_queue = Queue()
    seg.input_queue.put(text_item())
    engine = _engine([seg])
    engine.cancel.set()
    engine._inline_queue_driver(seg, 1)
    assert seg.output_queue.empty()


def test_inline_driver_drops_a_stop_which_cannot_be_forwarded():
    """A stop which cannot reach the next segment still ends the driver."""
    seg = _inline_segment(TextReverser())
    seg.input_queue = Queue()
    seg.output_queue = Queue()
    seg.input_queue.put(Stop())
    engine = _engine([seg])
    # the get succeeds, the propagation of the stop is the first aborted put
    engine.cancel = _CancelAfter(1)
    engine._inline_queue_driver(seg, 1)
    assert seg.output_queue.empty()


def test_coordinator_drops_a_stop_which_cannot_be_forwarded():
    """The coordinator gives up on a next segment which cannot receive the stop."""
    seg = ConcurrentSegment(
        name="stage", stage=TextReverser(), retry_manager=RetryManager(), concurrency=1
    )
    seg.output_queue = Queue()
    engine = _engine([seg])
    engine.cancel = _CancelAfter(1)
    engine._coordinator(seg, 1)
    assert seg.output_queue.empty()


def test_coordinator_records_its_own_failure():
    """A coordinator failing by itself reports the error instead of dying silently."""
    seg = ConcurrentSegment(
        name="stage", stage=TextReverser(), retry_manager=RetryManager(), concurrency=1
    )
    seg.futures = None
    engine = _engine([seg])
    engine._coordinator(seg, 1)
    assert [type(e) for e in engine.errors] == [TypeError]


def test_results_end_when_the_run_is_cancelled():
    """A caller reading an inline run stops as soon as the run is cancelled."""
    engine = _engine(
        [_inline_segment(TextReverser())], source=ListSource([text_item()])
    )
    engine.cancel.set()
    assert list(engine.iter_results()) == []


def test_results_end_when_the_queue_is_gone():
    """A consumer losing its queue mid-read ends the run, it has nothing left to read."""
    first, last = _inline_segment(TextReverser()), _inline_segment(TextReverser())
    queue = _GoneQueue()
    queue.put(text_item())
    first.output_queue = last.input_queue = queue
    engine = _engine([first, last])
    assert list(engine.iter_results()) == []


def test_join_threads_never_joins_the_calling_thread():
    """A shutdown requested from a pipeline thread does not join (and die on) itself."""
    engine = _engine([])
    # joining the current thread raises RuntimeError, so the loop must skip it
    engine._threads.append(threading.current_thread())
    engine.join_threads(timeout=0.01)


def test_drain_queues_survives_a_closed_queue():
    """Draining a queue which is closed is a no-op, not an error."""
    seg = _inline_segment(TextReverser())
    seg.input_queue = _ClosedQueue()
    engine = _engine([seg])
    engine.drain_queues()


def test_shutdown_executors_reports_every_failure_and_clears_them(caplog):
    """A failure tearing down executors, logs receiver or manager never hides the next."""
    engine = _engine([])
    engine._executors.append(_FailingShutdown())
    engine._logs_receiver = _FailingShutdown()
    engine._manager = _FailingShutdown()
    with caplog.at_level(logging.ERROR):
        engine._shutdown_executors()
    assert [r.getMessage() for r in caplog.records] == [
        "Problems in shutting down an executor",
        "Problems in stopping the logs receiver",
        "Problems in shutting down the manager",
    ]
    assert not engine._executors
    assert engine._logs_receiver is None
    assert engine._manager is None


def test_shutdown_survives_an_uncancellable_future():
    """A future refusing the cancellation never breaks the shutdown."""
    seg = ConcurrentSegment(
        name="stage", stage=TextReverser(), retry_manager=RetryManager(), concurrency=1
    )
    seg.futures = [_UncancellableFuture()]
    _engine([seg]).shutdown(run_on_end=False)


def test_inline_segment_critical_error_is_fatal_for_the_run():
    """An inline stage raising a critical error aborts the whole run."""
    seg = _inline_segment(CriticalIOErrorStage())
    seg.input_queue = Queue()
    seg.output_queue = Queue()
    seg.input_queue.put(text_item())
    engine = _engine([seg], error_manager=ErrorManager().raise_on_critical_error())
    engine._inline_queue_driver(seg, 1)
    assert [type(e) for e in engine.errors] == [IOError]
    assert engine.cancel.is_set()
    assert seg.output_queue.empty()


def test_source_loop_records_a_fatal_source_error():
    """A source failing with a non-Exception error reports it and ends its loop."""

    class FatalSource(ListSource):
        def pop(self):
            raise KeyboardInterrupt("test source interrupt")

    seg = _inline_segment(TextReverser())
    seg.input_queue = Queue()
    engine = _engine([seg], source=FatalSource([]))
    engine._source_loop()
    assert [type(e) for e in engine.errors] == [KeyboardInterrupt]
    assert engine.cancel.is_set()


def test_inline_driver_records_a_fatal_stage_error():
    """An inline chain failing with a non-Exception error ends the driver reporting it."""
    seg = _inline_segment(CustomizableBrokenStage([KeyboardInterrupt]))
    seg.input_queue = Queue()
    seg.output_queue = Queue()
    seg.input_queue.put(text_item())
    engine = _engine([seg])
    engine._inline_queue_driver(seg, 1)
    assert [type(e) for e in engine.errors] == [KeyboardInterrupt]
    assert seg.output_queue.empty()


def test_inline_run_reports_a_failing_source():
    """A source failing in the middle of a plain run reaches the caller."""
    engine = _engine([_inline_segment(TextReverser())], source=ErrorSource())
    with pytest.raises(ValueError):
        list(engine.iter_results())
