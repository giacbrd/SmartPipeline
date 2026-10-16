import logging
from logging.handlers import QueueHandler
from queue import Queue
from threading import Event, Thread

import pytest

from smartpipeline.defaults import LOGS_STOP_TIMEOUT
from smartpipeline.pipeline import _stage_initialization_with_logger
from smartpipeline.utils import LogsReceiver
from tests.utils import Logger, RandomTextSource, TextReverser, get_pipeline

__author__ = "Giacomo Berardi <giacbrd.com>"


def _log_record(message):
    return logging.LogRecord(
        "smartpipeline-test", logging.INFO, __file__, 1, message, None, None
    )


def test_logging_pipeline_forwards_stage_records(caplog):
    """Stage logs from thread and process workers reach the root logger."""
    with caplog.at_level(logging.INFO):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(100))
            .append("logger1", Logger(), concurrency=2)
            .append("reverser0", TextReverser(), concurrency=1)
            .build()
        )
        list(pipeline.run())
        messages = [
            record for record in caplog.records if record.levelno == logging.INFO
        ]
        assert len(messages) == 100
        assert all("logging item " in record.msg.lower() for record in messages)
        caplog.clear()
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append_concurrently("logger1", Logger, concurrency=1, executor="processes")
            .append("reverser0", TextReverser(), concurrency=1)
            .build()
        )
        list(pipeline.run())
        messages = [
            record for record in caplog.records if record.levelno == logging.INFO
        ]
        assert len(messages) == 10
        assert all("logging item " in record.msg.lower() for record in messages)
        assert any(
            "testing concurrent initialization" in record.msg.lower()
            for record in caplog.records
        )
        caplog.clear()
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(100))
            .append("reverser1", TextReverser(), concurrency=2)
            .append("logger2", Logger(), concurrency=2, executor="processes")
            .append("reverser2", TextReverser())
            .build()
        )
        list(pipeline.run())
        messages = [
            record for record in caplog.records if record.levelno == logging.INFO
        ]
        assert len(messages) == 100
        assert all("logging item " in record.msg.lower() for record in messages)

        assert isinstance(
            _stage_initialization_with_logger(Queue(), TextReverser, [], {}),
            TextReverser,
        )
        assert any(isinstance(h, QueueHandler) for h in logging.getLogger().handlers)

        with pytest.raises(AttributeError):
            get_pipeline().append(
                "reverser", TextReverser()
            ).build()._get_logs_receiver_queue()


def test_logs_receiver_stop(caplog):
    """stopping the logs receiver must not wait for records received after the sentinel"""
    with caplog.at_level(logging.INFO):
        logs_queue = Queue()
        receiver = LogsReceiver(logs_queue)
        receiver.start()
        assert receiver.queue is logs_queue
        logs_queue.put(_log_record("first record"))
        # a stage which is dying can log after the sentinel: that record is received by nobody
        # and its task is never done, so the queue cannot be joined
        logs_queue.put(None)
        logs_queue.put(_log_record("lost record"))
        stopper = Thread(target=receiver.stop, daemon=True)
        stopper.start()
        stopper.join(LOGS_STOP_TIMEOUT * 2)
        assert not stopper.is_alive(), "the logs receiver has not been stopped"
        assert len([r for r in caplog.records if r.levelno == logging.INFO]) == 1
    # stopping a receiver which is not running must not raise
    receiver.stop()
    LogsReceiver(Queue()).stop()


def test_logs_receiver_eof_ends_thread():
    """The LogsReceiver thread ends when its queue raises EOF."""

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
    """Stopping warns instead of blocking when the receiver thread is stuck."""
    done = Event()
    sleeper = Thread(target=lambda: done.wait(10), daemon=True)
    sleeper.start()
    receiver = LogsReceiver(Queue(), stop_timeout=0.01)
    receiver._thread = sleeper
    try:
        with caplog.at_level(logging.WARNING):
            receiver.stop()
        assert receiver._thread is sleeper
        assert any("did not end" in record.msg for record in caplog.records)
    finally:
        done.set()
        sleeper.join(timeout=2)


def test_logs_receiver_double_start():
    """Starting an already running LogsReceiver is a no-op."""
    receiver = LogsReceiver(Queue())
    receiver.start()
    first = receiver._thread
    receiver.start()
    assert receiver._thread is first
    receiver.stop()
    assert receiver._thread is None
