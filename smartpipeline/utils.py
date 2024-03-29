from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from multiprocessing.managers import SyncManager
from queue import Queue

__author__ = "Giacomo Berardi <giacbrd.com>"

from typing import Any, OrderedDict


def last_key(ordered_dict: OrderedDict[str, Any]) -> str:
    return next(reversed(ordered_dict.keys()))


class ConcurrentCounter(ABC):
    """
    Interface for a counter that is safe for concurrent access
    """

    @abstractmethod
    def __iadd__(self, incr: int) -> ConcurrentCounter:
        return self

    @property
    @abstractmethod
    def value(self) -> int:
        return 0


class ThreadCounter(ConcurrentCounter):
    """
    Thread safe counter
    """

    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def __iadd__(self, incr: int) -> ThreadCounter:
        with self._lock:
            self._value += incr
        return self

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


class ProcessCounter(ConcurrentCounter):
    """
    Process safe counter
    """

    def __init__(self, manager: SyncManager):
        self._value = manager.Value("i", 0)
        self._lock = manager.Lock()

    def __iadd__(self, incr: int) -> ProcessCounter:
        with self._lock:
            self._value.value += incr
        return self

    @property
    def value(self) -> int:
        with self._lock:
            return self._value.value


class LogsReceiver:
    """Read from the queue where sub-processes send their log records and logs them in the main process"""

    def __init__(self, logs_queue: Queue):
        self._logs_queue = logs_queue
        self._thread = None

    def start(self):
        if self._thread is None:

            def _receiver(queue):
                while True:
                    record = queue.get()
                    if record is None:
                        queue.task_done()
                        break
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
                    queue.task_done()

            self._thread = threading.Thread(target=_receiver, args=(self._logs_queue,))
            self._thread.start()

    def stop(self):
        if self._thread is not None:
            self._logs_queue.put(None)
            self._logs_queue.join()
            self._thread.join()
        self._thread = None

    @property
    def queue(self) -> Queue:
        return self._logs_queue
