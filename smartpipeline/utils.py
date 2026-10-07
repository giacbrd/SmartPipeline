from __future__ import annotations

import logging
import queue
import threading
from abc import ABC, abstractmethod
from multiprocessing.managers import SyncManager
from queue import Queue

__author__ = "Giacomo Berardi <giacbrd.com>"

from typing import Any, Callable, Optional, OrderedDict

from smartpipeline.defaults import CONCURRENCY_WAIT, LOGS_STOP_TIMEOUT
from smartpipeline.item import Item
from smartpipeline.stage import ItemsQueue


def last_key(ordered_dict: OrderedDict[str, Any]) -> str:
    return next(reversed(ordered_dict.keys()))


def put_or_abort(
    items_queue: ItemsQueue,
    item: Optional[Item],
    *guards: Callable[[], bool],
    timeout: float = CONCURRENCY_WAIT,
) -> bool:
    """
    Put an item in a queue without ever blocking indefinitely.

    When the queue is full, the `guards` conditions are recurrently checked and, as soon as
    any of them is met, the item is dropped and `False` is returned, so that a producer is
    never stuck waiting for consumers which are not able to consume anymore (e.g. dead
    stage runners, or a pipeline which is terminating).

    :param items_queue: The queue where to put the item
    :param item: The item to put in the queue
    :param guards: Conditions which, when met, abort the put and drop the item
    :param timeout: Time to wait for queue space before checking the `guards`
    :return: True if the item has been put in the queue, False if the put has been aborted
    """
    while True:
        try:
            items_queue.put(item, block=True, timeout=timeout)
            return True
        except queue.Full:
            if any(guard() for guard in guards):
                return False


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

    def __init__(self, logs_queue: Queue, stop_timeout: float = LOGS_STOP_TIMEOUT):
        """
        :param logs_queue: The queue where log records are sent
        :param stop_timeout: Time to wait for the receiver thread to end when stopping
        """
        self._logs_queue = logs_queue
        self._stop_timeout = stop_timeout
        self._thread = None

    def start(self):
        if self._thread is None:

            def _receiver(queue):
                while True:
                    try:
                        record = queue.get()
                    except EOFError:
                        # the queue has been closed, e.g. by the multiprocessing manager
                        break
                    if record is None:
                        queue.task_done()
                        break
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
                    queue.task_done()

            self._thread = threading.Thread(
                target=_receiver, args=(self._logs_queue,), daemon=True
            )
            self._thread.start()

    def stop(self):
        """
        Send the sentinel to end the receiver thread and wait for it, without ever waiting
        indefinitely: records logged after the sentinel are received by nobody, so the queue
        cannot be joined, as its unfinished tasks would never be marked as done
        """
        if self._thread is not None:
            self._logs_queue.put(None)
            self._thread.join(timeout=self._stop_timeout)
            if self._thread.is_alive():
                logging.warning(
                    "The logs receiver thread did not end in %s seconds",
                    self._stop_timeout,
                )
            else:
                self._thread = None

    @property
    def queue(self) -> Queue:
        return self._logs_queue
