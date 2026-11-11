from __future__ import annotations

import logging
import queue
import threading
from queue import Queue

__author__ = "Giacomo Berardi <giacbrd.com>"

from typing import Callable, Optional, Sequence

from smartpipeline.defaults import CONCURRENCY_WAIT, LOGS_STOP_TIMEOUT
from smartpipeline.item import DataT, Item
from smartpipeline.stage import ItemsQueue


def raise_collected_errors(errors: Sequence[Exception], message: str) -> None:
    """
    Report collected fatal errors without ever hanging silently.

    A single error is re-raised unchanged, so callers keep seeing the original
    exception type; several errors are grouped into one `ExceptionGroup` so none
    is lost. Does nothing when there are no errors.
    """
    if not errors:
        return
    if len(errors) == 1:
        raise errors[0]
    raise ExceptionGroup(message, errors)


def _guards_met(guards: Sequence[Callable[[], bool]]) -> bool:
    """
    True as soon as any guard is met, treating a broken shared state as a guard.

    The guards may read multiprocessing proxies: once the manager has been shut down
    (by a concurrent `close()`) any read on them fails, and there is nothing left to
    produce or consume, so the loop must abort instead of raising.
    """
    for guard in guards:
        try:
            if guard():
                return True
        except (EOFError, OSError):
            return True
    return False


def put_or_abort(
    items_queue: ItemsQueue,
    item: Optional[Item[DataT]],
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
        # Check before putting too: once a guard is met the item must be dropped even
        # if the queue still has room, otherwise a producer can keep feeding a pipeline
        # which has already terminated.
        if _guards_met(guards):
            return False
        try:
            items_queue.put(item, block=True, timeout=timeout)
            return True
        except queue.Full:
            continue
        except (EOFError, OSError):
            # the multiprocessing manager is gone (a concurrent close() shut it down):
            # nothing can be queued anymore, so abort instead of crashing the producer
            return False


def get_or_abort(
    items_queue: ItemsQueue,
    *guards: Callable[[], bool],
    timeout: float = CONCURRENCY_WAIT,
) -> Optional[Item[DataT]]:
    """
    Get an item from a queue without ever blocking indefinitely.

    When the queue is empty, the `guards` conditions are recurrently checked and, as soon as
    any of them is met, `None` is returned, so that a consumer is never stuck waiting for
    producers which are not able to produce anymore (e.g. a failed pipeline).
    Queues never contain `None`, so it unambiguously signals the abort.

    :param items_queue: The queue where to get the item from
    :param guards: Conditions which, when met, abort the get and return `None`
    :param timeout: Time to wait for an item before checking the `guards`
    :return: The item, or `None` when the get has been aborted
    """
    while True:
        if _guards_met(guards):
            return None
        try:
            return items_queue.get(block=True, timeout=timeout)
        except queue.Empty:
            continue
        except (EOFError, OSError):
            # the multiprocessing manager is gone (a concurrent close() shut it down):
            # there is nothing left to read, so abort the get instead of raising
            return None


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
                    except (EOFError, OSError):
                        # the queue has been closed, e.g. by the multiprocessing manager
                        break
                    if record is None:
                        try:
                            queue.task_done()
                        except (EOFError, OSError):
                            pass
                        break
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
                    try:
                        queue.task_done()
                    except (EOFError, OSError):
                        break

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
            try:
                self._logs_queue.put(None)
            except (EOFError, OSError):
                pass
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
