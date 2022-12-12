from __future__ import annotations

import logging
import queue
import time
from logging.handlers import QueueHandler
from threading import Event
from typing import Callable, List, Optional, Sequence

from smartpipeline.defaults import CONCURRENCY_WAIT
from smartpipeline.error.exceptions import RetryError
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.stage import BatchStage, ItemsQueue, Stage, StageType
from smartpipeline.utils import ConcurrentCounter, ProcessCounter

__author__ = "Giacomo Berardi <giacbrd.com>"


def process(
    stage: Stage,
    item: Item,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
) -> Item:
    """
    Execute the :meth:`.stage.Stage.process` method of a stage for an item
    """
    if error_manager.check_critical_errors(item):
        return item
    time1 = time.time()
    # keeping track of caught exceptions
    caught_retryable_exceptions = []
    while (
        len(caught_retryable_exceptions) <= retry_manager.max_retries
        or retry_manager.max_retries == 0
    ):
        try:
            stage.logger.debug("%s is processing %s", stage, item)
            processed_item = stage.process(item)
            stage.logger.debug("%s has finished processing %s", stage, processed_item)
            # this can't be in a finally, otherwise it would register the `error_manager.handle` time
            processed_item.set_timing(stage.name, time.time() - time1)
            return processed_item
        except retry_manager.retryable_errors as rexc:
            caught_retryable_exceptions.append(rexc)
            if (
                retry_manager.max_retries == 0
            ):  # we have already done an attempt, no more retries
                break  # breaking the loop and attach to the item a RetryError
            time.sleep(
                pow(2, len(caught_retryable_exceptions) - 1) * retry_manager.backoff
            )
        except Exception as e:
            stage.logger.debug("%s has failed processing %s", stage, item)
            item.set_timing(stage.name, time.time() - time1)
            error_manager.handle(e, stage, item)
            return item
    stage.logger.debug(
        "%s has failed processing the %s many times with %s errors",
        stage,
        item,
        len(caught_retryable_exceptions),
    )
    item.set_timing(stage.name, time.time() - time1)
    for rexc in caught_retryable_exceptions:
        error_manager.handle(RetryError().with_exception(rexc), stage, item)
    return item


def process_batch(
    stage: BatchStage,
    items: Sequence[Item],
    error_manager: ErrorManager,
    retry_manager: RetryManager,
) -> List[Optional[Item]]:
    """
    Execute the :meth:`.stage.BatchStage.process_batch` method of a batch stage for a batch of items
    """
    ret: List[Optional[Item]] = [None] * len(items)
    to_process = {}
    for i, item in enumerate(items):
        if error_manager.check_critical_errors(item):
            ret[i] = item
        else:
            stage.logger.debug("%s is going to process %s", stage, item)
            to_process[i] = item
    time1 = time.time()
    # keeping track of caught exceptions
    caught_retryable_exceptions = []
    while (
        len(caught_retryable_exceptions) <= retry_manager.max_retries
        or retry_manager.max_retries == 0
    ):
        try:
            stage.logger.debug("%s is processing %s items", stage, len(to_process))
            processed = stage.process_batch(list(to_process.values()))
            stage.logger.debug(
                "%s has finished processing %s items", stage, len(to_process)
            )
            spent = (time.time() - time1) / (len(to_process) or 1.0)
            for n, i in enumerate(to_process.keys()):
                item = processed[n]
                item.set_timing(stage.name, spent)
                ret[i] = item
            return ret
        except retry_manager.retryable_errors as rexc:
            caught_retryable_exceptions.append(rexc)
            if (
                retry_manager.max_retries == 0
            ):  # we have already done an attempt, no more retries
                break  # breaking the loop and attach to the item a RetryError
            time.sleep(
                pow(2, len(caught_retryable_exceptions) - 1) * retry_manager.backoff
            )
        except Exception as e:
            stage.logger.debug(
                "%s had failures in processing %s items", stage, len(to_process)
            )
            spent = (time.time() - time1) / (len(to_process) or 1.0)
            for i, item in to_process.items():
                item.set_timing(stage.name, spent)
                error_manager.handle(e, stage, item)
                ret[i] = item
            return ret
    stage.logger.debug(
        "%s has failed in processing %s items many times with %s errors",
        stage,
        len(to_process),
        len(caught_retryable_exceptions),
    )
    spent = (time.time() - time1) / (len(to_process) or 1.0)
    for i, item in to_process.items():
        item.set_timing(stage.name, spent)
        for rexc in caught_retryable_exceptions:
            error_manager.handle(RetryError().with_exception(rexc), stage, item)
        ret[i] = item
    return ret


def stage_executor(
    stage: Stage,
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    terminated: Event,
    has_started_counter: ConcurrentCounter,
    counter: ConcurrentCounter,
    logs_queue: queue.Queue[logging.LogRecord],
    queue_timeout: float = CONCURRENCY_WAIT,
):
    """
    Consume items from an input queue, process and put them in an output queue, indefinitely,
    until a termination event is set
    """
    if isinstance(counter, ProcessCounter):
        if logs_queue is not None:
            root_logger = logging.getLogger()
            # only by comparing string of queues we obtain their "original" address
            if not any(
                isinstance(handler, QueueHandler)
                and str(handler.queue) == str(logs_queue)
                for handler in root_logger.handlers
            ):
                handler = QueueHandler(logs_queue)
                root_logger.addHandler(handler)
        # call these only if the stage and the error manager are copies of the original,
        # ergo this executor is running in a child process
        error_manager.on_start()
        stage.on_start()
    has_started_counter += 1
    while True:
        if terminated.is_set() and in_queue.empty():
            if isinstance(counter, ProcessCounter):
                error_manager.on_end()
                stage.on_end()
            return
        try:
            item = in_queue.get(block=True, timeout=queue_timeout)
        except queue.Empty:
            continue
        if isinstance(item, Stop):
            out_queue.put(item, block=True)
            in_queue.task_done()
        elif item is not None:
            try:
                item = process(stage, item, error_manager, retry_manager)
            except Exception as e:
                if isinstance(counter, ProcessCounter):
                    error_manager.on_end()
                    stage.on_end()
                raise e
            else:
                if item is not None:
                    out_queue.put(item, block=True)
                    if not isinstance(item, Stop):
                        counter += 1
            finally:
                in_queue.task_done()


def batch_stage_executor(
    stage: BatchStage,
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    terminated: Event,
    has_started_counter: ConcurrentCounter,
    counter: ConcurrentCounter,
    logs_queue: queue.Queue[logging.LogRecord],
):
    """
    Consume items in batches from an input queue, process and put them in an output queue, indefinitely,
    until a termination event is set
    """
    if isinstance(counter, ProcessCounter):
        if logs_queue is not None:
            root_logger = logging.getLogger()
            # only by comparing string of queues we obtain their "original" address
            if not any(
                isinstance(handler, QueueHandler)
                and str(handler.queue) == str(logs_queue)
                for handler in root_logger.handlers
            ):
                handler = QueueHandler(logs_queue)
                root_logger.addHandler(handler)
        # call these only if the stage and the error manager are copies of the original,
        # ergo this executor is running in a child process
        error_manager.on_start()
        stage.on_start()
    has_started_counter += 1
    while True:
        if terminated.is_set() and in_queue.empty():
            if isinstance(counter, ProcessCounter):
                error_manager.on_end()
                stage.on_end()
            return
        items = []
        try:
            for _ in range(stage.size):
                item = in_queue.get(block=True, timeout=stage.timeout)
                # give priority to the Stop event item
                if isinstance(item, Stop):
                    out_queue.put(item, block=True)
                elif item is not None:
                    items.append(item)
                in_queue.task_done()
        except queue.Empty:
            if not any(items):
                continue
        if any(items):
            try:
                items = process_batch(stage, items, error_manager, retry_manager)
            except Exception as e:
                if isinstance(counter, ProcessCounter):
                    error_manager.on_end()
                    stage.on_end()
                raise e
            else:
                for item in items:
                    if item is not None:
                        out_queue.put(item, block=True)
                        if not isinstance(item, Stop):
                            counter += 1


StageExecutor = Callable[
    [StageType, ItemsQueue, ItemsQueue, ErrorManager, Event, ConcurrentCounter], None
]
