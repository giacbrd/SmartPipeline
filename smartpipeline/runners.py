from __future__ import annotations

import logging
import queue
import time
from logging.handlers import QueueHandler
from threading import Event
from typing import Callable, List, Optional, Protocol, Sequence, TypeVar

from smartpipeline.defaults import CONCURRENCY_WAIT
from smartpipeline.error.exceptions import RetryError
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.stage import BatchStage, ItemsQueue, Stage, StageType
from smartpipeline.utils import ConcurrentCounter, ProcessCounter, put_or_abort

__author__ = "Giacomo Berardi <giacbrd.com>"

StageTypeContravariant = TypeVar(
    "StageTypeContravariant", bound=StageType, contravariant=True
)


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
    time1 = time.perf_counter()
    # keeping track of caught exceptions
    caught_retryable_exceptions: List[Exception] = []
    while (
        len(caught_retryable_exceptions) <= retry_manager.max_retries
        or retry_manager.max_retries == 0
    ):
        try:
            stage.logger.debug("%s is processing %s", stage, item)
            processed_item = stage.process(item)
            stage.logger.debug("%s has finished processing %s", stage, processed_item)
            # this can't be in a finally, otherwise it would register the `error_manager.handle` time
            processed_item.set_timing(stage.name, time.perf_counter() - time1)
            return processed_item
        except retry_manager.retryable_errors as retryable_exc:
            caught_retryable_exceptions.append(retryable_exc)
            if (
                retry_manager.max_retries == 0
            ):  # we have already done an attempt, no more retries
                break  # breaking the loop and attach to the item a RetryError
            time.sleep(
                pow(2, len(caught_retryable_exceptions) - 1) * retry_manager.backoff
            )
        except Exception as e:
            stage.logger.debug("%s has failed processing %s", stage, item)
            item.set_timing(stage.name, time.perf_counter() - time1)
            error_manager.handle(e, stage, item)
            return item
    stage.logger.debug(
        "%s has failed processing the %s many times with %s errors",
        stage,
        item,
        len(caught_retryable_exceptions),
    )
    item.set_timing(stage.name, time.perf_counter() - time1)
    for rexc in caught_retryable_exceptions:
        error_manager.handle(
            RetryError(f"{type(rexc).__name__}: {rexc}").with_exception(rexc),
            stage,
            item,
        )
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
    time1 = time.perf_counter()
    # keeping track of caught exceptions
    caught_retryable_exceptions: List[Exception] = []
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
            spent = (time.perf_counter() - time1) / (len(to_process) or 1.0)
            for n, i in enumerate(to_process.keys()):
                item = processed[n]
                item.set_timing(stage.name, spent)
                ret[i] = item
            return ret
        except retry_manager.retryable_errors as retryable_exc:
            caught_retryable_exceptions.append(retryable_exc)
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
            spent = (time.perf_counter() - time1) / (len(to_process) or 1.0)
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
    spent = (time.perf_counter() - time1) / (len(to_process) or 1.0)
    for i, item in to_process.items():
        item.set_timing(stage.name, spent)
        for rexc in caught_retryable_exceptions:
            error_manager.handle(
                RetryError(f"{type(rexc).__name__}: {rexc}").with_exception(rexc),
                stage,
                item,
            )
        ret[i] = item
    return ret


def stage_runner(
    stage: Stage,
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    terminated: Event,
    has_started_counter: ConcurrentCounter,
    counter: ConcurrentCounter,
    logs_queue: Optional[queue.Queue[logging.LogRecord]],
    fatal_event: Optional[Event] = None,
) -> None:
    """
    Consume items from an input queue, process and put them in an output queue, indefinitely,
    until a termination event is set

    :param fatal_event: Optional event which is set by this runner when it exits because of an
        error, so that any other execution blocked on a queue is able to give up immediately
    """
    on_multiprocess = isinstance(counter, ProcessCounter)
    # a runner always delivers the items it has consumed, even when the termination has been
    # alerted: it exits by itself when its input queue is empty (see the loop below), while a
    # fatal error means that nobody is able to consume the items anymore
    guards: List[Callable[[], bool]] = []
    if fatal_event is not None:
        guards.append(fatal_event.is_set)
    if on_multiprocess:
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
            if on_multiprocess:
                error_manager.on_end()
                stage.on_end()
            return
        try:
            item = in_queue.get(block=True, timeout=CONCURRENCY_WAIT)
        except queue.Empty:
            continue
        if isinstance(item, Stop):
            stopped = put_or_abort(out_queue, item, *guards)
            in_queue.task_done()
            if not stopped:
                return
        elif item is not None:
            try:
                item = process(stage, item, error_manager, retry_manager)
            except Exception as e:
                # alert any other execution blocked on a queue that this runner is not
                # able to consume items anymore
                if fatal_event is not None:
                    fatal_event.set()
                if on_multiprocess:
                    error_manager.on_end()
                    stage.on_end()
                raise e
            else:
                aborted = False
                if item is not None:
                    aborted = not put_or_abort(out_queue, item, *guards)
                    if not aborted and not isinstance(item, Stop):
                        counter += 1
                if aborted:
                    return
            finally:
                in_queue.task_done()


def batch_stage_runner(
    stage: BatchStage,
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    terminated: Event,
    has_started_counter: ConcurrentCounter,
    counter: ConcurrentCounter,
    logs_queue: Optional[queue.Queue[logging.LogRecord]],
    fatal_event: Optional[Event] = None,
) -> None:
    """
    Consume items in batches from an input queue, process and put them in an output queue, indefinitely,
    until a termination event is set

    :param fatal_event: Optional event which is set by this runner when it exits because of an
        error, so that any other execution blocked on a queue is able to give up immediately
    """
    on_multiprocess = isinstance(counter, ProcessCounter)
    # a runner always delivers the items it has consumed, even when the termination has been
    # alerted: it exits by itself when its input queue is empty (see the loop below), while a
    # fatal error means that nobody is able to consume the items anymore
    guards: List[Callable[[], bool]] = []
    if fatal_event is not None:
        guards.append(fatal_event.is_set)
    if on_multiprocess:
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
            if on_multiprocess:
                error_manager.on_end()
                stage.on_end()
            return
        items: List[Item] = []
        got_stop = False
        try:
            for _ in range(stage.size):
                item = in_queue.get(block=True, timeout=stage.timeout)
                # the Stop item is always the last one put in the output queue, so it is
                # forwarded only after the items of this batch have been put: nothing else
                # will arrive after it, so there is no need to wait for a full batch
                if isinstance(item, Stop):
                    got_stop = True
                elif item is not None:
                    items.append(item)
                in_queue.task_done()
                if got_stop:
                    break
        except queue.Empty:
            if not any(items) and not got_stop:
                continue
        if any(items):
            try:
                processed_items = process_batch(
                    stage, items, error_manager, retry_manager
                )
            except Exception as e:
                # alert any other execution blocked on a queue that this runner is not
                # able to consume items anymore
                if fatal_event is not None:
                    fatal_event.set()
                if on_multiprocess:
                    error_manager.on_end()
                    stage.on_end()
                raise e
            else:
                for final_item in processed_items:
                    if final_item is not None:
                        if not put_or_abort(out_queue, final_item, *guards):
                            return
                        if not isinstance(final_item, Stop):
                            counter += 1
        if got_stop:
            if not put_or_abort(out_queue, Stop(), *guards):
                return


class StageRunner(Protocol[StageTypeContravariant]):
    """
    Type of the functions which run a stage concurrently, consuming and producing items
    from/to queues, until the `terminated` event is set
    """

    def __call__(
        self,
        stage: StageTypeContravariant,
        in_queue: ItemsQueue,
        out_queue: ItemsQueue,
        error_manager: ErrorManager,
        retry_manager: RetryManager,
        terminated: Event,
        has_started_counter: ConcurrentCounter,
        counter: ConcurrentCounter,
        logs_queue: Optional[queue.Queue[logging.LogRecord]],
        fatal_event: Optional[Event] = None,
    ) -> None:
        ...
