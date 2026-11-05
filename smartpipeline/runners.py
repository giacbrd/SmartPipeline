from __future__ import annotations

import logging
import queue
import time
from logging.handlers import QueueHandler
from threading import Event
from typing import Callable, List, Optional, Protocol, Sequence, TypeVar

from smartpipeline.defaults import CONCURRENCY_WAIT
from smartpipeline.error.exceptions import BatchOutputError, RetryError
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import DataT, Item, Stop
from smartpipeline.stage import BatchStage, ItemsQueue, Stage, StageType
from smartpipeline.utils import (
    ConcurrentCounter,
    ProcessCounter,
    put_or_abort,
    raise_collected_errors,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

StageTypeContravariant = TypeVar(
    "StageTypeContravariant", bound=StageType, contravariant=True
)

T = TypeVar("T")


def _setup_process_logging(
    logs_queue: Optional[queue.Queue[logging.LogRecord]],
) -> None:
    """Forward worker process logs to the main process through `logs_queue`."""
    if logs_queue is None:
        return
    root_logger = logging.getLogger()
    # only by comparing string of queues we obtain their "original" address
    if not any(
        isinstance(handler, QueueHandler) and str(handler.queue) == str(logs_queue)
        for handler in root_logger.handlers
    ):
        root_logger.addHandler(QueueHandler(logs_queue))


def _call_on_start(
    error_manager: ErrorManager,
    stage: StageType,
    fatal_event: Optional[Event] = None,
) -> None:
    """
    Run `on_start` lifecycle hooks of a worker copy.

    Any failure is fatal for the whole pipeline: waiting executions must be
    unblocked through `fatal_event` and the caller must see the exception
    instead of hanging without feedback.
    """
    try:
        # call these only if the stage and the error manager are copies of the original,
        # ergo this executor is running in a child process
        error_manager.on_start()
        stage.on_start()
    except Exception:
        stage.logger.exception("%s failed on_start", stage)
        if fatal_event is not None:
            fatal_event.set()
        raise


def _call_on_end_best_effort(
    error_manager: ErrorManager,
    stage: StageType,
) -> list[Exception]:
    """
    Run `on_end` lifecycle hooks attempting both, never raising.

    :return: The raised errors, error-manager first, skipping hooks that succeeded.
    """
    errors: list[Exception] = []
    try:
        error_manager.on_end()
    except Exception as e:
        errors.append(e)
    try:
        stage.on_end()
    except Exception as e:
        errors.append(e)
    return errors


def _raise_on_end_errors(
    errors: list[Exception],
    stage: StageType,
    fatal_event: Optional[Event] = None,
) -> None:
    """Report `on_end` failures as fatal pipeline errors."""
    if not errors:
        return
    stage.logger.exception("%s failed on_end", stage)
    if fatal_event is not None:
        fatal_event.set()
    raise_collected_errors(errors, f"{stage} failed on_end")


def _call_on_end_after_abort(
    on_multiprocess: bool,
    error_manager: ErrorManager,
    stage: StageType,
) -> None:
    """
    Close a worker copy which is exiting because its output was dropped.

    A worker copy owns its `on_start` resources, so it must still run `on_end`
    when a put is aborted by termination or by a fatal error elsewhere. Failures
    are logged: the original termination error must not be masked.
    """
    if not on_multiprocess:
        return
    for end_error in _call_on_end_best_effort(error_manager, stage):
        stage.logger.exception(
            "%s failed on_end after an aborted put", stage, exc_info=end_error
        )


def _retry(call: Callable[[], T], retry_manager: RetryManager) -> T | RetryError:
    """
    Run `call`, retrying only `retry_manager.retryable_errors` with exponential backoff.

    Sleeps `2**(n-1) * backoff` between attempts, never after the last one, and
    returns the built `RetryError` on exhaustion. Other exceptions propagate.
    """
    caught: list[Exception] = []
    while True:
        try:
            return call()
        except retry_manager.retryable_errors as exc:
            if isinstance(exc, BatchOutputError):
                # A broken batch contract is a programming error, never retry it.
                raise
            caught.append(exc)
            if len(caught) > retry_manager.max_retries:
                break
            time.sleep(pow(2, len(caught) - 1) * retry_manager.backoff)
    last = caught[-1]
    attempts = "; ".join(f"{type(e).__name__}: {e}" for e in caught)
    error = RetryError(f"failed after {len(caught)} attempts: {attempts}")
    error.with_exception(last)
    return error


def process(
    stage: Stage[DataT],
    item: Item[DataT],
    error_manager: ErrorManager,
    retry_manager: RetryManager,
) -> Item[DataT]:
    """
    Execute the :meth:`.stage.Stage.process` method of a stage for an item
    """
    if error_manager.check_critical_errors(item):
        return item
    time1 = time.perf_counter()

    def _call() -> Item[DataT]:
        stage.logger.debug("%s is processing %s", stage, item)
        processed_item = stage.process(item)
        stage.logger.debug("%s has finished processing %s", stage, processed_item)
        # this can't be in a finally, otherwise it would register the `error_manager.handle` time
        return processed_item

    try:
        result = _retry(_call, retry_manager)
    except Exception as e:
        stage.logger.debug("%s has failed processing %s", stage, item)
        item.set_timing(stage.name, time.perf_counter() - time1)
        error_manager.handle(e, stage, item)
        return item
    if isinstance(result, RetryError):
        stage.logger.debug(
            "%s has failed processing the %s many times with %s errors",
            stage,
            item,
            retry_manager.max_retries + 1,
        )
        item.set_timing(stage.name, time.perf_counter() - time1)
        error_manager.handle(result, stage, item)
        return item
    result.set_timing(stage.name, time.perf_counter() - time1)
    return result


def process_batch(
    stage: BatchStage[DataT],
    items: Sequence[Item[DataT]],
    error_manager: ErrorManager,
    retry_manager: RetryManager,
) -> List[Optional[Item[DataT]]]:
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

    def _call() -> Sequence[Item[DataT]]:
        stage.logger.debug("%s is processing %s items", stage, len(to_process))
        processed = stage.process_batch(list(to_process.values()))
        if len(processed) != len(to_process):
            raise BatchOutputError(
                f"{stage} returned {len(processed)} items for a batch of "
                f"{len(to_process)}; BatchStage.process_batch() must return "
                "exactly one item per input item"
            )
        stage.logger.debug(
            "%s has finished processing %s items", stage, len(to_process)
        )
        return processed

    try:
        result = _retry(_call, retry_manager)
    except BatchOutputError:
        # A broken batch contract is a programming error: it must not be
        # retried nor downgraded to a per-item error.
        raise
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
    if isinstance(result, RetryError):
        stage.logger.debug(
            "%s has failed in processing %s items many times with %s errors",
            stage,
            len(to_process),
            retry_manager.max_retries + 1,
        )
        spent = (time.perf_counter() - time1) / (len(to_process) or 1.0)
        for i, item in to_process.items():
            item.set_timing(stage.name, spent)
            error_manager.handle(result, stage, item)
            ret[i] = item
        return ret
    spent = (time.perf_counter() - time1) / (len(to_process) or 1.0)
    for n, i in enumerate(to_process.keys()):
        item = result[n]
        item.set_timing(stage.name, spent)
        ret[i] = item
    return ret


def stage_runner(
    stage: Stage[DataT],
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
        _setup_process_logging(logs_queue)
        _call_on_start(error_manager, stage, fatal_event)
    has_started_counter += 1
    while True:
        if terminated.is_set() and in_queue.empty():
            if on_multiprocess:
                _raise_on_end_errors(
                    _call_on_end_best_effort(error_manager, stage),
                    stage,
                    fatal_event,
                )
            return
        try:
            item = in_queue.get(block=True, timeout=CONCURRENCY_WAIT)
        except queue.Empty:
            continue
        if isinstance(item, Stop):
            stopped = put_or_abort(out_queue, item, *guards)
            in_queue.task_done()
            if not stopped:
                _call_on_end_after_abort(on_multiprocess, error_manager, stage)
                return
        elif item is not None:
            try:
                item = process(stage, item, error_manager, retry_manager)
            except Exception:
                # alert any other execution blocked on a queue that this runner is not
                # able to consume items anymore
                if fatal_event is not None:
                    fatal_event.set()
                if on_multiprocess:
                    # best effort closing, the original processing error is kept
                    for end_error in _call_on_end_best_effort(error_manager, stage):
                        stage.logger.exception(
                            "%s failed on_end after a processing error",
                            stage,
                            exc_info=end_error,
                        )
                raise
            else:
                aborted = False
                if item is not None:
                    aborted = not put_or_abort(out_queue, item, *guards)
                    if not aborted and not isinstance(item, Stop):
                        counter += 1
                if aborted:
                    _call_on_end_after_abort(on_multiprocess, error_manager, stage)
                    return
            finally:
                in_queue.task_done()


def batch_stage_runner(
    stage: BatchStage[DataT],
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
        _setup_process_logging(logs_queue)
        _call_on_start(error_manager, stage, fatal_event)
    has_started_counter += 1
    while True:
        if terminated.is_set() and in_queue.empty():
            if on_multiprocess:
                _raise_on_end_errors(
                    _call_on_end_best_effort(error_manager, stage),
                    stage,
                    fatal_event,
                )
            return
        items: List[Item[DataT]] = []
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
            except Exception:
                # alert any other execution blocked on a queue that this runner is not
                # able to consume items anymore
                if fatal_event is not None:
                    fatal_event.set()
                if on_multiprocess:
                    # best effort closing, the original processing error is kept
                    for end_error in _call_on_end_best_effort(error_manager, stage):
                        stage.logger.exception(
                            "%s failed on_end after a processing error",
                            stage,
                            exc_info=end_error,
                        )
                raise
            else:
                for final_item in processed_items:
                    if final_item is not None:
                        if not put_or_abort(out_queue, final_item, *guards):
                            _call_on_end_after_abort(
                                on_multiprocess, error_manager, stage
                            )
                            return
                        if not isinstance(final_item, Stop):
                            counter += 1
        if got_stop:
            if not put_or_abort(out_queue, Stop(), *guards):
                _call_on_end_after_abort(on_multiprocess, error_manager, stage)
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
    ) -> None: ...
