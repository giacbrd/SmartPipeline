from __future__ import annotations

import logging
import queue
import time
from logging.handlers import QueueHandler
from threading import Event
from typing import Callable, List, Optional, Sequence, TypeVar

from smartpipeline.error.exceptions import BatchOutputError, RetryError
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import DataT, Item, Stop
from smartpipeline.stage import BatchStage, ItemsQueue, Stage
from smartpipeline.utils import get_or_abort, put_or_abort, raise_collected_errors

__author__ = "Giacomo Berardi <giacbrd.com>"


T = TypeVar("T")


def _setup_process_logging(
    logs_queue: Optional[queue.Queue[logging.LogRecord]],
) -> None:
    """Forward worker process logs to the main process through `logs_queue`."""
    if logs_queue is None:
        return
    root_logger = logging.getLogger()
    if not any(
        isinstance(handler, QueueHandler) and str(handler.queue) == str(logs_queue)
        for handler in root_logger.handlers
    ):
        root_logger.addHandler(QueueHandler(logs_queue))


def _call_on_end_best_effort(
    error_manager: ErrorManager,
    stage: Stage | BatchStage,
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
    stage: Stage | BatchStage,
) -> None:
    """Report `on_end` failures as fatal pipeline errors."""
    if not errors:
        return
    stage.logger.exception("%s failed on_end", stage)
    raise_collected_errors(errors, f"{stage} failed on_end")


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


def _single_worker_body(
    stage: Stage[DataT],
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    cancel: Event,
) -> None:
    """Run a plain stage: one blocking get, one process, one abortable put."""
    while True:
        item: Optional[Item[DataT]] = get_or_abort(in_queue, cancel.is_set)
        if item is None:
            return
        in_queue.task_done()
        if isinstance(item, Stop):
            return
        result = process(stage, item, error_manager, retry_manager)
        if not put_or_abort(out_queue, result, cancel.is_set):
            return


def _batch_worker_body(
    stage: BatchStage[DataT],
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    cancel: Event,
) -> None:
    """Run a batch stage: fill up to `size` items, flush on timeout or `Stop`."""
    while True:
        first: Optional[Item[DataT]] = get_or_abort(in_queue, cancel.is_set)
        if first is None:
            return
        in_queue.task_done()
        if isinstance(first, Stop):
            return
        batch: list[Item[DataT]] = [first]
        got_stop = False
        while len(batch) < stage.size:
            if cancel.is_set():
                return
            try:
                nxt = in_queue.get(block=True, timeout=stage.timeout)
            except queue.Empty:
                break
            in_queue.task_done()
            if isinstance(nxt, Stop):
                got_stop = True
                break
            if nxt is not None:
                batch.append(nxt)
        results = process_batch(stage, batch, error_manager, retry_manager)
        for res in results:
            if res is None:
                continue
            if not put_or_abort(out_queue, res, cancel.is_set):
                return
        if got_stop:
            return


def worker_loop(
    stage: Stage[DataT] | BatchStage[DataT],
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    retry_manager: RetryManager,
    cancel: Event,
    logs_queue: Optional[queue.Queue[logging.LogRecord]] = None,
) -> None:
    """
    Run one concurrent worker: consume items, process them, produce results.

    A plain stage pulls one item with a blocking get; a batch stage pulls up to
    `size` items, flushing the partial batch on `timeout`. Results are put with
    `put_or_abort`, so a worker blocked on a full queue exits when `cancel` is set.
    On `Stop` the pending batch is flushed and the worker exits without forwarding
    it: the coordinator forwards `Stop` downstream once every worker is done.
    Processing errors propagate to the done-callback, which records them and sets
    `cancel`. In a worker process (`logs_queue` given) `on_start` runs at entry and
    `on_end` in a `finally`: `on_end` failures are logged when another error is
    already propagating and raised otherwise.
    """
    is_process_worker = logs_queue is not None
    if is_process_worker:
        _setup_process_logging(logs_queue)
        try:
            error_manager.on_start()
            stage.on_start()
        except Exception:
            stage.logger.exception("%s failed on_start", stage)
            raise
        try:
            if isinstance(stage, BatchStage):
                _batch_worker_body(
                    stage, in_queue, out_queue, error_manager, retry_manager, cancel
                )
            else:
                assert isinstance(stage, Stage)
                _single_worker_body(
                    stage, in_queue, out_queue, error_manager, retry_manager, cancel
                )
        except BaseException:
            for end_error in _call_on_end_best_effort(error_manager, stage):
                stage.logger.exception(
                    "%s failed on_end after a processing error",
                    stage,
                    exc_info=end_error,
                )
            raise
        else:
            _raise_on_end_errors(_call_on_end_best_effort(error_manager, stage), stage)
            return
    else:
        if isinstance(stage, BatchStage):
            _batch_worker_body(
                stage, in_queue, out_queue, error_manager, retry_manager, cancel
            )
        else:
            assert isinstance(stage, Stage)
            _single_worker_body(
                stage, in_queue, out_queue, error_manager, retry_manager, cancel
            )
