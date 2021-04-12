import logging
import queue
import time
from threading import Event
from typing import Sequence, Callable, Optional, List
from smartpipeline.utils import ConcurrentCounter, ProcessCounter
from smartpipeline.defaults import CONCURRENCY_WAIT
from smartpipeline.error.handling import ErrorManager
from smartpipeline.item import DataItem, Stop
from smartpipeline.stage import Stage, BatchStage, ItemsQueue, StageType

__author__ = "Giacomo Berardi <giacbrd.com>"

_logger = logging.getLogger(__name__)


def process(stage: Stage, item: DataItem, error_manager: ErrorManager) -> DataItem:
    """
    Execute the :meth:`.stage.Stage.process` method of a stage for an item
    """
    if error_manager.check_critical_errors(item):
        return item
    time1 = time.time()
    try:
        _logger.debug(f"{stage} is processing {item}")
        processed_item = stage.process(item)
        _logger.debug(f"{stage} has finished processing {processed_item}")
    except Exception as e:
        _logger.debug(f"{stage} has failed processing {item}")
        item.set_timing(stage.name, time.time() - time1)
        error_manager.handle(e, stage, item)
        return item
    # this can't be in a finally, otherwise it would register the `error_manager.handle` time
    processed_item.set_timing(stage.name, time.time() - time1)
    return processed_item


def process_batch(
    stage: BatchStage, items: Sequence[DataItem], error_manager: ErrorManager
) -> List[Optional[DataItem]]:
    """
    Execute the :meth:`.stage.BatchStage.process_batch` method of a batch stage for a batch of items
    """
    ret: List[Optional[DataItem]] = [None] * len(items)
    to_process = {}
    for i, item in enumerate(items):
        if error_manager.check_critical_errors(item):
            ret[i] = item
        else:
            _logger.debug(f"{stage} is going to process {item}")
            to_process[i] = item
    time1 = time.time()
    try:
        _logger.debug(f"{stage} is processing {len(to_process)} items")
        processed = stage.process_batch(list(to_process.values()))
        _logger.debug(f"{stage} has finished processing {len(to_process)} items")
    except Exception as e:
        _logger.debug(f"{stage} had failures in processing {len(to_process)} items")
        spent = (time.time() - time1) / (len(to_process) or 1.0)
        for i, item in to_process.items():
            item.set_timing(stage.name, spent)
            error_manager.handle(e, stage, item)
            ret[i] = item
        return ret
    spent = (time.time() - time1) / (len(to_process) or 1.0)
    for n, i in enumerate(to_process.keys()):
        item = processed[n]
        item.set_timing(stage.name, spent)
        ret[i] = item
    return ret


def stage_executor(
    stage: Stage,
    in_queue: ItemsQueue,
    out_queue: ItemsQueue,
    error_manager: ErrorManager,
    terminated: Event,
    has_started_counter: ConcurrentCounter,
    counter: ConcurrentCounter,
):
    """
    Consume items from an input queue, process and put them in a output queue, indefinitely,
    until a termination event is set
    """
    if isinstance(counter, ProcessCounter):
        # call these only if the stage and the error manager are copies of the original,
        # ergo this executor is running in a child process
        error_manager.on_start()
        stage.on_start()
    has_started_counter += 1
    while True:
        if terminated.is_set() and in_queue.empty():
            return
        try:
            item = in_queue.get(block=True, timeout=CONCURRENCY_WAIT)
        except queue.Empty:
            continue
        if isinstance(item, Stop):
            out_queue.put(item, block=True)
            in_queue.task_done()
        elif item is not None:
            try:
                item = process(stage, item, error_manager)
            except Exception as e:
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
    terminated: Event,
    has_started_counter: ConcurrentCounter,
    counter: ConcurrentCounter,
):
    """
    Consume items in batches from an input queue, process and put them in a output queue, indefinitely,
    until a termination event is set
    """
    if isinstance(counter, ProcessCounter):
        # call these only if the stage and the error manager are copies of the original,
        # ergo this executor is running in a child process
        error_manager.on_start()
        stage.on_start()
    has_started_counter += 1
    while True:
        if terminated.is_set() and in_queue.empty():
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
                items = process_batch(stage, items, error_manager)
            except Exception as e:
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
