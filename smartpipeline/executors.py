import queue
import time
from threading import Event
from typing import Sequence, Callable, Union

from smartpipeline.defaults import CONCURRENCY_WAIT
from smartpipeline.error import ErrorManager
from smartpipeline.item import DataItem, Stop
from smartpipeline.stage import Stage, BatchStage, ItemsQueue

__author__ = 'Giacomo Berardi <giacbrd.com>'

from smartpipeline.utils import ConcurrentCounter


def process(stage: Stage, item: DataItem, error_manager: ErrorManager) -> DataItem:
    if error_manager.check_errors(item):
        return item
    time1 = time.time()
    try:
        ret = stage.process(item)
    except Exception as e:
        item.set_timing(stage.name, (time.time() - time1) * 1000.)
        error_manager.handle(e, stage, item)
        return item
    # this can't be in a finally, otherwise it would register the `error_manager.handle` time
    item.set_timing(stage.name, (time.time() - time1) * 1000.)
    return ret


def process_batch(stage: BatchStage, items: Sequence[DataItem], error_manager: ErrorManager) -> Sequence[DataItem]:
    ret = [None] * len(items)
    to_process = {}
    for i, item in enumerate(items):
        if error_manager.check_errors(item):
            ret[i] = item
        else:
            to_process[i] = item
    time1 = time.time()
    try:
        processed = stage.process_batch(list(to_process.values()))
    except Exception as e:
        spent = ((time.time() - time1) * 1000.) / (len(to_process) or 1.)
        for i, item in to_process.items():
            item.set_timing(stage.name, spent)
            error_manager.handle(e, stage, item)
            ret[i] = item
        return ret
    spent = ((time.time() - time1) * 1000.) / (len(to_process) or 1.)
    for n, i in enumerate(to_process.keys()):
        item = processed[n]
        item.set_timing(stage.name, spent)
        ret[i] = item
    return ret


def stage_executor(stage: Stage, in_queue: ItemsQueue, out_queue: ItemsQueue, error_manager: ErrorManager,
                   terminated: Event, counter: ConcurrentCounter):
    stage.on_fork()
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


def batch_stage_executor(stage: BatchStage, in_queue: ItemsQueue, out_queue: ItemsQueue, error_manager: ErrorManager,
                         terminated: Event, counter: ConcurrentCounter):
    stage.on_fork()
    while True:
        if terminated.is_set() and in_queue.empty():
            return
        items = []
        try:
            for _ in range(stage.size()):
                item = in_queue.get(block=True, timeout=stage.timeout())
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
    [Union[Stage, BatchStage], ItemsQueue, ItemsQueue, ErrorManager, Event, ConcurrentCounter], None]
