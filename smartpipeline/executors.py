import queue
import time
from abc import ABC, abstractmethod
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing.managers import BaseManager

from smartpipeline.error import ErrorManager
from smartpipeline.stage import DataItem, Source, Stop, Stage
from smartpipeline.utils import new_queue

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Container(ABC):

    @abstractmethod
    def get_item(self, block=False) -> DataItem:
        return None

    def init_queue(self):
        self._out_queue = new_queue()
        return self._out_queue


class SourceContainer(Container):
    def __init__(self):
        self._source = None
        # use the next two attributes jointly so we do not need to use a queue if we only work synchronously
        self._next_item = None
        self._out_queue = self.init_queue()

    def set(self, source: Source):
        self._source = source

    def is_set(self):
        return self._source is not None

    def prepend_item(self, item: DataItem):
        if self._next_item is not None:
            self._out_queue.put(item)
        else:
            self._next_item = item

    def get_item(self, block=False):
        if self._next_item is not None:
            ret = self._next_item
            try:
                self._next_item = self._out_queue.get(block=block)
            except queue.Empty:
                self._next_item = None
            return ret
        else:
            return self._source.pop()

    def get(self, block):
        return self.get_item(block)


def _process(stage: Stage, item: DataItem, error_manager: ErrorManager) -> DataItem:
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


def _mp_stage_processor(stage, in_queue, out_queue, error_manager):
    while True:
        item = in_queue.get(block=True)
        if isinstance(item, Stop):
            out_queue.put(item, block=True)
            return
        elif item is not None:
            item = _process(stage, item, error_manager)
            out_queue.put(item, block=True)


def _simple_stage_processor(stage_container):
    while True:
        item = stage_container.process()
        if isinstance(item, Stop):
            return


class StageContainer(Container):
    def __init__(self, name: str, stage: Stage, error_manager: ErrorManager):
        self._error_manager = error_manager
        self._name = name
        stage.set_name(name)
        self._stage = stage
        self._last_processed = None
        self._out_queue = None
        self._previous = None
        self._is_stopped = False

    @property
    def name(self):
        return self._name

    @property
    def stage(self):
        return self._stage

    @property
    def out_queue(self):
        return self._out_queue

    def get_stage(self):
        return self._stage

    def is_stopped(self):
        return self._is_stopped

    def process(self) -> DataItem:
        item = self._previous.get_item()
        if isinstance(item, Stop):
            self._is_stopped = True
        elif item is not None:
            item = _process(self.stage, item, self._error_manager)
        self._put_item(item)
        return item

    def set_previous_stage(self, container: Container):
        self._previous = container

    def get_item(self, block=False):
        # if the stage is stopped this is always a Stop
        ret = self._last_processed
        self._last_processed = None
        if self._out_queue is not None:
            try:
                ret = self._out_queue.get(block=block)
            except queue.Empty:
                return None
        return ret

    def _put_item(self, item):
        self._last_processed = item
        if self._out_queue is not None and self._last_processed is not None:
            self._out_queue.put(self._last_processed, block=True)


class ConcurrentStageContainer(StageContainer):
    # FIXME manage the fact that a developer could have missed the `return item` (so he returns a None) in the overloaded `process` method of his stage
    # FIXME signal to clean queues in round robin way
    # FIXME manage queue size, timeout, minimum time between processes
    def __init__(self, name: str, stage: Stage, error_manager, concurrency=1, use_threads=True):
        super().__init__(name, stage, error_manager)
        self._concurrency = concurrency
        self._use_threads = use_threads
        self._stage_executor = None
        self._stage_executor = self._get_stage_executor()
        self._out_queue = self.init_queue()
        self._previous_queue = None
        self._future = None

    def _get_stage_executor(self):
        if self._stage_executor is None:
            executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
            self._stage_executor = executor(max_workers=self._concurrency)
        return self._stage_executor

    def set_previous_stage(self, container: Container):
        super().set_previous_stage(container)
        self._previous_queue = self._previous.init_queue()
        if isinstance(container, SourceContainer):
            self._previous_queue = container

    def run(self):
        for _ in range(self._concurrency):
            if isinstance(self._stage_executor, ThreadPoolExecutor):
                self._future = self._stage_executor.submit(_simple_stage_processor, self)
            else:
                self._future = self._stage_executor.submit(_mp_stage_processor, self.stage, self._previous_queue, self._out_queue, self._error_manager)
                self._future.result()

    def shutdown(self):
        self._stage_executor.shutdown()

    def is_stopped(self):
        return (self._future.done() or self._future.cancelled()) and self._out_queue.empty()
