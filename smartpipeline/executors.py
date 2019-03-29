import queue
import time
from abc import ABC, abstractmethod
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event as TEvent

from smartpipeline.error import ErrorManager
from smartpipeline.stage import DataItem, Stop, Stage
from smartpipeline.utils import mp_queue, mp_event

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Container(ABC):

    @abstractmethod
    def get_item(self, block=False) -> DataItem:
        return None

    @abstractmethod
    def is_stopped(self) -> bool:
        return False

    def init_queue(self):
        self._out_queue = mp_queue()
        return self._out_queue

    @property
    def out_queue(self):
        return self._out_queue


class SourceContainer(Container):
    def __init__(self):
        self._source = None
        # use the next two attributes jointly so we do not need to use a queue if we only work synchronously
        self._next_item = None
        self._internal_queue = mp_queue()
        self._out_queue = None
        self._stop_sent = False

    def __str__(self):
        return 'Container for source {}'.format(self._source)

    def set(self, source):
        self._source = source

    def is_set(self):
        return self._source is not None

    def is_stopped(self):
        return getattr(self._source, 'is_stopped', False)

    # this only used with concurrent stages
    def pop_into_queue(self):
        item = self._get_next_item()
        if not self._stop_sent:
            self._out_queue.put(item, block=True)
        if isinstance(item, Stop):
            self._stop_sent = True

    # only used for processing single items
    def prepend_item(self, item: DataItem):
        if self._out_queue is not None:
            self._out_queue.put(item, block=True)
        elif self._next_item is not None:
            self._internal_queue.put(item)
        else:
            self._next_item = item

    def get_item(self, block=True):
        if self._out_queue is not None:
            item = self._out_queue.get(block=block)
        else:
            item = self._get_next_item()
        return item

    def _get_next_item(self):
        ret = self._next_item
        if ret is not None:
            try:
                self._next_item = self._internal_queue.get(block=False)
            except queue.Empty:
                if self.is_stopped():
                    self._next_item = Stop()
                else:
                    self._next_item = None
            return ret
        else:
            ret = self._source.pop()
            if self.is_stopped():
                return Stop()
            else:
                return ret


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


def _stage_processor(stage, in_queue, out_queue, error_manager, terminated):
    while True:
        if terminated.is_set():
            return
        try:
            item = in_queue.get(block=True, timeout=0.1)  #FIXME parametrize timeout
        except queue.Empty:
            continue
        if isinstance(item, Stop):
            out_queue.put(item, block=True)
            in_queue.task_done()
        elif item is not None:
            item = _process(stage, item, error_manager)
            out_queue.put(item, block=True)
            in_queue.task_done()


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
        self._stop_sent = False

    def __str__(self):
        return 'Container {} for stage {}'.format(self._name, self._stage)

    @property
    def name(self):
        return self._name

    @property
    def stage(self):
        return self._stage

    def get_stage(self):
        return self._stage

    def is_stopped(self):
        return self._is_stopped

    def is_terminated(self):
        return self.is_stopped()

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
        ret = self._last_processed
        self._last_processed = None
        if self._out_queue is not None:
            try:
                ret = self._out_queue.get(block=block)
                self._out_queue.task_done()
            except queue.Empty:
                return None
        return ret

    def _put_item(self, item):
        self._last_processed = item
        if self._out_queue is not None and self._last_processed is not None and not self._stop_sent:
            self._out_queue.put(self._last_processed, block=True)
        if isinstance(self._last_processed, Stop):
            self._stop_sent = True


class ConcurrentStageContainer(StageContainer):
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
        self._terminate_event = None

    def _get_stage_executor(self):
        if self._stage_executor is None:
            executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
            self._stage_executor = executor(max_workers=self._concurrency)  #TODO one executor per stage? why workers are equivalent to concurrency?
        return self._stage_executor

    def terminate(self):
        self._terminate_event.set()

    def set_previous_stage(self, container: Container):
        super().set_previous_stage(container)
        self._previous_queue = self._previous.init_queue()

    def run(self):
        if isinstance(self._stage_executor, ThreadPoolExecutor):
            self._terminate_event = TEvent()
        else:
            self._terminate_event = mp_event()
        for _ in range(self._concurrency):
            self._future = self._stage_executor.submit(_stage_processor, self.stage, self._previous_queue, self._out_queue, self._error_manager, self._terminate_event)

    def shutdown(self):
        self._stage_executor.shutdown()

    def empty_queues(self):
        return self._previous_queue.empty() and self._out_queue.empty()

    def is_terminated(self):
        return (self._future.done() or self._future.cancelled()) and self._terminate_event.is_set()
