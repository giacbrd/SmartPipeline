import queue
import time
from abc import ABC, abstractmethod
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Manager

from smartpipeline.error import Error, CriticalError

__author__ = 'Giacomo Berardi <giacbrd.com>'


class DataItem:
    def __init__(self):
        self._errors = []
        self._critical_errors = []
        self._meta = {}
        self._payload = {}
        self._timings = {}

    def has_errors(self):
        return any(self._errors)

    def has_critical_errors(self):
        return any(self._critical_errors)

    def errors(self):
        for e in self._errors:
            yield e

    def critical_errors(self):
        for e in self._critical_errors:
            yield e

    @property
    def payload(self):
        return self._payload

    def add_error(self, stage, exception):
        if hasattr(exception, 'set_stage'):
            if not type(exception) is Error:
                raise ValueError("Add a pipeline error or a generic exception.")
            exception.set_stage(stage)
            self._errors.append(exception)
        else:
            error = Error()
            error.with_exception(exception)
            error.set_stage(stage)
            self._errors.append(error)

    def add_critical_error(self, stage, exception):
        if hasattr(exception, 'set_stage'):
            if not type(exception) is CriticalError:
                raise ValueError("Add a critical pipeline error or a generic exception.")
            exception.set_stage(stage)
            self._critical_errors.append(exception)
        else:
            error = CriticalError()
            error.with_exception(exception)
            error.set_stage(stage)
            self._critical_errors.append(error)

    def set_metadata(self, field, value):
        self._meta[field] = value
        return self

    def get_metadata(self, field):
        return self._meta.get(field)

    def set_timing(self, stage, ms):
        self._timings[stage] = ms
        return self

    def get_timing(self, stage):
        return self._timings.get(stage)

    @property
    def id(self):
        return self._payload.get('id') or self._meta.get('id') or id(self)

    def __str__(self):
        return 'Data Item {} with payload {}...'.format(self.id, str(self._payload)[:100])


class Stage(ABC):

    def set_name(self, name):
        self._name = name

    @property
    def name(self):
        return getattr(self, '_name', '<undefined>')

    @abstractmethod
    def process(self, item: DataItem):
        return item

    def __str__(self):
        return 'Stage {}'.format(self.name)


class Source(ABC):

    @abstractmethod
    def pop(self):
        return None

    def get_item(self):
        return self.pop()
    
    def send_stop(self):
        self._is_stopped = True
        
    def has_stopped(self):
        current = getattr(self, '_is_stopped', False)
        self._is_stopped = False
        return current


class Stop(DataItem):
    pass


def _stage_processor(stage_container):
    while True:
        #FIXME manage the fact that a developer could have missed the `return item` (so he returns a None) in the overloaded `process` method of his stage
        # FIXME signal to clean queues in round robin way
        item = stage_container.process()
        if isinstance(item, Stop):
            return
        # FIXME manage queue size, timeout, minimum time between processes


class StageContainer:
    def __init__(self, name: str, stage: Stage, error_manager):
        self._error_manager = error_manager
        self._name = name
        stage.set_name(name)
        self._stage = stage
        self._last_processed = None
        self._out_queue = None
        self._previous = None
        self._is_stopped = False

    def get_stage(self):
        return self._stage

    def process(self):
        item = self._previous.get_item()
        if isinstance(item, Stop):
            self._is_stopped = True
        elif item is not None:
            item = self._process(item)
        self._put_item(item)
        return item

    def _process(self, item):
        time1 = time.time()
        try:
            ret = self._stage.process(item)
        except Exception as e:
            item.set_timing(self._name, (time.time() - time1) * 1000.)
            self._error_manager.handle(e, self._stage, item)
            return item
        # this can't be in a finally, otherwise it would register the `error_manager.handle` time
        item.set_timing(self._name, (time.time() - time1) * 1000.)
        return ret

    def set_previous_stage(self, stage_container):
        self._previous = stage_container
        
    def get_item(self, block=False):
        if self._out_queue is not None:
            try:
                # empty all remained processed items before sending a stop to next stages
                if self._is_stopped and self._out_queue.empty():
                    ret = Stop()
                else:
                    ret = self._out_queue.get(block=block)
            except queue.Empty:
                return None
        else:
            ret = self._last_processed
            self._last_processed = None
        return ret

    def _put_item(self, item):
        self._last_processed = item
        if self._out_queue is not None:
            self._out_queue.put(self._last_processed, block=True)


class ConcurrentStageContainer(StageContainer):
    def __init__(self, name: str, stage: Stage, error_manager, concurrency=1, use_threads=True):
        super().__init__(name, stage, error_manager)
        self._concurrency = concurrency
        self._use_threads = use_threads
        self._stage_executor = None
        self._stage_executor = self._get_stage_executor()
        self._queue_manager = Manager()
        self._out_queue = self._queue_manager.Queue()

    def _get_stage_executor(self):
        if self._stage_executor is None:
            executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
            self._stage_executor = executor(max_workers=self._concurrency)
        return self._stage_executor

    def run(self):
        self._stage_executor.submit(_stage_processor, self)

    def shutdown(self):
        self._stage_executor.shutdown()

    def get_item(self, block=True):
        return super().get_item(block=block)
