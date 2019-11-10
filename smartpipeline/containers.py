import concurrent
import queue
from abc import ABC, abstractmethod, abstractproperty
from concurrent.futures import wait
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Sequence, Union

from smartpipeline.error import ErrorManager
from smartpipeline.executors import process, process_batch, stage_executor, batch_stage_executor
from smartpipeline.stage import Stage, BatchStage, NameMixin
from smartpipeline.item import DataItem, Stop

__author__ = 'Giacomo Berardi <giacbrd.com>'


class InQueued(ABC):

    @property
    @abstractmethod
    def out_queue(self):
        pass

    @abstractmethod
    def init_queue(self, initializer):
        pass


class BaseContainer(InQueued):
    def __init__(self):
        self._is_stopped = False
        self._is_terminated = False
        self._out_queue = None
        self._counter = 0

    @abstractmethod
    def get_processed(self, block=False, timeout=None) -> DataItem:
        return DataItem()

    def init_queue(self, initializer):
        self._out_queue = initializer()
        return self._out_queue

    @property
    def out_queue(self):
        return self._out_queue

    def is_stopped(self):
        return self._is_stopped

    def stop(self):
        self._is_stopped = True

    def is_terminated(self):
        return self._is_terminated

    def terminate(self):
        self._is_terminated = True

    def count(self):
        return self._counter

    def increase_count(self):
        self._counter += 1


class ConnectedStageMixin:
    @property
    def previous(self):
        return self._previous

    def set_previous(self, container):
        self._previous = container


class FallibleMixin:
    def set_error_manager(self, error_manager):
        self._error_manager = error_manager

    @property
    def error_manager(self):
        return self._error_manager


class NamedStageMixin:
    def set_stage(self, name: str, stage: NameMixin):
        self._name = name
        stage.set_name(name)
        self._stage = stage

    @property
    def name(self):
        return self._name

    @property
    def stage(self):
        return self._stage


class SourceContainer(BaseContainer):
    def __init__(self):
        super().__init__()
        self._source = None
        # use the next two attributes jointly so we do not need to use a queue if we only work synchronously
        self._next_item = None
        self._internal_queue_obj = None
        self._stop_sent = False
        self._queue_initializer = None

    @property
    def _internal_queue(self):
        if self._internal_queue_obj is None:
            if self._queue_initializer is None:
                self._set_internal_queue_initializer()
            self._internal_queue_obj = self._queue_initializer()
        return self._internal_queue_obj

    def _set_internal_queue_initializer(self, initializer=queue.Queue):
        self._queue_initializer = initializer

    def __str__(self):
        return 'BaseContainer for source {}'.format(self._source)

    def set(self, source):
        self._source = source

    def is_set(self):
        return self._source is not None or self._next_item is not None or self._out_queue is not None

    def is_stopped(self):
        if self._source is not None:
            return getattr(self._source, 'is_stopped', False)
        else:
            return self._is_stopped

    def stop(self):
        if self._source is not None:
            self._source.stop()
        self._is_stopped = True

    # only used with concurrent stages
    def pop_into_queue(self, as_possible=False):
        """
        Pop from the source but put the item in the queue that will be read from the first stage of the pipeline
        :param as_possible: If True, don't put just one item but as many items as the source can supply
        """
        while True:
            item = self._get_next_item()
            if item is None or self._stop_sent:
                break
            else:
                self.out_queue.put(item, block=True)
                if not isinstance(item, Stop):
                    self.increase_count()
            if isinstance(item, Stop):
                self._stop_sent = True
                break

    # only used for processing single items
    def prepend_item(self, item: DataItem):
        if self._next_item is not None:
            self._internal_queue.put(item)
        else:
            self._next_item = item

    def get_processed(self, block=True, timeout=None):
        if self.out_queue is not None:
            item = self.out_queue.get(block=block, timeout=timeout)
        else:
            item = self._get_next_item()
            if item is not None and not isinstance(item, Stop):
                self.increase_count()
        return item

    def _get_next_item(self):
        ret = self._next_item
        self._next_item = None
        if ret is not None:
            try:
                self._next_item = self._internal_queue.get(block=False)
            except queue.Empty:
                if self.is_stopped():
                    self._next_item = Stop()
            return ret
        elif self._source is not None:
            ret = self._source.pop()
            if self.is_stopped():
                return Stop()
            else:
                return ret
        elif self.is_stopped():
            return Stop()


class StageContainer(BaseContainer, NamedStageMixin, FallibleMixin, ConnectedStageMixin):
    def __init__(self, name: str, stage: Stage, error_manager: ErrorManager):
        super().__init__()
        self.set_error_manager(error_manager)
        self.set_stage(name, stage)
        self._last_processed = None

    def __str__(self):
        return 'Container for stage {}'.format(self._stage)

    def process(self) -> DataItem:
        item = self.previous.get_processed()
        if isinstance(item, Stop):
            self.stop()
        elif item is not None:
            item = process(self.stage, item, self._error_manager)
        # the processed item is set as current output of this stage
        self._put_item(item)
        return item

    def get_processed(self, block=False, timeout=None):
        ret = self._last_processed
        self._last_processed = None
        # if we are in a concurrent stage the items are put in this queue after processing
        if self.out_queue is not None:
            try:
                ret = self.out_queue.get(block=block, timeout=timeout)
                self.out_queue.task_done()
            except queue.Empty:
                return None
        return ret

    def _put_item(self, item):
        self._last_processed = item
        if self.out_queue is not None and self._last_processed is not None and not self.is_terminated():
            self.out_queue.put(self._last_processed, block=True)
        if item is not None and not isinstance(item, Stop):
            self.increase_count()


class BatchStageContainer(BaseContainer, NamedStageMixin, FallibleMixin, ConnectedStageMixin):
    def __init__(self, name: str, stage: BatchStage, error_manager: ErrorManager):
        super().__init__()
        self.set_error_manager(error_manager)
        self.set_stage(name, stage)
        self.__result_queue = queue.SimpleQueue()
        self._last_processed = []

    def process(self) -> Sequence[DataItem]:
        items = []
        extra_items = []
        for _ in range(self.stage.size()):
            item = self.previous.get_processed(timeout=self.stage.timeout())
            if isinstance(item, Stop):
                self.stop()
                self._put_item([item])
                extra_items.append(item)
            elif item is not None:
                items.append(item)
        if any(items):
            items = process_batch(self.stage, items, self.error_manager)
            self._put_item(items)
        return items + extra_items

    def get_processed(self, block=False, timeout=None):
        if self.out_queue is not None:
            try:
                for _ in range(self.stage.size()):
                    item = self.out_queue.get(block=block, timeout=timeout)
                    if item is not None:
                        self.__result_queue.put_nowait(item)
                    self.out_queue.task_done()
            except queue.Empty:
                try:
                    return self.__result_queue.get(block=block, timeout=timeout)
                except queue.Empty:
                    return None
        elif self._last_processed:
            for item in self._last_processed:
                self.__result_queue.put_nowait(item)
            self._last_processed = []
        try:
            return self.__result_queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None

    def _put_item(self, items: Sequence[DataItem]):
        self._last_processed.extend(items)
        if self.out_queue is not None and self._last_processed is not None and not self.is_terminated():
            for item in self._last_processed:
                self.out_queue.put(item, block=True)
        for item in self._last_processed:
            if item is not None and not isinstance(item, Stop):
                self.increase_count()


class ConcurrencyMixin(InQueued, ConnectedStageMixin):

    def init_concurrency(self, queue_initializer, counter_initializer, terminate_event_initializer, concurrency=1, use_threads=True):
        self._concurrency = concurrency
        self._use_threads = use_threads
        self._stage_executor = None
        self._previous_queue = None
        self._futures = []
        self._queue_initializer = queue_initializer
        self._out_queue = self._queue_initializer()
        self._counter = counter_initializer()
        self._counter_initializer = counter_initializer
        self._terminate_event = terminate_event_initializer()

    def queues_empty(self):
        return self._previous_queue.empty() and self.out_queue.empty()

    def queues_join(self):
        self._previous_queue.join()
        self.out_queue.join()

    def empty_queues(self):
        for q in (self._previous_queue, self.out_queue):
            while True:
                try:
                    q.get_nowait()
                    q.task_done()
                except queue.Empty:
                    break

    def set_previous(self, container):
        self._previous = container
        if isinstance(self._previous, (ConcurrentStageContainer, BatchConcurrentStageContainer)) and not self._previous.use_threads:
            self._previous_queue = self._previous.out_queue  # give priority to the previous queue initializer
        else:
            self._previous_queue = self._previous.init_queue(self._queue_initializer)

    def _get_stage_executor(self):
        if self._stage_executor is None:
            executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
            # TODO one executor per stage? why max_workers are equivalent to concurrency?
            self._stage_executor = executor(max_workers=self._concurrency)
        return self._stage_executor

    @property
    def use_threads(self):
        return self._use_threads

    def shutdown(self):
        for future in self._futures:
            future.cancel()
        if self._stage_executor is not None:
            self._stage_executor.shutdown()

    def __del__(self):
        self.shutdown()

    def check_errors(self):
        for future in self._futures:
            try:
                ex = future.exception(timeout=0)
            except concurrent.futures.TimeoutError:
                continue
            if ex is not None:
                raise ex

    def count(self):
        return self._counter.value if self._counter else 0

    def terminate(self):
        self._terminate_event.set()
        wait(self._futures)

    def is_terminated(self):
        return all(future.done() or future.cancelled() for future in self._futures) and self._terminate_event.is_set()

    def _run(self, stage, _executor, in_queue, out_queue, error_manager):
        ex = self._get_stage_executor()
        self._counter = self._counter_initializer()
        self._terminate_event.clear()
        for _ in range(self._concurrency):
            self._futures.append(ex.submit(_executor, stage, in_queue, out_queue,
                                           error_manager, self._terminate_event, self._counter))


class ConcurrentStageContainer(ConcurrencyMixin, StageContainer):
    def __init__(self, name: str, stage: Stage, error_manager, queue_initializer, counter_initializer, terminate_event_initializer, concurrency=1, use_threads=True):
        super().__init__(name, stage, error_manager)
        self.init_concurrency(queue_initializer, counter_initializer, terminate_event_initializer, concurrency, use_threads)

    def run(self, _executor=stage_executor):
        super()._run(self.stage, _executor, self._previous_queue, self.out_queue, self.error_manager)


class BatchConcurrentStageContainer(ConcurrencyMixin, BatchStageContainer):
    def __init__(self, name: str, stage: BatchStage, error_manager, queue_initializer, counter_initializer, terminate_event_initializer, concurrency=1, use_threads=True):
        super().__init__(name, stage, error_manager)
        self.init_concurrency(queue_initializer, counter_initializer, terminate_event_initializer, concurrency, use_threads)

    def run(self, _executor=batch_stage_executor):
        super()._run(self.stage, _executor, self._previous_queue, self.out_queue, self.error_manager)
