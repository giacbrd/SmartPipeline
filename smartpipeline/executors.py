import concurrent
import queue
import time
from abc import ABC, abstractmethod
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import islice
from typing import Sequence

from smartpipeline import CONCURRENCY_WAIT
from smartpipeline.error import ErrorManager
from smartpipeline.stage import DataItem, Stop, Stage, BatchStage, BaseStage

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Container(ABC):

    @abstractmethod
    def get_processed(self, block=False) -> DataItem:
        return None

    @abstractmethod
    def is_stopped(self) -> bool:
        return False

    def init_queue(self, initializer):
        self._out_queue = initializer()
        return self._out_queue

    @property
    def out_queue(self):
        return self._out_queue


class SourceContainer(Container):
    def __init__(self):
        self._source = None
        # use the next two attributes jointly so we do not need to use a queue if we only work synchronously
        self._next_item = None
        self.__internal_queue = None
        self._out_queue = None
        self._stop_sent = False
        self._is_stopped = False
        self._queue_initializer = None

    @property
    def _internal_queue(self):
        if self.__internal_queue is None:
            if self._queue_initializer is None:
                self.set_internal_queue_initializer()
            self.__internal_queue = self._queue_initializer()
        return self.__internal_queue

    def set_internal_queue_initializer(self, initializer=queue.Queue):
        self._queue_initializer = initializer

    def __str__(self):
        return 'Container for source {}'.format(self._source)

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
        else:
            self._is_stopped = True

    # only used with concurrent stages
    def pop_into_queue(self):
        item = self._get_next_item()
        if not self._stop_sent:
            self._out_queue.put(item, block=True)
        if isinstance(item, Stop):
            self._stop_sent = True

    # only used for processing single items
    def prepend_item(self, item: DataItem):
        if self._next_item is not None:
            self._internal_queue.put(item)
        else:
            self._next_item = item

    def get_processed(self, block=True):
        if self._out_queue is not None:
            item = self._out_queue.get(block=block)
        else:
            item = self._get_next_item()
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


def _process(stage: Stage, item: DataItem, error_manager: ErrorManager) -> DataItem:
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


def _process_batch(stage: BatchStage, items: Sequence[DataItem], error_manager: ErrorManager) -> Sequence[DataItem]:
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
        spent = ((time.time() - time1) * 1000.) / len(to_process)
        for i, item in to_process.items():
            item.set_timing(stage.name, spent)
            error_manager.handle(e, stage, item)
            ret[i] = item
        return ret
    spent = ((time.time() - time1) * 1000.) / len(to_process)
    for n, i in enumerate(to_process.keys()):
        item = processed[n]
        item.set_timing(stage.name, spent)
        ret[i] = item
    return ret


def _stage_processor(stage, in_queue, out_queue, error_manager, terminated):
    while True:
        if terminated.is_set():
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
                item = _process(stage, item, error_manager)
            except Exception as e:
                raise e
            else:
                out_queue.put(item, block=True)
            finally:
                in_queue.task_done()


def _batch_stage_processor(stage: BatchStage, in_queue, out_queue, error_manager, terminated):
    stop_item = None
    while True:
        is_terminated = terminated.is_set()
        if stop_item is not None:
            out_queue.put(stop_item, block=True)
            in_queue.task_done()
            if not is_terminated:
                continue
        if is_terminated:
            return
        items = []
        try:
            for _ in range(stage.size()):
                item = in_queue.get(block=True, timeout=stage.timeout())
                if item is not None:
                    items.append(item)
        except queue.Empty:
            if not any(items):
                continue
        end_index = -1
        for i, item in enumerate(items):
            if isinstance(item, Stop):
                stop_item = item
                end_index = i
                break
        if end_index > -1:
            items = items[:end_index]
        if any(items):
            try:
                items = _process_batch(stage, items, error_manager)
            except Exception as e:
                raise e
            else:
                for item in items:
                    out_queue.put(item, block=True)
            finally:
                in_queue.task_done()


class StageContainer(Container):
    def __init__(self, name: str, stage: BaseStage, error_manager: ErrorManager):
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

    def set_error_manager(self, error_manager):
        self._error_manager = error_manager

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
        item = self._previous.get_processed()
        if isinstance(item, Stop):
            self._is_stopped = True
        elif item is not None:
            item = _process(self.stage, item, self._error_manager)
        # the processed item is set as current output of this stage
        self._put_item(item)
        return item

    def set_previous_stage(self, container: Container):
        self._previous = container

    def get_processed(self, block=False):
        ret = self._last_processed
        self._last_processed = None
        # if we are in a concurrent stage the items are put in this queue after processing
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


class BatchStageContainer(StageContainer):

    def process(self) -> Sequence[DataItem]:
        prev = self._previous.get_processed()
        if isinstance(prev, Sequence):
            items = prev
        else:
            items = [prev]
        for _ in range(self.stage.size() - 1):
            items.append(self._previous.get_processed())
        if any(isinstance(item, Stop) for item in items):
            self._is_stopped = True
        if any(items):
            items = _process_batch(self.stage, items, self._error_manager)
        # the processed item is set as current output of this stage
        self._put_item(items)
        return items

    def get_processed(self, block=True):
        ret = self._last_processed
        self._last_processed = None
        if self._out_queue is not None:
            ret = []
            for _ in range(self.stage.size()):
                try:
                    ret.append(self._out_queue.get(block=block, timeout=self.stage.timeout()))
                except queue.Empty:
                    return ret or None
                finally:
                    self._out_queue.task_done()
        return ret

    def _put_item(self, items: Sequence[DataItem]):
        self._last_processed = items
        if self._out_queue is not None and self._last_processed is not None and not self._stop_sent:
            for item in self._last_processed:
                self._out_queue.put(item, block=True)
        if any(isinstance(item, Stop) for item in self._last_processed):
            self._stop_sent = True


class ConcurrentStageContainer(StageContainer):
    # FIXME manage queue size, timeout, minimum time between processes
    def __init__(self, name: str, stage: Stage, error_manager, queue_initializer, concurrency=1, use_threads=True):
        super().__init__(name, stage, error_manager)
        self._concurrency = concurrency
        self._use_threads = use_threads
        self._stage_executor = None
        self._stage_executor = None
        self._queue_initializer = queue_initializer
        self._out_queue = self.init_queue(self._queue_initializer)
        self._previous_queue = None
        self._futures = []
        self._terminate_event = None

    def _get_stage_executor(self):
        if self._stage_executor is None:
            executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
            self._stage_executor = executor(max_workers=self._concurrency)  # TODO one executor per stage? why max_workers are equivalent to concurrency?
        return self._stage_executor

    def terminate(self):
        self._terminate_event.set()

    @property
    def use_threads(self):
        return self._use_threads

    def set_previous_stage(self, container: Container):
        super().set_previous_stage(container)
        if isinstance(self._previous, ConcurrentStageContainer) and not self._previous.use_threads:
            self._previous_queue = self._previous.out_queue  # give priority to the preevious execut queue initializer
        else:
            self._previous_queue = self._previous.init_queue(self._queue_initializer)

    def run(self, terminate_event_initializer, _processor=_stage_processor):
        ex = self._get_stage_executor()
        self._terminate_event = terminate_event_initializer()
        for _ in range(self._concurrency):
            self._futures.append(
                ex.submit(_processor, self.stage, self._previous_queue, self._out_queue,
                                            self._error_manager, self._terminate_event))

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

    def queues_empty(self):
        return self._previous_queue.empty() and self._out_queue.empty()

    def queues_join(self):
        return self._previous_queue.join() and self._out_queue.join()

    def is_terminated(self):
        return all(future.done() or future.cancelled() for future in self._futures) and self._terminate_event.is_set()

    def empty_queues(self):
        for queue in (self._previous_queue, self._out_queue):
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()


class BatchConcurrentStageContainer(ConcurrentStageContainer, BatchStageContainer):

    def run(self, terminate_event_initializer, _processor=_batch_stage_processor):
        super().run(terminate_event_initializer, _processor)
