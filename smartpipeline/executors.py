import concurrent
import queue
import time
from abc import ABC, abstractmethod
from concurrent.futures import wait
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Sequence

from smartpipeline import CONCURRENCY_WAIT
from smartpipeline.error import ErrorManager
from smartpipeline.stage import DataItem, Stop, Stage, BatchStage, BaseStage

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Container(ABC):

    @abstractmethod
    def get_processed(self, block=False, timeout=None) -> DataItem:
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
        self._internal_queue_obj = None
        self._out_queue = None
        self._stop_sent = False
        self._is_stopped = False
        self._queue_initializer = None

    @property
    def _internal_queue(self):
        if self._internal_queue_obj is None:
            if self._queue_initializer is None:
                self.set_internal_queue_initializer()
            self._internal_queue_obj = self._queue_initializer()
        return self._internal_queue_obj

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
                self._out_queue.put(item, block=True)
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
        if self._out_queue is not None:
            item = self._out_queue.get(block=block, timeout=timeout)
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


def _stage_processor(stage, in_queue, out_queue, error_manager, terminated):
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
                item = _process(stage, item, error_manager)
            except Exception as e:
                raise e
            else:
                if item is not None:
                    out_queue.put(item, block=True)
            finally:
                in_queue.task_done()


def _batch_stage_processor(stage: BatchStage, in_queue, out_queue, error_manager, terminated):
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
                items = _process_batch(stage, items, error_manager)
            except Exception as e:
                raise e
            else:
                for item in items:
                    if item is not None:
                        out_queue.put(item, block=True)


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
        self._is_terminated = False

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
        return self._is_terminated

    def terminate(self):
        self._is_terminated = True

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

    def get_processed(self, block=False, timeout=None):
        ret = self._last_processed
        self._last_processed = None
        # if we are in a concurrent stage the items are put in this queue after processing
        if self._out_queue is not None:
            try:
                ret = self._out_queue.get(block=block, timeout=timeout)
                self._out_queue.task_done()
            except queue.Empty:
                return None
        return ret

    def _put_item(self, item):
        self._last_processed = item
        if self._out_queue is not None and self._last_processed is not None and not self._is_terminated:
            self._out_queue.put(self._last_processed, block=True)


class BatchStageContainer(StageContainer):
    def __init__(self, name: str, stage: BaseStage, error_manager: ErrorManager):
        super().__init__(name, stage, error_manager)
        self.__result_queue = queue.Queue()
        self._last_processed = []

    def process(self) -> Sequence[DataItem]:
        items = []
        extra_items = []
        for _ in range(self.stage.size()):
            item = self._previous.get_processed(timeout=self.stage.timeout())
            if isinstance(item, Stop):
                self._is_stopped = True
                self._put_item([item])
                extra_items.append(item)
            elif item is not None:
                items.append(item)
        if any(items):
            items = _process_batch(self.stage, items, self._error_manager)
            self._put_item(items)
        return items + extra_items

    def get_processed(self, block=False, timeout=None):
        if self._out_queue is not None:
            try:
                for _ in range(self.stage.size()):
                    item = self._out_queue.get(block=block, timeout=timeout)
                    if item is not None:
                        self.__result_queue.put_nowait(item)
                    self._out_queue.task_done()
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
        if self._out_queue is not None and self._last_processed is not None and not self._is_terminated:
            for item in self._last_processed:
                self._out_queue.put(item, block=True)


class ConcurrentStageContainer(StageContainer):
    # FIXME manage queue size, timeout, minimum time between processes
    def __init__(self, name: str, stage: BaseStage, error_manager, queue_initializer, concurrency=1, use_threads=True):
        super().__init__(name, stage, error_manager)
        self._concurrency = concurrency
        self._use_threads = use_threads
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
        wait(self._futures)

    @property
    def use_threads(self):
        return self._use_threads

    def set_previous_stage(self, container: Container):
        super().set_previous_stage(container)
        if isinstance(self._previous, ConcurrentStageContainer) and not self._previous.use_threads:
            self._previous_queue = self._previous.out_queue  # give priority to the previous queue initializer
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
