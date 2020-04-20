"""
Containers encapsulate stages and manage their execution
"""

import concurrent
import queue
from queue import Queue
from abc import ABC, abstractmethod
from concurrent.futures import wait, Executor, Future
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event
from typing import Sequence, Optional, Callable
from smartpipeline.utils import ConcurrentCounter
from smartpipeline.error.handling import ErrorManager
from smartpipeline.executors import (
    process,
    process_batch,
    stage_executor,
    batch_stage_executor,
    StageExecutor,
)
from smartpipeline.stage import Stage, BatchStage, Source, ItemsQueue, StageType
from smartpipeline.item import DataItem, Stop

__author__ = "Giacomo Berardi <giacbrd.com>"


QueueInitializer = Callable[[], ItemsQueue]
CounterInitializer = Callable[[], ConcurrentCounter]
EventInitializer = Callable[[], Event]


class InQueued(ABC):
    """
    Interface for containers which exposes an output queue for items
    """

    @property
    @abstractmethod
    def out_queue(self) -> ItemsQueue:
        """
        Get the output queue instance
        """
        pass

    @abstractmethod
    def init_queue(self, initializer: QueueInitializer):
        """
        Initialize the output queue with a specific constructor (function)
        """
        pass


class BaseContainer(InQueued):
    """
    Base interface for all containers
    """

    def __init__(self):
        self._is_stopped = False
        self._is_terminated = False
        self._out_queue = None
        self._counter = 0

    @abstractmethod
    def get_processed(
        self, block: bool = False, timeout: Optional[int] = None
    ) -> Optional[DataItem]:
        """
        Get the oldest processed item waiting to be retrieved

        :param block: Wait for the next item to be processed if no one available
        :param timeout: Time to wait when `block` is True
        :return: A processed item or None if: no item waiting to be retrieved; timeout expires on a blocked call
        """
        pass

    def init_queue(self, initializer: QueueInitializer) -> ItemsQueue:
        self._out_queue = initializer()
        return self._out_queue

    @property
    def out_queue(self) -> ItemsQueue:
        return self._out_queue

    def is_stopped(self) -> bool:
        return self._is_stopped

    def stop(self):
        """
        Set the container stage as stopped, usually when the pipeline has ended its work (i.e. the source has called :meth:`.stage.Source.stop`)
        """
        self._is_stopped = True

    def is_terminated(self) -> bool:
        return self._is_terminated

    def terminate(self):
        """
        Turn off the container (e.g. threads and processes) and the relative stage
        """
        self._is_terminated = True

    def count(self) -> int:
        """
        Return the number of items that have been seen by this container
        """
        return self._counter

    def increase_count(self):
        """
        Increase the counter of items "seen" by the container/stage
        """
        self._counter += 1


class ConnectedStageMixin:
    """
    A mixin for the containers that encapsulate stages that can be connected to others (i.e. in a pipeline)
    """

    @property
    def previous(self) -> BaseContainer:
        """
        Get the container from which this container receives items
        """
        return self._previous

    def set_previous(self, container: BaseContainer):
        """
        Set the container from which this container will receive items
        """
        self._previous = container


class FallibleMixin:
    """
    A mixin for the containers that encapsulate stages that can produce errors during processing
    """

    def set_error_manager(self, error_manager: ErrorManager):
        self._error_manager = error_manager

    @property
    def error_manager(self) -> ErrorManager:
        return self._error_manager


class NamedStageMixin:
    """
    A mixin for basic containers of stages
    """

    def set_stage(self, name: str, stage: StageType):
        self._name = name
        self._stage = stage
        self._stage.set_name(name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def stage(self) -> StageType:
        return self._stage


class SourceContainer(BaseContainer):
    """
    A container specific for sources
    """

    def __init__(self):
        super().__init__()
        self._source = None
        # the next two attributes are used jointly for "manually" prepending items
        self._next_item = None
        self._internal_queue_obj = None
        self._stop_sent = False
        self._queue_initializer = None

    @property
    def _internal_queue(self) -> ItemsQueue:
        """
        A special queue used for "manually" enrich the source with items
        """
        if self._internal_queue_obj is None:
            if self._queue_initializer is None:
                self._set_internal_queue_initializer()
            self._internal_queue_obj = self._queue_initializer()
        return self._internal_queue_obj

    def _set_internal_queue_initializer(self, initializer: ItemsQueue = queue.Queue):
        self._queue_initializer = initializer

    def __str__(self) -> str:
        return "Base container for source {}".format(self._source)

    def set(self, source: Source):
        """
        Set the actual source for this container
        """
        self._source = source

    def is_set(self) -> bool:
        """
        True if this container is ready to produce items in output
        """
        return (
            self._source is not None
            or self._next_item is not None
            or self._out_queue is not None
        )

    def is_stopped(self) -> bool:
        """
        True if this container has ended the production of items
        """
        if self._source is not None:
            return getattr(self._source, "is_stopped", False)
        else:
            return self._is_stopped

    def stop(self):
        """
        Stop this source, the container won't produce items anymore
        """
        if self._source is not None:
            self._source.stop()
        self._is_stopped = True

    def pop_into_queue(self):
        """
        Pop from the source but put the item in the queue that will be read from the first stage of the pipeline.
        Only used with concurrent stages
        """
        while True:
            item = self._get_next_item()
            if self._stop_sent:
                return
            elif item is None:
                continue
            else:
                self.out_queue.put(item, block=True)
                if not isinstance(item, Stop):
                    self.increase_count()
            if isinstance(item, Stop):
                self._stop_sent = True
                return

    def prepend_item(self, item: Optional[DataItem]):
        """
        Enrich the source with items "manually".
        Only used for processing single items
        """
        if self._next_item is not None:
            self._internal_queue.put(item)
        else:
            self._next_item = item

    def get_processed(
        self, block: bool = True, timeout: Optional[int] = None
    ) -> Optional[DataItem]:
        if self.out_queue is not None:
            item = self.out_queue.get(block=block, timeout=timeout)
        else:
            item = self._get_next_item()
            if item is not None and not isinstance(item, Stop):
                self.increase_count()
        return item

    def _get_next_item(self) -> DataItem:
        """
        Obtain the next item to send to output according to the source status and "manually" added items
        """
        ret = self._next_item
        self._next_item = None
        if ret is not None:
            try:
                self._next_item = self._internal_queue.get(block=False)
                self._internal_queue.task_done()
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


class StageContainer(
    BaseContainer, NamedStageMixin, FallibleMixin, ConnectedStageMixin
):
    """
    The standard container for basic stages
    """

    def __init__(self, name: str, stage: Stage, error_manager: ErrorManager):
        super().__init__()
        self.set_error_manager(error_manager)
        self.set_stage(name, stage)
        self._last_processed = None

    def __str__(self) -> str:
        return "Container for stage {}".format(self._stage)

    def process(self) -> DataItem:
        item = self.previous.get_processed()
        if isinstance(item, Stop):
            self.stop()
        elif item is not None:
            item = process(self.stage, item, self._error_manager)
        self._put_item(item)
        return item

    def get_processed(
        self, block: bool = False, timeout: Optional[int] = None
    ) -> Optional[DataItem]:
        ret = self._last_processed
        self._last_processed = None
        # if we are in a concurrent stage the items are obtained exclusively from the output queue
        if self.out_queue is not None:
            try:
                ret = self.out_queue.get(block=block, timeout=timeout)
                self.out_queue.task_done()
            except queue.Empty:
                return None
        return ret

    def _put_item(self, item: DataItem):
        """
        A processed item is set as next output.
        If we are processing asynchronously (e.g. concurrent stage) it is put in the output queue,
        otherwise a reference to the this last processed item is set
        """
        self._last_processed = item
        if (
            self.out_queue is not None
            and self._last_processed is not None
            and not self.is_terminated()
        ):
            self.out_queue.put(self._last_processed, block=True)
        if item is not None and not isinstance(item, Stop):
            self.increase_count()


class BatchStageContainer(
    BaseContainer, NamedStageMixin, FallibleMixin, ConnectedStageMixin
):
    """
    Container for batch stages
    """

    def __init__(self, name: str, stage: BatchStage, error_manager: ErrorManager):
        super().__init__()
        self.set_error_manager(error_manager)
        self.set_stage(name, stage)
        # TODO next two varibales are for non-concurrent container, that is currently never used, it doesn't work
        self.__result_queue: ItemsQueue = queue.SimpleQueue()
        self._last_processed: Sequence[DataItem] = []

    def process(self) -> Sequence[DataItem]:
        items = []
        # items that we want to put as last in a batch, ergo in output
        extra_items = []
        for _ in range(self.stage.size):
            item = self.previous.get_processed(timeout=self.stage.timeout)
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

    def get_processed(
        self, block: bool = False, timeout: Optional[int] = None
    ) -> Optional[DataItem]:
        if (
            self.out_queue is not None
            and self.__result_queue.qsize() < self.out_queue.qsize()
        ):
            try:
                # let's free the output queue (so this stage con continue processing) and keep the items internally
                for _ in range(self.stage.size):
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
        """
        A batch of processed items is set as next output.
        If we are processing asynchronously (e.g. concurrent stage) they are put in the output queue,
        otherwise a list of last processed items is extended
        """
        self._last_processed.extend(items)
        if (
            self.out_queue is not None
            and self._last_processed is not None
            and not self.is_terminated()
        ):
            for item in self._last_processed:
                self.out_queue.put(item, block=True)
        for item in self._last_processed:
            if item is not None and not isinstance(item, Stop):
                self.increase_count()

    @property
    def size(self) -> int:
        return self.stage.size


class ConcurrentContainer(InQueued, ConnectedStageMixin):
    """
    Base container for stages that must process concurrently and asynchronously
    """

    def init_concurrency(
        self,
        queue_initializer: QueueInitializer,
        counter_initializer: CounterInitializer,
        terminate_event_initializer: EventInitializer,
        concurrency: int = 1,
        use_threads: bool = True,
    ):
        """
        Initialization of instance members

        :param queue_initializer: Constructor for output, and eventually input, queue
        :param counter_initializer: Constructor for items counter, it counts items seen by concurrent stage executions
        :param terminate_event_initializer: Constructor for the event for alerting all concurrent stage executions for termination
        :param concurrency: Number of maximum concurrent stage executions
        :param use_threads: True for using threads for concurrency, otherwise use multiprocess
        """
        self._concurrency = concurrency
        self._use_threads = use_threads
        self._stage_executor = None
        self._previous_queue = None
        self._futures: Sequence[Future] = []
        self._queue_initializer = queue_initializer
        self._out_queue = self._queue_initializer()
        self._counter = counter_initializer()
        self._counter_initializer = counter_initializer
        self._terminate_event = terminate_event_initializer()

    def queues_empty(self) -> bool:
        """
        True of both input and output queues are empty
        """
        return self._previous_queue.empty() and self.out_queue.empty()

    def queues_join(self):
        self._previous_queue.join()
        self.out_queue.join()

    def empty_queues(self):
        """
        Delete all items in both input and output queues
        """
        for q in (self._previous_queue, self.out_queue):
            while True:
                try:
                    q.get_nowait()
                    q.task_done()
                except queue.Empty:
                    break

    def set_previous(self, container: BaseContainer):
        self._previous = container
        if (
            isinstance(
                self._previous,
                (ConcurrentStageContainer, BatchConcurrentStageContainer),
            )
            and not self._previous.use_threads
        ):
            # give priority to the previous queue initializer
            self._previous_queue = self._previous.out_queue
        else:
            self._previous_queue = self._previous.init_queue(self._queue_initializer)

    def _get_stage_executor(self) -> Executor:
        """
        Get and eventually generate a pool executor where concurrent stage executions run
        """
        if self._stage_executor is None:
            executor = ThreadPoolExecutor if self._use_threads else ProcessPoolExecutor
            # TODO one executor per stage? why max_workers are equivalent to concurrency?
            self._stage_executor = executor(max_workers=self._concurrency)
        return self._stage_executor

    @property
    def use_threads(self) -> bool:
        """
        True if we are using threads, False if we are using multiprocess
        """
        return self._use_threads

    def shutdown(self):
        """
        Turn off the container pool executor
        """
        for future in self._futures:
            future.cancel()
        if self._stage_executor is not None:
            self._stage_executor.shutdown()

    def __del__(self):
        self.shutdown()

    def check_errors(self):
        """
        Look for exceptions generated by stage executions inside threads/processes, eventually re-raise exceptions
        """
        for future in self._futures:
            try:
                ex = future.exception(timeout=0)
            except concurrent.futures.TimeoutError:
                continue
            if ex is not None:
                raise ex

    def count(self) -> int:
        return self._counter.value if self._counter else 0

    def terminate(self):
        """
        Alert stage executors for termination
        """
        self._terminate_event.set()
        wait(self._futures)

    def is_terminated(self) -> bool:
        """
        Check if termination has been set and all stage executions have been terminated
        """
        return (
            all(future.done() or future.cancelled() for future in self._futures)
            and self._terminate_event.is_set()
        )

    def _run(
        self,
        stage: StageType,
        _executor: StageExecutor,
        in_queue: ItemsQueue,
        out_queue: ItemsQueue,
        error_manager: ErrorManager,
    ):
        """
        Start the concurrent execution of stage processing.
        The stage will consume and produce from input/output queues concurrently

        :param stage: Stage instance
        :param _executor: Function to run concurrently in the executor, which performs the stage executions
        :param in_queue: Previous stage output queue
        :param out_queue: Output queue
        :param error_manager: Error manager instance
        """
        ex = self._get_stage_executor()
        self._counter = self._counter_initializer()
        self._terminate_event.clear()
        for _ in range(self._concurrency):
            self._futures.append(
                ex.submit(
                    _executor,
                    stage,
                    in_queue,
                    out_queue,
                    error_manager,
                    self._terminate_event,
                    self._counter,
                )
            )


class ConcurrentStageContainer(ConcurrentContainer, StageContainer):
    """
    Standard stage container with concurrency
    """

    def __init__(
        self,
        name: str,
        stage: Stage,
        error_manager: ErrorManager,
        queue_initializer: QueueInitializer,
        counter_initializer: CounterInitializer,
        terminate_event_initializer: EventInitializer,
        concurrency: int = 1,
        use_threads: bool = True,
    ):
        """
        :param name: Stage name
        :param stage: Stage instance
        :param error_manager: Error manager instance
        :param queue_initializer: Constructor for output, and eventually input, queue
        :param counter_initializer: Constructor for items counter, it counts items seen by concurrent stage executions
        :param terminate_event_initializer: Constructor for the event for alerting all concurrent stage executions for termination
        :param concurrency: Number of maximum concurrent stage executions
        :param use_threads: True for using threads for concurrency, otherwise use multiprocess
        """
        super().__init__(name, stage, error_manager)
        self.init_concurrency(
            queue_initializer,
            counter_initializer,
            terminate_event_initializer,
            concurrency,
            use_threads,
        )

    def run(self, _executor: StageExecutor = stage_executor):
        """
        Start the concurrent execution of stage processing.
        The stage will consume and produce from input/output queues concurrently

        :param _executor: Function to run in the executor, which performs the stage executions
        """
        super()._run(
            self.stage,
            _executor,
            self._previous_queue,
            self.out_queue,
            self.error_manager,
        )


class BatchConcurrentStageContainer(ConcurrentContainer, BatchStageContainer):
    """
    Batch stage container with concurrency
    """

    def __init__(
        self,
        name: str,
        stage: BatchStage,
        error_manager: ErrorManager,
        queue_initializer: QueueInitializer,
        counter_initializer: CounterInitializer,
        terminate_event_initializer: EventInitializer,
        concurrency: int = 1,
        use_threads: bool = True,
    ):
        """
        :param name: Stage name
        :param stage: Stage instance
        :param error_manager: Error manager instance
        :param queue_initializer: Constructor for output, and eventually input, queue
        :param counter_initializer: Constructor for items counter, it counts items seen by concurrent stage executions
        :param terminate_event_initializer: Constructor for the event for alerting all concurrent stage executions for termination
        :param concurrency: Number of maximum concurrent stage executions
        :param use_threads: True for using threads for concurrency, otherwise use multiprocess
        """
        super().__init__(name, stage, error_manager)
        self.init_concurrency(
            queue_initializer,
            counter_initializer,
            terminate_event_initializer,
            concurrency,
            use_threads,
        )

    def run(self, _executor: StageExecutor = batch_stage_executor):
        """
        Start the concurrent execution of stage processing.
        The stage will consume and produce from input/output queues concurrently

        :param _executor: Function to run in the executor, which performs the stage executions
        """
        super()._run(
            self.stage,
            _executor,
            self._previous_queue,
            self.out_queue,
            self.error_manager,
        )
