from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures._base import Executor, Future
from concurrent.futures.thread import ThreadPoolExecutor
from logging.handlers import QueueHandler
from multiprocessing import Manager, get_context
from queue import Queue
from threading import Event, Thread
from typing import (
    Any,
    Callable,
    Generator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from smartpipeline.containers import (
    BaseContainer,
    BatchConcurrentStageContainer,
    BatchStageContainer,
    ConcurrentStageContainer,
    ConnectedStageMixin,
    SourceContainer,
    StageContainer,
)
from smartpipeline.defaults import CONCURRENCY_WAIT, MAX_QUEUES_SIZE
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import Item, Stop
from smartpipeline.stage import BatchStage, ItemsQueue, Source, StageType
from smartpipeline.utils import (
    LastOrderedDict,
    LogsReceiver,
    ProcessCounter,
    ThreadCounter,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def _stage_initialization_with_logger(logs_queue, stage_class, args, kwargs):
    root_logger = logging.getLogger()
    # only by comparing string of queues we obtain their "original" address
    if not any(
        isinstance(handler, QueueHandler) and str(handler.queue) == str(logs_queue)
        for handler in root_logger.handlers
    ):
        handler = QueueHandler(logs_queue)
        root_logger.addHandler(handler)
    return stage_class(*args, **kwargs)


class Pipeline:
    def __init__(
        self,
        max_init_workers: Optional[int] = None,
        max_queues_size: int = MAX_QUEUES_SIZE,
    ):
        """
        :param max_init_workers: Number of workers to use for concurrent initialization of stages, default the number of CPUs
        :param max_queues_size: Maximum size of any queue instanced for the pipeline (stage input and output queues)
        """
        self._containers = LastOrderedDict()
        self._error_manager = ErrorManager()
        self._max_init_workers = max_init_workers
        self._init_executor = None
        self._wait_previous_executor = None
        self._pipeline_executor = None
        self._max_queues_size = max_queues_size
        self._out_queue = None
        self._enqueue_source = False
        # we approach sharing memory between processes exclusively through a `multiprocessing.Manager`
        self._sync_manager = None
        # an empty source, on which we can only occasionally send items
        self._count = 0
        self._executors_ready = False
        self._logs_receiver = None
        self._name = f"{self.__class__.__name__}-{str(uuid.uuid4())[:8]}"
        self._logger = logging.getLogger(self._name)
        self._source_name = f"SourceOf{self._name}"
        self._source_container = SourceContainer(self._source_name)

    def _start_logs_receiver(self) -> LogsReceiver:
        if self._logs_receiver is None:
            if self._sync_manager is None:
                self._sync_manager = Manager()
            logs_queue = self._sync_manager.Queue()
            self._logs_receiver = LogsReceiver(logs_queue)
            self._logs_receiver.start()
        return self._logs_receiver

    def _stop_logs_receiver(self):
        if self._logs_receiver is not None:
            self._logs_receiver.stop()
            self._logs_receiver = None

    def _get_logs_receiver_queue(self) -> Queue:
        if self._logs_receiver is not None:
            return self._logs_receiver.queue
        else:
            raise AttributeError("Logs queue for multiprocessing never initialized")

    @property
    def name(self) -> str:
        """
        Pipeline unique name, equivalent to its logger name
        """
        return self._name

    def _new_mp_queue(self) -> ItemsQueue:
        """
        Construct queue for multiprocessing communication
        """
        # why not use multiprocessing.Queue or JoinableQueue instead of the manager?
        # answer https://stackoverflow.com/a/45236748 and the lack of "safety" of task_done() and join()
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return self._sync_manager.Queue(maxsize=self._max_queues_size)

    def _new_queue(self) -> ItemsQueue:
        """
        Construct queue for communication
        """
        return Queue(maxsize=self._max_queues_size)

    def _new_mp_event(self) -> Event:
        """
        Construct synchronization event for multiprocessing
        """
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return self._sync_manager.Event()

    @staticmethod
    def _new_event() -> Event:
        """
        Construct synchronization event
        """
        return Event()

    def _new_mp_counter(self) -> ProcessCounter:
        """
        Construct a safe counter for multiprocessing
        """
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return ProcessCounter(self._sync_manager)

    @staticmethod
    def _new_counter() -> ThreadCounter:
        """
        Construct a safe counter for threads
        """
        return ThreadCounter()

    def _wait_executors(self, wait_seconds: float = CONCURRENCY_WAIT):
        """
        Wait for all containers to start

        :param wait_seconds: Recurrently wait these seconds for all stage initializers to finish
        """
        if self._executors_ready:
            return
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        while not all(self._containers.values()):
            time.sleep(wait_seconds)
        self._wait_previous_executor.shutdown(wait=True)
        for name, container in self._containers.items():
            if isinstance(
                container, (ConcurrentStageContainer, BatchConcurrentStageContainer)
            ):
                container.run()
        # finalize initialization of the error manager shared by this and other stage threads
        self._error_manager.on_start()
        self._executors_ready = True
        self._logger.debug("Pipeline ready to run")

    def shutdown(self):
        """
        Execute shutdown of various pool executors and multiprocessing stuff.
        The developer should not need to call it explicitly.
        """
        if self._out_queue is not None:
            self._out_queue.join()
        if self._init_executor is not None:
            self._init_executor.shutdown()
        for name, container in self._containers.items():
            if isinstance(
                container, (ConcurrentStageContainer, BatchConcurrentStageContainer)
            ):
                container.shutdown()
        if self._sync_manager is not None:
            self._sync_manager.shutdown()

    def build(self) -> Pipeline:
        """
        Pipeline builder method
        """
        if not any(self._containers):
            raise ValueError("Must append at least a stage")
        self._logger.debug("Building the pipeline on stages: %s", self._log_stages())
        self._wait_executors()
        return self

    def _exit_run(self, source_thread, terminator_thread):
        self.stop()
        if source_thread is not None:
            source_thread.join()
        # TODO in case of errors we loose pending items!
        self._terminate_all(force=True)
        if terminator_thread is not None:
            terminator_thread.join()
        self.shutdown()
        self._count += 1

    def run(self) -> Generator[Item, None, None]:
        """
        Run the pipeline given a source and a concatenation of stages.
        Get the sequence of items through iteration

        :return: Iterator over processed items
        :raises ValueError: When a source has not been set for the pipeline
        """
        if not self._source_container.is_set():
            raise ValueError("Set the data source for this pipeline")
        self._logger.debug("Running the pipeline on stages: %s", self._log_stages())
        counter = 0
        last_stage_name = self._last_stage_name()
        terminator_thread = None
        source_thread = None
        # in case the first stage is concurrent
        if self._enqueue_source:
            source_thread = Thread(target=self._source_container.pop_into_queue)
            source_thread.start()
        while True:
            for name, container in self._containers.items():
                try:
                    # concurrent stages run by themselves in threads/processes
                    if not isinstance(
                        container,
                        (ConcurrentStageContainer, BatchConcurrentStageContainer),
                    ):
                        container.process()
                    # but me must periodically check for errors
                    else:
                        container.check_errors()
                except Exception as e:
                    self._exit_run(source_thread, terminator_thread)
                    raise e
                # retrieve finally processed items from the last stage
                if name == last_stage_name:
                    for _ in range(
                        container.size
                        if isinstance(container, BatchStageContainer)
                        else 1
                    ):
                        item = container.get_processed()
                        if item is not None:
                            if not isinstance(item, Stop):
                                try:
                                    yield item
                                except GeneratorExit:
                                    # when the caller exits the run loop terminates everything
                                    self._exit_run(source_thread, terminator_thread)
                                    return
                                finally:
                                    counter += 1
                                self._count += 1
                            # if a stop is finally signaled, start termination of all containers
                            elif (
                                not self._all_terminated() and terminator_thread is None
                            ):
                                terminator_thread = Thread(target=self._terminate_all)
                                terminator_thread.start()
                        # an item is None if the final output queue is empty
                        else:
                            break
            # exit the loop only when all items have been returned
            if self._all_empty() and counter >= self._source_container.count():
                if source_thread is not None:
                    source_thread.join()
                if terminator_thread is not None:
                    terminator_thread.join()
                    self.shutdown()
                return

    @property
    def count(self) -> int:
        """
        Get the number of items processed by all executed runs, also for items which have failed

        :return: Count of processed items
        """
        return self._count

    def _terminate_all(
        self, force: bool = False, wait_seconds: float = CONCURRENCY_WAIT
    ):
        """
        Terminate all running containers

        :param force: If True do not wait for a container to process all items produced by the source
        :param wait_seconds: Time to wait before pinging again a container for its termination
        """
        self._logger.debug("Terminating the pipeline")
        # scroll the pipeline by its order (this is critical for correctness)
        # and terminate stages after the relative queues are empty
        for container in self._containers.values():
            if not force:
                # ensure the stage has processed all source items
                while container.count() < self._source_container.count():
                    time.sleep(wait_seconds)
            container.terminate()
            if isinstance(container, ConcurrentStageContainer):
                if force:
                    # empty the queues, losing pending items
                    container.empty_queues()
                while not container.is_terminated():
                    time.sleep(wait_seconds)
                container.queues_join()
                while not container.queues_empty():
                    time.sleep(wait_seconds)
            if (
                not isinstance(container, ConcurrentStageContainer)
                or not container.parallel
            ):
                container.stage.on_end()
        self._error_manager.on_end()
        self._stop_logs_receiver()
        self._logger.debug("Termination done")

    def _all_terminated(self) -> bool:
        """
        Check if all containers have been alerted for termination and are exited
        """
        return all(container.is_terminated() for container in self._containers.values())

    def _all_empty(self) -> bool:
        """
        Check if all containers are terminated and there are not items left in the queues
        """
        return self._all_terminated() and all(
            container.queues_empty()
            for container in self._containers.values()
            if isinstance(
                container, (ConcurrentStageContainer, BatchConcurrentStageContainer)
            )
        )

    def process(self, item: Item) -> Item:
        """
        Process a single item synchronously (no concurrency) through the pipeline
        """
        self._logger.debug("Processing %s on stages: %s", item, self._log_stages())
        last_stage_name = self._containers.last_key()
        self._source_container.prepend_item(item)
        for name, container in self._containers.items():
            container.process()
            if name == last_stage_name:
                return container.get_processed(block=True)

    def process_async(
        self, item: Item, callback: Optional[Callable[[Item], Any]] = None
    ):
        """
        Process a single item asynchronously through the pipeline, stages may run concurrently.
        The call returns immediately, processed items are retrieved with :meth:`.Pipeline.get_item`

        :param callback: A function to call after a successful process of the item
        """
        self._logger.debug(
            "Processing asynchronously %s on stages: %s", item, self._log_stages()
        )
        if callback is not None:
            item.set_callback(callback)
        self._source_container.prepend_item(item)
        self._start_pipeline_executor()

    def stop(self):
        """
        Tell the source to stop to generate items and consequently terminate the pipeline
        after all "remaining" items are processed.
        """
        self._source_container.stop()

    def get_item(self, block: bool = True) -> Item:
        """
        Get a single item from the asynchronous execution of the pipeline on single items from :meth:`.Pipeline.process_async`

        :param block: If True wait indefinitely for the next processed item
        :raises ValueError: When there is not output queue set, the pipeline is not running asynchronously
        :raises queue.Empty: When we do not block and the queue is empty
        """
        if self._out_queue is not None:
            item = self._out_queue.get(block)
            self._out_queue.task_done()
            return item
        else:
            raise ValueError(
                "No pipeline is running asynchronously, not item can be retrieved from the output queue"
            )

    def set_source(self, source: Source) -> Pipeline:
        """
        Set the source of the pipeline: a subclass of :class:`.stage.Source`
        """
        source.set_name(self._source_name)
        self._source_container.set(source)
        return self

    def set_error_manager(self, error_manager: ErrorManager) -> Pipeline:
        """
        Set the error manager for handling errors from each stage item processing
        """
        self._error_manager = error_manager
        for container in self._containers.values():
            container.set_error_manager(self._error_manager)
        return self

    def _last_stage_name(self) -> str:
        if self._containers:
            return self._containers.last_key()

    def _wait_for_previous(
        self,
        container: ConnectedStageMixin,
        last_stage_name: str,
        wait_seconds: float = CONCURRENCY_WAIT,
    ):
        """
        Given a container we want to append to the pipeline, wait for the last one (added to the pipeline) to be created

        :param container: A container to add to the pipeline
        :param last_stage_name: Name of the last stage currently in the pipeline
        :param wait_seconds: Time to recurrently wait the construction of the container relative to the last stage in the pipeline
        """

        def _waiter():
            if last_stage_name is not None:
                while self._containers[last_stage_name] is None:
                    time.sleep(wait_seconds)
                container.set_previous(self._containers[last_stage_name])
            else:
                container.set_previous(self._source_container)

        executor = self._get_wait_previous_executor()
        executor.submit(_waiter)

    def _build_container(
        self,
        name: str,
        stage: StageType,
        concurrency: int,
        parallel: bool,
        retryable_errors: Tuple[Type[Exception], ...],
        max_retries: int,
        backoff: Union[float, int],
    ) -> BaseContainer:
        """
        Get a new container instance according to the pipeline configuration

        :param name: Stage name
        :param stage: A stage instance
        :param concurrency: Number of concurrent stage executions, if 0 then just create the non-concurrent containers
        :param parallel: If True use multiprocessing, otherwise threads
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. When the maximum number of retries is hit, a :class:`.error.exceptions.RetryError` is raised
        :param max_retries: maximum number of retries for the stage before raising a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        """
        retry_manager = RetryManager(
            backoff=backoff, max_retries=max_retries, retryable_errors=retryable_errors
        )
        if concurrency <= 0:
            constructor = (
                BatchStageContainer if isinstance(stage, BatchStage) else StageContainer
            )
            # if not concurrent we must explicitly finalize initialization of this single stage object
            stage.on_start()
            return constructor(name, stage, self._error_manager, retry_manager)
        else:
            constructor = (
                BatchConcurrentStageContainer
                if isinstance(stage, BatchStage)
                else ConcurrentStageContainer
            )
            if parallel:
                self._start_logs_receiver()
                logs_queue = self._get_logs_receiver_queue()
                return constructor(
                    name,
                    stage,
                    self._error_manager,
                    retry_manager,
                    self._new_mp_queue,
                    self._new_mp_counter,
                    self._new_mp_event,
                    concurrency,
                    parallel,
                    logs_queue,
                )
            else:
                # if the stage is executed on multiple threads we must finalize initialization once,
                # while on multiprocessing each process executor calls it for its own copy of the stage
                stage.on_start()
                return constructor(
                    name,
                    stage,
                    self._error_manager,
                    retry_manager,
                    self._new_queue,
                    self._new_counter,
                    self._new_event,
                    concurrency,
                    parallel,
                )

    def get_stage(self, name: str) -> StageType:
        """
        Get a stage instance by its name
        """
        return self._containers.get(name).stage

    def append(
        self,
        name: str,
        stage: StageType,
        concurrency: int = 0,
        parallel: bool = False,
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
    ) -> Pipeline:
        """
        Append a stage to the pipeline just after the last one appended, or after the source if it is the first stage

        :param name: Name for identify the stage in the pipeline, it is also set in the stage and it must be unique in the pipeline
        :param stage: Instance of a stage
        :param concurrency: Number of concurrent stage executions, if 0 then threads/processes won't be involved for this stage
        :param parallel: If True use multiprocessing, otherwise threads
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. When the maximum number of retries is hit, a :class:`.error.exceptions.RetryError` is raised
        :param max_retries: maximum number of retries for the stage before raising a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        """
        self._executors_ready = False
        # FIXME here we force a BatchStage to run on a thread, but we would leave it on the main thread
        if concurrency < 1 and isinstance(stage, BatchStage):
            parallel = False
            concurrency = 1
        self._check_stage_name(name)
        self._check_retries_params(retryable_errors, max_retries, backoff)
        container = self._build_container(
            name, stage, concurrency, parallel, retryable_errors, max_retries, backoff
        )
        if concurrency > 0:
            # if it is concurrent and it is the first stage, make the source work on a output queue
            if not self._containers:
                self._enqueue_source = True
        self._wait_for_previous(
            container, self._last_stage_name()
        )  # wait that previous stage is initialized
        self._containers[name] = container
        return self

    # deprecated
    append_stage = append  # pragma: no cover

    def append_concurrently(
        self,
        name: str,
        stage_class: Callable,
        args: Sequence = None,
        kwargs: Mapping = None,
        concurrency: int = 0,
        parallel: bool = False,
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
    ) -> Pipeline:
        """
        Append a stage class to the pipeline just after the last one appended, or after the source if it is the first stage.
        The stage construction will be executed concurrently respect to the general pipeline construction

        :param name: Name for identify the stage in the pipeline, it is also set in the stage and it must be unique in the pipeline
        :param stage_class: Class of a stage
        :param args: List of arguments for the stage constructor
        :param kwargs: Dictionary of keyed arguments for the stage constructor
        :param concurrency: Number of concurrent stage executions, if 0 then threads/processes won't be involved for this stage
        :param parallel: If True use multiprocessing, otherwise threads (also for concurrent construction)
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. When the maximum number of retries is hit, a :class:`.error.exceptions.RetryError` is raised
        :param max_retries: maximum number of retries for the stage before raising a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        """
        self._executors_ready = False
        parallel_construction = parallel
        # FIXME here we force a BatchStage to run on a thread, but we would leave it on the main thread
        if concurrency < 1 and issubclass(stage_class, BatchStage):
            parallel = False
            concurrency = 1
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        self._check_stage_name(name)
        self._check_retries_params(retryable_errors, max_retries, backoff)
        # if it is concurrent and it is the first stage, make the source work on a output queue
        if concurrency > 0 and not self._containers:
            self._enqueue_source = True
        last_stage_name = self._last_stage_name()
        # set it immediately so the order of the calls of this method is followed in `_containers`
        self._containers[name] = None
        self._start_logs_receiver()
        logs_queue = self._get_logs_receiver_queue()
        executor = self._get_init_executor(parallel_construction)
        if parallel_construction:
            future = executor.submit(
                _stage_initialization_with_logger, logs_queue, stage_class, args, kwargs
            )
        else:
            future = executor.submit(stage_class, *args, **kwargs)

        def append_stage(stage_future: Future):
            stage = stage_future.result()
            container = self._build_container(
                name,
                stage,
                concurrency,
                parallel,
                retryable_errors,
                max_retries,
                backoff,
            )
            self._wait_for_previous(container, last_stage_name)
            self._containers[name] = container

        future.add_done_callback(append_stage)
        return self

    # deprecated
    append_stage_concurrently = append_concurrently

    def _get_init_executor(self, parallel: bool = False) -> Executor:
        """
        Get a pool executor for concurrent stage initialization

        :param parallel: True if the executor uses multiprocessing, otherwise treads
        """
        if self._init_executor is None:
            if not parallel:
                self._init_executor = ThreadPoolExecutor(
                    max_workers=self._max_init_workers
                )
            else:
                self._init_executor = ProcessPoolExecutor(
                    max_workers=self._max_init_workers, mp_context=get_context("spawn")
                )
        return self._init_executor

    def _get_wait_previous_executor(self) -> Executor:
        """
        Get a pool executor for the function that will recurrently wait for a container to be ready
        """
        if self._wait_previous_executor is None:
            self._wait_previous_executor = ThreadPoolExecutor()
        return self._wait_previous_executor

    def _start_pipeline_executor(self) -> Thread:
        """
        Get a thread where to run a pipeline that accepts asynchronous processing of single items
        """
        if self._pipeline_executor is None:
            self._init_out_queue()

            def pipeline_runner():
                for item in self.run():
                    item.callback()
                    self._out_queue.put(item)

            self._pipeline_executor = Thread(target=pipeline_runner)
            self._pipeline_executor.start()
        return self._pipeline_executor

    def _check_stage_name(self, name: str):
        """
        Check if a stage name is not already defined in the pipeline
        :raises ValueError: Stage name is already defined in the pipeline
        """
        if name in self._containers:
            raise ValueError(f"The stage name {name} is already used in this pipeline")

    @staticmethod
    def _check_retries_params(
        retryable_errors: Tuple[Type[Exception], ...],
        max_retries: int,
        backoff: Union[float, int],
    ):
        """
        Check the values used to apply the retry strategy for a stage
        """
        if not (isinstance(backoff, (float, int)) and backoff >= 0):
            raise ValueError(
                "The `backoff` parameter must be either a float or int and its value must be >= 0"
            )
        if not (isinstance(max_retries, int) and max_retries >= 0):
            raise ValueError(
                "The `max_retries` parameter must be an int and its value must be >= 0"
            )
        if not (
            isinstance(retryable_errors, tuple)
            and all(issubclass(err, Exception) for err in retryable_errors)
        ):
            raise ValueError(
                "The `retryable_errors` parameter must be a tuple and all its elements must be Exceptions"
            )

    def _init_out_queue(self):
        """
        Get the internal output pipeline for asynchronous processing of single items
        """
        self._out_queue = self._new_queue()

    def _log_stages(self):
        return ", ".join(self._containers.keys())
