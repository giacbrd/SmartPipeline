from __future__ import annotations

import logging
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures._base import Executor, Future
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from inspect import isclass
from logging.handlers import QueueHandler
from multiprocessing import Manager, get_context
from multiprocessing.managers import SyncManager
from queue import Queue
from threading import Event, RLock, Thread, current_thread
from typing import Any, Callable, Generator, Generic, Mapping, Optional
from typing import OrderedDict as OrderedDictType
from typing import Self, Sequence, Tuple, Type, Union

from smartpipeline.containers import (
    BatchConcurrentStageContainer,
    BatchStageContainer,
    ConcurrentContainerTypes,
    ConcurrentStageContainer,
    ConnectedStageMixin,
    ContainerType,
    ExecutorType,
    SourceContainer,
    StageContainer,
)
from smartpipeline.defaults import (
    CONCURRENCY_WAIT,
    MAX_QUEUES_SIZE,
    PIPELINE_CLOSE_TIMEOUT,
    RUN_LOOP_BACKOFF,
)
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import DataT, Item, Stop
from smartpipeline.stage import BatchStage, ItemsQueue, Source, Stage
from smartpipeline.utils import (
    LogsReceiver,
    ProcessCounter,
    ThreadCounter,
    last_key,
    raise_collected_errors,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def _stage_initialization_with_logger(
    logs_queue: Queue,
    stage_class: Type[Stage[DataT]] | Type[BatchStage[DataT]],
    args: Sequence,
    kwargs: Mapping,
) -> Stage[DataT] | BatchStage[DataT]:
    root_logger = logging.getLogger()
    # only by comparing string of queues we obtain their "original" address
    if not any(
        isinstance(handler, QueueHandler) and str(handler.queue) == str(logs_queue)
        for handler in root_logger.handlers
    ):
        handler = QueueHandler(logs_queue)
        root_logger.addHandler(handler)
    return stage_class(*args, **kwargs)


class _EmptyStage(Stage[Any]):
    def process(self, item: Item[Any]) -> Item[Any]:
        return item


class _AsyncPipelineError:
    """An error raised by the asynchronous pipeline runner, delivered by `get_item`."""

    def __init__(self, error: BaseException):
        self.error = error


class PipelineState(Enum):
    """
    Lifecycle state of a :class:`.Pipeline`.

    A pipeline is built once, can process single items synchronously, and can run
    its source once. After a run (or an explicit :meth:`.Pipeline.close`) it is closed
    and cannot be reused.
    """

    INITIAL = "initial"
    BUILT = "built"
    RUNNING = "running"
    CLOSED = "closed"


class PipelineStateError(RuntimeError):
    """Raised when an operation is not allowed in the current pipeline state."""


class Pipeline(Generic[DataT]):
    """
    Main class for designing the sequence of stages and execute them,
    e.g. `Pipeline[MyData]` runs `Item[MyData]` items through `Stage[MyData]` stages.
    `Pipeline` without parameters defaults to `Dict[str, Any]` payloads.
    """

    def __init__(
        self,
        max_init_workers: Optional[int] = None,
        max_queues_size: int = MAX_QUEUES_SIZE,
    ):
        """
        :param max_init_workers: Number of workers to use for concurrent initialization of stages, default the number of CPUs
        :param max_queues_size: Maximum size of any queue instanced for the pipeline (stage input and output queues)
        """
        self._containers: OrderedDictType[
            str, StageContainer[DataT] | BatchStageContainer[DataT]
        ] = OrderedDict()
        self._error_manager = ErrorManager()
        self._max_init_workers = max_init_workers
        self._init_executor: Optional[Executor] = None
        self._wait_previous_executor: Optional[Executor] = None
        self._pipeline_executor: Optional[Thread] = None
        self._max_queues_size = max_queues_size
        self._out_queue: Optional[Queue[Any]] = None
        self._enqueue_source = False
        # `process` is synchronous and cannot drive the runners of concurrent stages
        self._has_concurrent_stages = False
        # we approach sharing memory between processes exclusively through a `multiprocessing.Manager`
        self._sync_manager: Optional[SyncManager] = None
        self._count = 0
        self._executors_ready = False
        self._state = PipelineState.INITIAL
        # The build failure is kept and raised again, instead of leaving a closed
        # pipeline which reports only a generic state error.
        self._build_error: Optional[BaseException] = None
        self._stages_started = False
        # Stages whose `on_start` ran in this process: their `on_end` must run here too.
        # Process-based stages initialize and close their own copies in the workers.
        self._started_in_main: list[tuple[str, ContainerType]] = []
        self._error_manager_started = False
        self._terminated_all = False
        self._run_finished = False
        self._close_requested = False
        self._resources_released = False
        self._closing = False
        # Serializes the start phase with termination/close: a close() which races
        # with `_start_stages()` must not release the executors of runners which are
        # still being submitted. Reentrant because `_abort_start()` (called from the
        # start phase) terminates and releases resources on the same thread.
        self._lifecycle_lock = RLock()
        self._fatal_event: Optional[Event] = None
        self._source_thread: Optional[Thread] = None
        self._logs_receiver: Optional[LogsReceiver] = None
        self._name = f"{self.__class__.__name__}-{str(uuid.uuid4())[:8]}"
        self._logger = logging.getLogger(self._name)
        self._source_name = f"SourceOf{self._name}"
        # Initialize with a trivial "support" source, on which we can eventually send items when using `process` methods
        self._source_container: SourceContainer[DataT] = SourceContainer(
            self._source_name
        )
        # A fake container for the pipeline to support data structures initialization
        self._fake_container: StageContainer[DataT] = StageContainer(
            f"FakeContainerOf{self._name}",
            _EmptyStage(),
            ErrorManager(),
            RetryManager(),
        )
        # failures of concurrent stage constructions (`append_concurrently`),
        # callbacks run in other threads so errors are stored and raised on `build()`
        self._init_errors: list[Exception] = []

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

    @property
    def state(self) -> PipelineState:
        """
        Current lifecycle state of the pipeline
        """
        return self._state

    def _require_initial(self, action: str) -> None:
        if self._state is not PipelineState.INITIAL:
            raise PipelineStateError(
                f"Cannot {action} a pipeline in state '{self._state.value}'; "
                "configure the pipeline before calling build()"
            )

    def _require_not_running(self, action: str) -> None:
        if self._state in (PipelineState.RUNNING, PipelineState.CLOSED):
            raise PipelineStateError(
                f"Cannot {action} a pipeline in state '{self._state.value}'"
            )

    def _ensure_built(self) -> None:
        if self._state is PipelineState.INITIAL:
            self.build()
        elif self._state is PipelineState.BUILT:
            return
        elif self._state is PipelineState.CLOSED:
            if self._build_error is not None:
                raise self._build_error
            raise PipelineStateError(
                "The pipeline is closed; a Pipeline can be run only once"
            )
        else:
            raise PipelineStateError("The pipeline is already running")

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

    def _new_shared_event(self) -> Event:
        """
        Construct a synchronization event shared by all the containers and stage runners of
        this pipeline, multiprocessing safe when the pipeline uses processes
        """
        return (
            self._new_mp_event()
            if self._sync_manager is not None
            else self._new_event()
        )

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
        Wait for all containers to be constructed and prepare them to run.

        This only validates and wires the pipeline: stages and workers are started
        later, by :meth:`.Pipeline.run` or :meth:`.Pipeline.process_async`.

        :param wait_seconds: Recurrently wait these seconds for all stage initializers to finish
        :raises Exception: Whatever exception has been raised by a stage construction,
            instead of hanging without feedback
        """
        if self._executors_ready:
            return
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        # A stage container which is still a placeholder is under construction: stage
        # constructions (and their done-callbacks, which run synchronously when the
        # relative future completes) are already joined by the shutdown above, so normally
        # no placeholder is left at this point. This loop only covers the narrow window in
        # which a `ProcessPoolExecutor` result delivery (and its callback replacing the
        # placeholder) lags behind, and it gives up on a construction error instead of
        # hanging without feedback.
        while not self._init_errors and any(
            container is self._fake_container for container in self._containers.values()
        ):
            time.sleep(wait_seconds)
        if self._wait_previous_executor is not None:
            self._wait_previous_executor.shutdown(wait=True)
            self._wait_previous_executor = None
        if self._init_errors:
            # The errors are kept, not cleared: the pipeline cannot be built, so any further
            # build()/run() must report the failure again instead of hanging on a stage
            # container which will never be ready.
            raise_collected_errors(
                list(self._init_errors), "pipeline stage initialization failed"
            )
        # all the containers are now ready, we can share the event which alerts a stage runner error
        self._fatal_event = self._new_shared_event()
        self._source_container.set_fatal_event(self._fatal_event)
        for name, container in self._containers.items():
            container.set_fatal_event(self._fatal_event)
        self._executors_ready = True
        self._logger.debug("Pipeline prepared to run")

    def _start_stages(self):
        """
        Run the start phase: `on_start` hooks and concurrent stage runners.

        Inline and threaded stages initialize once in this process; process-based
        stages initialize their own copies inside each worker. If the start phase
        fails, already started stages are terminated and their `on_end` is called.

        The phase holds the lifecycle lock, so a concurrent :meth:`.Pipeline.close`
        waits for it instead of releasing the executors of runners which are still
        being submitted; when the close request comes first, the phase does not
        start anything.
        """
        if self._stages_started:
            return
        with self._lifecycle_lock:
            if self._stages_started:
                return
            if self._close_requested or self._state is PipelineState.CLOSED:
                # A close() won the race: it owns the termination of this pipeline.
                return
            self._terminated_all = False
            self._started_in_main.clear()
            self._error_manager_started = False
            try:
                for name, container in self._containers.items():
                    if self._close_requested:
                        # close() asked to stop while starting: never submit more
                        # runners, it terminates the ones already started
                        return
                    if isinstance(container, ConcurrentContainerTypes):
                        if container.executor == "threads":
                            container.stage.on_start()
                            self._started_in_main.append((name, container))
                        container.run()
                    else:
                        container.stage.on_start()
                        self._started_in_main.append((name, container))
                # The main error manager starts only after the process-based runners
                # have been submitted: its `on_start` may open non-picklable resources,
                # and worker copies must be serialized before that.
                if self._close_requested:
                    return
                self._error_manager.on_start()
                self._error_manager_started = True
            except BaseException:
                self._abort_start()
                raise
            self._stages_started = True
            self._logger.debug("Pipeline stages started")

    def _abort_start(self):
        """Terminate a pipeline whose start phase failed, without masking the original error."""
        self._logger.exception("Pipeline start failed")
        self._state = PipelineState.CLOSED
        try:
            self._terminate_all(force=True)
        except BaseException:
            self._logger.exception(
                "Problems in terminating the pipeline after a start failure"
            )
        finally:
            self._release_resources()

    def _release_resources(self):
        """
        Release executors, the logs receiver and the multiprocessing manager.

        Idempotent: it can be called by both the run loop and :meth:`.Pipeline.close`.
        It holds the lifecycle lock, so it never releases something which another
        thread is still starting.
        """
        with self._lifecycle_lock:
            if self._resources_released:
                return
            self._resources_released = True
            if self._init_executor is not None:
                try:
                    self._init_executor.shutdown(wait=True)
                except BaseException:
                    self._logger.exception(
                        "Problems in shutting down the init executor"
                    )
                self._init_executor = None
            if self._wait_previous_executor is not None:
                try:
                    self._wait_previous_executor.shutdown(wait=True)
                except BaseException:
                    self._logger.exception(
                        "Problems in shutting down the linker executor"
                    )
                self._wait_previous_executor = None
            for container in self._containers.values():
                if isinstance(container, ConcurrentContainerTypes):
                    try:
                        container.shutdown()
                    except BaseException:
                        self._logger.exception(
                            "Problems in shutting down %s", container
                        )
            self._stop_logs_receiver()
            if self._sync_manager is not None:
                try:
                    self._sync_manager.shutdown()
                except BaseException:
                    self._logger.exception(
                        "Problems in shutting down the multiprocessing manager"
                    )
                self._sync_manager = None

    def _join_thread(self, thread: Optional[Thread], description: str, timeout: float):
        if thread is None or thread is current_thread() or not thread.is_alive():
            return
        thread.join(timeout=timeout)
        if thread.is_alive():
            self._logger.warning(
                "The %s did not end in %s seconds", description, timeout
            )

    def close(self, timeout: float = PIPELINE_CLOSE_TIMEOUT):
        """
        Close the pipeline, idempotently.

        Running workers are terminated, pending queues are drained, and `on_end` hooks
        run for every stage whose `on_start` completed. Executors and the multiprocessing
        manager are released. A closed pipeline cannot be built or run again.

        When another thread is starting the pipeline, the close request stops the start
        phase and this call waits for it, so the runners it submits are terminated too.

        :param timeout: Time to wait for the source thread and the asynchronous runner
        """
        if self._state is PipelineState.CLOSED:
            return
        if self._closing:
            return
        self._closing = True
        self._state = PipelineState.CLOSED
        termination_error: Optional[BaseException] = None
        try:
            self._close_requested = True
            self._run_finished = True
            try:
                self.stop()
            except BaseException:
                self._logger.exception("Problems in stopping the pipeline")
            # Serialized with `_start_stages`: when it is in progress this waits for
            # it (the close request makes it stop early) and terminates every runner
            # it has submitted, instead of releasing busy executors underneath it.
            # `_terminate_all` is idempotent, so it is safe also when nothing started.
            with self._lifecycle_lock:
                try:
                    self._terminate_all(force=True)
                except BaseException as e:
                    termination_error = e
                # consumers are gone: unblock a source thread waiting on a full queue
                if self._fatal_event is not None:
                    self._fatal_event.set()
            self._join_thread(self._source_thread, "source thread", timeout)
            self._source_container.empty_out_queue()
            # joined outside the lifecycle lock: the asynchronous runner may still
            # have to acquire it while it exits
            self._join_thread(
                self._pipeline_executor, "asynchronous pipeline runner", timeout
            )
            self._release_resources()
        finally:
            self._closing = False
        if termination_error is not None:
            raise termination_error

    def build(self) -> Self:
        """
        Validate and prepare the pipeline.

        Stage construction is completed and containers are wired, but no stage or
        worker is started: :meth:`.Pipeline.run` and :meth:`.Pipeline.process_async`
        start them. Calling ``build()`` again on a built pipeline is a no-op.

        :raises ValueError: When no stage has been appended
        :raises PipelineStateError: When the pipeline is running or closed
        """
        # Serialized with close(): a pipeline closed while it is being built must not
        # be resurrected to BUILT after its resources have been released.
        with self._lifecycle_lock:
            if self._state is PipelineState.BUILT:
                return self
            if self._state is PipelineState.CLOSED:
                if self._build_error is not None:
                    raise self._build_error
                raise PipelineStateError(
                    "The pipeline is closed; a Pipeline can be built only once"
                )
            if self._state is PipelineState.RUNNING:
                raise PipelineStateError("The pipeline is already running")
            if not any(self._containers):
                raise ValueError("Must append at least a stage")
            self._logger.debug(
                "Building the pipeline on stages: %s", self._log_stages()
            )
            try:
                self._wait_executors()
            except BaseException as e:
                self._build_error = e
                self._state = PipelineState.CLOSED
                self._release_resources()
                raise
            self._state = PipelineState.BUILT
            return self

    def _is_fatal(self) -> bool:
        """
        True when a stage runner exited because of an error
        """
        return self._fatal_event is not None and self._fatal_event.is_set()

    def _check_errors(self):
        """
        Look for exceptions generated by stage runners of concurrent stages, eventually
        re-raising the first one, so that the pipeline terminates as any other error

        :raises Exception: Whatever exception has been raised by a stage runner
        """
        for container in self._containers.values():
            if isinstance(container, ConcurrentContainerTypes):
                container.check_errors()

    def _finish_run(
        self,
        source_thread: Optional[Thread],
        terminator_thread: Optional[Thread],
        force: bool = False,
        suppress_errors: bool = False,
    ):
        """
        Terminate the pipeline, run the closing hooks and release its resources.

        :param force: Do not wait for inline containers to consume their pending items
        :param suppress_errors: Log termination errors instead of raising them, so an
            original processing error is never masked
        """
        with self._lifecycle_lock:
            if self._run_finished:
                return
            self._run_finished = True
            self._close_requested = True
        errors: list[Exception] = []
        try:
            try:
                self.stop()
            except BaseException:
                self._logger.exception("Problems in terminating the pipeline")
            if not self._terminated_all:
                try:
                    self._terminate_all(force=force)
                except Exception as e:
                    errors.append(e)
            # On a forced termination the consumers are gone: unblock a source thread
            # which is waiting on a full queue before joining it.
            if force and self._fatal_event is not None:
                self._fatal_event.set()
            if source_thread is not None and source_thread is not current_thread():
                source_thread.join()
            if (
                terminator_thread is not None
                and terminator_thread is not current_thread()
            ):
                terminator_thread.join()
        finally:
            try:
                self._release_resources()
            except BaseException:
                self._logger.exception("Problems in releasing the pipeline resources")
            self._state = PipelineState.CLOSED
        if errors and not suppress_errors:
            raise_collected_errors(errors, "pipeline termination failed")

    def _exit_run(
        self, source_thread: Optional[Thread], terminator_thread: Optional[Thread]
    ):
        """
        Terminate the pipeline and all its executions after a critical error, without ever
        masking the original exception
        """
        try:
            self._source_container.empty_out_queue()
            self._finish_run(
                source_thread, terminator_thread, force=True, suppress_errors=True
            )
        except BaseException:
            self._logger.exception("Problems in terminating the pipeline")

    def run(self) -> Generator[Item[DataT], None, None]:
        """
        Run the pipeline given a source and a concatenation of stages.
        Get the sequence of items through iteration

        :return: Iterator over processed items
        :raises ValueError: When a source has not been set for the pipeline
        """
        # The source is checked before building or starting the pipeline, so no worker
        # is left running while the error is raised.
        if not self._source_container.is_set():
            raise ValueError("Set the data source for this pipeline")
        if self._state is PipelineState.RUNNING:
            raise PipelineStateError("The pipeline is already running")
        self._ensure_built()
        self._start_stages()
        # close() may have run while the stages were starting
        if self._state is PipelineState.CLOSED:
            return
        self._state = PipelineState.RUNNING
        self._logger.debug("Running the pipeline on stages: %s", self._log_stages())
        counter = 0
        last_stage_name = self._last_stage_name()
        terminator_thread = None
        terminator_errors: list[Exception] = []

        def _raise_terminator_error() -> None:
            raise_collected_errors(terminator_errors, "pipeline termination failed")

        source_errors_queue: Optional[Queue[Exception]] = None
        source_thread = None
        # in case the first stage is concurrent
        if self._enqueue_source:
            source_errors_queue = Queue()
            source_thread = Thread(
                target=self._source_container.pop_into_queue,
                kwargs={"errors_queue": source_errors_queue},
            )
            self._source_thread = source_thread
            source_thread.start()
        try:
            while True:
                progressed = False
                for name, container in self._containers.items():
                    # first, check errors from the source
                    if (
                        source_errors_queue is not None
                        and not source_errors_queue.empty()
                    ):
                        source_error = source_errors_queue.get(block=False)
                        if source_error is not None:
                            raise source_error
                    # concurrent stages run by themselves in threads/processes
                    if not isinstance(container, ConcurrentContainerTypes):
                        progressed |= bool(container.process())
                        # a put is aborted only when terminated or when a stage runner
                        # failed, in the latter case we raise its exception
                        if container.has_aborted_put():
                            self._check_errors()
                    # but we must periodically check them for errors
                    else:
                        container.check_errors()
                    # retrieve finally processed items from the last stage
                    if name == last_stage_name:
                        for _ in range(
                            container.size
                            if isinstance(container, BatchStageContainer)
                            else 1
                        ):
                            item = container.get_processed()
                            # an item is None if the final output queue is empty
                            if item is None:
                                break
                            progressed = True
                            if not isinstance(item, Stop):
                                # count the item as delivered before yielding it, so an
                                # early `close()` of the generator does not lose it
                                self._count += 1
                                try:
                                    yield item
                                finally:
                                    counter += 1
                            # if a stop is finally signaled, start termination of all containers
                            elif (
                                not self._all_terminated() and terminator_thread is None
                            ):

                                def _terminator():
                                    try:
                                        self._terminate_all()
                                    except Exception as e:
                                        # the thread would die silently, store the error
                                        # so the main loop raises it instead of hanging
                                        terminator_errors.append(e)

                                terminator_thread = Thread(target=_terminator)
                                terminator_thread.start()
                # a failed termination must surface instead of hanging the run loop
                if terminator_errors:
                    _raise_terminator_error()
                # back off instead of busy spinning when no container made any progress
                # in this iteration (e.g. while concurrent runners work or termination drains)
                if not progressed:
                    time.sleep(RUN_LOOP_BACKOFF)
                # an explicit close() from another thread ends the run loop
                if self._close_requested:
                    return
                # exit the loop only when all items have been returned
                if self._all_empty() and counter >= self._source_container.count():
                    self._finish_run(source_thread, terminator_thread)
                    if terminator_errors:
                        _raise_terminator_error()
                    return
        except BaseException:
            # Includes KeyboardInterrupt and GeneratorExit: never leave runners behind.
            self._exit_run(source_thread, terminator_thread)
            raise

    @property
    def count(self) -> int:
        """
        Number of items delivered to the caller.

        Counts the items yielded by :meth:`.Pipeline.run` and the items delivered by
        :meth:`.Pipeline.process_async`, including the items which carry soft errors.
        Items processed one by one with :meth:`.Pipeline.process` are not counted.

        :return: Count of delivered items
        """
        return self._count

    def _terminate_all(
        self, force: bool = False, wait_seconds: float = CONCURRENCY_WAIT
    ):
        """
        Terminate all running containers

        `on_end` lifecycle hooks are always attempted on every stage, errors are
        collected and raised at the end (as an `ExceptionGroup` when more than
        one) instead of aborting termination midway and hanging the pipeline
        without feedback.

        :param force: If True do not wait for a container to process all items produced by the source
        :param wait_seconds: Time to wait before pinging again a container for its termination
        """
        # Serialized with the start phase: terminate either before it starts or after
        # it completes, never while it is submitting runners to the executors.
        with self._lifecycle_lock:
            self._terminate_all_locked(force=force, wait_seconds=wait_seconds)

    def _terminate_all_locked(
        self, force: bool = False, wait_seconds: float = CONCURRENCY_WAIT
    ):
        if self._terminated_all:
            return
        self._terminated_all = True
        self._logger.debug("Terminating the pipeline")
        errors: list[Exception] = []
        if force and self._fatal_event is not None:
            # Force termination discards pending items: unblock any producer which
            # is waiting on a full queue, otherwise the runners never terminate.
            self._fatal_event.set()
        started_names = {name for name, _ in self._started_in_main}
        # scroll the pipeline by its order (this is critical for correctness)
        # and terminate stages after the relative queues are empty
        for name, container in self._containers.items():
            try:
                if not force:
                    self._wait_for_stopped(container, wait_seconds)
                container.terminate()
                if isinstance(container, ConcurrentContainerTypes):
                    if force:
                        # empty the queues and make the container check that is terminated
                        container.empty_queues()
                    while not container.is_terminated():
                        time.sleep(wait_seconds)
                    if force:
                        # empty the queues, losing all remaining pending items.
                        # Do not join them: the source thread may still put an item
                        # after the runners are gone, and nobody would call task_done.
                        container.empty_queues()
                    else:
                        container.queues_join()
                        while not container.queues_empty():
                            time.sleep(wait_seconds)
                    try:
                        container.check_errors()
                    except Exception as e:
                        self._logger.exception(
                            "Concurrent stage %s failed on termination", name
                        )
                        errors.append(e)
            except Exception as e:
                errors.append(e)
            # `on_end` is paired with a successful `on_start` in this process.
            # Process-based stages close their worker copies in the workers themselves.
            if name in started_names:
                try:
                    container.stage.on_end()
                except Exception as e:
                    self._logger.exception("Stage %s failed on_end", name)
                    errors.append(e)
        if self._error_manager_started:
            try:
                self._error_manager.on_end()
            except Exception as e:
                self._logger.exception("Error manager failed on_end")
                errors.append(e)
        self._stop_logs_receiver()
        raise_collected_errors(errors, "pipeline termination failed")
        self._logger.debug("Termination done")

    def _wait_for_stopped(
        self, container: ContainerType, wait_seconds: float = CONCURRENCY_WAIT
    ):
        """
        Wait for a container to consume all the items produced by the source.

        The `Stop` item is the only reliable signal of a container which has consumed
        everything, a counter comparison would hang whenever a stage doesn't deliver all the
        items it receives (e.g. a batch stage which drops items on errors).

        :param container: The container to wait for
        :param wait_seconds: Time to wait before pinging again the container
        """
        if isinstance(container, ConcurrentContainerTypes):
            # concurrent containers don't get the `Stop` item, their runners end by themselves
            # as soon as the termination is set and their input queue is empty
            return
        while not container.is_stopped() and not self._is_fatal():
            time.sleep(wait_seconds)

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
            if isinstance(container, ConcurrentContainerTypes)
        )

    def _get_processed_or_abort(
        self, container: StageContainer[DataT] | BatchStageContainer[DataT]
    ) -> Optional[Item[DataT]]:
        """
        Wait for the item processed by the last container without ever blocking indefinitely:
        when a stage runner fails we give up and raise its exception

        :param container: The last container of the pipeline
        :return: The processed item or None if no item is available
        """
        while True:
            item = container.get_processed(block=True, timeout=CONCURRENCY_WAIT)
            if item is not None:
                return item
            if self._is_fatal():
                self._wait_for_runner_error()
                return None

    def _wait_for_runner_error(
        self, retries: int = 10, wait_seconds: float = CONCURRENCY_WAIT
    ):
        """
        Wait for the exception of a failed stage runner to be available and raise it.

        The fatal event is set by a runner just before it raises, so its future may not be
        completed yet when the event is observed.

        :param retries: How many times the runner errors are checked for
        :param wait_seconds: Time to wait between two checks
        :raises Exception: Whatever exception has been raised by a stage runner
        """
        for _ in range(retries):
            self._check_errors()
            time.sleep(wait_seconds)
        self._logger.warning("Stage runner error not surfaced after %d checks", retries)

    def process(self, item: Item[DataT]) -> Item[DataT]:
        """
        Process a single item synchronously (no concurrency) through the pipeline.

        A built pipeline can process several items one by one until it is closed.

        :raises ValueError: When the pipeline contains concurrent stages
        :raises PipelineStateError: When the pipeline is running or closed
        """
        if self._has_concurrent_stages:
            raise ValueError(
                "Pipeline.process() only supports pipelines without concurrent stages; "
                "use run() or process_async()"
            )
        self._ensure_built()
        self._start_stages()
        # close() may have run while the pipeline was starting
        if self._state is PipelineState.CLOSED:
            raise PipelineStateError("The pipeline is closed")
        self._logger.debug("Processing %s on stages: %s", item, self._log_stages())
        last_stage_name = last_key(self._containers)
        self._source_container.prepend_item(item)
        ret = None
        for name, container in self._containers.items():
            container.process()
            if name == last_stage_name:
                ret = self._get_processed_or_abort(container)
        if self._is_fatal():
            self._wait_for_runner_error()
        return ret or item

    def process_async(
        self, item: Item[DataT], callback: Optional[Callable[[Item[DataT]], Any]] = None
    ):
        """
        Process a single item asynchronously through the pipeline, stages may run concurrently.
        The call returns immediately, processed items are retrieved with :meth:`.Pipeline.get_item`

        :param callback: A function to call after a successful process of the item
        :raises PipelineStateError: When the pipeline is closed
        """
        # A running pipeline accepts more items, a closed one cannot.
        if self._state is not PipelineState.RUNNING:
            self._ensure_built()
        self._start_stages()
        if self._state is PipelineState.CLOSED:
            raise PipelineStateError("The pipeline is closed")
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

    def get_item(self, block: bool = True) -> Item[DataT]:
        """
        Get a single item from the asynchronous execution of the pipeline on single items from :meth:`.Pipeline.process_async`

        :param block: If True wait indefinitely for the next processed item
        :raises ValueError: When there is not output queue set, the pipeline is not running asynchronously
        :raises queue.Empty: When we do not block and the queue is empty
        :raises BaseException: Whatever exception has made the asynchronous runner fail
        """
        if self._out_queue is not None:
            item = self._out_queue.get(block)
            self._out_queue.task_done()
            if isinstance(item, _AsyncPipelineError):
                raise item.error
            return item
        else:
            raise ValueError(
                "No pipeline is running asynchronously, no item can be retrieved from the output queue"
            )

    def set_source(self, source: Source[DataT]) -> Self:
        """
        Set the source of the pipeline: a subclass of :class:`.stage.Source`

        A built pipeline without a source can still be given one, as long as it has
        not started running.

        :raises PipelineStateError: When the pipeline is running or closed
        """
        self._require_not_running("set a source on")
        source.set_name(self._source_name)
        self._source_container.set(source)
        return self

    def set_error_manager(self, error_manager: ErrorManager) -> Self:
        """
        Set the error manager for handling errors from each stage item processing

        :raises PipelineStateError: When the pipeline has already been built
        """
        self._require_initial("set an error manager on")
        self._error_manager = error_manager
        for container in self._containers.values():
            container.set_error_manager(self._error_manager)
        return self

    def _last_stage_name(self) -> Optional[str]:
        if self._containers:
            return last_key(self._containers)
        return None

    def _wait_for_previous(
        self,
        container: ConnectedStageMixin,
        last_stage_name: Optional[str],
        wait_seconds: float = CONCURRENCY_WAIT,
    ):
        """
        Given a container we want to append to the pipeline, wait for the last one (added to the pipeline) to be created

        :param container: A container to add to the pipeline
        :param last_stage_name: Name of the last stage currently in the pipeline
        :param wait_seconds: Time to recurrently wait the construction of the container relative to the last stage in the pipeline
        """
        # Fast path: when the previous container is already constructed we can link
        # synchronously, without delegating to a background thread. The async waiter
        # below is only needed when the previous stage is still under concurrent
        # construction (i.e. a placeholder fake container). Linking synchronously
        # avoids a race where `process()`/`run()` is called before the linker runs,
        # which surfaced as `AttributeError: ... has no attribute '_previous'`.
        if last_stage_name is None:
            container.set_previous(self._source_container)
            return
        previous = self._containers.get(last_stage_name)
        if previous is not None and previous is not self._fake_container:
            container.set_previous(previous)
            return

        def _waiter():
            # `last_stage_name` is never None here: the fast path above already
            # links synchronously when the pipeline is still empty
            if last_stage_name is not None:
                while (
                    self._containers[last_stage_name] is self._fake_container
                    and not self._init_errors
                ):
                    time.sleep(wait_seconds)
                if self._init_errors:
                    return
                container.set_previous(self._containers[last_stage_name])

        executor = self._get_wait_previous_executor()
        executor.submit(_waiter)

    def _build_container(
        self,
        name: str,
        stage: Stage[DataT] | BatchStage[DataT],
        concurrency: int,
        executor: ExecutorType,
        retryable_errors: Tuple[Type[Exception], ...],
        max_retries: int,
        backoff: Union[float, int],
    ) -> StageContainer[DataT] | BatchStageContainer[DataT]:
        """
        Get a new container instance according to the pipeline configuration

        :param name: Stage name
        :param stage: A stage instance
        :param concurrency: Number of concurrent stage executions, if 0 then just create the non-concurrent containers
        :param executor: "threads" for using threads, "processes" for using multiprocessing
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. After the final failed attempt, a :class:`.error.exceptions.RetryError` soft error is attached to the item
        :param max_retries: maximum number of retries for the stage before attaching a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        """
        retry_manager = RetryManager(
            backoff=backoff, max_retries=max_retries, retryable_errors=retryable_errors
        )
        if concurrency <= 0:
            if isinstance(stage, BatchStage):
                # unreachable through the public API (`append` and `append_concurrently`
                # always force batch stages to `concurrency=1`): guard the private contract
                raise ValueError("Batch stages require concurrency >= 1")
            return StageContainer(name, stage, self._error_manager, retry_manager)
        if executor == "processes":
            self._start_logs_receiver()
            logs_queue = self._get_logs_receiver_queue()
            if isinstance(stage, BatchStage):
                return BatchConcurrentStageContainer(
                    name,
                    stage,
                    self._error_manager,
                    retry_manager,
                    self._new_mp_queue,
                    self._new_mp_counter,
                    self._new_mp_event,
                    concurrency,
                    executor,
                    logs_queue,
                )
            return ConcurrentStageContainer(
                name,
                stage,
                self._error_manager,
                retry_manager,
                self._new_mp_queue,
                self._new_mp_counter,
                self._new_mp_event,
                concurrency,
                executor,
                logs_queue,
            )
        if isinstance(stage, BatchStage):
            return BatchConcurrentStageContainer(
                name,
                stage,
                self._error_manager,
                retry_manager,
                self._new_queue,
                self._new_counter,
                self._new_event,
                concurrency,
                executor,
            )
        return ConcurrentStageContainer(
            name,
            stage,
            self._error_manager,
            retry_manager,
            self._new_queue,
            self._new_counter,
            self._new_event,
            concurrency,
            executor,
        )

    def get_stage(self, name: str) -> Optional[Stage[DataT] | BatchStage[DataT]]:
        """
        Get a stage instance by its name
        """
        ret = self._containers.get(name)
        return ret.stage if ret else None

    def append(
        self,
        name: str,
        stage: Stage[DataT] | BatchStage[DataT],
        concurrency: int = 0,
        executor: ExecutorType = "threads",
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
    ) -> Self:
        """
        Append a stage to the pipeline just after the last one appended, or after the source if it is the first stage

        :param name: Name for identify the stage in the pipeline, it is also set in the stage and it must be unique in the pipeline
        :param stage: Instance of a stage
        :param concurrency: Number of concurrent stage executions, if 0 then threads/processes won't be involved for this stage
        :param executor: "threads" for using threads, "processes" for using multiprocessing
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. After the final failed attempt, a :class:`.error.exceptions.RetryError` soft error is attached to the item
        :param max_retries: maximum number of retries for the stage before attaching a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        :raises PipelineStateError: When the pipeline has already been built
        """
        self._require_initial("append a stage to")
        self._executors_ready = False
        # FIXME here we force a BatchStage to run on a thread, but we would leave it on the main thread
        if concurrency < 1 and isinstance(stage, BatchStage):
            executor = "threads"
            concurrency = 1
        self._check_stage_name(name)
        self._check_retries_params(retryable_errors, max_retries, backoff)
        container = self._build_container(
            name, stage, concurrency, executor, retryable_errors, max_retries, backoff
        )
        if concurrency > 0:
            self._has_concurrent_stages = True
            # if it is concurrent and it is the first stage, make the source work on a output queue
            if not self._containers:
                self._enqueue_source = True
        self._wait_for_previous(
            container, self._last_stage_name()
        )  # wait that previous stage is initialized
        self._containers[name] = container
        return self

    def append_concurrently(
        self,
        name: str,
        stage_class: Type[Stage[DataT]] | Type[BatchStage[DataT]],
        args: Optional[Sequence] = None,
        kwargs: Optional[Mapping] = None,
        concurrency: int = 0,
        executor: ExecutorType = "threads",
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
    ) -> Self:
        """
        Append a stage class to the pipeline just after the last one appended, or after the source if it is the first stage.
        The stage construction will be executed concurrently respect to the general pipeline construction

        :param name: Name for identify the stage in the pipeline, it is also set in the stage and it must be unique in the pipeline
        :param stage_class: Class of a stage
        :param args: List of arguments for the stage constructor
        :param kwargs: Dictionary of keyed arguments for the stage constructor
        :param concurrency: Number of concurrent stage executions, if 0 then threads/processes won't be involved for this stage
        :param executor: "threads" for using threads, "processes" for using multiprocessing (also for concurrent construction)
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. After the final failed attempt, a :class:`.error.exceptions.RetryError` soft error is attached to the item
        :param max_retries: maximum number of retries for the stage before attaching a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        :raises PipelineStateError: When the pipeline has already been built
        """
        self._require_initial("append a stage to")
        if not isclass(stage_class):
            raise ValueError(
                "Must pass stage classes, and not stage instances, as argument"
            )
        self._executors_ready = False
        executor_construction = executor
        # FIXME here we force a BatchStage to run on a thread, but we would like to leave it on the main thread
        if concurrency < 1 and issubclass(stage_class, BatchStage):
            executor = "threads"
            concurrency = 1
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        self._check_stage_name(name)
        self._check_retries_params(retryable_errors, max_retries, backoff)
        if concurrency > 0:
            self._has_concurrent_stages = True
            # if it is concurrent, and it is the first stage, make the source work on a output queue
            if not self._containers:
                self._enqueue_source = True
        last_stage_name = self._last_stage_name()
        # set it immediately with an empty stage container so the order of the calls of this method is followed in `_containers`
        self._containers[name] = self._fake_container
        init_executor = self._get_init_executor(executor_construction)
        if executor_construction == "processes":
            logs_queue = self._start_logs_receiver().queue
            future = init_executor.submit(
                _stage_initialization_with_logger, logs_queue, stage_class, args, kwargs
            )
        else:
            future = init_executor.submit(stage_class, *args, **kwargs)

        def finish_append(stage_future: Future):
            # the pipeline may have been built or closed while the stage was under
            # construction: never create queues or containers after that
            if self._state is not PipelineState.INITIAL:
                return
            try:
                stage = stage_future.result()
                if self._state is not PipelineState.INITIAL:
                    return
                container = self._build_container(
                    name,
                    stage,
                    concurrency,
                    executor,
                    retryable_errors,
                    max_retries,
                    backoff,
                )
                self._wait_for_previous(container, last_stage_name)
                self._containers[name] = container
            except Exception as e:
                # done callbacks swallow exceptions, store it so `build()`
                # raises it instead of running a fake stage without feedback
                self._init_errors.append(e)

        future.add_done_callback(finish_append)
        return self

    def _get_init_executor(self, executor: ExecutorType = "threads") -> Executor:
        """
        Get a pool executor for concurrent stage initialization

        :param executor: "processes" if the executor uses multiprocessing, "threads" otherwise
        """
        if self._init_executor is None:
            if executor == "threads":
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
                runner = self.run()
                try:
                    for item in runner:
                        try:
                            item.callback()
                        except BaseException as e:
                            # the callback runs in this thread: deliver its error to
                            # `get_item` instead of dying silently and blocking it
                            self._out_queue.put(_AsyncPipelineError(e))
                            return
                        self._out_queue.put(item)
                except BaseException as e:
                    self._out_queue.put(_AsyncPipelineError(e))
                finally:
                    # closing also terminates the pipeline when the callback failed
                    runner.close()

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
