from __future__ import annotations

import logging
import queue
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from inspect import isclass
from queue import Queue
from threading import Event, RLock, Thread, current_thread
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
    Literal,
    Mapping,
    Optional,
)
from typing import OrderedDict as OrderedDictType
from typing import Self, Sequence, Tuple, Type, Union, cast, get_args, overload

from smartpipeline.defaults import (
    CONCURRENCY_WAIT,
    MAX_QUEUES_SIZE,
    PIPELINE_CLOSE_TIMEOUT,
)
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import DataT, Item, Stop
from smartpipeline.runners import process as _process_stage
from smartpipeline.segments import Engine, compile_segments, run_stage_on_ends
from smartpipeline.stage import BatchStage, ItemsQueue, Source, Stage
from smartpipeline.utils import put_or_abort, raise_collected_errors

__author__ = "Giacomo Berardi <giacbrd.com>"


__all__ = ["Pipeline", "StageSpec", "PipelineState", "PipelineStateError"]


ExecutorType = Literal["threads", "processes"]


@dataclass(frozen=True)
class StageSpec(Generic[DataT]):
    """
    Declarative description of a stage collected by :meth:`.Pipeline.append`.

    A spec holds either an already constructed stage instance or the stage class and
    the arguments needed to construct it. The pipeline turns specs into stages in
    :meth:`.Pipeline.build`, in a single deterministic pass.
    """

    name: str
    stage: Optional[Stage[DataT] | BatchStage[DataT]] = None
    stage_class: Optional[Type[Stage[DataT]] | Type[BatchStage[DataT]]] = None
    init_args: Tuple[Any, ...] = ()
    init_kwargs: Mapping[str, Any] = field(default_factory=dict)
    concurrency: int = 0
    executor: ExecutorType = "threads"
    retryable_errors: Tuple[Type[Exception], ...] = ()
    max_retries: int = 0
    backoff: Union[float, int] = 0.0

    def __post_init__(self):
        if (self.stage is None) == (self.stage_class is None):
            raise ValueError(
                "A stage spec requires exactly one of a stage instance or a stage class"
            )

    @property
    def retry_manager(self) -> RetryManager:
        """
        The retry configuration of this stage, built once and reused for every item.
        """
        cached = getattr(self, "_retry_manager", None)
        if cached is None:
            cached = RetryManager(
                backoff=self.backoff,
                max_retries=self.max_retries,
                retryable_errors=self.retryable_errors,
            )
            object.__setattr__(self, "_retry_manager", cached)
        return cached

    def construct(self) -> Stage[DataT] | BatchStage[DataT]:
        """
        Return the stage instance, constructing it when the spec holds a class.
        """
        if self.stage is not None:
            return self.stage
        assert self.stage_class is not None
        return self.stage_class(*self.init_args, **self.init_kwargs)


class _AsyncPipelineError:
    """An error raised by the asynchronous pipeline runner, delivered by `get_item`."""

    def __init__(self, error: BaseException):
        self.error = error


class _AsyncEndOfStream:
    """End-of-stream marker for the asynchronous runner, so `get_item` stops blocking."""


_ASYNC_END = _AsyncEndOfStream()


class _AsyncSource(Source[DataT]):
    """A source pulling items submitted with `process_async` from an internal queue."""

    def __init__(self, async_queue: Queue, stopped: Event):
        self._async_queue = async_queue
        self._stopped_event = stopped

    def pop(self) -> Optional[Item[DataT]]:
        try:
            item: Optional[Item[DataT]] = self._async_queue.get(
                block=True, timeout=CONCURRENCY_WAIT
            )
            return item
        except queue.Empty:
            if self._stopped_event.is_set():
                self.stop()
            return None


class _IterableSource(Source[DataT]):
    """Wrap an iterable of items as a source, stopping when exhausted."""

    def __init__(self, iterable: Iterable[Item[DataT]]):
        self._iterator = iter(iterable)

    def pop(self) -> Optional[Item[DataT]]:
        try:
            return next(self._iterator)
        except StopIteration:
            self.stop()
            return None


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
        max_queues_size: int = MAX_QUEUES_SIZE,
    ):
        """
        :param max_queues_size: Maximum size of any queue instanced for the pipeline (stage input and output queues)
        """
        self._stage_specs: OrderedDictType[str, StageSpec[DataT]] = OrderedDict()
        self._stages: OrderedDictType[str, Stage[DataT] | BatchStage[DataT]] = (
            OrderedDict()
        )
        self._error_manager = ErrorManager()
        self._source: Optional[Source[DataT]] = None
        self._max_queues_size = max_queues_size
        self._count = 0
        self._soft_errors_count = 0
        self._critical_errors_count = 0
        self._state = PipelineState.INITIAL
        self._build_error: Optional[BaseException] = None
        self._stopped = False
        self._lock = RLock()
        self._closed = Event()
        self._engine: Optional[Engine[DataT]] = None
        self._out_queue: Optional[Queue[Any]] = None
        self._pipeline_executor: Optional[Thread] = None
        self._async_queue: Optional[Queue] = None
        self._callbacks: dict[str, Callable[[Item[DataT]], Any]] = {}
        self._async_stopped: Event = Event()
        self._async_ready: Event = Event()
        self._started_main: list[tuple[str, Stage[DataT] | BatchStage[DataT]]] = []
        self._error_manager_started = False
        self._process_lock = RLock()
        self._process_started = False
        self._stages_log: Optional[str] = None
        self._name = f"{self.__class__.__name__}-{str(uuid.uuid4())[:8]}"
        self._logger = logging.getLogger(self._name)
        self._source_name = f"SourceOf{self._name}"

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

    def _construct_stages(
        self, specs: Sequence[StageSpec[DataT]]
    ) -> list[Stage[DataT] | BatchStage[DataT]]:
        """
        Construct every stage from its spec in order.

        Instances passed to :meth:`.Pipeline.append` are used as they are. Stage classes
        are constructed one by one in append order; heavy setup belongs in `on_start`.
        """
        stages: list[Stage[DataT] | BatchStage[DataT]] = []
        errors: list[Exception] = []
        for spec in specs:
            try:
                stages.append(spec.construct())
            except Exception as e:
                errors.append(e)
        raise_collected_errors(errors, "pipeline stage construction failed")
        return stages

    def _has_concurrent(self) -> bool:
        return any(spec.concurrency > 0 for spec in self._stage_specs.values())

    def close(self, timeout: float = PIPELINE_CLOSE_TIMEOUT) -> None:
        """
        Close the pipeline, idempotently.

        Running workers are cancelled, threads are joined, queues are drained, and
        `on_end` hooks run for every stage whose `on_start` completed. Executors and
        the multiprocessing manager are released. A closed pipeline cannot be built or
        run again. A second `close()` racing with another one waits until the first
        has finished.
        """
        with self._lock:
            if self._closed.is_set():
                return
            if self._state is PipelineState.CLOSED:
                wait_for_close = True
            else:
                self._state = PipelineState.CLOSED
                wait_for_close = False
        if wait_for_close:
            self._closed.wait()
            return
        termination_error: Optional[BaseException] = None
        try:
            self._stopped = True
            self._async_stopped.set()
            if self._source is not None:
                try:
                    self._source.stop()
                except Exception:
                    pass
            engine = self._engine
            if engine is not None:
                # abort and wait for the consuming thread before tearing the engine down,
                # so no queue is read or written after its manager has been shut
                engine.cancel.set()
                self._join_background(timeout)
                try:
                    engine.shutdown(run_on_end=True)
                except BaseException as e:
                    termination_error = e
            else:
                with self._process_lock:
                    started, self._started_main = self._started_main, []
                    em_started, self._error_manager_started = (
                        self._error_manager_started,
                        False,
                    )
                    try:
                        run_stage_on_ends(
                            started,
                            self._error_manager,
                            em_started,
                        )
                    except BaseException as e:
                        termination_error = e
            self._join_background(timeout)
            # no item can be delivered anymore, so drop the callbacks still remembered
            self._callbacks.clear()
        finally:
            self._closed.set()
            self._async_ready.set()
        if termination_error is not None:
            raise termination_error

    def _join_background(self, timeout: float):
        executor = self._pipeline_executor
        if executor is not None and executor is not current_thread():
            executor.join(timeout=timeout)
            if executor.is_alive():
                self._logger.warning(
                    "The asynchronous pipeline runner did not end in %s seconds",
                    timeout,
                )

    def build(self) -> Self:
        """
        Validate and prepare the pipeline.

        Stage classes are constructed and stored, but no stage or worker is started:
        :meth:`.Pipeline.run` and :meth:`.Pipeline.process_async` start them.
        Calling ``build()`` again on a built pipeline is a no-op.

        :raises ValueError: When no stage has been appended
        :raises PipelineStateError: When the pipeline is running or closed
        """
        with self._lock:
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
            if not self._stage_specs:
                raise ValueError("Must append at least a stage")

            self._logger.debug(
                "Building the pipeline on stages: %s", self._log_stages()
            )
            specs = list(self._stage_specs.values())
            try:
                stages = self._construct_stages(specs)
                for spec, stage in zip(specs, stages):
                    stage.set_name(spec.name)
            except BaseException as e:
                self._build_error = e
                self._state = PipelineState.CLOSED
                self._closed.set()
                self._async_ready.set()
                raise
            self._stages = OrderedDict(zip(self._stage_specs.keys(), stages))
            self._stages_log = ", ".join(self._stages.keys())
            self._state = PipelineState.BUILT
            return self

    @property
    def count(self) -> int:
        """
        Number of items delivered to the caller.

        Counts the items yielded by :meth:`.Pipeline.run` and the items delivered by
        :meth:`.Pipeline.get_item`, including the items which carry soft errors.
        Items processed one by one with :meth:`.Pipeline.process` are not counted.

        :return: Count of delivered items
        """
        return self._count

    @property
    def soft_errors_count(self) -> int:
        """
        Number of soft errors attached to the items delivered to the caller.

        Counts the :class:`.error.exceptions.SoftError` instances (including
        :class:`.error.exceptions.RetryError`) carried by the items yielded by
        :meth:`.Pipeline.run` or delivered by :meth:`.Pipeline.get_item`.
        Items processed one by one with :meth:`.Pipeline.process` are not counted.

        :return: Count of soft errors on delivered items
        """
        return self._soft_errors_count

    @property
    def critical_errors_count(self) -> int:
        """
        Number of critical errors attached to the items delivered to the caller.

        Counts the :class:`.error.exceptions.CriticalError` instances carried by
        the items yielded by :meth:`.Pipeline.run` or delivered by
        :meth:`.Pipeline.get_item`. Critical items skip downstream stages but are
        still delivered, so they are included. Items processed one by one with
        :meth:`.Pipeline.process` are not counted.

        :return: Count of critical errors on delivered items
        """
        return self._critical_errors_count

    def _record_delivery(self, item: Item[DataT]) -> None:
        """Count a delivered item and its errors, keeping the three counters aligned."""
        self._count += 1
        self._soft_errors_count += sum(1 for _ in item.soft_errors())
        self._critical_errors_count += sum(1 for _ in item.critical_errors())

    def _ensure_process_started(self) -> None:
        if self._process_started:
            return
        try:
            for name, stage in self._stages.items():
                stage.on_start()
                if self._closed.is_set():
                    run_stage_on_ends([(name, stage)], self._error_manager, False)
                    raise PipelineStateError("The pipeline is closed")
                self._started_main.append((name, stage))
            self._error_manager.on_start()
            self._error_manager_started = True
            self._process_started = True
        except PipelineStateError:
            raise
        except BaseException:
            started, self._started_main = self._started_main, []
            self._state = PipelineState.CLOSED
            self._closed.set()
            # the error manager never started if its own on_start raised
            run_stage_on_ends(started, self._error_manager, False)
            raise

    def process(self, item: Item[DataT]) -> Item[DataT]:
        """
        Process a single item synchronously (no concurrency) through the pipeline.

        A built pipeline can process several items one by one until it is closed.
        It runs the stages directly, with no containers and no queues.

        :raises ValueError: When the pipeline contains concurrent stages
        :raises PipelineStateError: When the pipeline is running or closed
        """
        self._ensure_built()
        if self._has_concurrent():
            raise ValueError(
                "Pipeline.process() only supports pipelines without concurrent stages; "
                "use run() or process_async()"
            )
        with self._process_lock:
            self._ensure_process_started()
            if self._state is PipelineState.CLOSED:  # pragma: no cover
                raise PipelineStateError("The pipeline is closed")
            self._logger.debug("Processing %s on stages: %s", item, self._log_stages())
            if self._stopped or isinstance(item, Stop):
                raise PipelineStateError(
                    "Cannot process item: the pipeline source is stopped"
                )
            for spec, stage in zip(self._stage_specs.values(), self._stages.values()):
                assert not isinstance(stage, BatchStage)
                item = _process_stage(
                    stage, item, self._error_manager, spec.retry_manager
                )
                if isinstance(item, Stop):
                    self._stopped = True
                    raise PipelineStateError(
                        "Cannot process item: the pipeline source is stopped"
                    )
            return item

    def _make_engine(self, source: Source[DataT]) -> Engine[DataT]:
        specs = list(self._stage_specs.values())
        stages = [self._stages[name] for name in self._stage_specs.keys()]
        segments = compile_segments(specs, stages)
        return Engine(
            segments,
            self._error_manager,
            source,
            self._max_queues_size,
            # a pipeline can have processed items before running: the stages already
            # started must not receive a second `on_start`
            started_main=self._started_main,
            error_manager_started=self._error_manager_started,
        )

    def run(self) -> Generator[Item[DataT], None, None]:
        """
        Run the pipeline given a source and a concatenation of stages.
        Get the sequence of items through iteration

        :return: Iterator over processed items
        :raises ValueError: When a source has not been set for the pipeline
        """
        if self._source is None:
            raise ValueError("Set the data source for this pipeline")
        self._ensure_built()
        assert self._source is not None
        with self._lock:
            if self._state is PipelineState.CLOSED:  # pragma: no cover
                # only reachable by a close() taking the lock right after the check
                # above: there is nothing to run, the pipeline is already terminated
                return
            if self._state is not PipelineState.BUILT:
                raise PipelineStateError("The pipeline is already running")
            self._engine = engine = self._make_engine(self._source)
            try:
                engine.start()
            except BaseException:
                self._state = PipelineState.CLOSED
                self._engine = None
                self._closed.set()
                raise
            self._state = PipelineState.RUNNING
        self._logger.debug("Running the pipeline on stages: %s", self._log_stages())
        try:
            for result in engine.iter_results():
                self._record_delivery(result)
                yield result
        except BaseException as run_error:
            try:
                self.close()
            except Exception:
                self._logger.exception("Problems in closing after a run failure")
            raise run_error
        else:
            self.close()

    def process_async(
        self, item: Item[DataT], callback: Optional[Callable[[Item[DataT]], Any]] = None
    ):
        """
        Process a single item asynchronously through the pipeline, stages may run concurrently.
        The call returns immediately, processed items are retrieved with :meth:`.Pipeline.get_item`

        :param callback: A function to call after a successful process of the item; it runs in this process, also when stages use workers in other processes
        :raises PipelineStateError: When the pipeline is closed or its source has stopped
        """
        if self._state is not PipelineState.RUNNING:
            self._ensure_built()
        if self._state is PipelineState.CLOSED:  # pragma: no cover
            # only reachable by a close() landing between the build check above
            # and this one
            raise PipelineStateError("The pipeline is closed")
        if self._stopped or isinstance(item, Stop):
            raise PipelineStateError(
                "Cannot process item: the pipeline source is stopped"
            )
        self._logger.debug(
            "Processing asynchronously %s on stages: %s", item, self._log_stages()
        )
        if callback is not None:
            item.set_callback(callback)
        if self._async_queue is None:
            with self._lock:
                if self._async_queue is None:
                    if self._state is PipelineState.CLOSED:  # pragma: no cover
                        # only reachable by a close() owning the lock before this thread
                        raise PipelineStateError("The pipeline is closed")
                    if self._state is not PipelineState.BUILT:
                        raise PipelineStateError("The pipeline is already running")
                    async_queue: Queue = Queue()
                    async_source: Source[DataT] = _AsyncSource(
                        async_queue, self._async_stopped
                    )
                    async_source.set_name(self._source_name)
                    self._engine = engine = self._make_engine(async_source)
                    try:
                        engine.start()
                    except BaseException as e:
                        self._build_error = e
                        self._state = PipelineState.CLOSED
                        self._engine = None
                        self._closed.set()
                        self._async_ready.set()
                        raise
                    self._state = PipelineState.RUNNING
                    # the consumer must exist only while the engine is the running one, so a
                    # racing `close()` cannot find a half-started run
                    self._init_out_queue()
                    self._start_background_consumer(engine)
                    self._async_queue = async_queue
                    self._async_ready.set()
        if self._state is PipelineState.CLOSED:  # pragma: no cover
            raise PipelineStateError("The pipeline is closed")
        running = self._engine
        if item._callback_fun is not None and running is not None:
            if running.has_processes:
                # the copy a process worker gets has no callback, which may not even be
                # picklable: keep it here, by the id travelling with the item. One entry
                # per item object carrying a callback (not per copy), dropped by close()
                self._callbacks[item.callback_id] = item._callback_fun
        assert self._async_queue is not None
        self._async_queue.put(item)

    def _start_background_consumer(self, engine: Engine[DataT]) -> None:
        out_queue = self._out_queue
        assert out_queue is not None

        def _deliver(payload: Any) -> None:
            """
            Put a result in the output queue, dropping it as soon as a `close()` has been
            requested, instead of blocking forever on a queue nobody reads anymore.
            """
            try:
                out_queue.put_nowait(payload)
                return
            except queue.Full:
                pass
            put_or_abort(
                cast(ItemsQueue, out_queue),
                payload,
                lambda: self._state is PipelineState.CLOSED,
            )

        def _consume() -> None:
            try:
                for result in engine.iter_results():
                    try:
                        self._run_callback(result)
                    except BaseException as e:
                        _deliver(_AsyncPipelineError(e))
                        return
                    self._record_delivery(result)
                    if not put_or_abort(
                        cast(ItemsQueue, out_queue), result, engine.cancel.is_set
                    ):
                        return
                try:
                    engine.check_errors()
                except BaseException as e:  # pragma: no cover
                    # only reachable by an error recorded after iter_results' own check
                    _deliver(_AsyncPipelineError(e))
                    return
                _deliver(_ASYNC_END)
            except BaseException as e:
                _deliver(_AsyncPipelineError(e))
            finally:
                if self._state is not PipelineState.CLOSED:
                    # otherwise another thread already owns the termination
                    try:
                        self.close()
                    except Exception:
                        self._logger.exception("Problems in closing an async run")

        thread = Thread(target=_consume, daemon=True)
        self._pipeline_executor = thread
        thread.start()

    def _run_callback(self, item: Item[DataT]) -> None:
        """
        Call the callback submitted for a delivered item, in this process.

        An item which ran on inline or thread stages still carries its callback; a copy
        coming back from a process worker does not, so it is taken back from the ones
        :meth:`.Pipeline.process_async` remembered. Does nothing for an item submitted
        without a callback. Failures propagate to the asynchronous consumer, which
        reports them through :meth:`.Pipeline.get_item`.
        """
        remembered = self._callbacks.get(item.callback_id) if self._callbacks else None
        if remembered is not None:
            # the item came back from a worker process without its callback: give it
            # back, so Item.callback() works on it as it does on a thread run's item
            item.set_callback(remembered)
        item.callback()

    def stop(self):
        """
        Tell the source to stop to generate items and consequently terminate the pipeline
        after all "remaining" items are processed.
        """
        self._stopped = True
        if self._source is not None:
            try:
                self._source.stop()
            except Exception:
                pass
        self._async_stopped.set()
        self._async_ready.set()

    def get_item(self, block: bool = True) -> Item[DataT]:
        """
        Get a single item from the asynchronous execution of the pipeline on single items from :meth:`.Pipeline.process_async`

        When called before the first :meth:`.Pipeline.process_async`, it waits for the
        asynchronous run to start (or for the pipeline to be closed/stopped) instead of
        raising just because the output queue does not exist yet.

        :param block: If True wait for the run to start and then for the next processed item
        :raises ValueError: When `block` is False and no asynchronous run has started
        :raises PipelineStateError: When the run ends or the pipeline is closed/stopped before an item is available
        :raises queue.Empty: When we do not block and the queue is empty
        :raises BaseException: Whatever exception has made the asynchronous runner fail
        """
        if self._out_queue is None:
            if not block:
                raise ValueError(
                    "No pipeline is running asynchronously, no item can be retrieved from the output queue"
                )
            self._async_ready.wait()
            with self._lock:
                if self._out_queue is None:
                    raise self._build_error or PipelineStateError(
                        "asynchronous run ended"
                    )
        assert self._out_queue is not None
        item = self._out_queue.get(block)
        if isinstance(item, _AsyncPipelineError):
            try:
                self._out_queue.put_nowait(_ASYNC_END)
            except queue.Full:  # pragma: no cover
                pass
            raise item.error
        if isinstance(item, _AsyncEndOfStream):
            self._out_queue.put(item)
            raise PipelineStateError("asynchronous run ended")
        return item

    def set_source(self, source: Source[DataT] | Iterable[Item[DataT]]) -> Self:
        """
        Set the source of the pipeline: a subclass of :class:`.stage.Source`
        or any iterable of items (e.g. a generator), wrapped in a private source
        which stops when exhausted.

        A built pipeline without a source can still be given one, as long as it has
        not started running.

        :raises PipelineStateError: When the pipeline is running or closed
        """
        self._require_not_running("set a source on")
        wrapped: Source[DataT] = (
            source if isinstance(source, Source) else _IterableSource(source)
        )
        wrapped.set_name(self._source_name)
        self._source = wrapped
        return self

    def set_error_manager(self, error_manager: ErrorManager) -> Self:
        """
        Set the error manager for handling errors from each stage item processing

        :raises PipelineStateError: When the pipeline has already been built
        """
        self._require_initial("set an error manager on")
        self._error_manager = error_manager
        return self

    def get_stage(self, name: str) -> Optional[Stage[DataT] | BatchStage[DataT]]:
        """
        Get a stage instance by its name.

        A class-based stage exists only after :meth:`.Pipeline.build`, so before that
        this returns None for it; an instance passed to :meth:`.Pipeline.append` is
        available immediately.
        """
        stage = self._stages.get(name)
        if stage is not None:
            return stage
        spec = self._stage_specs.get(name)
        return spec.stage if spec is not None else None

    @overload
    def append(
        self,
        name: str,
        stage: Stage[DataT] | BatchStage[DataT],
        *,
        concurrency: int = 0,
        executor: ExecutorType = "threads",
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
    ) -> Self: ...

    @overload
    def append(
        self,
        name: str,
        stage: Type[Stage[DataT]] | Type[BatchStage[DataT]],
        *,
        concurrency: int = 0,
        executor: ExecutorType = "threads",
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Self: ...

    def append(
        self,
        name: str,
        stage: (
            Stage[DataT]
            | BatchStage[DataT]
            | Type[Stage[DataT]]
            | Type[BatchStage[DataT]]
        ),
        *,
        concurrency: int = 0,
        executor: ExecutorType = "threads",
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0.0,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Self:
        """
        Append a stage to the pipeline just after the last one appended, or after the source if it is the first stage.

        ``stage`` can be an instance or a class, with different construction timing:

        * An **instance** is used as it is. It exists immediately, its name is set by
          this call, and :meth:`.Pipeline.get_stage` returns it before ``build()``.
          ``init_args`` and ``init_kwargs`` are not accepted with an instance.
        * A **class** is stored and constructed by :meth:`.Pipeline.build`,
          one by one in append order; its constructor receives
          ``init_args`` and ``init_kwargs``. **A class-based stage does not exist until
          ``build()`` runs**: before that, :meth:`.Pipeline.get_stage` returns ``None``
          for it.

        ``build()`` constructs each class in order, collecting every failure.
        No stage worker starts until :meth:`.Pipeline.run` or
        :meth:`.Pipeline.process_async`.

        :param name: Name to identify the stage in the pipeline, it must be a non-empty string, it is set in the stage and it must be unique in the pipeline
        :param stage: Instance of a stage, or a stage class to construct at build time
        :param concurrency: Number of concurrent stage executions, if 0 then threads/processes won't be involved for this stage
        :param executor: "threads" for using threads, "processes" for using multiprocessing
        :param retryable_errors: list of exceptions for which the stage applies an exponential backoff strategy. After the final failed attempt, a :class:`.error.exceptions.RetryError` soft error is attached to the item
        :param max_retries: maximum number of retries for the stage before attaching a RetryError(SoftError) (default 0)
        :param backoff: backoff factor for the `exponential backoff strategy` used by the stage when it raises one of the exceptions declared in `retryable_errors` param (default 0.0)
        :param init_args: Positional arguments for the stage constructor, only valid with a stage class
        :param init_kwargs: Keyword arguments for the stage constructor, only valid with a stage class
        :raises PipelineStateError: When the pipeline has already been built
        :raises ValueError: When the stage or its configuration is not valid
        """
        self._require_initial("append a stage to")
        self._check_stage_name(name)
        self._check_retries_params(retryable_errors, max_retries, backoff)
        if not (isinstance(concurrency, int) and concurrency >= 0):
            raise ValueError(
                "The `concurrency` parameter must be an int and its value must be >= 0"
            )
        accepted_executors = get_args(ExecutorType)
        if executor not in accepted_executors:
            raise ValueError(
                "The `executor` parameter must be "
                + " or ".join(repr(value) for value in accepted_executors)
            )

        if isclass(stage):
            stage_cls = cast(Type[Stage[DataT]] | Type[BatchStage[DataT]], stage)
            if not issubclass(stage_cls, (Stage, BatchStage)):
                raise ValueError("A stage class must extend Stage or BatchStage")
            stage_instance = None
            constructor_args = tuple(init_args or ())
            constructor_kwargs = dict(init_kwargs or {})
            is_batch = issubclass(stage_cls, BatchStage)
        else:
            if init_args is not None or init_kwargs is not None:
                raise ValueError(
                    "`init_args` and `init_kwargs` are only valid when appending a stage class"
                )
            stage_instance = cast(Stage[DataT] | BatchStage[DataT], stage)
            if not isinstance(stage_instance, (Stage, BatchStage)):
                raise ValueError(
                    "A stage must be an instance of Stage or BatchStage, or a stage class"
                )
            stage_cls = None
            constructor_args = ()
            constructor_kwargs = {}
            is_batch = isinstance(stage_instance, BatchStage)
            stage_instance.set_name(name)

        if is_batch and concurrency < 1:
            executor = "threads"
            concurrency = 1

        self._stage_specs[name] = StageSpec(
            name=name,
            stage=stage_instance,
            stage_class=stage_cls,
            init_args=constructor_args,
            init_kwargs=constructor_kwargs,
            concurrency=concurrency,
            executor=executor,
            retryable_errors=retryable_errors,
            max_retries=max_retries,
            backoff=backoff,
        )
        return self

    def _check_stage_name(self, name: str):
        """
        Check that a stage name is meaningful and not already defined in the pipeline
        :raises ValueError: Stage name is not a non-empty string
        :raises ValueError: Stage name is already defined in this pipeline
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("The stage name must be a non-empty string")
        if name in self._stage_specs:
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
        self._out_queue = Queue(maxsize=self._max_queues_size)

    def _log_stages(self):
        if self._stages_log is not None:
            return self._stages_log
        names = self._stages.keys() if self._stages else self._stage_specs.keys()
        return ", ".join(names)
