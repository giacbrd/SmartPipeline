from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

"""Segments: the execution units of a pipeline.

A pipeline compiles into an ordered list of segments with queues only between them:

* :class:`ConcurrentSegment` is one concurrent stage (a batch stage is always one)
  with N workers on a thread or process executor.
* :class:`InlineSegment` is a maximal run of consecutive ``concurrency=0`` stages,
  executed as a plain function chain by one driver.

The producer of a segment input sends one ``Stop`` per consumer worker. A worker
exits when it reads a ``Stop`` (flushing its batch) without forwarding it. Once every
worker of segment k is done, the coordinator puts one ``Stop`` per worker of segment
k+1 into the next queue. Every item is therefore enqueued before the first ``Stop``.
"""

import copy
import logging
import queue
from concurrent.futures import Future, ThreadPoolExecutor, wait
from concurrent.futures.process import ProcessPoolExecutor
from dataclasses import dataclass, field
from multiprocessing import Manager, get_context
from multiprocessing.managers import SyncManager
from threading import Event, Lock, RLock, Thread, current_thread
from typing import Any, Callable, Generator, Generic, Literal, Optional, Sequence

from smartpipeline.defaults import MAX_QUEUES_SIZE
from smartpipeline.error.handling import ErrorManager, RetryManager
from smartpipeline.item import DataT, Item, Stop
from smartpipeline.runners import process, worker_loop
from smartpipeline.stage import BatchStage, ItemsQueue, Source, Stage
from smartpipeline.utils import (
    LogsReceiver,
    _guards_met,
    get_or_abort,
    put_or_abort,
    raise_collected_errors,
)

__all__ = [
    "InlineSegment",
    "ConcurrentSegment",
    "Segment",
    "compile_segments",
    "Engine",
]

ExecutorName = Literal["threads", "processes"]
StageInstance = Stage[Any] | BatchStage[Any]


def run_stage_on_ends(
    started_main: list[tuple[str, StageInstance]],
    error_manager: ErrorManager,
    error_manager_started: bool,
) -> None:
    """Run `on_end` for every started main-process stage and the error manager."""
    errors: list[Exception] = []
    for _, stage in list(started_main):
        try:
            stage.on_end()
        except Exception as e:
            errors.append(e)
    if error_manager_started:
        try:
            error_manager.on_end()
        except Exception as e:
            errors.append(e)
    raise_collected_errors(errors, "pipeline termination failed")


@dataclass
class InlineSegment(Generic[DataT]):
    """A maximal run of consecutive inline stages, run as one function chain."""

    names: list[str] = field(default_factory=list)
    stages: list[Stage[DataT]] = field(default_factory=list)
    retry_managers: list[RetryManager] = field(default_factory=list)
    input_queue: Optional[ItemsQueue] = None
    output_queue: Optional[ItemsQueue] = None
    thread: Optional[Thread] = None

    @property
    def num_workers(self) -> int:
        return 1

    def run_chain(self, item: Item[DataT], error_manager: ErrorManager) -> Item[DataT]:
        for stage, retry_manager in zip(self.stages, self.retry_managers):
            item = process(stage, item, error_manager, retry_manager)
        return item


@dataclass
class ConcurrentSegment(Generic[DataT]):
    """One concurrent stage with N workers on a thread or process executor."""

    name: str = ""
    stage: Stage[DataT] | BatchStage[DataT] | None = None
    retry_manager: RetryManager | None = None
    concurrency: int = 1
    executor_name: ExecutorName = "threads"
    input_queue: Optional[ItemsQueue] = None
    output_queue: Optional[ItemsQueue] = None
    executor: Any = None
    futures: list[Future] = field(default_factory=list)

    @property
    def num_workers(self) -> int:
        return self.concurrency


Segment = InlineSegment | ConcurrentSegment


def compile_segments(
    specs: Sequence[Any], stages: Sequence[Stage[Any] | BatchStage[Any]]
) -> list[Segment]:
    """Compile specs and stages into inline runs and single concurrent stages."""
    segments: list[Segment] = []
    inline_names: list[str] = []
    inline_stages: list[Stage[Any]] = []
    inline_retries: list[RetryManager] = []

    def _flush_inline() -> None:
        if inline_stages:
            segments.append(
                InlineSegment(
                    names=list(inline_names),
                    stages=list(inline_stages),
                    retry_managers=list(inline_retries),
                )
            )
            inline_names.clear()
            inline_stages.clear()
            inline_retries.clear()

    for spec, stage in zip(specs, stages):
        retry_manager = spec.retry_manager
        if spec.concurrency <= 0:
            assert not isinstance(stage, BatchStage)
            assert isinstance(stage, Stage)
            inline_names.append(spec.name)
            inline_stages.append(stage)
            inline_retries.append(retry_manager)
        else:
            _flush_inline()
            segments.append(
                ConcurrentSegment(
                    name=spec.name,
                    stage=stage,
                    retry_manager=retry_manager,
                    concurrency=spec.concurrency,
                    executor_name=spec.executor,
                )
            )
    _flush_inline()
    return segments


class Engine(Generic[DataT]):
    """Owns queues, executors, threads and the cancel signal for one run."""

    def __init__(
        self,
        segments: list[InlineSegment[DataT] | ConcurrentSegment[DataT]],
        error_manager: ErrorManager,
        source: Source[DataT],
        max_queues_size: int = MAX_QUEUES_SIZE,
        started_main: Optional[list[tuple[str, StageInstance]]] = None,
        error_manager_started: bool = False,
    ):
        """
        :param started_main: Stages whose `on_start` already ran in this process, so that
            they are not started twice (a pipeline can have processed items with
            :meth:`.Pipeline.process` before running its source)
        """
        self.segments = segments
        self.error_manager = error_manager
        self.source = source
        self.max_queues_size = max_queues_size
        self.has_processes = any(
            isinstance(s, ConcurrentSegment) and s.executor_name == "processes"
            for s in segments
        )
        self._manager: Optional[SyncManager] = None
        if self.has_processes:
            self._manager = Manager()
        if self._manager is not None:
            self.cancel: Event = self._manager.Event()
        else:
            self.cancel = Event()
        self.errors: list[BaseException] = []
        self._errors_lock = Lock()
        self._threads: list[Thread] = []
        self._executors: list[Any] = []
        self._logs_receiver: Optional[LogsReceiver] = None
        self._logs_queue: Optional[queue.Queue[logging.LogRecord]] = None
        # `process()` may already have started the main-process stages of this pipeline
        self.started_main = list(started_main) if started_main is not None else []
        self.error_manager_started = error_manager_started
        # guards the main-process stages, so `on_end` never runs while a stage is still
        # processing an item on the caller's thread; reentrant, since a stage may close
        # the pipeline from inside its own `process`
        self._stage_lock = RLock()

    def _new_queue(self) -> ItemsQueue:
        if self._manager is not None:
            return self._manager.Queue(maxsize=self.max_queues_size)
        return queue.Queue(maxsize=self.max_queues_size)

    def _record(self, error: BaseException) -> None:
        with self._errors_lock:
            self.errors.append(error)
        self.cancel.set()

    def _done_callback(self, future: Future) -> None:
        if future.cancelled():
            # a future cancelled on shutdown never ran, so it reports no error
            return
        try:
            exc = future.exception()
        except Exception as e:
            self._record(e)
            return
        if exc is not None:
            self._record(exc)

    def _wire_queues(self) -> None:
        n = len(self.segments)
        if n == 1 and isinstance(self.segments[0], InlineSegment):
            return
        for i in range(n - 1):
            q = self._new_queue()
            seg, nxt = self.segments[i], self.segments[i + 1]
            seg.output_queue = q  # type: ignore[union-attr]
            nxt.input_queue = q  # type: ignore[union-attr]
        first, last = self.segments[0], self.segments[-1]
        first.input_queue = self._new_queue()  # type: ignore[union-attr]
        if isinstance(last, ConcurrentSegment):
            last.output_queue = self._new_queue()

    def _ensure_logs(self) -> Optional[queue.Queue[logging.LogRecord]]:
        if not self.has_processes:
            return None
        assert self._manager is not None
        if self._logs_receiver is None:
            logs_queue = self._manager.Queue()
            self._logs_receiver = LogsReceiver(logs_queue)
            self._logs_receiver.start()
            self._logs_queue = logs_queue
        return self._logs_queue

    def start(self) -> None:
        self._wire_queues()
        logs_queue = self._ensure_logs()
        started = {name for name, _ in self.started_main}
        try:
            for seg in self.segments:
                if isinstance(seg, InlineSegment):
                    for name, stage in zip(seg.names, seg.stages):
                        if name in started:
                            continue
                        stage.on_start()
                        self.started_main.append((name, stage))
                else:
                    assert isinstance(seg, ConcurrentSegment)
                    assert seg.stage is not None and seg.retry_manager is not None
                    if seg.executor_name == "threads" and seg.name not in started:
                        seg.stage.on_start()
                        self.started_main.append((seg.name, seg.stage))
            for seg in self.segments:
                if not isinstance(seg, ConcurrentSegment):
                    continue
                assert seg.stage is not None and seg.retry_manager is not None
                if seg.executor_name == "threads":
                    t_executor: Any = ThreadPoolExecutor(max_workers=seg.concurrency)
                    executor = t_executor
                else:
                    p_executor: Any = ProcessPoolExecutor(
                        max_workers=seg.concurrency,
                        mp_context=get_context("spawn"),
                    )
                    executor = p_executor
                seg.executor = executor
                self._executors.append(executor)
                assert seg.input_queue is not None and seg.output_queue is not None
                # Copy the error manager for process workers: `submit` only enqueues
                # the task, its arguments are pickled later by a background thread.
                # The main `on_start` below may open non-picklable resources, so the
                # workers must serialize the pre-start copy, not the live object.
                worker_error_manager = (
                    copy.copy(self.error_manager)
                    if seg.executor_name == "processes"
                    else self.error_manager
                )
                for _ in range(seg.concurrency):
                    future = executor.submit(
                        worker_loop,
                        seg.stage,
                        seg.input_queue,
                        seg.output_queue,
                        worker_error_manager,
                        seg.retry_manager,
                        self.cancel,
                        logs_queue if seg.executor_name == "processes" else None,
                    )
                    future.add_done_callback(self._done_callback)
                    seg.futures.append(future)
            if not self.error_manager_started:
                self.error_manager.on_start()
                self.error_manager_started = True
        except BaseException:
            # workers are already submitted: abort and join them before the main-process
            # hooks, so `on_end` never runs while a worker is still using the stage
            self.cancel.set()
            self._shutdown_executors()
            try:
                run_stage_on_ends(
                    self.started_main, self.error_manager, self.error_manager_started
                )
            except BaseException:
                # the start failure is the reason of the rollback, it must not be masked
                logging.exception("Problems in the start-phase rollback termination")
            raise
        for i, seg in enumerate(self.segments):
            is_last = i == len(self.segments) - 1
            target: Callable[..., None]
            args: tuple[Any, ...]
            if isinstance(seg, InlineSegment):
                if is_last:
                    continue
                nxt = self.segments[i + 1]
                target = self._inline_queue_driver
                args = (seg, nxt.num_workers)
            else:
                assert isinstance(seg, ConcurrentSegment)
                if is_last:
                    num_stops = 1
                else:
                    nxt = self.segments[i + 1]
                    num_stops = nxt.num_workers
                target = self._coordinator
                args = (seg, num_stops)
            thread = Thread(target=target, args=args, daemon=True)
            if isinstance(seg, InlineSegment):
                seg.thread = thread
            self._threads.append(thread)
        if not (
            len(self.segments) == 1 and isinstance(self.segments[0], InlineSegment)
        ):
            thread = Thread(target=self._source_loop, daemon=True)
            self._threads.append(thread)
        for thread in self._threads:
            thread.start()

    def _source_loop(self) -> None:
        first = self.segments[0]
        assert first.input_queue is not None
        try:
            while True:
                if self.cancel.is_set():
                    return
                try:
                    item = self.source.pop()
                except Exception as e:
                    self._record(e)
                    return
                if item is not None and not put_or_abort(
                    first.input_queue, item, self.cancel.is_set
                ):
                    return
                # the check comes after the put: a source stopping while returning its
                # last item must not lose it
                if self.source.is_stopped:
                    for _ in range(first.num_workers):
                        if not put_or_abort(
                            first.input_queue, Stop(), self.cancel.is_set
                        ):
                            return
                    return
        except BaseException as e:
            self._record(e)

    def _inline_queue_driver(
        self, seg: InlineSegment[DataT], next_workers: int
    ) -> None:
        assert seg.input_queue is not None and seg.output_queue is not None
        try:
            while True:
                item: Optional[Item[DataT]] = get_or_abort(
                    seg.input_queue, self.cancel.is_set
                )
                if item is None:
                    return
                seg.input_queue.task_done()
                if isinstance(item, Stop):
                    for _ in range(next_workers):
                        if not put_or_abort(
                            seg.output_queue, Stop(), self.cancel.is_set
                        ):
                            return
                    return
                try:
                    item = seg.run_chain(item, self.error_manager)
                except Exception as e:
                    self._record(e)
                    return
                if not put_or_abort(seg.output_queue, item, self.cancel.is_set):
                    return
        except BaseException as e:
            self._record(e)

    def _coordinator(self, seg: ConcurrentSegment[DataT], next_workers: int) -> None:
        try:
            _, not_done = wait(seg.futures, timeout=None)
            # `wait` returns when every future is done; errors are already recorded
            # by the done-callbacks which also set `cancel`.
            if self.cancel.is_set() or self.errors:
                return
            assert seg.output_queue is not None
            # The next input queue is this output queue, except for the last segment
            # whose output is consumed by the caller.
            target = seg.output_queue
            for _ in range(next_workers):
                if not put_or_abort(target, Stop(), self.cancel.is_set):
                    return
        except BaseException as e:
            self._record(e)

    def check_errors(self) -> None:
        with self._errors_lock:
            pending = list(self.errors)
        if not pending:
            return
        for e in pending:
            if not isinstance(e, Exception):
                raise e
        raise_collected_errors(
            [e for e in pending if isinstance(e, Exception)],
            "pipeline run failed",
        )

    def iter_results(self) -> Generator[Item[DataT], None, None]:
        """Yield every item of the run, handling `Stop`, `cancel` and errors."""
        if len(self.segments) == 1 and isinstance(self.segments[0], InlineSegment):
            seg = self.segments[0]
            while True:
                self.check_errors()
                if self.cancel.is_set():
                    return
                try:
                    raw = self.source.pop()
                except Exception as e:
                    self._record(e)
                    self.check_errors()
                    return  # pragma: no cover -- the check above always raises here
                if raw is not None:
                    with self._stage_lock:
                        if self.cancel.is_set():
                            return
                        result = seg.run_chain(raw, self.error_manager)
                    yield result
                # as in `_source_loop`, an item returned together with the stop signal
                # is delivered before ending the flow
                if self.source.is_stopped:
                    self.check_errors()
                    return
        else:
            last = self.segments[-1]
            # the caller consumes the final output queue, or the input queue of the
            # last segment when that segment runs inline on this very thread
            results: Optional[ItemsQueue] = (
                last.input_queue
                if isinstance(last, InlineSegment)
                else last.output_queue
            )
            assert results is not None
            while True:
                self.check_errors()
                item: Optional[Item[DataT]] = get_or_abort(results, self.cancel.is_set)
                if item is None:
                    self.check_errors()
                    return
                try:
                    results.task_done()
                except (EOFError, OSError):
                    # a concurrent close() shut down the manager while this thread was
                    # consuming: the run is over, there is nothing left to read
                    self.check_errors()
                    return
                if isinstance(item, Stop):
                    self.check_errors()
                    return
                if isinstance(last, InlineSegment):
                    with self._stage_lock:
                        if _guards_met((self.cancel.is_set,)):
                            return
                        item = last.run_chain(item, self.error_manager)
                yield item

    def join_threads(self, timeout: Optional[float] = None) -> None:
        for thread in list(self._threads):
            if thread is current_thread():
                continue
            thread.join(timeout=timeout)

    def drain_queues(self) -> None:
        for seg in self.segments:
            for qname in ("input_queue", "output_queue"):
                q = getattr(seg, qname, None)
                if q is None:
                    continue
                while True:
                    try:
                        q.get_nowait()
                        q.task_done()
                    except queue.Empty:
                        break
                    except (ValueError, AttributeError, EOFError, OSError):
                        # the queue is closed or its manager is gone
                        break

    def _run_on_end(self) -> None:
        # never close a stage while the caller's thread is still processing on it
        with self._stage_lock:
            run_stage_on_ends(
                self.started_main, self.error_manager, self.error_manager_started
            )

    def _shutdown_executors(self) -> None:
        for executor in self._executors:
            try:
                executor.shutdown(wait=True, cancel_futures=True)
            except Exception:
                logging.exception("Problems in shutting down an executor")
        self._executors.clear()
        if self._logs_receiver is not None:
            try:
                self._logs_receiver.stop()
            except Exception:
                logging.exception("Problems in stopping the logs receiver")
            self._logs_receiver = None
        if self._manager is not None:
            try:
                self._manager.shutdown()
            except Exception:
                logging.exception("Problems in shutting down the manager")
            self._manager = None

    def shutdown(self, run_on_end: bool = True) -> None:
        self.cancel.set()
        # Futures still running user code cannot be interrupted: cancel the ones which
        # never started, before waiting for the workers to be done.
        for seg in self.segments:
            if isinstance(seg, ConcurrentSegment):
                for future in seg.futures:
                    try:
                        future.cancel()
                    except Exception:
                        pass
        # shortcut: a worker stuck inside user code cannot be interrupted, so this
        # join and the executor shutdown wait for it; upgrade by killing/abandoning
        # stuck workers if real-world hangs appear
        self.join_threads()
        self.drain_queues()
        try:
            if run_on_end:
                self._run_on_end()
        finally:
            self._shutdown_executors()
