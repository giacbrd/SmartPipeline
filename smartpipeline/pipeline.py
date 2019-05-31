import time
import uuid
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Manager
from queue import Queue
from threading import Thread, Event
from typing import Sequence

from smartpipeline import CONCURRENCY_WAIT
from smartpipeline.error import ErrorManager
from smartpipeline.executors import SourceContainer, StageContainer, ConcurrentStageContainer, BatchStageContainer, \
    BatchConcurrentStageContainer
from smartpipeline.stage import Stop, BatchStage
from smartpipeline.utils import OrderedDict

__author__ = 'Giacomo Berardi <giacbrd.com>'


class FakeContainer:
    def __init__(self, item):
        self._item = item

    def get_item(self):
        return self._item


class Pipeline:

    def __init__(self):
        self._concurrencies = {}
        self._stages = OrderedDict()
        self._error_manager = ErrorManager()
        self._max_init_workers = None  # default: number of CPUs
        self._init_executor = None
        self._wait_previous_executor = None
        self._source_name = None
        self._pipeline_executor = None
        self._out_queue = None
        self._enqueue_source = False
        self._sync_manager = None
        self._source_container = SourceContainer()  # an empty source, on which we can only occasionally send items

    def new_mp_queue(self):
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return self._sync_manager.Queue()

    def new_queue(self):
        return Queue()

    def new_mp_event(self):
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return self._sync_manager.Event()

    def new_event(self):
        return Event()

    def _wait_executors(self):
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        while not all(self._stages.values()):
            time.sleep(CONCURRENCY_WAIT)
        self._wait_previous_executor.shutdown(wait=True)
        for name, stage in self._stages.items():
            if isinstance(stage, ConcurrentStageContainer):
                if stage.use_threads:
                    stage.run(self.new_event)
                else:
                    stage.run(self.new_mp_event)

    def shutdown(self):
        if self._out_queue is not None:
            self._out_queue.join()
        # if self._init_executor is not None:
        #     self._init_executor.shutdown()
        # FIXME stage shutdown may raise exception, the executor gets stuck
        # for name, stage in self._stages.items():
        #     if isinstance(stage, ConcurrentStageContainer):
        #         stage.shutdown()
        if self._sync_manager is not None:
            self._sync_manager.shutdown()

    def __del__(self):
        self.shutdown()

    def run(self):
        self._wait_executors()
        if not self._source_container.is_set():
            raise ValueError("Set the data source for this pipeline")
        last_stage_name = self._last_stage_name()
        terminator = None
        while True:
            if self._enqueue_source:  # in the case the first stage is concurrent
                self._source_container.pop_into_queue()
            for name, stage in self._stages.items():
                # concurrent stages run by theirself in threads/processes
                if not isinstance(stage, ConcurrentStageContainer):
                    stage.process()
                else:
                    try:
                        stage.check_errors()
                    except Exception as e:
                        self.stop()
                        self._terminate_all(force=True)  #TODO in case of errors we lost pending items!
                        self.shutdown()
                        raise e
                if name == last_stage_name:
                    items = stage.get_processed()
                    if not isinstance(items, Sequence):
                        items = [items]
                    for item in items:
                        if item is not None:
                            if not isinstance(item, Stop):
                                yield item
                            elif not self._all_terminated() and terminator is None:
                                terminator = Thread(target=self._terminate_all)
                                terminator.start()
                                break
            if self._all_empty():
                if terminator is not None:
                    terminator.join()
                self.shutdown()
                return

    def _terminate_all(self, force=False):
        # scroll the pipeline by its order and terminate stages after the relative queues are empty
        for stage in self._stages.values():
            if isinstance(stage, ConcurrentStageContainer):
                stage.terminate()
                if force:
                    stage.empty_queues()  # empty the queues, losing pending items
                while not stage.queues_empty() and not stage.is_terminated():
                    time.sleep(CONCURRENCY_WAIT)
                    continue

    def _all_terminated(self):
        return all(stage.is_terminated() for stage in self._stages.values())

    def _all_empty(self):
        return self._all_terminated() and all(
            stage.queues_empty() for stage in self._stages.values() if isinstance(stage, ConcurrentStageContainer))

    def process(self, item):
        last_stage_name = self._stages.last_key()
        self._source_container.prepend_item(item)
        for name, stage in self._stages.items():
            stage.process()
            if name == last_stage_name:
                return stage.get_processed(block=True)

    def process_async(self, item, callback=None):
        item.set_callback(callback)
        self._source_container.prepend_item(item)
        self._start_pipeline_executor()

    def stop(self):
        self._source_container.stop()

    def get_item(self, block=True):
        if self._out_queue is not None:
            item = self._out_queue.get(block)
            self._out_queue.task_done()
            return item
        else:
            raise ValueError("No pipeline is running asynchronously, not item can be retrieved from the output queue")

    def set_source(self, source):
        self._source_container.set(source)
        self._source_name = uuid.uuid4()
        return self

    def set_error_manager(self, error_manager):
        self._error_manager = error_manager
        for container in self._stages.values():
            container.set_error_manager(self._error_manager)
        return self

    def set_max_init_workers(self, max_workers):
        self._max_init_workers = max_workers
        return self

    def _last_stage_name(self):
        if self._stages:
            return self._stages.last_key()

    def _last_stage(self):
        if self._stages:
            return self._stages[self._last_stage_name()]
        else:
            return self._source_container

    def _wait_for_previous(self, container, last_stage_name):
        def _waiter():
            if last_stage_name is not None:
                while self._stages[last_stage_name] is None:
                    time.sleep(CONCURRENCY_WAIT)
                container.set_previous_stage(self._stages[last_stage_name])
            else:
                container.set_previous_stage(self._source_container)

        executor = self._get_wait_previous_executor()
        executor.submit(_waiter)

    def _get_container(self, name, stage, concurrency, use_threads):
        if concurrency <= 0:
            constructor = BatchStageContainer if isinstance(stage, BatchStage) else StageContainer
            return constructor(name, stage, self._error_manager)
        else:
            constructor = BatchConcurrentStageContainer if isinstance(stage, BatchStage) else ConcurrentStageContainer
            return constructor(name, stage, self._error_manager,
                                        self.new_queue if use_threads else self.new_mp_queue, concurrency, use_threads)

    def append_stage(self, name, stage, concurrency=0, use_threads=True):
        self._check_stage_name(name)
        container = self._get_container(name, stage, concurrency, use_threads)
        if concurrency > 0:
            if not self._stages:
                self._enqueue_source = True
        self._wait_for_previous(container, self._last_stage_name())  # wait that previous stage is initialized
        self._stages[name] = container
        return self

    def get_stage(self, stage_name):
        return self._stages.get(stage_name).get_stage()

    def append_stage_concurrently(self, name, stage_class, args=None, kwargs=None, concurrency=0, use_threads=True):
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        self._check_stage_name(name)
        if concurrency > 0 and not self._stages:  # first stage added
            self._enqueue_source = True
        last_stage_name = self._last_stage_name()
        self._stages[name] = None  # so the order of the calls of this method is followed in `_stages`
        future = self._get_init_executor(use_threads).submit(stage_class, *args, **kwargs)

        def append_stage(stage_future):
            stage = stage_future.result()
            container = self._get_container(name, stage, concurrency, use_threads)
            self._wait_for_previous(container, last_stage_name)
            self._stages[name] = container

        future.add_done_callback(append_stage)
        return self

    def _get_init_executor(self, use_threads=True):
        if self._init_executor is None:
            executor = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
            self._init_executor = executor(max_workers=self._max_init_workers)
        return self._init_executor

    def _get_wait_previous_executor(self):
        if self._wait_previous_executor is None:
            self._wait_previous_executor = ThreadPoolExecutor()
        return self._wait_previous_executor

    def _start_pipeline_executor(self):
        if self._pipeline_executor is None:
            self._init_out_queue()

            def pipeline_runner():
                for item in self.run():
                    item.callback()
                    self._out_queue.put(item)

            self._pipeline_executor = Thread(target=pipeline_runner, daemon=True)
            self._pipeline_executor.start()
        return self._pipeline_executor

    def _check_stage_name(self, name):
        if name in self._stages:
            raise ValueError('The stage name {} is already used in this pipeline'.format(name))

    def _init_out_queue(self):
        self._out_queue = self.new_queue()
