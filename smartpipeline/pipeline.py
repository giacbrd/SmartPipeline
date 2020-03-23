import time
import uuid
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Manager
from queue import Queue
from threading import Thread, Event

from smartpipeline.defaults import CONCURRENCY_WAIT
from smartpipeline.error import ErrorManager
from smartpipeline.containers import SourceContainer, StageContainer, ConcurrentStageContainer, BatchStageContainer, \
    BatchConcurrentStageContainer
from smartpipeline.stage import BatchStage
from smartpipeline.item import Stop
from smartpipeline.utils import OrderedDict, ThreadCounter, ProcessCounter

__author__ = 'Giacomo Berardi <giacbrd.com>'


class FakeContainer:
    def __init__(self, item):
        self._item = item

    def get_item(self):
        return self._item


class Pipeline:

    def __init__(self):
        self._concurrencies = {}
        self._containers = OrderedDict()
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

    def _new_mp_queue(self):
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return self._sync_manager.Queue()

    @staticmethod
    def _new_queue():
        return Queue()

    def _new_mp_event(self):
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return self._sync_manager.Event()

    @staticmethod
    def _new_event():
        return Event()

    def _new_mp_counter(self):
        if self._sync_manager is None:
            self._sync_manager = Manager()
        return ProcessCounter(self._sync_manager)

    @staticmethod
    def _new_counter():
        return ThreadCounter()

    def _wait_executors(self, wait_seconds=CONCURRENCY_WAIT):
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        while not all(self._containers.values()):
            time.sleep(wait_seconds)
        self._wait_previous_executor.shutdown(wait=True)
        for name, stage in self._containers.items():
            if isinstance(stage, (ConcurrentStageContainer, BatchConcurrentStageContainer)):
                stage.run()

    def shutdown(self):
        if self._out_queue is not None:
            self._out_queue.join()
        # if self._init_executor is not None:
        #     self._init_executor.shutdown()
        # FIXME stage shutdown may raise exception, the executor gets stuck
        # for name, stage in self._containers.items():
        #     if isinstance(stage, (ConcurrentStageContainer, BatchConcurrentStageContainer)):
        #         stage.shutdown()
        if self._sync_manager is not None:
            self._sync_manager.shutdown()

    def __del__(self):
        self.shutdown()

    def run(self):
        counter = 0
        self._wait_executors()
        if not self._source_container.is_set():
            raise ValueError("Set the data source for this pipeline")
        last_stage_name = self._last_stage_name()
        terminator = None
        while True:
            if self._enqueue_source:  # in the case the first stage is concurrent
                self._source_container.pop_into_queue()
            for name, container in self._containers.items():
                # concurrent stages run by themself in threads/processes
                if not isinstance(container, (ConcurrentStageContainer, BatchConcurrentStageContainer)):
                    container.process()
                else:
                    try:
                        container.check_errors()
                    except Exception as e:
                        self.stop()
                        self._terminate_all(force=True)  # TODO in case of errors we loose pending items!
                        self.shutdown()
                        raise e
                if name == last_stage_name:
                    for _ in range(container.stage.size() if isinstance(container, BatchStageContainer) else 1):
                        item = container.get_processed()
                        if item is not None:
                            if not isinstance(item, Stop):
                                yield item
                                counter += 1
                            elif not self._all_terminated() and terminator is None:
                                terminator = Thread(target=self._terminate_all)
                                terminator.start()
                        else:
                            break
            # exit the loop only when all items have been returned
            if self._all_empty() and counter >= self._source_container.count():
                if terminator is not None:
                    terminator.join()
                    self.shutdown()
                return

    def _terminate_all(self, force=False, wait_seconds=CONCURRENCY_WAIT):
        # scroll the pipeline by its order and terminate stages after the relative queues are empty
        for container in self._containers.values():
            if not force:
                # ensure the stage has processed all source items
                while container.count() < self._source_container.count():
                    time.sleep(wait_seconds)
            container.terminate()
            if isinstance(container, ConcurrentStageContainer):
                if force:  # empty the queues, losing pending items
                    container.empty_queues()
                while not container.is_terminated():
                    time.sleep(wait_seconds)
                container.queues_join()
                while not container.queues_empty():
                    time.sleep(wait_seconds)

    def _all_terminated(self):
        return all(container.is_terminated() for container in self._containers.values())

    def _all_empty(self):
        return self._all_terminated() and all(
            container.queues_empty() for container in self._containers.values()
                if isinstance(container, (ConcurrentStageContainer, BatchConcurrentStageContainer)))

    def process(self, item):
        last_stage_name = self._containers.last_key()
        self._source_container.prepend_item(item)
        for name, container in self._containers.items():
            container.process()
            if name == last_stage_name:
                return container.get_processed(block=True)

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
            raise ValueError('No pipeline is running asynchronously, not item can be retrieved from the output queue')

    def set_source(self, source):
        self._source_container.set(source)
        self._source_name = uuid.uuid4()
        return self

    def set_error_manager(self, error_manager):
        self._error_manager = error_manager
        for container in self._containers.values():
            container.set_error_manager(self._error_manager)
        return self

    def set_max_init_workers(self, max_workers):
        self._max_init_workers = max_workers
        return self

    def _last_stage_name(self):
        if self._containers:
            return self._containers.last_key()

    def _last_stage(self):
        if self._containers:
            return self._containers[self._last_stage_name()]
        else:
            return self._source_container

    def _wait_for_previous(self, container, last_stage_name, wait_seconds=CONCURRENCY_WAIT):
        def _waiter():
            if last_stage_name is not None:
                while self._containers[last_stage_name] is None:
                    time.sleep(wait_seconds)
                container.set_previous(self._containers[last_stage_name])
            else:
                container.set_previous(self._source_container)

        executor = self._get_wait_previous_executor()
        executor.submit(_waiter)

    def _get_container(self, name, stage, concurrency, use_threads):
        if concurrency <= 0:
            constructor = BatchStageContainer if isinstance(stage, BatchStage) else StageContainer
            return constructor(name, stage, self._error_manager)
        else:
            constructor = BatchConcurrentStageContainer if isinstance(stage, BatchStage) else ConcurrentStageContainer
            if use_threads:
                return constructor(name, stage, self._error_manager,
                                   self._new_queue, self._new_counter, self._new_event, concurrency, use_threads)
            else:
                return constructor(name, stage, self._error_manager, self._new_mp_queue,
                                   self._new_mp_counter, self._new_mp_event, concurrency, use_threads)

    def get_stage(self, stage_name):
        return self._containers.get(stage_name).stage

    def append_stage(self, name, stage, concurrency=0, use_threads=True):
        # FIXME here we force a BatchStage to run on a thread, but we would leave it on the main thread
        if concurrency < 1 and isinstance(stage, BatchStage):
            use_threads = True
            concurrency = 1
        self._check_stage_name(name)
        container = self._get_container(name, stage, concurrency, use_threads)
        if concurrency > 0:
            if not self._containers:
                self._enqueue_source = True
        self._wait_for_previous(container, self._last_stage_name())  # wait that previous stage is initialized
        self._containers[name] = container
        return self

    def append_stage_concurrently(self, name, stage_class, args=None, kwargs=None, concurrency=0, use_threads=True):
        # FIXME here we force a BatchStage to run on a thread, but we would leave it on the main thread
        if concurrency < 1 and issubclass(stage_class, BatchStage):
            use_threads = True
            concurrency = 1
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        self._check_stage_name(name)
        if concurrency > 0 and not self._containers:  # first stage added
            self._enqueue_source = True
        last_stage_name = self._last_stage_name()
        self._containers[name] = None  # so the order of the calls of this method is followed in `_containers`
        future = self._get_init_executor(use_threads).submit(stage_class, *args, **kwargs)

        def append_stage(stage_future):
            stage = stage_future.result()
            container = self._get_container(name, stage, concurrency, use_threads)
            self._wait_for_previous(container, last_stage_name)
            self._containers[name] = container

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
        if name in self._containers:
            raise ValueError(f'The stage name {name} is already used in this pipeline')

    def _init_out_queue(self):
        self._out_queue = self._new_queue()
