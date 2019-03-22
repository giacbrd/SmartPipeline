import time
import uuid
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor

from smartpipeline.error import ErrorManager
from smartpipeline.stage import Stop
from smartpipeline.executors import SourceContainer, StageContainer, ConcurrentStageContainer
from smartpipeline.utils import OrderedDict, new_queue

__author__ = 'Giacomo Berardi <giacbrd.com>'


class FakeContainer:
    def __init__(self, item):
        self._item = item

    def get_item(self):
        return self._item


class Pipeline:

    def __init__(self):
        self._concurrencies = {}
        self._raise_on_critical = False
        self._skip_on_critical = False
        self._stages = OrderedDict()
        self.error_manager = ErrorManager()
        self._source_container = SourceContainer()
        self.max_workers = None  # default: number of CPUs
        self._init_executor = None
        self._source_name = None
        self._pipeline_executor = None
        self._out_queue = None
        self._enqueue_source = False

    def _wait_executors(self):
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        for name, stage in self._stages.items():
            if isinstance(stage, ConcurrentStageContainer):
                stage.run()

    def _shutdown(self):
        if self._init_executor is not None:
            self._init_executor.shutdown()
        for name, stage in self._stages.items():
            if isinstance(stage, ConcurrentStageContainer):
                stage.shutdown()
        self._stop_pipeline()

    def _stop_pipeline(self):
        if self._out_queue is not None:
            self._out_queue.join()
        if self._pipeline_executor is not None:
            self._pipeline_executor.shutdown()
        self._out_queue = None

    def __del__(self):
        self._shutdown()

    def run(self):
        self._wait_executors()
        if not self._source_container.is_set():
            raise ValueError("Set the data source for this pipeline")
        last_stage_name = self._stages.last_key()
        while True:
            if self._enqueue_source:
                self._source_container.pop_into_queue()
            for name, stage in self._stages.items():
                if not isinstance(stage, ConcurrentStageContainer):
                    stage.process()
                if name == last_stage_name:
                    item = stage.get_item()
                    if item is not None:
                        if not isinstance(item, Stop):
                            yield item
                        elif self._all_empty():
                            return

    def _all_empty(self):
        return all(stage.is_stopped() for stage in self._stages.values())

    def process(self, item):
        last_stage_name = self._stages.last_key()
        self._source_container.prepend_item(item)
        for name, stage in self._stages.items():
            stage.process(block=True)
            if name == last_stage_name:
                return stage.get_item(block=True)

    def process_async(self, item, callback=None):
        item.set_callback(callback)
        self._source_container.prepend_item(item)
        self._start_pipeline_executor()

    def get_item(self, block=True):
        if self._out_queue is not None:
            self._out_queue.get(block)
        else:
            raise ValueError("No pipeline is running asynchronously, not item can be retrieved from the output queue")

    def set_source(self, source):
        self._source_container.set(source)
        self._source_name = uuid.uuid4()
        return self

    def set_error_manager(self, error_manager):
        self.error_manager = error_manager
        return self

    def set_max_workers(self, max_workers):
        self.max_workers = max_workers
        return self

    def raise_on_critical_error(self):
        self._raise_on_critical = True
        return self

    def skip_on_critical_error(self):
        self._skip_on_critical = True
        return self

    def _last_stage(self):
        if self._stages:
            return self._stages[self._stages.last_key()]
        else:
            return self._source_container

    def append_stage(self, name, stage, concurrency=0, use_threads=True):
        self._check_stage_name(name)
        if concurrency <= 0:
            container = StageContainer(name, stage, self.error_manager)
        else:
            container = ConcurrentStageContainer(name, stage, self.error_manager, concurrency, use_threads)
            if not self._stages:
                self._enqueue_source = True
        container.set_previous_stage(self._last_stage())
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
        if concurrency > 0 and not self._stages:
            self._enqueue_source = True
        last_stage_name = self._stages.last_key()
        self._stages[name] = None  # so the order of the calls of this method is followed in `_stages`
        future = self._get_init_executor(use_threads).submit(stage_class, args, kwargs)

        def append_stage(stage_future):
            stage = stage_future.result()
            if concurrency <= 0:
                container = StageContainer(name, stage, self.error_manager)
            else:
                container = ConcurrentStageContainer(name, stage, self.error_manager, concurrency, use_threads)
            container.set_previous_stage(self._stages[last_stage_name])
            self._stages[name] = container

        future.add_done_callback(append_stage)
        return self

    def _get_init_executor(self, use_threads=True):
        if self._init_executor is None:
            executor = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
            self._init_executor = executor(max_workers=self.max_workers)
        return self._init_executor

    def _start_pipeline_executor(self):
        if self._pipeline_executor is None:
            self._out_queue = new_queue()
            self._pipeline_executor = ThreadPoolExecutor(max_workers=self.max_workers)

            def pipeline_runner():
                for item in self.run():
                    self._out_queue.put(item)
                    item.callback()

            self._pipeline_executor.submit(pipeline_runner)
        return self._pipeline_executor

    def _check_stage_name(self, name):
        if name in self._stages:
            raise ValueError('The stage name {} is already used in this pipeline'.format(name))

    def check_item_errors(self, item):
        if item.has_critical_errors():
            if self._raise_on_critical:
                for e in item.critical_errors():
                    raise e.get_exception()
            if self._skip_on_critical:
                return True
        return False

