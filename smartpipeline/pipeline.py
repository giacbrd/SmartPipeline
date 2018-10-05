import time
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Queue

from smartpipeline.error import ErrorManager
from smartpipeline.utils import OrderedDict

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Pipeline:

    def __init__(self):
        self._concurrencies = {}
        self._raise_on_critical = False
        self._skip_on_critical = False
        self._stages = OrderedDict()
        self.error_manager = ErrorManager()
        self.source = None
        self.max_workers = None  # number of CPUs
        self._init_executor = None
        self._stage_executors = {}
        self._in_queue = None
        self._queues = OrderedDict()

    def _get_stage_processor(self, stage, queues):
        def _processor():
            while True:
                item = queues[0].get(block=True)
                item = stage.process(item)
                if self._check_item_errors(item):
                    return item
                queues[1].put(item, block=True)
                # FIXME manage queue size and timeout
        return _processor

    def _wait_executors(self):
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        if self._stage_executors:
            for name in self._stage_executors.keys():
                executor = self._get_stage_executor(name)
                executor.submit(self._get_stage_processor(self._stages[name], self._queues[name]))

    def __del__(self):
        if self._init_executor is not None:
            self._init_executor.shutdown()
        if self._stage_executors:
            for executor in self._stage_executors.values():
                executor.shutdown()

    def run(self):
        self._wait_executors()
        if self.source is None:
            raise ValueError("Set the data source for this pipeline")
        if self._stage_executors:
            return self._run_concurrently()
        item = self.source.pop()
        while item is not None:
            yield self.process(item)
            item = self.source.pop()

    def _run_concurrently(self):
        item = self.source.pop()
        while item is not None:
            prev_stage = self.source
            for name, stage in self._stages.items():
                # stage is concurrent
                if self._concurrencies.get(name, 0) > 0:
                    # if previous stage is not concurrent or it is the source (in the main thread)
                    if prev_stage == self.source or prev_stage not in self._stage_executors:
                        self._queues[name][0].put(item, block=True)
                        continue
                    # if this is the last stage
                    if self._stages.last_key() == name:
                        item = self._queues[name][1].get(block=True)
                else:
                    # if previous stage is concurrent
                    if prev_stage in self._stage_executors:
                        item = self._queues[prev_stage][1].get(block=True)
                    item = self._process(stage, name, item)
                prev_stage = name
                if self._check_item_errors(item):
                    break
            yield item
            item = self.source.pop()

    def process(self, item):
        if self._stage_executors:
            raise Exception('Cannot process a single item when some stages are concurrent')
        for name, stage in self._stages.items():
            item = self._process(stage, name, item)
            if self._check_item_errors(item):
                return item
        return item

    def set_source(self, source):
        self.source = source
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

    def append_stage(self, name, stage, concurrency=0, use_threads=False):
        self._check_stage_name(name)
        stage.set_name(name)
        if concurrency > 0:
            self._concurrencies[name] = concurrency
            self._init_worker(name)
        self._stages[name] = stage
        return self

    def append_stage_concurrently(self, name, stage_class, args=[], kwargs={}, concurrency=0, use_threads=False):
        self._check_stage_name(name)
        if concurrency > 0:
            self._concurrencies[name] = concurrency
            self._init_worker(name)
        self._stages[name] = None  # so the order of the calls of this method is followed
        future = self._get_init_executor().submit(stage_class, args, kwargs)

        def append_stage(stage_future):
            self._stages[name] = stage_future.result()

        future.add_done_callback(append_stage)
        return self

    def _get_init_executor(self):
        if self._init_executor is None:
            self._init_executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self._init_executor

    def _get_stage_executor(self, name):
        if name not in self._stage_executors or self._stage_executors[name] is None:
            self._stage_executors[name] = ProcessPoolExecutor(max_workers=self._concurrencies.get(name, 1))
        return self._stage_executors[name]

    def _process(self, stage, stage_name, item):
        time1 = time.time()
        try:
            ret = stage.process(item)
        except Exception as e:
            item.set_timing(stage_name, (time.time() - time1) * 1000.)
            self.error_manager.handle(e, stage, item)
            return item
        # this can't be in a finally, otherwise it would register the error_manager time
        item.set_timing(stage_name, (time.time() - time1) * 1000.)
        return ret

    def _init_worker(self, name):
        #FIXME also build executor
        if self._stages:
            last_stage = self._stages.last_key()
            assert last_stage != name, 'This worker is initialized after the stage {} is built'.format(name)
            prev_queues = self._queues.get(last_stage)
            if prev_queues is None:
                in_queue = Queue()
                self._queues[last_stage] = (None, in_queue)
            else:
                in_queue = prev_queues[1]
        else:
            self._in_queue = in_queue = Queue()
        self._queues[name] = (in_queue, Queue())

    def _check_stage_name(self, name):
        if name in self._stages or name in self._queues:
            raise ValueError('The stage name {} is already used in this pipeline'.format(name))

    def _check_item_errors(self, item):
        if item.has_critical_errors():
            if self._raise_on_critical:
                for e in item.critical_errors():
                    raise e.get_exception()
            if self._skip_on_critical:
                return True
        return False
