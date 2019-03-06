import time
import uuid
from _queue import Empty
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Queue, Manager

from smartpipeline.error import ErrorManager
from smartpipeline.stage import Source
from smartpipeline.utils import OrderedDict

__author__ = 'Giacomo Berardi <giacbrd.com>'


def _stage_processor(stage, in_queue, out_queue):
    while True:
        item = in_queue.get(block=True)
        #FIXME manage the fact that a developer could have missed the `return item` (so he returns a None) in the overloaded `process` method of his stage
        if item is None:
            out_queue.put(None, block=True)
            return
        item = stage.process(item)
        out_queue.put(item, block=True)
        # FIXME manage queue size, timeout, minimum time between processes


class Pipeline:

    def __init__(self):
        self._concurrencies = {}
        self._raise_on_critical = False
        self._skip_on_critical = False
        self._stages = OrderedDict()
        self.error_manager = ErrorManager()
        self.source = None
        self.max_workers = None  # default: number of CPUs
        self._init_executor = None
        self._stage_executors = {}
        self._in_queue = None
        self._queues = OrderedDict()
        self._queue_manager = Manager()
        self._source_name = None

    def _wait_executors(self):
        if self._init_executor is not None:
            self._init_executor.shutdown(wait=True)
            self._init_executor = None
        if self._stage_executors:
            for name in self._stage_executors.keys():
                executor = self._get_stage_executor(name)
                executor.submit(_stage_processor, self._stages[name], self._queues[name][0], self._queues[name][1])

    def _shutdown(self):
        for queues in self._queues.values():  # interrupt all processor
            if queues[0] is not None:
                queues[0].put(None)
        if self._init_executor is not None:
            self._init_executor.shutdown()
        if self._stage_executors:
            for executor in self._stage_executors.values():
                executor.shutdown()

    def __del__(self):
        self._shutdown()

    def run(self):
        self._wait_executors()
        if self.source is None:
            raise ValueError("Set the data source for this pipeline")
        if self._stage_executors:
            for item in self._run_concurrently():
                yield item
        else:
            item = self.source.pop()
            while item is not None:
                yield self.process(item)
                item = self.source.pop()

    def _run_concurrently(self):
        item = 'FLAG'  # if the item is set to the FLAG, we must get the next one from a queue
        while item is not None or not self._concurrent_queues_are_empy():  # the second condition is checked at the end while processing last items
            item = 'FLAG'
            for name, stage in self._stages.items():
                if name not in self._stage_executors:
                    in_queue, out_queue = self._queues[name]
                    if item != 'FLAG':
                        item = stage.process(item)
                    else:
                        # in_queue must exists
                        item = in_queue.get(block=True)
                        if item is None:  # the source has sent the stop
                            #FIXME
                            continue
                        item = stage.process(item)
                    if out_queue is not None:  # next stage is concurrent
                        assert item is not None
                        out_queue.put(item, block=True)
                        item = 'FLAG'  # next non current stage will get an item from a queue
            last_queue = self._queues[self._stages.last_key()][1]
            if last_queue is not None:
                item = last_queue.get(block=True)
            yield item
        self._shutdown()

    def process(self, item):
        if self._stage_executors:
            raise Exception('Cannot process a single item when some stages are concurrent')  #FIXME on should be able to asyncronously process single items
        for name, stage in self._stages.items():
            item = self._process(stage, name, item)
            if self.check_item_errors(item):
                return item
        return item

    def set_source(self, source):
        self.source = source
        self._in_queue = source
        self._source_name = uuid.uuid4()
        self._queues[self._source_name] = (None, self.source)
        self._queues.move_to_end(self._source_name, last=False)
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

    def append_stage(self, name, stage, concurrency=0, use_threads=True):
        self._check_stage_name(name)
        stage.set_name(name)
        self._init_queues(name, concurrency > 0)
        if concurrency > 0:
            self._init_worker(concurrency, name, use_threads)
        self._stages[name] = stage
        return self

    def get_stage(self, stage_name):
        return self._stages.get(stage_name)

    def append_stage_concurrently(self, name, stage_class, args=None, kwargs=None, concurrency=0, use_threads=True):
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        self._check_stage_name(name)
        self._init_queues(name, concurrency > 0)
        if concurrency > 0:  #FIXME extract in one method
            self._init_worker(concurrency, name, use_threads)
        self._stages[name] = None  # so the order of the calls of this method is followed in `_stages`
        future = self._get_init_executor(use_threads).submit(stage_class, args, kwargs)

        def append_stage(stage_future):
            self._stages[name] = stage_future.result()

        future.add_done_callback(append_stage)
        return self

    def _init_worker(self, concurrency, name, use_threads):
        self._concurrencies[name] = concurrency
        return self._get_stage_executor(name, use_threads)

    def _get_init_executor(self, use_threads=True):
        if self._init_executor is None:
            executor = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
            self._init_executor = executor(max_workers=self.max_workers)
        return self._init_executor

    def _get_stage_executor(self, name, use_threads=True):
        if name not in self._stage_executors or self._stage_executors[name] is None:
            executor = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
            self._stage_executors[name] = executor(max_workers=self._concurrencies.get(name, 1))
        return self._stage_executors[name]

    def _process(self, stage, stage_name, item):
        time1 = time.time()
        try:
            ret = stage.process(item)
        except Exception as e:
            item.set_timing(stage_name, (time.time() - time1) * 1000.)
            self.error_manager.handle(e, stage, item)
            return item
        # this can't be in a finally, otherwise it would register the `error_manager.handle` time
        item.set_timing(stage_name, (time.time() - time1) * 1000.)
        return ret

    def _init_queues(self, name, concurrent):
        self._queues[name] = [None, None]
        last_stage = None
        if self._stages:
            last_stage = self._stages.last_key()
            assert last_stage != name, 'This worker is initialized after the stage {} is built'.format(name)
        elif self._source_name:
            last_stage = self._source_name
        if last_stage is None and concurrent:  # first stage and no source set
            self._in_queue = self._queue_manager.Queue()
            self._queues[name] = (self._in_queue, self._queue_manager.Queue())
        else:
            prev_queues = self._queues.get(last_stage)
            if prev_queues[1] is None and concurrent:  # last stage is not concurrent
                in_queue = self._queue_manager.Queue()
                self._queues[last_stage] = (prev_queues[0], in_queue)
            else:
                in_queue = prev_queues[1]
            self._queues[name] = [in_queue, self._queue_manager.Queue() if concurrent else None]

    def _check_stage_name(self, name):
        if name in self._stages or name in self._queues:
            raise ValueError('The stage name {} is already used in this pipeline'.format(name))

    def check_item_errors(self, item):
        if item.has_critical_errors():
            if self._raise_on_critical:
                for e in item.critical_errors():
                    raise e.get_exception()
            if self._skip_on_critical:
                return True
        return False

    def _concurrent_queues_are_empy(self):
        for name, queues in self._queues.items():
            if queues[0] is not None and not isinstance(queues[0], Source) and not queues[0].empty():
                return False
            elif queues[1] is not None and not isinstance(queues[1], Source) and not queues[1].empty():
                return False
        return True

