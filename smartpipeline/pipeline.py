import time
from collections import OrderedDict
from concurrent.futures.process import ProcessPoolExecutor

from smartpipeline.error import ErrorManager

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Pipeline:

    def __init__(self):
        self._raise_on_critical = False
        self._skip_on_critical = False
        self._stages = OrderedDict()
        self.error_manager = ErrorManager()
        self.source = None
        self.max_workers = None  # number of CPUs
        self._executor = None

    def _wait(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

    def run(self):
        self._wait()
        if self.source is None:
            raise ValueError("Set the data source for this pipeline")
        item = self.source.pop()
        while item:
            yield self.process(item)
            item = self.source.pop()

    def process(self, item):
        self._wait()
        for name, stage in self._stages.items():
            item = self._process(stage, name, item)
            if item.has_critical_errors():
                if self._raise_on_critical:
                    for e in item.critical_errors():
                        raise e.get_exception()
                if self._skip_on_critical:
                    break
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

    def append_stage(self, name, stage, concurrency=0):
        stage.set_name(name)
        self._stages[name] = stage
        return self

    def append_stage_concurrently(self, name, stage_class, args=[], kwargs={}, concurrency=0):
        self._stages[name] = None  # so the order of the calls of this method is followed
        future = self._get_executor().submit(stage_class, args, kwargs)

        def append_stage(stage_future):
            self._stages[name] = stage_future.result()

        future.add_done_callback(append_stage)
        return self

    def _get_executor(self):
        if self._executor is None:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self._executor

    def _process(self, stage, stage_name, item):
        try:
            time1 = time.time()
            ret = stage.process(item)
        except Exception as e:
            item.set_timing(stage_name, (time.time() - time1) * 1000.)
            self.error_manager.handle(e, stage, item)
            return item
        # this can't be in a finally, otherwise it would register the error_manager time
        item.set_timing(stage_name, (time.time() - time1) * 1000.)
        return ret
