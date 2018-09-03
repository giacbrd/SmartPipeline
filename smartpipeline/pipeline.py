from collections import OrderedDict

from smartpipeline.error import ErrorManager

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Pipeline:

    def __init__(self):
        self._raise_on_critical = False
        self._stages = OrderedDict()
        self.error_manager = ErrorManager()
        self.source = None

    def run(self):
        if self.source is None:
            raise ValueError("Set the data source for this pipeline")
        item = self.source.pop()
        while item:
            yield self.process(item)
            item = self.source.pop()

    def process(self, item):
        for stage in self._stages.values():
            item = self._process(stage, item)
            if item.has_critical_errors():
                if self._raise_on_critical:
                    for e in item.critical_errors():
                        raise e.get_exception()
                break
        return item

    def set_source(self, source):
        self.source = source
        return self

    def set_error_manager(self, error_manager):
        self.error_manager = error_manager
        return self

    def raise_on_critical_error(self):
        self._raise_on_critical = True
        return self

    def append_stage(self, name, stage):
        stage.set_name(name)
        self._stages[name] = stage
        return self

    def _process(self, stage, item):
        ret = item
        try:
            ret = stage.process(item)
        except Exception as e:
            self.error_manager.handle(e, stage, item)
            return item
        finally:
            return ret

