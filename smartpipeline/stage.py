from abc import ABC, abstractmethod
from typing import Union, Sequence

from smartpipeline.error import Error, CriticalError

__author__ = 'Giacomo Berardi <giacbrd.com>'


class DataItem:
    def __init__(self):
        self._errors = []
        self._critical_errors = []
        self._meta = {}
        self._payload = {}
        self._timings = {}
        self._callback_fun = None

    def has_errors(self):
        return any(self._errors)

    def has_critical_errors(self):
        return any(self._critical_errors)

    def errors(self):
        for e in self._errors:
            yield e

    def critical_errors(self):
        for e in self._critical_errors:
            yield e

    @property
    def payload(self):
        return self._payload

    def add_error(self, stage, exception: Union[Error, Exception]):
        if hasattr(exception, 'set_stage'):
            if not type(exception) is Error:
                raise ValueError("Add a pipeline error or a generic exception.")
            exception.set_stage(stage)
            self._errors.append(exception)
        else:
            error = Error()
            error.with_exception(exception)
            error.set_stage(stage)
            self._errors.append(error)

    def add_critical_error(self, stage, exception: Union[Error, Exception]):
        if hasattr(exception, 'set_stage'):
            if not type(exception) is CriticalError:
                raise ValueError("Add a critical pipeline error or a generic exception.")
            exception.set_stage(stage)
            self._critical_errors.append(exception)
        else:
            error = CriticalError()
            error.with_exception(exception)
            error.set_stage(stage)
            self._critical_errors.append(error)

    def set_metadata(self, field: str, value):
        self._meta[field] = value
        return self

    def get_metadata(self, field: str):
        return self._meta.get(field)

    @property
    def metadata_fields(self):
        return self._meta.keys()

    def set_timing(self, stage: str, ms: float):
        self._timings[stage] = ms
        return self

    def get_timing(self, stage: str):
        return self._timings.get(stage)

    @property
    def timed_stages(self):
        return self._timings.keys()

    @property
    def id(self):
        ret = self._payload.get('id')
        if ret is None:
            ret = self._meta.get('id')
            if ret is None:
                ret = id(self)
        return ret

    def __str__(self):
        return 'Data Item {} with payload {}...'.format(self.id, str(self._payload)[:100])

    def set_callback(self, fun):
        self._callback_fun = fun

    def callback(self):
        if self._callback_fun is not None:
            self._callback_fun(self)


class BaseStage(ABC):

    def set_name(self, name: str):
        self._name = name

    @property
    def name(self):
        return getattr(self, '_name', '<undefined>')

    def __str__(self):
        return 'Stage {}'.format(self.name)


class Stage(BaseStage):

    @abstractmethod
    def process(self, item: DataItem) -> DataItem:
        return item


class BatchStage(BaseStage):

    @abstractmethod
    def process_batch(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        return items

    def __str__(self):
        return 'Batch stage {}'.format(self.name)

    @abstractmethod
    def size(self) -> int:
        return 0

    @abstractmethod
    def timeout(self) -> float:
        """Seconds to wait before flushing a batch"""
        return 0


class Source(ABC):

    @abstractmethod
    def pop(self) -> DataItem:
        return None

    def get_item(self, block=False) -> DataItem:
        return self.pop()

    def stop(self):
        self._is_stopped = True

    @property
    def is_stopped(self):
        return getattr(self, '_is_stopped', False)


class Stop(DataItem):
    def __str__(self):
        return 'Stop signal {}'.format(self.id)
