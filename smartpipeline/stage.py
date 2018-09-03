import os
from abc import ABC, abstractmethod

from inpipeline.core.error import Error, CriticalError

__author__ = 'Giacomo Berardi <giacbrd.com>'


class DataItem:
    def __init__(self):
        self._errors = []
        self._critical_errors = []
        self._meta = {}
        self._payload = {}

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

    def add_error(self, stage, exception):
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

    def add_critical_error(self, stage, exception):
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

    def set_metadata(self, field, value):
        self._meta[field] = value
        return self

    def get_metadata(self, field):
        return self._meta.get(field)

    @property
    def id(self):
        return self._payload.get('id') or self._meta.get('id') or id(self)

    def __str__(self):
        return 'Data Item {} with payload {}...'.format(self.id, str(self._payload)[:100])


class FileItem(DataItem):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def __str__(self):
        return 'Data Item {}, file path {}, with payload {}...'.format(self.id, self.path, str(self._payload)[:100])

    @property
    def id(self):
        return os.path.basename(self.path) or super().id()


class Stage(ABC):

    def set_name(self, name):
        self._name = name

    @property
    def name(self):
        return getattr(self, '_name', '<undefined>')

    @abstractmethod
    def process(self, item: DataItem):
        return item

    def __str__(self):
        return 'Stage {}'.format(self.name)


class Source(ABC):

    @abstractmethod
    def pop(self):
        return None
