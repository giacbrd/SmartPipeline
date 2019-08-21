import threading
from collections import OrderedDict as ODict
from multiprocessing import Lock

__author__ = 'Giacomo Berardi <giacbrd.com>'


class OrderedDict(ODict):

    def last_key(self):
        return next(reversed(self.keys()))


class ThreadCounter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def __iadd__(self, incr):
        with self._lock:
            self._value += incr
        return self

    @property
    def value(self):
        with self._lock:
            return self._value


class ProcessCounter:
    def __init__(self, manager):
        self._value = manager.Value('i', 0)
        self._lock = manager.Lock()

    def __iadd__(self, incr):
        with self._lock:
            self._value.value += incr
        return self

    @property
    def value(self):
        with self._lock:
            return self._value.value
