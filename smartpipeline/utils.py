import threading
from abc import ABC, abstractmethod
from collections import OrderedDict

__author__ = "Giacomo Berardi <giacbrd.com>"


class LastOrderedDict(OrderedDict):
    def last_key(self):
        return next(reversed(self.keys()))


class ConcurrentCounter(ABC):
    """
    Interface for a counter that is safe for concurrent access
    """

    @abstractmethod
    def __iadd__(self, incr):
        return self

    @abstractmethod
    def value(self):
        return 0


class ThreadCounter(ConcurrentCounter):
    """
    Thread safe counter
    """

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


class ProcessCounter(ConcurrentCounter):
    """
    Process safe counter
    """

    def __init__(self, manager):
        self._value = manager.Value("i", 0)
        self._lock = manager.Lock()

    def __iadd__(self, incr):
        with self._lock:
            self._value.value += incr
        return self

    @property
    def value(self):
        with self._lock:
            return self._value.value
