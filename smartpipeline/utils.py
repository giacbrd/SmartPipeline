from collections import OrderedDict as ODict
from multiprocessing import Manager

__author__ = 'Giacomo Berardi <giacbrd.com>'


class OrderedDict(ODict):

    def last_key(self):
        return next(reversed(self.keys()))


_QUEUE_MANAGER = None


def mp_queue():
    global _QUEUE_MANAGER
    if _QUEUE_MANAGER is None:
        _QUEUE_MANAGER = Manager()
    return _QUEUE_MANAGER.Queue()


def mp_event():
    global _QUEUE_MANAGER
    if _QUEUE_MANAGER is None:
        _QUEUE_MANAGER = Manager()
    return _QUEUE_MANAGER.Event()
