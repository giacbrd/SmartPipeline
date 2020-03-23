from queue import Queue
from abc import ABC, abstractmethod
from typing import Sequence

from smartpipeline.item import DataItem

__author__ = 'Giacomo Berardi <giacbrd.com>'


class NameMixin:

    def set_name(self, name: str):
        self._name = name

    @property
    def name(self):
        return getattr(self, '_name', f'{self.__class__.name}_{id(self)}')


class Processor(ABC):

    @abstractmethod
    def process(self, item: DataItem) -> DataItem:
        return item


class BatchProcessor(ABC):

    @abstractmethod
    def process_batch(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        return items


class Stage(NameMixin, Processor):

    def __str__(self):
        return 'Stage {}'.format(self.name)


class BatchStage(NameMixin, BatchProcessor):

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


ItemsQueue = "Queue[DataItem]"
