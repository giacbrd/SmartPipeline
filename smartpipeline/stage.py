from queue import Queue
from abc import ABC, abstractmethod
from typing import Sequence, Union, Any, Optional

from smartpipeline.item import DataItem

__author__ = "Giacomo Berardi <giacbrd.com>"


class NameMixin:
    def set_name(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return getattr(self, "_name", f"{self.__class__.name}_{id(self)}")


class ConcurrentMixin:
    def on_fork(self) -> Any:
        return


class Processor(ABC):
    @abstractmethod
    def process(self, item: DataItem) -> DataItem:
        return item


class BatchProcessor(ABC):
    @abstractmethod
    def process_batch(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        return items


class Stage(NameMixin, ConcurrentMixin, Processor):
    def __str__(self) -> str:
        return "Stage {}".format(self.name)


class BatchStage(NameMixin, ConcurrentMixin, BatchProcessor):
    def __init__(self, size: int, timeout: Optional[float] = None):
        self._size = size
        self._timeout = timeout

    def __str__(self) -> str:
        return "Batch stage {}".format(self.name)

    def size(self) -> int:
        return self._size

    def timeout(self) -> Optional[float]:
        """Seconds to wait before flushing a batch"""
        return self._timeout


class Source(ABC):
    @abstractmethod
    def pop(self) -> Optional[DataItem]:
        return

    def get_item(self, block: bool = False) -> Optional[DataItem]:
        return self.pop()

    def stop(self):
        self._is_stopped = True

    @property
    def is_stopped(self) -> bool:
        return getattr(self, "_is_stopped", False)


ItemsQueue = "Queue[DataItem]"
StageType = Union[Stage, BatchStage]
