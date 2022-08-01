import uuid
from abc import ABC, abstractmethod
from typing import Sequence, Union, Any, Optional, Tuple, Type

from smartpipeline.item import DataItem

__author__ = "Giacomo Berardi <giacbrd.com>"


class NameMixin:
    """
    Simple mixin for setting a name to an object
    """

    def set_name(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        if getattr(self, "_name", None) is None:
            self._name = f"{self.__class__.name}_{uuid.uuid4()}"
        return self._name

    def __str__(self) -> str:
        return self.name


class ConcurrentMixin:
    def on_start(self) -> Any:
        """
        Called after concurrent stage executor initialization in a process (only when multiprocessing concurrency)
        or simply after construction, by the pipeline.
        The stage in the executor is a copy of the original,
        by overriding this method one can initialize variables specifically for the copies, that is mandatory
        when they are not serializable.
        """
        pass


class CloseableMixin:
    def on_end(self) -> Any:
        """
        Called when the stage terminates, useful for executing closing operations (e.g. on files)
        """
        pass


class Processor(ABC):
    @abstractmethod
    def process(self, item: DataItem) -> DataItem:
        """
        Process a single item received by the stage.
        Must be overridden for properly defining a stage

        :return: The same item instance processed and enriched by the stage
        """
        return item


class BatchProcessor(ABC):
    @abstractmethod
    def process_batch(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        """
        Process a batch of items received by the stage.
        Must be overridden for properly defining a batch stage

        :return: The same batch with items processed and enriched by the stage
        """
        return items


class Stage(NameMixin, ConcurrentMixin, CloseableMixin, Processor):
    """
    Extend this class and override :meth:`.Stage.process` for defining a stage
    """

    def __str__(self) -> str:
        return f"Stage {self.name}"


class BatchStage(NameMixin, ConcurrentMixin, CloseableMixin, BatchProcessor):
    """
    Extend this class and override :meth:`.BatchStage.process_batch` for defining a batch stage
    """

    def __init__(self, size: int, timeout: Optional[float] = None):
        """
        :param size: Maximum size of item batches that can be processed together
        :param timeout: Seconds to wait before flushing a batch (calling :meth:`.BatchStage.process_batch` on it)
        """
        self._size = size
        self._timeout = timeout

    def __str__(self) -> str:
        return "Batch stage {}".format(self.name)

    @property
    def size(self) -> int:
        """
        Get the maximum size of item batches that can be processed together
        """
        return self._size

    @property
    def timeout(self) -> Optional[float]:
        """
        Seconds to wait before flushing a batch (calling :meth:`.BatchStage.process_batch` on it)
        """
        return self._timeout


class Source(ABC):
    """
    Extend this for defining a pipeline source
    """

    @abstractmethod
    def pop(self) -> Optional[DataItem]:
        """
        Generate items for feeding a pipeline.
        Must be overridden for properly defining a source.
        Call :meth:`.Source.stop` when item generation is ended

        :return: The generated item, if None it is simply ignored (e.g. after calling :meth:`.Source.stop`)
        """
        pass

    def get_item(self, block: bool = False) -> Optional[DataItem]:
        return self.pop()

    def stop(self):
        """
        Declare the end item generation, this event will be spread through the pipeline
        """
        self._is_stopped = True

    @property
    def is_stopped(self) -> bool:
        """
        True if the source has called the stop event
        """
        return getattr(self, "_is_stopped", False)


ItemsQueue = "Queue[DataItem]"
StageType = Union[Stage, BatchStage]
