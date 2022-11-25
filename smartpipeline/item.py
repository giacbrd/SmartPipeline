from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, Generator, KeysView, Union

from smartpipeline.defaults import DATA_SNIPPET_SIZE
from smartpipeline.error.exceptions import CriticalError, SoftError

__author__ = "Giacomo Berardi <giacbrd.com>"


class Item:
    """
    An item containing a unit of data to process through a pipeline.
    Is ia usually generated by the :class:`.stage.Source` subclass and retrieved at the end of a :class:`.pipeline.Pipeline`.
    """

    def __init__(self):
        self._soft_errors = []
        self._critical_errors = []
        self._meta = {}
        self._payload = {}
        self._timings = {}
        self._callback_fun = None
        self._id = None

    def __str__(self) -> str:
        return f"Data item {self.id} with payload {self.data_snippet()}..."

    @property
    def data(self) -> Dict[str, Any]:
        """
        Access to the actual data contained in the item

        :return: A dictionary in which organizing data by fields (recommended to be JSON serializable)
        """
        return self._payload

    # deprecated
    payload = data  # pragma: no cover

    def data_snippet(self, max_size: int = DATA_SNIPPET_SIZE):
        """
        A short string representation of the :attr:`.Item.data` (recommended to override this method)

        :param max_size: Maximum size of the string representation
        """
        return str(self.data)[:max_size]

    def set_metadata(self, field: str, value: Any) -> Item:  # pragma: no cover
        """
        DEPRECATED: Add a metadata, something we want to remember but keep outside the actual data in :attr:`.Item.data`

        :param field: Name of the metadata variable
        """
        self._meta[field] = value
        return self

    def get_metadata(self, field: str) -> Any:  # pragma: no cover
        """
        DEPRECATED: Get a metadata value by its name

        :return: A value or None if the metadata does not exist in the item
        """
        return self._meta.get(field)

    @property
    def metadata(self) -> Dict[str, Any]:
        """
        Get the actual metadata dictionary
        """
        return self._meta

    def set_timing(self, stage_name: str, seconds: float) -> Item:
        """
        Set the time spent by a stage (referenced by its name) for processing the item
        """
        self._timings[stage_name] = seconds
        return self

    def get_timing(self, stage_name: str) -> float:
        """
        Get the time spent by a stage (referenced by its name) for processing the item

        :return: The time in seconds or None if the item has not been processed by the stage
        """
        return self._timings.get(stage_name)

    @property
    def timed_stages(self) -> KeysView[str]:
        """
        Get the stage names for which the item has a process time
        """
        return self._timings.keys()

    @property
    def id(self) -> Any:
        """
        Get the unique identifier of the item.
        It is recommended to override this in order to properly compute it from the :attr:`.Item.data`
        """

        ret = self._payload.get("id")
        if ret is None:
            ret = self._meta.get("id")
            if ret is None:
                if self._id is None:
                    self._id = str(uuid.uuid4())
                return self._id
        return ret

    def set_callback(self, fun: Callable[[Item], Any]):
        """
        Set a function to call after a successful asynchronous execution of a pipeline on the item (through :meth:`.pipeline.Pipeline.process_async`)
        """
        self._callback_fun = fun

    def callback(self) -> Any:
        """
        Call the function set with :meth:`.Item.set_callback`
        """
        if self._callback_fun is not None:
            return self._callback_fun(self)

    def has_soft_errors(self) -> bool:
        """
        True if the item has raised an :class:`.error.exceptions.SoftError` in some stage processing
        """
        return any(self._soft_errors)

    def has_critical_errors(self) -> bool:
        """
        True if the item has raised a :class:`.error.exceptions.CriticalError` or any un-managed exception in some stage processing
        """
        return any(self._critical_errors)

    def soft_errors(self) -> Generator[SoftError, None, None]:
        """
        Iter over :class:`.error.exceptions.SoftError` instances eventually generated in some stage processing
        """
        for e in self._soft_errors:
            yield e

    def critical_errors(self) -> Generator[CriticalError, None, None]:
        """
        Iter over :class:`.error.exceptions.CriticalError` instances or any un-managed exception eventually generated in some stage processing
        """
        for e in self._critical_errors:
            yield e

    def add_soft_error(
        self, stage: str, exception: Union[SoftError, Exception]
    ) -> SoftError:
        """
        *This is called internally by the* :class:`.error.handling.ErrorManager` *when the exception is handled*.
        Add an :class:`.error.exceptions.SoftError` generated in a stage (referenced by its name) for the item.

        :param exception: It can be an :class:`.error.exceptions.SoftError` instance or any exception, which will be encapsulated in an :class:`.error.exceptions.SoftError`
        """
        if type(exception) is not CriticalError:
            if isinstance(exception, SoftError):
                exception.set_stage(stage)
                self._soft_errors.append(exception)
                return exception
            elif isinstance(exception, Exception):
                error = SoftError(str(exception))
                error.with_exception(exception)
                error.set_stage(stage)
                self._soft_errors.append(error)
                return error
        raise ValueError("Add a pipeline SoftError or a generic exception")

    def add_critical_error(
        self, stage: str, exception: Union[CriticalError, Exception]
    ) -> CriticalError:
        """
        *This is called internally by the* :class:`.error.handling.ErrorManager` *when the exception is handled*.
        Add a :class:`.error.exceptions.CriticalError` generated in a stage (referenced by its name) for the item

        :param exception: It can be a :class:`.error.exceptions.CriticalError` instance or any exception, which will be encapsulated in a :class:`.error.exceptions.CriticalError`
        """
        if type(exception) is not SoftError:
            if isinstance(exception, CriticalError):
                exception.set_stage(stage)
                self._critical_errors.append(exception)
                return exception
            elif isinstance(exception, Exception):
                error = CriticalError(str(exception))
                error.with_exception(exception)
                error.set_stage(stage)
                self._critical_errors.append(error)
                return error
        raise ValueError("Add a pipeline CriticalError or a generic exception")


# deprecated
DataItem = Item  # pragma: no cover


class Stop(Item):
    """
    Core "signal" item (*do not use this explicitly*) used in pipelines for determining the end of a flow of items,
    like an event passed through all the stages
    """

    def __str__(self) -> str:
        return f"Stop signal {self.id}"
