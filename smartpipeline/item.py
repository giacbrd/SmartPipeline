from __future__ import annotations

from typing import Union, Generator, Any, KeysView, Callable, Dict
from smartpipeline.defaults import PAYLOAD_SNIPPET_SIZE
from smartpipeline.error.exceptions import Error, CriticalError

__author__ = "Giacomo Berardi <giacbrd.com>"


class DataItem:
    """
    An item containing a unit of data to process through a pipeline.
    They are usually generated by the :class:`.stage.Source` subclass and retrieved at the end of a :class:`.pipeline.Pipeline`.
    """

    def __init__(self):
        self._errors = []
        self._critical_errors = []
        self._meta = {}
        self._payload = {}
        self._timings = {}
        self._callback_fun = None

    def __str__(self) -> str:
        return f"Data item {self.id} with payload {self.payload_snippet()}..."

    @property
    def payload(self) -> Dict[str, Any]:
        """
        Access to the actual data contained in the item

        :return: A dictionary in which organizing data by fields (recommended to be JSON serializable)
        """
        return self._payload

    def payload_snippet(self, max_size: int = PAYLOAD_SNIPPET_SIZE):
        """
        A short string representation of the :attr:`.DataItem.payload` (recommended to override this method)

        :param max_size: Maximum size of the string representation
        """
        return str(self.payload)[:max_size]

    def set_metadata(self, field: str, value: Any) -> DataItem:
        """
        Add a metadata, something we want to remember but keeping it outside the :attr:`.DataItem.payload`

        :param field: Name of the metadata variable
        """
        self._meta[field] = value
        return self

    def get_metadata(self, field: str) -> Any:
        """
        Get a metadata value by its name

        :return: A value or None if the metadata does not exist in the item
        """
        return self._meta.get(field)

    @property
    def metadata_fields(self) -> KeysView[str]:
        """
        Get all the metadata names defined in this item
        """
        return self._meta.keys()

    def set_timing(self, stage_name: str, milliseconds: float) -> DataItem:
        """
        Set the time spent by a stage (referenced by its name) for processing the item
        """
        self._timings[stage_name] = milliseconds
        return self

    def get_timing(self, stage_name: str) -> float:
        """
        Get the time spent by a stage (referenced by its name) for processing the item

        :return: The time in milliseconds or None if the item has not ben processed by the stage
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
        It is recommended to override this in order to properly compute it from the :attr:`.DataItem.payload`
        """
        ret = self._payload.get("id")
        if ret is None:
            ret = self._meta.get("id")
            if ret is None:
                ret = id(self)
        return ret

    def set_callback(self, fun: Callable[[DataItem], Any]):
        """
        Set a function to call after a successful asynchronous execution of a pipeline on the item (through :meth:`.pipeline.Pipeline.process_async`)
        """
        self._callback_fun = fun

    def callback(self) -> Any:
        """
        Call the function set with :meth:`.DataItem.set_callback`
        """
        if self._callback_fun is not None:
            return self._callback_fun(self)

    def has_errors(self) -> bool:
        """
        True if the item has raised an :class:`.error.exceptions.Error` in some stage process
        """
        return any(self._errors)

    def has_critical_errors(self) -> bool:
        """
        True if the item has raised a :class:`.error.exceptions.CriticalError` or any un-managed exception in some stage process
        """
        return any(self._critical_errors)

    def errors(self) -> Generator[Error, None, None]:
        """
        Iter over :class:`.error.exceptions.Error` instances eventually generated in some stage process
        """
        for e in self._errors:
            yield e

    def critical_errors(self) -> Generator[CriticalError, None, None]:
        """
        Iter over :class:`.error.exceptions.CriticalError` instances or any un-managed exception eventually generated in some stage process
        """
        for e in self._critical_errors:
            yield e

    def add_error(self, stage: str, exception: Union[Error, Exception]) -> Error:
        """
        Add an :class:`.error.exceptions.Error` generated in a stage (referenced by its name) for the item

        :param exception: It can be an :class:`.error.exceptions.Error` instance or any exception, which will be encapsulated in an :class:`.error.exceptions.Error`
        """
        # if it is an instance of `Error`
        if type(exception) is Error:
            exception.set_stage(stage)
            self._errors.append(exception)
            return exception
        elif isinstance(exception, Exception) and type(exception) is not CriticalError:
            error = Error()
            error.with_exception(exception)
            error.set_stage(stage)
            self._errors.append(error)
            return error
        else:
            raise ValueError("Add a pipeline Error or a generic exception")

    def add_critical_error(
        self, stage: str, exception: Union[CriticalError, Exception]
    ) -> CriticalError:
        """
        Add a :class:`.error.exceptions.CriticalError` generated in a stage (referenced by its name) for the item

        :param exception: It can be a :class:`.error.exceptions.CriticalError` instance or any exception, which will be encapsulated in a :class:`.error.exceptions.CriticalError`
        """
        # if it is an instance of `CriticalError`
        if type(exception) is CriticalError:
            exception.set_stage(stage)
            self._critical_errors.append(exception)
            return exception
        elif isinstance(exception, Exception) and type(exception) is not Error:
            error = CriticalError()
            error.with_exception(exception)
            error.set_stage(stage)
            self._critical_errors.append(error)
            return error
        else:
            raise ValueError("Add a pipeline CriticalError or a generic exception")


class Stop(DataItem):
    """
    Core "signal" item (do not use this explicitly) used in pipelines for determining the end of a flow of items,
    like an event passed through all the stages
    """

    def __str__(self) -> str:
        return "Stop signal {}".format(self.id)
