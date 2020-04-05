from __future__ import annotations

from typing import Union, Generator, Any, KeysView, Callable, Dict
from smartpipeline.defaults import PAYLOAD_SNIPPET_SIZE
from smartpipeline.error.exceptions import Error, CriticalError

__author__ = "Giacomo Berardi <giacbrd.com>"


class DataItem:
    def __init__(self):
        self._errors = []
        self._critical_errors = []
        self._meta = {}
        self._payload = {}
        self._timings = {}
        self._callback_fun = None

    def payload_snippet(self, max_size: int = PAYLOAD_SNIPPET_SIZE):
        return str(self.payload)[:max_size]

    def has_errors(self) -> bool:
        return any(self._errors)

    def has_critical_errors(self) -> bool:
        return any(self._critical_errors)

    def errors(self) -> Generator[Error, None, None]:
        for e in self._errors:
            yield e

    def critical_errors(self) -> Generator[CriticalError, None, None]:
        for e in self._critical_errors:
            yield e

    @property
    def payload(self) -> Dict[str, Any]:
        return self._payload

    def add_error(self, stage: str, exception: Union[Error, Exception]) -> Error:
        if hasattr(exception, "set_stage"):
            if not type(exception) is Error:
                raise ValueError("Add a pipeline error or a generic exception.")
            exception.set_stage(stage)
            self._errors.append(exception)
            return exception
        else:
            error = Error()
            error.with_exception(exception)
            error.set_stage(stage)
            self._errors.append(error)
            return error

    def add_critical_error(
        self, stage: str, exception: Union[CriticalError, Exception]
    ) -> CriticalError:
        if hasattr(exception, "set_stage"):
            if not type(exception) is CriticalError:
                raise ValueError(
                    "Add a critical pipeline error or a generic exception."
                )
            exception.set_stage(stage)
            self._critical_errors.append(exception)
            return exception
        else:
            error = CriticalError()
            error.with_exception(exception)
            error.set_stage(stage)
            self._critical_errors.append(error)
            return error

    def set_metadata(self, field: str, value: Any) -> DataItem:
        self._meta[field] = value
        return self

    def get_metadata(self, field: str) -> Any:
        return self._meta.get(field)

    @property
    def metadata_fields(self) -> KeysView[str]:
        return self._meta.keys()

    def set_timing(self, stage_name: str, ms: float) -> DataItem:
        self._timings[stage_name] = ms
        return self

    def get_timing(self, stage_name: str) -> float:
        return self._timings.get(stage_name)

    @property
    def timed_stages(self) -> KeysView[str]:
        return self._timings.keys()

    @property
    def id(self) -> Any:
        ret = self._payload.get("id")
        if ret is None:
            ret = self._meta.get("id")
            if ret is None:
                ret = id(self)
        return ret

    def __str__(self) -> str:
        return f"Data Item {self.id} with payload {self.payload_snippet()}..."

    def set_callback(self, fun: Callable):
        self._callback_fun = fun

    def callback(self) -> Any:
        if self._callback_fun is not None:
            return self._callback_fun(self)


class Stop(DataItem):
    def __str__(self) -> str:
        return "Stop signal {}".format(self.id)
