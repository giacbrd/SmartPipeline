from __future__ import annotations
from typing import Optional


class Error(Exception):
    """
    Base exception type for stages
    """

    def set_stage(self, stage: str):
        self._pipeline_stage = stage

    def get_stage(self) -> Optional[str]:
        return getattr(self, "_pipeline_stage", None)

    def with_exception(self, exception: Exception) -> Error:
        """
        Set the original exception (if any) that has generated this error,
        equivalent to `explicit exception chaining <https://www.python.org/dev/peps/pep-3134/#explicit-exception-chaining>`_
        """
        self.__cause__ = exception
        return self

    def get_exception(self) -> Optional[Exception]:
        """
        Get the original exception (if any) that has generated this error,
        equivalent to the `__cause__ <https://www.python.org/dev/peps/pep-3134/#explicit-exception-chaining>`_ attribute
        """
        return self.__cause__ or None


class SoftError(Error):
    """
    A type of exception which usually only provokes skipping a stage for an item
    """

    pass


class CriticalError(Error):
    """
    A type of exception which usually provokes skipping the whole pipeline for an item
    """

    pass
