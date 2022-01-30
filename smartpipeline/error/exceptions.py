from __future__ import annotations
from typing import Optional


class Error(Exception):
    """
    Base exception type for stages
    """

    def set_stage(self, stage: str):
        self._pipeline_stage = stage
        self._cause = None

    def get_stage(self) -> Optional[str]:
        return getattr(self, "_pipeline_stage", None)

    def with_exception(self, exception: Exception) -> Error:
        """
        Set the original exception (if any) that has gener ated this error,
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


class RetryError(SoftError):
    """
    A stage processing an input item may raise a TimeoutError. If so, the stage tries to reprocess the
    item several times, with the exponential backoff retry strategy. After several attempts a RetryError is raised and
    that stage is skipped for that item
    """

    pass
