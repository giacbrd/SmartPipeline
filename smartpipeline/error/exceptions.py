from __future__ import annotations
from typing import Optional


class Error(Exception):
    """
    Base exception type which usually only provokes skipping a stage for an item
    """

    def set_stage(self, stage: str):
        self._pipeline_stage = stage

    def get_stage(self) -> Optional[str]:
        return getattr(self, "_pipeline_stage", None)

    def with_exception(self, exception: Exception) -> Error:
        """
        Set the original exception (if any) that has generated this error
        """
        self._pipeline_exception = exception
        return self

    def get_exception(self) -> Exception:
        """
        Get the original exception (if any) that has generated this error
        """
        return getattr(self, "_pipeline_exception", Exception())

    def __str__(self) -> str:
        error = super(Error, self).__str__().strip()
        exception = str(self.get_exception()).strip()
        return "\n".join((error, exception)).strip()


class CriticalError(Error):
    """
    A type of exception which usually provokes skipping the whole pipeline for an item
    """

    pass
