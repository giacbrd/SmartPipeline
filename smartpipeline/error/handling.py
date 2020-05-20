from __future__ import annotations
import logging
from typing import Optional

from smartpipeline.error.exceptions import Error, CriticalError
from smartpipeline.item import DataItem
from smartpipeline.stage import NameMixin

__author__ = "Giacomo Berardi <giacbrd.com>"


class ErrorManager:
    """
    Basic error handling of a pipeline, principally manages :class:`.exceptions.Error` and :class:`.exceptions.CriticalError` types
    """

    def __init__(self):
        self._raise_on_critical = False
        self._skip_on_critical = True
        self._logger = logging.getLogger(self.__class__.__name__)

    def raise_on_critical_error(self) -> ErrorManager:
        """
        If a :class:`.exceptions.CriticalError` or any un-managed exception is met, raise it externally, kill the pipeline
        """
        self._raise_on_critical = True
        return self

    def no_skip_on_critical_error(self) -> ErrorManager:
        """
        Change default behaviour on :class:`.exceptions.CriticalError`: just skip the current stage .
        Valid only if :meth:`.ErrorManager.raise_on_critical_error` is not set
        """
        self._skip_on_critical = False
        return self

    def handle(
        self, error: Exception, stage: NameMixin, item: DataItem
    ) -> Optional[CriticalError]:
        """
        Manage an error produced by a stage

        :param error: It can be a generic exception or an error from :mod:`.exceptions` explicitly raised by a stage
        :param stage: Stage which raised the exception during processing
        :param item: Item which raised the exception when processed
        :return: If the handled error results to be critical return the generated :class:`.exceptions.CriticalError`
        """
        if type(error) is Error:
            item_error = item.add_error(stage.name, error)
        else:
            # any un-managed exception is a potential critical error
            item_error = item.add_critical_error(stage.name, error)
        exc_info = (type(error), error, error.__traceback__)
        self._logger.exception(self._generate_message(stage, item), exc_info=exc_info)
        if isinstance(item_error, CriticalError):
            exception = self._check_critical(item_error)
            if exception:
                return item_error

    @staticmethod
    def _generate_message(stage: NameMixin, item: DataItem) -> str:
        return "The stage {} has generated an error on item {}".format(stage, item)

    def _check_critical(self, error: CriticalError) -> Optional[Exception]:
        """
        Manage a critical error, usually after an item has been processed by a stage

        :return: The exception which caused a critical error if any
        """
        ex = error.get_exception()
        if self._raise_on_critical:
            raise ex
        elif self._skip_on_critical:
            return ex

    def check_critical_errors(self, item: DataItem) -> Optional[Exception]:
        """
        Check the critical errors registered for an item
        """
        if item.has_critical_errors():
            for er in item.critical_errors():
                ex = self._check_critical(er)
                if ex:
                    return ex
