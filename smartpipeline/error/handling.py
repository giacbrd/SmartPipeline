from __future__ import annotations
import logging
from typing import Optional

from smartpipeline.error.exceptions import Error, CriticalError
from smartpipeline.item import DataItem
from smartpipeline.stage import NameMixin

__author__ = 'Giacomo Berardi <giacbrd.com>'


class ErrorManager:
    def __init__(self):
        self._raise_on_critical = False
        self._skip_on_critical = True
        self._logger = logging.getLogger(self.__class__.__name__)

    def raise_on_critical_error(self) -> ErrorManager:
        self._raise_on_critical = True
        return self

    def no_skip_on_critical_error(self) -> ErrorManager:
        self._skip_on_critical = False
        return self

    def handle(self, error: Exception, stage: NameMixin, item: DataItem) -> Optional[Exception]:
        """
        Manage errors produced by a stage
        :param error: it can be a generic exception or an error explicitly thrown by a stage
        :param stage:
        :param item:
        """
        if type(error) is Error:
            item.add_error(stage.name, error)
        elif type(error) is CriticalError:
            item.add_critical_error(stage.name, error)
        else:
            # any unmanaged exception is a critical error
            item.add_critical_error(stage.name, error)
        exc_info = (type(error), error, error.__traceback__)
        self._logger.exception(self._generate_message(stage, item), exc_info=exc_info)
        return self.check_errors(item)

    def _generate_message(self, stage: NameMixin, item: DataItem) -> str:
        return 'The stage {} has generated an error on item {}'.format(stage, item)

    def check_errors(self, item: DataItem) -> Optional[Exception]:
        if item.has_critical_errors():
            for er in item.critical_errors():
                ex = er.get_exception()
                if self._raise_on_critical:
                    raise ex
                elif self._skip_on_critical:
                    return ex
        return None
