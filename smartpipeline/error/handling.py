from __future__ import annotations
import logging
from typing import Optional, Union, Any, Tuple, Type

from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.item import DataItem
from smartpipeline.stage import NameMixin

__author__ = "Giacomo Berardi <giacbrd.com>"

_logger = logging.getLogger(__name__)


class ErrorManager:
    """
    Basic error handling of a pipeline, principally manages :class:`.exceptions.SoftError` and :class:`.exceptions.CriticalError` types
    """

    _raise_on_critical = False
    _skip_on_critical = True

    def raise_on_critical_error(self) -> ErrorManager:
        """
        Set the error manager so that if a :class:`.exceptions.CriticalError` or any un-managed exception is met,
        raise it "externally", killing the pipeline
        """
        self._raise_on_critical = True
        return self

    def no_skip_on_critical_error(self) -> ErrorManager:
        """
        Change default behaviour of the error manager on :class:`.exceptions.CriticalError`: only skip the stage which raises it,
        like the :class:`.exceptions.SoftError` .
        Valid only if :meth:`.ErrorManager.raise_on_critical_error` is not set
        """
        self._skip_on_critical = False
        return self

    def on_start(self) -> Any:
        """
        Called for a concurrent stage executor in a process (only when multiprocessing concurrency)
        or simply after construction, by the pipeline.
        The error manager in the executor is a copy of the original,
        by overriding this method one can initialize variables specifically for the copies, that is mandatory
        when they are not serializable.
        """
        pass

    def on_end(self) -> Any:
        """
        Called when the pipeline terminates, useful for executing closing operations (e.g. on files)
        """
        pass

    def handle(
        self, error: Exception, stage: NameMixin, item: DataItem
    ) -> Optional[CriticalError]:
        """
        Manage an error produced by a stage

        :param error: It can be a generic exception or an error from :mod:`.exceptions` explicitly raised by a stage
        :param stage: Stage which raised the exception during processing
        :param item: Item which raised the exception when processed
        :return: If the handled error results to be critical return the generated :class:`.exceptions.CriticalError`
        :raises Exception: When a :meth:`.ErrorManager.raise_on_critical_error` has been set and the error is critical
        """
        if isinstance(error, SoftError):
            item_error = item.add_soft_error(stage.name, error)
        else:
            # any un-managed exception is a potential critical error
            item_error = item.add_critical_error(stage.name, error)
        exc_info = (type(item_error), item_error, item_error.__traceback__)
        _logger.exception(self._generate_message(stage, item), exc_info=exc_info)
        if isinstance(item_error, CriticalError):
            exception = self._check_critical(item_error)
            if exception:
                return item_error

    @staticmethod
    def _generate_message(stage: NameMixin, item: DataItem) -> str:
        return f"{stage} has generated an error on item {item}"

    def _check_critical(self, error: CriticalError) -> Union[Exception, CriticalError]:
        """
        Manage a critical error, usually after an item has been processed by a stage

        :return: The exception which caused a critical error if any, otherwise the :class:`.exceptions.CriticalError` itself
        :raises Exception: When a :meth:`.ErrorManager.raise_on_critical_error` has been set
        """
        ex = error.get_exception()
        if self._raise_on_critical:
            raise ex or error
        elif self._skip_on_critical:
            return ex or error

    def check_critical_errors(self, item: DataItem) -> Optional[Exception]:
        """
        Check the critical errors registered for an item
        """
        if item.has_critical_errors():
            for er in item.critical_errors():
                ex = self._check_critical(er)
                if ex:
                    return ex


class RetryManager:
    """
    This class encapsulate the parameters used to handle the retry strategy in case some kind of error are raise by the stage
    """

    def __init__(
        self,
        retryable_errors: Tuple[Type[Exception], ...] = tuple(),
        max_retries: int = 0,
        backoff: Union[float, int] = 0,
    ):
        """
        :param retryable_errors: tuple of errors types which the retry strategy is applied for
        :param max_retries: maximum number of attempts for a which a stage is run in case of one of the `retryable_errors` is raised during its execution
        :param backoff: weight for the exponential back-off strategy
        """
        self._backoff = backoff
        self._max_retries = max_retries
        self._retryable_errors = retryable_errors

    @property
    def backoff(self) -> float:
        return self._backoff

    @property
    def max_retries(self) -> int:
        return self._max_retries

    @property
    def retryable_errors(self) -> Tuple[Type[Exception], ...]:
        return self._retryable_errors
