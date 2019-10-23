import logging

__author__ = 'Giacomo Berardi <giacbrd.com>'


class Error(Exception):

    def set_stage(self, stage):
        self._intelmatch_pipeline_stage = stage

    def get_stage(self):
        return getattr(self, '_intelmatch_pipeline_stage', None)

    def with_exception(self, exception):
        self._intelmatch_pipeline_exception = exception
        return self

    def get_exception(self):
        return getattr(self, '_intelmatch_pipeline_exception', Exception())

    def __str__(self):
        error = super(Error, self).__str__().strip()
        exception = str(self.get_exception()).strip()
        return '\n'.join((error, exception)).strip()


class CriticalError(Error):
    pass


class ErrorManager:
    def __init__(self):
        self._raise_on_critical = False
        self._skip_on_critical = True
        self._logger = logging.getLogger(self.__class__.__name__)

    def raise_on_critical_error(self):
        self._raise_on_critical = True
        return self

    def no_skip_on_critical_error(self):
        self._skip_on_critical = False
        return self

    def handle(self, error: Exception, stage, item):
        """
        Manage errors produced by a stage
        :param error: it can be a generic exception or an error explicitly thrown by a stage
        :param stage:
        :param item:
        """
        if type(error) is Error:
            item.add_error(stage, error)
        elif type(error) is CriticalError:
            item.add_critical_error(stage, error)
        else:
            # any exception is a critical error
            item.add_critical_error(stage, error)
        exc_info = (type(error), error, error.__traceback__)
        self._logger.exception(self._generate_message(stage, item), exc_info=exc_info)
        return self.check_errors(item)

    def _generate_message(self, stage, item):
        return 'The stage {} ha generated an error on item {}'.format(stage, item)

    def check_errors(self, item):
        if item.has_critical_errors():
            for er in item.critical_errors():
                ex = er.get_exception()
                if self._raise_on_critical:
                    raise ex
                elif self._skip_on_critical:
                    return ex
        return None
