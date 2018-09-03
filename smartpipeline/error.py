import logging

__author__ = 'Giacomo Berardi <giacbrd.com>'


logger = logging.getLogger(__name__)


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
        return super(Error, self).__str__() or str(self.get_exception())


class CriticalError(Error):
    pass


class ErrorManager:
    def handle(self, error: Exception, stage, item):
        """
        Manage errors produced by a stage
        :param error: it can be a generic exception or an error esplictely thrown by a stage
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
        logger.exception(self._generate_message(stage, item), exc_info=exc_info)

    def _generate_message(self, stage, item):
        return 'The stage {} ha generated an error on data item {}'.format(stage, item)


# example of logger with Elasticsearch
#
# class ErrorLogger(ErrorManager):
#
#     def __init__(self, es_host, es_index, es_doctype):
#         self.es_doctype = es_doctype
#         self.es_host = es_host
#         self.es_index = es_index
#         self.es_client = Elasticsearch(es_host)
#
#     def handle(self, error, stage, item):
#         super(ErrorLogger, self).handle(error, stage, item)
#         if hasattr(error, 'get_exception'):
#             exception = error.get_exception()
#         else:
#             exception = error
#         self.es_client.index(index=self.es_index, doc_type=self.es_doctype, body={
#             'traceback': exception.__traceback__,
#             'stage': str(stage),
#             'item': str(item),
#             'exception': type(exception),
#             'message': str(error)
#         })
