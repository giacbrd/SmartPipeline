import os

from smartpipeline.error import ErrorManager
from smartpipeline.stage import DataItem

__author__ = 'Giacomo Berardi <giacbrd.com>'


class FilePathItem(DataItem):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def __str__(self):
        return 'Data Item {}, file path {}, with payload {}...'.format(self.id, self.path, str(self._payload)[:100])

    @property
    def id(self):
        return os.path.basename(self.path) or super().id()


class FileObjItem(FilePathItem):
    def __init__(self, file):
        super().__init__(file.name)
        self.file = file


class ESErrorLogger(ErrorManager):

    def __init__(self, es_host, es_index, es_doctype):
        from elasticsearch import Elasticsearch
        self.es_doctype = es_doctype
        self.es_host = es_host
        self.es_index = es_index
        self.es_client = Elasticsearch(es_host)

    def handle(self, error, stage, item):
        super(ESErrorLogger, self).handle(error, stage, item)
        if hasattr(error, 'get_exception'):
            exception = error.get_exception()
        else:
            exception = error
        self.es_client.index(index=self.es_index, doc_type=self.es_doctype, body={
            'traceback': exception.__traceback__,
            'stage': str(stage),
            'item': str(item),
            'exception': type(exception),
            'message': str(error)
        })
