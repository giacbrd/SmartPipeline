import os

from smartpipeline.error import ErrorManager
from smartpipeline.stage import Source, FileItem

__author__ = 'Giacomo Berardi <giacbrd.com>'


class LocalFilesSource(Source):
    def __init__(self, dir_path, postfix=''):
        self.postfix = postfix
        self.dir_path = dir_path
        self._iterator = self._iter_files()

    def _iter_files(self):
        for fname in os.listdir(self.dir_path):
            if fname.endswith(self.postfix) and not fname.startswith('.'):
                yield os.path.join(self.dir_path, fname)

    def pop(self):
        file_path = next(self._iterator, None)
        if file_path:
            item = FileItem(file_path)
            return item


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