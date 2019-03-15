import os
from smartpipeline.stage import Source, FilePathItem

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
            item = FilePathItem(file_path)
            return item
