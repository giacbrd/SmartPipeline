import os
from typing import Generator, Optional, Any, IO

from smartpipeline.stage import Source
from smartpipeline.item import DataItem

__author__ = "Giacomo Berardi <giacbrd.com>"


class LocalFilesSource(Source):
    """
    Generate a special file item for any file found in a directory
    """

    def __init__(self, dir_path: str, postfix: str = ""):
        self.postfix = postfix
        self.dir_path = dir_path
        self._iterator = self._iter_files()

    def _iter_files(self) -> Generator[str, None, None]:
        for fname in os.listdir(self.dir_path):
            if fname.endswith(self.postfix) and not fname.startswith("."):
                yield os.path.join(self.dir_path, fname)

    def pop(self) -> Optional[DataItem]:
        file_path = next(self._iterator, None)
        if file_path:
            item = FilePathItem(file_path)
            return item
        else:
            self.stop()


class FilePathItem(DataItem):
    """
    An item with a pointer to a file path
    """

    def __init__(self, path: str):
        super().__init__()
        self.path = path

    def __str__(self) -> str:
        return f"Data item {self.id}, file path {self.path}, with payload {self.payload_snippet()}..."

    @property
    def id(self) -> Any:
        return os.path.basename(self.path) or super().id()


class FileObjItem(FilePathItem):
    """
    An item with a pointer to a file object
    """

    def __init__(self, file: IO):
        super().__init__(file.name)
        self.file = file
