import os
from typing import Any, Dict

from smartpipeline.helpers import FilePathItem, LocalFilesSource
from smartpipeline.pipeline import Pipeline
from tests.utils import TEXT_SAMPLES, TextExtractor, run_with_timeout

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_file_items(file_directory_source_fx):
    """LocalFilesSource yields FilePathItems whose text gets extracted."""
    os.mkdir(os.path.join(file_directory_source_fx, "subdir"))
    pipeline: Pipeline[Dict[str, Any]] = (
        Pipeline[Dict[str, Any]]()
        .set_source(LocalFilesSource(dir_path=file_directory_source_fx))
        .append("extractor", TextExtractor())
        .build()
    )
    items = run_with_timeout(pipeline)
    assert len(items) == len(TEXT_SAMPLES)
    for item in items:
        assert isinstance(item, FilePathItem)
        assert item.data.get("text") in TEXT_SAMPLES


def test_fileitem():
    """FilePathItem exposes its path and starts with empty data."""
    item = FilePathItem("/path/to/file")
    assert item.id == "file"
    assert not item.data
    assert item.path in str(item)
    empty_item = FilePathItem("")
    assert isinstance(empty_item.id, str) and empty_item.id
    dir_item = FilePathItem("/path/to/dir/")
    assert isinstance(dir_item.id, str) and dir_item.id
