from smartpipeline.helpers import FilePathItem, LocalFilesSource
from smartpipeline.pipeline import Pipeline
from tests.utils import TEXT_SAMPLES, TextExtractor

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_file_items(file_directory_source_fx):
    pipeline = (
        Pipeline()
        .set_source(LocalFilesSource(dir_path=file_directory_source_fx))
        .append("extractor", TextExtractor())
        .build()
    )
    items = list(pipeline.run())
    assert len(items) == len(TEXT_SAMPLES)
    for item in items:
        assert isinstance(item, FilePathItem)
        assert item.data.get("text") in TEXT_SAMPLES


def test_fileitem():
    item = FilePathItem("/path/to/file")
    assert item.id
    assert not item.data
    assert item.path in str(item)
