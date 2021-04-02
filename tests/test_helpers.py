from smartpipeline.helpers import LocalFilesSource, FilePathItem
from smartpipeline.pipeline import Pipeline
from tests.utils import TextExtractor, TEXT_SAMPLES

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_file_items(file_directory_source_fx):
    pipeline = (
        Pipeline()
        .set_source(LocalFilesSource(dir_path=file_directory_source_fx))
        .append_stage("extractor", TextExtractor())
        .build()
    )
    items = list(pipeline.run())
    assert len(items) == len(TEXT_SAMPLES)
    for item in items:
        assert isinstance(item, FilePathItem)
        assert item.payload.get("text") in TEXT_SAMPLES
