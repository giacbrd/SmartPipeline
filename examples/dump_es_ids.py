"""Given a file of IDs (one per line) of documents indexed in an Elasticsearch cluster,
generate a jsonl dump of the documents"""

__author__ = "Giacomo Berardi <giacbrd.com>"

import json
import logging
from argparse import ArgumentParser
from typing import IO, Optional, Sequence

from elasticsearch import Elasticsearch

from smartpipeline.item import DataItem
from smartpipeline.pipeline import Pipeline
from smartpipeline.stage import Source, BatchStage, Stage

logging.basicConfig(
    format="%(asctime)s - %(message)s", level=logging.INFO,
)

_logger = logging.getLogger(__name__)


class FileIter(Source):
    def __init__(self, file_obj: IO):
        self._file_obj = file_obj

    def pop(self) -> Optional[DataItem]:
        line = next(self._file_obj, None)
        if line is not None:
            line = line.strip()
            # send only non-empty lines of a file to the pipeline
            if line:
                item = DataItem()
                item.payload["_id"] = line
                return item
        else:
            self.stop()


# by using a batch stage we can retrieve multiple items at once, reducing ES calls
class ESRetrieve(BatchStage):
    def __init__(self, es_hosts: str, es_indices: str):
        super().__init__(size=10, timeout=5)
        self._es_indices = es_indices.strip().split(",")
        self._es_hosts = es_hosts
        self._es_client = None
        self._retrieve = None

    def on_start(self):
        self._es_client = Elasticsearch(self._es_hosts)
        # use Elasticsearch mget when a single index is specified
        if len(self._es_indices) == 1 and not self._es_client.indices.exists_alias(
            self._es_indices
        ):
            self._retrieve = self._mget
        else:
            self._retrieve = self._search

    @staticmethod
    def _mget(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        body = {"docs": [{"_id": item.payload["_id"]} for item in items]}
        resp = self._es_client.mget(body=body, index=self._es_indices)
        for i, doc in enumerate(resp["docs"]):
            if "error" not in doc:
                items[i].payload.update(doc)
        return items

    @staticmethod
    def _search(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        query = {"query": {"ids": {"values": [item.payload["_id"] for item in items]}}}
        resp = self._es_client.search(body=query, index=self._es_indices)
        for i, doc in enumerate(resp["hits"]["hits"]):
            items[i].payload.update(doc)
        return items

    def process_batch(self, items: Sequence[DataItem]) -> Sequence[DataItem]:
        return self._retrieve(self, items)


class JsonlDump(Stage):
    def __init__(self, file_obj: IO):
        self._file_obj = file_obj

    def process(self, item: DataItem) -> DataItem:
        self._file_obj.write(f"{json.dumps(item.payload)}\n")
        return item


def get_pipeline(input_file, output_file, es_hosts, es_indices):
    return (
        Pipeline()
        .set_source(FileIter(file_obj=input_file))
        .append_stage(
            "es_retrieve",
            ESRetrieve(es_hosts=es_hosts, es_indices=es_indices),
            concurrency=4,
            parallel=True,
        )
        .append_stage("jsonl_dump", JsonlDump(file_obj=output_file))
        .build()
    )


def main(args):
    with open(args.input) as input_file:
        with open(args.output, "w") as output_file:
            pipeline = get_pipeline(
                input_file=input_file,
                output_file=output_file,
                es_hosts=args.hosts,
                es_indices=args.indices,
            )
            for item in pipeline.run():
                _logger.info(f"Processed {item}")


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "-i", "--input", help="File with a ES document ID per line", required=True
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output jsonl file, one ES document per line",
        required=True,
    )
    parser.add_argument("-e", "--hosts", help="ES hosts", required=True)
    parser.add_argument(
        "-x",
        "--indices",
        help="List of index names or aliases separated by comma",
        default="_all",
    )
    args = parser.parse_args()
    main(args)
