## SmartPipeline

A framework for developing data pipelines following a simple design pattern

![pipeline comic](https://imgs.xkcd.com/comics/data_pipeline.png "A pipeline comic")

<sub><sup>from https://xkcd.com</sup></sub>

#### Short Documentation

Imagine you have to perform several processes on data, a typical *Task Queues* application.
SmartPipeline give you the tools to design and formalize simple data pipelines,
in which tasks are sequentially encapsulated in pipeline stages.

It also give you a lot of features more, like concurrency and parallelization on pipeline stages.

An optimal solution for fast data analysis prototypes that can be immediately ready for production.

##### API

Just instantiate a `Pipeline` and add stages with `append_stage()` or `append_stage_concurrently()` method.
You can set a special source stage that will ingest items into the pipeline by executing `run()`,
or you can process each single item with `process()` or `process_async()`.
In order to obtain the resulting item from the latter `get_item()` must be called.
Several other methods allow to configure the pipeline.

Stages can be defined extending the `Stage` or `BatchStage` classes,
sources by extending the `Source` class.
Batch stages allow to process more items at once.
Different item types can be defined extending the `DataItem` class,
the actual itwm data is carried in its `payload` attribute, a read-only dictionary,
but metadata can be managed as support with the apposite methods.

Specific errors can be generated by stages and managed by the pipeline.
`Error` instances are stage errors that do not interrupt an item processing through the pipeline, 
they have to be explicitly raised.
A `CriticalError` is raised for any non captured exception, or explicitly, 
it stops the processing of an item so the pipeline starts with the next one.

Soon the complete documentation!

#### Install

Install the source files directly (soon on PyPI):

```bash
pip install -e git://github.com/giacbrd/SmartPipeline.git#egg=smartpipeline
```

#### Examples

Example of a pipeline that processes local files contained in a `document_files` directory, 
extracts texts and finds VAT codes occurrences.
Finally it indexes the result in an Elasticsearch cluster.
Errors are eventually logged in the Elasticsearch cluster.

```python
from smartpipeline.pipeline import Pipeline
from smartpipeline.stage import Stage
from smartpipeline.error import ErrorManager, Error
from smartpipeline.helpers import LocalFilesSource, FilePathItem
from elasticsearch import Elasticsearch
import logging, re

class ESErrorLogger(ErrorManager):
    """An error manager that writes error info into an Elasticsearch index"""
    def __init__(self, es_host, es_index):
        super().__init__()
        self.es_client = Elasticsearch(es_host)
        self.es_index = es_index

    def handle(self, error, stage, item):
        super(ESErrorLogger, self).handle(error, stage, item)
        if isinstance(error, Error):
            error = error.get_exception()
        self.es_client.index(index=self.es_index, body={
            'stage': str(stage),
            'item': str(item),
            'exception': type(error),
            'message': str(error)
        })

class TextExtractor(Stage):
    """Read the text content of files"""
    def process(self, item: FilePathItem) -> DataItem:
        try:
            with open(item.path) as f:
                item.payload['text'] = f.read()
        except IOError:
            raise Error(f'Problems in reading file {item.path}')    
        return item

class VatFinder(Stage):
    """Identify Italian VAT codes in texts"""
    def __init__(self):
        self.regex = re.compile('^[A-Za-z]{2,4}(?=.{2,12}$)[-_\s0-9]*(?:[a-zA-Z][-_\s0-9]*){0,2}$')

    def process(self, item: DataItem) -> DataItem:
        vat_codes = []
        for vat_match in self.regex.finditer(item.payload.get('text', '')):
            vat_codes.append((vat_match.start(), vat_match.end()))
        item.payload['vat_codes'] = vat_codes    
        return item

class Indexer(Stage):
    """Write item payloads into an Elasticsearch index"""
    def __init__(self, es_host, es_index):
        self.es_client = Elasticsearch(es_host)
        self.es_index = es_index
    
    def process(self, item: DataItem) -> DataItem:
        self.es_client.index(index=self.es_index, body=item.payload)
        return item

pipeline = Pipeline().set_error_manager(
    ESErrorLogger(es_host='localhost:9200', es_index='error_logs').raise_on_critical_error()
    ).set_source(
        LocalFilesSource('./document_files', postfix='.html')
    ).append_stage(
        'text_extractor', 
        TextExtractor(), concurrency=2
    ).append_stage(
        'vat_finder', 
        VatFinder()
    ).append_stage(
        'indexer', 
        Indexer(es_host='localhost:9200', es_index='documents')
    )

for item in pipeline.run():
    logging.info(f'Processed document: {item}')
```