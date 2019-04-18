## SmartPipeline

A framework for developing simple data pipelines.

![pipeline comic](https://imgs.xkcd.com/comics/data_pipeline.png "A pipeline comic")

<sub><sup>from https://xkcd.com</sup></sub>

#### Install

Install the source files directly:

```bash
pip install -e git://github.com/giacbrd/SmartPipeline.git#egg=smartpipeline
```

#### Documentation

soon


#### Examples

Example of a pipeline that processes local files and extract texts and tables.
Pipeline definition and running (assumed that stage classes are defined by the developer)

```python
pipeline = Pipeline()\
    .set_error_manager(ESErrorLogger(es_host='http://localhost:9200', es_index='logging', es_doctype='log')\
    .set_source(LocalFilesSource(samples, postfix='.pdf'))\
    .append_stage('text_extractor', TextExtractor(), concurrency=2)\
    .append_stage('table_extractor', TableExtractor())\
    .append_stage('indexer', Indexer(es_host='http://localhost:9200', es_index='documents', es_doctype='document'))
for item in pipeline.run():
    logger.info('Processed: {}'.format(item))
```

`Error` instances are stage errors that do not interrupt an item processing through the pipeline, 
they have to be explicitly raised.
A `CriticalError` is raised for any non captured exception, or explicitly, 
it stops the processing of an item so the pipeline starts with the next one.

Example of an error manager that logs errors into an Elasticsearch index

```python
class ESErrorLogger(ErrorManager):

    def __init__(self, es_host, es_index, es_doctype):
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
```