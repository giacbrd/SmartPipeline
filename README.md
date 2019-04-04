## Smart Pipeline

A simple framework for developing data pipelines.

#### Install

Install the source files directly:

```bash
pip install -e git://github.com/giacbrd/SmartPipeline.git#egg=smartpipeline
```

#### Examples

Process local files and extract texts and tables, finally index them.
Pipeline definition and running (assumed that stage classes are defined by the developer)

```python
pipeline = Pipeline()\
    .set_error_manager(ESErrorLogger(es_host='http://localhost:9200', es_index='logging', es_doctype='log')\
    .set_source(LocalFilesSource(samples, postfix='.pdf'))\
    .append_stage('text_extractor', TextExtractor())\
    .append_stage('table_extractor', TableExtractor())\
    .append_stage('indexer', Indexer(es_host='http://localhost:9200', es_index='documents', es_doctype='document'))
for item in pipeline.run():
    logger.info(item)
```

`Error` are stage errors that do not interrupt an item processing through the pipeline, it has to be explicitly raised.
A `CriticalError` is raised for any non captured exception or explicitly, 
it stop the processing of an item and the pipeline goes to the successive item.

An error manager that logs errors into an Elasticsearch index

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