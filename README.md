## Smart Pipeline

A simple framework for developing data pipelines.

#### Example 

Process local files and extract texts and tables, finally index them.
Pipeline definition and running (assumed that stage classes are defined by the developer)


```
pipeline = Pipeline()\
    .set_error_manager(ErrorLogger(es_host='http://localhost:9200', es_index='logging', es_doctype='log')\
    .set_source(LocalFilesSource(samples))\
    .append_stage('text_extractor', TextExtractor())\
    .append_stage('table_extractor', TableExtractor())\
    .append_stage('indexer', Indexer(es_host='http://localhost:9200', es_index='documents', es_doctype='document'))
for _ in pipeline.run():
    continue
```


`Error` are stage errors that do not interrupt an item processing through the pipeline, it has to be explicitly raised.
A `CriticalError` is raised for any non captured exception or explicitly, 
it stop the processing of an item and the pipeline goes to the successive item.