Tutorial
========

Defining the source
-------------------

For defining your own processing pipeline you need to define the source and the various stages.
The :class:`.Source` should be extended and the :meth:`.Source.pop` method overridden.
This method returns a single new item every time is called,
in cases when no new item is available it can return ``None``.
This usually happens when the source has no more items to generate,
so the method :meth:`.Source.stop` must be called.
A stop can happen when the resources from which we retrieve data items are exhausted,
e.g.: all the documents in a index have been retrieved;
all the lines on CSV file have been read.
A source can also generate items indefinitely, then the pipeline will never end.

In this example we define a source that generates 1000 items with a random variable string in
the payload of each one.

.. code-block:: python

    class RandomTextSource(Source):
        def __init__(self, total=1000):
            self._total = total
            self._counter = 0

        def pop(self):
            self._counter += 1
            # when 1000 items are reached we declare a stop,
            # this will be still valid for all the next calls of pop()
            if self._counter > self._total:
                self.stop()
                return
            else:
                item = DataItem()
                text = ''.join(
                    random.choices(
                        string.ascii_letters + string.digits,
                        k=random.randint(50, 200)
                    )
                )
                item.payload.update({
                    "text": text,
                    "id": hash(text)
                })
                item.set_metadata('count', self._counter)
                return item

Defining your stages
--------------------

The stage method :meth:`.Stage.process` (or :meth:`.BatchStage.process_batch`) is where the actual
item data processing happens.
A stage receives a single item and returns it after processing.
Concurrent stages will call the method on different subsets of the data flow, concurrently.

A simple example of a stage that takes the previously generated items and substitute specific
patterns in the string with a fixed string.

.. code-block:: python

    class TextReplacer(Stage):
        def __init__(self, substitution):
            self._pattern = re.compile("\d\d\D\D")
            self._sub = substitution

        def process(self, item):
            new_text = re.sub(self._sub, item.payload["text"])
            if item.payload["text"] == new_text:
                # even if we raise Error the item will continue its path through the pipeline
                raise Error("Text has not been modified")
            item.payload["text"] = new_text
            return item

We also raise a :class:`.Error` in case no modifications are made to the content of the item.
The :class:`.ErrorManager` will take care of this but the item will be processed by next steps in
the pipeline.
By extending :class:`.ErrorManager` you can define custom handling for these kind of
"soft" errors, but also for :class:`.CriticalError` and all other exceptions.

Setting and running the pipeline
--------------------------------

Once you have your set of stages you can add them in sequence in a Pipeline instance.
:meth:`.Pipeline.append_stage` is the main method for adding stages to a pipeline,
it must define their unique names and eventually their concurrency.
The ``concurrency`` parameter is default to 0, a stage is concurrent when the value is 1 or greater.
In case of values greater than 1, and setting ``use_threads`` to ``False``,
Python multiprocess will be used, stage processing will run in parallel,
thus stages will be copied in each process.

Consider using threads when I/O blocking operations are prevalent,
while using multiprocess when stages execute long computations on data.
In case of no concurrency the pipeline simply run a chain of :meth:`.Stage.process`,
while with concurrency Python queues are involved, so items may be serialized.

Another method is :meth:`.Pipeline.append_stage_concurrently`,
which allows to execute stages creation concurrently with other stages appending.
Useful when the creation is slow,
e.g., the stage carries the construction of big data structures.

Finally, in the example, we define another stage that reduces text size and we run the pipeline

.. code-block:: python

    class TextReducer(Stage):
        def process(self, item):
            item.payload["text"] = item.payload["text"][:40]
            return item

    pipeline = (
        Pipeline()
        .set_source(RandomTextSource())
        .append_stage("text_replacer", TextReplacer(substitution="XXX"))
        .append_stage("text_reducer", TextReducer())
    )

    for item in pipeline.run():
        print(item.payload["text"])

A different example in which we process 100 single items concurrently with :meth:`.Pipeline.process_async`

.. code-block:: python

    pipeline = (
        Pipeline()
        .append_stage("text_replacer", TextReplacer(substitution="XXX"), concurrency=3)
        .append_stage("text_reducer", TextReducer(), concurrency=1)
    )
    # "manually" send 100 items to the pipeline
    for _ in range(100):
        item = DataItem()
        text = ''.join(
            random.choices(
                string.ascii_letters + string.digits,
                k=random.randint(50, 200)
            )
        )
        item.payload.update({
            "text": text,
            "id": hash(text)
        })
        pipeline.process_async(item)
    # retrieve the processed items
    for _ in range(100):
        print(pipeline.get_item().payload["text"])

It is possible to use :meth:`.Pipeline.process` when no stage is concurrent,
each item will be processed and returned directly by the method.

A further example
-------------------

Example of a pipeline that processes local files contained in ``./document_files`` directory,
extracts texts and finds VAT codes occurrences.
Finally it indexes the result in an Elasticsearch cluster.
Errors are eventually logged in the Elasticsearch cluster.
Here the developer has defined his own custom error manager and obviously the stages.
The source must be usually defined, here a straightforward ready one (from the library) has been used,
together with a custom data item type that provide a file reference.

.. code-block:: python

    from smartpipeline.pipeline import Pipeline
    from smartpipeline.stage import Stage, NameMixin
    from smartpipeline.item import DataItem
    from smartpipeline.error.handling import ErrorManager
    from smartpipeline.error.exceptions import Error
    from smartpipeline.helpers import LocalFilesSource, FilePathItem
    from elasticsearch import Elasticsearch
    from typing import Optional
    import logging, re


    class ESErrorLogger(ErrorManager):
        """An error manager that writes error info into an Elasticsearch index"""

        def __init__(self, es_host: str, es_index: str):
            super().__init__()
            self.es_client = Elasticsearch(es_host)
            self.es_index = es_index

        def handle(
            self, error: Exception, stage: NameMixin, item: DataItem
        ) -> Optional[Exception]:
            if isinstance(error, Error):
                error = error.get_exception()
            self.es_client.index(
                index=self.es_index,
                body={
                    "stage": str(stage),
                    "item": str(item),
                    "exception": type(error),
                    "message": str(error),
                },
            )
            return super().handle(error, stage, item)


    class TextExtractor(Stage):
        """Read the text content of files"""

        def process(self, item: FilePathItem) -> DataItem:
            try:
                with open(item.path) as f:
                    item.payload["text"] = f.read()
            except IOError as e:
                # even if we are unable to read the file content the item will processed by next stages
                raise Error(f"Problems in reading file {item.path}").with_exception(e)
            return item


    class VatFinder(Stage):
        """Identify Italian VAT codes in texts"""

        def __init__(self):
            self.regex = re.compile(
                "^[A-Za-z]{2,4}(?=.{2,12}$)[-_\s0-9]*(?:[a-zA-Z][-_\s0-9]*){0,2}$"
            )

        def process(self, item: DataItem) -> DataItem:
            vat_codes = []
            for vat_match in self.regex.finditer(item.payload.get("text", "")):
                vat_codes.append((vat_match.start(), vat_match.end()))
            item.payload["vat_codes"] = vat_codes
            return item


    class Indexer(Stage):
        """Write item payloads into an Elasticsearch index"""

        def __init__(self, es_host: str, es_index: str):
            self.es_client = Elasticsearch(es_host)
            self.es_index = es_index

        def process(self, item: DataItem) -> DataItem:
            self.es_client.index(index=self.es_index, body=item.payload)
            return item


    pipeline = (
        Pipeline()
        .set_error_manager(
            ESErrorLogger(
                es_host="localhost:9200", es_index="error_logs"
            ).raise_on_critical_error()
        )
        .set_source(LocalFilesSource("./document_files", postfix=".html"))
        .append_stage("text_extractor", TextExtractor(), concurrency=2)
        .append_stage("vat_finder", VatFinder())
        .append_stage("indexer", Indexer(es_host="localhost:9200", es_index="documents"))
    )

    for item in pipeline.run():
        logging.info(f"Processed document: {item}")