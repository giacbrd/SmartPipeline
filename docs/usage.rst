Guide
=====

Data items
----------

The unit of data of a pipeline is the item,
which is represented by :class:`.Item` class or a subclass of it.
Data is kept in the :attr:`.Item.data`, a read-only dictionary.
Other methods allow to enrich an item with metadata, extra stuff as temporary data or descriptors of the item,
so as to isolate in the data all, and only, the data the pipeline produces.
If the pipeline is going to work on more processes (see later the ``parallel`` parameter)
an item must me serializable with `pickle <https://docs.python.org/3/library/pickle.html>`_ module,
both its data and metadata.

Defining the source
-------------------

For defining your own processing pipeline you need to define the source and the stages.
The :class:`.Source` should be extended and the :meth:`.Source.pop` method overridden.
This method returns a single new item every time is called,
it can also return ``None`` if it cannot provide a new item when called.
When the source has no more items to generate,
the method :meth:`.Source.stop` must be called.
A stop can happen when the resources from which we retrieve data items are exhausted,
e.g.: all the documents in a index have been retrieved or
all the lines on CSV file have been read.
A source can also generate items indefinitely, then the pipeline will never end.

In this example we define a source that generates 1000 items with a random variable string in
the data of each one.

.. code-block:: python

    class RandomTextSource(Source):
        def __init__(self, total=1000):
            self._total = total
            self._counter = 0

        def pop(self):
            self._counter += 1
            # when 1000 items are reached we declare a stop,
            # the stop will be still valid for all the next calls of pop()
            if self._counter > self._total:
                self.stop()
                return
            else:
                item = Item()
                text = ''.join(
                    random.choices(
                        string.ascii_letters + string.digits,
                        k=random.randint(50, 200)
                    )
                )
                item.data.update({
                    "text": text,
                    "id": hash(text)
                })
                item.set_metadata('count', self._counter)
                return item

Defining your stages
--------------------

A stage is also defined as a class and created when added to a pipeline.

The stage method :meth:`.Stage.process` (or :meth:`.BatchStage.process_batch`) is where the actual
item data processing happens.
A stage receives a single item and returns it, enriched with stage computations.

A batch stage, instead, processes multiple items at once.
This is useful when the computation can exploit handling more data instead of single items,
e.g.: on a HTTP API that accepts lists of values, one would benefit by making less calls;
on a machine learning model that is optimized for predicting multiple samples.

Concurrent stages will call the method on different subsets of the data flow, concurrently.

Each stage provides its own logger in :attr:`.Stage.logger`.

A simple example of a stage that takes the items generated in the previous example and substitutes specific
patterns in the string with a fixed string.

.. code-block:: python

    class TextReplacer(Stage):
        def __init__(self, substitution):
            self._pattern = re.compile("\d\d\D\D")
            self._sub = substitution

        def process(self, item):
            new_text = re.sub(self._sub, item.data["text"])
            if item.data["text"] == new_text:
                # even if we raise SoftError the item will continue its path through the pipeline
                raise SoftError("Text has not been modified")
            item.data["text"] = new_text
            return item

Error handling
--------------

In the previous code snippet we raise a :class:`.SoftError` in case no modifications are made to the content of the item.
The :class:`.ErrorManager` will take care of this but the item will still be processed by the next stages in the pipeline.
By extending :class:`.ErrorManager` you can define custom handling for these kind of "soft" errors,
but also for all other exceptions.

:class:`.SoftError` exceptions have to be explicitly raised.
A stage soft error does not interrupt an item processing through the pipeline,
the item processing is skipped just for the stage.
Be careful on batch stages: raising a soft error, while iterating on batch items, will make skip
also all the items of the batch following the item that has produced the error.

A :class:`.CriticalError` is raised for any non captured exception, or may be raised explicitly:
it stops the processing of an item so that the pipeline goes ahead with the next one.

It is recommended to use the
`explicit exception chaining <https://www.python.org/dev/peps/pep-3134/#explicit-exception-chaining>`_
when explicitly raising a :class:`.SoftError` or a :class:`.CriticalError` exception.

Setting and running the pipeline
--------------------------------

Once you have your set of stages you can add them in sequence to a Pipeline instance that behave as a "builder".
:meth:`.Pipeline.append_stage` is the main method for adding stages to a pipeline,
it must define their unique names and eventually their concurrency.
The ``concurrency`` parameter is default to 0, a stage is concurrent when the value is 1 or greater.
In case of values greater than 1, and by setting ``parallel`` to ``True``,
Python multiprocessing is used: stage concurrent executions will run in parallel,
thus stage instances will be copied in each process.

Consider using threads when I/O blocking operations are prevalent,
while using multiprocessing when stages execute long computations on data.
In case of no concurrency the pipeline simply runs a "chain" of :meth:`.Stage.process` on each item,
while with concurrency Python queues are involved and items may be serialized.

If you intend to define stages that can run on multiple processes,
please read :ref:`concurrency-section` about further, important details.

Through :meth:`.Pipeline.append_stage` one can also define a retry policy on some specific errors
(see method documentation for further details).

Another method is :meth:`.Pipeline.append_stage_concurrently`,
which allows to execute stages creation concurrently with other stages appending calls.
Useful when long tasks must be executed at creation,
e.g., the stage carries the construction of big data structures.

Remember to call :meth:`.Pipeline.build` at the end of stages "concatenation".

Finally, from the previous example, we define another stage that reduces text size and we run the pipeline

.. code-block:: python

    class TextReducer(Stage):
        def process(self, item):
            item.data["text"] = item.data["text"][:40]
            return item

    pipeline = (
        Pipeline()
        .set_source(RandomTextSource())
        .append_stage("text_replacer", TextReplacer(substitution="XXX"))
        .append_stage("text_reducer", TextReducer())
        .build()
    )

    for item in pipeline.run():
        print(item.data["text"])

A different example in which we process 100 items concurrently with :meth:`.Pipeline.process_async`,
without running the pipeline but explicitly executing a pipeline processing on each one.
Note that no source is defined here.

.. code-block:: python

    pipeline = (
        Pipeline()
        .append_stage("text_replacer", TextReplacer(substitution="XXX"), concurrency=3)
        .append_stage("text_reducer", TextReducer(), concurrency=1)
        .build()
    )
    # "manually" send 100 items to the pipeline
    for _ in range(100):
        item = Item()
        text = ''.join(
            random.choices(
                string.ascii_letters + string.digits,
                k=random.randint(50, 200)
            )
        )
        item.data.update({
            "text": text,
            "id": hash(text)
        })
        pipeline.process_async(item)
    # retrieve the processed items
    for _ in range(100):
        print(pipeline.get_item().data["text"])
    # explicitly stop the pipeline when there are no more items
    pipeline.stop()

It is possible to use :meth:`.Pipeline.process` when no stage is concurrent,
each item will be processed and returned directly by this method.

A further example
-----------------

Example of a pipeline that processes local files contained in ``./document_files`` directory,
extracts texts and finds VAT codes occurrences.
Finally it indexes the result in an Elasticsearch cluster.
Errors are eventually logged in the Elasticsearch cluster.
Here the developer has defined his own custom error manager and obviously the stages.
The source must be usually defined, here a trivial one (from the codebase) has been used,
together with a custom data item type that provides a file reference.

More, executables examples can be found in the root sub-directory ``examples``.

.. code-block:: python

    from smartpipeline.pipeline import Pipeline
    from smartpipeline.stage import Stage, NameMixin
    from smartpipeline.item import Item
    from smartpipeline.error.handling import ErrorManager
    from smartpipeline.error.exceptions import SoftError
    from smartpipeline.helpers import LocalFilesSource, FilePathItem
    from elasticsearch import Elasticsearch
    from typing import Optional
    import logging, re


    class ESErrorLogger(ErrorManager):
        """An error manager that writes error info into an Elasticsearch index"""

        def __init__(self, es_host: str, es_index: str):
            self.es_host = es_host
            self.es_index = es_index
            self.es_client = Elasticsearch(self.es_host)

        def handle(
            self, error: Exception, stage: NameMixin, item: Item
        ) -> Optional[Exception]:
            if isinstance(error, SoftError):
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

        def process(self, item: FilePathItem) -> Item:
            try:
                with open(item.path) as f:
                    item.data["text"] = f.read()
            except IOError as e:
                # even if we are unable to read the file content the item will processed by next stages
                # we encapsulate the exception in a "soft error"
                raise SoftError(f"Problems in reading file {item.path}") from e
            return item


    class VatFinder(Stage):
        """Identify Italian VAT codes in texts"""

        def __init__(self):
            self.regex = re.compile(
                "^[A-Za-z]{2,4}(?=.{2,12}$)[-_\s0-9]*(?:[a-zA-Z][-_\s0-9]*){0,2}$"
            )

        def process(self, item: Item) -> Item:
            vat_codes = []
            for vat_match in self.regex.finditer(item.data.get("text", "")):
                vat_codes.append((vat_match.start(), vat_match.end()))
            item.data["vat_codes"] = vat_codes
            return item


    class Indexer(Stage):
        """Write item payloads into an Elasticsearch index"""

        def __init__(self, es_host: str, es_index: str):
            self.es_host = es_host
            self.es_index = es_index
            self.es_client = Elasticsearch(self.es_host)

        def process(self, item: Item) -> Item:
            self.es_client.index(index=self.es_index, body=item.data)
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
        .build()
    )

    for item in pipeline.run():
        logging.info("Processed document: %s", item)

.. _concurrency-section:

Parallel stages and ``on_start`` method
---------------------------------------

The only way Python allows to run code in parallel is through multiple OS processes, with the package
`multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ (threads cannot run in parallel
because the `GIL <https://en.wikipedia.org/wiki/Global_interpreter_lock>`_).

When we submit a Python function to a spawned/forked process we are actually copying memory from the current process
to the new one, because OS processes cannot share memory, differently from multi-threading.
In order to do this (at least for spawned processes) the data we want to pass to a new process must be serialized.
Even communication between processes involves copying data from one to another (e.g. through queues).
Moreover, for child processes that are not created with "fork" method,
the memory of the parent won't be copied completely.

Therefore, if we decide to run a pipeline stage concurrently and in parallel,
the stage is going to be copied to each process.
This means that the stage must be "pickleable":
serializable with the `pickle <https://docs.python.org/3/library/pickle.html>`_ module.
If we want to define non-serializable attributes in our stage object and run it on more processes,
we must find a way generate these attributes for each object copy in each process.

This is what :meth:`.Stage.on_start` method solves. It is simply used to initialize attributes "a posteriori".
It is normally called after ``__init__``, but in case of execution on multiple processes,
it is called once, on the stage copy, at process start.
This allow stateful stages, locally to each process;
it is also useful for safety and for avoiding copying large data.

Also for :class:`.ErrorManager` it is necessary to define :meth:`.ErrorManager.on_start`,
because the manager must be coupled with a stage when it is copied.

Let's take back the previous examples, the error manger and a stage needs to be modified if we want to run the stage in
parallel. The inconvenience here is the Elasticsearch client,
which is not serializable (try it by yourself, e.g., :code:`pickle.dumps(Elasticsearch('localhost'))`).
Moreover, an Elasticsearch client open a connection, consequently it is obvious we desire an independent connection in
each process, sharing one is unpractical.

This is how we refactor the original ``__init__`` methods

.. code-block:: python

    class ESErrorLogger(ErrorManager):

        def __init__(self, es_host: str, es_index: str):
            self.es_host = es_host
            self.es_index = es_index
            self.es_client = None

        def on_start(self):
            self.es_client = Elasticsearch(self.es_host)


    class Indexer(Stage):

        def __init__(self, es_host: str, es_index: str):
            self.es_host = es_host
            self.es_index = es_index
            self.es_client = None

        def on_start(self):
            self.es_client = Elasticsearch(self.es_host)

The effort for the developer is minimal, but the advantage big.
We can now execute these pipeline abstractions in parallel,
not just stateless methods as we would normally do with multiprocessing.
In general, it is convenient to always override ``on_start`` if attributes we are going to construct require
this special treatment, so that the stage will be always compatible with both three ways of run it: sequentially,
concurrently on threads or on processes.

A complementary method is ``on_end``, both for stages and error manager,
which allows to call operations at pipeline exit, even when this is caused by an error.
Useful, for example, for closing files or connections we have opened in ``on_start``.


Parallel stages and logging
```````````````````````````

The ``on_start`` method is especially useful for configuring stage loggers.

Unfortunately a stage logger configuration, like the log level, and even
the global logging configuration, won't be inherited by stages running on sub-processes
(this actually happens when the fork method is not used for creating child processes).

By defining the logging configuration of :attr:`.Stage.logger` inside the ``on_start`` overriding,
one can solve this issue.