Guide
=====

Data items
----------

The unit of data in a pipeline is the *item*,
represented by the :class:`.Item` class or a subclass of it.
The actual payload lives in :attr:`.Item.data`, a read-only dictionary,
while :attr:`.Item.metadata` holds auxiliary information such as
temporary values or descriptors of the item.

If a stage runs on multiple processes (see the ``executor`` parameter in
:doc:`concurrency`), items are sent between processes through queues,
so both ``data`` and ``metadata`` must be serializable with the
`pickle <https://docs.python.org/3/library/pickle.html>`_ module.

Defining the source
-------------------

To build your own pipeline you define a source and a sequence of stages.
Extend :class:`.Source` and override :meth:`.Source.pop`.
This method is called repeatedly and must return one new item per call.
It may return ``None`` when no item is currently available,
without ending the stream.
When the source is exhausted, it must call :meth:`.Source.stop`.
Typical cases are: all documents in an index have been read,
or all lines of a CSV file have been consumed.
A source may also produce items indefinitely, in which case the pipeline never ends.

The following example defines a source that generates 1000 items,
each carrying a random string in its data.

.. code-block:: python

    class RandomTextSource(Source):
        def __init__(self, total=1000):
            self._total = total
            self._counter = 0

        def pop(self):
            self._counter += 1
            # once 1000 items have been generated we signal the end;
            # the stop condition stays valid for all subsequent pop() calls
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
                item.metadata['count'] = self._counter
                return item

Defining your stages
--------------------

A stage is a subclass of :class:`.Stage` or :class:`.BatchStage`.

The actual processing happens in :meth:`.Stage.process` for single items,
or in :meth:`.BatchStage.process_batch` for lists of items.

A stage receives one item and returns the same item, enriched by its computation.
A batch stage processes several items at once.
Batching pays off when the underlying operation is cheaper in bulk,
for example an HTTP API that accepts lists of values (fewer calls),
or a machine learning model optimized for predicting on multiple samples.

When a stage is concurrent, its ``process`` method is invoked
on different subsets of the data flow at the same time
(see :doc:`concurrency`).

Each stage exposes its own logger through :attr:`.Stage.logger`.

The following stage takes the items produced by the previous example
and replaces every occurrence of a pattern with a fixed string.

.. code-block:: python

    class TextReplacer(Stage):
        def __init__(self, substitution):
            self._pattern = re.compile(r"\d\d\D\D")
            self._sub = substitution

        def process(self, item):
            new_text = self._pattern.sub(self._sub, item.data["text"])
            if item.data["text"] == new_text:
                # a SoftError skips this stage for the item,
                # but the item still continues through the pipeline
                raise SoftError("Text has not been modified")
            item.data["text"] = new_text
            return item

Error handling
--------------

The snippet above raises a :class:`.SoftError` when the text is unchanged.
The :class:`.ErrorManager` handles the error,
but the item is still passed to the following stages.
By subclassing :class:`.ErrorManager` you can customize how these "soft" errors,
and any other exception, are handled.

:class:`.SoftError` must be raised explicitly.
It skips processing only for the stage that raised it, not for the whole pipeline.
Note the batch-stage caveat: if you raise while iterating over a batch,
the remaining items of that batch are skipped as well.

A :class:`.CriticalError` is raised for any uncaught exception,
or explicitly by the stage code.
It stops the processing of the current item,
and the pipeline moves on to the next one.

When raising :class:`.SoftError` or :class:`.CriticalError` explicitly,
use `explicit exception chaining <https://www.python.org/dev/peps/pep-3134/#explicit-exception-chaining>`_
(``raise ... from ...``) so the original cause is preserved.

Setting and running the pipeline
--------------------------------

Stages are added to a :class:`.Pipeline` instance in sequence, with a builder pattern.
:meth:`.Pipeline.append` is the main method for adding stages.
Each stage needs a unique name, plus an optional concurrency setting.
``concurrency`` defaults to 0, which means inline execution.
A value of 1 or greater makes the stage concurrent.
Combined with ``executor="processes"``, concurrent executions run in separate processes;
otherwise they run in separate threads.
With concurrency, items travel through queues and may be serialized.

As a rule of thumb, use threads when stages are mostly waiting on I/O,
and processes when stages perform heavy computation.
Without concurrency the pipeline is a simple chain of :meth:`.Stage.process` calls,
one item after the other.

If you plan to run stages on multiple processes,
read :doc:`concurrency` first: there are important constraints on what
a stage may hold and how it must be initialized.

:meth:`.Pipeline.append` also accepts a retry policy for selected errors
(see the method documentation for details).

:meth:`.Pipeline.append_concurrently` accepts a stage *class* (plus ``args``/``kwargs``)
instead of an instance, so stage construction runs in the background
while you keep appending other stages.
This is useful when construction itself is slow,
for example when it builds large in-memory data structures.

Always finish the concatenation with :meth:`.Pipeline.build`.

The example below adds a second stage that truncates the text, then runs the pipeline.

.. code-block:: python

    class TextReducer(Stage):
        def process(self, item):
            item.data["text"] = item.data["text"][:40]
            return item

    pipeline = (
        Pipeline()
        .set_source(RandomTextSource())
        .append("text_replacer", TextReplacer(substitution="XXX"))
        .append("text_reducer", TextReducer())
        .build()
    )

    for item in pipeline.run():
        print(item.data["text"])

The next example processes 100 items concurrently with
:meth:`.Pipeline.process_async` (here "async" has nothing to do with ``asyncio``).
No source is set: items are submitted explicitly, one by one.

.. code-block:: python

    pipeline = (
        Pipeline()
        .append("text_replacer", TextReplacer(substitution="XXX"), concurrency=3)
        .append("text_reducer", TextReducer(), concurrency=1)
        .build()
    )
    # "manually" send 100 items into the pipeline
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
    # explicitly stop the pipeline once no more items will arrive
    pipeline.stop()

Use :meth:`.Pipeline.process` only when no stage is concurrent:
the item is processed synchronously and returned directly by the call.

A further example
-----------------

This pipeline processes the local files in the ``./document_files`` directory,
extracts their text, finds VAT code occurrences,
and indexes the result in an Elasticsearch cluster.
Errors are logged to a separate Elasticsearch index
through a custom error manager.
The source used here is a helper shipped with the library,
combined with a custom item type that carries a file reference.

Further runnable examples live in the ``examples`` directory at the repository root.

.. code-block:: python

    from smartpipeline.pipeline import Pipeline
    from smartpipeline.stage import Stage, AliveMixin
    from smartpipeline.item import Item
    from smartpipeline.error.handling import ErrorManager
    from smartpipeline.error.exceptions import SoftError, CriticalError
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
            self, error: Exception, stage: AliveMixin, item: Item
        ) -> Optional[CriticalError]:
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
                # the item still reaches the next stages;
                # we wrap the failure in a "soft error"
                raise SoftError(f"Problems in reading file {item.path}") from e
            return item


    class VatFinder(Stage):
        """Identify Italian VAT codes in texts"""

        def __init__(self):
            self.regex = re.compile(
                r"^[A-Za-z]{2,4}(?=.{2,12}$)[-_\s0-9]*(?:[a-zA-Z][-_\s0-9]*){0,2}$"
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
        .append("text_extractor", TextExtractor(), concurrency=2)
        .append("vat_finder", VatFinder())
        .append("indexer", Indexer(es_host="localhost:9200", es_index="documents"))
        .build()
    )

    for item in pipeline.run():
        logging.info("Processed document: %s", item)
